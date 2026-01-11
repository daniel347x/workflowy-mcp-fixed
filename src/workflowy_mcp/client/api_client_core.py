"""WorkFlowy API client - Core CRUD operations and validation."""

import json
import sys
import os
from typing import Any
from datetime import datetime

import httpx

from ..models import (
    APIConfiguration,
    AuthenticationError,
    NetworkError,
    NodeCreateRequest,
    NodeListRequest,
    NodeNotFoundError,
    NodeUpdateRequest,
    RateLimitError,
    TimeoutError,
    WorkFlowyNode,
)

# Rate limit protection: delay between API operations (seconds)
# Workflowy's rate limits are generous but undefined. Conservative 1.0s works reliably.
# Experimental: 0.25s may work if API can handle it (test cautiously).
API_RATE_LIMIT_DELAY = 0.25  # Reduced from 1.0s - EXPERIMENTAL


def log_event(message: str, component: str = "CLIENT") -> None:
    """Log an event to stderr with timestamp and consistent formatting."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 🗡️ prefix makes it easy to grep/spot in the console
    print(f"[{timestamp}] 🗡️ [{component}] {message}", file=sys.stderr, flush=True)


def _log(message: str, component: str = "CLIENT") -> None:
    """Unified log wrapper used throughout this client.

    This ensures ALL logging goes through the same DAGGER+DATETIME+TAG
    prefix and uses plain print(..., file=sys.stderr), which reliably
    surfaces in the MCP connector console (unlike the standard
    logging module, which FastMCP eats).
    """
    log_event(message, component)


class _ClientLogger:
    """Lightweight logger that delegates to _log / log_event.

    This replaces logger.info/logger.warning/logger.error in this file
    without relying on Python's logging module (which is swallowed by
    FastMCP). Methods accept arbitrary *args/**kwargs for compatibility
    but only the first message argument is used.
    """

    def __init__(self, component: str = "CLIENT") -> None:
        self._component = component

    def _msg(self, msg: object) -> str:
        try:
            return str(msg)
        except Exception:
            return repr(msg)

    def info(self, msg: object, *args: object, **kwargs: object) -> None:  # noqa: D401
        """Info-level log (no explicit level tag; message already descriptive)."""
        _log(self._msg(msg), self._component)

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"WARNING: {self._msg(msg)}", self._component)

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"ERROR: {self._msg(msg)}", self._component)

    def debug(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"DEBUG: {self._msg(msg)}", self._component)

    def exception(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"EXCEPTION: {self._msg(msg)}", self._component)


class WorkFlowyClientCore:
    """Core WorkFlowy API client - CRUD operations and validation."""

    def __init__(self, config: APIConfiguration):
        """Initialize the WorkFlowy API client."""
        self.config = config
        self.base_url = config.base_url
        self._client: httpx.AsyncClient | None = None

        # /nodes-export cache and dirty tracking.
        # _nodes_export_cache stores the last /nodes-export payload (flat nodes list).
        # _nodes_export_dirty_ids holds UUIDs whose subtrees/ancestors have been
        # mutated since the last refresh. A "*" entry means "treat everything as dirty".
        self._nodes_export_cache: dict[str, Any] | None = None
        self._nodes_export_cache_timestamp = None
        self._nodes_export_dirty_ids: set[str] = set()
        # Cache maintenance mode: "lazy" (mark-dirty only) vs "eager" (patch cache when possible).
        # Currently mainly used for logging/diagnostics; helpers below always
        # respect the presence/absence of an actual cache snapshot.
        self._nodes_export_cache_mode: str = "eager"
    
    def _log_debug(self, message: str) -> None:
        """Log debug messages to stderr (unified logging)."""
        # Console Visibility ONLY - keep reconcile_debug.log clean for weaves
        log_event(message, "CLIENT_DEBUG")

    @staticmethod
    def _segment_whitelisted_markup(text: str) -> list[dict[str, str]]:
        """Segment text into plain-text vs whitelisted markup ranges.

        Only *properly matched* whitelisted tags are treated as markup:
        - <b>..</b>, <i>..</i>, <s>..</s>, <code>..</code> with NO attributes
        - <span class="colored c-...">..</span> (exact class pattern)

        Any unmatched tags, unknown tags, or spans with other attributes are
        treated as plain text and will be escaped by the normal whitener.

        Returns a list of segments like:
            [{"kind": "text", "value": "..."},
             {"kind": "markup", "value": "<b>foo</b>"}, ...]
        """
        import re

        if not text:
            return [{"kind": "text", "value": text}]

        tag_pattern = re.compile(r'<(/?)(span|code|b|i|s)([^>]*)>', re.IGNORECASE)
        stack: list[dict[str, object]] = []
        candidates: list[tuple[int, int]] = []

        for m in tag_pattern.finditer(text):
            slash, tag, attrs = m.group(1), m.group(2).lower(), m.group(3) or ""
            start, end = m.start(), m.end()

            if slash:  # closing tag
                if not stack:
                    continue
                top = stack[-1]
                if top["tag"] != tag:
                    continue
                top = stack.pop()
                if top.get("allowed"):
                    # Record full range from opening '<tag>' start to closing '</tag>' end
                    candidates.append((int(top["start"]), end))
            else:  # opening tag
                allowed = False
                attrs_stripped = attrs.trim() if hasattr(attrs, 'trim') else attrs.strip()
                if tag in {"b", "i", "s", "code"}:
                    # Only accept markup if there are *no* attributes
                    allowed = attrs_stripped == ""
                elif tag == "span":
                    # Allow any span that carries a 'colored' class; precise color
                    # matching / auto-wrap is handled later in the whitening stage.
                    if "colored" in attrs_stripped:
                        allowed = True
                stack.append({"tag": tag, "start": start, "allowed": allowed})

        if not candidates:
            segments = [{"kind": "text", "value": text}]
        else:
            # Reduce to non-overlapping outermost ranges
            candidates.sort(key=lambda r: r[0])
            merged: list[tuple[int, int]] = []
            last_end = -1
            for start, end in candidates:
                if start >= last_end:
                    merged.append((start, end))
                    last_end = end
                else:
                    # Nested or overlapping range; outermost already captured, so skip
                    continue

            segments: list[dict[str, str]] = []
            pos = 0
            for start, end in merged:
                if start > pos:
                    segments.append({"kind": "text", "value": text[pos:start]})
                segments.append({"kind": "markup", "value": text[start:end]})
                pos = end
            if pos < len(text):
                segments.append({"kind": "text", "value": text[pos:]})

        return segments

    @staticmethod
    def _extract_whitener_mode(value: str | None) -> tuple[str | None, str | None]:
        """Detect and strip per-value WORKFLOWY_WHITENER_MODE tokens.

        Pattern (anywhere in the string):
            WORKFLOWY_WHITENER_MODE=<mode>

        Recognized modes:
            raw    – bypass whitener for this value only
            normal – explicit no-op (whitener still runs)

        Returns:
            (mode, cleaned_value)
            mode is None if no token or unrecognized mode.
        """
        if value is None:
            return (None, None)

        import re

        text = value
        pattern = re.compile(r"WORKFLOWY_WHITENER_MODE\s*=\s*([A-Za-z0-9_\-]+)")
        modes = pattern.findall(text)
        if not modes:
            return (None, text)

        mode = modes[-1].strip().lower()
        cleaned = pattern.sub("", text).strip()

        if mode not in {"raw", "normal"}:
            # Unrecognized mode: treat as no special behavior but still
            # remove the token so it doesn't leak into Workflowy.
            return (None, cleaned)

        return (mode, cleaned)

    @staticmethod
    def _validate_note_field(note: str | None, skip_newline_check: bool = False) -> tuple[str | None, str | None]:
        """Validate and smart-escape note field for Workflowy compatibility.
        
        Workflowy GUI rendering (Dec 2025):
        - Whitelisted XML tags render correctly: <b>, <i>, <s>, <code>, and
          selected <span class="colored c-...">…</span>
        - <a href> tags cause rendering failures
        - Bare/unpaired brackets (< or >) cause content to vanish
        
        New behavior with parser:
        - Properly matched whitelisted tags are preserved as markup ranges
        - EVERYTHING ELSE (including stray <b>, non-whitelisted tags, and
          arbitrary <foo>) is treated as text and has < and > escaped
        
        This guarantees that discussionary text like `/file-<tag>.json` and
        unmatched `<b>` from notes remain visible as literal text instead of
        breaking rendering.
        """
        if note is None:
            return (None, None)

        # Per-node whitener override via inline token in the NOTE text
        mode, clean_note = WorkFlowyClientCore._extract_whitener_mode(note)
        if mode == "raw":
            return (clean_note, None)
        if clean_note is not None:
            note = clean_note

        text = note
        segments = WorkFlowyClientCore._segment_whitelisted_markup(text)

        import re
        wrapped_segments = []
        for seg in segments:
            if seg["kind"] == "markup":
                v = seg["value"]
                if re.fullmatch(r'<span\s+class="colored\s+(?:c|bc)-[^"]+">.*?</span>', v):
                    seg = {"kind": "markup", "value": f"<b>{v}</b>"}
            wrapped_segments.append(seg)
        segments = wrapped_segments

        result_chars: list[str] = []
        for seg in segments:
            if seg["kind"] == "markup":
                # Preserve whitelisted markup exactly as-is
                result_chars.append(seg["value"])
            else:
                # Plain text: escape < and > so they can't break rendering
                for ch in seg["value"]:
                    if ch == '<':
                        result_chars.append('&lt;')
                    elif ch == '>':
                        result_chars.append('&gt;')
                    else:
                        result_chars.append(ch)

        escaped_note = ''.join(result_chars)
        return (escaped_note, None)
    
    @staticmethod
    def _validate_name_field(name: str | None) -> tuple[str | None, str | None]:
        """Validate and smart-escape name field for Workflowy compatibility.
        
        Workflowy NAME field behavior (Dec 2025):
        - API decodes entities ONCE on input before storage
        - GUI decodes entities AGAIN when rendering
        - Result: Must DOUBLE-ENCODE for proper display
        
        New behavior with parser:
        - Properly matched whitelisted tags (<b>/<i>/<s>/<code> and allowed
          <span class="colored c-...">) are preserved as markup ranges
        - EVERYTHING ELSE is treated as plain text and has < and > escaped
        - Then we double-encode '&' across the entire string so that a
          round-trip API+GUI decode yields the intended characters
        """
        if name is None:
            return (None, None)

        # Per-node whitener override via inline token in the NAME text
        mode, clean_name = WorkFlowyClientCore._extract_whitener_mode(name)
        if mode == "raw":
            return (clean_name, None)
        if clean_name is not None:
            name = clean_name

        text = name
        segments = WorkFlowyClientCore._segment_whitelisted_markup(text)

        import re
        wrapped_segments = []
        for seg in segments:
            if seg["kind"] == "markup":
                v = seg["value"]
                if re.fullmatch(r'<span\s+class="colored\s+(?:c|bc)-[^"]+">.*?</span>', v):
                    seg = {"kind": "markup", "value": f"<b>{v}</b>"}
            wrapped_segments.append(seg)
        segments = wrapped_segments

        # SINGLE-STAGE: escape &, <, > in text segments only; leave markup untouched
        result_chars: list[str] = []
        for seg in segments:
            if seg["kind"] == "markup":
                result_chars.append(seg["value"])
            else:
                for ch in seg["value"]:
                    if ch == '&':
                        result_chars.append('&amp;')
                    elif ch == '<':
                        result_chars.append('&lt;')
                    elif ch == '>':
                        result_chars.append('&gt;')
                    else:
                        result_chars.append(ch)

        escaped_name = ''.join(result_chars)
        return (escaped_name, None)

    @staticmethod
    def _dewhiten_text(value: str | None) -> str | None:
        """Single-pass dewhitening for SCRY (/nodes-export) names and notes.

        Decodes &amp;, &lt;, &gt; once to recover semantic text. We *only* touch
        these three entities so that other entities (e.g. &nbsp;) remain literal.
        """
        if value is None or not isinstance(value, str):
            return value
        return value.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            headers = {
                "Authorization": f"Bearer {self.config.api_key.get_secret_value()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=headers,
                timeout=httpx.Timeout(self.config.timeout),
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "WorkFlowyClientCore":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def _handle_response(self, response: httpx.Response) -> dict[str, Any]:
        """Handle API response and errors."""
        if response.status_code == 401:
            raise AuthenticationError("Invalid API key or unauthorized access")

        if response.status_code == 404:
            raise NodeNotFoundError(
                node_id=response.request.url.path.split("/")[-1], message="Resource not found"
            )

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            raise RateLimitError(retry_after=int(retry_after) if retry_after else None)

        if response.status_code >= 500:
            raise NetworkError(f"Server error: {response.status_code}")

        if response.status_code >= 400:
            try:
                error_data = response.json()
                message = error_data.get("error", "API request failed")
            except (json.JSONDecodeError, KeyError):
                message = f"API error: {response.status_code}"
            raise NetworkError(message)

        try:
            return response.json()  # type: ignore[no-any-return]
        except json.JSONDecodeError as err:
            raise NetworkError("Invalid response format from API") from err

    def _mark_nodes_export_dirty(self, node_ids: list[str] | None = None) -> None:
        """Mark parts of the cached /nodes-export snapshot as dirty.

        When the cache is populated, this is used by mutating operations to
        record which UUIDs (or entire regions via "*") have changed since the
        last refresh. Subsequent export_nodes(...) calls can decide whether
        they can safely reuse the cached snapshot for a given subtree or must
        re-fetch from the API.
        """
        # If there is no cache, there's nothing to mark.
        if self._nodes_export_cache is None:
            return

        # node_ids=None is the conservative "everything is dirty" sentinel.
        if node_ids is None:
            self._nodes_export_dirty_ids.add("*")
            return

        for nid in node_ids:
            if nid:
                self._nodes_export_dirty_ids.add(nid)

    def _get_nodes_export_cache_nodes(self) -> list[dict[str, Any]]:
        """Return the flat `nodes` list from the /nodes-export cache or raise.

        These helpers are intentionally strict: if the cache is missing or
        malformed, callers should fall back to marking regions dirty and let
        export_nodes_impl(...) refresh from the API.
        """
        if self._nodes_export_cache is None:
            raise RuntimeError("nodes-export cache is not initialized")
        nodes = self._nodes_export_cache.get("nodes")
        if not isinstance(nodes, list):
            raise RuntimeError("nodes-export cache is missing 'nodes' list")
        return nodes

    async def update_cached_node_name(self, node_id: str, new_name: str) -> bool:
        """Update the name for a single node in the cached /nodes-export snapshot.

        This does NOT call the Workflowy API. It only patches the in-memory
        cache so that subsequent operations (auto-beacon, Cartographer, etc.)
        see the up-to-date name/tags without requiring a full /nodes-export
        refresh.

        Returns True if a node was updated, False if the cache is missing
        or node_id was not found.
        """
        if self._nodes_export_cache is None:
            self._log_debug(
                f"update_cached_node_name: no cache present, cannot update {node_id}"
            )
            return False

        try:
            nodes = self._get_nodes_export_cache_nodes()
        except Exception as exc:  # pragma: no cover - defensive logging
            self._log_debug(
                f"update_cached_node_name: cache malformed when updating {node_id}: {exc}"
            )
            return False

        target: dict[str, Any] | None = None
        for n in nodes:
            if str(n.get("id")) == str(node_id):
                target = n
                break

        if target is None:
            self._log_debug(
                f"update_cached_node_name: node_id {node_id} not found in cache"
            )
            return False

        # Cache stores dewhitened semantic text; node_name from DOM is already semantic.
        target["name"] = new_name
        if "nm" in target:
            target["nm"] = new_name

        return True

    def _normalize_node_for_nodes_export_cache(self, node_dict: dict[str, Any]) -> dict[str, Any]:
        """Normalize a node dict for insertion into the /nodes-export cache.

        - Ensures `id` is a string.
        - Dewhitens name/nm and note/no so cache stays in semantic space,
          matching the behavior of export_nodes_impl(...).
        """
        normalized: dict[str, Any] = dict(node_dict) if node_dict is not None else {}
        node_id = normalized.get("id")
        if node_id is not None:
            normalized["id"] = str(node_id)
        for key in ("name", "nm", "note", "no"):
            if key in normalized:
                normalized[key] = self._dewhiten_text(normalized.get(key))  # type: ignore[arg-type]
        return normalized

    def _apply_create_to_nodes_export_cache(self, node: WorkFlowyNode) -> None:
        """Eagerly insert a newly created node into the /nodes-export cache."""
        nodes = self._get_nodes_export_cache_nodes()
        node_dict = self._normalize_node_for_nodes_export_cache(node.model_dump())
        nodes.append(node_dict)
        # New node is now represented; clear any stale dirty flag for it.
        if node.id in self._nodes_export_dirty_ids:
            self._nodes_export_dirty_ids.discard(node.id)

    def _apply_update_to_nodes_export_cache(self, node: WorkFlowyNode) -> None:
        """Eagerly update an existing node inside the /nodes-export cache.

        Raises RuntimeError if the node cannot be found so callers can mark
        the path dirty and fall back to a full /nodes-export refresh.
        """
        nodes = self._get_nodes_export_cache_nodes()
        node_id = node.id
        updated = self._normalize_node_for_nodes_export_cache(node.model_dump())
        for idx, existing in enumerate(nodes):
            if str(existing.get("id")) == node_id:
                # Preserve any extraneous keys from the existing snapshot but
                # overwrite core fields from the fresh node state.
                merged = dict(existing)
                merged.update(updated)
                nodes[idx] = merged
                if node_id in self._nodes_export_dirty_ids:
                    self._nodes_export_dirty_ids.discard(node_id)
                return
        # Node not present in cache – signal caller to fall back to dirty flag.
        self._log_debug(f"_apply_update_to_nodes_export_cache: node {node_id} not in cache; marking dirty")
        raise RuntimeError(f"nodes-export cache missing node {node_id}")

    def _apply_delete_to_nodes_export_cache(self, node_id: str) -> None:
        """Eagerly remove a deleted node (and its descendants) from the cache."""
        nodes = self._get_nodes_export_cache_nodes()
        # Build parent->children index once for this snapshot.
        children_by_parent: dict[str, list[str]] = {}
        for n in nodes:
            nid = n.get("id")
            if not nid:
                continue
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                children_by_parent.setdefault(str(pid), []).append(str(nid))

        # Collect all descendants of node_id.
        target_id = str(node_id)
        to_remove: set[str] = {target_id}
        stack = [target_id]
        visited: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            for child_id in children_by_parent.get(cur, []):
                if child_id not in to_remove:
                    to_remove.add(child_id)
                    stack.append(child_id)

        if len(to_remove) == 1 and all(str(n.get("id")) != target_id for n in nodes):
            # Nothing to remove; treat as soft failure so caller can mark dirty.
            self._log_debug(f"_apply_delete_to_nodes_export_cache: node {node_id} not in cache; marking dirty")
            raise RuntimeError(f"nodes-export cache missing node {node_id}")

        new_nodes = [n for n in nodes if str(n.get("id")) not in to_remove]
        self._nodes_export_cache["nodes"] = new_nodes
        # Clear dirty flag for the deleted node itself; ancestors may still be dirty.
        if target_id in self._nodes_export_dirty_ids:
            self._nodes_export_dirty_ids.discard(target_id)

    def _apply_move_to_nodes_export_cache(self, node_id: str, new_parent_id: str | None) -> None:
        """Eagerly update parent_id for a moved node in the cache."""
        nodes = self._get_nodes_export_cache_nodes()
        target_id = str(node_id)
        found = False
        for n in nodes:
            if str(n.get("id")) == target_id:
                n["parent_id"] = new_parent_id
                if "parentId" in n:
                    n["parentId"] = new_parent_id
                found = True
                break
        if not found:
            self._log_debug(f"_apply_move_to_nodes_export_cache: node {node_id} not in cache; marking dirty")
            raise RuntimeError(f"nodes-export cache missing node {node_id}")
        # Clear dirty flags for the moved node; parents may still be marked dirty separately.
        if target_id in self._nodes_export_dirty_ids:
            self._nodes_export_dirty_ids.discard(target_id)

    async def refresh_nodes_export_cache(self, max_retries: int = 10) -> dict[str, Any]:
        """Force a fresh /nodes-export call and update the in-memory cache.

        This is exposed via an MCP tool so Dan (or an agent) can explicitly
        refresh the snapshot used by UUID Navigator and NEXUS without waiting
        for an auto-refresh trigger.
        """
        # Import here to avoid circular dependency
        from .api_client_etch import export_nodes_impl

        # Clear any previous cache and dirty markers first.
        self._nodes_export_cache = None
        self._nodes_export_cache_timestamp = None
        self._nodes_export_dirty_ids.clear()

        # Delegate to export_nodes with caching disabled for this call.
        data = await export_nodes_impl(
            self,
            node_id=None,
            max_retries=max_retries,
            use_cache=False,
            force_refresh=True,
            _allow_api_refresh=True,
        )
        nodes = data.get("nodes", []) or []

        return {
            "success": True,
            "node_count": len(nodes),
            "timestamp": datetime.now().isoformat(),
        }

    async def create_node(
        self, request: NodeCreateRequest, _internal_call: bool = False, max_retries: int = 10
    ) -> WorkFlowyNode:
        """Create a new node in WorkFlowy with exponential backoff retry.
        
        Args:
            request: Node creation request
            _internal_call: Internal flag - bypasses single-node forcing function (not exposed to MCP)
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio

        logger = _ClientLogger()

        # Check for single-node override token (skip if internal call)
        if not _internal_call:
            SINGLE_NODE_TOKEN = "<<<I_REALLY_NEED_SINGLE_NODE>>>"
            
            if request.name and request.name.startswith(SINGLE_NODE_TOKEN):
                # Strip token and proceed
                request.name = request.name.replace(SINGLE_NODE_TOKEN, "", 1)
            else:
                # Suggest ETCH instead
                raise NetworkError("""⚠️ PREFER ETCH - Use workflowy_etch for consistency and capability

You called workflowy_create_single_node, but workflowy_etch has identical performance.

✅ RECOMMENDED (same speed, more capability):
  workflowy_etch(
    parent_id="...",
    nodes=[{"name": "Your node", "note": "...", "children": []}]
  )

📚 Benefits of ETCH:
  - Same 1 tool call (no performance difference)
  - Validation and auto-escaping built-in
  - Works for 1 node or 100 nodes (consistent pattern)
  - Trains you to think in tree structures

⚙️ OVERRIDE (if you truly need single-node operation):
  workflowy_create_single_node(
    name="<<<I_REALLY_NEED_SINGLE_NODE>>>Your node",
    ...
  )

🎯 Build the ETCH habit - it's your go-to tool!
""")
        
        # Validate and escape name field
        processed_name, name_warning = self._validate_name_field(request.name)
        if processed_name is not None:
            request.name = processed_name
        if name_warning:
            logger.info(name_warning)
        
        # Validate and escape note field
        # Skip newline check if internal call (for bulk operations testing)
        processed_note, note_warning = self._validate_note_field(request.note, skip_newline_check=_internal_call)
        
        if processed_note is None and note_warning:  # Blocking error
            raise NetworkError(note_warning)
        
        # Strip override token if present
        if processed_note and processed_note.startswith("<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>"):
            processed_note = processed_note.replace("<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>", "", 1)
        
        # Use processed (escaped) note
        request.note = processed_note
        
        # Log warning if escaping occurred
        if note_warning and "AUTO-ESCAPED" in note_warning:
            logger.info(note_warning)

        retry_count = 0
        base_delay = 1.0

        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)

            try:
                response = await self.client.post("/nodes/", json=request.model_dump(exclude_none=True))
                data = await self._handle_response(response)
                # Create endpoint returns just {"item_id": "..."}
                item_id = data.get("item_id")
                if not item_id:
                    raise NetworkError(f"Invalid response from create endpoint: {data}")

                # Fetch the created node to get actual saved state (including note field)
                get_response = await self.client.get(f"/nodes/{item_id}")
                node_data = await self._handle_response(get_response)
                node = WorkFlowyNode(**node_data["node"])

                # Best-effort: eagerly maintain the /nodes-export cache when present.
                try:
                    if self._nodes_export_cache is not None:
                        self._apply_create_to_nodes_export_cache(node)
                    else:
                        self._mark_nodes_export_dirty([node.id])
                except Exception:
                    # Cache maintenance must never affect API behavior; fall back to dirty flag.
                    try:
                        self._mark_nodes_export_dirty([node.id])
                    except Exception:
                        pass

                return node

            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on create_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise

            except NetworkError as e:
                retry_count += 1
                _log(
                    f"Network error on create_node: {e}. Retry {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise

            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("create_node") from err

        raise NetworkError("create_node failed after maximum retries")

    async def update_node(self, node_id: str, request: NodeUpdateRequest, max_retries: int = 10) -> WorkFlowyNode:
        """Update an existing node with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to update
            request: Node update request
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio

        logger = _ClientLogger()
        
        # Validate and escape name field if being updated
        if request.name is not None:
            processed_name, name_warning = self._validate_name_field(request.name)
            if processed_name is not None:
                request.name = processed_name
            if name_warning:
                logger.info(name_warning)
        
        # Validate and escape note field if being updated
        if request.note is not None:
            # Note: update_node doesn't have _internal_call flag yet, always validates
            processed_note, message = self._validate_note_field(request.note)
            
            if processed_note is None and message:  # Blocking error
                raise NetworkError(message)
            
            # Strip override token if present
            if processed_note and processed_note.startswith("<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>"):
                processed_note = processed_note.replace("<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>", "", 1)
            
            # Use processed (escaped) note
            request.note = processed_note
            
            # Log warning if escaping occurred
            if message and "AUTO-ESCAPED" in message:
                logger.info(message)
        
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
            try:
                response = await self.client.post(
                    f"/nodes/{node_id}", json=request.model_dump(exclude_none=True)
                )
                data = await self._handle_response(response)
                # API returns {"status": "ok"} - fetch updated node
                if isinstance(data, dict) and data.get('status') == 'ok':
                    get_response = await self.client.get(f"/nodes/{node_id}")
                    node_data = await self._handle_response(get_response)
                    node = WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    node = WorkFlowyNode(**data)

                # Best-effort: eagerly maintain the /nodes-export cache when possible.
                try:
                    if self._nodes_export_cache is not None:
                        self._apply_update_to_nodes_export_cache(node)
                    else:
                        self._mark_nodes_export_dirty([node_id])
                except Exception:
                    # Cache maintenance must never affect API behavior; fall back to dirty flag.
                    try:
                        self._mark_nodes_export_dirty([node_id])
                    except Exception:
                        pass

                return node
                    
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on update_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise
                    
            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on update_node: {e}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise
                    
            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("update_node") from err
        
        raise NetworkError("update_node failed after maximum retries")

    async def get_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Retrieve a specific node by ID with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to retrieve
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
            try:
                response = await self.client.get(f"/nodes/{node_id}")
                data = await self._handle_response(response)
                # API returns {"node": {...}} structure
                if isinstance(data, dict) and "node" in data:
                    return WorkFlowyNode(**data["node"])
                else:
                    # Fallback for unexpected format
                    return WorkFlowyNode(**data)
                    
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on get_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise
                    
            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on get_node: {e}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise
                    
            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("get_node") from err
        
        raise NetworkError("get_node failed after maximum retries")

    async def list_nodes(self, request: NodeListRequest, max_retries: int = 10) -> tuple[list[WorkFlowyNode], int]:
        """List nodes with optional filtering and exponential backoff retry.
        
        Args:
            request: Node list request
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
            try:
                # exclude_none=True ensures parent_id is omitted entirely for root nodes
                # (API requires absence of parameter, not null value)
                # Build params manually to ensure snake_case (API expects parent_id not parentId)
                params = {}
                if request.parentId is not None:
                    params['parent_id'] = request.parentId
                response = await self.client.get("/nodes", params=params)
                response_data: list[Any] | dict[str, Any] = await self._handle_response(response)

                # Assuming API returns an array of nodes directly
                # (Need to verify actual response structure)
                nodes: list[WorkFlowyNode] = []
                if isinstance(response_data, dict):
                    if "nodes" in response_data:
                        nodes = [WorkFlowyNode(**node_data) for node_data in response_data["nodes"]]
                elif isinstance(response_data, list):
                    nodes = [WorkFlowyNode(**node_data) for node_data in response_data]

                total = len(nodes)  # API doesn't provide a total count
                return nodes, total
                
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on list_nodes. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise
                    
            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on list_nodes: {e}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise
                    
            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("list_nodes") from err
        
        raise NetworkError("list_nodes failed after maximum retries")

    async def delete_node(self, node_id: str, max_retries: int = 10) -> bool:
        """Delete a node and all its children with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to delete
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio
        from .api_client_etch import _log_to_file_helper

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
            try:
                response = await self.client.delete(f"/nodes/{node_id}")
                # Delete endpoint returns just a message, not nested data
                await self._handle_response(response)
                # If we reached here after one or more retries, log success to reconcile log
                if retry_count > 0:
                    success_msg = (
                        f"delete_node {node_id} succeeded after {retry_count + 1}/{max_retries} attempts "
                        f"following rate limiting or transient errors."
                    )
                    logger.info(success_msg)
                    _log_to_file_helper(success_msg, "reconcile")

                # Best-effort: eagerly maintain the /nodes-export cache when possible.
                try:
                    if self._nodes_export_cache is not None:
                        self._apply_delete_to_nodes_export_cache(node_id)
                    else:
                        self._mark_nodes_export_dirty([node_id])
                except Exception:
                    try:
                        self._mark_nodes_export_dirty([node_id])
                    except Exception:
                        pass

                return True
                
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                retry_msg = (
                    f"Rate limited on delete_node {node_id}. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                logger.warning(retry_msg)
                _log_to_file_helper(retry_msg, "reconcile")
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    final_msg = (
                        f"delete_node {node_id} exhausted retries ({retry_count}/{max_retries}) "
                        f"due to rate limiting – aborting."
                    )
                    logger.error(final_msg)
                    _log_to_file_helper(final_msg, "reconcile")
                    raise
                    
            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on delete_node: {e}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise
                    
            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("delete_node") from err
        
        raise NetworkError("delete_node failed after maximum retries")

    async def complete_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Mark a node as completed with exponential backoff retry."""
        import asyncio

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0

        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)

            try:
                response = await self.client.post(f"/nodes/{node_id}/complete")
                data = await self._handle_response(response)
                # API returns {"status": "ok"} - fetch updated node
                if isinstance(data, dict) and data.get('status') == 'ok':
                    get_response = await self.client.get(f"/nodes/{node_id}")
                    node_data = await self._handle_response(get_response)
                    node = WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    node = WorkFlowyNode(**data)

                # Best-effort: eagerly patch completion state into /nodes-export cache.
                try:
                    if self._nodes_export_cache is not None:
                        self._apply_update_to_nodes_export_cache(node)
                    else:
                        self._mark_nodes_export_dirty([node_id])
                except Exception:
                    try:
                        self._mark_nodes_export_dirty([node_id])
                    except Exception:
                        pass

                return node

            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on complete_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise

            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on complete_node: {e}. Retry {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise

            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("complete_node") from err

        raise NetworkError("complete_node failed after maximum retries")

    async def uncomplete_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Mark a node as not completed with exponential backoff retry."""
        import asyncio

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0

        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)

            try:
                response = await self.client.post(f"/nodes/{node_id}/uncomplete")
                data = await self._handle_response(response)
                # API returns {"status": "ok"} - fetch updated node
                if isinstance(data, dict) and data.get('status') == 'ok':
                    get_response = await self.client.get(f"/nodes/{node_id}")
                    node_data = await self._handle_response(get_response)
                    node = WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    node = WorkFlowyNode(**data)

                # Best-effort: eagerly patch completion state into /nodes-export cache.
                try:
                    if self._nodes_export_cache is not None:
                        self._apply_update_to_nodes_export_cache(node)
                    else:
                        self._mark_nodes_export_dirty([node_id])
                except Exception:
                    try:
                        self._mark_nodes_export_dirty([node_id])
                    except Exception:
                        pass

                return node

            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on uncomplete_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise

            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on uncomplete_node: {e}. Retry {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise

            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("uncomplete_node") from err

        raise NetworkError("uncomplete_node failed after maximum retries")

    async def move_node(
        self,
        node_id: str,
        parent_id: str | None = None,
        position: str = "top",
        max_retries: int = 10,
    ) -> bool:
        """Move a node to a new parent with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to move
            parent_id: The new parent node ID (UUID, target key like 'inbox', or None for root)
            position: Where to place the node ('top' or 'bottom', default 'top')
            max_retries: Maximum retry attempts (default 10)
            
        Returns:
            True if move was successful
        """
        import asyncio

        logger = _ClientLogger()
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force delay at START of each iteration (rate limit protection)
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
            try:
                payload = {"position": position}
                if parent_id is not None:
                    payload["parent_id"] = parent_id
                
                response = await self.client.post(f"/nodes/{node_id}/move", json=payload)
                data = await self._handle_response(response)
                # API returns {"status": "ok"}
                success = data.get("status") == "ok"

                if success:
                    # Best-effort: eagerly maintain the /nodes-export cache when possible.
                    try:
                        if self._nodes_export_cache is not None:
                            self._apply_move_to_nodes_export_cache(node_id, parent_id)
                        else:
                            ids: list[str] = [node_id]
                            if parent_id is not None:
                                ids.append(parent_id)
                            self._mark_nodes_export_dirty(ids)
                    except Exception:
                        # Cache maintenance must never affect API behavior; fall back to dirty flags.
                        try:
                            ids = [node_id]
                            if parent_id is not None:
                                ids.append(parent_id)
                            self._mark_nodes_export_dirty(ids)
                        except Exception:
                            pass

                return success
                
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                logger.warning(
                    f"Rate limited on move_node. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    raise
                    
            except NetworkError as e:
                retry_count += 1
                logger.warning(
                    f"Network error on move_node: {e}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise
                    
            except httpx.TimeoutException as err:
                retry_count += 1
                
                logger.warning(
                    f"Timeout error: {err}. Retry {retry_count}/{max_retries}"
                )
                
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise TimeoutError("move_node") from err
        
        raise NetworkError("move_node failed after maximum retries")
