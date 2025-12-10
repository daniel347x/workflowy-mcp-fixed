"""WorkFlowy API client implementation."""

import json
import sys
import os
from typing import Any

import httpx

# Import reconciliation algorithm
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from workflowy_move_reconcile import reconcile_tree
except ImportError:
    # Fallback if module not found - will use old algorithm
    reconcile_tree = None

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

from .nexus_helper import (
    mark_all_nodes_as_potentially_truncated,
    merge_glimpses_for_terrain,
    build_original_ids_seen_from_glimpses,
    build_terrain_export_from_glimpse,
)

from datetime import datetime

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


# Module-level variable to track current WEAVE context for tag-specific logging
_current_weave_context = {"json_file": None}

def _log_to_file_helper(message: str, log_type: str = "reconcile") -> None:
    """Log message to a tag-specific debug file (best-effort).

    Args:
        message: The message to log
        log_type: "reconcile" -> reconcile_debug.log
                    "etch"      -> etch_debug.log
    
    If _current_weave_context["json_file"] is set, logs go to the same directory
    as the JSON file (tag-specific). Otherwise, falls back to global temp/.
    """
    try:
        from datetime import datetime
        
        filename = "reconcile_debug.log"
        if log_type == "etch":
            filename = "etch_debug.log"
        elif log_type == "nexus":
            filename = "nexus_debug.log"
        elif log_type in ("jewel", "jewelstorm"):
            filename = "jewelstorm_debug.log"
        
        # Determine log directory (tag-specific if in WEAVE context)
        json_file = _current_weave_context.get("json_file")
        if json_file and os.path.exists(json_file):
            # Tag-specific: put debug log in same directory as JSON
            log_path = os.path.join(os.path.dirname(json_file), filename)
        else:
            # Global fallback
            log_path = fr"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\{filename}"
        
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        with open(log_path, "a", encoding="utf-8") as dbg:
            dbg.write(f"[{ts}] {message}\n")
    except Exception:
        # Never let logging failures affect API behavior
        pass


def _log_glimpse_to_file(operation_type: str, node_id: str, result: dict[str, Any]) -> None:
    """Log GLIMPSE operations to persistent markdown files (best-effort).
    
    Args:
        operation_type: "glimpse" or "glimpse_full"
        node_id: Root node UUID that was glimpsed
        result: The result dict returned by glimpse operation
    """
    try:
        from datetime import datetime
        import json as json_module
        
        base_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\uuid_and_glimpse_explorer"
        filename = f"{operation_type}.md"
        log_path = os.path.join(base_dir, filename)
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"## {timestamp}\n\n")
            f.write(f"**Node ID:** `{node_id}`\n")
            f.write(f"**Root Name:** {result.get('root', {}).get('name', 'Unknown')}\n")
            f.write(f"**Node Count:** {result.get('node_count', 0)}\n")
            f.write(f"**Depth:** {result.get('depth', 0)}\n")
            f.write(f"**Source:** {result.get('_source', 'unknown')}\n\n")
            
            # Write truncated JSON preview (first 50 lines of pretty-printed result)
            json_preview = json_module.dumps(result, indent=2, ensure_ascii=False)
            preview_lines = json_preview.split('\n')[:50]
            if len(preview_lines) < len(json_preview.split('\n')):
                preview_lines.append("... (truncated)")
            f.write("```json\n")
            f.write('\n'.join(preview_lines))
            f.write("\n```\n\n---\n\n")
    except Exception:
        # Never let logging failures affect API behavior
        pass

def is_pid_running(pid: int) -> bool:
    """Check if a process ID is currently running.
    
    Args:
        pid: Process ID to check
        
    Returns:
        True if process exists and is running
    """
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        # Fallback without psutil (Windows only)
        import subprocess
        result = subprocess.run(
            ['tasklist', '/FI', f'PID eq {pid}', '/NH'],
            capture_output=True,
            text=True,
            timeout=5
        )
        return str(pid) in result.stdout
    except Exception:
        return False


def scan_active_weaves(nexus_runs_base: str) -> list[dict[str, Any]]:
    """Scan nexus_runs directory for active detached WEAVE processes.
    
    Returns list of active jobs with nexus_tag, PID, and journal path.
    """
    from pathlib import Path
    
    nexus_runs_dir = Path(nexus_runs_base)
    if not nexus_runs_dir.exists():
        return []
    
    active = []
    
    for tag_dir in nexus_runs_dir.iterdir():
        if not tag_dir.is_dir():
            continue
        
        pid_file = tag_dir / ".weave.pid"
        journal_file = tag_dir / "enchanted_terrain.weave_journal.json"
        
        if not pid_file.exists():
            continue
        
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            if is_pid_running(pid):
                # Process is alive
                job_info = {
                    "job_id": f"weave-enchanted-{tag_dir.name}",
                    "nexus_tag": tag_dir.name,
                    "pid": pid,
                    "status": "running",
                    "detached": True,
                    "mode": "enchanted",
                    "journal": str(journal_file) if journal_file.exists() else None
                }
                
                # Read journal for progress if available
                if journal_file.exists():
                    try:
                        with open(journal_file, 'r') as jf:
                            journal = json.load(jf)
                        job_info["entries_completed"] = len(journal.get("entries", []))
                        job_info["started_at"] = journal.get("last_run_started_at")
                        if journal.get("last_run_completed"):
                            job_info["status"] = "completed"
                        if journal.get("last_run_error"):
                            job_info["status"] = "failed"
                            job_info["error"] = journal.get("last_run_error")
                    except Exception:
                        pass
                
                active.append(job_info)
            else:
                # Stale PID file - clean it up
                try:
                    pid_file.unlink()
                    log_event(f"Cleaned up stale PID file for {tag_dir.name}", "CLEANUP")
                except Exception:
                    pass
        except Exception as e:
            log_event(f"Error scanning {tag_dir.name}: {e}", "SCAN")
            continue
    
    return active


# 2-LETTER ACTION CODE MAPPING (module-level constant for exploration)
# Used by all exploration functions to translate compact codes to full action names
EXPLORATION_ACTION_2LETTER = {
    "EL": "engulf_leaf_into_gem_for_editing",
    "PL": "preserve_leaf_in_ether_untouched",
    "UL": "update_leaf_node_and_engulf_in_gemstorm",
    "RB": "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states",
    "PB": "preserve_branch_node_in_ether_untouched__when_no_engulfed_children",
    "UB": "update_branch_node_and_engulf_in_gemstorm__descendants_unaffected",
    "UN": "update_branch_note_and_engulf_in_gemstorm__descendants_unaffected",
    "AB": "auto_decide_branch_no_change_required",
    "EF": "engulf_all_showing_undecided_descendants_into_gem_for_editing",
    "PF": "preserve_all_showing_undecided_descendants_in_ether",
    "UT": "update_tag_and_engulf_in_gemstorm",
    "SS": "set_scratchpad",
    "AS": "append_scratchpad",
    "AH": "add_hint",
    "PD": "peek_descendants_as_frontier",
    "SX": "search_descendants_for_text",
    "PA": "preserve_all_remaining_nodes_in_ether_at_finalization",
    "RF": "resume_guided_frontier",
}


class WorkFlowyClient:
    """Async client for WorkFlowy API operations."""

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
        
    def _log_debug(self, message: str) -> None:
        """Log debug messages to stderr (unified logging)."""
        # Console Visibility ONLY - keep reconcile_debug.log clean for weaves
        log_event(message, "CLIENT_DEBUG")
    
    def _log_to_file(self, message: str, log_type: str = "reconcile") -> None:
        """Log message to a specific debug file (best-effort)."""
        _log_to_file_helper(message, log_type)

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
        mode, clean_note = WorkFlowyClient._extract_whitener_mode(note)
        if mode == "raw":
            return (clean_note, None)
        if clean_note is not None:
            note = clean_note

        text = note
        segments = WorkFlowyClient._segment_whitelisted_markup(text)

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

        # Legacy implementation (unreachable, kept for reference):
        if note is None:
            return (None, None)
        
        import re
        
        # STEP 1: Normalize <a href> tags to "text (URL)" format
        # Pattern matches: <a href="URL">text</a> or <a href='URL'>text</a>
        link_pattern = re.compile(r'<a\s+href=["\']([^"\'>]+)["\'][^>]*>([^<]+)</a>', re.IGNORECASE)
        normalized = link_pattern.sub(r'\2 (\1)', note)
        links_normalized = (normalized != note)
        
        # STEP 2: Escape & first (before bracket processing to avoid double-escape)
        escaped = normalized.replace('&', '&amp;')
        
        # STEP 3: Strict whitelist for XML tags
        # ONLY preserve: <b>, <i>, <s>, <code> (and their closing variants)
        # All other angle brackets get escaped
        xml_tag_pattern = re.compile(r'</?(?:b|i|s|code)>', re.IGNORECASE)
        
        # Find all whitelisted XML tag positions to preserve them
        valid_tags = [(m.start(), m.end()) for m in xml_tag_pattern.finditer(escaped)]
        
        # Build result character by character, escaping < > outside valid tag ranges
        result = []
        i = 0
        entities_escaped = ('&' in note)  # Track if we made changes
        
        while i < len(escaped):
            char = escaped[i]
            
            # Check if current position is inside a valid XML tag
            inside_tag = any(start <= i < end for start, end in valid_tags)
            
            if char == '<' and not inside_tag:
                result.append('&lt;')
                entities_escaped = True
            elif char == '>' and not inside_tag:
                result.append('&gt;')
                entities_escaped = True
            else:
                result.append(char)
            
            i += 1
        
        escaped_note = ''.join(result)
        
        # Build warning message if any transformations occurred
        if links_normalized and entities_escaped:
            return (escaped_note, "✅ NORMALIZED: <a href> → text (URL), escaped bare brackets, preserved whitelisted tags")
        elif links_normalized:
            return (escaped_note, "✅ NORMALIZED: <a href> → text (URL)")
        elif entities_escaped:
            return (escaped_note, "✅ SMART-ESCAPED: Escaped bare brackets (& < >), preserved whitelisted tags")
        
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
        mode, clean_name = WorkFlowyClient._extract_whitener_mode(name)
        if mode == "raw":
            return (clean_name, None)
        if clean_name is not None:
            name = clean_name

        text = name
        segments = WorkFlowyClient._segment_whitelisted_markup(text)

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

        # Legacy implementation (unreachable, kept for reference):
        if name is None:
            return (None, None)
        
        import re
        
        # STEP 1: Normalize <a href> tags to "text (URL)" format
        # Pattern matches: <a href="URL">text</a> or <a href='URL'>text</a>
        link_pattern = re.compile(r'<a\s+href=["\']([^"\'>]+)["\'][^>]*>([^<]+)</a>', re.IGNORECASE)
        normalized = link_pattern.sub(r'\2 (\1)', name)
        links_normalized = (normalized != name)
        
        # STEP 2: DOUBLE-ENCODE & first: & → &amp;amp;
        # (API decodes to &amp;, GUI decodes to &)
        escaped = normalized.replace('&', '&amp;amp;')
        
        # STEP 3: Strict whitelist for XML tags
        # ONLY preserve: <b>, <i>, <s>, <code> (and their closing variants)
        # All other angle brackets get DOUBLE-ENCODED
        xml_tag_pattern = re.compile(r'</?(?:b|i|s|code)>', re.IGNORECASE)
        
        # Must search in ORIGINAL (before & encoding) to find tag positions correctly
        valid_tags_in_original = [(m.start(), m.end()) for m in xml_tag_pattern.finditer(normalized)]
        
        # Build result from NORMALIZED (not original), applying double-encoding for bare brackets
        result = []
        i = 0
        entities_escaped = ('&' in name or links_normalized)  # Track if we made changes
        
        for j, char in enumerate(normalized):
            inside_tag = any(start <= j < end for start, end in valid_tags_in_original)
            
            if char == '&':
                result.append('&amp;amp;')  # Already handled, consistent with escaped var
            elif char == '<' and not inside_tag:
                result.append('&amp;lt;')  # DOUBLE-ENCODE
                entities_escaped = True
            elif char == '>' and not inside_tag:
                result.append('&amp;gt;')  # DOUBLE-ENCODE
                entities_escaped = True
            else:
                result.append(char)
        
        escaped_name = ''.join(result)
        
        # Build warning message if any transformations occurred
        if links_normalized and entities_escaped:
            return (escaped_name, "✅ NORMALIZED NAME: <a href> → text (URL), double-encoded bare brackets, preserved whitelisted tags")
        elif links_normalized:
            return (escaped_name, "✅ NORMALIZED NAME: <a href> → text (URL)")
        elif entities_escaped:
            return (escaped_name, "✅ SMART-ESCAPED NAME: Double-encoded bare brackets, preserved whitelisted tags")
        
        return (escaped_name, None)

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

    async def __aenter__(self) -> "WorkFlowyClient":
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

    def _log_to_file(self, message: str, log_type: str = "reconcile") -> None:
        """Log message to a tag-specific debug file (best-effort).

        Args:
            message: The message to log
            log_type: "reconcile" -> reconcile_debug.log
                      "etch"      -> etch_debug.log
        
        Uses _current_weave_context to determine tag-specific path.
        """
        try:
            from datetime import datetime
            
            filename = "reconcile_debug.log"
            if log_type == "etch":
                filename = "etch_debug.log"
            
            # Use tag-specific path (same logic as _log_to_file_helper)
            json_file = _current_weave_context.get("json_file")
            if json_file and os.path.exists(json_file):
                log_path = os.path.join(os.path.dirname(json_file), filename)
            else:
                log_path = fr"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\{filename}"
            
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            with open(log_path, "a", encoding="utf-8") as dbg:
                dbg.write(f"[{ts}] {message}\n")
        except Exception:
            # Never let logging failures affect API behavior
            pass

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

    async def refresh_nodes_export_cache(self, max_retries: int = 10) -> dict[str, Any]:
        """Force a fresh /nodes-export call and update the in-memory cache.

        This is exposed via an MCP tool so Dan (or an agent) can explicitly
        refresh the snapshot used by UUID Navigator and NEXUS without waiting
        for an auto-refresh trigger.
        """
        from datetime import datetime

        # Clear any previous cache and dirty markers first.
        self._nodes_export_cache = None
        self._nodes_export_cache_timestamp = None
        self._nodes_export_dirty_ids.clear()

        # Delegate to export_nodes with caching disabled for this call.
        data = await self.export_nodes(node_id=None, max_retries=max_retries, use_cache=False, force_refresh=True)
        nodes = data.get("nodes", []) or []

        return {
            "success": True,
            "node_count": len(nodes),
            "timestamp": datetime.now().isoformat(),
        }

    async def create_node(self, request: NodeCreateRequest, _internal_call: bool = False, max_retries: int = 10) -> WorkFlowyNode:
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

                # Best-effort: mark this node as dirty in the /nodes-export cache so that
                # any subtree exports including it can trigger a refresh when needed.
                try:
                    self._mark_nodes_export_dirty([node.id])
                except Exception:
                    # Cache dirty marking must never affect API behavior
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
            max_retries: Maximum retry attempts (default 5)
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

                # Best-effort: mark this node as dirty so that subsequent
                # /nodes-export-based operations touching this subtree can
                # trigger a refresh when needed.
                try:
                    self._mark_nodes_export_dirty([node_id])
                except Exception:
                    # Cache dirty marking must never affect API behavior
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
            max_retries: Maximum retry attempts (default 5)
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
            max_retries: Maximum retry attempts (default 5)
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
            max_retries: Maximum retry attempts (default 5)
        """
        import asyncio

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
                    self._log_to_file(success_msg, "reconcile")

                # Best-effort: mark this node as dirty so any subsequent
                # /nodes-export-based operations that rely on it will trigger
                # a refresh when needed.
                try:
                    self._mark_nodes_export_dirty([node_id])
                except Exception:
                    # Cache dirty marking must never affect API behavior
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
                self._log_to_file(retry_msg, "reconcile")
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    final_msg = (
                        f"delete_node {node_id} exhausted retries ({retry_count}/{max_retries}) "
                        f"due to rate limiting – aborting."
                    )
                    logger.error(final_msg)
                    self._log_to_file(final_msg, "reconcile")
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
                    return WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    return WorkFlowyNode(**data)

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
                    return WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    return WorkFlowyNode(**data)

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
            max_retries: Maximum retry attempts (default 5)
            
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
                    # Best-effort: mark this node (and its new parent, if any)
                    # as dirty so path-based exports will refresh as needed.
                    try:
                        ids: list[str] = [node_id]
                        if parent_id is not None:
                            ids.append(parent_id)
                        self._mark_nodes_export_dirty(ids)
                    except Exception:
                        # Cache dirty marking must never affect API behavior
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

    @staticmethod
    def _dewhiten_text(value: str | None) -> str | None:
        """Single-pass dewhitening for SCRY (/nodes-export) names and notes.

        Decodes &amp;, &lt;, &gt; once to recover semantic text. We *only* touch
        these three entities so that other entities (e.g. &nbsp;) remain literal.
        """
        if value is None or not isinstance(value, str):
            return value
        return value.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    async def export_nodes(
        self,
        node_id: str | None = None,
        max_retries: int = 10,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Export all nodes or filter to specific node's subtree.

        This wraps the /nodes-export endpoint with an in-memory cache and
        dirty-id tracking so repeated subtree exports can often reuse a cached
        snapshot. A refresh is triggered when:
        - There is no cache yet
        - force_refresh=True
        - A subtree request's path-to-root intersects a dirty UUID recorded by
          mutating operations (or the "*" sentinel is present).
        """
        import asyncio
        from datetime import datetime

        logger = _ClientLogger()

        async def fetch_and_cache() -> dict[str, Any]:
            """Call /nodes-export with retries and update the cache."""
            retry_count = 0
            base_delay = 1.0

            while retry_count < max_retries:
                # Force delay at START of each iteration (rate limit protection)
                await asyncio.sleep(API_RATE_LIMIT_DELAY)

                try:
                    # API exports all nodes as flat list (no parameters supported)
                    response = await self.client.get("/nodes-export")
                    data = await self._handle_response(response)

                    all_nodes = data.get("nodes", []) or []

                    # SCRY reverse-whitening (dewhitening) for names/notes:
                    # decode &amp;, &lt;, &gt; once so subsequent writes can safely
                    # re-whiten using _validate_name_field/_validate_note_field.
                    for node in all_nodes:
                        for key in ("name", "nm", "note", "no"):
                            if key in node:
                                node[key] = self._dewhiten_text(node.get(key))

                    total_before_filter = len(all_nodes)

                    # Annotate actual count from API (before any subtree filtering)
                    data["_total_fetched_from_api"] = total_before_filter

                    # Update cache and clear dirty markers
                    self._nodes_export_cache = data
                    self._nodes_export_cache_timestamp = datetime.now()
                    self._nodes_export_dirty_ids.clear()

                    if retry_count > 0:
                        success_msg = (
                            "export_nodes (full account) succeeded after "
                            f"{retry_count + 1}/{max_retries} attempts following "
                            "rate limiting or transient errors."
                        )
                        logger.info(success_msg)
                        self._log_to_file(success_msg, "reconcile")

                    return data

                except RateLimitError as e:
                    retry_count += 1
                    retry_after = getattr(e, "retry_after", None) or (base_delay * (2 ** retry_count))
                    retry_msg = (
                        f"Rate limited on export_nodes. Retry after {retry_after}s. "
                        f"Attempt {retry_count}/{max_retries}"
                    )
                    logger.warning(retry_msg)
                    self._log_to_file(retry_msg, "reconcile")

                    if retry_count < max_retries:
                        await asyncio.sleep(retry_after)
                    else:
                        raise

                except NetworkError as e:
                    retry_count += 1
                    logger.warning(
                        f"Network error on export_nodes: {e}. Retry {retry_count}/{max_retries}"
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
                        raise TimeoutError("export_nodes") from err

            raise NetworkError("export_nodes failed after maximum retries")

        # Decide whether to use the cached snapshot or fetch from the API.
        if (not use_cache) or force_refresh or self._nodes_export_cache is None:
            data = await fetch_and_cache()
        else:
            data = self._nodes_export_cache

            # For subtree requests, only refresh if the path-to-root intersects a
            # dirty UUID (or global "*" sentinel). This keeps unrelated regions
            # fast even after mutations elsewhere.
            if node_id is not None and self._nodes_export_dirty_ids:
                all_nodes = data.get("nodes", []) or []
                nodes_by_id = {n.get("id"): n for n in all_nodes if n.get("id")}

                if node_id not in nodes_by_id:
                    logger.info(
                        f"export_nodes: node_id {node_id} not found in cached snapshot; refreshing cache"
                    )
                    data = await fetch_and_cache()
                else:
                    dirty = self._nodes_export_dirty_ids
                    path_hits_dirty = False
                    cur = node_id
                    visited: set[str] = set()

                    while cur and cur not in visited:
                        visited.add(cur)
                        if cur in dirty or "*" in dirty:
                            path_hits_dirty = True
                            break
                        parent_id = (
                            nodes_by_id[cur].get("parent_id")
                            or nodes_by_id[cur].get("parentId")
                        )
                        cur = parent_id

                    if path_hits_dirty:
                        logger.info(
                            f"export_nodes: path from {node_id} intersects dirty ids; refreshing cache"
                        )
                        data = await fetch_and_cache()
                    else:
                        logger.info(
                            f"export_nodes: using cached /nodes-export for subtree rooted at {node_id}"
                        )

        # At this point, 'data' is either fresh from API or from a safe cache.
        all_nodes = data.get("nodes", []) or []
        total_before_filter = len(all_nodes)

        # If no filtering requested, return everything (with _total_fetched_from_api annotated).
        if node_id is None:
            if "_total_fetched_from_api" not in data:
                data["_total_fetched_from_api"] = total_before_filter
            return data

        # Filter to specific node and its descendants.
        included_ids = {node_id}
        nodes_by_id = {node["id"]: node for node in all_nodes if node.get("id")}

        def add_descendants(parent_id: str) -> None:
            for node in all_nodes:
                if node.get("parent_id") == parent_id and node["id"] not in included_ids:
                    included_ids.add(node["id"])
                    add_descendants(node["id"])

        if node_id in nodes_by_id:
            add_descendants(node_id)

        filtered_nodes = [node for node in all_nodes if node["id"] in included_ids]

        return {
            "nodes": filtered_nodes,
            "_total_fetched_from_api": data.get("_total_fetched_from_api", total_before_filter),
            "_filtered_count": len(filtered_nodes),
        }

    async def bulk_export_to_file(
        self,
        node_id: str,
        output_file: str,
        include_metadata: bool = True,
        use_efficient_traversal: bool = False,
        max_depth: int | None = None,
        child_count_limit: int | None = None,
        max_nodes: int | None = None,
    ) -> dict[str, Any]:
        """Export node tree to hierarchical JSON file AND Markdown file.
        
        Args:
            node_id: Root node UUID to export from
            output_file: Absolute path where JSON should be written
            include_metadata: Include created_at, modified_at fields (default True)
            use_efficient_traversal: Use BFS traversal (default False)
            max_depth: Optional depth limit for exported tree (None = full depth)
            child_count_limit: Optional maximum immediate child count to fully display
                per parent. If a parent has more children than this limit, its children
                are treated as an opaque subtree in the editable JSON while counts are
                still computed from the full tree.
            max_nodes: Optional maximum total node count. If the SCRY would
                export more than this number of nodes, the operation aborts with
                a clear error (no JSON written). This mirrors the safety guard
                used by workflowy_scry(size_limit).
            
        Returns:
            {"success": True, "file_path": "...", "markdown_file": "...", "node_count": N, "depth": M}
        """
        # EFFICIENT TRAVERSAL: Use list_nodes BFS instead of fetching entire account
        total_nodes_fetched = 0
        api_calls_made = 0
        if use_efficient_traversal:
            from collections import deque
            flat_nodes = []
            queue = deque([node_id])
            visited = set()
            
            while queue:
                parent = queue.popleft()
                if parent in visited:
                    continue
                visited.add(parent)
                
                # Fetch immediate children only
                request = NodeListRequest(parentId=parent)
                children, count = await self.list_nodes(request)
                api_calls_made += 1  # Track each list_nodes API call
                total_nodes_fetched += count  # Nodes fetched for this subtree
                
                for child in children:
                    child_dict = child.model_dump()
                    # Ensure parent_id is recorded for hierarchy reconstruction.
                    # Some model dumps include parentId=None; in that case, we
                    # override to the BFS parent we just queried.
                    parent_id = child_dict.get("parent_id") or child_dict.get("parentId")
                    if not parent_id:
                        child_dict["parent_id"] = parent
                    flat_nodes.append(child_dict)
                    queue.append(child.id)
            
            # Add the root node itself
            root_node_data = await self.get_node(node_id)
            api_calls_made += 1  # Track get_node API call
            flat_nodes.insert(0, root_node_data.model_dump())
            total_nodes_fetched += 1
        else:
            # OLD METHOD: Fetch entire account (100K+ nodes for Dan!)
            raw_data = await self.export_nodes(node_id)
            flat_nodes = raw_data.get("nodes", [])
            # total_nodes_fetched reflects how many nodes /nodes-export returned
            # for this account; the subtree guard below operates on the
            # post-filter count (len(flat_nodes)).
            total_nodes_fetched = raw_data.get("_total_fetched_from_api", len(flat_nodes))
            api_calls_made = 1  # Single export_nodes call (but fetches ALL nodes!)
        
        try:
            # Compute ACTUAL subtree size (post-filter-to-root) for the safety
            # guard. This is the only value we compare against max_nodes.
            subtree_node_count = len(flat_nodes)
            
            # Global safety: prevent accidental massive SCRY exports based on the
            # subtree size, not the total account size fetched from /nodes-export.
            if max_nodes is not None and subtree_node_count > max_nodes:
                raise NetworkError(
                    "SCRY subtree size "
                    f"({subtree_node_count} nodes) exceeds limit ({max_nodes} nodes).\n\n"
                    "Options:\n"
                    f"1. Increase max_nodes parameter (if your use case truly requires more than {max_nodes} nodes)\n"
                    "2. Use max_depth or child_count_limit to truncate the SCRY\n"
                    "3. Use nexus_scry (tag-based) + nexus_ignite_shards for coarse terrain + targeted shards\n\n"
                    f"(Underlying /nodes-export fetched {total_nodes_fetched} node(s) from the account; "
                    "the guard now compares against the post-filter subtree only.)"
                )
            
            if not flat_nodes:
                return {
                    "success": True,
                    "file_path": output_file,
                    "markdown_file": None,
                    "node_count": 0,
                    "depth": 0
                }
            
            # Build hierarchical tree from flat list
            hierarchical_tree = self._build_hierarchy(flat_nodes, include_metadata)

            # Preserve root node info and build complete path to Dagger root
            root_node_info = None
            root_immediate_child_count: int | None = None
            if hierarchical_tree and len(hierarchical_tree) == 1:
                root_node = hierarchical_tree[0]
                
                # Walk up parent chain to build complete path
                path_uuids = [root_node.get('id')]
                path_names = [root_node.get('name')]
                
                current_parent_id = root_node.get('parent_id')
                while current_parent_id:
                    try:
                        parent_node_data = await self.get_node(current_parent_id)
                        path_uuids.insert(0, parent_node_data.id)
                        path_names.insert(0, parent_node_data.nm or 'Untitled')
                        current_parent_id = parent_node_data.parentId
                    except Exception:
                        # Stop if we can't fetch parent (permissions, deleted, etc.)
                        break
                
                root_node_info = {
                    'id': root_node.get('id'),
                    'name': root_node.get('name'),
                    'parent_id': root_node.get('parent_id'),
                    'full_path_uuids': path_uuids,
                    'full_path_names': path_names
                }
                # Capture the FULL immediate child count of the root *before*
                # any truncation is applied to the editable JSON.
                root_immediate_child_count = len(root_node.get('children') or [])
                # Extract only children (skip root for round-trip editing)
                hierarchical_tree = root_node.get('children', [])
            else:
                # SCRY exports should always have a single logical root under the
                # requested node_id. Multiple roots indicate inconsistent parent_id
                # relationships in the flat export or a malformed JSON edit.
                raise NetworkError(
                    "SCRY export produced multiple root nodes under the requested node_id. "
                    "This indicates inconsistent parent_id relationships in the source "
                    "JSON or a bug in the export pipeline. Re-scry the Workflowy node, "
                    "and if the problem persists, inspect the raw /nodes-export data."
                )
            
            # Decide root-level truncation semantics for immediate children.
            # By default, SCRY exports include the full immediate child set of
            # the root. If child_count_limit is provided and the root has more
            # children than this limit, we treat the root as an opaque parent:
            # nodes[] is emptied and metadata flags record that the root has
            # hidden children, so DELETE logic never treats missing root
            # children as implicit deletions.
            root_has_hidden_children = False
            root_children_status = "complete"
            
            if (
                child_count_limit is not None
                and root_immediate_child_count is not None
                and root_immediate_child_count > child_count_limit
            ):
                root_has_hidden_children = True
                root_children_status = "truncated_by_count"
                # Root is an opaque parent at this SCRY granularity: do not show
                # any immediate children in nodes[]. Counts remain available via
                # root_immediate_child_count and deeper SCRYs or GLIMPses.
                hierarchical_tree = []
            
            # Annotate counts and children_status, and optionally truncate children
            # for large/deep trees in the EDITABLE JSON. The reconciliation algorithm
            # understands children_status != 'complete' as an opaque subtree: it will
            # not attempt per-child deletes/reorders under those parents while still
            # allowing parent moves/deletes and new children to be added safely.
            if hierarchical_tree:
                self._annotate_child_counts_and_truncate(
                    hierarchical_tree,
                    max_depth=max_depth,
                    child_count_limit=child_count_limit,
                    current_depth=1,
                )
            
            # Calculate overall tree depth after optional truncation (for reporting)
            tree_depth = self._calculate_max_depth(hierarchical_tree)
            
            # Compute ledger of all node IDs seen in this SCRY (before truncation)
            original_ids_seen = sorted({n.get('id') for n in flat_nodes if n.get('id')})
            
            # Compute preview_tree FIRST (before building export_package)
            prefix_for_preview = self._infer_preview_prefix_from_path(output_file)
            preview_tree = None
            if prefix_for_preview:
                preview_tree = self._annotate_preview_ids_and_build_tree(
                    hierarchical_tree,
                    prefix_for_preview,
                )
            
            # Wrap with metadata for safe round-trip editing
            export_package = {
                "export_timestamp": hierarchical_tree[0].get('modifiedAt') if hierarchical_tree else None,
                "export_root_children_status": root_children_status,
                "__preview_tree__": preview_tree,
                "export_root_id": node_id,
                "export_root_name": root_node_info.get('name') if root_node_info else 'Unknown',
                "nodes": hierarchical_tree,
                "original_ids_seen": original_ids_seen
            }
            
            # Write JSON file (working copy)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_package, f, indent=2, ensure_ascii=False)
            
            # Create JSON backup (.original.json)
            json_backup = output_file.replace('.json', '.original.json')
            with open(json_backup, 'w', encoding='utf-8') as f:
                json.dump(export_package, f, indent=2, ensure_ascii=False)
            
            # Generate and write Markdown file (working copy)
            markdown_file = output_file.replace('.json', '.md')
            markdown_content = self._generate_markdown(hierarchical_tree, root_node_info=root_node_info)
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            # Create Markdown backup (.original.md)
            markdown_backup = output_file.replace('.json', '.original.md')
            with open(markdown_backup, 'w', encoding='utf-8') as f:
                f.write(markdown_content)

            # Create Keystone backup
            keystone_path = None
            try:
                import shutil
                from datetime import datetime
                import uuid

                backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
                os.makedirs(backup_dir, exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                node_name_slug = "".join(c for c in root_node_info.get('name', 'Unknown') if c.isalnum() or c in " _-").rstrip()[:50]
                short_uuid = str(uuid.uuid4())[:6]

                keystone_filename = f"{timestamp}-{node_name_slug}-{short_uuid}.json"
                keystone_path = os.path.join(backup_dir, keystone_filename)

                shutil.copy2(output_file, keystone_path)
            except Exception as e:
                # Log but don't fail the main export if backup fails
                print(f"Keystone backup creation failed: {e}")

            return {
                "success": True,
                "file_path": output_file,
                "markdown_file": markdown_file,
                "keystone_backup_path": keystone_path,
                "node_count": len(flat_nodes),
                "depth": tree_depth,
                "total_nodes_fetched": total_nodes_fetched,
                "api_calls_made": api_calls_made,
                "efficient_traversal": use_efficient_traversal
            }
            
        except Exception as e:
            raise NetworkError(f"Bulk export failed: {str(e)}") from e
    
    def _build_hierarchy(
        self,
        flat_nodes: list[dict[str, Any]],
        include_metadata: bool = True
    ) -> list[dict[str, Any]]:
        """Convert flat node list to hierarchical tree structure.
        
        Args:
            flat_nodes: Flat list of nodes with parent_id references
            include_metadata: Whether to include metadata fields
            
        Returns:
            List of root nodes with nested children
        """
        # Pass 1: Build lookup dictionary and add children arrays
        nodes_by_id: dict[str, dict[str, Any]] = {}
        for node in flat_nodes:
            node_copy = node.copy()
            
            # Strip metadata if requested
            if not include_metadata:
                node_copy.pop('created_at', None)
                node_copy.pop('modified_at', None)
                node_copy.pop('createdAt', None)
                node_copy.pop('modifiedAt', None)
            
            node_copy['children'] = []
            nodes_by_id[node['id']] = node_copy
        
        # Pass 2: Link children to parents
        root_nodes = []
        for node in nodes_by_id.values():
            parent_id = node.get('parent_id') or node.get('parentId')
            if parent_id and parent_id in nodes_by_id:
                nodes_by_id[parent_id]['children'].append(node)
            else:
                # No parent or parent not in set = root node
                root_nodes.append(node)

        # Pass 3: Sort each sibling group by Workflowy 'priority' field so that
        # DFS and all traversals see siblings in the same order as Workflowy UI.
        def _sort_children_by_priority(nodes_list: list[dict[str, Any]]) -> None:
            for n in nodes_list:
                children = n.get('children') or []
                if children:
                    children.sort(key=lambda c: c.get('priority', 0))
                    _sort_children_by_priority(children)

        _sort_children_by_priority(root_nodes)

        return root_nodes
    
    def _calculate_max_depth(self, nodes: list[dict[str, Any]], current_depth: int = 1) -> int:
        """Calculate maximum depth of node tree.
        
        Args:
            nodes: List of nodes at current level
            current_depth: Current depth (starts at 1)
            
        Returns:
            Maximum depth of tree
        """
        if not nodes:
            return current_depth - 1
        
        max_child_depth = current_depth
        for node in nodes:
            if node.get('children'):
                child_depth = self._calculate_max_depth(node['children'], current_depth + 1)
                max_child_depth = max(max_child_depth, child_depth)
        
        return max_child_depth
    
    def _limit_depth(self, nodes: list[dict[str, Any]], max_depth: int, current_depth: int = 1) -> list[dict[str, Any]]:
        """Limit node tree to specified depth.
        
        Args:
            nodes: List of nodes at current level
            max_depth: Maximum depth to include (1=direct children only, 2=two levels, etc.)
            current_depth: Current depth in recursion (starts at 1)
            
        Returns:
            Depth-limited tree (children arrays truncated at max_depth)
        """
        if not nodes or current_depth > max_depth:
            return []
        
        limited_nodes = []
        for node in nodes:
            node_copy = node.copy()
            
            if current_depth < max_depth and node.get('children'):
                # Recursively limit children
                node_copy['children'] = self._limit_depth(node['children'], max_depth, current_depth + 1)
            else:
                # At max depth - truncate children
                node_copy['children'] = []
            
            limited_nodes.append(node_copy)
        
        return limited_nodes
    
    def _annotate_child_counts_and_truncate(
        self,
        nodes: list[dict[str, Any]],
        max_depth: int | None = None,
        child_count_limit: int | None = None,
        current_depth: int = 1,
    ) -> int:
        """Annotate child/descendant counts and optionally truncate children.

        This operates on the EDITABLE JSON used by NEXUS scrolls. It computes
        per-node:

        - immediate_child_count__human_readable_only: number of direct children
          in the FULL tree for this SCRY/GLIMPSE
        - total_descendant_count__human_readable_only: total descendants
          (excluding the node itself) in the FULL tree for this SCRY/GLIMPSE
        - children_status:
            - "complete"                => children list is fully loaded/editable
            - "truncated_by_depth"      => depth limit applied at this level
            - "truncated_by_count"      => child_count_limit applied at this level

        For nodes whose children_status != "complete", the reconciliation
        algorithm treats the children as an opaque subtree: it will not perform
        per-child deletes/reorders, but parent moves/deletes and new children
        remain safe. The *_human_readable_only fields are provided purely for
        human inspection and debugging.

        Args:
            nodes: List of nodes at this level (full children still attached)
            max_depth: Optional depth limit (None = no depth truncation)
            child_count_limit: Optional maximum immediate child count to fully
                materialize per parent (None = no count truncation)
            current_depth: Current depth in recursion (starts at 1)

        Returns:
            Total descendant count (sum over all nodes in this list), used by
            callers if they need aggregate information.
        """
        if not nodes:
            return 0

        total_descendants_here = 0

        for node in nodes:
            children = node.get('children') or []

            # First, recursively annotate children so counts are based on the
            # FULL tree before any truncation is applied.
            child_desc_total = 0
            if children:
                child_desc_total = self._annotate_child_counts_and_truncate(
                    children,
                    max_depth=max_depth,
                    child_count_limit=child_count_limit,
                    current_depth=current_depth + 1,
                )

            immediate_count = len(children)
            total_desc_for_node = child_desc_total + immediate_count

            node['immediate_child_count__human_readable_only'] = immediate_count
            node['total_descendant_count__human_readable_only'] = total_desc_for_node

            # Decide whether this node's children should be truncated in the
            # EDITABLE JSON view.
            status = 'complete'
            truncate_children = False

            # Depth-based truncation wins first: at or beyond max_depth, we
            # keep the node itself but hide its children while still exposing
            # accurate counts.
            if max_depth is not None and current_depth >= max_depth and immediate_count > 0:
                status = 'truncated_by_depth'
                truncate_children = True

            # If not truncated by depth, consider child_count_limit.
            if (
                not truncate_children
                and child_count_limit is not None
                and immediate_count > child_count_limit
            ):
                status = 'truncated_by_count'
                truncate_children = True

            if truncate_children:
                # Children remain present in the FULL tree we already used for
                # counts, but are hidden in the editable JSON so the agent is
                # not forced to manage enormous child arrays.
                node['children'] = []

            node['children_status'] = status
            total_descendants_here += total_desc_for_node

        return total_descendants_here

    @staticmethod
    def _infer_preview_prefix_from_path(path: str) -> str | None:
        """Infer preview_id prefix from a known NEXUS JSON filename.

        This is best-effort and only used for coarse TERRAIN-style exports
        written by bulk_export_to_file. Other NEXUS helpers pass an explicit
        prefix when writing phantom_gem/shimmering/enchanted payloads.
        """
        base = os.path.basename(path)
        if base == "coarse_terrain.json":
            return "CT"
        if base == "phantom_gem.json":
            return "PG"
        if base == "shimmering_terrain.json":
            return "ST"
        if base == "enchanted_terrain.json":
            return "ET"
        return None

    @staticmethod
    def _annotate_preview_ids_and_build_tree(
        nodes: list[dict[str, Any]] | None,
        prefix: str,
        max_note_chars: int = 1024,
    ) -> list[str]:
        """Assign preview_id to each node and build a human-readable tree.

        preview_id format: "<PREFIX>-1.2.3" where the numeric path is
        1-based, computed per artifact. This helper is purely for agents/humans;
        NEXUS / JEWELSTORM algorithms never read preview_id or the returned
        preview lines.
        """
        if not nodes:
            return []

        # PASS 1: Assign preview_id to all nodes and collect max width
        all_preview_ids: list[str] = []

        def assign_ids(node: dict[str, Any], path_parts: list[str]) -> None:
            if not isinstance(node, dict):
                return
            path_str = ".".join(path_parts)
            preview_id = f"{prefix}-{path_str}"
            node["preview_id"] = preview_id
            all_preview_ids.append(preview_id)
            children = node.get("children") or []
            for idx, child in enumerate(children, start=1):
                if isinstance(child, dict):
                    assign_ids(child, path_parts + [str(idx)])

        for root_index, root in enumerate(nodes, start=1):
            if isinstance(root, dict):
                assign_ids(root, [str(root_index)])

        # Compute max width for right-padding
        max_id_width = max((len(pid) for pid in all_preview_ids), default=0)

        # PASS 2: Build aligned preview lines
        lines: list[str] = []

        def walk(node: dict[str, Any], path_parts: list[str]) -> None:
            if not isinstance(node, dict):
                return
            preview_id = node.get("preview_id", "")
            padded_id = preview_id.ljust(max_id_width)

            # Build one-line preview
            depth = max(1, len(path_parts))
            indent = " " * 4 * (depth - 1)
            children = node.get("children") or []
            has_child_dicts = any(isinstance(c, dict) for c in children)
            bullet = "•" if not has_child_dicts else "⦿"
            name = node.get("name") or "Untitled"
            note = node.get("note") or ""
            if isinstance(note, str) and note:
                flat = note.replace("\n", "\\n")
                if len(flat) > max_note_chars:
                    flat = flat[:max_note_chars]
                name_part = f"{name} [{flat}]"
            else:
                name_part = name
            lines.append(f"[{padded_id}] {indent}{bullet} {name_part}")

            # Recurse into children with extended path
            for idx, child in enumerate(children, start=1):
                if isinstance(child, dict):
                    walk(child, path_parts + [str(idx)])

        for root_index, root in enumerate(nodes, start=1):
            if isinstance(root, dict):
                walk(root, [str(root_index)])

        return lines

    @staticmethod
    def _build_frontier_preview_lines(
        frontier: list[dict[str, Any]] | None,
        max_note_chars: int = 1024,
    ) -> list[str]:
        """Build a one-line-per-handle preview for exploration frontiers.

        Format:
            [A.1      ] ⦿ Name [note-preview]
        with the handle right-padded to the longest handle width and
        indentation based on handle depth (segments separated by '.').
        """
        if not frontier:
            return []

        handles = [str(entry.get("handle", "")) for entry in frontier]
        if not handles:
            return []
        max_len = max(len(h) for h in handles)

        lines: list[str] = []
        for entry in frontier:
            handle = str(entry.get("handle", ""))
            label = handle.ljust(max_len)
            # Depth from handle segments (A, A.1, A.1.3.2, ...)
            depth = max(1, len(handle.split("."))) if handle else 1
            indent = " " * 4 * (depth - 1)

            is_leaf = bool(entry.get("is_leaf"))
            bullet = "•" if is_leaf else "⦿"

            name = entry.get("name_preview") or "Untitled"
            note = entry.get("note_preview") or ""
            if isinstance(note, str) and note:
                flat = note.replace("\n", "\\n")
                if len(flat) > max_note_chars:
                    flat = flat[:max_note_chars]
                name_part = f"{name} [{flat}]"
            else:
                name_part = name

            lines.append(f"[{label}] {indent}{bullet} {name_part}")

        return lines

    def _generate_markdown(
        self,
        nodes: list[dict[str, Any]],
        level: int = 1,
        parent_path_uuids: list[str] | None = None,
        parent_path_names: list[str] | None = None,
        root_node_info: dict[str, Any] | None = None
    ) -> str:
        """Convert hierarchical nodes to Markdown format with UUID metadata.
        
        Args:
            nodes: List of nodes at current level
            level: Current heading level (1-6)
            parent_path_uuids: Accumulated UUID path from root (for recursion)
            parent_path_names: Accumulated name path from root (for recursion)
            root_node_info: Info about the actual exported root node (excluded from JSON)
            
        Returns:
            Markdown-formatted string with hidden XML metadata
        """
        if parent_path_uuids is None:
            parent_path_uuids = []
        if parent_path_names is None:
            parent_path_names = []
            
        markdown_lines = []
        
        # Add root metadata at top of file (level 1 only)
        if level == 1 and root_node_info:
            root_uuid = root_node_info.get('id', '')
            root_name = root_node_info.get('name', 'Root')
            full_path_uuids = root_node_info.get('full_path_uuids', [root_uuid])
            full_path_names = root_node_info.get('full_path_names', [root_name])
            
            # Truncated UUID path for readability
            truncated_path = ' > '.join([uuid[:8] + '...' for uuid in full_path_uuids])
            # Full name path for orientation
            name_path = ' > '.join(full_path_names)
            
            markdown_lines.append(f'<!-- EXPORTED_ROOT_UUID: {root_uuid} -->')
            markdown_lines.append(f'<!-- EXPORTED_ROOT_NAME: {root_name} -->')
            markdown_lines.append(f'<!-- EXPORTED_ROOT_PATH_UUIDS: {truncated_path} -->')
            markdown_lines.append(f'<!-- EXPORTED_ROOT_PATH_NAMES: {name_path} -->')
            markdown_lines.append('')
        
        for node in nodes:
            node_uuid = node.get('id', '')
            node_name = node.get('name', 'Untitled')
            
            # Build paths for this node
            current_path_uuids = parent_path_uuids + [node_uuid]
            current_path_names = parent_path_names + [node_name]
            
            # Create heading (limit to h6)
            heading_level = min(level, 6)
            heading = '#' * heading_level + ' ' + node_name
            markdown_lines.append(heading)
            
            # Add metadata (hidden in Obsidian reading view)
            markdown_lines.append(f'<!-- NODE_UUID: {node_uuid} -->')
            
            # Truncated UUID path (first 8 chars of each UUID for readability)
            truncated_uuids = [uuid[:8] + '...' for uuid in current_path_uuids]
            uuid_path = ' > '.join(truncated_uuids)
            markdown_lines.append(f'<!-- NODE_PATH_UUIDS: {uuid_path} -->')
            
            # Name path (full names for orientation)
            name_path = ' > '.join(current_path_names)
            markdown_lines.append(f'<!-- NODE_PATH_NAMES: {name_path} -->')
            
            markdown_lines.append('')  # Blank line after metadata
            
            # Add note content if present
            note = node.get('note')
            if note and note.strip():
                # Quick detection: check if note looks like markdown (has # headers)
                is_markdown = any(line.strip().startswith('#') for line in note.split('\n'))
                language = 'markdown' if is_markdown else 'text'
                
                # Wrap in 12-backtick delimiter (overkill prevents conflicts)
                markdown_lines.append('````````````' + language)
                markdown_lines.append(note)
                markdown_lines.append('````````````')
                markdown_lines.append('')  # Blank line after code block
            
            # Recursively process children with updated paths
            children = node.get('children', [])
            if children:
                child_markdown = self._generate_markdown(
                    children, 
                    level + 1,
                    current_path_uuids,
                    current_path_names
                )
                markdown_lines.append(child_markdown)
            
        return '\n'.join(markdown_lines)
    
    def _strip_metadata_comments(self, text: str | None) -> str | None:
        """Remove export metadata comments from text.
        
        Args:
            text: Text that may contain our metadata HTML comments
            
        Returns:
            Cleaned text with metadata comments removed
        """
        if not text:
            return text
            
        import re
        
        # Remove our specific metadata comment patterns
        patterns = [
            r'<!-- EXPORTED_ROOT_UUID:.*? -->',
            r'<!-- EXPORTED_ROOT_NAME:.*? -->',
            r'<!-- NODE_UUID:.*? -->',
            r'<!-- NODE_PATH_UUIDS:.*? -->',
            r'<!-- NODE_PATH_NAMES:.*? -->'
        ]
        
        cleaned = text
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.DOTALL)
        
        # Clean up any resulting multiple blank lines
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        return cleaned.strip() if cleaned.strip() else None
    
    def generate_markdown_from_json(
        self,
        json_file: str,
    ) -> dict[str, Any]:
        """Convert JSON file to Markdown (without metadata - for edited JSON review).
        
        Args:
            json_file: Path to JSON file (from bulk_export or edited)
            
        Returns:
            {"success": True, "markdown_file": "..."}
        """
        try:
            # Read JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both bare array AND metadata wrapper (from bulk_export)
            if isinstance(data, dict) and "nodes" in data:
                nodes = data["nodes"]  # Extract from metadata wrapper
            elif isinstance(data, list):
                nodes = data  # Already bare array
            else:
                return {
                    "success": False,
                    "markdown_file": None,
                    "error": f"JSON must be array or dict with 'nodes' key. Got: {type(data).__name__}"
                }
            
            # Generate Markdown WITH metadata (enables UUID tracking in diffs)
            markdown_content = self._generate_markdown(nodes, level=1)
            
            # Write to .md file (same name as JSON)
            markdown_file = json_file.replace('.json', '.md')
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            return {
                "success": True,
                "markdown_file": markdown_file
            }
            
        except Exception as e:
            return {
                "success": False,
                "markdown_file": None,
                "error": f"Failed to generate markdown: {str(e)}"
            }
    
    def _generate_markdown_simple(
        self,
        nodes: list[dict[str, Any]],
        level: int = 1
    ) -> str:
        """Convert nodes to Markdown WITHOUT metadata (for edited JSON).
        
        Args:
            nodes: List of nodes at current level
            level: Current heading level (1-6)
            
        Returns:
            Clean Markdown without UUID metadata
        """
        markdown_lines = []
        
        for node in nodes:
            node_name = node.get('name', 'Untitled')
            
            # Create heading (limit to h6)
            heading_level = min(level, 6)
            heading = '#' * heading_level + ' ' + node_name
            markdown_lines.append(heading)
            markdown_lines.append('')  # Blank line after heading
            
            # Add note content if present
            note = node.get('note')
            if note and note.strip():
                # Quick detection: check if note looks like markdown (has # headers)
                is_markdown = any(line.strip().startswith('#') for line in note.split('\n'))
                language = 'markdown' if is_markdown else 'text'
                
                # Wrap in 12-backtick delimiter
                markdown_lines.append('````````````' + language)
                markdown_lines.append(note)
                markdown_lines.append('````````````')
                markdown_lines.append('')  # Blank line after code block
            
            # Recursively process children
            children = node.get('children', [])
            if children:
                child_markdown = self._generate_markdown_simple(children, level + 1)
                markdown_lines.append(child_markdown)
            
        return '\n'.join(markdown_lines)
    
    async def _write_glimpse_as_terrain(
        self,
        node_id: str,
        ws_glimpse: dict[str, Any],
        output_file: str,
    ) -> dict[str, Any]:
        """Write WebSocket GLIMPSE as a TERRAIN export file.
        
        This helper uses the same shared logic as nexus_glimpse to produce a
        TERRAIN file from a WebSocket GLIMPSE result:
        
        1. Fetch API GLIMPSE FULL for complete child count validation
        2. Merge WebSocket + API via shared helpers (has_hidden_children, children_status)
        3. Build original_ids_seen ledger
        4. Construct TERRAIN wrapper
        5. Write JSON and Markdown files
        
        Args:
            node_id: Root node UUID being glimpsed
            ws_glimpse: WebSocket GLIMPSE result dict
            output_file: Absolute path where TERRAIN JSON should be written
            
        Returns:
            Compact summary with file paths and stats
        """
        logger = _ClientLogger()
        
        # STEP 1: Get API GLIMPSE FULL for complete tree structure
        logger.info(f"📡 GLIMPSE TERRAIN Step 1: API fetch for complete tree structure...")
        try:
            # Use high size_limit to avoid artificial truncation - we need FULL tree
            api_glimpse = await self.workflowy_scry(
                node_id=node_id,
                use_efficient_traversal=False,
                depth=None,  # Full depth
                size_limit=50000  # High limit - we need complete structure
            )
        except Exception as e:
            logger.warning(f"⚠️ API fetch failed: {e} - falling back to WebSocket-only (UNSAFE for WEAVE!)")
            api_glimpse = None
        
        # STEP 2: MERGE - Use shared helpers for TERRAIN export logic
        logger.info(
            "🔀 GLIMPSE TERRAIN Step 2: Merging WebSocket + API results via shared helpers..."
        )
        
        # Use shared helper to annotate has_hidden_children / children_status
        children, root_children_status, api_merge_performed = merge_glimpses_for_terrain(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )
        
        # Build original_ids_seen ledger
        original_ids_seen = build_original_ids_seen_from_glimpses(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )
        
        # Construct TERRAIN wrapper
        terrain_data = build_terrain_export_from_glimpse(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            children=children,
            root_children_status=root_children_status,
            original_ids_seen=original_ids_seen,
        )
        
        # STEP 3: Write TERRAIN JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(terrain_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise NetworkError(f"Failed to write TERRAIN JSON: {e}") from e
        
        # STEP 4: Generate and write Markdown file
        markdown_file = output_file.replace('.json', '.md')
        try:
            # Extract root info for Markdown header
            ws_root = ws_glimpse.get("root") or {}
            root_node_info = {
                'id': node_id,
                'name': ws_root.get('name', 'Root'),
                'parent_id': ws_root.get('parent_id'),
                'full_path_uuids': [node_id],
                'full_path_names': [ws_root.get('name', 'Root')]
            }
            
            markdown_content = self._generate_markdown(
                children,
                level=1,
                root_node_info=root_node_info
            )
            
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
        except Exception as e:
            raise NetworkError(f"Failed to write TERRAIN Markdown: {e}") from e
        
        return {
            "success": True,
            "mode": "file",
            "terrain_file": output_file,
            "markdown_file": markdown_file,
            "node_count": ws_glimpse.get("node_count", 0),
            "depth": ws_glimpse.get("depth", 0),
            "_source": "glimpse_merged",
            "_api_merge_performed": api_merge_performed,
        }
    
    async def workflowy_glimpse(self, node_id: str, use_efficient_traversal: bool = False, output_file: str | None = None, _ws_connection=None, _ws_queue=None) -> dict[str, Any]:
        """Load entire node tree into context (no file intermediary).
        
        GLIMPSE command - direct context loading for agent analysis.
        
        Can use WebSocket connection to Chrome extension for DOM extraction (bypasses API).
        Falls back to API fetch if WebSocket not available.
        
        Args:
            node_id: Root node UUID to read from
            output_file: Optional absolute path; if provided, write a TERRAIN-style
                export package (same format as nexus_glimpse / bulk_export_to_file)
                using the shared nexus_helper functions. WebSocket GLIMPSE + API SCRY
                merged for has_hidden_children / children_status annotations.
            _ws_connection: WebSocket connection from server.py (internal parameter)
            _ws_queue: WebSocket message queue from server.py (internal parameter)
            
        Returns:
            When output_file is None (default):
                {
                    "success": True,
                    "root": {"id": "...", "name": "...", "note": "..."},
                    "children": [...],
                    "node_count": N,
                    "depth": M,
                    "_source": "websocket" | "api"
                }
            
            When output_file is provided:
                {
                    "success": True,
                    "mode": "file",
                    "terrain_file": output_file,
                    "markdown_file": "...",
                    "node_count": N,
                    "depth": M,
                    "_source": "glimpse_merged",
                    "_api_merge_performed": bool
                }
            
        Note: root metadata lets you read the node's prompt/content.
              children array is for round-trip editing (prevents root duplication).
        """
        import asyncio
        import json as json_module

        logger = _ClientLogger()
        
        # ===== TRY WEBSOCKET FIRST (if connected and queue available) =====
        if _ws_connection and _ws_queue:
            try:
                logger.info(f"🔌 Attempting WebSocket DOM extraction for node {node_id[:8]}...")
                
                # Send request to extension
                request = {
                    "action": "extract_dom",
                    "node_id": node_id
                }
                await _ws_connection.send(json_module.dumps(request))
                logger.info("  Request sent to extension, awaiting response from queue...")
                
                # Wait for response from QUEUE (not direct recv - avoids concurrency conflict)
                response = await asyncio.wait_for(
                    _ws_queue.get(),
                    timeout=5.0
                )
                
                logger.info(f"  📦 WebSocket response from queue: {response.get('node_count', 0)} nodes")
                logger.info(f"  🔍 Response keys: {list(response.keys())}")
                logger.info(f"  🔍 Has 'success': {response.get('success')}")
                logger.info(f"  🔍 Has 'children': {'children' in response}")
                
                # Validate response structure
                if response.get('success') and 'children' in response:
                    # Add source indicator
                    response['_source'] = 'websocket'
                    logger.info("✅ GLIMPSE via WebSocket successful")
                    
                    # Attach preview_id + preview_tree for in-memory GLIMPSE tree (WG-1.2.3)
                    # COMPUTE FIRST, then insert early in response dict
                    preview_tree_for_response = None
                    try:
                        root_obj = response.get("root") or {}
                        children = response.get("children") or []
                        if isinstance(root_obj, dict):
                            # Attach children to root for preview traversal; children list
                            # itself remains the primary contract for agents.
                            root_obj.setdefault("children", children)
                            preview_tree_for_response = self._annotate_preview_ids_and_build_tree(
                                [root_obj],
                                prefix="WG",
                            )
                        else:
                            preview_tree_for_response = self._annotate_preview_ids_and_build_tree(
                                children,
                                prefix="WG",
                            )
                    except Exception:
                        # Preview must never break GLIMPSE; ignore errors.
                        pass
                    
                    # Insert preview_tree early (after success/_source, before children/root/node_count)
                    if preview_tree_for_response is not None:
                        # Rebuild response dict with preview_tree in early position
                        old_response = response
                        response = {
                            "success": old_response.get("success"),
                            "_source": old_response.get("_source"),
                            "node_count": old_response.get("node_count"),
                            "depth": old_response.get("depth"),
                            "preview_tree": preview_tree_for_response,
                            "root": old_response.get("root"),
                            "children": old_response.get("children"),
                        }
                    
                    # Log to persistent file
                    _log_glimpse_to_file("glimpse", node_id, response)
                    
                    # IF output_file requested: write TERRAIN using shared helpers
                    if output_file is not None:
                        return await self._write_glimpse_as_terrain(
                            node_id=node_id,
                            ws_glimpse=response,
                            output_file=output_file,
                        )
                    
                    return response
                else:
                    logger.warning("  WebSocket response invalid")
                    raise NetworkError(
                        "WebSocket GLIMPSE failed - invalid response structure.\n\n"
                        "Options:\n"
                        "1. Verify Workflowy desktop app is open and connected\n"
                        "2. Check extension console for errors\n"
                        "3. Use workflowy_scry() to fetch complete tree via API\n\n"
                        "Note: workflowy_glimpse() requires WebSocket connection to extract \n"
                        "only expanded nodes. Use glimpseFull for Mode 2 (hunting) operations."
                    )
                    
            except asyncio.TimeoutError:
                logger.warning("  WebSocket timeout (5s)")
                raise NetworkError(
                    "WebSocket GLIMPSE timeout (5 seconds).\n\n"
                    "Possible causes:\n"
                    "- Extension not connected or not responding\n"
                    "- DOM extraction taking longer than expected\n"
                    "- Workflowy desktop app not running\n\n"
                    "Options:\n"
                    "1. Verify Workflowy desktop app is open\n"
                    "2. Check extension console for errors\n"
                    "3. Use workflowy_scry() to fetch complete tree via API\n\n"
                    "Note: workflowy_glimpse() is WebSocket-only (Mode 1: Dan shows you).\n"
                    "Use glimpseFull for Mode 2 (Agent hunts) operations."
                )
            except Exception as e:
                logger.warning(f"  WebSocket error: {e}")
                raise NetworkError(
                    f"WebSocket GLIMPSE error: {str(e)}\n\n"
                    "WebSocket connection failed or extension error occurred.\n\n"
                    "Options:\n"
                    "1. Verify Workflowy desktop app is open and extension loaded\n"
                    "2. Check Workflowy console (F12) for extension errors\n"
                    "3. Restart Workflowy desktop app\n"
                    "4. Use workflowy_scry() to fetch complete tree via API\n\n"
                    "Note: workflowy_glimpse() requires active WebSocket connection.\n"
                    "Use glimpseFull when WebSocket unavailable."
                ) from e
        
        # If WebSocket connection not available, raise error immediately
        raise NetworkError(
            "WebSocket GLIMPSE unavailable - no WebSocket connection.\n\n"
            "GLIMPSE requires WebSocket connection to Workflowy desktop app.\n\n"
            "Options:\n"
            "1. Ensure Workflowy desktop app is running\n"
            "2. Verify extension is loaded and connected (check console)\n"
            "3. Restart MCP connector to initialize WebSocket server\n"
            "4. Use workflowy_scry() to fetch complete tree via API\n\n"
            "Mode 1 (Dan shows you): Requires WebSocket - use glimpse()\n"
            "Mode 2 (Agent hunts): Bypass WebSocket - use glimpseFull()"
        )
    
    async def workflowy_scry(
        self,
        node_id: str,
        use_efficient_traversal: bool = False,
        depth: int | None = None,
        size_limit: int = 1000,
        output_file: str | None = None,
    ) -> dict[str, Any]:
        """Load entire node tree via API (bypass WebSocket).
        
        Mode 2 (Agent hunts) - Full API fetch regardless of WebSocket availability.
        
        Use when:
        - Agent needs to hunt for parent UUIDs (Key Files doesn't have it)
        - Dan wants complete node tree regardless of expansion state
        - WebSocket selective extraction not needed
        
        Args:
            node_id: Root node UUID to read from
            use_efficient_traversal: Use BFS traversal (default False)
            depth: Maximum depth to traverse (1=direct children only, 2=two levels, None=full tree)
            size_limit: Maximum number of nodes to return (raises error if exceeded)
            output_file: Optional absolute path; if provided, write a TERRAIN-style
                export package (same format as nexus_scry / bulk_export_to_file)
                instead of returning the in-memory tree. In this mode the
                response is a small summary pointing at the JSON/Markdown files.
            
        Returns:
            Same format as workflowy_glimpse with _source="api" when output_file is None,
            or a compact summary when output_file is provided.
        """
        import logging
        logger = _ClientLogger()

        # If caller requested TERRAIN-style file output, delegate to bulk_export_to_file
        # to ensure we exactly match the NEXUS SCRY / TERRAIN format, including
        # export_root_* metadata and original_ids_seen ledger.
        if output_file is not None:
            result = await self.bulk_export_to_file(
                node_id=node_id,
                output_file=output_file,
                include_metadata=True,
                use_efficient_traversal=use_efficient_traversal,
                max_depth=depth,
                child_count_limit=None,
                max_nodes=size_limit,
            )
            return {
                "success": bool(result.get("success", True)),
                "mode": "file",
                "terrain_file": output_file,
                "markdown_file": result.get("markdown_file"),
                "keystone_backup_path": result.get("keystone_backup_path"),
                "node_count": result.get("node_count"),
                "depth": result.get("depth"),
                "total_nodes_fetched": result.get("total_nodes_fetched"),
                "api_calls_made": result.get("api_calls_made"),
                "efficient_traversal": result.get("efficient_traversal"),
                "_source": "api",
            }
        
        # EFFICIENT TRAVERSAL: Use list_nodes BFS instead of fetching entire account
        total_nodes_fetched = 0
        if use_efficient_traversal:
            from collections import deque
            flat_nodes = []
            queue = deque([node_id])
            visited = set()
            
            while queue:
                parent = queue.popleft()
                if parent in visited:
                    continue
                visited.add(parent)
                
                # Fetch immediate children only
                request = NodeListRequest(parentId=parent)
                children, count = await self.list_nodes(request)
                total_nodes_fetched += count  # Use the count from list_nodes
                
                for child in children:
                    child_dict = child.model_dump()
                    # Ensure parent_id is recorded for hierarchy reconstruction.
                    # Some model dumps include parentId=None; in that case, we
                    # override to the BFS parent we just queried.
                    parent_id = child_dict.get("parent_id") or child_dict.get("parentId")
                    if not parent_id:
                        child_dict["parent_id"] = parent
                    flat_nodes.append(child_dict)
                    queue.append(child.id)
            
            # Add the root node itself
            root_node_data = await self.get_node(node_id)
            flat_nodes.insert(0, root_node_data.model_dump())
            total_nodes_fetched += 1  # Count the root node too
        else:
            # OLD METHOD: Fetch entire account (100K+ nodes for Dan!)
            raw_data = await self.export_nodes(node_id)
            all_nodes = raw_data.get("nodes", [])
            total_nodes_fetched = len(all_nodes)
            flat_nodes = all_nodes  # Then filters client-side
        
        try:
            
            if not flat_nodes:
                return {
                    "success": True,
                    "root": None,
                    "children": [],
                    "node_count": 0,
                    "depth": 0,
                    "_source": "api"
                }
            
            # Check size_limit BEFORE building hierarchy (early exit)
            if size_limit is not None and len(flat_nodes) > size_limit:
                raise NetworkError(
                    f"Tree size ({len(flat_nodes)} nodes) exceeds limit ({size_limit} nodes).\n\n"
                    f"Options:\n"
                    f"1. Increase size_limit parameter: glimpseFull(node_id, size_limit={len(flat_nodes)})\n"
                    f"2. Use depth parameter to limit traversal: glimpseFull(node_id, depth=2)\n"
                    f"3. Use GLIMPSE (WebSocket) for selective extraction\n\n"
                    f"This safety prevents accidental 50MB+ tree fetches."
                )
            
            # Build hierarchical tree
            hierarchical_tree = self._build_hierarchy(flat_nodes, include_metadata=True)

            # Attach preview_id + preview_tree for GLIMPSE FULL (WS-1.2.3)
            try:
                preview_tree = self._annotate_preview_ids_and_build_tree(
                    hierarchical_tree,
                    prefix="WS",
                )
            except Exception:
                preview_tree = []

            # LOGGING: inspect root candidates from hierarchy for debugging
            try:
                self._log_debug(f"workflowy_scry: node_id={node_id} use_efficient_traversal={use_efficient_traversal} flat_nodes={len(flat_nodes)} roots={len(hierarchical_tree)}")
                for idx, root_candidate in enumerate(hierarchical_tree[:10]):
                    self._log_debug(f"  root_candidate[{idx}]: id={root_candidate.get('id')} name={root_candidate.get('name')} parent_id={root_candidate.get('parent_id')} children={len(root_candidate.get('children') or [])}")
            except Exception:
                # Logging must never break GLIMPSE FULL
                pass

            # Extract root node metadata and children separately
            root_metadata = None
            children = []
            
            # Strategy 1: Single root found
            if hierarchical_tree and len(hierarchical_tree) == 1:
                root_node = hierarchical_tree[0]
                self._log_debug(f"workflowy_scry: using single-root path id={root_node.get('id')} name={root_node.get('name')}")
                root_metadata = {
                    "id": root_node.get('id'),
                    "name": root_node.get('name'),
                    "note": root_node.get('note'),
                    "parent_id": root_node.get('parent_id')
                }
                children = root_node.get('children', [])
                
            # Strategy 2: Multiple roots - Find the one matching requested node_id
            else:
                target_root = next((r for r in hierarchical_tree if r.get("id") == node_id), None)
                
                if target_root:
                    self._log_debug(f"workflowy_scry: multiple roots ({len(hierarchical_tree)}), but found target root id={target_root.get('id')} name={target_root.get('name')}. Using it.")
                    root_metadata = {
                        "id": target_root.get('id'),
                        "name": target_root.get('name'),
                        "note": target_root.get('note'),
                        "parent_id": target_root.get('parent_id')
                    }
                    children = target_root.get('children', [])
                else:
                    # Fallback: Return all roots as children (artificial root behavior)
                    self._log_debug(f"workflowy_scry: multiple roots ({len(hierarchical_tree)}) and target {node_id} NOT found in top level; returning list directly")
                    children = hierarchical_tree
            
            # Apply depth limiting if requested
            if depth is not None:
                children = self._limit_depth(children, depth)
            
            # Calculate max depth
            max_depth = self._calculate_max_depth(children)
            
            result = {
                "success": True,
                "_source": "api",  # Indicate data came from API (not WebSocket)
                "node_count": len(flat_nodes),
                "depth": max_depth,
                "preview_tree": preview_tree,
                "root": root_metadata,
                "children": children,
            }
            
            # Log to persistent file
            _log_glimpse_to_file("glimpse_full", node_id, result)
            
            return result
            
        except Exception as e:
            raise NetworkError(f"GLIMPSE FULL failed: {str(e)}") from e
    
    async def workflowy_etch(
        self,
        parent_id: str,
        nodes: list[dict[str, Any]] | str,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        """Create multiple nodes from JSON structure (no file intermediary).
        
        ETCH command - simple additive node creation (no UUIDs, no updates/moves).
        
        TWO MODES:
        
        DEFAULT (replace_all=False):
        - Match existing children by name (case-sensitive, trimmed)
        - Skip if name exists (walk down tree, add new children only)
        - NO updates, NO deletes, NO moves - just additions
        - Use case: Add VYRTHEXes, documentation nodes, walk existing structure
        
        REPLACE MODE (replace_all=True):
        - Delete ALL existing children first
        - Create fresh tree from source
        - Use case: "I don't like what's there, etch these instead"
        
        ASSUMPTIONS:
        - Unique names per parent level (duplicate names = nondeterministic match)
        - Case-sensitive name matching
        - No Unicode normalization (uses simple .strip())
        - No sibling reordering (new nodes appended at bottom)
        
        FOR COMPLEX OPERATIONS (updates/moves/deletes with UUID preservation):
        Use bulk_import_from_file (NEXUS scroll) instead.
        
        FUTURE ENHANCEMENTS (deferred):
        - dry_run flag (preview without executing)
        - case_insensitive option
        - Unicode normalization
        - name_normalizer hook
        
        Args:
            parent_id: Parent UUID where nodes should be created
            nodes: List of node objects (NO UUIDs - just name/note/children):
                   [{
                       "name": "Node name",
                       "note": "Optional note content",
                       "children": [
                           {"name": "Child 1", "note": null, "children": []},
                           {"name": "Child 2", "note": null, "children": []}
                       ]
                   }]
            replace_all: If True, delete ALL existing children before creating.
                        Default False (additive mode).
        
        Returns:
            {
                "success": True/False,
                "nodes_created": N,
                "root_node_ids": [...],
                "skipped": N,  # Only present if append_only=True
                "api_calls": N,
                "retries": N,
                "rate_limit_hits": N,
                "errors": [...]
            }
        """
        import asyncio
        import json

        logger = _ClientLogger()
        
        # 🔧 AUTO-FIX: Detect if nodes is stringified JSON instead of list
        stringify_strategy_used = None
        if isinstance(nodes, str):
            logger.warning("⚠️ Received stringified JSON - attempting multiple parse strategies")
            
            # Strategy 1: Direct JSON parse (if already valid)
            try:
                nodes = json.loads(nodes)
                stringify_strategy_used = "Strategy 1: Direct json.loads()"
                logger.info(f"✅ {stringify_strategy_used}")
            except json.JSONDecodeError:
                
                # Strategy 2: Unicode escape decode (CASE 1 style - escaped quotes/unicode)
                try:
                    decoded = nodes.encode().decode('unicode_escape')
                    nodes = json.loads(decoded)
                    stringify_strategy_used = "Strategy 2: Unicode escape decode (CASE 1 style)"
                    logger.info(f"✅ {stringify_strategy_used}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    
                    # Strategy 3: Triple-backslash quote replacement (CASE 2 style)
                    try:
                        # Raw string to avoid escape interpretation
                        fixed = nodes.replace(r'\\"', '"')
                        nodes = json.loads(fixed)
                        stringify_strategy_used = "Strategy 3: Triple-backslash replacement (CASE 2 style)"
                        logger.info(f"✅ {stringify_strategy_used}")
                    except json.JSONDecodeError:
                        
                        # Strategy 4: Strip ASCII control characters (except preserved \n and \t)
                        try:
                            import re
                            # Remove ASCII control chars except \n (0x0A) and \t (0x09)
                            # This catches unescaped control characters that break JSON parsing
                            cleaned = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', nodes)
                            nodes = json.loads(cleaned)
                            stringify_strategy_used = "Strategy 4: Strip control characters"
                            logger.info(f"✅ {stringify_strategy_used}")
                        except json.JSONDecodeError:
                            
                            # Strategy 5: Aggressive escape normalization
                            try:
                                # Fix common escape issues: normalize literal \n, \t, \r sequences
                                # But avoid double-escaping already-escaped ones
                                fixed = nodes
                                fixed = fixed.replace('\\n', '\\\\n')  # Escape newlines
                                fixed = fixed.replace('\\t', '\\\\t')  # Escape tabs
                                fixed = fixed.replace('\\r', '\\\\r')  # Escape carriage returns
                                # Undo double-escaping if we over-escaped
                                fixed = fixed.replace('\\\\\\\\n', '\\\\n')
                                fixed = fixed.replace('\\\\\\\\t', '\\\\t')
                                fixed = fixed.replace('\\\\\\\\r', '\\\\r')
                                nodes = json.loads(fixed)
                                stringify_strategy_used = "Strategy 5: Aggressive escape normalization"
                                logger.info(f"✅ {stringify_strategy_used}")
                            except json.JSONDecodeError:
                                
                                # Strategy 6: Character-by-character escape builder (nuclear option)
                                try:
                                    result = []
                                    in_string = False
                                    escape_next = False
                                    i = 0
                                    while i < len(nodes):
                                        char = nodes[i]
                                        
                                        if escape_next:
                                            result.append(char)
                                            escape_next = False
                                            i += 1
                                            continue
                                        
                                        if char == '\\':
                                            escape_next = True
                                            result.append(char)
                                            i += 1
                                            continue
                                        
                                        if char == '"' and (i == 0 or nodes[i-1] != '\\'):
                                            in_string = not in_string
                                            result.append(char)
                                            i += 1
                                            continue
                                        
                                        # Control character inside string - properly escape it
                                        if in_string and ord(char) < 32:
                                            # Convert control char to escaped form
                                            if char == '\n':
                                                result.append('\\\\n')
                                            elif char == '\t':
                                                result.append('\\\\t')
                                            elif char == '\r':
                                                result.append('\\\\r')
                                            else:
                                                # Other control chars: use unicode escape
                                                result.append(f'\\\\u{ord(char):04x}')
                                        else:
                                            result.append(char)
                                        
                                        i += 1
                                    
                                    rebuilt = ''.join(result)
                                    nodes = json.loads(rebuilt)
                                    stringify_strategy_used = "Strategy 6: Character-by-character escape builder (nuclear)"
                                    logger.info(f"✅ {stringify_strategy_used}")
                                except json.JSONDecodeError:
                                    
                                    # Strategy 7: AST literal_eval fallback (last resort)
                                    try:
                                        import ast
                                        # ast.literal_eval can sometimes parse things json.loads can't
                                        nodes = ast.literal_eval(nodes)
                                        # Verify it's actually a list
                                        if not isinstance(nodes, list):
                                            raise ValueError("AST parse succeeded but result is not a list")
                                        stringify_strategy_used = "Strategy 7: AST literal_eval fallback"
                                        logger.info(f"✅ {stringify_strategy_used}")
                                    except (ValueError, SyntaxError) as e:
                                        # All strategies exhausted - return comprehensive error
                                        return {
                                            "success": False,
                                            "nodes_created": 0,
                                            "root_node_ids": [],
                                            "api_calls": 0,
                                            "retries": 0,
                                            "rate_limit_hits": 0,
                                            "errors": [
                                                f"Parameter 'nodes' is a string but could not parse with any strategy.",
                                                f"Tried 7 parsing strategies:",
                                                f"  (1) Direct JSON parse",
                                                f"  (2) Unicode escape decode",
                                                f"  (3) Triple-backslash replacement",
                                                f"  (4) Strip control characters",
                                                f"  (5) Aggressive escape normalization",
                                                f"  (6) Character-by-character escape builder",
                                                f"  (7) AST literal_eval fallback",
                                                f"Final error: {str(e)}",
                                                f"Hint: Use actual list structure, not stringified JSON"
                                            ]
                                        }
        
        # 🔁 SECOND-PASS DECODE: Handle valid JSON strings that themselves contain JSON
        # e.g. "\"[{\\\"name\\\": \\\"Test\\\"}]\"" → first load gives string "[{\"name\": \"Test\"}]"
        if isinstance(nodes, str):
            try:
                second = json.loads(nodes)
                stringify_strategy_used = (stringify_strategy_used or "Strategy 1: Direct json.loads()") + " + Second-pass json.loads()"
                nodes = second
            except json.JSONDecodeError:
                # Leave as-is; final validation below will report a clear error
                pass

        # 🔥 FINAL VALIDATION: Ensure parsed result is valid JSON
        # This catches any corruption that survived the parsing gauntlet
        if not isinstance(nodes, list):
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [
                    f"Parsing succeeded but result is not a list (got {type(nodes).__name__}).",
                    f"Strategy used: {stringify_strategy_used}",
                    f"Hint: Ensure your JSON represents a list of node objects"
                ]
            }
        
        # Verify the parsed JSON can be re-serialized (catches structural corruption)
        try:
            _ = json.dumps(nodes)
            if stringify_strategy_used:
                logger.info(f"✅ Final validation: Parsed JSON is valid and re-serializable")
        except (TypeError, ValueError) as e:
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [
                    f"Parsing succeeded but result cannot be re-serialized as JSON.",
                    f"Strategy used: {stringify_strategy_used}",
                    f"Re-serialization error: {str(e)}",
                    f"Hint: The parsed structure contains non-JSON-serializable objects"
                ]
            }
        
        # 🔥🔥🔥 VALIDATION CHECKPOINT - NOTE FIELDS ONLY 🔥🔥🔥
        # 
        # Validate NOTE fields (angle brackets + literal backslash-n)
        # NAME fields tested separately - validation TBD based on test results
        #
        # 🔥🔥🔥 END VALIDATION CHECKPOINT 🔥🔥🔥
        
        def validate_and_escape_nodes_recursive(nodes_list: list[dict[str, Any]], path: str = "root") -> tuple[bool, str | None, list[str]]:
            """Recursively validate and auto-escape NAME and NOTE fields.
            
            Enforces the ETHER invariant that node names must be non-empty
            (no empty or whitespace-only names), and auto-escapes angle brackets
            for both names and notes.
            
            Returns:
                (success, error_message, warnings_list)
            """
            warnings = []
            
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Validate NAME field: must be a non-empty, non-whitespace string
                name = node.get('name')
                if not isinstance(name, str) or not name.strip():
                    return (
                        False,
                        f"Node: {node_path}\n\n"
                        "Name must be a non-empty, non-whitespace string. "
                        "Empty names are not valid Workflowy nodes.",
                        warnings,
                    )
                
                processed_name, name_warning = self._validate_name_field(name)
                if processed_name is not None:
                    node['name'] = processed_name
                if name_warning:
                    warnings.append(f"Node: {node_path} - Name escaped")
                
                # Validate and escape NOTE field
                note = node.get('note')
                if note:
                    processed_note, note_warning = self._validate_note_field(note, skip_newline_check=False)
                    
                    if processed_note is None and note_warning:  # Blocking error
                        return (False, f"Node: {node_path}\n\n{note_warning}", warnings)
                    
                    # Update node with escaped/validated note
                    node['note'] = processed_note
                    
                    # Collect warning if escaping occurred
                    if note_warning and "AUTO-ESCAPED" in note_warning:
                        warnings.append(f"Node: {node_path} - Note escaped")
                
                # Recursively process children
                children = node.get('children', [])
                if children:
                    success, error_msg, child_warnings = validate_and_escape_nodes_recursive(children, node_path)
                    if not success:
                        return (False, error_msg, warnings)
                    warnings.extend(child_warnings)
            
            return (True, None, warnings)
        
        # Run validation on NAME and NOTE fields
        success, error_msg, warnings = validate_and_escape_nodes_recursive(nodes)
        
        if not success:
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [error_msg or "Note field validation failed"]
            }
        
        # Log warnings if any escaping occurred
        if warnings:
            logger.info(f"\u2705 Auto-escaped angle brackets in {len(warnings)} node(s)")
            for warning in warnings:
                logger.info(f"  - {warning}")
        
        if not isinstance(nodes, list):
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": ["Parameter 'nodes' must be a list"]
            }
        
        # Stats tracking
        stats = {
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "nodes_created": 0,
            "skipped": 0,
            "errors": []
        }
        
        # 🗑️ REPLACE_ALL MODE: Wipe and replace
        if replace_all:
            logger.info("🗑️ replace_all=True - Deleting all existing children")
            try:
                request = NodeListRequest(parentId=parent_id)
                existing_children, _ = await self.list_nodes(request)
                stats["api_calls"] += 1
                
                for child in existing_children:
                    try:
                        await self.delete_node(child.id)
                        logger.info(f"  Deleted: {child.nm}")
                        stats["api_calls"] += 1
                    except Exception as e:
                        logger.warning(f"  Failed to delete {child.nm}: {e}")
            except Exception as e:
                logger.warning(f"Could not list/delete existing children: {e}")
            
            nodes_to_create = nodes  # Create all nodes (clean slate)
            existing_names = set()  # No name-matching needed
        
        # 📝 DEFAULT MODE: Additive (skip existing by name)
        else:
            # Always match by name and skip existing (simplified ETCH)
            try:
                request = NodeListRequest(parentId=parent_id)
                existing_children, _ = await self.list_nodes(request)
                stats["api_calls"] += 1
                
                # Build set of existing names (case-sensitive, trimmed)
                existing_names = {child.nm.strip() for child in existing_children if child.nm}
                
                # Filter: only create nodes that don't exist by name
                nodes_to_create = [
                    node for node in nodes 
                    if node.get('name', '').strip() not in existing_names
                ]
                
                stats["skipped"] = len(nodes) - len(nodes_to_create)
                
                if stats["skipped"] > 0:
                    logger.info(f"📝 Skipped {stats['skipped']} existing node(s) (matched by name)")
                
                if not nodes_to_create:
                    # All nodes already exist by name
                    return {
                        "success": True,
                        "nodes_created": 0,
                        "root_node_ids": [],
                        "skipped": stats["skipped"],
                        "api_calls": stats["api_calls"],
                        "retries": 0,
                        "rate_limit_hits": 0,
                        "errors": [],
                        "message": "All nodes already exist (matched by name) - nothing to create"
                    }
            except Exception as e:
                logger.warning(f"Could not check existing: {e} - proceeding to create all")
                nodes_to_create = nodes
                existing_names = set()
        
        async def create_node_with_retry(
            request: NodeCreateRequest,
            max_retries: int = 10,
            internal: bool = False
        ) -> WorkFlowyNode | None:
            """Create node with exponential backoff retry.
            
            Args:
                request: Node creation request
                max_retries: Maximum retry attempts
                internal: Pass True to bypass single-node forcing function
            """
            retry_count = 0
            base_delay = 1.0
            
            while retry_count < max_retries:
                # Force delay at START of each iteration (rate limit protection)
                await asyncio.sleep(API_RATE_LIMIT_DELAY)
                
                try:
                    stats["api_calls"] += 1
                    node = await self.create_node(request, _internal_call=internal)
                    return node
                    
                except RateLimitError as e:
                    stats["rate_limit_hits"] += 1
                    stats["retries"] += 1
                    retry_count += 1
                    
                    retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                    logger.warning(
                        f"Rate limited. Retry after {retry_after}s. "
                        f"Total retries: {stats['retries']}"
                    )
                    
                    if retry_count < max_retries:
                        await asyncio.sleep(retry_after)
                    else:
                        raise
                        
                except NetworkError as e:
                    stats["retries"] += 1
                    retry_count += 1
                    
                    logger.warning(
                        f"Network error: {e}. Retry {retry_count}/{max_retries}"
                    )
                    
                    if retry_count < max_retries:
                        await asyncio.sleep(base_delay * (2 ** retry_count))
                    else:
                        raise
            
            return None
        
        async def create_tree(
            parent_id: str,
            nodes: list[dict[str, Any]]
        ) -> list[str]:
            """Recursively create node tree."""
            created_ids = []
            
            for node_data in nodes:
                try:
                    node_name = node_data['name']
                    
                    # Skip if node already exists by name (default additive behavior)
                    if not replace_all and node_name in existing_names:
                        stats["skipped"] += 1
                        logger.info(f"Skipped existing node: {node_name}")
                        continue
                    
                    # Build request
                    request = NodeCreateRequest(
                        name=node_name,
                        parent_id=parent_id,
                        note=node_data.get('note'),
                        layoutMode=node_data.get('layout_mode'),
                        position=node_data.get('position', 'bottom')
                    )
                    
                    # Create with retry logic (includes validation via create_node)
                    # Pass _internal_call=True to bypass single-node forcing function
                    node = await create_node_with_retry(request, internal=True)
                    
                    if node:
                        created_ids.append(node.id)
                        stats["nodes_created"] += 1
                        self._log_to_file(f"  Created: {node_name} ({node.id})", "etch")
                        
                        # Recursively create children
                        if 'children' in node_data and node_data['children']:
                            await create_tree(node.id, node_data['children'])
                    
                except Exception as e:
                    error_msg = f"Failed to create node '{node_data.get('name', 'unknown')}': {str(e)}"
                    logger.error(error_msg)
                    self._log_to_file(error_msg, "etch")
                    stats["errors"].append(error_msg)
                    # Continue with other nodes
                    continue
            
            return created_ids
        
        # Create the tree
        try:
            self._log_to_file(f"Starting ETCH (replace_all={replace_all}) for parent {parent_id}", "etch")
            root_ids = await create_tree(parent_id, nodes)
            
            # Log summary if retries occurred
            if stats["retries"] > 0:
                log_event(
                    f"⚠️ Bulk write completed with {stats['retries']} retries "
                    f"({stats['rate_limit_hits']} rate limit hits). "
                    f"Consider reducing import speed.",
                    "ETCH"
                )
            
            self._log_to_file(f"ETCH Complete: {stats['nodes_created']} nodes created", "etch")
            
            result = {
                "success": len(stats["errors"]) == 0,
                "nodes_created": stats["nodes_created"],
                "root_node_ids": root_ids,
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"]
            }
            
            # Add skipped count (always tracked in additive mode)
            if not replace_all:
                result["skipped"] = stats["skipped"]
            
            # Add stringify strategy if auto-fix was used
            if stringify_strategy_used:
                result["_stringify_autofix"] = stringify_strategy_used

            # Best-effort: mark the parent as dirty so subsequent
            # /nodes-export-based operations under this parent will trigger
            # a refresh when needed.
            if result.get("success", False):
                try:
                    self._mark_nodes_export_dirty([parent_id])
                except Exception:
                    # Cache dirty marking must never affect API behavior
                    pass
            
            return result
            
        except Exception as e:
            error_msg = f"Bulk write failed: {str(e)}"
            log_event(error_msg, "ETCH")
            stats["errors"].append(error_msg)
            
            result = {
                "success": False,
                "nodes_created": stats["nodes_created"],
                "root_node_ids": [],
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"]
            }
            
            if skip_duplicates and not replace_all:
                result["skipped"] = stats["skipped"]
            
            if stringify_strategy_used:
                result["_stringify_autofix"] = stringify_strategy_used
            
            return result
    
    async def bulk_import_from_file(
        self,
        json_file: str,
        parent_id: str = None,
        dry_run: bool = False,
        import_policy: str = 'strict',
        auto_upgrade_to_jewel: bool = True,
    ) -> dict[str, Any]:
        """Create multiple Workflowy nodes from JSON file.
        
        Uses move-aware reconciliation algorithm (CREATE/MOVE/REORDER/UPDATE/DELETE).
        Preserves UUIDs when nodes are moved (not delete+create).
        
        Args:
            json_file: Absolute path to JSON file with node structure
            parent_id: Parent UUID where nodes should be created (optional - reads from JSON if not provided)
            dry_run: If True, returns operation plan without executing (default False)
            import_policy: 'strict' (abort on mismatch) | 'rebase' (use file's root) | 'clone' (strip IDs)
            
        Returns:
            {
                "success": True/False,
                "nodes_created": N,
                "nodes_updated": N,
                "nodes_deleted": N,
                "nodes_moved": N,
                "root_node_ids": [...],
                "api_calls": N,
                "retries": N,
                "rate_limit_hits": N,
                "errors": [...]
            }
        """
        import asyncio

        logger = _ClientLogger()

        # Set WEAVE context for tag-specific debug logging
        _current_weave_context["json_file"] = json_file
        
        # Read JSON file
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            # Attach a persistent jewel_file path hint used by the reconciler
            # for per-node JEWEL upgrades.
            if isinstance(payload, dict):
                payload['jewel_file'] = json_file
        except Exception as e:
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [f"Failed to read JSON file: {str(e)}"]
            }
        
        # Handle ONLY new format: dict with metadata + nodes
        # Legacy bare-array format is no longer supported for NEXUS weaves.
        if not (isinstance(payload, dict) and 'nodes' in payload):
            raise NetworkError(
                "NEXUS JSON must be an object with 'export_root_id' and 'nodes' keys (metadata wrapper). "
                "Re-scry the Workflowy node to regenerate a valid SCRI file."
            )
        
        export_root_id = payload.get('export_root_id')
        nodes_to_create = payload.get('nodes')
        log_event(f"Detected export package with export_root_id={export_root_id}", "WEAVE")
        
        # Validate header fields
        if not export_root_id or not isinstance(nodes_to_create, list):
            raise NetworkError(
                "NEXUS SCRY header malformed: 'export_root_id' missing or 'nodes' is not a list. "
                "Do not strip or rewrite the guardian block; re-scry if needed."
            )
        
        # Minimal weave journal to detect incomplete/corrupted previous runs,
        # plus per-node operation entries written via reconcile_tree callbacks.
        weave_journal_path: str | None = None
        weave_journal: dict[str, Any] | None = None
        journal_warning: str | None = None
        log_weave_entry_fn = None
        if not dry_run:
            weave_journal_path = json_file.replace('.json', '.weave_journal.json')
            try:
                if os.path.exists(weave_journal_path):
                    with open(weave_journal_path, 'r', encoding='utf-8') as jf:
                        prev = json.load(jf)
                    if not prev.get('last_run_completed', True):
                        journal_warning = (
                            "Previous weave on this JSON did not complete cleanly at "
                            f"{prev.get('last_run_started_at')}. JEWEL/ETHER sync may be inconsistent; "
                            "crash-resume semantics are not guaranteed."
                        )
                        log_event(journal_warning, "WEAVE")
            except Exception as e:  # noqa: BLE001
                log_event(f"Failed to read existing weave journal {weave_journal_path}: {e}", "WEAVE")
                journal_warning = None

            from datetime import datetime
            weave_journal = {
                "json_file": json_file,
                "last_run_started_at": datetime.now().isoformat(),
                "last_run_completed": False,
                "last_run_error": None,
                # Phase field helps distinguish how far a failed run progressed:
                #   stub_created      – initial journal written before reconciliation
                #   before_reconcile  – about to call reconcile_tree(...)
                #   completed         – reconcile_tree finished and journal finalized
                #   error             – exception handled in bulk_import_from_file
                "phase": "stub_created",
                "entries": [],
            }
            try:
                with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                    json.dump(weave_journal, jf, indent=2)
            except Exception as e:  # noqa: BLE001
                log_event(f"Failed to write weave journal {weave_journal_path}: {e}", "WEAVE")
                weave_journal_path = None
                weave_journal = None

            # Per-node journal entry callback used by reconcile_tree to record
            # operation-level resumability. Each entry is appended to the
            # 'entries' array and the entire journal is re-written
            # best-effort after each node-level operation.
            def log_weave_entry_fn(entry: dict[str, Any]) -> None:  # type: ignore[assignment]
                nonlocal weave_journal, weave_journal_path
                if weave_journal is None or weave_journal_path is None:
                    return
                try:
                    from datetime import datetime as _dt
                    e = dict(entry)
                    e.setdefault("timestamp", _dt.now().isoformat())
                    entries = weave_journal.setdefault("entries", [])
                    entries.append(e)
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf2:
                        json.dump(weave_journal, jf2, indent=2)
                except Exception as e2:  # noqa: BLE001
                    log_event(
                        f"Failed to append per-node entry to weave journal {weave_journal_path}: "
                        f"{type(e2).__name__}: {e2}",
                        "WEAVE"
                    )
        
        # Use export_root_id as default if parent_id not provided
        target_backup_file = None
        if parent_id is None:
            parent_id = export_root_id
            log_event(f"Using export_root_id as parent_id: {parent_id}", "WEAVE")
        else:
            # parent_id was explicitly provided - check if it's different from export_root_id
            if export_root_id and parent_id != export_root_id:
                # AUTO-BACKUP: They're overriding the parent - backup target first!
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                target_backup_file = json_file.replace('.json', f'.target_backup_{timestamp}.json')
                log_event(f"Parent override detected! export_root_id={export_root_id}, provided parent_id={parent_id}", "WEAVE")
                log_event(f"Auto-backing up target to: {target_backup_file}", "WEAVE")
                try:
                    backup_result = await self.bulk_export_to_file(parent_id, target_backup_file)
                    log_event(f"Target backup complete: {backup_result.get('node_count', 0)} nodes", "WEAVE")
                except Exception as e:
                    log_event(f"Target backup failed: {e}", "WEAVE")
                    # Continue anyway - backup failure shouldn't block import
        
        # 🔥 VALIDATE & AUTO-ESCAPE NAME AND NOTE FIELDS 🔥
        def validate_and_escape_nodes_recursive(nodes_list: list[dict[str, Any]], path: str = "root") -> tuple[bool, str | None, list[str]]:
            """Recursively validate and auto-escape name and note fields in node tree.
            
            Enforces the ETHER invariant that node names must be non-empty
            (no empty or whitespace-only names), and auto-escapes angle brackets
            for both names and notes.
            
            Returns:
                (success, error_message, warnings_list)
            """
            warnings = []
            
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Validate NAME field: must be a non-empty, non-whitespace string
                name = node.get('name')
                if not isinstance(name, str) or not name.strip():
                    return (
                        False,
                        f"Node: {node_path}\n\n"
                        "Name must be a non-empty, non-whitespace string. "
                        "Empty names are invalid in enchanted_terrain.json.",
                        warnings,
                    )
                
                processed_name, name_warning = self._validate_name_field(name)
                if processed_name is not None:
                    node['name'] = processed_name
                if name_warning:
                    warnings.append(f"Node: {node_path} - Name escaped")
                
                # Validate and escape NOTE field
                note = node.get('note')
                if note:
                    processed_note, note_warning = self._validate_note_field(note)
                    
                    if processed_note is None and note_warning:  # Blocking error
                        return (False, f"Node: {node_path}\n\n{note_warning}", warnings)
                    
                    # Update node with escaped note
                    node['note'] = processed_note
                    
                    # Collect warning if escaping occurred
                    if note_warning and "AUTO-ESCAPED" in note_warning:
                        warnings.append(f"Node: {node_path} - Note escaped")
                
                # Recursively validate children
                children = node.get('children', [])
                if children:
                    success, error_msg, child_warnings = validate_and_escape_nodes_recursive(children, node_path)
                    if not success:
                        return (False, error_msg, warnings)
                    warnings.extend(child_warnings)
            
            return (True, None, warnings)
        
        # Run validation and escaping on entire tree (modifies nodes in-place)
        success, error_msg, warnings = validate_and_escape_nodes_recursive(nodes_to_create)
        
        if not success:
            # Validation/escaping failed BEFORE reconcile_tree was invoked.
            # Record this in the weave journal (if present) so a "stub_only"
            # journal can be distinguished from a crash-before-reconcile.
            if weave_journal is not None and weave_journal_path is not None:
                try:
                    from datetime import datetime as _dt
                    weave_journal["last_run_completed"] = False
                    weave_journal["last_run_error"] = error_msg or "Note/name field validation failed before reconcile_tree"
                    weave_journal["last_run_failed_at"] = _dt.now().isoformat()
                    weave_journal["phase"] = "error_validation"
                    with open(weave_journal_path, "w", encoding="utf-8") as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception as e2:  # noqa: BLE001
                    log_event(
                        f"Failed to update weave journal on validation error: {type(e2).__name__}: {e2}",
                        "WEAVE",
                    )
            # Also emit a WEAVE-level log line to the detached worker log.
            log_event(
                f"WEAVE validation failed before reconcile_tree: {error_msg or 'Note/name field validation failed'}",
                "WEAVE",
            )
            # Clear WEAVE context before returning.
            _current_weave_context["json_file"] = None
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [error_msg or "Note field validation failed"]
            }
        
        # Log warnings if any escaping occurred
        if warnings:
            log_event(f"✅ Auto-escaped angle brackets in {len(warnings)} note(s)", "WEAVE")
            for warning in warnings:
                log_event(f"  - {warning}", "WEAVE")
        
        # ============ MOVE-AWARE RECONCILIATION ============
        
        # Import reconciliation algorithm
        from .workflowy_move_reconcile import reconcile_tree
        
        # Stats tracking
        stats = {
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "nodes_created": 0,
            "nodes_updated": 0,
            "nodes_deleted": 0,
            "nodes_moved": 0,
            "errors": []
        }
        
        # ============ ASYNC API WRAPPERS ============
        
        async def list_nodes_wrapper(parent_uuid: str) -> list[dict]:
            """Wrapper for list_nodes - returns list of dicts."""
            request = NodeListRequest(parentId=parent_uuid)
            nodes, _ = await self.list_nodes(request)
            stats["api_calls"] += 1
            return [n.model_dump() for n in nodes]
        
        async def create_node_wrapper(parent_uuid: str, data: dict) -> str:
            """Wrapper for create_node - returns new UUID."""
            request = NodeCreateRequest(
                name=data.get('name'),
                parent_id=parent_uuid,
                note=data.get('note'),
                layoutMode=(data.get('data') or {}).get('layoutMode'),  # Handle data=None gracefully
                position='bottom'
            )
            # Reconciliation-scope file logging (WEAVE only; normal client usage untouched)
            self._log_to_file(
                f"WEAVE CREATE: name={data.get('name')} parent={parent_uuid}",
                "reconcile",
            )
            node = await self.create_node(request, _internal_call=True)
            stats["api_calls"] += 1
            stats["nodes_created"] += 1
            return node.id
        
        async def update_node_wrapper(node_uuid: str, data: dict) -> None:
            """Wrapper for update_node."""
            request = NodeUpdateRequest(
                name=data.get('name'),
                note=data.get('note'),
                layoutMode=(data.get('data') or {}).get('layoutMode')  # Handle data=None gracefully
            )
            # Reconciliation-scope file logging
            self._log_to_file(
                f"WEAVE UPDATE: id={node_uuid} name={data.get('name')}",
                "reconcile",
            )
            await self.update_node(node_uuid, request)
            stats["api_calls"] += 1
            stats["nodes_updated"] += 1
        
        async def delete_node_wrapper(node_uuid: str) -> None:
            """Wrapper for delete_node."""
            # Reconciliation-scope file logging
            self._log_to_file(
                f"WEAVE DELETE: id={node_uuid}",
                "reconcile",
            )
            await self.delete_node(node_uuid)
            stats["api_calls"] += 1
            stats["nodes_deleted"] += 1
        
        async def move_node_wrapper(node_uuid: str, new_parent_uuid: str, position: str = "top") -> None:
            """Wrapper for move_node."""
            # Reconciliation-scope file logging
            self._log_to_file(
                f"WEAVE MOVE: id={node_uuid} parent={new_parent_uuid} position={position}",
                "reconcile",
            )
            await self.move_node(node_uuid, new_parent_uuid, position)
            stats["api_calls"] += 1
            stats["nodes_moved"] += 1
        
        async def export_nodes_wrapper(node_uuid: str) -> dict:
            """Wrapper for export_nodes - single bulk API call."""
            # Reconciliation-scope file logging
            self._log_to_file(
                f"WEAVE EXPORT: root={node_uuid}",
                "reconcile",
            )
            raw_data = await self.export_nodes(node_uuid)
            stats["api_calls"] += 1
            return raw_data
        
        # ============ EXECUTE RECONCILIATION ============
        
        try:
            # Pass full payload (including export_root_id and guardian metadata)
            # so the reconciliation algorithm can enforce parent consistency.
            # The reconciler will also populate a per-node JEWEL sync ledger
            # (created/fetched/jewel_updated) that we can use to decide whether
            # the weave is safely resumable after timeouts.
            #
            # BEFORE calling reconcile_tree, update the journal phase so that a
            # "stub-only" journal can be distinguished from a failure that
            # occurs inside reconcile_tree (or later). If we see a journal
            # with phase="stub_created" only, the process died before we ever
            # reached reconciliation. If we see phase="before_reconcile" but no
            # per-node entries, the failure happened inside or after
            # reconcile_tree.
            if weave_journal is not None and weave_journal_path is not None:
                try:
                    weave_journal["phase"] = "before_reconcile"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception as e2:  # noqa: BLE001
                    log_event(
                        f"Failed to update weave journal phase before_reconcile: {e2}",
                        "WEAVE",
                    )
            result_plan = await reconcile_tree(
                source_json=payload,
                parent_uuid=parent_id,
                list_nodes=list_nodes_wrapper,
                create_node=create_node_wrapper,
                update_node=update_node_wrapper,
                delete_node=delete_node_wrapper,
                move_node=move_node_wrapper,
                export_nodes=export_nodes_wrapper,
                import_policy=import_policy,
                dry_run=dry_run,
                log_weave_entry=log_weave_entry_fn,
                log_to_file_msg=lambda m: _log_to_file_helper(m, "reconcile"),
            )
            
            # If dry_run, return the plan
            if dry_run and result_plan:
                return {
                    "success": True,
                    "dry_run": True,
                    "plan": result_plan,
                    "nodes_created": 0,
                    "nodes_updated": 0,
                    "nodes_deleted": 0,
                    "nodes_moved": 0,
                    "root_node_ids": [],
                    "api_calls": 0,
                    "retries": 0,
                    "rate_limit_hits": 0,
                    "errors": []
                }
            
            # OPTIONAL JEWEL UPGRADE: If auto_upgrade_to_jewel is enabled and
            # the reconciler returned a plan with per-node source_path / id
            # mappings, we call nexus_json_tools.transform_jewel() to write
            # the UUIDs back into the JEWEL JSON on disk. This upgrades the
            # creation JSON into a true JEWEL so that subsequent weaves are
            # incremental and UUID-aware.
            if auto_upgrade_to_jewel and not dry_run and isinstance(result_plan, dict):
                try:
                    ts_summary = result_plan.get("jewel_sync_summary") or {}
                    all_safe = ts_summary.get("all_safe", True)
                    unsafe_entries = ts_summary.get("unsafe_entries", [])
                    creates = result_plan.get("creates") or []
                    jewel_path = None
                    if isinstance(payload, dict):
                        jewel_path = payload.get("jewel_file")
                    if jewel_path and creates:
                        import importlib
                        client_dir = os.path.dirname(os.path.abspath(__file__))
                        wf_mcp_dir = os.path.dirname(client_dir)
                        mcp_servers_dir = os.path.dirname(wf_mcp_dir)
                        project_root = os.path.dirname(mcp_servers_dir)
                        if project_root not in sys.path:
                            sys.path.insert(0, project_root)
                        nexus_tools = importlib.import_module("nexus_json_tools")

                        ops = []
                        for c in creates:
                            cid = c.get("id")
                            path = c.get("source_path")
                            if not cid or path is None:
                                continue
                            ops.append({
                                "op": "SET_ATTRS_BY_PATH",
                                "path": path,
                                "attrs": {"id": cid},
                            })
                        if ops:
                            jewel_result = nexus_tools.transform_jewel(
                                jewel_file=jewel_path,
                                operations=ops,
                                dry_run=False,
                                stop_on_error=True,
                            )
                            logger.info(
                                "JEWEL upgrade applied via transform_jewel: "
                                f"success={jewel_result.get('success', True)} "
                                f"ops_applied={len(ops)}"
                            )
                except Exception as e:
                    logger.error(f"JEWEL auto-upgrade failed: {e}")

            # Reconciliation complete - gather root IDs
            root_ids = [n.get('id') for n in nodes_to_create if n.get('id')]

            # Mark weave journal as completed (best-effort).
            if weave_journal is not None and weave_journal_path is not None:
                try:
                    from datetime import datetime
                    weave_journal["last_run_completed"] = True
                    weave_journal["last_run_error"] = None
                    weave_journal["last_run_completed_at"] = datetime.now().isoformat()
                    weave_journal["phase"] = "completed"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Failed to update weave journal {weave_journal_path}: {e}")
            
            # Clear WEAVE context before returning success
            _current_weave_context["json_file"] = None
            
            result = {
                "success": len(stats["errors"]) == 0,
                "nodes_created": stats["nodes_created"],
                "nodes_updated": stats["nodes_updated"],
                "nodes_deleted": stats["nodes_deleted"],
                "nodes_moved": stats["nodes_moved"],
                "root_node_ids": root_ids,
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"],
            }
            
            # Add backup file info if auto-backup was created
            if target_backup_file:
                result["target_backup"] = target_backup_file

            # Surface weave journal location + previous incomplete flag to caller
            if weave_journal_path is not None and not dry_run:
                result["weave_journal"] = {
                    "path": weave_journal_path,
                    "previous_incomplete_run": bool(journal_warning),
                }

            # Best-effort: mark the reconciliation root as dirty so subsequent
            # /nodes-export-based operations under this parent will trigger a
            # refresh when needed.
            if not dry_run and parent_id:
                try:
                    self._mark_nodes_export_dirty([parent_id])
                except Exception:
                    # Cache dirty marking must never affect API behavior
                    pass
            
            return result
            
        except Exception as e:
            error_msg = f"Bulk import failed: {str(e)}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

            # Also append a final ERROR line to the reconcile debug log so the
            # log file clearly shows the failure cause at the end of the run.
            try:
                from datetime import datetime
                # Use tag-specific path (same logic as _log_to_file_helper)
                json_file_ctx = _current_weave_context.get("json_file")
                if json_file_ctx and os.path.exists(json_file_ctx):
                    log_path = os.path.join(os.path.dirname(json_file_ctx), "reconcile_debug.log")
                else:
                    log_path = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\reconcile_debug.log"
                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                with open(log_path, "a", encoding="utf-8") as dbg:
                    dbg.write(f"[{ts}] ERROR: {error_msg}\n")
            except Exception:
                # Logging to reconcile_debug.log is best-effort only
                pass

            # Mark weave journal as failed (best-effort).
            try:
                from datetime import datetime
                if weave_journal is not None and weave_journal_path is not None:
                    weave_journal["last_run_completed"] = False
                    weave_journal["last_run_error"] = error_msg
                    weave_journal["last_run_failed_at"] = datetime.now().isoformat()
                    weave_journal["phase"] = "error"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
            except Exception:
                # Journal updates must never break error reporting
                pass
            
            # Clear WEAVE context before returning
            _current_weave_context["json_file"] = None
            
            return {
                "success": False,
                "nodes_created": stats["nodes_created"],
                "nodes_updated": stats["nodes_updated"],
                "nodes_deleted": stats["nodes_deleted"],
                "nodes_moved": stats["nodes_moved"],
                "root_node_ids": [],
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"]
            }

    def nexus_list_keystones(self) -> dict[str, Any]:
        """List all available NEXUS Keystone backups."""
        backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
        if not os.path.exists(backup_dir):
            return {"success": True, "keystones": [], "message": "Backup directory does not exist."}
        
        keystones = []
        for filename in os.listdir(backup_dir):
            if filename.endswith(".json"):
                parts = filename.replace('.json', '').split('-')
                if len(parts) >= 3:
                    keystone_id = parts[-1]
                    timestamp = parts[0]
                    node_name = "-".join(parts[1:-1])
                    keystones.append({
                        "keystone_id": keystone_id,
                        "timestamp": timestamp,
                        "node_name": node_name,
                        "filename": filename
                    })
        
        return {"success": True, "keystones": sorted(keystones, key=lambda k: k['timestamp'], reverse=True)}

    async def nexus_restore_keystone(self, keystone_id: str) -> dict[str, Any]:
        """Restore a Workflowy node tree from a NEXUS Keystone backup."""
        backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
        
        target_file = None
        for filename in os.listdir(backup_dir):
            if keystone_id in filename and filename.endswith(".json"):
                target_file = os.path.join(backup_dir, filename)
                break

        if not target_file:
            return {"success": False, "error": f"Keystone with ID '{keystone_id}' not found."}

        # The bulk_import_from_file function will handle the restoration.
        # It reads the export_root_id from the JSON and uses it as the parent_id.
        return await self.bulk_import_from_file(json_file=target_file)

    def nexus_purge_keystones(self, keystone_ids: list[str]) -> dict[str, Any]:
        """Delete one or more NEXUS Keystone backup files."""
        backup_dir = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_backups"
        purged_files = []
        errors = []

        for keystone_id in keystone_ids:
            found = False
            for filename in os.listdir(backup_dir):
                if keystone_id in filename and filename.endswith(".json"):
                    try:
                        os.remove(os.path.join(backup_dir, filename))
                        purged_files.append(filename)
                        found = True
                        break 
                    except Exception as e:
                        errors.append(f"Failed to delete {filename}: {e}")
            if not found and not any(keystone_id in e for e in errors):
                errors.append(f"Keystone with ID '{keystone_id}' not found.")

        return {"success": len(errors) == 0, "purged_count": len(purged_files), "purged_files": purged_files, "errors": errors}

    def nexus_transform_jewel(
        self,
        jewel_file: str,
        operations: list[dict[str, Any]],
        dry_run: bool = False,
        stop_on_error: bool = True,
    ) -> dict[str, Any]:
        """Apply JEWELSTORM semantic operations to a NEXUS working_gem JSON file.

        This is an offline operation that delegates to nexus_json_tools.transform_jewel,
        using the same project-root import strategy as other NEXUS helpers.
        """
        import importlib

        try:
            client_dir = os.path.dirname(os.path.abspath(__file__))
            wf_mcp_dir = os.path.dirname(client_dir)
            mcp_servers_dir = os.path.dirname(wf_mcp_dir)
            project_root = os.path.dirname(mcp_servers_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            nexus_tools = importlib.import_module("nexus_json_tools")
        except Exception as e:  # noqa: BLE001
            raise NetworkError(f"Failed to import nexus_json_tools for transform_jewel: {e}") from e

        try:
            return nexus_tools.transform_jewel(  # type: ignore[attr-defined]
                jewel_file=jewel_file,
                operations=operations,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
            )
        except Exception as e:  # noqa: BLE001
            raise NetworkError(f"transform_jewel failed: {e}") from e

    
    def _get_nexus_dir(self, nexus_tag: str) -> str:
        """Resolve base directory for a CORINTHIAN NEXUS run (pure getter).

        Looks under temp\\nexus_runs for either:
        - <nexus_tag>
        - <TIMESTAMP>__<nexus_tag>

        Picks the lexicographically last match (latest run). Does NOT create
        any directories; callers that initialize state (nexus_scry, nexus_glimpse,
        Cartographer) are responsible for creating new run dirs.
        """
        from pathlib import Path

        base_dir = Path(
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
        )
        if not base_dir.exists():
            raise NetworkError(
                "No NEXUS runs directory exists yet under temp\\nexus_runs; "
                "run nexus_scry(...), nexus_glimpse(...), or Cartographer first for this tag."
            )

        candidates: list[Path] = []
        suffix = f"__{nexus_tag}"
        for child in base_dir.iterdir():
            if not child.is_dir():
                continue
            name = child.name
            if name == nexus_tag or name.endswith(suffix):
                candidates.append(child)

        if not candidates:
            raise NetworkError(
                f"No NEXUS run directory found for tag '{nexus_tag}'. "
                "Run nexus_scry(...), nexus_glimpse(...), or Cartographer first."
            )

        chosen = sorted(candidates, key=lambda p: p.name)[-1]
        return str(chosen)

    async def nexus_scry(
        self,
        nexus_tag: str,
        workflowy_root_id: str,
        max_depth: int,
        child_limit: int,
        reset_if_exists: bool = False,
        max_nodes: int | None = None,
    ) -> dict[str, Any]:
        """Tag-scoped SCRY → coarse_terrain.json for a CORINTHIAN NEXUS.

        This is the initiating stage of the PHANTOM GEMSTONE pipeline. It:
        - Exports a hierarchical SCRY of the Workflowy subtree rooted at
          ``workflowy_root_id`` using ``bulk_export_to_file``, with the given
          ``max_depth`` and ``child_limit`` parameters controlling vertical and
          horizontal truncation.
        - Optionally enforces a global node cap via ``max_nodes``; if the SCRY
          would export more than this many nodes, the operation aborts with a
          clear error (no JSON written).
        - Writes the result to ``coarse_terrain.json`` under the directory for
          ``nexus_tag``.

        The resulting JSON is the coarse TERRAIN (T0) used by later stages:
        - nexus_ignite_shards (IGNITE SHARDS → phantom_gem)
        - nexus_anchor_gems   (ANCHOR GEMS → shimmering_terrain)
        - nexus_anchor_jewels (ANCHOR JEWELS → enchanted_terrain)
        - nexus_weave_enchanted (WEAVE → Workflowy)

        Unlike ``nexus_glimpse(mode="full")``, which may legitimately produce
        T0 = S0 = T1 in a single step (because Dan's UI expansion already
        encodes the GEM/SHIMMERING decision), ``nexus_scry`` is explicitly
        **T0-only**: it produces only ``coarse_terrain.json``. S0 and T1 are
        introduced later via ``nexus_ignite_shards`` and ``nexus_anchor_gems``.
        """
        import shutil
        from pathlib import Path
        from datetime import datetime

        # Resolve base directory for this NEXUS tag. NEXUS runs are organized as
        # timestamped subdirectories under temp\\nexus_runs, e.g.:
        #   <YYYY-MM-DD_HH-MM-SS>__<nexus_tag>
        base_dir = Path(
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        # Optional cleanup: if reset_if_exists=True, remove any existing runs
        # for this tag before creating a fresh timestamped directory.
        if reset_if_exists:
            suffix = f"__{nexus_tag}"
            for child in base_dir.iterdir():
                if not child.is_dir():
                    continue
                name = child.name
                if name == nexus_tag or name.endswith(suffix):
                    shutil.rmtree(child, ignore_errors=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_dir = base_dir / f"{timestamp}__{nexus_tag}"
        run_dir.mkdir(parents=True, exist_ok=False)
        coarse_path = run_dir / "coarse_terrain.json"

        # Use the existing bulk_export_to_file helper to perform the SCRY.
        # We deliberately use use_efficient_traversal=False so that
        # max_depth/child_limit semantics are handled by the NEXUS export
        # pipeline (annotate_child_counts_and_truncate), and we pass max_nodes
        # through to enforce a hard upper bound on SCRY size when desired.
        result = await self.bulk_export_to_file(
            node_id=workflowy_root_id,
            output_file=str(coarse_path),
            include_metadata=True,
            use_efficient_traversal=False,
            max_depth=max_depth,
            child_count_limit=child_limit,
            max_nodes=max_nodes,
        )

        # Optionally, record a tiny manifest stub for this NEXUS run. We keep
        # it minimal here; richer tracking can be layered on later.
        manifest_path = run_dir / "nexus_manifest.json"
        try:
            from datetime import datetime

            manifest = {
                "nexus_tag": nexus_tag,
                "workflowy_root_id": workflowy_root_id,
                "max_depth": max_depth,
                "child_limit": child_limit,
                "max_nodes": max_nodes,
                "stage": "coarse_terrain",
                "timestamp": datetime.now().isoformat(timespec="seconds"),
            }
            with open(manifest_path, "w", encoding="utf-8") as mf:
                json.dump(manifest, mf, indent=2)
        except Exception:
            # Manifest is best-effort only; never block NEXUS on this.
            pass

        return {
            "success": bool(result.get("success", True)),
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "node_count": result.get("node_count"),
            "depth": result.get("depth"),
        }

    async def nexus_ignite_shards(
        self,
        nexus_tag: str,
        root_ids: list[str],
        max_depth: int | None = None,
        child_limit: int | None = None,
        per_root_limits: dict[str, dict[str, int]] | None = None,
    ) -> dict[str, Any]:
        """IGNITE SHARDS → phantom_gem.json (PHANTOM GEM S0).

        This stage performs a shards-only deeper SCRY for the selected roots and
        writes the result to ``phantom_gem.json`` under the directory for
        ``nexus_tag``. It does **not** produce a new terrain file; instead, it
        prepares the PHANTOM GEM that later stages (ANCHOR GEMS, QUILLSTORM,
        ANCHOR JEWELS) will use.

        Depth/child semantics:
        - The underlying export fetches full subtrees for each root, but
          ``max_depth`` and ``child_limit`` are applied at the JSON level via
          _annotate_child_counts_and_truncate, mirroring the NEXUS SCRY
          behavior used in bulk_export_to_file.
        - per_root_limits (if provided) can override max_depth/child_limit on a
          per-root basis: {root_id: {"max_depth": d, "child_limit": c}}.

        SAFETY INVARIANT:
        - The set of roots must be disjoint: no root may be an ancestor or
          descendant of another root in the coarse_terrain tree. If such a
          relationship is detected, this tool fails with a clear error rather
          than attempting to construct an overlapping PHANTOM GEM.
        """
        import logging

        logger = _ClientLogger()

        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_path = os.path.join(run_dir, "phantom_gem.json")

        # Ensure coarse terrain exists for this nexus_tag (Tool 1 must run first).
        if not os.path.exists(coarse_path):
            raise NetworkError(
                "coarse_terrain.json not found for nexus_tag. "
                "Call nexus_scry(...) before nexus_ignite_shards(...)."
            )

        if not root_ids:
            # Nothing to ignite; write an empty phantom gem and return.
            empty_payload = {"nexus_tag": nexus_tag, "roots": [], "nodes": []}
            try:
                with open(phantom_path, "w", encoding="utf-8") as f:
                    json.dump(empty_payload, f, indent=2, ensure_ascii=False)
            except Exception as e:
                raise NetworkError(f"Failed to write empty phantom_gem.json: {e}") from e

            return {
                "success": True,
                "nexus_tag": nexus_tag,
                "phantom_gem": phantom_path,
                "roots": [],
                "node_count": 0,
            }

        # HARD SAFETY: roots must be pairwise disjoint (no ancestor/descendant
        # relationships) according to the coarse_terrain tree.
        try:
            with open(coarse_path, "r", encoding="utf-8") as f:
                coarse_data = json.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to read coarse_terrain.json: {e}") from e

        if not (isinstance(coarse_data, dict) and "nodes" in coarse_data):
            raise NetworkError(
                "coarse_terrain.json must be an export package with 'nodes' key. "
                "Re-scry the NEXUS via nexus_scry(...) if this is not the case."
            )

        terrain_nodes: list[dict[str, Any]] = coarse_data.get("nodes", [])

        # Initialize original_ids_seen ledger from coarse TERRAIN if present,
        # otherwise derive it from the current coarse nodes (fallback for
        # pre-ledger files). This ledger will be extended with any new node
        # IDs discovered by deep SCRYs for shard roots.
        original_ids_seen: set[str] = set()
        if isinstance(coarse_data.get("original_ids_seen"), list):
            original_ids_seen.update(
                str(nid) for nid in coarse_data.get("original_ids_seen", []) if nid
            )
        else:
            def _collect_ids_from_nodes(nodes: list[dict[str, Any]]) -> None:
                for node in nodes or []:
                    if not isinstance(node, dict):
                        continue
                    nid = node.get("id")
                    if nid:
                        original_ids_seen.add(str(nid))
                    _collect_ids_from_nodes(node.get("children") or [])

            _collect_ids_from_nodes(terrain_nodes)
            # Also include the coarse export root itself if present
            if export_root_id:
                original_ids_seen.add(str(export_root_id))

        # Build parent and node maps from hierarchical nodes (id -> parent_id).
        parent_by_id: dict[str, str | None] = {}
        node_by_id: dict[str, dict[str, Any]] = {}

        def _index_parents(nodes: list[dict[str, Any]], parent_id: str | None) -> None:
            for node in nodes:
                nid = node.get("id")
                if nid:
                    parent_by_id[nid] = parent_id
                    node_by_id[nid] = node
                    children = node.get("children") or []
                    if children:
                        _index_parents(children, nid)

        # Treat the coarse_terrain export_root_id as the synthetic parent of all
        # top-level nodes so that any pair of roots at least shares this ancestor.
        export_root_id = coarse_data.get("export_root_id")
        _index_parents(terrain_nodes, export_root_id)

        # Normalize roots: dedupe while preserving order.
        unique_root_ids: list[str] = []
        for rid in root_ids:
            if rid not in unique_root_ids:
                unique_root_ids.append(rid)
        roots_set = set(unique_root_ids)

        # Ensure all roots exist in the coarse terrain tree.
        missing = [rid for rid in roots_set if rid not in parent_by_id]
        if missing:
            raise NetworkError(
                "nexus_ignite_shards: one or more roots are not present in coarse_terrain.json "
                f"for nexus_tag={nexus_tag}: {missing}. "
                "Choose roots from the current coarse SCRY (nexus_scry)."
            )

        # Enforce disjointness: walk ancestor chain for each root and ensure we
        # never encounter another root_id in that chain.
        for rid in roots_set:
            parent = parent_by_id.get(rid)
            while parent is not None:
                if parent in roots_set:
                    raise NetworkError(
                        "nexus_ignite_shards: invalid root set; roots must be disjoint.\n"
                        f"Root '{rid}' is a descendant of root '{parent}'.\n"
                        "Choose either the ancestor or the deeper branch, but not both."
                    )
                parent = parent_by_id.get(parent)

        # Compute the nearest common ancestor (LCA) of all shard roots within
        # the coarse_terrain tree. This is metadata only for now (export_root
        # for the PHANTOM GEM); we leave the existing 'nodes' forest shape
        # unchanged for compatibility with downstream tools.
        def _ancestor_chain(nid: str) -> list[str]:
            chain: list[str] = []
            cur = nid
            while cur is not None:
                chain.append(cur)
                cur = parent_by_id.get(cur)
            return chain

        ancestor_lists: list[list[str]] = []
        for rid in unique_root_ids:
            ancestor_lists.append(_ancestor_chain(rid))

        # Start from the first root's ancestor chain and intersect with others
        common_ancestors: set[str] = set(ancestor_lists[0])
        for chain in ancestor_lists[1:]:
            common_ancestors &= set(chain)

        if not common_ancestors:
            raise NetworkError(
                "nexus_ignite_shards: could not find a common ancestor for shard roots "
                f"{sorted(list(roots_set))} within coarse_terrain. Ensure all shards come "
                "from the same SCRY (nexus_scry) subtree."
            )

        # Nearest common ancestor = first ancestor of the first root that is in
        # the intersection set (closest to the leaves).
        lca_id = next(a for a in ancestor_lists[0] if a in common_ancestors)

        # Resolve a human-readable name for the LCA if possible.
        if lca_id == export_root_id:
            lca_name = coarse_data.get("export_root_name", "Root")
        else:
            lca_node = node_by_id.get(lca_id) or {}
            lca_name = lca_node.get("name", "Root")

        deep_subtrees: dict[str, dict[str, Any]] = {}
        roots_resolved: list[str] = []
        total_nodes_fetched = 0

        def _collect_ids_from_subtree(node: dict[str, Any]) -> None:
            """Accumulate all Workflowy node IDs reachable from this subtree.

            Used to extend original_ids_seen with any nodes revealed only by
            deep SCRY at shard roots (beyond what coarse TERRAIN saw).
            """
            if not isinstance(node, dict):
                return
            nid = node.get("id")
            if nid:
                original_ids_seen.add(str(nid))
            for child in node.get("children") or []:
                _collect_ids_from_subtree(child)

        per_root_limits = per_root_limits or {}

        for root_id in unique_root_ids:
            limits = per_root_limits.get(root_id, {})
            root_max_depth = limits.get("max_depth", max_depth)
            root_child_limit = limits.get("child_limit", child_limit)

            try:
                raw = await self.export_nodes(node_id=root_id)
            except Exception as e:
                logger.error(f"nexus_ignite_shards: export failed for root {root_id}: {e}")
                continue

            flat_nodes = raw.get("nodes", [])
            if not flat_nodes:
                logger.warning(f"nexus_ignite_shards: no nodes returned for root {root_id}")
                continue

            total_nodes_fetched += raw.get("_total_fetched_from_api", len(flat_nodes))

            # Build hierarchy and locate the subtree for this root
            tree = self._build_hierarchy(flat_nodes, include_metadata=True)
            if not tree:
                logger.warning(f"nexus_ignite_shards: hierarchy empty for root {root_id}")
                continue

            root_subtree = None
            for candidate in tree:
                if candidate.get("id") == root_id:
                    root_subtree = candidate
                    break

            if root_subtree is None:
                root_subtree = tree[0]
                logger.warning(
                    "nexus_ignite_shards: could not find root %s in hierarchy; "
                    "using first root %s",
                    root_id,
                    root_subtree.get("id"),
                )

            # Annotate/truncate subtree according to limits for this root
            self._annotate_child_counts_and_truncate(
                [root_subtree],
                max_depth=root_max_depth,
                child_count_limit=root_child_limit,
                current_depth=1,
            )

            deep_subtrees[root_id] = root_subtree
            roots_resolved.append(root_id)

            # Extend ledger with all IDs from this deep subtree
            _collect_ids_from_subtree(root_subtree)

        if not roots_resolved:
            phantom_payload = {
                "nexus_tag": nexus_tag,
                "roots": [],
                "export_root_id": export_root_id,
                "export_root_name": coarse_data.get("export_root_name", "Root"),
                "original_ids_seen": sorted(original_ids_seen),
                "nodes": [],
            }
        else:
            # Build GEM skeleton from coarse TERRAIN root down to each shard root.
            skeleton_nodes_by_id: dict[str, dict[str, Any]] = {}
            top_level_ids: list[str] = []
            top_level_nodes: list[dict[str, Any]] = []

            def ensure_skeleton_node(node_id: str) -> dict[str, Any]:
                source = node_by_id.get(node_id)
                if not source:
                    raise NetworkError(
                        f"nexus_ignite_shards: node {node_id} not found in coarse_terrain.json"
                    )
                if node_id in skeleton_nodes_by_id:
                    return skeleton_nodes_by_id[node_id]
                skeleton = {k: v for k, v in source.items() if k != "children"}
                skeleton["children"] = []
                skeleton_nodes_by_id[node_id] = skeleton
                return skeleton

            for root_id in roots_resolved:
                # Walk from shard root up to the coarse TERRAIN root, then back down
                path: list[str] = []
                cur = root_id
                while cur is not None and cur != export_root_id:
                    path.append(cur)
                    cur = parent_by_id.get(cur)
                if cur != export_root_id:
                    raise NetworkError(
                        "nexus_ignite_shards: root "
                        f"{root_id} does not descend from coarse export_root_id {export_root_id}"
                    )
                path.reverse()

                parent_id = export_root_id
                for nid in path:
                    node_skel = ensure_skeleton_node(nid)
                    if parent_id == export_root_id:
                        if nid not in top_level_ids:
                            top_level_ids.append(nid)
                            top_level_nodes.append(node_skel)
                    else:
                        parent_skel = skeleton_nodes_by_id[parent_id]
                        children_list = parent_skel.get("children") or []
                        if not any(
                            isinstance(c, dict) and c.get("id") == nid
                            for c in children_list
                        ):
                            children_list.append(node_skel)
                            parent_skel["children"] = children_list
                    parent_id = nid

            # Replace skeleton nodes at each shard root with the deep SCRY subtrees
            for root_id, root_subtree in deep_subtrees.items():
                parent_id = parent_by_id.get(root_id)
                if parent_id is None or parent_id == export_root_id:
                    for idx, nid in enumerate(top_level_ids):
                        if nid == root_id:
                            top_level_nodes[idx] = root_subtree
                            break
                else:
                    parent_skel = skeleton_nodes_by_id.get(parent_id)
                    if not parent_skel:
                        raise NetworkError(
                            f"nexus_ignite_shards: parent {parent_id} of shard {root_id} missing in skeleton"
                        )
                    children_list = parent_skel.get("children") or []
                    replaced = False
                    for idx, child in enumerate(children_list):
                        if isinstance(child, dict) and child.get("id") == root_id:
                            children_list[idx] = root_subtree
                            replaced = True
                            break
                    if not replaced:
                        children_list.append(root_subtree)
                    parent_skel["children"] = children_list
                skeleton_nodes_by_id[root_id] = root_subtree

            # Compute preview_tree FIRST
            phantom_preview = None
            try:
                phantom_preview = self._annotate_preview_ids_and_build_tree(
                    top_level_nodes,
                    prefix="PG",
                )
            except Exception:
                # Preview is best-effort only; never break IGNITE.
                pass
            
            phantom_payload = {
                "nexus_tag": nexus_tag,
                "__preview_tree__": phantom_preview,
                "export_root_id": export_root_id,
                "export_root_name": coarse_data.get("export_root_name", "Root"),
                "nodes": top_level_nodes,
                "roots": roots_resolved,
                "original_ids_seen": sorted(original_ids_seen),
            }

        # Best-effort: update coarse_terrain.json with the expanded ledger so
        # downstream consumers (e.g. anchor_gems) can see all originally-seen
        # IDs without recomputing.
        try:
            coarse_data["original_ids_seen"] = sorted(original_ids_seen)
            with open(coarse_path, "w", encoding="utf-8") as f:
                json.dump(coarse_data, f, indent=2, ensure_ascii=False)
        except Exception:
            logger.warning("nexus_ignite_shards: failed to update coarse_terrain original_ids_seen ledger")

        _log_to_file_helper(
            f"nexus_ignite_shards[{nexus_tag}]: roots={roots_resolved}, export_root_id={export_root_id}, node_count={total_nodes_fetched}",
            "nexus",
        )

        try:
            with open(phantom_path, "w", encoding="utf-8") as f:
                json.dump(phantom_payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to write phantom_gem.json: {e}") from e

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "phantom_gem": phantom_path,
            "roots": roots_resolved,
            "node_count": total_nodes_fetched,
        }

    async def nexus_glimpse(
        self,
        nexus_tag: str,
        workflowy_root_id: str,
        reset_if_exists: bool = False,
        mode: str = "full",
        _ws_connection=None,
        _ws_queue=None,
    ) -> dict[str, Any]:
        """GLIMPSE → TERRAIN + PHANTOM GEM (zero API calls).

        Ultimate usability: instead of nexus_scry + nexus_ignite_shards (both via API),
        GLIMPSE what you've expanded in Workflowy → both TERRAIN and PHANTOM GEM created
        from that single local WebSocket extraction.

        - Zero Workflowy /nodes-export API calls
        - Dan controls granularity (expand what you want, GLIMPSE captures it)
        - PHANTOM GEM = exactly what Dan sees
        - Instant (no rate-limit delays)

        Args:
            nexus_tag: Human-readable tag for this NEXUS run.
            workflowy_root_id: Root node UUID to GLIMPSE.
            reset_if_exists: If True, overwrite existing NEXUS state for this tag.
            mode: Output mode control:
                "full" (default) - Write T0 + S0 + T1 (all identical). Skip directly to QUILLSTORM.
                "coarse_terrain_only" - Write only T0. Use for hybrid workflow: GLIMPSE for map,
                                        then nexus_ignite_shards on specific roots later.
            _ws_connection: WebSocket connection from server.py (internal)
            _ws_queue: WebSocket message queue from server.py (internal)

        Returns:
            mode="full":
                {"success": True, "nexus_tag": str, "coarse_terrain": path, "phantom_gem": path,
                 "shimmering_terrain": path, "node_count": int, "depth": int, "_source": "glimpse"}
            mode="coarse_terrain_only":
                {"success": True, "nexus_tag": str, "coarse_terrain": path,
                 "node_count": int, "depth": int, "_source": "glimpse"}
        """
        import shutil
        from pathlib import Path
        from datetime import datetime

        # Validate mode parameter
        if mode not in ("full", "coarse_terrain_only"):
            raise NetworkError(
                f"Invalid mode '{mode}'. Must be 'full' or 'coarse_terrain_only'."
            )

        base_dir = Path(
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        # Determine run_dir: reuse latest existing run for this tag when present,
        # otherwise create a new timestamped directory. When reset_if_exists=True,
        # always create a fresh timestamped directory (and optionally clean out
        # older runs for this tag).
        existing_dir: Path | None = None
        if not reset_if_exists:
            try:
                existing = self._get_nexus_dir(nexus_tag)
                existing_dir = Path(existing)
            except NetworkError:
                existing_dir = None

        if reset_if_exists:
            suffix = f"__{nexus_tag}"
            for child in base_dir.iterdir():
                if not child.is_dir():
                    continue
                name = child.name
                if name == nexus_tag or name.endswith(suffix):
                    shutil.rmtree(child, ignore_errors=True)
            existing_dir = None

        if existing_dir is not None:
            run_dir = existing_dir
        else:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            run_dir = base_dir / f"{timestamp}__{nexus_tag}"
            run_dir.mkdir(parents=True, exist_ok=False)

        coarse_path = run_dir / "coarse_terrain.json"
        phantom_gem_path = run_dir / "phantom_gem.json"
        shimmering_path = run_dir / "shimmering_terrain.json"

        logger = _ClientLogger()
        
        # STEP 1: Get WebSocket GLIMPSE (what Dan expanded in UI)
        try:
            logger.info(f"🔌 NEXUS GLIMPSE Step 1: WebSocket extraction for {workflowy_root_id[:8]}...")
            ws_glimpse = await self.workflowy_glimpse(workflowy_root_id, _ws_connection=_ws_connection, _ws_queue=_ws_queue)
        except Exception as e:
            raise NetworkError(f"GLIMPSE (WebSocket) failed for root {workflowy_root_id}: {e}") from e

        if not ws_glimpse.get("success"):
            raise NetworkError(
                f"GLIMPSE (WebSocket) returned failure for root {workflowy_root_id}: "
                f"{ws_glimpse.get('error', 'unknown error')}"
            )
        
        # STEP 2: Get API GLIMPSE FULL (complete tree structure with all children)
        logger.info(f"📡 NEXUS GLIMPSE Step 2: API fetch for complete tree structure...")
        try:
            # Use high size_limit to avoid artificial truncation - we need the FULL tree
            api_glimpse = await self.workflowy_scry(
                node_id=workflowy_root_id,
                use_efficient_traversal=False,
                depth=None,  # Full depth
                size_limit=50000  # High limit - we need complete structure
            )
        except Exception as e:
            logger.warning(f"⚠️ API fetch failed: {e} - falling back to WebSocket-only (UNSAFE for WEAVE!)")
            # Fallback: use WebSocket-only result but mark ALL nodes as potentially having hidden children
            children = ws_glimpse.get("children", [])
            mark_all_nodes_as_potentially_truncated(children)
            api_glimpse = None
        
        # STEP 3: MERGE - Use shared helpers for TERRAIN export logic
        logger.info(
            "🔀 NEXUS GLIMPSE Step 3: Merging WebSocket + API results via shared helpers..."
        )

        # Use shared helper to annotate has_hidden_children / children_status
        # on the WebSocket subtree and derive the root_children_status.
        children, root_children_status, api_merge_performed = merge_glimpses_for_terrain(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )

        # Build original_ids_seen ledger for this NEXUS tag. Prefer the full
        # API view when available (complete subtree under workflowy_root_id),
        # falling back to the WebSocket subtree when API is unavailable.
        original_ids_seen = build_original_ids_seen_from_glimpses(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )

        # Construct TERRAIN wrapper in the standard NEXUS format.
        terrain_data = build_terrain_export_from_glimpse(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            children=children,
            root_children_status=root_children_status,
            original_ids_seen=original_ids_seen,
        )

        # Compute and attach preview_tree IMMEDIATELY (before writing files)
        terrain_preview = None
        try:
            terrain_preview = self._annotate_preview_ids_and_build_tree(
                terrain_data.get("nodes") or [],
                prefix="CT",
            )
        except Exception:
            # Preview is best-effort only; never break GLIMPSE.
            pass
        
        # Insert preview early in terrain_data dict
        if terrain_preview is not None:
            # Rebuild dict with terrain_preview in early position
            terrain_data = {
                "export_timestamp": terrain_data.get("export_timestamp"),
                "export_root_children_status": terrain_data.get("export_root_children_status"),
                "__preview_tree__": terrain_preview,
                "export_root_id": terrain_data.get("export_root_id"),
                "export_root_name": terrain_data.get("export_root_name"),
                "nodes": terrain_data.get("nodes"),
                "original_ids_seen": terrain_data.get("original_ids_seen"),
            }

        # Always write coarse_terrain.json
        try:
            with open(coarse_path, "w", encoding="utf-8") as f:
                json.dump(terrain_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise NetworkError(f"Failed to write coarse_terrain.json: {e}") from e

        result = {
            "success": True,
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "node_count": ws_glimpse.get("node_count", 0),
            "depth": ws_glimpse.get("depth", 0),
            "_source": "glimpse_merged",  # Indicate WebSocket+API merge was performed
            "_api_merge_performed": api_merge_performed,
            "mode": mode,
        }

        if mode == "full":
            # GLIMPSE path: T0 = S0 = T1 (all identical)
            # Write phantom_gem.json and shimmering_terrain.json
            try:
                with open(phantom_gem_path, "w", encoding="utf-8") as f:
                    json.dump(terrain_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                raise NetworkError(f"Failed to write phantom_gem.json: {e}") from e

            try:
                with open(shimmering_path, "w", encoding="utf-8") as f:
                    json.dump(terrain_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                raise NetworkError(f"Failed to write shimmering_terrain.json: {e}") from e

            result["phantom_gem"] = str(phantom_gem_path)
            result["shimmering_terrain"] = str(shimmering_path)

        # mode="coarse_terrain_only": only coarse_terrain.json written
        # User will call nexus_ignite_shards later to create phantom_gem.json
        
        # Log to persistent file (use ws_glimpse which has the merged tree data)
        _log_glimpse_to_file("glimpse", workflowy_root_id, ws_glimpse)

        return result


    async def nexus_anchor_gems(self, nexus_tag: str) -> dict[str, Any]:
        """ANCHOR GEMS → shimmering_terrain.json.

        This stage imprints the PHANTOM GEM (phantom_gem.json) into the coarse
        TERRAIN (coarse_terrain.json) to produce SHIMMERING TERRAIN
        (shimmering_terrain.json) for the given nexus_tag.

        Files under this nexus_tag directory:
        - coarse_terrain.json   (T0)
        - phantom_gem.json      (S0; unrefracted GEM)
        - shimmering_terrain.json (T1; anchored gems)

        The phantom_gem remains unchanged as the witness GEM S0; later stages
        (QUILLSTORM on phantom_gem → phantom_jewel.json and
        nexus_anchor_jewels) consume it.
        """
        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_path = os.path.join(run_dir, "phantom_gem.json")
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")

        if not os.path.exists(coarse_path):
            raise NetworkError(
                "coarse_terrain.json not found for nexus_tag. "
                "Call nexus_scry(...) before nexus_anchor_gems(...)."
            )

        if not os.path.exists(phantom_path):
            raise NetworkError(
                "phantom_gem.json not found for nexus_tag. "
                "Call nexus_ignite_shards(...) before nexus_anchor_gems(...)."
            )

        try:
            with open(coarse_path, "r", encoding="utf-8") as f:
                coarse_data = json.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to read coarse_terrain.json: {e}") from e

        try:
            with open(phantom_path, "r", encoding="utf-8") as f:
                phantom_data = json.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to read phantom_gem.json: {e}") from e

        # Expect phantom_gem payload of the form:
        # {"nexus_tag": ..., "roots": [R1, R2, ...], "nodes": [subtree_R1, subtree_R2, ...]}
        phantom_roots: list[str] = phantom_data.get("roots", [])
        phantom_nodes: list[dict[str, Any]] = phantom_data.get("nodes", [])

        if not phantom_roots or not phantom_nodes:
            # Nothing to anchor; copy coarse terrain forward unchanged.
            try:
                with open(shimmering_path, "w", encoding="utf-8") as f:
                    json.dump(coarse_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                raise NetworkError(f"Failed to write shimmering_terrain.json: {e}") from e

            return {
                "success": True,
                "nexus_tag": nexus_tag,
                "shimmering_terrain": shimmering_path,
                "roots": [],
            }

        # Build a lookup from phantom node id → subtree over the entire GEM tree
        subtree_by_id: dict[str, dict[str, Any]] = {}

        def _index_phantom_subtrees(nodes: list[dict[str, Any]]) -> None:
            for node in nodes or []:
                if not isinstance(node, dict):
                    continue
                nid = node.get("id")
                if nid:
                    subtree_by_id[nid] = node
                children = node.get("children") or []
                if children:
                    _index_phantom_subtrees(children)

        _index_phantom_subtrees(phantom_nodes)

        # Coarse terrain is an export package with metadata and "nodes" array.
        # We only modify the editable "nodes" list, leaving header untouched.
        if not (isinstance(coarse_data, dict) and "nodes" in coarse_data):
            raise NetworkError(
                "coarse_terrain.json must be an export package with 'nodes' key. "
                "Re-scry the NEXUS via nexus_scry(...) if this is not the case."
            )

        terrain_nodes: list[dict[str, Any]] = coarse_data.get("nodes", [])

        def replace_subtree_in_list(nodes: list[dict[str, Any]]) -> None:
            """Recursively replace any subtree whose id matches a phantom root."""
            for idx, node in enumerate(nodes):
                nid = node.get("id")
                if nid in subtree_by_id:
                    # Replace this node with the phantom subtree deep copy
                    nodes[idx] = subtree_by_id[nid]
                else:
                    children = node.get("children") or []
                    if children:
                        replace_subtree_in_list(children)

        replace_subtree_in_list(terrain_nodes)

        # Merge original_ids_seen ledgers from TERRAIN and GEM (if present).
        original_ids_seen: set[str] = set(coarse_data.get("original_ids_seen", []) or [])
        original_ids_seen.update(phantom_data.get("original_ids_seen", []) or [])
        if original_ids_seen:
            coarse_data["original_ids_seen"] = sorted({str(nid) for nid in original_ids_seen})

        # Merge explicitly_preserved_ids from TERRAIN and GEM (if present).
        explicitly_preserved_ids: set[str] = set(coarse_data.get("explicitly_preserved_ids", []) or [])
        explicitly_preserved_ids.update(phantom_data.get("explicitly_preserved_ids", []) or [])
        if explicitly_preserved_ids:
            coarse_data["explicitly_preserved_ids"] = sorted({str(nid) for nid in explicitly_preserved_ids})

        # Write shimmering terrain out; header from coarse_terrain is preserved.
        coarse_data["nodes"] = terrain_nodes

        # Compute and attach shimmering_preview EARLY
        shimmering_preview = None
        try:
            shimmering_preview = self._annotate_preview_ids_and_build_tree(
                coarse_data.get("nodes") or [],
                prefix="ST",
            )
        except Exception:
            # Preview is best-effort only; never break ANCHOR GEMS.
            pass
        
        # Rebuild coarse_data with shimmering_preview in early position
        if shimmering_preview is not None:
            coarse_data = {
                "export_timestamp": coarse_data.get("export_timestamp"),
                "export_root_children_status": coarse_data.get("export_root_children_status"),
                "__preview_tree__": shimmering_preview,
                "export_root_id": coarse_data.get("export_root_id"),
                "export_root_name": coarse_data.get("export_root_name"),
                "nodes": coarse_data.get("nodes"),
                "original_ids_seen": coarse_data.get("original_ids_seen"),
                "explicitly_preserved_ids": coarse_data.get("explicitly_preserved_ids"),
            }

        try:
            with open(shimmering_path, "w", encoding="utf-8") as f:
                json.dump(coarse_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to write shimmering_terrain.json: {e}") from e

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "shimmering_terrain": shimmering_path,
            "roots": phantom_roots,
        }

    def _launch_detached_weave(
        self,
        mode: str,
        nexus_tag: str | None = None,
        json_file: str | None = None,
        parent_id: str | None = None,
        dry_run: bool = False,
        import_policy: str = "strict"
    ) -> dict[str, Any]:
        """Launch NEXUS WEAVE as a detached background process (survives MCP restart).
        
        Supports two modes:
        - ENCHANTED: Uses nexus_tag to find enchanted_terrain.json
        - DIRECT: Uses explicit json_file path
        
        Returns:
            {"success": True, "job_id": "weave-<id>", "pid": 12345, "detached": True}
        """
        import subprocess
        
        # Validate inputs based on mode
        if mode == 'enchanted':
            if not nexus_tag:
                raise ValueError("nexus_tag required for enchanted mode")
            run_dir = self._get_nexus_dir(nexus_tag)
            enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")
            if not os.path.exists(enchanted_path):
                raise NetworkError(
                    "enchanted_terrain.json not found. "
                    "Call nexus_anchor_jewels(...) before weave."
                )
            job_id = f"weave-enchanted-{nexus_tag}"
        else:  # direct mode
            if not json_file:
                raise ValueError("json_file required for direct mode")
            if not os.path.exists(json_file):
                raise NetworkError(f"JSON file not found: {json_file}")
            job_id = f"weave-direct-{Path(json_file).stem}"
        
        # Determine worker script path
        worker_script = os.path.join(os.path.dirname(__file__), "..", "weave_worker.py")
        worker_script = os.path.abspath(worker_script)
        
        if not os.path.exists(worker_script):
            raise NetworkError(f"weave_worker.py not found at {worker_script}")
        
        # Build command line arguments
        cmd = [sys.executable, worker_script, '--mode', mode, '--dry-run', str(dry_run).lower()]
        
        if mode == 'enchanted':
            cmd.extend(['--nexus-tag', nexus_tag])
        else:  # direct
            cmd.extend(['--json-file', json_file])
            if parent_id:
                cmd.extend(['--parent-id', parent_id])
            cmd.extend(['--import-policy', import_policy])
        
        # Determine log file location (same directory as journal/PID file)
        if mode == 'enchanted':
            run_dir = self._get_nexus_dir(nexus_tag)
            log_file = os.path.join(run_dir, ".weave.log")
        else:  # direct
            json_path = Path(json_file)
            log_file = str(json_path.parent / ".weave.log")
        
        # Prepare environment (pass API key and nexus_runs path to worker)
        env = os.environ.copy()
        env['WORKFLOWY_API_KEY'] = self.config.api_key.get_secret_value()
        
        # Pass nexus_runs base directory (so worker doesn't have to calculate it)
        # Hardcoded to match _get_nexus_dir() base location
        nexus_runs_base = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        env['NEXUS_RUNS_BASE'] = nexus_runs_base
        
        # Open log file for stdout/stderr capture
        log_handle = open(log_file, 'w', encoding='utf-8')
        
        # Launch detached process
        log_event(f"Launching detached WEAVE worker ({mode} mode), logs: {log_file}", "DETACHED")
        
        try:
            process = subprocess.Popen(
                cmd,
                env=env,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0,
                start_new_session=True if sys.platform != 'win32' else False,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT  # Redirect stderr to same file as stdout
            )
            
            pid = process.pid
            log_event(f"Detached worker launched: PID={pid}, mode={mode}, log={log_file}", "DETACHED")
            
            # Note: log_handle will be closed by the parent process eventually,
            # but the child process inherits the file descriptor and keeps writing
            
            return {
                "success": True,
                "job_id": job_id,
                "pid": pid,
                "detached": True,
                "mode": mode,
                "log_file": log_file,
                "note": f"Worker process detached - survives MCP restart. Logs: {log_file}"
            }
            
        except Exception as e:
            raise NetworkError(f"Failed to launch detached WEAVE worker: {e}") from e
    
    def nexus_weave_enchanted_detached(self, nexus_tag: str, dry_run: bool = False) -> dict[str, Any]:
        """Launch ENCHANTED TERRAIN WEAVE as detached process."""
        return self._launch_detached_weave(
            mode='enchanted',
            nexus_tag=nexus_tag,
            dry_run=dry_run
        )
    
    def nexus_weave_detached(self, json_file: str, parent_id: str | None = None, 
                            dry_run: bool = False, import_policy: str = 'strict') -> dict[str, Any]:
        """Launch DIRECT WEAVE as detached process."""
        return self._launch_detached_weave(
            mode='direct',
            json_file=json_file,
            parent_id=parent_id,
            dry_run=dry_run,
            import_policy=import_policy
        )

    async def nexus_weave_enchanted(self, nexus_tag: str, dry_run: bool = False) -> dict[str, Any]:
        """WEAVE ENCHANTED TERRAIN → ETHER (Workflowy).
        
        Final step of PHANTOM GEMSTONE NEXUS. Reads enchanted_terrain.json and
        applies it to Workflowy via bulk_import_from_file (reconciliation algorithm).
        
        Args:
            nexus_tag: NEXUS tag identifying the run
            dry_run: If True, preview operations without executing
            
        Returns:
            Result from bulk_import_from_file (nodes created/updated/deleted/moved)
        """
        run_dir = self._get_nexus_dir(nexus_tag)
        enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")
        
        if not os.path.exists(enchanted_path):
            raise NetworkError(
                "enchanted_terrain.json not found for nexus_tag. "
                "Call nexus_anchor_jewels(...) before nexus_weave_enchanted(...)."
            )
        
        # Use bulk_import_from_file (reconciliation algorithm)
        # parent_id=None means it reads export_root_id from JSON metadata
        return await self.bulk_import_from_file(
            json_file=enchanted_path,
            parent_id=None,
            dry_run=dry_run,
            import_policy='strict',
        )
    
    async def nexus_anchor_jewels(self, nexus_tag: str) -> dict[str, Any]:
        """ANCHOR JEWELS → enchanted_terrain.json.

        This stage performs the 3-way SHARD FUSE (T/S0/S1) at the JSON level,
        using the existing fuse-shard-3way implementation in nexus_json_tools.

        Inputs under this nexus_tag directory:
        - shimmering_terrain.json  (T1)
        - phantom_gem.json         (S0; witness GEM)
        - phantom_jewel.json       (S1; morphed GEM via QUILLSTORM on S0)

        Output:
        - enchanted_terrain.json   (T2), ready for WEAVE back into Workflowy.
        """
        import shutil
        import importlib

        run_dir = self._get_nexus_dir(nexus_tag)
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")
        phantom_gem_path = os.path.join(run_dir, "phantom_gem.json")
        phantom_jewel_path = os.path.join(run_dir, "phantom_jewel.json")
        enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")

        if not os.path.exists(shimmering_path):
            raise NetworkError(
                "shimmering_terrain.json not found for nexus_tag. "
                "Call nexus_anchor_gems(...) before nexus_anchor_jewels(...)."
            )

        if not os.path.exists(phantom_gem_path):
            raise NetworkError(
                "phantom_gem.json not found for nexus_tag. "
                "Call nexus_ignite_shards(...) before nexus_anchor_jewels(...)."
            )

        if not os.path.exists(phantom_jewel_path):
            raise NetworkError(
                "phantom_jewel.json not found for nexus_tag. "
                "Create it by applying a QUILLSTORM to phantom_gem.json "
                "(QUILLSTRIKE → edits → QUILLMORPH)."
            )

        # Start from a copy of shimmering_terrain.json so T1 is preserved.
        try:
            shutil.copy2(shimmering_path, enchanted_path)
        except Exception as e:
            raise NetworkError(
                "Failed to create enchanted_terrain.json from shimmering_terrain.json: "
                f"{e}"
            ) from e

        # Import nexus_json_tools from the project root so we can call its
        # fuse-shard-3way CLI entrypoint programmatically.
        try:
            client_dir = os.path.dirname(os.path.abspath(__file__))
            wf_mcp_dir = os.path.dirname(client_dir)
            mcp_servers_dir = os.path.dirname(wf_mcp_dir)
            project_root = os.path.dirname(mcp_servers_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            nexus_tools = importlib.import_module("nexus_json_tools")
        except Exception as e:
            raise NetworkError(f"Failed to import nexus_json_tools: {e}") from e

        # Call fuse-shard-3way as if via CLI, but with enchanted_terrain.json as
        # the target SCRY so shimmering_terrain.json remains unchanged.
        try:
            # nexus_json_tools.main() may call sys.exit(), so we catch
            # SystemExit explicitly to interpret non-zero as an error.
            argv = [
                enchanted_path,
                "fuse-shard-3way",
                "--witness-shard",
                phantom_gem_path,
                "--morphed-shard",
                phantom_jewel_path,
                "--target-scry",
                enchanted_path,
            ]
            try:
                nexus_tools.main(argv)
            except SystemExit as se:
                code = se.code or 0
                if code != 0:
                    raise NetworkError(
                        f"nexus_anchor_jewels: fuse-shard-3way exited with code {code}"
                    ) from se
        except Exception as e:
            raise NetworkError(f"nexus_anchor_jewels: fuse-shard-3way failed: {e}") from e

        # Compute and attach enchanted_preview for ENCHANTED TERRAIN (ET-1.2.3).
        try:
            with open(enchanted_path, "r", encoding="utf-8") as f:
                enchanted_data = json.load(f)
            if isinstance(enchanted_data, dict) and isinstance(enchanted_data.get("nodes"), list):
                enchanted_preview = self._annotate_preview_ids_and_build_tree(
                    enchanted_data.get("nodes") or [],
                    prefix="ET",
                )
                # Rebuild dict with enchanted_preview in early position
                enchanted_data = {
                    "export_timestamp": enchanted_data.get("export_timestamp"),
                    "export_root_children_status": enchanted_data.get("export_root_children_status"),
                    "__preview_tree__": enchanted_preview,
                    "export_root_id": enchanted_data.get("export_root_id"),
                    "export_root_name": enchanted_data.get("export_root_name"),
                    "nodes": enchanted_data.get("nodes"),
                    "original_ids_seen": enchanted_data.get("original_ids_seen"),
                    "explicitly_preserved_ids": enchanted_data.get("explicitly_preserved_ids"),
                }
                with open(enchanted_path, "w", encoding="utf-8") as f:
                    json.dump(enchanted_data, f, indent=2, ensure_ascii=False)
        except Exception:
            # Preview is best-effort only; never break ANCHOR JEWELS.
            pass

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "shimmering_terrain": shimmering_path,
            "enchanted_terrain": enchanted_path,
        }

    def _get_explore_sessions_dir(self) -> str:
        """Return base directory for exploration session JSON files and ensure it exists.

        Sessions are stored outside the NEXUS run tree so that multiple nexus_tag
        values can share the same exploration mechanism. Each session file is
        named <session_id>.json and contains the cached tree plus handle/state
        metadata.
        """
        base_dir = (
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_explore_sessions"
        )
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    def nexus_list_exploration_sessions(
        self,
        nexus_tag: str | None = None,
    ) -> dict[str, Any]:
        """List all exploration sessions (optionally filter by nexus_tag).
        
        Returns:
            {
              "success": true,
              "sessions": [
                {
                  "session_id": "...",
                  "nexus_tag": "...",
                  "created_at": "...",
                  "updated_at": "...",
                  "steps": 5,
                  "exploration_mode": "dfs_guided",
                  "editable": false
                },
                ...
              ]
            }
        """
        import json as json_module
        from pathlib import Path
        
        sessions_dir = self._get_explore_sessions_dir()
        sessions_path = Path(sessions_dir)
        
        results = []
        for session_file in sessions_path.glob("*.json"):
            try:
                with open(session_file, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                
                tag = session.get("nexus_tag")
                
                # Filter by tag if requested
                if nexus_tag and tag != nexus_tag:
                    continue
                
                results.append({
                    "session_id": session.get("session_id"),
                    "nexus_tag": tag,
                    "created_at": session.get("created_at"),
                    "updated_at": session.get("updated_at"),
                    "steps": session.get("steps", 0),
                    "exploration_mode": session.get("exploration_mode"),
                    "editable": session.get("editable", False),
                })
            except Exception:
                # Skip corrupted session files
                continue
        
        # Sort by created_at (most recent first)
        results.sort(key=lambda s: s.get("created_at", ""), reverse=True)
        
        return {
            "success": True,
            "sessions": results,
            "total": len(results),
        }

    async def nexus_resume_exploration(
        self,
        session_id: str | None = None,
        nexus_tag: str | None = None,
        frontier_size: int = 25,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Resume an exploration session after MCP restart or in new conversation.
        
        Loads session from disk and returns current frontier (exactly as if you
        had just called nexus_explore_step with no actions).
        
        Args:
            session_id: Session ID to resume (takes precedence if both provided)
            nexus_tag: Alternative - find latest session for this tag
            frontier_size: How many frontier entries to return
            include_history_summary: Include status summary
            
        Returns:
            Same format as nexus_explore_step_v2 (but with empty walks since no
            walk requests were made - just current state view)
        """
        import json as json_module
        from pathlib import Path
        
        sessions_dir = self._get_explore_sessions_dir()
        
        # Resolve to specific session_id
        if session_id:
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            if not os.path.exists(session_path):
                raise NetworkError(f"Session '{session_id}' not found")
        elif nexus_tag:
            # Find latest session for this tag
            # Pattern matches: YYYY-MM-DD_HH-MM-SS__<nexus_tag>-<hex>.json
            tag_sessions = []
            for f in Path(sessions_dir).glob(f"*__{nexus_tag}-*.json"):
                tag_sessions.append(f)
            if not tag_sessions:
                raise NetworkError(f"No sessions found for nexus_tag '{nexus_tag}'")
            # Sort by filename (lexicographic = chronological due to timestamp prefix)
            latest = sorted(tag_sessions)[-1]
            session_path = str(latest)
            session_id = latest.stem  # Extract session_id from filename
        else:
            raise NetworkError("Must provide either session_id or nexus_tag")
        
        # Load session
        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)
        
        # Check if already finalized
        tag = session.get("nexus_tag")
        if tag:
            try:
                run_dir = self._get_nexus_dir(tag)
                phantom_path = os.path.join(run_dir, "phantom_gem.json")
                if os.path.exists(phantom_path):
                    # Session already finalized
                    return {
                        "success": True,
                        "session_id": session_id,
                        "status": "completed",
                        "message": f"Session already finalized - phantom_gem.json exists for tag '{tag}'",
                        "phantom_gem": phantom_path,
                    }
            except NetworkError:
                # No NEXUS dir yet - session still in progress
                pass
        
        # Compute current frontier using existing helper
        frontier = self._compute_exploration_frontier(
            session,
            frontier_size=frontier_size,
            max_depth_per_frontier=1,
        )

        # Build preview for this frontier (handle-padded, depth-indented)
        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist this flat frontier so that the very next nexus_explore_step
        # call uses this as the active frontier for actions (EF/PF/etc.).
        from datetime import datetime as _dt
        session["last_frontier_flat"] = frontier
        session["updated_at"] = _dt.utcnow().isoformat() + "Z"
        with open(session_path, "w", encoding="utf-8") as f:
            json_module.dump(session, f, indent=2, ensure_ascii=False)
        
        # Build history summary if requested
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        
        history_summary = None
        if include_history_summary:
            history_summary = {
                "open": [h for h, st in state.items() if st.get("status") == "open"],
                "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                "steps": session.get("steps", 0),
            }
        
        # Return in v2 format (walks=[] since this is just resume, not a walk request)
        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": session.get("nexus_tag"),
            "status": "in_progress",
            "action_key_primary_aliases": {
                "RB": "reserve_branch_for_children",
            },
            "action_key": EXPLORATION_ACTION_2LETTER,
            "scratchpad": session.get("scratchpad", ""),
            "frontier_preview": frontier_preview,
            "frontier_tree": self._build_frontier_tree_from_flat(frontier),
            "walks": [],  # Empty - no walks requested, just showing current state
            "skipped_walks": [],
            "decisions_applied": [],
            "history_summary": history_summary,
            "session_meta": {
                "created_at": session.get("created_at"),
                "updated_at": session.get("updated_at"),
                "exploration_mode": session.get("exploration_mode"),
                "editable": session.get("editable"),
                "steps": session.get("steps", 0),
            }
        }

    def _apply_search_filter_to_frontier(
        self,
        frontier: list[dict[str, Any]],
        search_filter: dict[str, Any],
        handles: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Apply persistent search filter to a frontier (SECONDARY implementation).

        This is used by dfs_guided_explicit mode when search_filter was provided
        at START. It filters the normal DFS frontier to show only nodes that
        match the search criteria, preserving DFS order and leaf budget semantics.

        Args:
            frontier: Normal DFS frontier (leaf chunks)
            search_filter: Search criteria from session (search_text, case_sensitive, etc.)
            handles: Session handles dict (needed for name/note lookup)

        Returns:
            Filtered frontier (same structure, subset of entries)
        """
        import re

        search_text = search_filter.get("search_text", "")
        case_sensitive = search_filter.get("case_sensitive", False)
        whole_word = search_filter.get("whole_word", False)
        use_regex = search_filter.get("regex", False)

        if not search_text:
            return frontier  # No filter text, return original

        filtered_frontier = []

        for entry in frontier:
            h = entry.get("handle")
            meta = handles.get(h, {}) or {}
            name = meta.get("name", "")
            note = meta.get("note", "")

            # Combine name + note for searching
            searchable_text = f"{name}\n{note}"

            # Match logic (same as PRIMARY implementation)
            matches = False
            if use_regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(search_text, searchable_text, flags):
                        matches = True
                except re.error:
                    # Invalid regex - skip this handle
                    pass
            elif whole_word:
                pattern = r"\b" + re.escape(search_text) + r"\b"
                flags = 0 if case_sensitive else re.IGNORECASE
                if re.search(pattern, searchable_text, flags):
                    matches = True
            else:
                # Simple substring matching
                if case_sensitive:
                    if search_text in searchable_text:
                        matches = True
                else:
                    if search_text.lower() in searchable_text.lower():
                        matches = True

            if matches:
                # Update guidance to indicate this is a SEARCH match
                filtered_entry = dict(entry)
                if entry.get("is_leaf"):
                    filtered_entry["guidance"] = "SEARCH MATCH - leaf: EL=engulf, PL=preserve"
                else:
                    filtered_entry["guidance"] = "SEARCH MATCH - branch: RB=reserve, PB=preserve"
                filtered_frontier.append(filtered_entry)

        return filtered_frontier

    def _compute_search_frontier(
        self,
        session: dict[str, Any],
        search_actions: list[dict[str, Any]],
        scope: str = "undecided",
        max_results: int = 80,
    ) -> list[dict[str, Any]]:
        """Compute frontier from SEARCH results (text matching in names/notes).

        This frontier builder is used when one or more search_descendants_for_text
        actions are present in nexus_explore_step. It:
        - Searches all handles under the specified branch (or "R" for entire tree)
        - Filters by scope ("undecided" or "all")
        - Applies AND logic across multiple searches (intersection)
        - Returns matching nodes + all their ancestors as frontier entries
        - Respects max_results limit (prioritizes matches over ancestors)

        Args:
            session: Exploration session dict
            search_actions: List of search_descendants_for_text actions
            scope: "undecided" (default) or "all"
            max_results: Maximum frontier entries (default 80, same as global_frontier_limit)

        Returns:
            Flat frontier list (convert to tree via _build_frontier_tree_from_flat)
        """
        import re

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        def _collect_hints_from_ancestors(handle: str) -> list[str]:
            """Collect hints from all ancestors of a given handle (closest first)."""
            hints: list[str] = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_meta = handles.get(parent, {}) or {}
                parent_hints = parent_meta.get("hints") or []
                hints.extend(parent_hints)
                parent = parent_meta.get("parent")
            return hints

        # Start with all handles (will be filtered by search)
        candidate_handles = set(handles.keys())
        candidate_handles.discard("R")  # Never include synthetic root in search results

        # Apply each search in sequence (AND logic)
        for search_action in search_actions:
            handle_filter = search_action.get("handle", "R")
            search_text = search_action.get("search_text", "")
            case_sensitive = search_action.get("case_sensitive", False)
            whole_word = search_action.get("whole_word", False)
            use_regex = search_action.get("regex", False)

            if not search_text:
                continue  # Skip empty searches

            # Filter to descendants under the specified handle
            if handle_filter != "R":
                # Only consider handles that descend from handle_filter
                filtered_candidates = set()
                for h in candidate_handles:
                    if h == handle_filter or h.startswith(handle_filter + "."):
                        filtered_candidates.add(h)
                candidate_handles = filtered_candidates

            # Build matching set for this search
            matching_handles = set()

            for h in candidate_handles:
                meta = handles.get(h, {}) or {}
                name = meta.get("name", "")
                note = meta.get("note", "")

                # Combine name + note for searching
                searchable_text = f"{name}\n{note}"

                # Match logic
                if use_regex:
                    try:
                        flags = 0 if case_sensitive else re.IGNORECASE
                        if re.search(search_text, searchable_text, flags):
                            matching_handles.add(h)
                    except re.error:
                        # Invalid regex - skip this handle
                        pass
                elif whole_word:
                    # Whole word matching
                    pattern = r"\b" + re.escape(search_text) + r"\b"
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(pattern, searchable_text, flags):
                        matching_handles.add(h)
                else:
                    # Simple substring matching
                    if case_sensitive:
                        if search_text in searchable_text:
                            matching_handles.add(h)
                    else:
                        if search_text.lower() in searchable_text.lower():
                            matching_handles.add(h)

            # Intersection with previous search results (AND logic)
            candidate_handles &= matching_handles

        # Apply scope filter
        if scope == "undecided":
            undecided_statuses = {"unseen", "candidate", "open"}
            # Precompute undecided handles for quick membership checks
            undecided_handles = {
                h for h, st in state.items()
                if st.get("status") in undecided_statuses
            }

            def is_structural_leaf(handle: str) -> bool:
                return not (handles.get(handle, {}) or {}).get("children")

            def has_undecided_matching_descendant(branch: str) -> bool:
                # Walk descendants structurally; a descendant counts if it is
                # both in candidate_handles (matches search) and undecided.
                child_handles = (handles.get(branch, {}) or {}).get("children") or []
                stack = list(child_handles)
                seen: set[str] = set()
                while stack:
                    ch = stack.pop()
                    if ch in seen:
                        continue
                    seen.add(ch)
                    if ch in candidate_handles and ch in undecided_handles:
                        return True
                    child_children = (handles.get(ch, {}) or {}).get("children") or []
                    stack.extend(child_children)
                return False

            filtered = set()
            for h in candidate_handles:
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")
                if is_structural_leaf(h):
                    if status in undecided_statuses:
                        filtered.add(h)
                else:
                    # Branch: keep only if it has at least one undecided matching descendant.
                    if has_undecided_matching_descendant(h):
                        filtered.add(h)
            candidate_handles = filtered

        # Apply max_results limit to MATCHES, then include ALL required ancestors
        # Strategy: Limit controls how many search hits are shown, but each hit
        # gets its complete ancestor path to root (navigational requirement)
        matches_sorted = sorted(candidate_handles)  # Actual search matches
        
        # Limit the number of matches
        limited_matches = matches_sorted[:max_results]
        
        # Build frontier: limited matches + ALL their ancestors up to root
        final_frontier_handles = set(limited_matches)
        for h in limited_matches:
            ancestor_h = handles.get(h, {}).get("parent")
            while ancestor_h and ancestor_h != "R":
                final_frontier_handles.add(ancestor_h)
                ancestor_h = handles.get(ancestor_h, {}).get("parent")
        
        # Sort for stable output (matches first in handle order, then ancestors)
        matches_in_frontier = sorted([h for h in final_frontier_handles if h in limited_matches])
        ancestors_in_frontier = sorted([h for h in final_frontier_handles if h not in limited_matches])
        final_frontier_list = matches_in_frontier + ancestors_in_frontier

        # Build frontier entries
        frontier: list[dict[str, Any]] = []
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        for h in final_frontier_list:  # Respects max_results for matches; includes all required ancestors
            meta = handles.get(h, {}) or {}
            st = state.get(h, {"status": "unseen"})
            child_handles = meta.get("children", []) or []
            is_leaf = not child_handles

            # Note preview
            note_full = meta.get("note") or ""
            if MAX_NOTE_PREVIEW is None:
                note_preview = note_full
            else:
                note_preview = (
                    note_full
                    if len(note_full) <= MAX_NOTE_PREVIEW
                    else note_full[:MAX_NOTE_PREVIEW]
                )

            local_hints = meta.get("hints") or []
            hints_from_ancestors = _collect_hints_from_ancestors(h)

            # SEARCH-specific guidance
            if h in limited_matches:
                # This node matched the search
                if is_leaf:
                    guidance = "MATCH - leaf: EL=engulf, PL=preserve"
                else:
                    guidance = "MATCH - branch: RB=reserve, PB=preserve, EF=engulf_showing, PF=preserve_showing"
            else:
                # This is an ancestor of a match (navigational context)
                guidance = "ancestor (navigational context)"

            entry = {
                "handle": h,
                "parent_handle": meta.get("parent"),
                "name_preview": meta.get("name", ""),
                "note_preview": note_preview,
                "child_count": len(child_handles),
                "depth": meta.get("depth", 0),
                "status": st.get("status", "candidate"),
                "is_leaf": is_leaf,
                "guidance": guidance,
            }
            if local_hints:
                entry["hints"] = local_hints
            if hints_from_ancestors:
                entry["hints_from_ancestors"] = hints_from_ancestors

            frontier.append(entry)

        return frontier

    def _compute_exploration_frontier(
        self,
        session: dict[str, Any],
        frontier_size: int,
        max_depth_per_frontier: int,
    ) -> list[dict[str, Any]]:
        """Compute the next frontier for an exploration session.

        STRICT DFS MODES (dfs_guided / dfs_full_walk)
        ---------------------------------------------
        Reinterprets "multiple frontiers" as **leaf chunks**:

        - Precompute a DFS order over all handles (R, A, A.1, A.1.1, ...).
        - At each step in strict modes, find the *first* sibling leaf group in
          DFS order, then accumulate additional sibling leaf groups until a
          leaf-count budget is reached.
        - The frontier for this step is exactly those undecided leaves. The
          agent is expected to make explicit accept/reject decisions on each
          leaf in the frontier in the next call.
        - Once all leaves under a branch are decided, ancestor behavior is
          handled by decision logic (not this function):
            * ancestors with accepted leaves are auto-included structurally
            * ancestors with only rejected leaves become candidates for
              branch-level accept_subtree/reject_subtree decisions

        NON-STRICT / LEGACY MODES
        -------------------------
        For exploration_mode values other than {dfs_guided, dfs_full_walk}, we
        retain the previous round-robin "open parent" behavior: any handle with
        status=open contributes its children as frontier entries, in strips,
        until frontier_size entries are collected.

        Note: max_depth_per_frontier is ignored in strict DFS modes; the leaf
        chunking algorithm walks depth-first using the cached tree and the
        exploration state, not a depth cap.
        """
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        search_filter = session.get("search_filter")  # SECONDARY: persistent filter (explicit mode only)

        def _collect_hints_from_ancestors(handle: str) -> list[str]:
            """Collect hints from all ancestors of a given handle (closest first)."""
            hints: list[str] = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_meta = handles.get(parent, {}) or {}
                parent_hints = parent_meta.get("hints") or []
                hints.extend(parent_hints)
                parent = parent_meta.get("parent")
            return hints

        frontier: list[dict[str, Any]] = []

        exploration_mode = session.get("exploration_mode", "manual")
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        # ========= STRICT DFS / DFS-GUIDED MODES: LEAF-CHUNK FRONTIER =========
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:

            # For BOTH strict and guided modes, also surface undecided branch
            # ancestors leading to frontier leaves so agent/human can see
            # structure and optionally choose branch-level actions (ES/ST/EA/SR).
            include_branch_ancestors = True

            def _is_covered_by_subtree(h: str) -> bool:
                """True if some ancestor has a subtree selection covering this handle.

                A handle is considered covered if any ancestor handle has:
                  - status in {finalized, closed} and
                  - selection_type == "subtree".

                In that case, we treat this handle as already accounted for in
                the minimal covering tree and do not surface it in the DFS
                frontier.
                """
                parent_handle = handles.get(h, {}).get("parent")
                while parent_handle:
                    anc_state = state.get(parent_handle, {})
                    if (
                        anc_state.get("status") in {"finalized", "closed"}
                        and anc_state.get("selection_type") == "subtree"
                    ):
                        return True
                    parent_handle = handles.get(parent_handle, {}).get("parent")
                return False

            def _is_decided(h: str) -> bool:
                """True if this handle should be excluded from frontier.
                
                In dfs_guided (non-strict) mode:
                - Only explicitly decided nodes (finalized/closed) are excluded
                - Descendants under shells/subtrees STILL APPEAR in frontier
                - This enables frontier-based bulk actions and explicit control
                
                In dfs_full_walk (strict) mode:
                - Covered-by-subtree nodes are also excluded (strict discipline)
                """
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")
                if status in {"finalized", "closed"}:
                    return True
                
                # NON-STRICT MODE: Do NOT hide descendants under subtree ancestors
                # (enables frontier bulk actions and explicit individual decisions)
                if exploration_mode == "dfs_guided_bulk":
                    return False
                
                # STRICT MODE: Respect subtree coverage (original discipline)
                if _is_covered_by_subtree(h):
                    return True
                
                return False

            # Collect undecided leaves in BFS order over the *active* tree.
            # Decided nodes/subtrees are pruned by _is_decided(), so this
            # effectively walks only the remaining search space. We still
            # chunk leaves by sibling groups below to avoid splitting siblings
            # across frontiers.
            from collections import deque

            if frontier_size <= 0:
                return frontier

            # Leaf budget per step: show ~frontier_size leaves, allowing a small
            # leeway so that sibling groups are not arbitrarily split.
            leaf_target = max(1, frontier_size)
            leaf_leeway = max(1, min(leaf_target, 10))  # simple guardrail

            bfs_leaves: list[str] = []
            queue: deque[str] = deque(["R"])
            visited: set[str] = set()

            while queue and len(bfs_leaves) < leaf_target + leaf_leeway:
                h = queue.popleft()
                if h in visited:
                    continue
                visited.add(h)

                # Never treat synthetic root as a frontier entry; just expand its children
                if h == "R":
                    for ch in handles.get("R", {}).get("children", []) or []:
                        if ch not in visited:
                            queue.append(ch)
                    continue

                # Structural children regardless of decision state
                child_handles = handles.get(h, {}).get("children", []) or []
                is_structural_leaf = not child_handles

                st = state.get(h, {"status": "unseen"})
                status = st.get("status")
                is_undecided = status in {"unseen", "candidate", "open"}

                # Only undecided structural leaves become leaf candidates;
                # decided leaves are still traversed for completeness, but do not
                # appear in the leaf frontier.
                if is_structural_leaf and is_undecided:
                    bfs_leaves.append(h)

                # Always traverse children so descendants under decided branches
                # are still considered for future frontiers.
                for ch in child_handles:
                    if ch not in visited:
                        queue.append(ch)

            undecided_leaves: list[str] = bfs_leaves

            if not undecided_leaves:
                # Nothing left to decide in the active tree
                return frontier

            # Now apply the existing sibling‑chunk logic over the BFS‑ordered
            # leaves so that siblings under the same parent appear together.
            selected: list[str] = []
            selected_set: set[str] = set()
            total_selected = 0
            idx = 0

            while idx < len(undecided_leaves) and total_selected < leaf_target + leaf_leeway:
                leaf = undecided_leaves[idx]
                idx += 1
                if leaf in selected_set:
                    continue

                parent_handle = handles.get(leaf, {}).get("parent")
                sibling_group: list[str] = []

                if parent_handle:
                    # All undecided leaf siblings under the same parent
                    for ch in handles.get(parent_handle, {}).get("children", []) or []:
                        if ch in selected_set:
                            continue
                        if _is_decided(ch):
                            continue
                        ch_children = handles.get(ch, {}).get("children", []) or []
                        if not ch_children:
                            sibling_group.append(ch)
                else:
                    # Root‑level leaf (no parent in handle graph)
                    sibling_group.append(leaf)

                if not sibling_group:
                    continue

                # If we've already satisfied the target and this sibling group
                # would push us well beyond target+leeway, stop accumulating.
                if (
                    total_selected >= leaf_target
                    and total_selected + len(sibling_group) > leaf_target + leaf_leeway
                ):
                    break

                for ch in sibling_group:
                    if ch in selected_set:
                        continue
                    selected_set.add(ch)
                    selected.append(ch)
                    total_selected += 1

            if not selected:
                # No undecided leaves suitable for this step (all covered or
                # already decided by subtree selections). Branch-level frontiers
                # are handled by decision logic rather than this function.
                return frontier

            # NON-STRICT MODE: Collect ALL branch ancestors for frontier (enables proper nesting)
            branch_ancestors_for_frontier: list[str] = []
            if include_branch_ancestors:
                # For each selected leaf, walk ancestors and collect ALL of them
                # (both decided and undecided) so frontier_tree can nest properly
                branch_set: set[str] = set()
                for leaf_h in selected:
                    ancestor_h = handles.get(leaf_h, {}).get("parent")
                    while ancestor_h and ancestor_h != "R":
                        # Include ALL ancestors (decided or undecided)
                        # This enables _build_frontier_tree_from_flat to properly
                        # nest children under their parents in frontier_tree
                        if ancestor_h not in branch_set:
                            branch_set.add(ancestor_h)
                        # Walk up to next ancestor
                        ancestor_h = handles.get(ancestor_h, {}).get("parent")
                
                # Convert to sorted list (DFS order = alphabetic + numeric handle order)
                branch_ancestors_for_frontier = sorted(list(branch_set))

            MAX_CHILDREN_HINT = 0  # strict leaf frontier: children_hint not needed
            # In editable sessions, do not truncate note_preview; otherwise cap length.
            MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

            # BRANCH ENTRIES FIRST (non-strict mode only)
            if include_branch_ancestors and branch_ancestors_for_frontier:
                for h in branch_ancestors_for_frontier:
                    meta = handles.get(h, {}) or {}
                    st = state.get(h, {"status": "unseen"})
                    child_handles = meta.get("children", []) or []

                    children_hint: list[str] = []
                    local_hints = meta.get("hints") or []
                    hints_from_ancestors = _collect_hints_from_ancestors(h)

                    # Note preview (token-bounded)
                    note_full = meta.get("note") or ""
                    if MAX_NOTE_PREVIEW is None:
                        note_preview = note_full
                    else:
                        note_preview = (
                            note_full
                            if len(note_full) <= MAX_NOTE_PREVIEW
                            else note_full[:MAX_NOTE_PREVIEW]
                        )

                    # Mode-aware branch guidance
                    # Check if branch has ANY descendants showing in current leaf frontier
                    selected_set = set(selected)  # Current leaf frontier handles
                    has_showing_descendants = any(
                        leaf_h.startswith(h + ".") for leaf_h in selected_set
                    )
                    
                    if exploration_mode == "dfs_guided_bulk" and has_showing_descendants:
                        guidance = "branch: RB=reserve, PB=preserve, UB|UN=update, AB=auto, EF=engulf_showing, PF=preserve_showing"
                    else:
                        guidance = "branch: RB=reserve, PB=preserve, UB|UN=update, AB=auto"

                    entry = {
                        "handle": h,
                        "parent_handle": meta.get("parent"),
                        "name_preview": meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(child_handles),
                        "depth": meta.get("depth", 0),
                        "status": st.get("status", "candidate"),
                        "is_leaf": False,  # These are branches by definition
                        "guidance": guidance,
                        "_frontier_role": "branch_ancestor",  # Mark for agent clarity
                    }
                    if children_hint:
                        entry["children_hint"] = children_hint
                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)

            # LEAF ENTRIES
            for h in selected:
                meta = handles.get(h, {}) or {}
                st = state.get(h, {"status": "unseen"})
                child_handles = meta.get("children", []) or []
                is_leaf = not child_handles

                children_hint: list[str] = []
                local_hints = meta.get("hints") or []
                hints_from_ancestors = _collect_hints_from_ancestors(h)

                # Note preview (token-bounded)
                note_full = meta.get("note") or ""
                if MAX_NOTE_PREVIEW is None:
                    note_preview = note_full
                else:
                    note_preview = (
                        note_full
                        if len(note_full) <= MAX_NOTE_PREVIEW
                        else note_full[:MAX_NOTE_PREVIEW]
                    )

                # Leaf-specific guidance (compact)
                guidance = "leaf: EL=engulf, PL=preserve"

                entry = {
                    "handle": h,
                    "parent_handle": meta.get("parent"),
                    "name_preview": meta.get("name", ""),
                    "note_preview": note_preview,
                    "child_count": len(child_handles),
                    "depth": meta.get("depth", 0),
                    "status": st.get("status", "candidate"),
                    "is_leaf": is_leaf,
                    "guidance": guidance,
                }
                if children_hint:
                    entry["children_hint"] = children_hint
                if local_hints:
                    entry["hints"] = local_hints
                if hints_from_ancestors:
                    entry["hints_from_ancestors"] = hints_from_ancestors
                
                frontier.append(entry)

            # SORT frontier by handle using natural/numeric ordering
            # Natural sort: B.2 < B.10 (numeric), not B.10 < B.2 (string)
            # This ensures:
            # - A, A.1, A.2, ..., A.10, A.11 (not A.1, A.10, A.11, A.2)
            # - B, B.2, B.3, ..., B.10, B.11 (not B, B.10, B.11, B.2)
            # - B.2, B.2.1, B.2.2, ..., B.2.10 (children grouped under parent)
            def natural_sort_key(entry):
                handle = entry.get("handle", "")
                parts = handle.split(".")
                # Convert numeric parts to int for natural ordering
                result = []
                for part in parts:
                    if part.isdigit():
                        result.append((0, int(part)))  # Numeric sort
                    else:
                        result.append((1, part))  # Alphabetic sort
                return result
            
            frontier.sort(key=natural_sort_key)

            # SECONDARY SEARCH: Apply persistent search filter (explicit mode only)
            if search_filter is not None and exploration_mode == "dfs_guided_explicit":
                frontier = self._apply_search_filter_to_frontier(
                    frontier=frontier,
                    search_filter=search_filter,
                    handles=handles,
                )

            return frontier

        # ========= NON-STRICT / LEGACY MODES: ROUND-ROBIN OPEN PARENTS =========

        # Candidate parents: any handle that is currently open.
        # Candidate handles themselves are frontier entries, not parents, until
        # the agent explicitly opens them.
        candidate_parents = [
            h for h, st in state.items() if st.get("status") == "open"
        ]

        # Limit on how many immediate child names we surface per entry to keep
        # the frontier compact while still providing strong guidance.
        MAX_CHILDREN_HINT = 10
        # In editable sessions, do not truncate note_preview; otherwise cap length.
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        if not candidate_parents or frontier_size <= 0:
            return frontier

        # Round-robin across open parents: walk each parent's children in strips
        # (A child, B child, C child, then A's next, etc.) until we either fill
        # the frontier or run out of candidates.
        parent_indices: dict[str, int] = {h: 0 for h in candidate_parents}

        while len(frontier) < frontier_size:
            made_progress = False

            for parent_handle in candidate_parents:
                meta = handles.get(parent_handle) or {}
                child_handles = meta.get("children", []) or []

                idx = parent_indices.get(parent_handle, 0)
                # Advance this parent's index until we either exhaust its
                # children or find a child that is not closed/finalized.
                while idx < len(child_handles) and len(frontier) < frontier_size:
                    child_handle = child_handles[idx]
                    parent_indices[parent_handle] = idx + 1
                    idx += 1

                    child_state = state.get(child_handle, {"status": "unseen"})
                    if child_state.get("status") in {"closed", "finalized"}:
                        continue

                    child_meta = handles.get(child_handle) or {}
                    grandchild_handles = child_meta.get("children", []) or []

                    # Build children_hint: preview of this node's immediate children
                    # by name, in original order, capped at MAX_CHILDREN_HINT.
                    children_hint: list[str] = []
                    if grandchild_handles:
                        for ch in grandchild_handles[:MAX_CHILDREN_HINT]:
                            ch_meta = handles.get(ch) or {}
                            ch_name = ch_meta.get("name")
                            if ch_name:
                                children_hint.append(ch_name)

                    is_leaf = len(grandchild_handles) == 0
                    if is_leaf:
                        guidance = "leaf: EL=engulf, PL=preserve"
                    else:
                        guidance = "branch: OP=open, RB=reserve"

                    # Note preview (token-bounded)
                    note_full = child_meta.get("note") or ""
                    if MAX_NOTE_PREVIEW is None:
                        note_preview = note_full
                    else:
                        note_preview = (
                            note_full
                            if len(note_full) <= MAX_NOTE_PREVIEW
                            else note_full[:MAX_NOTE_PREVIEW]
                        )

                    local_hints = child_meta.get("hints") or []
                    hints_from_ancestors = _collect_hints_from_ancestors(child_handle)

                    entry = {
                        "handle": child_handle,
                        "parent_handle": parent_handle,
                        "name_preview": child_meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(grandchild_handles),
                        "depth": child_meta.get("depth", 0),
                        "status": child_state.get("status", "candidate"),
                        "is_leaf": is_leaf,
                        "guidance": guidance,
                    }
                    # Only include non-empty lists
                    if children_hint:
                        entry["children_hint"] = children_hint
                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)

                    made_progress = True
                    break  # Move to next parent in round-robin

                if len(frontier) >= frontier_size:
                    break

            if not made_progress:
                # No parent was able to contribute a new frontier entry.
                break

        return frontier

    def _build_frontier_tree_from_flat(
        self,
        frontier: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build a nested frontier tree from the flat frontier list.

        This is a presentation helper only; it does not affect exploration
        semantics. Each node in the returned tree is a shallow copy of the
        flat entry with an added ``children`` list.
        """
        if not frontier:
            return []

        # Shallow-copy entries and initialize children lists
        by_handle: dict[str, dict[str, Any]] = {}
        for entry in frontier:
            node = dict(entry)
            node["children"] = []
            by_handle[node["handle"]] = node

        roots: list[dict[str, Any]] = []
        for handle, node in by_handle.items():
            parent = node.get("parent_handle")
            if parent and parent in by_handle:
                by_handle[parent]["children"].append(node)
            else:
                roots.append(node)

        # Stable ordering by handle for readability
        roots.sort(key=lambda n: n["handle"])
        for node in by_handle.values():
            node["children"].sort(key=lambda n: n["handle"])
        
        # Remove empty children arrays (token optimization)
        for node in by_handle.values():
            if not node["children"]:
                del node["children"]

        return roots

    async def nexus_start_exploration(
        self,
        nexus_tag: str,
        root_id: str,
        source_mode: str = "glimpse_full",
        max_nodes: int = 200000,
        session_hint: str | None = None,
        frontier_size: int = 25,
        max_depth_per_frontier: int = 1,
        editable: bool = False,
        search_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Initialize an exploration session over a Workflowy subtree.

        v1 implementation uses workflowy_scry regardless of source_mode
        (summon/glimpse_full/existing are treated identically for now). The
        GLIMPSE result is cached in a JSON session file along with handle and
        state metadata, and an initial frontier is returned.
        
        Args:
            nexus_tag: Human-readable tag for this exploration
            root_id: Workflowy node UUID to explore
            source_mode: Reserved (currently all use glimpse_full)
            max_nodes: Safety cap on tree size
            session_hint: Controls exploration_mode ('bulk'/'guided_bulk' → dfs_guided_bulk)
            frontier_size: Leaf budget for guided modes
            max_depth_per_frontier: Reserved for future
            editable: Enable update_node/tag actions if True
            search_filter: SECONDARY search (dfs_guided_explicit only) - Persistent filter
                applied to every frontier. Dict with keys: search_text (required),
                case_sensitive (default False), whole_word (default False),
                regex (default False). Filters normal DFS frontier to show only matches.
        """
        import logging
        import uuid
        from datetime import datetime
        import os
        import glob
        from pathlib import Path

        logger = _ClientLogger()

        # VALIDATION: Disallow reuse of existing nexus_tag to prevent stale TERRAIN contamination
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        sessions_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_explore_sessions"
        )
        
        # Check nexus_runs for any directory ending with __<nexus_tag>
        if base_dir.exists():
            pattern = str(base_dir / f"*__{nexus_tag}")
            existing_runs = glob.glob(pattern)
            if existing_runs:
                raise NetworkError(
                    f"nexus_tag '{nexus_tag}' already exists.\n\n"
                    f"Existing NEXUS run found in:\n  {existing_runs[0]}\n\n"
                    "This prevents stale TERRAIN reuse. Choose a unique tag or:\n"
                    f"  1. Delete existing run: remove temp/nexus_runs/*__{nexus_tag}\n"
                    f"  2. Resume existing session: nexus_resume_exploration(nexus_tag='{nexus_tag}')\n"
                    f"  3. Use different tag: nexus_start_exploration(nexus_tag='{nexus_tag}-v2', ...)"
                )
        
        # Check nexus_explore_sessions for any file matching pattern *__<nexus_tag>-*.json
        if sessions_dir.exists():
            pattern = str(sessions_dir / f"*__{nexus_tag}-*.json")
            existing_sessions = glob.glob(pattern)
            if existing_sessions:
                raise NetworkError(
                    f"nexus_tag '{nexus_tag}' already has exploration sessions.\n\n"
                    f"Existing session found:\n  {existing_sessions[0]}\n\n"
                    "This prevents tag collision. Choose a unique tag or:\n"
                    f"  1. Resume existing session: nexus_resume_exploration(nexus_tag='{nexus_tag}')\n"
                    f"  2. Use different tag: nexus_start_exploration(nexus_tag='{nexus_tag}-v2', ...)"
                )

        # Determine exploration mode from session_hint.
        # DEFAULT: dfs_guided_explicit (explicit leaf coverage, no bulk actions).
        # Optional override: request dfs_guided_bulk for bulk descendant actions.
        exploration_mode = "dfs_guided_explicit"
        strict_completeness = False  # NEW: Disables PA action in bulk mode
        
        if session_hint:
            hint = session_hint.strip().lower()
            if "bulk" in hint or "guided_bulk" in hint or "non_strict" in hint:
                exploration_mode = "dfs_guided_bulk"
            if "strict_completeness" in hint or "strict" in hint:
                strict_completeness = True
        
        # SECONDARY SEARCH IMPLEMENTATION: Validate search_filter for explicit mode only
        if search_filter is not None:
            if exploration_mode != "dfs_guided_explicit":
                raise NetworkError(
                    "search_filter parameter is only available in dfs_guided_explicit mode.\n\n"
                    "The SECONDARY search implementation (persistent filter at START) is designed "
                    "for strict DFS exploration with filtered frontiers.\n\n"
                    "For bulk mode, use the PRIMARY search implementation: "
                    "search_descendants_for_text action in nexus_explore_step."
                )
            # Validate search_filter structure
            if not isinstance(search_filter, dict) or not search_filter.get("search_text"):
                raise NetworkError(
                    "search_filter must be a dict with at least 'search_text' key.\n\n"
                    "Example: search_filter={'search_text': 'spare', 'case_sensitive': False}"
                )

        # Fetch subtree once via GLIMPSE FULL (Agent hunts mode)
        glimpse = await self.workflowy_scry(
            node_id=root_id,
            use_efficient_traversal=False,
            depth=None,
            size_limit=max_nodes,
        )

        if not glimpse.get("success"):
            raise NetworkError(
                f"nexus_start_exploration: glimpseFull failed for root {root_id}: "
                f"{glimpse.get('error', 'unknown error')}"
            )

        root_meta = glimpse.get("root") or {
            "id": root_id,
            "name": "Root",
            "note": None,
            "parent_id": None,
        }
        root_children = glimpse.get("children", []) or []

        root_node = {
            "id": root_meta.get("id", root_id),
            "name": root_meta.get("name", "Root"),
            "note": root_meta.get("note"),
            "parent_id": root_meta.get("parent_id"),
            "children": root_children,
        }

        # In editable sessions, stash original_name/original_note so FINALIZE can
        # reconstruct both coarse TERRAIN (original view) and JEWEL (edited view)
        # from a single minimal covering tree.
        def _stash_original_fields(node: dict) -> None:
            if "original_name" not in node:
                node["original_name"] = node.get("name")
            if "original_note" not in node:
                node["original_note"] = node.get("note")
            for child in node.get("children") or []:
                _stash_original_fields(child)

        if editable:
            _stash_original_fields(root_node)

        # LOGGING: observe chosen root vs requested root and child count
        self._log_debug(f"nexus_start_exploration: root_id={root_id} root_node_id={root_node['id']} root_node_name={root_node.get('name')} children={len(root_children)}")

        # Assign handles R, A/B/C..., A.1, A.2, etc.
        handles: dict[str, dict[str, Any]] = {}

        def alpha_handle(index: int) -> str:
            """Convert 0-based index to Excel-like column name (A, B, ... AA, AB...)."""
            letters = ""
            n = index
            while True:
                n, rem = divmod(n, 26)
                letters = chr(ord("A") + rem) + letters
                if n == 0:
                    break
                n -= 1
            return letters

        def walk(node: dict[str, Any], handle: str, parent_handle: str | None, depth: int, top_level: bool = False) -> None:
            children = node.get("children", []) or []
            child_handles: list[str] = []

            if top_level:
                # First level under R uses alphabetic handles A, B, C, ...
                for idx, child in enumerate(children):
                    ch = alpha_handle(idx)
                    child_handles.append(ch)
                    walk(child, ch, "R", depth + 1, top_level=False)
            else:
                for idx, child in enumerate(children):
                    ch = f"{handle}.{idx + 1}"
                    child_handles.append(ch)
                    walk(child, ch, handle, depth + 1, top_level=False)

            handles[handle] = {
                "id": node.get("id"),
                "name": node.get("name", "Untitled"),
                "note": node.get("note"),
                "parent": parent_handle,
                "children": child_handles,
                "depth": depth,
                "hints": [],
            }

        # Root handle R
        walk(root_node, "R", None, 0, top_level=True)

        # Initial state: R open, direct children candidate
        state: dict[str, dict[str, Any]] = {
            "R": {"status": "open", "max_depth": None}
        }
        for child_handle in handles.get("R", {}).get("children", []) or []:
            state[child_handle] = {"status": "candidate", "max_depth": None}

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        session_id = f"{timestamp}__{nexus_tag}-{uuid.uuid4().hex[:8]}"
        session = {
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "root_id": root_node["id"],
            "root_name": root_node.get("name"),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "source_mode": source_mode,
            "exploration_mode": exploration_mode,
            "max_nodes": max_nodes,
            "editable": bool(editable),
            "strict_completeness": strict_completeness,  # NEW: Disables PA in bulk mode
            "handles": handles,
            "state": state,
            "scratchpad": "",
            "root_node": root_node,
            "steps": 0,
            "search_filter": search_filter,  # SECONDARY: persistent filter for explicit mode
            "glimpse_stats": {
                "node_count": glimpse.get("node_count", 0),
                "depth": glimpse.get("depth", 0),
                "_source": glimpse.get("_source", "api"),
            },
        }

        # Compute initial frontier
        frontier = self._compute_exploration_frontier(
            session,
            frontier_size=frontier_size,
            max_depth_per_frontier=max_depth_per_frontier,
        )

        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist session to disk
        try:
            sessions_dir = self._get_explore_sessions_dir()
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            with open(session_path, "w", encoding="utf-8") as f:
                json.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to persist exploration session {session_id}: {e}")
            raise NetworkError(f"Failed to persist exploration session: {e}") from e

        root_summary = {
            "name": root_node.get("name", "Root"),
            "child_count": len(handles.get("R", {}).get("children", []) or []),
        }

        frontier_tree = self._build_frontier_tree_from_flat(frontier)

        # Build mode-aware guidance for initial frontier
        if exploration_mode == "dfs_guided_explicit":
            step_guidance = [
                "🎯 EXPLICIT MODE: Auto-frontier. No navigation needed.",
                "",
                "💎 DECISION OUTCOMES:",
                "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                "",
                "Leaf actions: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                "Branch actions (when all descendants decided): flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)",
                "No bulk actions available in explicit mode. PA (preserve_all) never available in explicit mode.",
                "",
                "🔍 SEARCH: SECONDARY implementation (set at START via search_filter param) - Filters normal DFS frontier to show only matches. PRIMARY implementation (search_descendants_for_text action in STEP) not available in explicit mode."
            ]
        elif exploration_mode == "dfs_guided_bulk":
            # Check strict_completeness for initial guidance
            if strict_completeness:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier with bulk actions.",
                    "",
                    "🛡️ STRICT COMPLETENESS ACTIVE - PA action DISABLED",
                    "",
                    "⚠️ COMMON AGENT FAILURE MODE: Agents routinely opt out early with PA after a few frontiers.",
                    "DON'T BE ONE OF THOSE AGENTS. This mode forces thorough exploration.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                    "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                    "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                    "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA) - DISABLED in strict_completeness mode",
                    "",
                    "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                ]
            else:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier with bulk actions.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                    "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                    "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                    "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA)",
                    "",
                    "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                ]
        else:
            step_guidance = [
                "🎯 LEGACY MODE: Manual navigation.",
                "",
                "💎 DECISION OUTCOMES:",
                "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                "",
                "Navigate: open (OP), close (CL)",
                "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL)",
                "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)"
            ]

        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "exploration_mode": exploration_mode,
            "action_key_primary_aliases": {
                "RB": "reserve_branch_for_children",
            },
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "root_handle": "R",
            "frontier_preview": frontier_preview,
            "root_summary": root_summary,
            "frontier_tree": frontier_tree,
            "scratchpad": session.get("scratchpad", ""),
            "stats": {
                "total_nodes_indexed": glimpse.get("node_count", 0),
                "truncated": False,
            },
        }

    async def nexus_explore_step_v2(
        self,
        session_id: str,
        decisions: list[dict[str, Any]] | None = None,
        walks: list[dict[str, Any]] | None = None,
        max_parallel_walks: int = 4,
        global_frontier_limit: int = 80,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Exploration step with GEMSTORM action vocabulary.

        GUIDED MODES (dfs_guided_explicit / dfs_guided_bulk):
            - decisions: list of { handle, action, ... }
              Actions (canonical names):
                - 'engulf_leaf_into_gem_for_editing' = Bring leaf into GEM (editable/deletable in JEWELSTORM)
                - 'preserve_leaf_in_ether_untouched' = Preserve leaf in ETHER (protected from changes, will NOT be deleted)
                - 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states' = Bring branch node into GEM as shell (node editable, can add children, existing descendants keep their own protection states)
                - 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children' = Preserve branch node in ETHER (protected from changes, descendants unaffected unless individually engulfed)
            - walks: IGNORED in strict modes (engine controls DFS traversal)
            - global_frontier_limit: leaf budget per step (default 80)

        LEGACY / MANUAL MODES:
            - walks: list of { origin, max_steps } – ray roots and per-ray depth
            - max_parallel_walks: hard cap on how many origins we actually walk

        Response:
            {
              "session_id": ...,
              "walks": [
                {
                  "origin": "H_10",
                  "requested_max_steps": 2,
                  "complete": false,
                  "frontier": [
                    {
                      "handle": "H_10_1",
                      "parent_handle": "H_10",
                      "steps_from_origin": 1,
                      "depth_in_tree": 4,
                      "status": "candidate",
                      "name_preview": "...",
                      "child_count": 3,
                      "children_hint": [...],
                    },
                    ...
                  ],
                },
                ...
              ],
              "skipped_walks": [
                {
                  "origin": "H_12",
                  "reason": "overlap_with_shallower_origin",
                  "conflicting_origin": "H_11",
                }
              ],
              "decisions_applied": [...],
              "scratchpad": "...",
              "history_summary": {...}  # optional
            }
        """
        import json as json_module
        from datetime import datetime
        logger = _ClientLogger()

        decisions = decisions or []
        walks = walks or []

        # --- 1) Load session JSON ---
        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        if not os.path.exists(session_path):
            raise NetworkError(f"Exploration session '{session_id}' not found.")

        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}
        editable_mode = bool(session.get("editable", False))
        # Persisted frontier from the previous step (flat list). When present,
        # this represents exactly what the agent/human saw as the frontier in
        # the last call and is the default action frontier for the NEXT call
        # unless overridden by an explicit SEARCH (SX) in the current step.
        last_frontier_flat = session.get("last_frontier_flat") or []

        # If this session is running in a strict DFS mode, we ignore WALK
        # requests entirely and surface a single leaf-chunk frontier via the
        # shared _compute_exploration_frontier helper. DECISIONS (if any) have
        # already been applied above via _nexus_explore_step_internal.
        exploration_mode = session.get("exploration_mode", "manual")
        skipped_decisions: list[dict[str, Any]] = []
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:
            # EXPAND 2-LETTER CODES FIRST (before separating search actions)
            if decisions:
                for decision in decisions:
                    act = decision.get("action")
                    if act in EXPLORATION_ACTION_2LETTER:
                        decision["action"] = EXPLORATION_ACTION_2LETTER[act]
            
            # Separate PEEK, SEARCH, and other actions (all codes already expanded)
            peek_actions = [d for d in (decisions or []) if d.get("action") == "peek_descendants_as_frontier"]
            search_actions = [d for d in (decisions or []) if d.get("action") == "search_descendants_for_text"]
            resume_actions = [d for d in (decisions or []) if d.get("action") == "resume_guided_frontier"]
            # Other decisions = everything except peek/search/resume
            other_decisions = [
                d for d in (decisions or [])
                if d.get("action") not in {
                    "peek_descendants_as_frontier",
                    "search_descendants_for_text",
                    "resume_guided_frontier"
                }
            ]

            # All actions except SEARCH (which v2 handles) go to the internal engine:
            # - PEEK: to build _peek_frontier and stash it
            # - RESUME: to clear special frontiers and bump steps
            # - Other GEMSTORM decisions: engulf/preserve/branch shells, etc.
            non_search_decisions = (peek_actions or []) + (resume_actions or []) + (other_decisions or [])

            # PRE-COMPUTE the correct frontier for this step (search/peek/normal DFS)
            # This frontier will be passed to _nexus_explore_step_internal() so bulk actions
            # (EF/PF) operate on the correct set.
            #
            # Precedence:
            #   1) If this step has SX actions → use SEARCH frontier (PRIMARY)
            #   2) Else if session has stashed _peek_frontier → use PEEK frontier
            #   3) Else if we have stored last_frontier_flat → use that
            #   4) Else → compute fresh DFS frontier
            
            if search_actions:
                # SEARCH takes priority
                effective_search_actions = search_actions
                correct_frontier_for_bulk = self._compute_search_frontier(
                    session=session,
                    search_actions=effective_search_actions,
                    scope="undecided",
                    max_results=global_frontier_limit,
                )
            elif session.get("_peek_frontier"):
                # PEEK frontier stashed from previous step
                correct_frontier_for_bulk = session.get("_peek_frontier", [])
                logger.info(
                    f"Using stashed PEEK frontier from previous step "
                    f"({len(correct_frontier_for_bulk)} entries)"
                )
            elif last_frontier_flat:
                # Normal frontier from previous step
                correct_frontier_for_bulk = last_frontier_flat
            else:
                # Compute fresh DFS frontier
                correct_frontier_for_bulk = self._compute_exploration_frontier(
                    session,
                    frontier_size=global_frontier_limit,
                    max_depth_per_frontier=1,
                )
            
            # In strict DFS modes, we must APPLY DECISIONS before computing the
            # next (final) frontier. Delegate decision semantics to the v1 helper,
            # passing the pre-computed frontier so bulk actions use the correct set.
            if non_search_decisions:
                internal_result = await self._nexus_explore_step_internal(
                    session_id=session_id,
                    actions=non_search_decisions,
                    frontier_size=global_frontier_limit,
                    max_depth_per_frontier=1,
                    _precomputed_frontier=correct_frontier_for_bulk,  # NEW: pass search/normal frontier
                )
                skipped_decisions = internal_result.get("skipped_decisions", []) or []
                with open(session_path, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                handles = session.get("handles", {}) or {}
                state = session.get("state", {}) or {}
                root_node = session.get("root_node") or {}
                editable_mode = bool(session.get("editable", False))


            # Build mode-aware step guidance
            if exploration_mode == "dfs_guided_explicit":
                step_guidance = [
                    "🎯 EXPLICIT MODE: Auto-frontier. No navigation needed.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Leaf actions: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                    "Branch actions (when all descendants decided): flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)",
                    "No bulk actions available in explicit mode.",
                    "",
                    "🔍 SEARCH (not available in explicit mode by default - PRIMARY implementation is bulk mode only)"
                ]
            elif exploration_mode == "dfs_guided_bulk":
                # Check if strict_completeness is active
                strict_completeness_active = session.get("strict_completeness", False)
                
                if strict_completeness_active:
                    step_guidance = [
                        "🎯 BULK MODE: Auto-frontier with bulk actions.",
                        "",
                        "🛡️ STRICT COMPLETENESS ACTIVE - PA action DISABLED",
                        "",
                        "⚠️ COMMON AGENT FAILURE MODE: Agents routinely opt out early with PA after a few frontiers.",
                        "DON'T BE ONE OF THOSE AGENTS. This mode forces thorough exploration.",
                        "",
                        "💎 DECISION OUTCOMES:",
                        "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                        "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                        "",
                        "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                        "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                        "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                        "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA) - DISABLED in strict_completeness mode",
                        "",
                        "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                    ]
                else:
                    step_guidance = [
                        "🎯 BULK MODE: Auto-frontier with bulk actions.",
                        "",
                        "💎 DECISION OUTCOMES:",
                        "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                        "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                        "",
                        "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                        "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                        "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                        "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA)",
                        "",
                        "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                    ]
            else:
                step_guidance = [
                    "🎯 LEGACY MODE: Manual navigation.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Navigate: open (OP), close (CL)",
                    "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL)",
                    "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)"
                ]

            # PEEK STEP: if this step includes a PEEK action, return the peek frontier now.
            # The stashed _peek_frontier will then be used for bulk decisions in the next step.
            if peek_actions:
                peek_frontier = session.get("_peek_frontier", [])
                if peek_frontier:
                    frontier = peek_frontier
                    frontier_tree = self._build_frontier_tree_from_flat(frontier)
                    frontier_preview = self._build_frontier_preview_lines(frontier)

                    # Persist for next step
                    from datetime import datetime as _dt
                    session["last_frontier_flat"] = frontier
                    session["updated_at"] = _dt.utcnow().isoformat() + "Z"
                    with open(session_path, "w", encoding="utf-8") as f:
                        json_module.dump(session, f, indent=2, ensure_ascii=False)

                    # Build history_summary from current state if requested
                    history_summary = None
                    if include_history_summary:
                        history_summary = {
                            "open": [h for h, st in state.items() if st.get("status") == "open"],
                            "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                            "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                        }

                    # Return immediately - do not fall through to search/DFS logic
                    return {
                        "success": True,
                        "session_id": session_id,
                        "nexus_tag": session.get("nexus_tag"),
                        "status": "in_progress",
                        "exploration_mode": exploration_mode,
                        "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                        "action_key": EXPLORATION_ACTION_2LETTER,
                        "step_guidance": step_guidance,
                        "frontier_preview": frontier_preview,
                        "frontier_tree": frontier_tree,
                        "walks": [],  # strict DFS guided: no per-ray walks
                        "skipped_walks": [],
                        "decisions_applied": decisions,
                        "skipped_decisions": skipped_decisions,
                        "scratchpad": session.get("scratchpad", ""),
                        "history_summary": history_summary,
                    }


            # Re-compute final frontier after decisions applied (state may have changed)
            #
            # Precedence for NEXT step's frontier:
            #   1) If this step had SEARCH → re-compute search frontier
            #   2) Else if this step had PEEK → keep peeked frontier (already stashed)
            #   3) Else → re-compute normal DFS frontier
            
            # Re-compute final frontier after decisions applied (state may have changed)
            #
            # PEEK frontier is ONE-TIME-USE: consumed in the step where decisions are
            # applied, then cleared. Always return normal DFS frontier for NEXT step.

            if search_actions:
                # SEARCH in current step → return search frontier
                frontier = self._compute_search_frontier(
                    session=session,
                    search_actions=search_actions,
                    scope="undecided",
                    max_results=global_frontier_limit,
                )
            elif session.get("_peek_frontier"):
                # Stashed peek from PREVIOUS step was consumed for bulk actions
                # Clear it and return normal DFS frontier with updated state
                del session["_peek_frontier"]
                if "_peek_root_handle" in session:
                    del session["_peek_root_handle"]
                if "_peek_max_nodes" in session:
                    del session["_peek_max_nodes"]

                logger.info("Peek frontier consumed by bulk decisions - cleared and returning to DFS")

                # Recompute normal DFS frontier (reflects updated state after bulk decisions)
                frontier = self._compute_exploration_frontier(
                    session,
                    frontier_size=global_frontier_limit,
                    max_depth_per_frontier=1,
                )
            else:
                # Normal DFS frontier (no peek/search override)
                frontier = self._compute_exploration_frontier(
                    session,
                    frontier_size=global_frontier_limit,
                    max_depth_per_frontier=1,
                )

            frontier_tree = self._build_frontier_tree_from_flat(frontier)
            frontier_preview = self._build_frontier_preview_lines(frontier)

            # Persist this flat frontier so the NEXT step's actions operate on
            # exactly what was shown to the agent/human in THIS step, unless
            # overridden by an explicit SEARCH (SX) in that next step.
            session["last_frontier_flat"] = frontier

            # Update session timestamp and persist.
            from datetime import datetime as _dt
            session["updated_at"] = _dt.utcnow().isoformat() + "Z"
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)

            # Build history summary if requested.
            history_summary = None
            if include_history_summary:
                history_summary = {
                    "open": [h for h, st in state.items() if st.get("status") == "open"],
                    "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                    "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                }

            return {
                "success": True,
                "session_id": session_id,
                "nexus_tag": session.get("nexus_tag"),
                "status": "in_progress",
                "exploration_mode": exploration_mode,
                "action_key_primary_aliases": {
                    "EB": "reserve_branch_for_children",
                },
                "action_key": EXPLORATION_ACTION_2LETTER,
                "step_guidance": step_guidance,
                "frontier_preview": frontier_preview,
                "frontier_tree": frontier_tree,
                "walks": [],  # Strict DFS: no per-ray walks; see top-level frontier.
                "skipped_walks": [],
                "decisions_applied": decisions,
                "skipped_decisions": skipped_decisions,
                "scratchpad": session.get("scratchpad", ""),
                "history_summary": history_summary,
            }

    async def nexus_explore_step(
        self,
        session_id: str,
        decisions: list[dict[str, Any]] | None = None,
        walks: list[dict[str, Any]] | None = None,
        max_parallel_walks: int = 4,
        global_frontier_limit: int = 80,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Exploration step: separate DECISIONS from WALKs and allow multiple
        independent rays (origins) in a single call.

        Request:
            decisions: list of { handle, action, ... } – accept/reject/finalize/etc
            walks: list of { origin, max_steps } – ray roots and per-ray depth
            max_parallel_walks: hard cap on how many origins we actually walk
            global_frontier_limit: total frontier entries to emit across all rays

        Response:
            {
              "session_id": ...,
              "walks": [...],
              "skipped_walks": [...],
              "decisions_applied": [...],
              "scratchpad": "...",
              "history_summary": {...}  # optional
            }
        """
        # v2 is now THE ONLY VERSION - forward to nexus_explore_step_v2
        return await self.nexus_explore_step_v2(
            session_id=session_id,
            decisions=decisions,
            walks=walks,
            max_parallel_walks=max_parallel_walks,
            global_frontier_limit=global_frontier_limit,
            include_history_summary=include_history_summary,
        )

    async def _nexus_explore_step_internal(
        self,
        session_id: str,
        actions: list[dict[str, Any]] | None = None,
        frontier_size: int = 5,
        max_depth_per_frontier: int = 1,
        _precomputed_frontier: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Internal v1-style exploration (used by v2 for decisions delegation).

        This is the old nexus_explore_step implementation, kept as an internal
        helper so that nexus_explore_step_v2 can delegate decision-processing
        to it while handling multi-ray walks itself.
        """
        import logging
        import json as json_module
        from datetime import datetime

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        skipped_decisions: list[dict[str, Any]] = []

        if not os.path.exists(session_path):
            raise NetworkError(f"Exploration session '{session_id}' not found.")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load exploration session '{session_id}': {e}") from e

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        exploration_mode = session.get("exploration_mode", "manual")
        root_node = session.get("root_node") or {}

        # Build id→node index for editable sessions so that note/tag updates
        # mutate the cached tree that finalize_exploration will read.
        node_by_id: dict[str, dict[str, Any]] = {}

        def _index_tree_for_edit(node: dict[str, Any]) -> None:
            nid = node.get("id")
            if nid:
                node_by_id[nid] = node
            for child in node.get("children", []) or []:
                _index_tree_for_edit(child)

        if root_node:
            _index_tree_for_edit(root_node)

        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        # Lazy-loaded map of guardian override tokens for this session. These
        # allow Dan to bypass strict dfs_full_walk enforcement on a
        # per-branch basis by providing a secret token tied to this session_id
        # and handle.
        guardian_overrides: dict[str, str] | None = None

        def _load_guardian_overrides() -> dict[str, str]:
            """Load per-branch guardian override tokens for strict DFS sessions.

            File format (per session_id):
                {
                  "branch_overrides": {
                    "A": "token-1",
                    "B.3": "token-2"
                  }
                }

            Any error (missing file, parse failure, etc.) is treated as "no
            overrides" rather than failing the exploration step.
            """
            nonlocal guardian_overrides
            if guardian_overrides is not None:
                return guardian_overrides

            base_dir = (
                r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_explore_guardians"
            )
            path = os.path.join(base_dir, f"{session_id}.json")
            try:
                if not os.path.exists(path):
                    guardian_overrides = {}
                    return guardian_overrides
                with open(path, "r", encoding="utf-8") as gf:
                    data = json_module.load(gf)
                raw = data.get("branch_overrides") or {}
                # Normalize keys/values to strings
                guardian_overrides = {str(k): str(v) for k, v in raw.items()}
            except Exception:
                # On any error, fall back to no overrides rather than failing
                # the session.
                guardian_overrides = {}
            return guardian_overrides

        def _summarize_descendants(branch_handle: str) -> dict[str, Any]:
            """Summarize descendant decisions for a branch handle.

            Returns a dict with:
                {
                    "descendant_count": int,
                    "has_decided": bool,
                    "has_undecided": bool,
                    "accepted_leaf_count": int,
                    "rejected_leaf_count": int,
                }

            We intentionally only count *leaf* accepts/rejects for the
            accepted/rejected counters; non-leaf decisions still influence
            has_decided/has_undecided.
            """
            # Gather all descendant handles via BFS
            queue: list[str] = list(handles.get(branch_handle, {}).get("children", []) or [])
            descendants: list[str] = []
            while queue:
                h = queue.pop(0)
                descendants.append(h)
                child_handles = handles.get(h, {}).get("children", []) or []
                if child_handles:
                    queue.extend(child_handles)

            if not descendants:
                return {
                    "descendant_count": 0,
                    "has_decided": False,
                    "has_undecided": False,
                    "accepted_leaf_count": 0,
                    "rejected_leaf_count": 0,
                }

            accepted_leaves = 0
            rejected_leaves = 0
            has_decided = False
            has_undecided = False

            for h in descendants:
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")
                if status in {"finalized", "closed"}:
                    has_decided = True
                else:
                    has_undecided = True

                # Leaf = no children in the cached tree
                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles
                if not is_leaf:
                    continue

                if status == "finalized":
                    accepted_leaves += 1
                elif status == "closed":
                    rejected_leaves += 1

            return {
                "descendant_count": len(descendants),
                "has_decided": has_decided,
                "has_undecided": has_undecided,
                "accepted_leaf_count": accepted_leaves,
                "rejected_leaf_count": rejected_leaves,
            }

        def _auto_complete_ancestors_from_decision(start_handle: str) -> None:
            """Auto-complete ancestors when all descendants are decided (RULE 1).

            This is the generalized form used for both leaf-level and
            branch-level decisions. Starting from ``start_handle``, walk upward
            and auto-complete ancestors whose entire subtrees are decided:

            - If any descendant leaf was accepted → ancestor auto-ACCEPT as a
              path element (status='finalized', selection_type='path').
            - If all descendant leaves were rejected → ancestor auto-REJECT
              (status='closed').

            We never override explicit subtree selections (selection_type='subtree');
            those remain authoritative shells/true subtrees. This runs in all
            exploration modes (dfs_full_walk, dfs_guided, legacy), so agents
            always benefit from smart backtracking once a region is fully decided.
            """
            current = start_handle
            while True:
                parent_handle = (handles.get(current) or {}).get("parent")
                if not parent_handle:
                    # Reached synthetic root or no parent; nothing further to do.
                    break

                # Do not override explicit subtree selections (shells/true subtrees)
                parent_entry = state.get(
                    parent_handle,
                    {"status": "unseen", "max_depth": None, "selection_type": None},
                )
                if parent_entry.get("selection_type") == "subtree":
                    current = parent_handle
                    continue

                summary = _summarize_descendants(parent_handle)
                if summary["descendant_count"] == 0:
                    # No real descendants under this ancestor; nothing to auto-complete.
                    break

                # If any descendant remains undecided, we must not auto-complete
                # this ancestor yet; DFS (or manual exploration) will revisit
                # once all leaves/branches are walked.
                if summary["has_undecided"]:
                    break

                accepted_leaves = summary["accepted_leaf_count"]
                rejected_leaves = summary["rejected_leaf_count"]

                # Ensure entry exists in state for this ancestor
                parent_entry = state.setdefault(
                    parent_handle,
                    {"status": "unseen", "max_depth": None, "selection_type": None},
                )

                if accepted_leaves > 0:
                    # At least one descendant leaf accepted: ancestor becomes a
                    # PATH ELEMENT tying accepted leaves back toward the root.
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "finalized"
                        # Mark this ancestor explicitly as a PATH element so
                        # finalize_exploration does NOT treat it as a subtree
                        # selection. Its inclusion in the minimal covering tree
                        # comes from accepted leaves walking ancestors, not from
                        # subtree-selected semantics.
                        parent_entry["selection_type"] = "path"
                elif rejected_leaves > 0:
                    # All descendant leaves rejected (has_undecided is False):
                    # the entire branch can be treated as rejected.
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "closed"
                        parent_entry["selection_type"] = None
                        parent_entry["max_depth"] = None
                else:
                    # All descendants decided but no leaf outcomes recorded; this
                    # is an edge case (e.g., internal nodes only). We stop here.
                    break

                # Move one level up and see if that ancestor is now fully decided too.
                current = parent_handle

        def _auto_complete_ancestors_from_leaf(leaf_handle: str) -> None:
            """Backward-compatible helper for leaf decisions.

            Preserved for clarity; delegates to the generic ancestor
            auto-completion logic so that leaf decisions and branch decisions
            share the same RULE 1 behavior.
            """
            _auto_complete_ancestors_from_decision(leaf_handle)

        # Apply actions
        actions = actions or []
        
        # 2-LETTER ACTION CODE EXPANDER: Translate shorthand to full action names
        # This runs FIRST so aliases can also use 2-letter codes
        for action in actions:
            act = action.get("action")
            if act in EXPLORATION_ACTION_2LETTER:
                action["action"] = EXPLORATION_ACTION_2LETTER[act]
        
        # ACTION ALIAS MAPPER: Translate human-friendly aliases to machine actions
        ACTION_ALIASES = {
            "reserve_branch_for_children": "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states",
        }
        
        for action in actions:
            act = action.get("action")
            if act in ACTION_ALIASES:
                action["action"] = ACTION_ALIASES[act]
        
        # Sort actions for correct execution order:
        # 1. ENGULF actions first, then PRESERVE actions, then OTHER
        # 2. Frontier bulk actions (engulf/preserve all showing) run near the end
        # 3. Global "preserve_all_remaining_nodes_in_ether_at_finalization" runs last
        # 4. Within each category, LEAFMOST first (deeper handles first)
        def action_sort_key(action: dict[str, Any]) -> tuple[int, int]:
            """Sort key: (category_priority, depth_priority).
            
            Category: ENGULF (0) before PRESERVE (1) before OTHER (2),
            then frontier bulk (3), then global preserve_all_remaining_nodes_in_ether_at_finalization (4).
            Depth: Leafmost first (negative depth, so deeper = lower number = earlier)
            """
            act = action.get("action", "")
            handle = action.get("handle", "")
            
            # Category priority
            if act in {"engulf_leaf_into_gem_for_editing", "engulf_shell_in_gemstorm"}:
                category = 0  # ENGULF first
            elif act in {"preserve_leaf_in_ether_untouched", "preserve_branch_node_in_ether_untouched__when_no_engulfed_children"}:
                category = 1  # PRESERVE second
            elif act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                category = 3  # Frontier bulk near the end
            elif act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                category = 4  # Global preserve_all_remaining_nodes_in_ether_at_finalization last
            else:
                category = 2  # Other actions in the middle (open, close, hints, etc.)
            
            # Depth priority: count dots in handle (A.B.C.D has 3 dots = depth 4)
            # Negate so deeper handles sort first
            depth = -handle.count(".")
            
            return (category, depth)
        
        actions.sort(key=action_sort_key)
        
        # Use pre-computed frontier (from v2 caller) OR compute it here if not provided
        session["handles"] = handles
        session["state"] = state
        if _precomputed_frontier is not None:
            # v2 pre-computed the correct frontier (search or normal) for bulk actions
            current_step_frontier = _precomputed_frontier
        else:
            # Legacy path: compute normal DFS frontier here
            current_step_frontier = self._compute_exploration_frontier(
                session,
                frontier_size=frontier_size,
                max_depth_per_frontier=max_depth_per_frontier,
            )
        
        # Snapshot of state before this step for re-decision guards
        prev_state: dict[str, dict[str, Any]] = {
            h: state.get(h, {}).copy() for h in state.keys()
        }
        
        peek_results: list[dict[str, Any]] = []
        for action in actions:
            act = action.get("action")
            
            # Handle-less global actions
            if act in {"set_scratchpad", "append_scratchpad", "preserve_all_remaining_nodes_in_ether_at_finalization"}:
                if act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                    # BULK MODE ONLY: Preserve all remaining undecided nodes in ETHER
                    if exploration_mode != "dfs_guided_bulk":
                        raise NetworkError(
                            "preserve_all_remaining_nodes_in_ether_at_finalization action is only available in non-strict mode (dfs_guided_bulk).\n\n"
                            "In strict DFS mode (dfs_guided_explicit), you must explicitly decide every leaf."
                        )
                    
                    # STRICT COMPLETENESS: Disable PA even in bulk mode
                    strict_completeness = session.get("strict_completeness", False)
                    if strict_completeness:
                        raise NetworkError(
                            "preserve_all_remaining_nodes_in_ether_at_finalization (PA) is disabled in strict_completeness mode.\n\n"
                            "⚠️ COMMON AGENT FAILURE MODE: Agents routinely opt out early after just a few frontiers, \n"
                            "using the catch-all PA to skip thorough exploration. DON'T BE ONE OF THOSE AGENTS.\n\n"
                            "strict_completeness forces you to look at ALL frontiers and make explicit decisions.\n\n"
                            "Use this mode for:\n"
                            "  • Terminology cleanup (must review every node)\n"
                            "  • Critical refactoring (can't afford to miss anything)\n"
                            "  • Documentation updates requiring thoroughness\n\n"
                            "To proceed: Make explicit decisions on all nodes via EL/PL (leaves) or RB/PB (branches)."
                        )
                    
                    preserved_count = 0
                    structurally_included = []
                    
                    for h in handles:
                        h_state = state.get(h, {})
                        h_status = h_state.get("status")
                        
                        # Treat missing status as undecided (same as "unseen")
                        if h_status in ("unseen", "candidate", "open", None):
                            # Check if this handle or any descendant is engulfed
                            h_summary = _summarize_descendants(h)
                            if h_summary["accepted_leaf_count"] > 0:
                                # Has engulfed descendants - this handle lies on a path to one
                                # or more engulfed leaves. It must be treated as DECIDED so
                                # completeness checks do not flag it as uncovered.
                                f_entry = state.setdefault(
                                    h,
                                    {"status": "unseen", "max_depth": None, "selection_type": None},
                                )
                                if f_entry.get("status") not in {"finalized", "closed"}:
                                    f_entry["status"] = "finalized"
                                    # Explicitly mark as PATH so finalize_exploration understands
                                    # this is an ancestor anchor, not a subtree selection.
                                    f_entry["selection_type"] = "path"
                                structurally_included.append(h)
                            else:
                                # No engulfed descendants - preserve this node in ETHER
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    
                    logger.info(
                        f"preserve_all_remaining_nodes_in_ether_at_finalization: {preserved_count} nodes preserved in ETHER, "
                        f"{len(structurally_included)} branches with engulfed descendants will be structurally included"
                    )
                    continue
                
                # Scratchpad actions
                if act in {"set_scratchpad", "append_scratchpad"}:
                    content = action.get("content") or ""
                    existing = session.get("scratchpad") or ""
                    if act == "set_scratchpad":
                        session["scratchpad"] = content
                    else:
                        if existing:
                            session["scratchpad"] = existing + "\n" + content
                        else:
                            session["scratchpad"] = content
                    continue

            handle = action.get("handle")
            max_depth = action.get("max_depth")

            # FRONTIER-BASED BULK ACTIONS (non-strict mode only)
            if act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                if exploration_mode != "dfs_guided_bulk":
                    raise NetworkError(
                        f"Action '{act}' is only available in non-strict mode (dfs_guided).\n\n"
                        "In strict DFS mode, you must explicitly decide each leaf individually."
                    )
                
                if handle not in handles:
                    raise NetworkError(f"Unknown handle in bulk action: '{handle}'")
                
                # Helper: check if frontier_handle descends from branch_handle
                def _is_descendant_of(frontier_handle: str, branch_handle: str) -> bool:
                    """True if frontier_handle is a descendant of branch_handle."""
                    cur = frontier_handle
                    seen: set[str] = set()
                    while cur and cur not in seen:
                        if cur == branch_handle:
                            return True
                        seen.add(cur)
                        cur = handles.get(cur, {}).get("parent")
                    return False
                
                # Filter frontier to descendants of this branch
                matching_frontier_entries = [
                    entry for entry in current_step_frontier
                    if _is_descendant_of(entry["handle"], handle)
                ]
                
                if not matching_frontier_entries:
                    logger.info(
                        f"Bulk action on '{handle}': no matching frontier entries found "
                        f"(frontier may be empty or descendants not visible this step)"
                    )
                    continue
                
                # Apply bulk decision to all matching frontier entries
                if act == "engulf_all_showing_undecided_descendants_into_gem_for_editing":
                    engulfed_leaf_count = 0
                    engulfed_branch_count = 0
                    skipped_already_decided = 0
                    
                    for entry in matching_frontier_entries:
                        fh = entry["handle"]
                        f_entry = state.get(fh, {"status": "unseen"})
                        
                        # Skip if already decided (silent - bulk operations are forgiving)
                        if f_entry.get("status") in {"finalized", "closed"}:
                            skipped_already_decided += 1
                            continue
                        
                        if entry["is_leaf"]:
                            # Leaf: standard engulf behavior
                            f_entry = state.setdefault(
                                fh,
                                {"status": "unseen", "max_depth": None, "selection_type": None},
                            )
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "leaf"
                            f_entry["max_depth"] = max_depth
                            _auto_complete_ancestors_from_leaf(fh)
                            engulfed_leaf_count += 1
                        else:
                            # Branch: treat as shell subtree in GEM (children semantics preserved)
                            f_entry = state.setdefault(
                                fh,
                                {"status": "unseen", "max_depth": None, "selection_type": None},
                            )
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "subtree"
                            f_entry["max_depth"] = max_depth
                            f_entry["subtree_mode"] = "shell"
                            # Do NOT auto-complete ancestors here; shells are explicit coverage points.
                            engulfed_branch_count += 1
                    
                    logger.info(
                        "engulf_frontier_descendants: "
                        f"{engulfed_leaf_count} leaves engulfed, "
                        f"{engulfed_branch_count} branches engulfed as shells, "
                        f"{skipped_already_decided} already decided"
                    )
                
                elif act == "preserve_all_showing_undecided_descendants_in_ether":
                    preserved_count = 0
                    skipped_already_decided = 0
                    
                    for entry in matching_frontier_entries:
                        fh = entry["handle"]
                        f_entry = state.get(fh, {"status": "unseen"})
                        
                        # Skip if already decided (silent - bulk operations are forgiving)
                        if f_entry.get("status") in {"finalized", "closed"}:
                            skipped_already_decided += 1
                            continue
                        
                        # Preserve this node in ETHER (leaf or branch)
                        f_entry = state.setdefault(fh, {"status": "unseen", "max_depth": None, "selection_type": None})
                        f_entry["status"] = "closed"
                        f_entry["selection_type"] = None
                        f_entry["max_depth"] = None
                        _auto_complete_ancestors_from_leaf(fh)
                        preserved_count += 1
                    
                    logger.info(
                        f"preserve_frontier_descendants: {preserved_count} nodes preserved in ETHER, "
                        f"{skipped_already_decided} already decided"
                    )
                
                continue  # Bulk action processed, move to next action

            if handle not in handles:
                raise NetworkError(f"Unknown handle in actions: '{handle}'")

            if act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm", "update_tag_and_engulf_in_gemstorm"} and not editable_mode:
                raise NetworkError(
                    "This exploration session was started with editable=False; "
                    "update_node_and_engulf_in_gemstorm / update_note_and_engulf_in_gemstorm / update_tag_and_engulf_in_gemstorm "
                    "are only allowed when editable=True. Start a new session with "
                    "editable=True if you want to change names/notes/tags as you explore."
                )

            # In dfs_guided / dfs_full_walk modes, enforce that most actions
            # apply only to the current DFS focus handle. This keeps navigation
            # deterministic and brain-dead simple for agents: one node at a
            # time.
            # In non-strict mode, allow preserve actions outside frontier for cleanup
            preserve_actions_allowed_outside_frontier = {"preserve_leaf_in_ether_untouched", "preserve_branch_node_in_ether_untouched__when_no_engulfed_children"} if exploration_mode == "dfs_guided_bulk" else set()
            
            # Frontier gating disabled: decisions allowed on any known handle; frontier is descriptive only.
            if False and exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"} and act not in ({"add_hint", "peek_descendants_as_frontier", "resume_guided_frontier"} | preserve_actions_allowed_outside_frontier):
                # Recompute current DFS frontier based on the *current* state
                # before applying this action. Any handle in the current
                # leaf-chunk frontier is a valid focus for decisions in this
                # step; actions outside that set are rejected as out-of-order.
                session["handles"] = handles
                session["state"] = state
                current_frontier = self._compute_exploration_frontier(
                    session,
                    frontier_size=max(frontier_size, 1),
                    max_depth_per_frontier=max_depth_per_frontier,
                )
                allowed_handles = {entry["handle"] for entry in current_frontier}

                # Allow branch-wide corrections from anywhere via re-decisions,
                # but require all other decisions to target a handle that is
                # currently in the strict DFS leaf frontier.
                if allowed_handles and handle not in allowed_handles:
                    # Harmless: skip out-of-frontier decisions and record them
                    # for the caller instead of raising an error.
                    skipped_decisions.append(
                        {
                            "handle": handle,
                            "action": act,
                            "reason": "not_in_current_leaf_frontier",
                            "current_leaf_frontier": sorted(list(allowed_handles)),
                        }
                    )
                    continue

            if handle not in state:
                state[handle] = {"status": "unseen", "max_depth": None, "selection_type": None}

            entry = state[handle]
            # Ensure selection_type key is always present for downstream logic
            if "selection_type" not in entry:
                entry["selection_type"] = None

            if act == "peek_descendants_as_frontier":
                # PEEK DESCENDANTS AS FRONTIER: Build a proper frontier from descendants
                # of the specified handle, matching SEARCH frontier behavior.
                #
                # Behavior:
                # - BFS from handle root up to max_nodes
                # - Returns frontier structure (same format as SEARCH)
                # - Stashes frontier as _peek_frontier in session
                # - Next step uses peeked frontier (or resume_guided_frontier clears it)
                #
                # If SEARCH is also active in same step:
                # - SEARCH takes priority (constrains peek to search results)
                # - Peek operates on intersection of: descendants(handle) ∩ search_matches
                
                max_nodes = action.get("max_nodes") or 200
                try:
                    max_nodes_int = int(max_nodes)
                except (TypeError, ValueError):
                    max_nodes_int = 200
                if max_nodes_int <= 0:
                    max_nodes_int = 200
                
                # Determine peek root: if SEARCH is active in THIS step, constrain
                # peek to search results under this handle
                search_actions = [a for a in actions if a.get("action") == "search_descendants_for_text"]
                
                if search_actions:
                    # SEARCH + PEEK: Constrain peek to search results under handle
                    # First compute full search frontier
                    search_frontier_full = self._compute_search_frontier(
                        session=session,
                        search_actions=search_actions,
                        scope="undecided",
                        max_results=10000,  # High limit - get all matches first
                    )
                    
                    # Filter to descendants of peek handle only
                    def _is_descendant_of_peek_root(h: str, peek_root: str) -> bool:
                        if h == peek_root:
                            return True
                        cur = handles.get(h, {}).get("parent")
                        seen: set[str] = set()
                        while cur and cur not in seen:
                            if cur == peek_root:
                                return True
                            seen.add(cur)
                            cur = handles.get(cur, {}).get("parent")
                        return False
                    
                    constrained_frontier = [
                        entry for entry in search_frontier_full
                        if _is_descendant_of_peek_root(entry["handle"], handle)
                    ]
                    
                    # Limit to max_nodes
                    peek_frontier = constrained_frontier[:max_nodes_int]
                    
                    logger.info(
                        f"peek_descendants_as_frontier: SEARCH+PEEK mode, "
                        f"showing {len(peek_frontier)} descendants of '{handle}' that match search"
                    )
                else:
                    # NORMAL PEEK: BFS from handle root
                    from collections import deque
                    
                    queue: deque[str] = deque([handle])
                    visited: set[str] = set()
                    peek_handles: list[str] = []
                    
                    # BFS accumulation up to max_nodes
                    while queue and len(peek_handles) < max_nodes_int:
                        h = queue.popleft()
                        if h in visited:
                            continue
                        visited.add(h)
                        
                        # Add this handle to peek results
                        peek_handles.append(h)
                        
                        # Enqueue children for next BFS level
                        child_handles = handles.get(h, {}).get("children", []) or []
                        for ch in child_handles:
                            if ch not in visited:
                                queue.append(ch)
                    
                    # Build frontier entries from peek_handles
                    # (Same structure as _compute_search_frontier returns)
                    peek_frontier: list[dict[str, Any]] = []
                    MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW
                    
                    for h in peek_handles:
                        meta = handles.get(h, {}) or {}
                        st = state.get(h, {"status": "unseen"})
                        child_handles = meta.get("children", []) or []
                        is_leaf = not child_handles
                        
                        # Note preview (token-bounded)
                        note_full = meta.get("note") or ""
                        if MAX_NOTE_PREVIEW is None:
                            note_preview = note_full
                        else:
                            note_preview = (
                                note_full
                                if len(note_full) <= MAX_NOTE_PREVIEW
                                else note_full[:MAX_NOTE_PREVIEW]
                            )
                        
                        local_hints = meta.get("hints") or []
                        hints_from_ancestors = []  # Skip ancestor hints for peek
                        
                        # Guidance based on leaf vs branch
                        if is_leaf:
                            guidance = "PEEK - leaf: EL=engulf, PL=preserve"
                        else:
                            guidance = "PEEK - branch: RB=reserve, PB=preserve, EF=engulf_showing, PF=preserve_showing"
                        
                        entry = {
                            "handle": h,
                            "parent_handle": meta.get("parent"),
                            "name_preview": meta.get("name", ""),
                            "note_preview": note_preview,
                            "child_count": len(child_handles),
                            "depth": meta.get("depth", 0),
                            "status": st.get("status", "candidate"),
                            "is_leaf": is_leaf,
                            "guidance": guidance,
                        }
                        if local_hints:
                            entry["hints"] = local_hints
                        if hints_from_ancestors:
                            entry["hints_from_ancestors"] = hints_from_ancestors
                        
                        peek_frontier.append(entry)
                    
                    logger.info(
                        f"peek_descendants_as_frontier: BFS from '{handle}', "
                        f"showing {len(peek_frontier)} descendants (max_nodes={max_nodes_int})"
                    )
                
                # STASH peek frontier in session for next step
                # (Matches SEARCH behavior - frontier persists until consumed or cleared)
                session["_peek_frontier"] = peek_frontier
                session["_peek_root_handle"] = handle
                session["_peek_max_nodes"] = max_nodes_int
                
                # Also attach as peek_results for THIS step's response
                # (So agent sees what was peeked immediately)
                peek_results.append({
                    "root_handle": handle,
                    "max_nodes": max_nodes_int,
                    "nodes_returned": len(peek_frontier),
                    "truncated": len(visited) >= max_nodes_int if not search_actions else False,
                    "frontier": peek_frontier,
                    "note": "Frontier stashed - will be used in next step unless overridden by search or resume_guided_frontier"
                })
                
                continue
            
            if act == "resume_guided_frontier":
                # RESUME GUIDED FRONTIER: Clear any stashed SEARCH/PEEK frontiers
                # and return to normal DFS leaf-chunk frontier.
                #
                # This is a no-op action (no handle required) that explicitly
                # requests "give me next normal DFS batch".
                #
                # Use cases:
                # - After SEARCH: reviewed matches, want to resume normal DFS
                # - After PEEK: inspected subtree, want to continue main exploration
                # - Explicit continue (clearer than empty decisions array)
                
                # Clear stashed frontiers (if any)
                if "_search_frontier" in session:
                    del session["_search_frontier"]
                    logger.info("resume_guided_frontier: cleared stashed SEARCH frontier")
                
                if "_peek_frontier" in session:
                    del session["_peek_frontier"]
                    del session["_peek_root_handle"]
                    del session["_peek_max_nodes"]
                    logger.info("resume_guided_frontier: cleared stashed PEEK frontier")
                
                # No other state changes - this just clears the frontier override
                # Next _compute_exploration_frontier call will return normal DFS
                continue

            if act == "add_hint":
                hint_text = action.get("hint")
                if not isinstance(hint_text, str) or not hint_text.strip():
                    raise NetworkError("add_hint action requires non-empty 'hint' string")
                meta = handles.get(handle) or {}
                existing_hints = meta.get("hints")
                if not isinstance(existing_hints, list):
                    existing_hints = []
                existing_hints.append(hint_text)
                meta["hints"] = existing_hints
                handles[handle] = meta
                continue

            # Leaf-first enhancements
            if act == "engulf_leaf_into_gem_for_editing":
                entry["status"] = "finalized"
                entry["selection_type"] = "leaf"
                entry["max_depth"] = max_depth
                _auto_complete_ancestors_from_leaf(handle)
            elif act == "preserve_leaf_in_ether_untouched":
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
                _auto_complete_ancestors_from_leaf(handle)
            elif act == "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states":
                summary = _summarize_descendants(handle)
                desc_count = summary["descendant_count"]
                has_decided = summary["has_decided"]
                has_undecided = summary["has_undecided"]
                accepted_leaves = summary["accepted_leaf_count"]
                rejected_leaves = summary["rejected_leaf_count"]

                if desc_count == 0:
                    # No descendants at all: this is effectively a leaf; require
                    # leaf-level decisions instead of subtree semantics.
                    raise NetworkError(
                        f"Handle '{handle}' has no descendants; use 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched' "
                        "for leaves instead of 'engulf_shell_in_gemstorm'."
                    )

                # STRICT DFS FULL-WALK MODE:
                # In dfs_full_walk, engulf_shell_in_gemstorm is ONLY allowed when all
                # descendants are preserved in ETHER (no undecided, no engulfed leaves).
                if exploration_mode == "dfs_guided_explicit":
                    if has_undecided or accepted_leaves > 0:
                        session["handles"] = handles
                        session["state"] = state
                        current_frontier = self._compute_exploration_frontier(
                            session,
                            frontier_size=1,
                            max_depth_per_frontier=max_depth_per_frontier,
                        )
                        next_handle = (
                            current_frontier[0]["handle"] if current_frontier else None
                        )
                        next_name = (
                            (handles.get(next_handle) or {}).get("name", "Untitled")
                            if next_handle
                            else None
                        )
                        base_msg = (
                            f"Strict DFS mode is active for this exploration session; "
                            f"cannot engulf_shell_in_gemstorm on branch '{handle}' while any descendants "
                            "remain undecided or any leaves have already been engulfed.\n\n"
                            "Walk the branch in depth-first order and make leaf-level decisions "
                            "with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched'."
                        )
                        if next_handle:
                            base_msg += (
                                f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                                "Decide there before attempting any branch-level accept/reject."
                            )
                        raise NetworkError(base_msg)
                    # If we get here in dfs_full_walk, all descendants are decided
                    # and preserved in ETHER; the existing non-strict logic below will treat
                    # this as a branch-only shell (children opaque).

                # NON-STRICT MODE (dfs_guided): Allow reserve_branch_for_children
                # even when descendants are undecided or mixed.
                if exploration_mode == "dfs_guided_bulk":
                    # In dfs_guided, engulf_shell_in_gemstorm (reserve_branch_for_children)
                    # is allowed regardless of descendant state. This enables:
                    # "I want this section as a home for new docs, regardless of
                    # how much I've explored under it."
                    #
                    # Semantics:
                    # - Branch itself → GEM as shell (editable, can add children)
                    # - Existing descendants → stay as-is:
                    #     * Already engulfed → remain engulfed (in GEM)
                    #     * Already preserved → remain preserved (in ETHER, protected)
                    #     * Undecided → effectively preserved in ETHER (covered by shell subtree selection)
                    #
                    # At finalize time, undecided descendants under a shell are
                    # considered covered (ancestor has selection_type='subtree').
                    entry["status"] = "finalized"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    entry["subtree_mode"] = "shell"
                    # Do NOT call _auto_complete_ancestors - shell is explicit decision
                    continue

                # STRICT MODE (dfs_full_walk) or legacy: enforce stricter rules
                # Mixed case: some descendants decided, some not → force DFS
                # further down rather than allowing a coarse branch decision.
                if has_decided and has_undecided:
                    session["handles"] = handles
                    session["state"] = state
                    current_frontier = self._compute_exploration_frontier(
                        session,
                        frontier_size=1,
                        max_depth_per_frontier=max_depth_per_frontier,
                    )
                    next_handle = current_frontier[0]["handle"] if current_frontier else None
                    next_name = (
                        (handles.get(next_handle) or {}).get("name", "Untitled")
                        if next_handle
                        else None
                    )
                    base_msg = (
                        f"Cannot engulf_shell_in_gemstorm on branch '{handle}' while some descendants are "
                        "still undecided and others have already been decided.\n\n"
                        "Finish exploring this branch in depth-first order first."
                    )
                    if next_handle:
                        base_msg += (
                            f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                            "Make a leaf-level decision there with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched', "
                            ""
                        )
                    raise NetworkError(base_msg)

                # All descendants undecided: branch-only shell, children opaque.
                if not has_decided and has_undecided:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    entry["subtree_mode"] = "shell"
                # All descendants decided: behavior depends on leaf outcomes.
                elif has_decided and not has_undecided:
                    if accepted_leaves > 0:
                        # Some leaves already engulfed – engulfing the shell is
                        # a no-op. The minimal covering tree will be built from
                        # leaf decisions (branch becomes a path element).
                        _auto_complete_ancestors_from_decision(handle)
                        continue
                    # All descendants preserved in ETHER: include branch-only shell (no children).
                    if rejected_leaves > 0:
                        entry["status"] = "finalized"
                        entry["selection_type"] = "subtree"
                        entry["max_depth"] = max_depth
                        entry["subtree_mode"] = "shell"
                    else:
                        # Should not happen (decided but no leaf outcomes), but
                        # guard defensively.
                        raise NetworkError(
                            f"accept_subtree on '{handle}' encountered an inconsistent descendant summary."
                        )
                else:
                    # No descendants and no decisions – already handled above.
                    raise NetworkError(
                        f"accept_subtree on '{handle}' is not applicable in the current state."
                    )

                # After any successful subtree accept, attempt ancestor auto-completion
                # so that fully decided regions auto-backtrack in all modes.
                _auto_complete_ancestors_from_decision(handle)

            elif act == "preserve_branch_node_in_ether_untouched__when_no_engulfed_children":
                summary = _summarize_descendants(handle)
                desc_count = summary["descendant_count"]
                has_decided = summary["has_decided"]
                has_undecided = summary["has_undecided"]
                accepted_leaves = summary["accepted_leaf_count"]

                if desc_count == 0:
                    raise NetworkError(
                        f"Handle '{handle}' has no descendants; use 'preserve_leaf_in_ether_untouched' "
                        "for leaves instead of 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children'."
                    )

                # STRICT DFS FULL-WALK MODE:
                # In dfs_full_walk, preserve_branch_node_in_ether_untouched__when_no_engulfed_children is ONLY allowed when all
                # descendants are decided (no undecided). Engulfed leaves are
                # handled by the generic check below (we never allow a branch-
                # wide preserve that silently overrides engulfed leaves).
                if exploration_mode == "dfs_guided_explicit" and has_undecided:
                    session["handles"] = handles
                    session["state"] = state
                    current_frontier = self._compute_exploration_frontier(
                        session,
                        frontier_size=1,
                        max_depth_per_frontier=max_depth_per_frontier,
                    )
                    next_handle = (
                        current_frontier[0]["handle"] if current_frontier else None
                    )
                    next_name = (
                        (handles.get(next_handle) or {}).get("name", "Untitled")
                        if next_handle
                        else None
                    )
                    base_msg = (
                    f"Strict DFS mode is active for this exploration session; "
                    f"cannot preserve_branch_node_in_ether_untouched__when_no_engulfed_children on branch '{handle}' while any descendants "
                    "remain undecided.\n\n"
                    "Walk the branch in depth-first order and make leaf-level decisions "
                    "with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched'."
                    )
                    if next_handle:
                        base_msg += (
                            f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                            "Decide there before attempting any branch-level accept/reject."
                        )
                    raise NetworkError(base_msg)

                # NON-STRICT MODE: Smart preserve logic
                if exploration_mode == "dfs_guided_bulk":
                    # In non-strict, allow preserve_branch even with mixed/engulfed descendants
                    # by smartly preserving only non-engulfed nodes in ETHER and structurally including
                    # branches that have engulfed descendants
                    
                    preserved_count = 0
                    structurally_included = []
                    
                    for h in handles:
                        if not (h.startswith(handle + ".")):
                            continue  # Not a descendant
                        
                        h_state = state.get(h, {})
                        h_status = h_state.get("status")
                        
                        if h_status == "finalized":
                            # Already engulfed - will be structurally included by ancestor logic
                            continue
                        elif h_status in ("unseen", "candidate", "open"):
                            # Check if this is a branch with engulfed descendants
                            h_summary = _summarize_descendants(h)
                            if h_summary["accepted_leaf_count"] > 0:
                                # Branch has engulfed descendants - will be structurally included
                                structurally_included.append(h)
                            else:
                                # No engulfed descendants - preserve this node in ETHER
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    
                    # Log what happened for agent visibility
                    logger.info(
                        f"Smart preserve on '{handle}': {preserved_count} nodes preserved in ETHER, "
                        f"{len(structurally_included)} branches structurally included (have engulfed descendants)"
                    )
                
                # STRICT MODE: Enforce all-decided requirement
                elif has_decided and has_undecided:
                    session["handles"] = handles
                    session["state"] = state
                    current_frontier = self._compute_exploration_frontier(
                        session,
                        frontier_size=1,
                        max_depth_per_frontier=max_depth_per_frontier,
                    )
                    next_handle = current_frontier[0]["handle"] if current_frontier else None
                    next_name = (
                        (handles.get(next_handle) or {}).get("name", "Untitled")
                        if next_handle
                        else None
                    )
                    base_msg = (
                        f"Cannot preserve_branch_node_in_ether_untouched__when_no_engulfed_children on branch '{handle}' while some descendants are "
                        "still undecided and others have already been decided.\n\n"
                        "Finish exploring this branch in depth-first order first."
                    )
                    if next_handle:
                        base_msg += (
                            f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                            "Make a leaf-level decision there with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched', "
                            ""
                        )
                    raise NetworkError(base_msg)

                # STRICT MODE: All descendants decided but some leaves accepted
                elif has_decided and not has_undecided and accepted_leaves > 0:
                    raise NetworkError(
                        f"Cannot preserve_branch_node_in_ether_untouched__when_no_engulfed_children on branch '{handle}' because one or more leaves "
                        "under this branch have already been engulfed in the GEMSTORM. "
                        "Revisit those leaves with 'preserve_leaf_in_ether_untouched' (nodes can be re-decided at any time)."
                    )

                # All descendants undecided, or all decided with no accepted leaves:
                # mark this branch as a rejected subtree so completeness logic
                # can treat descendants as covered.
                entry["status"] = "closed"
                entry["selection_type"] = "subtree"
                entry["max_depth"] = max_depth

                # After any successful subtree reject, attempt ancestor auto-completion
                # so that fully rejected regions auto-backtrack in all modes.
                _auto_complete_ancestors_from_decision(handle)
            elif act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm"}:
                if not editable_mode:
                    raise NetworkError(
                        "update_node_and_engulf_in_gemstorm / update_note_and_engulf_in_gemstorm is only allowed when session.editable=True"
                    )

                new_name = action.get("name")
                new_note = action.get("note")

                meta = handles.get(handle) or {}
                node_id_for_edit = meta.get("id")
                if not node_id_for_edit:
                    raise NetworkError(f"Handle '{handle}' has no associated node id; cannot update node.")

                target_node = node_by_id.get(node_id_for_edit)
                if not target_node:
                    raise NetworkError(
                        f"Node id '{node_id_for_edit}' not found in cached exploration tree; cannot update node."
                    )

                if new_name is not None:
                    if not isinstance(new_name, str):
                        raise NetworkError(
                            "update_node_and_engulf_in_gemstorm requires 'name' to be a string if provided"
                        )
                    target_node["name"] = new_name
                    meta["name"] = new_name

                if new_note is not None:
                    if not isinstance(new_note, str):
                        raise NetworkError(
                            "update_node_and_engulf_in_gemstorm requires 'note' to be a string if provided"
                        )
                    target_node["note"] = new_note
                    meta["note"] = new_note

                handles[handle] = meta

                child_handles = handles.get(handle, {}).get("children", []) or []
                is_leaf = not child_handles

                entry = state.setdefault(handle, {"status": "unseen", "max_depth": None, "selection_type": None})

                if is_leaf:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors_from_leaf(handle)
                else:
                    # Branch case: behave like immediate subtree finalize
                    entry["status"] = "finalized"
                    if entry.get("selection_type") is None:
                        entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors_from_decision(handle)

            elif act == "update_tag_and_engulf_in_gemstorm":
                if not editable_mode:
                    raise NetworkError("update_tag_and_engulf_in_gemstorm is only allowed when session.editable=True")

                raw_tag = action.get("tag")
                if not isinstance(raw_tag, str) or not raw_tag.strip():
                    raise NetworkError("update_tag_and_engulf_in_gemstorm requires non-empty 'tag' string")

                tag = raw_tag.strip()
                if not tag.startswith("#"):
                    tag = f"#{tag}"

                meta = handles.get(handle) or {}
                node_id_for_edit = meta.get("id")
                if not node_id_for_edit:
                    raise NetworkError(f"Handle '{handle}' has no associated node id; cannot add tag.")

                target_node = node_by_id.get(node_id_for_edit)
                if not target_node:
                    raise NetworkError(
                        f"Node id '{node_id_for_edit}' not found in cached exploration tree; cannot add tag."
                    )

                current_name = target_node.get("name") or ""
                tokens = current_name.split()
                if tag not in tokens:
                    new_name = f"{current_name} {tag}".strip()
                    target_node["name"] = new_name
                    meta["name"] = new_name
                    handles[handle] = meta

                child_handles = handles.get(handle, {}).get("children", []) or []
                is_leaf = not child_handles

                entry = state.setdefault(handle, {"status": "unseen", "max_depth": None, "selection_type": None})

                if is_leaf:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors_from_leaf(handle)
                else:
                    # Branch case: behave like immediate subtree finalize
                    entry["status"] = "finalized"
                    if entry.get("selection_type") is None:
                        entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors_from_decision(handle)
            else:
                raise NetworkError(f"Unsupported exploration action: '{act}'")

        session["handles"] = handles
        session["state"] = state
        session["steps"] = int(session.get("steps", 0)) + 1
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"

        try:
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to persist exploration session '{session_id}' after step: {e}")
            raise NetworkError(f"Failed to persist exploration session: {e}") from e

        # Build mode-aware step guidance
        if exploration_mode == "dfs_guided_explicit":
            step_guidance = [
                "🎯 EXPLICIT MODE: Auto-frontier. No navigation needed.",
                "",
                "💎 DECISION OUTCOMES:",
                "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                "",
                "Leaf actions: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                "Branch actions (when all descendants decided): flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)",
                "No bulk actions available in explicit mode. PA (preserve_all) never available in explicit mode.",
                "",
                "🔍 SEARCH: SECONDARY implementation (set at START via search_filter param) - Filters normal DFS frontier to show only matches. PRIMARY implementation (search_descendants_for_text action in STEP) not available in explicit mode."
            ]
        elif exploration_mode == "dfs_guided_bulk":
            # Check strict_completeness for step guidance
            strict_completeness_active = session.get("strict_completeness", False)
            
            if strict_completeness_active:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier with bulk actions.",
                    "",
                    "🛡️ STRICT COMPLETENESS ACTIVE - PA action DISABLED",
                    "",
                    "⚠️ COMMON AGENT FAILURE MODE: Agents routinely opt out early with PA after a few frontiers.",
                    "DON'T BE ONE OF THOSE AGENTS. This mode forces thorough exploration.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                    "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                    "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                    "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA) - DISABLED in strict_completeness mode",
                    "",
                    "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                ]
            else:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier with bulk actions.",
                    "",
                    "💎 DECISION OUTCOMES:",
                    "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                    "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                    "",
                    "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL), update_leaf_node_and_engulf_in_gemstorm (UL)",
                    "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB, alias: reserve_branch_for_children), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB), update_branch_node_and_engulf_in_gemstorm__descendants_unaffected (UB), update_branch_note_and_engulf_in_gemstorm__descendants_unaffected (UN), auto_decide_branch_no_change_required (AB)",
                    "Bulk over current frontier: engulf_all_showing_undecided_descendants_into_gem_for_editing (EF), preserve_all_showing_undecided_descendants_in_ether (PF)",
                    "Global preserve: preserve_all_remaining_nodes_in_ether_at_finalization (PA)",
                    "",
                    "🔍 SEARCH: search_descendants_for_text (SX) - Returns frontier of matches + ancestors. Params: handle='R'|branch, search_text='...', case_sensitive=False, whole_word=False, regex=False, scope='undecided'|'all'. Multiple searches use AND logic. Next step without search returns to normal DFS frontier."
                ]
        else:
            step_guidance = [
                "🎯 LEGACY MODE: Manual navigation.",
                "",
                "💎 DECISION OUTCOMES:",
                "  ENGULF → Node brought into GEM → Editable/deletable in JEWELSTORM → Changes apply to ETHER",
                "  PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)",
                "",
                "Navigate: open (OP), close (CL)",
                "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL)",
                "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (EB), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)"
            ]

        result: dict[str, Any] = {
            "success": True,
            "session_id": session_id,
            "action_key_primary_aliases": {
                "RB": "reserve_branch_for_children",
            },
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "scratchpad": session.get("scratchpad", ""),
        }

        if peek_results:
            result["peek_results"] = peek_results
        if skipped_decisions:
            result["skipped_decisions"] = skipped_decisions
            summary_handles = sorted({d.get("handle") for d in skipped_decisions if d.get("handle")})
            result["skipped_decisions_summary"] = (
                "NODES SKIPPED (harmless, but no-op): " + ", ".join(summary_handles)
            )

        return result

    async def nexus_finalize_exploration(
        self,
        session_id: str,
        include_terrain: bool = True,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """Finalize an exploration session into NEXUS artifacts (coarse terrain + GEM).

        New API (aligned with nexus_glimpse):

        - ALWAYS ensures a NEXUS run directory for this nexus_tag and writes/returns
          a coarse_terrain.json path.
        - mode="full" (default when include_terrain=True):
            * writes phantom_gem.json (GEM S0) under the run dir
            * calls nexus_anchor_gems(...) to produce shimmering_terrain.json (T1)
            * returns paths for coarse_terrain, phantom_gem, shimmering_terrain
        - mode="coarse_terrain_only" (default when include_terrain=False):
            * still writes phantom_gem.json for JEWELSTORM use
            * does NOT anchor gems; shimmering_terrain.json is not created here
            * returns only the coarse_terrain path

        The legacy include_terrain parameter is mapped to mode when mode is None:
        include_terrain=True  -> mode="full"
        include_terrain=False -> mode="coarse_terrain_only"

        v2 implementation (leaf-first aware):
        - Reads the session JSON (tree, handles, state)
        - Interprets finalized handles with an optional selection_type:
            • selection_type == "leaf"    → treat as an accepted leaf
            • selection_type == "subtree" → treat as an accepted subtree root
            • selection_type is None      → backwards-compatible, treated as subtree
        - Computes the MINIMAL COVERING TREE over the cached root tree:
            • All descendants of accepted subtrees are included
            • For accepted leaves, all ancestors up to the root are included
            • Siblings and unrelated branches are pruned
        - The resulting set of needed nodes is partitioned into disjoint
          subtrees (roots whose parent is not needed). These become the
          phantom_gem roots and nodes.
        - Writes phantom_gem.json under the NEXUS run directory for nexus_tag
          with the standard payload: {nexus_tag, roots, nodes}
        - Always ensures coarse_terrain.json exists (minimal TERRAIN) for this tag.
        """
        import logging
        import json as json_module
        import copy

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")

        if not os.path.exists(session_path):
            raise NetworkError(f"Exploration session '{session_id}' not found.")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load exploration session '{session_id}': {e}") from e

        nexus_tag = session.get("nexus_tag")
        if not nexus_tag:
            raise NetworkError(
                f"Exploration session '{session_id}' missing nexus_tag; cannot finalize."
            )

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}

        # Build basic indexes over the cached tree
        node_by_id: dict[str, dict[str, Any]] = {}
        parent_by_id: dict[str, str | None] = {}
        children_by_id: dict[str, list[str]] = {}

        def index_tree(node: dict[str, Any], parent_id: str | None) -> None:
            nid = node.get("id")
            if nid:
                node_by_id[nid] = node
                parent_by_id[nid] = parent_id
                children_by_id.setdefault(nid, [])
            for child in node.get("children", []) or []:
                cid = child.get("id")
                if cid:
                    children_by_id.setdefault(nid, []).append(cid)
                index_tree(child, nid)

        index_tree(root_node, None)

        # Compute explicitly_preserved_ids: nodes explicitly preserved in ETHER (protected from deletion).
        explicitly_preserved_ids: set[str] = set()
        preserved_subtree_root_ids: set[str] = set()

        for handle_key, st in state.items():
            # Skip synthetic root handle
            if handle_key == "R":
                continue
            status = st.get("status")
            if status != "closed":
                continue
            meta = handles.get(handle_key) or {}
            node_id_for_handle = meta.get("id")
            if not node_id_for_handle:
                continue
            child_handles = handles.get(handle_key, {}).get("children", []) or []
            if child_handles and st.get("selection_type") == "subtree":
                # Branch-level preserve_branch_node_in_ether_untouched__when_no_engulfed_children: protect entire subtree
                preserved_subtree_root_ids.add(node_id_for_handle)
            else:
                # Leaf-level preserve or closed singleton: protect just this node
                explicitly_preserved_ids.add(node_id_for_handle)

        # Expand preserved subtrees to include all descendants under each preserved root.
        for preserved_root_id in preserved_subtree_root_ids:
            if preserved_root_id not in node_by_id:
                logger.warning(
                    f"nexus_finalize_exploration: preserved subtree root id {preserved_root_id} "
                    "not found in cached tree; skipping descendant expansion."
                )
                continue
            stack_ids: list[str] = [preserved_root_id]
            while stack_ids:
                cur_id = stack_ids.pop()
                if cur_id in explicitly_preserved_ids:
                    continue
                explicitly_preserved_ids.add(cur_id)
                for child_id in children_by_id.get(cur_id, []):
                    stack_ids.append(child_id)

        # Build original_ids_seen ledger from the cached exploration tree: all
        # node IDs reachable under the exploration root (before any pruning).
        original_ids_seen: set[str] = set(node_by_id.keys())

        # Collect finalized entries with selection types
        finalized_entries: list[tuple[str, str, str | None, int | None]] = []
        for handle, st in state.items():
            if st.get("status") != "finalized":
                continue
            meta = handles.get(handle) or {}
            node_id = meta.get("id")
            if not node_id:
                logger.warning(
                    f"nexus_finalize_exploration: handle '{handle}' has no associated node id; skipping."
                )
                continue
            selection_type = st.get("selection_type") or "subtree"  # backwards-compatible default
            max_depth = st.get("max_depth")
            finalized_entries.append((handle, node_id, selection_type, max_depth))

        # If no finalized entries exist yet, look for branch shells that were
        # flagged during exploration but never upgraded to explicit subtree
        # selections. When all descendants of a shell are decided (no
        # undecided nodes) and no leaves under it were engulfed, we can safely
        # auto-finalize that shell here.
        if not finalized_entries:
            auto_shells: list[str] = []
            for handle_key, st in state.items():
                if st.get("subtree_mode") != "shell":
                    continue
                meta = handles.get(handle_key) or {}
                node_id_for_handle = meta.get("id")
                if not node_id_for_handle:
                    continue

                summary = _summarize_descendants_for_handle(handle_key)
                desc_count = summary["descendant_count"]
                has_undecided = summary["has_undecided"]
                accepted_leaves = summary["accepted_leaf_count"]

                # Require a real subtree, fully decided, with no engulfed leaves.
                if desc_count <= 0:
                    continue
                if has_undecided:
                    continue
                if accepted_leaves > 0:
                    continue

                # Upgrade this shell to an explicit subtree selection.
                st["status"] = "finalized"
                st["selection_type"] = "subtree"
                st["subtree_mode"] = "shell"
                auto_shells.append(handle_key)

            if auto_shells:
                finalized_entries = []
                for handle_key, st in state.items():
                    if st.get("status") != "finalized":
                        continue
                    meta = handles.get(handle_key) or {}
                    node_id_for_handle = meta.get("id")
                    if not node_id_for_handle:
                        continue
                    selection_type = st.get("selection_type") or "subtree"
                    max_depth = st.get("max_depth")
                    finalized_entries.append(
                        (handle_key, node_id_for_handle, selection_type, max_depth)
                    )

        if not finalized_entries:
            raise NetworkError(
                "Exploration complete but no finalized paths to export.\n\n"
                f"Session '{session_id}' contains no handles marked as engulfed leaves or subtrees.\n\n"
                "To produce a GEM you must explicitly accept something into the storm:\n"
                "  • Use 'engulf_leaf_into_gem_for_editing' (EL) on one or more leaves, or\n"
                "  • Use 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states' (RB / reserve_branch_for_children)\n"
                "    on one or more branches, or\n"
                "  • In dfs_guided_bulk mode, consider 'preserve_all_remaining_nodes_in_ether_at_finalization_during_finalization' (SA)\n"
                "    after you have flagged/engulfed the branches you care about.\n\n"
                "Once at least one leaf or branch is finalized, run nexus_finalize_exploration again."
            )

        # Helper: summarize descendants for finalize-time validation
        def _summarize_descendants_for_handle(branch_handle: str) -> dict[str, Any]:
            """Summarize descendant decisions for completeness validation.

            Mirrors _summarize_descendants from _nexus_explore_step_internal but
            local to nexus_finalize_exploration for validation purposes.
            """
            queue: list[str] = list(handles.get(branch_handle, {}).get("children", []) or [])
            descendants: list[str] = []
            while queue:
                h = queue.pop(0)
                descendants.append(h)
                child_handles = handles.get(h, {}).get("children", []) or []
                if child_handles:
                    queue.extend(child_handles)

            if not descendants:
                return {
                    "descendant_count": 0,
                    "has_decided": False,
                    "has_undecided": False,
                    "accepted_leaf_count": 0,
                    "rejected_leaf_count": 0,
                }

            accepted_leaves = 0
            rejected_leaves = 0
            has_decided = False
            has_undecided = False

            for h in descendants:
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")

                if status in {"finalized", "closed"}:
                    has_decided = True
                else:
                    has_undecided = True

                # Leaf = no children in the cached tree
                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles
                if not is_leaf:
                    continue

                if status == "finalized":
                    accepted_leaves += 1
                elif status == "closed":
                    rejected_leaves += 1

            return {
                "descendant_count": len(descendants),
                "has_decided": has_decided,
                "has_undecided": has_undecided,
                "accepted_leaf_count": accepted_leaves,
                "rejected_leaf_count": rejected_leaves,
            }

        # COMPLETENESS CHECK: ensure every handle is either explicitly decided
        # (status in {finalized, closed}) or covered by an ancestor subtree
        # decision (selection_type == "subtree"). This prevents accidental
        # partial exploration like the Claude failure case.
        #
        # TIGHTENED (Dec 2025): Handles with status='finalized' and
        # selection_type='path' are ONLY considered decided if they actually
        # anchor engulfed leaves. Otherwise they're bogus path elements (from
        # auto-completion bugs) and should be flagged as undecided.
        uncovered_handles: list[str] = []

        for handle, meta in handles.items():
            # Skip synthetic root handle
            if handle == "R":
                continue

            st = state.get(handle, {"status": "unseen"})
            status = st.get("status")
            sel_type = st.get("selection_type")

            # Special validation: finalized/path must anchor engulfed leaves
            if status == "finalized" and sel_type == "path":
                summary = _summarize_descendants_for_handle(handle)
                if summary["accepted_leaf_count"] == 0:
                    # Bogus path element - no engulfed leaves under it.
                    # Treat as undecided so it shows in uncovered_handles.
                    status = "unseen"  # Override for completeness logic below
                else:
                    # True path element - considered decided
                    continue

            # Normal decided cases
            if status in {"finalized", "closed"}:
                continue

            # Check ancestor chain for a subtree decision that covers this node
            ancestor_handle = meta.get("parent")
            covered_by_subtree = False
            while ancestor_handle:
                anc_state = state.get(ancestor_handle, {})
                if (
                    anc_state.get("status") in {"finalized", "closed"}
                    and anc_state.get("selection_type") == "subtree"
                ):
                    covered_by_subtree = True
                    break
                ancestor_handle = handles.get(ancestor_handle, {}).get("parent")

            if not covered_by_subtree:
                uncovered_handles.append(handle)

        if uncovered_handles:
            # Build a human-readable summary of uncovered top-level and nested
            # handles to make the mistake visible and correctable.
            details_lines: list[str] = []
            for h in uncovered_handles[:50]:  # cap to avoid overwhelming output
                meta = handles.get(h, {})
                name = meta.get("name", "Untitled")
                parent_handle = meta.get("parent") or "<root>"
                details_lines.append(f"- {h} (name='{name}', parent='{parent_handle}')")

            more_note = "" if len(uncovered_handles) <= 50 else f"\n…and {len(uncovered_handles) - 50} more handles."  # noqa: E501

            # Check exploration mode for smart preserve suggestion
            exploration_mode = session.get("exploration_mode", "manual")
            mode_specific_hints = ""
            if exploration_mode == "dfs_guided":
                mode_specific_hints = (
                    "\n\n💡 NON-STRICT MODE OPTIONS:\n"
                    "\n1. For branch nodes you want to add children under:\n"
                    "     {'handle': 'A.4.2', 'action': 'engulf_shell_in_gemstorm'}\n"
                    "   (Human-friendly alias: 'reserve_branch_for_children')\n"
                    "\n2. To preserve all remaining undecided nodes in ETHER (with smart handling):\n"
                    "     {'action': 'preserve_all_remaining_nodes_in_ether_at_finalization'}\n"
                    "   This will:\n"
                    "     • Preserve all non-engulfed descendants in ETHER\n"
                    "     • Structurally include branches with engulfed descendants\n"
                    "     • Then you can finalize successfully"
                )

            raise NetworkError(
                "Incomplete exploration detected during nexus_finalize_exploration.\n\n"
                "You attempted to finalize without accounting for all branches.\n\n"
                "Handles that are neither explicitly decided (engulfed/preserved) nor covered "
                "by an engulfed/preserved subtree decision:\n"
                + "\n".join(details_lines)
                + more_note
                + "\n\nTo proceed safely, either:\n"
                "• Visit these handles and decide with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched', OR\n"
                "• Use 'engulf_shell_in_gemstorm' / 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children' on an ancestor branch to cover them."
                + mode_specific_hints
                + "\n\nOnce every handle is accounted for, nexus_finalize_exploration will succeed "
                "and the phantom gem will be a true minimal covering tree."
            )

        # Build a reverse map from node_id to handle for convenience
        handle_by_node_id: dict[str, str] = {}
        for h, meta in handles.items():
            nid = meta.get("id")
            if nid:
                handle_by_node_id[nid] = h

        needed_ids: set[str] = set()

        # Classify subtree selections into "shell" (branch-only) vs full-subtree
        subtree_shell_node_ids: set[str] = set()
        true_subtree_root_ids: set[str] = set()

        for handle, node_id, selection_type, _max_depth in finalized_entries:
            if selection_type != "subtree":
                continue
            st = state.get(handle, {})
            if st.get("subtree_mode") == "shell":
                subtree_shell_node_ids.add(node_id)
            else:
                true_subtree_root_ids.add(node_id)

        # 1) Accept full subtrees: include subtree roots and ALL descendants,
        #    EXCEPT nodes explicitly preserved in ETHER (protected).
        for node_id in true_subtree_root_ids:
            if node_id not in node_by_id:
                logger.warning(
                    f"nexus_finalize_exploration: subtree root id {node_id} not found in tree; skipping."
                )
                continue
            stack = [node_id]
            while stack:
                cur = stack.pop()
                # Skip nodes explicitly preserved in ETHER (descendant preserves
                # override full-subtree engulf decisions for this node).
                if cur in needed_ids or cur in explicitly_preserved_ids:
                    continue
                needed_ids.add(cur)
                for child_id in children_by_id.get(cur, []):
                    stack.append(child_id)

        # 2) Include branch-only shells (no descendants from this selection)
        for node_id in subtree_shell_node_ids:
            if node_id not in node_by_id:
                logger.warning(
                    f"nexus_finalize_exploration: subtree shell id {node_id} not found in tree; skipping."
                )
                continue
            needed_ids.add(node_id)

        # 3) Accept leaves: include leaf and all ancestors up to root
        accepted_leaf_ids: set[str] = set()

        for nid, _node in node_by_id.items():
            # Leaf = no children in cached tree
            if children_by_id.get(nid):
                continue
            handle_for_node = handle_by_node_id.get(nid)
            if not handle_for_node:
                continue
            st = state.get(handle_for_node, {"status": "unseen"})
            if st.get("status") == "finalized":
                accepted_leaf_ids.add(nid)

        for leaf_id in accepted_leaf_ids:
            cur = leaf_id
            while cur is not None:
                if cur in needed_ids:
                    break
                needed_ids.add(cur)
                cur = parent_by_id.get(cur)

        # No node whose final exploration status is 'finalized' (leaf, subtree,
        # or path element) should remain in explicitly_preserved_ids. The preserve
        # ledger is strictly for nodes whose final state is preserved-in-ETHER.
        if explicitly_preserved_ids:
            finalized_node_ids: set[str] = set()
            for handle_key, st in state.items():
                if st.get("status") != "finalized":
                    continue
                meta = handles.get(handle_key) or {}
                node_id_for_handle = meta.get("id")
                if node_id_for_handle:
                    finalized_node_ids.add(node_id_for_handle)
            if finalized_node_ids:
                explicitly_preserved_ids -= finalized_node_ids

        if not needed_ids:
            raise NetworkError(
                f"Exploration session '{session_id}' computed empty minimal covering tree; "
                f"no accepted leaves or subtrees."
            )

        # Assign gem roles: default path_element, override for leaves and explicit subtree selections
        role_by_node_id: dict[str, str] = {}
        for nid in needed_ids:
            role_by_node_id[nid] = "path_element"

        for nid in subtree_shell_node_ids:
            if nid in role_by_node_id:
                role_by_node_id[nid] = "subtree_selected"

        for nid in true_subtree_root_ids:
            if nid in role_by_node_id:
                role_by_node_id[nid] = "subtree_selected"

        for nid in accepted_leaf_ids:
            if nid in role_by_node_id:
                role_by_node_id[nid] = "leaf_selected"

        # Determine disjoint roots of the minimal covering forest: nodes that are
        # needed but whose parent is either None or not needed.
        root_ids: list[str] = []
        for nid in needed_ids:
            parent_id = parent_by_id.get(nid)
            if parent_id is None or parent_id not in needed_ids:
                root_ids.append(nid)

        # Stable ordering: preserve original traversal order as much as possible
        # by walking the cached tree and selecting needed roots in that order.
        # NOTE: ordered_root_ids now serves as a semantic summary of which
        # branches became top-level foci in the minimal covering tree; the
        # actual GEM tree is rooted at the exploration root.
        ordered_root_ids: list[str] = []

        def collect_roots_in_order(node: dict[str, Any]) -> None:
            nid = node.get("id")
            if nid in root_ids and nid not in ordered_root_ids:
                ordered_root_ids.append(nid)
            for child in node.get("children", []) or []:
                collect_roots_in_order(child)

        collect_roots_in_order(root_node)

        # Ensure the minimal covering set is connected to the exploration root
        # by closing needed_ids upward along ancestor chains. This guarantees
        # that the final GEM is a single tree slice under the exploration root,
        # matching the terrain-style shape used by IGNITE SHARDS.
        root_explore_id = root_node.get("id")
        if root_explore_id:
            needed_ids.add(root_explore_id)
        for nid in list(needed_ids):
            cur = parent_by_id.get(nid)
            while cur is not None:
                if cur in needed_ids:
                    break
                needed_ids.add(cur)
                cur = parent_by_id.get(cur)

        # Helper: recursively copy only needed nodes under the exploration root
        def copy_pruned_subtree(node: dict[str, Any]) -> dict[str, Any] | None:
            nid = node.get("id")
            if nid not in needed_ids:
                return None

            new_node = {k: v for k, v in node.items() if k != "children"}
            role = role_by_node_id.get(nid)
            if role:
                # Preserve explicit shell semantics for subtree-selected shells so
                # that downstream WEAVE can treat children as opaque even after
                # JEWELSTORM adds new children under this parent.
                if role == "subtree_selected" and nid in subtree_shell_node_ids:
                    new_node["subtree_mode"] = "shell"
            new_children: list[dict[str, Any]] = []
            for child in node.get("children", []) or []:
                pruned_child = copy_pruned_subtree(child)
                if pruned_child is not None:
                    new_children.append(pruned_child)
            new_node["children"] = new_children
            return new_node

        pruned_root = copy_pruned_subtree(root_node)
        if pruned_root is None:
            raise NetworkError(
                "nexus_finalize_exploration: root node was pruned; "
                "no nodes available to build phantom_gem."
            )

        # Minimal covering tree: gem_nodes holds the pruned children under the
        # exploration root (R). EXPLORATION IS SPECIAL: coarse TERRAIN, GEM, and
        # SHIMMERING TERRAIN all share this minimal covering skeleton.
        gem_nodes: list[dict[str, Any]] = pruned_root.get("children", [])

        # --- PROJECT TO ORIGINAL VIEW (for coarse TERRAIN + GEM + SHIMMERING) ---
        import copy as _copy

        coarse_nodes = _copy.deepcopy(gem_nodes)

        def _project_to_original(node: dict) -> None:
            """Use original_name/original_note if present, then drop them.

            This yields an "original" view (ETHER snapshot at exploration
            start) without any original_* fields left in the JSON.
            """
            if "original_name" in node:
                node["name"] = node.get("original_name")
            if "original_note" in node:
                node["note"] = node.get("original_note")
            node.pop("original_name", None)
            node.pop("original_note", None)
            for child in node.get("children") or []:
                _project_to_original(child)

        for n in coarse_nodes:
            _project_to_original(n)

        # --- PROJECT TO EDITED VIEW (for JEWEL) ---
        jewel_nodes = _copy.deepcopy(gem_nodes)

        def _strip_original_fields(node: dict) -> None:
            node.pop("original_name", None)
            node.pop("original_note", None)
            for child in node.get("children") or []:
                _strip_original_fields(child)

        for n in jewel_nodes:
            _strip_original_fields(n)

        # NEXUS run directory initialization for this exploration-derived tag.
        # If a run dir already exists for this tag (from nexus_scry/nexus_glimpse/
        # Cartographer), reuse the latest one. Otherwise, create a new timestamped
        # directory under temp\\nexus_runs.
        from pathlib import Path
        from datetime import datetime

        base_dir = Path(
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        try:
            existing = self._get_nexus_dir(nexus_tag)
            run_dir = Path(existing)
        except NetworkError:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            run_dir = base_dir / f"{timestamp}__{nexus_tag}"
            run_dir.mkdir(parents=True, exist_ok=False)

        phantom_path = run_dir / "phantom_gem.json"
        coarse_path = run_dir / "coarse_terrain.json"
        shimmering_path = run_dir / "shimmering_terrain.json"

        # Pack exploration scratchpad and per-handle hints into the JEWEL wrapper
        scratchpad_text = session.get("scratchpad", "")
        hints_export: list[dict[str, Any]] = []
        for handle_key, meta in handles.items():
            handle_hints = meta.get("hints") or []
            if not handle_hints:
                continue
            hints_export.append(
                {
                    "handle": handle_key,
                    "node_id": meta.get("id"),
                    "name": meta.get("name"),
                    "depth": meta.get("depth"),
                    "hints": handle_hints,
                }
            )

        export_root_id = session.get("root_id")
        export_root_name = session.get("root_name") or root_node.get("name", "Root")
        export_timestamp = None  # Exploration does not track per-node timestamps

        # Compute exploration_preview FIRST
        exploration_preview = None
        try:
            exploration_preview = self._annotate_preview_ids_and_build_tree(
                coarse_nodes,
                prefix="CT",
            )
        except Exception:
            # Preview is best-effort only; never break finalize_exploration.
            pass
        
        # --- GEM (S0) & COARSE TERRAIN (T0) & SHIMMERING TERRAIN (T1) ---
        # EXPLORATION IS SPECIAL: GEM == COARSE TERRAIN == SHIMMERING TERRAIN
        gem_wrapper = {
            "export_timestamp": export_timestamp,
            "export_root_children_status": "complete",
            "__preview_tree__": exploration_preview,
            "export_root_id": export_root_id,
            "export_root_name": export_root_name,
            "nodes": coarse_nodes,
            "original_ids_seen": sorted(original_ids_seen),
            "explicitly_preserved_ids": sorted(explicitly_preserved_ids),
        }

        try:
            with open(phantom_path, "w", encoding="utf-8") as f:
                json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to write phantom_gem.json: {e}") from e

        try:
            with open(coarse_path, "w", encoding="utf-8") as f:
                json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(
                f"Failed to write coarse_terrain.json during finalize_exploration: {e}"
            )

        try:
            with open(shimmering_path, "w", encoding="utf-8") as f:
                json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to write shimmering_terrain.json: {e}") from e

        # NOTE: JEWEL (S1) IS NO LONGER AUTO-CREATED HERE.
        # JEWELSTORM now owns JEWEL creation (phantom_jewel.json) via
        # JEWELSTRIKE → transform_jewel → JEWELMORPH. nexus_finalize_exploration
        # produces only coarse_terrain (T0) and phantom_gem (S0); shimmering_terrain
        # (T1) is a convenience mirror of T0 for EXPLORATION's special case.

        result: dict[str, Any] = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "phantom_gem": str(phantom_path),
            "shimmering_terrain": str(shimmering_path),
            "finalized_branch_count": len(ordered_root_ids),
            "node_count": len(needed_ids),
            "mode": mode,
        }

        return result
