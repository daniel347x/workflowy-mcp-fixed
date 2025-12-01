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

from datetime import datetime

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


def _log_to_file_helper(message: str, log_type: str = "reconcile") -> None:
    """Log message to a specific debug file (best-effort).

    Args:
        message: The message to log
        log_type: "reconcile" -> reconcile_debug.log
                    "etch"      -> etch_debug.log
    """
    try:
        from datetime import datetime
        
        filename = "reconcile_debug.log"
        if log_type == "etch":
            filename = "etch_debug.log"
        
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
    def _validate_note_field(note: str | None, skip_newline_check: bool = False) -> tuple[str | None, str | None]:
        """Validate and auto-escape note field for Workflowy compatibility.
        
        Handles:
        1. Angle brackets (auto-escape to HTML entities - Workflowy renderer bug workaround)
        
        REMOVED: Literal backslash-n validation (moved to MCP connector level)
        
        Args:
            note: Note content to validate/escape
            skip_newline_check: DEPRECATED - check removed, parameter kept for compatibility
            
        Returns:
            (processed_note, warning_message)
            - processed_note: Escaped/fixed note
            - warning_message: Info message if changes made
        """
        if note is None:
            return (None, None)
        
        # Check for override token (for documentation that needs literal sequences)
        OVERRIDE_TOKEN = "<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>"
        if note.startswith(OVERRIDE_TOKEN):
            # Strip token and return as-is
            return (note, None)  # Caller strips token before API call
        
        # CHECK 1: Auto-escape angle brackets (Workflowy renderer bug workaround)
        # Web interface auto-escapes < to &lt; and > to &gt;
        # API doesn't - we must do it manually
        escaped_note = note
        angle_bracket_escaped = False
        
        if '<' in note or '>' in note:
            escaped_note = note.replace('<', '&lt;').replace('>', '&gt;')
            angle_bracket_escaped = True
        
        # Return processed note with optional warning
        if angle_bracket_escaped:
            warning_msg = """✅ AUTO-ESCAPED: Angle brackets converted to HTML entities

🐛 WORKFLOWY RENDERER BUG: The API doesn't auto-escape < and > like the web interface does.
   Angle brackets cause notes to display as completely blank.

⚙️ AUTO-FIX APPLIED:
   Your < characters were converted to &lt;
   Your > characters were converted to &gt;
   
   This matches how Workflowy's web interface handles angle brackets.
   Your note will display correctly.

📖 Bug documentation: SATCHEL VYRTHEX in Deployment Documentation Validation ARC
"""
            return (escaped_note, warning_msg)
        
        return (escaped_note, None)
    
    @staticmethod
    def _validate_name_field(name: str | None) -> tuple[str | None, str | None]:
        """Validate and auto-escape name field for Workflowy compatibility.
        
        Handles:
        1. Angle brackets (auto-escape to HTML entities - Workflowy renderer bug workaround)
        
        Args:
            name: Node name to validate/escape
            
        Returns:
            (processed_name, warning_message)
            - processed_name: Escaped/fixed name
            - warning_message: Info message if changes made
        """
        if name is None:
            return (None, None)
        
        # Auto-escape angle brackets (Workflowy renderer bug workaround)
        escaped_name = name
        angle_bracket_escaped = False
        
        if '<' in name or '>' in name:
            escaped_name = name.replace('<', '&lt;').replace('>', '&gt;')
            angle_bracket_escaped = True
        
        # Return processed name with optional warning
        if angle_bracket_escaped:
            warning_msg = "✅ AUTO-ESCAPED: Angle brackets in node name converted to HTML entities"
            return (escaped_name, warning_msg)
        
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
        """Log message to a specific debug file (best-effort).

        Args:
            message: The message to log
            log_type: "reconcile" -> reconcile_debug.log
                      "etch"      -> etch_debug.log
        """
        try:
            from datetime import datetime
            
            filename = "reconcile_debug.log"
            if log_type == "etch":
                filename = "etch_debug.log"
            
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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)

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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)

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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)

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
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
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
                # Force 1s delay at START of each iteration (rate limit protection)
                await asyncio.sleep(1.0)

                try:
                    # API exports all nodes as flat list (no parameters supported)
                    response = await self.client.get("/nodes-export")
                    data = await self._handle_response(response)

                    all_nodes = data.get("nodes", []) or []
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
            api_calls_made += 1  # Track get_node API call
            flat_nodes.insert(0, root_node_data.model_dump())
            total_nodes_fetched += 1
        else:
            # OLD METHOD: Fetch entire account (100K+ nodes for Dan!)
            raw_data = await self.export_nodes(node_id)
            flat_nodes = raw_data.get("nodes", [])
            # Extract the ACTUAL count from API (before filtering)
            total_nodes_fetched = raw_data.get("_total_fetched_from_api", len(flat_nodes))
            api_calls_made = 1  # Single export_nodes call (but fetches ALL nodes!)
        
        try:
            
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
                # Extract only children (skip root for round-trip editing)
                hierarchical_tree = root_node.get('children', [])
            
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
            
            # Wrap with metadata for safe round-trip editing
            export_package = {
                "export_root_id": node_id,
                "export_root_name": root_node_info.get('name') if root_node_info else 'Unknown',
                "export_timestamp": hierarchical_tree[0].get('modifiedAt') if hierarchical_tree else None,
                "nodes": hierarchical_tree
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

        - immediate_child_count: number of direct children in the FULL tree
        - total_descendant_count: total descendants (excluding the node itself)
        - children_status:
            - "complete"                => children list is fully loaded/editable
            - "truncated_by_depth"      => depth limit applied at this level
            - "truncated_by_count"      => child_count_limit applied at this level

        For nodes whose children_status != "complete", the reconciliation
        algorithm treats the children as an opaque subtree: it will not perform
        per-child deletes/reorders, but parent moves/deletes and new children
        remain safe.

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

            node['immediate_child_count'] = immediate_count
            node['total_descendant_count'] = total_desc_for_node

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
    
    async def workflowy_glimpse(self, node_id: str, use_efficient_traversal: bool = False, _ws_connection=None, _ws_queue=None) -> dict[str, Any]:
        """Load entire node tree into context (no file intermediary).
        
        GLIMPSE command - direct context loading for agent analysis.
        
        Can use WebSocket connection to Chrome extension for DOM extraction (bypasses API).
        Falls back to API fetch if WebSocket not available.
        
        Args:
            node_id: Root node UUID to read from
            _ws_connection: WebSocket connection from server.py (internal parameter)
            
        Returns:
            {
                "success": True,
                "root": {"id": "...", "name": "...", "note": "..."},  # Root node metadata
                "children": [...],  # Children only (for round-trip editing)
                "node_count": N,
                "depth": M,
                "_source": "websocket" | "api"  # How data was obtained
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
                    
                    # Log to persistent file
                    _log_glimpse_to_file("glimpse", node_id, response)
                    
                    return response
                else:
                    logger.warning("  WebSocket response invalid")
                    raise NetworkError(
                        "WebSocket GLIMPSE failed - invalid response structure.\n\n"
                        "Options:\n"
                        "1. Verify Workflowy desktop app is open and connected\n"
                        "2. Check extension console for errors\n"
                        "3. Use workflowy_glimpse_full() to fetch complete tree via API\n\n"
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
                    "3. Use workflowy_glimpse_full() to fetch complete tree via API\n\n"
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
                    "4. Use workflowy_glimpse_full() to fetch complete tree via API\n\n"
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
            "4. Use workflowy_glimpse_full() to fetch complete tree via API\n\n"
            "Mode 1 (Dan shows you): Requires WebSocket - use glimpse()\n"
            "Mode 2 (Agent hunts): Bypass WebSocket - use glimpseFull()"
        )
    
    async def workflowy_glimpse_full(self, node_id: str, use_efficient_traversal: bool = False, depth: int | None = None, size_limit: int = 1000) -> dict[str, Any]:
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
            
        Returns:
            Same format as workflowy_glimpse with _source="api"
        """
        import logging
        logger = _ClientLogger()
        
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

            # LOGGING: inspect root candidates from hierarchy for debugging
            try:
                self._log_debug(f"workflowy_glimpse_full: node_id={node_id} use_efficient_traversal={use_efficient_traversal} flat_nodes={len(flat_nodes)} roots={len(hierarchical_tree)}")
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
                self._log_debug(f"workflowy_glimpse_full: using single-root path id={root_node.get('id')} name={root_node.get('name')}")
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
                    self._log_debug(f"workflowy_glimpse_full: multiple roots ({len(hierarchical_tree)}), but found target root id={target_root.get('id')} name={target_root.get('name')}. Using it.")
                    root_metadata = {
                        "id": target_root.get('id'),
                        "name": target_root.get('name'),
                        "note": target_root.get('note'),
                        "parent_id": target_root.get('parent_id')
                    }
                    children = target_root.get('children', [])
                else:
                    # Fallback: Return all roots as children (artificial root behavior)
                    self._log_debug(f"workflowy_glimpse_full: multiple roots ({len(hierarchical_tree)}) and target {node_id} NOT found in top level; returning list directly")
                    children = hierarchical_tree
            
            # Apply depth limiting if requested
            if depth is not None:
                children = self._limit_depth(children, depth)
            
            # Calculate max depth
            max_depth = self._calculate_max_depth(children)
            
            result = {
                "success": True,
                "root": root_metadata,
                "children": children,
                "node_count": len(flat_nodes),
                "depth": max_depth,
                "_source": "api"  # Indicate data came from API (not WebSocket)
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
                        fixed = nodes.replace(r'\\\"', '"')
                        nodes = json.loads(fixed)
                        stringify_strategy_used = "Strategy 3: Triple-backslash replacement (CASE 2 style)"
                        logger.info(f"✅ {stringify_strategy_used}")
                    except json.JSONDecodeError as e:
                        return {
                            "success": False,
                            "nodes_created": 0,
                            "root_node_ids": [],
                            "api_calls": 0,
                            "retries": 0,
                            "rate_limit_hits": 0,
                            "errors": [
                                f"Parameter 'nodes' is a string but could not parse with any strategy.",
                                f"Tried: (1) direct parse, (2) unicode_escape decode, (3) triple-backslash replacement",
                                f"Final error: {str(e)}",
                                f"Hint: Use actual list structure, not stringified JSON"
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
                # Force 1s delay at START of each iteration (rate limit protection)
                await asyncio.sleep(1.0)
                
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
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Failed to update weave journal {weave_journal_path}: {e}")
            
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
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
            except Exception:
                # Journal updates must never break error reporting
                pass
            
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

    def _annotate_glimpse_children_status(self, nodes: list[dict[str, Any]]) -> None:
        """Annotate children_status for GLIMPSE-extracted nodes (recursive).
        
        Sets children_status based on has_hidden_children metadata from WebSocket:
        - has_hidden_children=True → 'truncated_by_expansion' (SAFE: don't delete hidden)
        - has_hidden_children=False → 'complete' (SAFE: can reconcile children)
        
        This prevents accidental deletion of collapsed branches during NEXUS WEAVE.
        
        DEPRECATED: This method is replaced by the WebSocket+API merge logic in nexus_glimpse.
        Kept for backwards compatibility with old code paths.
        """
        for node in nodes:
            has_hidden = node.get('has_hidden_children', False)
            
            if has_hidden:
                node['children_status'] = 'truncated_by_expansion'
            else:
                node['children_status'] = 'complete'
            
            # Recursively annotate grandchildren
            grandchildren = node.get('children', [])
            if grandchildren:
                self._annotate_glimpse_children_status(grandchildren)
    
    def _mark_all_nodes_as_potentially_truncated(self, nodes: list[dict[str, Any]]) -> None:
        """Mark ALL nodes as potentially having hidden children (UNSAFE fallback).
        
        Used when API fetch fails and we can't verify true child counts.
        This makes WEAVE operations CONSERVATIVE - they won't delete anything
        that might be a collapsed branch.
        
        Args:
            nodes: Node tree to mark (modified in-place)
        """
        for node in nodes:
            # Conservative: assume hidden children exist
            node["has_hidden_children"] = True
            node["children_status"] = "truncated_by_expansion"
            
            # Recursively mark grandchildren
            grandchildren = node.get("children", []) or []
            if grandchildren:
                self._mark_all_nodes_as_potentially_truncated(grandchildren)
    
    def _get_nexus_dir(self, nexus_tag: str) -> str:
        """Resolve base directory for a CORINTHIAN NEXUS run and ensure it exists.

        This keeps all intermediate JSON under a single tree per nexus_tag, e.g.:
        E:\\...\\TODO\\temp\\nexus_runs\\<nexus_tag>\\coarse_terrain.json
        """
        base_dir = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
        run_dir = os.path.join(base_dir, nexus_tag)
        os.makedirs(run_dir, exist_ok=True)
        return run_dir

    async def nexus_summon(
        self,
        nexus_tag: str,
        workflowy_root_id: str,
        max_depth: int,
        child_limit: int,
    ) -> dict[str, Any]:
        """SCRY → coarse_terrain.json for a new CORINTHIAN NEXUS.

        This is the initiating stage of the PHANTOM GEMSTONE pipeline. It:
        - Exports a hierarchical SCRY of the Workflowy subtree rooted at
          ``workflowy_root_id`` using bulk_export_to_file, with the given
          ``max_depth`` and ``child_limit`` parameters.
        - Writes the result to ``coarse_terrain.json`` under the directory for
          ``nexus_tag``.

        The resulting JSON is the coarse TERRAIN (T0) used by later stages:
        - nexus_ignite_shards (IGNITE SHARDS → phantom_gem)
        - nexus_anchor_gems   (ANCHOR GEMS → shimmering_terrain)
        - nexus_anchor_jewels (ANCHOR JEWELS → enchanted_terrain)
        - nexus_weave_enchanted (WEAVE → Workflowy)
        """
        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")

        # Use the existing bulk_export_to_file helper to perform the SCRY.
        # We deliberately use use_efficient_traversal=False so that
        # max_depth/child_limit semantics are handled by the NEXUS export
        # pipeline (annotate_child_counts_and_truncate).
        result = await self.bulk_export_to_file(
            node_id=workflowy_root_id,
            output_file=coarse_path,
            include_metadata=True,
            use_efficient_traversal=False,
            max_depth=max_depth,
            child_count_limit=child_limit,
        )

        # Optionally, record a tiny manifest stub for this NEXUS run. We keep
        # it minimal here; richer tracking can be layered on later.
        manifest_path = os.path.join(run_dir, "nexus_manifest.json")
        try:
            from datetime import datetime

            manifest = {
                "nexus_tag": nexus_tag,
                "workflowy_root_id": workflowy_root_id,
                "max_depth": max_depth,
                "child_limit": child_limit,
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
            "coarse_terrain": coarse_path,
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
                "Call nexus_summon(...) before nexus_ignite_shards(...)."
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
                "Re-summon the NEXUS via nexus_summon(...) if this is not the case."
            )

        terrain_nodes: list[dict[str, Any]] = coarse_data.get("nodes", [])

        # Build parent map from hierarchical nodes (id -> parent_id).
        parent_by_id: dict[str, str | None] = {}

        def _index_parents(nodes: list[dict[str, Any]], parent_id: str | None) -> None:
            for node in nodes:
                nid = node.get("id")
                if nid:
                    parent_by_id[nid] = parent_id
                    children = node.get("children") or []
                    if children:
                        _index_parents(children, nid)

        _index_parents(terrain_nodes, None)

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
                "Choose roots from the current coarse SCRY (nexus_summon)."
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

        gem_nodes: list[dict[str, Any]] = []
        roots_resolved: list[str] = []
        total_nodes_fetched = 0

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

            gem_nodes.append(root_subtree)
            roots_resolved.append(root_id)

        phantom_payload = {
            "nexus_tag": nexus_tag,
            "roots": roots_resolved,
            "nodes": gem_nodes,
        }

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

        Ultimate usability: instead of nexus_summon + nexus_ignite_shards (both via API),
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

        # Validate mode parameter
        if mode not in ("full", "coarse_terrain_only"):
            raise NetworkError(
                f"Invalid mode '{mode}'. Must be 'full' or 'coarse_terrain_only'."
            )

        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_gem_path = os.path.join(run_dir, "phantom_gem.json")
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")

        if os.path.exists(run_dir):
            if not reset_if_exists:
                raise NetworkError(
                    f"NEXUS state already exists for tag '{nexus_tag}'. "
                    "Use reset_if_exists=True to overwrite."
                )
            try:
                shutil.rmtree(run_dir)
            except Exception as e:
                raise NetworkError(f"Failed to reset NEXUS state for tag '{nexus_tag}': {e}") from e

        try:
            os.makedirs(run_dir, exist_ok=True)
        except Exception as e:
            raise NetworkError(f"Failed to create NEXUS directory for tag '{nexus_tag}': {e}") from e

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
            api_glimpse = await self.workflowy_glimpse_full(
                node_id=workflowy_root_id,
                use_efficient_traversal=False,
                depth=None,  # Full depth
                size_limit=50000  # High limit - we need complete structure
            )
        except Exception as e:
            logger.warning(f"⚠️ API fetch failed: {e} - falling back to WebSocket-only (UNSAFE for WEAVE!)")
            # Fallback: use WebSocket-only result but mark ALL nodes as potentially having hidden children
            children = ws_glimpse.get("children", [])
            self._mark_all_nodes_as_potentially_truncated(children)
            api_glimpse = None
        
        # STEP 3: MERGE - Cross-reference to detect hidden children
        logger.info(f"🔀 NEXUS GLIMPSE Step 3: Merging WebSocket + API results to detect hidden children...")
        
        if api_glimpse and api_glimpse.get("success"):
            # Build lookup: WebSocket node ID → API node (for comparing child counts)
            api_children = api_glimpse.get("children", [])
            api_nodes_by_id: dict[str, dict[str, Any]] = {}
            
            def index_api_nodes(nodes: list[dict[str, Any]]) -> None:
                """Build flat lookup of all API nodes by ID."""
                for node in nodes:
                    nid = node.get("id")
                    if nid:
                        api_nodes_by_id[nid] = node
                    grandchildren = node.get("children", []) or []
                    if grandchildren:
                        index_api_nodes(grandchildren)
            
            index_api_nodes(api_children)
            logger.info(f"   API returned {len(api_nodes_by_id)} total nodes for comparison")
            
            # Merge: For each WebSocket node, check if API shows more children
            ws_children = ws_glimpse.get("children", [])
            nodes_with_hidden = 0
            
            def merge_hidden_children_flags(ws_nodes: list[dict[str, Any]]) -> None:
                """Recursively annotate has_hidden_children based on API comparison."""
                nonlocal nodes_with_hidden
                
                for ws_node in ws_nodes:
                    nid = ws_node.get("id")
                    if not nid:
                        # New node (no UUID) - can't have hidden children
                        ws_node["has_hidden_children"] = False
                        ws_node["children_status"] = "complete"
                        continue
                    
                    # Find corresponding API node
                    api_node = api_nodes_by_id.get(nid)
                    
                    if not api_node:
                        # Node not in API result - treat as complete (edge case)
                        ws_node["has_hidden_children"] = False
                        ws_node["children_status"] = "complete"
                        continue
                    
                    # Compare child counts
                    ws_children_count = len(ws_node.get("children", []) or [])
                    api_children_count = len(api_node.get("children", []) or [])
                    
                    if api_children_count > ws_children_count:
                        # API shows more children - WebSocket has hidden/collapsed children!
                        ws_node["has_hidden_children"] = True
                        ws_node["children_status"] = "truncated_by_expansion"
                        nodes_with_hidden += 1
                        logger.info(f"   ⚠️ Node '{ws_node.get('name')}' ({nid[:8]}...): WebSocket={ws_children_count} children, API={api_children_count} children → HIDDEN CHILDREN DETECTED")
                    else:
                        # Counts match - children are fully visible
                        ws_node["has_hidden_children"] = False
                        ws_node["children_status"] = "complete"
                    
                    # Recursively merge grandchildren
                    ws_grandchildren = ws_node.get("children", []) or []
                    if ws_grandchildren:
                        merge_hidden_children_flags(ws_grandchildren)
            
            merge_hidden_children_flags(ws_children)
            logger.info(f"   ✅ Merge complete: {nodes_with_hidden} nodes have hidden children (SAFE for WEAVE)")
            
            children = ws_children  # Use merged WebSocket result with safety flags
        else:
            # API fetch failed - use WebSocket-only but mark as UNSAFE
            logger.warning("⚠️ Using WebSocket-only result - ALL nodes marked as potentially truncated (UNSAFE for WEAVE unless manually verified)")
            children = ws_glimpse.get("children", [])
            self._mark_all_nodes_as_potentially_truncated(children)

        # Mirror bulk_export_to_file structure: metadata wrapper + children only
        # Root info goes in metadata, NOT in nodes array (prevents root duplication)
        terrain_data = {
            "export_root_id": workflowy_root_id,
            "export_root_name": ws_glimpse["root"]["name"],
            "export_timestamp": None,  # GLIMPSE doesn't have timestamp
            "nodes": children,  # Children with merged WebSocket+API safety flags
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
            "coarse_terrain": coarse_path,
            "node_count": ws_glimpse.get("node_count", 0),
            "depth": ws_glimpse.get("depth", 0),
            "_source": "glimpse_merged",  # Indicate WebSocket+API merge was performed
            "_api_merge_performed": api_glimpse is not None,
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

            result["phantom_gem"] = phantom_gem_path
            result["shimmering_terrain"] = shimmering_path

        # mode="coarse_terrain_only": only coarse_terrain.json written
        # User will call nexus_ignite_shards later to create phantom_gem.json
        
        # Log to persistent file (use ws_glimpse which has the merged tree data)
        _log_glimpse_to_file("glimpse", workflowy_root_id, ws_glimpse)

        return result

    async def nexus_glimpse_full(
        self,
        nexus_tag: str,
        workflowy_root_id: str,
        reset_if_exists: bool = False,
        mode: str = "full",
        max_depth: int | None = None,
        child_limit: int | None = None,
        max_nodes: int = 200000,
    ) -> dict[str, Any]:
        """GLIMPSE FULL → TERRAIN + PHANTOM GEM (API-based, ignores UI expansion).

        API-based cousin of nexus_glimpse. Fetches the complete subtree via Workflowy API,
        regardless of what's expanded in the UI. Unlike nexus_summon, this REQUIRES a
        full-depth tree with no truncation.

        Use when:
        • Tree is too large for Dan to manually expand
        • You want the complete subtree for understanding/context
        • Agent-driven workflows where Dan isn't interacting with Workflowy

        Args:
            nexus_tag: Human-readable tag for this NEXUS run.
            workflowy_root_id: Root node UUID to fetch.
            reset_if_exists: If True, overwrite existing NEXUS state for this tag.
            mode: Output mode control:
                "full" (default) - Write T0 + S0 + T1 (all identical). Skip directly to QUILLSTORM.
                "coarse_terrain_only" - Write only T0. Use for hybrid workflow: GLIMPSE for map,
                                        then nexus_ignite_shards on specific roots later.
            max_depth: Optional depth limit (errors if tree exceeds this).
            child_limit: Optional child count limit per node (errors if any node exceeds this).
            max_nodes: Maximum total node count (default 200000). Errors if tree is larger.

        Returns:
            mode="full":
                {"success": True, "nexus_tag": str, "coarse_terrain": path, "phantom_gem": path,
                 "shimmering_terrain": path, "node_count": int, "depth": int, "_source": "api"}
            mode="coarse_terrain_only":
                {"success": True, "nexus_tag": str, "coarse_terrain": path,
                 "node_count": int, "depth": int, "_source": "api"}

        Raises:
            NetworkError: If tree would be truncated (exceeds max_depth, child_limit, or max_nodes)
        """
        import shutil

        # Validate mode parameter
        if mode not in ("full", "coarse_terrain_only"):
            raise NetworkError(
                f"Invalid mode '{mode}'. Must be 'full' or 'coarse_terrain_only'."
            )

        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_gem_path = os.path.join(run_dir, "phantom_gem.json")
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")

        if os.path.exists(run_dir):
            if not reset_if_exists:
                raise NetworkError(
                    f"NEXUS state already exists for tag '{nexus_tag}'. "
                    "Use reset_if_exists=True to overwrite."
                )
            try:
                shutil.rmtree(run_dir)
            except Exception as e:
                raise NetworkError(f"Failed to reset NEXUS state for tag '{nexus_tag}': {e}") from e

        try:
            os.makedirs(run_dir, exist_ok=True)
        except Exception as e:
            raise NetworkError(f"Failed to create NEXUS directory for tag '{nexus_tag}': {e}") from e

        # Fetch full tree via API
        try:
            glimpse_result = await self.workflowy_glimpse_full(
                node_id=workflowy_root_id,
                use_efficient_traversal=False,
                depth=None,  # Always fetch full depth
                size_limit=max_nodes,
            )
        except Exception as e:
            raise NetworkError(f"GLIMPSE FULL failed for root {workflowy_root_id}: {e}") from e

        if not glimpse_result.get("success"):
            raise NetworkError(
                f"GLIMPSE FULL returned failure for root {workflowy_root_id}: "
                f"{glimpse_result.get('error', 'unknown error')}"
            )

        # Extract metadata and children
        root_meta = glimpse_result.get("root") or {
            "id": workflowy_root_id,
            "name": "Root",
            "note": None,
        }
        children = glimpse_result.get("children", []) or []
        node_count = glimpse_result.get("node_count", len(children) + 1)
        tree_depth = glimpse_result.get("depth", 0)

        # HARD REQUIREMENT: No truncation allowed for nexus_glimpse_full
        # Check if limits would have caused truncation
        if max_depth is not None and tree_depth > max_depth:
            raise NetworkError(
                f"nexus_glimpse_full requires full-depth tree. "
                f"Tree depth ({tree_depth}) exceeds max_depth ({max_depth}). "
                f"Either increase max_depth or use nexus_summon for truncated trees."
            )

        if child_limit is not None:
            # Check if any node in the tree exceeds child_limit
            def check_child_limit(nodes: list[dict[str, Any]]) -> tuple[bool, str, int]:
                for node in nodes:
                    node_children = node.get("children", []) or []
                    if len(node_children) > child_limit:
                        return True, node.get("name", "Unnamed"), len(node_children)
                    exceeded, name, count = check_child_limit(node_children)
                    if exceeded:
                        return exceeded, name, count
                return False, "", 0

            exceeded, node_name, actual_count = check_child_limit(children)
            if exceeded:
                raise NetworkError(
                    f"nexus_glimpse_full requires full-depth tree. "
                    f"Node '{node_name}' has {actual_count} children, exceeding child_limit ({child_limit}). "
                    f"Either increase child_limit or use nexus_summon for truncated trees."
                )

        # Build terrain data structure
        terrain_data = {
            "export_root_id": workflowy_root_id,
            "export_root_name": root_meta.get("name", "Root"),
            "export_timestamp": None,
            "nodes": children,
        }

        # Annotate children_status as 'complete' (API fetch is always complete)
        def annotate_complete(nodes: list[dict[str, Any]]) -> None:
            for node in nodes:
                node["children_status"] = "complete"
                node["has_hidden_children"] = False
                grandchildren = node.get("children", []) or []
                if grandchildren:
                    annotate_complete(grandchildren)

        annotate_complete(children)

        # Always write coarse_terrain.json
        try:
            with open(coarse_path, "w", encoding="utf-8") as f:
                json.dump(terrain_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise NetworkError(f"Failed to write coarse_terrain.json: {e}") from e

        result = {
            "success": True,
            "nexus_tag": nexus_tag,
            "coarse_terrain": coarse_path,
            "node_count": node_count,
            "depth": tree_depth,
            "_source": "api",
            "mode": mode,
        }

        if mode == "full":
            # GLIMPSE FULL path: T0 = S0 = T1 (all identical)
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

            result["phantom_gem"] = phantom_gem_path
            result["shimmering_terrain"] = shimmering_path

        # mode="coarse_terrain_only": only coarse_terrain.json written
        # User will call nexus_ignite_shards later to create phantom_gem.json
        
        # Log to persistent file (use glimpse_result which has the tree data)
        _log_glimpse_to_file("glimpse_full", workflowy_root_id, glimpse_result)

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
                "Call nexus_summon(...) before nexus_anchor_gems(...)."
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

        # Build a lookup from phantom root id → subtree
        subtree_by_id: dict[str, dict[str, Any]] = {}
        for subtree in phantom_nodes:
            rid = subtree.get("id")
            if rid:
                subtree_by_id[rid] = subtree

        # Coarse terrain is an export package with metadata and "nodes" array.
        # We only modify the editable "nodes" list, leaving header untouched.
        if not (isinstance(coarse_data, dict) and "nodes" in coarse_data):
            raise NetworkError(
                "coarse_terrain.json must be an export package with 'nodes' key. "
                "Re-summon the NEXUS via nexus_summon(...) if this is not the case."
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

        # Write shimmering terrain out; header from coarse_terrain is preserved.
        coarse_data["nodes"] = terrain_nodes
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

    def _compute_exploration_frontier(
        self,
        session: dict[str, Any],
        frontier_size: int,
        max_depth_per_frontier: int,
    ) -> list[dict[str, Any]]:
        """Compute the next frontier for an exploration session.

        This is a simple handle-based frontier computation:
        - Candidate parents are handles with status in {open}.
        - For each candidate parent, we surface its direct children (one level
          down) that are not closed or finalized, in original Workflowy order.
        - We stop once frontier_size entries have been collected.

        max_depth_per_frontier is reserved for future use (e.g. allowing
        multi-level expansions in a single step). For v1 we always surface
        only direct children of the candidate parents.

        Leaf-first guidance (v2 enhancement): frontier entries now include
        "is_leaf" and a short "guidance" string to steer agents toward
        making decisions at leaves (accept_leaf / reject_leaf) rather than
        prematurely rejecting whole branches.
        """
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}

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

        # DFS-GUIDED / DFS-FULL-WALK MODES: Present exactly one undecided handle
        # in a canonical depth-first order. This gently nudges agents (and in
        # strict mode, mechanically forces them) to advance through the tree one
        # node at a time, making explicit decisions, instead of skipping large
        # unexplored branches.
        if exploration_mode in {"dfs_guided", "dfs_full_walk"}:

            def _is_covered_or_decided(h: str) -> bool:
                """Return True if this handle is already accounted for in DFS.

                A handle is accounted for if:
                - Its own status is finalized/closed, OR
                - It is an *opened* branch (status == "open" and has children),
                  in which case DFS should descend to its children rather than
                  revisiting the branch handle itself, OR
                - It is under an ancestor whose selection_type == "subtree" and
                  that ancestor is finalized/closed.
                """
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")

                # Explicitly decided handles are always covered
                if status in {"finalized", "closed"}:
                    return True

                # In dfs_guided mode, once a branch has been opened we skip the
                # branch handle itself and walk its children instead.
                if status == "open" and (handles.get(h, {}) or {}).get("children"):
                    return True

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

            dfs_order: list[str] = []

            def _visit(handle: str) -> None:
                # Skip synthetic root handle as a frontier entry
                if handle != "R":
                    dfs_order.append(handle)
                for ch in handles.get(handle, {}).get("children", []) or []:
                    _visit(ch)

            _visit("R")

            next_handle: str | None = None
            for h in dfs_order:
                if not _is_covered_or_decided(h):
                    next_handle = h
                    break

            if next_handle is None:
                # Everything is decided or covered; no further frontier.
                return frontier

            meta = handles.get(next_handle, {}) or {}
            grandchild_handles = meta.get("children", []) or []

            # Build children_hint for context when the node has children.
            children_hint: list[str] = []
            MAX_CHILDREN_HINT = 10
            if grandchild_handles:
                for ch in grandchild_handles[:MAX_CHILDREN_HINT]:
                    ch_meta = handles.get(ch, {}) or {}
                    ch_name = ch_meta.get("name")
                    if ch_name:
                        children_hint.append(ch_name)

            is_leaf = len(grandchild_handles) == 0
            if is_leaf:
                guidance = (
                    "Leaf node. Prefer making decisions here: use "
                    "'accept_leaf' or 'reject_leaf'. Rejecting an entire branch "
                    "is appropriate only when you are sure all of its contents "
                    "are irrelevant."
                )
            else:
                guidance = (
                    "Branch node. dfs-guided mode will walk you through its "
                    "descendants; use 'accept_subtree' or 'reject_subtree' only "
                    "when you intentionally want to include or exclude this "
                    "whole branch without inspecting each leaf."
                )
            local_hints = meta.get("hints") or []
            hints_from_ancestors = _collect_hints_from_ancestors(next_handle)

            frontier.append(
                {
                    "handle": next_handle,
                    "path": next_handle,
                    "parent_handle": meta.get("parent"),
                    "name_preview": meta.get("name", ""),
                    "child_count": len(grandchild_handles),
                    "children_hint": children_hint,
                    "depth": meta.get("depth", 0),
                    "status": state.get(next_handle, {}).get("status", "candidate"),
                    "is_leaf": is_leaf,
                    "guidance": guidance,
                    "hints": local_hints,
                    "hints_from_ancestors": hints_from_ancestors,
                }
            )

            return frontier

        # Candidate parents: any handle that is currently open.
        # Candidate handles themselves are frontier entries, not parents, until
        # the agent explicitly opens them.
        candidate_parents = [
            h for h, st in state.items() if st.get("status") == "open"
        ]

        # Limit on how many immediate child names we surface per entry to keep
        # the frontier compact while still providing strong guidance.
        MAX_CHILDREN_HINT = 10

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
                        guidance = (
                            "Leaf node. Prefer making decisions here: use "
                            "'accept_leaf' or 'reject_leaf'. Backtrack or close "
                            "an entire branch only after you've inspected its leaves."
                        )
                    else:
                        guidance = (
                            "Branch node. Consider 'open' to explore children; "
                            "use 'accept_subtree' sparingly. Exploring down to "
                            "leaves yields smaller, more targeted phantom gems."
                        )
                    local_hints = child_meta.get("hints") or []
                    hints_from_ancestors = _collect_hints_from_ancestors(child_handle)

                    frontier.append(
                        {
                            "handle": child_handle,
                            "path": child_handle,
                            "parent_handle": parent_handle,
                            "name_preview": child_meta.get("name", ""),
                            "child_count": len(grandchild_handles),
                            "children_hint": children_hint,
                            "depth": child_meta.get("depth", 0),
                            "status": child_state.get("status", "candidate"),
                            "is_leaf": is_leaf,
                            "guidance": guidance,
                            "hints": local_hints,
                            "hints_from_ancestors": hints_from_ancestors,
                        }
                    )

                    made_progress = True
                    break  # Move to next parent in round-robin

                if len(frontier) >= frontier_size:
                    break

            if not made_progress:
                # No parent was able to contribute a new frontier entry.
                break

        return frontier

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
    ) -> dict[str, Any]:
        """Initialize an exploration session over a Workflowy subtree.

        v1 implementation uses workflowy_glimpse_full regardless of source_mode
        (summon/glimpse_full/existing are treated identically for now). The
        GLIMPSE result is cached in a JSON session file along with handle and
        state metadata, and an initial frontier is returned.
        """
        import logging
        import uuid
        from datetime import datetime

        logger = _ClientLogger()

        # Determine exploration mode from session_hint.
        # DEFAULT: dfs_full_walk (strict full leaf walk, DO OR DIE).
        # Optional override: explicitly request dfs_guided for softer behavior.
        exploration_mode = "dfs_full_walk"
        if session_hint:
            hint = session_hint.strip().lower()
            if "dfs_guided" in hint or "non_strict" in hint:
                exploration_mode = "dfs_guided"

        # Fetch subtree once via GLIMPSE FULL (Agent hunts mode)
        glimpse = await self.workflowy_glimpse_full(
            node_id=root_id,
            use_efficient_traversal=True,
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

        session_id = f"{nexus_tag}-{uuid.uuid4().hex[:8]}"
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
            "handles": handles,
            "state": state,
            "scratchpad": "",
            "root_node": root_node,
            "steps": 0,
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

        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "root_handle": "R",
            "root_summary": root_summary,
            "frontier": frontier,
            "scratchpad": session.get("scratchpad", ""),
            "stats": {
                "total_nodes_indexed": glimpse.get("node_count", 0),
                "truncated": False,
            },
        }

    async def nexus_explore_step(
        self,
        session_id: str,
        actions: list[dict[str, Any]] | None = None,
        frontier_size: int = 5,
        max_depth_per_frontier: int = 1,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Apply exploration actions and return the next frontier.

        This is the agent's primary loop for exploration. Each call can:
        - mark one or more handles as open / close / finalize / reopen, and
        - in leaf-first mode, use accept_leaf / reject_leaf / accept_subtree /
          reject_subtree / backtrack / reopen_branch to drive selection.
        - request a new frontier of up to frontier_size entries.
        """
        import logging
        import json as json_module
        from datetime import datetime

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
        peek_results: list[dict[str, Any]] = []
        for action in actions:
            act = action.get("action")
            # Scratchpad actions do not require a handle and can be applied at any time.
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

            if handle not in handles:
                raise NetworkError(f"Unknown handle in actions: '{handle}'")

            if act in {"update_node_and_flag_for_acceptance", "update_note_and_flag_for_acceptance", "update_tag_and_flag_for_acceptance"} and not editable_mode:
                raise NetworkError(
                    "This exploration session was started with editable=False; "
                    "update_node_and_flag_for_acceptance/update_note_and_flag_for_acceptance/update_tag_and_flag_for_acceptance "
                    "are only allowed when editable=True. Start a new session with "
                    "editable=True if you want to change names/notes/tags as you explore."
                )

            # In dfs_guided / dfs_full_walk modes, enforce that most actions
            # apply only to the current DFS focus handle. This keeps navigation
            # deterministic and brain-dead simple for agents: one node at a
            # time.
            if exploration_mode in {"dfs_guided", "dfs_full_walk"} and act not in {"reopen_branch", "add_hint", "peek_descendants"}:
                # Recompute current DFS frontier based on the *current* state
                # before applying this action.
                session["handles"] = handles
                session["state"] = state
                current_frontier = self._compute_exploration_frontier(
                    session,
                    frontier_size=1,
                    max_depth_per_frontier=max_depth_per_frontier,
                )
                current_handle = current_frontier[0]["handle"] if current_frontier else None

                # Allow branch-wide corrections from anywhere via reopen_branch,
                # but require all other decisions to target the current handle.
                if act not in {"reopen_branch", "add_hint"} and current_handle and handle != current_handle:
                    meta = handles.get(current_handle, {}) or {}
                    current_name = meta.get("name", "Untitled")
                    raise NetworkError(
                        "DFS-guided exploration is currently focused on handle "
                        f"'{current_handle}' ({current_name!r}).\n\n"
                        f"You attempted to apply '{act}' to handle '{handle}', which "
                        "would violate the depth-first traversal order.\n\n"
                        "To proceed safely, either:\n"
                        "• Make a decision about the current handle using one of:\n"
                        "  - 'accept_leaf' / 'reject_leaf' (for leaves)\n"
                        "  - 'accept_subtree' / 'reject_subtree' (for whole branches),\n"
                        "  - 'backtrack' (close this branch without including it), or\n"
                        "• Use 'reopen_branch' on a previously decided branch that you\n"
                        "  intentionally want to revisit.\n\n"
                        "This keeps NEXUS exploration deterministic and ensures minimal,\n"
                        "reproducible phantom gems across conversations."
                    )

            if handle not in state:
                state[handle] = {"status": "unseen", "max_depth": None, "selection_type": None}

            entry = state[handle]
            # Ensure selection_type key is always present for downstream logic
            if "selection_type" not in entry:
                entry["selection_type"] = None

            if act == "peek_descendants":
                # Read-only peek at descendants of a handle, with a configurable
                # node cap to keep responses compact. Does not alter state.
                max_nodes = action.get("max_nodes") or 200
                try:
                    max_nodes_int = int(max_nodes)
                except (TypeError, ValueError):
                    max_nodes_int = 200
                if max_nodes_int <= 0:
                    max_nodes_int = 200

                queue: list[str] = [handle]
                visited = 0
                nodes_preview: list[dict[str, Any]] = []

                while queue and visited < max_nodes_int:
                    h = queue.pop(0)
                    meta = handles.get(h, {}) or {}
                    st = state.get(h, {"status": "unseen"})
                    child_handles = meta.get("children", []) or []
                    is_leaf = not child_handles

                    nodes_preview.append(
                        {
                            "handle": h,
                            "name": meta.get("name", "Untitled"),
                            "depth": meta.get("depth", 0),
                            "child_count": len(child_handles),
                            "is_leaf": is_leaf,
                            "status": st.get("status", "unseen"),
                        }
                    )
                    visited += 1

                    for ch in child_handles:
                        queue.append(ch)

                truncated = bool(queue)
                peek_results.append(
                    {
                        "root_handle": handle,
                        "max_nodes": max_nodes_int,
                        "nodes_returned": len(nodes_preview),
                        "truncated": truncated,
                        "nodes": nodes_preview,
                    }
                )
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

            if act == "open":
                entry["status"] = "open"
            elif act == "close":
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
            elif act == "finalize":
                # Backwards-compatible: plain finalize means subtree selection
                entry["status"] = "finalized"
                entry["max_depth"] = max_depth
                if entry.get("selection_type") is None:
                    entry["selection_type"] = "subtree"
            elif act == "reopen":
                entry["status"] = "open"
                entry["selection_type"] = None
                entry["max_depth"] = None

            # Leaf-first enhancements
            elif act == "accept_leaf":
                entry["status"] = "finalized"
                entry["selection_type"] = "leaf"
                entry["max_depth"] = max_depth
                _auto_complete_ancestors_from_leaf(handle)
            elif act == "reject_leaf":
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
                _auto_complete_ancestors_from_leaf(handle)
            elif act == "replace_leaf_node_with_appended_scratch_note":
                # Capture good wording from a leaf node into the scratchpad while
                # excluding the node from the minimal phantom gem. This behaves
                # like a reject_leaf decision for gem construction, but preserves
                # the leaf's name/note in the exploration scratchpad for later use.
                child_handles = handles.get(handle, {}).get("children", []) or []
                if child_handles:
                    raise NetworkError(
                        "replace_leaf_node_with_appended_scratch_note is only valid on leaf "
                        "handles (no children in cached tree)."
                    )
                meta = handles.get(handle) or {}
                name = meta.get("name", "Untitled")
                node_id_for_scratch = meta.get("id")
                note_preview = meta.get("note") or ""

                # Build a compact, human-readable scratch entry
                header_parts = [f"[DROPPED_LEAF] {handle}", f'"{name}"']
                if node_id_for_scratch:
                    header_parts.append(f"id={node_id_for_scratch}")
                header_line = " ".join(header_parts)

                scratch_lines = [header_line]
                if note_preview.strip():
                    scratch_lines.append(note_preview)
                else:
                    scratch_lines.append("<no note>")

                snippet = "\n".join(scratch_lines)
                existing_scratch = session.get("scratchpad") or ""
                if existing_scratch:
                    session["scratchpad"] = existing_scratch + "\n\n" + snippet
                else:
                    session["scratchpad"] = snippet

                # Semantically treat this as a rejected leaf for the minimal gem
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
                _auto_complete_ancestors_from_leaf(handle)
            elif act == "accept_subtree":
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
                        f"Handle '{handle}' has no descendants; use 'accept_leaf' / 'reject_leaf' "
                        "for leaves instead of 'accept_subtree'."
                    )

                # STRICT DFS FULL-WALK MODE:
                # In dfs_full_walk, branch-level accept is forbidden while any
                # descendant remains undecided, unless a guardian override
                # token is provided for this handle.
                if exploration_mode == "dfs_full_walk" and has_undecided:
                    guardian_token = action.get("guardian_token")
                    overrides = _load_guardian_overrides()
                    expected = overrides.get(handle)
                    if not (guardian_token and expected and guardian_token == expected):
                        # No matching guardian override → force the agent to walk
                        # the remaining leaves.
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
                            f"cannot accept_subtree on branch '{handle}' while any descendants "
                            "remain undecided.\n\n"
                            "Walk the branch in depth-first order and make leaf-level decisions "
                            "with 'accept_leaf' / 'reject_leaf'."
                        )
                        if next_handle:
                            base_msg += (
                                f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                                "Decide there before attempting any branch-level accept/reject."
                            )
                        raise NetworkError(base_msg)
                    # If guardian override matches, fall through to the normal
                    # (non-strict) semantics below.

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
                        f"Cannot accept_subtree on branch '{handle}' while some descendants are "
                        "still undecided and others have already been decided.\n\n"
                        "Finish exploring this branch in depth-first order first."
                    )
                    if next_handle:
                        base_msg += (
                            f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                            "Make a leaf-level decision there with 'accept_leaf' / 'reject_leaf', "
                            "or close/reopen branches as needed."
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
                        # Some leaves already accepted – accepting the subtree is
                        # a no-op. The minimal covering tree will be built from
                        # leaf decisions (branch becomes a path element).
                        _auto_complete_ancestors_from_decision(handle)
                        continue
                    # All descendants rejected: include branch-only shell (no children).
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

            elif act == "reject_subtree":
                summary = _summarize_descendants(handle)
                desc_count = summary["descendant_count"]
                has_decided = summary["has_decided"]
                has_undecided = summary["has_undecided"]
                accepted_leaves = summary["accepted_leaf_count"]

                if desc_count == 0:
                    raise NetworkError(
                        f"Handle '{handle}' has no descendants; use 'reject_leaf' "
                        "for leaves instead of 'reject_subtree'."
                    )

                # STRICT DFS FULL-WALK MODE:
                # In dfs_full_walk, branch-level reject is forbidden while any
                # descendant remains undecided, unless a guardian override
                # token is provided for this handle.
                if exploration_mode == "dfs_full_walk" and has_undecided:
                    guardian_token = action.get("guardian_token")
                    overrides = _load_guardian_overrides()
                    expected = overrides.get(handle)
                    if not (guardian_token and expected and guardian_token == expected):
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
                            f"cannot reject_subtree on branch '{handle}' while any descendants "
                            "remain undecided.\n\n"
                            "Walk the branch in depth-first order and make leaf-level decisions "
                            "with 'accept_leaf' / 'reject_leaf'."
                        )
                        if next_handle:
                            base_msg += (
                                f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                                "Decide there before attempting any branch-level accept/reject."
                            )
                        raise NetworkError(base_msg)
                    # If guardian override matches, fall through to the normal
                    # (non-strict) semantics below.

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
                        f"Cannot reject_subtree on branch '{handle}' while some descendants are "
                        "still undecided and others have already been decided.\n\n"
                        "Finish exploring this branch in depth-first order first."
                    )
                    if next_handle:
                        base_msg += (
                            f"\n\nNext DFS node is '{next_handle}' ({next_name!r}). "
                            "Make a leaf-level decision there with 'accept_leaf' / 'reject_leaf', "
                            "or close/reopen branches as needed."
                        )
                    raise NetworkError(base_msg)

                # All descendants decided but some leaves accepted – do not allow
                # a branch-wide reject that silently overrides positive decisions.
                if has_decided and not has_undecided and accepted_leaves > 0:
                    raise NetworkError(
                        f"Cannot reject_subtree on branch '{handle}' because one or more leaves "
                        "under this branch have already been accepted. "
                        "Revisit those leaves with 'reject_leaf' or use 'reopen_branch' first."
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
            elif act == "backtrack":
                # In this v2 implementation, backtrack behaves like a careful
                # branch-level close. If the branch is already decided, we
                # require explicit reopen_branch instead of silently changing
                # history.
                if entry.get("status") in {"finalized", "closed"}:
                    raise NetworkError(
                        f"Handle '{handle}' has already been decided. Use 'reopen_branch' "
                        "to re-open this branch for exploration."
                    )
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
            elif act == "reopen_branch":
                # Re-open this handle and all descendants for re-exploration.
                # The branch root becomes open; descendants become candidates.
                queue = [handle]
                while queue:
                    h = queue.pop(0)
                    st = state.get(h, {"status": "unseen", "max_depth": None, "selection_type": None})
                    if h == handle:
                        st["status"] = "open"
                    else:
                        st["status"] = "candidate"
                    st["max_depth"] = None
                    st["selection_type"] = None
                    state[h] = st

                    child_handles = handles.get(h, {}).get("children", []) or []
                    for ch in child_handles:
                        queue.append(ch)
            elif act in {"update_node_and_flag_for_acceptance", "update_note_and_flag_for_acceptance"}:
                if not editable_mode:
                    raise NetworkError(
                        "update_node_and_flag_for_acceptance/update_note_and_flag_for_acceptance is only allowed when session.editable=True"
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
                            "update_node_and_flag_for_acceptance requires 'name' to be a string if provided"
                        )
                    target_node["name"] = new_name
                    meta["name"] = new_name

                if new_note is not None:
                    if not isinstance(new_note, str):
                        raise NetworkError(
                            "update_node_and_flag_for_acceptance requires 'note' to be a string if provided"
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
                    entry["accept_on_finalize"] = True

            elif act == "update_tag_and_flag_for_acceptance":
                if not editable_mode:
                    raise NetworkError("update_tag_and_flag_for_acceptance is only allowed when session.editable=True")

                raw_tag = action.get("tag")
                if not isinstance(raw_tag, str) or not raw_tag.strip():
                    raise NetworkError("update_tag_and_flag_for_acceptance requires non-empty 'tag' string")

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
                    entry["accept_on_finalize"] = True
            else:
                raise NetworkError(f"Unsupported exploration action: '{act}'")

        # Recompute frontier
        session["handles"] = handles
        session["state"] = state
        frontier = self._compute_exploration_frontier(
            session,
            frontier_size=frontier_size,
            max_depth_per_frontier=max_depth_per_frontier,
        )

        # Update session state and persist
        session["steps"] = int(session.get("steps", 0)) + 1
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"

        try:
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to persist exploration session '{session_id}' after step: {e}")
            raise NetworkError(f"Failed to persist exploration session: {e}") from e

        history_summary = None
        if include_history_summary:
            history_summary = {
                "open": [h for h, st in state.items() if st.get("status") == "open"],
                "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                "steps": session["steps"],
            }

        result: dict[str, Any] = {
            "success": True,
            "session_id": session_id,
            "frontier": frontier,
            "scratchpad": session.get("scratchpad", ""),
            "guidance": (
                "Leaf-first exploration is encouraged. Open branches to reach leaves; "
                "make decisions with 'accept_leaf' / 'reject_leaf' where possible. "
                "Use 'accept_subtree' sparingly for whole branches, and 'reopen_branch' "
                "if you need to reconsider a decided branch."
            ),
        }
        if history_summary is not None:
            result["history_summary"] = history_summary
        if peek_results:
            result["peek_results"] = peek_results

        return result

    async def nexus_finalize_exploration(
        self,
        session_id: str,
        include_terrain: bool = True,
    ) -> dict[str, Any]:
        """Finalize an exploration session into phantom_gem.json (+ optional terrain).

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
        - If include_terrain=True and no coarse_terrain.json exists yet, writes
          a minimal TERRAIN using the cached root tree.
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

        # Interpret any accept_on_finalize flags from editable sessions:
        # promote flagged branches to explicit subtree selections before building
        # the minimal gem.
        for handle, st in state.items():
            if st.get("accept_on_finalize") and st.get("status") not in {"finalized", "closed"}:
                st["status"] = "finalized"
                if st.get("selection_type") is None:
                    st["selection_type"] = "subtree"

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

        if not finalized_entries:
            raise NetworkError(
                f"Exploration session '{session_id}' has no finalized paths to export."
            )

        # COMPLETENESS CHECK: ensure every handle is either explicitly decided
        # (status in {finalized, closed}) or covered by an ancestor subtree
        # decision (selection_type == "subtree"). This prevents accidental
        # partial exploration like the Claude failure case.
        uncovered_handles: list[str] = []

        for handle, meta in handles.items():
            # Skip synthetic root handle
            if handle == "R":
                continue

            st = state.get(handle, {"status": "unseen"})
            status = st.get("status")

            # Explicitly decided handles are fine
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

            raise NetworkError(
                "Incomplete exploration detected during nexus_finalize_exploration.\n\n"
                "You attempted to finalize without accounting for all branches.\n\n"
                "Handles that are neither explicitly accepted/rejected nor covered "
                "by an accepted/rejected subtree decision:\n"
                + "\n".join(details_lines)
                + more_note
                + "\n\nTo proceed safely, either:\n"
                "• Visit these handles and decide with 'accept_leaf' / 'reject_leaf', OR\n"
                "• Use 'accept_subtree' / 'reject_subtree' on an ancestor branch to cover them.\n\n"
                "Once every handle is accounted for, nexus_finalize_exploration will succeed "
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

        # 1) Accept full subtrees: include subtree roots and ALL descendants
        for node_id in true_subtree_root_ids:
            if node_id not in node_by_id:
                logger.warning(
                    f"nexus_finalize_exploration: subtree root id {node_id} not found in tree; skipping."
                )
                continue
            stack = [node_id]
            while stack:
                cur = stack.pop()
                if cur in needed_ids:
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
        ordered_root_ids: list[str] = []

        def collect_roots_in_order(node: dict[str, Any]) -> None:
            nid = node.get("id")
            if nid in root_ids and nid not in ordered_root_ids:
                ordered_root_ids.append(nid)
            for child in node.get("children", []) or []:
                collect_roots_in_order(child)

        collect_roots_in_order(root_node)

        # Helper: recursively copy only needed nodes under a given root id
        def copy_pruned_subtree(node: dict[str, Any]) -> dict[str, Any] | None:
            nid = node.get("id")
            if nid not in needed_ids:
                return None

            new_node = {k: v for k, v in node.items() if k != "children"}
            role = role_by_node_id.get(nid)
            if role:
                new_node["gem_role"] = role
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

        gem_nodes: list[dict[str, Any]] = []
        for root_id in ordered_root_ids:
            original = node_by_id.get(root_id)
            if not original:
                continue
            pruned = copy_pruned_subtree(original)
            if pruned is not None:
                gem_nodes.append(pruned)

        run_dir = self._get_nexus_dir(nexus_tag)
        phantom_path = os.path.join(run_dir, "phantom_gem.json")
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")

        # Pack exploration scratchpad and per-handle hints into the phantom gem
        # so GEMSTORM / transform_gem can use this scaffolding later.
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

        phantom_payload = {
            "nexus_tag": nexus_tag,
            "roots": ordered_root_ids,
            "nodes": gem_nodes,
            "scratchpad": scratchpad_text,
            "hints": hints_export,
        }

        try:
            os.makedirs(run_dir, exist_ok=True)
            with open(phantom_path, "w", encoding="utf-8") as f:
                json_module.dump(phantom_payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to write phantom_gem.json: {e}") from e

        # Optionally create a minimal coarse_terrain.json if one does not exist
        if include_terrain and not os.path.exists(coarse_path):
            try:
                export_root_id = session.get("root_id")
                export_root_name = session.get("root_name") or root_node.get("name", "Root")
                export_timestamp = None  # Exploration does not track per-node timestamps

                # IMPORTANT: children only – do NOT include the root node itself here.
                # This keeps the NEXUS invariant that 'nodes' holds the children of
                # export_root_id, matching bulk_export_to_file and nexus_glimpse,
                # and prevents the reconciliation algorithm from trying to create
                # the root as a child of itself.
                root_children = root_node.get("children", []) or []

                coarse_payload = {
                    "export_root_id": export_root_id,
                    "export_root_name": export_root_name,
                    "export_timestamp": export_timestamp,
                    "nodes": root_children,
                }
                with open(coarse_path, "w", encoding="utf-8") as f:
                    json_module.dump(coarse_payload, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(
                    f"Failed to write coarse_terrain.json during finalize_exploration: {e}"
                )

        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "phantom_gem": phantom_path,
            "coarse_terrain": coarse_path if os.path.exists(coarse_path) else None,
            "finalized_branch_count": len(ordered_root_ids),
            "node_count": len(gem_nodes),
        }
