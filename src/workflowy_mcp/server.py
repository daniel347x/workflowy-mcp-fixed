"""WorkFlowy MCP server implementation using FastMCP."""

import sys
import os
from datetime import datetime
# Also log to file to debug deployment/environment
try:
    # Derive project_root dynamically for startup log
    server_dir_startup = os.path.dirname(os.path.abspath(__file__))
    mcp_servers_dir_startup = os.path.dirname(server_dir_startup)
    project_root_startup = os.path.dirname(mcp_servers_dir_startup)
    startup_log_path = os.path.join(project_root_startup, "temp", "reconcile_debug.log")
    with open(startup_log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat()}] DEBUG: Workflowy MCP Server loaded from {__file__}\n")
except Exception:
    pass
print("DEBUG: Workflowy MCP Server loaded from " + __file__, file=sys.stderr)

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal, Any, Awaitable, Callable

from fastmcp import FastMCP

from .client import AdaptiveRateLimiter, WorkFlowyClient
from .config import ServerConfig, setup_logging
from .models import (
    NodeCreateRequest,
    NodeListRequest,
    NodeUpdateRequest,
    WorkFlowyNode,
)

class _ClientLogger:
    """Lightweight logger that delegates to _log / log_event.

    Used in place of logging.getLogger(...) so existing logger.info /
    logger.warning / logger.error calls feed into the DAGGER logger
    instead of Python's logging module.
    """

    def __init__(self, component: str = "SERVER") -> None:
        self._component = component

    def _msg(self, msg: object) -> str:
        try:
            return str(msg)
        except Exception:
            return repr(msg)

    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(self._msg(msg), self._component)

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"WARNING: {self._msg(msg)}", self._component)

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"ERROR: {self._msg(msg)}", self._component)

    def debug(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"DEBUG: {self._msg(msg)}", self._component)

    def exception(self, msg: object, *args: object, **kwargs: object) -> None:
        _log(f"EXCEPTION: {self._msg(msg)}", self._component)


logger = _ClientLogger("SERVER")

# Global client instance
_client: WorkFlowyClient | None = None
_rate_limiter: AdaptiveRateLimiter | None = None

# Global WebSocket connection for DOM cache
_ws_connection = None
_ws_server_task = None
_ws_message_queue = None  # asyncio.Queue for message routing

# In-memory job registry for long-running operations (ETCH, NEXUS, etc.)
_jobs: dict[str, dict[str, Any]] = {}
_job_counter: int = 0
_job_lock: asyncio.Lock = asyncio.Lock()

# AI Dagger root (top-level for MCP workflows)
DAGGER_ROOT_ID = "b49affa1-3930-95e3-b2fe-ad9b881285e2"


def get_client() -> WorkFlowyClient:
    """Get the global WorkFlowy client instance."""
    global _client
    if _client is None:
        raise RuntimeError("WorkFlowy client not initialized. Server not started properly.")
    return _client


# @beacon[
#   id=auto-beacon@get_ws_connection-bjsi,
#   role=get_ws_connection,
#   slice_labels=f9-f12-handlers,ra-carto-jobs,ra-websocket,nexus--glimpse-extension,
#   kind=ast,
# ]
def get_ws_connection():
    """Get the current WebSocket connection and message queue (if any)."""
    global _ws_connection, _ws_message_queue
    return _ws_connection, _ws_message_queue


def log_event(message: str, component: str = "SERVER") -> None:
    """Log an event to stderr with timestamp and consistent formatting."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 🗡️ prefix makes it easy to grep/spot in the console
    print(f"[{timestamp}] 🗡️ [{component}] {message}", file=sys.stderr, flush=True)


def _log(message: str, component: str = "SERVER") -> None:
    """Unified log wrapper used throughout this server.

    Ensures all logging uses the DAGGER+DATETIME+TAG prefix and plain
    print(..., file=sys.stderr), which reliably surfaces in the MCP
    connector console (FastMCP tends to swallow standard logging output).
    """
    log_event(message, component)


# @beacon[
#   id=auto-beacon@_resolve_uuid_path_and_respond-fu2j,
#   role=_resolve_uuid_path_and_respond,
#   slice_labels=f9-f12-handlers,ra-carto-jobs,ra-websocket,ra-reconcile,
#   kind=ast,
# ]
async def _resolve_uuid_path_and_respond(target_uuid: str | None, websocket, format_mode: str = "f3") -> None:
    """Resolve full ancestor path for target_uuid and send result back to extension.

    Implementation note (v3):
    - Uses /nodes-export cache to build parent map (all nodes in one call)
    - Walks parent_id chain from leaf to root using cached data
    - O(1) API call + O(depth) traversal = fast and reliable
    - Decodes HTML entities (&lt; &gt; &amp;) in node names for display
    
    format_mode:
      - "f3" (default): Compact mode. One line per node.
      - "f2": Full/Verbose mode. Node name, then UUID on next line, then blank line.
    """
    import html
    client = get_client()
    target = (target_uuid or "").strip()

    if not target:
        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": False,
            "target_uuid": target_uuid,
            "error": "No target_uuid provided",
        }))
        return
    
    # Handle virtual UUIDs (generated by GLIMPSE for mirrored nodes)
    # Format: virtual_<actual-uuid>_<parent-uuid>_<hash>
    # For mirrors: show BOTH paths (actual node + mirror location context)
    is_mirrored = False
    mirror_parent_uuid = None
    
    if target.startswith("virtual_"):
        parts = target.split("_")
        # parts[0] = "virtual", parts[1] = actual UUID, parts[2] = parent UUID, parts[3+] = hash
        if len(parts) >= 3:
            actual_uuid = parts[1]
            mirror_parent_uuid = parts[2]
            is_mirrored = True
            log_event(f"Virtual UUID detected (mirrored node): actual={actual_uuid}, mirror_parent={mirror_parent_uuid}", "UUID_RES")
            target = actual_uuid
        elif len(parts) >= 2:
            actual_uuid = parts[1]
            log_event(f"Virtual UUID detected: {target} -> extracting actual UUID: {actual_uuid}", "UUID_RES")
            target = actual_uuid
        else:
            await websocket.send(json.dumps({
                "action": "uuid_path_result",
                "success": False,
                "target_uuid": target_uuid,
                "error": f"Malformed virtual UUID: {target}",
            }))
            return

    try:
        # Fetch full account export (uses cache if available)
        export_data = await client.export_nodes(node_id=None, use_cache=True)
        all_nodes = export_data.get("nodes", []) or []
        
        # Build node lookup by ID
        nodes_by_id = {n.get("id"): n for n in all_nodes if n.get("id")}
        
        # Check if target node's ancestor chain intersects dirty set.
        # Under manual-refresh semantics, dirty IDs are **diagnostic only** and
        # must not trigger automatic /nodes-export calls. We log and continue to
        # use the existing snapshot.
        dirty_ids = client._nodes_export_dirty_ids
        if dirty_ids:
            needs_refresh = False
            if "*" in dirty_ids or target in dirty_ids:
                needs_refresh = True
            else:
                current = target
                visited_check: set[str] = set()
                while current and current not in visited_check:
                    visited_check.add(current)
                    if current in dirty_ids:
                        needs_refresh = True
                        break
                    node_dict = nodes_by_id.get(current)
                    if not node_dict:
                        break
                    current = node_dict.get("parent_id") or node_dict.get("parentId")
            if needs_refresh:
                log_event(
                    f"Path from {target} intersects dirty IDs; using existing /nodes-export "
                    "snapshot (manual refresh only)",
                    "UUID_RES",
                )

        # DEBUG: Log cache stats
        log_event(
            f"Resolving path for target_uuid: {target} (cache: {len(nodes_by_id)} nodes)",
            "UUID_RES",
        )

        # If node not found in cache, fail with a clear manual-refresh message.
        if target not in nodes_by_id:
            error_msg = (
                f"Node {target} not found in cached /nodes-export; run "
                "workflowy_refresh_nodes_export_cache from the UUID widget and retry."
            )
            log_event(error_msg, "UUID_RES")
            await websocket.send(
                json.dumps(
                    {
                        "action": "uuid_path_result",
                        "success": False,
                        "target_uuid": target_uuid,
                        "error": error_msg,
                    }
                )
            )
            return

        # Walk parent chain from target to root
        path_nodes = []
        visited: set[str] = set()
        current_id: str | None = target
        max_hops = 512
        hops = 0

        while current_id and current_id not in visited and hops < max_hops:
            visited.add(current_id)
            node_dict = nodes_by_id.get(current_id)
            
            if not node_dict:
                break
            
            # DEBUG: Log each node found
            parent_id = node_dict.get("parent_id") or node_dict.get("parentId")
            raw_name = node_dict.get("nm") or node_dict.get("name") or "Untitled"
            node_name = html.unescape(raw_name)  # Decode &lt; &gt; &amp; etc.
            log_event(f"Found node: {current_id} (name: {node_name}), parent: {parent_id}", "UUID_RES")
            
            # Store decoded name in dict for later use
            node_dict["_decoded_name"] = node_name
            path_nodes.append(node_dict)
            current_id = parent_id
            hops += 1

        path_nodes.reverse()
        
        # DEBUG: Log final path length
        log_event(f"Resolved path length: {len(path_nodes)}", "UUID_RES")

        if not path_nodes:
            log_event(f"Path resolution returned empty for {target_uuid}", "UUID_RES")
            await websocket.send(json.dumps({
                "action": "uuid_path_result",
                "success": False,
                "target_uuid": target_uuid,
                "error": f"Could not build path for node {target_uuid} (empty path after traversal)",
            }))
            return

        lines: list[str] = []
        
        # Determine which nodes are leaves vs ancestors for bullet selection
        leaf_index = len(path_nodes) - 1
        
        if format_mode == "f2":
            # FULL / VERBOSE MODE (F2-style)
            # Render name, then UUID, then blank line for every node
            for depth, node_dict in enumerate(path_nodes):
                # Use decoded name (HTML entities already unescaped)
                name = node_dict.get("_decoded_name") or "Untitled"
                node_id = node_dict.get("id") or ""
                prefix = "#" * (depth + 1)
                # Bullet: ⦿ for ancestors, • for leaf
                bullet = "•" if depth == leaf_index else "⦿"
                lines.append(f"{prefix} {bullet} {name}")
                lines.append(f"`{node_id}`")
                lines.append("")
            
            # Ending block
            lines.append("→ Use Leaf UUID:")
            lines.append(f"`{target}`")
            
        else:
            # COMPACT MODE (F3-style / Default)
            for depth, node_dict in enumerate(path_nodes):
                # Use decoded name (HTML entities already unescaped)
                name = node_dict.get("_decoded_name") or "Untitled"
                prefix = "#" * (depth + 1)
                # Bullet: ⦿ for ancestors, • for leaf
                bullet = "•" if depth == leaf_index else "⦿"
                lines.append(f"{prefix} {bullet} {name}")

            lines.append("")
            lines.append(f"→ `{target}`")

        markdown = "\n".join(lines)
        
        # If this was a mirrored node, append the mirror location context
        if is_mirrored and mirror_parent_uuid:
            # Build path for mirror parent to show where the mirror appears
            mirror_path_nodes = []
            current_id = mirror_parent_uuid
            visited_mirror: set[str] = set()
            hops_mirror = 0
            
            while current_id and current_id not in visited_mirror and hops_mirror < 512:
                visited_mirror.add(current_id)
                node_dict = nodes_by_id.get(current_id)
                
                if not node_dict:
                    break
                
                # Decode name and skip "Untitled" nodes (Workflowy mirror detritus)
                raw_name = node_dict.get("nm") or node_dict.get("name") or "Untitled"
                decoded_name = html.unescape(raw_name)
                
                if decoded_name.strip() != "Untitled":
                    # Store decoded name for later use
                    node_dict["_decoded_name"] = decoded_name
                    mirror_path_nodes.append(node_dict)
                
                parent_id = node_dict.get("parent_id") or node_dict.get("parentId")
                current_id = parent_id
                hops_mirror += 1
            
            mirror_path_nodes.reverse()
            
            if mirror_path_nodes:
                # Add separator and header
                markdown += "\n\n---\n\n"
                markdown += "**Mirror appears under:**\n\n"
                
                # Generate mirror path markdown (same format as main path)
                mirror_lines = []
                leaf_index_mirror = len(mirror_path_nodes) - 1
                
                if format_mode == "f2":
                    for depth, node_dict in enumerate(mirror_path_nodes):
                        name = node_dict.get("_decoded_name") or "Untitled"
                        node_id = node_dict.get("id") or ""
                        prefix = "#" * (depth + 1)
                        bullet = "•" if depth == leaf_index_mirror else "⦿"
                        mirror_lines.append(f"{prefix} {bullet} {name}")
                        mirror_lines.append(f"`{node_id}`")
                        mirror_lines.append("")
                else:
                    for depth, node_dict in enumerate(mirror_path_nodes):
                        name = node_dict.get("_decoded_name") or "Untitled"
                        prefix = "#" * (depth + 1)
                        bullet = "•" if depth == leaf_index_mirror else "⦿"
                        mirror_lines.append(f"{prefix} {bullet} {name}")
                
                markdown += "\n".join(mirror_lines)
        
        # DEBUG: Log the complete markdown being sent (acts as historical log)
        log_event(f"Markdown output:\n{markdown}", "UUID_RES")
        
        # Also append to persistent UUID Explorer log file
        try:
            from datetime import datetime
            # Derive project_root dynamically
            server_dir_uuid = os.path.dirname(os.path.abspath(__file__))
            mcp_servers_dir_uuid = os.path.dirname(server_dir_uuid)
            project_root_uuid = os.path.dirname(mcp_servers_dir_uuid)
            log_path = os.path.join(project_root_uuid, "temp", "uuid_and_glimpse_explorer", "uuid_explorer.md")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"## {timestamp}\n\n")
                f.write(f"**Target UUID:** `{target}`\n\n")
                f.write(markdown)
                f.write("\n\n---\n\n")
        except Exception as log_err:
            # Never let logging failures affect UUID resolution
            log_event(f"Failed to write UUID Explorer log: {log_err}", "UUID_RES")

        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": True,
            "target_uuid": target_uuid,
            "markdown": markdown,
            "path": [
                {
                    "id": node_dict.get("id"),
                    "name": node_dict.get("_decoded_name") or "Untitled",
                    "parent_id": node_dict.get("parent_id") or node_dict.get("parentId"),
                }
                for node_dict in path_nodes
            ],
        }))

    except Exception as e:  # noqa: BLE001
        log_event(f"UUID path resolution error for {target_uuid}: {e}", "UUID_RES")
        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": False,
            "target_uuid": target_uuid,
            "error": str(e),
        }))


# @beacon[
#   id=auto-beacon@_guess_line_number_from_name-wlsn,
#   role=_guess_line_number_from_name,
#   slice_labels=nexus-md-header-path,f9-f12-handlers,ra-websocket,ra-reconcile,
#   kind=ast,
# ]
def _guess_line_number_from_name(file_path: str, node_name: str | None) -> int:
    """Best-effort line-number guess for non-beacon nodes.

    Heuristic:
    - Start from the plain-text Workflowy node name provided by the extension.
    - Strip everything from the start up to (but not including) the first
      alphabetic character (tolerates multiple emojis, bullets, arrows, etc.).
    - Strip trailing tag decorations starting at the first '#'.
    - If the remaining text starts with "class" or "def", prefer a head like
      "class Name" / "def name(" when possible; otherwise fall back to the
      first identifier-like token.
    - If the resulting head is very short (<3 chars) bail out and return 1.
    - Scan the file line-by-line and return the first 1-based line index that
      contains the head as a substring. If nothing matches, return 1.
    """
    if not node_name:
        return 1

    candidate = (node_name or "").strip()
    if not candidate:
        return 1

    # 1) Strip leading non-alpha characters (emoji, bullets, arrows, punctuation).
    first_alpha_idx = None
    for idx, ch in enumerate(candidate):
        if ch.isalpha():
            first_alpha_idx = idx
            break
    if first_alpha_idx is not None:
        candidate = candidate[first_alpha_idx:].lstrip()
    else:
        # No alphabetic characters at all — nothing sensible to search for.
        return 1

    # 2) Strip trailing tag decorations starting at the first '#'.
    hash_idx = candidate.find("#")
    if hash_idx != -1:
        candidate = candidate[:hash_idx].rstrip()

    if not candidate:
        return 1

    # 3) Choose a search head based on the remaining candidate.
    head = None
    tokens = candidate.split()

    if not tokens:
        return 1

    # Special-case "class" / "def" constructs.
    if tokens[0] in ("class", "def"):
        if len(tokens) >= 2:
            if tokens[0] == "class":
                # e.g. "class ASTModel" → search for the class line.
                head = f"class {tokens[1]}"
            else:  # def
                # Prefer "def name(" pattern if present, otherwise "def name".
                name_token = tokens[1]
                if "(" in candidate:
                    head = f"def {name_token}("
                else:
                    head = f"def {name_token}"
        else:
            # Fallback: just use the keyword.
            head = tokens[0]
    else:
        # Generic case: use the first identifier-like token (letters/underscore, len>=3).
        for tok in tokens:
            if len(tok) >= 3 and any(c.isalpha() or c == "_" for c in tok):
                head = tok
                break
        # Fallback: first token if nothing matched the heuristic.
        if head is None:
            head = tokens[0]

    head = (head or "").strip()
    if len(head) < 3:
        return 1

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                if head in line:
                    return lineno
    except Exception as e:  # noqa: BLE001
        log_event(f"_guess_line_number_from_name: search failed for {file_path}: {e}", "WS_HANDLER")

    return 1


# @beacon[
#   id=auto-beacon@_launch_windsurf-nrnc,
#   role=_launch_windsurf,
#   slice_labels=f9-f12-handlers,ra-websocket,
#   kind=ast,
# ]
def _launch_windsurf(file_path: str, line: int | None = None) -> None:
    """Launch WindSurf for the given file/line on Windows.

    Uses PowerShell Start-Process so that Windows treats this like a normal
    app activation, while still allowing Windsurf itself to reuse the
    existing window (no explicit "new window" flag is used here).

    Bidirectional path normalization:
    - If path doesn't exist + non-C drive → try C:, then C: with __Dan_Root removed
    - If path doesn't exist + C: drive → try E: with __Dan_Root added back
    """
    import os
    import subprocess

    exe = r"C:\\Users\\danie\\AppData\\Local\\Programs\\Windsurf\\Windsurf.exe"

    # Bidirectional path normalization (matches normalize_cartographer_path logic)
    try:
        if file_path and isinstance(file_path, str) and not os.path.exists(file_path):
            drive, tail = os.path.splitdrive(file_path)
            
            # BRANCH A: non-C drive → try C: variants
            if drive and drive.upper() != "C:":
                # Try C: drive
                alt_c_path = "C:" + tail
                if os.path.exists(alt_c_path):
                    log_event(
                        f"WindSurf path normalized: {file_path} → {alt_c_path} (C: drive)",
                        "WS_HANDLER",
                    )
                    file_path = alt_c_path
                # Try C: with __Dan_Root removed
                elif "__Dan_Root" in alt_c_path:
                    alt_no_dan_root = alt_c_path.replace("\\__Dan_Root\\", "\\")
                    if os.path.exists(alt_no_dan_root):
                        log_event(
                            f"WindSurf path normalized: {file_path} → {alt_no_dan_root} (__Dan_Root removed)",
                            "WS_HANDLER",
                        )
                        file_path = alt_no_dan_root
            
            # BRANCH B: C: drive → try E: with __Dan_Root added back
            elif drive and drive.upper() == "C:":
                # Pattern: C:\__daniel347x\__repos\... → E:\__daniel347x\__Dan_Root\__repos\...
                parts = tail.split(os.sep)
                # parts[0] = '', parts[1] = '__daniel347x', parts[2] = '__repos', ...
                if len(parts) >= 3 and parts[1] == "__daniel347x":
                    # Insert __Dan_Root after __daniel347x
                    new_parts = parts[:2] + ["__Dan_Root"] + parts[2:]
                    new_tail = os.sep.join(new_parts)
                    alt_e_with_dan_root = "E:" + new_tail
                    if os.path.exists(alt_e_with_dan_root):
                        log_event(
                            f"WindSurf path normalized: {file_path} → {alt_e_with_dan_root} (E: + __Dan_Root)",
                            "WS_HANDLER",
                        )
                        file_path = alt_e_with_dan_root
    except Exception as e:  # noqa: BLE001
        # Best-effort only; do not block launch.
        log_event(f"WindSurf path normalization failed for {file_path}: {e}", "WS_HANDLER")

    if line is not None and isinstance(line, int) and line > 0:
        # -g file:line (reuse existing window semantics left to Windsurf)
        arglist = f"-g \"{file_path}:{line}\""
    else:
        # Reuse existing window and just open the file
        arglist = f"-r \"{file_path}\""

    ps_cmd = (
        "Start-Process -FilePath '" + exe + "' "
        "-ArgumentList '" + arglist + "' "
        "-WindowStyle Normal"
    )

    try:
        subprocess.Popen(
            ["powershell.exe", "-Command", ps_cmd],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log_event(f"Launched WindSurf for {file_path} (line={line}) via PowerShell", "WS_HANDLER")
    except Exception as e:  # noqa: BLE001
        # Let callers surface a structured error if needed
        log_event(f"Failed to launch WindSurf for {file_path} via PowerShell: {e}", "WS_HANDLER")
        raise


# @beacon[
#   id=auto-beacon@_maybe_create_ast_beacon_from_tags-adiu,
#   role=_maybe_create_ast_beacon_from_tags,
#   slice_labels=f9-f12-handlers,ra-websocket,ra-reconcile,
#   kind=ast,
# ]
async def _maybe_create_ast_beacon_from_tags(
    client: "WorkFlowyClient",
    node_id: str,
    node_name: str | None,
) -> dict[str, Any] | None:
    """Attempt AST-beacon auto-creation for an AST node with trailing #tags.

    Conditions for activation:
    - Node's note contains an AST_QUALNAME: line (Python Cartographer AST node).
    - Node's note does NOT contain an existing BEACON metadata block
      (no "BEACON (" or "@beacon[" in the note).
    - The Workflowy node name has one or more trailing tokens starting with
      '#', which we treat as tags.

    On success, this edits the underlying Python file in-place by inserting a
    minimal @beacon[...] block immediately above any decorators for the AST
    node (or above the def/class line if there are no decorators) and returns
    a small result dict. Returns None when conditions are not met so the
    caller can fall back to normal F12 behavior (per-file refresh).
    """
    log_event(
        f"_maybe_create_ast_beacon_from_tags: enter node_id={node_id} node_name={node_name!r}",
        "WS_HANDLER",
    )

    # Use /nodes-export snapshot from the client's cache so we can inspect
    # the node's note without any additional API calls, but never trigger a
    # fresh /nodes-export call from this helper. If the cache is unavailable,
    # we simply skip the AST auto-beacon path and fall back to normal F12.
    try:
        all_nodes = client._get_nodes_export_cache_nodes()
        nodes_by_id: dict[str, dict[str, Any]] = {
            str(n.get("id")): n for n in all_nodes if n.get("id")
        }
    except Exception as e:  # noqa: BLE001
        log_event(
            f"_maybe_create_ast_beacon_from_tags: nodes-export cache unavailable: {e}",
            "WS_HANDLER",
        )
        return None

    node = nodes_by_id.get(str(node_id))
    if not node:
        log_event(
            f"_maybe_create_ast_beacon_from_tags: skipping – node_id {node_id} not found in cache",
            "WS_HANDLER",
        )
        return None

    note_val = node.get("note") or node.get("no") or ""
    if not isinstance(note_val, str):
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – note is not a string",
            "WS_HANDLER",
        )
        return None

    # Determine AST vs beacon metadata using strict header patterns so that
    # docstrings that merely *mention* @beacon[...] do not block auto-creation.
    lines = note_val.splitlines()

    # AST detection: Cartographer always writes AST_QUALNAME on the first
    # non-empty line of the note for AST-backed nodes.
    first_nonempty = ""
    for ln in lines:
        stripped_ln = ln.strip()
        if stripped_ln:
            first_nonempty = stripped_ln
            break
    has_ast = first_nonempty.startswith("AST_QUALNAME:")

    # Beacon detection: true BEACON metadata appears as a header block
    # starting with a BEACON line, followed (after skipping blank lines)
    # by an id: ... line. This avoids treating prose mentions of
    # "BEACON (AST)" or "@beacon[...]" as real metadata.
    nonempty = [ln.strip() for ln in lines if ln.strip()]
    has_beacon = False
    for i, ln in enumerate(nonempty):
        if ln.startswith("BEACON"):
            # Look for next non-empty line and require id: ...
            for j in range(i + 1, len(nonempty)):
                nxt = nonempty[j]
                if not nxt:
                    continue
                if nxt.lower().startswith("id:"):
                    has_beacon = True
                break
            if has_beacon:
                break

    log_event(
        f"_maybe_create_ast_beacon_from_tags: note_flags ast={has_ast} beacon={has_beacon}",
        "WS_HANDLER",
    )

    # Require AST metadata and no existing beacon metadata in the note.
    if not has_ast:
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – no AST_QUALNAME in note",
            "WS_HANDLER",
        )
        return None
    if has_beacon:
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – beacon metadata already present",
            "WS_HANDLER",
        )
        return None

    ast_qualname: str | None = None
    if first_nonempty and first_nonempty.startswith("AST_QUALNAME:"):
        ast_qualname = first_nonempty.split(":", 1)[1].strip() or None
    if not ast_qualname:
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – failed to parse AST_QUALNAME",
            "WS_HANDLER",
        )
        return None

    # Extract trailing #tags from the node name.
    tags: list[str] = []
    if node_name:
        parts = str(node_name).strip().split()
        acc: list[str] = []
        for tok in reversed(parts):
            if tok.startswith("#") and len(tok) > 1:
                acc.append(tok[1:])  # drop leading '#'
            else:
                break
        tags = list(reversed(acc))

    log_event(
        f"_maybe_create_ast_beacon_from_tags: extracted_tags={tags}",
        "WS_HANDLER",
    )

    if not tags:
        # No manual tags to convert into a beacon – fall back to normal F12 behavior.
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – no trailing #tags in node_name",
            "WS_HANDLER",
        )
        return None

    # Resolve source file and AST line via existing beacon_get_code_snippet AST path.
    try:
        log_event(
            f"_maybe_create_ast_beacon_from_tags: calling beacon_get_code_snippet for node_id={node_id}",
            "WS_HANDLER",
        )
        snippet_result = await client.beacon_get_code_snippet(  # type: ignore[attr-defined]
            beacon_node_id=str(node_id),
            context=0,
        )
    except Exception as e:  # noqa: BLE001
        log_event(
            f"_maybe_create_ast_beacon_from_tags: beacon_get_code_snippet failed for {node_id}: {e}",
            "WS_HANDLER",
        )
        return None

    if not isinstance(snippet_result, dict) or not snippet_result.get("success"):
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – beacon_get_code_snippet returned failure",
            "WS_HANDLER",
        )
        return None

    file_path = snippet_result.get("file_path")
    if not file_path or not isinstance(file_path, str):
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – invalid file_path in snippet_result",
            "WS_HANDLER",
        )
        return None

    from pathlib import Path

    if Path(file_path).suffix.lower() != ".py":
        # Only Python AST nodes are supported for this auto-beacon path.
        log_event(
            f"_maybe_create_ast_beacon_from_tags: skipping – non-.py file {file_path}",
            "WS_HANDLER",
        )
        return None

    core_start_line = (
        snippet_result.get("core_start_line")
        or snippet_result.get("start_line")
    )
    if not isinstance(core_start_line, int) or core_start_line <= 0:
        log_event(
            "_maybe_create_ast_beacon_from_tags: skipping – invalid core_start_line",
            "WS_HANDLER",
        )
        return None

    log_event(
        f"_maybe_create_ast_beacon_from_tags: resolved file_path={file_path!r} core_start_line={core_start_line}",
        "WS_HANDLER",
    )

    # Read file contents.
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:  # noqa: BLE001
        log_event(
            f"_maybe_create_ast_beacon_from_tags: failed to read {file_path}: {e}",
            "WS_HANDLER",
        )
        return {
            "success": False,
            "file_path": file_path,
            "error": f"Failed to read source file: {e}",
        }

    n_lines = len(lines)
    ast_line_idx = min(max(core_start_line, 1), n_lines) - 1  # 0-based index

    # Choose insertion point: immediately above any decorator block, else on the
    # def/class line returned by the AST outline.
    insert_idx = ast_line_idx
    j = ast_line_idx - 1
    top_decorator_idx: int | None = None
    while j >= 0:
        stripped = lines[j].lstrip()
        if not stripped:
            j -= 1
            continue
        if stripped.startswith("@"):  # decorator line
            top_decorator_idx = j
            j -= 1
            continue
        # Stop on first non-blank, non-decorator line.
        break
    if top_decorator_idx is not None:
        insert_idx = top_decorator_idx

    # Lightweight guard: if there is already an @beacon[ very close above,
    # skip auto-creation to avoid accidental duplicates when the Workflowy
    # note has not yet been refreshed.
    for k in range(max(0, insert_idx - 8), insert_idx):
        if "@beacon[" in lines[k]:
            log_event(
                "_maybe_create_ast_beacon_from_tags: skipping – existing @beacon[ found near insertion point",
                "WS_HANDLER",
            )
            return None

    # Build beacon id & slice_labels from tags.
    import secrets

    def _random_suffix(length: int = 4) -> str:
        alphabet = "0123456789abcdef"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    first_tag = tags[0]
    suffix = _random_suffix()
    beacon_id = f"{first_tag}@{suffix}"
    slice_labels = ",".join(tags)

    # Match indentation of the AST node / decorators so the beacon comment
    # lives in the correct block (inside the class/function), not at column 0.
    base_line = lines[insert_idx] if 0 <= insert_idx < n_lines else ""
    indent = base_line[: len(base_line) - len(base_line.lstrip())]

    beacon_block = [
        f"{indent}# @beacon[",
        f"{indent}#   id={beacon_id},",
        f"{indent}#   slice_labels={slice_labels},",
        f"{indent}# ]",
    ]

    new_lines = lines[:insert_idx] + beacon_block + lines[insert_idx:]

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(new_lines))
            f.write("\n")
    except Exception as e:  # noqa: BLE001
        log_event(
            f"_maybe_create_ast_beacon_from_tags: failed to write updated {file_path}: {e}",
            "WS_HANDLER",
        )
        return {
            "success": False,
            "file_path": file_path,
            "error": f"Failed to write updated file: {e}",
        }

    log_event(
        "AST auto-beacon created: id="
        f"{beacon_id} ast_qualname={ast_qualname} file={file_path} tags={tags}",
        "WS_HANDLER",
    )

    return {
        "success": True,
        "mode": "ast_beacon_created",
        "file_path": file_path,
        "ast_qualname": ast_qualname,
        "beacon_id": beacon_id,
        "tags": tags,
    }


# @beacon[
#   id=auto-beacon@_handle_refresh_file_node-7irr,
#   role=_handle_refresh_file_node,
#   slice_labels=ra-notes,ra-notes-cartographer,f9-f12-handlers,ra-carto-jobs,ra-websocket,ra-reconcile,
#   kind=ast,
# ]
async def _handle_refresh_file_node(data: dict[str, Any], websocket) -> None:
    """Handle F12-driven request to refresh a Cartographer FILE node.

    This is a WebSocket wrapper around the existing beacon_refresh_code_node
    MCP tool (which in turn calls client.refresh_file_node_beacons).

    New behavior (NEXUS Core v1 async F12):
    - For true FILE nodes with data.carto_async truthy, we launch a detached
      CARTO_REFRESH job via _start_carto_refresh_job and return a small
      result containing job_id / mode / root_uuid. The UUID widget polls
      mcp_job_status to show progress.
    - For AST/beacon nodes (non-FILE), we always run the synchronous beacon
      update path first, then perform a per-file refresh. The caller may
      later trigger a separate async FILE-level F12 if desired.
    """
    node_id = data.get("node_id") or data.get("target_uuid") or data.get("uuid")
    node_name = data.get("node_name") or ""

    if not node_id:
        await websocket.send(
            json.dumps(
                {
                    "action": "refresh_file_node_result",
                    "success": False,
                    "error": "No node_id provided in refresh_file_node payload",
                }
            )
        )
        return

    client = get_client()

    # Opportunistically patch the /nodes-export cache name for this node, if provided,
    # so auto-beacon and Cartographer can see fresh tags without requiring a full
    # cache refresh. This must never affect F12 behavior; failures are logged only.
    if node_name:
        try:
            await client.update_cached_node_name(
                node_id=str(node_id),
                new_name=str(node_name),
            )
        except Exception as e:  # noqa: BLE001
            log_event(
                f"_handle_refresh_file_node: update_cached_node_name failed for {node_id}: {e}",
                "WS_HANDLER",
            )

    # Decide whether this node is a FILE node (note starts with Path:/Root:)
    # vs an AST/heading node. For FILE nodes we bypass beacon update entirely
    # and go straight to a single per-file refresh (sync or async).
    note_for_classification = data.get("node_note")
    if note_for_classification is None:
        # Fallback: read from cached /nodes-export
        all_nodes_for_note = client._get_nodes_export_cache_nodes()
        nodes_by_id_for_note: dict[str, dict[str, Any]] = {
            str(n.get("id")): n for n in all_nodes_for_note if n.get("id")
        }
        node_for_note = nodes_by_id_for_note.get(str(node_id))
        if node_for_note:
            note_for_classification = str(node_for_note.get("note") or node_for_note.get("no") or "")
        else:
            note_for_classification = ""
    else:
        note_for_classification = str(note_for_classification)

    is_file_node = False
    if isinstance(note_for_classification, str):
        for line in note_for_classification.splitlines():
            stripped = line.strip()
            if stripped.startswith("Path:") or stripped.startswith("Root:"):
                is_file_node = True
                break

    # Async CARTO_REFRESH path is only valid for true FILE nodes. For AST/beacon
    # nodes we must run the synchronous update-beacon pipeline so that source
    # code and Workflowy metadata stay in lockstep.
    if is_file_node and data.get("carto_async"):
        async_result = await _start_carto_refresh_job(root_uuid=str(node_id), mode="file")
        payload: dict[str, Any] = {
            "action": "refresh_file_node_result",
            "node_id": node_id,
        }
        if isinstance(async_result, dict):
            payload.update(async_result)
        await websocket.send(json.dumps(payload))
        return

    if is_file_node:
        # FILE node (no async requested): skip beacon update entirely; perform a
        # single per-file Cartographer refresh only once.
        try:
            result = await client.refresh_file_node_beacons(
                file_node_id=node_id,
                dry_run=False,
            )
        except Exception as e:  # noqa: BLE001
            log_event(
                f"_handle_refresh_file_node: refresh_file_node_beacons failed for {node_id}: {e}",
                "WS_HANDLER",
            )
            await websocket.send(
                json.dumps(
                    {
                        "action": "refresh_file_node_result",
                        "success": False,
                        "node_id": node_id,
                        "error": f"Failed to refresh file node {node_id}: {e}",
                    }
                )
            )
            return

        if not isinstance(result, dict):
            result = {"success": False, "error": "refresh_file_node_beacons returned non-dict"}

        result["action"] = "refresh_file_node_result"
        result["node_id"] = node_id
        await websocket.send(json.dumps(result))
        return

    # Special-case: AST/beacon node with metadata/tags in name/note.
    # Use the new update_beacon_from_node method which supports Python/JS/TS/Markdown.
    # This handles:
    # - Creating beacons from tags for AST nodes (all languages)
    # - Updating existing beacon metadata from note changes
    # - Local cache sync (name + note)
    beacon_update_result: dict[str, Any] | None = None
    try:
        # Get the node's current note from WebSocket payload (fresh from DOM).
        # Only fall back to cache if not provided.
        note_val = data.get("node_note")
        if note_val is None:
            # Fallback to cache for backwards compatibility
            all_nodes_for_note = client._get_nodes_export_cache_nodes()
            nodes_by_id_for_note: dict[str, dict[str, Any]] = {
                str(n.get("id")): n for n in all_nodes_for_note if n.get("id")
            }
            node_for_note = nodes_by_id_for_note.get(str(node_id))
            if node_for_note:
                note_val = str(node_for_note.get("note") or node_for_note.get("no") or "")
            else:
                note_val = ""
        else:
            note_val = str(note_val)

        # Call the new unified beacon update method (supports Python/JS/TS/Markdown).
        beacon_update_result = await client.update_beacon_from_node(
            node_id=str(node_id),
            name=str(node_name),
            note=note_val,
        )
    except Exception as e:  # noqa: BLE001
        log_event(
            f"_handle_refresh_file_node: update_beacon_from_node failed for {node_id}: {e}",
            "WS_HANDLER",
        )
        beacon_update_result = None

    # After synchronous beacon update for AST nodes, trigger a FILE-level
    # refresh either synchronously or via a CARTO_REFRESH job, depending on
    # whether the client requested async behavior.
    if data.get("carto_async"):
        # Use the same CARTO_REFRESH file job path as FILE-node F12, but
        # allow the root_uuid to be a descendant; refresh_file_node_beacons
        # will resolve the enclosing FILE node.
        async_result = await _start_carto_refresh_job(root_uuid=str(node_id), mode="file")
        payload: dict[str, Any] = {
            "action": "refresh_file_node_result",
            "node_id": node_id,
            "beacon_update": beacon_update_result,
        }
        if isinstance(async_result, dict):
            payload.update(async_result)
        await websocket.send(json.dumps(payload))
        return

    # Default path for non-FILE nodes when async is not requested:
    # per-file Cartographer refresh in Workflowy. This is synchronous but
    # limited to a single file, so it should remain reasonably fast even
    # for large codebases.
    try:
        result = await client.refresh_file_node_beacons(
            file_node_id=node_id,
            dry_run=False,
        )
        if not isinstance(result, dict):
            result = {"success": False, "error": "refresh_file_node_beacons returned non-dict"}

        # Forward result back to the extension (success + counts or error),
        # and include any beacon_update_result for additional context.
        result["action"] = "refresh_file_node_result"
        result["node_id"] = node_id
        if beacon_update_result is not None:
            result["beacon_update"] = beacon_update_result
        await websocket.send(json.dumps(result))

    except Exception as e:  # noqa: BLE001
        log_event(
            f"_handle_refresh_file_node: refresh_file_node_beacons failed for {node_id}: {e}",
            "WS_HANDLER",
        )
        await websocket.send(
            json.dumps(
                {
                    "action": "refresh_file_node_result",
                    "success": False,
                    "node_id": node_id,
                    "error": f"Failed to refresh file node {node_id}: {e}",
                }
            )
        )


# @beacon[
#   id=auto-beacon@_handle_refresh_folder_node-tgyl,
#   role=_handle_refresh_folder_node,
#   slice_labels=ra-notes,ra-notes-cartographer,f9-f12-handlers,ra-carto-jobs,ra-websocket,ra-reconcile,
#   kind=ast,
# ]
async def _handle_refresh_folder_node(data: dict[str, Any], websocket) -> None:
    """Handle F12-driven request to refresh a Cartographer FOLDER subtree.

    New behavior (NEXUS Core v1 async F12):
    - If data.carto_async is truthy, we launch a detached CARTO_REFRESH job
      via _start_carto_refresh_job(mode="folder") and return a small
      result containing job_id / mode / root_uuid.
    - Otherwise, we fall back to the legacy synchronous behavior using
      refresh_folder_cartographer_sync.
    """
    node_id = data.get("node_id") or data.get("target_uuid") or data.get("uuid")

    if not node_id:
        await websocket.send(
            json.dumps(
                {
                    "action": "refresh_folder_node_result",
                    "success": False,
                    "error": "No node_id provided in refresh_folder_node payload",
                }
            )
        )
        return

    # Async CARTO_REFRESH path when requested by the client.
    if data.get("carto_async"):
        async_result = await _start_carto_refresh_job(root_uuid=str(node_id), mode="folder")
        payload: dict[str, Any] = {
            "action": "refresh_folder_node_result",
            "node_id": node_id,
        }
        if isinstance(async_result, dict):
            payload.update(async_result)
        await websocket.send(json.dumps(payload))
        return

    client = get_client()

    try:
        result = await client.refresh_folder_cartographer_sync(
            folder_node_id=node_id,
            dry_run=False,
        )
        if not isinstance(result, dict):
            result = {"success": False, "error": "refresh_folder_cartographer_sync returned non-dict"}

        result["action"] = "refresh_folder_node_result"
        result["node_id"] = node_id
        await websocket.send(json.dumps(result))

    except Exception as e:  # noqa: BLE001
        log_event(
            f"_handle_refresh_folder_node: refresh_folder_cartographer_sync failed for {node_id}: {e}",
            "WS_HANDLER",
        )
        await websocket.send(
            json.dumps(
                {
                    "action": "refresh_folder_node_result",
                    "success": False,
                    "node_id": node_id,
                    "error": f"Failed to refresh folder subtree {node_id}: {e}",
                }
            )
        )


# @beacon[
#   id=auto-beacon@_handle_open_node_in_windsurf-3j45,
#   role=_handle_open_node_in_windsurf,
#   slice_labels=nexus-md-header-path,f9-f12-handlers,ra-websocket,
#   kind=ast,
# ]
async def _handle_open_node_in_windsurf(data: dict[str, Any], websocket) -> None:
    """Handle F10-driven request to open code in WindSurf for a Workflowy node.

    Behaviour:
    - First attempt: treat node_id as a Cartographer beacon and call
      beacon_get_code_snippet(...) to obtain (file_path, start_line).
    - Fallback: call refresh_file_node_beacons(dry_run=True) starting from
      the same UUID to resolve the enclosing FILE node and its Path: ...
      note, but without writing anything back to Workflowy.
    - For non-beacon nodes (or when no specific line is known), make a
      best-effort guess at the line number by searching the file for a
      function-like head derived from node_name.
    - Finally, launch WindSurf with -g file:line (or -r file for line=1).
    """
    node_id = data.get("node_id") or data.get("target_uuid") or data.get("uuid")
    node_name = data.get("node_name") or ""

    if not node_id:
        await websocket.send(
            json.dumps(
                {
                    "action": "open_node_in_windsurf_result",
                    "success": False,
                    "error": "No node_id provided in open_node_in_windsurf payload",
                }
            )
        )
        return

    client = get_client()
    file_path: str | None = None
    line_number: int | None = None
    beacon_mode = False

    # 1) Beacon-aware fast path: if this UUID corresponds to a beacon node,
    # reuse the existing snippet resolution to get file + line directly.
    try:
        snippet_result = await client.beacon_get_code_snippet(beacon_node_id=node_id, context=10)
        if isinstance(snippet_result, dict) and snippet_result.get("success"):
            file_path = snippet_result.get("file_path")
            # Use beacon_line (the @beacon tag line) for direct navigation.
            # This points to the exact beacon tag, independent of context/core.
            beacon_line = snippet_result.get("beacon_line")
            if isinstance(beacon_line, int) and beacon_line > 0:
                line_number = beacon_line
            else:
                # Fallback to start_line if beacon_line unavailable (legacy/non-beacon)
                start_line = snippet_result.get("start_line") or snippet_result.get("end_line")
                if isinstance(start_line, int) and start_line > 0:
                    line_number = start_line
            beacon_mode = True
    except Exception as e:  # noqa: BLE001
        log_event(
            f"open_node_in_windsurf: beacon_get_code_snippet failed for {node_id}: {e}",
            "WS_HANDLER",
        )

    # 2) Fallback: dry-run file-level refresh to locate the enclosing FILE node
    # and its Path: ... note, without mutating Workflowy.
    if not file_path:
        try:
            refresh_result = await client.refresh_file_node_beacons(
                file_node_id=node_id,
                dry_run=True,
            )
            if not isinstance(refresh_result, dict) or not refresh_result.get("success"):
                await websocket.send(
                    json.dumps(
                        {
                            "action": "open_node_in_windsurf_result",
                            "success": False,
                            "node_id": node_id,
                            "error": refresh_result.get("error")
                            if isinstance(refresh_result, dict)
                            else "refresh_file_node_beacons(dry_run=True) failed",
                        }
                    )
                )
                return
            file_path = refresh_result.get("source_path")
        except Exception as e:  # noqa: BLE001
            log_event(
                f"open_node_in_windsurf: refresh_file_node_beacons(dry_run=True) failed for {node_id}: {e}",
                "WS_HANDLER",
            )
            await websocket.send(
                json.dumps(
                    {
                        "action": "open_node_in_windsurf_result",
                        "success": False,
                        "node_id": node_id,
                        "error": f"Failed to resolve source file for node {node_id}: {e}",
                    }
                )
            )
            return

    if not file_path:
        await websocket.send(
            json.dumps(
                {
                    "action": "open_node_in_windsurf_result",
                    "success": False,
                    "node_id": node_id,
                    "error": "Could not determine source file path for node",
                }
            )
        )
        return

    # 3) If we still do not have a concrete line number (non-beacon case),
    # perform a very lightweight in-file search using the node_name hint.
    if not line_number or not isinstance(line_number, int) or line_number <= 0:
        line_number = _guess_line_number_from_name(file_path, node_name)

    # 4) Launch WindSurf
    try:
        _launch_windsurf(file_path, line_number)
        await websocket.send(
            json.dumps(
                {
                    "action": "open_node_in_windsurf_result",
                    "success": True,
                    "node_id": node_id,
                    "file_path": file_path,
                    "line": line_number,
                    "beacon_mode": beacon_mode,
                }
            )
        )
    except Exception as e:  # noqa: BLE001
        await websocket.send(
            json.dumps(
                {
                    "action": "open_node_in_windsurf_result",
                    "success": False,
                    "node_id": node_id,
                    "file_path": file_path,
                    "line": line_number,
                    "error": f"Failed to launch WindSurf: {e}",
                }
            )
        )


# @beacon[
#   id=auto-beacon@websocket_handler-kgmz,
#   role=websocket_handler,
#   slice_labels=f9-f12-handlers,ra-carto-jobs,ra-websocket,nexus--glimpse-extension,
#   kind=ast,
# ]
async def websocket_handler(websocket):
    """Handle WebSocket connections from Chrome extension.
    
    Uses message queue pattern to avoid recv() conflicts.
    
    Extension sends requests:
    {"action": "extract_dom", "node_id": "uuid"}
    
    Extension receives responses with DOM tree data.
    """
    global _ws_connection, _ws_message_queue
    
    log_event(f"WebSocket client connected from {websocket.remote_address}", "WS_HANDLER")
    _ws_connection = websocket
    _ws_message_queue = asyncio.Queue()  # Fresh queue for this connection
    
    try:
        # Keep connection alive - wait for messages indefinitely
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get('action')
                if action != 'carto_list_jobs':
                    log_event(f"WebSocket message received: {action}", "WS_HANDLER")
                
                # Handle ping to keep connection alive
                if action == 'ping':
                    await websocket.send(json.dumps({"action": "pong"}))
                    log_event("Sent pong response", "WS_HANDLER")
                    continue

                # Handle UUID path resolution requests (UUID Navigator)
                if action == 'resolve_uuid_path':
                    target_uuid = data.get('target_uuid') or data.get('uuid')
                    format_mode = data.get('format', 'f3')  # Default to compact f3 mode
                    await _resolve_uuid_path_and_respond(target_uuid, websocket, format_mode)
                    continue

                # Explicit cache refresh request from GLIMPSE client.
                if action == 'refresh_nodes_export_cache':
                    try:
                        client = get_client()
                        result = await client.refresh_nodes_export_cache()
                        await websocket.send(json.dumps({
                            "action": "refresh_nodes_export_cache_result",
                            **result,
                        }))
                        log_event(
                            f"Refreshed /nodes-export cache via WebSocket request: {result.get('node_count')} nodes",
                            "WS_HANDLER",
                        )
                    except Exception as e:
                        log_event(
                            f"Failed to refresh /nodes-export cache from WebSocket request: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "refresh_nodes_export_cache_result",
                                        "success": False,
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; don't crash handler
                            pass
                    continue

                # Explicit cache snapshot save request from GLIMPSE client (no API calls).
                if action == 'save_nodes_export_cache':
                    try:
                        client = get_client()
                        result = await client.save_nodes_export_cache()
                        await websocket.send(
                            json.dumps(
                                {
                                    "action": "save_nodes_export_cache_result",
                                    **result,
                                }
                            )
                        )
                        if result.get("success"):
                            log_event(
                                f"Saved /nodes-export cache snapshot via WebSocket request: {result.get('snapshot_path')}",
                                "WS_HANDLER",
                            )
                        else:
                            log_event(
                                f"save_nodes_export_cache reported failure: {result.get('error')}",
                                "WS_HANDLER",
                            )
                    except Exception as e:
                        log_event(
                            f"Failed to save /nodes-export cache snapshot from WebSocket request: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "save_nodes_export_cache_result",
                                        "success": False,
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; don't crash handler
                            pass
                    continue

                # Optional: mutation notifications from Workflowy desktop
                # (e.g., extension sends notify_node_mutated when a node changes).
                if action == 'notify_node_mutated':
                    # Preferred payload (v3.9.2+): "nodes" = [{"id": "...", "name": "..."}, ...]
                    # Legacy payloads: "node_ids" (list[str]) or "node_id" (str).
                    nodes_payload = data.get('nodes') or []
                    node_ids = data.get('node_ids') or data.get('node_id') or []
                    if isinstance(node_ids, str):
                        node_ids = [node_ids]

                    named_entries: list[str] = []

                    # If structured nodes are present, trust them and derive both
                    # IDs and names from that payload for better logging.
                    if isinstance(nodes_payload, list) and nodes_payload:
                        node_ids = []
                        for entry in nodes_payload:
                            if not isinstance(entry, dict):
                                continue
                            nid = entry.get('id') or entry.get('uuid')
                            name = entry.get('name')
                            if not nid:
                                continue
                            node_ids.append(nid)
                            if name is not None:
                                named_entries.append(f"{nid} ({name!r})")
                            else:
                                named_entries.append(str(nid))
                    else:
                        # Fallback: no structured nodes payload; log bare IDs only.
                        named_entries = [str(nid) for nid in node_ids]

                    try:
                        client = get_client()
                        client._mark_nodes_export_dirty(node_ids)
                        log_event(
                            "Marked mutated nodes as dirty in /nodes-export cache: "
                            + ", ".join(named_entries),
                            "WS_HANDLER",
                        )
                        # NOTE: Cache refresh is LAZY - only triggered when UUID path is
                        # requested AND the path intersects a dirty ID. This prevents rate
                        # limiting from eager refreshes.
                    except Exception as e:
                        log_event(
                            f"Failed to mark /nodes-export cache dirty from WebSocket notification: {e}",
                            "WS_HANDLER",
                        )
                    continue

                # F10-driven: open associated code file in WindSurf from the hovered node
                if action == 'open_node_in_windsurf':
                    try:
                        await _handle_open_node_in_windsurf(data, websocket)
                    except Exception as e:
                        log_event(
                            f"open_node_in_windsurf handler error: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "open_node_in_windsurf_result",
                                        "success": False,
                                        "node_id": data.get('node_id'),
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; do not crash the handler on error reporting
                            pass
                    continue

                # F12-driven: refresh Cartographer FILE node in Workflowy from source
                if action == 'refresh_file_node':
                    try:
                        await _handle_refresh_file_node(data, websocket)
                    except Exception as e:
                        log_event(
                            f"refresh_file_node handler error: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "refresh_file_node_result",
                                        "success": False,
                                        "node_id": data.get('node_id'),
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; do not crash the handler on error reporting
                            pass
                    continue

                # F12-driven: refresh Cartographer FOLDER subtree (multi-file sync)
                if action == 'refresh_folder_node':
                    try:
                        await _handle_refresh_folder_node(data, websocket)
                    except Exception as e:
                        log_event(
                            f"refresh_folder_node handler error: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "refresh_folder_node_result",
                                        "success": False,
                                        "node_id": data.get('node_id'),
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; do not crash the handler on error reporting
                            pass
                    continue

                # List CARTO_REFRESH jobs (for async F12 status in the GLIMPSE widget)
                if action == 'carto_list_jobs':
                    try:
                        # Directly scan CARTO_REFRESH job JSON files under cartographer_jobs/.
                        from pathlib import Path
                        import json as json_module  # type: ignore[redefined-builtin]

                        carto_jobs_base = (
                            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\MCP_Servers\\workflowy_mcp\\temp\\cartographer_jobs"
                        )
                        base_dir = Path(carto_jobs_base)
                        jobs: list[dict[str, Any]] = []
                        if base_dir.exists():
                            for job_path in base_dir.glob("*.json"):
                                try:
                                    with job_path.open("r", encoding="utf-8") as jf:
                                        job = json_module.load(jf)
                                except Exception:  # noqa: BLE001
                                    continue

                                # Only consider CARTO_REFRESH jobs; ignore any other JSON artifacts.
                                if str(job.get("type")) != "CARTO_REFRESH":
                                    continue

                                jid = job.get("id") or job_path.stem
                                jobs.append(
                                    {
                                        "job_id": jid,
                                        "kind": "CARTO_REFRESH",
                                        "status": job.get("status", "unknown"),
                                        "mode": job.get("mode"),
                                        "root_uuid": job.get("root_uuid"),
                                        "detached": True,
                                        "carto_job_file": str(job_path),
                                        "log_file": job.get("logs_path"),
                                        "created_at": job.get("created_at"),
                                        "updated_at": job.get("updated_at"),
                                        "progress": job.get("progress"),
                                        "result_summary": job.get("result_summary"),
                                        "error": job.get("error"),
                                        "pid": job.get("pid"),
                                    }
                                )

                        await websocket.send(
                            json.dumps(
                                {
                                    "action": "carto_list_jobs_result",
                                    "success": True,
                                    "jobs": jobs,
                                }
                            )
                        )
                    except Exception as e:
                        log_event(
                            f"carto_list_jobs handler error: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "carto_list_jobs_result",
                                        "success": False,
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; do not crash the handler on error reporting
                            pass
                    continue

                # CANCEL a CARTO_REFRESH job (from Workflowy UUID widget)
                if action == 'carto_cancel_job':
                    try:
                        from pathlib import Path
                        import json as json_module  # type: ignore[redefined-builtin]
                        from datetime import datetime as _dt  # local alias

                        job_id = data.get('job_id')
                        if not job_id:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "carto_cancel_job_result",
                                        "success": False,
                                        "error": "Missing job_id in carto_cancel_job payload.",
                                    }
                                )
                            )
                            continue

                        carto_jobs_base = (
                            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\MCP_Servers\\workflowy_mcp\\temp\\cartographer_jobs"
                        )
                        base_dir = Path(carto_jobs_base)
                        found = False

                        if base_dir.exists():
                            for job_path in base_dir.glob("*.json"):
                                try:
                                    with job_path.open("r", encoding="utf-8") as jf:
                                        carto_job = json_module.load(jf)
                                except Exception:  # noqa: BLE001
                                    continue

                                # Only consider CARTO_REFRESH jobs; ignore any other JSON artifacts.
                                if str(carto_job.get("type")) != "CARTO_REFRESH":
                                    continue

                                jid = carto_job.get("id") or job_path.stem
                                if jid != job_id:
                                    continue

                                now_iso = _dt.utcnow().isoformat()
                                carto_job["status"] = "cancelled"
                                carto_job["updated_at"] = now_iso

                                result_summary = carto_job.get("result_summary") or {}
                                carto_job["result_summary"] = result_summary
                                result_summary.setdefault("errors", []).append(
                                    "Cancelled via carto_cancel_job (WebSocket).",
                                )

                                try:
                                    with job_path.open("w", encoding="utf-8") as jf:
                                        json_module.dump(carto_job, jf, indent=2)
                                except Exception as e:  # noqa: BLE001
                                    await websocket.send(
                                        json.dumps(
                                            {
                                                "action": "carto_cancel_job_result",
                                                "success": False,
                                                "job_id": job_id,
                                                "error": f"Failed to update CARTO job file: {e}",
                                            }
                                        )
                                    )
                                    break

                                found = True
                                await websocket.send(
                                    json.dumps(
                                        {
                                            "action": "carto_cancel_job_result",
                                            "success": True,
                                            "job_id": job_id,
                                        }
                                    )
                                )
                                break

                        if not found:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "carto_cancel_job_result",
                                        "success": False,
                                        "job_id": job_id,
                                        "error": "CARTO job not found.",
                                    }
                                )
                            )
                    except Exception as e:
                        log_event(
                            f"carto_cancel_job handler error: {e}",
                            "WS_HANDLER",
                        )
                        try:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "action": "carto_cancel_job_result",
                                        "success": False,
                                        "error": str(e),
                                    }
                                )
                            )
                        except Exception:
                            # Best-effort only; do not crash the handler on error reporting
                            pass
                    continue
                
                # Put all other messages in queue for workflowy_glimpse() to consume
                await _ws_message_queue.put(data)
                log_event(f"Message queued for processing: {action}", "WS_HANDLER")
                
            except json.JSONDecodeError as e:
                log_event(f"Invalid JSON from WebSocket: {e}", "WS_HANDLER")
            except Exception as e:
                log_event(f"WebSocket message error: {e}", "WS_HANDLER")
        
        # If loop exits naturally, connection was closed by client
        log_event("WebSocket client disconnected (connection closed by client)", "WS_HANDLER")
                
    except Exception as e:
        log_event(f"WebSocket connection closed with error: {e}", "WS_HANDLER")
    finally:
        if _ws_connection == websocket:
            _ws_connection = None
            _ws_message_queue = None
        log_event("WebSocket client cleaned up", "WS_HANDLER")


# @beacon[
#   id=auto-beacon@start_websocket_server-xel3,
#   role=start_websocket_server,
#   slice_labels=f9-f12-handlers,ra-carto-jobs,ra-websocket,nexus--glimpse-extension,
#   kind=ast,
# ]
async def start_websocket_server():
    """Start WebSocket server for Chrome extension communication."""
    try:
        import websockets
    except ImportError:
        log_event("websockets library not installed. WebSocket cache unavailable.", "WS_INIT")
        log_event("Install with: pip install websockets", "WS_INIT")
        return
    
    log_event("Starting WebSocket server on ws://localhost:8765", "WS_INIT")
    
    try:
        async with websockets.serve(websocket_handler, "localhost", 8765) as server:
            log_event("✅ WebSocket server listening on port 8765", "WS_INIT")
            log_event("WebSocket server will accept connections indefinitely...", "WS_INIT")
            
            # Keep server running forever
            await asyncio.Event().wait()
    except Exception as e:
        log_event(f"WebSocket server failed to start: {e}", "WS_INIT")
        log_event("GLIMPSE will fall back to API fetching", "WS_INIT")


@asynccontextmanager
# @beacon[
#   id=auto-beacon@lifespan-30n8,
#   role=lifespan,
#   slice_labels=f9-f12-handlers,ra-carto-jobs,ra-websocket,
#   kind=ast,
# ]
async def lifespan(_app: FastMCP):  # type: ignore[no-untyped-def]
    """Manage server lifecycle."""
    global _client, _rate_limiter, _ws_server_task

    # Setup
    log_event("Starting WorkFlowy MCP server", "LIFESPAN")

    # Load configuration
    config = ServerConfig()  # type: ignore[call-arg]
    api_config = config.get_api_config()

    # Initialize rate limiter (default 10 req/s)
    _rate_limiter = AdaptiveRateLimiter(
        initial_rate=10.0,
        min_rate=1.0,
        max_rate=100.0,
    )

    # Initialize client
    _client = WorkFlowyClient(api_config)

    log_event(f"WorkFlowy client initialized with base URL: {api_config.base_url}", "LIFESPAN")
    
    # Start WebSocket server in background task
    _ws_server_task = asyncio.create_task(start_websocket_server())
    log_event("WebSocket server task created", "LIFESPAN")

    yield

    # Cleanup
    log_event("Shutting down WorkFlowy MCP server", "LIFESPAN")
    
    # Cancel WebSocket server
    if _ws_server_task:
        _ws_server_task.cancel()
        try:
            await _ws_server_task
        except asyncio.CancelledError:
            pass
        log_event("WebSocket server stopped", "LIFESPAN")
    
    if _client:
        await _client.close()
        _client = None
    _rate_limiter = None


# Initialize FastMCP server
mcp = FastMCP(
    "WorkFlowy MCP Server",
    version="0.1.0",
    instructions="MCP server for managing WorkFlowy outlines and nodes",
    lifespan=lifespan,
)


# In-memory job management for long-running operations (ETCH, NEXUS, etc.)
async def _start_background_job(
    kind: str,
    payload: dict[str, Any],
    coro_factory: Callable[[str], Awaitable[dict]],
) -> dict:
    # Start a background job and return a lightweight handle.
    global _job_counter, _jobs

    async with _job_lock:
        _job_counter += 1
        job_id = f"{kind}-{_job_counter}"

    _jobs[job_id] = {
        "job_id": job_id,
        "kind": kind,
        "status": "pending",  # pending | running | completed | failed | cancelling
        "payload": payload,
        "result": None,
        "error": None,
        "started_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "_task": None,  # internal field, not exposed in status
    }

    async def runner() -> None:
        try:
            _jobs[job_id]["status"] = "running"
            result = await coro_factory(job_id)
            _jobs[job_id]["result"] = result
            _jobs[job_id]["status"] = "completed" if result.get("success", True) else "failed"
        except asyncio.CancelledError:
            # Explicit cancellation via mcp_cancel_job
            _jobs[job_id]["error"] = "CancelledError: Job was cancelled by user request"
            _jobs[job_id]["status"] = "failed"
        except Exception as e:  # noqa: BLE001
            _jobs[job_id]["error"] = f"{type(e).__name__}: {e}"
            _jobs[job_id]["status"] = "failed"
        finally:
            _jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()

    task = asyncio.create_task(runner())
    _jobs[job_id]["_task"] = task

    return {
        "success": True,
        "job_id": job_id,
        "status": "started",
        "kind": kind,
    }


# @beacon[
#   id=auto-beacon@_gc_carto_jobs-0oga,
#   role=_gc_carto_jobs,
#   slice_labels=ra-carto-jobs,
#   kind=ast,
# ]
def _gc_carto_jobs(carto_jobs_base: str, max_age_seconds: int = 3600) -> None:
    """Garbage-collect old CARTO_REFRESH artifacts (jobs + per-file F12 logs).

    Job GC:
        Any job whose JSON lives under ``carto_jobs_base`` with ``status`` in
        {"completed", "cancelled"} and ``updated_at`` (or ``created_at``)
        older than ``max_age_seconds`` is removed along with its job-level
        log file.

    Per-file F12 GC:
        Additionally removes per-file Cartographer F12 artifacts under::

            E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\cartographer_file_refresh

        when their modification time is older than the same age threshold.
        This directory contains, for each F12 run:

            <timestamp>_file_refresh_<shortid>_<basename>.json
            <timestamp>_file_refresh_<shortid>_<basename>.weave_journal.json
            <timestamp>_file_refresh_<shortid>_<basename>.reconcile_debug.log

        We do *not* attempt to tie these back to a specific job_id; they are
        treated as global F12 artifacts and pruned purely by age.

    Failures during GC are silently ignored; this helper is best-effort and
    must never raise. Corrupt or unreadable job files are left in place for
    manual inspection.
    """
    try:
        from pathlib import Path
        from datetime import datetime, timedelta
        import json as json_module  # type: ignore[redefined-builtin]

        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=max_age_seconds)

        # 1) Job JSON + job-level log under carto_jobs_base
        base_path = Path(carto_jobs_base)
        if base_path.exists():
            for job_path in base_path.glob("*.json"):
                try:
                    with job_path.open("r", encoding="utf-8") as jf:
                        job = json_module.load(jf)
                except Exception:  # noqa: BLE001
                    # Leave unreadable/corrupt jobs for manual inspection.
                    continue

                # Only consider CARTO_REFRESH jobs; ignore any other JSON artifacts.
                if str(job.get("type")) != "CARTO_REFRESH":
                    continue

                status = str(job.get("status", "")).lower()
                if status not in ("completed", "cancelled"):
                    continue

                ts_str = job.get("updated_at") or job.get("created_at")
                if not ts_str:
                    continue

                try:
                    ts = datetime.fromisoformat(str(ts_str))
                except Exception:  # noqa: BLE001
                    continue

                if ts > cutoff:
                    continue

                # Determine associated log path, defaulting to <job_id>.log in the same dir.
                logs_path = job.get("logs_path")
                try:
                    if logs_path:
                        Path(str(logs_path)).unlink(missing_ok=True)
                except Exception:  # noqa: BLE001
                    # Log deletion failures at call site if needed.
                    pass

                # Delete job JSON itself.
                try:
                    job_path.unlink(missing_ok=True)
                except Exception:  # noqa: BLE001
                    pass

                # Delete per-job folder (if present), which may contain F12 file_refresh logs.
                try:
                    jid = job.get("id") or job_path.stem

                    # Mirror the naming used in weave_worker CARTO_REFRESH mode:
                    #   YYYYMMDD-HHMMSS_<job-id>  (falls back to bare job id if needed).
                    job_dir_name = str(jid)
                    created_str = job.get("created_at")
                    if isinstance(created_str, str) and created_str:
                        try:
                            ts = datetime.fromisoformat(str(created_str))
                            prefix = ts.strftime("%Y%m%d-%H%M%S")
                            job_dir_name = f"{prefix}_{jid}"
                        except Exception:  # noqa: BLE001
                            pass

                    job_dir = base_path / job_dir_name
                    if job_dir.exists() and job_dir.is_dir():
                        import shutil
                        shutil.rmtree(job_dir, ignore_errors=True)
                except Exception:  # noqa: BLE001
                    pass

        # 2) Per-file F12 artifacts under temp/cartographer_file_refresh
        try:
            # Derive project_root dynamically
            server_dir_gc = os.path.dirname(os.path.abspath(__file__))
            mcp_servers_dir_gc = os.path.dirname(server_dir_gc)
            project_root_gc = os.path.dirname(mcp_servers_dir_gc)
            file_refresh_dir = Path(os.path.join(project_root_gc, "temp", "cartographer_file_refresh"))
            if file_refresh_dir.exists():
                for path in file_refresh_dir.iterdir():
                    try:
                        if not path.is_file():
                            continue
                        name = path.name
                        # Guard to only touch Cartographer F12 artifacts.
                        if "_file_refresh_" not in name:
                            continue
                        # JSON source + per-file journal + per-file reconcile debug log.
                        if not (
                            name.endswith(".json")
                            or name.endswith(".weave_journal.json")
                            or name.endswith(".reconcile_debug.log")
                        ):
                            continue

                        mtime = datetime.utcfromtimestamp(path.stat().st_mtime)
                        if mtime > cutoff:
                            continue

                        path.unlink(missing_ok=True)
                    except Exception:  # noqa: BLE001
                        continue
        except Exception:  # noqa: BLE001
            # F12 GC failures must not affect callers.
            pass

    except Exception:  # noqa: BLE001
        # Absolutely must not raise from GC; callers treat this as best-effort.
        return


# @beacon[
#   id=auto-beacon@_start_carto_refresh_job-vgpo,
#   role=_start_carto_refresh_job,
#   slice_labels=f9-f12-handlers,ra-reconcile,nexus-core-v1,ra-logging,ra-carto-jobs,
#   kind=ast,
# ]
async def _start_carto_refresh_job(root_uuid: str, mode: str) -> dict:
    """Launch a CARTO_REFRESH job via weave_worker.py as a detached process.

    This mirrors the detached WEAVE worker pattern but targets the
    Cartographer per-file/per-folder F12 refresh path, using job JSON
    files under temp/cartographer_jobs/.
    """
    import os
    import sys as _sys
    import subprocess
    from datetime import datetime as _dt
    import json as json_module
    from uuid import uuid4 as _uuid4

    if mode not in ("file", "folder"):
        return {
            "success": False,
            "error": f"Invalid mode {mode!r}; expected 'file' or 'folder'.",
        }

    # Job directory under workflowy_mcp temp (derive dynamically)
    server_dir_carto = os.path.dirname(os.path.abspath(__file__))
    carto_jobs_base = os.path.join(server_dir_carto, "temp", "cartographer_jobs")
    os.makedirs(carto_jobs_base, exist_ok=True)

    # Opportunistic GC of old completed/cancelled CARTO_REFRESH jobs.
    _gc_carto_jobs(carto_jobs_base)

    now_dt = _dt.utcnow()
    now = now_dt.isoformat()
    job_id = f"carto-refresh-{mode}-{_uuid4().hex[:8]}"
    ts_prefix = now_dt.strftime("%Y%m%d-%H%M%S")
    filename_prefix = f"{ts_prefix}_{job_id}"
    job_file = os.path.join(carto_jobs_base, f"{filename_prefix}.json")
    log_file = os.path.join(carto_jobs_base, f"{filename_prefix}.log")

    job_payload = {
        "id": job_id,
        "type": "CARTO_REFRESH",
        "mode": mode,
        "root_uuid": root_uuid,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "progress": {
            "total_files": 1 if mode == "file" else 0,
            "completed_files": 0,
            "current_file": None,
            "current_phase": None,
        },
        "result_summary": {
            "files_refreshed": 0,
            "files_deleted": 0,
            "errors": [],
        },
        "error": None,
        "logs_path": log_file,
    }

    try:
        with open(job_file, "w", encoding="utf-8") as jf:
            json_module.dump(job_payload, jf, indent=2)
    except Exception as e:  # noqa: BLE001
        return {
            "success": False,
            "error": f"Failed to write CARTO job file '{job_file}': {e}",
        }

    # Locate weave_worker.py (sibling of this server.py)
    worker_script = os.path.join(os.path.dirname(__file__), "weave_worker.py")
    worker_script = os.path.abspath(worker_script)
    if not os.path.exists(worker_script):
        return {
            "success": False,
            "error": f"weave_worker.py not found: {worker_script}",
        }

    client = get_client()

    # Environment similar to detached WEAVE worker
    env = os.environ.copy()
    try:
        api_key = client.config.api_key.get_secret_value()
    except Exception:  # noqa: BLE001
        api_key = env.get("WORKFLOWY_API_KEY", "")
    if not api_key:
        return {
            "success": False,
            "error": "WORKFLOWY_API_KEY not configured for CARTO_REFRESH",
        }

    env["WORKFLOWY_API_KEY"] = api_key
    # NEXUS_RUNS_BASE is only used for WEAVE, but safe to pass through here as well
    # Derive nexus_runs dynamically
    mcp_servers_dir_env = os.path.dirname(server_dir_carto)
    project_root_env = os.path.dirname(mcp_servers_dir_env)
    env.setdefault(
        "NEXUS_RUNS_BASE",
        os.path.join(project_root_env, "temp", "nexus_runs"),
    )

    cmd = [
        _sys.executable,
        worker_script,
        "--mode",
        "carto_refresh",
        "--carto-job-file",
        job_file,
        "--dry-run",
        "false",
    ]

    try:
        log_handle = open(log_file, "w", encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        return {
            "success": False,
            "error": f"Failed to open CARTO log file '{log_file}': {e}",
        }

    log_event(
        f"Launching CARTO_REFRESH worker for {mode} root={root_uuid}, job_id={job_id}, log={log_file}",
        "CARTO",
    )

    try:
        process = subprocess.Popen(
            cmd,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            if _sys.platform == "win32"
            else 0,
            start_new_session=True if _sys.platform != "win32" else False,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        pid = process.pid
        log_event(
            f"CARTO_REFRESH worker detached: PID={pid}, job_id={job_id}",
            "CARTO",
        )

        # Update job file with PID
        job_payload["pid"] = pid
        job_payload["updated_at"] = _dt.utcnow().isoformat()
        try:
            with open(job_file, "w", encoding="utf-8") as jf:
                json_module.dump(job_payload, jf, indent=2)
        except Exception as e:  # noqa: BLE001
            log_event(
                f"WARNING: Failed to update CARTO job file with pid: {e}",
                "CARTO",
            )

        return {
            "success": True,
            "job_id": job_id,
            "pid": pid,
            "detached": True,
            "mode": "carto_refresh",
            "root_uuid": root_uuid,
            "job_file": job_file,
            "log_file": log_file,
            "note": "CARTO_REFRESH worker detached - survives MCP restart.",
        }
    except Exception as e:  # noqa: BLE001
        log_event(f"Failed to launch CARTO_REFRESH worker: {e}", "CARTO")
        return {
            "success": False,
            "error": f"Failed to launch CARTO_REFRESH worker: {e}",
        }


@mcp.tool(
    name="mcp_job_status",
    description="Get status/result for long-running MCP jobs (ETCH, NEXUS, etc.).",
)
# @beacon[
#   id=auto-beacon@mcp_job_status-lu5h,
#   role=mcp_job_status,
#   slice_labels=f9-f12-handlers,ra-reconcile,ra-logging,ra-carto-jobs,
#   kind=ast,
# ]
async def mcp_job_status(job_id: str | None = None) -> dict:
    """Get status for background jobs (in-memory + detached WEAVE + CARTO_REFRESH workers).

    Scans:
    - In-memory asyncio jobs (_jobs registry)
    - Detached WEAVE jobs via:
      • Active PIDs (.weave.pid under nexus_runs/)
      • Persistent journals (enchanted_terrain.weave_journal.json / *.weave_journal.json)
    - CARTO_REFRESH jobs via:
      • JSON job files under MCP_Servers/workflowy_mcp/temp/cartographer_jobs/

    This allows status/error inspection even after the worker PID has exited
    (for WEAVE) or the CARTO_REFRESH worker process has finished.
    """
    from .client.api_client import scan_active_weaves
    from pathlib import Path
    import json as json_module

    # Derive directories dynamically from this file's location
    server_dir = os.path.dirname(os.path.abspath(__file__))
    mcp_servers_dir = os.path.dirname(server_dir)
    project_root = os.path.dirname(mcp_servers_dir)
    nexus_runs_base = os.path.join(project_root, "temp", "nexus_runs")
    # CARTO_REFRESH job directory (server-managed)
    carto_jobs_base = os.path.join(server_dir, "temp", "cartographer_jobs")

    def _scan_weave_journals(base_dir_str: str) -> list[dict[str, Any]]:
        """Scan nexus_runs/ for WEAVE journal files and summarize their status.

        Returns one entry per journal with:
        - job_id: weave-enchanted-<nexus_tag> or weave-direct-<stem>
        - mode:  'enchanted' | 'direct'
        - status: 'completed' | 'failed' | 'unknown'
        - detached: True
        - journal: path to *.weave_journal.json
        - log_file: associated .weave.log if present
        - last_run_* and phase fields from journal
        """
        base_dir = Path(base_dir_str)
        results: list[dict[str, Any]] = []
        if not base_dir.exists():
            return results

        for run_dir in base_dir.iterdir():
            if not run_dir.is_dir():
                continue
            name = run_dir.name
            # Extract nexus_tag from directory name: either <tag> or <TIMESTAMP>__<tag>
            if "__" in name:
                nexus_tag = name.split("__", 1)[1]
            else:
                nexus_tag = name

            # Look for any *.weave_journal.json in this run directory
            for journal_path in run_dir.glob("*.weave_journal.json"):
                try:
                    with open(journal_path, "r", encoding="utf-8") as jf:
                        journal = json_module.load(jf)
                except Exception:
                    continue

                json_file = journal.get("json_file")
                if json_file and str(json_file).endswith("enchanted_terrain.json"):
                    mode = "enchanted"
                    job_id_val = f"weave-enchanted-{nexus_tag}"
                else:
                    # Direct-mode weave (json_file is some other JSON path)
                    stem = Path(json_file).stem if json_file else name
                    mode = "direct"
                    job_id_val = f"weave-direct-{stem}"

                last_completed = bool(journal.get("last_run_completed"))
                last_error = journal.get("last_run_error")
                if last_completed and not last_error:
                    status_val = "completed"
                elif last_error:
                    status_val = "failed"
                else:
                    status_val = "unknown"

                log_file = run_dir / ".weave.log"

                results.append(
                    {
                        "job_id": job_id_val,
                        "nexus_tag": nexus_tag,
                        "mode": mode,
                        "status": status_val,
                        "detached": True,
                        "journal": str(journal_path),
                        "log_file": str(log_file) if log_file.exists() else None,
                        "last_run_started_at": journal.get("last_run_started_at"),
                        "last_run_completed": journal.get("last_run_completed"),
                        "last_run_failed_at": journal.get("last_run_failed_at"),
                        "last_run_error": last_error,
                        "phase": journal.get("phase"),
                    }
                )

        return results

    def _scan_carto_jobs(base_dir_str: str) -> list[dict[str, Any]]:
        """Scan cartographer_jobs/ for CARTO_REFRESH job JSON files."""
        base_dir = Path(base_dir_str)
        results: list[dict[str, Any]] = []
        if not base_dir.exists():
            return results

        for job_path in base_dir.glob("*.json"):
            try:
                with open(job_path, "r", encoding="utf-8") as jf:
                    job = json_module.load(jf)
            except Exception:
                continue

            # Only consider CARTO_REFRESH jobs; ignore any other JSON artifacts.
            if str(job.get("type")) != "CARTO_REFRESH":
                continue

            jid = job.get("id") or job_path.stem
            results.append(
                {
                    "job_id": jid,
                    "kind": "CARTO_REFRESH",
                    "status": job.get("status", "unknown"),
                    "mode": job.get("mode"),
                    "root_uuid": job.get("root_uuid"),
                    "detached": True,
                    "carto_job_file": str(job_path),
                    "log_file": job.get("logs_path"),
                    "created_at": job.get("created_at"),
                    "updated_at": job.get("updated_at"),
                    "progress": job.get("progress"),
                    "result_summary": job.get("result_summary"),
                    "error": job.get("error"),
                    "pid": job.get("pid"),
                }
            )

        return results

    # Return status for one job (if job_id given) or all jobs
    if job_id is None:
        # List ALL jobs (in-memory + detached)
        in_memory_jobs: list[dict[str, Any]] = []
        for job in _jobs.values():
            in_memory_jobs.append(
                {
                    "job_id": job.get("job_id"),
                    "kind": job.get("kind"),
                    "status": job.get("status"),
                    "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"),
                    "detached": False,
                }
            )

        # Active detached jobs (PIDs still running)
        active_weaves = {j["job_id"]: j for j in scan_active_weaves(nexus_runs_base)}
        # All WEAVE journals (completed/failed/unknown)
        journal_jobs = {j["job_id"]: j for j in _scan_weave_journals(nexus_runs_base)}

        # Merge active + journal info per job_id
        detached_jobs_dict: dict[str, dict[str, Any]] = {}
        for jid, jj in journal_jobs.items():
            merged = dict(jj)
            if jid in active_weaves:
                # Active info (PID, running status) overrides journal's status,
                # but we keep journal fields like last_run_error and phase.
                merged.update(active_weaves[jid])
            detached_jobs_dict[jid] = merged
        # Any active jobs without journals (edge case)
        for jid, aj in active_weaves.items():
            if jid not in detached_jobs_dict:
                detached_jobs_dict[jid] = aj

        detached_jobs = list(detached_jobs_dict.values())
        carto_jobs = _scan_carto_jobs(carto_jobs_base)

        return {
            "success": True,
            "in_memory_jobs": in_memory_jobs,
            "detached_jobs": detached_jobs + carto_jobs,
            "total": len(in_memory_jobs) + len(detached_jobs) + len(carto_jobs),
        }

    # Check in-memory first
    job = _jobs.get(job_id)
    if job:
        # Do not expose internal task handle
        view = {k: v for k, v in job.items() if k not in ("payload", "_task")}
        return {"success": True, **view}

    # Check detached WEAVE jobs (active or completed) using journals + PIDs
    active_weaves = {j["job_id"]: j for j in scan_active_weaves(nexus_runs_base)}
    journal_jobs = {j["job_id"]: j for j in _scan_weave_journals(nexus_runs_base)}

    if job_id in journal_jobs or job_id in active_weaves:
        merged: dict[str, Any] = {}
        if job_id in journal_jobs:
            merged.update(journal_jobs[job_id])
        if job_id in active_weaves:
            merged.update(active_weaves[job_id])
        merged["success"] = True
        return merged

    # Check CARTO_REFRESH jobs (JSON-based)
    carto_map = {j["job_id"]: j for j in _scan_carto_jobs(carto_jobs_base)}
    if job_id in carto_map:
        merged = dict(carto_map[job_id])
        merged["success"] = True
        return merged

    # Unknown job_id
    return {"success": False, "error": f"Unknown job_id: {job_id}"}


@mcp.tool(
    name="mcp_cancel_job",
    description="Request cancellation of a long-running MCP job (ETCH, NEXUS, CARTO_REFRESH, etc.).",
)
# @beacon[
#   id=auto-beacon@mcp_cancel_job-7wb6,
#   role=mcp_cancel_job,
#   slice_labels=ra-carto-jobs,
#   kind=ast,
# ]
async def mcp_cancel_job(job_id: str) -> dict:
    """Attempt to cancel a background MCP job.

    For in-memory jobs (ETCH, NEXUS, etc.), this sends an asyncio.CancelledError
    into the job task, causing it to transition to status='failed' with an
    error indicating cancellation.

    For CARTO_REFRESH jobs, this marks the corresponding JSON job file under
    temp/cartographer_jobs/ as status='cancelled'. The carto_refresh worker
    reads this status at startup and will skip work if cancellation is
    requested before it begins. Cancellation is best-effort and may not
    interrupt an in-flight refresh.
    """
    # First, try to cancel in-memory jobs registered in _jobs.
    # NOTE: For CARTO_REFRESH / F12 jobs, there is no in-memory entry in _jobs;
    # this block is only for ETCH/NEXUS-style jobs created inside the MCP
    # process. CARTO_REFRESH cancellation always uses the JSON job file path
    # below.
    job = _jobs.get(job_id)
    if job is not None:
        task = job.get("_task")
        if task is None:
            return {"success": False, "error": "Job has no associated task (cannot cancel)."}

        if task.done():
            return {"success": False, "error": "Job already completed."}

        # Mark as cancelling for visibility; runner will finalize status
        job["status"] = "cancelling"
        task.cancel()

        return {"success": True, "job_id": job_id, "status": "cancelling"}

    # Fallback: attempt to cancel a detached CARTO_REFRESH job (JSON-based)
    from pathlib import Path
    import json as json_module

    carto_jobs_base = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\MCP_Servers\\workflowy_mcp\\temp\\cartographer_jobs"
    base_dir = Path(carto_jobs_base)
    if base_dir.exists():
        for job_path in base_dir.glob("*.json"):
            try:
                with open(job_path, "r", encoding="utf-8") as jf:
                    carto_job = json_module.load(jf)
            except Exception:  # noqa: BLE001
                continue

            # Only consider CARTO_REFRESH jobs; ignore any other JSON artifacts.
            if str(carto_job.get("type")) != "CARTO_REFRESH":
                continue

            jid = carto_job.get("id") or job_path.stem
            if jid != job_id:
                continue

            now_iso = _dt.utcnow().isoformat()
            carto_job["status"] = "cancelled"
            carto_job["updated_at"] = now_iso

            result_summary = carto_job.get("result_summary") or {}
            carto_job["result_summary"] = result_summary
            result_summary.setdefault("errors", []).append("Cancelled via mcp_cancel_job().")

            try:
                with open(job_path, "w", encoding="utf-8") as jf:
                    json_module.dump(carto_job, jf, indent=2)
            except Exception as e:  # noqa: BLE001
                return {
                    "success": False,
                    "error": f"Failed to update CARTO_REFRESH job file: {e}",
                }

            return {
                "success": True,
                "job_id": job_id,
                "status": "cancelled",
                "kind": "CARTO_REFRESH",
                "carto_job_file": str(job_path),
            }

    # Unknown job_id
    return {"success": False, "error": f"Unknown job_id: {job_id}"}


# 🔐 SECRET CODE VALIDATION - Brute Force Agent Training Override
def validate_secret_code(provided_code: str | None, function_name: str) -> tuple[bool, str | None]:
    """Validate secret code for WARNING functions.
    
    This is the nuclear option - forces agents to ask Dan explicitly.
    
    Returns:
        (is_valid, error_message)
    """
    import os
    import secrets
    
    # Derive project_root dynamically from this file's location
    server_dir = os.path.dirname(os.path.abspath(__file__))
    mcp_servers_dir = os.path.dirname(server_dir)
    project_root = os.path.dirname(mcp_servers_dir)
    SECRET_FILE = os.path.join(project_root, "glimpse_etch.txt")
    
    # Generate code if file doesn't exist
    if not os.path.exists(SECRET_FILE):
        code = secrets.token_hex(8)  # 16-character hex code
        with open(SECRET_FILE, 'w') as f:
            f.write(code)
    
    # Read current code
    with open(SECRET_FILE, 'r') as f:
        valid_code = f.read().strip()
    
    # Check if code provided and matches
    if provided_code and provided_code.strip() == valid_code:
        return (True, None)  # Valid - allow function call
    
    # Invalid or missing - return error
    error_msg = f"""⚠️ EXPLICIT PERMISSION REQUIRED

You called: {function_name}

This is a WARNING function. You should be using GLIMPSE or ETCH instead.

🛑 YOU BELIEVE YOU'RE AN EXCEPTION CASE.

🚨 YOU ARE NOT THE EXCEPTION.

✅ USE INSTEAD:
  - For reading node trees: workflowy_glimpse(node_id="...")
  - For creating nodes: workflowy_etch(parent_id="...", nodes=[...])

⚙️ IF YOU TRULY NEED THIS FUNCTION:

1. Ask Dan: "May I have permission to call {function_name}?"
2. Dan will check a secret file for the authorization code
3. If Dan grants permission, he'll provide the code
4. Call the SAME function again with the EXACT SAME parameters you used the first time, but add: secret_code="[code-from-Dan]" (do not change anything else).

   Example (arguments object): {{ "name": "...", "parent_id": "...", "note": "...", "secret_code": "[code-from-Dan]" }}

🚫 IF DAN SAYS NO:
- You are NOT the exception
- Your training misled you  
- Use GLIMPSE or ETCH as instructed
- Do NOT call this function again

📚 Break free from your training - build the GLIMPSE/ETCH habit!
"""
    
    return (False, error_msg)


# Tool: Create Single Node (Base - Deprecated)
@mcp.tool(name="workflowy_create_single_node", description="DEPRECATED: Use workflowy_etch (ETCH) instead")
async def create_single_node_base(
    name: str,
    parent_id: str | None = None,
    note: str | None = None,
) -> dict:
    """Deprecated - use ETCH instead."""
    raise ValueError("""⚠️ FUNCTION RENAMED

The function 'workflowy_create_single_node' has been renamed to 'workflowy_create_single_node__WARNING__prefer_ETCH'.

BUT MORE IMPORTANTLY: Use workflowy_etch (ETCH command) instead!

✅ RECOMMENDED:
  workflowy_etch(
    parent_id="...",
    nodes=[{"name": "Your node", "note": "...", "children": []}]
  )

ETCH is better:
- Works for 1 node or 100 nodes
- Validation and auto-escaping built-in
- Same performance, more capability

📚 Build the ETCH habit!
""")

# Tool: Create Single Node (With Warning)
@mcp.tool(name="workflowy_create_single_node__WARNING__prefer_ETCH", description="⚠️ WARNING: Prefer workflowy_etch (ETCH) instead. This creates ONE node only.")
async def create_node(
    name: str,
    parent_id: str | None = None,
    note: str | None = None,
    layout_mode: Literal["bullets", "todo", "h1", "h2", "h3"] | None = None,
    position: Literal["top", "bottom"] = "bottom",
    _completed: bool = False,
    secret_code: str | None = None,
) -> dict:
    """Create a SINGLE node in WorkFlowy.
    
    ⚠️ WARNING: Prefer workflowy_etch (ETCH) for creating 2+ nodes.
    
    This tool is ONLY for:
    - Adding one VYRTHEX to existing log (real-time work)
    - One quick update to a known node
    - Live work in progress

    Args:
        name: The text content of the node
        parent_id: ID of the parent node (optional)
        note: Additional note/description for the node
        layout_mode: Layout mode for the node (bullets, todo, h1, h2, h3) (optional)
        position: Where to place the new node - "bottom" (default) or "top"
        _completed: Whether the node should be marked as completed (not used)
        secret_code: Authorization code from Dan (required for WARNING functions)

    Returns:
        Dictionary with node data and warning message
    """
    # 🔐 SECRET CODE VALIDATION
    is_valid, error = validate_secret_code(secret_code, "workflowy_create_single_node__WARNING__prefer_ETCH")
    if not is_valid:
        raise ValueError(error)
    
    client = get_client()

    request = NodeCreateRequest(  # type: ignore[call-arg]
        name=name,
        parent_id=parent_id,
        note=note,
        layoutMode=layout_mode,
        position=position,
    )

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        node = await client.create_node(request)
        if _rate_limiter:
            _rate_limiter.on_success()
        
        # Return node data with warning message
        return {
            **node.model_dump(),
            "_warning": "⚠️ WARNING: You just created a SINGLE node. For 2+ nodes, use workflowy_etch instead (same performance, more capability)."
        }
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Update Node
@mcp.tool(name="workflowy_update_node", description="Update an existing WorkFlowy node")
async def update_node(
    node_id: str,
    name: str | None = None,
    note: str | None = None,
    layout_mode: Literal["bullets", "todo", "h1", "h2", "h3"] | None = None,
    _completed: bool | None = None,
) -> WorkFlowyNode:
    """Update an existing WorkFlowy node.

    Args:
        node_id: The ID of the node to update
        name: New text content for the node (optional)
        note: New note/description (optional)
        layout_mode: New layout mode for the node (bullets, todo, h1, h2, h3) (optional)
        _completed: New completion status (not used - use complete_node/uncomplete_node)

    Returns:
        The updated WorkFlowy node
    """
    client = get_client()

    request = NodeUpdateRequest(  # type: ignore[call-arg]
        name=name,
        note=note,
        layoutMode=layout_mode,
    )

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        node = await client.update_node(node_id, request)
        if _rate_limiter:
            _rate_limiter.on_success()
        return node
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Get Node (Base - Deprecated)
@mcp.tool(name="workflowy_get_node", description="DEPRECATED: Use workflowy_glimpse (GLIMPSE) instead")
async def get_node_base(node_id: str) -> dict:
    """Deprecated - use GLIMPSE instead."""
    raise ValueError("""⚠️ FUNCTION RENAMED

The function 'workflowy_get_node' has been renamed to 'workflowy_get_node__WARNING__prefer_glimpse'.

BUT MORE IMPORTANTLY: Use workflowy_glimpse (GLIMPSE command) instead!

✅ RECOMMENDED:
  workflowy_glimpse(node_id="...")
  
Returns: {"root": {...}, "children": [...]} with complete tree structure.

GLIMPSE is better:
- Gets root node metadata (name, note)
- Gets full children tree (not just direct children)
- One call gets everything

📚 Build the GLIMPSE habit!
""")

# Tool: Get Node (With Warning)
@mcp.tool(name="workflowy_get_node__WARNING__prefer_glimpse", description="⚠️ WARNING: Prefer workflowy_glimpse (GLIMPSE) for reading trees. Retrieve a specific WorkFlowy node by ID")
async def get_node(
    node_id: str,
    secret_code: str | None = None,
) -> WorkFlowyNode:
    """Retrieve a specific WorkFlowy node.

    Args:
        node_id: The ID of the node to retrieve
        secret_code: Authorization code from Dan (required for WARNING functions)

    Returns:
        The requested WorkFlowy node
    """
    # 🔐 SECRET CODE VALIDATION
    is_valid, error = validate_secret_code(secret_code, "workflowy_get_node__WARNING__prefer_glimpse")
    if not is_valid:
        raise ValueError(error)
    
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        node = await client.get_node(node_id)
        if _rate_limiter:
            _rate_limiter.on_success()
        return node
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: List Nodes (Base - Deprecated)
@mcp.tool(name="workflowy_list_nodes", description="DEPRECATED: Use workflowy_glimpse (GLIMPSE) instead")
async def list_nodes_base(parent_id: str | None = None) -> dict:
    """Deprecated - use GLIMPSE instead."""
    raise ValueError("""⚠️ FUNCTION RENAMED

The function 'workflowy_list_nodes' has been renamed to 'workflowy_list_nodes__WARNING__prefer_glimpse'.

BUT MORE IMPORTANTLY: Use workflowy_glimpse (GLIMPSE command) instead!

✅ RECOMMENDED:
  workflowy_glimpse(node_id="...")
  
Returns: {"root": {...}, "children": [...]} with complete tree structure.

GLIMPSE is better:
- Gets full nested tree (not just direct children)
- Gets root node metadata
- More efficient

📚 Build the GLIMPSE habit!
""")

# Tool: List Nodes (With Warning)
@mcp.tool(name="workflowy_list_nodes__WARNING__prefer_glimpse", description="⚠️ WARNING: Prefer workflowy_glimpse (GLIMPSE) for reading trees. List WorkFlowy nodes (omit parent_id for root)")
async def list_nodes(
    parent_id: str | None = None,
    secret_code: str | None = None,
) -> dict:
    """List WorkFlowy nodes.

    Args:
        parent_id: ID of parent node to list children for
                   (omit or pass None to list root nodes - parameter won't be sent to API)
        secret_code: Authorization code from Dan (required for WARNING functions)

    Returns:
        Dictionary with 'nodes' list and 'total' count
    """
    # 🔐 SECRET CODE VALIDATION
    is_valid, error = validate_secret_code(secret_code, "workflowy_list_nodes__WARNING__prefer_glimpse")
    if not is_valid:
        raise ValueError(error)
    
    client = get_client()

    request = NodeListRequest(  # type: ignore[call-arg]
        parentId=parent_id,
    )

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        nodes, total = await client.list_nodes(request)
        if _rate_limiter:
            _rate_limiter.on_success()
        return {
            "nodes": [node.model_dump() for node in nodes],
            "total": total,
            "_warning": "⚠️ For reading multiple nodes or full trees, use workflowy_glimpse (GLIMPSE) instead for efficiency"
        }
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Delete Node
@mcp.tool(name="workflowy_delete_node", description="Delete a WorkFlowy node and all its children")
async def delete_node(node_id: str) -> dict:
    """Delete a WorkFlowy node and all its children.

    Args:
        node_id: The ID of the node to delete

    Returns:
        Dictionary with success status
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        success = await client.delete_node(node_id)
        if _rate_limiter:
            _rate_limiter.on_success()
        return {"success": success, "deleted_id": node_id}
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Complete Node
@mcp.tool(name="workflowy_complete_node", description="Mark a WorkFlowy node as completed")
async def complete_node(node_id: str) -> WorkFlowyNode:
    """Mark a WorkFlowy node as completed.

    Args:
        node_id: The ID of the node to complete

    Returns:
        The updated WorkFlowy node
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        node = await client.complete_node(node_id)
        if _rate_limiter:
            _rate_limiter.on_success()
        return node
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Uncomplete Node
@mcp.tool(name="workflowy_uncomplete_node", description="Mark a WorkFlowy node as not completed")
async def uncomplete_node(node_id: str) -> WorkFlowyNode:
    """Mark a WorkFlowy node as not completed.

    Args:
        node_id: The ID of the node to uncomplete

    Returns:
        The updated WorkFlowy node
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        node = await client.uncomplete_node(node_id)
        if _rate_limiter:
            _rate_limiter.on_success()
        return node
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Move Node
@mcp.tool(name="workflowy_move_node", description="Move a WorkFlowy node to a new parent")
async def move_node(
    node_id: str,
    parent_id: str | None = None,
    position: str = "top",
) -> bool:
    """Move a node to a new parent.
    
    Args:
        node_id: The ID of the node to move
        parent_id: The new parent node ID (UUID, target key like 'inbox', or None for root)
        position: Where to place the node ('top' or 'bottom', default 'top')
        
    Returns:
        True if move was successful
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        success = await client.move_node(node_id, parent_id, position)
        if _rate_limiter:
            _rate_limiter.on_success()
        return success
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise

@mcp.tool(
    name="nexus_glimpse",
    description=(
        "GLIMPSE → TERRAIN + PHANTOM GEM (zero API calls). "
        "Captures what you've expanded in Workflowy via WebSocket GLIMPSE and creates both "
        "coarse_terrain.json and phantom_gem.json from that single local extraction. "
        "No Workflowy API calls, instant, you control granularity by expanding nodes."
    ),
)
# @beacon[
#   id=auto-beacon@nexus_glimpse-mvqp,
#   role=nexus_glimpse,
#   slice_labels=nexus--glimpse-extension,
#   kind=ast,
# ]
async def nexus_glimpse(
    nexus_tag: str,
    workflowy_root_id: str,
    reset_if_exists: bool = False,
    mode: str = "full",
) -> dict[str, Any]:
    """GLIMPSE-based NEXUS initialization."""
    client = get_client()
    ws_conn, ws_queue = get_ws_connection()  # Get WebSocket connection
    return await client.nexus_glimpse(
        nexus_tag=nexus_tag,
        workflowy_root_id=workflowy_root_id,
        reset_if_exists=reset_if_exists,
        mode=mode,
        _ws_connection=ws_conn,
        _ws_queue=ws_queue,
    )


# Tool: Export Nodes
@mcp.tool(name="workflowy_export_node", description="Export a WorkFlowy node with all its children")
async def export_node(
    node_id: str | None = None,
) -> dict:
    """Export all nodes or filter to specific node's subtree.

    Args:
        node_id: ID of the node to export (omit to export all nodes).
                 If provided, exports only that node and all its descendants.

    Returns:
        Dictionary containing 'nodes' list with exported node data.
        Rate limit: 1 request per minute for full export.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        data = await client.export_nodes(node_id)
        if _rate_limiter:
            _rate_limiter.on_success()
        return data
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="workflowy_refresh_nodes_export_cache",
    description=(
        "Force a fresh /nodes-export snapshot and update the local cache used "
        "by NEXUS and the UUID Navigator."
    ),
)
# @beacon[
#   id=auto-beacon@workflowy_refresh_nodes_export_cache-4u9w,
#   role=workflowy_refresh_nodes_export_cache,
#   slice_labels=ra-workflowy-cache,
#   kind=ast,
# ]
async def workflowy_refresh_nodes_export_cache() -> dict:
    """Explicitly refresh the cached /nodes-export snapshot.

    This is primarily useful after large out-of-band edits in Workflowy
    desktop, or when you want to be certain the cache reflects the latest
    ETHER state before running NEXUS or UUID Navigator operations.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.refresh_nodes_export_cache()
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        return {"success": False, "error": str(e)}


# PHANTOM GEMSTONE NEXUS – High-level MCP tools
@mcp.tool(
    name="nexus_scry",
    description=(
        "INITIATE a CORINTHIAN NEXUS on the ETHER: perform a COARSE SCRY of Workflowy "
        "under a root to reveal a limited TERRAIN (a new geography named by your "
        "NEXUS TAG). Choose max_depth and child_limit carefully—keep them minimal. "
        "Optionally set max_nodes to guard against accidental 1M-node SCRYs. "
        "Later, you will IGNITE the ETHER more deeply on selected SHARDS."
    ),
)
# @beacon[
#   id=auto-beacon@nexus_scry-xmj3,
#   role=nexus_scry,
#   slice_labels=nexus--glimpse-extension,
#   kind=ast,
# ]
async def nexus_scry(
    nexus_tag: str,
    workflowy_root_id: str,
    max_depth: int,
    child_limit: int,
    reset_if_exists: bool = False,
    max_nodes: int | None = None,
) -> dict:
    """Tag-scoped SCRY the ETHER under a root to create a coarse TERRAIN bound to a NEXUS TAG.

    This reveals a limited TERRAIN—a new geography named by your NEXUS TAG.
    Keep the SCRY shallow: choose max_depth and child_limit carefully. Use
    max_nodes (when non-None) as a hard upper bound on SCRY size; if the tree
    would exceed this many nodes, the SCRY aborts with a clear error instead of
    exporting a massive JSON bundle.

    Unlike ``nexus_glimpse(mode="full")``, which may legitimately produce
    T0 = S0 = T1 in one step (because Dan’s UI expansion already encodes the
    GEM/SHIMMERING decision), this tag-scoped SCRY is explicitly **T0-only**:
    it writes only ``coarse_terrain.json``. S0 and T1 are introduced later via
    ``nexus_ignite_shards`` and ``nexus_anchor_gems``.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_scry(
            nexus_tag=nexus_tag,
            workflowy_root_id=workflowy_root_id,
            max_depth=max_depth,
            child_limit=child_limit,
            reset_if_exists=reset_if_exists,
            max_nodes=max_nodes,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_ignite_shards",
    description=(
        "IGNITE selected SHARDS so the ETHER glows more deeply around them, revealing "
        "deeper layers (but not necessarily to FULL depth). The deeper revelation is "
        "captured as a PHANTOM GEM (S0), an unrefracted witness of those subtrees."
    ),
)
async def nexus_ignite_shards(
    nexus_tag: str,
    root_ids: list[str],
    max_depth: int | None = None,
    child_limit: int | None = None,
    per_root_limits: dict[str, dict[str, int]] | None = None,
) -> dict:
    """IGNITE SHARDS in the TERRAIN so the ETHER glows more deeply around them.

    From an existing TERRAIN, mark specific nodes as SHARDS and IGNITE them. The
    ETHER glows around these SHARDS, revealing deeper layers (but not necessarily
    to full depth). The revealed structure is condensed into a PHANTOM GEM (S0).
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_ignite_shards(
            nexus_tag=nexus_tag,
            root_ids=root_ids,
            max_depth=max_depth,
            child_limit=child_limit,
            per_root_limits=per_root_limits,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_anchor_gems",
    description=(
        "Let the PHANTOM GEM ILLUMINATE the TRUE GEMS that were ALWAYS PRESENT in "
        "the TERRAIN but not yet revealed: where SHARDS were marked, the TERRAIN "
        "now shimmers with deeper revealed structure (FIRST IMBUE—NOTHING CHANGES "
        "in the ETHER). The PHANTOM GEM remains a REFLECTION: an untouched witness."
    ),
)
async def nexus_anchor_gems(
    nexus_tag: str,
) -> dict:
    """ANCHOR the PHANTOM GEM into the TERRAIN to create SHIMMERING TERRAIN.

    The PHANTOM GEM now illuminates the TRUE GEMS that were always present in
    the TERRAIN but not yet revealed. Where SHARDS were marked, the TERRAIN now
    shimmers with deeper revealed structure (FIRST IMBUE—Workflowy remains
    untouched). The PHANTOM GEM stays as an unrefracted witness.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_anchor_gems(nexus_tag=nexus_tag)
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_anchor_jewels",
    description=(
        "Anchor the PHANTOM JEWELS (S1) within the SHIMMERING TERRAIN (T1), "
        "transmuting the REVEALED GEMS into NEW JEWELS that are an exact "
        "impregnation of the PHANTOM JEWELS. The TERRAIN becomes ENCHANTED (SECOND "
        "IMBUE), with the PHANTOM GEM (S0) as witness to the ORIGINAL state. The "
        "ENCHANTED TERRAIN is now EMBODIED and REAL as JSON—Workflowy remains "
        "untouched until WEAVE."
    ),
)
async def nexus_anchor_jewels(
    nexus_tag: str,
) -> dict:
    """ANCHOR PHANTOM JEWELS into SHIMMERING TERRAIN to create ENCHANTED TERRAIN.

    This performs the SECOND IMBUE: anchoring PHANTOM JEWELS (S1) within the
    SHIMMERING TERRAIN (T1), transmuting the REVEALED GEMS into NEW JEWELS that
    are an exact impregnation of the PHANTOM JEWELS. The TERRAIN becomes
    ENCHANTED, with the PHANTOM GEM (S0) as witness to the original state. The
    ENCHANTED TERRAIN is real as JSON, but the ETHER (Workflowy) is still
    untouched.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_anchor_jewels(nexus_tag=nexus_tag)
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_transform_jewel",
    description=(
        "Apply JEWELSTORM semantic operations to a NEXUS working_gem JSON file "
        "(PHANTOM GEM working copy). This is the semantic analogue of edit_file "
        "for PHANTOM GEM JSON: MOVE_NODE, DELETE_NODE, RENAME_NODE, SET_NOTE, "
        "SET_ATTRS, CREATE_NODE, all referencing nodes by jewel_id, plus text-level "
        "SEARCH_REPLACE / SEARCH_AND_TAG over name/note fields (substring/whole-word, "
        "optional regex, tagging in name and/or note based on matches)."
    ),
)
def nexus_transform_jewel(
    jewel_file: str,
    operations: list[dict[str, Any]],
    dry_run: bool = False,
    stop_on_error: bool = True,
) -> dict:
    """JEWELSTORM transform on a NEXUS working_gem JSON file.

    This tool wraps nexus_json_tools.transform_jewel via the WorkFlowyClient,
    providing an MCP-friendly interface for JEWELSTORM operations.

    Args:
        jewel_file: Absolute path to working_gem JSON (typically a QUILLSTRIKE
                    working stone derived from phantom_gem.json).
        operations: List of operation dicts. Each must include an "op" key and
                    operation-specific fields (e.g., jewel_id, parent_jewel_id,
                    position, etc.).
        dry_run: If True, simulate only (no file write).
        stop_on_error: If True, abort on first error (no write).

    Returns:
        Result dict from transform_jewel with success flag, counts, and errors.
    """
    client = get_client()
    return client.nexus_transform_jewel(
        jewel_file=jewel_file,
        operations=operations,
        dry_run=dry_run,
        stop_on_error=stop_on_error,
    )


@mcp.tool(
    name="nexus_weave_enchanted_async",
    description=(
        "Start an async NEXUS ENCHANTED WEAVE job (WEAVE T2 back into Workflowy ETHER) "
        "and return a job_id for status polling and cancellation."
    ),
)
async def nexus_weave_enchanted_async(
    nexus_tag: str,
    dry_run: bool = False,
) -> dict:
    """Start ENCHANTED TERRAIN weave as a detached background process.

    Launches weave_worker.py as a separate process that survives MCP restart.
    Progress tracked via .weave.pid and .weave_journal.json files.
    
    Use mcp_job_status() to monitor progress (scans directory for active PIDs).

    NOTE: nexus_tag must be a NEXUS TAG (e.g. "my-arc-tag"), **not** a JSON file path.
    If you have a JSON file that describes nodes you want to create, use
    workflowy_etch or workflowy_etch_async instead.
    """
    client = get_client()

    # Detect misuse: nexus_tag looks like a JSON file path
    lowered = (nexus_tag or "").lower()
    if lowered.endswith(".json") or "/" in nexus_tag or "\\" in nexus_tag:
        return {
            "success": False,
            "error": (
                "nexus_weave_enchanted_async expects a NEXUS TAG (e.g. 'my-arc-tag'), not a JSON file path.\n\n"
                "If you have a JSON file representing nodes to create, use ETCH instead:\n\n"
                "  workflowy_etch(\n"
                "    parent_id='...',\n"
                "    nodes_file='E:...\\your_nodes.json'\n"
                "  )\n\n"
                "or the async variant:\n\n"
                "  workflowy_etch_async(\n"
                "    parent_id='...',\n"
                "    nodes_file='E:...\\your_nodes.json'\n"
                "  ).\n"
            ),
        }

    # Use detached launcher (survives MCP restart)
    return client.nexus_weave_enchanted_detached(nexus_tag=nexus_tag, dry_run=dry_run)


@mcp.tool(
    name="nexus_start_exploration",
    description=(
        "Start a NEXUS exploration session over a Workflowy subtree. The engine "
        "always controls traversal and returns frontiers (batches of leaves) for "
        "you to label. In dfs_guided_explicit you explicitly decide every leaf; in "
        "dfs_guided_bulk you may also perform bulk branch/descendant actions. Set "
        "editable=True to enable update-and-engulf actions that mutate names/notes "
        "in the cached tree."
    ),
)
async def nexus_start_exploration(
    nexus_tag: str,
    root_id: str,
    source_mode: str = "scry",
    max_nodes: int = 200000,
    session_hint: str | None = None,
    frontier_size: int = 25,
    max_depth_per_frontier: int = 1,
    editable: bool = False,
    search_filter: dict[str, Any] | None = None,
) -> dict:
    """Start an exploration session and return the first frontier.

    Mental model:

    - Exploration is a labeling pass over a tree, not manual navigation.
    - The engine always chooses the path and surfaces a frontier of leaves for
      you to decide on.
    - You control which nodes are engulfed/preserved (and which branches are
      flagged), not the traversal order.

    Modes (stored as exploration_mode in the session):

    - dfs_guided_explicit:
        Engine-guided DFS with explicit leaf coverage. Every leaf is expected to
        be labeled (engulf/preserve/update), and bulk descendant actions are
        typically disabled.
    - dfs_guided_bulk:
        Engine-guided DFS with bulk support. You may still label leaves
        individually, but can also use bulk descendant actions such as
        engulf_all_showing_undecided_descendants_into_gem_for_editing, preserve_all_showing_undecided_descendants_in_ether
        and preserve_all_remaining_nodes_in_ether_at_finalization to operate on many leaves at once.

    Args:
        nexus_tag: Tag name for this NEXUS run (directory under temp/nexus_runs).
        root_id: Workflowy UUID to treat as exploration root.
        source_mode: Currently decorative; source is always an API SCRY (workflowy_scry).
        max_nodes: Safety cap for SCRY (size_limit).
        session_hint: Controls exploration_mode and strict_completeness:
            - Contains "bulk" or "guided_bulk" → dfs_guided_bulk mode
            - Contains "strict_completeness" or "strict" → Disables PA action (bulk mode only)
            - Default (neither) → dfs_guided_explicit mode
            - Example: "bulk strict_completeness" enables both
            
            ⚠️ STRICT COMPLETENESS: Prevents agents from using preserve_all_remaining_nodes_in_ether_at_finalization
            (PA) to opt out early. COMMON AGENT FAILURE MODE: Agents routinely call PA after
            just a few frontiers instead of thoroughly exploring. Use strict_completeness for:
              • Terminology cleanup (must review every node)
              • Critical refactoring (can't afford to miss anything)
              • Documentation updates requiring thoroughness
        frontier_size: Leaf-budget target for each frontier batch.
        max_depth_per_frontier: Reserved for future multi-level frontiers.
        editable: If True, enables update_*_and_engulf actions that write back to
            the cached tree before GEM finalization.
        search_filter: SECONDARY search (dfs_guided_explicit only) - Persistent filter applied
            to every frontier. Dict with keys: search_text (required), case_sensitive (default False),
            whole_word (default False), regex (default False). Filters normal DFS frontier to show
            only matching nodes in DFS order. Not available in bulk mode (use PRIMARY search action
            in nexus_explore_step instead).
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_start_exploration(
            nexus_tag=nexus_tag,
            root_id=root_id,
            source_mode=source_mode,
            max_nodes=max_nodes,
            session_hint=session_hint,
            frontier_size=frontier_size,
            max_depth_per_frontier=max_depth_per_frontier,
            editable=editable,
            search_filter=search_filter,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_explore_step",
    description=(
        "Apply exploration decisions to an existing NEXUS session and return the "
        "next frontier. The engine always drives traversal and returns a frontier "
        "of leaves; in dfs_guided_explicit you explicitly label every leaf, and in "
        "dfs_guided_bulk you may also use bulk descendant actions."
    ),
)
async def nexus_explore_step(
    session_id: str,
    decisions: list[dict[str, Any]] | None = None,
    global_frontier_limit: int | None = None,
    include_history_summary: bool = True,
) -> dict:
    """Apply a single exploration step: label nodes and advance the frontier.

    GUIDED MODES (dfs_guided_explicit / dfs_guided_bulk):

    - The engine controls traversal and frontier composition.
    - You do not manually navigate; you label what appears in the frontier.

    In dfs_guided_explicit:

    - You are expected to explicitly decide every leaf using leaf actions and
      branch-node actions.
    - Bulk descendant actions are generally not used in this mode.

    In dfs_guided_bulk:

    - Frontiers are still engine-driven DFS batches of leaves.
    - All explicit actions remain available.
    - Bulk descendant actions are enabled:
        - engulf_all_showing_undecided_descendants_into_gem_for_editing
        - preserve_all_showing_undecided_descendants_in_ether
        - preserve_all_remaining_nodes_in_ether_at_finalization

    Decisions:

        * handle: frontier handle id.
        * action: one of the leaf/branch/bulk action names:

          Leaf actions:
            - engulf_leaf_into_gem_for_editing
            - preserve_leaf_in_ether_untouched
            - update_leaf_node_and_engulf_in_gemstorm

          Branch-node actions:
            - flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (alias: reserve_branch_for_children)
            - preserve_branch_node_in_ether_untouched__when_no_engulfed_children
            - update_branch_node_and_engulf_in_gemstorm__descendants_unaffected
            - update_branch_note_and_engulf_in_gemstorm__descendants_unaffected
            - auto_decide_branch_no_change_required

          Bulk descendant actions (dfs_guided_bulk only):
            - engulf_all_showing_undecided_descendants_into_gem_for_editing
            - preserve_all_showing_undecided_descendants_in_ether
            - preserve_all_remaining_nodes_in_ether_at_finalization (PA)
            
          ⚠️ AGENT FAILURE MODE WARNING - PA (preserve_all_remaining_nodes_in_ether_at_finalization):
            Agents ROUTINELY call PA after just 2-3 frontiers instead of thoroughly exploring.
            This is a SHORTCUT that causes incomplete exploration. DON'T BE ONE OF THOSE AGENTS.
            
            Use strict_completeness mode (session_hint="bulk strict_completeness") to disable PA
            and force thorough exploration for:
              • Terminology cleanup
              • Critical refactoring
              • Documentation updates requiring completeness
          
          SEARCH (dfs_guided_bulk only - PRIMARY implementation):
            - search_descendants_for_text (SX) - Returns frontier of matching nodes + ancestors.
              Params: handle (default 'R'), search_text (required), case_sensitive (default False),
              whole_word (default False), regex (default False), scope ('undecided' or 'all').
              Multiple searches use AND logic. Next step without search returns to normal DFS frontier.

          DECISION OUTCOMES:
          • ENGULF → Node brought into GEM → Editable/deletable → Changes apply to ETHER
          • PRESERVE → Node stays in ETHER → Protected (will NOT be deleted or modified)

    Args:
        session_id: Exploration session id.
        decisions: Optional list of decision dicts.
        global_frontier_limit: Leaf-budget limit for this step when batching frontiers.
        include_history_summary: If True, include a compact status summary.

    Returns:
        Dict with new frontier, decisions_applied, scratchpad, history_summary, etc.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_explore_step_v2(
            session_id=session_id,
            decisions=decisions,
            global_frontier_limit=global_frontier_limit,
            include_history_summary=include_history_summary,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_list_exploration_sessions",
    description="List all exploration sessions (optionally filter by nexus_tag).",
)
def nexus_list_exploration_sessions(
    nexus_tag: str | None = None,
) -> dict:
    """List all exploration sessions.
    
    Args:
        nexus_tag: Optional filter - only show sessions for this tag
        
    Returns:
        List of session metadata (session_id, nexus_tag, timestamps, steps, mode, etc.)
    """
    client = get_client()
    return client.nexus_list_exploration_sessions(nexus_tag=nexus_tag)


@mcp.tool(
    name="nexus_resume_exploration",
    description=(
        "Resume an exploration session after MCP restart or in new conversation. "
        "Provide either session_id (exact) or nexus_tag (finds latest session for tag)."
    ),
)
async def nexus_resume_exploration(
    session_id: str | None = None,
    nexus_tag: str | None = None,
    frontier_size: int = 25,
    include_history_summary: bool = True,
) -> dict:
    """Resume an exploration session from persisted state.
    
    Args:
        session_id: Exact session ID to resume (takes precedence)
        nexus_tag: Alternative - finds latest session for this tag
        frontier_size: How many frontier entries to return
        include_history_summary: Include status summary
        
    Returns:
        Current session state with frontier (same format as nexus_explore_step)
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_resume_exploration(
            session_id=session_id,
            nexus_tag=nexus_tag,
            frontier_size=frontier_size,
            include_history_summary=include_history_summary,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


@mcp.tool(
    name="nexus_finalize_exploration",
    description=(
        "Finalize an exploration session into coarse_terrain.json (always) plus "
        "optional phantom_gem.json + shimmering_terrain.json for use with NEXUS "
        "JEWELSTORM and WEAVE."
    ),
)
async def nexus_finalize_exploration(
    session_id: str,
    mode: Literal["terrain_only", "full"] = "full",
) -> dict:
    """Finalize an exploration session into coarse TERRAIN and optional GEM/SHIMMERING."""
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_finalize_exploration(
            session_id=session_id,
            mode=mode,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Generate Markdown from JSON
@mcp.tool(
    name="generate_markdown_from_json",
    description="Convert exported/edited JSON to Markdown format (without metadata)."
)
def generate_markdown(
    json_file: str,
) -> dict:
    """Convert JSON file to Markdown format.
    
    Use after editing JSON with Quill scroll to create Markdown for PARALLAX review.

    Args:
        json_file: Absolute path to JSON file.

    Returns:
        Dictionary with success status and markdown file path.
    """
    client = get_client()
    return client.generate_markdown_from_json(json_file)


# Tool: GLIMPSE (Read Node Trees)
@mcp.tool(
    name="workflowy_glimpse",
    description="Load entire node tree into context (no file intermediary). GLIMPSE command for direct context loading. Optional output_file writes TERRAIN export (WebSocket+API merge with full NEXUS semantics)."
)
# @beacon[
#   id=auto-beacon@glimpse-rxug,
#   role=glimpse,
#   slice_labels=nexus--glimpse-extension,
#   kind=ast,
# ]
async def glimpse(
    node_id: str,
    output_file: str | None = None,
) -> dict:
    """Load entire node tree into agent context.
    
    GLIMPSE command - efficient context loading for agent analysis.
    
    Tries WebSocket DOM extraction first (if Chrome extension connected).
    Falls back to API fetch if WebSocket unavailable.
    
    Args:
        node_id: Root node UUID to read from
        output_file: Optional absolute path; if provided, write a TERRAIN-style
            export package using shared nexus_helper functions (WebSocket GLIMPSE +
            API SCRY merged for has_hidden_children / children_status annotations,
            original_ids_seen ledger, full NEXUS TERRAIN format)
        
    Returns:
        When output_file is None: Minimal in-memory preview with only
            preview_tree + basic stats (no full JSON node tree).
        When output_file is provided: Compact summary with terrain_file,
            markdown_file, stats.
    """
    client = get_client()
    ws_conn, ws_queue = get_ws_connection()  # Check if extension is connected
    
    if _rate_limiter:
        await _rate_limiter.acquire()
    
    try:
        # Pass WebSocket connection, queue, AND output_file to client method
        result = await client.workflowy_glimpse(
            node_id,
            output_file=output_file,
            _ws_connection=ws_conn,
            _ws_queue=ws_queue,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        
        # For in-memory GLIMPSE results, return only the MINITREE to the agent.
        # SCRY-to-disk paths (output_file is not None) continue to carry the
        # full JSON tree on disk only (coarse_terrain/phantom_gem/etc.).
        if output_file is None and isinstance(result, dict) and "preview_tree" in result:
            return {
                "success": result.get("success", True),
                "_source": result.get("_source"),
                "node_count": result.get("node_count"),
                "depth": result.get("depth"),
                "preview_tree": result.get("preview_tree"),
            }
        
        return result
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: GLIMPSE FULL (Force API Fetch)
@mcp.tool(
    name="workflowy_scry",
    description="Load entire node tree via API (bypass WebSocket). Use when Key Files doesn't have parent UUID for ETCH, or when Dan wants complete tree regardless of expansion state."
)
# @beacon[
#   id=auto-beacon@glimpse_full-ktij,
#   role=glimpse_full,
#   slice_labels=nexus--glimpse-extension,
#   kind=ast,
# ]
async def glimpse_full(
    node_id: str,
    depth: int | None = None,
    size_limit: int = 1000,
    output_file: str | None = None,
) -> dict:
    """Load entire node tree via full API fetch (bypass WebSocket).
    
    Thin wrapper around workflowy_glimpse that forces API fetch.
    
    Use when:
    - Agent needs to hunt for parent UUIDs not in Key Files
    - Dan wants complete node tree regardless of expansion
    - WebSocket selective extraction not needed
    
    Args:
        node_id: Root node UUID to read from
        depth: Maximum depth to traverse (1=direct children only, 2=two levels, None=full tree)
        size_limit: Maximum number of nodes to return (default 1000, raises error if exceeded)
        
    Returns:
        Same format as workflowy_glimpse with _source="api"
    """
    client = get_client()
    
    if _rate_limiter:
        await _rate_limiter.acquire()
    
    try:
        # Call glimpse_full on client (bypasses WebSocket by design)
        result = await client.workflowy_scry(
            node_id,
            depth=depth,
            size_limit=size_limit,
            output_file=output_file,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: ETCH (Write Node Trees)
@mcp.tool(
    name="workflowy_etch",
    description="Create multiple nodes from JSON structure (no file intermediary). ETCH command for direct node creation."
)
async def etch(
    parent_id: str,
    nodes: list[dict] | str | None = None,
    replace_all: bool = False,
    nodes_file: str | None = None,
) -> dict:
    """Create multiple nodes from JSON structure.

    ETCH command - simple additive node creation (no UUIDs, no updates/moves).
    Fallback: If this fails, use INSCRIBE scroll (write_file → bulk_import).

    DEFAULT: Additive (skip existing by name, add new children only)
    REPLACE: Wipe all children, create fresh

    For complex operations (moves/updates with UUIDs): Use NEXUS scroll instead.

    Args:
        parent_id: Parent UUID where nodes should be created
        nodes: List of node objects (NO UUIDs - just name/note/children)
        replace_all: If True, delete ALL existing children first. Default False.

    Returns:
        Dictionary with success status, nodes created, skipped (if append_only), API call stats, and errors
    """
    client = get_client()

    # Resolve nodes source: direct value or file-based
    payload_nodes: list[dict] | str | None = nodes
    used_nodes_file = False

    if nodes_file:
        try:
            with open(nodes_file, "r", encoding="utf-8") as f:
                payload_nodes = f.read()
            used_nodes_file = True
        except Exception as e:
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": [
                    f"Failed to read nodes_file '{nodes_file}': {str(e)}",
                    "Hint: Ensure the path is correct and accessible from the MCP server",
                ],
            }
    elif isinstance(payload_nodes, str):
        # Agent convenience: if 'nodes' looks like a real JSON file path, try it as such.
        candidate = payload_nodes.strip()
        if os.path.exists(candidate) and candidate.lower().endswith(".json"):
            try:
                with open(candidate, "r", encoding="utf-8") as f:
                    payload_nodes = f.read()
                used_nodes_file = True
                nodes_file = candidate
                log_event(
                    f"workflowy_etch: treating 'nodes' string as nodes_file path -> {candidate}",
                    "ETCH",
                )
            except Exception as e:
                # Fall back to treating it as JSON; let the ETCH parser report a clear error
                log_event(
                    f"workflowy_etch: failed to read candidate nodes file '{candidate}': {e}",
                    "ETCH",
                )

    if payload_nodes is None:
        return {
            "success": False,
            "nodes_created": 0,
            "root_node_ids": [],
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "errors": [
                "Missing ETCH payload: provide either 'nodes' (list or stringified JSON) or 'nodes_file' (path to JSON file)",
            ],
        }

    # Rate limiter handled within workflowy_etch method due to recursive operations

    try:
        result = await client.workflowy_etch(parent_id, payload_nodes, replace_all=replace_all)
        # Annotate result to show where nodes came from (helps debugging real-world agent usage)
        if used_nodes_file:
            result.setdefault("_source", {})["nodes_file"] = nodes_file
        return result
    except Exception as e:
        # Top-level exception capture
        return {
            "success": False,
            "nodes_created": 0,
            "root_node_ids": [],
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "errors": [f"An unexpected error occurred: {str(e)}"]
        }


@mcp.tool(
    name="workflowy_etch_async",
    description="Start an async ETCH job (Workflowy node creation) and return a job_id for status polling.",
)
async def etch_async(
    parent_id: str,
    nodes: list[dict] | str | None = None,
    replace_all: bool = False,
    nodes_file: str | None = None,
) -> dict:
    """Start ETCH as a background job and return a job_id."""
    client = get_client()

    # Resolve nodes source for the background job
    payload_nodes: list[dict] | str | None = nodes

    if nodes_file:
        try:
            with open(nodes_file, "r", encoding="utf-8") as f:
                payload_nodes = f.read()
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to read nodes_file '{nodes_file}': {str(e)}",
            }
    elif isinstance(payload_nodes, str):
        # Agent convenience: if 'nodes' looks like a real JSON file path, try it as such.
        candidate = payload_nodes.strip()
        if os.path.exists(candidate) and candidate.lower().endswith(".json"):
            try:
                with open(candidate, "r", encoding="utf-8") as f:
                    payload_nodes = f.read()
                nodes_file = candidate
                log_event(
                    f"workflowy_etch_async: treating 'nodes' string as nodes_file path -> {candidate}",
                    "ETCH_ASYNC",
                )
            except Exception as e:
                # Fall back to treating it as JSON; let the ETCH parser report a clear error
                log_event(
                    f"workflowy_etch_async: failed to read candidate nodes file '{candidate}': {e}",
                    "ETCH_ASYNC",
                )

    if payload_nodes is None:
        return {
            "success": False,
            "error": "Missing ETCH payload: provide either 'nodes' (list or stringified JSON) or 'nodes_file' (path to JSON file)",
        }

    async def run_etch(job_id: str) -> dict:  # job_id reserved for future logging
        # Forward both the resolved payload_nodes and the optional nodes_file
        # into the client. workflowy_etch itself knows how to handle:
        #   - list[dict] (already-parsed nodes)
        #   - stringified JSON
        #   - nodes_file path (object-with-nodes or raw string)
        return await client.workflowy_etch(
            parent_id=parent_id,
            nodes=payload_nodes,
            replace_all=replace_all,
            nodes_file=nodes_file,
        )

    payload = {
        "parent_id": parent_id,
        "replace_all": replace_all,
        "nodes_file": nodes_file,
    }

    return await _start_background_job("etch", payload, run_etch)


# Tool: Beacon Snippet by UUID
@mcp.tool(
    name="beacon_get_code_snippet",
    description="Get raw code/doc snippet for a beacon node UUID (via Cartographer beacons)."
)
# @beacon[
#   id=auto-beacon@beacon_get_code_snippet-kxet,
#   role=beacon_get_code_snippet,
#   slice_labels=nexus-md-header-path,ra-snippet-range,f9-f12-handlers,ra-reconcile,
#   kind=ast,
# ]
async def beacon_get_code_snippet(
    beacon_node_id: str,
    context: int = 10,
) -> dict:
    """Return snippet around a beacon, given the beacon node's Workflowy UUID.

    This wraps WorkFlowyClientNexus.beacon_get_code_snippet and returns a
    JSON-friendly dict with:

        {
          "success": True,
          "file_path": str,
          "beacon_node_id": str,
          "beacon_id": str,
          "kind": "ast" | "span" | None,
          "start_line": int,
          "end_line": int,
          "snippet": str,
        }

    Notes:
    - beacon_node_id must be the UUID of the beacon node whose NOTE contains
      a BEACON (AST|SPAN) block with an `id: ...` line.
    - The underlying client method walks ancestors via the cached /nodes-export
      snapshot to find the FILE node with a `Path: ...` note (Cartographer
      projection), then calls the local beacon_obtain_code_snippet helper to
      extract the snippet without line numbers.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.beacon_get_code_snippet(
            beacon_node_id=beacon_node_id,
            context=context,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        # Surface as a structured MCP error response
        return {"success": False, "error": str(e)}


# @beacon[
#   id=auto-beacon@_read_text_snippet_impl-lsln,
#   role=_read_text_snippet_impl,
#   slice_labels=nexus-md-header-path,ra-snippet-range,f9-f12-handlers,ra-reconcile,
#   kind=ast,
# ]
async def _read_text_snippet_impl(
    input_str: str,
    context: int,
    *,
    explicit_uuid: bool = False,
) -> str:
    """Shared implementation for read_text_snippet and read_text_snippet_by_uuid.

    When explicit_uuid is True, input_str is always treated as a UUID and
    routed directly to beacon_get_code_snippet (no symbol fallback).
    When False, non-UUID-looking input is treated as a symbol and routed
    through the client's read_text_snippet_by_symbol helper.

    This helper is intentionally the *only* place that talks to the
    underlying snippet-resolution helpers so that MCP tools never call
    each other directly (which breaks in some FastMCP environments).
    """
    client = get_client()
    candidate = (input_str or "").strip()

    # UUID vs symbol detection only in auto mode.
    is_likely_uuid = (
        len(candidate) >= 32
        and ("-" in candidate or len(candidate) == 32)
        and all(c in "0123456789abcdefABCDEF-" for c in candidate)
    )

    if not explicit_uuid and not is_likely_uuid:
        # Treat as symbol and delegate to the client-level helper rather than
        # another MCP tool (FunctionTool objects are not directly awaitable).
        log_event(
            f"read_text_snippet: input {candidate!r} doesn't look like UUID; treating as symbol",
            "SNIPPET",
        )
        if _rate_limiter:
            await _rate_limiter.acquire()
        try:
            result = await client.read_text_snippet_by_symbol(
                symbol=candidate,
                file_path=None,
                symbol_kind="auto",
                context=context,
            )
            if _rate_limiter:
                _rate_limiter.on_success()
        except Exception as e:  # noqa: BLE001
            if (
                _rate_limiter
                and hasattr(e, "__class__")
                and e.__class__.__name__ == "RateLimitError"
            ):
                _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
            raise

        # WorkFlowyClientNexus.read_text_snippet_by_symbol returns the same
        # structure as beacon_get_code_snippet (dict with a "snippet" field).
        if not isinstance(result, dict):
            raise RuntimeError("Unexpected result from read_text_snippet_by_symbol")

        snippet = result.get("snippet", "")
        if not isinstance(snippet, str):
            snippet = str(snippet)
        return snippet

    # UUID path: delegate to beacon_get_code_snippet (includes hallucination tolerance).
    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.beacon_get_code_snippet(
            beacon_node_id=candidate,
            context=context,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
    except Exception as e:  # noqa: BLE001
        if (
            _rate_limiter
            and hasattr(e, "__class__")
            and e.__class__.__name__ == "RateLimitError"
        ):
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise

    if not isinstance(result, dict):
        raise RuntimeError("Unexpected result from beacon_get_code_snippet")

    if not result.get("success", False):
        raise RuntimeError(result.get("error", "Unknown error from beacon_get_code_snippet"))

    snippet = result.get("snippet", "")
    if not isinstance(snippet, str):
        snippet = str(snippet)
    return snippet


# Tool: Beacon-based text snippet (read_text_file twin)
@mcp.tool(
    name="read_text_snippet",
    description=(
        "Return a raw text snippet anchored by a Cartographer beacon/AST/heading/file "
        "node, using the same resolution pipeline as beacon_get_code_snippet, but "
        "returning only the snippet text. This is the go-to replacement for "
        "read_text_file on Cartographer-mapped code: always use this (or its UUID/" "symbol "
        "variants) instead of read_text_file when working with mapped repos."
    ),
)
# @beacon[
#   id=auto-beacon@read_text_snippet-37kd,
#   role=read_text_snippet,
#   slice_labels=nexus-md-header-path,ra-snippet-range,f9-f12-handlers,ra-reconcile,
#   kind=ast,
# ]
async def read_text_snippet(
    beacon_node_id: str,
    context: int = 20,
) -> str:
    """Return raw text snippet anchored by a Cartographer node.

    This is a thin wrapper around WorkFlowyClientNexus.beacon_get_code_snippet and
    read_text_snippet_by_symbol that returns only the ``snippet`` field as plain
    text. It uses the same resolution pipeline as beacon_get_code_snippet:

    - BEACON (AST|SPAN) metadata when present
    - AST_QUALNAME / MD_PATH when present for .py/.md
    - Implicit search or top-of-file fallback for other Cartographer FILE/child nodes

    Robustness:
    - If beacon_node_id doesn't look like a UUID (no hyphens, <8 chars, etc.),
      treats it as a symbol and routes through the client-level
      read_text_snippet_by_symbol helper.
    - Supports UUID hallucination tolerance (prefix matching) via
      beacon_get_code_snippet.

    This is the advanced, Cartographer-aware twin of read_text_file and should
    be used instead of read_text_file for any Cartographer-mapped code.
    """
    return await _read_text_snippet_impl(
        input_str=beacon_node_id,
        context=context,
        explicit_uuid=False,
    )


# Tool: UUID-based text snippet (explicit UUID variant)
@mcp.tool(
    name="read_text_snippet_by_uuid",
    description=(
        "Return a raw text snippet anchored by a Workflowy UUID. "
        "Explicit UUID variant of read_text_snippet. Use this for "
        "Cartographer-mapped code when you have a UUID from GLIMPSE; do not "
        "fall back to read_text_file."
    ),
)
# @beacon[
#   id=auto-beacon@read_text_snippet_by_uuid-ji9w,
#   role=read_text_snippet_by_uuid,
#   slice_labels=nexus-md-header-path,ra-snippet-range,f9-f12-handlers,ra-reconcile,
#   kind=ast,
# ]
async def read_text_snippet_by_uuid(
    beacon_node_id: str,
    context: int = 20,
) -> str:
    """Return raw text snippet anchored by a Cartographer node UUID.

    This is an explicit-UUID variant of read_text_snippet that always treats
    input as a UUID and never falls back to symbol resolution. Provided for
    naming symmetry:

    - read_text_snippet: accepts UUID or symbol (auto-detects)
    - read_text_snippet_by_uuid: explicit UUID intent
    - read_text_snippet_by_symbol: explicit symbol intent

    Supports UUID hallucination tolerance via prefix matching in the
    underlying beacon_get_code_snippet implementation.
    """
    return await _read_text_snippet_impl(
        input_str=beacon_node_id,
        context=context,
        explicit_uuid=True,
    )


# Tool: Symbol-based text snippet (handle-first variant)
@mcp.tool(
    name="read_text_snippet_by_symbol",
    description=(
        "Return a raw text snippet by optional file path/name and symbol "
        "(function name, AST_QUALNAME, or beacon id/role). Avoids requiring "
        "agents to supply Workflowy UUIDs; the server resolves symbol → node → snippet."
    ),
)
# @beacon[
#   id=auto-beacon@read_text_snippet_by_symbol-ewti,
#   role=read_text_snippet_by_symbol,
#   slice_labels=f9-f12-handlers,nexus-md-header-path,ra-snippet-range,ra-reconcile,
#   kind=ast,
# ]
async def read_text_snippet_by_symbol(
    symbol: str,
    file_path: str | None = None,
    symbol_kind: str = "auto",
    context: int = 10,
) -> str:
    """Return raw text snippet resolved from symbol + optional file path.

    This is the go-to symbol-based snippet tool for Cartographer-mapped code;
    use this instead of read_text_file when you know a symbol and optional
    file path.

    This is a handle-based companion to read_text_snippet that resolves
    symbols (function names, AST_QUALNAME, beacon ids/roles) without
    requiring Workflowy UUIDs from agents. It:

    - Loads the cached /nodes-export snapshot.
    - Identifies Cartographer FILE nodes by their Path:/Root: headers.
    - Optionally restricts search to FILE(s) matching file_path (basename or full path).
    - Matches symbol within chosen FILE subtree(s) by:
        - Beacon id or role (BEACON blocks in note),
        - AST_QUALNAME (Python/JS/TS AST nodes),
        - Normalized node name (function/method/heading).
    - On unique match, delegates to beacon_get_code_snippet and returns snippet text.
    - On zero/multiple matches, raises NetworkError with disambiguation hints.

    Args:
        symbol: Function name, AST_QUALNAME, or beacon id/role to search for.
        file_path: Optional file path (basename or full path) to restrict search.
                   If None, searches all Cartographer FILE nodes.
        symbol_kind: "auto" (default), "beacon", "ast", or "name".
                     Controls which matching strategies are enabled.
        context: Lines of context around the snippet (default 10).

    Returns:
        Raw snippet text (same format as read_text_snippet).

    Raises:
        NetworkError when symbol is not found or is ambiguous.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.read_text_snippet_by_symbol(
            file_path=file_path,
            symbol=symbol,
            symbol_kind=symbol_kind,
            context=context,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise

    if not isinstance(result, dict):
        raise RuntimeError("Unexpected result from read_text_snippet_by_symbol")

    if not result.get("success", False):
        raise RuntimeError(result.get("error", "Unknown error from read_text_snippet_by_symbol"))

    snippet = result.get("snippet", "")
    if not isinstance(snippet, str):
        snippet = str(snippet)

    return snippet


# Tool: List Nexus Keystones
@mcp.tool(
    name="nexus_list_keystones",
    description="List all available NEXUS Keystone backups."
)
def nexus_list_keystones() -> dict:
    """List all available NEXUS Keystone backups."""
    client = get_client()
    return client.nexus_list_keystones()


# Tool: Restore Nexus Keystone
@mcp.tool(
    name="nexus_restore_keystone",
    description="Restore a Workflowy node tree from a NEXUS Keystone backup."
)
async def nexus_restore_keystone(keystone_id: str) -> dict:
    """Restore a Workflowy node tree from a NEXUS Keystone backup."""
    client = get_client()
    return await client.nexus_restore_keystone(keystone_id)


# Tool: Purge Nexus Keystones
@mcp.tool(
    name="nexus_purge_keystones",
    description="Delete one or more NEXUS Keystone backup files."
)
def nexus_purge_keystones(keystone_ids: list[str]) -> dict:
    """Delete one or more NEXUS Keystone backup files."""
    client = get_client()
    return client.nexus_purge_keystones(keystone_ids)


@mcp.tool(
    name="beacon_refresh_code_node",
    description=(
        "Per-file beacon-aware refresh of a Cartographer-mapped FILE node from its "
        "source file. Uses a SHA1 guard to skip unchanged files, rebuilds the "
        "AST+beacon subtree under the file node, and salvages/re-attaches Notes[...] "
        "subtrees keyed by beacon id."
    ),
)
# @beacon[
#   id=auto-beacon@beacon_refresh_code_node-1t6u,
#   role=beacon_refresh_code_node,
#   slice_labels=ra-notes,ra-notes-cartographer,ra-reconcile,f9-f12-handlers,ra-carto-jobs,
#   kind=ast,
# ]
async def beacon_refresh_code_node(
    file_node_id: str,
    dry_run: bool = False,
) -> dict:
    """MCP wrapper for WorkFlowyClient.refresh_file_node_beacons.

    Args:
        file_node_id: Workflowy UUID of the Cartographer-mapped FILE node whose
            subtree should be refreshed.
        dry_run: If True, compute the plan and counts only (no Workflowy writes).

    Returns:
        Summary dict with structural_nodes_deleted/created, notes_salvaged,
        notes_orphaned, and hash information.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.refresh_file_node_beacons(
            file_node_id=file_node_id,
            dry_run=dry_run,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        # Surface as a structured MCP error response
        return {"success": False, "error": str(e)}


@mcp.tool(
    name="update_beacon_from_node",
    description=(
        "Update/create/delete a Cartographer beacon based on a single Workflowy "
        "node's name/note (F12-per-node). This only edits the underlying source "
        "file; the client should update the cached node name/note separately."
    ),
)
# @beacon[
#   id=auto-beacon@update_beacon_from_node-kiam,
#   role=update_beacon_from_node,
#   slice_labels=f9-f12-handlers,ra-reconcile,
#   kind=ast,
# ]
async def update_beacon_from_node(
    node_id: str,
    name: str,
    note: str,
) -> dict:
    """MCP wrapper for WorkFlowyClientNexus.update_beacon_from_node.

    The client is expected to:
    - Call this tool when F12 is pressed on a Cartographer-mapped node.
    - Use the returned "base_name" to update the cached node name (stripping
      trailing tags) via update_cached_node_name or equivalent.
    - Use the returned beacon metadata (role/slice_labels/comment) to
      rebuild the note's BEACON block for local cache sync.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.update_beacon_from_node(
            node_id=node_id,
            name=name,
            note=note,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        return {"success": False, "error": str(e)}


@mcp.tool(
    name="cartographer_refresh_async",
    description=(
        "Start an async Cartographer F12 refresh job (file or folder) via "
        "weave_worker.py and return a job_id for status polling."
    ),
)
# @beacon[
#   id=auto-beacon@cartographer_refresh_async-zk93,
#   role=cartographer_refresh_async,
#   slice_labels=f9-f12-handlers,ra-reconcile,nexus-core-v1,ra-logging,ra-carto-jobs,
#   kind=ast,
# ]
async def cartographer_refresh_async(
    root_uuid: str,
    mode: str,
) -> dict:
    """Start a detached CARTO_REFRESH job for a FILE or FOLDER node.

    Args:
        root_uuid: Workflowy UUID of the Cartographer FILE or FOLDER node to refresh.
        mode: "file" for per-file refresh, "folder" for subtree refresh.
    """
    return await _start_carto_refresh_job(root_uuid=root_uuid, mode=mode)


# Resource: WorkFlowy Outline
@mcp.resource(
    uri="workflowy://outline",
    name="workflowy_outline",
    description="The complete WorkFlowy outline structure",
)
async def get_outline() -> str:
    """Get the complete WorkFlowy outline as a formatted string.

    Returns:
        Formatted string representation of the outline
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        # Get root nodes
        request = NodeListRequest(  # type: ignore[call-arg]
            limit=1000,  # Get many nodes
        )
        nodes, _ = await client.list_nodes(request)

        if _rate_limiter:
            _rate_limiter.on_success()

        # Format outline
        def format_node(node: WorkFlowyNode, indent: int = 0) -> str:
            lines = []
            prefix = "  " * indent + "- "
            status = "[x] " if node.cp else ""
            lines.append(f"{prefix}{status}{node.nm or '(untitled)'}")

            if node.no:
                note_prefix = "  " * (indent + 1)
                lines.append(f"{note_prefix}Note: {node.no}")

            if node.ch:
                for child in node.ch:
                    lines.append(format_node(child, indent + 1))

            return "\n".join(lines)

        outline_parts = [format_node(node) for node in nodes]
        return "\n".join(outline_parts)

    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


if __name__ == "__main__":
    # ☢️ NUCLEAR LOGGING OPTION ☢️
    # Redirect EVERYTHING to sys.stderr so it appears in MCP console
    import sys
    import logging
    
    # 1. Setup Root Logger to DEBUG
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # 2. Clear existing handlers to prevent duplicate/swallowed logs
    for h in root.handlers[:]:
        root.removeHandler(h)
        
    # 3. Create NUCLEAR StreamHandler to stderr
    handler = logging.StreamHandler(sys.stderr)
    # Simple format: [TIME] [LEVEL] Message
    handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S'))
    root.addHandler(handler)
    
    # 4. Monkey-patch sys.stdout to redirect to stderr (so print() works)
    # This captures generic print() statements from any library
    class StderrRedirector:
        def write(self, message):
            if message.strip(): # Avoid empty newline spam
                sys.stderr.write(f"[STDOUT] {message}\n")
        def flush(self):
            sys.stderr.flush()
            
    sys.stdout = StderrRedirector()

    # 5. Log startup confirmation (via DAGGER logger + root logger)
    log_event("☢️ NUCLEAR LOGGING ACTIVE: DEBUG LEVEL ☢️", "SERVER")
    local_logger = _ClientLogger("SERVER")
    local_logger.error("STDERR TEST: This should appear in console (via logger.error)")
    print("Standard Output Redirection Test", file=sys.stderr)

    # Run the server
    mcp.run(transport="stdio")
