"""WorkFlowy MCP server implementation using FastMCP."""

import sys
from datetime import datetime
# Also log to file to debug deployment/environment
try:
    with open(r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\reconcile_debug.log", "a", encoding="utf-8") as f:
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

logger = logging.getLogger(__name__)

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
DAGGER_ROOT_ID = "9ebad31a-2994-4d6f-be50-3499f8c53144"


def get_client() -> WorkFlowyClient:
    """Get the global WorkFlowy client instance."""
    global _client
    if _client is None:
        raise RuntimeError("WorkFlowy client not initialized. Server not started properly.")
    return _client


def get_ws_connection():
    """Get the current WebSocket connection and message queue (if any)."""
    global _ws_connection, _ws_message_queue
    return _ws_connection, _ws_message_queue


async def _resolve_uuid_path_and_respond(target_uuid: str | None, websocket) -> None:
    """Resolve full ancestor path for target_uuid and send result back to extension.

    Primary strategy:
      - Use /nodes-export via export_nodes(DAGGER_ROOT_ID) to get the full AI Dagger
        subtree as a flat list.
      - Reconstruct the path from DAGGER_ROOT_ID down to target_uuid using
        parent_id/parentId fields.

    Fallback strategy:
      - If target_uuid is not under the Dagger root (or export fails), fall back
        to walking parentId via get_node() up to the account root.

    Both strategies format markdown identically to F3 behavior (names at each
    level, leaf UUID only) so the UUID Navigator widget always shows a complete
    path, even when the node is not currently visible in the DOM.
    """
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

    # First try: reconstruct path from Dagger subtree via /nodes-export
    path_from_dagger: list[dict[str, Any]] | None = None
    try:
        export_data = await client.export_nodes(DAGGER_ROOT_ID)
        flat_nodes = export_data.get("nodes", []) or []
        nodes_by_id: dict[str, dict[str, Any]] = {}
        for node in flat_nodes:
            node_id = node.get("id")
            if node_id:
                nodes_by_id[node_id] = node

        if target in nodes_by_id:
            tmp_path: list[dict[str, Any]] = []
            visited_ids: set[str] = set()
            current_id = target

            while current_id and current_id in nodes_by_id and current_id not in visited_ids:
                visited_ids.add(current_id)
                node = nodes_by_id[current_id]
                tmp_path.append(node)
                parent_id = node.get("parent_id") or node.get("parentId")
                if not parent_id:
                    break
                current_id = parent_id

            tmp_path.reverse()
            path_from_dagger = tmp_path if tmp_path else None
    except Exception as e:  # noqa: BLE001
        logger.warning(f"UUID path DAGGER export failed for {target_uuid}: {e}")
        path_from_dagger = None

    def _name_from_flat(node_dict: dict[str, Any]) -> str:
        return (node_dict.get("name")
                or node_dict.get("nm")
                or "Untitled")

    # If we found the node under Dagger, use that path
    if path_from_dagger:
        lines: list[str] = []
        for depth, node_dict in enumerate(path_from_dagger):
            prefix = "#" * (depth + 1)
            lines.append(f"{prefix} {_name_from_flat(node_dict)}")

        lines.append("")
        lines.append(f"→ `{target}`")

        markdown = "\n".join(lines)

        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": True,
            "target_uuid": target_uuid,
            "markdown": markdown,
            "path": [
                {
                    "id": node_dict.get("id"),
                    "name": _name_from_flat(node_dict),
                    "parent_id": node_dict.get("parent_id") or node_dict.get("parentId"),
                }
                for node_dict in path_from_dagger
            ],
        }))
        return

    # Fallback: Walk parentId chain via get_node (account-root path)
    try:
        path_nodes = []
        visited: set[str] = set()
        current_id = target

        while current_id and current_id not in visited:
            visited.add(current_id)
            node = await client.get_node(current_id)
            path_nodes.append(node)
            current_id = getattr(node, "parentId", None)

        path_nodes.reverse()

        if not path_nodes:
            await websocket.send(json.dumps({
                "action": "uuid_path_result",
                "success": False,
                "target_uuid": target_uuid,
                "error": f"Node {target_uuid} not found in API",
            }))
            return

        lines: list[str] = []
        for depth, node in enumerate(path_nodes):
            name = getattr(node, "nm", None) or "Untitled"
            prefix = "#" * (depth + 1)
            lines.append(f"{prefix} {name}")

        lines.append("")
        lines.append(f"→ `{target}`")

        markdown = "\n".join(lines)

        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": True,
            "target_uuid": target_uuid,
            "markdown": markdown,
            "path": [
                {
                    "id": node.id,
                    "name": getattr(node, "nm", None) or "Untitled",
                    "parent_id": getattr(node, "parentId", None),
                }
                for node in path_nodes
            ],
        }))

    except Exception as e:  # noqa: BLE001
        logger.error(f"UUID path resolution error for {target_uuid}: {e}")
        await websocket.send(json.dumps({
            "action": "uuid_path_result",
            "success": False,
            "target_uuid": target_uuid,
            "error": str(e),
        }))


async def websocket_handler(websocket):
    """Handle WebSocket connections from Chrome extension.
    
    Uses message queue pattern to avoid recv() conflicts.
    
    Extension sends requests:
    {"action": "extract_dom", "node_id": "uuid"}
    
    Extension receives responses with DOM tree data.
    """
    global _ws_connection, _ws_message_queue
    
    logger.info(f"WebSocket client connected from {websocket.remote_address}")
    _ws_connection = websocket
    _ws_message_queue = asyncio.Queue()  # Fresh queue for this connection
    
    try:
        # Keep connection alive - wait for messages indefinitely
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get('action')
                logger.info(f"WebSocket message received: {action}")
                
                # Handle ping to keep connection alive
                if action == 'ping':
                    await websocket.send(json.dumps({"action": "pong"}))
                    logger.info("Sent pong response")
                    continue

                # Handle UUID path resolution requests (UUID Navigator)
                if action == 'resolve_uuid_path':
                    target_uuid = data.get('target_uuid') or data.get('uuid')
                    await _resolve_uuid_path_and_respond(target_uuid, websocket)
                    continue
                
                # Put all other messages in queue for workflowy_glimpse() to consume
                await _ws_message_queue.put(data)
                logger.info(f"Message queued for processing: {action}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON from WebSocket: {e}")
            except Exception as e:
                logger.error(f"WebSocket message error: {e}")
        
        # If loop exits naturally, connection was closed by client
        logger.info("WebSocket client disconnected (connection closed by client)")
                
    except Exception as e:
        logger.info(f"WebSocket connection closed with error: {e}")
    finally:
        if _ws_connection == websocket:
            _ws_connection = None
            _ws_message_queue = None
        logger.info("WebSocket client cleaned up")


async def start_websocket_server():
    """Start WebSocket server for Chrome extension communication."""
    try:
        import websockets
    except ImportError:
        logger.warning("websockets library not installed. WebSocket cache unavailable.")
        logger.warning("Install with: pip install websockets")
        return
    
    logger.info("Starting WebSocket server on ws://localhost:8765")
    
    try:
        async with websockets.serve(websocket_handler, "localhost", 8765) as server:
            logger.info("✅ WebSocket server listening on port 8765")
            logger.info("WebSocket server will accept connections indefinitely...")
            
            # Keep server running forever
            await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"WebSocket server failed to start: {e}")
        logger.error("GLIMPSE will fall back to API fetching")


@asynccontextmanager
async def lifespan(_app: FastMCP):  # type: ignore[no-untyped-def]
    """Manage server lifecycle."""
    global _client, _rate_limiter, _ws_server_task

    # Setup
    logger.info("Starting WorkFlowy MCP server")

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

    logger.info(f"WorkFlowy client initialized with base URL: {api_config.base_url}")
    
    # Start WebSocket server in background task
    _ws_server_task = asyncio.create_task(start_websocket_server())
    logger.info("WebSocket server task created")

    yield

    # Cleanup
    logger.info("Shutting down WorkFlowy MCP server")
    
    # Cancel WebSocket server
    if _ws_server_task:
        _ws_server_task.cancel()
        try:
            await _ws_server_task
        except asyncio.CancelledError:
            pass
        logger.info("WebSocket server stopped")
    
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


@mcp.tool(
    name="mcp_job_status",
    description="Get status/result for long-running MCP jobs (ETCH, NEXUS, etc.).",
)
async def mcp_job_status(job_id: str | None = None) -> dict:
    # Return status for one job (if job_id given) or all jobs.
    if job_id is None:
        jobs = []
        for job in _jobs.values():
            jobs.append({
                "job_id": job.get("job_id"),
                "kind": job.get("kind"),
                "status": job.get("status"),
                "started_at": job.get("started_at"),
                "finished_at": job.get("finished_at"),
            })
        return {"success": True, "jobs": jobs}

    job = _jobs.get(job_id)
    if not job:
        return {"success": False, "error": f"Unknown job_id: {job_id}"}

    # Do not expose internal task handle
    view = {k: v for k, v in job.items() if k not in ("payload", "_task")}
    return {"success": True, **view}


@mcp.tool(
    name="mcp_cancel_job",
    description="Request cancellation of a long-running MCP job (ETCH, NEXUS, etc.).",
)
async def mcp_cancel_job(job_id: str) -> dict:
    """Attempt to cancel a background MCP job.

    This sends an asyncio.CancelledError into the job task. The job will
    transition to status='failed' with an error message indicating
    cancellation.
    """
    job = _jobs.get(job_id)
    if not job:
        return {"success": False, "error": f"Unknown job_id: {job_id}"}

    task = job.get("_task")
    if task is None:
        return {"success": False, "error": "Job has no associated task (cannot cancel)."}

    if task.done():
        return {"success": False, "error": "Job already completed."}

    # Mark as cancelling for visibility; runner will finalize status
    job["status"] = "cancelling"
    task.cancel()

    return {"success": True, "job_id": job_id, "status": "cancelling"}


# 🔐 SECRET CODE VALIDATION - Brute Force Agent Training Override
def validate_secret_code(provided_code: str | None, function_name: str) -> tuple[bool, str | None]:
    """Validate secret code for WARNING functions.
    
    This is the nuclear option - forces agents to ask Dan explicitly.
    
    Returns:
        (is_valid, error_message)
    """
    import os
    import secrets
    
    SECRET_FILE = r"E:\__daniel347x\glimpse_etch.txt"
    
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

@mcp.tool(
    name="nexus_glimpse_full",
    description=(
        "GLIMPSE FULL → TERRAIN + PHANTOM GEM (API-based, ignores UI expansion). "
        "API cousin of nexus_glimpse that fetches the complete subtree via Workflowy API. "
        "Unlike nexus_summon, this REQUIRES a full-depth tree (errors if truncation needed). "
        "Use for agent-driven workflows or when tree is too large to manually expand."
    ),
)
async def nexus_glimpse_full(
    nexus_tag: str,
    workflowy_root_id: str,
    reset_if_exists: bool = False,
    mode: str = "full",
    max_depth: int | None = None,
    child_limit: int | None = None,
    max_nodes: int = 200000,
) -> dict[str, Any]:
    """API-based GLIMPSE FULL (complete subtree, no truncation)."""
    client = get_client()
    return await client.nexus_glimpse_full(
        nexus_tag=nexus_tag,
        workflowy_root_id=workflowy_root_id,
        reset_if_exists=reset_if_exists,
        mode=mode,
        max_depth=max_depth,
        child_limit=child_limit,
        max_nodes=max_nodes,
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


# PHANTOM GEMSTONE NEXUS – High-level MCP tools
@mcp.tool(
    name="nexus_summon",
    description=(
        "INITIATE a CORINTHIAN NEXUS on the ETHER: perform a COARSE SCRY of Workflowy "
        "under a root to reveal a limited TERRAIN (a new geography named by your "
        "NEXUS TAG). Choose max_depth and child_limit carefully—keep them minimal. "
        "Later, you will IGNITE the ETHER more deeply on selected SHARDS."
    ),
)
async def nexus_summon(
    nexus_tag: str,
    workflowy_root_id: str,
    max_depth: int,
    child_limit: int,
    reset_if_exists: bool = False,
) -> dict:
    """SCRY the ETHER under a root to create a coarse TERRAIN bound to a NEXUS TAG.

    This reveals a limited TERRAIN—a new geography named by your NEXUS TAG.
    Keep the SCRY shallow: choose max_depth and child_limit carefully.
    Later you will IGNITE the ETHER more deeply on selected SHARDS.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_summon(
            nexus_tag=nexus_tag,
            workflowy_root_id=workflowy_root_id,
            max_depth=max_depth,
            child_limit=child_limit,
            reset_if_exists=reset_if_exists,
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
        "SET_ATTRS, CREATE_NODE, all referencing nodes by jewel_id."
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
    name="nexus_weave_enchanted",
    description=(
        "WEAVE the ENCHANTED TERRAIN (T2) back into the living Workflowy ETHER, "
        "completing the PHANTOM GEMSTONE NEXUS. Can run as a DRY RUN to simulate "
        "which parts of the ETHER would be transmuted before actually weaving."
    ),
)
async def nexus_weave_enchanted(
    nexus_tag: str,
    dry_run: bool = False,
) -> dict:
    """WEAVE ENCHANTED TERRAIN back into the ETHER.

    Take the ENCHANTED TERRAIN (T2) you have crafted in JSON form and WEAVE it
    back into the living Workflowy ETHER. In dry_run mode, simulate the
    transmutation without touching the ETHER; otherwise, perform the weave with
    reconcile safeguards.
    """
    client = get_client()

    # Rate limiting is managed within the client for reconcile operations.
    try:
        result = await client.nexus_weave_enchanted(nexus_tag=nexus_tag, dry_run=dry_run)
        return result
    except Exception as e:  # noqa: BLE001
        return {
            "success": False,
            "errors": [f"WEAVE failed: {type(e).__name__}: {e}"],
        }


@mcp.tool(
    name="nexus_start_exploration",
    description=(
        "Initialize an exploration session over a Workflowy subtree and return "
        "an initial frontier of handles for agent-driven navigation. Set editable=True "
        "to enable in-session note/tag edits that will be reflected in the phantom gem."
    ),
)
async def nexus_start_exploration(
    nexus_tag: str,
    root_id: str,
    source_mode: str = "glimpse_full",
    max_nodes: int = 200000,
    session_hint: str | None = None,
    frontier_size: int = 25,
    max_depth_per_frontier: int = 1,
    editable: bool = False,
) -> dict:
    """Start an exploration session over a Workflowy subtree."""
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
        "Apply exploration actions to an exploration session and return the next frontier "
        "of handles. Supported actions include: open/close/finalize/reopen; "
        "accept_leaf/reject_leaf; accept_subtree/reject_subtree (optional guardian_token "
        "for strict dfs_full_walk overrides); backtrack/reopen_branch; add_hint "
        "(per-handle); set_scratchpad/append_scratchpad (session-global registry); "
        "peek_descendants; replace_leaf_node_with_appended_scratch_note; and, when "
        "the session was started with editable=True, update_note_and_flag_for_acceptance/" 
        "update_tag_and_flag_for_acceptance to mutate the cached tree while marking nodes "
        "for inclusion in the phantom gem."
    ),
)
async def nexus_explore_step(
    session_id: str,
    actions: list[dict[str, Any]] | None = None,
    frontier_size: int = 5,
    max_depth_per_frontier: int = 1,
    include_history_summary: bool = True,
) -> dict:
    """Apply exploration actions and return the next frontier.

    actions[i]["action"] may be one of:
      - "open", "close", "finalize", "reopen"
      - "accept_leaf", "reject_leaf"
      - "accept_subtree", "reject_subtree" (with optional "guardian_token")
      - "backtrack", "reopen_branch"
      - "add_hint" (attach free-text hints to a handle)
      - "set_scratchpad", "append_scratchpad" (maintain session-global REGISTRY text)
      - "peek_descendants"
      - "replace_leaf_node_with_appended_scratch_note"
      - "update_note_and_flag_for_acceptance" (editable sessions only)
      - "update_tag_and_flag_for_acceptance" (editable sessions only)
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_explore_step(
            session_id=session_id,
            actions=actions,
            frontier_size=frontier_size,
            max_depth_per_frontier=max_depth_per_frontier,
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
        "Finalize an exploration session into phantom_gem.json (+ optional "
        "coarse_terrain.json) for use with NEXUS JEWELSTORM and WEAVE."
    ),
)
async def nexus_finalize_exploration(
    session_id: str,
    include_terrain: bool = True,
) -> dict:
    """Finalize an exploration session into PHANTOM GEM (+ optional TERRAIN)."""
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.nexus_finalize_exploration(
            session_id=session_id,
            include_terrain=include_terrain,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:  # noqa: BLE001
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: Bulk Export to JSON File
@mcp.tool(
    name="nexus_scry",
    description="Export a Workflowy node and its entire subtree to a JSON file, creating a Keystone backup."
)
async def nexus_scry(
    node_id: str,
    output_file: str,
    include_metadata: bool = True,
    max_depth: int | None = None,
    child_count_limit: int | None = None,
) -> dict:
    """Export a node tree to a hierarchical JSON file.

    Args:
        node_id: The UUID of the root node to export from.
        output_file: The absolute path where the JSON output file should be written.
        include_metadata: Whether to include metadata fields like created_at and modified_at (default True).
        max_depth: Optional depth limit for the EDITABLE JSON/Markdown view (None = full depth).
        child_count_limit: Optional maximum immediate child count to fully materialize per
            parent in the EDITABLE JSON. Parents whose immediate child count exceeds this
            limit are treated as opaque subtrees in the editable JSON while accurate
            counts are still computed from the full tree.

    Returns:
        A dictionary with success status, file path, node count, and tree depth.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.bulk_export_to_file(
            node_id=node_id,
            output_file=output_file,
            include_metadata=include_metadata,
            use_efficient_traversal=False,
            max_depth=max_depth,
            child_count_limit=child_count_limit,
        )
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:
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
    description="Load entire node tree into context (no file intermediary). GLIMPSE command for direct context loading."
)
async def glimpse(
    node_id: str,
) -> dict:
    """Load entire node tree into agent context.
    
    GLIMPSE command - efficient context loading for agent analysis.
    
    Tries WebSocket DOM extraction first (if Chrome extension connected).
    Falls back to API fetch if WebSocket unavailable.
    
    Args:
        node_id: Root node UUID to read from
        
    Returns:
        Dictionary with root metadata, children tree, node count, depth, and source indicator
    """
    client = get_client()
    ws_conn, ws_queue = get_ws_connection()  # Check if extension is connected
    
    if _rate_limiter:
        await _rate_limiter.acquire()
    
    try:
        # Pass WebSocket connection AND queue to client method
        result = await client.workflowy_glimpse(node_id, _ws_connection=ws_conn, _ws_queue=ws_queue)
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_limit(getattr(e, "retry_after", None))
        raise


# Tool: GLIMPSE FULL (Force API Fetch)
@mcp.tool(
    name="workflowy_glimpse_full",
    description="Load entire node tree via API (bypass WebSocket). Use when Key Files doesn't have parent UUID for ETCH, or when Dan wants complete tree regardless of expansion state."
)
async def glimpse_full(
    node_id: str,
    depth: int | None = None,
    size_limit: int = 1000,
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
        result = await client.workflowy_glimpse_full(node_id, depth=depth, size_limit=size_limit)
        if _rate_limiter:
            _rate_limiter.on_success()
        return result
    except Exception as e:
        if _rate_limiter and hasattr(e, "__class__") and e.__class__.__name__ == "RateLimitError":
            _rate_limiter.on_rate_lock(getattr(e, "retry_after", None))
        raise


# Tool: ETCH (Write Node Trees)
@mcp.tool(
    name="workflowy_etch",
    description="Create multiple nodes from JSON structure (no file intermediary). ETCH command for direct node creation."
)
async def etch(
    parent_id: str,
    nodes: list[dict] | str,
    replace_all: bool = False,
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

    # Rate limiter handled within workflowy_etch method due to recursive operations

    try:
        result = await client.workflowy_etch(parent_id, nodes, replace_all=replace_all)
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
    nodes: list[dict] | str,
    replace_all: bool = False,
) -> dict:
    """Start ETCH as a background job and return a job_id."""
    client = get_client()

    async def run_etch(job_id: str) -> dict:  # job_id reserved for future logging
        return await client.workflowy_etch(parent_id, nodes, replace_all=replace_all)

    payload = {
        "parent_id": parent_id,
        "replace_all": replace_all,
    }

    return await _start_background_job("etch", payload, run_etch)


# Tool: Weave (Bulk Import from JSON File)
@mcp.tool(
    name="nexus_weave",
    description="Create/update a Workflowy node tree from a hierarchical JSON file."
)
async def nexus_weave(
    json_file: str,
    parent_id: str | None = None,
    dry_run: bool = False,
    import_policy: str = 'strict',
    max_sync_nodes: int = 100,
) -> dict:
    """Create a tree of nodes from a JSON structure.

    Args:
        json_file: The absolute path to the JSON file containing the node structure.
        parent_id: The UUID of the parent node under which to create the new nodes.

    Returns:
        A dictionary with success status, nodes created, API call stats, and any errors.
    """
    client = get_client()

    # Rate limiter is handled within the bulk_import_from_file method
    # due to the recursive nature of the operation.

    # SAFETY CHECK: Large JSON trees must use nexus_weave_async by default.
    if max_sync_nodes and max_sync_nodes > 0 and not dry_run:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, dict) and 'nodes' in data:
                root_nodes = data['nodes']
            elif isinstance(data, list):
                root_nodes = data
            else:
                root_nodes = []

            def _count_nodes(nodes: list[dict]) -> int:
                total = 0
                stack = list(nodes)
                while stack:
                    n = stack.pop()
                    total += 1
                    stack.extend(n.get('children') or [])
                return total

            node_count = _count_nodes(root_nodes)
            if node_count > max_sync_nodes:
                return {
                    "success": False,
                    "nodes_created": 0,
                    "root_node_ids": [],
                    "api_calls": 0,
                    "retries": 0,
                    "rate_limit_hits": 0,
                    "errors": [
                        (
                            "nexus_weave (sync) aborted: JSON contains "
                            f"{node_count} nodes, exceeding max_sync_nodes={max_sync_nodes}. "
                            "Use nexus_weave_async for large trees, or explicitly increase "
                            "max_sync_nodes if you understand the risks."
                        )
                    ],
                    "node_count": node_count,
                    "max_sync_nodes": max_sync_nodes,
                }
        except Exception as e:  # noqa: BLE001
            logger.warning(f"nexus_weave: failed to inspect JSON for node count: {e}")

    try:
        result = await client.bulk_import_from_file(json_file, parent_id, dry_run, import_policy)
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
    name="nexus_weave_async",
    description="Start an async NEXUS weave (bulk import from JSON) and return a job_id for status polling.",
)
async def nexus_weave_async(
    json_file: str,
    parent_id: str | None = None,
    dry_run: bool = False,
    import_policy: str = "strict",
) -> dict:
    """Start NEXUS weave as a background job and return a job_id."""
    client = get_client()

    async def run_weave(job_id: str) -> dict:  # job_id reserved for future logging
        return await client.bulk_import_from_file(json_file, parent_id, dry_run, import_policy)

    payload = {
        "json_file": json_file,
        "parent_id": parent_id,
        "dry_run": dry_run,
        "import_policy": import_policy,
    }

    return await _start_background_job("nexus_weave", payload, run_weave)


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
    # Setup logging
    setup_logging()
    
    # FORCE VISIBLE LOGGING TO STDERR (Override)
    # This ensures logs show up in the MCP connector console
    import sys
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    
    # Check if a handler exists; if not, add StreamHandler(sys.stderr)
    has_console = any(isinstance(h, logging.StreamHandler) and h.stream == sys.stderr for h in root.handlers)
    if not has_console:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        root.addHandler(handler)

    # Run the server

    mcp.run(transport="stdio")
