"""WorkFlowy MCP server implementation using FastMCP."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Literal

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
4. Call again with: secret_code="[code-from-Dan]"

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

The function 'workflowy_create_single_node' has been renamed to 'workflowy_create_single_node__WARNING__prefer_bulk_import'.

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
@mcp.tool(name="workflowy_create_single_node__WARNING__prefer_bulk_import", description="⚠️ WARNING: For 2+ nodes, use workflowy_bulk_import instead (vastly more efficient). This creates ONE node only.")
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
    
    ⚠️ WARNING: For creating 2+ nodes, use workflowy_bulk_import instead.
    
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
    is_valid, error = validate_secret_code(secret_code, "workflowy_create_single_node__WARNING__prefer_bulk_import")
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
            "_warning": "⚠️ WARNING: You just created a SINGLE node. For 2+ nodes, use workflowy_bulk_import instead (2 tool calls vs 10+)."
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


# Tool: Bulk Export to JSON File
@mcp.tool(
    name="workflowy_bulk_export",
    description="Export a Workflowy node and its entire subtree to a JSON file."
)
async def bulk_export(
    node_id: str,
    output_file: str,
    include_metadata: bool = True,
) -> dict:
    """Export a node tree to a hierarchical JSON file.

    Args:
        node_id: The UUID of the root node to export from.
        output_file: The absolute path where the JSON output file should be written.
        include_metadata: Whether to include metadata fields like created_at and modified_at (default True).

    Returns:
        A dictionary with success status, file path, node count, and tree depth.
    """
    client = get_client()

    if _rate_limiter:
        await _rate_limiter.acquire()

    try:
        result = await client.bulk_export_to_file(node_id, output_file, include_metadata)
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
    size_limit: int | None = None,
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
        size_limit: Maximum number of nodes to return (raises error if exceeded)
        
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


# Tool: Bulk Import from JSON File
@mcp.tool(
    name="workflowy_bulk_import",
    description="Create multiple Workflowy nodes from a hierarchical JSON file."
)
async def bulk_import(
    json_file: str,
    parent_id: str | None = None,
    dry_run: bool = False,
    import_policy: str = 'strict',
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

    # Run the server

    mcp.run(transport="stdio")
