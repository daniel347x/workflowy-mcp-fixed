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


class WorkFlowyClient:
    """Async client for WorkFlowy API operations."""

    def __init__(self, config: APIConfiguration):
        """Initialize the WorkFlowy API client."""
        self.config = config
        self.base_url = config.base_url
        self._client: httpx.AsyncClient | None = None
    
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

    def _log_reconcile_retry(self, message: str) -> None:
        """Best-effort mirror of rate-limit retry info into reconcile_debug.log.

        This is in addition to normal logger output and is safe to call even when
        no reconciliation is running (it simply appends to the standard log file
        path used by the NEXUS reconciliation algorithm).
        """
        try:
            from datetime import datetime
            log_path = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\reconcile_debug.log"
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            with open(log_path, "a", encoding="utf-8") as dbg:
                dbg.write(f"[{ts}] {message}\n")
        except Exception:
            # Never let logging failures affect API behavior
            pass

    async def create_node(self, request: NodeCreateRequest, _internal_call: bool = False, max_retries: int = 10) -> WorkFlowyNode:
        """Create a new node in WorkFlowy with exponential backoff retry.
        
        Args:
            request: Node creation request
            _internal_call: Internal flag - bypasses single-node forcing function (not exposed to MCP)
            max_retries: Maximum retry attempts (default 10)
        """
        import asyncio
        import logging

        logger = logging.getLogger(__name__)

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
        
        # Validate and escape note field
        # Skip newline check if internal call (for bulk operations testing)
        processed_note, message = self._validate_note_field(request.note, skip_newline_check=_internal_call)
        
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
                response = await self.client.post("/nodes/", json=request.model_dump(exclude_none=True))
                data = await self._handle_response(response)
                # Create endpoint returns just {"item_id": "..."}
                item_id = data.get("item_id")
                if not item_id:
                    raise NetworkError(f"Invalid response from create endpoint: {data}")

                # Fetch the created node to get actual saved state (including note field)
                get_response = await self.client.get(f"/nodes/{item_id}")
                node_data = await self._handle_response(get_response)
                return WorkFlowyNode(**node_data["node"])

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
                logger.warning(
                    f"Network error on create_node: {e}. Retry {retry_count}/{max_retries}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise

            except httpx.TimeoutException as err:
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
        import logging
        
        logger = logging.getLogger(__name__)
        
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
                    return WorkFlowyNode(**node_data["node"])
                else:
                    # Fallback for unexpected format
                    return WorkFlowyNode(**data)
                    
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
                raise TimeoutError("update_node") from err
        
        raise NetworkError("update_node failed after maximum retries")

    async def get_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Retrieve a specific node by ID with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to retrieve
            max_retries: Maximum retry attempts (default 5)
        """
        import asyncio
        import logging
        
        logger = logging.getLogger(__name__)
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
                raise TimeoutError("get_node") from err
        
        raise NetworkError("get_node failed after maximum retries")

    async def list_nodes(self, request: NodeListRequest, max_retries: int = 10) -> tuple[list[WorkFlowyNode], int]:
        """List nodes with optional filtering and exponential backoff retry.
        
        Args:
            request: Node list request
            max_retries: Maximum retry attempts (default 5)
        """
        import asyncio
        import logging
        
        logger = logging.getLogger(__name__)
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
                raise TimeoutError("list_nodes") from err
        
        raise NetworkError("list_nodes failed after maximum retries")

    async def delete_node(self, node_id: str, max_retries: int = 10) -> bool:
        """Delete a node and all its children with exponential backoff retry.
        
        Args:
            node_id: The ID of the node to delete
            max_retries: Maximum retry attempts (default 5)
        """
        import asyncio
        import logging
        
        logger = logging.getLogger(__name__)
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
                    self._log_reconcile_retry(success_msg)
                return True
                
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                retry_msg = (
                    f"Rate limited on delete_node {node_id}. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                logger.warning(retry_msg)
                self._log_reconcile_retry(retry_msg)
                
                if retry_count < max_retries:
                    await asyncio.sleep(retry_after)
                else:
                    final_msg = (
                        f"delete_node {node_id} exhausted retries ({retry_count}/{max_retries}) "
                        f"due to rate limiting – aborting."
                    )
                    logger.error(final_msg)
                    self._log_reconcile_retry(final_msg)
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
                raise TimeoutError("delete_node") from err
        
        raise NetworkError("delete_node failed after maximum retries")

    async def complete_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Mark a node as completed with exponential backoff retry."""
        import asyncio
        import logging

        logger = logging.getLogger(__name__)
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
                raise TimeoutError("complete_node") from err

        raise NetworkError("complete_node failed after maximum retries")

    async def uncomplete_node(self, node_id: str, max_retries: int = 10) -> WorkFlowyNode:
        """Mark a node as not completed with exponential backoff retry."""
        import asyncio
        import logging

        logger = logging.getLogger(__name__)
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
        import logging
        
        logger = logging.getLogger(__name__)
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
                return data.get("status") == "ok"
                
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
                raise TimeoutError("move_node") from err
        
        raise NetworkError("move_node failed after maximum retries")

    async def export_nodes(
        self,
        node_id: str | None = None,
        max_retries: int = 10,
    ) -> dict[str, Any]:
        """Export all nodes or filter to specific node's subtree with exponential backoff retry.
        
        Args:
            node_id: Optional node ID to export only that node and its descendants.
                     If None, exports all nodes in the account.
            max_retries: Maximum retry attempts (default 5)
            
        Returns:
            Dictionary with 'nodes' list containing all exported nodes.
            If node_id is provided, filters to only that node and descendants.
        """
        import asyncio
        import logging
        
        logger = logging.getLogger(__name__)
        retry_count = 0
        base_delay = 1.0
        
        while retry_count < max_retries:
            # Force 1s delay at START of each iteration (rate limit protection)
            await asyncio.sleep(1.0)
            
            try:
                # API exports all nodes as flat list (no parameters supported)
                response = await self.client.get("/nodes-export")
                data = await self._handle_response(response)
                
                all_nodes = data.get("nodes", [])
                total_before_filter = len(all_nodes)
                
                # If no filtering requested, return everything
                if node_id is None:
                    data["_total_fetched_from_api"] = total_before_filter
                    if retry_count > 0:
                        success_msg = (
                            f"export_nodes (full account or subtree) succeeded after {retry_count + 1}/{max_retries} attempts "
                            f"following rate limiting or transient errors."
                        )
                        logger.info(success_msg)
                        self._log_reconcile_retry(success_msg)
                    return data
                
                # Filter to specific node and its descendants
                
                # Build set of node IDs to include (target node + all descendants)
                included_ids = {node_id}
                nodes_by_id = {node["id"]: node for node in all_nodes}
                
                # Find all descendants recursively
                def add_descendants(parent_id: str) -> None:
                    for node in all_nodes:
                        if node.get("parent_id") == parent_id and node["id"] not in included_ids:
                            included_ids.add(node["id"])
                            add_descendants(node["id"])
                
                # Start with target node's children
                if node_id in nodes_by_id:
                    add_descendants(node_id)
                
                # Filter nodes list
                filtered_nodes = [node for node in all_nodes if node["id"] in included_ids]
                
                if retry_count > 0:
                    success_msg = (
                        f"export_nodes (filtered subtree {node_id}) succeeded after {retry_count + 1}/{max_retries} attempts "
                        f"following rate limiting or transient errors."
                    )
                    logger.info(success_msg)
                    self._log_reconcile_retry(success_msg)
                
                return {
                    "nodes": filtered_nodes,
                    "_total_fetched_from_api": total_before_filter,
                    "_filtered_count": len(filtered_nodes)
                }
                
            except RateLimitError as e:
                retry_count += 1
                retry_after = getattr(e, 'retry_after', None) or (base_delay * (2 ** retry_count))
                retry_msg = (
                    f"Rate limited on export_nodes. Retry after {retry_after}s. "
                    f"Attempt {retry_count}/{max_retries}"
                )
                logger.warning(retry_msg)
                self._log_reconcile_retry(retry_msg)
                
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
                raise TimeoutError("export_nodes") from err
        
        raise NetworkError("export_nodes failed after maximum retries")

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
        import logging
        
        logger = logging.getLogger(__name__)
        
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
        logger = logging.getLogger(__name__)
        
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
            
            # Extract root node metadata and children separately
            root_metadata = None
            children = []
            
            if hierarchical_tree and len(hierarchical_tree) == 1:
                root_node = hierarchical_tree[0]
                root_metadata = {
                    "id": root_node.get('id'),
                    "name": root_node.get('name'),
                    "note": root_node.get('note'),
                    "parent_id": root_node.get('parent_id')
                }
                children = root_node.get('children', [])
            else:
                # Multiple roots or no clear root - return as-is
                children = hierarchical_tree
            
            # Apply depth limiting if requested
            if depth is not None:
                children = self._limit_depth(children, depth)
            
            # Calculate max depth
            max_depth = self._calculate_max_depth(children)
            
            return {
                "success": True,
                "root": root_metadata,
                "children": children,
                "node_count": len(flat_nodes),
                "depth": max_depth,
                "_source": "api"  # Indicate data came from API (not WebSocket)
            }
            
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
        import logging
        import json
        
        logger = logging.getLogger(__name__)
        
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
        
        def validate_and_escape_notes_recursive(nodes_list: list[dict[str, Any]], path: str = "root") -> tuple[bool, str | None, list[str]]:
            """Recursively validate and auto-escape NOTE fields only.
            
            Returns:
                (success, error_message, warnings_list)
            """
            warnings = []
            
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Validate and escape NOTE field
                note = node.get('note')
                if note:
                    processed_note, message = self._validate_note_field(note, skip_newline_check=False)
                    
                    if processed_note is None and message:  # Blocking error
                        return (False, f"Node: {node_path}\n\n{message}", warnings)
                    
                    # Update node with escaped/validated note
                    node['note'] = processed_note
                    
                    # Collect warning if escaping occurred
                    if message and "AUTO-ESCAPED" in message:
                        warnings.append(f"Node: {node_path} - Angle brackets auto-escaped")
                
                # Recursively process children
                children = node.get('children', [])
                if children:
                    success, error_msg, child_warnings = validate_and_escape_notes_recursive(children, node_path)
                    if not success:
                        return (False, error_msg, warnings)
                    warnings.extend(child_warnings)
            
            return (True, None, warnings)
        
        # Run validation on NOTE fields
        success, error_msg, warnings = validate_and_escape_notes_recursive(nodes)
        
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
            logger.info(f"\u2705 Auto-escaped angle brackets in {len(warnings)} note(s)")
            for warning in warnings:
                logger.info(f"  - {warning}")
                
        # Validate and escape NOTE fields in entire tree (modifies nodes in-place)
        success, error_msg, warnings = validate_and_escape_notes_recursive(nodes)
        
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
            logger.info(f"\u2705 Auto-escaped angle brackets in {len(warnings)} note(s)")
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
                        
                        # Recursively create children
                        if 'children' in node_data and node_data['children']:
                            await create_tree(node.id, node_data['children'])
                    
                except Exception as e:
                    error_msg = f"Failed to create node '{node_data.get('name', 'unknown')}': {str(e)}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
                    # Continue with other nodes
                    continue
            
            return created_ids
        
        # Create the tree
        try:
            root_ids = await create_tree(parent_id, nodes)
            
            # Log summary if retries occurred
            if stats["retries"] > 0:
                logger.warning(
                    f"⚠️ Bulk write completed with {stats['retries']} retries "
                    f"({stats['rate_limit_hits']} rate limit hits). "
                    f"Consider reducing import speed."
                )
            
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
            
            return result
            
        except Exception as e:
            error_msg = f"Bulk write failed: {str(e)}"
            logger.error(error_msg)
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
        import logging
        
        logger = logging.getLogger(__name__)

        # Read JSON file
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                payload = json.load(f)
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
        logger.info(f"Detected export package with export_root_id={export_root_id}")
        
        # Validate header fields
        if not export_root_id or not isinstance(nodes_to_create, list):
            raise NetworkError(
                "NEXUS SCRY header malformed: 'export_root_id' missing or 'nodes' is not a list. "
                "Do not strip or rewrite the guardian block; re-scry if needed."
            )
        
        # Use export_root_id as default if parent_id not provided
        target_backup_file = None
        if parent_id is None:
            parent_id = export_root_id
            logger.info(f"Using export_root_id as parent_id: {parent_id}")
        else:
            # parent_id was explicitly provided - check if it's different from export_root_id
            if export_root_id and parent_id != export_root_id:
                # AUTO-BACKUP: They're overriding the parent - backup target first!
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                target_backup_file = json_file.replace('.json', f'.target_backup_{timestamp}.json')
                logger.warning(f"Parent override detected! export_root_id={export_root_id}, provided parent_id={parent_id}")
                logger.info(f"Auto-backing up target to: {target_backup_file}")
                try:
                    backup_result = await self.bulk_export_to_file(parent_id, target_backup_file)
                    logger.info(f"Target backup complete: {backup_result.get('node_count', 0)} nodes")
                except Exception as e:
                    logger.error(f"Target backup failed: {e}")
                    # Continue anyway - backup failure shouldn't block import
        
        # 🔥 VALIDATE & AUTO-ESCAPE NOTE FIELDS (angle brackets & newline escapes) 🔥
        def validate_and_escape_nodes_recursive(nodes_list: list[dict[str, Any]], path: str = "root") -> tuple[bool, str | None, list[str]]:
            """Recursively validate and auto-escape all note fields in node tree.
            
            Returns:
                (success, error_message, warnings_list)
            """
            warnings = []
            
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Validate and escape this node's note field
                note = node.get('note')
                if note:
                    processed_note, message = self._validate_note_field(note)
                    
                    if processed_note is None and message:  # Blocking error
                        return (False, f"Node: {node_path}\n\n{message}", warnings)
                    
                    # Update node with escaped note
                    node['note'] = processed_note
                    
                    # Collect warning if escaping occurred
                    if message and "AUTO-ESCAPED" in message:
                        warnings.append(f"Node: {node_path} - Angle brackets auto-escaped")
                
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
            logger.info(f"✅ Auto-escaped angle brackets in {len(warnings)} note(s)")
            for warning in warnings:
                logger.info(f"  - {warning}")
        
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
            await self.update_node(node_uuid, request)
            stats["api_calls"] += 1
            stats["nodes_updated"] += 1
        
        async def delete_node_wrapper(node_uuid: str) -> None:
            """Wrapper for delete_node."""
            await self.delete_node(node_uuid)
            stats["api_calls"] += 1
            stats["nodes_deleted"] += 1
        
        async def move_node_wrapper(node_uuid: str, new_parent_uuid: str, position: str = "top") -> None:
            """Wrapper for move_node."""
            await self.move_node(node_uuid, new_parent_uuid, position)
            stats["api_calls"] += 1
            stats["nodes_moved"] += 1
        
        async def export_nodes_wrapper(node_uuid: str) -> dict:
            """Wrapper for export_nodes - single bulk API call."""
            raw_data = await self.export_nodes(node_uuid)
            stats["api_calls"] += 1
            return raw_data
        
        # ============ EXECUTE RECONCILIATION ============
        
        try:
            # Pass full payload (including export_root_id and guardian metadata)
            # so the reconciliation algorithm can enforce parent consistency.
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
                dry_run=dry_run
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
            
            # Reconciliation complete - gather root IDs
            root_ids = [n.get('id') for n in nodes_to_create if n.get('id')]
            
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
                "errors": stats["errors"]
            }
            
            # Add backup file info if auto-backup was created
            if target_backup_file:
                result["target_backup"] = target_backup_file
            
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
        backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
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
