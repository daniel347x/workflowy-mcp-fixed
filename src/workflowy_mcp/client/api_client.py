"""WorkFlowy API client implementation."""

import json
from typing import Any

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


class WorkFlowyClient:
    """Async client for WorkFlowy API operations."""

    def __init__(self, config: APIConfiguration):
        """Initialize the WorkFlowy API client."""
        self.config = config
        self.base_url = config.base_url
        self._client: httpx.AsyncClient | None = None

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

    async def create_node(self, request: NodeCreateRequest) -> WorkFlowyNode:
        """Create a new node in WorkFlowy."""
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
        except httpx.TimeoutException as err:
            raise TimeoutError("create_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def update_node(self, node_id: str, request: NodeUpdateRequest) -> WorkFlowyNode:
        """Update an existing node."""
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
        except httpx.TimeoutException as err:
            raise TimeoutError("update_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def get_node(self, node_id: str) -> WorkFlowyNode:
        """Retrieve a specific node by ID."""
        try:
            response = await self.client.get(f"/nodes/{node_id}")
            data = await self._handle_response(response)
            # API returns {"node": {...}} structure
            if isinstance(data, dict) and "node" in data:
                return WorkFlowyNode(**data["node"])
            else:
                # Fallback for unexpected format
                return WorkFlowyNode(**data)
        except httpx.TimeoutException as err:
            raise TimeoutError("get_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def list_nodes(self, request: NodeListRequest) -> tuple[list[WorkFlowyNode], int]:
        """List nodes with optional filtering."""
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
        except httpx.TimeoutException as err:
            raise TimeoutError("list_nodes") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def delete_node(self, node_id: str) -> bool:
        """Delete a node and all its children."""
        try:
            response = await self.client.delete(f"/nodes/{node_id}")
            # Delete endpoint returns just a message, not nested data
            await self._handle_response(response)
            return True
        except httpx.TimeoutException as err:
            raise TimeoutError("delete_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def complete_node(self, node_id: str) -> WorkFlowyNode:
        """Mark a node as completed."""
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
        except httpx.TimeoutException as err:
            raise TimeoutError("complete_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def uncomplete_node(self, node_id: str) -> WorkFlowyNode:
        """Mark a node as not completed."""
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
        except httpx.TimeoutException as err:
            raise TimeoutError("uncomplete_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def move_node(
        self,
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
        try:
            payload = {"position": position}
            if parent_id is not None:
                payload["parent_id"] = parent_id
            
            response = await self.client.post(f"/nodes/{node_id}/move", json=payload)
            data = await self._handle_response(response)
            # API returns {"status": "ok"}
            return data.get("status") == "ok"
        except httpx.TimeoutException as err:
            raise TimeoutError("move_node") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def export_nodes(
        self,
        node_id: str | None = None,
    ) -> dict[str, Any]:
        """Export all nodes or filter to specific node's subtree.
        
        Args:
            node_id: Optional node ID to export only that node and its descendants.
                     If None, exports all nodes in the account.
            
        Returns:
            Dictionary with 'nodes' list containing all exported nodes.
            If node_id is provided, filters to only that node and descendants.
        """
        try:
            # API exports all nodes as flat list (no parameters supported)
            response = await self.client.get("/nodes-export")
            data = await self._handle_response(response)
            
            # If no filtering requested, return everything
            if node_id is None:
                return data
            
            # Filter to specific node and its descendants
            all_nodes = data.get("nodes", [])
            
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
            
            return {"nodes": filtered_nodes}
            
        except httpx.TimeoutException as err:
            raise TimeoutError("export_nodes") from err
        except httpx.NetworkError as e:
            raise NetworkError(f"Network error: {str(e)}") from e

    async def bulk_export_to_file(
        self,
        node_id: str,
        output_file: str,
        include_metadata: bool = True,
    ) -> dict[str, Any]:
        """Export node tree to hierarchical JSON file.
        
        Args:
            node_id: Root node UUID to export from
            output_file: Absolute path where JSON should be written
            include_metadata: Include created_at, modified_at fields (default True)
            
        Returns:
            {"success": True, "file_path": "...", "node_count": N, "depth": M}
        """
        try:
            # Fetch flat node list
            raw_data = await self.export_nodes(node_id)
            flat_nodes = raw_data.get("nodes", [])
            
            if not flat_nodes:
                return {
                    "success": True,
                    "file_path": output_file,
                    "node_count": 0,
                    "depth": 0
                }
            
            # Build hierarchical tree from flat list
            hierarchical_tree = self._build_hierarchy(flat_nodes, include_metadata)
            
            # Calculate max depth
            max_depth = self._calculate_max_depth(hierarchical_tree)
            
            # Write to file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(hierarchical_tree, f, indent=2, ensure_ascii=False)
            
            return {
                "success": True,
                "file_path": output_file,
                "node_count": len(flat_nodes),
                "depth": max_depth
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
    
    async def bulk_import_from_file(
        self,
        json_file: str,
        parent_id: str,
    ) -> dict[str, Any]:
        """Create multiple Workflowy nodes from JSON file.
        
        Args:
            json_file: Absolute path to JSON file with node structure
            parent_id: Parent UUID where nodes should be created
            
        Returns:
            {
                "success": True/False,
                "nodes_created": N,
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
                nodes_to_create = json.load(f)
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
        
        if not isinstance(nodes_to_create, list):
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "api_calls": 0,
                "retries": 0,
                "rate_limit_hits": 0,
                "errors": ["JSON must contain an array of nodes"]
            }
        
        # Stats tracking
        stats = {
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "nodes_created": 0,
            "errors": []
        }
        
        async def create_node_with_retry(
            request: NodeCreateRequest,
            max_retries: int = 5
        ) -> WorkFlowyNode | None:
            """Create node with exponential backoff retry."""
            retry_count = 0
            base_delay = 1.0
            
            while retry_count < max_retries:
                try:
                    # Fixed safety delay (100ms between calls)
                    await asyncio.sleep(0.1)
                    stats["api_calls"] += 1
                    
                    node = await self.create_node(request)
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
                    # Build request
                    request = NodeCreateRequest(
                        name=node_data['name'],
                        parent_id=parent_id,
                        note=node_data.get('note'),
                        layoutMode=node_data.get('layout_mode'),
                        position=node_data.get('position', 'bottom')
                    )
                    
                    # Create with retry logic
                    node = await create_node_with_retry(request)
                    
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
            root_ids = await create_tree(parent_id, nodes_to_create)
            
            # Log summary if retries occurred
            if stats["retries"] > 0:
                logger.warning(
                    f"⚠️ Import completed with {stats['retries']} retries "
                    f"({stats['rate_limit_hits']} rate limit hits). "
                    f"Consider reducing import speed."
                )
            
            return {
                "success": len(stats["errors"]) == 0,
                "nodes_created": stats["nodes_created"],
                "root_node_ids": root_ids,
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"]
            }
            
        except Exception as e:
            error_msg = f"Bulk import failed: {str(e)}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            
            return {
                "success": False,
                "nodes_created": stats["nodes_created"],
                "root_node_ids": [],
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "rate_limit_hits": stats["rate_limit_hits"],
                "errors": stats["errors"]
            }
