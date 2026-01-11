"""WorkFlowy API client - ETCH and tree export/import operations."""

import json
import sys
import os
from typing import Any
from datetime import datetime

from ..models import (
    NetworkError,
    NodeCreateRequest,
    NodeListRequest,
    RateLimitError,
)

from .api_client_core import (
    WorkFlowyClientCore,
    _ClientLogger,
    log_event,
    API_RATE_LIMIT_DELAY,
)

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


class WorkFlowyClientEtch(WorkFlowyClientCore):
    """ETCH operations and tree export/import - extends Core."""

    def _log_to_file(self, message: str, log_type: str = "reconcile") -> None:
        """Log message to a tag-specific debug file (best-effort)."""
        _log_to_file_helper(message, log_type)

    async def export_nodes(
        self,
        node_id: str | None = None,
        max_retries: int = 10,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Export all nodes or filter to specific node's subtree.
        
        Delegates to module-level export_nodes_impl to avoid circular imports.
        """
        return await export_nodes_impl(
            self,
            node_id=node_id,
            max_retries=max_retries,
            use_cache=use_cache,
            force_refresh=force_refresh,
        )

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
        """Export node tree to hierarchical JSON + Markdown.
        
        Delegates to module-level bulk_export_to_file_impl.
        """
        return await bulk_export_to_file_impl(
            self,
            node_id=node_id,
            output_file=output_file,
            include_metadata=include_metadata,
            use_efficient_traversal=use_efficient_traversal,
            max_depth=max_depth,
            child_count_limit=child_count_limit,
            max_nodes=max_nodes,
        )

    @staticmethod
    def _infer_preview_prefix_from_path(path: str) -> str | None:
        """Infer preview_id prefix from a known NEXUS JSON filename."""
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
        max_note_chars: int | None = None,
    ) -> list[str]:
        """Assign preview_id to each node and build a human-readable tree.

        Notes are rendered inline on the same line as the node name.
        By default (max_note_chars=None), notes are **not clipped**; all
        newlines are flattened to literal ``"\\n"`` so each node stays
        on a single line of the preview tree.
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
                if isinstance(max_note_chars, int) and max_note_chars > 0 and len(flat) > max_note_chars:
                    flat = flat[:max_note_chars]
                name_part = f"{name} [{flat}]"
            else:
                name_part = name

            # Include Workflowy UUID first, then local preview_id for cross-referencing
            node_uuid = node.get("id") or ""
            uuid_label = str(node_uuid)
            lines.append(f"[{uuid_label}] [{padded_id}] {indent}{bullet} {name_part}")

            # Recurse into children with extended path
            for idx, child in enumerate(children, start=1):
                if isinstance(child, dict):
                    walk(child, path_parts + [str(idx)])

        for root_index, root in enumerate(nodes, start=1):
            if isinstance(root, dict):
                walk(root, [str(root_index)])

        return lines

    def _build_hierarchy(
        self,
        flat_nodes: list[dict[str, Any]],
        include_metadata: bool = True
    ) -> list[dict[str, Any]]:
        """Convert flat node list to hierarchical tree structure."""
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

        # Pass 3: Sort each sibling group by Workflowy 'priority' field
        def _sort_children_by_priority(nodes_list: list[dict[str, Any]]) -> None:
            for n in nodes_list:
                children = n.get('children') or []
                if children:
                    children.sort(key=lambda c: c.get('priority', 0))
                    _sort_children_by_priority(children)

        _sort_children_by_priority(root_nodes)

        return root_nodes
    
    def _calculate_max_depth(self, nodes: list[dict[str, Any]], current_depth: int = 1) -> int:
        """Calculate maximum depth of node tree."""
        if not nodes:
            return current_depth - 1
        
        max_child_depth = current_depth
        for node in nodes:
            if node.get('children'):
                child_depth = self._calculate_max_depth(node['children'], current_depth + 1)
                max_child_depth = max(max_child_depth, child_depth)
        
        return max_child_depth
    
    def _limit_depth(self, nodes: list[dict[str, Any]], max_depth: int, current_depth: int = 1) -> list[dict[str, Any]]:
        """Limit node tree to specified depth."""
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
        """Annotate child/descendant counts and optionally truncate children."""
        if not nodes:
            return 0

        total_descendants_here = 0

        for node in nodes:
            children = node.get('children') or []

            # First, recursively annotate children
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

            # Decide truncation
            status = 'complete'
            truncate_children = False

            if max_depth is not None and current_depth >= max_depth and immediate_count > 0:
                status = 'truncated_by_depth'
                truncate_children = True

            if (
                not truncate_children
                and child_count_limit is not None
                and immediate_count > child_count_limit
            ):
                status = 'truncated_by_count'
                truncate_children = True

            if truncate_children:
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
        """Convert hierarchical nodes to Markdown format with UUID metadata."""
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
            
            # Truncated UUID path
            truncated_uuids = [uuid[:8] + '...' for uuid in current_path_uuids]
            uuid_path = ' > '.join(truncated_uuids)
            markdown_lines.append(f'<!-- NODE_PATH_UUIDS: {uuid_path} -->')
            
            # Name path
            name_path = ' > '.join(current_path_names)
            markdown_lines.append(f'<!-- NODE_PATH_NAMES: {name_path} -->')
            
            markdown_lines.append('')
            
            # Add note content if present
            note = node.get('note')
            if note and note.strip():
                # Quick detection: check if note looks like markdown
                is_markdown = any(line.strip().startswith('#') for line in note.split('\n'))
                language = 'markdown' if is_markdown else 'text'
                
                # Wrap in 12-backtick delimiter
                markdown_lines.append('````````````' + language)
                markdown_lines.append(note)
                markdown_lines.append('````````````')
                markdown_lines.append('')
            
            # Recursively process children
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
    
    def generate_markdown_from_json(self, json_file: str) -> dict[str, Any]:
        """Convert JSON file to Markdown (without metadata - for edited JSON review)."""
        try:
            # Read JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both bare array AND metadata wrapper
            if isinstance(data, dict) and "nodes" in data:
                nodes = data["nodes"]
            elif isinstance(data, list):
                nodes = data
            else:
                return {
                    "success": False,
                    "markdown_file": None,
                    "error": f"JSON must be array or dict with 'nodes' key. Got: {type(data).__name__}"
                }
            
            # Generate Markdown WITH metadata
            markdown_content = self._generate_markdown(nodes, level=1)
            
            # Write to .md file
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

    async def workflowy_etch(
        self,
        parent_id: str,
        nodes: list[dict[str, Any]] | str,
        replace_all: bool = False,
        nodes_file: str | None = None,
    ) -> dict[str, Any]:
        """Create multiple nodes from JSON structure (ETCH command).

        Args:
            parent_id: Target Workflowy parent id.
            nodes: Either a parsed list[dict] structure or a JSON string
                representing that list. Ignored when nodes_file is provided.
            replace_all: If True, delete all existing children under
                parent_id first; otherwise additive.
            nodes_file: Optional path to a file containing ETCH payload.
                - If the file contains a JSON object with a top-level
                  "nodes" key, that value is used.
                - Else, the entire file content is treated as a JSON
                  string to be parsed the same way as the "nodes" string
                  parameter (including autofix strategies).
        """
        import asyncio

        logger = _ClientLogger()

        # Optional file-based payload: if nodes_file is provided, it wins
        # over the inline "nodes" argument.
        if nodes_file:
            try:
                with open(nodes_file, "r", encoding="utf-8") as f:
                    file_text = f.read()
                # Try JSON object with top-level "nodes" first
                try:
                    maybe_json = json.loads(file_text)
                    if isinstance(maybe_json, dict) and "nodes" in maybe_json:
                        nodes = maybe_json["nodes"]
                    else:
                        # Not a dict-with-nodes; treat entire file_text as
                        # the stringified nodes payload
                        nodes = file_text
                    logger.info(
                        f"📄 workflowy_etch: loaded nodes from file '{nodes_file}'"
                    )
                except json.JSONDecodeError:
                    # Not valid JSON as a whole; assume raw JSON string
                    # payload for nodes and let the normal string handler
                    # below deal with it (including autofix).
                    nodes = file_text
                    logger.info(
                        f"📄 workflowy_etch: using raw file text from '{nodes_file}'"
                    )
            except Exception as e:
                error_msg = (
                    f"Failed to read nodes_file '{nodes_file}': {e}"
                )
                self._log_to_file(error_msg, "etch")
                return {
                    "success": False,
                    "nodes_created": 0,
                    "root_node_ids": [],
                    "errors": [error_msg],
                }
        
        # Auto-fix stringified JSON
        stringify_strategy_used = None
        if isinstance(nodes, str):
            logger.warning("⚠️ Received stringified JSON - attempting parse strategies")

            # Strategy 1: direct json.loads() on the full string
            try:
                parsed = json.loads(nodes)
                nodes = parsed
                stringify_strategy_used = "Strategy 1: Direct json.loads()"
                logger.info(f"✅ {stringify_strategy_used}")
            except json.JSONDecodeError as e:
                # Dan debug note:
                # In practice we've seen cases where the string is a valid JSON
                # array followed by a few stray characters (e.g. the outer '}'
                # from the request body). json.loads() raises
                # JSONDecodeError('Extra data', ...).
                #
                # Rather than naively stripping N characters, we do a
                # _structure-aware_ trim:
                #  - Find the last closing bracket ']' in the string.
                #  - If it exists, take everything up to and including that
                #    bracket and try json.loads() again.
                #  - This preserves the full array and only discards trailing
                #    junk after a syntactically complete list literal.
                #
                # We only apply this once; if it still fails, we surface a
                # clear error and log to etch_debug.log.
                last_bracket = nodes.rfind("]")
                if last_bracket != -1:
                    candidate = nodes[: last_bracket + 1]
                    try:
                        parsed2 = json.loads(candidate)
                        nodes = parsed2
                        stringify_strategy_used = "Strategy 2: Trim after last ']' (recover from trailing junk)"
                        self._log_to_file(
                            "STRINGIFY AUTOFIX: Strategy 2 used. "
                            f"Original len={len(nodes)}, trimmed_len={len(candidate)}. "
                            f"Original JSONDecodeError={repr(e)}",
                            "etch",
                        )
                        logger.info(f"✅ {stringify_strategy_used}")
                    except json.JSONDecodeError as e2:
                        # Still not valid JSON even after trimming at the
                        # last ']'. At this point we give up and return a
                        # structured error so the caller can see it in the
                        # MCP tool response.
                        error_msg = (
                            "Failed to parse stringified JSON after Strategy 1 "
                            "and Strategy 2. "
                            f"Strategy 1 error={repr(e)}, Strategy 2 error={repr(e2)}"
                        )
                        self._log_to_file(error_msg, "etch")
                        return {
                            "success": False,
                            "nodes_created": 0,
                            "root_node_ids": [],
                            "errors": [error_msg],
                        }
                else:
                    # No closing bracket at all; there's nothing safe to trim
                    # against. Log and fail clearly.
                    error_msg = (
                        "Failed to parse stringified JSON: no closing ']' "
                        f"found. Original error={repr(e)}"
                    )
                    self._log_to_file(error_msg, "etch")
                    return {
                        "success": False,
                        "nodes_created": 0,
                        "root_node_ids": [],
                        "errors": [error_msg],
                    }
        
        # Validate nodes is a list
        if not isinstance(nodes, list):
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "errors": ["Parameter 'nodes' must be a list"]
            }
        
        # Validation checkpoint - NOTE fields only
        def validate_and_escape_nodes_recursive(
            nodes_list: list[dict[str, Any]], path: str = "root"
        ) -> tuple[bool, str | None, list[str]]:
            """Recursively validate and auto-escape NAME and NOTE fields."""
            warnings = []
            
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Validate NAME
                name = node.get('name')
                if not isinstance(name, str) or not name.strip():
                    return (
                        False,
                        f"Node: {node_path}\n\nName must be non-empty string.",
                        warnings,
                    )
                
                processed_name, name_warning = self._validate_name_field(name)
                if processed_name is not None:
                    node['name'] = processed_name
                if name_warning:
                    warnings.append(f"{node_path} - Name escaped")
                
                # Validate NOTE
                note = node.get('note')
                if note:
                    processed_note, note_warning = self._validate_note_field(note, skip_newline_check=False)
                    
                    if processed_note is None and note_warning:
                        return (False, f"Node: {node_path}\n\n{note_warning}", warnings)
                    
                    node['note'] = processed_note
                    
                    if note_warning and "AUTO-ESCAPED" in note_warning:
                        warnings.append(f"{node_path} - Note escaped")
                
                # Recurse
                children = node.get('children', [])
                if children:
                    success, error_msg, child_warnings = validate_and_escape_nodes_recursive(children, node_path)
                    if not success:
                        return (False, error_msg, warnings)
                    warnings.extend(child_warnings)
            
            return (True, None, warnings)
        
        success, error_msg, warnings = validate_and_escape_nodes_recursive(nodes)
        
        if not success:
            return {
                "success": False,
                "nodes_created": 0,
                "root_node_ids": [],
                "errors": [error_msg or "Validation failed"]
            }
        
        if warnings:
            logger.info(f"✅ Auto-escaped {len(warnings)} node(s)")
        
        # Stats tracking
        stats = {
            "api_calls": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "nodes_created": 0,
            "skipped": 0,
            "errors": []
        }
        
        # REPLACE_ALL or DEFAULT mode
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
                logger.warning(f"Could not list/delete existing: {e}")
            
            nodes_to_create = nodes
            existing_names = set()
        else:
            # Additive mode - skip existing by name
            try:
                request = NodeListRequest(parentId=parent_id)
                existing_children, _ = await self.list_nodes(request)
                stats["api_calls"] += 1
                
                existing_names = {child.nm.strip() for child in existing_children if child.nm}
                nodes_to_create = [
                    node for node in nodes 
                    if node.get('name', '').strip() not in existing_names
                ]
                
                stats["skipped"] = len(nodes) - len(nodes_to_create)
                
                if stats["skipped"] > 0:
                    logger.info(f"📝 Skipped {stats['skipped']} existing node(s)")
                
                if not nodes_to_create:
                    return {
                        "success": True,
                        "nodes_created": 0,
                        "root_node_ids": [],
                        "skipped": stats["skipped"],
                        "api_calls": stats["api_calls"],
                        "message": "All nodes already exist - nothing to create"
                    }
            except Exception as e:
                logger.warning(f"Could not check existing: {e}")
                nodes_to_create = nodes
                existing_names = set()
        
        # Create tree recursively
        async def create_tree(parent_id: str, nodes: list[dict[str, Any]]) -> list[str]:
            """Recursively create node tree."""
            created_ids = []
            
            for node_data in nodes:
                try:
                    node_name = node_data['name']
                    
                    if not replace_all and node_name in existing_names:
                        stats["skipped"] += 1
                        continue
                    
                    request = NodeCreateRequest(
                        name=node_name,
                        parent_id=parent_id,
                        note=node_data.get('note'),
                        layoutMode=node_data.get('layout_mode'),
                        position=node_data.get('position', 'bottom')
                    )
                    
                    # Create with retry (internal call)
                    node = await self.create_node(request, _internal_call=True)
                    
                    if node:
                        created_ids.append(node.id)
                        stats["nodes_created"] += 1
                        self._log_to_file(f"  Created: {node_name} ({node.id})", "etch")
                        
                        # Recursively create children
                        if 'children' in node_data and node_data['children']:
                            await create_tree(node.id, node_data['children'])
                
                except Exception as e:
                    error_msg = f"Failed to create '{node_data.get('name', 'unknown')}': {str(e)}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
                    continue
            
            return created_ids
        
        try:
            self._log_to_file(f"ETCH start (replace_all={replace_all}) parent={parent_id}", "etch")
            root_ids = await create_tree(parent_id, nodes_to_create)
            
            self._log_to_file(f"ETCH complete: {stats['nodes_created']} created", "etch")
            
            result = {
                "success": len(stats["errors"]) == 0,
                "nodes_created": stats["nodes_created"],
                "root_node_ids": root_ids,
                "api_calls": stats["api_calls"],
                "retries": stats["retries"],
                "errors": stats["errors"]
            }
            
            if not replace_all:
                result["skipped"] = stats["skipped"]
            
            if stringify_strategy_used:
                result["_stringify_autofix"] = stringify_strategy_used

            # Mark parent dirty
            if result.get("success", False):
                try:
                    self._mark_nodes_export_dirty([parent_id])
                except Exception:
                    pass
            
            return result
            
        except Exception as e:
            error_msg = f"ETCH failed: {str(e)}"
            log_event(error_msg, "ETCH")
            stats["errors"].append(error_msg)
            
            return {
                "success": False,
                "nodes_created": stats["nodes_created"],
                "errors": stats["errors"]
            }


async def export_nodes_impl(
    client: WorkFlowyClientCore,
    node_id: str | None = None,
    max_retries: int = 10,
    use_cache: bool = True,
    force_refresh: bool = False,
    _allow_api_refresh: bool = False,
) -> dict[str, Any]:
    """Export all nodes or filter to specific node's subtree (implementation).

    This is extracted as a module-level function to avoid circular imports.

    _allow_api_refresh is an internal guard: only explicit cache-refresh
    operations (triggered by Dan via the UUID widget) are permitted to
    call /nodes-export. All other callers must rely on the existing
    in-memory snapshot and will fail fast if no cache is present.
    """
    import asyncio

    logger = _ClientLogger()

    async def fetch_and_cache() -> dict[str, Any]:
        """Call /nodes-export with retries and update cache."""
        retry_count = 0
        base_delay = 1.0

        if not _allow_api_refresh:
            raise NetworkError(
                "export_nodes_impl: automatic /nodes-export refresh is disabled; "
                "run workflowy_refresh_nodes_export_cache from the UUID widget."
            )

        while retry_count < max_retries:
            await asyncio.sleep(API_RATE_LIMIT_DELAY)

            try:
                response = await client.client.get("/nodes-export")
                data = await client._handle_response(response)

                all_nodes = data.get("nodes", []) or []

                # Dewhiten names/notes
                for node in all_nodes:
                    for key in ("name", "nm", "note", "no"):
                        if key in node:
                            node[key] = client._dewhiten_text(node.get(key))

                total_before_filter = len(all_nodes)
                data["_total_fetched_from_api"] = total_before_filter

                # Update cache
                client._nodes_export_cache = data
                client._nodes_export_cache_timestamp = datetime.now()
                client._nodes_export_dirty_ids.clear()

                if retry_count > 0:
                    success_msg = f"export_nodes succeeded after {retry_count + 1}/{max_retries} attempts"
                    logger.info(success_msg)
                    _log_to_file_helper(success_msg, "reconcile")

                return data

            except Exception as e:
                retry_count += 1

                # Make rate-limit waits explicit in logs so F12/WEAVE behavior is
                # easier to interpret when /nodes-export is throttled.
                if isinstance(e, RateLimitError):
                    retry_after_hint = None
                    try:
                        # RateLimitError.details may carry a "retry_after" hint.
                        retry_after_hint = getattr(e, "details", {}).get("retry_after")
                    except Exception:
                        retry_after_hint = None

                    retry_delay = base_delay * (2 ** retry_count)
                    msg = (
                        "export_nodes_impl: Rate limited on /nodes-export. "
                        f"Retry {retry_count}/{max_retries} after {retry_delay:.1f}s "
                        f"(retry_after={retry_after_hint!r})"
                    )
                    logger.warning(msg)
                    _log_to_file_helper(msg, "reconcile")
                else:
                    logger.warning(
                        f"Export error: {e}. Retry {retry_count}/{max_retries}"
                    )

                if retry_count < max_retries:
                    await asyncio.sleep(base_delay * (2 ** retry_count))
                else:
                    raise

        raise NetworkError("export_nodes failed after maximum retries")

    # Decide cache vs fetch
    if (not use_cache) or force_refresh or client._nodes_export_cache is None:
        if not _allow_api_refresh:
            # No automatic cache refresh; require explicit refresh via widget.
            raise NetworkError(
                "nodes-export cache is not initialized; run "
                "workflowy_refresh_nodes_export_cache from the UUID widget."
            )
        data = await fetch_and_cache()
    else:
        data = client._nodes_export_cache

        # Check dirty IDs for subtree requests
        if node_id is not None and client._nodes_export_dirty_ids:
            all_nodes = data.get("nodes", []) or []
            nodes_by_id = {n.get("id"): n for n in all_nodes if n.get("id")}

            if node_id not in nodes_by_id:
                # In manual mode, this indicates a stale snapshot; surface a
                # clear error instead of silently triggering a fresh export.
                logger.warning(
                    "export_nodes_impl: node %s not present in cached /nodes-export; "
                    "automatic refresh is disabled", node_id,
                )
                raise NetworkError(
                    "nodes-export cache does not include requested node; run "
                    "workflowy_refresh_nodes_export_cache from the UUID widget."
                )
            else:
                dirty = client._nodes_export_dirty_ids
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
                    # Honor Dan's preference: do NOT trigger API refresh here; just log.
                    logger.info(
                        "export_nodes_impl: path from %s hits dirty; using existing "
                        "nodes-export cache (automatic refresh disabled)",
                        node_id,
                    )
                else:
                    logger.info(f"Using cached export for {node_id}")

    # Filter if needed
    all_nodes = data.get("nodes", []) or []
    total_before_filter = len(all_nodes)

    if node_id is None:
        if "_total_fetched_from_api" not in data:
            data["_total_fetched_from_api"] = total_before_filter
        return data

    # Filter to subtree
    included_ids = {node_id}
    nodes_by_id = {node["id"]: node for node in all_nodes if node.get("id")}

    def add_descendants(parent_id: str) -> None:
        """Recursively add all descendants whose parent matches parent_id.

        IMPORTANT: /nodes-export historically used `parent_id` (snake_case) in
        its payload, but our in-memory cache may also contain nodes that were
        appended via single-node operations (create_node, etc.) whose parent
        linkage lives in `parentId` (camelCase). To avoid losing newly-created
        children in subtree exports, we must honor BOTH keys here.
        """
        for node in all_nodes:
            nid = node.get("id")
            if not nid:
                continue
            pid = node.get("parent_id") or node.get("parentId")
            if pid == parent_id and nid not in included_ids:
                included_ids.add(nid)
                add_descendants(str(nid))

    if node_id in nodes_by_id:
        add_descendants(node_id)

    filtered_nodes = [node for node in all_nodes if node["id"] in included_ids]

    return {
        "nodes": filtered_nodes,
        "_total_fetched_from_api": data.get("_total_fetched_from_api", total_before_filter),
        "_filtered_count": len(filtered_nodes),
    }


async def bulk_export_to_file_impl(
    client: WorkFlowyClientCore,
    node_id: str,
    output_file: str,
    include_metadata: bool = True,
    use_efficient_traversal: bool = False,
    max_depth: int | None = None,
    child_count_limit: int | None = None,
    max_nodes: int | None = None,
) -> dict[str, Any]:
    """Export node tree to hierarchical JSON + Markdown (implementation)."""
    # Full implementation from original
    # [Lines ~800-1100 from original api_client.py]
    # Delegated to standalone function to avoid circular imports
    
    # Efficient traversal or full export
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
            
            request = NodeListRequest(parentId=parent)
            children, count = await client.list_nodes(request)
            api_calls_made += 1
            total_nodes_fetched += count
            
            for child in children:
                child_dict = child.model_dump()
                parent_id_val = child_dict.get("parent_id") or child_dict.get("parentId")
                if not parent_id_val:
                    child_dict["parent_id"] = parent
                flat_nodes.append(child_dict)
                queue.append(child.id)
        
        root_node_data = await client.get_node(node_id)
        api_calls_made += 1
        flat_nodes.insert(0, root_node_data.model_dump())
        total_nodes_fetched += 1
    else:
        raw_data = await export_nodes_impl(client, node_id)
        flat_nodes = raw_data.get("nodes", [])
        total_nodes_fetched = raw_data.get("_total_fetched_from_api", len(flat_nodes))
        api_calls_made = 1
    
    # Safety check
    subtree_node_count = len(flat_nodes)
    if max_nodes is not None and subtree_node_count > max_nodes:
        raise NetworkError(
            f"SCRY size ({subtree_node_count}) exceeds limit ({max_nodes}).\n\n"
            f"Options:\n"
            f"1. Increase max_nodes\n"
            f"2. Use max_depth or child_count_limit\n"
            f"3. Use nexus_scry + nexus_ignite_shards"
        )
    
    if not flat_nodes:
        return {
            "success": True,
            "file_path": output_file,
            "node_count": 0,
            "depth": 0
        }
    
    # Build hierarchy (delegate to client method through type cast)
    if not isinstance(client, WorkFlowyClientEtch):
        raise NetworkError("Client must be WorkFlowyClientEtch for hierarchy building")
    
    hierarchical_tree = client._build_hierarchy(flat_nodes, include_metadata)

    # Extract root info
    root_node_info = None
    root_immediate_child_count = None
    if hierarchical_tree and len(hierarchical_tree) == 1:
        root_node = hierarchical_tree[0]
        
        # Build path to root
        path_uuids = [root_node.get('id')]
        path_names = [root_node.get('name')]
        
        current_parent_id = root_node.get('parent_id')
        while current_parent_id:
            try:
                parent_node_data = await client.get_node(current_parent_id)
                path_uuids.insert(0, parent_node_data.id)
                path_names.insert(0, parent_node_data.nm or 'Untitled')
                current_parent_id = parent_node_data.parentId
            except Exception:
                break
        
        root_node_info = {
            'id': root_node.get('id'),
            'name': root_node.get('name'),
            'parent_id': root_node.get('parent_id'),
            'full_path_uuids': path_uuids,
            'full_path_names': path_names
        }
        root_immediate_child_count = len(root_node.get('children') or [])
        hierarchical_tree = root_node.get('children', [])
    else:
        raise NetworkError("SCRY produced multiple roots - invalid structure")
    
    # Determine root truncation
    root_has_hidden_children = False
    root_children_status = "complete"
    
    if (
        child_count_limit is not None
        and root_immediate_child_count is not None
        and root_immediate_child_count > child_count_limit
    ):
        root_has_hidden_children = True
        root_children_status = "truncated_by_count"
        hierarchical_tree = []
    
    # Annotate counts and truncate
    if hierarchical_tree:
        client._annotate_child_counts_and_truncate(
            hierarchical_tree,
            max_depth=max_depth,
            child_count_limit=child_count_limit,
            current_depth=1,
        )
    
    tree_depth = client._calculate_max_depth(hierarchical_tree)
    
    # Build ledger
    original_ids_seen = sorted({n.get('id') for n in flat_nodes if n.get('id')})
    
    # Compute preview
    prefix = client._infer_preview_prefix_from_path(output_file)
    preview_tree = None
    if prefix:
        preview_tree = client._annotate_preview_ids_and_build_tree(hierarchical_tree, prefix)
    
    # Wrap with metadata
    export_package = {
        "export_timestamp": hierarchical_tree[0].get('modifiedAt') if hierarchical_tree else None,
        "export_root_children_status": root_children_status,
        "__preview_tree__": preview_tree,
        "export_root_id": node_id,
        "export_root_name": root_node_info.get('name') if root_node_info else 'Unknown',
        "nodes": hierarchical_tree,
        "original_ids_seen": original_ids_seen
    }
    
    # Write files
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_package, f, indent=2, ensure_ascii=False)
    
    json_backup = output_file.replace('.json', '.original.json')
    with open(json_backup, 'w', encoding='utf-8') as f:
        json.dump(export_package, f, indent=2, ensure_ascii=False)
    
    markdown_file = output_file.replace('.json', '.md')
    markdown_content = client._generate_markdown(hierarchical_tree, root_node_info=root_node_info)
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    markdown_backup = output_file.replace('.json', '.original.md')
    with open(markdown_backup, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

    # Create Keystone
    keystone_path = None
    try:
        import shutil
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
        print(f"Keystone backup failed: {e}")

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
