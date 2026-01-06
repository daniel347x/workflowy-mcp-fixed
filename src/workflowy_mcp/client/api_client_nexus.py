"""WorkFlowy API client - NEXUS pipeline operations."""

import json
import sys
import os
from typing import Any
from datetime import datetime
from pathlib import Path

from ..models import NetworkError, NodeCreateRequest, NodeUpdateRequest

from .api_client_core import (
    WorkFlowyClientCore,
    _ClientLogger,
    log_event,
)

from .api_client_etch import (
    WorkFlowyClientEtch,
    _log_to_file_helper,
    _current_weave_context,
    export_nodes_impl,
    bulk_export_to_file_impl,
)

from .nexus_helper import (
    mark_all_nodes_as_potentially_truncated,
    merge_glimpses_for_terrain,
    build_original_ids_seen_from_glimpses,
    build_terrain_export_from_glimpse,
)


def _log_glimpse_to_file(operation_type: str, node_id: str, result: dict[str, Any]) -> None:
    """Log GLIMPSE operations to persistent markdown files."""
    try:
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
            
            # Truncated JSON preview
            json_preview = json_module.dumps(result, indent=2, ensure_ascii=False)
            preview_lines = json_preview.split('\n')[:50]
            if len(preview_lines) < len(json_preview.split('\n')):
                preview_lines.append("... (truncated)")
            f.write("```json\n")
            f.write('\n'.join(preview_lines))
            f.write("\n```\n\n---\n\n")
    except Exception:
        pass


def is_pid_running(pid: int) -> bool:
    """Check if a process ID is currently running."""
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
    """Scan nexus_runs directory for active detached WEAVE processes."""
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
                job_info = {
                    "job_id": f"weave-enchanted-{tag_dir.name}",
                    "nexus_tag": tag_dir.name,
                    "pid": pid,
                    "status": "running",
                    "detached": True,
                    "mode": "enchanted",
                    "journal": str(journal_file) if journal_file.exists() else None
                }
                
                # Read journal for progress
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
                # Stale PID - clean up
                try:
                    pid_file.unlink()
                    log_event(f"Cleaned stale PID for {tag_dir.name}", "CLEANUP")
                except Exception:
                    pass
        except Exception as e:
            log_event(f"Error scanning {tag_dir.name}: {e}", "SCAN")
            continue
    
    return active


class WorkFlowyClientNexus(WorkFlowyClientEtch):
    """NEXUS pipeline operations - extends Etch."""

    def _get_nexus_dir(self, nexus_tag: str) -> str:
        """Resolve base directory for a CORINTHIAN NEXUS run."""
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        if not base_dir.exists():
            raise NetworkError(
                "No NEXUS runs directory; run nexus_scry/nexus_glimpse first"
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
                f"No NEXUS run for tag '{nexus_tag}'. Run nexus_scry/nexus_glimpse first."
            )

        chosen = sorted(candidates, key=lambda p: p.name)[-1]
        return str(chosen)

    async def workflowy_glimpse(
        self, node_id: str, use_efficient_traversal: bool = False,
        output_file: str | None = None, _ws_connection=None, _ws_queue=None
    ) -> dict[str, Any]:
        """Load node tree via WebSocket (GLIMPSE command)."""
        import asyncio
        import json as json_module

        logger = _ClientLogger()
        
        # Try WebSocket first
        if _ws_connection and _ws_queue:
            try:
                logger.info(f"🔌 WebSocket extraction for {node_id[:8]}...")
                
                request = {"action": "extract_dom", "node_id": node_id}
                await _ws_connection.send(json_module.dumps(request))
                
                response = await asyncio.wait_for(_ws_queue.get(), timeout=5.0)
                
                if response.get('success') and 'children' in response:
                    response['_source'] = 'websocket'
                    logger.info("✅ WebSocket GLIMPSE successful")
                    
                    # Attach preview_tree
                    preview_tree = None
                    try:
                        root_obj = response.get("root") or {}
                        children = response.get("children") or []
                        if isinstance(root_obj, dict):
                            root_obj.setdefault("children", children)
                            preview_tree = self._annotate_preview_ids_and_build_tree([root_obj], "WG")
                        else:
                            preview_tree = self._annotate_preview_ids_and_build_tree(children, "WG")
                    except Exception:
                        pass
                    
                    if preview_tree is not None:
                        old = response
                        response = {
                            "success": old.get("success"),
                            "_source": old.get("_source"),
                            "node_count": old.get("node_count"),
                            "depth": old.get("depth"),
                            "preview_tree": preview_tree,
                            "root": old.get("root"),
                            "children": old.get("children"),
                        }
                    
                    _log_glimpse_to_file("glimpse", node_id, response)
                    
                    # Write TERRAIN if requested
                    if output_file is not None:
                        return await self._write_glimpse_as_terrain(node_id, response, output_file)
                    
                    return response
                else:
                    raise NetworkError("WebSocket invalid response")
                    
            except asyncio.TimeoutError:
                raise NetworkError("WebSocket timeout (5s)")
            except Exception as e:
                raise NetworkError(f"WebSocket error: {e}") from e
        
        raise NetworkError("WebSocket unavailable - use workflowy_scry")
    
    async def workflowy_scry(
        self,
        node_id: str,
        use_efficient_traversal: bool = False,
        depth: int | None = None,
        size_limit: int = 1000,
        output_file: str | None = None,
    ) -> dict[str, Any]:
        """Load node tree via API (Mode 2 - Agent hunts)."""
        logger = _ClientLogger()

        # Delegate to bulk_export if output_file requested
        if output_file is not None:
            result = await bulk_export_to_file_impl(
                self,
                node_id=node_id,
                output_file=output_file,
                include_metadata=True,
                use_efficient_traversal=use_efficient_traversal,
                max_depth=depth,
                child_count_limit=None,
                max_nodes=size_limit,
            )
            return {
                "success": True,
                "mode": "file",
                "terrain_file": output_file,
                "markdown_file": result.get("markdown_file"),
                "node_count": result.get("node_count"),
                "depth": result.get("depth"),
                "_source": "api",
            }
        
        # In-memory return
        if use_efficient_traversal:
            from collections import deque
            from ..models import NodeListRequest
            
            flat_nodes = []
            queue = deque([node_id])
            visited = set()
            
            while queue:
                parent = queue.popleft()
                if parent in visited:
                    continue
                visited.add(parent)
                
                request = NodeListRequest(parentId=parent)
                children, count = await self.list_nodes(request)
                
                for child in children:
                    child_dict = child.model_dump()
                    parent_id_val = child_dict.get("parent_id") or child_dict.get("parentId")
                    if not parent_id_val:
                        child_dict["parent_id"] = parent
                    flat_nodes.append(child_dict)
                    queue.append(child.id)
            
            root_node_data = await self.get_node(node_id)
            flat_nodes.insert(0, root_node_data.model_dump())
        else:
            raw_data = await export_nodes_impl(self, node_id)
            flat_nodes = raw_data.get("nodes", [])
        
        if not flat_nodes:
            return {
                "success": True,
                "root": None,
                "children": [],
                "node_count": 0,
                "depth": 0,
                "_source": "api"
            }
        
        # Size limit handling
        # Old behavior: hard error when subtree exceeds size_limit.
        # New behavior (ground-truth semantics): optionally TRUNCATE instead of erroring,
        # and mark nodes with children_status='truncated_by_max_nodes'.
        #
        # Rationale: Exploration sessions must be able to ingest partial trees while
        # preserving epistemic completeness metadata.
        #
        # Default remains SAFE: if size_limit is exceeded and no truncation is requested,
        # raise NetworkError.
        allow_truncate_by_max_nodes = True
        did_truncate_by_max_nodes = False
        if size_limit and len(flat_nodes) > size_limit:
            if not allow_truncate_by_max_nodes:
                raise NetworkError(
                    f"Tree size ({len(flat_nodes)}) exceeds limit ({size_limit}).\n\n"
                    f"Options:\n"
                    f"1. Increase size_limit\n"
                    f"2. Use depth parameter\n"
                    f"3. Use GLIMPSE (WebSocket)"
                )

            did_truncate_by_max_nodes = True

            # Keep root + the first (size_limit - 1) nodes (conservative truncation).
            # Note: flat_nodes is a pre-order-ish list from /nodes-export filtered to subtree.
            # This truncation is intentionally coarse; downstream logic must treat it as incomplete.
            flat_nodes = flat_nodes[: max(1, size_limit)]
        
        # Build hierarchy
        hierarchical_tree = self._build_hierarchy(flat_nodes, True)

        # Ground truth completeness semantics:
        # workflowy_scry() (API) returns a full subtree (subject only to explicit size_limit/depth).
        # Therefore, we can safely mark children_status='complete' for all nodes we actually have.
        # NOTE: If a depth limit is applied below, those nodes will be truncated structurally
        # and must NOT remain marked complete (handled after depth limiting).
        def _stamp_children_status_complete(nodes: list[dict[str, Any]]) -> None:
            for n in nodes or []:
                if not isinstance(n, dict):
                    continue
                n["children_status"] = "complete"
                _stamp_children_status_complete(n.get("children") or [])

        _stamp_children_status_complete(hierarchical_tree)

        # If we truncated the flat export due to size_limit, the returned tree is incomplete.
        # Mark all nodes as truncated_by_max_nodes (fail-closed).
        if did_truncate_by_max_nodes:
            def _stamp_children_status_truncated(nodes: list[dict[str, Any]]) -> None:
                for n in nodes or []:
                    if not isinstance(n, dict):
                        continue
                    n["children_status"] = "truncated_by_max_nodes"
                    _stamp_children_status_truncated(n.get("children") or [])
            _stamp_children_status_truncated(hierarchical_tree)

        # Attach preview
        try:
            preview_tree = self._annotate_preview_ids_and_build_tree(hierarchical_tree, "WS")
        except Exception:
            preview_tree = []

        # Extract root
        root_metadata = None
        children = []
        root_children_status = "complete"
        
        if hierarchical_tree and len(hierarchical_tree) == 1:
            root_node = hierarchical_tree[0]
            root_metadata = {
                "id": root_node.get('id'),
                "name": root_node.get('name'),
                "note": root_node.get('note'),
                "parent_id": root_node.get('parent_id')
            }
            children = root_node.get('children', [])
            root_children_status = root_node.get("children_status", "complete")
        else:
            target_root = next((r for r in hierarchical_tree if r.get("id") == node_id), None)
            if target_root:
                root_metadata = {
                    "id": target_root.get('id'),
                    "name": target_root.get('name'),
                    "note": target_root.get('note'),
                    "parent_id": target_root.get('parent_id')
                }
                children = target_root.get('children', [])
                root_children_status = target_root.get("children_status", "complete")
            else:
                children = hierarchical_tree
                root_children_status = "unknown"
        
        # Apply depth limit
        if depth is not None:
            children = self._limit_depth(children, depth)

            # After limiting depth, mark any nodes that were truncated by depth.
            # Rule: if a node has children in the ORIGINAL tree but now has children=[], it was truncated.
            def _mark_truncated_by_depth(nodes: list[dict[str, Any]], current_depth: int = 1) -> None:
                if not nodes:
                    return
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    # At the cutoff depth, we forced children=[].
                    if current_depth >= depth:
                        # If this node originally had children, it is truncated.
                        # We cannot reliably know "original had children" after _limit_depth,
                        # so we conservatively mark branches at cutoff depth as truncated when
                        # they are not leaves in the flat export.
                        # Easiest reliable signal: use child_count computed from flat_nodes.
                        pass
                    for ch in (n.get("children") or []):
                        _mark_truncated_by_depth([ch], current_depth + 1)

            # Conservative but safe: any node at depth==depth that is NOT a leaf in the flat export
            # should be marked truncated_by_depth. We can detect that via a set of ids that had
            # children in the original hierarchy.
            ids_with_children: set[str] = set()
            try:
                # Build from hierarchical_tree (pre-limit) which still exists in local scope above.
                def _collect_ids_with_children(nodes0: list[dict[str, Any]]) -> None:
                    for nn in nodes0 or []:
                        if not isinstance(nn, dict):
                            continue
                        cid = nn.get("id")
                        ch0 = nn.get("children") or []
                        if cid and isinstance(ch0, list) and len(ch0) > 0:
                            ids_with_children.add(str(cid))
                        _collect_ids_with_children(ch0)

                _collect_ids_with_children(hierarchical_tree)
            except Exception:
                ids_with_children = set()

            def _stamp_depth_status(nodes1: list[dict[str, Any]], current_depth: int = 1) -> None:
                for nn in nodes1 or []:
                    if not isinstance(nn, dict):
                        continue
                    nid = nn.get("id")
                    if current_depth >= depth:
                        if nid and str(nid) in ids_with_children:
                            nn["children_status"] = "truncated_by_depth"
                    _stamp_depth_status(nn.get("children") or [], current_depth + 1)

            _stamp_depth_status(children, current_depth=1)
        
        max_depth = self._calculate_max_depth(children)
        
        result = {
            "success": True,
            "_source": "api",
            "node_count": len(flat_nodes),
            "depth": max_depth,
            "preview_tree": preview_tree,
            "root": root_metadata,
            "children": children,
            "export_root_children_status": root_children_status,
        }
        
        _log_glimpse_to_file("glimpse_full", node_id, result)
        
        return result

    async def _write_glimpse_as_terrain(
        self,
        node_id: str,
        ws_glimpse: dict[str, Any],
        output_file: str,
    ) -> dict[str, Any]:
        """Write WebSocket GLIMPSE as TERRAIN export file."""
        logger = _ClientLogger()
        
        # Fetch API for complete structure
        logger.info("📡 API fetch for complete structure...")
        try:
            api_glimpse = await self.workflowy_scry(
                node_id=node_id,
                use_efficient_traversal=False,
                depth=None,
                size_limit=50000
            )
        except Exception as e:
            logger.warning(f"⚠️ API fetch failed: {e}")
            api_glimpse = None
        
        # Merge via helpers
        logger.info("🔀 Merging WebSocket + API...")
        
        children, root_children_status, api_merge_performed = merge_glimpses_for_terrain(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )
        
        original_ids_seen = build_original_ids_seen_from_glimpses(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )
        
        terrain_data = build_terrain_export_from_glimpse(
            workflowy_root_id=node_id,
            ws_glimpse=ws_glimpse,
            children=children,
            root_children_status=root_children_status,
            original_ids_seen=original_ids_seen,
        )
        
        # Write files
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(terrain_data, f, ensure_ascii=False, indent=2)
        
        markdown_file = output_file.replace('.json', '.md')
        ws_root = ws_glimpse.get("root") or {}
        root_node_info = {
            'id': node_id,
            'name': ws_root.get('name', 'Root'),
            'parent_id': ws_root.get('parent_id'),
            'full_path_uuids': [node_id],
            'full_path_names': [ws_root.get('name', 'Root')]
        }
        
        markdown_content = self._generate_markdown(children, level=1, root_node_info=root_node_info)
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
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

    async def beacon_get_code_snippet(
        self,
        beacon_node_id: str,
        context: int = 10,
    ) -> dict[str, Any]:
        """Resolve a beacon UUID to (file_path, beacon_id, kind) and snippet.

        This uses the cached /nodes-export snapshot to locate:
        - the beacon node (note contains BEACON (AST|SPAN) with id: ... and kind: ...), and
        - the ancestor FILE node whose note starts with "Path: ...".

        Once (file_path, beacon_id) are known, it delegates to the
        beacon_obtain_code_snippet.get_snippet_data(...) helper to
        compute (start_line, end_line, lines) and returns a raw snippet
        (no prepended line numbers) plus line bounds.

        Returns a dict:
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

        If any step fails (missing beacon, missing Path:, import error,
        snippet failure), a NetworkError is raised with a descriptive
        message for the MCP tool layer to surface.
        """
        logger = _ClientLogger()

        # Step 1: Load /nodes-export snapshot (cached if available)
        raw = await export_nodes_impl(self, node_id=None, use_cache=True, force_refresh=False)
        all_nodes = raw.get("nodes", []) or []
        nodes_by_id: dict[str, dict[str, Any]] = {
            str(n.get("id")): n for n in all_nodes if n.get("id")
        }

        # If beacon not found, try one forced refresh before failing
        if beacon_node_id not in nodes_by_id:
            logger.info(
                f"beacon_get_code_snippet: {beacon_node_id} not in cache; refreshing /nodes-export"
            )
            raw = await export_nodes_impl(self, node_id=None, use_cache=False, force_refresh=True)
            all_nodes = raw.get("nodes", []) or []
            nodes_by_id = {str(n.get("id")): n for n in all_nodes if n.get("id")}

        beacon_node = nodes_by_id.get(beacon_node_id)
        if not beacon_node:
            raise NetworkError(
                f"Beacon node {beacon_node_id!r} not found in /nodes-export snapshot"
            )

        # Step 2: Parse beacon note for id and kind
        note = beacon_node.get("note") or beacon_node.get("no") or ""
        beacon_id: str | None = None
        kind: str | None = None
        if isinstance(note, str):
            for line in note.splitlines():
                stripped = line.strip()
                if stripped.startswith("id:"):
                    beacon_id = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("kind:"):
                    kind = stripped.split(":", 1)[1].strip()

        if not beacon_id:
            raise NetworkError(
                f"Beacon node {beacon_node_id!r} has no 'id:' line in note; "
                "cannot determine beacon id"
            )

        # Step 3: Walk ancestors to find FILE node with Path: ... in its note
        current_id = beacon_node_id
        visited: set[str] = set()
        file_node: dict[str, Any] | None = None

        while current_id and current_id not in visited:
            visited.add(current_id)
            node = nodes_by_id.get(current_id)
            if not node:
                break

            n_note = node.get("note") or node.get("no") or ""
            if isinstance(n_note, str) and "Path:" in n_note:
                # Heuristic: FILE nodes created by Cartographer store the
                # local path on the first line as "Path: E:\...".
                file_node = node
                break

            parent_id = node.get("parent_id") or node.get("parentId")
            current_id = str(parent_id) if parent_id else None

        if not file_node:
            raise NetworkError(
                f"Could not find ancestor FILE node with 'Path:' note for beacon {beacon_node_id!r}"
            )

        file_note = file_node.get("note") or file_node.get("no") or ""
        file_path: str | None = None
        if isinstance(file_note, str):
            for line in file_note.splitlines():
                stripped = line.strip()
                if stripped.startswith("Path:"):
                    file_path = stripped[len("Path:") :].strip()
                    break

        if not file_path:
            raise NetworkError(
                f"Ancestor FILE node for {beacon_node_id!r} is missing a 'Path:' line"
            )

        # Step 4: Delegate to beacon_obtain_code_snippet.get_snippet_data
        try:
            import beacon_obtain_code_snippet as bos  # type: ignore[import]
        except Exception as e:  # noqa: BLE001
            raise NetworkError(
                "Could not import beacon_obtain_code_snippet module; "
                "ensure it is on PYTHONPATH for the MCP server. "
                f"Underlying error: {e}"
            ) from e

        try:
            start, end, lines = bos.get_snippet_data(file_path, beacon_id, context)  # type: ignore[attr-defined]
        except Exception as e:  # noqa: BLE001
            raise NetworkError(
                f"Failed to obtain snippet for beacon id {beacon_id!r} in file {file_path!r}: {e}"
            ) from e

        snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""

        return {
            "success": True,
            "file_path": file_path,
            "beacon_node_id": beacon_node_id,
            "beacon_id": beacon_id,
            "kind": kind,
            "start_line": start,
            "end_line": end,
            "snippet": snippet_text,
        }

    async def nexus_scry(
        self,
        nexus_tag: str,
        workflowy_root_id: str,
        max_depth: int,
        child_limit: int,
        reset_if_exists: bool = False,
        max_nodes: int | None = None,
    ) -> dict[str, Any]:
        """Tag-scoped SCRY → coarse_terrain.json."""
        import shutil

        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

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

        result = await bulk_export_to_file_impl(
            self,
            node_id=workflowy_root_id,
            output_file=str(coarse_path),
            include_metadata=True,
            use_efficient_traversal=False,
            max_depth=max_depth,
            child_count_limit=child_limit,
            max_nodes=max_nodes,
        )

        # Manifest
        manifest_path = run_dir / "nexus_manifest.json"
        try:
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
            pass

        return {
            "success": True,
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
        """IGNITE SHARDS → phantom_gem.json."""
        logger = _ClientLogger()

        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_path = os.path.join(run_dir, "phantom_gem.json")

        if not os.path.exists(coarse_path):
            raise NetworkError("coarse_terrain.json not found; run nexus_scry first")

        if not root_ids:
            empty = {"nexus_tag": nexus_tag, "roots": [], "nodes": []}
            with open(phantom_path, "w", encoding="utf-8") as f:
                json.dump(empty, f, indent=2, ensure_ascii=False)
            return {
                "success": True,
                "nexus_tag": nexus_tag,
                "phantom_gem": phantom_path,
                "roots": [],
                "node_count": 0,
            }

        # Read coarse terrain
        with open(coarse_path, "r", encoding="utf-8") as f:
            coarse_data = json.load(f)

        terrain_nodes = coarse_data.get("nodes", [])
        export_root_id = coarse_data.get("export_root_id")

        # Build ledger
        original_ids_seen: set[str] = set()
        if isinstance(coarse_data.get("original_ids_seen"), list):
            original_ids_seen.update(str(nid) for nid in coarse_data.get("original_ids_seen", []) if nid)
        else:
            def _collect(nodes: list[dict[str, Any]]) -> None:
                for node in nodes or []:
                    if isinstance(node, dict):
                        nid = node.get("id")
                        if nid:
                            original_ids_seen.add(str(nid))
                        _collect(node.get("children") or [])
            _collect(terrain_nodes)
            if export_root_id:
                original_ids_seen.add(str(export_root_id))

        # Build indexes
        parent_by_id: dict[str, str | None] = {}
        node_by_id: dict[str, dict[str, Any]] = {}

        def _index(nodes: list[dict[str, Any]], parent_id: str | None) -> None:
            for node in nodes:
                nid = node.get("id")
                if nid:
                    parent_by_id[nid] = parent_id
                    node_by_id[nid] = node
                    children = node.get("children") or []
                    if children:
                        _index(children, nid)

        _index(terrain_nodes, export_root_id)

        # Normalize roots
        unique_root_ids = []
        for rid in root_ids:
            if rid not in unique_root_ids:
                unique_root_ids.append(rid)
        roots_set = set(unique_root_ids)

        # Check existence
        missing = [rid for rid in roots_set if rid not in parent_by_id]
        if missing:
            raise NetworkError(f"Roots not in coarse_terrain: {missing}")

        # Enforce disjointness
        for rid in roots_set:
            parent = parent_by_id.get(rid)
            while parent is not None:
                if parent in roots_set:
                    raise NetworkError(
                        f"Roots not disjoint: '{rid}' descends from '{parent}'"
                    )
                parent = parent_by_id.get(parent)

        # Deep SCRY for each root
        deep_subtrees: dict[str, dict[str, Any]] = {}
        roots_resolved = []
        total_nodes = 0
        per_root_limits = per_root_limits or {}

        for root_id in unique_root_ids:
            limits = per_root_limits.get(root_id, {})
            root_max_depth = limits.get("max_depth", max_depth)
            root_child_limit = limits.get("child_limit", child_limit)

            try:
                raw = await export_nodes_impl(self, node_id=root_id)
            except Exception as e:
                logger.error(f"Export failed for {root_id}: {e}")
                continue

            flat = raw.get("nodes", [])
            if not flat:
                continue

            total_nodes += raw.get("_total_fetched_from_api", len(flat))

            tree = self._build_hierarchy(flat, True)
            if not tree:
                continue

            root_subtree = next((c for c in tree if c.get("id") == root_id), tree[0])

            self._annotate_child_counts_and_truncate(
                [root_subtree],
                max_depth=root_max_depth,
                child_count_limit=root_child_limit,
                current_depth=1,
            )

            deep_subtrees[root_id] = root_subtree
            roots_resolved.append(root_id)

            # Extend ledger
            def _collect_subtree(node: dict[str, Any]) -> None:
                if isinstance(node, dict):
                    nid = node.get("id")
                    if nid:
                        original_ids_seen.add(str(nid))
                    for child in node.get("children") or []:
                        _collect_subtree(child)
            _collect_subtree(root_subtree)

        # Build GEM skeleton
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
            skeleton_by_id: dict[str, dict[str, Any]] = {}
            top_level_ids: list[str] = []
            top_level_nodes: list[dict[str, Any]] = []

            def ensure_skeleton(nid: str) -> dict[str, Any]:
                source = node_by_id.get(nid)
                if not source:
                    raise NetworkError(f"Node {nid} not in coarse_terrain")
                if nid in skeleton_by_id:
                    return skeleton_by_id[nid]
                skel = {k: v for k, v in source.items() if k != "children"}
                skel["children"] = []
                skeleton_by_id[nid] = skel
                return skel

            for root_id in roots_resolved:
                path: list[str] = []
                cur = root_id
                while cur is not None and cur != export_root_id:
                    path.append(cur)
                    cur = parent_by_id.get(cur)
                if cur != export_root_id:
                    raise NetworkError(f"Root {root_id} doesn't descend from export_root")
                path.reverse()

                parent_id_iter = export_root_id
                for nid in path:
                    node_skel = ensure_skeleton(nid)
                    if parent_id_iter == export_root_id:
                        if nid not in top_level_ids:
                            top_level_ids.append(nid)
                            top_level_nodes.append(node_skel)
                    else:
                        parent_skel = skeleton_by_id[parent_id_iter]
                        children_list = parent_skel.get("children") or []
                        if not any(isinstance(c, dict) and c.get("id") == nid for c in children_list):
                            children_list.append(node_skel)
                            parent_skel["children"] = children_list
                    parent_id_iter = nid

            # Replace skeletons with deep subtrees
            for root_id, subtree in deep_subtrees.items():
                parent_id = parent_by_id.get(root_id)
                if parent_id is None or parent_id == export_root_id:
                    for idx, nid in enumerate(top_level_ids):
                        if nid == root_id:
                            top_level_nodes[idx] = subtree
                            break
                else:
                    parent_skel = skeleton_by_id.get(parent_id)
                    if parent_skel:
                        children_list = parent_skel.get("children") or []
                        for idx, child in enumerate(children_list):
                            if isinstance(child, dict) and child.get("id") == root_id:
                                children_list[idx] = subtree
                                break
                skeleton_by_id[root_id] = subtree

            # Compute preview
            phantom_preview = None
            try:
                phantom_preview = self._annotate_preview_ids_and_build_tree(top_level_nodes, "PG")
            except Exception:
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

        # Update coarse terrain ledger
        try:
            coarse_data["original_ids_seen"] = sorted(original_ids_seen)
            with open(coarse_path, "w", encoding="utf-8") as f:
                json.dump(coarse_data, f, indent=2, ensure_ascii=False)
        except Exception:
            logger.warning("Failed to update coarse_terrain ledger")

        _log_to_file_helper(
            f"nexus_ignite_shards[{nexus_tag}]: roots={roots_resolved}, node_count={total_nodes}",
            "nexus",
        )

        with open(phantom_path, "w", encoding="utf-8") as f:
            json.dump(phantom_payload, f, indent=2, ensure_ascii=False)

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "phantom_gem": phantom_path,
            "roots": roots_resolved,
            "node_count": total_nodes,
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
        """GLIMPSE → TERRAIN + PHANTOM GEM (zero API calls)."""
        import shutil

        if mode not in ("full", "coarse_terrain_only"):
            raise NetworkError(f"Invalid mode '{mode}'")

        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        # Determine run_dir
        existing_dir = None
        if not reset_if_exists:
            try:
                existing = self._get_nexus_dir(nexus_tag)
                existing_dir = Path(existing)
            except NetworkError:
                pass

        if reset_if_exists:
            suffix = f"__{nexus_tag}"
            for child in base_dir.iterdir():
                if not child.is_dir():
                    continue
                if child.name == nexus_tag or child.name.endswith(suffix):
                    shutil.rmtree(child, ignore_errors=True)
            existing_dir = None

        if existing_dir:
            run_dir = existing_dir
        else:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            run_dir = base_dir / f"{timestamp}__{nexus_tag}"
            run_dir.mkdir(parents=True, exist_ok=False)

        coarse_path = run_dir / "coarse_terrain.json"
        phantom_gem_path = run_dir / "phantom_gem.json"
        shimmering_path = run_dir / "shimmering_terrain.json"

        logger = _ClientLogger()
        
        # WebSocket GLIMPSE
        logger.info(f"🔌 NEXUS GLIMPSE: WebSocket for {workflowy_root_id[:8]}...")
        ws_glimpse = await self.workflowy_glimpse(
            workflowy_root_id, _ws_connection=_ws_connection, _ws_queue=_ws_queue
        )

        if not ws_glimpse.get("success"):
            raise NetworkError(f"GLIMPSE failed: {ws_glimpse.get('error')}")
        
        # API GLIMPSE for complete structure
        logger.info("📡 API fetch...")
        try:
            api_glimpse = await self.workflowy_scry(
                node_id=workflowy_root_id,
                use_efficient_traversal=False,
                depth=None,
                size_limit=50000
            )
        except Exception as e:
            logger.warning(f"⚠️ API failed: {e}")
            children = ws_glimpse.get("children", [])
            mark_all_nodes_as_potentially_truncated(children)
            api_glimpse = None
        
        # Merge
        logger.info("🔀 Merging...")
        children, root_children_status, api_merge = merge_glimpses_for_terrain(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )

        original_ids = build_original_ids_seen_from_glimpses(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            api_glimpse=api_glimpse,
        )

        terrain_data = build_terrain_export_from_glimpse(
            workflowy_root_id=workflowy_root_id,
            ws_glimpse=ws_glimpse,
            children=children,
            root_children_status=root_children_status,
            original_ids_seen=original_ids,
        )

        # Attach preview
        terrain_preview = None
        try:
            terrain_preview = self._annotate_preview_ids_and_build_tree(
                terrain_data.get("nodes") or [], "CT"
            )
        except Exception:
            pass
        
        if terrain_preview:
            terrain_data = {
                "export_timestamp": terrain_data.get("export_timestamp"),
                "export_root_children_status": terrain_data.get("export_root_children_status"),
                "__preview_tree__": terrain_preview,
                "export_root_id": terrain_data.get("export_root_id"),
                "export_root_name": terrain_data.get("export_root_name"),
                "nodes": terrain_data.get("nodes"),
                "original_ids_seen": terrain_data.get("original_ids_seen"),
            }

        # Write coarse_terrain
        with open(coarse_path, "w", encoding="utf-8") as f:
            json.dump(terrain_data, f, ensure_ascii=False, indent=2)

        result = {
            "success": True,
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "node_count": ws_glimpse.get("node_count", 0),
            "depth": ws_glimpse.get("depth", 0),
            "_source": "glimpse_merged",
            "_api_merge_performed": api_merge,
            "mode": mode,
        }

        if mode == "full":
            # Write phantom_gem and shimmering
            with open(phantom_gem_path, "w", encoding="utf-8") as f:
                json.dump(terrain_data, f, ensure_ascii=False, indent=2)

            with open(shimmering_path, "w", encoding="utf-8") as f:
                json.dump(terrain_data, f, ensure_ascii=False, indent=2)

            result["phantom_gem"] = str(phantom_gem_path)
            result["shimmering_terrain"] = str(shimmering_path)

        _log_glimpse_to_file("glimpse", workflowy_root_id, ws_glimpse)

        return result

    async def nexus_anchor_gems(self, nexus_tag: str) -> dict[str, Any]:
        """ANCHOR GEMS → shimmering_terrain.json."""
        run_dir = self._get_nexus_dir(nexus_tag)
        coarse_path = os.path.join(run_dir, "coarse_terrain.json")
        phantom_path = os.path.join(run_dir, "phantom_gem.json")
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")

        if not os.path.exists(coarse_path):
            raise NetworkError("coarse_terrain not found")
        if not os.path.exists(phantom_path):
            raise NetworkError("phantom_gem not found")

        with open(coarse_path, "r", encoding="utf-8") as f:
            coarse_data = json.load(f)
        with open(phantom_path, "r", encoding="utf-8") as f:
            phantom_data = json.load(f)

        phantom_roots = phantom_data.get("roots", [])
        phantom_nodes = phantom_data.get("nodes", [])

        if not phantom_roots or not phantom_nodes:
            # Nothing to anchor
            with open(shimmering_path, "w", encoding="utf-8") as f:
                json.dump(coarse_data, f, indent=2, ensure_ascii=False)
            return {
                "success": True,
                "nexus_tag": nexus_tag,
                "shimmering_terrain": shimmering_path,
                "roots": [],
            }

        # Build phantom subtree index
        subtree_by_id: dict[str, dict[str, Any]] = {}

        def _index_phantom(nodes: list[dict[str, Any]]) -> None:
            for node in nodes or []:
                if isinstance(node, dict):
                    nid = node.get("id")
                    if nid:
                        subtree_by_id[nid] = node
                    children = node.get("children") or []
                    if children:
                        _index_phantom(children)

        _index_phantom(phantom_nodes)

        terrain_nodes = coarse_data.get("nodes", [])

        def replace_subtree(nodes: list[dict[str, Any]]) -> None:
            for idx, node in enumerate(nodes):
                nid = node.get("id")
                if nid in subtree_by_id:
                    nodes[idx] = subtree_by_id[nid]
                else:
                    children = node.get("children") or []
                    if children:
                        replace_subtree(children)

        replace_subtree(terrain_nodes)

        # Merge ledgers
        original_ids: set[str] = set(coarse_data.get("original_ids_seen", []) or [])
        original_ids.update(phantom_data.get("original_ids_seen", []) or [])
        if original_ids:
            coarse_data["original_ids_seen"] = sorted(original_ids)

        explicitly_preserved: set[str] = set(coarse_data.get("explicitly_preserved_ids", []) or [])
        explicitly_preserved.update(phantom_data.get("explicitly_preserved_ids", []) or [])
        if explicitly_preserved:
            coarse_data["explicitly_preserved_ids"] = sorted(explicitly_preserved)

        coarse_data["nodes"] = terrain_nodes

        # Attach shimmering preview
        shimmering_preview = None
        try:
            shimmering_preview = self._annotate_preview_ids_and_build_tree(
                coarse_data.get("nodes") or [], "ST"
            )
        except Exception:
            pass
        
        if shimmering_preview:
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

        with open(shimmering_path, "w", encoding="utf-8") as f:
            json.dump(coarse_data, f, indent=2, ensure_ascii=False)

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "shimmering_terrain": shimmering_path,
            "roots": phantom_roots,
        }

    def nexus_transform_jewel(
        self,
        jewel_file: str,
        operations: list[dict[str, Any]],
        dry_run: bool = False,
        stop_on_error: bool = True,
    ) -> dict[str, Any]:
        """Apply JEWELSTORM semantic operations to working_gem."""
        import importlib

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

        try:
            return nexus_tools.transform_jewel(
                jewel_file=jewel_file,
                operations=operations,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
            )
        except Exception as e:
            raise NetworkError(f"transform_jewel failed: {e}") from e

    async def nexus_anchor_jewels(self, nexus_tag: str) -> dict[str, Any]:
        """ANCHOR JEWELS → enchanted_terrain.json."""
        import shutil
        import importlib

        run_dir = self._get_nexus_dir(nexus_tag)
        shimmering_path = os.path.join(run_dir, "shimmering_terrain.json")
        phantom_gem_path = os.path.join(run_dir, "phantom_gem.json")
        phantom_jewel_path = os.path.join(run_dir, "phantom_jewel.json")
        enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")

        if not os.path.exists(shimmering_path):
            raise NetworkError("shimmering_terrain not found")
        if not os.path.exists(phantom_gem_path):
            raise NetworkError("phantom_gem not found")
        if not os.path.exists(phantom_jewel_path):
            raise NetworkError("phantom_jewel not found - run QUILLSTORM first")

        # Copy shimmering to enchanted
        shutil.copy2(shimmering_path, enchanted_path)

        # Import nexus_json_tools
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

        # Call fuse-shard-3way
        try:
            argv = [
                enchanted_path,
                "fuse-shard-3way",
                "--witness-shard", phantom_gem_path,
                "--morphed-shard", phantom_jewel_path,
                "--target-scry", enchanted_path,
            ]
            try:
                nexus_tools.main(argv)
            except SystemExit as se:
                if (se.code or 0) != 0:
                    raise NetworkError(f"fuse-shard-3way exited {se.code}") from se
        except Exception as e:
            raise NetworkError(f"fuse-shard-3way failed: {e}") from e

        # Attach enchanted preview
        try:
            with open(enchanted_path, "r", encoding="utf-8") as f:
                enchanted_data = json.load(f)
            if isinstance(enchanted_data, dict) and isinstance(enchanted_data.get("nodes"), list):
                enchanted_preview = self._annotate_preview_ids_and_build_tree(
                    enchanted_data.get("nodes") or [], "ET"
                )
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
            pass

        return {
            "success": True,
            "nexus_tag": nexus_tag,
            "shimmering_terrain": shimmering_path,
            "enchanted_terrain": enchanted_path,
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
        """Launch WEAVE as detached background process."""
        import subprocess
        
        # Validate
        if mode == 'enchanted':
            if not nexus_tag:
                raise ValueError("nexus_tag required")
            run_dir = self._get_nexus_dir(nexus_tag)
            enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")
            if not os.path.exists(enchanted_path):
                raise NetworkError("enchanted_terrain not found")
            job_id = f"weave-enchanted-{nexus_tag}"
        else:
            if not json_file:
                raise ValueError("json_file required")
            if not os.path.exists(json_file):
                raise NetworkError(f"JSON not found: {json_file}")
            job_id = f"weave-direct-{Path(json_file).stem}"
        
        # Worker script
        worker_script = os.path.join(os.path.dirname(__file__), "..", "weave_worker.py")
        worker_script = os.path.abspath(worker_script)
        
        if not os.path.exists(worker_script):
            raise NetworkError(f"weave_worker.py not found: {worker_script}")
        
        # Build command
        cmd = [sys.executable, worker_script, '--mode', mode, '--dry-run', str(dry_run).lower()]
        
        if mode == 'enchanted':
            cmd.extend(['--nexus-tag', nexus_tag])
        else:
            cmd.extend(['--json-file', json_file])
            if parent_id:
                cmd.extend(['--parent-id', parent_id])
            cmd.extend(['--import-policy', import_policy])
        
        # Log file
        if mode == 'enchanted':
            run_dir = self._get_nexus_dir(nexus_tag)
            log_file = os.path.join(run_dir, ".weave.log")
        else:
            log_file = str(Path(json_file).parent / ".weave.log")
        
        # Environment
        env = os.environ.copy()
        env['WORKFLOWY_API_KEY'] = self.config.api_key.get_secret_value()
        env['NEXUS_RUNS_BASE'] = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        
        log_handle = open(log_file, 'w', encoding='utf-8')
        
        log_event(f"Launching detached WEAVE ({mode}), log: {log_file}", "DETACHED")
        
        try:
            process = subprocess.Popen(
                cmd,
                env=env,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0,
                start_new_session=True if sys.platform != 'win32' else False,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT
            )
            
            pid = process.pid
            log_event(f"Detached worker: PID={pid}, mode={mode}", "DETACHED")
            
            return {
                "success": True,
                "job_id": job_id,
                "pid": pid,
                "detached": True,
                "mode": mode,
                "log_file": log_file,
                "note": f"Worker detached - survives MCP restart. Log: {log_file}"
            }
            
        except Exception as e:
            raise NetworkError(f"Failed to launch detached WEAVE: {e}") from e
    
    def nexus_weave_enchanted_detached(self, nexus_tag: str, dry_run: bool = False) -> dict[str, Any]:
        """Launch ENCHANTED WEAVE as detached process."""
        return self._launch_detached_weave(mode='enchanted', nexus_tag=nexus_tag, dry_run=dry_run)
    
    def nexus_weave_detached(
        self, json_file: str, parent_id: str | None = None,
        dry_run: bool = False, import_policy: str = 'strict'
    ) -> dict[str, Any]:
        """Launch DIRECT WEAVE as detached process."""
        return self._launch_detached_weave(
            mode='direct', json_file=json_file, parent_id=parent_id,
            dry_run=dry_run, import_policy=import_policy
        )

    async def nexus_weave_enchanted(self, nexus_tag: str, dry_run: bool = False) -> dict[str, Any]:
        """WEAVE ENCHANTED TERRAIN → ETHER."""
        run_dir = self._get_nexus_dir(nexus_tag)
        enchanted_path = os.path.join(run_dir, "enchanted_terrain.json")
        
        if not os.path.exists(enchanted_path):
            raise NetworkError("enchanted_terrain not found")
        
        # Import bulk_import here to avoid circular dependency
        from .api_client_etch import WorkFlowyClientEtch
        
        # Call bulk_import via self (we inherit from WorkFlowyClientEtch)
        return await self.bulk_import_from_file(
            json_file=enchanted_path,
            parent_id=None,
            dry_run=dry_run,
            import_policy='strict',
        )

    async def bulk_import_from_file(
        self,
        json_file: str,
        parent_id: str | None = None,
        dry_run: bool = False,
        import_policy: str = 'strict',
        auto_upgrade_to_jewel: bool = True,
    ) -> dict[str, Any]:
        """Create nodes from JSON using reconciliation algorithm."""
        import asyncio
        from ..models import NodeListRequest, NodeCreateRequest, NodeUpdateRequest

        logger = _ClientLogger()

        # Set WEAVE context
        _current_weave_context["json_file"] = json_file
        
        # Read JSON
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                payload['jewel_file'] = json_file
        except Exception as e:
            return {
                "success": False,
                "errors": [f"Failed to read JSON: {e}"]
            }
        
        # Validate format
        if not (isinstance(payload, dict) and 'nodes' in payload):
            raise NetworkError("NEXUS JSON must have 'export_root_id' and 'nodes'")
        
        export_root_id = payload.get('export_root_id')
        nodes_to_create = payload.get('nodes')
        
        if not export_root_id or not isinstance(nodes_to_create, list):
            raise NetworkError("SCRY header malformed")
        
        # Weave journal
        weave_journal_path = None
        weave_journal = None
        journal_warning = None
        log_weave_entry_fn = None
        
        if not dry_run:
            weave_journal_path = json_file.replace('.json', '.weave_journal.json')
            try:
                if os.path.exists(weave_journal_path):
                    with open(weave_journal_path, 'r', encoding='utf-8') as jf:
                        prev = json.load(jf)
                    if not prev.get('last_run_completed', True):
                        journal_warning = f"Previous weave incomplete at {prev.get('last_run_started_at')}"
                        log_event(journal_warning, "WEAVE")
            except Exception:
                pass

            weave_journal = {
                "json_file": json_file,
                "last_run_started_at": datetime.now().isoformat(),
                "last_run_completed": False,
                "phase": "stub_created",
                "entries": [],
            }
            try:
                with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                    json.dump(weave_journal, jf, indent=2)
            except Exception:
                weave_journal = None
                weave_journal_path = None

            def log_weave_entry_fn(entry: dict[str, Any]) -> None:
                if weave_journal is None or weave_journal_path is None:
                    return
                try:
                    e = dict(entry)
                    e.setdefault("timestamp", datetime.now().isoformat())
                    weave_journal.setdefault("entries", []).append(e)
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf2:
                        json.dump(weave_journal, jf2, indent=2)
                except Exception:
                    pass
        
        # Parent resolution
        target_backup_file = None
        if parent_id is None:
            parent_id = export_root_id
            log_event(f"Using export_root_id as parent: {parent_id}", "WEAVE")
        else:
            if export_root_id and parent_id != export_root_id:
                # Auto-backup target
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                target_backup_file = json_file.replace('.json', f'.target_backup_{timestamp}.json')
                log_event(f"Parent override - backing up target: {target_backup_file}", "WEAVE")
                try:
                    await bulk_export_to_file_impl(self, parent_id, target_backup_file, True, False, None, None, None)
                    log_event("Target backup complete", "WEAVE")
                except Exception as e:
                    log_event(f"Backup failed: {e}", "WEAVE")
        
        # Validate & escape
        def validate_recursive(nodes_list: list[dict[str, Any]], path: str = "root") -> tuple[bool, str | None, list[str]]:
            warnings = []
            for idx, node in enumerate(nodes_list):
                node_path = f"{path}[{idx}].{node.get('name', 'unnamed')}"
                
                # Name
                name = node.get('name')
                if not isinstance(name, str) or not name.strip():
                    return (False, f"{node_path}: Name required", warnings)
                
                processed_name, name_warning = self._validate_name_field(name)
                if processed_name:
                    node['name'] = processed_name
                if name_warning:
                    warnings.append(f"{node_path} - Name")
                
                # Note
                note = node.get('note')
                if note:
                    processed_note, note_warning = self._validate_note_field(note)
                    if processed_note is None and note_warning:
                        return (False, f"{node_path}: {note_warning}", warnings)
                    node['note'] = processed_note
                    if note_warning and "AUTO-ESCAPED" in note_warning:
                        warnings.append(f"{node_path} - Note")
                
                # Recurse
                children = node.get('children', [])
                if children:
                    success, error, child_warnings = validate_recursive(children, node_path)
                    if not success:
                        return (False, error, warnings)
                    warnings.extend(child_warnings)
            
            return (True, None, warnings)
        
        success, error_msg, warnings = validate_recursive(nodes_to_create)
        
        if not success:
            if weave_journal and weave_journal_path:
                try:
                    weave_journal["last_run_completed"] = False
                    weave_journal["last_run_error"] = error_msg
                    weave_journal["phase"] = "error_validation"
                    with open(weave_journal_path, "w", encoding="utf-8") as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception:
                    pass
            log_event(f"Validation failed: {error_msg}", "WEAVE")
            _current_weave_context["json_file"] = None
            return {
                "success": False,
                "errors": [error_msg or "Validation failed"]
            }
        
        if warnings:
            log_event(f"✅ Auto-escaped {len(warnings)} fields", "WEAVE")
        
        # Reconciliation
        from .workflowy_move_reconcile import reconcile_tree
        
        stats = {
            "api_calls": 0,
            "nodes_created": 0,
            "nodes_updated": 0,
            "nodes_deleted": 0,
            "nodes_moved": 0,
            "errors": []
        }
        
        # Wrappers
        async def list_wrapper(parent_uuid: str) -> list[dict]:
            request = NodeListRequest(parentId=parent_uuid)
            nodes, _ = await self.list_nodes(request)
            stats["api_calls"] += 1
            return [n.model_dump() for n in nodes]
        
        async def create_wrapper(parent_uuid: str, data: dict) -> str:
            request = NodeCreateRequest(
                name=data.get('name'),
                parent_id=parent_uuid,
                note=data.get('note'),
                layoutMode=(data.get('data') or {}).get('layoutMode'),
                position='bottom'
            )
            _log_to_file_helper(f"WEAVE CREATE: {data.get('name')}", "reconcile")
            node = await self.create_node(request, _internal_call=True)
            stats["api_calls"] += 1
            stats["nodes_created"] += 1
            return node.id
        
        async def update_wrapper(node_uuid: str, data: dict) -> None:
            request = NodeUpdateRequest(
                name=data.get('name'),
                note=data.get('note'),
                layoutMode=(data.get('data') or {}).get('layoutMode')
            )
            _log_to_file_helper(f"WEAVE UPDATE: {node_uuid}", "reconcile")
            await self.update_node(node_uuid, request)
            stats["api_calls"] += 1
            stats["nodes_updated"] += 1
        
        async def delete_wrapper(node_uuid: str) -> None:
            _log_to_file_helper(f"WEAVE DELETE: {node_uuid}", "reconcile")
            await self.delete_node(node_uuid)
            stats["api_calls"] += 1
            stats["nodes_deleted"] += 1
        
        async def move_wrapper(node_uuid: str, new_parent: str, position: str = "top") -> None:
            _log_to_file_helper(f"WEAVE MOVE: {node_uuid}", "reconcile")
            await self.move_node(node_uuid, new_parent, position)
            stats["api_calls"] += 1
            stats["nodes_moved"] += 1
        
        async def export_wrapper(node_uuid: str) -> dict:
            _log_to_file_helper(f"WEAVE EXPORT: {node_uuid}", "reconcile")
            data = await export_nodes_impl(self, node_uuid)
            stats["api_calls"] += 1
            return data
        
        # Execute reconciliation
        try:
            if weave_journal and weave_journal_path:
                try:
                    weave_journal["phase"] = "before_reconcile"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception:
                    pass
            
            result_plan = await reconcile_tree(
                source_json=payload,
                parent_uuid=parent_id,
                list_nodes=list_wrapper,
                create_node=create_wrapper,
                update_node=update_wrapper,
                delete_node=delete_wrapper,
                move_node=move_wrapper,
                export_nodes=export_wrapper,
                import_policy=import_policy,
                dry_run=dry_run,
                log_weave_entry=log_weave_entry_fn,
                log_to_file_msg=lambda m: _log_to_file_helper(m, "reconcile"),
            )
            
            if dry_run and result_plan:
                return {
                    "success": True,
                    "dry_run": True,
                    "plan": result_plan,
                }
            
            # JEWEL auto-upgrade
            if auto_upgrade_to_jewel and not dry_run and isinstance(result_plan, dict):
                try:
                    creates = result_plan.get("creates") or []
                    jewel_path = payload.get("jewel_file") if isinstance(payload, dict) else None
                    if jewel_path and creates:
                        import importlib
                        client_dir = os.path.dirname(os.path.abspath(__file__))
                        project_root = os.path.dirname(os.path.dirname(os.path.dirname(client_dir)))
                        if project_root not in sys.path:
                            sys.path.insert(0, project_root)
                        nexus_tools = importlib.import_module("nexus_json_tools")

                        ops = []
                        for c in creates:
                            cid = c.get("id")
                            path = c.get("source_path")
                            if cid and path is not None:
                                ops.append({"op": "SET_ATTRS_BY_PATH", "path": path, "attrs": {"id": cid}})
                        if ops:
                            nexus_tools.transform_jewel(jewel_path, ops, False, True)
                            logger.info(f"JEWEL upgrade: {len(ops)} ops")
                except Exception as e:
                    logger.error(f"JEWEL upgrade failed: {e}")

            # Complete journal
            if weave_journal and weave_journal_path:
                try:
                    weave_journal["last_run_completed"] = True
                    weave_journal["phase"] = "completed"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception:
                    pass
            
            _current_weave_context["json_file"] = None
            
            root_ids = [n.get('id') for n in nodes_to_create if n.get('id')]
            
            result = {
                "success": True,
                "nodes_created": stats["nodes_created"],
                "nodes_updated": stats["nodes_updated"],
                "nodes_deleted": stats["nodes_deleted"],
                "nodes_moved": stats["nodes_moved"],
                "root_node_ids": root_ids,
                "api_calls": stats["api_calls"],
            }
            
            if target_backup_file:
                result["target_backup"] = target_backup_file
            
            if weave_journal_path and not dry_run:
                result["weave_journal"] = {
                    "path": weave_journal_path,
                    "previous_incomplete": bool(journal_warning),
                }

            if not dry_run and parent_id:
                try:
                    self._mark_nodes_export_dirty([parent_id])
                except Exception:
                    pass
            
            return result
            
        except Exception as e:
            error_msg = f"Bulk import failed: {e}"
            logger.error(error_msg)

            # Log to reconcile_debug
            try:
                json_file_ctx = _current_weave_context.get("json_file")
                if json_file_ctx and os.path.exists(json_file_ctx):
                    log_path = os.path.join(os.path.dirname(json_file_ctx), "reconcile_debug.log")
                else:
                    log_path = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\reconcile_debug.log"
                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                with open(log_path, "a", encoding="utf-8") as dbg:
                    dbg.write(f"[{ts}] ERROR: {error_msg}\n")
            except Exception:
                pass

            # Mark journal failed
            if weave_journal and weave_journal_path:
                try:
                    weave_journal["last_run_completed"] = False
                    weave_journal["last_run_error"] = error_msg
                    weave_journal["phase"] = "error"
                    with open(weave_journal_path, 'w', encoding='utf-8') as jf:
                        json.dump(weave_journal, jf, indent=2)
                except Exception:
                    pass
            
            _current_weave_context["json_file"] = None
            
            return {
                "success": False,
                "errors": [error_msg]
            }

    def nexus_list_keystones(self) -> dict[str, Any]:
        """List NEXUS Keystone backups."""
        backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
        if not os.path.exists(backup_dir):
            return {"success": True, "keystones": [], "message": "No backup dir"}
        
        keystones = []
        for filename in os.listdir(backup_dir):
            if filename.endswith(".json"):
                parts = filename.replace('.json', '').split('-')
                if len(parts) >= 3:
                    keystones.append({
                        "keystone_id": parts[-1],
                        "timestamp": parts[0],
                        "node_name": "-".join(parts[1:-1]),
                        "filename": filename
                    })
        
        return {
            "success": True,
            "keystones": sorted(keystones, key=lambda k: k['timestamp'], reverse=True)
        }

    async def nexus_restore_keystone(self, keystone_id: str) -> dict[str, Any]:
        """Restore from Keystone backup."""
        backup_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_backups"
        
        target_file = None
        for filename in os.listdir(backup_dir):
            if keystone_id in filename and filename.endswith(".json"):
                target_file = os.path.join(backup_dir, filename)
                break

        if not target_file:
            return {"success": False, "error": f"Keystone '{keystone_id}' not found"}

        return await self.bulk_import_from_file(json_file=target_file)

    def nexus_purge_keystones(self, keystone_ids: list[str]) -> dict[str, Any]:
        """Delete Keystone backups."""
        backup_dir = r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_backups"
        purged = []
        errors = []

        for kid in keystone_ids:
            found = False
            for filename in os.listdir(backup_dir):
                if kid in filename and filename.endswith(".json"):
                    try:
                        os.remove(os.path.join(backup_dir, filename))
                        purged.append(filename)
                        found = True
                        break
                    except Exception as e:
                        errors.append(f"Failed to delete {filename}: {e}")
            if not found:
                errors.append(f"Keystone '{kid}' not found")

        return {
            "success": len(errors) == 0,
            "purged_count": len(purged),
            "purged_files": purged,
            "errors": errors
        }

    async def refresh_file_node_beacons(
        self,
        file_node_id: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Per-file beacon-aware refresh of a Cartographer-mapped FILE node.

        - Uses a SHA1 guard to skip work when the underlying source file has not
          changed.
        - Rebuilds ONLY the structural subtree under the given file node using
          Cartographer in single-file mode.
        - Salvages Notes[...] subtrees whose beacon `id:` is unchanged and
          reattaches them under the new beacon nodes.

        This method intentionally DOES NOT touch siblings of the file node, and
        it preserves top-level "Notes" manual roots under the file.
        """
        logger = _ClientLogger()
        log_event(
            f"refresh_file_node_beacons start (file_node_id={file_node_id}, dry_run={dry_run})",
            "BEACON",
        )

        # 4.1 Resolve file node & source path
        try:
            raw = await export_nodes_impl(self, file_node_id)
        except Exception as e:  # noqa: BLE001
            raise NetworkError(f"export_nodes failed for {file_node_id}: {e}") from e

        flat_nodes = raw.get("nodes") or []
        if not flat_nodes:
            return {
                "success": False,
                "error": f"File node {file_node_id} not found or has empty subtree",
            }

        file_node = None
        for n in flat_nodes:
            if str(n.get("id")) == str(file_node_id):
                file_node = n
                break

        if not file_node:
            return {
                "success": False,
                "error": f"File node {file_node_id} not present in exported subtree",
            }

        note_text = str(file_node.get("note") or "")
        source_path: str | None = None
        existing_sha1: str | None = None
        for line in note_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("Path:"):
                source_path = stripped.split(":", 1)[1].strip() or None
            elif stripped.startswith("Source-SHA1:"):
                existing_sha1 = stripped.split(":", 1)[1].strip() or None

        if not source_path:
            raise NetworkError(
                f"File node {file_node_id} note is missing 'Path:' line; cannot refresh",
            )

        if not os.path.isfile(source_path):
            raise NetworkError(f"Source file not found at Path: {source_path}")

        # 4.2 Hash guard
        def _compute_sha1(path: str) -> str:
            import hashlib

            h = hashlib.sha1()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    h.update(chunk)
            return h.hexdigest()

        file_sha1 = _compute_sha1(source_path)

        if existing_sha1 and existing_sha1 == file_sha1:
            log_event(
                f"refresh_file_node_beacons: unchanged (hash match) for {source_path}",
                "BEACON",
            )
            return {
                "success": True,
                "file_node_id": file_node_id,
                "source_path": source_path,
                "previous_sha1": existing_sha1,
                "file_sha1": file_sha1,
                "changed": False,
                "structural_nodes_deleted": 0,
                "structural_nodes_created": 0,
                "notes_salvaged": 0,
                "notes_orphaned": 0,
            }

        # Build parent->children index for current subtree
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            parent_id = n.get("parent_id") or n.get("parentId")
            if parent_id:
                children_by_parent.setdefault(str(parent_id), []).append(n)

        def _is_notes_name(raw_name: str) -> bool:
            name = (raw_name or "").strip()
            if not name:
                return False
            # Strip a single leading non-alphanumeric symbol (emoji, bullet, etc.)
            if name and not name[0].isalnum():
                name = name[1:].lstrip()
            return name.lower().startswith("notes")

        # 4.3 Salvage Notes under existing beacon nodes
        saved_notes: dict[str, list[str]] = {}
        total_notes_to_salvage = 0

        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            note = n.get("note") or ""
            if "BEACON (" not in str(note):
                continue
            beacon_id_val: str | None = None
            for line in str(note).splitlines():
                stripped = line.strip()
                if stripped.startswith("id:"):
                    beacon_id_val = stripped.split(":", 1)[1].strip()
                    break
            if not beacon_id_val:
                continue

            sid = str(nid)
            children = children_by_parent.get(sid, [])
            for child in children:
                cid = child.get("id")
                if not cid:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname):
                    saved_notes.setdefault(beacon_id_val, []).append(str(cid))
                    total_notes_to_salvage += 1

        # Create or locate 'Notes (parking)' under the file node
        parking_node_id: str | None = None
        parking_created_here = False

        file_children = children_by_parent.get(str(file_node_id), [])
        for child in file_children:
            cid = child.get("id")
            if not cid:
                continue
            cname = str(child.get("name") or "")
            if _is_notes_name(cname) and "parking" in cname.lower():
                parking_node_id = str(cid)
                break

        if total_notes_to_salvage and parking_node_id is None and not dry_run:
            req = NodeCreateRequest(
                name="🅿️ Notes (parking)",
                parent_id=str(file_node_id),
                note=None,
                layoutMode=None,
                position="bottom",
            )
            try:
                created = await self.create_node(req, _internal_call=True)
                parking_node_id = created.id
                parking_created_here = True
            except Exception as e:  # noqa: BLE001
                raise NetworkError(f"Failed to create 'Notes (parking)' node: {e}") from e

        notes_moved_to_parking = 0
        if not dry_run and parking_node_id and saved_notes:
            for notes_list in saved_notes.values():
                for nid in notes_list:
                    try:
                        await self.move_node(nid, parking_node_id, "bottom")
                        notes_moved_to_parking += 1
                    except Exception as e:  # noqa: BLE001
                        log_event(
                            f"Failed to move notes node {nid} to parking: {e}",
                            "BEACON",
                        )

        # 4.4 Delete structural subtree under file node (non-Notes and non-parking)
        structural_deleted = 0
        if file_children:
            for child in file_children:
                cid = child.get("id")
                if not cid:
                    continue
                sid = str(cid)
                if parking_node_id and sid == parking_node_id:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname):
                    # Manual Notes root – preserve
                    continue
                structural_deleted += 1
                if not dry_run:
                    try:
                        await self.delete_node(sid)
                    except Exception as e:  # noqa: BLE001
                        log_event(
                            f"Failed to delete child {sid} under file node {file_node_id}: {e}",
                            "BEACON",
                        )

        # 4.4 (cont.) Rebuild structural subtree from Cartographer single-file map
        # Import nexus_map_codebase dynamically from project root
        try:
            import importlib

            client_dir = os.path.dirname(os.path.abspath(__file__))
            wf_mcp_dir = os.path.dirname(client_dir)
            mcp_servers_dir = os.path.dirname(wf_mcp_dir)
            project_root = os.path.dirname(mcp_servers_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            cartographer = importlib.import_module("nexus_map_codebase")
        except Exception as e:  # noqa: BLE001
            raise NetworkError(f"Failed to import nexus_map_codebase: {e}") from e

        try:
            file_map = cartographer.map_codebase(source_path)  # type: ignore[attr-defined]
        except Exception as e:  # noqa: BLE001
            raise NetworkError(
                f"Cartographer map_codebase failed for {source_path}: {e}",
            ) from e

        new_children = file_map.get("children") or []
        structural_created = 0

        # Collect beacon ids present in new map (used by dry_run and by reattachment)
        def _collect_beacon_ids(nodes: list[dict[str, Any]]) -> set[str]:
            ids: set[str] = set()
            for node in nodes or []:
                note = node.get("note") or ""
                if "BEACON (" in str(note):
                    for line in str(note).splitlines():
                        stripped = line.strip()
                        if stripped.startswith("id:"):
                            val = stripped.split(":", 1)[1].strip()
                            if val:
                                ids.add(val)
                            break
                children = node.get("children") or []
                if children:
                    ids.update(_collect_beacon_ids(children))
            return ids

        new_beacon_ids = _collect_beacon_ids(new_children)

        new_beacon_nodes: dict[str, str] = {}

        if not dry_run:
            async def _create_subtree(parent_uuid: str, node: dict[str, Any]) -> None:
                nonlocal structural_created
                name = str(node.get("name") or "").strip() or "..."
                note = node.get("note")
                data = node.get("data") or {}
                layout_mode = data.get("layoutMode")

                req = NodeCreateRequest(
                    name=name,
                    parent_id=parent_uuid,
                    note=note,
                    layoutMode=layout_mode,
                    position="bottom",
                )
                wf_node = await self.create_node(req, _internal_call=True)
                structural_created += 1

                # Capture beacon id mapping for reattachment
                beacon_id_val: str | None = None
                note_text_new = str(note or "")
                if "BEACON (" in note_text_new:
                    for line in note_text_new.splitlines():
                        stripped = line.strip()
                        if stripped.startswith("id:"):
                            beacon_id_val = stripped.split(":", 1)[1].strip()
                            break
                if beacon_id_val:
                    new_beacon_nodes[beacon_id_val] = wf_node.id

                for child in node.get("children") or []:
                    await _create_subtree(wf_node.id, child)

            for child in new_children:
                await _create_subtree(str(file_node_id), child)
        else:
            # Dry run: only count structural nodes that would be created
            def _count_nodes(nodes: list[dict[str, Any]]) -> int:
                total = 0
                for node in nodes or []:
                    total += 1
                    total += _count_nodes(node.get("children") or [])
                return total

            structural_created = _count_nodes(new_children)

        # 4.5 Reattach Notes based on beacon id
        notes_salvaged = 0
        notes_orphaned = 0
        salvaged_root_id: str | None = None

        if saved_notes:
            if dry_run:
                for beacon_id, ids_list in saved_notes.items():
                    if beacon_id in new_beacon_ids:
                        notes_salvaged += len(ids_list)
                    else:
                        notes_orphaned += len(ids_list)
            else:
                # Non-dry-run: reattach under new beacons or 'Notes (salvaged)'
                if new_beacon_nodes:
                    for beacon_id, ids_list in saved_notes.items():
                        target_parent = new_beacon_nodes.get(beacon_id)
                        if target_parent:
                            for nid in ids_list:
                                try:
                                    await self.move_node(nid, target_parent, "bottom")
                                    notes_salvaged += 1
                                except Exception as e:  # noqa: BLE001
                                    log_event(
                                        f"Failed to reattach notes node {nid} to beacon {beacon_id}: {e}",
                                        "BEACON",
                                    )
                        else:
                            # Orphaned – move under 'Notes (salvaged)'
                            if not ids_list:
                                continue
                            if salvaged_root_id is None:
                                req = NodeCreateRequest(
                                    name="🧩 Notes (salvaged)",
                                    parent_id=str(file_node_id),
                                    note=None,
                                    layoutMode=None,
                                    position="bottom",
                                )
                                try:
                                    created = await self.create_node(
                                        req,
                                        _internal_call=True,
                                    )
                                    salvaged_root_id = created.id
                                except Exception as e:  # noqa: BLE001
                                    raise NetworkError(
                                        f"Failed to create 'Notes (salvaged)' node: {e}",
                                    ) from e
                            for nid in ids_list:
                                try:
                                    await self.move_node(nid, salvaged_root_id, "bottom")
                                    notes_orphaned += 1
                                except Exception as e:  # noqa: BLE001
                                    log_event(
                                        f"Failed to move notes node {nid} to salvaged root: {e}",
                                        "BEACON",
                                    )

        # If we created a fresh parking node and successfully moved notes off it, delete it
        if not dry_run and parking_created_here and parking_node_id:
            try:
                await self.delete_node(parking_node_id)
            except Exception:
                # Non-fatal
                pass

        # 4.6 Update hash in file node note (append or replace Source-SHA1 line)
        previous_sha1 = existing_sha1
        if not dry_run:
            lines = note_text.splitlines() if note_text else []
            new_line = f"Source-SHA1: {file_sha1}"
            replaced = False
            for idx, line in enumerate(lines):
                if line.strip().startswith("Source-SHA1:"):
                    lines[idx] = new_line
                    replaced = True
                    break
            if not replaced:
                if lines and lines[-1].strip():
                    lines.append("")
                lines.append(new_line)
            new_note = "\n".join(lines)
            try:
                update_req = NodeUpdateRequest(
                    name=file_node.get("name"),
                    note=new_note,
                    layoutMode=(file_node.get("data") or {}).get("layoutMode"),
                )
                await self.update_node(str(file_node_id), update_req)
            except Exception as e:  # noqa: BLE001
                raise NetworkError(f"Failed to update file node hash note: {e}") from e

            # Mark nodes-export cache dirty for this subtree
            try:
                self._mark_nodes_export_dirty([str(file_node_id)])
            except Exception:
                pass

        log_event(
            "refresh_file_node_beacons complete for "
            f"{source_path} (deleted={structural_deleted}, created={structural_created}, "
            f"notes_salvaged={notes_salvaged}, notes_orphaned={notes_orphaned})",
            "BEACON",
        )

        return {
            "success": True,
            "file_node_id": file_node_id,
            "source_path": source_path,
            "previous_sha1": previous_sha1,
            "file_sha1": file_sha1,
            "changed": True,
            "dry_run": dry_run,
            "structural_nodes_deleted": structural_deleted,
            "structural_nodes_created": structural_created,
            "notes_identified_for_salvage": total_notes_to_salvage,
            "notes_salvaged": notes_salvaged,
            "notes_orphaned": notes_orphaned,
        }
