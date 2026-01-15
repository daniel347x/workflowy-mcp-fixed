"""WorkFlowy API client - NEXUS pipeline operations."""

import json
import sys
import os
from typing import Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field

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


# @beacon[
#   id=auto-beacon@parse_path_or_root_from_note-cw31,
#   role=parse_path_or_root_from_note,
#   slice_labels=parse_path_or_root_from_note,nexus-test,
#   kind=ast,
# ]
def parse_path_or_root_from_note(note_str: str | None) -> str | None:
    """Extract a filesystem path from a Cartographer note header.

    Recognizes both "Path:" (per-file metadata) and "Root:" (Cartographer root).
    Returns the value after the colon, stripped, or None if not found.
    """
    for line in (note_str or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("Path:") or stripped.startswith("Root:"):
            return stripped.split(":", 1)[1].strip() or None
    return None


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


# @beacon[
#   id=auto-beacon@_is_notes_name-bpic,
#   role=_is_notes_name,
#   slice_labels=ra-notes,ra-notes-salvage,
#   kind=ast,
# ]
def _is_notes_name(raw_name: str) -> bool:
    """Detect Notes[...] roots (strip leading emoji/bullets).

    IMPORTANT (Jan 2026):
    We now require an explicit NOTES emoji/badge prefix so that ordinary
    headings like "Notes on X" in source files are not treated as persistent
    NOTES subtrees during Cartographer refresh/reconcile.

    Recognized prefixes:
    - 📝  user-authored persistent NOTES roots
    - 🅿️  internal "Notes (parking)" nodes
    - 🧩  internal "Notes (salvaged)" nodes
    - 💥  impact/critical notes
    - 🌟  highlighted/starred notes
    - 💡  idea/insight notes
    - 📌  pinned notes
    - 📓  dark notebook notes
    - 🧾  receipt/log notes
    - 🧠  brain/knowledge notes
    - 🕯️  candle/guiding-light notes
    - 🧨  explosive/experimental notes
    """
    name = (raw_name or "").strip()
    if not name:
        return False

    # Require one of the known NOTES markers as the leading character.
    # This gates out plain headings like "Notes on ..." that are part of the
    # source syntax tree rather than persistent NOTES subtrees.
    first = name[0]
    if first not in {"📝", "🅿️", "🧩", "💥", "🌟", "💡", "📌", "📓", "🧾", "🧠", "🕯️", "🧨"}:
        return False

    # Strip the leading marker and any immediate non-alphanumeric decoration
    # (bullets, arrows, etc.), then look for a "notes..." prefix in the
    # remaining text.
    candidate = name[1:].lstrip()
    if not candidate:
        return False
    if not candidate[0].isalnum():
        candidate = candidate[1:].lstrip()
        if not candidate:
            return False

    return candidate.lower().startswith("notes")


@dataclass
# @beacon[
#   id=auto-beacon@NotesSalvageContext-rkks,
#   role=NotesSalvageContext,
#   slice_labels=ra-notes,ra-notes-salvage,
#   kind=ast,
# ]
class NotesSalvageContext:
    """Shared NOTES salvage/restore metadata for a single FILE node.

    This context is produced before we mutate a Cartographer-mapped FILE subtree
    (legacy full-rebuild path) and is also used in the incremental F12 path
    as belt-and-suspenders protection.
    """

    # Beacon-based salvage: beacon_id -> [notes_root_id, ...]
    saved_notes_by_beacon: dict[str, list[str]] = field(default_factory=dict)
    # FILE-level path-based salvage: canonical_path/None -> [notes_root_id, ...]
    stashed_notes_by_path: dict[str | None, list[str]] = field(default_factory=dict)
    # Notes under non-FILE, non-beacon parents keyed by original parent_id
    generic_notes_by_parent: dict[str, list[str]] = field(default_factory=dict)

    # Parking node used only in the legacy path when we temporarily move
    # NOTES subtrees out from under beacon nodes while we delete/rebuild the
    # structural subtree.
    parking_node_id: str | None = None
    parking_created_here: bool = False

    # Counters for telemetry
    total_notes_to_salvage: int = 0
    notes_moved_to_parking: int = 0


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
        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.workflowy_scry._stamp_children_status_complete-cl74,
        #   role=WorkFlowyClientNexus.workflowy_scry._stamp_children_status_complete,
        #   slice_labels=WorkFlowyClientNexus-workflowy_scry-_stamp_children_status_complete,nexus-foo,
        #   kind=ast,
        # ]
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
                "parent_id": root_node.get('parent_id') or root_node.get('parentId')
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
                    "parent_id": target_root.get('parent_id') or target_root.get('parentId')
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
            'parent_id': ws_root.get('parent_id') or ws_root.get('parentId'),
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

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.beacon_get_code_snippet-c2ov,
    #   role=WorkFlowyClientNexus.beacon_get_code_snippet,
    #   slice_labels=nexus-md-header-path,ra-snippet-range,f9-f12-handlers,
    #   kind=ast,
    # ]
    async def beacon_get_code_snippet(
        self,
        beacon_node_id: str,
        context: int = 10,
    ) -> dict[str, Any]:
        """Resolve a Workflowy UUID to (file_path, beacon_id/kind or heuristic) and snippet.

        Primary (beacon) mode:
        - beacon_node_id points to a Cartographer beacon node whose note contains
          "BEACON (AST|SPAN)" with an `id:` (and optional `kind:`) line.
        - We locate the enclosing FILE node (note starts with "Path: ...").
        - Delegate to beacon_obtain_code_snippet.get_snippet_data(...) which
          understands language-specific beacon semantics (AST vs span, etc.).

        Extended (non-beacon Cartographer node) mode:
        - If the node's note does *not* contain beacon metadata, we still
          resolve the enclosing FILE node via the same Path: heuristic.
        - We then derive a simple search token from the node's *name* by:
            * stripping trailing tags (e.g. "#model-forward")
            * stripping leading non-alphanumeric decoration/emoji
        - We search for that token in the source file and return a context
          window around the first matching line.

        This allows the MCP tool to return source for *any* Cartographer node
        (AST or span), not just beacon nodes.
        """
        logger = _ClientLogger()

        # Helper: derive a quick-and-dirty search token from a Cartographer node name.
        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.beacon_get_code_snippet._derive_search_text_from_node_name-qsxf,
        #   role=WorkFlowyClientNexus.beacon_get_code_snippet._derive_search_text_from_node_name,
        #   slice_labels=WorkFlowyClientNexus-beacon_get_code_snippet-_derive_search_text_from_node_name,nexus-test,
        #   kind=ast,
        # ]
        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.beacon_get_code_snippet._derive_search_text_from_node_name-beqx,
        #   role=WorkFlowyClientNexus.beacon_get_code_snippet._derive_search_text_from_node_name,
        #   slice_labels=WorkFlowyClientNexus-beacon_get_code_snippet-_derive_search_text_from_node_name,nexus-foo,
        #   kind=ast,
        # ]
        def _derive_search_text_from_node_name(raw_name: str) -> str:
            if not raw_name:
                return ""
            name = str(raw_name).strip()
            if not name:
                return ""

            # Drop trailing tags like "#model-forward" or "#docs-intro".
            tokens = name.split()
            while tokens and (str(tokens[-1]).startswith("#") or str(tokens[-1]).startswith("🔱")):
                tokens.pop()
            core = " ".join(tokens).strip()
            if not core:
                core = name  # fall back to full name if everything looked like a tag

            # Strip leading non-alphanumeric / decoration (emoji, bullets, etc.).
            i = 0
            while i < len(core) and not (core[i].isalnum() or core[i] in {"_", "$"}):
                i += 1
            core = core[i:].lstrip()
            return core

        # Helper: heuristic snippet around first occurrence of search_text.
        def _heuristic_snippet_for_node(file_path: str, search_text: str, ctx: int) -> tuple[int, int, list[str]]:
            if not search_text:
                raise RuntimeError("Empty search text for heuristic snippet lookup")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    lines_local = f.read().splitlines()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"Failed to read source file {file_path!r}: {exc}") from exc

            anchor: int | None = None
            for idx, line in enumerate(lines_local, start=1):
                if search_text in line:
                    anchor = idx
                    break

            if anchor is None:
                raise RuntimeError(
                    f"Search token {search_text!r} not found in file {file_path!r}"
                )

            lo = max(1, anchor - ctx)
            hi = min(len(lines_local), anchor + ctx)
            return lo, hi, lines_local

        # Step 1: Load /nodes-export snapshot (cached if available)
        # Gracefully handle cache-not-initialized so we can still do prefix matching.
        nodes_by_id: dict[str, dict[str, Any]] = {}
        try:
            raw = await export_nodes_impl(self, node_id=None, use_cache=True, force_refresh=False)
            all_nodes = raw.get("nodes", []) or []
            nodes_by_id = {
                str(n.get("id")): n for n in all_nodes if n.get("id")
            }
            logger.info(
                f"beacon_get_code_snippet: /nodes-export snapshot loaded with {len(nodes_by_id)} nodes"
            )
        except NetworkError as e:
            # Cache not initialized – we'll try force_refresh below or fail cleanly
            if "not initialized" in str(e):
                logger.info(
                    f"beacon_get_code_snippet: cache not initialized; will attempt force_refresh"
                )
            else:
                raise

        # If node not found in the current snapshot, rely on prefix matching only.
        # We deliberately avoid triggering a fresh /nodes-export here because
        # automatic refresh is disabled by design; Dan prefers explicit cache
        # refresh via the workflowy_refresh_nodes_export_cache tool.
        if beacon_node_id not in nodes_by_id:
            if not nodes_by_id:
                # No snapshot available at all; surface the same guidance as
                # export_nodes_impl rather than attempting an implicit refresh.
                raise NetworkError(
                    "nodes-export cache is not initialized; run "
                    "workflowy_refresh_nodes_export_cache from the UUID widget."
                )
            logger.info(
                f"beacon_get_code_snippet: {beacon_node_id!r} not in cached /nodes-export; "
                "will rely on prefix matching only (no automatic refresh)"
            )
            # nodes_by_id remains as-is; prefix matching below may still succeed
            # if the caller supplied a truncated or slightly corrupted UUID.

        beacon_node = nodes_by_id.get(beacon_node_id)

        # UUID hallucination tolerance: if exact match fails, try prefix matching.
        # Iteratively shave off segments from the end (e.g., ade00478-cf38-4a0c-...
        # → ade00478-cf38-4a0c → ade00478-cf38 → ade00478), supporting both
        # hyphenated and non-hyphenated UUIDs.
        if not beacon_node:
            candidate_id = beacon_node_id.replace("-", "")  # normalize to no-hyphens
            min_prefix_len = 8
            for prefix_len in range(len(candidate_id), min_prefix_len - 1, -1):
                prefix = candidate_id[:prefix_len]
                matches = [
                    nid for nid in nodes_by_id.keys()
                    if str(nid).replace("-", "").startswith(prefix)
                ]
                if len(matches) == 1:
                    beacon_node = nodes_by_id.get(matches[0])
                    logger.info(
                        f"beacon_get_code_snippet: UUID prefix match {beacon_node_id!r} → {matches[0]}"
                    )
                    break
                if len(matches) > 1:
                    # Ambiguous prefix; don't guess.
                    break

        if not beacon_node:
            raise NetworkError(
                f"Node {beacon_node_id!r} not found in /nodes-export snapshot (tried prefix matching)"
            )

        # Use the canonical UUID of the resolved node as the starting point for
        # ancestor traversal. This ensures that prefix-matched UUIDs behave the
        # same as exact matches when locating the enclosing FILE node.
        start_uuid = (
            str(beacon_node.get("id")) if beacon_node and beacon_node.get("id") else beacon_node_id
        )

        node_name = str(beacon_node.get("name") or "").strip()

        # Step 2: Parse beacon note for id and kind (if present)
        note = beacon_node.get("note") or beacon_node.get("no") or ""
        beacon_id: str | None = None
        kind: str | None = None
        has_beacon_metadata = False
        if isinstance(note, str) and ("BEACON (" in note or "@beacon[" in note):
            for line in note.splitlines():
                stripped = line.strip()
                if stripped.startswith("id:"):
                    beacon_id = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("kind:"):
                    kind = stripped.split(":", 1)[1].strip()
            if beacon_id:
                has_beacon_metadata = True

        # Step 3: Walk ancestors to find FILE node with Path: ... in its note
        logger.info(
            f"beacon_get_code_snippet: starting FILE ancestor search from {start_uuid!r}"
        )
        current_id = start_uuid
        visited: set[str] = set()
        file_node: dict[str, Any] | None = None

        while current_id and current_id not in visited:
            visited.add(current_id)
            node = nodes_by_id.get(current_id)
            if not node:
                logger.warning(
                    f"beacon_get_code_snippet: node {current_id!r} missing from nodes_by_id during ancestor walk"
                )
                break

            n_note = node.get("note") or node.get("no") or ""
            node_name_dbg = str(node.get("name") or "").strip()
            note_preview = ""
            first_line = ""
            if isinstance(n_note, str):
                lines_local = n_note.splitlines()
                if lines_local:
                    first_line = lines_local[0].strip()
                    note_preview = "\n".join(lines_local[:2])
            has_path_header = first_line.startswith("Path:") or first_line.startswith("Root:")
            logger.debug(
                f"beacon_get_code_snippet: visit node={current_id!r} name={node_name_dbg!r} "
                f"has_Path={has_path_header} note_preview={note_preview!r}"
            )

            if has_path_header:
                # Heuristic: FILE nodes created by Cartographer store the
                # local path on the first line as "Path: E:\\...".
                file_node = node
                break

            parent_id = node.get("parent_id") or node.get("parentId")
            current_id = str(parent_id) if parent_id else None

        if not file_node:
            logger.error(
                "beacon_get_code_snippet: no ancestor FILE node with 'Path:' "
                f"for {beacon_node_id!r}; visited_chain={list(visited)}"
            )
            raise NetworkError(
                "Could not find ancestor FILE node with 'Path:' note for "
                f"node {beacon_node_id!r}"
            )

        file_note = file_node.get("note") or file_node.get("no") or ""
        file_path: str | None = None

        def _extract_path_from_note(note_str: str | None) -> str | None:
            if not isinstance(note_str, str):
                return None
            for line in note_str.splitlines():
                stripped = line.strip()
                if stripped.startswith("Path:"):
                    val = stripped.split(":", 1)[1].strip()
                    if val:
                        return val
            return None

        # First attempt: use the /nodes-export snapshot (may be slightly stale).
        file_path = _extract_path_from_note(file_note)

        # If snapshot does not yet reflect the Path header (e.g. after a recent
        # Cartographer refresh that has not been mirrored into the cached
        # /nodes-export), fall back to a targeted /nodes/<id> fetch for this FILE
        # node to obtain its most up-to-date note.
        if not file_path:
            try:
                file_id = str(file_node.get("id"))
                fresh_node = await self.get_node(file_id)
                fresh_dict = fresh_node.model_dump()
                fresh_note = fresh_dict.get("note") or fresh_dict.get("no")
                file_path = _extract_path_from_note(fresh_note)
                if not file_path:
                    logger.warning(
                        "beacon_get_code_snippet: FILE node %r still missing 'Path:' after fresh get_node",
                        file_id,
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "beacon_get_code_snippet: failed to refresh FILE node %r for Path lookup: %s",
                    file_node.get("id"),
                    e,
                )

        if not file_path:
            raise NetworkError(
                f"Ancestor FILE node for {beacon_node_id!r} is missing a 'Path:' line"
            )

        # Step 4A: Beacon-aware snippet (AST/span) using helper module
        # For Markdown AST headings with MD_PATH, prefer MD_PATH semantics (same as non-beacon AST nodes)
        if has_beacon_metadata and beacon_id and not (
            Path(file_path).suffix.lower() in {".md", ".markdown"} and (kind or "").strip().lower() == "ast"
        ):
            try:
                import importlib

                # Resolve project_root similarly to refresh_file_node_beacons so that
                # beacon_obtain_code_snippet.py can live at the project root alongside
                # nexus_map_codebase.py.
                client_dir = os.path.dirname(os.path.abspath(__file__))
                wf_mcp_dir = os.path.dirname(client_dir)
                mcp_servers_dir = os.path.dirname(wf_mcp_dir)
                project_root = os.path.dirname(mcp_servers_dir)
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)

                bos = importlib.import_module("beacon_obtain_code_snippet")
            except Exception as e:  # noqa: BLE001
                raise NetworkError(
                    "Could not import beacon_obtain_code_snippet module; "
                    "ensure it resides at the project root alongside nexus_map_codebase.py. "
                    f"Underlying error: {e}"
                ) from e

            try:
                snippet_result = bos.get_snippet_data(file_path, beacon_id, context)  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001
                raise NetworkError(
                    f"Failed to obtain snippet for beacon id {beacon_id!r} in file {file_path!r}: {e}"
                ) from e

            # Unpack the tuple from helper: prefer enriched 7-tuple but support legacy 6-tuple.
            if isinstance(snippet_result, tuple) and len(snippet_result) == 7:
                start, end, lines, core_start, core_end, beacon_line, metadata = snippet_result
            elif isinstance(snippet_result, tuple) and len(snippet_result) == 6:
                start, end, lines, core_start, core_end, beacon_line = snippet_result
                metadata = {
                    "resolution_strategy": "beacon",
                    "confidence": 1.0,
                    "ambiguity": "none",
                    "candidates": None,
                }
            else:
                # Legacy fallback (shouldn't happen with updated helper)
                start, end, lines = snippet_result[:3]  # type: ignore[misc]
                core_start, core_end = start, end
                beacon_line = start
                metadata = {
                    "resolution_strategy": "beacon",
                    "confidence": 1.0,
                    "ambiguity": "none",
                    "candidates": None,
                }

            snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""
            return {
                "success": True,
                "file_path": file_path,
                "beacon_node_id": beacon_node_id,
                "beacon_id": beacon_id,
                "kind": kind,
                "start_line": start,
                "end_line": end,
                "core_start_line": core_start,
                "core_end_line": core_end,
                "beacon_line": beacon_line,
                "snippet": snippet_text,
                "resolution_strategy": metadata.get("resolution_strategy"),
                "confidence": metadata.get("confidence"),
                "ambiguity": metadata.get("ambiguity"),
                "candidates": metadata.get("candidates"),
            }

        # Step 4B: Non-beacon Cartographer node – resolve via AST_QUALNAME when available
        ext = Path(file_path).suffix.lower()

        # For Python AST nodes, prefer AST_QUALNAME when present; otherwise fall through
        # to text-like file handling below.
        if ext == ".py":
            ast_qualname: str | None = None
            if isinstance(note, str):
                for line in note.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("AST_QUALNAME:"):
                        ast_qualname = stripped.split(":", 1)[1].strip()
                        break

            if ast_qualname:
                try:
                    import importlib

                    client_dir = os.path.dirname(os.path.abspath(__file__))
                    wf_mcp_dir = os.path.dirname(client_dir)
                    mcp_servers_dir = os.path.dirname(wf_mcp_dir)
                    project_root = os.path.dirname(mcp_servers_dir)
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)

                    bos = importlib.import_module("beacon_obtain_code_snippet")
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        "Could not import beacon_obtain_code_snippet module for AST_QUALNAME-based "
                        f"resolution; underlying error: {e}"
                    ) from e

                try:
                    snippet_result = bos.get_snippet_for_ast_qualname(  # type: ignore[attr-defined]
                        file_path,
                        ast_qualname,
                        context,
                    )
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        f"Failed to obtain AST_QUALNAME-based snippet for node {beacon_node_id!r} "
                        f"in file {file_path!r}: {e}"
                    ) from e

                if isinstance(snippet_result, tuple) and len(snippet_result) == 7:
                    start, end, lines, core_start, core_end, beacon_line, metadata = snippet_result
                else:
                    # Extremely unlikely; enforce explicit error rather than guessing.
                    raise NetworkError(
                        "get_snippet_for_ast_qualname returned unexpected shape; "
                        "expected 7-tuple."
                    )

                snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""

                return {
                    "success": True,
                    "file_path": file_path,
                    "beacon_node_id": beacon_node_id,
                    "beacon_id": ast_qualname,
                    "kind": "node",
                    "start_line": start,
                    "end_line": end,
                    "core_start_line": core_start,
                    "core_end_line": core_end,
                    "beacon_line": beacon_line,
                    "snippet": snippet_text,
                    "resolution_strategy": metadata.get("resolution_strategy"),
                    "confidence": metadata.get("confidence"),
                    "ambiguity": metadata.get("ambiguity"),
                    "candidates": metadata.get("candidates"),
                }

        # @beacon[
        #   id=carto-js-ts@js_ts_ast_qualname_resolution,
        #   slice_labels=carto-js-ts,carto-js-ts-snippets,
        #   kind=span,
        #   comment=JS/TS AST_QUALNAME resolution - mirrors Python AST_QUALNAME handling for JS/TS files,
        # ]
        # For JS/TS AST nodes, prefer AST_QUALNAME when present; otherwise fall through
        # to text-like file handling below.
        if ext in {".js", ".jsx", ".ts", ".tsx"}:
            ast_qualname: str | None = None
            if isinstance(note, str):
                for line in note.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("AST_QUALNAME:"):
                        ast_qualname = stripped.split(":", 1)[1].strip()
                        break

            if ast_qualname:
                try:
                    import importlib

                    client_dir = os.path.dirname(os.path.abspath(__file__))
                    wf_mcp_dir = os.path.dirname(client_dir)
                    mcp_servers_dir = os.path.dirname(wf_mcp_dir)
                    project_root = os.path.dirname(mcp_servers_dir)
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)

                    bos = importlib.import_module("beacon_obtain_code_snippet")
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        "Could not import beacon_obtain_code_snippet module for JS/TS AST_QUALNAME-based "
                        f"resolution; underlying error: {e}"
                    ) from e

                try:
                    snippet_result = bos.get_snippet_for_ast_qualname_js_ts(  # type: ignore[attr-defined]
                        file_path,
                        ast_qualname,
                        context,
                    )
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        f"Failed to obtain JS/TS AST_QUALNAME-based snippet for node {beacon_node_id!r} "
                        f"in file {file_path!r}: {e}"
                    ) from e

                if isinstance(snippet_result, tuple) and len(snippet_result) == 7:
                    start, end, lines, core_start, core_end, beacon_line, metadata = snippet_result
                else:
                    # Extremely unlikely; enforce explicit error rather than guessing.
                    raise NetworkError(
                        "get_snippet_for_ast_qualname_js_ts returned unexpected shape; "
                        "expected 7-tuple."
                    )

                snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""

                return {
                    "success": True,
                    "file_path": file_path,
                    "beacon_node_id": beacon_node_id,
                    "beacon_id": ast_qualname,
                    "kind": "node",
                    "start_line": start,
                    "end_line": end,
                    "core_start_line": core_start,
                    "core_end_line": core_end,
                    "beacon_line": beacon_line,
                    "snippet": snippet_text,
                    "resolution_strategy": metadata.get("resolution_strategy"),
                    "confidence": metadata.get("confidence"),
                    "ambiguity": metadata.get("ambiguity"),
                    "candidates": metadata.get("candidates"),
                }

        # For Markdown heading nodes, prefer MD_PATH when present; otherwise fall through
        # to text-like file handling below.
        if ext in {".md", ".markdown"}:
            md_path_lines: list[str] = []
            if isinstance(note, str):
                in_block = False
                for line in note.splitlines():
                    stripped = line.strip()
                    if not in_block:
                        if stripped.startswith("MD_PATH:"):
                            in_block = True
                            continue
                    else:
                        if stripped == "---":
                            break
                        if stripped:
                            md_path_lines.append(stripped)

            if md_path_lines:
                try:
                    import importlib

                    client_dir = os.path.dirname(os.path.abspath(__file__))
                    wf_mcp_dir = os.path.dirname(client_dir)
                    mcp_servers_dir = os.path.dirname(wf_mcp_dir)
                    project_root = os.path.dirname(mcp_servers_dir)
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)

                    bos = importlib.import_module("beacon_obtain_code_snippet")
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        "Could not import beacon_obtain_code_snippet module for MD_PATH-based "
                        f"resolution; underlying error: {e}"
                    ) from e

                try:
                    snippet_result = bos.get_snippet_for_md_path(  # type: ignore[attr-defined]
                        file_path,
                        md_path_lines,
                        context,
                    )
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        f"Failed to obtain MD_PATH-based snippet for node {beacon_node_id!r} "
                        f"in file {file_path!r}: {e}"
                    ) from e

                if isinstance(snippet_result, tuple) and len(snippet_result) == 7:
                    start, end, lines, core_start, core_end, beacon_line, metadata = snippet_result
                else:
                    raise NetworkError(
                        "get_snippet_for_md_path returned unexpected shape; expected 7-tuple."
                    )

                snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""

                return {
                    "success": True,
                    "file_path": file_path,
                    "beacon_node_id": beacon_node_id,
                    # Represent the MD_PATH in a compact single-line form for debugging.
                    "beacon_id": " | ".join(md_path_lines),
                    "kind": "node",
                    "start_line": start,
                    "end_line": end,
                    "core_start_line": core_start,
                    "core_end_line": core_end,
                    "beacon_line": beacon_line,
                    "snippet": snippet_text,
                    "resolution_strategy": metadata.get("resolution_strategy"),
                    "confidence": metadata.get("confidence"),
                    "ambiguity": metadata.get("ambiguity"),
                    "candidates": metadata.get("candidates"),
                }

        # For nodes without AST/MD_PATH metadata, fall back to a file-level snippet
        # for text-like files. This behaves like a targeted read_file() using the
        # node's name as an anchor when possible.
        TEXT_EXTS = {
            ".py",
            ".md",
            ".markdown",
            ".txt",
            ".json",
            ".yaml",
            ".yml",
            ".toml",
            ".ini",
            ".cfg",
            ".sql",
            ".sh",
            ".ps1",
            ".bat",
            ".js",
            ".ts",
            ".tsx",
            ".css",
            ".html",
            ".c",
            ".h",
            ".hpp",
            ".hh",
            ".hxx",
            ".cpp",
            ".cc",
            ".cxx",
        }
        base_name = Path(file_path).name
        is_text_like = ext in TEXT_EXTS or (
            ext == "" and base_name in {"Dockerfile", "dockerfile", "Makefile", "makefile"}
        )
        if is_text_like:
            lines: list[str] = []
            start = end = 0
            strategy = "file_top"
            search_text = _derive_search_text_from_node_name(node_name)
            if search_text:
                try:
                    start, end, lines = _heuristic_snippet_for_node(
                        file_path,
                        search_text,
                        context,
                    )
                    strategy = "file_anchor_search"
                except Exception:  # noqa: BLE001
                    # Fall through to top-of-file snippet
                    lines = []
            if not lines:
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.read().splitlines()
                except Exception as exc:  # noqa: BLE001
                    raise NetworkError(
                        f"Failed to read source file {file_path!r}: {exc}"
                    ) from exc
                start = 1
                end = min(len(lines), max(1, context * 2))
                strategy = "file_top"
            snippet_text = "\n".join(lines[start - 1 : end]) if lines else ""
            return {
                "success": True,
                "file_path": file_path,
                "beacon_node_id": beacon_node_id,
                "beacon_id": Path(file_path).name,
                "kind": "file",
                "start_line": start,
                "end_line": end,
                "core_start_line": start,
                "core_end_line": end,
                "beacon_line": start,
                "snippet": snippet_text,
                "resolution_strategy": strategy,
                "confidence": 0.7,
                "ambiguity": "none",
                "candidates": None,
            }

        # Non-text: still require explicit metadata
        raise NetworkError(
            "Non-beacon snippet resolution is only implemented for Python AST "
            "nodes with AST_QUALNAME, Markdown nodes with MD_PATH, or text-like files; "
            f"node {beacon_node_id!r} is in file {file_path!r} "
            f"with extension {ext!r}."
        )

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol-ozt9,
    #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol,
    #   slice_labels=f9-f12-handlers,nexus-md-header-path,ra-snippet-range,
    #   kind=ast,
    # ]
    async def read_text_snippet_by_symbol(
        self,
        file_path: str | None,
        symbol: str,
        symbol_kind: str = "auto",
        context: int = 10,
    ) -> dict[str, Any]:
        """Resolve a snippet by optional file path/name and symbol.

        This is a handle-based companion to beacon_get_code_snippet/read_text_snippet
        that avoids requiring the caller to supply a Workflowy UUID. It works by:

        1. Loading the cached /nodes-export snapshot (refreshing once on demand).
        2. Locating Cartographer FILE nodes via their Path:/Root: note header.
        3. Optionally restricting the search to a single FILE based on `file_path`
           (full path or basename); otherwise searching all FILE nodes.
        4. Within the chosen FILE subtree(s), attempting to match `symbol` as:

           - Beacon id or role (BEACON blocks in the note),
           - AST_QUALNAME (Python/JS/TS AST nodes),
           - Normalized node name (function/method/heading name).

        5. If exactly one node matches across the requested symbol kinds, delegating
           to beacon_get_code_snippet(...) using that node's UUID and returning its
           full result dict, including `snippet`.

        If zero or multiple matches are found, a NetworkError is raised describing
        the mismatch/ambiguity so the caller can refine `file_path` or `symbol`.
        """
        logger = _ClientLogger()

        symbol = (symbol or "").strip()
        if not symbol:
            raise NetworkError("read_text_snippet_by_symbol: symbol must be a non-empty string")

        # 1) Load /nodes-export snapshot (cached if available).
        raw = await export_nodes_impl(self, node_id=None, use_cache=True, force_refresh=False)
        all_nodes = raw.get("nodes", []) or []
        nodes_by_id: dict[str, dict[str, Any]] = {
            str(n.get("id")): n for n in all_nodes if n.get("id")
        }
        if not nodes_by_id:
            raise NetworkError(
                "read_text_snippet_by_symbol: /nodes-export snapshot is empty; cannot resolve symbol",
            )

        # Build parent map and detect HIDE CARTOGRAPHER sentinels so we can
        # ignore entire Cartographer mappings that live under those nodes.
        parent_by_id: dict[str, str] = {}
        for n in all_nodes:
            nid = n.get("id")
            pid = n.get("parent_id") or n.get("parentId")
            if nid and pid:
                parent_by_id[str(nid)] = str(pid)

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._is_hide_cartographer_node-9s4h,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._is_hide_cartographer_node,
        #   slice_labels=ra-snippet-range,
        #   kind=ast,
        # ]
        def _is_hide_cartographer_node(node: dict[str, Any]) -> bool:
            raw_name = str(node.get("name") or "")
            name = raw_name.strip()
            if not name:
                return False
            tokens = name.split()
            # Drop trailing #tags.
            while tokens and str(tokens[-1]).startswith("#"):
                tokens.pop()
            if not tokens:
                return False
            core = " ".join(tokens).strip().lower()
            return core == "hide cartographer"

        hide_ids: set[str] = set()
        for n in nodes_by_id.values():
            if _is_hide_cartographer_node(n):
                nid = n.get("id")
                if nid:
                    hide_ids.add(str(nid))

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._is_under_hidden_subtree-y3qp,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._is_under_hidden_subtree,
        #   slice_labels=ra-snippet-range,
        #   kind=ast,
        # ]
        def _is_under_hidden_subtree(node_id: str) -> bool:
            cur = node_id
            visited: set[str] = set()
            while cur and cur not in visited:
                if cur in hide_ids:
                    return True
                visited.add(cur)
                cur = parent_by_id.get(cur)
            return False

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._canonical_path-0zpv,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._canonical_path,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _canonical_path(path: str) -> str:
            return os.path.normcase(os.path.normpath(path))

        # 2) Identify Cartographer FILE nodes via Path:/Root: headers,
        # excluding any that live under a "HIDE CARTOGRAPHER" sentinel.
        file_nodes: list[tuple[dict[str, Any], str]] = []
        for n in nodes_by_id.values():
            n_note = n.get("note") or n.get("no") or ""
            path = parse_path_or_root_from_note(str(n_note))
            if not path:
                continue
            nid = n.get("id")
            sid = str(nid) if nid else None
            if sid and _is_under_hidden_subtree(sid):
                continue
            file_nodes.append((n, path))

        if not file_nodes:
            raise NetworkError(
                "read_text_snippet_by_symbol: no Cartographer-mapped FILE nodes with Path:/Root: found",
            )

        # 3) Restrict FILE set by file_path when provided (full path or basename).
        search_roots: list[tuple[dict[str, Any], str]]
        if file_path:
            fp = file_path.strip()
            if not fp:
                search_roots = file_nodes
            else:
                is_full = any(sep in fp for sep in ("\\", "/")) or ":" in fp
                matches: list[tuple[dict[str, Any], str]]
                if is_full:
                    target_norm = _canonical_path(fp)
                    matches = [
                        (n, p) for (n, p) in file_nodes
                        if _canonical_path(p) == target_norm
                    ]
                else:
                    matches = [
                        (n, p) for (n, p) in file_nodes
                        if os.path.basename(p) == fp
                    ]
                if not matches:
                    raise NetworkError(
                        "read_text_snippet_by_symbol: no FILE node with Path: matching "
                        f"{file_path!r} was found",
                    )

                # Prefer matches that correspond to real files on disk and do
                # not contain an ellipsis placeholder in the path. This avoids
                # false-positive ambiguities from stale or placeholder Path:
                # values such as "E:\\...\\TODO\\nexus_map_codebase.py".
                existing_matches = [
                    (n, p)
                    for (n, p) in matches
                    if os.path.isfile(p) and "..." not in p
                ]
                if existing_matches:
                    matches = existing_matches

                paths = sorted({p for _, p in matches})
                if len(paths) > 1:
                    raise NetworkError(
                        "read_text_snippet_by_symbol: ambiguous file_path; matches: "
                        + ", ".join(repr(p) for p in paths),
                    )
                search_roots = matches

        else:
            search_roots = file_nodes

        # 4) Build children index for subtree walks.
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        for n in all_nodes:
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                children_by_parent.setdefault(str(pid), []).append(n)

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._iter_subtree-gums,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._iter_subtree,
        #   slice_labels=f9-f12-handlers,
        #   kind=ast,
        # ]
        def _iter_subtree(root_id: str):
            stack = [str(root_id)]
            visited: set[str] = set()
            while stack:
                nid = stack.pop()
                if nid in visited:
                    continue
                visited.add(nid)
                node = nodes_by_id.get(nid)
                if not node:
                    continue
                yield node
                for child in children_by_parent.get(nid, []) or []:
                    cid = child.get("id")
                    if cid:
                        stack.append(str(cid))

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._extract_beacon_fields-gb8d,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._extract_beacon_fields,
        #   slice_labels=f9-f12-handlers,ra-reconcile,
        #   kind=ast,
        # ]
        def _extract_beacon_fields(note_str: str) -> tuple[str | None, str | None]:
            beacon_id_val: str | None = None
            role_val: str | None = None
            if "BEACON (" not in note_str and "@beacon[" not in note_str:
                return None, None
            for line in note_str.splitlines():
                stripped = line.strip()
                if stripped.startswith("id:"):
                    beacon_id_val = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("role:"):
                    role_val = stripped.split(":", 1)[1].strip()
            return beacon_id_val or None, role_val or None

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._extract_ast_qualname-0hpl,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._extract_ast_qualname,
        #   slice_labels=f9-f12-handlers,ra-reconcile,
        #   kind=ast,
        # ]
        def _extract_ast_qualname(note_str: str) -> str | None:
            for line in note_str.splitlines():
                stripped = line.strip()
                if stripped.startswith("AST_QUALNAME:"):
                    return stripped.split(":", 1)[1].strip() or None
            return None

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._normalize_node_name-rkap,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._normalize_node_name,
        #   slice_labels=f9-f12-handlers,
        #   kind=ast,
        # ]
        def _normalize_node_name(raw_name: str | None) -> str:
            if not raw_name:
                return ""
            name = str(raw_name)
            # Strip leading decoration/emoji until alnum/underscore/dollar.
            i = 0
            while i < len(name) and not (name[i].isalnum() or name[i] in {"_", "$"}):
                i += 1
            name = name[i:].strip()
            if not name:
                return ""
            tokens = name.split()
            # Drop trailing #tags.
            while tokens and str(tokens[-1]).startswith("#"):
                tokens.pop()
            if not tokens:
                return ""
            # Drop structural prefixes like "ƒ", "class", "function", "method".
            if tokens[0] in {"ƒ", "class", "function", "method"}:
                tokens = tokens[1:]
            if not tokens:
                return ""
            core = " ".join(tokens)
            # Strip parameters after '('.
            paren = core.find("(")
            if paren != -1:
                core = core[:paren].strip()
            return core

        # Buckets for matches by strategy.
        node_to_file_path: dict[str, str] = {}
        beacon_hits: set[str] = set()
        ast_hits: set[str] = set()
        name_hits: set[str] = set()

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.read_text_snippet_by_symbol._record_hit-oous,
        #   role=WorkFlowyClientNexus.read_text_snippet_by_symbol._record_hit,
        #   slice_labels=f9-f12-handlers,
        #   kind=ast,
        # ]
        def _record_hit(node: dict[str, Any], owner_path: str, bucket: str) -> None:
            nid = node.get("id")
            if not nid:
                return
            s_nid = str(nid)
            node_to_file_path.setdefault(s_nid, owner_path)
            if bucket == "beacon":
                beacon_hits.add(s_nid)
            elif bucket == "ast":
                ast_hits.add(s_nid)
            elif bucket == "name":
                name_hits.add(s_nid)

        # Determine which strategies are enabled.
        use_beacon = symbol_kind in {"auto", "beacon"}
        use_ast = symbol_kind in {"auto", "ast"}
        use_name = symbol_kind in {"auto", "name"}

        # 5) Scan selected FILE subtrees and collect matches.
        for file_node, owner_path in search_roots:
            root_id = str(file_node.get("id")) if file_node.get("id") else None
            if not root_id:
                continue
            for node in _iter_subtree(root_id):
                note_str = str(node.get("note") or node.get("no") or "")

                if use_beacon:
                    beacon_id_val, role_val = _extract_beacon_fields(note_str)
                    if beacon_id_val == symbol or role_val == symbol:
                        _record_hit(node, owner_path, "beacon")

                if use_ast:
                    ast_q = _extract_ast_qualname(note_str)
                    if ast_q:
                        # If symbol includes a dot, treat it as a full AST_QUALNAME
                        # and require exact equality. If it has no dot, treat it
                        # as a short name and match against the final segment of
                        # the qualname. This lets callers use either
                        # "WorkFlowyClientNexus.read_text_snippet_by_symbol" or
                        # just "read_text_snippet_by_symbol" (with
                        # symbol_kind="ast") while still surfacing ambiguities
                        # when multiple matches exist.
                        if "." in symbol:
                            if ast_q == symbol:
                                _record_hit(node, owner_path, "ast")
                        else:
                            short = ast_q.rsplit(".", 1)[-1]
                            if short == symbol:
                                _record_hit(node, owner_path, "ast")

                if use_name:
                    norm_name = _normalize_node_name(node.get("name"))
                    if norm_name and norm_name == symbol:
                        _record_hit(node, owner_path, "name")

        # 6) Compute union of hits across enabled strategies and enforce uniqueness.
        active_sets: list[set[str]] = []
        if use_beacon:
            active_sets.append(beacon_hits)
        if use_ast:
            active_sets.append(ast_hits)
        if use_name:
            active_sets.append(name_hits)

        union_ids: set[str] = set()
        for s in active_sets:
            union_ids.update(s)

        if not union_ids:
            raise NetworkError(
                "read_text_snippet_by_symbol: no matching node found for symbol "
                f"{symbol!r} (symbol_kind={symbol_kind!r})",
            )

        if len(union_ids) > 1:
            # Build a concise ambiguity report.
            details: list[str] = []
            for nid in sorted(union_ids):
                node = nodes_by_id.get(nid) or {}
                nname = node.get("name") or "(no name)"
                fpath = node_to_file_path.get(nid) or "(unknown Path)"
                buckets: list[str] = []
                if nid in beacon_hits:
                    buckets.append("beacon")
                if nid in ast_hits:
                    buckets.append("ast")
                if nid in name_hits:
                    buckets.append("name")
                details.append(
                    f"id={nid} name={nname!r} file={fpath!r} via={'+'.join(buckets)}"
                )
            raise NetworkError(
                "read_text_snippet_by_symbol: symbol is ambiguous; candidates:\n"
                + "\n".join(details),
            )

        target_id = next(iter(union_ids))
        logger.info(
            "read_text_snippet_by_symbol: resolved symbol %r (kind=%r) to node %s",
            symbol,
            symbol_kind,
            target_id,
        )

        # 7) Delegate to beacon_get_code_snippet for the actual snippet extraction.
        result = await self.beacon_get_code_snippet(
            beacon_node_id=target_id,
            context=context,
        )

        if not isinstance(result, dict) or not result.get("success"):
            raise NetworkError(
                "read_text_snippet_by_symbol: beacon_get_code_snippet failed for node "
                f"{target_id!r} (symbol={symbol!r})",
            )

        # Attach resolution metadata for debugging/inspection.
        result.setdefault("resolved_symbol", symbol)
        result.setdefault("resolved_symbol_kind", symbol_kind)
        result.setdefault("resolved_node_id", target_id)
        result.setdefault("resolved_file_path", node_to_file_path.get(target_id))

        return result

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
                    parent_skel = skeleton_by_id.get(parent_id) or skeleton_by_id.get(parent_id)
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

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.bulk_import_from_file-7d6e,
    #   role=WorkFlowyClientNexus.bulk_import_from_file,
    #   slice_labels=ra-reconcile,f9-f12-handlers,
    #   kind=ast,
    # ]
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
        
        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.bulk_import_from_file.move_wrapper-f1w3,
        #   role=WorkFlowyClientNexus.bulk_import_from_file.move_wrapper,
        #   slice_labels=WorkFlowyClientNexus-bulk_import_from_file-move_wrapper,nexus-text,
        #   kind=ast,
        # ]
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

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.nexus_list_keystones-mzxs,
    #   role=WorkFlowyClientNexus.nexus_list_keystones,
    #   slice_labels=WorkFlowyClientNexus-nexus_list_keystones,nexus-test,
    #   kind=ast,
    # ]
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

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus._refresh_file_node_beacons_legacy-zjoy,
    #   role=WorkFlowyClientNexus._refresh_file_node_beacons_legacy,
    #   slice_labels=ra-notes,ra-notes-salvage,ra-notes-cartographer,ra-reconcile,f9-f12-handlers,
    #   kind=ast,
    # ]
    async def _refresh_file_node_beacons_legacy(
        self,
        file_node_id: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Per-file beacon-aware refresh of a Cartographer-mapped FILE node.

        Legacy implementation: full structural rebuild under the FILE node using
        Cartographer in single-file mode, with hash guard and robust NOTES
        salvage/restore semantics.

        This method intentionally DOES NOT touch siblings of the file node, and
        it preserves top-level "Notes" manual roots under the file.

        NOTE: file_node_id may point to any descendant under a Cartographer-mapped
        FILE node (e.g. a beacon node). In that case we walk the ancestor chain
        via the cached /nodes-export snapshot to locate the nearest ancestor whose
        note contains a 'Path:' line and treat THAT node as the file node to
        refresh.
        """
        logger = _ClientLogger()
        log_event(
            f"refresh_file_node_beacons (legacy) start (file_node_id={file_node_id}, dry_run={dry_run})",
            "BEACON",
        )

        # 1) Optional: resolve arbitrary descendant UUID → enclosing FILE node UUID.
        # This mirrors beacon_get_code_snippet semantics so callers can pass a
        # beacon or child node UUID and still refresh the correct FILE node.
        try:
            raw_cache = await export_nodes_impl(
                self,
                node_id=None,
                use_cache=True,
                force_refresh=False,
            )
            all_nodes_cache = raw_cache.get("nodes", []) or []
            nodes_by_id_cache: dict[str, dict[str, Any]] = {
                str(n.get("id")): n for n in all_nodes_cache if n.get("id")
            }
        except Exception:
            nodes_by_id_cache = {}

        if nodes_by_id_cache and str(file_node_id) in nodes_by_id_cache:
            current_id: str | None = str(file_node_id)
            visited_ids: set[str] = set()
            resolved_file_node_id: str | None = None

            while current_id and current_id not in visited_ids:
                visited_ids.add(current_id)
                node = nodes_by_id_cache.get(current_id)
                if not node:
                    break

                n_note = node.get("note") or node.get("no") or ""
                if isinstance(n_note, str) and "Path:" in n_note:
                    resolved_file_node_id = current_id
                    break

                parent_id_val = node.get("parent_id") or node.get("parentId")
                current_id = str(parent_id_val) if parent_id_val else None

            if resolved_file_node_id and resolved_file_node_id != file_node_id:
                log_event(
                    "refresh_file_node_beacons (legacy): resolved descendant UUID "
                    f"{file_node_id} → FILE node {resolved_file_node_id}",
                    "BEACON",
                )
                file_node_id = resolved_file_node_id

        # 2) Resolve file node & source path
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

        file_node: dict[str, Any] | None = None
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
        source_path = parse_path_or_root_from_note(note_text)
        existing_sha1: str | None = None
        for line in note_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("Source-SHA1:"):
                existing_sha1 = stripped.split(":", 1)[1].strip() or None

        if not source_path:
            raise NetworkError(
                f"File node {file_node_id} note is missing 'Path:'/'Root:' line; cannot refresh",
            )

        if not os.path.isfile(source_path):
            raise NetworkError(f"Source file not found at Path: {source_path}")

        # 3) Hash guard
        def _compute_sha1(path: str) -> str:
            import hashlib

            h = hashlib.sha1()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    h.update(chunk)
            return h.hexdigest()

        def _compute_line_count(path: str) -> int | None:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return sum(1 for _ in f)
            except Exception:
                return None

        file_sha1 = _compute_sha1(source_path)

        if existing_sha1 and existing_sha1 == file_sha1:
            log_event(
                f"refresh_file_node_beacons (legacy): unchanged (hash match) for {source_path}",
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

        previous_sha1 = existing_sha1

        # 4) Build parent->children index for current subtree (used for structural
        # deletion; salvage helpers build their own view as needed).
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            parent_id = n.get("parent_id") or n.get("parentId")
            if parent_id:
                children_by_parent.setdefault(str(parent_id), []).append(n)

        file_children = children_by_parent.get(str(file_node_id), [])

        # 5) Collect NOTES salvage context (shared with incremental F12 path).
        salvage_ctx = await self._collect_notes_salvage_context_for_file(
            flat_nodes=flat_nodes,
            file_node_id=str(file_node_id),
            source_path=source_path,
            dry_run=dry_run,
            mode="legacy",
        )

        # 6) Delete structural subtree under file node (non-Notes and non-parking)
        structural_deleted = 0
        if file_children:
            for child in file_children:
                cid = child.get("id")
                if not cid:
                    continue
                sid = str(cid)
                if salvage_ctx.parking_node_id and sid == salvage_ctx.parking_node_id:
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

        # 7) Rebuild structural subtree from Cartographer single-file map
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

        # 8) Update file node note header with unified Path/LINE COUNT/Source-SHA1
        file_line_count: int | None = None
        if not dry_run:
            file_line_count = _compute_line_count(source_path)

            # Split existing note into header (Path/LINE COUNT/Source-SHA1) and tail
            lines = note_text.splitlines() if note_text else []
            header_lines: list[str] = []
            tail_lines: list[str] = []
            in_header = True
            for line in lines:
                stripped = line.strip()
                if in_header and (
                    stripped.startswith("Path:")
                    or stripped.startswith("LINE COUNT:")
                    or stripped.startswith("Source-SHA1:")
                    or stripped == ""
                ):
                    header_lines.append(line)
                else:
                    in_header = False
                    tail_lines.append(line)

            try:
                # Use shared formatter from Cartographer to build canonical header
                header = cartographer.format_file_note(  # type: ignore[attr-defined]
                    source_path,
                    line_count=file_line_count,
                    sha1=file_sha1,
                )
            except Exception as e:  # noqa: BLE001
                # Fallback: preserve legacy hash-only behavior to avoid breaking refresh
                log_event(
                    "refresh_file_node_beacons (legacy): format_file_note failed "
                    f"({e}); falling back to legacy hash-only header update",
                    "BEACON",
                )
                new_line = f"Source-SHA1: {file_sha1}"
                if lines:
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
                else:
                    new_note = new_line
            else:
                # Append any non-header tail content exactly as-is, separated by a blank line.
                if tail_lines:
                    new_note = header + "\n\n" + "\n".join(tail_lines)
                else:
                    new_note = header

            try:
                update_req = NodeUpdateRequest(
                    name=file_node.get("name"),
                    note=new_note,
                    layoutMode=(file_node.get("data") or {}).get("layoutMode"),
                )
                await self.update_node(str(file_node_id), update_req)
            except Exception as e:  # noqa: BLE001
                raise NetworkError(f"Failed to update file node note: {e}") from e

            # Mark nodes-export cache dirty for this subtree
            try:
                self._mark_nodes_export_dirty([str(file_node_id)])
            except Exception:
                pass

        # 9) Restore NOTES from salvage context (beacon-based + path-based) and
        # optionally clean up any parking node that was created.
        restore_counts = await self._restore_notes_for_file(
            file_node_id=str(file_node_id),
            source_path=source_path,
            salvage_ctx=salvage_ctx,
            dry_run=dry_run,
        )

        notes_salvaged = restore_counts.get("notes_salvaged", 0)
        notes_orphaned = restore_counts.get("notes_orphaned", 0)
        total_notes_stashed_by_path = restore_counts.get("notes_stashed_by_path", 0)
        notes_path_reattached = restore_counts.get("notes_path_reattached", 0)
        notes_path_unmatched = restore_counts.get("notes_path_unmatched", 0)

        log_event(
            "refresh_file_node_beacons (legacy) complete for "
            f"{source_path} (deleted={structural_deleted}, created={structural_created}, "
            f"notes_salvaged={notes_salvaged}, notes_orphaned={notes_orphaned}, "
            f"notes_stashed_by_path={total_notes_stashed_by_path}, "
            f"notes_path_reattached={notes_path_reattached}, notes_path_unmatched={notes_path_unmatched})",
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
            "notes_identified_for_salvage": salvage_ctx.total_notes_to_salvage,
            "notes_salvaged": notes_salvaged,
            "notes_orphaned": notes_orphaned,
            "notes_stashed_by_path": total_notes_stashed_by_path,
            "notes_path_reattached": notes_path_reattached,
            "notes_path_unmatched": notes_path_unmatched,
        }

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.refresh_file_node_beacons-wgth,
    #   role=WorkFlowyClientNexus.refresh_file_node_beacons,
    #   slice_labels=ra-notes,ra-notes-cartographer,ra-notes-salvage,ra-reconcile,f9-f12-handlers,
    #   kind=ast,
    # ]
    async def refresh_file_node_beacons(
        self,
        file_node_id: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Incremental, beacon-aware per-file refresh (F12).

        CURRENT BEHAVIOR (safe scaffold):
        - Builds a per-file NEXUS-style JSON (terrain/gem-like) shaped for
          workflowy_move_reconcile.reconcile_tree.
        - Runs reconcile_tree(...) in dry_run mode to compute a plan only
          (no Workflowy mutations).
        - Writes that JSON to temp/cartographer_file_refresh for inspection.
        - Then delegates the actual mutation to _refresh_file_node_beacons_legacy,
          so runtime behavior remains identical to the legacy full-rebuild path.

        This gives us:
        - A reproducible, file-scoped source_json for testing incremental
          reconciliation.
        - A dry-run plan from the same engine used by WEAVE.

        Once the incremental path is battle-tested, this function becomes the
        natural place to switch from legacy full-rebuild to true incremental
        reconcile_tree-based updates.
        """
        logger = _ClientLogger()
        log_event(
            f"refresh_file_node_beacons (incremental scaffold) start (file_node_id={file_node_id}, dry_run={dry_run})",
            "BEACON",
        )

        incremental_probe: dict[str, Any] | None = None
        resolved_from_descendant = False
        salvage_ctx: NotesSalvageContext | None = None

        # 0) Optional descendant→FILE resolution so callers may pass a beacon or AST
        # child UUID and still get a file-scoped incremental refresh, matching the
        # behavior of _refresh_file_node_beacons_legacy.
        try:
            raw_cache = await export_nodes_impl(
                self,
                node_id=None,
                use_cache=True,
                force_refresh=False,
            )
            all_nodes_cache = raw_cache.get("nodes", []) or []
            nodes_by_id_cache: dict[str, dict[str, Any]] = {
                str(n.get("id")): n for n in all_nodes_cache if n.get("id")
            }
        except Exception:
            nodes_by_id_cache = {}

        if nodes_by_id_cache and str(file_node_id) in nodes_by_id_cache:
            current_id: str | None = str(file_node_id)
            visited_ids: set[str] = set()
            resolved_file_node_id: str | None = None

            while current_id and current_id not in visited_ids:
                visited_ids.add(current_id)
                node = nodes_by_id_cache.get(current_id)
                if not node:
                    break

                n_note = node.get("note") or node.get("no") or ""
                if isinstance(n_note, str) and "Path:" in n_note:
                    # This is the enclosing FILE node created by Cartographer.
                    resolved_file_node_id = current_id
                    break

                parent_id_val = node.get("parent_id") or node.get("parentId")
                current_id = str(parent_id_val) if parent_id_val else None

            if resolved_file_node_id and resolved_file_node_id != file_node_id:
                log_event(
                    "refresh_file_node_beacons (incremental): resolved descendant UUID "
                    f"{file_node_id} → FILE node {resolved_file_node_id}",
                    "BEACON",
                )
                file_node_id = resolved_file_node_id
                resolved_from_descendant = True

        try:
            import importlib
            import json as json_module
            import copy
            import datetime


            # 1) Export the FILE subtree to discover current structure and Path
            try:
                raw = await export_nodes_impl(self, file_node_id)
            except Exception as e:  # noqa: BLE001
                raise NetworkError(f"export_nodes failed for {file_node_id} (incremental scaffold): {e}") from e

            flat_nodes = raw.get("nodes") or []
            if not flat_nodes:
                raise NetworkError(
                    f"File node {file_node_id} not found or has empty subtree (incremental scaffold)",
                )

            file_node: dict[str, Any] | None = None
            for n in flat_nodes:
                if str(n.get("id")) == str(file_node_id):
                    file_node = n
                    break

            if not file_node:
                raise NetworkError(
                    f"File node {file_node_id} not present in exported subtree (incremental scaffold)",
                )

            note_text = str(file_node.get("note") or "")
            source_path = parse_path_or_root_from_note(note_text)

            if not source_path:
                raise NetworkError(
                    f"File node {file_node_id} note is missing 'Path:'/'Root:' line; cannot build incremental map",
                )

            # NEW: directory-aware routing – if Path/Root points to a directory,
            # delegate to the FOLDER-level Cartographer sync instead of the
            # per-file incremental pipeline.
            if os.path.isdir(source_path):
                log_event(
                    "refresh_file_node_beacons: Path/Root points to directory; "
                    f"routing to refresh_folder_cartographer_sync for node {file_node_id}",
                    "BEACON",
                )
                folder_result = await self.refresh_folder_cartographer_sync(
                    folder_node_id=str(file_node_id),
                    dry_run=dry_run,
                )
                if isinstance(folder_result, dict):
                    folder_result.setdefault("routed_from", "refresh_file_node_beacons")
                return folder_result

            # Optional: collect NOTES salvage metadata (and in non-dry-run
            # mode, physically move beacon-scoped NOTES up under the FILE
            # node) *before* any reconcile_tree mutations.
            salvage_ctx = await self._collect_notes_salvage_context_for_file(
                flat_nodes=flat_nodes,
                file_node_id=str(file_node_id),
                source_path=source_path,
                dry_run=dry_run,
                mode="incremental",
            )

            # 2) Build an in-memory hierarchy for the existing Workflowy subtree
            hierarchical_tree = self._build_hierarchy(flat_nodes, True)
            ether_root: dict[str, Any] | None = None
            if hierarchical_tree and len(hierarchical_tree) == 1:
                ether_root = hierarchical_tree[0]
            else:
                for cand in hierarchical_tree or []:
                    if str(cand.get("id")) == str(file_node_id):
                        ether_root = cand
                        break
            if ether_root is None:
                # Fall back to a minimal shell with just the file node stats.
                ether_root = {
                    "id": str(file_node_id),
                    "name": file_node.get("name"),
                    "note": note_text,
                    "children": [],
                }

            # 3) Import Cartographer and build a fresh per-file map
            client_dir = os.path.dirname(os.path.abspath(__file__))
            wf_mcp_dir = os.path.dirname(client_dir)
            mcp_servers_dir = os.path.dirname(wf_mcp_dir)
            project_root = os.path.dirname(mcp_servers_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

            try:
                cartographer = importlib.import_module("nexus_map_codebase")
            except Exception as e:  # noqa: BLE001
                raise NetworkError(
                    f"Failed to import nexus_map_codebase in incremental scaffold: {e}",
                ) from e

            try:
                file_map = cartographer.map_codebase(source_path)  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001
                raise NetworkError(
                    f"Cartographer map_codebase failed for {source_path} (incremental scaffold): {e}",
                ) from e

            new_children = copy.deepcopy(file_map.get("children") or [])

            # 4) Seed IDs into the Cartographer tree by matching existing
            # Workflowy children by name via reconcile_trees.
            source_root = {
                "name": file_node.get("name") or "",
                "note": note_text,
                "children": new_children,
            }

            try:
                # Prefer the Cartographer-aware reconciliation when available.
                if hasattr(cartographer, "reconcile_trees_cartographer"):
                    cartographer.reconcile_trees_cartographer(  # type: ignore[attr-defined]
                        source_root,
                        ether_root,
                    )
                else:
                    cartographer.reconcile_trees(source_root, ether_root)  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001
                raise NetworkError(
                    f"reconcile_trees(source_root, ether_root) failed in incremental scaffold: {e}",
                ) from e

            # 5) Build a per-file source_json shaped for reconcile_tree
            original_ids_seen: set[str] = set()
            for n in flat_nodes:
                nid = n.get("id")
                if nid:
                    original_ids_seen.add(str(nid))

            # Build a parent→children index so we can preserve entire Notes[...] subtrees
            # (roots and all descendants) via explicitly_preserved_ids.
            children_by_parent: dict[str, list[dict[str, Any]]] = {}
            for n in flat_nodes:
                pid = n.get("parent_id") or n.get("parentId")
                if pid:
                    children_by_parent.setdefault(str(pid), []).append(n)

            explicitly_preserved_ids: set[str] = set()

            def _collect_notes_descendants(root_id: str) -> None:
                for child in children_by_parent.get(root_id, []) or []:
                    cid = child.get("id")
                    if not cid:
                        continue
                    cid_str = str(cid)
                    if cid_str in explicitly_preserved_ids:
                        continue
                    explicitly_preserved_ids.add(cid_str)
                    _collect_notes_descendants(cid_str)

            for n in flat_nodes:
                nid = n.get("id")
                if not nid:
                    continue
                name = str(n.get("name") or "")
                if _is_notes_name(name):
                    root_id = str(nid)
                    explicitly_preserved_ids.add(root_id)
                    _collect_notes_descendants(root_id)

            source_json: dict[str, Any] = {
                "export_root_id": str(file_node_id),
                "export_root_name": file_node.get("name"),
                "export_root_children_status": "complete",
                "nodes": source_root.get("children") or [],
                "original_ids_seen": sorted(original_ids_seen),
                "explicitly_preserved_ids": sorted(explicitly_preserved_ids),
            }

            # 6) Persist JSON to temp/cartographer_file_refresh for inspection
            source_json_path: str | None = None
            try:
                file_refresh_dir = os.path.join(project_root, "temp", "cartographer_file_refresh")
                os.makedirs(file_refresh_dir, exist_ok=True)
                short_id = str(file_node_id).replace("-", "")[:8]
                base_name = os.path.basename(source_path)
                timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
                source_json_path = os.path.join(
                    file_refresh_dir,
                    f"{timestamp}_file_refresh_{short_id}_{base_name}.json",
                )
                with open(source_json_path, "w", encoding="utf-8") as f:
                    json_module.dump(source_json, f, indent=2, ensure_ascii=False)
                _log_to_file_helper(
                    f"refresh_file_node_beacons incremental source_json written: {source_json_path}",
                    "reconcile",
                )
            except Exception as e:  # noqa: BLE001
                source_json_path = None
                _log_to_file_helper(
                    f"refresh_file_node_beacons incremental: failed to write source_json file: {e}",
                    "reconcile",
                )

            # 7) Call reconcile_tree to compute and optionally apply an incremental
            # plan for this FILE node. This uses the same reconciliation engine as
            # WEAVE but scoped to a single file.
            from ..models import NodeListRequest
            from .workflowy_move_reconcile import reconcile_tree as _reconcile_tree_for_f12

            stats: dict[str, Any] = {
                "api_calls": 0,
                "nodes_created": 0,
                "nodes_updated": 0,
                "nodes_deleted": 0,
                "nodes_moved": 0,
            }

            async def list_wrapper(parent_uuid: str) -> list[dict[str, Any]]:
                request = NodeListRequest(parentId=parent_uuid)
                nodes, _ = await self.list_nodes(request)
                stats["api_calls"] += 1
                return [n.model_dump() for n in nodes]

            async def create_wrapper(parent_uuid: str, data: dict) -> str:
                req = NodeCreateRequest(
                    name=data.get("name"),
                    parent_id=parent_uuid,
                    note=data.get("note"),
                    layoutMode=(data.get("data") or {}).get("layoutMode"),
                    position="bottom",
                )
                _log_to_file_helper(f"F12 CREATE: {data.get('name')}", "reconcile")
                node = await self.create_node(req, _internal_call=True)
                stats["api_calls"] += 1
                stats["nodes_created"] += 1
                return node.id

            async def update_wrapper(node_uuid: str, data: dict) -> None:
                req = NodeUpdateRequest(
                    name=data.get("name"),
                    note=data.get("note"),
                    layoutMode=(data.get("data") or {}).get("layoutMode"),
                )
                _log_to_file_helper(f"F12 UPDATE: {node_uuid}", "reconcile")
                await self.update_node(node_uuid, req)
                stats["api_calls"] += 1
                stats["nodes_updated"] += 1

            async def delete_wrapper(node_uuid: str) -> None:
                _log_to_file_helper(f"F12 DELETE: {node_uuid}", "reconcile")
                await self.delete_node(node_uuid)
                stats["api_calls"] += 1
                stats["nodes_deleted"] += 1

            async def move_wrapper(node_uuid: str, new_parent: str, position: str = "top") -> None:
                _log_to_file_helper(f"F12 MOVE: {node_uuid}", "reconcile")
                await self.move_node(node_uuid, new_parent, position)
                stats["api_calls"] += 1
                stats["nodes_moved"] += 1

            async def export_wrapper(node_uuid: str) -> dict:
                # Use the same bulk export helper as the rest of the client.
                _log_to_file_helper(f"F12 EXPORT: {node_uuid}", "reconcile")
                data = await export_nodes_impl(self, node_uuid)
                stats["api_calls"] += 1
                return data

            try:
                reconcile_result = await _reconcile_tree_for_f12(
                    source_json=source_json,
                    parent_uuid=str(file_node_id),
                    list_nodes=list_wrapper,
                    create_node=create_wrapper,
                    update_node=update_wrapper,
                    delete_node=delete_wrapper,
                    move_node=move_wrapper,
                    export_nodes=export_wrapper,
                    import_policy="strict",
                    dry_run=dry_run,
                    skip_delete_bulk_export_wait=True,
                    log_to_file_msg=lambda m: _log_to_file_helper(m, "reconcile"),
                )
            except Exception as e:  # noqa: BLE001
                _log_to_file_helper(
                    f"refresh_file_node_beacons incremental: reconcile_tree failed: {e}",
                    "reconcile",
                )
                raise

            incremental_probe = {
                "success": True,
                "source_json_file": source_json_path,
                "reconcile_result": reconcile_result,
                "file_node_id_effective": str(file_node_id),
                "source_path": source_path,
                "resolved_from_descendant": resolved_from_descendant,
                "stats": stats,
            }
            log_event(
                "refresh_file_node_beacons incremental scaffold complete "
                f"(file_node_id_effective={file_node_id}, source_json_file={source_json_path}, dry_run={dry_run})",
                "BEACON",
            )

        except Exception as e:  # noqa: BLE001
            import traceback

            tb = traceback.format_exc()
            # Incremental scaffold is strictly best-effort. Any error falls back
            # to the legacy implementation while still recording the failure in
            # the probe metadata so we can debug later.
            incremental_probe = {
                "success": False,
                "error": str(e),
                "file_node_id_effective": str(file_node_id),
                "resolved_from_descendant": resolved_from_descendant,
                "traceback": tb,
            }
            _log_to_file_helper(
                "refresh_file_node_beacons incremental scaffold failed (falling back to legacy): "
                f"{e}\n{tb}",
                "reconcile",
            )
            log_event(
                "refresh_file_node_beacons incremental scaffold failed; falling back to legacy "
                f"(file_node_id_effective={file_node_id}): {e}",
                "BEACON",
            )

        # 8) If incremental scaffold succeeded, return its result directly without
        # calling the legacy full-rebuild path. Legacy remains available as a
        # fallback when the incremental path fails.
        if incremental_probe is not None and incremental_probe.get("success"):
            result: dict[str, Any] = {
                "success": True,
                "file_node_id": incremental_probe.get("file_node_id_effective"),
                "source_path": incremental_probe.get("source_path"),
                "dry_run": dry_run,
                "reconcile_result": incremental_probe.get("reconcile_result"),
            }
            stats = incremental_probe.get("stats")
            if isinstance(stats, dict):
                result.update(
                    {
                        "nodes_created": stats.get("nodes_created"),
                        "nodes_updated": stats.get("nodes_updated"),
                        "nodes_deleted": stats.get("nodes_deleted"),
                        "nodes_moved": stats.get("nodes_moved"),
                        "api_calls": stats.get("api_calls"),
                    }
                )

            # Optional belt-and-suspenders NOTES restore pass on top of the
            # explicitly_preserved_ids guard used by reconcile_tree. This is
            # metadata-only for collection, but in the non-dry-run path we
            # allow it to actually move NOTES back under their intended parents
            # when structural alignment changes in unexpected ways.
            if not dry_run and salvage_ctx is not None:
                try:
                    effective_file_node_id = str(
                        incremental_probe.get("file_node_id_effective") or file_node_id
                    )
                    effective_source_path = str(
                        incremental_probe.get("source_path") or ""
                    )
                    restore_counts = await self._restore_notes_for_file(
                        file_node_id=effective_file_node_id,
                        source_path=effective_source_path,
                        salvage_ctx=salvage_ctx,
                        dry_run=dry_run,
                    )
                    result["notes_restore"] = restore_counts
                except Exception as e:  # noqa: BLE001
                    _log_to_file_helper(
                        "refresh_file_node_beacons incremental: _restore_notes_for_file "
                        f"failed: {e}",
                        "reconcile",
                    )

            result["_incremental_probe"] = incremental_probe
            if not dry_run:
                try:
                    self._mark_nodes_export_dirty(
                        [str(incremental_probe.get("file_node_id_effective") or file_node_id)]
                    )
                except Exception:
                    pass
            return result

        # 9) Incremental scaffold failed – delegate mutation to the existing
        # legacy implementation while preserving probe metadata for debugging.
        legacy_result = await self._refresh_file_node_beacons_legacy(
            file_node_id=file_node_id,
            dry_run=dry_run,
        )

        if isinstance(legacy_result, dict) and incremental_probe is not None:
            legacy_result.setdefault("_incremental_probe", incremental_probe)

        return legacy_result

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.update_beacon_from_node-enct,
    #   role=WorkFlowyClientNexus.update_beacon_from_node,
    #   slice_labels=f9-f12-handlers,
    #   kind=ast,
    # ]
    async def update_beacon_from_node(
        self,
        node_id: str,
        name: str,
        note: str,
    ) -> dict[str, Any]:
        """Update/create/delete a Cartographer beacon from a single Workflowy node.

        This is the F12-per-node counterpart to refresh_file_node_beacons. It:
        - Resolves the enclosing FILE node and its Path: header.
        - Classifies the node as Python/JS/TS/Markdown via file extension.
        - Delegates to nexus_map_codebase.update_beacon_from_node_* helpers
          to decide whether to update an existing beacon, create a new one
          from tags, or delete a stale beacon.
        - Returns a small JSON summary for the client.

        NOTE: This method does not mutate Workflowy directly; it only updates
        the underlying source files. The client should separately update the
        local /nodes-export cache name (e.g., via update_cached_node_name)
        based on the "base_name" returned by the helper.
        """
        logger = _ClientLogger()
        log_event(
            f"update_beacon_from_node start (node_id={node_id})",
            "BEACON",
        )

        # 1) Load /nodes-export snapshot and locate the target node.
        raw = await export_nodes_impl(self, node_id=None, use_cache=True, force_refresh=False)
        all_nodes = raw.get("nodes", []) or []
        nodes_by_id: dict[str, dict[str, Any]] = {
            str(n.get("id")): n for n in all_nodes if n.get("id")
        }
        wf_node = nodes_by_id.get(str(node_id))
        if not wf_node:
            raise NetworkError(f"update_beacon_from_node: node {node_id!r} not found in /nodes-export cache")

        # 2) Walk ancestors to find FILE node with Path:/Root: in note.
        current_id = str(node_id)
        visited: set[str] = set()
        file_node: dict[str, Any] | None = None

        while current_id and current_id not in visited:
            visited.add(current_id)
            n = nodes_by_id.get(current_id)
            if not n:
                break

            n_note = n.get("note") or n.get("no") or ""
            # Check if note has Path: or Root: as a line prefix (not just anywhere in note).
            # This prevents false matches on method/class nodes whose docstrings mention "Path:".
            if isinstance(n_note, str):
                for line in n_note.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("Path:") or stripped.startswith("Root:"):
                        file_node = n
                        break
            if file_node is not None:
                break

            parent_id_val = n.get("parent_id") or n.get("parentId")
            current_id = str(parent_id_val) if parent_id_val else None

        if not file_node:
            raise NetworkError(
                "update_beacon_from_node: could not find ancestor FILE node with Path:/Root: "
                f"for node {node_id!r} (visited_chain={list(visited)})",
            )

        file_note = file_node.get("note") or file_node.get("no") or ""
        source_path = parse_path_or_root_from_note(str(file_note))
        if not source_path:
            raise NetworkError(
                "update_beacon_from_node: ancestor FILE node missing Path:/Root: for "
                f"node {node_id!r}",
            )

        ext = os.path.splitext(source_path)[1].lower()

        # 3) Import nexus_map_codebase and delegate to language-specific helper.
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
            raise NetworkError(
                f"update_beacon_from_node: failed to import nexus_map_codebase: {e}",
            ) from e

        if ext == ".py":
            helper = getattr(cartographer, "update_beacon_from_node_python", None)
        elif ext in {".js", ".jsx", ".ts", ".tsx"}:
            helper = getattr(cartographer, "update_beacon_from_node_js_ts", None)
        elif ext in {".md", ".markdown"}:
            helper = getattr(cartographer, "update_beacon_from_node_markdown", None)
        elif ext == ".sql":
            helper = getattr(cartographer, "update_beacon_from_node_sql", None)
        elif ext in {".yml", ".yaml"}:
            helper = getattr(cartographer, "update_beacon_from_node_yaml", None)
        elif ext in {".sh", ".bash"}:
            helper = getattr(cartographer, "update_beacon_from_node_shell", None)
        else:
            helper = None

        if helper is None:
            return {
                "success": False,
                "reason": "unsupported_extension",
                "source_path": source_path,
                "node_id": node_id,
                "ext": ext,
            }

        log_event(
            f"update_beacon_from_node dispatch (node_id={node_id}, "
            f"source_path={source_path!r}, ext={ext}, name={name!r})",
            "BEACON",
        )

        try:
            result = helper(source_path, name, note)  # type: ignore[misc]
        except Exception as e:  # noqa: BLE001
            raise NetworkError(
                f"update_beacon_from_node: helper failed for {source_path!r}: {e}",
            ) from e

        if not isinstance(result, dict):
            result = {"raw_result": result}

        result.setdefault("success", True)
        result.setdefault("node_id", node_id)
        result.setdefault("source_path", source_path)

        log_event(
            "update_beacon_from_node helper_result "
            f"(op={result.get('operation')}, "
            f"lang={result.get('language')}, "
            f"beacon_id={result.get('beacon_id')!r}, "
            f"ast_qualname={result.get('ast_qualname')!r}, "
            f"tags={result.get('tags')})",
            "BEACON",
        )

        # 4) Update local /nodes-export cache name + note to reflect on-disk state.
        # This is critical so that subsequent operations (WEAVE, F12 file refresh)
        # see the updated beacon metadata without requiring a full /nodes-export
        # refresh from the API.
        base_name_from_helper = result.get("base_name")
        if base_name_from_helper and isinstance(base_name_from_helper, str):
            # For AST/beacon nodes, the helper stripped trailing tags.
            # Update the cached node name to match.
            try:
                updated = await self.update_cached_node_name(
                    str(node_id),
                    base_name_from_helper,
                )
                result["cache_name_updated"] = updated
            except Exception as e:  # noqa: BLE001
                log_event(
                    f"update_beacon_from_node: failed to update cached name for {node_id!r}: {e}",
                    "BEACON",
                )
                result["cache_name_updated"] = False

        # 5) Optionally update the cached note with the updated beacon metadata.
        # This is more involved, so for now we only do it when the helper
        # explicitly returned beacon metadata in the result.
        operation = result.get("operation")
        if operation in {"updated_beacon", "created_beacon"}:
            beacon_id_from_helper = result.get("beacon_id")
            role_from_helper = result.get("role") or ""
            slice_labels_from_helper = result.get("slice_labels") or ""
            comment_from_helper = result.get("comment") or ""

            if beacon_id_from_helper:
                # Rebuild the BEACON (...) block that should be in the note.
                # Match the language-specific header format used by apply_*_beacons.
                lang = result.get("language") or "python"
                if lang == "python":
                    beacon_header = "BEACON (AST)"
                elif lang == "js_ts":
                    beacon_header = "BEACON (JS/TS AST)"
                elif lang == "markdown":
                    beacon_header = "BEACON (MD AST)"
                elif lang == "sql":
                    beacon_header = "BEACON (SQL SPAN)"
                elif lang == "shell":
                    beacon_header = "BEACON (SH SPAN)"
                elif lang == "yaml":
                    beacon_header = "BEACON (YAML SPAN)"
                else:
                    beacon_header = "BEACON (AST)"

                # Preserve "kind: span" if already present in the original cached note.
                # Otherwise default to "kind: ast" (for AST-backed beacons).
                original_kind = "ast"
                original_note = str(wf_node.get("note") or wf_node.get("no") or "")
                if isinstance(original_note, str):
                    for line in original_note.splitlines():
                        stripped = line.strip()
                        if stripped.startswith("kind:"):
                            # Extract the kind value (e.g., "span" or "ast")
                            kind_val = stripped.split(":", 1)[1].strip()
                            if kind_val:
                                original_kind = kind_val
                            break

                beacon_meta_lines = [
                    beacon_header,
                    f"id: {beacon_id_from_helper}",
                    f"role: {role_from_helper}",
                    f"slice_labels: {slice_labels_from_helper}",
                    f"kind: {original_kind}",
                ]
                if comment_from_helper:
                    beacon_meta_lines.append(f"comment: {comment_from_helper}")

                # For AST nodes, the note structure is:
                #   AST_QUALNAME: ...
                #   ---
                #   [optional docstring or body]
                #
                #   BEACON (...)
                #   id: ...
                #   role: ...
                #   slice_labels: ...
                #   kind: ast
                #   comment: ...
                #
                # We rebuild the note by:
                # - Keeping the AST_QUALNAME / MD_PATH header and any existing body.
                # - Replacing or appending the BEACON block.
                original_note = str(wf_node.get("note") or wf_node.get("no") or "")
                note_lines = original_note.splitlines()

                # Find the existing BEACON block (if any) and remove it.
                beacon_start_idx: int | None = None
                beacon_end_idx: int | None = None
                for idx, line in enumerate(note_lines):
                    stripped = line.strip()
                    if stripped.startswith("BEACON ("):
                        beacon_start_idx = idx
                    elif beacon_start_idx is not None and stripped.startswith("kind:"):
                        # End of beacon block is the line after "kind: ..."
                        beacon_end_idx = idx
                        break

                # Rebuild note: keep header/body, replace beacon block.
                if beacon_start_idx is not None and beacon_end_idx is not None:
                    # Strip away old beacon block.
                    new_note_lines = note_lines[:beacon_start_idx] + note_lines[beacon_end_idx + 1 :]
                else:
                    new_note_lines = note_lines[:]

                # Append new beacon block.
                if new_note_lines and new_note_lines[-1].strip():
                    new_note_lines.append("")  # Blank line separator
                new_note_lines.extend(beacon_meta_lines)

                new_note = "\n".join(new_note_lines)

                # Update the cached note via _apply_update_to_nodes_export_cache.
                # This is a bit indirect, but it's the existing mechanism.
                try:
                    # Build a minimal node update dict.
                    from ..models import WorkFlowyNode

                    updated_node = WorkFlowyNode(
                        id=str(node_id),
                        name=base_name_from_helper or name,
                        note=new_note,
                    )
                    self._apply_update_to_nodes_export_cache(updated_node)
                    result["cache_note_updated"] = True
                except Exception as e:  # noqa: BLE001
                    log_event(
                        f"update_beacon_from_node: failed to update cached note for {node_id!r}: {e}",
                        "BEACON",
                    )
                    result["cache_note_updated"] = False

        # Emit a concise summary log for this beacon operation.
        try:
            op = result.get("operation")
            lang = result.get("language") or "unknown"
            beacon_id_from_helper = result.get("beacon_id")
            base_name_logged = result.get("base_name") or name
            log_event(
                f"update_beacon_from_node done (op={op}, lang={lang}, node_id={node_id}, "
                f"name={base_name_logged!r}, beacon_id={beacon_id_from_helper!r})",
                "BEACON",
            )
        except Exception:
            # Logging must never interfere with tool behavior.
            pass

        # 6) Auto-refresh the enclosing FILE node so the Ether rendering updates immediately.
        # This mirrors the manual F12-on-FILE workflow but happens automatically after
        # the beacon update completes, giving immediate visual feedback in Workflowy.
        file_node_id_for_refresh = str(file_node.get("id"))
        try:
            # Safety check: ensure source_path points to a file, not a directory.
            if os.path.isfile(source_path):
                log_event(
                    f"update_beacon_from_node: auto-refreshing FILE node {file_node_id_for_refresh} "
                    f"(path={source_path!r})",
                    "BEACON",
                )
                refresh_result = await self.refresh_file_node_beacons(
                    file_node_id=file_node_id_for_refresh,
                    dry_run=False,
                )
                result["auto_refresh"] = {
                    "success": True,
                    "file_node_id": file_node_id_for_refresh,
                    "stats": {
                        "nodes_created": refresh_result.get("nodes_created"),
                        "nodes_updated": refresh_result.get("nodes_updated"),
                        "nodes_deleted": refresh_result.get("nodes_deleted"),
                        "nodes_moved": refresh_result.get("nodes_moved"),
                    },
                }
            else:
                # source_path is not a file (possibly a directory or missing);
                # skip auto-refresh without error.
                log_event(
                    f"update_beacon_from_node: skipping auto-refresh (source_path={source_path!r} "
                    f"is not a file)",
                    "BEACON",
                )
                result["auto_refresh"] = {
                    "success": False,
                    "reason": "source_path_not_a_file",
                }
        except Exception as e:  # noqa: BLE001
            # Auto-refresh is best-effort; failures are logged but do not break
            # the beacon update itself.
            log_event(
                f"update_beacon_from_node: auto-refresh failed for FILE {file_node_id_for_refresh}: {e}",
                "BEACON",
            )
            result["auto_refresh"] = {
                "success": False,
                "error": str(e),
            }

        return result

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus.refresh_folder_cartographer_sync-62ih,
    #   role=WorkFlowyClientNexus.refresh_folder_cartographer_sync,
    #   slice_labels=ra-notes,ra-notes-salvage,ra-notes-cartographer,f9-f12-handlers,
    #   kind=ast,
    # ]
    async def refresh_folder_cartographer_sync(
        self,
        folder_node_id: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Multi-file Cartographer sync for a FOLDER subtree.

        Behavior:
        - Recursively finds all descendant FILE nodes (Cartographer-mapped,
          i.e. nodes whose note contains a "Path:" line and whose name does
          not start with the folder emoji) and refreshes each via
          refresh_file_node_beacons.
        - Tracks which Paths are present on disk vs missing.
        - If the folder node itself has a Path: line that points to an
          existing directory, performs a second pass over the filesystem
          using the Cartographer whitelist and creates FILE nodes in
          Workflowy for any on-disk files not represented in the subtree,
          creating intermediate FOLDER nodes as needed.
        - NEW (Jan 2026): stashes immediate Notes[...] subtrees under the
          folder root keyed by PATH before mutating, then reattaches them
          under the FILE/FOLDER node whose note contains the same Path.

        This is effectively a "direct Cartographer sync" for the subtree
        rooted at folder_node_id, without going through GEM/JEWEL/WEAVE.
        """
        logger = _ClientLogger()
        log_event(
            f"refresh_folder_cartographer_sync start (folder_node_id={folder_node_id}, dry_run={dry_run})",
            "BEACON",
        )

        # 1) Export subtree under the folder root
        try:
            raw = await export_nodes_impl(self, folder_node_id)
        except Exception as e:  # noqa: BLE001
            raise NetworkError(f"export_nodes failed for {folder_node_id}: {e}") from e

        flat_nodes = raw.get("nodes") or []
        if not flat_nodes:
            return {
                "success": False,
                "error": f"Folder node {folder_node_id} not found or has empty subtree",
            }

        # Index nodes and parents
        node_by_id: dict[str, dict[str, Any]] = {}
        parent_of: dict[str, str] = {}
        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            s_nid = str(nid)
            node_by_id[s_nid] = n
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                parent_of[s_nid] = str(pid)

        # Children index for immediate child lookups (Notes stashing)
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                children_by_parent.setdefault(str(pid), []).append(n)

        root_node = node_by_id.get(str(folder_node_id))
        note_text_root = str(root_node.get("note") or "") if root_node else ""

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.refresh_folder_cartographer_sync._parse_path_from_note-cm7o,
        #   role=WorkFlowyClientNexus.refresh_folder_cartographer_sync._parse_path_from_note,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _parse_path_from_note(note_str: str) -> str | None:
            for line in note_str.splitlines():
                stripped = line.strip()
                # Accept both standard FILE metadata ("Path:") and the
                # Cartographer root header ("Root:"), treating them
                # equivalently for purposes of locating the filesystem
                # path associated with a node.
                if stripped.startswith("Path:") or stripped.startswith("Root:"):
                    val = stripped.split(":", 1)[1].strip()
                    return val or None
            return None

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.refresh_folder_cartographer_sync._is_folder_node-6j3u,
        #   role=WorkFlowyClientNexus.refresh_folder_cartographer_sync._is_folder_node,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _is_folder_node(node: dict[str, Any]) -> bool:
            name = str(node.get("name") or "").strip()
            return name.startswith("📂")

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.refresh_folder_cartographer_sync._canonical_path-z2s0,
        #   role=WorkFlowyClientNexus.refresh_folder_cartographer_sync._canonical_path,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _canonical_path(path: str) -> str:
            return os.path.normcase(os.path.normpath(path))


        # Root Path: (may be None or a directory path)
        root_path = _parse_path_from_note(note_text_root) if note_text_root else None
        if root_path and not os.path.isdir(root_path):
            log_event(
                f"refresh_folder_cartographer_sync: root Path=\"{root_path}\" is not a directory; ignoring disk sync",
                "BEACON",
            )
            root_path = None

        # 1a) Stash immediate Notes[...] subtrees keyed by PATH.
        #
        # stashed_notes_by_path maps canonical Path (or None) → list of Notes node IDs.
        # For folder-level sync we physically move Notes under the folder root
        # so they survive any per-file mutations, then later reattach by PATH.
        stashed_notes_by_path: dict[str | None, list[str]] = {}
        notes_moved_to_root = 0

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus.refresh_folder_cartographer_sync._stash_notes_node-kj4d,
        #   role=WorkFlowyClientNexus.refresh_folder_cartographer_sync._stash_notes_node,
        #   slice_labels=ra-notes,ra-notes-salvage,ra-notes-cartographer,
        #   kind=ast,
        # ]
        def _stash_notes_node(notes_node_id: str, owner_path: str | None) -> None:
            key = _canonical_path(owner_path) if owner_path else None
            stashed_notes_by_path.setdefault(key, []).append(notes_node_id)

        # Stash Notes directly under the folder root (they conceptually belong
        # to the folder-level Path, if any).
        if root_node is not None:
            root_children = children_by_parent.get(str(folder_node_id), [])
            for child in root_children:
                cid = child.get("id")
                if not cid:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname):
                    _stash_notes_node(str(cid), root_path)
                    # Already under the folder root; no move needed.

        # 2) Find descendant FILE nodes and refresh them via refresh_file_node_beacons
        present_files: set[str] = set()
        present_paths_raw: dict[str, str] = {}
        missing_paths: list[str] = []
        refreshed_count = 0
        refresh_errors: list[str] = []
        files_deleted = 0

        # Collect file nodes first so we can stash Notes before any per-file refresh.
        file_nodes_info: list[tuple[str, str, bool]] = []  # (node_id, source_path, exists_on_disk)

        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            s_nid = str(nid)
            note = n.get("note") or n.get("no") or ""
            if not isinstance(note, str) or "Path:" not in note:
                continue
            # Skip folder nodes; we only treat files here
            if _is_folder_node(n):
                continue

            source_path = _parse_path_from_note(str(note))
            if not source_path:
                continue

            canonical = _canonical_path(source_path)
            exists = os.path.isfile(source_path)
            if exists:
                present_files.add(canonical)
                # Prefer first-seen raw path for reporting
                present_paths_raw.setdefault(canonical, source_path)
                log_event(
                    "refresh_folder_cartographer_sync: FILE present on disk "
                    f"node_id={s_nid} Path=\"{source_path}\" canonical=\"{canonical}\"",
                    "BEACON",
                )
            else:
                missing_paths.append(source_path)
                log_event(
                    "refresh_folder_cartographer_sync: FILE missing on disk "
                    f"node_id={s_nid} Path=\"{source_path}\" canonical=\"{canonical}\"",
                    "BEACON",
                )

            # Record this FILE node for later refresh/deletion
            file_nodes_info.append((s_nid, source_path, exists))

            # Stash immediate Notes[...] children for this FILE node by its Path.
            file_children = children_by_parent.get(s_nid, [])
            for child in file_children:
                cid = child.get("id")
                if not cid:
                    continue
                cname = str(child.get("name") or "")
                if not _is_notes_name(cname):
                    continue
                _stash_notes_node(str(cid), source_path)
                # Physically move under the folder root so that these Notes are
                # decoupled from any structural changes to this FILE node.
                parent_for_child = parent_of.get(str(cid))
                if (
                    not dry_run
                    and parent_for_child
                    and str(parent_for_child) != str(folder_node_id)
                ):
                    try:
                        await self.move_node(str(cid), str(folder_node_id), "bottom")
                        notes_moved_to_root += 1
                    except Exception as e:  # noqa: BLE001
                        log_event(
                            "refresh_folder_cartographer_sync: failed to move Notes node "
                            f"{cid} under folder root {folder_node_id}: {e}",
                            "BEACON",
                        )

        # Now perform per-file refresh only when the file exists on disk
        for s_nid, source_path, exists in file_nodes_info:
            if not exists:
                continue

            try:
                log_event(
                    "refresh_folder_cartographer_sync: per-file refresh start "
                    f"node_id={s_nid} Path=\"{source_path}\" dry_run={dry_run}",
                    "BEACON",
                )
                result = await self.refresh_file_node_beacons(
                    file_node_id=s_nid,
                    dry_run=dry_run,
                )
                if isinstance(result, dict) and result.get("success"):
                    refreshed_count += 1
                    log_event(
                        "refresh_folder_cartographer_sync: per-file refresh success "
                        f"node_id={s_nid} Path=\"{source_path}\" stats="
                        f"nodes_created={result.get('nodes_created')} "
                        f"nodes_updated={result.get('nodes_updated')} "
                        f"nodes_deleted={result.get('nodes_deleted')} "
                        f"nodes_moved={result.get('nodes_moved')}",
                        "BEACON",
                    )
                else:
                    err_msg = None
                    if isinstance(result, dict):
                        err_msg = result.get("error") or result.get("message")
                    msg = (
                        "refresh_folder_cartographer_sync: per-file refresh error "
                        f"node_id={s_nid} Path=\"{source_path}\": "
                        f"{err_msg or 'unknown error from refresh_file_node_beacons'}"
                    )
                    log_event(msg, "BEACON")
                    refresh_errors.append(msg)
            except Exception as e:  # noqa: BLE001
                msg = (
                    "refresh_folder_cartographer_sync: exception during per-file refresh "
                    f"node_id={s_nid} Path=\"{source_path}\": {e}"
                )
                log_event(msg, "BEACON")
                refresh_errors.append(msg)

        # 2b) Delete FILE nodes whose source file no longer exists on disk
        if not dry_run:
            missing_count = sum(1 for _nid, _path, _exists in file_nodes_info if not _exists)
            log_event(
                "refresh_folder_cartographer_sync: starting deletion pass for "
                f"{missing_count} missing FILE nodes (folder_node_id={folder_node_id})",
                "BEACON",
            )
            for s_nid, source_path, exists in file_nodes_info:
                if exists:
                    continue
                try:
                    deleted_ok = await self.delete_node(s_nid)
                    if deleted_ok:
                        files_deleted += 1
                        log_event(
                            "refresh_folder_cartographer_sync: deleted FILE node "
                            f"node_id={s_nid} Path=\"{source_path}\"", "BEACON",
                        )
                    else:
                        msg = (
                            "refresh_folder_cartographer_sync: delete_node returned False for FILE "
                            f"node_id={s_nid} Path=\"{source_path}\""
                        )
                        log_event(msg, "BEACON")
                        refresh_errors.append(msg)
                except Exception as e:  # noqa: BLE001
                    msg = (
                        "refresh_folder_cartographer_sync: failed to delete FILE node "
                        f"{s_nid} for missing Path=\"{source_path}\": {e}"
                    )
                    log_event(msg, "BEACON")
                    refresh_errors.append(msg)

        # 3) Optional second phase: filesystem scan + creation of missing FILE nodes
        disk_scan_performed = False
        disk_files_norm: dict[str, str] = {}
        only_on_disk_keys: set[str] = set()
        new_files_created = 0
        new_folders_created = 0

        cartographer = None
        project_root: str | None = None

        if not dry_run and root_path:
            # Determine project_root and import nexus_map_codebase
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
                log_event(
                    f"refresh_folder_cartographer_sync: failed to import nexus_map_codebase: {e}",
                    "BEACON",
                )
                cartographer = None

            # Load Cartographer whitelist (optional)
            include_exts: set[str] | None = None
            if project_root is not None:
                try:
                    wl_path = os.path.join(project_root, "Components", "cartographer_whitelist.txt")
                    if os.path.isfile(wl_path):
                        exts: set[str] = set()
                        with open(wl_path, "r", encoding="utf-8") as wf:
                            for line in wf:
                                line = line.strip()
                                if not line or line.startswith("#"):
                                    continue
                                if not line.startswith("."):
                                    line = "." + line
                                exts.add(line.lower())
                        if exts:
                            include_exts = exts
                except Exception as e:  # noqa: BLE001
                    log_event(
                        f"refresh_folder_cartographer_sync: failed to load Cartographer whitelist: {e}",
                        "BEACON",
                    )
                    include_exts = None

            # Load .nexusignore patterns (optional, rooted at the folder Path)
            ignore_patterns: list[str] | None = None
            if root_path:
                try:
                    ignore_path = os.path.join(root_path, ".nexusignore")
                    if os.path.isfile(ignore_path):
                        patterns: list[str] = []
                        invalid: list[str] = []
                        with open(ignore_path, "r", encoding="utf-8") as ig:
                            for line in ig:
                                raw = line.rstrip("\n\r")
                                stripped = raw.strip()
                                if not stripped or stripped.startswith("#"):
                                    continue
                                if "/" in stripped or "\\" in stripped:
                                    invalid.append(stripped)
                                    continue
                                patterns.append(stripped)
                        if patterns:
                            ignore_patterns = patterns
                            log_event(
                                "refresh_folder_cartographer_sync: using .nexusignore at "
                                f"{ignore_path!r} with patterns={patterns!r}",
                                "BEACON",
                            )
                        if invalid:
                            log_event(
                                "refresh_folder_cartographer_sync: ignoring .nexusignore patterns with "
                                f"path separators: {invalid!r}",
                                "BEACON",
                            )
                except Exception as e:  # noqa: BLE001
                    log_event(
                        f"refresh_folder_cartographer_sync: failed to load .nexusignore under {root_path}: {e}",
                        "BEACON",
                    )
                    ignore_patterns = None

            def _is_ignored_name(name: str) -> bool:
                if not ignore_patterns:
                    return False
                # Simple fnmatch-style globbing over basenames (no path semantics).
                import fnmatch

                for pat in ignore_patterns:
                    if fnmatch.fnmatch(name, pat):
                        return True
                return False

            # Walk filesystem under root_path and collect files matching whitelist and .nexusignore
            try:
                for dirpath, dirnames, filenames in os.walk(root_path):
                    # Prune ignored subdirectories so os.walk does not descend into them.
                    if ignore_patterns:
                        dirnames[:] = [d for d in dirnames if not _is_ignored_name(d)]

                    for fname in filenames:
                        if fname == ".nexusignore":
                            continue
                        if _is_ignored_name(fname):
                            continue
                        _base, ext = os.path.splitext(fname)
                        ext = ext.lower()
                        if include_exts and ext not in include_exts:
                            continue
                        full_path = os.path.join(dirpath, fname)
                        key = _canonical_path(full_path)
                        disk_files_norm[key] = full_path
                disk_scan_performed = True
            except Exception as e:  # noqa: BLE001
                log_event(
                    f"refresh_folder_cartographer_sync: os.walk failed under {root_path}: {e}",
                    "BEACON",
                )

            if disk_scan_performed and cartographer is not None:
                # Compute set difference: files on disk that are not represented as FILE nodes
                disk_keys = set(disk_files_norm.keys())
                only_on_disk_keys = disk_keys - present_files

                # Build mapping from relative folder paths to Workflowy folder node IDs
                folder_node_by_relpath: dict[str, str] = {"": str(folder_node_id)}

                def _compute_folder_relpath(nid: str) -> str | None:
                    segments: list[str] = []
                    current = nid
                    visited_ids: set[str] = set()
                    while current and current not in visited_ids:
                        visited_ids.add(current)
                        if current == str(folder_node_id):
                            break
                        node_local = node_by_id.get(current)
                        if not node_local:
                            return None
                        if _is_folder_node(node_local):
                            raw_name = str(node_local.get("name") or "")
                            txt = raw_name
                            # Strip leading emoji/bullets
                            while txt and not txt[0].isalnum():
                                txt = txt[1:]
                            base = txt.strip() or raw_name.strip()
                            # NEW: strip trailing #tags (e.g. "📂 folder #tag1 #tag2")
                            tokens = base.split()
                            while tokens and str(tokens[-1]).startswith("#"):
                                tokens.pop()
                            folder_name = " ".join(tokens) if tokens else base
                            segments.append(folder_name)
                        parent_id_local = parent_of.get(current)
                        if not parent_id_local:
                            return None
                        current = parent_id_local
                    segments.reverse()
                    if not segments:
                        return ""
                    return os.path.join(*segments)

                for nid, node_local in node_by_id.items():
                    if not _is_folder_node(node_local):
                        continue
                    rel = _compute_folder_relpath(nid)
                    if rel is not None and rel not in folder_node_by_relpath:
                        folder_node_by_relpath[rel] = nid

                async def _create_subtree(parent_uuid: str, node: dict[str, Any]) -> None:
                    nonlocal new_files_created
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

                    for child in node.get("children") or []:
                        await _create_subtree(wf_node.id, child)

                # Create missing FILE nodes and any required intermediate FOLDER nodes
                for key in sorted(only_on_disk_keys):
                    file_path = disk_files_norm.get(key)
                    if not file_path:
                        continue
                    try:
                        rel_path = os.path.relpath(file_path, root_path)
                    except ValueError:
                        # On Windows, relpath may fail if drives differ; skip in that case
                        continue
                    parts = [p for p in rel_path.split(os.sep) if p]
                    if not parts:
                        continue
                    dir_parts = parts[:-1]

                    parent_uuid = str(folder_node_id)
                    current_rel = ""
                    for seg in dir_parts:
                        next_rel = seg if not current_rel else os.path.join(current_rel, seg)
                        folder_uuid = folder_node_by_relpath.get(next_rel)
                        if not folder_uuid:
                            folder_path = os.path.join(root_path, next_rel)
                            try:
                                folder_note = cartographer.format_file_note(  # type: ignore[attr-defined]
                                    folder_path,
                                    line_count=None,
                                    sha1=None,
                                )
                            except Exception:
                                folder_note = f"Path: {folder_path}"
                            req = NodeCreateRequest(
                                name=f"📂 {seg}",
                                parent_id=parent_uuid,
                                note=folder_note,
                                layoutMode=None,
                                position="bottom",
                            )
                            created_folder = await self.create_node(req, _internal_call=True)
                            folder_uuid = created_folder.id
                            folder_node_by_relpath[next_rel] = folder_uuid
                            new_folders_created += 1
                        parent_uuid = folder_uuid
                        current_rel = next_rel

                    # Now create the FILE subtree under parent_uuid using Cartographer
                    try:
                        file_map = cartographer.map_codebase(file_path)  # type: ignore[attr-defined]
                    except Exception as e:  # noqa: BLE001
                        log_event(
                            f"refresh_folder_cartographer_sync: map_codebase failed for {file_path}: {e}",
                            "BEACON",
                        )
                        continue

                    await _create_subtree(parent_uuid, file_map)
                    new_files_created += 1

                # Mark nodes-export cache dirty for the folder subtree
                try:
                    self._mark_nodes_export_dirty([str(folder_node_id)])
                except Exception:
                    pass

        # 4) Reattach stashed Notes[...] subtrees by PATH (if any)
        notes_reattached = 0
        notes_unmatched = 0

        if stashed_notes_by_path:
            # Re-export the refreshed subtree to discover current Path → node_id mapping.
            refreshed_nodes: list[dict[str, Any]] = []
            try:
                refreshed_raw = await export_nodes_impl(self, folder_node_id)
                refreshed_nodes = refreshed_raw.get("nodes") or []
            except Exception as e:  # noqa: BLE001
                log_event(
                    "refresh_folder_cartographer_sync: re-export for Notes reattach "
                    f"failed: {e}",
                    "BEACON",
                )
                refreshed_nodes = []

            path_to_node_id: dict[str, str] = {}
            for n in refreshed_nodes or []:
                nid = n.get("id")
                if not nid:
                    continue
                note_val = n.get("note") or n.get("no") or ""
                if not isinstance(note_val, str):
                    continue
                node_path = _parse_path_from_note(str(note_val))
                if not node_path:
                    continue
                key = _canonical_path(node_path)
                # Last writer wins; Paths should be unique per FILE/FOLDER.
                path_to_node_id[key] = str(nid)

            total_stashed = sum(len(v) for v in stashed_notes_by_path.values())

            if dry_run:
                # In dry-run mode, just compute how many would be reattached vs left
                for key, ids_list in stashed_notes_by_path.items():
                    if key is not None and key in path_to_node_id:
                        notes_reattached += len(ids_list)
                    else:
                        notes_unmatched += len(ids_list)
            else:
                for key, ids_list in stashed_notes_by_path.items():
                    if key is not None and key in path_to_node_id:
                        target_parent_id = path_to_node_id[key]
                        for nid in ids_list:
                            try:
                                await self.move_node(nid, target_parent_id, "bottom")
                                notes_reattached += 1
                            except Exception as e:  # noqa: BLE001
                                log_event(
                                    "refresh_folder_cartographer_sync: failed to reattach Notes "
                                    f"node {nid} under {target_parent_id}: {e}",
                                    "BEACON",
                                )
                    else:
                        # No matching PATH in the refreshed subtree; leave Notes under
                        # the folder root (their stashed location) and count them as
                        # unmatched.
                        notes_unmatched += len(ids_list)
        else:
            total_stashed = 0

        log_event(
            "refresh_folder_cartographer_sync complete for "
            f"{folder_node_id} (refreshed_files={refreshed_count}, new_files={new_files_created}, "
            f"new_folders={new_folders_created}, files_deleted={files_deleted}, "
            f"notes_stashed={total_stashed}, notes_reattached={notes_reattached}, "
            f"notes_unmatched={notes_unmatched})",
            "BEACON",
        )

        # Persist the updated /nodes-export cache snapshot when structural changes
        # have occurred so that future MCP restarts warm-start from a state that
        # matches the current Workflowy tree (including new FILE/FOLDER nodes and
        # removed FILE nodes).
        if not dry_run and (new_files_created or new_folders_created or files_deleted):
            try:
                await self.save_nodes_export_cache()
            except Exception as e:  # noqa: BLE001
                log_event(
                    "refresh_folder_cartographer_sync: save_nodes_export_cache failed after structural changes: "
                    f"{e}",
                    "BEACON",
                )

        return {
            "success": True,
            "folder_node_id": folder_node_id,
            "root_path": root_path,
            "dry_run": dry_run,
            "refreshed_file_nodes": refreshed_count,
            "refresh_errors": refresh_errors,
            "present_paths": sorted(present_paths_raw.values()),
            "missing_paths": missing_paths,
            "disk_scan_performed": disk_scan_performed,
            "disk_root": root_path if disk_scan_performed else None,
            "disk_files_considered": len(disk_files_norm) if disk_scan_performed else 0,
            "disk_files_only_on_disk": len(only_on_disk_keys) if disk_scan_performed else 0,
            "new_files_created": new_files_created,
            "new_folders_created": new_folders_created,
            "files_deleted": files_deleted,
            "notes_stashed": total_stashed,
            "notes_moved_to_root": notes_moved_to_root,
            "notes_reattached": notes_reattached,
            "notes_unmatched": notes_unmatched,
        }

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus._collect_notes_salvage_context_for_file-u655,
    #   role=WorkFlowyClientNexus._collect_notes_salvage_context_for_file,
    #   slice_labels=ra-notes,ra-notes-salvage,ra-notes-cartographer,
    #   kind=ast,
    # ]
    async def _collect_notes_salvage_context_for_file(
        self,
        *,
        flat_nodes: list[dict[str, Any]],
        file_node_id: str,
        source_path: str,
        dry_run: bool,
        mode: str = "legacy",
    ) -> NotesSalvageContext:
        """Collect NOTES salvage metadata for a single Cartographer FILE node.

        In "legacy" mode this mirrors the prior behavior of
        _refresh_file_node_beacons_legacy by:
        - Identifying beacon-scoped Notes[...] subtrees and, when not dry_run,
          moving them under a temporary 🅿️ Notes (parking) node.
        - Recording immediate FILE-level Notes[...] roots keyed by canonical
          Path so they can be reattached after structural changes.

        In "incremental" mode we still avoid a parking node, but when not
        dry_run we *do* physically move Notes[...] subtrees up under the FILE
        node before reconcile_tree runs (both beacon-scoped and generic
        non-beacon Notes). This fully decouples NOTES from structural
        mutations while still relying on explicitly_preserved_ids as the
        primary deletion guard.
        """

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus._collect_notes_salvage_context_for_file._canonical_path-uhh8,
        #   role=WorkFlowyClientNexus._collect_notes_salvage_context_for_file._canonical_path,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _canonical_path(path: str) -> str:
            return os.path.normcase(os.path.normpath(path))

        ctx = NotesSalvageContext()

        # Build parent→children and id→node indexes once for this snapshot.
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        nodes_by_id: dict[str, dict[str, Any]] = {}
        for n in flat_nodes:
            nid = n.get("id")
            if not nid:
                continue
            s_nid = str(nid)
            nodes_by_id[s_nid] = n
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                children_by_parent.setdefault(str(pid), []).append(n)

        # 1) FILE-level Notes[...] roots keyed by canonical Path.
        file_children = children_by_parent.get(str(file_node_id), [])
        file_path_key = _canonical_path(source_path) if source_path else None
        for child in file_children:
            cid = child.get("id")
            if not cid:
                continue
            cname = str(child.get("name") or "")
            if _is_notes_name(cname):
                ctx.stashed_notes_by_path.setdefault(file_path_key, []).append(str(cid))

        # 2) Beacon-scoped Notes[...] subtrees.
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
                    ctx.saved_notes_by_beacon.setdefault(beacon_id_val, []).append(str(cid))
                    ctx.total_notes_to_salvage += 1

        # 3) Generic Notes[...] under non-FILE, non-beacon parents. These would
        # otherwise block deletion of their structural parents during
        # incremental reconcile.
        salvaged_ids: set[str] = set()
        for ids_list in ctx.saved_notes_by_beacon.values():
            salvaged_ids.update(ids_list)

        for parent_id, children in children_by_parent.items():
            if parent_id == str(file_node_id):
                continue

            parent_node = nodes_by_id.get(parent_id) or {}
            parent_note = parent_node.get("note") or ""
            # Skip parents that are themselves beacon nodes – their Notes are
            # already tracked in saved_notes_by_beacon.
            if "BEACON (" in str(parent_note):
                continue

            for child in children:
                cid = child.get("id")
                if not cid:
                    continue
                cid_str = str(cid)
                if cid_str in salvaged_ids:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname):
                    ctx.generic_notes_by_parent.setdefault(parent_id, []).append(cid_str)
                    ctx.total_notes_to_salvage += 1

        # 4) Optional: pre-salvage moves.
        #
        # In legacy mode we use a dedicated 🅿️ Notes (parking) node under the
        # FILE and move beacon-scoped NOTES there before deleting the
        # structural subtree. In incremental mode we avoid a parking node but
        # still physically move NOTES up under the FILE node so they are
        # decoupled from any reconcile_tree mutations.
        if mode == "legacy":
            # Locate existing parking node if present.
            for child in file_children:
                cid = child.get("id")
                if not cid:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname) and "parking" in cname.lower():
                    ctx.parking_node_id = str(cid)
                    break

            # Create parking node when needed.
            if (
                ctx.total_notes_to_salvage
                and ctx.parking_node_id is None
                and not dry_run
            ):
                req = NodeCreateRequest(
                    name="🅿️ Notes (parking)",
                    parent_id=str(file_node_id),
                    note=None,
                    layoutMode=None,
                    position="bottom",
                )
                try:
                    created = await self.create_node(req, _internal_call=True)
                    ctx.parking_node_id = created.id
                    ctx.parking_created_here = True
                except Exception as e:  # noqa: BLE001
                    raise NetworkError(
                        f"Failed to create 'Notes (parking)' node: {e}",
                    ) from e

            # Move beacon-scoped Notes[...] under parking.
            if not dry_run and ctx.parking_node_id and ctx.saved_notes_by_beacon:
                for notes_list in ctx.saved_notes_by_beacon.values():
                    for nid in notes_list:
                        try:
                            await self.move_node(nid, ctx.parking_node_id, "bottom")
                            ctx.notes_moved_to_parking += 1
                        except Exception as e:  # noqa: BLE001
                            log_event(
                                f"Failed to move notes node {nid} to parking: {e}",
                                "BEACON",
                            )
        elif mode == "incremental":
            # In incremental mode we skip a parking node but still, when not
            # dry_run, move beacon-scoped and generic Notes[...] roots directly
            # under the FILE node. This ensures reconcile_tree never sees them
            # as children of a structural parent that might be moved or
            # deleted.
            if not dry_run:
                # Beacon-scoped first
                for notes_list in ctx.saved_notes_by_beacon.values():
                    for nid in notes_list:
                        try:
                            await self.move_node(nid, str(file_node_id), "bottom")
                            ctx.notes_moved_to_parking += 1
                        except Exception as e:  # noqa: BLE001
                            log_event(
                                f"Failed to move notes node {nid} to file root {file_node_id}: {e}",
                                "BEACON",
                            )
                # Then generic Notes under non-FILE, non-beacon parents
                for notes_list in ctx.generic_notes_by_parent.values():
                    for nid in notes_list:
                        try:
                            await self.move_node(nid, str(file_node_id), "bottom")
                            ctx.notes_moved_to_parking += 1
                        except Exception as e:  # noqa: BLE001
                            log_event(
                                f"Failed to move generic notes node {nid} to file root {file_node_id}: {e}",
                                "BEACON",
                            )

        # 5) Log summary for observability.
        try:
            log_event(
                "collect_notes_salvage_context_for_file "
                f"mode={mode} file={file_node_id} path={source_path} "
                f"beacon_roots={sum(len(v) for v in ctx.saved_notes_by_beacon.values())} "
                f"generic_parents={len(ctx.generic_notes_by_parent)} "
                f"notes_moved_pre_reconcile={ctx.notes_moved_to_parking}",
                "BEACON",
            )
        except Exception:
            pass

        return ctx

    # @beacon[
    #   id=auto-beacon@WorkFlowyClientNexus._restore_notes_for_file-aya1,
    #   role=WorkFlowyClientNexus._restore_notes_for_file,
    #   slice_labels=ra-notes,ra-notes-salvage,ra-notes-cartographer,
    #   kind=ast,
    # ]
    async def _restore_notes_for_file(
        self,
        *,
        file_node_id: str,
        source_path: str,
        salvage_ctx: NotesSalvageContext,
        dry_run: bool,
    ) -> dict[str, int]:
        """Restore NOTES after structural changes for a single FILE node.

        Responsibilities:
        - Re-export the FILE subtree and find new beacon nodes by `id:`.
        - Move beacon-scoped Notes[...] roots under their new parents (or under
          a 🧩 Notes (salvaged) node when the beacon disappeared).
        - Reattach FILE-level Notes[...] roots keyed by canonical Path so they
          remain directly under the FILE node.
        - Reattach generic Notes[...] that were temporarily moved off
          non-FILE, non-beacon parents in the incremental path, or salvage
          them under 🧩 Notes (salvaged) when their original parents vanish.
        - Clean up a parking node created during collection when appropriate.

        This helper is used by both the legacy full-rebuild path and the
        incremental F12 path (as a belt-and-suspenders layer on top of
        explicitly_preserved_ids).
        """

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus._restore_notes_for_file._canonical_path-cnpc,
        #   role=WorkFlowyClientNexus._restore_notes_for_file._canonical_path,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        def _canonical_path(path: str) -> str:
            return os.path.normcase(os.path.normpath(path))

        # 1) Re-export refreshed subtree under the FILE node.
        try:
            refreshed_raw = await export_nodes_impl(self, file_node_id)
            refreshed_nodes = refreshed_raw.get("nodes") or []
        except Exception as e:  # noqa: BLE001
            log_event(
                f"_restore_notes_for_file: export_nodes_impl failed for {file_node_id}: {e}",
                "BEACON",
            )
            refreshed_nodes = []

        node_by_id: dict[str, dict[str, Any]] = {}
        children_by_parent: dict[str, list[dict[str, Any]]] = {}
        for n in refreshed_nodes:
            nid = n.get("id")
            if not nid:
                continue
            s_nid = str(nid)
            node_by_id[s_nid] = n
            pid = n.get("parent_id") or n.get("parentId")
            if pid:
                children_by_parent.setdefault(str(pid), []).append(n)

        salvaged_root_id: str | None = None

        # @beacon[
        #   id=auto-beacon@WorkFlowyClientNexus._restore_notes_for_file._ensure_salvaged_root-s2b8,
        #   role=WorkFlowyClientNexus._restore_notes_for_file._ensure_salvaged_root,
        #   slice_labels=ra-notes,
        #   kind=ast,
        # ]
        async def _ensure_salvaged_root() -> str | None:
            """Locate or create the 🧩 Notes (salvaged) root under the FILE node."""
            nonlocal salvaged_root_id

            if dry_run:
                return None
            if salvaged_root_id is not None:
                return salvaged_root_id

            # Try to locate an existing salvaged root first.
            for child in children_by_parent.get(str(file_node_id), []) or []:
                cid = child.get("id")
                if not cid:
                    continue
                cname = str(child.get("name") or "")
                if _is_notes_name(cname) and "salvaged" in cname.lower():
                    salvaged_root_id = str(cid)
                    return salvaged_root_id

            # Create a new salvaged root.
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
            return salvaged_root_id

        # 2) Build beacon_id → new Workflowy node id mapping.
        new_beacon_nodes: dict[str, str] = {}
        for n in refreshed_nodes:
            nid = n.get("id")
            if not nid:
                continue
            note = n.get("note") or n.get("no") or ""
            if "BEACON (" not in str(note):
                continue
            beacon_id_val: str | None = None
            for line in str(note).splitlines():
                stripped = line.strip()
                if stripped.startswith("id:"):
                    beacon_id_val = stripped.split(":", 1)[1].strip()
                    break
            if beacon_id_val:
                new_beacon_nodes[beacon_id_val] = str(nid)

        # 3) Reattach beacon-scoped NOTES.
        notes_salvaged = 0
        notes_orphaned = 0

        if salvage_ctx.saved_notes_by_beacon:
            if dry_run:
                for beacon_id, ids_list in salvage_ctx.saved_notes_by_beacon.items():
                    if beacon_id in new_beacon_nodes:
                        notes_salvaged += len(ids_list)
                    else:
                        notes_orphaned += len(ids_list)
            else:
                for beacon_id, ids_list in salvage_ctx.saved_notes_by_beacon.items():
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
                        # Orphaned – move under 'Notes (salvaged)' under the FILE
                        # node if possible.
                        if not ids_list:
                            continue
                        root_id = await _ensure_salvaged_root()
                        for nid in ids_list:
                            if dry_run or root_id is None:
                                notes_orphaned += 1
                                continue
                            try:
                                await self.move_node(nid, root_id, "bottom")
                                notes_orphaned += 1
                            except Exception as e:  # noqa: BLE001
                                log_event(
                                    f"Failed to move notes node {nid} to salvaged root: {e}",
                                    "BEACON",
                                )

        # 4) Generic Notes[...] reattachment for incremental path.
        generic_reattached = 0
        generic_salvaged = 0

        if salvage_ctx.generic_notes_by_parent:
            if dry_run:
                for parent_id, ids_list in salvage_ctx.generic_notes_by_parent.items():
                    if parent_id in node_by_id:
                        generic_reattached += len(ids_list)
                    else:
                        generic_salvaged += len(ids_list)
            else:
                for parent_id, ids_list in salvage_ctx.generic_notes_by_parent.items():
                    parent_exists = parent_id in node_by_id
                    if parent_exists:
                        for nid in ids_list:
                            if nid not in node_by_id:
                                # Note vanished entirely; treat as orphan.
                                generic_salvaged += 1
                                continue
                            try:
                                await self.move_node(nid, parent_id, "bottom")
                                generic_reattached += 1
                            except Exception as e:  # noqa: BLE001
                                log_event(
                                    f"Failed to reattach generic notes node {nid} to parent {parent_id}: {e}",
                                    "BEACON",
                                )
                    else:
                        # Original structural parent is gone – salvage under
                        # 🧩 Notes (salvaged) if possible.
                        root_id = await _ensure_salvaged_root()
                        for nid in ids_list:
                            if dry_run or root_id is None:
                                generic_salvaged += 1
                                continue
                            try:
                                await self.move_node(nid, root_id, "bottom")
                                generic_salvaged += 1
                            except Exception as e:  # noqa: BLE001
                                log_event(
                                    f"Failed to move generic notes node {nid} to salvaged root: {e}",
                                    "BEACON",
                                )

        # 5) FILE-level path-based Notes[...] reattachment (immediate Notes under
        # the FILE node whose Path matches source_path).
        total_notes_stashed_by_path = sum(
            len(v) for v in salvage_ctx.stashed_notes_by_path.values()
        ) if salvage_ctx.stashed_notes_by_path else 0
        notes_path_reattached = 0
        notes_path_unmatched = 0

        if total_notes_stashed_by_path:
            if dry_run:
                for key, ids_list in salvage_ctx.stashed_notes_by_path.items():
                    if key is not None and key == _canonical_path(source_path):
                        notes_path_reattached += len(ids_list)
                    else:
                        notes_path_unmatched += len(ids_list)
            else:
                file_path_key_canonical = _canonical_path(source_path) if source_path else None
                for key, ids_list in salvage_ctx.stashed_notes_by_path.items():
                    if key is not None and key == file_path_key_canonical:
                        for nid in ids_list:
                            try:
                                await self.move_node(nid, str(file_node_id), "bottom")
                                notes_path_reattached += 1
                            except Exception as e:  # noqa: BLE001
                                log_event(
                                    "_restore_notes_for_file: failed to reattach FILE-level Notes "
                                    f"node {nid} under {file_node_id}: {e}",
                                    "BEACON",
                                )
                    else:
                        notes_path_unmatched += len(ids_list)

        # 6) Clean up parking node if we created it during collection.
        if not dry_run and salvage_ctx.parking_created_here and salvage_ctx.parking_node_id:
            try:
                await self.delete_node(salvage_ctx.parking_node_id)
            except Exception:
                # Non-fatal; at worst the parking node remains in the tree.
                pass

        # 7) Log summary and return counts.
        try:
            log_event(
                "_restore_notes_for_file "
                f"file={file_node_id} path={source_path} "
                f"identified={salvage_ctx.total_notes_to_salvage} "
                f"notes_salvaged={notes_salvaged} notes_orphaned={notes_orphaned} "
                f"generic_reattached={generic_reattached} generic_salvaged={generic_salvaged} "
                f"stashed_by_path={total_notes_stashed_by_path} "
                f"path_reattached={notes_path_reattached} path_unmatched={notes_path_unmatched} "
                f"notes_moved_pre_reconcile={salvage_ctx.notes_moved_to_parking}",
                "BEACON",
            )
        except Exception:
            pass

        return {
            "notes_identified_for_salvage": salvage_ctx.total_notes_to_salvage,
            "notes_salvaged": notes_salvaged,
            "notes_orphaned": notes_orphaned,
            "notes_stashed_by_path": total_notes_stashed_by_path,
            "notes_path_reattached": notes_path_reattached,
            "notes_path_unmatched": notes_path_unmatched,
            "notes_moved_to_parking": salvage_ctx.notes_moved_to_parking,
            "generic_notes_reattached": generic_reattached,
            "generic_notes_salvaged": generic_salvaged,
        }
