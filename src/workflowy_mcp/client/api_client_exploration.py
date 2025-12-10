"""WorkFlowy API client - Exploration state machine operations."""

import json
import sys
import os
import uuid
from typing import Any
from datetime import datetime
from pathlib import Path

from ..models import NetworkError

from .api_client_core import _ClientLogger, log_event
from .api_client_nexus import WorkFlowyClientNexus

# 2-LETTER ACTION CODE MAPPING (module-level constant)
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
    "LF": "lightning_flash_into_section",
    "MSD": "mark_section_for_deletion",
    "MSM": "mark_section_as_merge_target",
    "MSP": "mark_section_as_permanent",
    "ALS": "abandon_lightning_strike",
}


class WorkFlowyClientExploration(WorkFlowyClientNexus):
    """Exploration state machine - extends Nexus."""

    def _get_explore_sessions_dir(self) -> str:
        """Return exploration sessions directory."""
        base_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_explore_sessions"
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    @staticmethod
    def _build_frontier_preview_lines(
        frontier: list[dict[str, Any]] | None,
        max_note_chars: int = 1024,
    ) -> list[str]:
        """Build one-line-per-handle preview for frontiers."""
        if not frontier:
            return []

        handles = [str(entry.get("handle", "")) for entry in frontier]
        if not handles:
            return []
        max_len = max(len(h) for h in handles)

        lines = []
        for entry in frontier:
            handle = str(entry.get("handle", ""))
            label = handle.ljust(max_len)
            depth = max(1, len(handle.split(".")))
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

    def _build_frontier_tree_from_flat(self, frontier: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build nested tree from flat frontier."""
        if not frontier:
            return []

        by_handle: dict[str, dict[str, Any]] = {}
        for entry in frontier:
            node = dict(entry)
            node["children"] = []
            by_handle[node["handle"]] = node

        roots = []
        for handle, node in by_handle.items():
            parent = node.get("parent_handle")
            if parent and parent in by_handle:
                by_handle[parent]["children"].append(node)
            else:
                roots.append(node)

        # Natural sort
        def natural_key(node):
            handle = node.get("handle", "")
            parts = handle.split(".")
            result = []
            for part in parts:
                if part.isdigit():
                    result.append((0, int(part)))
                else:
                    result.append((1, part))
            return result
        
        roots.sort(key=natural_key)
        for node in by_handle.values():
            node["children"].sort(key=natural_key)
        
        # Remove empty children
        for node in by_handle.values():
            if not node["children"]:
                del node["children"]

        return roots

    def _compute_exploration_frontier(
        self,
        session: dict[str, Any],
        frontier_size: int,
        max_depth_per_frontier: int,
    ) -> list[dict[str, Any]]:
        """Compute next frontier for exploration session.
        
        DFS GUIDED modes: Leaf-chunk frontiers (accumulate sibling leaves in DFS order)
        LEGACY modes: Round-robin open parents
        """
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        search_filter = session.get("search_filter")
        exploration_mode = session.get("exploration_mode", "manual")
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        def _collect_hints(handle: str) -> list[str]:
            hints = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_hints = (handles.get(parent, {}) or {}).get("hints") or []
                hints.extend(parent_hints)
                parent = handles.get(parent, {}).get("parent")
            return hints

        frontier = []

        # DFS GUIDED MODES
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:
            from collections import deque

            if frontier_size <= 0:
                return frontier

            leaf_target = max(1, frontier_size)
            leaf_leeway = max(1, min(leaf_target, 10))

            # BFS to collect undecided leaves
            bfs_leaves = []
            queue = deque(["R"])
            visited = set()

            while queue and len(bfs_leaves) < leaf_target + leaf_leeway:
                h = queue.popleft()
                if h in visited:
                    continue
                visited.add(h)

                if h == "R":
                    for ch in handles.get("R", {}).get("children", []) or []:
                        if ch not in visited:
                            queue.append(ch)
                    continue

                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles

                st = state.get(h, {"status": "unseen"})
                is_undecided = st.get("status") in {"unseen", "candidate", "open"}

                if is_leaf and is_undecided:
                    ancestor_hints = _collect_hints(h)
                    if "SKELETON_PRUNED" in ancestor_hints:
                        # Hidden from BFS by Skeleton Walk pruning, but still present in handles/state.
                        continue
                    bfs_leaves.append(h)

                for ch in child_handles:
                    if ch not in visited:
                        queue.append(ch)

            if not bfs_leaves:
                # Optional pseudo-branch inclusion: if no undecided structural leaves remain,
                # surface undecided branches whose descendants are all decided as a "last-mile"
                # frontier, so the agent can explicitly decide them.
                pseudo_leaves: list[str] = []
                for h_all, meta_all in handles.items():
                    if h_all == "R":
                        continue
                    child_handles_all = (meta_all or {}).get("children", []) or []
                    # Only consider structural branches (true leaves already handled above)
                    if not child_handles_all:
                        continue
                    st_all = state.get(h_all, {"status": "unseen"})
                    if st_all.get("status") not in {"unseen", "candidate", "open"}:
                        continue

                    # Check descendants for any undecided node
                    has_undecided_desc = False
                    stack = list(child_handles_all)
                    seen_local: set[str] = set()
                    while stack and not has_undecided_desc:
                        ch = stack.pop()
                        if ch in seen_local:
                            continue
                        seen_local.add(ch)
                        st_ch = state.get(ch, {"status": "unseen"})
                        if st_ch.get("status") in {"unseen", "candidate", "open"}:
                            has_undecided_desc = True
                            break
                        stack.extend((handles.get(ch, {}) or {}).get("children", []) or [])

                    if not has_undecided_desc:
                        pseudo_leaves.append(h_all)

                # If we found pseudo-leaf branches, use them as our frontier; otherwise bail out
                if pseudo_leaves:
                    bfs_leaves = pseudo_leaves
                else:
                    return frontier

            # Chunk by siblings
            selected = []
            selected_set = set()
            total = 0
            idx = 0

            while idx < len(bfs_leaves) and total < leaf_target + leaf_leeway:
                leaf = bfs_leaves[idx]
                idx += 1
                if leaf in selected_set:
                    continue

                parent_handle = handles.get(leaf, {}).get("parent")
                sibling_group = []

                if parent_handle:
                    for ch in handles.get(parent_handle, {}).get("children", []) or []:
                        if ch in selected_set:
                            continue
                        st_ch = state.get(ch, {"status": "unseen"})
                        if st_ch.get("status") in {"finalized", "closed"}:
                            continue
                        ch_children = handles.get(ch, {}).get("children", []) or []
                        if not ch_children:
                            sibling_group.append(ch)
                else:
                    sibling_group.append(leaf)

                if not sibling_group:
                    continue

                if total >= leaf_target and total + len(sibling_group) > leaf_target + leaf_leeway:
                    break

                for ch in sibling_group:
                    if ch in selected_set:
                        continue
                    selected_set.add(ch)
                    selected.append(ch)
                    total += 1

            if not selected:
                return frontier

            # Collect branch ancestors
            include_branches = True
            branches = []
            if include_branches:
                branch_set = set()
                for leaf_h in selected:
                    ancestor = handles.get(leaf_h, {}).get("parent")
                    while ancestor and ancestor != "R":
                        if ancestor not in branch_set:
                            branch_set.add(ancestor)
                        ancestor = handles.get(ancestor, {}).get("parent")
                branches = sorted(branch_set)

            MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

            # Build branch entries
            if include_branches and branches:
                for h in branches:
                    meta = handles.get(h, {}) or {}
                    st = state.get(h, {"status": "unseen"})
                    child_handles = meta.get("children", []) or []

                    note_full = meta.get("note") or ""
                    if MAX_NOTE_PREVIEW is None:
                        note_preview = note_full
                    else:
                        note_preview = note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]

                    local_hints = meta.get("hints") or []
                    hints_from_ancestors = _collect_hints(h)

                    # Check if has showing descendants
                    has_showing = any(leaf_h.startswith(h + ".") for leaf_h in selected)
                    
                    if exploration_mode == "dfs_guided_bulk" and has_showing:
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
                        "is_leaf": False,
                        "guidance": guidance,
                        "_frontier_role": "branch_ancestor",
                    }
                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)

            # Build leaf entries
            for h in selected:
                meta = handles.get(h, {}) or {}
                st = state.get(h, {"status": "unseen"})
                child_handles = meta.get("children", []) or []

                note_full = meta.get("note") or ""
                if MAX_NOTE_PREVIEW is None:
                    note_preview = note_full
                else:
                    note_preview = note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]

                local_hints = meta.get("hints") or []
                hints_from_ancestors = _collect_hints(h)

                entry = {
                    "handle": h,
                    "parent_handle": meta.get("parent"),
                    "name_preview": meta.get("name", ""),
                    "note_preview": note_preview,
                    "child_count": len(child_handles),
                    "depth": meta.get("depth", 0),
                    "status": st.get("status", "candidate"),
                    "is_leaf": True,
                    "guidance": "leaf: EL=engulf, PL=preserve",
                }
                if local_hints:
                    entry["hints"] = local_hints
                if hints_from_ancestors:
                    entry["hints_from_ancestors"] = hints_from_ancestors
                
                frontier.append(entry)

            # Natural sort
            def natural_key(entry):
                handle = entry.get("handle", "")
                parts = handle.split(".")
                result = []
                for part in parts:
                    if part.isdigit():
                        result.append((0, int(part)))
                    else:
                        result.append((1, part))
                return result
            
            frontier.sort(key=natural_key)

            # Apply SECONDARY search filter (explicit mode only)
            if search_filter and exploration_mode == "dfs_guided_explicit":
                frontier = self._apply_search_filter_to_frontier(frontier, search_filter, handles)

            return frontier

        # LEGACY MODE: Round-robin open parents
        candidate_parents = [h for h, st in state.items() if st.get("status") == "open"]
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        if not candidate_parents or frontier_size <= 0:
            return frontier

        parent_indices = {h: 0 for h in candidate_parents}

        while len(frontier) < frontier_size:
            made_progress = False

            for parent_handle in candidate_parents:
                meta = handles.get(parent_handle) or {}
                child_handles = meta.get("children", []) or []

                idx = parent_indices.get(parent_handle, 0)
                while idx < len(child_handles) and len(frontier) < frontier_size:
                    child_handle = child_handles[idx]
                    parent_indices[parent_handle] = idx + 1
                    idx += 1

                    child_state = state.get(child_handle, {"status": "unseen"})
                    if child_state.get("status") in {"closed", "finalized"}:
                        continue

                    child_meta = handles.get(child_handle) or {}
                    grandchildren = child_meta.get("children", []) or []

                    is_leaf = not grandchildren
                    guidance = "leaf: EL=engulf, PL=preserve" if is_leaf else "branch: OP=open, RB=reserve"

                    note_full = child_meta.get("note") or ""
                    note_preview = note_full if MAX_NOTE_PREVIEW is None else (
                        note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]
                    )

                    local_hints = child_meta.get("hints") or []
                    hints_from_ancestors = _collect_hints(child_handle)

                    entry = {
                        "handle": child_handle,
                        "parent_handle": parent_handle,
                        "name_preview": child_meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(grandchildren),
                        "depth": child_meta.get("depth", 0),
                        "status": child_state.get("status", "candidate"),
                        "is_leaf": is_leaf,
                        "guidance": guidance,
                    }
                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)
                    made_progress = True
                    break

                if len(frontier) >= frontier_size:
                    break

            if not made_progress:
                break

        return frontier

    def _apply_search_filter_to_frontier(
        self,
        frontier: list[dict[str, Any]],
        search_filter: dict[str, Any],
        handles: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Apply persistent search filter (SECONDARY - explicit mode only)."""
        import re

        search_text = search_filter.get("search_text", "")
        case_sensitive = search_filter.get("case_sensitive", False)
        whole_word = search_filter.get("whole_word", False)
        use_regex = search_filter.get("regex", False)

        if not search_text:
            return frontier

        filtered = []

        for entry in frontier:
            h = entry.get("handle")
            meta = handles.get(h, {}) or {}
            name = meta.get("name", "")
            note = meta.get("note", "")
            searchable = f"{name}\n{note}"

            matches = False
            if use_regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(search_text, searchable, flags):
                        matches = True
                except re.error:
                    pass
            elif whole_word:
                pattern = r"\b" + re.escape(search_text) + r"\b"
                flags = 0 if case_sensitive else re.IGNORECASE
                if re.search(pattern, searchable, flags):
                    matches = True
            else:
                if case_sensitive:
                    if search_text in searchable:
                        matches = True
                else:
                    if search_text.lower() in searchable.lower():
                        matches = True

            if matches:
                filtered_entry = dict(entry)
                if entry.get("is_leaf"):
                    filtered_entry["guidance"] = "SEARCH MATCH - leaf: EL=engulf, PL=preserve"
                else:
                    filtered_entry["guidance"] = "SEARCH MATCH - branch: RB=reserve, PB=preserve"
                filtered.append(filtered_entry)

        return filtered

    def _compute_search_frontier(
        self,
        session: dict[str, Any],
        search_actions: list[dict[str, Any]],
        scope: str = "undecided",
        max_results: int = 80,
    ) -> list[dict[str, Any]]:
        """Compute frontier from SEARCH results (PRIMARY - bulk mode)."""
        import re

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        def _collect_hints(handle: str) -> list[str]:
            hints = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_hints = (handles.get(parent, {}) or {}).get("hints") or []
                hints.extend(parent_hints)
                parent = handles.get(parent, {}).get("parent")
            return hints

        all_handles = set(handles.keys())
        all_handles.discard("R")
        matches_union: set[str] = set()

        # OR logic across searches (each search_action contributes matches to the union)
        for search_action in search_actions:
            handle_filter = search_action.get("handle", "R")
            search_text = search_action.get("search_text", "")
            case_sensitive = search_action.get("case_sensitive", False)
            whole_word = search_action.get("whole_word", False)
            use_regex = search_action.get("regex", False)

            if not search_text:
                continue

            # Base candidate set for THIS search action
            if handle_filter != "R":
                base = {
                    h for h in all_handles
                    if h == handle_filter or h.startswith(handle_filter + ".")
                }
            else:
                base = set(all_handles)

            # Match within this base
            matching_for_action = set()
            for h in base:
                meta = handles.get(h, {}) or {}
                name = meta.get("name", "")
                note = meta.get("note", "")
                searchable = f"{name}\n{note}"

                if use_regex:
                    try:
                        flags = 0 if case_sensitive else re.IGNORECASE
                        if re.search(search_text, searchable, flags):
                            matching_for_action.add(h)
                    except re.error:
                        pass
                elif whole_word:
                    pattern = r"\b" + re.escape(search_text) + r"\b"
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(pattern, searchable, flags):
                        matching_for_action.add(h)
                else:
                    if case_sensitive:
                        if search_text in searchable:
                            matching_for_action.add(h)
                    else:
                        if search_text.lower() in searchable.lower():
                            matching_for_action.add(h)

            matches_union |= matching_for_action

        candidates = matches_union

        # Scope filter (undecided only)
        if scope == "undecided":
            UNDECIDED_STATUSES = {"unseen", "candidate", "open"}

            def is_undecided(handle: str) -> bool:
                """Treat missing state entries as 'unseen' (undecided)."""
                st = state.get(handle, {"status": "unseen"})
                return st.get("status") in UNDECIDED_STATUSES

            def has_any_undecided_desc(branch: str) -> bool:
                """Check if branch has ANY undecided descendants (not just search matches)."""
                children = (handles.get(branch, {}) or {}).get("children") or []
                stack = list(children)
                seen: set[str] = set()
                while stack:
                    ch = stack.pop()
                    if ch in seen:
                        continue
                    seen.add(ch)
                    # Check if THIS descendant is undecided (not whether it matches search)
                    if is_undecided(ch):
                        return True
                    stack.extend((handles.get(ch, {}) or {}).get("children") or [])
                return False

            filtered: set[str] = set()
            for h in candidates:
                is_leaf = not (handles.get(h, {}) or {}).get("children")
                if is_leaf:
                    # Leaf: include if undecided
                    if is_undecided(h):
                        filtered.add(h)
                else:
                    # Branch: include if it has ANY undecided descendants OR is itself undecided
                    if is_undecided(h) or has_any_undecided_desc(h):
                        filtered.add(h)
            candidates = filtered

        # Limit matches, include ancestors (including root 'R'), then sort naturally by handle
        matches = sorted(candidates)[:max_results]
        final_handles = set(matches)

        for h in matches:
            ancestor = handles.get(h, {}).get("parent")
            while ancestor:
                final_handles.add(ancestor)
                ancestor = handles.get(ancestor, {}).get("parent")

        def _natural_key_handle(handle: str) -> list[tuple[int, object]]:
            parts = handle.split(".")
            result: list[tuple[int, object]] = []
            for part in parts:
                if part.isdigit():
                    result.append((0, int(part)))
                else:
                    result.append((1, part))
            return result

        # Root 'R' first if present, then others in natural handle order
        if "R" in final_handles:
            others = [h for h in final_handles if h != "R"]
            final_list = ["R"] + sorted(others, key=_natural_key_handle)
        else:
            final_list = sorted(final_handles, key=_natural_key_handle)

        # Build entries
        frontier = []
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        for h in final_list:
            meta = handles.get(h, {}) or {}
            st = state.get(h, {"status": "unseen"})
            child_handles = meta.get("children", []) or []
            is_leaf = not child_handles

            note_full = meta.get("note") or ""
            note_preview = note_full if MAX_NOTE_PREVIEW is None else (
                note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]
            )

            local_hints = meta.get("hints") or []
            hints_from_ancestors = _collect_hints(h)

            if h in matches:
                guidance = "MATCH - leaf: EL=engulf, PL=preserve" if is_leaf else "MATCH - branch: RB=reserve, PB=preserve, EF=engulf_showing, PF=preserve_showing"
            else:
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

    def nexus_list_exploration_sessions(self, nexus_tag: str | None = None) -> dict[str, Any]:
        """List exploration sessions."""
        import json as json_module
        
        sessions_dir = self._get_explore_sessions_dir()
        sessions_path = Path(sessions_dir)
        
        results = []
        for session_file in sessions_path.glob("*.json"):
            try:
                with open(session_file, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                
                tag = session.get("nexus_tag")
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
                continue
        
        results.sort(key=lambda s: s.get("created_at", ""), reverse=True)
        
        return {"success": True, "sessions": results, "total": len(results)}

    async def nexus_resume_exploration(
        self,
        session_id: str | None = None,
        nexus_tag: str | None = None,
        frontier_size: int = 25,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Resume exploration session."""
        import json as json_module
        
        sessions_dir = self._get_explore_sessions_dir()
        
        # Resolve session_id
        if session_id:
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            if not os.path.exists(session_path):
                raise NetworkError(f"Session '{session_id}' not found")
        elif nexus_tag:
            tag_sessions = list(Path(sessions_dir).glob(f"*__{nexus_tag}-*.json"))
            if not tag_sessions:
                raise NetworkError(f"No sessions for '{nexus_tag}'")
            latest = sorted(tag_sessions)[-1]
            session_path = str(latest)
            session_id = latest.stem
        else:
            raise NetworkError("Provide session_id or nexus_tag")
        
        # Load session
        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)
        
        # Check if finalized
        tag = session.get("nexus_tag")
        if tag:
            try:
                run_dir = self._get_nexus_dir(tag)
                phantom = os.path.join(run_dir, "phantom_gem.json")
                if os.path.exists(phantom):
                    return {
                        "success": True,
                        "session_id": session_id,
                        "status": "completed",
                        "message": f"Already finalized - phantom_gem exists",
                        "phantom_gem": phantom,
                    }
            except NetworkError:
                pass
        
        # Compute frontier
        frontier = self._compute_exploration_frontier(session, frontier_size, 1)
        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist flat frontier
        session["last_frontier_flat"] = frontier
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"
        with open(session_path, "w", encoding="utf-8") as f:
            json_module.dump(session, f, indent=2, ensure_ascii=False)
        
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        
        history = None
        if include_history_summary:
            history = {
                "open": [h for h, st in state.items() if st.get("status") == "open"],
                "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                "steps": session.get("steps", 0),
            }
        
        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": tag,
            "status": "in_progress",
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "scratchpad": session.get("scratchpad", ""),
            "frontier_preview": frontier_preview,
            "frontier_tree": self._build_frontier_tree_from_flat(frontier),
            "walks": [],
            "skipped_walks": [],
            "decisions_applied": [],
            "history_summary": history,
            "session_meta": {
                "created_at": session.get("created_at"),
                "updated_at": session.get("updated_at"),
                "exploration_mode": session.get("exploration_mode"),
                "editable": session.get("editable"),
                "steps": session.get("steps", 0),
            }
        }

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
        """Initialize exploration session."""
        import glob

        logger = _ClientLogger()

        # Validate no existing tag
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        sessions_dir = Path(self._get_explore_sessions_dir())
        
        if base_dir.exists():
            pattern = str(base_dir / f"*__{nexus_tag}")
            if glob.glob(pattern):
                raise NetworkError(f"nexus_tag '{nexus_tag}' already exists")
        
        if sessions_dir.exists():
            pattern = str(sessions_dir / f"*__{nexus_tag}-*.json")
            if glob.glob(pattern):
                raise NetworkError(f"nexus_tag '{nexus_tag}' has existing sessions")

        # Determine mode
        exploration_mode = "dfs_guided_explicit"
        strict_completeness = False
        
        if session_hint:
            hint = session_hint.strip().lower()
            if "bulk" in hint or "guided_bulk" in hint or "non_strict" in hint:
                exploration_mode = "dfs_guided_bulk"
            if "strict_completeness" in hint or "strict" in hint:
                strict_completeness = True
        
        # Validate search_filter (explicit mode only)
        if search_filter is not None:
            if exploration_mode != "dfs_guided_explicit":
                raise NetworkError("search_filter only for dfs_guided_explicit mode")
            if not isinstance(search_filter, dict) or not search_filter.get("search_text"):
                raise NetworkError("search_filter must have 'search_text'")

        # Fetch tree
        glimpse = await self.workflowy_scry(node_id=root_id, depth=None, size_limit=max_nodes)

        if not glimpse.get("success"):
            raise NetworkError(f"Glimpse failed: {glimpse.get('error')}")

        root_meta = glimpse.get("root") or {"id": root_id, "name": "Root"}
        root_children = glimpse.get("children", []) or []

        root_node = {
            "id": root_meta.get("id", root_id),
            "name": root_meta.get("name", "Root"),
            "note": root_meta.get("note"),
            "parent_id": root_meta.get("parent_id"),
            "children": root_children,
        }

        # Stash original fields for editable
        if editable:
            def _stash(node: dict) -> None:
                if "original_name" not in node:
                    node["original_name"] = node.get("name")
                if "original_note" not in node:
                    node["original_note"] = node.get("note")
                for child in node.get("children") or []:
                    _stash(child)
            _stash(root_node)

        # Assign handles
        handles = {}

        def alpha_handle(index: int) -> str:
            letters = ""
            n = index
            while True:
                n, rem = divmod(n, 26)
                letters = chr(ord("A") + rem) + letters
                if n == 0:
                    break
                n -= 1
            return letters

        def walk(node: dict, handle: str, parent: str | None, depth: int, top: bool = False) -> None:
            children = node.get("children", []) or []
            child_handles = []

            if top:
                for idx, child in enumerate(children):
                    ch = alpha_handle(idx)
                    child_handles.append(ch)
                    walk(child, ch, "R", depth + 1, False)
            else:
                for idx, child in enumerate(children):
                    ch = f"{handle}.{idx + 1}"
                    child_handles.append(ch)
                    walk(child, ch, handle, depth + 1, False)

            handles[handle] = {
                "id": node.get("id"),
                "name": node.get("name", "Untitled"),
                "note": node.get("note"),
                "parent": parent,
                "children": child_handles,
                "depth": depth,
                "hints": [],
            }

        walk(root_node, "R", None, 0, top=True)

        # Initial state
        state = {"R": {"status": "open", "max_depth": None}}
        for ch in handles.get("R", {}).get("children", []) or []:
            state[ch] = {"status": "candidate", "max_depth": None}

        # Create session
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
            "strict_completeness": strict_completeness,
            "handles": handles,
            "state": state,
            "scratchpad": "",
            "root_node": root_node,
            "steps": 0,
            "search_filter": search_filter,
            "glimpse_stats": {
                "node_count": glimpse.get("node_count", 0),
                "depth": glimpse.get("depth", 0),
                "_source": glimpse.get("_source", "api"),
            },
        }

        # Compute frontier
        frontier = self._compute_exploration_frontier(session, frontier_size, max_depth_per_frontier)
        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist
        try:
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            with open(session_path, "w", encoding="utf-8") as f:
                json.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to persist session: {e}") from e

        # Build guidance
        if exploration_mode == "dfs_guided_explicit":
            step_guidance = [
                "🎯 EXPLICIT MODE: Auto-frontier",
                "Leaf: engulf_leaf_into_gem_for_editing (EL), preserve_leaf_in_ether_untouched (PL)",
                "Branch: flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states (RB), preserve_branch_node_in_ether_untouched__when_no_engulfed_children (PB)",
            ]
        elif exploration_mode == "dfs_guided_bulk":
            if strict_completeness:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier",
                    "🛡️ STRICT COMPLETENESS - PA disabled",
                    "Leaf: EL, PL, UL",
                    "Branch: RB, PB, UB, UN, AB",
                    "Bulk: EF, PF",
                    "Lightning: LF, MSD, MSM/MSP, ALS",
                ]
            else:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier",
                    "Leaf: EL, PL, UL",
                    "Branch: RB, PB, UB, UN, AB",
                    "Bulk: EF, PF",
                    "Global: PA",
                    "Lightning: LF, MSD, MSM/MSP, ALS",
                ]
        else:
            step_guidance = ["🎯 LEGACY MODE: Manual"]

        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "exploration_mode": exploration_mode,
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "root_handle": "R",
            "frontier_preview": frontier_preview,
            "root_summary": {
                "name": root_node.get("name", "Root"),
                "child_count": len(handles.get("R", {}).get("children", []) or []),
            },
            "frontier_tree": self._build_frontier_tree_from_flat(frontier),
            "scratchpad": "",
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
                - 'engulf_leaf_into_gem_for_editing' = Bring leaf into GEM
                - 'preserve_leaf_in_ether_untouched' = Preserve leaf in ETHER
                - 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states' = Branch shell
                - 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children' = Preserve branch
            - walks: IGNORED in guided modes
            - global_frontier_limit: leaf budget per step (default 80)

        LEGACY / MANUAL MODES:
            - walks: list of { origin, max_steps }
            - max_parallel_walks: hard cap on origins
        """
        import json as json_module

        logger = _ClientLogger()

        decisions = decisions or []
        walks = walks or []

        # Load session
        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}
        editable_mode = bool(session.get("editable", False))
        last_frontier_flat = session.get("last_frontier_flat") or []

        exploration_mode = session.get("exploration_mode", "manual")
        skipped_decisions: list[dict[str, Any]] = []
        
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:
            # Expand 2-letter codes
            if decisions:
                for decision in decisions:
                    act = decision.get("action")
                    if act in EXPLORATION_ACTION_2LETTER:
                        decision["action"] = EXPLORATION_ACTION_2LETTER[act]
            
            # Separate actions
            peek_actions = [d for d in (decisions or []) if d.get("action") == "peek_descendants_as_frontier"]
            search_actions = [d for d in (decisions or []) if d.get("action") == "search_descendants_for_text"]
            resume_actions = [d for d in (decisions or []) if d.get("action") == "resume_guided_frontier"]
            other_decisions = [
                d for d in (decisions or [])
                if d.get("action") not in {"peek_descendants_as_frontier", "search_descendants_for_text", "resume_guided_frontier"}
            ]

            non_search_decisions = (peek_actions or []) + (resume_actions or []) + (other_decisions or [])

            # Pre-compute frontier
            if search_actions:
                correct_frontier_for_bulk = self._compute_search_frontier(
                    session=session,
                    search_actions=search_actions,
                    scope="undecided",
                    max_results=global_frontier_limit,
                )
            elif session.get("_peek_frontier"):
                correct_frontier_for_bulk = session.get("_peek_frontier", [])
                logger.info(f"Using stashed PEEK frontier ({len(correct_frontier_for_bulk)} entries)")
            elif last_frontier_flat:
                correct_frontier_for_bulk = last_frontier_flat
            else:
                correct_frontier_for_bulk = self._compute_exploration_frontier(
                    session, frontier_size=global_frontier_limit, max_depth_per_frontier=1
                )
            
            # Apply non-search decisions
            if non_search_decisions:
                internal_result = await self._nexus_explore_step_internal(
                    session_id=session_id,
                    actions=non_search_decisions,
                    frontier_size=global_frontier_limit,
                    max_depth_per_frontier=1,
                    _precomputed_frontier=correct_frontier_for_bulk,
                )
                skipped_decisions = internal_result.get("skipped_decisions", []) or []
                with open(session_path, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                handles = session.get("handles", {}) or {}
                state = session.get("state", {}) or {}
                root_node = session.get("root_node") or {}
                editable_mode = bool(session.get("editable", False))

            # Build step guidance
            if exploration_mode == "dfs_guided_explicit":
                step_guidance = [
                    "🎯 EXPLICIT MODE: Auto-frontier",
                    "Leaf: EL, PL, UL",
                    "Branch: RB, PB",
                    "Lightning: LF=lightning strike, MSD=delete section, MSM/MSP=merge/keep, ALS=cancel lightning",
                ]
            elif exploration_mode == "dfs_guided_bulk":
                strict = session.get("strict_completeness", False)
                if strict:
                    step_guidance = [
                        "🎯 BULK MODE",
                        "🛡️ STRICT - PA disabled",
                        "Leaf: EL, PL",
                        "Branch: RB, PB",
                        "Bulk: EF, PF",
                        "Lightning: LF, MSD, MSM/MSP, ALS",
                    ]
                else:
                    step_guidance = [
                        "🎯 BULK MODE",
                        "Leaf: EL, PL",
                        "Branch: RB, PB",
                        "Bulk: EF, PF",
                        "Global: PA",
                        "Lightning: LF, MSD, MSM/MSP, ALS",
                    ]
            else:
                step_guidance = ["🎯 LEGACY MODE"]

            # Handle PEEK return
            if peek_actions:
                peek_frontier = session.get("_peek_frontier", [])
                if peek_frontier:
                    frontier = peek_frontier
                    frontier_tree = self._build_frontier_tree_from_flat(frontier)
                    frontier_preview = self._build_frontier_preview_lines(frontier)

                    session["last_frontier_flat"] = frontier
                    session["updated_at"] = datetime.utcnow().isoformat() + "Z"
                    with open(session_path, "w", encoding="utf-8") as f:
                        json_module.dump(session, f, indent=2, ensure_ascii=False)

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
                        "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                        "action_key": EXPLORATION_ACTION_2LETTER,
                        "step_guidance": step_guidance,
                        "frontier_preview": frontier_preview,
                        "frontier_tree": frontier_tree,
                        "walks": [],
                        "skipped_walks": [],
                        "decisions_applied": decisions,
                        "skipped_decisions": skipped_decisions,
                        "scratchpad": session.get("scratchpad", ""),
                        "history_summary": history_summary,
                    }

            # Compute final frontier
            if search_actions:
                frontier = self._compute_search_frontier(
                    session=session, search_actions=search_actions,
                    scope="undecided", max_results=global_frontier_limit
                )
            elif session.get("_peek_frontier"):
                del session["_peek_frontier"]
                if "_peek_root_handle" in session:
                    del session["_peek_root_handle"]
                if "_peek_max_nodes" in session:
                    del session["_peek_max_nodes"]
                logger.info("Peek consumed - returning to DFS")
                frontier = self._compute_exploration_frontier(
                    session, frontier_size=global_frontier_limit, max_depth_per_frontier=1
                )
            else:
                frontier = self._compute_exploration_frontier(
                    session, frontier_size=global_frontier_limit, max_depth_per_frontier=1
                )

            frontier_tree = self._build_frontier_tree_from_flat(frontier)
            frontier_preview = self._build_frontier_preview_lines(frontier)

            session["last_frontier_flat"] = frontier
            session["updated_at"] = datetime.utcnow().isoformat() + "Z"
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)

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
                "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                "action_key": EXPLORATION_ACTION_2LETTER,
                "step_guidance": step_guidance,
                "frontier_preview": frontier_preview,
                "frontier_tree": frontier_tree,
                "walks": [],
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
        """Exploration step - delegates to v2."""
        return await self.nexus_explore_step_v2(
            session_id, decisions, walks, max_parallel_walks,
            global_frontier_limit, include_history_summary
        )

    async def _nexus_explore_step_internal(
        self,
        session_id: str,
        actions: list[dict[str, Any]] | None = None,
        frontier_size: int = 5,
        max_depth_per_frontier: int = 1,
        _precomputed_frontier: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Internal v1-style exploration helper."""
        import json as json_module

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        skipped_decisions: list[dict[str, Any]] = []

        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load session: {e}") from e

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        exploration_mode = session.get("exploration_mode", "manual")
        root_node = session.get("root_node") or {}

        # Build node index for editable mode
        node_by_id: dict[str, dict[str, Any]] = {}

        def _index_tree(node: dict[str, Any]) -> None:
            nid = node.get("id")
            if nid:
                node_by_id[nid] = node
            for child in node.get("children", []) or []:
                _index_tree(child)

        if root_node:
            _index_tree(root_node)

        editable_mode = bool(session.get("editable", False))

        def _summarize_descendants(branch_handle: str) -> dict[str, Any]:
            """Summarize descendant decisions."""
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

        def _auto_complete_ancestors(start_handle: str) -> None:
            """Auto-complete ancestors when all descendants decided."""
            current = start_handle
            while True:
                parent_handle = (handles.get(current) or {}).get("parent")
                if not parent_handle:
                    break

                parent_entry = state.get(parent_handle, {"status": "unseen", "selection_type": None})
                if parent_entry.get("selection_type") == "subtree":
                    current = parent_handle
                    continue

                summary = _summarize_descendants(parent_handle)
                if summary["descendant_count"] == 0:
                    break
                if summary["has_undecided"]:
                    break

                accepted = summary["accepted_leaf_count"]
                rejected = summary["rejected_leaf_count"]

                parent_entry = state.setdefault(parent_handle, {"status": "unseen", "selection_type": None})

                if accepted > 0:
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "finalized"
                        parent_entry["selection_type"] = "path"
                elif rejected > 0:
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "closed"
                        parent_entry["selection_type"] = None
                else:
                    break

                current = parent_handle

        # Apply actions (abbreviated version - see witness stone for full ~800 lines)
        actions = actions or []
        
        # 2-letter expander
        for action in actions:
            act = action.get("action")
            if act in EXPLORATION_ACTION_2LETTER:
                action["action"] = EXPLORATION_ACTION_2LETTER[act]
        
        # Aliases
        ACTION_ALIASES = {"reserve_branch_for_children": "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states"}
        for action in actions:
            act = action.get("action")
            if act in ACTION_ALIASES:
                action["action"] = ACTION_ALIASES[act]
        
        # Sort actions
        def action_sort_key(action: dict[str, Any]) -> tuple[int, int]:
            act = action.get("action", "")
            handle = action.get("handle", "")
            
            if act in {"engulf_leaf_into_gem_for_editing", "engulf_shell_in_gemstorm"}:
                category = 0
            elif act in {"preserve_leaf_in_ether_untouched", "preserve_branch_node_in_ether_untouched__when_no_engulfed_children"}:
                category = 1
            elif act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                category = 3
            elif act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                category = 4
            else:
                category = 2
            
            depth = -handle.count(".")
            return (category, depth)
        
        actions.sort(key=action_sort_key)
        
        # Use precomputed frontier
        session["handles"] = handles
        session["state"] = state
        if _precomputed_frontier is not None:
            current_step_frontier = _precomputed_frontier
        else:
            current_step_frontier = self._compute_exploration_frontier(
                session, frontier_size=frontier_size, max_depth_per_frontier=max_depth_per_frontier
            )
        
        peek_results = []
        
        # Process each action
        for action in actions:
            act = action.get("action")
            
            # Global actions (no handle required)
            if act in {"set_scratchpad", "append_scratchpad", "preserve_all_remaining_nodes_in_ether_at_finalization"}:
                if act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                    if exploration_mode != "dfs_guided_bulk":
                        raise NetworkError("preserve_all only in bulk mode")
                    
                    strict = session.get("strict_completeness", False)
                    if strict:
                        raise NetworkError("PA disabled in strict_completeness mode")
                    
                    preserved_count = 0
                    for h in handles:
                        h_state = state.get(h, {})
                        h_status = h_state.get("status")
                        if h_status in ("unseen", "candidate", "open", None):
                            h_summary = _summarize_descendants(h)
                            if h_summary["accepted_leaf_count"] > 0:
                                f_entry = state.setdefault(h, {"status": "unseen", "selection_type": None})
                                if f_entry.get("status") not in {"finalized", "closed"}:
                                    f_entry["status"] = "finalized"
                                    f_entry["selection_type"] = "path"
                            else:
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    logger.info(f"preserve_all: {preserved_count} preserved")
                    continue
                
                if act in {"set_scratchpad", "append_scratchpad"}:
                    content = action.get("content") or ""
                    existing = session.get("scratchpad") or ""
                    if act == "set_scratchpad":
                        session["scratchpad"] = content
                    else:
                        session["scratchpad"] = (existing + "\n" + content) if existing else content
                    continue

            # Handle-optional global actions (process before handle validation)
            if act == "abandon_lightning_strike":
                session.pop("_peek_frontier", None)
                session.pop("_peek_root_handle", None)
                session.pop("_peek_max_nodes", None)
                logger.info("abandon_lightning_strike: cleared lightning peek state")
                continue

            if act == "resume_guided_frontier":
                if "_search_frontier" in session:
                    del session["_search_frontier"]
                if "_peek_frontier" in session:
                    del session["_peek_frontier"]
                    del session["_peek_root_handle"]
                    del session["_peek_max_nodes"]
                    logger.info("resume: cleared peek")
                continue

            handle = action.get("handle")
            max_depth = action.get("max_depth")

            # Bulk frontier actions
            if act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                if exploration_mode != "dfs_guided_bulk":
                    raise NetworkError(f"{act} only in bulk mode")
                
                if handle not in handles:
                    raise NetworkError(f"Unknown handle: {handle}")
                
                def _is_desc(fh: str, branch: str) -> bool:
                    cur = fh
                    seen = set()
                    while cur and cur not in seen:
                        if cur == branch:
                            return True
                        seen.add(cur)
                        cur = handles.get(cur, {}).get("parent")
                    return False
                
                matching = [e for e in current_step_frontier if _is_desc(e["handle"], handle)]
                
                if not matching:
                    logger.info(f"Bulk on '{handle}': no matches")
                    continue
                
                if act == "engulf_all_showing_undecided_descendants_into_gem_for_editing":
                    engulfed_leaves = 0
                    engulfed_branches = 0
                    for entry in matching:
                        fh = entry["handle"]
                        if state.get(fh, {}).get("status") in {"finalized", "closed"}:
                            continue
                        if entry["is_leaf"]:
                            f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "leaf"
                            f_entry["max_depth"] = max_depth
                            _auto_complete_ancestors(fh)
                            engulfed_leaves += 1
                        else:
                            f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "subtree"
                            f_entry["max_depth"] = max_depth
                            f_entry["subtree_mode"] = "shell"
                            engulfed_branches += 1
                    logger.info(f"Engulfed {engulfed_leaves} leaves, {engulfed_branches} branches")
                elif act == "preserve_all_showing_undecided_descendants_in_ether":
                    preserved = 0
                    for entry in matching:
                        fh = entry["handle"]
                        if state.get(fh, {}).get("status") in {"finalized", "closed"}:
                            continue
                        f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                        f_entry["status"] = "closed"
                        _auto_complete_ancestors(fh)
                        preserved += 1
                    logger.info(f"Preserved {preserved} nodes")
                continue

            if handle not in handles:
                raise NetworkError(f"Unknown handle: {handle}")

            if act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm", "update_tag_and_engulf_in_gemstorm"} and not editable_mode:
                raise NetworkError("Update actions require editable=True")

            if handle not in state:
                state[handle] = {"status": "unseen", "max_depth": None, "selection_type": None}

            entry = state[handle]
            if "selection_type" not in entry:
                entry["selection_type"] = None

            # PEEK / LIGHTNING FLASH action
            if act in {"peek_descendants_as_frontier", "lightning_flash_into_section"}:
                from collections import deque
                max_nodes = action.get("max_nodes") or 200
                try:
                    max_nodes = int(max_nodes)
                except (TypeError, ValueError):
                    max_nodes = 200
                if max_nodes <= 0:
                    max_nodes = 200
                
                queue = deque([handle])
                visited = set()
                peek_handles = []
                
                while queue and len(peek_handles) < max_nodes:
                    h = queue.popleft()
                    if h in visited:
                        continue
                    visited.add(h)
                    peek_handles.append(h)
                    for ch in handles.get(h, {}).get("children", []) or []:
                        if ch not in visited:
                            queue.append(ch)

                # Hard constraint: only keep the requested section handle and its descendants
                root_handle = handle
                constrained_peek_handles: list[str] = []
                for h in peek_handles:
                    if h == root_handle or h.startswith(root_handle + "."):
                        constrained_peek_handles.append(h)
                peek_handles = constrained_peek_handles
                
                peek_frontier = []
                MAX_NOTE = None if editable_mode else 1024
                for h in peek_handles:
                    meta = handles.get(h, {}) or {}
                    st = state.get(h, {"status": "unseen"})
                    child_handles = meta.get("children", []) or []
                    is_leaf = not child_handles
                    note_full = meta.get("note") or ""
                    note_preview = note_full if MAX_NOTE is None else (note_full if len(note_full) <= MAX_NOTE else note_full[:MAX_NOTE])
                    guidance = "PEEK - leaf: EL, PL" if is_leaf else "PEEK - branch: RB, PB, EF, PF"
                    peek_frontier.append({
                        "handle": h,
                        "parent_handle": meta.get("parent"),
                        "name_preview": meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(child_handles),
                        "depth": meta.get("depth", 0),
                        "status": st.get("status", "candidate"),
                        "is_leaf": is_leaf,
                        "guidance": guidance,
                    })
                
                session["_peek_frontier"] = peek_frontier
                session["_peek_root_handle"] = handle
                session["_peek_max_nodes"] = max_nodes
                peek_results.append({
                    "root_handle": handle,
                    "max_nodes": max_nodes,
                    "nodes_returned": len(peek_frontier),
                    "truncated": len(visited) >= max_nodes,
                    "frontier": peek_frontier,
                })
                continue

            # ADD_HINT action
            if act == "add_hint":
                hint = action.get("hint")
                if not isinstance(hint, str) or not hint.strip():
                    raise NetworkError("add_hint requires non-empty hint")
                meta = handles.get(handle) or {}
                existing = meta.get("hints")
                if not isinstance(existing, list):
                    existing = []
                existing.append(hint)
                meta["hints"] = existing
                handles[handle] = meta
                continue

            # SKELETON WALK: section-level delete/merge operations over lightning strike
            if act in {
                "mark_section_for_deletion",
                "mark_section_as_merge_target",
                "mark_section_as_permanent",
            }:
                peek_frontier = session.get("_peek_frontier")
                peek_root = session.get("_peek_root_handle")
                if not peek_frontier or peek_root != handle:
                    raise NetworkError(f"{act} requires active lightning strike rooted at handle '{handle}'")

                peek_handles = {e.get("handle") for e in peek_frontier}

                if act == "mark_section_for_deletion":
                    nodes_to_salvage = action.get("nodes_to_salvage_for_move") or []
                    invalid = [h for h in nodes_to_salvage if h not in peek_handles]
                    if invalid:
                        raise NetworkError(f"nodes_to_salvage_for_move not in lightning frontier: {invalid}")

                    # Salvage selected nodes for later move/merge
                    for h_salvage in nodes_to_salvage:
                        if h_salvage not in handles:
                            raise NetworkError(f"Unknown salvage handle: {h_salvage}")
                        sal_meta = handles.get(h_salvage) or {}
                        sal_children = sal_meta.get("children", []) or []
                        sal_entry = state.setdefault(
                            h_salvage,
                            {"status": "unseen", "selection_type": None},
                        )
                        if not sal_children:
                            sal_entry["status"] = "finalized"
                            sal_entry["selection_type"] = "leaf"
                            sal_entry["max_depth"] = max_depth
                        else:
                            sal_entry["status"] = "finalized"
                            sal_entry["selection_type"] = "subtree"
                            sal_entry["max_depth"] = max_depth
                            sal_entry.pop("subtree_mode", None)
                        _auto_complete_ancestors(h_salvage)

                    # Mark the root section handle as a subtree shell slated for deletion
                    root_entry = state.setdefault(
                        handle,
                        {"status": "unseen", "selection_type": None},
                    )
                    root_entry["status"] = "finalized"
                    root_entry["selection_type"] = "subtree"
                    root_entry["max_depth"] = max_depth
                    root_entry["subtree_mode"] = "shell"

                    # Hint to suppress BFS for this branch
                    root_meta = handles.get(handle) or {}
                    root_hints = root_meta.get("hints") or []
                    root_hints.append("SKELETON_PRUNED")
                    root_meta["hints"] = root_hints
                    handles[handle] = root_meta

                    # Scratchpad log
                    scratch = session.get("scratchpad") or ""
                    salvage_str = ", ".join(nodes_to_salvage) if nodes_to_salvage else ""
                    line = f"[SKELETON] mark_section_for_deletion: root={handle}, salvaged=[{salvage_str}]"
                    session["scratchpad"] = (scratch + "\n" + line) if scratch else line

                else:
                    # mark_section_as_merge_target / mark_section_as_permanent share semantics
                    target_entry = state.setdefault(
                        handle,
                        {"status": "unseen", "selection_type": None},
                    )
                    target_entry["status"] = "finalized"
                    target_entry["selection_type"] = "subtree"
                    target_entry["max_depth"] = max_depth
                    # Ensure this is a full editable subtree, not a shell
                    target_entry.pop("subtree_mode", None)

                    meta = handles.get(handle) or {}
                    hints = meta.get("hints") or []
                    hints.append("SKELETON_MERGE_TARGET")
                    meta["hints"] = hints
                    handles[handle] = meta

                    scratch = session.get("scratchpad") or ""
                    line = f"[SKELETON] merge target = {handle}"
                    session["scratchpad"] = (scratch + "\n" + line) if scratch else line

                # End lightning strike for this section
                for key in ("_peek_frontier", "_peek_root_handle", "_peek_max_nodes"):
                    session.pop(key, None)
                continue

            # LEAF actions
            if act == "engulf_leaf_into_gem_for_editing":
                entry["status"] = "finalized"
                entry["selection_type"] = "leaf"
                entry["max_depth"] = max_depth
                _auto_complete_ancestors(handle)
            elif act == "preserve_leaf_in_ether_untouched":
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
                _auto_complete_ancestors(handle)
            
            # BRANCH actions
            elif act == "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states":
                summary = _summarize_descendants(handle)
                desc_count = summary["descendant_count"]
                has_undecided = summary["has_undecided"]
                accepted = summary["accepted_leaf_count"]

                if desc_count == 0:
                    raise NetworkError(f"{handle} has no descendants - use leaf actions")

                if exploration_mode == "dfs_guided_explicit":
                    if has_undecided or accepted > 0:
                        raise NetworkError(f"Strict mode: cannot engulf branch '{handle}' with undecided/engulfed descendants")

                if exploration_mode == "dfs_guided_bulk":
                    entry["status"] = "finalized"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    entry["subtree_mode"] = "shell"
                    continue

                # All descendants decided: include as shell
                entry["status"] = "finalized"
                entry["selection_type"] = "subtree"
                entry["max_depth"] = max_depth
                if summary["has_decided"] and not summary["has_undecided"]:
                    if accepted > 0:
                        _auto_complete_ancestors(handle)
                        continue
                    entry["subtree_mode"] = "shell"
                else:
                    entry["subtree_mode"] = "shell"
                _auto_complete_ancestors(handle)

            elif act == "preserve_branch_node_in_ether_untouched__when_no_engulfed_children":
                summary = _summarize_descendants(handle)
                if summary["descendant_count"] == 0:
                    raise NetworkError(f"{handle} has no descendants - use leaf preserve")

                if exploration_mode == "dfs_guided_explicit" and summary["has_undecided"]:
                    raise NetworkError("Strict mode: all descendants must be decided first")

                if exploration_mode == "dfs_guided_bulk":
                    preserved_count = 0
                    for h in handles:
                        if not h.startswith(handle + "."):
                            continue
                        h_state = state.get(h, {})
                        if h_state.get("status") == "finalized":
                            continue
                        elif h_state.get("status") in ("unseen", "candidate", "open"):
                            h_sum = _summarize_descendants(h)
                            if h_sum["accepted_leaf_count"] == 0:
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    logger.info(f"Smart preserve: {preserved_count} nodes")
                else:
                    if summary["accepted_leaf_count"] > 0:
                        raise NetworkError(f"Cannot preserve '{handle}' - has engulfed leaves")
                    entry["status"] = "closed"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                _auto_complete_ancestors(handle)

            # UPDATE actions (editable mode)
            elif act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm"}:
                if not editable_mode:
                    raise NetworkError("Update actions require editable=True")
                new_name = action.get("name")
                new_note = action.get("note")
                meta = handles.get(handle) or {}
                node_id = meta.get("id")
                if not node_id:
                    raise NetworkError(f"{handle} has no node id")
                target = node_by_id.get(node_id)
                if not target:
                    raise NetworkError(f"Node {node_id} not in tree")
                if new_name:
                    target["name"] = new_name
                    meta["name"] = new_name
                if new_note:
                    target["note"] = new_note
                    meta["note"] = new_note
                handles[handle] = meta
                child_handles = handles.get(handle, {}).get("children", []) or []
                entry = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                if not child_handles:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors(handle)
                else:
                    entry["status"] = "finalized"
                    if entry.get("selection_type") is None:
                        entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors(handle)

            elif act == "update_tag_and_engulf_in_gemstorm":
                if not editable_mode:
                    raise NetworkError("Update tag requires editable=True")
                raw_tag = action.get("tag")
                if not isinstance(raw_tag, str) or not raw_tag.strip():
                    raise NetworkError("Tag required")
                tag = raw_tag.strip()
                if not tag.startswith("#"):
                    tag = f"#{tag}"
                meta = handles.get(handle) or {}
                node_id = meta.get("id")
                if not node_id:
                    raise NetworkError(f"{handle} has no node id")
                target = node_by_id.get(node_id)
                if not target:
                    raise NetworkError(f"Node {node_id} not in tree")
                current_name = target.get("name") or ""
                if tag not in current_name.split():
                    new_name = f"{current_name} {tag}".strip()
                    target["name"] = new_name
                    meta["name"] = new_name
                    handles[handle] = meta
                child_handles = handles.get(handle, {}).get("children", []) or []
                entry = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                if not child_handles:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    _auto_complete_ancestors(handle)
                else:
                    entry["status"] = "finalized"
                    if not entry.get("selection_type"):
                        entry["selection_type"] = "subtree"
                    _auto_complete_ancestors(handle)
            else:
                raise NetworkError(f"Unsupported action: {act}")
        
        session["handles"] = handles
        session["state"] = state
        session["steps"] = int(session.get("steps", 0)) + 1
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"

        try:
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to persist session: {e}") from e

        return {
            "success": True,
            "session_id": session_id,
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "scratchpad": session.get("scratchpad", ""),
            "skipped_decisions": skipped_decisions,
        }

    async def nexus_finalize_exploration(
        self,
        session_id: str,
        include_terrain: bool = True,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """Finalize exploration into NEXUS artifacts."""
        import json as json_module
        import copy

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")

        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load session: {e}") from e

        nexus_tag = session.get("nexus_tag")
        if not nexus_tag:
            raise NetworkError(f"Session missing nexus_tag")

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}

        # Build indexes
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

        # Compute preserved IDs
        explicitly_preserved_ids: set[str] = set()
        preserved_subtree_roots: set[str] = set()

        for handle_key, st in state.items():
            if handle_key == "R":
                continue
            status = st.get("status")
            if status != "closed":
                continue
            meta = handles.get(handle_key) or {}
            node_id = meta.get("id")
            if not node_id:
                continue
            child_handles = handles.get(handle_key, {}).get("children", []) or []
            if child_handles and st.get("selection_type") == "subtree":
                preserved_subtree_roots.add(node_id)
            else:
                explicitly_preserved_ids.add(node_id)

        # Expand preserved subtrees
        for root_id in preserved_subtree_roots:
            if root_id not in node_by_id:
                continue
            stack = [root_id]
            while stack:
                cur = stack.pop()
                if cur in explicitly_preserved_ids:
                    continue
                explicitly_preserved_ids.add(cur)
                for child_id in children_by_id.get(cur, []):
                    stack.append(child_id)

        original_ids_seen: set[str] = set(node_by_id.keys())

        # Collect finalized entries
        finalized_entries = []
        for handle, st in state.items():
            if st.get("status") != "finalized":
                continue
            meta = handles.get(handle) or {}
            node_id = meta.get("id")
            if not node_id:
                continue
            selection_type = st.get("selection_type") or "subtree"
            max_depth = st.get("max_depth")
            finalized_entries.append((handle, node_id, selection_type, max_depth))

        if not finalized_entries:
            raise NetworkError("No finalized paths - must accept at least one leaf or branch")

        # Build needed IDs from finalized entries
        handle_by_node_id = {}
        for h, meta in handles.items():
            nid = meta.get("id")
            if nid:
                handle_by_node_id[nid] = h

        needed_ids: set[str] = set()
        subtree_shells: set[str] = set()
        true_subtrees: set[str] = set()

        for handle, node_id, sel_type, _max_depth in finalized_entries:
            if sel_type != "subtree":
                continue
            st = state.get(handle, {})
            if st.get("subtree_mode") == "shell":
                subtree_shells.add(node_id)
            else:
                true_subtrees.add(node_id)

        # Include full subtrees
        for node_id in true_subtrees:
            if node_id not in node_by_id:
                continue
            stack = [node_id]
            while stack:
                cur = stack.pop()
                if cur in needed_ids or cur in explicitly_preserved_ids:
                    continue
                needed_ids.add(cur)
                for child_id in children_by_id.get(cur, []):
                    stack.append(child_id)

        # Include shells
        for node_id in subtree_shells:
            if node_id in node_by_id:
                needed_ids.add(node_id)

        # Include accepted leaves + ancestors
        accepted_leaf_ids = set()
        for nid in node_by_id.keys():
            if children_by_id.get(nid):
                continue
            handle = handle_by_node_id.get(nid)
            if not handle:
                continue
            st = state.get(handle, {})
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
            raise NetworkError("Empty minimal covering tree")

        # Ensure connected to root
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

        # Copy pruned tree
        def copy_pruned(node: dict[str, Any]) -> dict[str, Any] | None:
            nid = node.get("id")
            if nid not in needed_ids:
                return None
            new_node = {k: v for k, v in node.items() if k != "children"}
            if nid in subtree_shells:
                new_node["subtree_mode"] = "shell"
            new_children = []
            for child in node.get("children", []) or []:
                pruned = copy_pruned(child)
                if pruned:
                    new_children.append(pruned)
            new_node["children"] = new_children
            return new_node

        pruned_root = copy_pruned(root_node)
        if not pruned_root:
            raise NetworkError("Root pruned - no nodes available")

        gem_nodes = pruned_root.get("children", [])

        # Project to original view
        coarse_nodes = copy.deepcopy(gem_nodes)

        def _project_original(node: dict) -> None:
            if "original_name" in node:
                node["name"] = node.get("original_name")
            if "original_note" in node:
                node["note"] = node.get("original_note")
            node.pop("original_name", None)
            node.pop("original_note", None)
            for child in node.get("children") or []:
                _project_original(child)

        for n in coarse_nodes:
            _project_original(n)

        # Initialize NEXUS run dir
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
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

        export_root_id = session.get("root_id")
        export_root_name = session.get("root_name") or root_node.get("name", "Root")

        # Compute preview
        exploration_preview = None
        try:
            exploration_preview = self._annotate_preview_ids_and_build_tree(coarse_nodes, "CT")
        except Exception:
            pass
        
        gem_wrapper = {
            "export_timestamp": None,
            "export_root_children_status": "complete",
            "__preview_tree__": exploration_preview,
            "export_root_id": export_root_id,
            "export_root_name": export_root_name,
            "nodes": coarse_nodes,
            "original_ids_seen": sorted(original_ids_seen),
            "explicitly_preserved_ids": sorted(explicitly_preserved_ids),
        }

        with open(phantom_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        with open(coarse_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        with open(shimmering_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        return {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "phantom_gem": str(phantom_path),
            "shimmering_terrain": str(shimmering_path),
            "node_count": len(needed_ids),
            "mode": mode,
        }
