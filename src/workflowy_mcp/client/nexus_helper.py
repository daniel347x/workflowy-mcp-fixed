"""Shared NEXUS / GLIMPSE helper functions.

This module centralizes logic that was previously inlined inside
WorkFlowyClient methods (notably ``nexus_glimpse``) so that both
``nexus_glimpse`` *and* ``workflowy_glimpse`` (with an ``output_file``
option) can share the exact same TERRAIN-export semantics.

Key responsibilities:
- Detect hidden children by comparing WebSocket GLIMPSE and API GLIMPSE FULL
  (has_hidden_children + children_status per node).
- Provide a conservative fallback when API data is unavailable
  (mark_all_nodes_as_potentially_truncated).
- Build the ``original_ids_seen`` ledger used by Option-B deletion logic
  in the NEXUS WEAVE ("visible-then-removed" semantics under truncated
  parents).
- Construct the canonical TERRAIN wrapper used by NEXUS:

    {
      "export_root_id": str,
      "export_root_name": str,
      "export_timestamp": Any | None,
      "export_root_children_status": str,
      "original_ids_seen": list[str],
      "nodes": list[dict]
    }

These helpers are intentionally **pure** (no filesystem or network
side-effects). Callers are responsible for:
- Performing the underlying WebSocket GLIMPSE and/or API GLIMPSE FULL
  calls and passing the result dictionaries in.
- Writing the returned TERRAIN wrapper to disk if desired.

The code here is a direct extraction of the logic that was previously
inlined in ``WorkFlowyClient.nexus_glimpse`` and
``WorkFlowyClient._mark_all_nodes_as_potentially_truncated``, with
minimal adaptation to make it standalone.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

JsonDict = Dict[str, Any]


def mark_all_nodes_as_potentially_truncated(nodes: List[JsonDict]) -> None:
    """Mark ALL nodes as potentially having hidden children (UNSAFE fallback).

    This is the conservative fallback used when an API SCRY/GLIMPSE FULL
    is not available to validate true child counts. It ensures that
    downstream WEAVE logic will treat every node as if it might have
    collapsed/hidden children, preventing accidental deletions.

    Semantics applied to each node in-place:
    - ``has_hidden_children = True``
    - ``children_status = "truncated_by_expansion"``

    Args:
        nodes: Node tree to mark (modified in-place).
    """
    for node in nodes:
        node["has_hidden_children"] = True
        node["children_status"] = "truncated_by_expansion"
        grandchildren = node.get("children", []) or []
        if grandchildren:
            mark_all_nodes_as_potentially_truncated(grandchildren)


def _index_nodes_by_id(nodes: List[JsonDict]) -> Dict[str, JsonDict]:
    """Build a flat ``id -> node`` index from a hierarchical node list.

    This helper is intentionally minimal – it only cares about the
    ``id`` and ``children`` fields that ``merge_glimpses_for_terrain``
    needs.
    """
    by_id: Dict[str, JsonDict] = {}

    def _walk(lst: List[JsonDict]) -> None:
        for n in lst:
            if not isinstance(n, dict):
                continue
            nid = n.get("id")
            if nid:
                by_id[nid] = n
            children = n.get("children", []) or []
            if children:
                _walk(children)

    _walk(nodes)
    return by_id


def merge_glimpses_for_terrain(
    workflowy_root_id: str,
    ws_glimpse: JsonDict,
    api_glimpse: Optional[JsonDict] = None,
) -> Tuple[List[JsonDict], str, bool]:
    """Merge WebSocket + API GLIMPSE results to annotate hidden children.

    This is the core logic that NEXUS uses to produce safe TERRAIN data
    from a WebSocket GLIMPSE (what Dan has expanded in the UI) and an
    optional API GLIMPSE FULL (the complete tree regardless of expansion
    state).

    When API data is available and successful:
    - Each WebSocket node gets ``has_hidden_children`` and
      ``children_status`` set by comparing its child count to the
      corresponding API node.
    - The root's children_status is computed similarly by comparing the
      top-level child counts.

    When API data is unavailable or failed:
    - All nodes are marked via :func:`mark_all_nodes_as_potentially_truncated`.
    - ``root_children_status`` is set to ``"truncated_by_expansion"``.

    Args:
        workflowy_root_id: UUID of the Workflowy root being glimpsed.
        ws_glimpse: Result dict from the WebSocket GLIMPSE (Dan's UI view).
        api_glimpse: Optional result dict from ``workflowy_scry`` / GLIMPSE
            FULL. If ``None`` or unsuccessful, the conservative fallback is
            used.

    Returns:
        (children, root_children_status, api_merge_performed)

        - children: List of child nodes to be used under ``"nodes"`` in the
          TERRAIN wrapper (mutated in-place from ``ws_glimpse['children']``).
        - root_children_status: ``"complete"`` or ``"truncated_by_expansion"``
          describing the root's immediate children.
        - api_merge_performed: ``True`` if we actually compared against API
          structure; ``False`` if we fell back to WebSocket-only semantics.
    """
    # Default values for the root
    root_children_status = "complete"
    api_merge_performed = False

    ws_children: List[JsonDict] = ws_glimpse.get("children", []) or []

    # If we have a successful API GLIMPSE, attempt the full merge.
    if api_glimpse and api_glimpse.get("success"):
        api_children = api_glimpse.get("children", []) or []
        api_nodes_by_id = _index_nodes_by_id(api_children)
        api_merge_performed = True

        nodes_with_hidden = 0

        def _merge_flags(nodes: List[JsonDict]) -> None:
            nonlocal nodes_with_hidden
            for ws_node in nodes:
                if not isinstance(ws_node, dict):
                    continue
                nid = ws_node.get("id")
                if not nid:
                    # New node (no UUID) – cannot have hidden children in ETHER
                    ws_node["has_hidden_children"] = False
                    ws_node["children_status"] = "complete"
                    children = ws_node.get("children", []) or []
                    if children:
                        _merge_flags(children)
                    continue

                api_node = api_nodes_by_id.get(nid)
                if not api_node:
                    # No API counterpart – treat as complete but logically
                    # this should be rare (GLIMPSE should be subset of SCRY).
                    ws_node["has_hidden_children"] = False
                    ws_node["children_status"] = "complete"
                    children = ws_node.get("children", []) or []
                    if children:
                        _merge_flags(children)
                    continue

                ws_count = len(ws_node.get("children", []) or [])
                api_count = len(api_node.get("children", []) or [])

                if api_count > ws_count:
                    ws_node["has_hidden_children"] = True
                    ws_node["children_status"] = "truncated_by_expansion"
                    nodes_with_hidden += 1
                else:
                    ws_node["has_hidden_children"] = False
                    ws_node["children_status"] = "complete"

                children = ws_node.get("children", []) or []
                if children:
                    _merge_flags(children)

        _merge_flags(ws_children)

        # Root-level children_status: compare top-level children counts.
        api_root_children = api_glimpse.get("children", []) or []
        ws_root_children_count = len(ws_children)
        api_root_children_count = len(api_root_children)

        if api_root_children_count > ws_root_children_count:
            root_children_status = "truncated_by_expansion"
        else:
            root_children_status = "complete"

        return ws_children, root_children_status, api_merge_performed

    # No usable API GLIMPSE – conservative fallback for whole tree.
    mark_all_nodes_as_potentially_truncated(ws_children)
    root_children_status = "truncated_by_expansion"
    api_merge_performed = False

    return ws_children, root_children_status, api_merge_performed


def build_original_ids_seen_from_glimpses(
    workflowy_root_id: str,
    ws_glimpse: JsonDict,
    api_glimpse: Optional[JsonDict] = None,
) -> Set[str]:
    """Build the ``original_ids_seen`` ledger from GLIMPSE results.

    This ledger is used by the NEXUS WEAVE deletion logic (Option B) to
    distinguish "visible-then-removed" nodes under truncated parents
    from nodes that were simply never loaded.

    Preference order:
    - If a successful API GLIMPSE is available, we derive the ledger from
      the full API tree under the same root.
    - Otherwise, we fall back to the WebSocket GLIMPSE tree.

    Args:
        workflowy_root_id: UUID of the Workflowy root being glimpsed.
        ws_glimpse: WebSocket GLIMPSE dict.
        api_glimpse: Optional API GLIMPSE dict from ``workflowy_scry``.

    Returns:
        A set of all Workflowy node IDs known at export time, including the
        root itself when available.
    """
    original_ids_seen: Set[str] = set()

    try:
        if api_glimpse and api_glimpse.get("success"):
            api_root = api_glimpse.get("root") or {}
            api_children = api_glimpse.get("children") or []
            root_id_for_ledger = api_root.get("id") or workflowy_root_id
            if root_id_for_ledger:
                original_ids_seen.add(str(root_id_for_ledger))

            def _collect_api(nodes: List[JsonDict]) -> None:
                for n in nodes or []:
                    if not isinstance(n, dict):
                        continue
                    nid = n.get("id")
                    if nid:
                        original_ids_seen.add(str(nid))
                    _collect_api(n.get("children") or [])

            _collect_api(api_children)
        else:
            ws_root = ws_glimpse.get("root") or {}
            ws_children = ws_glimpse.get("children") or []
            root_id_for_ledger = ws_root.get("id") or workflowy_root_id
            if root_id_for_ledger:
                original_ids_seen.add(str(root_id_for_ledger))

            def _collect_ws(nodes: List[JsonDict]) -> None:
                for n in nodes or []:
                    if not isinstance(n, dict):
                        continue
                    nid = n.get("id")
                    if nid:
                        original_ids_seen.add(str(nid))
                    _collect_ws(n.get("children") or [])

            _collect_ws(ws_children)
    except Exception:
        # Ledger is advisory metadata only; failure to compute it must
        # never break GLIMPSE or NEXUS.
        return set()

    return original_ids_seen


def build_terrain_export_from_glimpse(
    workflowy_root_id: str,
    ws_glimpse: JsonDict,
    children: List[JsonDict],
    root_children_status: str,
    original_ids_seen: Set[str],
) -> JsonDict:
    """Construct a TERRAIN-style export wrapper from GLIMPSE data.

    This mirrors the structure produced by ``bulk_export_to_file`` and
    used throughout the NEXUS pipeline (SCRY / GLIMPSE / IGNITE / ANCHOR).

    Args:
        workflowy_root_id: UUID of the Workflowy root being exported.
        ws_glimpse: WebSocket GLIMPSE dict (used for root name).
        children: List of child nodes (typically merged via
            :func:`merge_glimpses_for_terrain`).
        root_children_status: Children status for the export root, e.g.
            ``"complete"`` or ``"truncated_by_expansion"``.
        original_ids_seen: Set of all node IDs known at export time.

    Returns:
        A dict suitable for JSON dumping as a TERRAIN export.
    """
    root = ws_glimpse.get("root") or {}
    export_root_name = root.get("name") or "Root"

    terrain_data: JsonDict = {
        "export_root_id": workflowy_root_id,
        "export_root_name": export_root_name,
        "export_timestamp": None,  # GLIMPSE path does not track timestamps
        "export_root_children_status": root_children_status,
        "original_ids_seen": sorted(original_ids_seen),
        "nodes": children,
    }
    return terrain_data


__all__ = [
    "mark_all_nodes_as_potentially_truncated",
    "merge_glimpses_for_terrain",
    "build_original_ids_seen_from_glimpses",
    "build_terrain_export_from_glimpse",
]
