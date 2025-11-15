"""
Move-aware tree reconciliation for Workflowy

This module provides a compact, robust reconciliation algorithm that syncs a
source JSON tree to an existing Workflowy subtree, preserving UUIDs via
move_node instead of delete+create.

Phases:
  1) Create (top-down)
  2) Move (root-to-leaf)
  3) Reorder siblings per parent (using only position="top"/"bottom")
  4) Update content
  5) Delete (delete-roots only)

To integrate, wire the API functions:
  - list_nodes(parent_id) -> List[dict]
  - create_node(parent_id, data: dict) -> str (new_id)
  - update_node(node_id, data: dict) -> None
  - delete_node(node_id) -> None
  - move_node(node_id, new_parent_id, position: str = "top" | "bottom") -> None

The algorithm expects list_nodes to return immediate children of a parent, and
nodes shaped roughly like GLIMPSE data (id, name, note, data, completed, etc.).

Reordering with only top/bottom:
- To realize an exact sibling order with only top/bottom control, move each desired
  child to TOP in reverse desired order (or to BOTTOM in forward desired order).
- After this pass, desired children appear in exact order; any extra/non-desired
  siblings remain above (for TOP) or below (for BOTTOM) and will be removed in the
  delete phase.
"""
from collections import deque, defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Optional, Iterable

Node = Dict


async def reconcile_tree(
    source_json: List[Node],
    parent_uuid: str,
    list_nodes: Callable[[str], List[Node]],
    create_node: Callable[[str, Dict], str],
    update_node: Callable[[str, Dict], None],
    delete_node: Callable[[str], None],
    move_node: Callable[..., None],
    export_nodes: Callable[[str], Dict] = None,
) -> None:
    """
    Reconcile the Workflowy subtree under parent_uuid to match source_json.

    - source_json: list of nodes (each may have 'id', 'name', 'note', 'data', 'completed', 'children')
    - parent_uuid: the Workflowy parent where source_json should be synced
    - API functions: list_nodes, create_node, update_node, delete_node, move_node

    The function performs Create -> Move -> Reorder -> Update -> Delete.
    """
    
    # Open debug log file
    debug_log = open(r'E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\reconcile_debug.log', 'w', encoding='utf-8')
    
    def log(msg):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm
        debug_log.write(f"[{timestamp}] {msg}\n")
        debug_log.flush()

    # ------------- helpers -------------
    def index_source(nodes: List[Node], parent_id: Optional[str] = None, order_acc: Optional[List[Node]] = None) -> List[Node]:
        # Preorder traversal; annotate desired parent and index
        if order_acc is None:
            order_acc = []
        for i, n in enumerate(nodes):
            n['_desired_parent'] = parent_id
            n['_desired_index'] = i
            order_acc.append(n)
            index_source(n.get('children', []), n.get('id'), order_acc)
        return order_acc

    async def snapshot_target(root_parent: str, use_efficient_traversal: bool = False, export_func = None):
        # Snapshot existing subtree under root_parent to map nodes
        Map_T: Dict[str, Node] = {}
        Parent_T: Dict[str, Optional[str]] = {}
        Children_T: Dict[str, List[str]] = defaultdict(list)
        
        if use_efficient_traversal:
            # BFS traversal using list_nodes (CAUSES RATE LIMITS - avoid!)
            q: deque[str] = deque([root_parent])
            visited_parents: set[str] = set()
            while q:
                p = q.popleft()
                if p in visited_parents:
                    continue
                visited_parents.add(p)
                children = await list_nodes(p)
                for ch in children:
                    cid = ch['id']
                    Map_T[cid] = ch
                    Parent_T[cid] = p
                    Children_T[p].append(cid)
                    q.append(cid)
        else:
            # RECOMMENDED: Single bulk export (empirically better, avoids rate limits)
            # This is how glimpseFull works - one API call gets everything
            log(f"   Using bulk export for snapshot (avoids rate limit issues)")
            
            if export_func is None:
                log(f"   WARNING: export_nodes not provided, falling back to BFS list_nodes (slow!)")
                # Fallback to BFS if export_nodes not available
                q: deque[str] = deque([root_parent])
                visited_parents: set[str] = set()
                while q:
                    p = q.popleft()
                    if p in visited_parents:
                        continue
                    visited_parents.add(p)
                    children = await list_nodes(p)
                    for ch in children:
                        cid = ch['id']
                        Map_T[cid] = ch
                        Parent_T[cid] = p
                        Children_T[p].append(cid)
                        q.append(cid)
            else:
                # Use bulk export - single API call!
                log(f"   Calling export_nodes({root_parent})...")
                raw_data = await export_func(root_parent)
                all_nodes = raw_data.get('nodes', [])
                log(f"   Bulk export returned {len(all_nodes)} nodes")
                
                # Build parent/children maps from flat list
                for node in all_nodes:
                    nid = node['id']
                    Map_T[nid] = node
                    parent_id = node.get('parent_id')
                    Parent_T[nid] = parent_id
                    if parent_id is not None:
                        Children_T[parent_id].append(nid)
        
        return Map_T, Parent_T, Children_T

    def desired_maps(source_nodes: List[Node], root_parent: str):
        Map_S: Dict[Optional[str], Node] = {}
        Parent_S: Dict[str, Optional[str]] = {}
        Order_S: Dict[Optional[str], List[Optional[str]]] = defaultdict(list)
        # Set desired parents for top-level to root_parent
        for i, n in enumerate(source_nodes):
            n['_desired_parent'] = root_parent
            n['_desired_index'] = i
        seq = index_source(source_nodes, root_parent)  # fills _desired_parent/_desired_index recursively
        for n in seq:
            nid = n.get('id')
            Map_S[nid] = n  # nid can be None for new nodes
            p = n.get('_desired_parent')
            if nid is not None:
                Parent_S[nid] = p
            if p is not None:
                Order_S[p].append(nid)
        return Map_S, Parent_S, Order_S

    def compute_delete_roots(to_delete: Iterable[str], Parent_T: Dict[str, Optional[str]]):
        to_delete_set = set(to_delete)
        roots: set[str] = set()
        for d in to_delete_set:
            cur = Parent_T.get(d)
            is_root = True
            while cur is not None:
                if cur in to_delete_set:
                    is_root = False
                    break
                cur = Parent_T.get(cur)
            if is_root:
                roots.add(d)
        return roots

    def node_data_equal(src: Optional[Node], tgt: Optional[Node]) -> bool:
        if src is None or tgt is None:
            return False
        return (
            src.get('name') == tgt.get('name') and
            (src.get('note') or None) == (tgt.get('note') or None) and
            (src.get('data') or {}) == (tgt.get('data') or {}) and
            bool(src.get('completed', False)) == bool(tgt.get('completed', False))
        )

    # ------------- phase 0: build maps -------------
    log(f"\n[PHASE 0] Building maps for parent {parent_uuid}")
    Map_T, Parent_T, Children_T = await snapshot_target(parent_uuid, use_efficient_traversal=False, export_func=export_nodes)
    log(f"   Target snapshot: {len(Map_T)} nodes found in Workflowy")
    log(f"   Target UUIDs: {list(Map_T.keys())}")
    Map_S, Parent_S, Order_S = desired_maps(source_json, parent_uuid)
    log(f"   Source JSON: {len(Map_S)} nodes expected")
    log(f"   Source UUIDs: {list(Map_S.keys())}")
    log(f"   Source UUIDs that are NOT None: {[k for k in Map_S.keys() if k is not None]}")
    log(f"   UUIDs in BOTH: {set(Map_T.keys()) & set(k for k in Map_S.keys() if k is not None)}")
    log(f"   UUIDs ONLY in Target: {set(Map_T.keys()) - set(k for k in Map_S.keys() if k is not None)}")
    log(f"   UUIDs ONLY in Source: {set(k for k in Map_S.keys() if k is not None) - set(Map_T.keys())}")

    # ------------- phase 1: CREATE (top-down) -------------
    create_count = 0
    async def ensure_created(nodes: List[Node], desired_parent: Optional[str]):
        nonlocal create_count
        for n in nodes:
            nid = n.get('id')
            node_name = n.get('name', 'unnamed')
            log(f"   Checking if exists: {nid} (name: {node_name})")
            if not nid or nid not in Map_T:
                create_count += 1
                log(f"      >>> CREATING new node '{node_name}' under parent {desired_parent}")
                payload = {
                    'name': n.get('name'),
                    'note': n.get('note'),
                    'data': n.get('data'),
                    'completed': bool(n.get('completed', False)),
                }
                new_id = await create_node(desired_parent, payload)
                log(f"      >>> CREATED with new UUID: {new_id}")
                n['id'] = new_id
                nid = new_id
                # reflect in target maps immediately
                Map_T[nid] = {'id': nid, 'parent_id': desired_parent, **payload}
                Parent_T[nid] = desired_parent
                if desired_parent is not None:
                    Children_T[desired_parent].append(nid)
            else:
                log(f"      (already exists, skipping create)")
            # recurse
            await ensure_created(n.get('children', []), nid)

    log(f"\n[PHASE 1] CREATE (top-down)")
    await ensure_created(source_json, parent_uuid)
    log(f"   CREATE phase complete - created {create_count} new nodes")

    # Recompute desired maps now that new nodes have IDs
    Map_S, Parent_S, Order_S = desired_maps(source_json, parent_uuid)

    # ------------- phase 2: MOVE (root-to-leaf following source) -------------
    log(f"\n[PHASE 2] MOVE (root-to-leaf)")
    def preorder(nodes: List[Node]):
        for n in nodes:
            yield n
            for c in preorder(n.get('children', [])):
                yield c

    move_count = 0
    for n in preorder(source_json):
        nid = n['id']
        desired_parent = n.get('_desired_parent')
        current_parent = Parent_T.get(nid)
        log(f"   Checking node {nid} (name: {n.get('name')})")
        log(f"      Current parent: {current_parent}")
        log(f"      Desired parent: {desired_parent}")
        if desired_parent != current_parent:
            move_count += 1
            log(f"      >>> MOVING {nid} from {current_parent} to {desired_parent}")
            # reparent; position will be normalized in reorder phase
            await move_node(nid, desired_parent, position="bottom")
            # update target maps
            if current_parent and nid in Children_T[current_parent]:
                Children_T[current_parent].remove(nid)
            Parent_T[nid] = desired_parent
            if desired_parent is not None:
                Children_T[desired_parent].append(nid)
        else:
            log(f"      (no move needed)")
    log(f"   MOVE phase complete - moved {move_count} nodes")

    # ------------- phase 3: REORDER siblings per parent -------------
    log(f"\n[PHASE 3] REORDER siblings")
    # With only top/bottom available, enforce exact order by moving each desired child to TOP in reverse order.
    # This yields the exact desired sequence at the top of the list; any extra siblings remain below and
    # will be removed in the delete phase.
    reorder_count = 0
    for p, desired in Order_S.items():
        if p is None:
            continue
        desired_ids = [x for x in desired if x is not None]
        log(f"   Parent {p}: Reordering {len(desired_ids)} children")
        log(f"      Desired order (reversed for TOP positioning): {list(reversed(desired_ids))}")
        for cid in reversed(desired_ids):
            reorder_count += 1
            log(f"      >>> REORDER: Moving {cid} to TOP of parent {p}")
            await move_node(cid, p, position="top")
    log(f"   REORDER phase complete - processed {len(Order_S)} parents, {reorder_count} reorder moves")

    # ------------- phase 4: UPDATE content -------------
    log(f"\n[PHASE 4] UPDATE content")
    for nid, tgt in list(Map_T.items()):
        src = Map_S.get(nid)
        if src:
            if not node_data_equal(src, tgt):
                payload = {
                    'name': src.get('name'),
                    'note': src.get('note'),
                    'data': src.get('data'),
                    'completed': bool(src.get('completed', False)),
                }
                await update_node(nid, payload)
    log(f"   UPDATE phase complete")

    # ------------- phase 5: DELETE (delete-roots only) -------------
    log(f"\n[PHASE 5] DELETE (delete-roots only)")
    ids_in_source = {n['id'] for n in preorder(source_json)}
    log(f"   Source IDs (from preorder): {ids_in_source}")

    # Snapshot Map_T again under the parent to catch any nodes not visited in source
    log(f"   Re-snapshotting target under parent {parent_uuid}...")
    Map_T2, Parent_T2, _ = await snapshot_target(parent_uuid, use_efficient_traversal=False, export_func=export_nodes)
    ids_in_target = set(Map_T2.keys())
    log(f"   Target IDs (after reconciliation): {ids_in_target}")

    to_delete = ids_in_target - ids_in_source
    log(f"   To delete (target - source): {to_delete}")
    delete_roots = compute_delete_roots(to_delete, Parent_T2)
    log(f"   Delete roots: {delete_roots}")
    log(f"   Target has {len(ids_in_target)} nodes, Source expects {len(ids_in_source)} nodes")
    log(f"   To delete: {len(to_delete)} nodes, Delete roots: {len(delete_roots)} nodes")
    for d in delete_roots:
        log(f"   >>> DELETING root: {d} (name: {Map_T2.get(d, {}).get('name', 'unknown')})")
        await delete_node(d)
    log(f"   DELETE phase complete")
    
    debug_log.close()


__all__ = [
    'reconcile_tree',
]
