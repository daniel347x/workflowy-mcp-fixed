"""
Move-aware tree reconciliation for Workflowy (with guardrails + dry-run + summary)

This module provides a compact, robust reconciliation algorithm that syncs a
source JSON tree to an existing Workflowy subtree, preserving UUIDs via
move_node instead of delete+create.

Phases:
  1) Create (top-down)
  2) Move (root-to-leaf)
  3) Reorder siblings per parent (using only position="top"/"bottom")
  4) Update content
  5) Delete (delete-roots only)

Guardrails added:
  - Support "root in file": if the source payload includes export_root_id (or can infer
    a single intended parent from top-level parent_id), we detect mismatches.
  - import_policy: 'strict' | 'rebase' | 'clone'
      * strict (default): abort on parent mismatch (no changes)
      * rebase: override parent_uuid with the detected export_root_id
      * clone: strip all IDs from source before reconcile and SKIP DELETE phase
  - Never delete the reconciliation root: exclude parent_uuid from delete candidates
    and from snapshot maps.
  - Protect parents referenced in source: do not delete any ID that appears as a
    parent_id in the source payload (protects container nodes like ARC RULES).
  - Dry-run and safety thresholds: dry_run prints planned operations without changing
    anything; max_delete_threshold requires force=True when exceeded.
  - Summary: writes an end-of-run summary to the log and returns a structured plan
    object when dry_run=True.

To integrate, wire the API functions:
  - list_nodes(parent_id) -> List[dict]
  - create_node(parent_id, data: dict) -> str (new_id)
  - update_node(node_id, data: dict) -> None
  - delete_node(node_id) -> None
  - move_node(node_id, new_parent_id, position: str = "top" | "bottom") -> None
  - export_nodes(parent_id) -> Dict  (optional, for efficient snapshot)

The algorithm expects list_nodes to return immediate children of a parent, and
nodes shaped roughly like GLIMPSE data (id, name, note, data, completed, etc.).

Reordering with only top/bottom:
- To realize an exact sibling order with only top/bottom control, move each desired
  child to TOP in reverse desired order (or to BOTTOM in forward desired order).
- After this pass, desired children appear in exact order; any extra/non-desired
  siblings remain above (for TOP) or below (for BOTTOM) and will be removed in the
  delete phase (unless import_policy == 'clone').
"""
from collections import deque, defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Optional, Iterable, Union, Any
import json
import os

Node = Dict

async def _perform_backup(
    parent_uuid_to_backup: str,
    export_nodes: Callable[[str], Dict],
    create_node: Callable[[str, Dict], str],
    list_nodes: Callable[[str], List[Node]],
    log: Callable[[str], None]
):
    """Exports a node tree and re-imports it into a dated backup location."""
    
    NEXUS_BACKUP_ROOT_ID = "c9fdeebf-b201-457b-8b91-00226b024444"
    
    log(f"[BACKUP] Starting point-in-time backup for node: {parent_uuid_to_backup}")

    try:
        # 1. Export the current state of the target node
        exported_data = await export_nodes(parent_uuid_to_backup)
        original_node_name = exported_data.get("export_root_name", "Unknown Node")
        log(f"[BACKUP] Successfully exported '{original_node_name}' with {len(exported_data.get('nodes', []))} child nodes.")

        # 2. Determine the backup destination folder (YYYY-MM)
        current_date = datetime.now()
        folder_name = current_date.strftime("%Y-%m")
        
        # Check if the folder already exists
        nexus_children = await list_nodes(NEXUS_BACKUP_ROOT_ID)
        backup_folder_id = None
        for child in nexus_children:
            if child.get("name") == folder_name:
                backup_folder_id = child.get("id")
                log(f"[BACKUP] Found existing backup folder '{folder_name}' with ID: {backup_folder_id}")
                break
        
        if not backup_folder_id:
            log(f"[BACKUP] No backup folder for '{folder_name}' found. Creating it now.")
            new_folder_node = {
                'name': folder_name,
                'note': f'Backups for {current_date.strftime("%B %Y")}'
            }
            backup_folder_id = await create_node(NEXUS_BACKUP_ROOT_ID, new_folder_node)
            log(f"[BACKUP] Created new backup folder with ID: {backup_folder_id}")

        # 3. Create the root node for this specific backup
        backup_node_name = f"Backup - {original_node_name} - {current_date.strftime('%Y-%m-%d %H:%M:%S')}"
        backup_root_payload = {'name': backup_node_name}
        new_backup_root_id = await create_node(backup_folder_id, backup_root_payload)
        log(f"[BACKUP] Created root backup node '{backup_node_name}' with ID: {new_backup_root_id}")

        # 4. Recursively import the exported nodes under the new backup root
        async def recursive_import(nodes: List[Node], new_parent_id: str):
            for node_data in nodes:
                children = node_data.pop("children", [])
                # Clean up metadata that shouldn't be copied
                node_data.pop("id", None)
                node_data.pop("parent_id", None)
                
                new_node_id = await create_node(new_parent_id, node_data)
                if children:
                    await recursive_import(children, new_node_id)
        
        await recursive_import(exported_data.get('nodes', []), new_backup_root_id)
        log(f"[BACKUP] Successfully imported all nodes into the backup location.")

    except Exception as e:
        log(f"[BACKUP] ERROR: Point-in-time backup failed: {type(e).__name__}: {str(e)}")
        # We don't re-raise the exception. The main operation should proceed even if backup fails,
        # but the failure is logged for debugging.

async def reconcile_tree(
    source_json: Union[List[Node], Dict],
    parent_uuid: str,
    list_nodes: Callable[[str], List[Node]],
    create_node: Callable[[str, Dict], str],
    update_node: Callable[[str, Dict], None],
    delete_node: Callable[[str], None],
    move_node: Callable[..., None],
    export_nodes: Callable[[str], Dict] = None,
    import_policy: str = 'strict',  # 'strict' | 'rebase' | 'clone'
    dry_run: bool = False,
    max_delete_threshold: Optional[int] = None,
    force: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Reconcile the Workflowy subtree under parent_uuid to match source_json.

    - source_json: list of nodes OR a dict with {'export_root_id': str, 'nodes': [...] }.
    - parent_uuid: the Workflowy parent where source_json should be synced.
    - import_policy:
        'strict' -> abort on parent mismatch
        'rebase' -> use export_root_id from file as the reconciliation root
        'clone'  -> strip IDs (create-only) and skip DELETE phase
    - dry_run: print planned operations, do not perform any mutations.
    - max_delete_threshold: if set and planned deletions exceed this number, abort unless force=True.
    - force: allow exceeding thresholds (never overrides protections).

    Returns: when dry_run=True, a structured plan dict with planned operations and counts; otherwise None.
    """
    
    # Open debug log file
    debug_log = open(r'E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\reconcile_debug.log', 'w', encoding='utf-8')
    
    def log(msg):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm
        debug_log.write(f"[{timestamp}] {msg}\n")
        debug_log.flush()

    # Perform a point-in-time backup before any operations
    if not dry_run and export_nodes is not None:
        await _perform_backup(parent_uuid, export_nodes, create_node, list_nodes, log)
    elif not dry_run:
        log("[BACKUP] Skipping backup because export_nodes function was not provided.")


    # Planning containers for summary and dry-run plan
    planned_creates: List[Dict[str, Any]] = []
    planned_moves: List[Dict[str, Any]] = []
    planned_reorders: List[Dict[str, Any]] = []
    planned_updates: List[Dict[str, Any]] = []
    planned_delete_roots: List[str] = []

    def build_plan(summary_only: bool = False) -> Dict[str, Any]:
        plan = {
            'settings': {
                'import_policy': import_policy,
                'dry_run': dry_run,
                'parent_uuid': parent_uuid,
                'intended_parent': intended_parent,
                'max_delete_threshold': max_delete_threshold,
                'force': force,
            },
            'counts': {
                'creates': len(planned_creates),
                'moves': len(planned_moves),
                'reorders': len(planned_reorders),
                'updates': len(planned_updates),
                'deletes': len(planned_delete_roots),
            },
            'protected_parent_refs': sorted(list(parent_refs)) if 'parent_refs' in locals() else [],
        }
        if not summary_only:
            plan.update({
                'creates': planned_creates,
                'moves': planned_moves,
                'reorders': planned_reorders,
                'updates': planned_updates,
                'delete_roots': planned_delete_roots,
            })
        return plan

    def log_summary_and_close(note: Optional[str] = None) -> Optional[Dict[str, Any]]:
        plan = build_plan()
        log("\n[SUMMARY]")
        if note:
            log(f"   Note: {note}")
        log(f"   Policy: {import_policy}, Dry-run: {dry_run}, Force: {force}")
        log(f"   Parent (effective): {parent_uuid} | Intended: {intended_parent}")
        log(f"   Protected parent refs: {plan['protected_parent_refs']}")
        for k, v in plan['counts'].items():
            log(f"   {k.capitalize()}: {v}")
        debug_log.close()
        return plan if dry_run else None

    # Normalize input: accept either list of nodes or dict with metadata + nodes
    source_nodes: List[Node]
    file_root: Optional[str] = None

    if isinstance(source_json, dict):
        file_root = source_json.get('export_root_id') or source_json.get('root_id') or source_json.get('parent_id')
        source_nodes = source_json.get('nodes', [])
        log(f"Detected source document with export_root_id={file_root}")
    else:
        source_nodes = source_json

    # Infer intended parent if not explicitly provided in file
    def infer_intended_parent(nodes: List[Node]) -> Optional[str]:
        parents = {n.get('parent_id') for n in nodes if isinstance(n, dict) and 'parent_id' in n}
        parents = {p for p in parents if p is not None}
        return next(iter(parents)) if len(parents) == 1 else None

    intended_parent = file_root or infer_intended_parent(source_nodes)
    if intended_parent:
        if intended_parent != parent_uuid:
            log(f"PARENT MISMATCH detected: intended_parent={intended_parent} (from file), provided parent_uuid={parent_uuid}")
            if import_policy == 'strict':
                log("Aborting due to import_policy='strict'. No changes made.")
                return log_summary_and_close(note="Strict parent mismatch abort")
            elif import_policy == 'rebase':
                log("Rebasing to file's export root (import_policy='rebase'). Overriding parent_uuid.")
                parent_uuid = intended_parent
            elif import_policy == 'clone':
                log("Clone mode: will strip IDs and skip DELETE phase (import_policy='clone').")
            else:
                log(f"Unknown import_policy '{import_policy}', defaulting to 'strict' abort.")
                return log_summary_and_close(note="Unknown policy; abort")
    else:
        log("No explicit export_root_id detected; proceeding with provided parent_uuid.")

    # In clone mode: strip IDs from source and mark skip_delete
    skip_delete = (import_policy == 'clone')

    def strip_ids(nodes: List[Node]):
        for n in nodes:
            if 'id' in n:
                del n['id']
            strip_ids(n.get('children', []))

    if skip_delete:
        strip_ids(source_nodes)

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
            log(f"   Using bulk export for snapshot (avoids rate limit issues)")
            
            if export_func is None:
                log(f"   WARNING: export_nodes not provided, falling back to BFS list_nodes (slow!)")
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
                
                # Build parent/children maps from flat list, EXCLUDING the root itself
                for node in all_nodes:
                    nid = node['id']
                    if nid == root_parent:
                        continue  # never include the reconciliation root in snapshot maps
                    Map_T[nid] = node
                    parent_id = node.get('parent_id')
                    Parent_T[nid] = parent_id
                    if parent_id is not None:
                        Children_T[parent_id].append(nid)
        
        return Map_T, Parent_T, Children_T

    def desired_maps(source_nodes_local: List[Node], root_parent: str):
        Map_S: Dict[Optional[str], Node] = {}
        Parent_S: Dict[str, Optional[str]] = {}
        Order_S: Dict[Optional[str], List[Optional[str]]] = defaultdict(list)
        # Set desired parents for top-level to root_parent
        for i, n in enumerate(source_nodes_local):
            n['_desired_parent'] = root_parent
            n['_desired_index'] = i
        seq = index_source(source_nodes_local, root_parent)  # fills _desired_parent/_desired_index recursively
        for n in seq:
            nid = n.get('id')
            Map_S[nid] = n  # nid can be None for new nodes
            p = n.get('_desired_parent')
            if nid is not None:
                Parent_S[nid] = p
            if p is not None:
                Order_S[p].append(nid)
        return Map_S, Parent_S, Order_S

    def compute_delete_roots(to_delete: Iterable[str], Parent_T_map: Dict[str, Optional[str]]):
        to_delete_set = set(to_delete)
        roots: set[str] = set()
        for d in to_delete_set:
            cur = Parent_T_map.get(d)
            is_root = True
            while cur is not None:
                if cur in to_delete_set:
                    is_root = False
                    break
                cur = Parent_T_map.get(cur)
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
    log(f"   Target snapshot: {len(Map_T)} nodes found in Workflowy (excluding root)")
    log(f"   Target UUIDs: {list(Map_T.keys())}")
    Map_S, Parent_S, Order_S = desired_maps(source_nodes, parent_uuid)
    log(f"   Source JSON: {len(Map_S)} nodes expected")
    log(f"   Source UUIDs: {list(Map_S.keys())}")
    log(f"   Source UUIDs that are NOT None: {[k for k in Map_S.keys() if k is not None]}")
    log(f"   UUIDs in BOTH: {set(Map_T.keys()) & set(k for k in Map_S.keys() if k is not None)}")
    log(f"   UUIDs ONLY in Target: {set(Map_T.keys()) - set(k for k in Map_S.keys() if k is not None)}")
    log(f"   UUIDs ONLY in Source: {set(k for k in Map_S.keys() if k is not None) - set(Map_T.keys())}")

    # Collect parent_refs to protect during deletion (all parent_id that appear in source)
    def preorder(nodes: List[Node]):
        for n in nodes:
            yield n
            for c in preorder(n.get('children', [])):
                yield c

    parent_refs: set[str] = set()
    for n in preorder(source_nodes):
        p = n.get('parent_id')
        if p:
            parent_refs.add(p)
    # Always protect the reconciliation root
    parent_refs.add(parent_uuid)
    log(f"   Protected parent refs (will not delete): {parent_refs}")

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
                if dry_run:
                    new_id = nid or f"DRYRUN-CREATE-{create_count}"
                    log(f"      [DRY-RUN] Would CREATE '{node_name}' under parent {desired_parent} -> {new_id}")
                else:
                    log(f"      >>> CREATING new node '{node_name}' under parent {desired_parent}")
                    try:
                        payload = {
                            'name': n.get('name'),
                            'note': n.get('note'),
                            'data': n.get('data'),
                            'completed': bool(n.get('completed', False)),
                        }
                        log(f"      >>> Payload prepared: name={payload.get('name')}, note_length={len(payload.get('note') or '')}, data={payload.get('data')}")
                        
                        if desired_parent is None:
                            log(f"      >>> ERROR: desired_parent is None for node '{node_name}'")
                            log(f"      >>> Node structure: {n}")
                            raise ValueError(f"Cannot create node '{node_name}' - desired_parent is None. Check parent_id in JSON.")
                        
                        new_id = await create_node(desired_parent, payload)
                        log(f"      >>> CREATED with new UUID: {new_id}")
                        n['id'] = new_id
                    except Exception as e:
                        log(f"      >>> CREATE FAILED for '{node_name}': {type(e).__name__}: {str(e)}")
                        log(f"      >>> Node details: id={nid}, desired_parent={desired_parent}")
                        log(f"      >>> Full node structure: {n}")
                        raise
                    nid = new_id
                    # reflect in target maps immediately
                    Map_T[nid] = {'id': nid, 'parent_id': desired_parent, **payload}
                    Parent_T[nid] = desired_parent
                    if desired_parent is not None:
                        Children_T[desired_parent].append(nid)
                # Simulate ID and maps so later phases can plan (both dry-run and real for summary)
                n['id'] = new_id
                planned_creates.append({'id': new_id, 'name': node_name, 'parent': desired_parent})
                if dry_run:
                    Map_T[n['id']] = {'id': n['id'], 'parent_id': desired_parent, 'name': n.get('name'), 'note': n.get('note'), 'data': n.get('data'), 'completed': bool(n.get('completed', False))}
                    Parent_T[n['id']] = desired_parent
                    if desired_parent is not None:
                        Children_T[desired_parent].append(n['id'])
            else:
                log(f"      (already exists, skipping create)")
            # recurse
            await ensure_created(n.get('children', []), n.get('id'))

    log(f"\n[PHASE 1] CREATE (top-down)")
    await ensure_created(source_nodes, parent_uuid)
    log(f"   CREATE phase complete - created {create_count} new nodes")

    # Recompute desired maps now that new nodes have IDs
    Map_S, Parent_S, Order_S = desired_maps(source_nodes, parent_uuid)

    # ------------- phase 2: MOVE (root-to-leaf following source) -------------
    log(f"\n[PHASE 2] MOVE (root-to-leaf)")

    move_count = 0
    for n in preorder(source_nodes):
        nid = n['id']
        desired_parent = n.get('_desired_parent')
        current_parent = Parent_T.get(nid)
        log(f"   Checking node {nid} (name: {n.get('name')})")
        log(f"      Current parent: {current_parent}")
        log(f"      Desired parent: {desired_parent}")
        if desired_parent != current_parent:
            move_count += 1
            if dry_run:
                log(f"      [DRY-RUN] Would MOVE {nid} from {current_parent} to {desired_parent}")
            else:
                try:
                    log(f"      >>> MOVING {nid} from {current_parent} to {desired_parent}")
                    await move_node(nid, desired_parent, position="bottom")
                    log(f"      >>> MOVE SUCCESS for {nid}")
                except Exception as e:
                    log(f"      >>> MOVE FAILED for {nid}: {type(e).__name__}: {str(e)}")
                    log(f"      >>> Node name: {n.get('name')}")
                    raise
            planned_moves.append({'id': nid, 'from': current_parent, 'to': desired_parent})
            # simulate/update maps
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
    reorder_count = 0
    for p, desired in Order_S.items():
        if p is None:
            continue
        desired_ids = [x for x in desired if x is not None]
        log(f"   Parent {p}: Reordering {len(desired_ids)} children")
        log(f"      Desired order (reversed for TOP positioning): {list(reversed(desired_ids))}")
        for idx, cid in enumerate(reversed(desired_ids)):
            reorder_count += 1
            if dry_run:
                log(f"      [DRY-RUN] Would REORDER: move {cid} to TOP of parent {p}")
            else:
                try:
                    log(f"      >>> REORDER: Moving {cid} to TOP of parent {p}")
                    await move_node(cid, p, position="top")
                    log(f"      >>> REORDER SUCCESS for {cid}")
                except Exception as e:
                    log(f"      >>> REORDER FAILED for {cid}: {type(e).__name__}: {str(e)}")
                    raise
            planned_reorders.append({'id': cid, 'parent': p, 'position': 'top', 'sequence_index': idx})
    log(f"   REORDER phase complete - processed {len(Order_S)} parents, {reorder_count} reorder moves")

    # ------------- phase 4: UPDATE content -------------
    log(f"\n[PHASE 4] UPDATE content")
    update_count = 0
    for nid, tgt in list(Map_T.items()):
        src = Map_S.get(nid)
        if src:
            if not node_data_equal(src, tgt):
                update_count += 1
                payload = {
                    'name': src.get('name'),
                    'note': src.get('note'),
                    'data': src.get('data'),
                    'completed': bool(src.get('completed', False)),
                }
                if dry_run:
                    log(f"      [DRY-RUN] Would UPDATE {nid} -> {payload}")
                else:
                    try:
                        log(f"      >>> UPDATING node {nid}: name={payload.get('name')}")
                        await update_node(nid, payload)
                        log(f"      >>> UPDATE SUCCESS for {nid}")
                    except Exception as e:
                        log(f"      >>> UPDATE FAILED for {nid}: {type(e).__name__}: {str(e)}")
                        log(f"      >>> Payload: {payload}")
                        raise
                planned_updates.append({'id': nid, 'payload': payload})
    log(f"   UPDATE phase complete - {update_count} updates")

    # ------------- phase 5: DELETE (delete-roots only) -------------
    log(f"\n[PHASE 5] DELETE (delete-roots only)")
    if skip_delete:
        log("   Clone mode active: SKIPPING DELETE phase by design.")
        return log_summary_and_close(note="Clone mode: no deletes")

    ids_in_source = {n['id'] for n in preorder(source_nodes)}
    log(f"   Source IDs (from preorder): {ids_in_source}")

    # Snapshot Map_T again under the parent to catch any nodes not visited in source
    log(f"   Re-snapshotting target under parent {parent_uuid}...")
    Map_T2, Parent_T2, _ = await snapshot_target(parent_uuid, use_efficient_traversal=False, export_func=export_nodes)
    ids_in_target = set(Map_T2.keys())

    # Guardrail: never consider the reconciliation root for deletion
    if parent_uuid in ids_in_target:
        ids_in_target.discard(parent_uuid)
    log(f"   Target IDs (after reconciliation, excluding root): {ids_in_target}")

    to_delete_before_protection = ids_in_target - ids_in_source
    # Guardrail: protect all parents referenced in source (containers)
    to_delete = to_delete_before_protection - parent_refs

    # Threshold checks
    planned_delete_roots_set = compute_delete_roots(to_delete, Parent_T2)
    planned_delete_roots[:] = list(planned_delete_roots_set)
    planned_delete_count = len(planned_delete_roots)
    log(f"   Planned delete candidates (after protections): {to_delete}")
    log(f"   Planned delete roots: {set(planned_delete_roots)} (count={planned_delete_count})")

    # Warn if protections saved container nodes
    protected_hit = len(to_delete_before_protection & parent_refs) > 0
    if protected_hit:
        log("   NOTE: Some potential deletes were PROTECTED because they are parent refs or the reconciliation root.")

    if dry_run:
        log("   [DRY-RUN] Skipping deletions. Dry-run complete.")
        return log_summary_and_close()

    if (max_delete_threshold is not None) and (planned_delete_count > max_delete_threshold) and (not force):
        log(f"   ABORTING: planned_delete_count {planned_delete_count} exceeds max_delete_threshold {max_delete_threshold} and force=False.")
        return log_summary_and_close(note="Aborted by delete threshold")

    for d in planned_delete_roots:
        try:
            node_name = Map_T2.get(d, {}).get('name', 'unknown')
            log(f"   >>> DELETING root: {d} (name: {node_name})")
            await delete_node(d)
            log(f"   >>> DELETE SUCCESS for {d}")
        except Exception as e:
            log(f"   >>> DELETE FAILED for {d}: {type(e).__name__}: {str(e)}")
            raise
    log(f"   DELETE phase complete")
    
    return log_summary_and_close()


__all__ = [
    'reconcile_tree',
]
