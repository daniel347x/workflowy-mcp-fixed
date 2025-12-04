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
import os
import sys
import importlib

Node = Dict


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
    log_weave_entry: Optional[Callable[[Dict[str, Any]], None]] = None,
    log_debug_msg: Optional[Callable[[str], None]] = None,
    log_to_file_msg: Optional[Callable[[str], None]] = None,
    debug_log_path: Optional[str] = None,
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
    
    # Open debug log file (tag-specific if provided, otherwise global fallback)
    # UPDATED: Only used when log_to_file_msg callback is NOT provided (legacy mode)
    debug_log = None
    if not log_to_file_msg:
        if debug_log_path:
            debug_log = open(debug_log_path, 'w', encoding='utf-8')
        else:
            # Fallback to global log (legacy behavior for direct calls)
            debug_log = open(r'E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\reconcile_debug.log', 'w', encoding='utf-8')

    # Record start time of this WEAVE for rate-limit aware phases (e.g., DELETE)
    weave_start_time = datetime.now()

    def log(msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # YYYY-MM-DD HH:MM:SS.mmm
        # 1. File Persistence (via callback PREFERRED, or fallback to local debug_log)
        if log_to_file_msg:
            log_to_file_msg(msg)  # Callback handles tag-specific path
        elif debug_log:
            debug_log.write(f"[{timestamp}] {msg}\n")
            debug_log.flush()
        
        # 2. Optional external debug callback (e.g., api_client _log_debug)
        if log_debug_msg:
            log_debug_msg(msg)

    def journal(entry: Dict[str, Any]) -> None:
        """Write a single per-node weave journal entry (best-effort).

        Each entry carries a resumability_safe flag so that if the process is
        interrupted between operations, the last line clearly indicates whether
        the last *completed* node-level operation left the weave in a state that
        is safe to resume.
        """
        if log_weave_entry is None:
            return
        try:
            e = dict(entry)
            e.setdefault("timestamp", datetime.now().isoformat())
            log_weave_entry(e)
        except Exception as e2:  # noqa: BLE001
            # Journal failures must never break reconciliation; log and continue.
            log(f"   [WARN] Failed to append weave journal entry: {type(e2).__name__}: {e2}")

    # Planning containers for summary and dry-run plan
    planned_creates: List[Dict[str, Any]] = []
    planned_moves: List[Dict[str, Any]] = []
    planned_reorders: List[Dict[str, Any]] = []
    planned_updates: List[Dict[str, Any]] = []
    planned_delete_roots: List[str] = []

    def build_plan(summary_only: bool = False) -> Dict[str, Any]:
        # Compute JEWEL sync safety summary: all created nodes must have
        # created/fetched/jewel_updated=True for the weave to be safely resummable.
        jewel_sync_summary: Dict[str, Any] = {
            'total_entries': len(jewel_sync_ledger) if 'jewel_sync_ledger' in locals() else 0,
            'all_safe': True,
            'unsafe_entries': [],
        }
        if 'jewel_sync_ledger' in locals():
            unsafe = [
                e for e in jewel_sync_ledger
                if not (e.get('created') and e.get('fetched') and e.get('jewel_updated'))
            ]
            jewel_sync_summary['unsafe_entries'] = unsafe
            jewel_sync_summary['all_safe'] = (len(unsafe) == 0)

        plan = {
            'settings': {
                'import_policy': import_policy,
                'dry_run': dry_run,
                'parent_uuid': parent_uuid,
                'intended_parent': intended_parent,
                'max_delete_threshold': max_delete_threshold,
                'force': force,
                'option_b_enabled': option_b_enabled,
                'original_ids_seen_count': len(original_ids_seen),
            },
            'counts': {
                'creates': len(planned_creates),
                'moves': len(planned_moves),
                'reorders': len(planned_reorders),
                'updates': len(planned_updates),
                'deletes': len(planned_delete_roots),
            },
            'protected_parent_refs': sorted(list(parent_refs)) if 'parent_refs' in locals() else [],
            'jewel_sync_summary': jewel_sync_summary,
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

        # JEWEL sync safety summary: whether all create/fetch/jewel_update
        # steps completed successfully for every created node.
        js = plan.get('jewel_sync_summary') or {}
        all_safe = js.get('all_safe', True)
        unsafe_entries = js.get('unsafe_entries', [])
        log("   JEWEL sync all_safe: %s" % all_safe)
        log(f"   JEWEL sync entries: {js.get('total_entries', 0)}")
        if unsafe_entries:
            log("   JEWEL sync UNSAFE entries (out-of-sync ETHER/JEWEL for created nodes):")
            for e in unsafe_entries:
                log(
                    f"      - name={e.get('name')} parent={e.get('parent')} "
                    f"id={e.get('id')} source_path={e.get('source_path')} "
                    f"created={e.get('created')} fetched={e.get('fetched')} "
                    f"jewel_updated={e.get('jewel_updated')}"
                )

        debug_log.close()
        return plan if dry_run else None

    async def update_jewel_with_uuid(
        jewel_path: Optional[str],
        source_path: List[int],
        new_id: str,
    ) -> bool:
        """Best-effort per-node JEWEL update.

        Writes the newly created UUID back into the JEWEL JSON at the given
        source_path using nexus_json_tools.transform_jewel(...).

        Returns True on success, False on any failure. Failures are logged but
        do not abort the weave; the JEWEL/ETHER sync guidance at the end will
        mark such entries as unsafe for crash-resume.
        """
        if not jewel_path:
            log("      >>> JEWEL UPDATE SKIPPED: no jewel_file in source_json metadata")
            return False
        try:
            log(
                f"      >>> JEWEL UPDATE for UUID {new_id} at path {source_path} "
                f"in {jewel_path}"
            )
            # Import nexus_json_tools from the project root (same strategy as
            # bulk_import_from_file and nexus_transform_jewel).
            client_dir = os.path.dirname(os.path.abspath(__file__))
            wf_mcp_dir = os.path.dirname(client_dir)
            mcp_servers_dir = os.path.dirname(wf_mcp_dir)
            project_root = os.path.dirname(mcp_servers_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            nexus_tools = importlib.import_module("nexus_json_tools")

            ops = [{
                "op": "SET_ATTRS_BY_PATH",
                "path": source_path,
                "attrs": {"id": new_id},
            }]
            result = nexus_tools.transform_jewel(  # type: ignore[attr-defined]
                jewel_file=jewel_path,
                operations=ops,
                dry_run=False,
                stop_on_error=True,
            )
            success = bool(result.get("success", True))
            if success:
                log("      >>> JEWEL UPDATE SUCCESS")
            else:
                log(f"      >>> JEWEL UPDATE FAILED: {result}")
            return success
        except Exception as e:
            log(
                f"      >>> JEWEL UPDATE ERROR for path {source_path}: "
                f"{type(e).__name__}: {e}"
            )
            return False

    # Normalize input: accept either list of nodes or dict with metadata + nodes
    source_nodes: List[Node]
    file_root: Optional[str] = None
    jewel_file: Optional[str] = None

    # Option B deletion metadata: track node IDs that were present in the
    # "original" source JSON before edits. This enables a visible-then-removed
    # distinction under truncated parents. When not provided, we fall back to
    # the conservative Option A behavior.
    original_ids_seen = set()
    option_b_enabled = False

    if isinstance(source_json, dict):
        file_root = source_json.get('export_root_id') or source_json.get('root_id') or source_json.get('parent_id')
        source_nodes = source_json.get('nodes', [])
        jewel_file = source_json.get('jewel_file')

        # Root-level children_status metadata (analogous to node.children_status).
        # When non-'complete', the reconciliation root is treated as truncated.
        root_children_status = source_json.get('export_root_children_status')

        # Optional Option B deletion ledger - IDs that were present in the
        # original source JSON before edits. This lets us distinguish
        # "visible-then-removed" from "never loaded" under truncated parents.
        original_ids_seen = set(source_json.get('original_ids_seen', []))
        option_b_enabled = len(original_ids_seen) > 0

        log(f"Detected source document with export_root_id={file_root} jewel_file={jewel_file}")
        log(f"   Root children_status: {root_children_status!r}")
        if option_b_enabled:
            log(f"   Option B deletion ledger: {len(original_ids_seen)} originally-seen node IDs")
        else:
            log("   No original_ids_seen ledger; using conservative Option A deletion semantics")
    else:
        source_nodes = source_json
        root_children_status = 'complete'
        log("Detected bare source node list (no jewel_file metadata)")

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

    def normalize_html_entities(s: str | None) -> str | None:
        """Normalize HTML entities exactly as _validate_name_field/_validate_note_field do.
        
        This ensures comparison uses the SAME normalization that update_node will apply,
        preventing spurious updates when ETHER has escaped entities but source doesn't.
        
        Escapes (in order):
        1. & → &amp; (must be first to avoid double-escaping)
        2. < → &lt;
        3. > → &gt;
        """
        if s is None:
            return None
        # Order matters: & first (so we don't double-escape &lt; → &amp;lt;)
        if '&' in s:
            s = s.replace('&', '&amp;')
        if '<' in s:
            s = s.replace('<', '&lt;')
        if '>' in s:
            s = s.replace('>', '&gt;')
        return s

    def node_data_equal(src: Optional[Node], tgt: Optional[Node]) -> bool:
        if src is None or tgt is None:
            return False

        # Normalize names via same HTML entity escaping used in _validate_name_field
        src_name = normalize_html_entities(src.get('name'))
        tgt_name = normalize_html_entities(tgt.get('name'))

        # Trim leading/trailing whitespace so harmless spaces do not trigger updates
        def _strip_or_empty(s: str | None) -> str:
            return (s or "").strip()

        same_name = _strip_or_empty(src_name) == _strip_or_empty(tgt_name)

        # Normalize notes via same HTML entity escaping used in _validate_note_field
        src_note = normalize_html_entities(src.get('note'))
        tgt_note = normalize_html_entities(tgt.get('note'))

        def _strip_or_none(s: str | None) -> Optional[str]:
            if s is None:
                return None
            stripped = s.strip()
            return stripped if stripped != "" else None

        same_note = _strip_or_none(src_note) == _strip_or_none(tgt_note)

        same_completed = bool(src.get('completed', False)) == bool(tgt.get('completed', False))

        # Ignore data entirely for equality (layoutMode is invisible to user, not required)
        # If we later use data for other purposes, we can strip layoutMode and compare residual.
        return same_name and same_note and same_completed

    # ------------- phase 0: build maps -------------
    log(f"\n[PHASE 0] Building maps for parent {parent_uuid}")
    Map_T, Parent_T, Children_T = await snapshot_target(parent_uuid, use_efficient_traversal=False, export_func=export_nodes)
    log(f"   Target snapshot: {len(Map_T)} nodes found in Workflowy (excluding root)")
    log(f"   Target UUIDs: {len(Map_T)} (list suppressed)")
    Map_S, Parent_S, Order_S = desired_maps(source_nodes, parent_uuid)
    log(f"   Source JSON: {len(Map_S)} nodes expected")
    log(f"   Source UUIDs: {list(Map_S.keys())}")
    log(f"   Source UUIDs that are NOT None: {[k for k in Map_S.keys() if k is not None]}")
    log(f"   UUIDs in BOTH: {set(Map_T.keys()) & set(k for k in Map_S.keys() if k is not None)}")
    uuids_only_in_target = set(Map_T.keys()) - set(k for k in Map_S.keys() if k is not None)
    log(f"   UUIDs ONLY in Target: {len(uuids_only_in_target)} (list suppressed)")
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

    # Detect parents whose children are not fully loaded in the source JSON.
    #
    # Truncation semantics are carried explicitly by children_status and by
    # subtree shell markers; we no longer infer truncation from numeric counts
    truncated_parents: set[str] = set()
    for nid, node in Map_S.items():
        if nid is None:
            continue

        status = node.get('children_status')
        children = node.get('children') or []

        if status is not None and status != 'complete':
            truncated_parents.add(nid)
            continue

        # Explicit NEXUS subtree shells: subtree_mode='shell' marks a branch-only
        # node whose children are intentionally opaque (even after JEWELSTORM
        # adds new children under this parent). Its children must never be
        # compared for deletes/reorders, so we always treat it as a truncated
        # parent regardless of status/child list.
        if node.get('subtree_mode') == 'shell':
            truncated_parents.add(nid)
            continue

    # Treat the reconciliation root as truncated when its export_root_children_status
    # is non-'complete', so its direct children obey the same Option B semantics
    # as other truncated parents, without moving the root into the nodes[] array.
    if root_children_status is not None and root_children_status != 'complete':
        truncated_parents.add(parent_uuid)

    if truncated_parents:
        log(f"   Truncated parents (children not fully loaded in source OR opaque shells): {truncated_parents}")
    else:
        log("   No truncated parents detected (all children fully loaded in source, no opaque shells)")

    # ------------- phase 1: CREATE (top-down) -------------
    create_count = 0
    # Per-node JEWEL sync ledger: track create/fetch/jewel_update so we know
    # whether a weave is safely resumable.
    jewel_sync_ledger: List[Dict[str, Any]] = []

    async def ensure_created(nodes: List[Node], desired_parent: Optional[str], source_path_prefix: List[int] | None = None):
        nonlocal create_count, jewel_sync_ledger
        if source_path_prefix is None:
            source_path_prefix = []

        for idx, n in enumerate(nodes):
            nid = n.get('id')
            node_name = n.get('name', 'unnamed')
            current_path = source_path_prefix + [idx]
            log(f"   Checking if exists: {nid} (name: {node_name})")
            if not nid or nid not in Map_T:
                create_count += 1
                if dry_run:
                    new_id = nid or f"DRYRUN-CREATE-{create_count}"
                    log(f"      [DRY-RUN] Would CREATE '{node_name}' under parent {desired_parent} -> {new_id}")
                    # Record in ledger as a simulated, fully-synced triplet so that
                    # dry-run plans treat it as resumable (no actual ETHER/JEWEL changes).
                    jewel_sync_ledger.append({
                        "name": node_name,
                        "parent": desired_parent,
                        "id": new_id,
                        "source_path": current_path,
                        "created": True,
                        "fetched": True,
                        "jewel_updated": True,
                    })
                else:
                    log(f"      >>> CREATING new node '{node_name}' under parent {desired_parent}")
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

                    # Initialize ledger entry for this node; we will fill in
                    # 'id', 'fetched', and 'jewel_updated' as each step succeeds.
                    ledger_entry = {
                        "name": node_name,
                        "parent": desired_parent,
                        "id": None,
                        "source_path": current_path,
                        "created": False,
                        "fetched": False,
                        "jewel_updated": False,
                    }

                    # Journal: node-level CREATE operation started (resumability
                    # is UNSAFE until JEWEL update completes).
                    journal(
                        {
                            "op": "create",
                            "status": "started",
                            "source_path": current_path,
                            "node_name": node_name,
                            "node_id": None,
                            "resumability_safe": False,
                            "message": (
                                f"node operation CREATE started for path {current_path} - "
                                "RESUMABILITY UNSAFE"
                            ),
                        }
                    )

                    try:
                        # Step 1: CREATE in ETHER
                        new_id = await create_node(desired_parent, payload)
                        log(f"      >>> CREATED with new UUID: {new_id}")
                        ledger_entry["id"] = new_id
                        ledger_entry["created"] = True

                        # Step 2: FETCH canonical node (get_node already wrapped
                        # inside create_node in api_client, but we mark it
                        # explicitly here for clarity of the sync contract.)
                        ledger_entry["fetched"] = True

                        # Step 3: JEWEL UPDATE – best-effort per-node write of the
                        # UUID back into the JEWEL JSON so reruns become
                        # incremental at node granularity.
                        updated = await update_jewel_with_uuid(jewel_file, current_path, new_id)
                        ledger_entry["jewel_updated"] = bool(updated)

                        # Journal: CREATE completed. Resumability is SAFE only
                        # when JEWEL update succeeded.
                        safe = bool(updated)
                        journal(
                            {
                                "op": "create",
                                "status": "completed_jewel_ok" if safe else "completed_jewel_failed",
                                "source_path": current_path,
                                "node_name": node_name,
                                "node_id": new_id,
                                "resumability_safe": safe,
                                "message": (
                                    f"node operation CREATE completed for node {new_id} at path {current_path} - "
                                    f"RESUMABILITY {'SAFE' if safe else 'UNSAFE (JEWEL UPDATE FAILED)'}"
                                ),
                            }
                        )

                        n['id'] = new_id
                    except Exception as e:
                        log(f"      >>> CREATE FAILED for '{node_name}': {type(e).__name__}: {str(e)}")
                        log(f"      >>> Node details: id={nid}, desired_parent={desired_parent}")
                        log(f"      >>> Full node structure: {n}")
                        # Even on failure we record the partial state so the
                        # summary can report which nodes are out-of-sync.
                        jewel_sync_ledger.append(ledger_entry)
                        raise

                    nid = new_id
                    # reflect in target maps immediately
                    Map_T[nid] = {'id': nid, 'parent_id': desired_parent, **payload}
                    Parent_T[nid] = desired_parent
                    if desired_parent is not None:
                        Children_T[desired_parent].append(nid)

                    # Record ledger entry for later JEWELSTORM
                    jewel_sync_ledger.append(ledger_entry)

                # Simulate ID and maps so later phases can plan (both dry-run and real for summary)
                n['id'] = new_id
                planned_creates.append({
                    'id': new_id,
                    'name': node_name,
                    'parent': desired_parent,
                    'source_path': current_path,
                })
                if dry_run:
                    Map_T[n['id']] = {
                        'id': n['id'],
                        'parent_id': desired_parent,
                        'name': n.get('name'),
                        'note': n.get('note'),
                        'data': n.get('data'),
                        'completed': bool(n.get('completed', False)),
                    }
                    Parent_T[n['id']] = desired_parent
                    if desired_parent is not None:
                        Children_T[desired_parent].append(n['id'])
            else:
                log(f"      (already exists, skipping create)")
            # recurse
            await ensure_created(n.get('children', []), n.get('id'), current_path)

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
                # Journal: MOVE operation started (resumability is UNSAFE until
                # the move completes, because the node's parent/position may be
                # mid-transition).
                journal(
                    {
                        "op": "move",
                        "status": "started",
                        "node_id": nid,
                        "from_parent": current_parent,
                        "to_parent": desired_parent,
                        "resumability_safe": False,
                        "message": (
                            f"node operation MOVE started for node {nid} from {current_parent} "
                            f"to {desired_parent} - RESUMABILITY UNSAFE until move completes"
                        ),
                    }
                )
                try:
                    log(f"      >>> MOVING {nid} from {current_parent} to {desired_parent}")
                    await move_node(nid, desired_parent, position="bottom")
                    log(f"      >>> MOVE SUCCESS for {nid}")
                    journal(
                        {
                            "op": "move",
                            "status": "completed",
                            "node_id": nid,
                            "from_parent": current_parent,
                            "to_parent": desired_parent,
                            "resumability_safe": True,
                            "message": (
                                f"node operation MOVE completed for node {nid} to parent {desired_parent} - "
                                "RESUMABILITY SAFE"
                            ),
                        }
                    )
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

    # Optimization: if there were no creates and no moves in this run,
    # then sibling order is already correct relative to the source JSON.
    # In that case we can safely skip the REORDER phase entirely to avoid
    # unnecessary move_node API calls.
    if create_count == 0 and move_count == 0:
        log("   REORDER phase skipped: no creates or moves; sibling order assumed correct")
    else:
        reorder_count = 0
        for p, desired in Order_S.items():
            if p is None:
                continue
            desired_ids = [x for x in desired if x is not None]
            if not desired_ids:
                continue

        # If this parent has truncated children in the source JSON, we treat its
        # existing children as an opaque blob: we can still move/delete the parent
        # as a whole, and we can append new children, but we do NOT try to
        # selectively reorder potentially-hidden originals.
            if p in truncated_parents:
                log(f"   Parent {p}: children_status != 'complete' in source; skipping REORDER to avoid touching hidden children")
                continue

        # Compare current order vs desired order. If they already match, skip
        # emitting no-op move operations that would just burn rate limit.
            current_children = Children_T.get(p, [])
            current_ids = [cid for cid in current_children if cid in desired_ids]
            if current_ids == desired_ids:
                log(f"   Parent {p}: children already in desired order; skipping REORDER")
                continue

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

                # DEBUG: Log precise src vs tgt differences driving this UPDATE
                # (data is no longer compared, so we don't log it)
                try:
                    log(f"      [DEBUG] UPDATE diff for {nid}:")
                    log(
                        f"         src.name={src.get('name')!r} | "
                        f"tgt.name={tgt.get('name')!r}"
                    )
                    log(
                        f"         src.note={repr(src.get('note'))} | "
                        f"tgt.note={repr(tgt.get('note'))}"
                    )
                    log(
                        f"         src.completed={src.get('completed', False)} | "
                        f"tgt.completed={tgt.get('completed', False)}"
                    )
                except Exception as e:
                    log(f"      [DEBUG] Failed to log UPDATE diff for {nid}: {type(e).__name__}: {e}")

                if dry_run:
                    log(f"      [DRY-RUN] Would UPDATE {nid} -> {payload}")
                else:
                    journal(
                        {
                            "op": "update",
                            "status": "started",
                            "node_id": nid,
                            "resumability_safe": False,
                            "message": (
                                f"node operation UPDATE started for node {nid} - "
                                "RESUMABILITY UNSAFE until update completes"
                            ),
                        }
                    )
                    try:
                        log(f"      >>> UPDATING node {nid}: name={payload.get('name')}")
                        await update_node(nid, payload)
                        log(f"      >>> UPDATE SUCCESS for {nid}")
                        journal(
                            {
                                "op": "update",
                                "status": "completed",
                                "node_id": nid,
                                "resumability_safe": True,
                                "message": (
                                    f"node operation UPDATE completed for node {nid} - "
                                    "RESUMABILITY SAFE"
                                ),
                            }
                        )
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

    # Known Workflowy behavior: /nodes-export is limited to ~1 call/minute.
    # We almost always hit rate limits here because a bulk export was already
    # performed at the start of the WEAVE (Phase 0), and DELETE typically
    # begins <60s later. To reduce noisy 429s and make behavior predictable,
    # wait until at least ~65 seconds have elapsed since weave_start_time
    # before issuing the DELETE-phase bulk export.
    try:
        import asyncio
        min_elapsed = 65.0  # seconds
        now = datetime.now()
        elapsed = (now - weave_start_time).total_seconds()
        if elapsed < min_elapsed:
            wait_seconds = min_elapsed - elapsed
            log(
                f"   DELETE phase: waiting {wait_seconds:.1f}s before bulk export "
                f"to respect Workflowy /nodes-export rate limit (elapsed since WEAVE_START={elapsed:.1f}s)"
            )
            await asyncio.sleep(wait_seconds)
            now2 = datetime.now()
            elapsed2 = (now2 - weave_start_time).total_seconds()
            log(
                f"   DELETE phase: wait complete; elapsed since WEAVE_START={elapsed2:.1f}s. "
                f"Proceeding to bulk export snapshot."
            )
    except Exception:
        # If anything goes wrong with timing/sleep, continue without blocking DELETE.
        pass

    ids_in_source = {n['id'] for n in preorder(source_nodes)}
    log(f"   Source IDs (from preorder): {ids_in_source}")

    # Snapshot Map_T again under the parent to catch any nodes not visited in source
    log(f"   Re-snapshotting target under parent {parent_uuid}...")
    Map_T2, Parent_T2, _ = await snapshot_target(parent_uuid, use_efficient_traversal=False, export_func=export_nodes)
    ids_in_target = set(Map_T2.keys())

    # Guardrail: never consider the reconciliation root for deletion
    if parent_uuid in ids_in_target:
        ids_in_target.discard(parent_uuid)
    log(f"   Target IDs (after reconciliation, excluding root): {len(ids_in_target)} IDs (list suppressed)")

    # Raw delete candidates: nodes present in target snapshot but absent from
    # the source JSON. For parents whose children were not fully loaded in the
    # source (truncated_parents), we must NOT interpret missing descendants as
    # deletes – they were simply never shown.
    def is_hidden_descendant(node_id: str, truncated_roots: set[str], parent_map: Dict[str, Optional[str]]) -> bool:
        cur = parent_map.get(node_id)
        while cur is not None:
            if cur in truncated_roots:
                return True
            cur = parent_map.get(cur)
        return False

    def parent_all_children_known(
        tid: str,
        parent_map: Dict[str, Optional[str]],
        source_nodes_by_id: Dict[Optional[str], Node],
    ) -> bool:
        # A target node is only eligible for automatic deletion when:
        # - It has a parent present in the source JSON, and
        # - That parent is marked with children_status == 'complete'.
        #
        # This confines DELETE to subtrees whose immediate children set we know
        # is complete (full SCRY or fully-expanded GLIMPSE parents).
        p = parent_map.get(tid)
        if p is None:
            # Never auto-delete the reconciliation root or nodes without a parent.
            return False

        parent_node = source_nodes_by_id.get(p)
        if not parent_node:
            # Parent branch was never present in the source JSON (outside GLIMPSE).
            return False

        status = parent_node.get('children_status', 'complete')
        # For full SCRY exports, children_status may be absent; treat that as complete.
        return status == 'complete'

    raw_delete_candidates = ids_in_target - ids_in_source

    def can_delete_candidate(tid: str) -> bool:
        """Decide whether a candidate ID is eligible for automatic deletion.

        Option A (default, conservative):
          - parent_all_children_known == True
          - and not is_hidden_descendant (no truncated ancestors)

        Option B (enabled when original_ids_seen is non-empty):
          - allow deletion of nodes that were present in the *original* source JSON
            and later removed, even when their parent is truncated, while still
            protecting nodes that were never loaded at all.
        """
        base_parent_known = parent_all_children_known(tid, Parent_T2, Map_S)
        hidden = is_hidden_descendant(tid, truncated_parents, Parent_T2)
        was_seen = tid in original_ids_seen

        if not option_b_enabled:
            return base_parent_known and not hidden

        # If this node lives under a truncated parent and was NEVER seen in the
        # original source JSON, we must not treat its absence as a delete – it
        # may simply be a hidden child we never loaded.
        hidden_protected = hidden and (not was_seen)

        # For nodes that *were* originally seen, we treat removal from the JSON
        # as an explicit delete, even if their parent is truncated. This is the
        # "visible-then-removed" case.
        parent_known_or_seen = base_parent_known or was_seen

        if hidden_protected:
            return False
        if not parent_known_or_seen:
            return False
        return True

    to_delete_before_protection = {tid for tid in raw_delete_candidates if can_delete_candidate(tid)}
    
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
            journal(
                {
                    "op": "delete",
                    "status": "started",
                    "node_id": d,
                    "resumability_safe": False,
                    "message": (
                        f"node operation DELETE started for node {d} (name: {node_name}) - "
                        "RESUMABILITY UNSAFE until delete completes"
                    ),
                }
            )
            await delete_node(d)
            log(f"   >>> DELETE SUCCESS for {d}")
            journal(
                {
                    "op": "delete",
                    "status": "completed",
                    "node_id": d,
                    "resumability_safe": True,
                    "message": (
                        f"node operation DELETE completed for node {d} (name: {node_name}) - "
                        "RESUMABILITY SAFE"
                    ),
                }
            )
        except Exception as e:
            log(f"   >>> DELETE FAILED for {d}: {type(e).__name__}: {str(e)}")
            raise
    log(f"   DELETE phase complete")
    
    # Final JEWEL/ETHER sync guidance:
    #
    # - If all created nodes have completed the create+fetch+jewel_update
    #   triad, the weave is safely resumable: re-running
    #   nexus_weave_enchanted_async against the same JSON will be incremental and
    #   UUID-aware.
    # - If ANY created node failed to update the JEWEL with its UUID, the
    #   run is NOT safely resumable; re-running may recreate or misalign
    #   nodes. In that case we emit an explicit OUT-OF-SYNC warning.
    plan_for_guidance = build_plan(summary_only=True)
    js = plan_for_guidance.get('jewel_sync_summary') or {}
    all_safe = js.get('all_safe', True)
    unsafe_entries = js.get('unsafe_entries', [])

    log("\n[GUIDANCE]")
    if all_safe:
        log("   JEWEL/ETHER sync is COMPLETE for all created nodes (create+fetch+jewel_update).")
        log("   It is safe to re-run the SAME nexus_weave_enchanted_async call against this JSON.")
        log("   The reconciliation algorithm is UUID-aware and will:")
        log("     • Reuse UUIDs from the JEWEL for existing nodes")
        log("     • Skip nodes that were already created in previous runs")
        log("     • Only create/move/update the remaining nodes that were not yet applied")
        log("")
        log("   This makes large NEXUS imports resumable after transient timeouts or rate limiting.")
    else:
        log("   JEWEL/ETHER sync is INCOMPLETE for one or more created nodes.")
        log("   At least one node satisfied create+fetch but FAILED to record its UUID in the JEWEL.")
        log("   This weave is NOT safely resumable with the current JSON; re-running may recreate or")
        log("   misalign nodes. See 'JEWEL sync UNSAFE entries' in the SUMMARY above for details.")
        if unsafe_entries:
            log("   Affected created nodes (name/parent/id/source_path):")
            for e in unsafe_entries:
                log(
                    f"      - name={e.get('name')} parent={e.get('parent')} "
                    f"id={e.get('id')} source_path={e.get('source_path')}"
                )

    return log_summary_and_close()


__all__ = [
    'reconcile_tree',
]
