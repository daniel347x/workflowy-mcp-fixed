"""nexus_json_tools.py

Utility CLI for manipulating NEXUS SCRY JSON files (Workflowy exports).

This provides a small, Workflowy-API-like set of commands that operate on the
SCRY JSON *offline*:
  - rename-node      (change the `name` of a node by id)
  - set-note         (change the `note` of a node by id)
  - delete-node      (remove a node and its subtree by id)
  - move-node        (reparent within the SCRY JSON)
  - extract-shard    (BURN THE LENS – extract a shard subtree from Territory)
  - burn-gem         (REHYDRATE GEM – splice full-depth subtrees into Territory and extract a GEM shard)
  - fuse-shard       (2-way replace of roots – legacy/simple mode)
  - fuse-shard-3way  (T/S0/S1 SHARD FUSE into Territory – new mode)

Designed to be used on QUILLSTRIKE working stones for SCRY files, before
running `nexus_weave` to write changes back into Workflowy.

Example (simple rename):

  python nexus_json_tools.py \
    E:\\...\\temp\\qm-XXXX-mcp-deploy-docs--nexus_mcp_deploy_47ec.json \
    rename-node --id 577dcdf0-... --name "New step title"

Example (3-way shard fuse):

  python nexus_json_tools.py dummy \
    fuse-shard-3way \
      --witness-shard E:\...\\shard-original.json \
      --morphed-shard E:\...\\shard-working.json \
      --target-scry   E:\...\\territory.json

Example (burn GEM from full SCRY into Territory):

  python nexus_json_tools.py territory.json \
    burn-gem \
      --full-scry E:\...\\full_scry_for_roots.json \
      --root-ids R1 R2 R3 \
      --output-shard E:\...\\gem_shard_S0.json

After edits, run:
  - QUILLMORPH on the working stone (to update the SCRY JSON)
  - generate_markdown_from_json()
  - nexus_weave() to apply to Workflowy.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set

JsonDict = Dict[str, Any]


def _build_jewel_preview_lines(
    roots: List[JsonDict],
    max_note_chars: int = 1024,
) -> List[str]:
    """Build a human-readable preview of the JEWEL / GEM tree.

    This preview is purely for agents/humans. It is never read by any NEXUS or
    JEWELSTORM algorithm and is safe to regenerate or discard at any time.
    """
    # PASS 1: Collect all jewel_id / id labels to compute max width
    all_labels: List[str] = []

    def collect_labels(node: JsonDict) -> None:
        jewel_id = node.get("jewel_id") or node.get("id") or "?"
        all_labels.append(str(jewel_id))
        children = node.get("children") or []
        for child in children:
            if isinstance(child, dict):
                collect_labels(child)

    for root in roots or []:
        if isinstance(root, dict):
            collect_labels(root)

    max_id_width = max((len(lbl) for lbl in all_labels), default=0)

    # PASS 2: Build aligned preview lines
    lines: List[str] = []

    def walk(node: JsonDict, depth: int) -> None:
        jewel_id = node.get("jewel_id") or node.get("id") or "?"
        id_label = str(jewel_id).ljust(max_id_width)
        indent = " " * 4 * depth
        children = node.get("children") or []
        has_child_dicts = any(isinstance(c, dict) for c in children)
        bullet = "•" if not has_child_dicts else "⦿"
        name = node.get("name") or "Untitled"
        note = node.get("note") or ""

        # Surface SKELETON role hints in the preview (delete vs merge targets)
        sk_hint = node.get("skeleton_hint")
        # NOTE (Dec 2025): subtree_mode='shell' is an editing-scope marker, not a
        # deletion-intent marker. Do NOT infer DELETE_SECTION from shell.
        if sk_hint == "DELETE_SECTION":
            hint_prefix = "[DELETE] "
        elif sk_hint == "MERGE_TARGET":
            hint_prefix = "[MERGE] "
        elif sk_hint == "PERMANENT_SECTION":
            hint_prefix = "[KEEP] "
        else:
            hint_prefix = ""

        if isinstance(note, str) and note:
            flat = note.replace("\n", "\\n")
            if len(flat) > max_note_chars:
                flat = flat[:max_note_chars]
            name_part = f"{hint_prefix}{name} [{flat}]"
        else:
            name_part = f"{hint_prefix}{name}"

        lines.append(f"[{id_label}] {indent}{bullet} {name_part}")
        for child in children:
            if isinstance(child, dict):
                walk(child, depth + 1)

    for root in roots or []:
        if isinstance(root, dict):
            walk(root, 0)
    return lines


def log_jewel(message: str) -> None:
    """Log JEWELSTORM operations to a persistent logfile (best-effort).

    This mirrors the DATETIME+prefix style used by the NEXUS WEAVE
    reconcile_debug.log, but writes to jewelstorm_debug.log so JEWELSTORM
    activity can be inspected independently of WEAVE.
    """
    try:
        log_path = (
            r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\jewelstorm_debug.log"
        )
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception:
        # Logging must never affect CLI behavior
        pass


def _normalize_node_key_order(node: JsonDict) -> JsonDict:
    """Ensure id / preview_id / jewel_id appear first in each node for readability."""
    front_keys = ["id", "preview_id", "jewel_id"]
    ordered: JsonDict = {}
    for k in front_keys:
        if k in node:
            ordered[k] = node[k]
    for k, v in node.items():
        if k not in ordered:
            ordered[k] = v
    children = ordered.get("children")
    if isinstance(children, list):
        new_children: List[JsonDict] = []
        for ch in children:
            if isinstance(ch, dict):
                new_children.append(_normalize_node_key_order(ch))
            else:
                new_children.append(ch)
        ordered["children"] = new_children
    return ordered


def transform_jewel(
    jewel_file: str,
    operations: List[Dict[str, Any]],
    dry_run: bool = False,
    stop_on_error: bool = False,
) -> Dict[str, Any]:
    """Apply JEWELSTORM semantic operations to a NEXUS working_gem JSON file.

    This is the semantic analogue of pattern-based edit_file() for PHANTOM GEM
    / working_gem JSON:
    - Operates purely offline (no Workflowy API calls)
    - Works on a local JSON file produced by JEWELSTRIKE (typically phantom_gem.json
      copied to a working stone)
    - Uses jewel_id as the stable identity handle inside JEWELSTORM
    - Never invents real Workflowy IDs: new nodes are written without an "id"
      field so NEXUS/WEAVE can treat them as CREATE operations

    Args:
        jewel_file: Path to working_gem JSON file
        operations: List of operation dictionaries, e.g.:
            {
              "op": "MOVE_NODE",
              "jewel_id": "J-001",
              "new_parent_jewel_id": "J-010",
              "position": "LAST" | "FIRST" | "BEFORE" | "AFTER",
              "relative_to_jewel_id": "J-999"  # for BEFORE/AFTER
            }
        dry_run: If True, simulate only (no file write)
        stop_on_error: If True, abort on first error (no write)

    Returns:
        Dict with success flag, counts, and error details. Example:
        {
          "success": True,
          "applied_count": 3,
          "dry_run": False,
          "nodes_created": 1,
          "nodes_deleted": 0,
          "nodes_moved": 1,
          "nodes_renamed": 1,
          "notes_updated": 0,
          "attrs_updated": 0,
          "errors": []
        }
    """
    import uuid

    # ------- Load JSON safely (convert die/SystemExit into structured error) -------
    try:
        data = load_json(jewel_file)
    except SystemExit as e:  # die() inside load_json uses sys.exit
        return {
            "success": False,
            "applied_count": 0,
            "dry_run": dry_run,
            "nodes_created": 0,
            "nodes_deleted": 0,
            "nodes_moved": 0,
            "nodes_renamed": 0,
            "notes_updated": 0,
            "attrs_updated": 0,
            "errors": [
                {
                    "index": -1,
                    "op": None,
                    "code": "LOAD_ERROR",
                    "message": f"Failed to load JSON from {jewel_file}: {e}",
                }
            ],
        }

    # Determine editable roots list (supports both export-package dict and bare list)
    nexus_tag_in_file: Optional[str] = None
    if isinstance(data, dict):
        nexus_tag_in_file = data.get("nexus_tag")

    if isinstance(data, dict) and isinstance(data.get("nodes"), list):
        original_roots = data["nodes"]
    elif isinstance(data, list):
        original_roots = data
    else:
        return {
            "success": False,
            "applied_count": 0,
            "dry_run": dry_run,
            "nodes_created": 0,
            "nodes_deleted": 0,
            "nodes_moved": 0,
            "nodes_renamed": 0,
            "notes_updated": 0,
            "attrs_updated": 0,
            "errors": [
                {
                    "index": -1,
                    "op": None,
                    "code": "FORMAT_ERROR",
                    "message": "JSON must be a dict with 'nodes' or a bare list of nodes for transform_jewel.",
                }
            ],
        }

    # Work on a deep copy so we never mutate the original unless we succeed
    roots: List[JsonDict] = copy.deepcopy(original_roots)

    # ------- Managed NEXUS witness context (optional) -------
    # If this working_gem came from JEWELSTRIKE, it may carry nexus_tag.
    # When present, we can locate the witness GEM (S0) for this run and use it
    # to compute ancestry relationships for additional safety checks.
    witness_parent_by_id: Optional[Dict[str, Optional[str]]] = None
    if nexus_tag_in_file:
        try:
            # Resolve nexus_tag -> phantom_gem.json (best-effort).
            # We avoid importing jewelstrike.py here because nexus_json_tools.py is often
            # imported from non-project CWDs. Instead, replicate the minimal resolution logic.
            from pathlib import Path

            base_dir = Path(
                r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
            )
            suffix = f"__{nexus_tag_in_file}"
            candidates = [
                child
                for child in base_dir.iterdir()
                if child.is_dir() and (child.name == nexus_tag_in_file or child.name.endswith(suffix))
            ]
            if candidates:
                run_dir = sorted(candidates, key=lambda p: p.name)[-1]
                phantom_path = run_dir / "phantom_gem.json"
                if phantom_path.exists():
                    witness_data = load_json(str(phantom_path))
                else:
                    witness_data = None
            else:
                witness_data = None

            if witness_data is not None:
                witness_roots = []
                if isinstance(witness_data, dict) and isinstance(witness_data.get("nodes"), list):
                    witness_roots = witness_data.get("nodes") or []
                elif isinstance(witness_data, list):
                    witness_roots = witness_data

                # Build parent map (by real Workflowy id) from witness tree
                w_parent: Dict[str, Optional[str]] = {}

                def _walk_w(node: JsonDict, parent_id: Optional[str]) -> None:
                    nid = node.get("id")
                    if nid:
                        w_parent[str(nid)] = parent_id
                    for ch in node.get("children") or []:
                        if isinstance(ch, dict):
                            _walk_w(ch, str(nid) if nid else parent_id)

                for r in witness_roots:
                    if isinstance(r, dict):
                        _walk_w(r, None)

                witness_parent_by_id = w_parent
        except Exception:
            # Managed witness context is best-effort; never block transform_jewel.
            witness_parent_by_id = None

    # ------- Indexing: jewel_id-based identity -------
    by_jewel_id: Dict[str, JsonDict] = {}
    parent_by_jewel_id: Dict[str, Optional[str]] = {}
    existing_ids: Set[str] = set()

    def _ensure_children_list(node: JsonDict) -> List[JsonDict]:
        children = node.get("children")
        if not isinstance(children, list):
            children = []
            node["children"] = children
        return children

    def _register_node(node: JsonDict, parent_jid: Optional[str]) -> None:
        """Register node and its subtree in jewel_id index.

        Identity:
        - Prefer "jewel_id" if present
        - Fallback to "id" for older files (pre-JEWELSTRIKE)
        """
        jewel_id = node.get("jewel_id") or node.get("id")
        if jewel_id:
            if jewel_id in by_jewel_id:
                raise ValueError(f"Duplicate jewel_id/id {jewel_id!r} in JSON tree")
            by_jewel_id[jewel_id] = node
            parent_by_jewel_id[jewel_id] = parent_jid
            existing_ids.add(jewel_id)

        children = node.get("children") or []
        if not isinstance(children, list):
            children = []
            node["children"] = children
        for child in children:
            if isinstance(child, dict):
                _register_node(child, jewel_id)

    for root in roots:
        if isinstance(root, dict):
            _register_node(root, None)

    def _new_jewel_id() -> str:
        """Generate a fresh, human-friendly jewel_id that cannot collide."""
        while True:
            candidate = "J-" + uuid.uuid4().hex[:8]
            if candidate not in existing_ids:
                existing_ids.add(candidate)
                return candidate

    def _resolve_to_jewel_id(identifier: str) -> str:
        """Auto-resolve Workflowy UUID to jewel_id if needed.
        
        If identifier looks like a Workflowy UUID (standard UUID format, not J-NNN),
        search the tree for a node with that id and return its jewel_id.
        
        If identifier is already a jewel_id (J-NNN format), return as-is.
        
        Args:
            identifier: Either a jewel_id (J-NNN) or Workflowy UUID
            
        Returns:
            jewel_id to use for the operation
            
        Raises:
            ValueError: If UUID not found in tree
        """
        import re
        
        # Check if it's a standard UUID format (8-4-4-4-12 hex pattern)
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        if uuid_pattern.match(identifier):
            # It's a Workflowy UUID - search for node with this id
            for jewel_id, node in by_jewel_id.items():
                if node.get("id") == identifier:
                    return jewel_id
            # Not found
            raise ValueError(
                f"Node with Workflowy UUID '{identifier}' not found in GEM. "
                "(Auto-resolved from jewel_id parameter - agent can use Workflowy UUIDs directly)"
            )
        else:
            # Already a jewel_id (J-NNN format) or other format - return as-is
            return identifier


    def _resolve_to_jewel_id(identifier: str) -> str:
        """Auto-resolve Workflowy UUID to jewel_id if needed.
        
        If identifier looks like a Workflowy UUID (standard UUID format, not J-NNN),
        search the tree for a node with that id and return its jewel_id.
        
        If identifier is already a jewel_id (J-NNN format), return as-is.
        
        Args:
            identifier: Either a jewel_id (J-NNN) or Workflowy UUID
            
        Returns:
            jewel_id to use for the operation
            
        Raises:
            ValueError: If UUID not found in tree
        """
        import re
        
        # Check if it's a standard UUID format (8-4-4-4-12 hex pattern)
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        
        if uuid_pattern.match(identifier):
            # It's a Workflowy UUID - search for node with this id
            for jewel_id, node in by_jewel_id.items():
                if node.get("id") == identifier:
                    return jewel_id
            # Not found
            raise ValueError(
                f"Node with Workflowy UUID '{identifier}' not found in GEM. "
                "(Auto-resolved from jewel_id parameter - agent can use Workflowy UUIDs directly)"
            )
        else:
            # Already a jewel_id (J-NNN format) or other format - return as-is
            return identifier


    def _get_node_and_parent_list(jewel_id: str) -> Tuple[JsonDict, Optional[str], List[JsonDict]]:
        # Auto-resolve Workflowy UUID to jewel_id if needed
        jewel_id = _resolve_to_jewel_id(jewel_id)
        
        node = by_jewel_id.get(jewel_id)
        if node is None:
            raise ValueError(f"Node with jewel_id/id {jewel_id!r} not found")
        parent_jid = parent_by_jewel_id.get(jewel_id)
        if parent_jid is None:
            siblings = roots
        else:
            parent_node = by_jewel_id.get(parent_jid)
            if parent_node is None:
                raise ValueError(f"Parent with jewel_id/id {parent_jid!r} not found for node {jewel_id!r}")
            siblings = _ensure_children_list(parent_node)
        return node, parent_jid, siblings

    def _assert_no_cycle(node_jewel_id: str, new_parent_jewel_id: Optional[str]) -> None:
        cur = new_parent_jewel_id
        while cur is not None:
            if cur == node_jewel_id:
                raise ValueError(
                    f"Cannot move node {node_jewel_id!r} under its own descendant {new_parent_jewel_id!r}"
                )
            cur = parent_by_jewel_id.get(cur)

    def _remove_subtree_from_index(node: JsonDict) -> None:
        jid = node.get("jewel_id") or node.get("id")
        if jid:
            by_jewel_id.pop(jid, None)
            parent_by_jewel_id.pop(jid, None)
            existing_ids.discard(jid)
        for child in node.get("children") or []:
            if isinstance(child, dict):
                _remove_subtree_from_index(child)

    def _has_any_witness_descendant(node_workflowy_id: Optional[str]) -> Optional[bool]:
        """Return True if witness ledger implies this node has at least one known descendant.

        Uses witness_parent_by_id (built from phantom_gem.json for nexus_tag) if available.

        - Returns None if witness context is unavailable.
        - Returns False if no witness descendants are found.

        Note: This is an epistemic test: "known descendants", not "all descendants in ETHER".
        """
        if not node_workflowy_id:
            return False
        if witness_parent_by_id is None:
            return None

        target = str(node_workflowy_id)
        # Any node in witness_parent_by_id whose ancestor chain includes target counts as a descendant.
        for nid, parent in witness_parent_by_id.items():
            cur = parent
            while cur is not None:
                if cur == target:
                    return True
                cur = witness_parent_by_id.get(cur)
        return False

    def _build_subtree_from_spec(spec: Dict[str, Any]) -> JsonDict:
        """Build a new subtree from a CREATE_NODE spec (Pattern A).

        Spec keys used:
          - name (required)
          - note (optional)
          - attrs or data (optional, mapped to node['data'])
          - jewel_id (optional; if omitted, auto-generated)
          - children (optional list of nested specs)
          - parent_id (optional, ignored - will be set during insertion)
        """
        if "name" not in spec:
            raise ValueError("CREATE_NODE spec requires 'name' field in node object")

        node: JsonDict = {"name": spec["name"]}

        if "note" in spec:
            node["note"] = spec["note"]

        attrs = spec.get("attrs") or spec.get("data")
        if isinstance(attrs, dict):
            node["data"] = copy.deepcopy(attrs)

        # Assign jewel_id (never assign real Workflowy id here)
        jewel_id = spec.get("jewel_id")
        if jewel_id is None:
            jewel_id = _new_jewel_id()
        else:
            if jewel_id in existing_ids:
                raise ValueError(f"CREATE_NODE requested duplicate jewel_id {jewel_id!r}")
            existing_ids.add(jewel_id)
        node["jewel_id"] = jewel_id

        # Recursively build children
        children_specs = spec.get("children") or []
        children_nodes: List[JsonDict] = []
        for child_spec in children_specs:
            if isinstance(child_spec, dict):
                child_node = _build_subtree_from_spec(child_spec)
                children_nodes.append(child_node)
        node["children"] = children_nodes
        return node

    def _classify_children_for_destructive_ops(node: JsonDict) -> Dict[str, Any]:
        """Classify immediate children for destructive JEWELSTORM ops.

        IMPORTANT:
        - This classification is about what is present/known in the local JSON.
        - "truncated" means the child set may be incomplete/unknown in ETHER.

        Returns a dict with:
            children: list of child dict nodes
            children_status: str
            truncated: bool
            has_loaded_ether_children: bool  # child has a real Workflowy id
            has_new_children: bool           # child has no id yet (JEWEL-only)
            category: "EMPTY" | "JEWEL_ONLY" | "ETHER_ONLY" | "MIXED"
        """
        children_raw = node.get("children") or []
        children = [c for c in children_raw if isinstance(c, dict)]
        # children_status is epistemic metadata used for WEAVE safety.
        # NEVER default missing to 'complete' (that can cause unsafe deletes).
        # Fail-closed: missing => unknown.
        children_status = node.get("children_status") or "unknown"
        truncated = children_status != "complete"
        has_loaded_ether_children = any(c.get("id") is not None for c in children)
        has_new_children = any(c.get("id") is None for c in children)

        if not children:
            category = "EMPTY"
        elif (not has_loaded_ether_children) and has_new_children and not truncated:
            category = "JEWEL_ONLY"
        elif has_loaded_ether_children and not has_new_children:
            category = "ETHER_ONLY"
        else:
            category = "MIXED"

        return {
            "children": children,
            "children_status": children_status,
            "truncated": truncated,
            "has_loaded_ether_children": has_loaded_ether_children,
            "has_new_children": has_new_children,
            "category": category,
        }

    # ------- Apply operations -------
    applied_count = 0
    nodes_created = 0
    nodes_deleted = 0
    nodes_moved = 0
    nodes_renamed = 0
    notes_updated = 0
    attrs_updated = 0
    errors: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # Usability: normalize operation ordering + skip invalid ops
    #
    # Goal:
    # - Apply DELETEs before MOVE/RENAME/etc. (avoid moving nodes that will be deleted)
    # - When multiple DELETE_NODE ops are provided, order deepest-first so descendant
    #   deletes happen before ancestor deletes (and optionally become redundant).
    # - Skip MOVE_NODE ops if the source node is deleted or is inside a deleted subtree.
    #
    # Notes:
    # - This is purely an offline JSON transform convenience layer.
    # - We keep non-delete ops in relative order after deletes.
    # -------------------------------------------------------------------------

    ops_in = list(operations or [])

    def _op_type(op: Dict[str, Any]) -> str:
        return str(op.get("op") or op.get("operation") or "").upper()

    delete_ops: List[Dict[str, Any]] = []
    other_ops: List[Dict[str, Any]] = []
    for op in ops_in:
        if _op_type(op) == "DELETE_NODE":
            delete_ops.append(op)
        else:
            other_ops.append(op)

    # Build a depth lookup for delete targets (deeper-first)
    def _resolve_jid_maybe(jid: Any) -> Optional[str]:
        if not jid:
            return None
        try:
            return _resolve_to_jewel_id(str(jid))
        except Exception:
            return str(jid)

    def _depth_of(jid: str) -> int:
        d = 0
        cur = parent_by_jewel_id.get(jid)
        while cur is not None:
            d += 1
            cur = parent_by_jewel_id.get(cur)
        return d

    delete_targets: List[str] = []
    for op in delete_ops:
        jid_raw = op.get("jewel_id")
        jid = _resolve_jid_maybe(jid_raw)
        if jid:
            delete_targets.append(jid)

    delete_targets_set = set(delete_targets)

    # Compute redundant deletes: if an ancestor is also being deleted, the child delete is redundant.
    redundant_deletes: set[str] = set()
    for jid in delete_targets_set:
        cur = parent_by_jewel_id.get(jid)
        while cur is not None:
            if cur in delete_targets_set:
                redundant_deletes.add(jid)
                break
            cur = parent_by_jewel_id.get(cur)

    # Filter + sort delete ops
    filtered_delete_ops: List[Dict[str, Any]] = []
    for op in delete_ops:
        jid = _resolve_jid_maybe(op.get("jewel_id"))
        if jid and jid in redundant_deletes:
            # Best-effort: record as a non-fatal "skipped" error so user sees it.
            errors.append({
                "index": -1,
                "op": op,
                "code": "SKIP_REDUNDANT_DELETE",
                "message": f"Skipping DELETE_NODE for {jid}: ancestor also deleted in same operation batch",
            })
            continue
        filtered_delete_ops.append(op)

    filtered_delete_ops.sort(
        key=lambda op: _depth_of(_resolve_jid_maybe(op.get("jewel_id")) or ""),
        reverse=True,
    )

    # Build deleted-subtree guard for MOVE_NODE skipping
    deleted_subtree_roots = {
        _resolve_jid_maybe(op.get("jewel_id"))
        for op in filtered_delete_ops
        if _resolve_jid_maybe(op.get("jewel_id"))
    }

    def _is_in_deleted_subtree(jid: str) -> bool:
        if jid in deleted_subtree_roots:
            return True
        cur = parent_by_jewel_id.get(jid)
        while cur is not None:
            if cur in deleted_subtree_roots:
                return True
            cur = parent_by_jewel_id.get(cur)
        return False

    filtered_other_ops: List[Dict[str, Any]] = []
    for op in other_ops:
        if _op_type(op) == "MOVE_NODE":
            jid = _resolve_jid_maybe(op.get("jewel_id"))
            if jid and _is_in_deleted_subtree(jid):
                errors.append({
                    "index": -1,
                    "op": op,
                    "code": "SKIP_MOVE_DELETED_SUBTREE",
                    "message": f"Skipping MOVE_NODE for {jid}: node is deleted or inside a deleted subtree",
                })
                continue
        filtered_other_ops.append(op)

    operations = filtered_delete_ops + filtered_other_ops

    for idx, op in enumerate(operations or []):
        op_type_raw = op.get("op") or op.get("operation")
        if not op_type_raw:
            err = {
                "index": idx,
                "op": op,
                "code": "MISSING_OP",
                "message": "Operation missing 'op' field",
            }
            errors.append(err)
            if stop_on_error:
                break
            continue

        op_type = str(op_type_raw).upper()

        # VALIDATION: Reject unknown fields in operation to prevent hallucinations
        VALID_FIELDS_BY_OP = {
            "MOVE_NODE": {"op", "operation", "jewel_id", "new_parent_jewel_id", "position", "relative_to_jewel_id"},
            "DELETE_NODE": {"op", "operation", "jewel_id", "confirm_delete_known_descendants_from_ether", "delete_from_ether", "mode"},
            "DELETE_ALL_CHILDREN": {"op", "operation", "jewel_id", "confirm_delete_known_descendants_from_ether", "delete_from_ether", "mode"},
            "RENAME_NODE": {"op", "operation", "jewel_id", "new_name", "name"},
            "SET_NOTE": {"op", "operation", "jewel_id", "new_note", "note"},
            "SET_ATTRS": {"op", "operation", "jewel_id", "attrs"},
            "CREATE_NODE": {"op", "operation", "parent_jewel_id", "position", "relative_to_jewel_id", "name", "note", "attrs", "data", "jewel_id", "children", "node"},
            "SET_ATTRS_BY_PATH": {"op", "operation", "path", "attrs"},
            # Text-level JEWELSTORM helpers
            "SEARCH_REPLACE": {"op", "operation", "search", "replace", "case_sensitive", "whole_word", "regex", "fields"},
            "SEARCH_AND_TAG": {"op", "operation", "search", "tag", "case_sensitive", "whole_word", "regex", "fields", "tag_in_name", "tag_in_note"},
        }
        
        valid_fields = VALID_FIELDS_BY_OP.get(op_type)
        if valid_fields:
            unknown_fields = set(op.keys()) - valid_fields
            if unknown_fields:
                err = {
                    "index": idx,
                    "op": op,
                    "code": "UNKNOWN_FIELDS",
                    "message": f"Operation '{op_type}' contains unknown field(s): {sorted(unknown_fields)}. Valid fields: {sorted(valid_fields)}",
                }
                errors.append(err)
                if stop_on_error:
                    break
                continue

        try:
            # MOVE_NODE
            if op_type == "MOVE_NODE":
                src_jid = op.get("jewel_id")
                if not src_jid:
                    raise ValueError("MOVE_NODE requires 'jewel_id'")

                position = str(op.get("position", "LAST")).upper()
                # Normalize synonyms
                if position == "BOTTOM":
                    position = "LAST"
                if position == "TOP":
                    position = "FIRST"
                rel_jid = op.get("relative_to_jewel_id")
                if rel_jid:
                    rel_jid = _resolve_to_jewel_id(rel_jid)
                new_parent_jid = op.get("new_parent_jewel_id")

                # VALIDATION: MOVE_NODE must have valid parent target
                # Three valid cases:
                #   1. new_parent_jewel_id provided and exists in tree
                #   2. position BEFORE/AFTER with relative_to_jewel_id (parent inferred)
                #   3. Neither provided → REJECT (ambiguous root move)
                if new_parent_jid is None and not rel_jid:
                    raise ValueError(
                        "MOVE_NODE requires either 'new_parent_jewel_id' or 'relative_to_jewel_id' (for BEFORE/AFTER). "
                        "Cannot move node without specifying where to place it. "
                        "To move to GEM root, explicitly provide the root jewel_id as new_parent_jewel_id."
                    )

                if position in {"BEFORE", "AFTER"}:
                    if not rel_jid:
                        raise ValueError(
                            "MOVE_NODE with position BEFORE/AFTER requires 'relative_to_jewel_id'"
                        )
                    # Target parent/siblings come from relative node
                    _, rel_parent_jid, rel_siblings = _get_node_and_parent_list(rel_jid)
                    target_parent_jid = rel_parent_jid
                    target_list = rel_siblings
                else:
                    # FIRST/LAST under explicit parent
                    target_parent_jid = new_parent_jid
                    parent_node = by_jewel_id.get(new_parent_jid)
                    if parent_node is None:
                        raise ValueError(f"MOVE_NODE new_parent_jewel_id {new_parent_jid!r} not found in tree")
                    target_list = _ensure_children_list(parent_node)

                # Cycle check
                _assert_no_cycle(src_jid, target_parent_jid)

                node, old_parent_jid, old_siblings = _get_node_and_parent_list(src_jid)

                # Remove from old siblings
                if node in old_siblings:
                    old_siblings.remove(node)

                # Insert into new location
                if position == "FIRST":
                    target_list.insert(0, node)
                elif position == "LAST":
                    target_list.append(node)
                elif position in {"BEFORE", "AFTER"}:
                    rel_node, _, _ = _get_node_and_parent_list(rel_jid)  # type: ignore[arg-type]
                    try:
                        rel_index = target_list.index(rel_node)
                    except ValueError as e:
                        raise ValueError(
                            f"Relative node {rel_jid!r} not found under chosen parent for MOVE_NODE"
                        ) from e
                    insert_index = rel_index if position == "BEFORE" else rel_index + 1
                    target_list.insert(insert_index, node)
                else:
                    raise ValueError(f"Unsupported MOVE_NODE position {position!r}")

                # Update parent mapping
                parent_by_jewel_id[src_jid] = target_parent_jid
                nodes_moved += 1

            # DELETE_NODE
            elif op_type == "DELETE_NODE":
                jid = op.get("jewel_id")
                if not jid:
                    raise ValueError("DELETE_NODE requires 'jewel_id'")

                # Acknowledgement flag (alias): prefer the newer, explicit name.
                delete_from_ether = bool(
                    op.get("confirm_delete_known_descendants_from_ether")
                    if "confirm_delete_known_descendants_from_ether" in op
                    else op.get("delete_from_ether")
                )
                mode_raw = op.get("mode")
                mode = str(mode_raw).upper() if mode_raw is not None else "SMART"

                node, parent_jid, siblings = _get_node_and_parent_list(jid)
                info = _classify_children_for_destructive_ops(node)
                children = info["children"]
                category = info["category"]
                truncated = bool(info.get("truncated"))

                # Legacy strict mode: preserve original FAIL_IF_HAS_CHILDREN semantics
                if mode == "FAIL_IF_HAS_CHILDREN":
                    if children:
                        raise ValueError(
                            f"DELETE_NODE {jid!r} refused: node has children and mode=FAIL_IF_HAS_CHILDREN"
                        )
                else:
                    # SMART semantics (default when mode not provided)
                    if category == "MIXED":
                        raise ValueError(
                            f"DELETE_NODE {jid!r} refused: mixed new/ETHER/truncated children not supported; "
                            "delete or move children individually first"
                        )

                    # If the child set is truncated/unknown, require explicit acknowledgement.
                    # Rationale: an EMPTY children list may simply mean "not loaded".
                    if truncated and not delete_from_ether:
                        raise ValueError(
                            f"DELETE_NODE {jid!r} refused: children_status != 'complete' (opaque/unknown subtree). "
                            "Set delete_from_ether=True to acknowledge this deletion may impact ETHER-backed descendants during WEAVE."
                        )

                    # Managed witness refinement: if this node has ANY known descendants in the
                    # witness GEM (S0), then deleting it is a subtree-impacting delete and must be
                    # explicitly acknowledged.
                    witness_has_desc = _has_any_witness_descendant(node.get("id"))
                    if witness_has_desc is True and not delete_from_ether:
                        raise ValueError(
                            f"DELETE_NODE {jid!r} refused: node has known descendants in witness GEM (S0); "
                            "set delete_from_ether=True to acknowledge subtree deletion impact"
                        )

                    # ETHER-backed children always require explicit acknowledgement.
                    if category == "ETHER_ONLY" and not delete_from_ether:
                        raise ValueError(
                            f"DELETE_NODE {jid!r} refused: node has ETHER-backed children; "
                            "set delete_from_ether=True to acknowledge deletion impact in Workflowy"
                        )
                    # EMPTY and JEWEL_ONLY are allowed when not truncated and (if witness context is available)
                    # no witness descendants exist.

                # Remove from siblings and index
                if node in siblings:
                    siblings.remove(node)
                _remove_subtree_from_index(node)
                nodes_deleted += 1

            # DELETE_ALL_CHILDREN
            elif op_type == "DELETE_ALL_CHILDREN":
                jid = op.get("jewel_id")
                if not jid:
                    raise ValueError("DELETE_ALL_CHILDREN requires 'jewel_id'")

                # Acknowledgement flag (alias): prefer the newer, explicit name.
                delete_from_ether = bool(
                    op.get("confirm_delete_known_descendants_from_ether")
                    if "confirm_delete_known_descendants_from_ether" in op
                    else op.get("delete_from_ether")
                )
                mode_raw = op.get("mode")
                mode = str(mode_raw).upper() if mode_raw is not None else "SMART"

                node = by_jewel_id.get(jid)
                if node is None:
                    raise ValueError(f"Node with jewel_id/id {jid!r} not found")

                info = _classify_children_for_destructive_ops(node)
                children = info["children"]
                category = info["category"]
                truncated = bool(info.get("truncated"))

                # Nothing to do
                if not children and not truncated:
                    pass
                else:
                    # For DELETE_ALL_CHILDREN: if child set is truncated/unknown, require explicit acknowledgement.
                    # We still refuse to proceed because this operation is inherently "surgical" and unsafe
                    # when we cannot prove the full child set is loaded.
                    if truncated:
                        raise ValueError(
                            f"DELETE_ALL_CHILDREN {jid!r} refused: children_status != 'complete' (opaque/unknown children). "
                            "Re-GLIMPSE / re-SCRY to load full children first, or delete the whole node with delete_from_ether=True."
                        )

                    if mode == "FAIL_IF_HAS_CHILDREN":
                        if children:
                            raise ValueError(
                                f"DELETE_ALL_CHILDREN {jid!r} refused: node has children and "
                                "mode=FAIL_IF_HAS_CHILDREN"
                            )
                    else:
                        if category == "MIXED":
                            raise ValueError(
                                f"DELETE_ALL_CHILDREN {jid!r} refused: mixed new/ETHER children not supported; "
                                "delete or move children individually first"
                            )
                        if category == "ETHER_ONLY" and not delete_from_ether:
                            raise ValueError(
                                f"DELETE_ALL_CHILDREN {jid!r} refused: children are ETHER-backed; "
                                "set delete_from_ether=True to acknowledge deletion impact in Workflowy"
                            )
                        # EMPTY and JEWEL_ONLY are always allowed here.

                    # Allowed path: remove all current children from index and node
                    for child in list(children):
                        _remove_subtree_from_index(child)
                    node["children"] = []
                    nodes_deleted += len(children)

            # RENAME_NODE
            elif op_type == "RENAME_NODE":
                jid = op.get("jewel_id")
                if not jid:
                    raise ValueError("RENAME_NODE requires 'jewel_id'")
                # Accept both 'name' and 'new_name' for consistency with CREATE_NODE
                new_name = op.get("name") or op.get("new_name")
                if new_name is None:
                    raise ValueError("RENAME_NODE requires 'name' or 'new_name'")

                node = by_jewel_id.get(jid)
                if node is None:
                    raise ValueError(f"Node with jewel_id/id {jid!r} not found")
                node["name"] = new_name
                nodes_renamed += 1

            # SET_NOTE
            elif op_type == "SET_NOTE":
                jid = op.get("jewel_id")
                if not jid:
                    raise ValueError("SET_NOTE requires 'jewel_id'")
                # Accept both 'note' and 'new_note' for consistency with CREATE_NODE
                new_note = op.get("note") if "note" in op else op.get("new_note")
                # Allow empty string / None to clear

                node = by_jewel_id.get(jid)
                if node is None:
                    raise ValueError(f"Node with jewel_id/id {jid!r} not found")
                node["note"] = new_note
                notes_updated += 1

            # SET_ATTRS
            elif op_type == "SET_ATTRS":
                jid = op.get("jewel_id")
                if not jid:
                    raise ValueError("SET_ATTRS requires 'jewel_id'")
                attrs = op.get("attrs") or {}
                if not isinstance(attrs, dict):
                    raise ValueError("SET_ATTRS 'attrs' must be a dict")

                node = by_jewel_id.get(jid)
                if node is None:
                    raise ValueError(f"Node with jewel_id/id {jid!r} not found")

                data = node.get("data")
                if not isinstance(data, dict):
                    data = {}

                allowed_keys = {"completed", "layoutMode", "priority", "tags"}
                for key, value in attrs.items():
                    if key not in allowed_keys:
                        raise ValueError(f"Unsupported attr key {key!r} in SET_ATTRS")
                    if value is None:
                        data.pop(key, None)
                    else:
                        data[key] = value

                if data:
                    node["data"] = data
                elif "data" in node:
                    del node["data"]

                attrs_updated += 1

            # CREATE_NODE (Pattern A with optional jewel_id)
            elif op_type == "CREATE_NODE":
                parent_jid = op.get("parent_jewel_id")
                position = str(op.get("position", "LAST")).upper()
                # Normalize synonyms
                if position == "BOTTOM":
                    position = "LAST"
                if position == "TOP":
                    position = "FIRST"
                rel_jid = op.get("relative_to_jewel_id")
                if rel_jid:
                    rel_jid = _resolve_to_jewel_id(rel_jid)

                # VALIDATION: CREATE_NODE must have valid parent
                # Three valid cases:
                #   1. parent_jewel_id provided and exists in tree
                #   2. position BEFORE/AFTER with relative_to_jewel_id (parent inferred)
                #   3. Neither provided → REJECT (ambiguous root creation)
                if parent_jid is None and not rel_jid:
                    raise ValueError(
                        "CREATE_NODE requires either 'parent_jewel_id' or 'relative_to_jewel_id' (for BEFORE/AFTER). "
                        "Cannot create node without specifying where to place it. "
                        "To create at GEM root, use parent_jewel_id='R' or provide the root jewel_id explicitly."
                    )

                if parent_jid is None:
                    # BEFORE/AFTER mode - parent inferred from relative node
                    parent_children = roots  # Will be overridden below when resolving relative
                    target_parent_jid = None  # Will be set from relative node's parent
                else:
                    parent_node = by_jewel_id.get(parent_jid)
                    if parent_node is None:
                        raise ValueError(f"CREATE_NODE parent_jewel_id {parent_jid!r} not found in tree")
                    parent_children = _ensure_children_list(parent_node)
                    target_parent_jid = parent_jid

                # Build node spec for subtree
                # Two formats supported:
                # 1. Compact: node fields at operation level (name, note, children, etc.)
                # 2. Wrapped: node fields inside "node" key
                if "node" in op:
                    spec = op["node"]
                else:
                    # Remove op-specific keys to get node spec
                    spec = {k: v for k, v in op.items() if k not in {
                        "op",
                        "operation",
                        "parent_jewel_id",
                        "position",
                        "relative_to_jewel_id",
                    }}

                new_node = _build_subtree_from_spec(spec)

                # Register subtree in indexes with correct parent mapping
                _register_node(new_node, target_parent_jid)

                # Insert relative to siblings
                if position == "FIRST":
                    parent_children.insert(0, new_node)
                elif position == "LAST":
                    parent_children.append(new_node)
                elif position in {"BEFORE", "AFTER"}:
                    if not rel_jid:
                        raise ValueError(
                            "CREATE_NODE with position BEFORE/AFTER requires 'relative_to_jewel_id'"
                        )
                    rel_node, _, rel_siblings = _get_node_and_parent_list(rel_jid)  # type: ignore[arg-type]
                    # Force siblings to be the same list as parent_children
                    if rel_siblings is not parent_children:
                        raise ValueError(
                            "CREATE_NODE BEFORE/AFTER relative_to_jewel_id must share the same parent"
                        )
                    try:
                        rel_index = parent_children.index(rel_node)
                    except ValueError as e:
                        raise ValueError(
                            f"Relative node {rel_jid!r} not found under chosen parent for CREATE_NODE"
                        ) from e
                    insert_index = rel_index if position == "BEFORE" else rel_index + 1
                    parent_children.insert(insert_index, new_node)
                else:
                    raise ValueError(f"Unsupported CREATE_NODE position {position!r}")

                nodes_created += 1

            # SET_ATTRS_BY_PATH (path-based attribute update, used for JEWEL UUID injection)
            elif op_type == "SET_ATTRS_BY_PATH":
                path = op.get("path")
                attrs = op.get("attrs") or {}
                if not isinstance(path, list) or not path:
                    raise ValueError("SET_ATTRS_BY_PATH requires non-empty 'path' list")
                if not isinstance(attrs, dict):
                    raise ValueError("SET_ATTRS_BY_PATH 'attrs' must be a dict")

                # Navigate by index path from roots
                current_list: List[JsonDict] = roots
                target_node: Optional[JsonDict] = None
                for level, idx in enumerate(path):
                    if not isinstance(idx, int):
                        raise ValueError(
                            f"SET_ATTRS_BY_PATH path index at position {level} must be int, got {type(idx).__name__}"
                        )
                    if idx < 0 or idx >= len(current_list):
                        raise ValueError(
                            f"SET_ATTRS_BY_PATH path index {idx} out of range at position {level}"
                        )
                    target_node = current_list[idx]
                    if level < len(path) - 1:
                        children = target_node.get("children")
                        if not isinstance(children, list):
                            raise ValueError(
                                f"SET_ATTRS_BY_PATH path descends into non-list children at position {level}"
                            )
                        current_list = children

                if target_node is None:
                    raise ValueError("SET_ATTRS_BY_PATH could not resolve target node from path")

                for key, value in attrs.items():
                    if key == "id":
                        if value is None:
                            target_node.pop("id", None)
                        else:
                            if not isinstance(value, str):
                                raise ValueError(
                                    "SET_ATTRS_BY_PATH 'id' value must be a string when not None"
                                )
                            target_node["id"] = value
                    else:
                        raise ValueError(
                            f"Unsupported attr key {key!r} in SET_ATTRS_BY_PATH (only 'id' is currently allowed)"
                        )

                attrs_updated += 1

            # SEARCH_REPLACE – text-level search/replace over name/note fields
            elif op_type == "SEARCH_REPLACE":
                import re

                search = op.get("search")
                if not isinstance(search, str) or not search:
                    raise ValueError("SEARCH_REPLACE requires non-empty 'search' string")
                replace = op.get("replace", "")
                if not isinstance(replace, str):
                    raise ValueError("SEARCH_REPLACE 'replace' must be a string")

                case_sensitive = bool(op.get("case_sensitive", False))
                whole_word = bool(op.get("whole_word", False))
                use_regex = bool(op.get("regex", False))

                fields_opt = str(op.get("fields", "both")).lower()
                if fields_opt not in {"name", "note", "both"}:
                    raise ValueError("SEARCH_REPLACE 'fields' must be 'name', 'note', or 'both'")

                flags = 0 if case_sensitive else re.IGNORECASE

                if use_regex:
                    pattern = re.compile(search, flags)
                else:
                    pat = re.escape(search)
                    if whole_word:
                        pat = r"\b" + pat + r"\b"
                    pattern = re.compile(pat, flags)

                def _apply_replace(text: Optional[str]) -> tuple[Optional[str], bool]:
                    if not isinstance(text, str) or text == "":
                        return text, False
                    new_text, count = pattern.subn(replace, text)
                    return new_text, count > 0

                renamed_nodes = 0
                updated_notes = 0

                for node in by_jewel_id.values():
                    name_changed = False
                    note_changed = False

                    if fields_opt in {"name", "both"}:
                        name = node.get("name")
                        new_name, changed = _apply_replace(name)
                        if changed:
                            node["name"] = new_name
                            name_changed = True

                    if fields_opt in {"note", "both"}:
                        note = node.get("note")
                        new_note, changed = _apply_replace(note)
                        if changed:
                            node["note"] = new_note
                            note_changed = True

                    if name_changed:
                        renamed_nodes += 1
                    if note_changed:
                        updated_notes += 1

                nodes_renamed += renamed_nodes
                notes_updated += updated_notes

            # SEARCH_AND_TAG – search, then add a tag to name and/or note
            elif op_type == "SEARCH_AND_TAG":
                import re

                search = op.get("search")
                if not isinstance(search, str) or not search:
                    raise ValueError("SEARCH_AND_TAG requires non-empty 'search' string")

                raw_tag = op.get("tag")
                if not isinstance(raw_tag, str) or not raw_tag.strip():
                    raise ValueError("SEARCH_AND_TAG requires non-empty 'tag' string")
                tag = raw_tag.strip()
                if not tag.startswith("#"):
                    tag = "#" + tag

                case_sensitive = bool(op.get("case_sensitive", False))
                whole_word = bool(op.get("whole_word", False))
                use_regex = bool(op.get("regex", False))

                fields_opt = str(op.get("fields", "both")).lower()
                if fields_opt not in {"name", "note", "both"}:
                    raise ValueError("SEARCH_AND_TAG 'fields' must be 'name', 'note', or 'both'")

                tag_in_name = bool(op.get("tag_in_name", True))
                tag_in_note = bool(op.get("tag_in_note", False))

                flags = 0 if case_sensitive else re.IGNORECASE

                if use_regex:
                    pattern = re.compile(search, flags)
                else:
                    pat = re.escape(search)
                    if whole_word:
                        pat = r"\b" + pat + r"\b"
                    pattern = re.compile(pat, flags)

                def _matches(text: Optional[str]) -> bool:
                    if not isinstance(text, str) or text == "":
                        return False
                    return bool(pattern.search(text))

                def _has_tag(text: Optional[str]) -> bool:
                    if not isinstance(text, str) or text == "":
                        return False
                    # Simple token-based check to avoid duplicate tags
                    return tag in text.split()

                renamed_nodes = 0
                updated_notes = 0

                for node in by_jewel_id.values():
                    name = node.get("name")
                    note = node.get("note")

                    match = False
                    if fields_opt in {"name", "both"} and _matches(name):
                        match = True
                    if fields_opt in {"note", "both"} and _matches(note):
                        match = True

                    if not match:
                        continue

                    name_changed = False
                    note_changed = False

                    if tag_in_name:
                        base_name = name or ""
                        if not _has_tag(base_name):
                            new_name = (base_name + " " + tag).strip()
                            node["name"] = new_name
                            name_changed = True

                    if tag_in_note:
                        base_note = note or ""
                        if not _has_tag(base_note):
                            if base_note:
                                new_note = base_note + "\n" + tag
                            else:
                                new_note = tag
                            node["note"] = new_note
                            note_changed = True

                    if name_changed:
                        renamed_nodes += 1
                    if note_changed:
                        updated_notes += 1

                nodes_renamed += renamed_nodes
                notes_updated += updated_notes

            else:
                raise ValueError(f"Unknown operation type {op_type!r}")

            applied_count += 1

        except Exception as e:  # noqa: BLE001
            errors.append(
                {
                    "index": idx,
                    "op": op,
                    "code": "OP_ERROR",
                    "message": str(e),
                }
            )
            if stop_on_error:
                break

    # Build preview tree from the updated working roots (for agents/humans).
    # This is ephemeral and never read by NEXUS/JEWELSTORM algorithms.
    preview_tree = _build_jewel_preview_lines(roots)

    # ------- Persist changes (if not dry-run and no stop-on-error failure) -------
    if not dry_run and (not stop_on_error or not errors):
        # Attach modified roots back to original structure
        if isinstance(data, dict) and isinstance(data.get("nodes"), list):
            # Recalculate counts FIRST (operates in-place on `roots`)
            wrapper = {"nodes": roots}
            recalc_all_counts_gem(wrapper)

            # Normalize key order for all nodes before writing
            normalized_roots = []
            for r in roots:
                if isinstance(r, dict):
                    normalized_roots.append(_normalize_node_key_order(r))
                else:
                    normalized_roots.append(r)
            roots = normalized_roots

            # Rebuild data dict but only update JEWEL-local fields.
            # We intentionally avoid touching NEXUS metadata such as
            # export_root_id/export_root_name/original_ids_seen/etc.,
            # because JEWELMORPH re-attaches the authoritative values
            # from phantom_gem.json.
            new_data = dict(data)
            new_data["nodes"] = roots
            new_data["__preview_tree__"] = preview_tree
            data = new_data
        elif isinstance(data, list):
            # For bare list JEWELs, normalize key order as well
            normalized_roots = []
            for r in roots:
                if isinstance(r, dict):
                    normalized_roots.append(_normalize_node_key_order(r))
                else:
                    normalized_roots.append(r)
            data = normalized_roots  # type: ignore[assignment]
            wrapper = {"nodes": roots}
            recalc_all_counts_gem(wrapper)

        save_json(jewel_file, data)  # type: ignore[arg-type]

    return {
        "preview_tree": preview_tree,
        "success": len(errors) == 0,
        "applied_count": applied_count,
        "dry_run": dry_run,
        "nodes_created": nodes_created,
        "nodes_deleted": nodes_deleted,
        "nodes_moved": nodes_moved,
        "nodes_renamed": nodes_renamed,
        "notes_updated": notes_updated,
        "attrs_updated": attrs_updated,
        "errors": errors,
    }


def die(msg: str) -> None:
    print(f"[nexus_json_tools] ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def load_json(path: str) -> JsonDict:
    if not os.path.isfile(path):
        die(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            die(f"Failed to parse JSON from {path}: {e}")


def save_json(path: str, data: JsonDict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _walk_nodes(nodes: List[JsonDict], parent: Optional[JsonDict]) -> Tuple[JsonDict, Optional[JsonDict]]:
    for node in nodes:
        yield node, parent
        children = node.get("children") or []
        if isinstance(children, list):
            for child, p in _walk_nodes(children, node):
                yield child, p


def find_node_and_parent(data: JsonDict, target_id: str) -> Tuple[Optional[JsonDict], Optional[JsonDict], Optional[List[JsonDict]]]:
    """Return (node, parent, siblings_list) for a given id, or (None, None, None).

    siblings_list is the list object that directly contains `node`.
    For root-level nodes, siblings_list is data["nodes"].
    """

    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        die("JSON root has no 'nodes' list – unexpected SCRY structure")

    # Root-level: parent=None, siblings=nodes
    for node in nodes:
        if node.get("id") == target_id:
            return node, None, nodes

    # Nested children
    for node, parent in _walk_nodes(nodes, None):
        if node.get("id") == target_id:
            if parent is None:
                # Should have been caught above, but guard anyway
                return node, None, nodes
            siblings = parent.get("children") or []
            if not isinstance(siblings, list):
                die("Parent has non-list 'children' field – unexpected SCRY structure")
            return node, parent, siblings

    return None, None, None


def recalc_counts_for_node(node: JsonDict) -> int:
    """Recalculate child/descendant counts (human-focused) WITHOUT breaking children_status.

    IMPORTANT DESIGN RULE (Dec 2025): children_status is an *epistemic* flag
    used for WEAVE safety (complete vs truncated/unknown). It must NEVER be
    overwritten by count-recalc utilities.

    This helper updates only the *_human_readable_only counters.

    Returns this node's total_descendant_count__human_readable_only.
    """
    children = node.get("children") or []
    if not isinstance(children, list):
        children = []
        node["children"] = children

    immediate = len(children)
    total_desc = 0
    for child in children:
        if isinstance(child, dict):
            child_total = recalc_counts_for_node(child)
            total_desc += 1 + child_total

    node["immediate_child_count__human_readable_only"] = immediate
    node["total_descendant_count__human_readable_only"] = total_desc

    # Fail-closed normalization: if children_status is missing, stamp "unknown".
    # WEAVE already treats missing as truncated, but stamping makes the invariant
    # explicit and prevents other tooling from assuming completeness.
    if "children_status" not in node or not node.get("children_status"):
        node["children_status"] = "unknown"

    return total_desc


def recalc_all_counts(data: JsonDict) -> None:
    """Recalculate *_human_readable_only counters for TERRAIN-like files.

    NOTE: This function name is kept for backward compatibility with existing
    CLI commands, but it is now children_status-safe.
    """
    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        return
    for node in nodes:
        if isinstance(node, dict):
            recalc_counts_for_node(node)


def recalc_counts_for_node_gem(node: JsonDict) -> int:
    """Recalculate counts for JEWEL working_gem files without changing children_status.

    This keeps truncation semantics (children_status) intact for GEM/JEWEL trees,
    while still giving humans up-to-date counts inside the working_gem.
    """
    children = node.get("children") or []
    if not isinstance(children, list):
        children = []
        node["children"] = children

    immediate = len(children)
    total_desc = 0
    for child in children:
        if isinstance(child, dict):
            child_total = recalc_counts_for_node_gem(child)
            total_desc += 1 + child_total

    node["immediate_child_count__human_readable_only"] = immediate
    node["total_descendant_count__human_readable_only"] = total_desc
    return total_desc


def recalc_all_counts_gem(data: JsonDict) -> None:
    """Best-effort count recalculation for JEWEL working_gem files.

    Unlike recalc_all_counts, this helper does NOT modify children_status, so
    GEM files can continue to carry TERRAIN-derived truncation state
    (truncated_by_depth / truncated_by_count) untouched.
    """
    nodes = data.get("nodes") or []
    if not isinstance(nodes, list):
        return
    for node in nodes:
        if isinstance(node, dict):
            recalc_counts_for_node_gem(node)


def cmd_rename_node(args: argparse.Namespace) -> None:
    data = load_json(args.file)
    node, parent, siblings = find_node_and_parent(data, args.id)
    if node is None:
        die(f"Node id not found: {args.id}")

    old_name = node.get("name")
    node["name"] = args.name
    recalc_all_counts(data)
    save_json(args.file, data)
    print(f"[nexus_json_tools] Renamed node {args.id!r}:\n  old: {old_name!r}\n  new: {args.name!r}")


def cmd_set_note(args: argparse.Namespace) -> None:
    data = load_json(args.file)
    node, parent, siblings = find_node_and_parent(data, args.id)
    if node is None:
        die(f"Node id not found: {args.id}")

    old_note = node.get("note")
    node["note"] = args.note
    recalc_all_counts(data)
    save_json(args.file, data)
    print(f"[nexus_json_tools] Updated note for node {args.id!r}.")
    if args.show_old:
        print("  OLD note:")
        print("  " + (old_note or "<None>").replace("\n", "\n  "))
    if args.show_new:
        print("  NEW note:")
        print("  " + args.note.replace("\n", "\n  "))


def cmd_move_node(args: argparse.Namespace) -> None:
    data = load_json(args.file)

    # 1. Find node and remove from old location
    node, old_parent, old_siblings = find_node_and_parent(data, args.id)
    if node is None:
        die(f"Node id not found: {args.id}")

    # Remove from old siblings
    old_siblings[:] = [n for n in old_siblings if n is not node]

    # 2. Find new parent
    # Special case: if parent-id is "root", we move to top-level nodes
    # But typically SCRY has a single export_root. We'll assume args.parent_id matches a node ID.

    # We need a way to find the new parent node object.
    # find_node_and_parent returns (node, parent, siblings).
    # If we search for the parent_id, the "node" returned is the parent we want.

    new_parent_node, _, _ = find_node_and_parent(data, args.parent_id)
    if new_parent_node is None:
        # Check if it matches the export_root_id (the top-level container)
        if data.get("export_root_id") == args.parent_id:
            # Moving to root level of the export
            target_children_list = data["nodes"]
        else:
            die(f"New parent node not found: {args.parent_id}")
    else:
        # It's a normal node
        target_children_list = new_parent_node.get("children")
        if target_children_list is None:
            target_children_list = []
            new_parent_node["children"] = target_children_list

    # 3. Insert into new location
    if args.position == "top":
        target_children_list.insert(0, node)
    else:
        target_children_list.append(node)

    # 4. Recalc counts (globally is safest)
    recalc_all_counts(data)
    save_json(args.file, data)
    print(f"[nexus_json_tools] Moved node {args.id!r} to parent {args.parent_id!r} ({args.position}).")


def cmd_extract_shard(args: argparse.Namespace) -> None:
    """Extract one or more roots (and all their descendants) into a shard JSON.

    NOTE: This is currently a 2-way shard: it contains only a single "nodes"
    list. For full 3-way T/S0/S1 semantics, use QUILLSTRIKE on this shard file
    to obtain a working stone + witness stone pair, then fuse via
    fuse-shard-3way.
    """
    source_data = load_json(args.file)
    shard_roots: List[JsonDict] = []

    for root_id in args.root_ids:
        node, _, _ = find_node_and_parent(source_data, root_id)
        if node is None:
            die(f"Shard root node not found: {root_id}")
        # Deep copy to ensure shard is independent
        shard_roots.append(copy.deepcopy(node))

    shard_data: JsonDict = {
        "shard_source_file": os.path.basename(args.file),
        "shard_root_ids": args.root_ids,
        "nodes": shard_roots,
    }

    save_json(args.output, shard_data)
    print(f"[nexus_json_tools] Extracted {len(shard_roots)} roots to shard: {args.output}")


def cmd_fuse_shard(args: argparse.Namespace) -> None:
    """Legacy 2-way fuse: replace root subtrees in target with shard roots.

    This is effectively a "shard is authoritative for its roots" operation:
    for each shard root id present in target, the entire subtree is replaced
    with the shard version. Hidden descendants in the Territory that are not
    present in the shard will be dropped.

    For safer, 3-way semantics that preserve hidden children unless explicitly
    removed/moved relative to a witness shard, use fuse-shard-3way instead.
    """
    shard_data = load_json(args.shard_file)
    target_data = load_json(args.target_scry)

    updates = 0

    # For each root in the shard, replace the corresponding node in target
    shard_nodes = shard_data.get("nodes", [])
    for shard_node in shard_nodes:
        s_id = shard_node.get("id")
        if not s_id:
            continue

        # Find matching node in target
        target_node, _, _ = find_node_and_parent(target_data, s_id)

        if target_node is None:
            print(
                f"[nexus_json_tools] Warning: Shard root {s_id} not found in target. "
                "Skipping (creation of new roots via fuse not yet supported)."
            )
            continue

        # UPDATE IN PLACE: replace the contents of target_node with shard_node,
        # preserving the parent/children list references that hold it.
        target_node.clear()
        target_node.update(shard_node)
        updates += 1

    recalc_all_counts(target_data)
    save_json(args.target_scry, target_data)
    print(f"[nexus_json_tools] Fused shard into target. Updated {updates} root subtrees.")


def _index_shard_roots(roots: List[JsonDict]) -> Tuple[Dict[str, JsonDict], Dict[str, Optional[str]], Dict[str, List[str]]]:
    """Build id -> node, id -> parent_id, and id -> child_ids for a shard tree."""
    by_id: Dict[str, JsonDict] = {}
    parent: Dict[str, Optional[str]] = {}
    children_ids: Dict[str, List[str]] = {}

    def walk(node: JsonDict, parent_id: Optional[str]) -> None:
        nid = node.get("id")
        if nid:
            by_id[nid] = node
            parent[nid] = parent_id
        kids = node.get("children") or []
        if isinstance(kids, list) and nid:
            children_ids[nid] = [c.get("id") for c in kids if isinstance(c, dict) and c.get("id")]
        for c in kids or []:
            if isinstance(c, dict):
                walk(c, nid)

    for root in roots or []:
        if isinstance(root, dict):
            walk(root, None)

    return by_id, parent, children_ids


def _index_territory(data: JsonDict) -> Tuple[Dict[str, JsonDict], Dict[str, Optional[str]], Dict[str, List[JsonDict]]]:
    """Build id -> node, id -> parent_id, id -> children_list for the Territory."""
    by_id: Dict[str, JsonDict] = {}
    parent: Dict[str, Optional[str]] = {}
    children_lists: Dict[str, List[JsonDict]] = {}

    nodes = data.get("nodes") or []

    def walk(node: JsonDict, parent_id: Optional[str]) -> None:
        nid = node.get("id")
        if nid:
            by_id[nid] = node
            parent[nid] = parent_id
        kids = node.get("children") or []
        if isinstance(kids, list) and nid:
            children_lists[nid] = kids
        for c in kids or []:
            if isinstance(c, dict):
                walk(c, nid)

    for root in nodes:
        if isinstance(root, dict):
            walk(root, None)

    return by_id, parent, children_lists


def _register_subtree_in_indexes(
    node: JsonDict,
    parent_id: Optional[str],
    t_by_id: Dict[str, JsonDict],
    t_parent: Dict[str, Optional[str]],
    t_children_lists: Dict[str, List[JsonDict]],
) -> None:
    if not isinstance(node, dict):
        return
    nid = node.get("id")
    if nid:
        t_by_id[nid] = node
        t_parent[nid] = parent_id
    children = node.get("children") or []
    if nid and isinstance(children, list):
        t_children_lists[nid] = children
    for child in children:
        if isinstance(child, dict):
            _register_subtree_in_indexes(child, nid, t_by_id, t_parent, t_children_lists)


def _remove_subtree_from_indexes(
    node: JsonDict,
    t_by_id: Dict[str, JsonDict],
    t_parent: Dict[str, Optional[str]],
    t_children_lists: Dict[str, List[JsonDict]],
) -> None:
    if not isinstance(node, dict):
        return
    for child in node.get("children") or []:
        if isinstance(child, dict):
            _remove_subtree_from_indexes(child, t_by_id, t_parent, t_children_lists)
    nid = node.get("id")
    if nid:
        t_by_id.pop(nid, None)
        t_parent.pop(nid, None)
        t_children_lists.pop(nid, None)


def _expand_with_ancestors(ids: Set[str], parent_map: Dict[str, Optional[str]]) -> None:
    queue = list(ids)
    while queue:
        nid = queue.pop()
        parent_id = parent_map.get(nid)
        if parent_id and parent_id not in ids:
            ids.add(parent_id)
            queue.append(parent_id)


def _collect_subtree_ids(root_id: str, children_ids: Dict[str, List[str]]) -> List[str]:
    seen: List[str] = []
    stack: List[str] = [root_id]
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.append(nid)
        for cid in children_ids.get(nid, []):
            stack.append(cid)
    return seen


# @beacon[
#   id=auto-beacon@_compute_delete_roots-95tm,
#   role=_compute_delete_roots,
#   slice_labels=ra-reconcile,
#   kind=ast,
# ]
def _compute_delete_roots(delete_ids: List[str], parent_map: Dict[str, Optional[str]]) -> List[str]:
    """Given a set of ids to delete and a parent map from S0, compute branch roots.

    This mirrors the logic used in workflowy_move_reconcile: we only need to
    explicitly delete the highest ancestors; their descendants go with them.
    """
    to_delete_set = set(delete_ids)
    roots: set[str] = set()
    for d in to_delete_set:
        cur = parent_map.get(d)
        is_root = True
        while cur is not None:
            if cur in to_delete_set:
                is_root = False
                break
            cur = parent_map.get(cur)
        if is_root:
            roots.add(d)
    return list(roots)


def _recalc_counts_preserving_truncation(
    territory: JsonDict,
    affected_ids: Set[str],
    original_status: Dict[str, Optional[str]],
    original_truncated: Dict[str, bool],
) -> None:
    """Recalculate *_human_readable_only counts while preserving truncation.

    This helper is used after destructive operations (e.g. 3-way shard fuse) to
    refresh the human-facing counters while keeping children_status aligned with
    the original TERRAIN semantics. No functional behavior depends on the
    *_human_readable_only fields; they are for inspection only.
    """
    if not affected_ids:
        return

    def helper(node: JsonDict) -> int:
        children = node.get("children") or []
        total = 0
        for child in children:
            if isinstance(child, dict):
                total += 1 + helper(child)
        nid = node.get("id")
        if nid and nid in affected_ids:
            node["immediate_child_count__human_readable_only"] = len(children)
            node["total_descendant_count__human_readable_only"] = total
            if original_truncated.get(nid, False):
                prev = original_status.get(nid)
                if prev and prev != "complete":
                    node["children_status"] = prev
                else:
                    node["children_status"] = "incomplete"
            else:
                node["children_status"] = "complete"
        return total

    for root in territory.get("nodes") or []:
        if isinstance(root, dict):
            helper(root)


def cmd_fuse_shard_3way(args: argparse.Namespace) -> None:
    """3-way SHARD FUSE: merge S1 into Territory using S0 as witness.

    Inputs:
      - Territory T:  args.target_scry (full SCRY JSON)
      - Witness S0:   args.witness_shard (original shard after BURN THE LENS)
      - Morphed S1:   args.morphed_shard (edited shard after REFRACTION)

    Semantics (per root):
      - Nodes never present in S0 are treated as *hidden* Territory nodes and are
        always preserved (not deleted or moved).
      - Nodes present in S0 but not in S1 are treated as explicit deletions: the
        corresponding subtrees in Territory are removed.
      - Nodes present in S1 but not in S0 are treated as creations: new
        subtrees are added under the parents indicated by S1.
      - Nodes present in both S0 and S1 are updated and possibly reparented to
        match S1's structure. Sibling order for these "visible" children
        follows S1, while hidden children keep their original relative order
        and remain grouped ahead of the visible block for that parent.

    This guarantees:
      - No unintended orphaning of hidden descendants.
      - Explicit delete/move operations are detected as differences between S0
        and S1, not inferred from absence in a shallow shard alone.
    """

    log_jewel(
        f"fuse-shard-3way: target={args.target_scry}, "
        f"witness={args.witness_shard}, morphed={args.morphed_shard}"
    )

    territory = load_json(args.target_scry)
    witness = load_json(args.witness_shard)
    morphed = load_json(args.morphed_shard)

    witness_roots = witness.get("nodes") or []
    morphed_roots = morphed.get("nodes") or []

    if not isinstance(witness_roots, list) or not isinstance(morphed_roots, list):
        die("Both witness and morphed shards must have a 'nodes' list")

    # Index S0, S1, and Territory
    s0_by_id, s0_parent, s0_children_ids = _index_shard_roots(witness_roots)
    s1_by_id, s1_parent, s1_children_ids = _index_shard_roots(morphed_roots)
    t_by_id, t_parent, t_children_lists = _index_territory(territory)

    # --- Invariant checks: enforce correct NEXUS pipeline before fusing ---
    ids_T1: Set[str] = set(t_by_id.keys())
    ids_S0: Set[str] = set(s0_by_id.keys())
    ids_S1: Set[str] = set(s1_by_id.keys())

    # (A) GEM must be fully embedded into TERRAIN: every GEM id should be
    # present in shimmering_terrain.json. If not, it likely means IGNITE SHARDS
    # was run after ATTACH GEMS (or ATTACH GEMS was skipped entirely).
    missing_in_T1 = ids_S0 - ids_T1
    if missing_in_T1:
        msg = (
            "nexus_anchor_jewels / fuse-shard-3way invariant violation: "
            "phantom_gem (S0) contains Workflowy ids that are not present in "
            "shimmering_terrain (T1). This usually means you ran IGNITE SHARDS "
            "after ATTACH GEMS or never called ATTACH GEMS for this tag."
        )
        log_jewel(msg + f" Offending ids (sample): {sorted(list(missing_in_T1))[:5]}")
        die(msg)

    # (B) JEWEL must not introduce new Workflowy ids that were never in the GEM.
    # New JEWEL nodes should be id-less; WEAVE will create real ids in ETHER.
    extra_S1_vs_S0 = ids_S1 - ids_S0
    if extra_S1_vs_S0:
        msg = (
            "nexus_anchor_jewels / fuse-shard-3way invariant violation: "
            "phantom_jewel (S1) contains Workflowy ids that do not appear in "
            "phantom_gem (S0). JEWELSTORM must not introduce new Workflowy ids; "
            "only id-less nodes are allowed as new children. Did you run a new "
            "SCRY/IGNITE or copy ids from another tree after capturing the GEM?"
        )
        log_jewel(msg + f" Offending ids (sample): {sorted(list(extra_S1_vs_S0))[:5]}")
        die(msg)

    original_status: Dict[str, Optional[str]] = {}
    original_truncated: Dict[str, bool] = {}
    for nid, node in t_by_id.items():
        if not nid:
            continue
        status = node.get("children_status")
        # Truncation semantics belong to the TERRAIN layer and are encoded via
        # children_status. We no longer infer truncation from count fields; the
        # *_human_readable_only counters are purely informational.
        original_status[nid] = status
        original_truncated[nid] = bool(status and status != "complete")

    affected_ids: Set[str] = set()

    # Determine which roots to process.
    # Normal case: intersection of root_ids present in both S0 and S1.
    # Special case: if JEWEL (S1) has no roots at all, interpret that as a
    # request to delete the entire GEM shard (all S0 roots) from Territory.
    s0_root_ids = [n.get("id") for n in witness_roots if isinstance(n, dict) and n.get("id")]
    s1_root_ids = {n.get("id") for n in morphed_roots if isinstance(n, dict) and n.get("id")}
    root_ids = [rid for rid in s0_root_ids if rid in s1_root_ids]

    if not root_ids:
        # If JEWEL is completely empty (no nodes at all), treat this as
        # "delete all GEM roots" rather than a structural error.
        if not morphed_roots:
            root_ids = s0_root_ids
        else:
            die("No overlapping shard roots between witness and morphed shards")

    updates = 0
    creates = 0
    deletes = 0
    moves = 0
    reorders = 0

    for root_id in root_ids:
        if root_id not in t_by_id:
            print(f"[nexus_json_tools] Warning: root {root_id} not found in Territory; skipping")
            continue

        # Collect subtree ids for this root in S0 and S1
        ids_s0 = set(_collect_subtree_ids(root_id, s0_children_ids)) if root_id in s0_by_id else set()
        ids_s1 = set(_collect_subtree_ids(root_id, s1_children_ids)) if root_id in s1_by_id else set()

        # Partition ids for this shard root
        delete_ids = list(ids_s0 - ids_s1)
        new_ids = ids_s1 - ids_s0
        common_ids = ids_s0 & ids_s1

        # Compute S1 depths for this shard root so that, if S1 ever introduces
        # new nodes *with* real Workflowy ids, we create parents before
        # children. In the canonical JEWELSTORM flow, new nodes are id-less and
        # handled separately below; this depth map is a forward-compatible
        # safeguard.
        depth_s1: Dict[str, int] = {}
        stack: List[Tuple[str, int]] = [(root_id, 0)] if root_id in ids_s1 else []
        while stack:
            cur_id, d = stack.pop()
            if cur_id in depth_s1:
                continue
            depth_s1[cur_id] = d
            for cid in s1_children_ids.get(cur_id, []):
                if cid in ids_s1:
                    stack.append((cid, d + 1))

        # --- CREATE: nodes present in S1 but not in S0 (id-based) ---
        # Process in top-down order so that any new parent ids are created
        # before their new children.
        ordered_new_ids = sorted(new_ids, key=lambda nid: depth_s1.get(nid, 0))
        for nid in ordered_new_ids:
            s1_node = s1_by_id.get(nid)
            if s1_node is None:
                continue
            parent_id = s1_parent.get(nid)
            if parent_id is None:
                parent_children = territory.get("nodes")
                if not isinstance(parent_children, list):
                    parent_children = []
                    territory["nodes"] = parent_children
            else:
                parent_node = t_by_id.get(parent_id)
                if parent_node is None:
                    print(
                        f"[nexus_json_tools] Warning: new node {nid} has parent {parent_id} "
                        "which is not present in Territory; skipping creation."
                    )
                    continue
                parent_children = parent_node.get("children")
                if not isinstance(parent_children, list):
                    parent_children = []
                    parent_node["children"] = parent_children
                    t_children_lists[parent_id] = parent_children

            new_node = copy.deepcopy(s1_node)
            parent_children.append(new_node)
            _register_subtree_in_indexes(new_node, parent_id, t_by_id, t_parent, t_children_lists)
            affected_ids.update(filter(None, [nid, parent_id]))
            creates += 1

        # --- CREATE: id-less direct children from S1 under existing parents ---
        # These are new subtrees introduced by JEWELSTORM that intentionally
        # have no Workflowy id yet. We append them to the corresponding Territory
        # parents so the reconciliation algorithm can CREATE them.
        for p, s1_parent_node in s1_by_id.items():
            if p not in ids_s1:
                continue
            t_parent_node = t_by_id.get(p)
            if t_parent_node is None:
                continue
            t_children = t_parent_node.get("children")
            if not isinstance(t_children, list):
                t_children = []
                t_parent_node["children"] = t_children
                t_children_lists[p] = t_children
            else:
                t_children_lists.setdefault(p, t_children)

            # Build a simple signature for existing id-less children to avoid
            # obvious duplicates when re-running fuse on the same Territory.
            existing_signatures: set[tuple[Any, Any]] = set()
            for child in t_children:
                if not isinstance(child, dict) or child.get("id") is not None:
                    continue
                existing_signatures.add((child.get("name"), child.get("note")))

            for child in s1_parent_node.get("children") or []:
                if not isinstance(child, dict) or child.get("id") is not None:
                    continue
                sig = (child.get("name"), child.get("note"))
                if sig in existing_signatures:
                    continue
                new_child = copy.deepcopy(child)
                t_children.append(new_child)
                existing_signatures.add(sig)
                affected_ids.add(p)
                creates += 1

        # --- MOVE: nodes in both S0 and S1 whose parent changed ---
        for nid in common_ids:
            s1_p = s1_parent.get(nid)
            t_p = t_parent.get(nid)
            if s1_p == t_p:
                continue
            if t_p is None:
                old_siblings = territory.get("nodes") or []
            else:
                old_siblings = t_children_lists.get(t_p) or []
            old_siblings[:] = [n for n in old_siblings if not (isinstance(n, dict) and n.get("id") == nid)]
            if t_p is not None:
                t_children_lists[t_p] = old_siblings

            if s1_p is None:
                new_siblings = territory.get("nodes")
                if not isinstance(new_siblings, list):
                    new_siblings = []
                    territory["nodes"] = new_siblings
            else:
                new_parent_node = t_by_id.get(s1_p)
                if new_parent_node is None:
                    print(
                        f"[nexus_json_tools] Warning: cannot reparent {nid} to {s1_p} "
                        "(parent not found in Territory); leaving in place."
                    )
                    continue
                new_siblings = new_parent_node.get("children")
                if not isinstance(new_siblings, list):
                    new_siblings = []
                    new_parent_node["children"] = new_siblings
                t_children_lists[s1_p] = new_siblings

            node_obj = t_by_id.get(nid)
            if node_obj is None:
                continue
            new_siblings.append(node_obj)
            t_parent[nid] = s1_p
            affected_ids.update(filter(None, [nid, t_p, s1_p]))
            moves += 1

        # --- DELETE: nodes present in S0 but not in S1 (after moves) ---
        delete_roots = _compute_delete_roots(delete_ids, s0_parent)
        for nid in delete_roots:
            t_node = t_by_id.get(nid)
            if t_node is None:
                continue
            parent_id = t_parent.get(nid)
            if parent_id is None:
                siblings = territory.get("nodes") or []
            else:
                siblings = t_children_lists.get(parent_id) or []
            siblings[:] = [n for n in siblings if not (isinstance(n, dict) and n.get("id") == nid)]
            _remove_subtree_from_indexes(t_node, t_by_id, t_parent, t_children_lists)
            if parent_id:
                affected_ids.add(parent_id)
            deletes += 1

        # --- REORDER: align order of visible children with S1, keep hidden + id-less children ---
        parents_to_consider = {s1_parent[nid] for nid in ids_s1} | {root_id}
        parents_to_consider = {p for p in parents_to_consider if p is not None and p in t_by_id}

        for p in parents_to_consider:
            desired_ids = s1_children_ids.get(p, [])
            if not desired_ids:
                continue

            children_list = t_children_lists.get(p)
            if children_list is None:
                parent_node = t_by_id[p]
                new_children_list: List[JsonDict] = []
                for cid in desired_ids:
                    child_node = t_by_id.get(cid)
                    if child_node is not None:
                        new_children_list.append(child_node)
                parent_node["children"] = new_children_list
                t_children_lists[p] = new_children_list
                if new_children_list:
                    reorders += 1
                affected_ids.add(p)
                continue

            hidden_children: List[JsonDict] = []
            for child in children_list:
                if not isinstance(child, dict):
                    continue
                cid = child.get("id")
                # Preserve id-less children (new JEWELSTORM subtrees and hidden Territory
                # nodes that never had an id in the shard) as part of the hidden block.
                if cid is None:
                    hidden_children.append(child)
                    continue
                # Also preserve Territory nodes that are outside the S0/S1 shard entirely.
                if cid not in ids_s0 and cid not in ids_s1:
                    hidden_children.append(child)

            new_children_list: List[JsonDict] = []
            new_children_list.extend(hidden_children)
            for cid in desired_ids:
                child_node = t_by_id.get(cid)
                if child_node is not None and child_node not in new_children_list:
                    new_children_list.append(child_node)

            if new_children_list != children_list:
                parent_node = t_by_id[p]
                parent_node["children"] = new_children_list
                t_children_lists[p] = new_children_list
                reorders += 1
                affected_ids.add(p)

        # --- UPDATE content for common ids ---
        for nid in common_ids:
            t_node = t_by_id.get(nid)
            s1_node = s1_by_id.get(nid)
            if t_node is None or s1_node is None:
                continue
            t_node["name"] = s1_node.get("name")
            t_node["note"] = s1_node.get("note")
            t_node["data"] = s1_node.get("data")
            t_node["completed"] = bool(s1_node.get("completed", False))
            affected_ids.add(nid)
            updates += 1

    # Expand affected set to include ancestors so ancestor counts stay correct
    affected_ids = {nid for nid in affected_ids if nid}
    _expand_with_ancestors(affected_ids, t_parent)

    _recalc_counts_preserving_truncation(territory, affected_ids, original_status, original_truncated)

    save_json(args.target_scry, territory)
    summary_msg = (
        "[nexus_json_tools] 3-way fuse complete: "
        f"updates={updates}, creates={creates}, deletes={deletes}, moves={moves}, reorders={reorders}."
    )
    print(summary_msg)
    log_jewel(summary_msg)


def cmd_delete_node(args: argparse.Namespace) -> None:
    data = load_json(args.file)
    node, parent, siblings = find_node_and_parent(data, args.id)
    if node is None or siblings is None:
        die(f"Node id not found: {args.id}")

    # Remove the node from its siblings list
    before_len = len(siblings)
    siblings[:] = [n for n in siblings if n is not node]
    after_len = len(siblings)
    if before_len == after_len:
        die(f"Internal error: node {args.id} not actually present in siblings list")

    recalc_all_counts(data)
    save_json(args.file, data)
    print(f"[nexus_json_tools] Deleted node {args.id!r} and its subtree.")


def cmd_burn_gem(args: argparse.Namespace) -> None:
    """Rehydrate selected roots from a full SCRY and extract a full GEM shard.

    Inputs:
      - args.file      = Territory SCRY JSON (possibly truncated by max_depth / child_count_limit)
      - --full-scry    = Full-depth SCRY JSON for the same Workflowy tree (or at
                         least for the selected roots).
      - --root-ids     = One or more node UUIDs identifying GEM roots (these
                         IDs must appear in BOTH Territory and full-scry).
      - --output-shard = Path for GEM shard JSON (S0 for these roots).

    Behavior:
      - For each root_id, we locate its subtree in the full SCRY and deep-copy
        that subtree into two places:
          1) Territory: we replace the corresponding truncated subtree under
             that root in Territory, effectively rehydrating it.
          2) GEM shard: we append the subtree to a new shard JSON (S0) that
             will be used as the witness shard for 3-way fuse.
      - After this, the Territory JSON now contains full subtrees for the GEM
        roots, and the GEM shard is a full-depth snapshot of those same
        subtrees.

    This allows a workflow of:
      1) Coarse SCRY (small depth/width) → Territory.
      2) Full SCRY for a few selected roots → full-scry.json.
      3) burn-gem over those roots to splice full GEMs into Territory and
         extract GEM shard S0.
      4) Edit GEM shard (S1) deeply.
      5) Run fuse-shard-3way (T/S0/S1) and then WEAVE.
    """

    territory = load_json(args.file)
    full_scry = load_json(args.full_scry)

    gem_roots: List[JsonDict] = []

    for root_id in args.root_ids:
        # 1) Locate full subtree in full_scry
        full_node, full_parent, full_siblings = find_node_and_parent(full_scry, root_id)
        if full_node is None:
            die(f"[burn-gem] Root id {root_id!r} not found in full-scry JSON {args.full_scry}")

        full_subtree = copy.deepcopy(full_node)
        gem_roots.append(copy.deepcopy(full_subtree))

        # 2) Locate corresponding node in Territory and replace its subtree
        terr_node, terr_parent, terr_siblings = find_node_and_parent(territory, root_id)
        if terr_node is None or terr_siblings is None:
            die(f"[burn-gem] Root id {root_id!r} not found in Territory JSON {args.file}")

        # Replace node in its siblings list
        replaced = False
        for i, n in enumerate(terr_siblings):
            if isinstance(n, dict) and n.get("id") == root_id:
                terr_siblings[i] = full_subtree
                replaced = True
                break
        if not replaced:
            die(f"[burn-gem] Internal error: unable to replace root {root_id!r} in Territory siblings list")

    # Save updated Territory (hydrated for these GEM roots)
    save_json(args.file, territory)

    # Save GEM shard (S0) containing all rehydrated roots
    shard_data: JsonDict = {
        "shard_source_file": os.path.basename(args.full_scry),
        "shard_root_ids": args.root_ids,
        "nodes": gem_roots,
    }
    save_json(args.output_shard, shard_data)

    print(
        f"[nexus_json_tools] burn-gem complete: hydrated {len(args.root_ids)} roots into Territory "
        f"and wrote GEM shard to {args.output_shard!r}."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manipulate NEXUS SCRY JSON files (rename/set-note/delete/move/shard fuse).",
    )
    parser.add_argument(
        "file",
        help="Path to SCRY JSON file (e.g., qm-XXXX-...--nexus_*.json). Not used for all commands.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # rename-node
    p_rename = subparsers.add_parser(
        "rename-node", help="Rename a node by id (update its 'name' field)",
    )
    p_rename.add_argument("--id", required=True, help="Node UUID to rename")
    p_rename.add_argument("--name", required=True, help="New name for the node")
    p_rename.set_defaults(func=cmd_rename_node)

    # set-note
    p_note = subparsers.add_parser(
        "set-note", help="Set/replace the 'note' field for a node",
    )
    p_note.add_argument("--id", required=True, help="Node UUID whose note to update")
    p_note.add_argument(
        "--note",
        required=True,
        help="New note text (use \n for newlines; JSON will store real newlines)",
    )
    p_note.add_argument(
        "--show-old",
        action="store_true",
        help="Print the previous note value for inspection",
    )
    p_note.add_argument(
        "--show-new",
        action="store_true",
        help="Print the new note value after change",
    )
    p_note.set_defaults(func=cmd_set_note)

    # delete-node
    p_del = subparsers.add_parser(
        "delete-node", help="Delete a node (and its subtree) by id",
    )
    p_del.add_argument("--id", required=True, help="Node UUID to delete")
    p_del.set_defaults(func=cmd_delete_node)

    # move-node
    p_move = subparsers.add_parser(
        "move-node", help="Move a node to a new parent within the SCRY JSON",
    )
    p_move.add_argument("--id", required=True, help="Node UUID to move")
    p_move.add_argument("--parent-id", required=True, help="New parent UUID (or export_root_id for top-level)")
    p_move.add_argument(
        "--position", choices=["top", "bottom"], default="bottom",
        help="Position in new parent's children list",
    )
    p_move.set_defaults(func=cmd_move_node)

    # extract-shard (2-way shard; S0 captured in a single file)
    p_extract = subparsers.add_parser(
        "extract-shard", help="Extract a subset of nodes (and their descendants) to a new JSON file",
    )
    p_extract.add_argument("--root-ids", required=True, nargs="+", help="List of Node UUIDs to be the roots of the shard")
    p_extract.add_argument("--output", required=True, help="Output path for the shard JSON")
    p_extract.set_defaults(func=cmd_extract_shard)

    # burn-gem (rehydrate selected roots from full SCRY and extract GEM shard)
    p_gem = subparsers.add_parser(
        "burn-gem", help="Rehydrate selected roots from a full SCRY into Territory and extract a GEM shard",
    )
    p_gem.add_argument(
        "--full-scry",
        required=True,
        help="Path to full-depth SCRY JSON for GEM roots (usually produced by nexus_scry with generous depth/child limits)",
    )
    p_gem.add_argument(
        "--root-ids",
        required=True,
        nargs="+",
        help="List of Node UUIDs to treat as GEM roots (must exist in both Territory and full-scry)",
    )
    p_gem.add_argument(
        "--output-shard",
        required=True,
        help="Output path for the GEM shard JSON (S0)",
    )
    p_gem.set_defaults(func=cmd_burn_gem)

    # fuse-shard (legacy 2-way replacement)
    p_fuse = subparsers.add_parser(
        "fuse-shard", help="Legacy 2-way: replace root subtrees in target with shard roots",
    )
    p_fuse.add_argument("--shard-file", required=True, help="Path to the modified shard JSON")
    p_fuse.add_argument("--target-scry", required=True, help="Path to the target SCRY JSON to update")
    p_fuse.set_defaults(func=cmd_fuse_shard)

    # fuse-shard-3way (new T/S0/S1 aware fuse)
    p_fuse3 = subparsers.add_parser(
        "fuse-shard-3way", help="3-way SHARD FUSE: Territory + witness shard S0 + morphed shard S1",
    )
    p_fuse3.add_argument(
        "--witness-shard",
        required=True,
        help="Path to original shard JSON (S0) captured right after BURN THE LENS",
    )
    p_fuse3.add_argument(
        "--morphed-shard",
        required=True,
        help="Path to modified shard JSON (S1) after REFRACTION/QUILLSTRIKE",
    )
    p_fuse3.add_argument(
        "--target-scry",
        required=True,
        help="Path to the Territory SCRY JSON to update (T)",
    )
    p_fuse3.set_defaults(func=cmd_fuse_shard_3way)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
