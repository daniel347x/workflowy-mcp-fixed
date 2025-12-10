#!/usr/bin/env python3
"""
JEWELSTRIKE - Strike the phantom gem to create working_gem + witness_gem

Part of the JEWELSTORM workflow (parallel to QUILLSTORM for PHANTOM GEMs).

Purpose: 
- Creates fresh working_gem (UUID-prefixed temp copy for semantic editing)
- Creates witness_gem (point-in-time backup for comparison)
- Adds jewel_id to all nodes (local handles for transform_jewel operations)
- GUARANTEES fresh read (working_gem created at invocation time)
- Enables parallel conversation safety (unique UUID prevents collision)

Usage:
    python jewelstrike.py "<nexus-tag>" [human-prefix]

Returns JSON to stdout:
    {
        "working_gem": "path/to/temp/jm-[hash]-[prefix]--phantom_gem.json",
        "witness_gem": "path/to/temp/jm-[hash]-[prefix]--phantom_gem.json.original.json", 
        "hash": "a3f7",
        "phantom_gem": "path/to/original/phantom_gem.json",
        "guardian_token": "hidden-from-agent"
    }
"""

import os
import sys
import json
import shutil
import secrets
from pathlib import Path

INDEX_FILENAME = "jewelstorm_index.json"


def build_jewel_map(nodes: list, depth: int = 0) -> dict:
    """Build indented jewel_map showing tree structure.
    
    Returns dict mapping jewel_id to indented name string.
    Uses bullets (•) and 4-space indentation per level.
    """
    jewel_map = {}
    indent = "    " * depth
    bullet = "• " if depth == 0 else "• "
    
    for node in nodes:
        jewel_id = node.get("jewel_id")
        name = node.get("name", "Untitled")
        
        # Truncate very long names
        if len(name) > 80:
            name = name[:77] + "..."
        
        jewel_map[jewel_id] = f"{indent}{bullet}{name}"
        
        # Recurse into children
        children = node.get("children", [])
        if children:
            child_map = build_jewel_map(children, depth + 1)
            jewel_map.update(child_map)
    
    return jewel_map


def _build_jewel_preview_lines(roots: list, max_note_chars: int = 1024) -> list[str]:
    """Build human-readable preview of JEWEL tree (JEWELSTRIKE initial state).

    Format:
        [J-001] ⦿ Node Name [note-preview]
    with 4-space indentation per depth level and truncated note previews.

    This helper is shared with nexus_json_tools.transform_jewel so both
    JEWELSTRIKE and transform_jewel produce consistent JEWEL previews.
    """
    # PASS 1: Collect all jewel_id / id labels to compute max width
    all_labels: list[str] = []

    def collect_labels(node: dict) -> None:
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
    lines: list[str] = []

    def walk(node: dict, depth: int) -> None:
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
        if not sk_hint and node.get("subtree_mode") == "shell":
            sk_hint = "DELETE_SECTION"
        if sk_hint == "DELETE_SECTION":
            hint_prefix = "[DELETE] "
        elif sk_hint == "MERGE_TARGET":
            hint_prefix = "[MERGE] "
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


def add_jewel_ids_recursive(node: dict, counter: list) -> None:
    """Add jewel_id to node and all descendants recursively, stripping noisy fields.

    This prepares the HOT VISIBLE GEM for JEWELSTORM by removing per-node
    metadata that is not needed for semantic editing (and inflates token
    usage):

    - completed
    - data (e.g., {"layoutMode": "bullets"})
    - createdAt / modifiedAt / completedAt

    The full metadata remains available in phantom_gem.json and, if desired,
    in witness/guardian copies. The working_gem focuses on structure and
    content (id, jewel_id, name, note, children).
    """
    if "jewel_id" not in node:
        node["jewel_id"] = f"J-{counter[0]:03d}"
        counter[0] += 1

    # Strip per-node noise fields for HOT VISIBLE GEM
    for key in ("completed", "data", "createdAt", "modifiedAt", "completedAt"):
        if key in node:
            node.pop(key, None)

    children = node.get("children") or []
    for child in children:
        add_jewel_ids_recursive(child, counter)


def jewelstrike(phantom_gem_file: str, human_prefix: str | None = None) -> dict:
    """
    JEWELSTRIKE the phantom gem - it sunders into working_gem + witness_gem.
    
    Creates:
    - working_gem: UUID-prefixed temp copy with jewel_ids added
    - witness_gem: Point-in-time backup for comparison
    
    Args:
        phantom_gem_file: Full path to phantom_gem.json to strike
        human_prefix: Optional human-friendly string for temp filenames
        
    Returns:
        Dictionary with paths to working_gem, witness_gem, hash, and guardian token
    """
    
    # Validate phantom_gem exists
    gem_path = Path(phantom_gem_file).resolve()
    if not gem_path.exists():
        return {
            "success": False,
            "error": f"Phantom gem file does not exist: {phantom_gem_file}"
        }

    # Temp directory (same as QUILLSTRIKE)
    temp_dir = Path(r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp")
    temp_dir.mkdir(exist_ok=True)

    # Guardian directory
    guardian_dir = temp_dir / ".jewel-guardian"
    guardian_dir.mkdir(exist_ok=True)

    # Load or initialize index
    index_path = guardian_dir / INDEX_FILENAME
    try:
        if index_path.exists():
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        else:
            index = {}
    except Exception:
        index = {}

    gem_key = str(gem_path)
    existing = index.get(gem_key)
    if existing is not None:
        return {
            "success": False,
            "error": (
                "💎 THE JEWEL GUARDIAN IS ALREADY ACTIVE. A prismatic field currently protects this phantom gem.\n\n"
                f"File: {gem_key}\n"
                f"Active hash: {existing.get('hash')}\n\n"
                "You cannot strike a new field while one is already shimmering.\n"
                "Use jeweldrop.py to collapse the existing session if it is stale."
            ),
        }

    # Generate 4-character hash for filename prefix
    hash_value = secrets.token_hex(2)

    # Generate 8-character guardian token
    guardian_token = secrets.token_hex(4)

    # Determine paths
    basename = gem_path.name
    
    if human_prefix:
        safe_prefix = "".join(c if c.isalnum() or c in "-_" else "-" for c in human_prefix).strip("-")
        file_prefix = f"jm-{hash_value}-{safe_prefix}--"
    else:
        file_prefix = f"jm-{hash_value}--"

    # Working gem
    working_gem = temp_dir / f"{file_prefix}{basename}"
    
    # Witness gem
    witness_gem = temp_dir / f"{file_prefix}{basename}.original.json"
    
    # Guardian gem (hidden)
    guardian_gem = guardian_dir / f"{hash_value}-{safe_prefix if human_prefix else 'gem'}.witness"
    
    # Delete stale files
    if witness_gem.exists():
        witness_gem.unlink()
    if guardian_gem.exists():
        guardian_gem.unlink()
    
    # Load phantom_gem JSON and add jewel_ids
    try:
        with open(gem_path, "r", encoding="utf-8") as f:
            gem_data = json.load(f)
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to read phantom_gem JSON: {e}"
        }

    # Write full operations reference to separate file in temp directory
    operations_reference = {
        "_HELP": "JEWELSTORM Operations Reference - Complete catalog for nexus_transform_jewel",
        "_NOTE": "This file is AUTO-GENERATED by jewelstrike.py and AUTO-DELETED by jewelmorph.py. Do not edit manually.",
        "MOVE_NODE": {
            "description": "Move a node to a new parent or reorder siblings",
            "required": ["jewel_id"],
            "optional": ["new_parent_jewel_id", "position", "relative_to_jewel_id"],
            "position_values": ["FIRST", "LAST", "BEFORE", "AFTER"],
            "notes": [
                "Must provide either new_parent_jewel_id OR relative_to_jewel_id (for BEFORE/AFTER)",
                "Position defaults to LAST if not specified",
                "BEFORE/AFTER require relative_to_jewel_id to specify anchor sibling"
            ]
        },
        "DELETE_NODE": {
            "description": "Delete a node and its entire subtree",
            "required": ["jewel_id"],
            "optional": ["delete_from_ether", "mode"],
            "modes": ["SMART (default)", "FAIL_IF_HAS_CHILDREN"],
            "notes": [
                "SMART mode: Allows deleting nodes with JEWEL-only children; requires delete_from_ether=true for ETHER-backed children",
                "Deleted nodes are removed from working_gem; JEWELMORPH preserves deletion in phantom_jewel.json"
            ]
        },
        "DELETE_ALL_CHILDREN": {
            "description": "Delete all immediate children of a node (keep the node itself)",
            "required": ["jewel_id"],
            "optional": ["delete_from_ether", "mode"],
            "notes": [
                "Same SMART/FAIL_IF_HAS_CHILDREN semantics as DELETE_NODE",
                "Use to clear a parent before adding new children"
            ]
        },
        "RENAME_NODE": {
            "description": "Change the name (title) of a node",
            "required": ["jewel_id"],
            "parameter_options": ["name (recommended)", "new_name (legacy)"],
            "notes": [
                "Both 'name' and 'new_name' are accepted for backward compatibility",
                "Use 'name' for consistency with CREATE_NODE"
            ]
        },
        "SET_NOTE": {
            "description": "Set or update the note field of a node",
            "required": ["jewel_id"],
            "parameter_options": ["note (recommended)", "new_note (legacy)"],
            "notes": [
                "Both 'note' and 'new_note' are accepted for backward compatibility",
                "Use 'note' for consistency with CREATE_NODE",
                "Pass empty string or null to clear the note"
            ]
        },
        "SET_ATTRS": {
            "description": "Set node attributes (completed, layoutMode, priority, tags)",
            "required": ["jewel_id", "attrs (dict)"],
            "allowed_attr_keys": ["completed", "layoutMode", "priority", "tags"],
            "notes": [
                "attrs must be a dict mapping allowed keys to values",
                "Set value to null to remove an attribute",
                "Example: {'completed': true, 'layoutMode': 'todo'}"
            ]
        },
        "CREATE_NODE": {
            "description": "Create a new node with optional children (recursive subtree)",
            "required": ["name"],
            "optional": ["parent_jewel_id", "position", "relative_to_jewel_id", "note", "attrs", "data", "jewel_id", "children", "node"],
            "position_values": ["FIRST", "LAST", "BEFORE", "AFTER"],
            "notes": [
                "Must provide either parent_jewel_id OR relative_to_jewel_id (for BEFORE/AFTER)",
                "New nodes are created WITHOUT Workflowy id (JEWELMORPH preserves this so WEAVE treats them as CREATE operations)",
                "Can provide jewel_id explicitly or let system auto-generate",
                "Supports nested children array for recursive tree creation",
                "Two formats: compact (name/note/children at op level) or wrapped (inside 'node' key)"
            ]
        },
        "SEARCH_REPLACE": {
            "description": "Find and replace text across all nodes in GEM",
            "required": ["search", "replace", "fields"],
            "optional": ["case_sensitive", "whole_word", "regex"],
            "fields_values": ["name", "note", "both"],
            "notes": [
                "Operates on working_gem after JEWELSTRIKE",
                "Returns nodes_renamed and notes_updated counts",
                "Use dry_run: true to preview changes"
            ]
        },
        "SEARCH_AND_TAG": {
            "description": "Find text and add #tag to matching nodes",
            "required": ["search", "tag", "fields"],
            "optional": ["case_sensitive", "whole_word", "regex"],
            "fields_values": ["name", "note", "both"],
            "notes": [
                "Same search params as SEARCH_REPLACE",
                "Adds #tag to specified field(s) when match found"
            ]
        },
        "SET_ATTRS_BY_PATH": {
            "description": "Set attributes on a node by index path (used internally for JEWEL UUID injection after WEAVE CREATE phase)",
            "required": ["path (list of ints)", "attrs (dict)"],
            "allowed_attr_keys": ["id (Workflowy UUID)"],
            "notes": [
                "Path is list of 0-based indexes into nodes array and nested children",
                "Example: path=[0, 2, 1] navigates to nodes[0].children[2].children[1]",
                "Currently only supports setting 'id' attribute (for WEAVE Phase 1 UUID sync)"
            ]
        },
        "WORKFLOW_REMINDER": {
            "JEWELSTORM_LIFECYCLE": [
                "1. JEWELSTRIKE <nexus-tag> → Creates working_gem with jewel_ids",
                "2. JEWELSTORM editing → Use nexus_transform_jewel MCP tool",
                "3. JEWELMORPH → Strips jewel_ids, reattaches metadata, produces phantom_jewel.json",
                "4. nexus_anchor_jewels → 3-way fuse (T1 + S0 + S1 → enchanted_terrain.json)",
                "5. nexus_weave_enchanted_async → Apply enchanted_terrain back to Workflowy ETHER"
            ],
            "IDENTITY_NOTES": [
                "jewel_id: Local handles for JEWELSTORM editing (J-001, J-002, etc.)",
                "id: Workflowy UUID (preserved for existing nodes, omitted for new nodes)",
                "New nodes without 'id' field → WEAVE creates them in Workflowy",
                "Existing nodes with 'id' field → WEAVE updates them in place"
            ]
        }
    }
    
    # Write full reference to separate file (same directory as working/witness gems)
    reference_file = working_gem.with_name(f"{file_prefix}operations_reference.json")
    try:
        with open(reference_file, "w", encoding="utf-8") as f:
            json.dump(operations_reference, f, ensure_ascii=False, indent=2)
    except Exception as e:
        # Non-fatal - working_gem can still function without external reference
        pass

    # Strip root-level heavy metadata from HOT VISIBLE GEM (phantom_gem still has it)
    for key in (
        "original_ids_seen",
        "explicitly_preserved_ids",
        "export_root_id",
        "export_root_name",
        "export_root_children_status",
        "jewel_file",
    ):
        if key in gem_data:
            gem_data.pop(key, None)
    
    # Add jewel_ids to all nodes
    counter = [1]  # Mutable counter for recursion
    nodes = gem_data.get("nodes", [])
    for node in nodes:
        add_jewel_ids_recursive(node, counter)

    # Normalize key order so id / preview_id / jewel_id appear at the top of each node
    def _normalize_node_key_order(node: dict) -> dict:
        front_keys = ["id", "preview_id", "jewel_id"]
        ordered: dict = {}
        for k in front_keys:
            if k in node:
                ordered[k] = node[k]
        for k, v in node.items():
            if k not in ordered:
                ordered[k] = v
        children = ordered.get("children")
        if isinstance(children, list):
            new_children = []
            for ch in children:
                if isinstance(ch, dict):
                    new_children.append(_normalize_node_key_order(ch))
                else:
                    new_children.append(ch)
            ordered["children"] = new_children
        return ordered

    if isinstance(nodes, list):
        gem_data["nodes"] = [_normalize_node_key_order(n) for n in nodes if isinstance(n, dict)]
    
    # Build JEWEL preview (ephemeral, for agents/humans; never read by algorithms)
    try:
        preview_tree = _build_jewel_preview_lines(nodes)
    except Exception:
        preview_tree = []
    
    # Build jewel_map showing tree structure with indentation
    jewel_map = build_jewel_map(nodes)
    
    # Create minimal inline header (replaces bloated operations catalog)
    minimal_header = {
        "__preview_tree__": preview_tree,
        "_jewelstorm_help": {
            "operations": "CREATE_NODE, DELETE_NODE, MOVE_NODE, RENAME_NODE, SET_NOTE, SET_ATTRS, SEARCH_REPLACE, SEARCH_AND_TAG",
            "full_docs": str(reference_file),
            "jewel_map": jewel_map
        }
    }
    
    # Inject minimal header at root level if this is an export package
    if isinstance(gem_data, dict) and "nodes" in gem_data:
        gem_data = {**minimal_header, **gem_data}
    
    # Write working_gem (with jewel_ids + operations help block)
    try:
        with open(working_gem, "w", encoding="utf-8") as f:
            json.dump(gem_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to write working_gem: {e}"
        }
    
    # Write witness_gem (with jewel_ids - same as working_gem initially)
    shutil.copy2(working_gem, witness_gem)
    
    # Write guardian_gem (with jewel_ids - HIDDEN validation copy)
    shutil.copy2(working_gem, guardian_gem)
    
    # Update index
    try:
        index[gem_key] = {
            "hash": hash_value,
            "human_prefix": safe_prefix if human_prefix else None,
            "working_gem": str(working_gem),
            "witness_gem": str(witness_gem),
            "guardian_token": guardian_token,
        }
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to update guardian index: {e}"
        }
    
    return {
        "success": True,
        "working_gem": str(working_gem),
        "witness_gem": str(witness_gem),
        "hash": hash_value,
        "phantom_gem": str(gem_path),
        "preview_tree": preview_tree,
    }


def resolve_phantom_gem_from_nexus_tag(nexus_tag: str) -> Path:
    """Resolve a NEXUS tag to its latest run dir and return phantom_gem.json path.

    Scans temp\\nexus_runs for directories named either:
    - <nexus_tag>
    - <TIMESTAMP>__<nexus_tag>

    Picks the lexicographically last match (latest run). Requires that
    phantom_gem.json exist in that directory. Raises FileNotFoundError with
    descriptive messages if anything is missing.
    """
    base_dir = Path(
        r"E:\\__daniel347x\\__Obsidian\\__Inking into Mind\\--TypingMind\\Projects - All\\Projects - Individual\\TODO\\temp\\nexus_runs"
    )
    if not base_dir.exists():
        raise FileNotFoundError(
            "No NEXUS runs directory exists yet under temp\\nexus_runs; "
            "run nexus_scry(...), nexus_glimpse(...), or Cartographer first for this tag."
        )

    suffix = f"__{nexus_tag}"
    candidates = [
        child
        for child in base_dir.iterdir()
        if child.is_dir() and (child.name == nexus_tag or child.name.endswith(suffix))
    ]
    if not candidates:
        raise FileNotFoundError(
            f"NEXUS tag '{nexus_tag}' has no run directory under temp\\nexus_runs; "
            "run nexus_scry(...), nexus_glimpse(..., mode='full'), or Cartographer first."
        )

    run_dir = sorted(candidates, key=lambda p: p.name)[-1]
    phantom_path = run_dir / "phantom_gem.json"
    if not phantom_path.exists():
        raise FileNotFoundError(
            f"NEXUS tag '{nexus_tag}' has no phantom_gem.json in {run_dir}; "
            "run nexus_ignite_shards(...) or nexus_glimpse(..., mode='full') first."
        )
    return phantom_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({
            "success": False,
            "error": "Usage: python jewelstrike.py <nexus-tag> [human-prefix] [--existing-jewel-from-exploration <jewel_json_path>]"
        }))
        sys.exit(1)

    nexus_tag = sys.argv[1]
    prefix: str | None = None
    existing_jewel: str | None = None

    # Parse optional human-prefix (positional) and override flag
    idx = 2
    if idx < len(sys.argv) and not sys.argv[idx].startswith("--"):
        prefix = sys.argv[idx]
        idx += 1

    while idx < len(sys.argv):
        arg = sys.argv[idx]
        if arg == "--existing-jewel-from-exploration" and idx + 1 < len(sys.argv):
            existing_jewel = sys.argv[idx + 1]
            idx += 2
        else:
            idx += 1

    try:
        phantom_gem_path = resolve_phantom_gem_from_nexus_tag(nexus_tag)
    except Exception as e:
        print(json.dumps({
            "success": False,
            "error": str(e)
        }))
        sys.exit(1)

    # Optional: seed phantom_jewel.json from an existing JEWEL produced by
    # exploration FINALIZE. This avoids confusion where agents only have a GEM
    # but no JEWEL to anchor.
    if existing_jewel is not None:
        try:
            src = Path(existing_jewel).resolve()
            if not src.exists():
                print(json.dumps({
                    "success": False,
                    "error": f"Existing JEWEL file not found: {existing_jewel}"
                }))
                sys.exit(1)

            # Minimal sanity check: must be JSON and contain 'nodes'
            with open(src, "r", encoding="utf-8") as f:
                jewel_data = json.load(f)
            if not (isinstance(jewel_data, dict) and "nodes" in jewel_data):
                print(json.dumps({
                    "success": False,
                    "error": "Existing JEWEL must be a JSON object with a 'nodes' key (exploration phantom_jewel style)"
                }))
                sys.exit(1)

            phantom_jewel_target = phantom_gem_path.with_name("phantom_jewel.json")
            shutil.copy2(src, phantom_jewel_target)
        except Exception as e:
            print(json.dumps({
                "success": False,
                "error": f"Failed to seed phantom_jewel.json from existing JEWEL: {e}"
            }))
            sys.exit(1)

    result = jewelstrike(str(phantom_gem_path), prefix)
    print(json.dumps(result, indent=2))

    if not result.get("success"):
        sys.exit(1)
