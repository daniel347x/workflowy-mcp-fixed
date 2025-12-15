#!/usr/bin/env python3
"""
JEWELMORPH - Phantom gem transmuted to phantom jewel

Part of the JEWELSTORM workflow (parallel to QUILLSTORM, but for PHANTOM GEMs).

Purpose:
- Validates JEWELSTRIKE session via guardian token and witness/guardian copies
- Ensures the working_gem has actually been inscribed (changed vs witness_gem)
- Strips jewel_id from all nodes in the working_gem
- Writes phantom_jewel.json alongside phantom_gem.json (atomic write)
- Cleans up working_gem + guardian copy and clears guardian index entry

Usage:
    python jewelmorph.py "<identifier>" "<guardian-token>"

identifier can be:
    - Raw hash (e.g., "6a3f")
    - Prefixed hash (e.g., "jm-6a3f")
    - Full prefix (e.g., "jm-6a3f-my-refactor", right-trimmed dashes)
    - Full working_gem filename (with or without path)
    - Phantom gem filename (looks up in index to find associated session)

Returns JSON to stdout:
    {
      "success": true,
      "hash": "a3f7",
      "phantom_gem": ".../phantom_gem.json",
      "phantom_jewel": ".../phantom_jewel.json",
      "working_gem_deleted": true
    }
"""

import json
import os
import sys
import shutil
from pathlib import Path
from typing import Any, Dict

INDEX_FILENAME = "jewelstorm_index.json"


def _load_index(index_path: Path) -> Dict[str, Any]:
    try:
        if index_path.exists():
            with open(index_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception:
        return {}


def _build_pj_preview_lines(nodes: list, max_note_chars: int = 1024) -> list[str]:
    """Build human-readable preview of phantom_jewel tree (PJ-1.2.3 format).

    Format:
        [PJ-1.2.3] ⦿ Node Name [note-preview]
    with 4-space indentation per depth level and truncated note previews.

    This uses path-based preview_id (not jewel_id) because phantom_jewel.json
    is a COLD artifact after JEWELMORPH (jewel_ids are stripped).
    """
    # PASS 1: Assign preview_id to all nodes and collect max width
    all_preview_ids: list[str] = []

    def assign_ids(node: dict, path_parts: list[str]) -> None:
        if not isinstance(node, dict):
            return
        path_str = ".".join(path_parts)
        preview_id = f"PJ-{path_str}"
        node["preview_id"] = preview_id
        all_preview_ids.append(preview_id)
        children = node.get("children") or []
        for idx, child in enumerate(children, start=1):
            if isinstance(child, dict):
                assign_ids(child, path_parts + [str(idx)])

    for root_index, root in enumerate(nodes, start=1):
        if isinstance(root, dict):
            assign_ids(root, [str(root_index)])

    max_id_width = max((len(pid) for pid in all_preview_ids), default=0)

    # PASS 2: Build aligned preview lines
    lines: list[str] = []

    def walk(node: dict, path_parts: list[str]) -> None:
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
            if len(flat) > max_note_chars:
                flat = flat[:max_note_chars]
            name_part = f"{name} [{flat}]"
        else:
            name_part = name
        lines.append(f"[{padded_id}] {indent}{bullet} {name_part}")

        # Recurse into children with extended path
        for idx, child in enumerate(children, start=1):
            if isinstance(child, dict):
                walk(child, path_parts + [str(idx)])

    for root_index, root in enumerate(nodes, start=1):
        if isinstance(root, dict):
            walk(root, [str(root_index)])

    return lines


def _strip_jewel_ids(obj: Any) -> None:
    """Recursively remove jewel_id from all nodes in-place."""
    if isinstance(obj, dict):
        if "jewel_id" in obj:
            obj.pop("jewel_id", None)
        # Handle both "children" (nodes) and "nodes" (export wrapper)
        children = obj.get("children") or obj.get("nodes") or []
        if isinstance(children, list):
            for child in children:
                _strip_jewel_ids(child)
    elif isinstance(obj, list):
        for item in obj:
            _strip_jewel_ids(item)


def _build_id_index(obj: Any, index: Dict[str, Dict[str, Any]]) -> None:
    """Build a mapping from Workflowy id → node for fast metadata lookup."""
    if isinstance(obj, dict):
        nid = obj.get("id")
        if isinstance(nid, str):
            index[nid] = obj
        children = obj.get("children") or obj.get("nodes") or []
        if isinstance(children, list):
            for child in children:
                _build_id_index(child, index)
    elif isinstance(obj, list):
        for item in obj:
            _build_id_index(item, index)


def _restore_per_node_metadata_from_phantom(gem_data: Dict[str, Any], phantom_data: Dict[str, Any]) -> None:
    """Reattach per-node metadata (completed/data/timestamps) from phantom_gem.

    The HOT VISIBLE GEM strips these fields for readability and token
    efficiency, but phantom_gem.json keeps the authoritative values. This
    function walks gem_data's nodes and, when an id matches a node in
    phantom_data, copies back the following keys if they are missing:

    - completed
    - data
    - createdAt
    - modifiedAt
    - completedAt
    """
    id_index: Dict[str, Dict[str, Any]] = {}
    _build_id_index(phantom_data, id_index)

    def _walk(node: Dict[str, Any]) -> None:
        nid = node.get("id")
        if isinstance(nid, str) and nid in id_index:
            src = id_index[nid]
            for key in ("completed", "data", "createdAt", "modifiedAt", "completedAt"):
                if key in src and key not in node:
                    node[key] = src[key]
        for child in node.get("children") or []:
            if isinstance(child, dict):
                _walk(child)

    for root in gem_data.get("nodes") or []:
        if isinstance(root, dict):
            _walk(root)


def jewelmorph(identifier: str, guardian_token: str) -> dict:
    """Finalize a JEWELSTORM session, producing phantom_jewel.json.

    Safety invariants (mirrors QUILLMORPH semantics):
    1. Resolve identifier → session (hash, phantom_gem_path, working_gem, witness_gem).
    2. Validate guardian_token against stored token (human-in-the-loop authorization).
    3. Validate witness_gem vs guardian_gem (no tampering with witness).
    4. Ensure working_gem differs from witness_gem (non-empty inscription).
    5. Strip jewel_id and atomically write phantom_jewel.json alongside phantom_gem.json.
    6. Remove working_gem + guardian copy and clear index entry.
    """

    # Temp + guardian directories (parallel to jewelstrike.py)
    temp_dir = Path(r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp")
    guardian_dir = temp_dir / ".jewel-guardian"
    index_path = guardian_dir / INDEX_FILENAME

    index = _load_index(index_path)

    hash_value = None
    phantom_gem_path: Path | None = None
    entry: Dict[str, Any] | None = None

    # Normalize identifier (strip jm- prefix, right-trim dashes)
    clean_id = identifier.strip().rstrip("-")
    if clean_id.startswith("jm-"):
        clean_id = clean_id[3:]

    # PHASE 1: Try to interpret identifier as a path
    potential_path = Path(identifier)
    if potential_path.exists():
        # Working gem in temp dir with jm- prefix
        if potential_path.parent == temp_dir and potential_path.name.startswith("jm-"):
            # Extract hash from filename
            filename = potential_path.name
            if "--" in filename:
                prefix_part = filename.split("--")[0]
                parts = prefix_part.split("-")
                if len(parts) >= 2:
                    hash_value = parts[1]
            else:
                parts = filename.split("-")
                if len(parts) >= 2:
                    hash_value = parts[1]
            # Find matching index entry by working_gem
            if hash_value:
                for key, val in index.items():
                    if val.get("hash") == hash_value and val.get("working_gem") == str(potential_path):
                        phantom_gem_path = Path(key)
                        entry = val
                        break
        else:
            # Treat as phantom_gem path – look up directly in index
            gem_key = str(potential_path.resolve())
            if gem_key in index:
                phantom_gem_path = potential_path.resolve()
                entry = index[gem_key]
                hash_value = entry.get("hash")

    # PHASE 2: If still unresolved, treat identifier as hash/prefix
    if not entry:
        if not hash_value and len(clean_id) >= 4:
            if all(c in "0123456789abcdef" for c in clean_id[:4].lower()):
                hash_value = clean_id[:4].lower()
        if hash_value:
            for key, val in index.items():
                if val.get("hash") == hash_value:
                    phantom_gem_path = Path(key)
                    entry = val
                    break

    if not entry or not phantom_gem_path or not hash_value:
        return {
            "success": False,
            "error": f"Could not resolve identifier to active JEWELSTORM session: {identifier}",
        }

    # Extract paths from entry
    working_gem_str = entry.get("working_gem")
    witness_gem_str = entry.get("witness_gem")
    human_prefix = entry.get("human_prefix")

    if not working_gem_str or not witness_gem_str:
        return {
            "success": False,
            "hash": hash_value,
            "error": "Guardian index entry is incomplete (missing working_gem or witness_gem)",
        }

    working_gem_path = Path(working_gem_str)
    witness_gem_path = Path(witness_gem_str)

    # Validate working_gem exists
    if not working_gem_path.exists():
        return {
            "success": False,
            "hash": hash_value,
            "error": f"Working gem does not exist: {working_gem_path}",
        }

    # Validate guardian token (human-in-the-loop gate)
    stored_token = entry.get("guardian_token")
    if stored_token:
        if not guardian_token or guardian_token.strip() != stored_token:
            return {
                "success": False,
                "hash": hash_value,
                "error": (
                    "✅ PRE-MORPH STAGE COMPLETE\n\n"
                    "JEWELSTRIKE session is active, but Guardian Token is required to transmute.\n\n"
                    "**MANDATORY NEXT STEP (HUMAN IN THE LOOP):**\n"
                    "1. Dan reviews the JEWELSTORM changes (e.g., via PARALLAX / Araxis).\n"
                    "2. Dan provides the Guardian Token from the JEWEL guardian index.\n"
                    "3. Re-run jewelmorph with the same identifier and the correct token.\n"
                ),
            }

    # Reconstruct guardian_gem path
    if human_prefix:
        guardian_filename = f"{hash_value}-{human_prefix}.witness"
    else:
        guardian_filename = f"{hash_value}-gem.witness"
    guardian_gem_path = guardian_dir / guardian_filename

    # Validate witness and guardian copies
    if not witness_gem_path.exists():
        return {
            "success": False,
            "hash": hash_value,
            "error": f"Witness gem missing: {witness_gem_path}",
        }
    if not guardian_gem_path.exists():
        return {
            "success": False,
            "hash": hash_value,
            "error": f"Guardian gem missing (JEWELSTRIKE may have failed): {guardian_gem_path}",
        }

    import filecmp

    # Witness vs guardian must be identical (no tampering with witness_gem)
    if not filecmp.cmp(str(witness_gem_path), str(guardian_gem_path), shallow=False):
        return {
            "success": False,
            "hash": hash_value,
            "error": (
                "💥 THE GEM FRACTURES! Witness Gem has been altered.\n\n"
                "Alert: The witness_gem was modified instead of the working_gem.\n"
                "JEWELMORPH is ABORTED to prevent corrupting PHANTOM JEWEL.\n\n"
                f"Witness: {witness_gem_path}\n"
                f"Guardian: {guardian_gem_path}\n"
            ),
        }

    # Ensure working_gem is actually different from witness_gem (non-empty JEWELSTORM)
    if filecmp.cmp(str(working_gem_path), str(witness_gem_path), shallow=False):
        # Best-effort cleanup of guardian + working_gem; keep witness_gem as baseline
        try:
            if guardian_gem_path.exists():
                guardian_gem_path.unlink()
            if working_gem_path.exists():
                working_gem_path.unlink()
        except Exception:
            pass

        return {
            "success": False,
            "hash": hash_value,
            "error": (
                "⚠️ THE GEM REMAINS UNTOUCHED. No JEWELSTORM inscription detected.\n\n"
                "working_gem is identical to witness_gem – no semantic changes were applied.\n"
                "Either no JEWELSTORM operations ran, or a different file was edited.\n\n"
                "💡 ACTION OPTIONS:\n"
                "  1. Apply transform_jewel operations to working_gem and try again, OR\n"
                "  2. Use jeweldrop.py to collapse this JEWELSTORM session if it is no longer needed.\n"
            ),
        }

    # PHASE 4: Strip jewel_id and write phantom_jewel.json atomically
    try:
        with open(working_gem_path, "r", encoding="utf-8") as f:
            gem_data = json.load(f)
    except Exception as e:
        return {
            "success": False,
            "hash": hash_value,
            "error": f"Failed to read working_gem JSON: {e}",
        }

    # Optionally restore root-level metadata from phantom_gem
    # (working_gem may omit large fields such as original_ids_seen for editability)
    phantom_data = None
    try:
        with open(phantom_gem_path, "r", encoding="utf-8") as f:
            phantom_data = json.load(f)
    except Exception:
        phantom_data = None

    if isinstance(phantom_data, dict):
        for key in (
            "original_ids_seen",
            "explicitly_preserved_ids",
            "export_root_id",
            "export_root_name",
            "export_root_children_status",
            "jewel_file",
        ):
            # Treat `None` the same as missing so JEWELMORPH can always
            # restore authoritative NEXUS metadata from phantom_gem.
            if key in phantom_data and (key not in gem_data or gem_data.get(key) is None):
                gem_data[key] = phantom_data[key]

    # Remove jewel_id fields everywhere
    _strip_jewel_ids(gem_data)

    # Build PJ-prefixed preview_tree (ephemeral, for agents/humans; never read by algorithms)
    nodes = gem_data.get("nodes") or []
    preview_tree = []
    try:
        preview_tree = _build_pj_preview_lines(nodes)
    except Exception:
        pass
    
    # Rebuild gem_data dict with preview_tree in early position (if dict with nodes)
    if isinstance(gem_data, dict) and isinstance(gem_data.get("nodes"), list):
        gem_data = {
            "export_root_id": gem_data.get("export_root_id"),
            "export_root_name": gem_data.get("export_root_name"),
            "export_timestamp": gem_data.get("export_timestamp"),
            "export_root_children_status": gem_data.get("export_root_children_status"),
            "__preview_tree__": preview_tree,
            "nodes": gem_data.get("nodes"),
            "original_ids_seen": gem_data.get("original_ids_seen"),
            "explicitly_preserved_ids": gem_data.get("explicitly_preserved_ids"),
        }

    # Determine phantom_jewel path (sibling to phantom_gem, fixed name)
    phantom_jewel_path = phantom_gem_path.with_name("phantom_jewel.json")
    tmp_path = phantom_jewel_path.with_suffix(".json.tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(gem_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, phantom_jewel_path)
    except Exception as e:
        return {
            "success": False,
            "hash": hash_value,
            "error": f"Failed to write phantom_jewel.json: {e}",
        }

    # Clean up working_gem, guardian_gem, and operations_reference.json
    try:
        if working_gem_path.exists():
            working_gem_path.unlink()
        if guardian_gem_path.exists():
            guardian_gem_path.unlink()
        
        # Delete operations_reference.json (auto-generated by JEWELSTRIKE)
        # Pattern: jm-[hash]-[prefix]--operations_reference.json
        ops_ref_pattern = working_gem_path.stem.replace("phantom_gem", "operations_reference")
        ops_ref_file = working_gem_path.parent / f"{ops_ref_pattern}.json"
        if ops_ref_file.exists():
            ops_ref_file.unlink()
    except Exception:
        # Cleanup is best-effort only
        pass

    working_gem_deleted = not working_gem_path.exists()

    # Remove index entry for this phantom_gem
    try:
        gem_key = str(phantom_gem_path.resolve())
        if gem_key in index and index[gem_key].get("hash") == hash_value:
            del index[gem_key]
            guardian_dir.mkdir(exist_ok=True)
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(index, f, indent=2, ensure_ascii=False)
    except Exception:
        # Index cleanup is best-effort only
        pass

    return {
        "success": True,
        "preview_tree": preview_tree,
        "hash": hash_value,
        "phantom_gem": str(phantom_gem_path.resolve()),
        "phantom_jewel": str(phantom_jewel_path.resolve()),
        "working_gem_deleted": working_gem_deleted,
    }


def main() -> None:
    """CLI entrypoint for jewelmorph."""
    args = sys.argv[1:]

    if len(args) != 2:
        print(json.dumps({
            "success": False,
            "error": "Usage: python jewelmorph.py \"<identifier>\" \"<guardian-token>\"",
        }))
        sys.exit(1)

    identifier = args[0]
    token = args[1]

    result = jewelmorph(identifier, token)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
