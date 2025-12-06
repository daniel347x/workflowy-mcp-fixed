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


def add_jewel_ids_recursive(node: dict, counter: list) -> None:
    """Add jewel_id to node and all descendants recursively."""
    if "jewel_id" not in node:
        node["jewel_id"] = f"J-{counter[0]:03d}"
        counter[0] += 1
    
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
    
    # Add jewel_ids to all nodes
    counter = [1]  # Mutable counter for recursion
    nodes = gem_data.get("nodes", [])
    for node in nodes:
        add_jewel_ids_recursive(node, counter)
    
    # Write working_gem (with jewel_ids)
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
