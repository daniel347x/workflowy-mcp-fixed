#!/usr/bin/env python3
"""
JEWELDROP - Collapse an active JEWELSTORM session (Protected)

Purpose:
- Safely remove working_gem, witness_gem, and guardian_gem for a JEWELSTORM session.
- Remove the corresponding entry from .jewel-guardian/jewelstorm_index.json.
- REQUIRES GUARDIAN TOKEN to authorize the drop (prevents accidental/unauthorized clearing).

Usage:
    python jeweldrop.py <identifier> <guardian_token>

identifier can be:
    - Raw hash (e.g., "6a3f")
    - Prefixed hash (e.g., "jm-6a3f")
    - Full prefix (e.g., "jm-6a3f-my-refactor", right-trimmed dashes)
    - Full working_gem filename (with or without path)
    - Phantom gem filename (looks up in index to find associated session)

Returns JSON to stdout:
    {
      "success": true,
      "dropped": true,
      "hash": "a3f7",
      "phantom_gem": ".../phantom_gem.json",
      "working_gem": "...",
      "witness_gem": "..."
    }
"""

import json
import sys
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


def jeweldrop(identifier: str, guardian_token: str) -> dict:
    """Drop a JEWELSTORM session by identifier, protected by guardian token.

    Args:
        identifier: See module docstring for flexible formats.
        guardian_token: Secret token to authorize the drop.

    Returns:
        Dictionary summarizing result and session details.
    """

    # Centralized temp + guardian locations (parallel to jewelstrike/jewelmorph)
    temp_dir = Path(r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp")
    guardian_dir = temp_dir / ".jewel-guardian"
    index_path = guardian_dir / INDEX_FILENAME

    index = _load_index(index_path)

    hash_value = None
    gem_key = None
    entry: Dict[str, Any] | None = None

    # Normalize identifier (strip jm- prefix, right-trim dashes)
    clean_id = identifier.strip().rstrip("-")
    if clean_id.startswith("jm-"):
        clean_id = clean_id[3:]

    # PHASE 1: Try to interpret identifier as a path
    potential_path = Path(identifier)
    if potential_path.exists():
        # Working_gem in temp dir with jm- prefix
        if potential_path.parent == temp_dir and potential_path.name.startswith("jm-"):
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
        else:
            # Treat as phantom_gem path – look up directly in index
            gem_candidate = str(potential_path.resolve())
            if gem_candidate in index:
                gem_key = gem_candidate
                entry = index[gem_key]
                hash_value = entry.get("hash")

    # PHASE 2: Try to interpret identifier as hash/prefix
    if not hash_value and len(clean_id) >= 4:
        if all(c in "0123456789abcdef" for c in clean_id[:4].lower()):
            hash_value = clean_id[:4].lower()

    # Look up session by hash in index if needed
    if hash_value and not entry:
        for key, val in index.items():
            if val.get("hash") == hash_value:
                gem_key = key
                entry = val
                break

    if not gem_key or not entry or not hash_value:
        return {
            "success": False,
            "error": f"Could not resolve identifier to active JEWELSTORM session: {identifier}",
        }

    # Validate Guardian Token
    stored_token = entry.get("guardian_token")
    if stored_token:
        if not guardian_token or guardian_token.strip() != stored_token:
            return {
                "success": False,
                "hash": hash_value,
                "error": (
                    "🛑 JEWEL GUARDIAN LOCK ENGAGED. You cannot drop this session.\n\n"
                    "The JEWEL field cannot be collapsed without the Guardian Token.\n\n"
                    "1. Ask Dan for the token (from the JEWEL guardian index).\n"
                    "2. Run: python jeweldrop.py <identifier> <token>.\n"
                ),
            }

    # Proceed with cleanup
    working_gem = entry.get("working_gem")
    witness_gem = entry.get("witness_gem")
    human_prefix = entry.get("human_prefix")

    # Guardian gem path: .jewel-guardian/<hash>[-prefix| -gem].witness
    guardian_gem = None
    try:
        if human_prefix:
            candidate = guardian_dir / f"{hash_value}-{human_prefix}.witness"
            guardian_gem = candidate if candidate.exists() else None
        else:
            candidate = guardian_dir / f"{hash_value}-gem.witness"
            guardian_gem = candidate if candidate.exists() else None
        # As a fallback, look for any *.witness starting with hash
        if guardian_gem is None:
            candidates = list(guardian_dir.glob(f"{hash_value}*.witness"))
            if candidates:
                guardian_gem = candidates[0]
    except Exception:
        guardian_gem = None

    # Best-effort file deletion
    try:
        if working_gem:
            wp = Path(working_gem)
            if wp.exists():
                wp.unlink()
        if witness_gem:
            wp = Path(witness_gem)
            if wp.exists():
                wp.unlink()
        if guardian_gem and guardian_gem.exists():
            guardian_gem.unlink()
    except Exception:
        # Cleanup is best-effort; we still proceed with index cleanup
        pass

    # Remove index entry
    try:
        if gem_key in index:
            del index[gem_key]
        guardian_dir.mkdir(exist_ok=True)
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
    except Exception:
        # Index cleanup is best-effort
        pass

    # Extract human/full prefix from working_gem filename (for diagnostics)
    full_prefix = None
    if working_gem:
        working_name = Path(working_gem).name
        if "--" in working_name:
            prefix_part = working_name.split("--")[0]
            parts = prefix_part.split("-")
            if len(parts) >= 3:
                # parts[0] = "jm", parts[1] = hash, parts[2:] = human prefix
                human_prefix = "-".join(parts[2:])
                full_prefix = f"jm-{hash_value}-{human_prefix}"
        else:
            full_prefix = f"jm-{hash_value}"

    return {
        "success": True,
        "dropped": True,
        "hash": hash_value,
        "phantom_gem": gem_key,
        "working_gem": working_gem,
        "witness_gem": witness_gem,
        "human_prefix": human_prefix,
        "full_prefix": full_prefix,
    }


def main() -> None:
    """CLI entrypoint for jeweldrop."""
    if len(sys.argv) != 3:
        print(json.dumps({
            "success": False,
            "error": "Usage: python jeweldrop.py <identifier> <guardian_token>",
        }))
        sys.exit(1)

    identifier = sys.argv[1]
    token = sys.argv[2]

    result = jeweldrop(identifier, token)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
