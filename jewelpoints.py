#!/usr/bin/env python3
"""
JEWELPOINTS - List all active JEWELSTORM sessions

Purpose:
- Display all active Jewel sessions from the JEWEL guardian index
- Show verbose details for each session (hash, prefix, phantom_gem, working_gem, witness_gem)
- Help track which JEWELSTORM sessions are active and what NEXUS tags they represent
- Guardian tokens are NEVER displayed (remain secret)

Usage:
    python jewelpoints.py

Returns JSON to stdout:
    {
        "sessions": [
            {
                "hash": "f075",
                "human_prefix": "my-edit",
                "full_prefix": "jm-f075-my-edit",
                "phantom_gem": "E:\\...\\temp\\nexus_runs\\my-tag\\phantom_gem.json",
                "working_gem": "E:\\...\\temp\\jm-f075-my-edit--phantom_gem.json",
                "witness_gem": "E:\\...\\temp\\jm-f075-my-edit--phantom_gem.json.witness.json"
            }
        ],
        "total": 1
    }
"""

import json
import sys
from pathlib import Path

INDEX_FILENAME = "jewelstorm_index.json"


def jewelpoints() -> dict:
    """List all active JEWELSTORM sessions with verbose details.
    
    Returns:
        Dictionary with list of active sessions and their details.
    """
    # Centralized temp + guardian locations
    temp_dir = Path(r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp")
    guardian_dir = temp_dir / ".jewel-guardian"
    index_path = guardian_dir / INDEX_FILENAME
    
    # Load index
    try:
        if index_path.exists():
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        else:
            index = {}
    except Exception:
        index = {}
    
    sessions = []
    
    for phantom_gem_key, entry in index.items():
        hash_value = entry.get("hash")
        working_path = entry.get("working_gem")
        witness_path = entry.get("witness_gem")
        
        # Extract human prefix from working gem filename
        human_prefix = None
        full_prefix = None
        
        if working_path:
            working_filename = Path(working_path).name
            
            # Parse: jm-[hash]-[prefix]--phantom_gem.json OR jm-[hash]--phantom_gem.json
            if "--" in working_filename:
                prefix_part = working_filename.split("--")[0]
                parts = prefix_part.split("-")
                if len(parts) >= 3:
                    # parts[0] = "jm", parts[1] = hash, parts[2:] = human prefix
                    human_prefix = "-".join(parts[2:])
                    full_prefix = f"jm-{hash_value}-{human_prefix}"
            
            if not full_prefix:
                full_prefix = f"jm-{hash_value}"
        
        session_info = {
            "hash": hash_value,
            "human_prefix": human_prefix,
            "full_prefix": full_prefix,
            "phantom_gem": phantom_gem_key,
            "working_gem": working_path,
            "witness_gem": witness_path
        }
        
        sessions.append(session_info)
    
    return {
        "success": True,
        "sessions": sessions,
        "total": len(sessions)
    }


def main() -> None:
    """CLI entrypoint for jewelpoints."""
    
    # No arguments required
    if len(sys.argv) > 1:
        print(json.dumps({
            "success": False,
            "error": "Usage: python jewelpoints.py (no arguments required)"
        }))
        sys.exit(1)
    
    result = jewelpoints()
    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
