#!/usr/bin/env python3
"""
NEXUS WEAVE Worker - Detached background process for WEAVE operations.

This runs independently of the MCP server and survives MCP restarts.
Progress is tracked via .weave_journal.json and .weave.pid files.

Supports two modes:
  1. ENCHANTED mode (PHANTOM GEMSTONE NEXUS): Uses nexus_tag to find enchanted_terrain.json
  2. DIRECT mode (SCRY & WEAVE): Uses explicit json_file path

Usage:
    # ENCHANTED mode
    python weave_worker.py --mode enchanted --nexus-tag <tag> --dry-run <true|false>
    
    # DIRECT mode  
    python weave_worker.py --mode direct --json-file <path> [--parent-id <uuid>] [--import-policy strict] --dry-run <true|false>

Files created/updated:
    .weave.pid                 # Worker PID (location depends on mode)
    <file>.weave_journal.json  # Progress log (created by bulk_import_from_file)
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def log_worker(message: str, component: str = "WEAVE_WORKER") -> None:
    """Log to stderr with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] 🗡️ [{component}] {message}", file=sys.stderr, flush=True)


async def main():
    """Main worker entry point."""
    
    parser = argparse.ArgumentParser(description='NEXUS WEAVE detached worker')
    parser.add_argument('--mode', required=True, choices=['enchanted', 'direct'],
                       help='ENCHANTED (nexus_tag-based) or DIRECT (json_file-based)')
    parser.add_argument('--nexus-tag', help='NEXUS tag (for enchanted mode)')
    parser.add_argument('--json-file', help='JSON file path (for direct mode)')
    parser.add_argument('--parent-id', help='Parent UUID (for direct mode, optional)')
    parser.add_argument('--dry-run', default='false', help='Dry run flag (true/false)')
    parser.add_argument('--import-policy', default='strict', help='Import policy (for direct mode)')
    
    args = parser.parse_args()
    
    mode = args.mode
    dry_run = args.dry_run.lower() in ('true', '1', 'yes')
    
    log_worker(f"Starting WEAVE worker in {mode.upper()} mode, dry_run={dry_run}")
    
    # Determine paths
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent.parent.parent  # Up to TODO root
    
    # Add project root to path so we can import from MCP server
    sys.path.insert(0, str(project_root))
    sys.path.insert(0, str(script_dir.parent))  # workflowy_mcp package
    
    # Import the client
    try:
        from client.api_client import WorkFlowyClient
        log_worker("Successfully imported WorkFlowyClient")
    except Exception as e:
        log_worker(f"Failed to import WorkFlowyClient: {e}")
        sys.exit(1)
    
    # Initialize client (read config from environment or defaults)
    api_key = os.environ.get('WORKFLOWY_API_KEY')
    if not api_key:
        log_worker("ERROR: WORKFLOWY_API_KEY not set in environment")
        sys.exit(1)
    
    # Create client config
    from models import APIConfiguration
    from pydantic import SecretStr
    
    config = APIConfiguration(
        api_key=SecretStr(api_key),
        timeout=900  # 15 minutes for individual API calls
    )
    
    client = WorkFlowyClient(config)
    
    # Determine PID file location and validate inputs based on mode
    pid_file = None
    
    if mode == 'enchanted':
        if not args.nexus_tag:
            log_worker("ERROR: --nexus-tag required for enchanted mode")
            sys.exit(1)
        
        nexus_tag = args.nexus_tag
        run_dir = project_root / "temp" / "nexus_runs" / nexus_tag
        
        if not run_dir.exists():
            log_worker(f"ERROR: NEXUS run directory not found: {run_dir}")
            sys.exit(1)
        
        pid_file = run_dir / ".weave.pid"
        log_worker(f"ENCHANTED mode: nexus_tag={nexus_tag}")
        
    else:  # direct mode
        if not args.json_file:
            log_worker("ERROR: --json-file required for direct mode")
            sys.exit(1)
        
        json_file = args.json_file
        json_path = Path(json_file)
        
        if not json_path.exists():
            log_worker(f"ERROR: JSON file not found: {json_file}")
            sys.exit(1)
        
        # PID file goes in same directory as JSON file
        pid_file = json_path.parent / ".weave.pid"
        log_worker(f"DIRECT mode: json_file={json_file}, parent_id={args.parent_id}")
    
    # Write PID file
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        log_worker(f"PID file written: {pid_file}")
    except Exception as e:
        log_worker(f"Failed to write PID file: {e}")
        # Continue anyway - not critical
    
    # Call the appropriate weave method
    try:
        if mode == 'enchanted':
            log_worker(f"Calling nexus_weave_enchanted for tag={nexus_tag}...")
            result = await client.nexus_weave_enchanted(nexus_tag=nexus_tag, dry_run=dry_run)
        else:  # direct mode
            log_worker(f"Calling bulk_import_from_file for {json_file}...")
            result = await client.bulk_import_from_file(
                json_file=json_file,
                parent_id=args.parent_id,
                dry_run=dry_run,
                import_policy=args.import_policy
            )
        
        log_worker(f"WEAVE completed successfully in {mode.upper()} mode: "
                  f"{result.get('nodes_created', 0)} created, "
                  f"{result.get('nodes_updated', 0)} updated, "
                  f"{result.get('nodes_deleted', 0)} deleted, "
                  f"{result.get('nodes_moved', 0)} moved")
        
        # Clean up PID file on success
        try:
            if pid_file and pid_file.exists():
                pid_file.unlink()
            log_worker("PID file cleaned up")
        except Exception:
            pass
        
        sys.exit(0)
        
    except Exception as e:
        log_worker(f"WEAVE failed with error: {e}")
        import traceback
        log_worker(traceback.format_exc())
        
        # Leave PID file in place so status check can see the failure
        # Journal will have the error details
        
        sys.exit(1)
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_worker("Worker interrupted by user")
        sys.exit(130)
