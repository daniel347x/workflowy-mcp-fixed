#!/usr/bin/env python3
"""
NEXUS WEAVE Worker - Detached background process for WEAVE operations.

This runs independently of the MCP server and survives MCP restarts.
Progress is tracked via .weave_journal.json and .weave.pid files.

Usage:
    python weave_worker.py <nexus_tag> <dry_run>

Files created/updated:
    nexus_runs/<tag>/.weave.pid                           # Worker PID
    nexus_runs/<tag>/enchanted_terrain.weave_journal.json # Progress log
"""

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
    if len(sys.argv) < 3:
        print(json.dumps({
            "success": False,
            "error": "Usage: python weave_worker.py <nexus_tag> <dry_run>"
        }))
        sys.exit(1)
    
    nexus_tag = sys.argv[1]
    dry_run = sys.argv[2].lower() in ('true', '1', 'yes')
    
    log_worker(f"Starting WEAVE worker for nexus_tag={nexus_tag}, dry_run={dry_run}")
    
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
    session_id = os.environ.get('WORKFLOWY_SESSION_ID')
    if not session_id:
        log_worker("ERROR: WORKFLOWY_SESSION_ID not set in environment")
        sys.exit(1)
    
    # Create client config
    from models import APIConfiguration
    config = APIConfiguration(
        session_id=session_id,
        timeout=900.0  # 15 minutes for individual API calls
    )
    
    client = WorkFlowyClient(config)
    
    # Determine nexus run directory
    nexus_runs_dir = project_root / "temp" / "nexus_runs"
    run_dir = nexus_runs_dir / nexus_tag
    
    if not run_dir.exists():
        log_worker(f"ERROR: NEXUS run directory not found: {run_dir}")
        sys.exit(1)
    
    # Write PID file
    pid_file = run_dir / ".weave.pid"
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        log_worker(f"PID file written: {pid_file}")
    except Exception as e:
        log_worker(f"Failed to write PID file: {e}")
        # Continue anyway - not critical
    
    # Call the actual weave method
    try:
        log_worker("Calling client.nexus_weave_enchanted...")
        result = await client.nexus_weave_enchanted(nexus_tag=nexus_tag, dry_run=dry_run)
        
        log_worker(f"WEAVE completed successfully: {result.get('nodes_created', 0)} created, "
                  f"{result.get('nodes_updated', 0)} updated, {result.get('nodes_deleted', 0)} deleted")
        
        # Clean up PID file on success
        try:
            if pid_file.exists():
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
