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
    parser.add_argument(
        '--mode',
        required=True,
        choices=['enchanted', 'direct', 'carto_refresh'],
        help=(
            'ENCHANTED (nexus_tag-based), DIRECT (json_file-based), or '
            'CARTO_REFRESH (Cartographer F12 job JSON-based)'
        ),
    )
    parser.add_argument('--nexus-tag', help='NEXUS tag (for enchanted mode)')
    parser.add_argument('--json-file', help='JSON file path (for direct mode)')
    parser.add_argument('--parent-id', help='Parent UUID (for direct mode, optional)')
    parser.add_argument('--dry-run', default='false', help='Dry run flag (true/false)')
    parser.add_argument('--import-policy', default='strict', help='Import policy (for direct mode)')
    parser.add_argument('--carto-job-file', help='CARTO_REFRESH job JSON file (for carto_refresh mode)')
    
    args = parser.parse_args()
    
    mode = args.mode
    dry_run = args.dry_run.lower() in ('true', '1', 'yes')
    
    log_worker(f"Starting WEAVE worker in {mode.upper()} mode, dry_run={dry_run}")
    
    # Determine paths
    script_dir = Path(__file__).parent.resolve()
    
    # Worker is in src/workflowy_mcp/, so add src/ to path for package imports
    src_dir = script_dir.parent
    sys.path.insert(0, str(src_dir))
    
    # Import the client
    try:
        from workflowy_mcp.client.api_client import WorkFlowyClient
        log_worker("Successfully imported WorkFlowyClient")
    except Exception as e:
        log_worker(f"Failed to import WorkFlowyClient: {e}")
        import traceback
        log_worker(traceback.format_exc())
        sys.exit(1)
    
    # Get nexus_runs base directory from environment (passed by launcher)
    # This avoids path calculation issues when worker is deployed vs source location
    nexus_runs_base = os.environ.get('NEXUS_RUNS_BASE')
    if mode in ('enchanted', 'direct') and not nexus_runs_base:
        log_worker("ERROR: NEXUS_RUNS_BASE not set in environment")
        log_worker("Launcher must pass the nexus_runs directory path via environment")
        sys.exit(1)
    
    # Initialize client (read config from environment or defaults)
    api_key = os.environ.get('WORKFLOWY_API_KEY')
    if not api_key:
        log_worker("ERROR: WORKFLOWY_API_KEY not set in environment")
        sys.exit(1)
    
    # Create client config
    from workflowy_mcp.models import APIConfiguration
    from pydantic import SecretStr
    
    config = APIConfiguration(
        api_key=SecretStr(api_key),
        timeout=900  # 15 minutes for individual API calls
    )
    
    client = WorkFlowyClient(config)

    # Ensure this worker has a fresh /nodes-export snapshot. Unlike the MCP
    # server (where /nodes-export refresh is manual via the UUID widget), the
    # detached WEAVE worker must be able to reconcile against ETHER on its
    # own. We therefore perform a one-time explicit refresh here.
    try:
        log_worker("Refreshing /nodes-export cache for WEAVE worker…")
        await client.refresh_nodes_export_cache()
        log_worker("/nodes-export cache refresh complete for WEAVE worker")
    except Exception as e:  # noqa: BLE001
        log_worker(f"ERROR: refresh_nodes_export_cache failed in WEAVE worker: {e}")
        import traceback
        log_worker(traceback.format_exc())
        sys.exit(1)
    
    # Determine PID file location and validate inputs based on mode
    pid_file = None
    
    if mode == 'enchanted':
        if not args.nexus_tag:
            log_worker("ERROR: --nexus-tag required for enchanted mode")
            sys.exit(1)
        
        nexus_tag = args.nexus_tag
        
        # Search for timestamped directory (same logic as api_client._get_nexus_dir)
        base_dir = Path(nexus_runs_base)
        candidates = []
        suffix = f"__{nexus_tag}"
        for child in base_dir.iterdir():
            if not child.is_dir():
                continue
            name = child.name
            if name == nexus_tag or name.endswith(suffix):
                candidates.append(child)
        
        if not candidates:
            log_worker(f"ERROR: NEXUS run directory not found for tag '{nexus_tag}'")
            log_worker(f"Searched in: {base_dir}")
            sys.exit(1)
        
        # Pick lexicographically last (latest timestamped directory)
        run_dir = sorted(candidates, key=lambda p: p.name)[-1]
        log_worker(f"Resolved nexus_tag '{nexus_tag}' to: {run_dir.name}")
        
        pid_file = run_dir / ".weave.pid"
        log_worker(f"ENCHANTED mode: nexus_tag={nexus_tag}")
        
    elif mode == 'direct':  # direct mode
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
    else:
        # CARTO_REFRESH mode does not use a .weave.pid file; PID is tracked via the CARTO job JSON.
        log_worker("CARTO_REFRESH mode: PID file will not be created (tracked via job JSON)")
    
    # Write PID file for WEAVE modes only
    if pid_file is not None:
        try:
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            log_worker(f"PID file written: {pid_file}")
        except Exception as e:
            log_worker(f"Failed to write PID file: {e}")
            # Continue anyway - not critical
    
    # @beacon[
    #   id=weave-worker@carto-refresh-job,
    #   role=weave worker – CARTO_REFRESH job runner,
    #   slice_labels=f9-f12-handlers,ra-reconcile,nexus-core-v1,
    #   kind=span,
    #   comment=Async CARTO_REFRESH job execution block in weave_worker main,
    # ]
    # CARTO_REFRESH mode: run Cartographer refresh via CARTO job JSON and exit
    if mode == 'carto_refresh':
        if not args.carto_job_file:
            log_worker("ERROR: --carto-job-file required for carto_refresh mode")
            await client.close()
            sys.exit(1)

        job_path = Path(args.carto_job_file)

        if not job_path.exists():
            log_worker(f"ERROR: CARTO job file not found: {job_path}")
            await client.close()
            sys.exit(1)

        # Load job JSON
        try:
            with open(job_path, 'r', encoding='utf-8') as jf:
                job = json.load(jf)
        except Exception as e:
            log_worker(f"ERROR: Failed to read CARTO job file: {e}")
            await client.close()
            sys.exit(1)

        # If job was cancelled before the worker started, exit early without work
        if job.get('status') == 'cancelled':
            log_worker(
                f"CARTO_REFRESH job {job.get('id') or job_path.stem} is already "
                "marked cancelled; exiting without running refresh."
            )
            await client.close()
            sys.exit(0)

        # Update job status to running
        now_iso = datetime.utcnow().isoformat()
        job['status'] = 'running'
        job['updated_at'] = now_iso
        job['pid'] = os.getpid()

        progress = job.get('progress') or {}
        job['progress'] = progress
        progress.setdefault('current_phase', 'refresh')

        result_summary = job.get('result_summary') or {}
        job['result_summary'] = result_summary
        result_summary.setdefault('errors', [])

        try:
            with open(job_path, 'w', encoding='utf-8') as jf:
                json.dump(job, jf, indent=2)
        except Exception as e:
            log_worker(f"ERROR: Failed to write CARTO job file: {e}")

        root_uuid = job.get('root_uuid')
        carto_mode = job.get('mode')

        exit_code = 0

        try:
            if not root_uuid or carto_mode not in ('file', 'folder'):
                raise ValueError("Invalid CARTO_REFRESH job payload (root_uuid/mode)")

            if carto_mode == 'file':
                log_worker(f"CARTO_REFRESH: refreshing FILE node {root_uuid}")
                result = await client.refresh_file_node_beacons(
                    file_node_id=root_uuid,
                    dry_run=False,
                )
                files_refreshed = 1
            else:
                log_worker(f"CARTO_REFRESH: refreshing FOLDER subtree {root_uuid}")

                def _carto_should_cancel() -> bool:
                    """Return True if the CARTO_REFRESH job JSON is marked cancelled.

                    This is a best-effort check invoked between per-file refreshes
                    inside refresh_folder_cartographer_sync. Errors while reading the
                    job JSON are logged but do not stop the job.
                    """
                    try:
                        with open(job_path, "r", encoding="utf-8") as jf:
                            fresh = json.load(jf)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to re-read job JSON for cancel check: {e}",
                            "CARTO",
                        )
                        return False

                    return str(fresh.get("status")) == "cancelled"

                result = await client.refresh_folder_cartographer_sync(
                    folder_node_id=root_uuid,
                    cancel_callback=_carto_should_cancel,
                )
                if isinstance(result, dict):
                    files_refreshed = result.get('refreshed_file_nodes', 0)
                else:
                    files_refreshed = 0

            job['status'] = 'completed'
            job['progress']['completed_files'] = job['progress'].get('total_files', files_refreshed or 1)
            job['result_summary']['files_refreshed'] = files_refreshed
            job['result_summary']['raw_result'] = result
        except Exception as e:
            exit_code = 1
            log_worker(f"CARTO_REFRESH failed with error: {e}")
            import traceback
            log_worker(traceback.format_exc())
            job['status'] = 'failed'
            job['error'] = str(e)
            job['result_summary'].setdefault('errors', []).append(str(e))
        finally:
            job['updated_at'] = datetime.utcnow().isoformat()
            try:
                with open(job_path, 'w', encoding='utf-8') as jf:
                    json.dump(job, jf, indent=2)
            except Exception as e:
                log_worker(f"ERROR: Failed to write CARTO job file: {e}")
            await client.close()
            sys.exit(exit_code)
    # @beacon-close[
    #   id=weave-worker@carto-refresh-job,
    # ]

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
