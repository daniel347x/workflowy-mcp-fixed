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


# WORKER BOUNDARY PLAN:
# This file currently serves two worlds: advanced detached WEAVE execution for
# the gemstone pipeline, and detached CARTO_REFRESH execution for the installable
# Cartographer/F12 experience. Preserve both behaviors, but split them cleanly.
# @beacon[
#   id=wkv1@file-boundary,
#   role=boundary split: weave_worker mixed detached carto-refresh core vs advanced weave lab,
#   slice_labels=nexus-portability,nexus-split-boundary,
#   kind=span,
#   show_span=false,
# ]
# @beacon[
#   id=worker@log_worker,
#   role=log_worker,
#   slice_labels=ra-logging,
#   kind=ast,
# ]
def log_worker(message: str, component: str = "WEAVE_WORKER") -> None:
    """Log to stderr with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] 🗡️ [{component}] {message}", file=sys.stderr, flush=True)


# @beacon[
#   id=worker@main,
#   role=main,
#   slice_labels=nexus-core-v1,
#   kind=ast,
# ]
async def main():
    """Main worker entry point."""

    # SPLIT BOUNDARY:
    # Argument parsing, environment bootstrap, client initialization, and worker
    # startup/cache preparation are shared detached-worker infrastructure that can
    # later be extracted to a neutral helper rather than living in both core and lab.
    # @beacon[
    #   id=wkv1@shared-bootstrap,
    #   role=boundary split: shared detached worker bootstrap and environment setup,
    #   slice_labels=nexus-portability,nexus-split-boundary,
    #   kind=span,
    #   show_span=false,
    # ]
    
    parser = argparse.ArgumentParser(description='NEXUS WEAVE detached worker')
    parser.add_argument(
        '--mode',
        required=True,
        choices=['enchanted', 'direct', 'carto_refresh', 'markdown_generate'],
        help=(
            'ENCHANTED (nexus_tag-based), DIRECT (json_file-based), '
            'CARTO_REFRESH (Cartographer F12 job JSON-based), or '
            'MARKDOWN_GENERATE (F12+3 detached pipeline, also CARTO job JSON-based)'
        ),
    )
    parser.add_argument('--nexus-tag', help='NEXUS tag (for enchanted mode)')
    parser.add_argument('--json-file', help='JSON file path (for direct mode)')
    parser.add_argument('--parent-id', help='Parent UUID (for direct mode, optional)')
    parser.add_argument('--dry-run', default='false', help='Dry run flag (true/false)')
    parser.add_argument('--import-policy', default='strict', help='Import policy (for direct mode)')
    parser.add_argument('--carto-job-file', help='CARTO job JSON file (for carto_refresh and markdown_generate modes)')
    
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
        # NOTE: carto_refresh and markdown_generate modes do NOT need NEXUS_RUNS_BASE
        # — they receive their root path via the CARTO job JSON file.
        log_worker("ERROR: NEXUS_RUNS_BASE not set in environment")
        log_worker("Launcher must pass the nexus_runs directory path via environment")
        sys.exit(1)
    
    # Initialize client (loader precedence: explicit config path -> discovered file -> env/defaults)
    try:
        from workflowy_mcp.config import get_server_config, get_server_config_meta

        server_config = get_server_config(reload=True)
        config_meta = get_server_config_meta()
        log_worker(
            "Resolved worker configuration "
            f"(source={config_meta.get('source')}, path={config_meta.get('config_path')})"
        )
    except Exception as e:
        log_worker(f"ERROR: failed to load server configuration: {e}")
        import traceback
        log_worker(traceback.format_exc())
        sys.exit(1)

    api_config = server_config.get_api_config().model_copy(
        update={"timeout": 900}
    )

    client = WorkFlowyClient(api_config)


    # @beacon[
    #   id=weave-worker@refresh_nodes_export_cache,
    #   role=weave worker – refresh_nodes_export_cache,
    #   slice_labels=ra-workflowy-cache,
    #   kind=span,
    #   comment=Async refresh_nodes_export_cache block in weave_worker main,
    # ]
    # Ensure this worker has a fresh /nodes-export snapshot. Unlike the MCP
    # server (where /nodes-export refresh is manual via the UUID widget), the
    # detached WEAVE worker must be able to reconcile against ETHER on its
    # own. We therefore perform a one-time explicit refresh here, but prefer
    # a warm-started snapshot when available.
    if getattr(client, "_nodes_export_cache", None) is not None:
        ts = getattr(client, "_nodes_export_cache_timestamp", None)
        if ts:
            log_worker(
                "Using warm-started /nodes-export cache snapshot for WEAVE worker "
                f"(timestamp={ts})"
            )
        else:
            log_worker("Using warm-started /nodes-export cache snapshot for WEAVE worker")
    else:
        if mode == "markdown_generate":
            log_worker(
                "ERROR: MARKDOWN_GENERATE worker has no warm-started /nodes-export snapshot. "
                "Primary MCP preflight must refresh/cache /nodes-export before spawning this worker."
            )
            sys.exit(1)
        try:
            log_worker("Refreshing /nodes-export cache for WEAVE worker…")
            await client.refresh_nodes_export_cache()
            log_worker("/nodes-export cache refresh complete for WEAVE worker")
        except Exception as e:  # noqa: BLE001
            log_worker(f"ERROR: refresh_nodes_export_cache failed in WEAVE worker: {e}")
            import traceback
            log_worker(traceback.format_exc())
            sys.exit(1)
    # @beacon-close[
    #   id=weave-worker@refresh_nodes_export_cache,
    # ]
    
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
        # CARTO_REFRESH and MARKDOWN_GENERATE modes do not use a .weave.pid file;
        # PID is tracked via the CARTO job JSON.
        log_worker(f"{mode.upper()} mode: PID file will not be created (tracked via job JSON)")
    
    # Write PID file for WEAVE modes only
    if pid_file is not None:
        try:
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            log_worker(f"PID file written: {pid_file}")
        except Exception as e:
            log_worker(f"Failed to write PID file: {e}")
            # Continue anyway - not critical
    
    # @beacon-close[
    #   id=wkv1@shared-bootstrap,
    # ]
    # CORE V1 KEEPER:
    # This branch is the detached Cartographer/F12 worker path and belongs in the
    # installable core surface. It should remain available even if advanced WEAVE
    # capabilities are removed from a simplified public release.
    # @beacon[
    #   id=wkv1@carto-refresh-core,
    #   role=core keeper: detached cartographer refresh job runner,
    #   slice_labels=nexus-portability,nexus-core-v1,
    #   kind=span,
    #   show_span=false,
    # ]
    # @beacon[
    #   id=weave-worker@carto-refresh-job,
    #   role=weave worker – CARTO_REFRESH job runner,
    #   slice_labels=f9-f12-handlers,ra-reconcile,nexus-core-v1,ra-carto-jobs,ra-bulk-visible-apply,ra-logging,
    #   kind=span,
    #   show_span=false,
    #   comment=Detached worker dispatch for CARTO_REFRESH jobs (FILE mode → refresh_file_node_beacons; FOLDER mode → refresh_folder_cartographer_sync). Reads job JSON, runs Cartographer reconcile, writes status back. RELEVANT TO F12+3 TIMEOUT: this entire branch runs OUT-OF-PROCESS via subprocess detach, so foreground MCP server is not directly waiting on it — BUT the foreground bulk-apply runner DOES await per-file refresh_file_node_beacons in-process (see _run_carto_bulk_visible_apply_job). HYPOTHESIS (unverified): if the foreground in-process refresh hangs while a separate detached CARTO_REFRESH worker is also active, deadlock-like symptoms could result. Closures _carto_should_cancel{,_file} and _carto_progress{,_for_file} live inside this branch and are individually beaconed.,
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

        # Derive a per-job directory and expose it via environment so that
        # refresh_file_node_beacons can route per-file JSON/journal/debug logs
        # into a job-specific folder under cartographer_jobs/.
        try:
            jid = job.get("id") or job_path.stem

            # Prefer a sortable timestamp prefix based on created_at if present.
            job_dir_name = str(jid)
            created_str = job.get("created_at")
            if isinstance(created_str, str) and created_str:
                try:
                    # created_at is ISO 8601; convert to YYYYMMDD-HHMMSS for folder naming.
                    dt = datetime.fromisoformat(created_str)
                    prefix = dt.strftime("%Y%m%d-%H%M%S")
                    job_dir_name = f"{prefix}_{jid}"
                except Exception:  # noqa: BLE001
                    # Fall back to bare job id if parsing fails.
                    pass

            job_dir = job_path.parent / job_dir_name
            job_dir.mkdir(parents=True, exist_ok=True)
            os.environ["CARTO_JOB_DIR"] = str(job_dir)
        except Exception as e:  # noqa: BLE001
            log_worker(
                f"CARTO_REFRESH: failed to initialize CARTO_JOB_DIR for job {job.get('id') or job_path.stem}: {e}",
                "CARTO",
            )

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
        # Initialize optional counters used by the UUID widget for richer status text.
        progress.setdefault('nodes_created', 0)
        progress.setdefault('nodes_updated', 0)
        progress.setdefault('nodes_moved', 0)
        progress.setdefault('nodes_deleted', 0)

        # Expose job file to downstream Cartographer refresh helpers so they can
        # report fine-grained apply-phase progress (best-effort).
        os.environ["CARTO_JOB_FILE"] = str(job_path)

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

                # @beacon[
                #   id=weave-worker@carto-refresh-cancel-file,
                #   role=weave worker – CARTO_REFRESH should_cancel (file mode),
                #   slice_labels=ra-carto-jobs,
                #   kind=ast,
                # ]
                def _carto_should_cancel_file() -> bool:
                    """Return True if the CARTO_REFRESH job JSON is marked cancelled.

                    This mirrors the folder-mode _carto_should_cancel but is
                    scoped to file-mode jobs that may route into folder
                    Cartographer sync when Path/Root points to a directory.
                    """
                    try:
                        with open(job_path, "r", encoding="utf-8") as jf:
                            fresh = json.load(jf)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to re-read job JSON for cancel check (file mode): {e}",
                            "CARTO",
                        )
                        return False

                    return str(fresh.get("status")) == "cancelled"

                # @beacon[
                #   id=weave-worker@carto-refresh-progress-file,
                #   role=weave worker  CARTO_REFRESH progress (file mode),
                #   slice_labels=ra-carto-jobs,
                #   kind=ast,
                # ]
                def _carto_progress_for_file(file_uuid: str, completed: int, total: int) -> None:
                    """Best-effort progress writer for CARTO_REFRESH file jobs that
                    actually run a folder-level Cartographer sync via
                    refresh_file_node_beacons. Mirrors the folder-mode
                    _carto_progress behavior."""
                    try:
                        with open(job_path, "r", encoding="utf-8") as jf:
                            fresh = json.load(jf)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to re-read job JSON for progress update (file mode): {e}",
                        )
                        return

                    progress_local = fresh.get("progress") or {}
                    fresh["progress"] = progress_local
                    if total > 0:
                        progress_local["total_files"] = total
                    progress_local["completed_files"] = max(0, int(completed))
                    if file_uuid:
                        progress_local["current_file"] = file_uuid

                    fresh["updated_at"] = datetime.utcnow().isoformat()
                    try:
                        with open(job_path, "w", encoding="utf-8") as jf:
                            json.dump(fresh, jf, indent=2)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to write job JSON for progress update (file mode): {e}",
                        )

                    # Instrumentation: log file-mode progress for debugging
                    try:
                        log_worker(
                            f"CARTO_REFRESH file-mode progress: completed={completed} total={total} file_uuid={file_uuid!r}",
                            "CARTO",
                        )
                    except Exception:
                        # Logging must never break progress updates
                        pass

                result = await client.refresh_file_node_beacons(
                    file_node_id=root_uuid,
                    dry_run=False,
                    cancel_callback=_carto_should_cancel_file,
                    progress_callback=_carto_progress_for_file,
                )
                files_refreshed = 1
            else:
                log_worker(f"CARTO_REFRESH: refreshing FOLDER subtree {root_uuid}")

                # @beacon[
                #   id=weave-worker@carto-refresh-cancel,
                #   role=weave worker – CARTO_REFRESH should_cancel,
                #   slice_labels=ra-carto-jobs,
                #   kind=ast,
                # ]
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

                # @beacon[
                #   id=weave-worker@carto-refresh-progress,
                #   role=weave worker – CARTO_REFRESH progress,
                #   slice_labels=ra-carto-jobs,
                #   kind=ast,
                # ]
                def _carto_progress(file_uuid: str, completed: int, total: int) -> None:
                    """Best-effort progress writer for CARTO_REFRESH folder jobs.

                    Updates the CARTO job JSON's progress.{total_files,completed_files,current_file}.
                    Errors are logged but never raised.
                    """
                    try:
                        with open(job_path, "r", encoding="utf-8") as jf:
                            fresh = json.load(jf)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to re-read job JSON for progress update: {e}",
                        )
                        return

                    progress_local = fresh.get("progress") or {}
                    fresh["progress"] = progress_local
                    if total > 0:
                        progress_local["total_files"] = total
                    progress_local["completed_files"] = max(0, int(completed))
                    if file_uuid:
                        progress_local["current_file"] = file_uuid

                    fresh["updated_at"] = datetime.utcnow().isoformat()
                    try:
                        with open(job_path, "w", encoding="utf-8") as jf:
                            json.dump(fresh, jf, indent=2)
                    except Exception as e:  # noqa: BLE001
                        log_worker(
                            f"CARTO_REFRESH: failed to write job JSON for progress update: {e}",
                        )

                result = await client.refresh_folder_cartographer_sync(
                    folder_node_id=root_uuid,
                    cancel_callback=_carto_should_cancel,
                    progress_callback=_carto_progress,
                )
                if isinstance(result, dict):
                    files_refreshed = result.get('refreshed_file_nodes', 0)
                else:
                    files_refreshed = 0

            # Preserve any more up-to-date progress written by refresh_folder_cartographer_sync
            # or file-mode callbacks, but ensure we at least record final totals.
            cancelled_flag = False
            total_from_result = 0
            if isinstance(result, dict):
                total_from_result = (
                    result.get('total_existing_files')
                    or result.get('refreshed_file_nodes')
                    or 0
                )

            # Incorporate the most recent on-disk progress written by callbacks
            latest_total = 0
            latest_completed = 0
            try:
                with open(job_path, "r", encoding="utf-8") as jf:
                    fresh = json.load(jf)
                fresh_progress = fresh.get("progress") or {}
                latest_total = int(fresh_progress.get("total_files") or 0)
                latest_completed = int(fresh_progress.get("completed_files") or 0)
                cancelled_flag = str(fresh.get("status")) == "cancelled"
            except Exception:  # noqa: BLE001
                pass

            existing_progress = job.get('progress') or {}
            job['progress'] = existing_progress

            progress_total = max(
                int(existing_progress.get('total_files') or 0),
                int(latest_total or 0),
                int(total_from_result or 0),
                int(files_refreshed or 1),
            )
            existing_progress['total_files'] = progress_total

            completed_final = max(
                int(existing_progress.get('completed_files') or 0),
                int(latest_completed or 0),
                int(files_refreshed or progress_total),
            )
            existing_progress['completed_files'] = completed_final

            job['result_summary']['files_refreshed'] = files_refreshed
            job['result_summary']['raw_result'] = result

            # Preserve an explicit "cancelled" status if the job JSON was
            # marked as such by an external actor (e.g. WebSocket
            # carto_cancel_job or mcp_cancel_job). Otherwise treat the job
            # as completed.
            if cancelled_flag:
                job['status'] = 'cancelled'
            else:
                job['status'] = 'completed'
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
    # @beacon-close[
    #   id=wkv1@carto-refresh-core,
    # ]

    # @beacon[
    #   id=weave-worker@markdown-generate-job,
    #   role=weave worker  MARKDOWN_GENERATE job runner (F12+3 detached pipeline),
    #   slice_labels=f9-f12-handlers,ra-reconcile,nexus-core-v1,ra-carto-jobs,ra-bulk-visible-apply,ra-logging,nexus-md-header-path,
    #   kind=span,
    #   show_span=false,
    #   comment=Detached worker dispatch for MARKDOWN_GENERATE jobs (F12+3). Reads CARTO job JSON for root_uuid, runs the 8-phase markdown generate pipeline (bulk-apply tags + write Markdown to disk + reapply beacons + post-flight refresh) via markdown_generate_pipeline.run_markdown_generate_pipeline(). Writes status/progress back to job JSON. Survives MCP server restart.,
    # ]
    # MARKDOWN_GENERATE mode: run F12+3 8-phase pipeline via CARTO job JSON and exit
    if mode == 'markdown_generate':
        if not args.carto_job_file:
            log_worker("ERROR: --carto-job-file required for markdown_generate mode")
            await client.close()
            sys.exit(1)

        job_path = Path(args.carto_job_file)
        if not job_path.exists():
            log_worker(f"ERROR: MARKDOWN_GENERATE job file not found: {job_path}")
            await client.close()
            sys.exit(1)

        # Load job JSON
        try:
            with open(job_path, 'r', encoding='utf-8') as jf:
                job = json.load(jf)
        except Exception as e:
            log_worker(f"ERROR: Failed to read MARKDOWN_GENERATE job file: {e}")
            await client.close()
            sys.exit(1)

        # If job was cancelled before the worker started, exit early without work
        if job.get('status') == 'cancelled':
            log_worker(
                f"MARKDOWN_GENERATE job {job.get('id') or job_path.stem} is already "
                "marked cancelled; exiting without running pipeline."
            )
            await client.close()
            sys.exit(0)

        root_uuid = job.get('root_uuid')
        if not root_uuid:
            log_worker("ERROR: MARKDOWN_GENERATE job has no root_uuid")
            await client.close()
            sys.exit(1)

        # Update job status to running
        now_iso = datetime.utcnow().isoformat()
        job['status'] = 'running'
        job['updated_at'] = now_iso
        job['pid'] = os.getpid()
        try:
            with open(job_path, 'w', encoding='utf-8') as jf:
                json.dump(job, jf, indent=2)
        except Exception as e:
            log_worker(f"ERROR: Failed to write MARKDOWN_GENERATE job file: {e}")

        # Expose job file to downstream helpers (mirrors carto_refresh pattern).
        os.environ["CARTO_JOB_FILE"] = str(job_path)

        log_worker(f"MARKDOWN_GENERATE: starting F12+3 pipeline for root_uuid={root_uuid}")

        exit_code = 0
        try:
            from workflowy_mcp.markdown_generate_pipeline import run_markdown_generate_pipeline
            result = await run_markdown_generate_pipeline(
                client=client,
                root_uuid=root_uuid,
                job_file=str(job_path),
            )
            if not result.get('success'):
                exit_code = 1
                log_worker(f"MARKDOWN_GENERATE: pipeline reported failure: {result.get('error')}")
            else:
                log_worker(
                    f"MARKDOWN_GENERATE: pipeline completed successfully "
                    f"(file_path={result.get('file_path')}, "
                    f"ast_beacons={result.get('ast_beacons_reapplied', 0)})"
                )
        except Exception as e:
            exit_code = 1
            log_worker(f"MARKDOWN_GENERATE failed with error: {e}")
            import traceback
            log_worker(traceback.format_exc())
            # Update job JSON with the failure (the pipeline itself may not
            # have had a chance to do this if the import or initial setup blew up).
            try:
                with open(job_path, 'r', encoding='utf-8') as jf:
                    fresh_job = json.load(jf)
                fresh_job['status'] = 'failed'
                fresh_job['error'] = str(e)
                fresh_job['updated_at'] = datetime.utcnow().isoformat()
                with open(job_path, 'w', encoding='utf-8') as jf:
                    json.dump(fresh_job, jf, indent=2)
            except Exception as inner_e:
                log_worker(f"ERROR: Failed to write MARKDOWN_GENERATE failure status: {inner_e}")
        finally:
            await client.close()
            sys.exit(exit_code)
    # @beacon-close[
    #   id=weave-worker@markdown-generate-job,
    # ]

    # LAB ONLY:
    # The remaining detached ENCHANTED/DIRECT WEAVE execution path is part of the
    # advanced NEXUS studio stack and should stay out of minimal Core v1 packaging.
    # @beacon[
    #   id=wkv1@detached-weave-lab,
    #   role=lab-only: detached enchanted-direct weave worker path,
    #   slice_labels=nexus-portability,nexus-lab-only,
    #   kind=span,
    #   show_span=false,
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


    # @beacon-close[
    #   id=wkv1@detached-weave-lab,
    # ]
# @beacon-close[
#   id=wkv1@file-boundary,
# ]
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_worker("Worker interrupted by user")
        sys.exit(130)
