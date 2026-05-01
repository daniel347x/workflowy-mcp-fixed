"""F12+3 Markdown Generate Pipeline (detached worker pipeline body).

This module contains the 8-phase pipeline that generates Markdown from a
Workflowy subtree, runs it through bulk-apply tag materialization, writes
the result to disk, reapplies on-disk beacons, and post-flight refreshes
Workflowy from disk.

ARCHITECTURE:
    Originally (April 2026), this 8-phase pipeline ran INLINE inside the
    MCP server's WebSocket handler `_handle_generate_markdown_file`. That
    blocked the MCP foreground event loop for 5-10 minutes per F12+3
    invocation, which made the entire MCP server (including chat tool
    calls) unresponsive while the pipeline ran.

    The fix (May 2026) was to extract the pipeline body into this module,
    which runs inside a detached `weave_worker.py` subprocess (mode
    `markdown_generate`). The MCP server's handler now just spawns the
    subprocess and returns immediately, mirroring the proven F12+1
    detached-worker pattern.

    See Workflowy documentation node:
      "4️⃣ CARTO BACKGROUND WORKERS & CACHE QUIESCENCE"
    for the full architectural rationale.

PIPELINE PHASES:
    [1] Pre-flight cache refresh + quiescent wait.
        Already done by the MCP handler before launching this worker;
        the worker warm-starts from the resulting fresh disk cache.

    [2] Resolve file_path; export Workflowy subtree from cache.

    [3] Bulk visible tag apply (file-scoped).
        Loops through every visible AST/beacon/MD_PATH-tagged node,
        applying #tag-based beacon updates from Workflowy node names
        to the on-disk source file. Triggers a per-file Cartographer
        refresh after the loop.

    [4] Wait for bulk apply + cascading cache refresh to drain.
        Ensures the cache reflects post-bulk-apply Workflowy state.

    [4.5] Reload worker's in-memory cache from disk.
        Critical step: the worker's RAM cache was frozen at startup,
        before phase [3]'s mutations. The cache refresh in phase [4]
        wrote the disk file, but the worker's in-memory copy is still
        stale. Phase [4.5] explicitly reloads from disk so phase [5]
        sees the post-mutation state.

    [5] Re-export the Workflowy subtree from the now-fresh cache.

    [6] Phase 1 disk write: nexus_to_tokens -> Markdown file.

    [7] Phase 2 disk write: reapply_markdown_ast_beacons rehydrates
        on-disk beacon HTML comments.

    [8] Post-flight file-refresh: another disk -> Workflowy refresh
        so MD_PATH metadata in Workflowy notes is re-canonicalized.
        This is launched as a separate detached CARTO_REFRESH job
        and we do NOT wait for it (fire-and-forget).
"""
from __future__ import annotations

import importlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _read_job_payload(job_file: str) -> dict[str, Any]:
    """Read the CARTO job JSON from disk."""
    with open(job_file, "r", encoding="utf-8") as jf:
        return json.load(jf)


def _write_job_payload(job_file: str, payload: dict[str, Any]) -> None:
    """Write the CARTO job JSON to disk (best-effort)."""
    try:
        with open(job_file, "w", encoding="utf-8") as jf:
            json.dump(payload, jf, indent=2)
    except Exception:
        # Don't let job-file write errors crash the pipeline.
        pass


def _append_log(log_file: str | None, message: str) -> None:
    """Append a timestamped line to the job log file (best-effort)."""
    if not log_file:
        return
    try:
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write(f"[{_now_iso()}] {message}\n")
    except Exception:
        pass


def _set_phase(
    job_file: str,
    job: dict[str, Any],
    log_file: str | None,
    phase: str,
    message: str,
) -> None:
    """Update job's current_phase and write a log line."""
    progress = job.setdefault("progress", {})
    progress["current_phase"] = phase
    job["updated_at"] = _now_iso()
    _write_job_payload(job_file, job)
    _append_log(log_file, f"[phase={phase}] {message}")


def _job_cancelled(job_file: str) -> bool:
    """Check if the job has been externally marked cancelled."""
    try:
        fresh = _read_job_payload(job_file)
        return str(fresh.get("status")) == "cancelled"
    except Exception:
        return False


async def run_markdown_generate_pipeline(
    *,
    client: Any,
    root_uuid: str,
    job_file: str,
) -> dict[str, Any]:
    """Run the 8-phase F12+3 Markdown generate pipeline.

    Args:
        client: A WorkFlowyClient instance (worker's own, with warm cache).
        root_uuid: Workflowy UUID of the file/subtree to generate Markdown for.
        job_file: Path to the CARTO job JSON file for status/progress updates.

    Returns:
        Result dict with success/error/file_path/counts. The job JSON is
        also updated to reflect the final state.
    """
    # Initial job state.
    job = _read_job_payload(job_file)
    log_file = job.get("logs_path")

    job["status"] = "running"
    job["pid"] = os.getpid()
    job["updated_at"] = _now_iso()
    progress = job.setdefault("progress", {})
    progress.setdefault("current_phase", "starting")
    progress.setdefault("nodes_created", 0)
    progress.setdefault("nodes_updated", 0)
    progress.setdefault("nodes_moved", 0)
    progress.setdefault("nodes_deleted", 0)
    result_summary = job.setdefault("result_summary", {})
    result_summary.setdefault("errors", [])
    _write_job_payload(job_file, job)
    _append_log(
        log_file,
        f"Starting F12+3 markdown generate pipeline for root_uuid={root_uuid}",
    )

    try:
        if _job_cancelled(job_file):
            _append_log(log_file, "Cancelled before pipeline started.")
            job["status"] = "cancelled"
            progress["current_phase"] = "cancelled"
            job["updated_at"] = _now_iso()
            _write_job_payload(job_file, job)
            return {"success": False, "cancelled": True}

        # ====================================================================
        # PHASE [2]: Resolve path + export subtree from worker's warm cache.
        # ====================================================================
        _set_phase(job_file, job, log_file, "phase-2-resolve-path", "Resolving file path from cache")

        # Import the path resolver.
        from .client.api_client_nexus import resolve_cartographer_path_from_node

        all_nodes = client._get_nodes_export_cache_nodes()
        nodes_by_id = {str(n.get("id")): n for n in all_nodes if n.get("id")}

        resolved = resolve_cartographer_path_from_node(
            node_id=str(root_uuid),
            nodes_by_id=nodes_by_id,
        )
        file_path = str(resolved.get("abs_path") or "")
        if not file_path:
            raise ValueError(f"Could not resolve absolute path for node {root_uuid}")
        _append_log(log_file, f"Resolved file_path: {file_path}")

        # ====================================================================
        # PHASE [3]: Bulk visible tag apply.
        # ====================================================================
        # The bulk-apply pre-pass loops through every visible AST/beacon node
        # and calls update_beacon_from_node + per-file refresh_file_node_beacons.
        # In the worker subprocess, we run this INLINE (not via a separate
        # subprocess) because we're already in a detached worker — the MCP
        # server's event loop is no longer involved.
        # ====================================================================
        if _job_cancelled(job_file):
            return _finish_cancelled(job_file, job, log_file)

        _set_phase(
            job_file,
            job,
            log_file,
            "phase-3-bulk-apply",
            "Running bulk visible tag apply",
        )

        # Build the synthetic visible tree from the worker's cache.
        export_result = await client.export_nodes(node_id=root_uuid)
        flat_nodes = export_result.get("nodes", []) or []
        if not flat_nodes:
            raise ValueError(f"Empty subtree export for {root_uuid}")

        hierarchical_tree = client._build_hierarchy(flat_nodes, True)
        if not hierarchical_tree:
            raise ValueError("Failed to build hierarchical tree.")

        root_node_for_apply: dict[str, Any] | None = None
        for candidate in hierarchical_tree:
            if str(candidate.get("id") or "") == str(root_uuid):
                root_node_for_apply = candidate
                break
        if root_node_for_apply is None:
            root_node_for_apply = hierarchical_tree[0]

        synthetic_visible_tree = {
            "root": {
                "id": str(root_node_for_apply.get("id") or root_uuid),
                "name": str(root_node_for_apply.get("name") or ""),
                "note": str(root_node_for_apply.get("note") or ""),
            },
            "children": list(root_node_for_apply.get("children") or []),
        }

        # Run the bulk apply inline in this worker (NOT spawning a sub-subprocess).
        bulk_apply_summary = await _run_bulk_apply_inline(
            client=client,
            visible_tree_payload=synthetic_visible_tree,
            job_file=job_file,
            log_file=log_file,
            cancel_check=lambda: _job_cancelled(job_file),
        )
        _append_log(
            log_file,
            f"Bulk apply complete: "
            f"node_updates={bulk_apply_summary.get('node_updates', 0)}, "
            f"files_refreshed={bulk_apply_summary.get('files_refreshed', 0)}, "
            f"files_skipped={bulk_apply_summary.get('files_skipped', 0)}",
        )

        if _job_cancelled(job_file):
            return _finish_cancelled(job_file, job, log_file)

        # ====================================================================
        # PHASE [4] + [4.5]: Refresh worker's cache so phase [5] sees
        # post-mutation state.
        #
        # The worker's in-memory cache was frozen at startup, BEFORE phase [3]
        # mutated Workflowy. We need to refresh it now.
        #
        # Worker-driven refresh strategy (vs. waiting for MCP server's watcher
        # to refresh and reloading from disk): we trigger our own refresh
        # call. This goes through the same Workflowy server-side rate limit
        # so it won't be redundant — if MCP server already refreshed in the
        # last 60s, this call coalesces with that. If not, we pay the rate
        # limit ourselves.
        #
        # See Workflowy documentation:
        #   "🔁 Mid-pipeline cache refresh: how phase [4] of F12+3 actually works"
        # ====================================================================
        _set_phase(
            job_file,
            job,
            log_file,
            "phase-4-refresh-cache",
            "Refreshing worker's /nodes-export cache after bulk apply",
        )

        if bulk_apply_summary.get("node_updates", 0) > 0 or bulk_apply_summary.get("files_refreshed", 0) > 0:
            try:
                refresh_result = await client.refresh_nodes_export_cache()
                _append_log(
                    log_file,
                    f"Cache refresh complete: nodes={refresh_result.get('node_count', '?')}",
                )
            except Exception as e:
                _append_log(log_file, f"WARNING: cache refresh failed: {e}")
                # Continue anyway — phase [5] will use whatever cache state exists.
        else:
            _append_log(log_file, "Skipping cache refresh (no mutations in phase [3])")

        if _job_cancelled(job_file):
            return _finish_cancelled(job_file, job, log_file)

        # ====================================================================
        # PHASE [5]: Re-export from now-fresh cache.
        # ====================================================================
        _set_phase(
            job_file,
            job,
            log_file,
            "phase-5-re-export",
            "Re-exporting subtree from refreshed cache",
        )

        export_result = await client.export_nodes(node_id=root_uuid)
        flat_nodes = export_result.get("nodes", []) or []
        if not flat_nodes:
            raise ValueError(f"Re-export returned empty nodes for {root_uuid}")

        hierarchical_tree = client._build_hierarchy(flat_nodes, True)
        if not hierarchical_tree:
            raise ValueError("Failed to rebuild hierarchical tree after bulk apply.")
        root_node = hierarchical_tree[0]

        # ====================================================================
        # PHASES [6] + [7]: Markdown generation + on-disk beacon reapplication.
        # ====================================================================
        if _job_cancelled(job_file):
            return _finish_cancelled(job_file, job, log_file)

        _set_phase(
            job_file,
            job,
            log_file,
            "phase-6-write-markdown",
            "Generating and writing Markdown to disk",
        )

        # Locate markdown_roundtrip module. It lives at the project root
        # (C:\Temp\workflowy-mcp-vladzima\), not inside src/. We walk up
        # from this file's location to find it.
        script_dir = Path(__file__).parent.resolve()  # .../src/workflowy_mcp/
        src_dir = script_dir.parent  # .../src/
        project_root = src_dir.parent  # C:\Temp\workflowy-mcp-vladzima\
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

        markdown_roundtrip = importlib.import_module("markdown_roundtrip")
        from markdown_it import MarkdownIt

        frontmatter_text, root_for_render = markdown_roundtrip.detach_yaml_frontmatter_child(
            root_node
        )

        raw_markdown = "\n".join(
            markdown_roundtrip.nexus_to_tokens(root_for_render, depth=0)
        )

        # Parse once for validation (don't re-render through mdformat — see
        # original handler comments about list-marker normalization).
        md = MarkdownIt("commonmark")
        _ = md.parse(raw_markdown)

        final_markdown = markdown_roundtrip.clean_html_entities(raw_markdown)
        if frontmatter_text is not None:
            frontmatter_clean = markdown_roundtrip.clean_html_entities(frontmatter_text).strip("\n")
            body_clean = final_markdown.lstrip("\n")
            final_markdown = f"---\n{frontmatter_clean}\n---\n\n{body_clean}"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(final_markdown)
        _append_log(log_file, f"Wrote Markdown to {file_path} ({len(final_markdown)} chars)")

        _set_phase(
            job_file,
            job,
            log_file,
            "phase-7-reapply-beacons",
            "Reapplying on-disk Markdown AST beacons",
        )

        beacon_results = markdown_roundtrip.reapply_markdown_ast_beacons(
            file_path, root_for_render
        )
        ast_beacon_count = len(beacon_results)
        _append_log(log_file, f"Reapplied {ast_beacon_count} AST beacons on disk")

        # ====================================================================
        # PHASE [8]: Post-flight file refresh (disk -> Workflowy).
        #
        # We launch this as a SEPARATE detached CARTO_REFRESH job and do
        # NOT await it. F12+3 is "complete" once the disk file is written;
        # the post-refresh just keeps Workflowy's metadata in sync but
        # isn't strictly part of the user's "generate markdown" intent.
        #
        # The widget will show this follow-up CARTO_REFRESH job in the
        # active jobs list, so the user knows it's happening.
        # ====================================================================
        _set_phase(
            job_file,
            job,
            log_file,
            "phase-8-post-refresh",
            "Launching detached post-flight file refresh",
        )

        # We can't import server.py functions here (we're in a worker
        # subprocess, server.py isn't imported). Instead, we directly
        # spawn a new weave_worker.py subprocess in carto_refresh mode.
        # But: we need to be careful — we're already a worker subprocess.
        # Spawning ANOTHER subprocess from inside us is fine, but the
        # MCP server's watcher loop will track both jobs.
        post_refresh_job_id = None
        try:
            post_refresh_job_id = _launch_post_refresh_subprocess(
                client=client,
                root_uuid=root_uuid,
                log_file=log_file,
            )
        except Exception as e:
            _append_log(log_file, f"WARNING: post-flight refresh launch failed: {e}")
            # Non-fatal: F12+3 succeeded. User will need to manually refresh.

        # ====================================================================
        # SUCCESS
        # ====================================================================
        progress["current_phase"] = "done"
        job["status"] = "completed"
        job["cache_refresh_required"] = True  # Phase [3] mutated Workflowy
        result_summary["file_path"] = file_path
        result_summary["ast_beacons_reapplied"] = ast_beacon_count
        result_summary["bulk_apply"] = bulk_apply_summary
        result_summary["post_refresh_job_id"] = post_refresh_job_id
        result_summary["message"] = (
            f"Generated markdown for {root_uuid} at {file_path} "
            f"(ast_beacons={ast_beacon_count}, "
            f"bulk_apply.node_updates={bulk_apply_summary.get('node_updates', 0)}, "
            f"post_refresh_job_id={post_refresh_job_id!r})"
        )
        job["updated_at"] = _now_iso()
        _write_job_payload(job_file, job)
        _append_log(log_file, "F12+3 pipeline completed successfully.")

        return {
            "success": True,
            "file_path": file_path,
            "ast_beacons_reapplied": ast_beacon_count,
            "bulk_apply": bulk_apply_summary,
            "post_refresh_job_id": post_refresh_job_id,
        }

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        _append_log(log_file, f"ERROR: F12+3 pipeline failed: {e}\n{tb}")
        progress["current_phase"] = "failed"
        job["status"] = "failed"
        job["error"] = str(e)
        result_summary.setdefault("errors", []).append(str(e))
        job["updated_at"] = _now_iso()
        _write_job_payload(job_file, job)
        return {"success": False, "error": str(e)}


def _finish_cancelled(
    job_file: str, job: dict[str, Any], log_file: str | None
) -> dict[str, Any]:
    """Mark job cancelled and return cancelled result."""
    progress = job.setdefault("progress", {})
    progress["current_phase"] = "cancelled"
    job["status"] = "cancelled"
    job["cache_refresh_required"] = False
    job["updated_at"] = _now_iso()
    _write_job_payload(job_file, job)
    _append_log(log_file, "Pipeline cancelled.")
    return {"success": False, "cancelled": True}


async def _run_bulk_apply_inline(
    *,
    client: Any,
    visible_tree_payload: dict[str, Any],
    job_file: str,
    log_file: str | None,
    cancel_check,
) -> dict[str, Any]:
    """Run the bulk-apply phase inline (no sub-subprocess).

    This mirrors `_run_carto_bulk_visible_apply_job` from server.py but runs
    directly in the current worker process instead of spawning yet another
    asyncio task. We're already in a detached worker, so we don't need to
    detach again.

    Returns a summary dict similar to the bulk-apply CARTO job's result_summary.
    """
    # Import the helper that walks the visible tree and groups candidates by
    # enclosing FILE node. This lives in server.py — we need to import it
    # carefully because server.py initializes a lot of MCP server state.
    # Solution: import the helper module-level functions only; server.py's
    # top-level FastMCP registration runs unconditionally on import, but
    # the worker process can tolerate that (it just registers handlers we
    # never invoke).
    from .server import (
        _build_bulk_visible_root_from_payload,
        _bulk_apply_per_file_inner_loop,
        _collect_bulk_visible_apply_groups,
    )

    summary = {
        "node_updates": 0,
        "files_refreshed": 0,
        "files_skipped": 0,
        "nodes_skipped": 0,
        # nodes_skipped_in_sync counts candidates filtered by
        # _node_name_tags_match_beacon_block_in_note (Bug #2 optimization).
        # Distinct from "nodes_skipped" which counts errors/missing-id cases.
        "nodes_skipped_in_sync": 0,
        "warnings": [],
        "errors": [],
    }

    # Build root node from payload.
    root_node = _build_bulk_visible_root_from_payload(
        {"visible_tree": visible_tree_payload}
    )
    if not isinstance(root_node, dict):
        summary["errors"].append("Invalid visible_tree payload.")
        return summary

    # Get cache-side nodes for grouping.
    try:
        all_nodes_cache = client._get_nodes_export_cache_nodes() or []
        nodes_by_id_cache = {
            str(n.get("id")): n
            for n in all_nodes_cache
            if isinstance(n, dict) and n.get("id")
        }
    except Exception:
        nodes_by_id_cache = {}

    # Group candidates by enclosing FILE node.
    grouped_files, grouping_warnings, skipped_unchanged = _collect_bulk_visible_apply_groups(
        root_node,
        nodes_by_id_cache=nodes_by_id_cache,
    )
    if grouping_warnings:
        summary["warnings"].extend(grouping_warnings)
        for msg in grouping_warnings:
            _append_log(log_file, f"WARNING: {msg}")
    if skipped_unchanged:
        summary["nodes_skipped"] += int(skipped_unchanged)
        _append_log(
            log_file,
            f"Skipping {skipped_unchanged} unchanged visible node(s) after semantic compare.",
        )

    file_items = sorted(
        grouped_files.items(),
        key=lambda kv: str(kv[1].get("file_node", {}).get("name") or kv[0]).lower(),
    )

    if not file_items:
        _append_log(log_file, "No eligible visible AST/beacon nodes found.")
        return summary

    _append_log(log_file, f"Processing {len(file_items)} file(s).")

    # Update progress in job JSON.
    job = _read_job_payload(job_file)
    progress = job.setdefault("progress", {})
    progress["total_files"] = len(file_items)
    progress["completed_files"] = 0
    _write_job_payload(job_file, job)

    processed_files = 0
    successful_file_refreshes = 0

    for file_id, file_group in file_items:
        if cancel_check():
            _append_log(log_file, "Cancellation requested; stopping bulk apply.")
            break

        file_node = file_group.get("file_node") or {}
        file_name = str(file_node.get("name") or file_id)
        candidate_nodes = file_group.get("nodes") or []

        # Update progress with current file.
        job = _read_job_payload(job_file)
        progress = job.setdefault("progress", {})
        progress["current_file"] = file_id
        _write_job_payload(job_file, job)
        _append_log(
            log_file,
            f"Updating {len(candidate_nodes)} visible node(s) for file {file_name} ({file_id}).",
        )

        # Per-candidate inner loop — delegated to the single-source-of-truth
        # helper _bulk_apply_per_file_inner_loop in server.py. Both server.py
        # and this file route through that helper so any future change to the
        # inner loop touches exactly ONE place (eliminates the May 2026
        # duplicate-codepath gotcha that hid Bug #2's fix from F12+3).
        def _pipeline_progress_tick() -> None:
            job_local = _read_job_payload(job_file)
            progress_local = job_local.setdefault("progress", {})
            progress_local["nodes_updated"] = summary["node_updates"]
            _write_job_payload(job_file, job_local)

        file_successful_node_updates, was_cancelled = (
            await _bulk_apply_per_file_inner_loop(
                client=client,
                candidate_nodes=candidate_nodes,
                file_name=file_name,
                summary=summary,
                cancel_check=cancel_check,
                log=lambda msg: _append_log(log_file, msg),
                progress_tick=_pipeline_progress_tick,
            )
        )

        if was_cancelled:
            break

        if file_successful_node_updates > 0:
            _append_log(
                log_file,
                f"Refreshing Workflowy FILE node once for {file_name} ({file_id}).",
            )
            try:
                refresh_result = await client.refresh_file_node_beacons(
                    file_node_id=file_id,
                    dry_run=False,
                )
                if isinstance(refresh_result, dict) and not refresh_result.get(
                    "success", True
                ):
                    raise RuntimeError(
                        refresh_result.get("error") or "refresh_file_node_beacons returned failure"
                    )
                successful_file_refreshes += 1
                summary["files_refreshed"] = successful_file_refreshes
                # Update progress with refresh stats.
                job = _read_job_payload(job_file)
                progress = job.setdefault("progress", {})
                progress["nodes_created"] = progress.get("nodes_created", 0) + int(
                    (refresh_result or {}).get("nodes_created", 0) or 0
                )
                progress["nodes_moved"] = progress.get("nodes_moved", 0) + int(
                    (refresh_result or {}).get("nodes_moved", 0) or 0
                )
                progress["nodes_deleted"] = progress.get("nodes_deleted", 0) + int(
                    (refresh_result or {}).get("nodes_deleted", 0) or 0
                )
                _write_job_payload(job_file, job)
                _append_log(
                    log_file,
                    f"Refreshed FILE node {file_name}: "
                    f"+{int((refresh_result or {}).get('nodes_created', 0) or 0)} "
                    f"~{int((refresh_result or {}).get('nodes_updated', 0) or 0)} "
                    f"↻{int((refresh_result or {}).get('nodes_moved', 0) or 0)} "
                    f"-{int((refresh_result or {}).get('nodes_deleted', 0) or 0)}",
                )
            except Exception as e:
                msg = f"FILE refresh failed for {file_name} ({file_id}): {e}"
                summary["errors"].append(msg)
                _append_log(log_file, f"ERROR: {msg}")
        else:
            summary["files_skipped"] += 1
            _append_log(
                log_file,
                f"No successful node updates for {file_name}; skipping FILE refresh.",
            )

        processed_files += 1
        # Update progress.
        job = _read_job_payload(job_file)
        progress = job.setdefault("progress", {})
        progress["completed_files"] = processed_files
        progress["current_file"] = None
        _write_job_payload(job_file, job)

    return summary


def _launch_post_refresh_subprocess(
    *,
    client: Any,
    root_uuid: str,
    log_file: str | None,
) -> str | None:
    """Launch a detached CARTO_REFRESH worker for post-flight file refresh.

    This is fire-and-forget. The MCP server's watcher loop will discover
    the new job JSON file under cartographer_jobs/ and track it.

    Returns the new job_id, or None on failure.
    """
    import subprocess
    from uuid import uuid4

    # Locate cartographer_jobs base dir. We use the same config-resolution
    # logic as server.py via the workflowy_mcp.config helpers.
    try:
        from workflowy_mcp.config import get_cartographer_jobs_dir
        carto_jobs_base = str(get_cartographer_jobs_dir())
    except Exception as e:
        _append_log(log_file, f"WARNING: could not resolve carto_jobs_base: {e}")
        return None

    os.makedirs(carto_jobs_base, exist_ok=True)

    now_dt = datetime.utcnow()
    now = now_dt.isoformat()
    job_id = f"carto-refresh-file-{uuid4().hex[:8]}"
    ts_prefix = now_dt.strftime("%Y%m%d-%H%M%S")
    filename_prefix = f"{ts_prefix}_{job_id}"
    new_job_file = os.path.join(carto_jobs_base, f"{filename_prefix}.json")
    new_log_file = os.path.join(carto_jobs_base, f"{filename_prefix}.log")

    job_payload = {
        "id": job_id,
        "type": "CARTO_REFRESH",
        "mode": "file",
        "root_uuid": root_uuid,
        "status": "queued",
        "cache_refresh_required": True,
        "created_at": now,
        "updated_at": now,
        "progress": {
            "total_files": 1,
            "completed_files": 0,
            "current_file": None,
            "current_phase": None,
        },
        "result_summary": {
            "files_refreshed": 0,
            "errors": [],
        },
        "error": None,
        "logs_path": new_log_file,
    }
    with open(new_job_file, "w", encoding="utf-8") as jf:
        json.dump(job_payload, jf, indent=2)

    # Locate weave_worker.py.
    worker_script = Path(__file__).parent / "weave_worker.py"
    worker_script = worker_script.resolve()
    if not worker_script.exists():
        _append_log(log_file, f"WARNING: weave_worker.py not found at {worker_script}")
        return None

    # Get API key from client config.
    try:
        api_key = client.config.api_key.get_secret_value()
    except Exception:
        api_key = os.environ.get("WORKFLOWY_API_KEY", "")
    if not api_key:
        _append_log(log_file, "WARNING: no WORKFLOWY_API_KEY available for post-refresh subprocess")
        return None

    env = os.environ.copy()
    env["WORKFLOWY_API_KEY"] = api_key

    cmd = [
        sys.executable,
        str(worker_script),
        "--mode",
        "carto_refresh",
        "--carto-job-file",
        new_job_file,
        "--dry-run",
        "false",
    ]

    log_handle = open(new_log_file, "w", encoding="utf-8")

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
    start_new_session = sys.platform != "win32"

    process = subprocess.Popen(
        cmd,
        env=env,
        creationflags=creationflags,
        start_new_session=start_new_session,
        stdin=subprocess.DEVNULL,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )

    # Update job file with PID.
    job_payload["pid"] = process.pid
    job_payload["updated_at"] = datetime.utcnow().isoformat()
    with open(new_job_file, "w", encoding="utf-8") as jf:
        json.dump(job_payload, jf, indent=2)

    _append_log(
        log_file,
        f"Launched post-flight CARTO_REFRESH subprocess (PID={process.pid}, job_id={job_id})",
    )
    return job_id
