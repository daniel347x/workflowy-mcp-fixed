"""WorkFlowy API client - Exploration state machine operations."""

import json
import sys
import os
import uuid
from typing import Any
from datetime import datetime
from pathlib import Path

from ..models import NetworkError

from .api_client_core import _ClientLogger, log_event
from .api_client_nexus import WorkFlowyClientNexus

# 2-LETTER ACTION CODE MAPPING (module-level constant)

# Step guidance constants (we are beginning migration toward shared guidance blocks)
UNDO_STEP_GUIDANCE_UR = "↩️ UNDO (SESSION ONLY): UR = reopen_node_to_undecided — resets a decided handle (finalized/closed) back to undecided (unseen) so it reappears in frontiers; clears local skeleton/prune hints; best-effort reopens auto-completed ancestors; does NOT change Workflowy ETHER (only session state)."
SP_STEP_GUIDANCE = "📝 SCRATCHPAD PREVIEW: SP = include scratchpad_preview in step output (optionally filter) — args: handle=<HANDLE>, include_ancestors=<bool>, include_descendants=<bool>. Multiple SP actions union filters. Unknown args rejected."
SC_STEP_GUIDANCE = "✅ SCRATCHPAD COMPLETE: SC = scratchpad_complete_by_id — args: scratch_id=SP-000123 (marks entry done=true; shows ✅ in scratchpad_preview; does not delete)."

EXPLORATION_ACTION_2LETTER = {
    "EL": "engulf_leaf_into_gem_for_editing",
    "PL": "preserve_leaf_in_ether_untouched",
    "UL": "update_leaf_node_and_engulf_in_gemstorm",
    "RB": "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states",
    "PB": "preserve_branch_node_in_ether_untouched__when_no_engulfed_children",
    "UB": "update_branch_node_and_engulf_in_gemstorm__descendants_unaffected",
    "UN": "update_branch_note_and_engulf_in_gemstorm__descendants_unaffected",
    "AB": "auto_decide_branch_no_change_required",
    "EF": "engulf_all_showing_undecided_descendants_into_gem_for_editing",
    "PF": "preserve_all_showing_undecided_descendants_in_ether",
    "UT": "update_tag_and_engulf_in_gemstorm",
    "SS": "append_scratchpad",  # DEPRECATED: SS now aliases to append_scratchpad
    "AS": "append_scratchpad",
    "AH": "add_hint",
    "PD": "peek_descendants_as_frontier",
    "SX": "search_descendants_for_text",
    "PA": "preserve_all_remaining_nodes_in_ether_at_finalization",
    "RF": "resume_guided_frontier",
    "LF": "lightning_flash_into_section",
    "MSD": "mark_section_for_deletion",
    "MSM": "mark_section_as_merge_target",
    "MSP": "mark_section_as_permanent",
    "ALS": "abandon_lightning_strike",

    # Undo (session-only): reopen a previously decided handle back to undecided.
    # WARNING: This only mutates the exploration session state machine; it does NOT undo any edits
    # made to Workflowy ETHER (those happen only at finalize/weave).
    "UR": "reopen_node_to_undecided",

    # Scratchpad enhancements
    "SP": "include_scratchpad_in_step_output",
    "SC": "scratchpad_complete_by_id",
}


def _normalize_scratchpad(session: dict[str, Any]) -> list[dict[str, Any]]:
    """Ensure session['scratchpad'] is a list of entry dicts and return it."""
    scratch = session.get("scratchpad")
    entries: list[dict[str, Any]] = []

    if isinstance(scratch, list):
        for item in scratch:
            if isinstance(item, dict):
                # Ensure 'note' is a string if present
                note = item.get("note")
                if note is not None and not isinstance(note, str):
                    item = dict(item)
                    item["note"] = str(note)
                entries.append(item)
            else:
                entries.append({"note": str(item)})
    elif isinstance(scratch, str):
        for line in scratch.splitlines():
            line = line.strip()
            if not line:
                continue
            entries.append({"note": line})
    elif scratch is not None:
        entries.append({"note": str(scratch)})

    session["scratchpad"] = entries
    return entries


def _append_scratchpad_entry(session: dict[str, Any], entry: dict[str, Any]) -> None:
    """Append a structured entry to the exploration scratchpad.

    Adds stable scratchpad IDs (SP-000001, ...) to enable completion / targeting.
    """
    scratch_entries = _normalize_scratchpad(session)

    # Assign stable scratchpad id if missing
    if isinstance(entry, dict) and "scratch_id" not in entry:
        counter = session.get("scratchpad_counter", 0)
        try:
            counter = int(counter)
        except (TypeError, ValueError):
            counter = 0
        counter += 1
        session["scratchpad_counter"] = counter
        entry = dict(entry)
        entry["scratch_id"] = f"SP-{counter:06d}"
        entry.setdefault("done", False)
        entry.setdefault("created_at", datetime.utcnow().isoformat() + "Z")

    scratch_entries.append(entry)
    session["scratchpad"] = scratch_entries


class WorkFlowyClientExploration(WorkFlowyClientNexus):
    """Exploration state machine - extends Nexus."""

    # ---
    # Exploration UX: Multi-action error reporting
    #
    # Exploration steps often bundle many semantically decoupled actions (PL/EL + MSM/MSD/MSP + LF, etc.).
    # Historically, a single invalid action raised NetworkError and made it unclear which prior actions
    # succeeded. We now prefer a "best-effort" execution model: attempt all safe actions, return a clear
    # per-action status report, and (if any failures occurred) DO NOT compute a new frontier/peek result.
    # This prevents "invisible lightning flashes" (LF state changing without output).
    # ---

    @staticmethod
    def _format_action_report_plain_text(
        action_reports: list[dict[str, Any]],
        note_max_chars: int = 240,
    ) -> str:
        """Render a human-readable plain-text report for partial failures.

        This is intended to be shown directly to the caller (TypingMind). We still optionally return
        structured action_reports for programmatic consumption.
        """

        total = len(action_reports)
        failed = [r for r in action_reports if r.get("status") == "failed"]
        ok = [r for r in action_reports if r.get("status") == "ok"]
        skipped = [r for r in action_reports if r.get("status") == "skipped_due_to_errors"]
        warned = [r for r in action_reports if r.get("status") == "ok_with_warning"]

        lines: list[str] = []
        lines.append(f"❌ ERROR: {len(failed)}/{total} actions failed. No new frontier computed. Ephemeral LF/peek/search state cleared.")
        lines.append(f"✅ {len(ok) + len(warned)}/{total} actions succeeded." + (f" ({len(warned)} with warnings)" if warned else ""))
        if skipped:
            lines.append(f"⏭️ {len(skipped)}/{total} actions skipped due to earlier error(s).")
        lines.append("")

        if failed:
            lines.append("ERRORS")
            for r in failed:
                i = r.get("i")
                act = r.get("action")
                handle = r.get("handle")
                msg = r.get("error") or "Unknown error"
                lines.append(f"- Step [{i}]: {act} handle={handle}")
                lines.append(f"  Reason: {msg}")
                rec = r.get("recommended_action_instead")
                if rec:
                    lines.append(f"  Recommended instead: {rec}")
                rec2 = r.get("recommended_handle_instead")
                if rec2:
                    lines.append(f"  Recommended handle: {rec2}")
                note = r.get("note")
                if isinstance(note, str) and note.strip():
                    n = note.strip().replace("\n", " ")
                    if len(n) > note_max_chars:
                        n = n[:note_max_chars] + "…"
                    lines.append(f"  Note: {n}")
            lines.append("")

        if warned:
            lines.append("WARNINGS")
            for r in warned:
                i = r.get("i")
                act = r.get("action")
                handle = r.get("handle")
                w = r.get("warning") or "(warning)"
                lines.append(f"- Step [{i}]: {act} handle={handle}")
                lines.append(f"  Warning: {w}")
            lines.append("")

        if ok:
            lines.append("SUCCEEDED")
            for r in ok:
                i = r.get("i")
                act = r.get("action")
                handle = r.get("handle")
                lines.append(f"- Step [{i}]: {act} handle={handle}")
            lines.append("")

        if skipped:
            lines.append("SKIPPED")
            for r in skipped:
                i = r.get("i")
                act = r.get("action")
                handle = r.get("handle")
                reason = r.get("skip_reason") or "Skipped due to earlier error(s)"
                lines.append(f"- Step [{i}]: {act} handle={handle}")
                lines.append(f"  Reason: {reason}")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _get_explore_sessions_dir(self) -> str:
        """Return exploration sessions directory."""
        base_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_explore_sessions"
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    def _get_explore_scratchpad_dir(self) -> str:
        """Return permanent scratchpad snapshots directory."""
        base_dir = r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_explore_scratchpads"
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    def _write_exploration_output(
        self,
        session_id: str,
        phase: str,
        payload: dict[str, Any],
        session: dict[str, Any] | None = None,
    ) -> None:
        """Write full exploration call output (plus session tree) to per-session file.

        This is best-effort only; failures are logged but do not affect the tool result.
        """
        import json as json_module
        from pathlib import Path as _Path

        try:
            base_dir = _Path(self._get_explore_sessions_dir())
            out_dir = base_dir / session_id
            out_dir.mkdir(parents=True, exist_ok=True)

            pattern = f"{phase}__{session_id}.*.json"
            max_index = 0
            for existing in out_dir.glob(pattern):
                stem_parts = existing.stem.split(".")
                if len(stem_parts) >= 2:
                    last = stem_parts[-1]
                    if last.isdigit():
                        max_index = max(max_index, int(last))
            index = max_index + 1
            out_path = out_dir / f"{phase}__{session_id}.{index}.json"

            full_output = dict(payload or {})
            if session is not None:
                _normalize_scratchpad(session)
                full_output["session"] = session
                try:
                    sp = session.get("scratchpad") or []
                    handles = session.get("handles") or {}
                    sp_preview = self._build_scratchpad_preview_lines(sp, handles)
                    if sp_preview:
                        full_output["scratchpad_preview"] = sp_preview
                except Exception:
                    # Preview is best-effort only
                    pass

            with open(out_path, "w", encoding="utf-8") as f:
                json_module.dump(full_output, f, indent=2, ensure_ascii=False)
        except Exception:
            logger = _ClientLogger()
            logger.error("Failed to write exploration output file", exc_info=True)

    @staticmethod
    def _build_frontier_preview_lines(
        frontier: list[dict[str, Any]] | None,
        max_note_chars: int | None = None,
    ) -> list[str]:
        """Build one-line-per-handle preview for frontiers.

        Notes are rendered inline on the same line as the entry name. By
        default (max_note_chars=None) notes are **not clipped**; all
        newlines are flattened to literal "\\n" so each entry stays on a
        single line of the preview.
        """
        if not frontier:
            return []

        handles = [str(entry.get("handle", "")) for entry in frontier]
        if not handles:
            return []
        max_len = max(len(h) for h in handles)

        lines = []
        for entry in frontier:
            handle = str(entry.get("handle", ""))
            label = handle.ljust(max_len)
            depth = max(1, len(handle.split(".")))

            # In some previews we include the synthetic root handle 'R' for context.
            # When 'R' is shown, all other nodes should appear visually nested beneath it.
            # We do this by adding one extra indent level for non-root handles.
            base_indent_level = (depth - 1)
            if handle and handle != "R":
                base_indent_level += 1
            indent = " " * 4 * base_indent_level

            is_leaf = bool(entry.get("is_leaf"))
            is_pseudo_leaf = bool(entry.get("is_pseudo_leaf"))

            # Bullet semantics:
            # - True leaf: "•"
            # - True branch: "⦿"
            # - Pseudo-leaf branch (branch surfaced as a leaf candidate): "◦"
            if is_leaf:
                bullet = "•"
            elif is_pseudo_leaf:
                bullet = "◦"
            else:
                bullet = "⦿"

            kind = "🍃" if is_leaf else "🪵"

            name = entry.get("name_preview") or "Untitled"
            note = entry.get("note_preview") or ""

            # Optional transient skeleton annotations (for lightning/structure previews)
            tag_prefix = ""
            sk = entry.get("skeleton_tmp")
            if sk:
                tags = []
                if isinstance(sk, str):
                    sk = [sk]
                for t in sk:
                    if not isinstance(t, str):
                        continue
                    if t.startswith("STRUCT:"):
                        root = t.split(":", 1)[1]
                        tags.append(f"[STRUCT {root}]")
                    elif t.startswith("LS:"):
                        root = t.split(":", 1)[1]
                        tags.append(f"[LS {root}]")
                    elif t.startswith("DTARGETSELF:"):
                        root = t.split(":", 1)[1]
                        tags.append(f"[DECIDED TARGET {root}]")
                    elif t.startswith("DTARGETDESC:"):
                        root = t.split(":", 1)[1]
                        tags.append(f"[DESCENDANT OF DECIDED TARGET {root}]")
                    elif t.startswith("LEAFSIB:"):
                        # Format: LEAFSIB:X/Y where X,Y are integers
                        payload = t.split(":", 1)[1]
                        try:
                            pos_str, total_str = payload.split("/", 1)
                            pos = int(pos_str)
                            total = int(total_str)

                            # UX: if this preview line is part of a STRUCT-mode lightning strike,
                            # reflect that in the first (leaf-sibling) prefix itself.
                            has_struct = any(isinstance(x, str) and x.startswith("STRUCT:") for x in sk)
                            if has_struct:
                                tags.append(f"[LEAF STRUCT {pos}/{total} siblings]")
                            else:
                                tags.append(f"[LEAF {pos}/{total} siblings]")
                        except Exception:
                            # Best-effort only; ignore malformed LEAFSIB tags
                            continue

                # Ensure LEAF sibling marker appears FIRST (best UX for STRUCT-mode previews)
                def _tag_priority(tag: str) -> tuple[int, str]:
                    if tag.startswith("[LEAF "):
                        return (0, tag)
                    if tag.startswith("[DECIDED TARGET "):
                        return (1, tag)
                    if tag.startswith("[DESCENDANT OF DECIDED TARGET "):
                        return (2, tag)
                    if tag.startswith("[STRUCT "):
                        return (3, tag)
                    if tag.startswith("[LS "):
                        return (4, tag)
                    return (5, tag)

                tags.sort(key=_tag_priority)
                if tags:
                    tag_prefix = " ".join(tags) + " "

            if isinstance(note, str) and note:
                flat = note.replace("\n", "\\n")
                if isinstance(max_note_chars, int) and max_note_chars > 0 and len(flat) > max_note_chars:
                    flat = flat[:max_note_chars]
                name_part = f"{tag_prefix}{name} [{flat}]"
            else:
                name_part = f"{tag_prefix}{name}" if tag_prefix else name

            # Frontier previews do not include scratchpad IDs; keep alignment stable without relying
            # on scratchpad-only variables.
            lines.append(f"[{label}] {indent}{bullet} {kind} {name_part}")

        # NOTE: scratchpad_preview (UNBOUND) is a scratchpad-only concept.
        # Frontier previews should never reference scratchpad variables.
        return lines

    @staticmethod
    @staticmethod
    def _extract_handle_from_scratchpad_entry(entry: dict[str, Any]) -> str | None:
        """Best-effort: extract a single handle from a scratchpad entry.

        Single-source-of-truth for scratchpad handle binding.

        Rules:
        - Prefer explicit handle keys (handle, branch_handle, etc.).
        - Fallback: scan all string fields for handle-like tokens; only return if EXACTLY ONE unique
          candidate is found.
        - Return None if ambiguous or not found.
        """
        import re

        if not isinstance(entry, dict):
            return None

        handle_re = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*$")
        handle_anywhere_re = re.compile(r"\b[A-Za-z]+(?:\.[A-Za-z0-9]+)+\b")

        HANDLE_KEYS = (
            "handle",
            "root_handle",
            "target_handle",
            "branch_handle",
            "node_handle",
            "leaf_handle",
        )

        for k in HANDLE_KEYS:
            v = entry.get(k)
            if isinstance(v, str):
                v2 = v.strip()
                if v2 and handle_re.match(v2):
                    return v2

        candidates: set[str] = set()
        for v in entry.values():
            if not isinstance(v, str):
                continue
            for m in handle_anywhere_re.findall(v):
                if handle_re.match(m):
                    candidates.add(m)

        if len(candidates) == 1:
            return next(iter(candidates))

        return None

    def _build_scratchpad_preview_lines(
        self,
        scratchpad: list[dict[str, Any]] | None,
        handles: dict[str, dict[str, Any]] | None = None,
        max_note_chars: int | None = None,
    ) -> list[str]:
        """Build a "true" mini-tree preview from scratchpad entries.

        Goals (resume baton handoff UX):
        - Include ANY scratchpad entry that can be tied to exactly one handle.
        - Include ALL ancestors up to R (stub lines if no note) so the mini-tree is complete.
        - Natural-handle order, with proper indentation.
        - Multiple notes per handle => multiple lines at same indentation.

        Notes are rendered inline on the same line as the entry name. Newlines are flattened to
        literal "\\n" so each preview entry stays one line.
        """
        import re

        if not scratchpad:
            return []

        handles_map = handles or {}
        DEFAULT_MAX_NOTE = 1024 if max_note_chars is None else max_note_chars
        # max_note_chars=None means "do not clip" (full text).
        if max_note_chars is None:
            DEFAULT_MAX_NOTE = None

        handle_re = re.compile(r"^[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*$")
        handle_anywhere_re = re.compile(r"\b[A-Za-z]+(?:\.[A-Za-z0-9]+)+\b")

        def _natural_handle_key(handle: str) -> list[tuple[int, object]]:
            parts = handle.split(".")
            result: list[tuple[int, object]] = []
            for part in parts:
                if part.isdigit():
                    result.append((0, int(part)))
                else:
                    result.append((1, part))
            return result

        # 1) Extract handle + note from scratchpad entries (best-effort)
        # We also track scratch_id per note line so previews can display stable IDs.
        notes_by_handle: dict[str, list[dict[str, Any]]] = {}
        unbound_notes: list[dict[str, Any]] = []  # notes that cannot be tied to exactly one handle

        for entry in scratchpad:
            if not isinstance(entry, dict):
                continue

            raw_note = entry.get("note")
            # Accept both 'note' and 'content' (AS/SS payload) as the note text.
            if raw_note is None:
                raw_note = entry.get("content")
            if raw_note is None:
                continue
            note = raw_note if isinstance(raw_note, str) else str(raw_note)

            scratch_id = entry.get("scratch_id")
            scratch_id = scratch_id if isinstance(scratch_id, str) else None

            handle = self._extract_handle_from_scratchpad_entry(entry)

            record = {"note": note, "scratch_id": scratch_id, "done": bool(entry.get("done") is True)}

            if handle is None:
                unbound_notes.append(record)
                continue

            notes_by_handle.setdefault(handle, []).append(record)


        if not notes_by_handle and not unbound_notes:
            return []

        # 2) Expand to include ancestors up to R (stub lines)
        expanded_handles: set[str] = set()
        for h in notes_by_handle.keys():
            cur = h
            seen: set[str] = set()
            while isinstance(cur, str) and cur and cur not in seen:
                seen.add(cur)
                expanded_handles.add(cur)
                if cur == "R":
                    break
                cur = (handles_map.get(cur) or {}).get("parent")
                if cur is None:
                    break

        # Compute scratch_id padding width for this preview.
        all_ids: list[str] = []
        for recs in notes_by_handle.values():
            for rec in recs:
                sid = rec.get("scratch_id")
                if isinstance(sid, str):
                    all_ids.append(sid)
        for rec in unbound_notes:
            sid = rec.get("scratch_id")
            if isinstance(sid, str):
                all_ids.append(sid)
        max_id_len = max((len(x) for x in all_ids), default=len("SP-000000"))

        ordered_handles = sorted(expanded_handles, key=_natural_handle_key)
        if "R" in expanded_handles:
            ordered_handles = ["R"] + [h for h in ordered_handles if h != "R"]

        # 3) Render
        # If there are no handle-bound entries, ordered_handles may be empty.
        max_len = max((len(h) for h in ordered_handles), default=1)
        lines: list[str] = []

        for h in ordered_handles:
            meta = handles_map.get(h) or {}
            children = meta.get("children") or []
            is_leaf = not children
            bullet = "•" if is_leaf else "⦿"
            kind = "🍃" if is_leaf else "🪵"
            name = meta.get("name") or "Untitled"

            label = h.ljust(max_len)
            depth = max(1, len(h.split(".")))
            base_indent_level = (depth - 1)
            if h and h != "R":
                base_indent_level += 1
            indent = " " * 4 * base_indent_level

            notes = notes_by_handle.get(h)

            if not notes:
                # Stub ancestor line (no note)
                id_blank = " " * max_id_len
                lines.append(f"[{id_blank}] [{label}] {indent}{bullet} {kind} {name}")
                continue

            for idx, rec in enumerate(notes):
                note = rec.get("note")
                note = note if isinstance(note, str) else str(note)
                flat = note.replace("\n", "\\n")
                if isinstance(DEFAULT_MAX_NOTE, int) and DEFAULT_MAX_NOTE > 0 and len(flat) > DEFAULT_MAX_NOTE:
                    flat = flat[:DEFAULT_MAX_NOTE]
                prefix = "" if idx == 0 else "↳ "

                done_prefix = "✅ " if (rec.get("done") is True) else ""

                sid = rec.get("scratch_id")
                sid = sid if isinstance(sid, str) else "SP-??????"
                sid_label = sid.ljust(max_id_len)

                name_part = f"{name} [{done_prefix}{prefix}{flat}]"
                lines.append(f"[{sid_label}] [{label}] {indent}{bullet} {kind} {name_part}")

        # 4) Render UNBOUND (global) entries (no handle)
        # These must appear in scratchpad_preview so baton handoff notes are visible on resume/finalize.
        if unbound_notes:
            lines.append("")
            lines.append("[UNBOUND]")
            for rec in unbound_notes:
                note = rec.get("note")
                note = note if isinstance(note, str) else str(note)
                flat = note.replace("\n", "\\n")
                if isinstance(DEFAULT_MAX_NOTE, int) and DEFAULT_MAX_NOTE > 0 and len(flat) > DEFAULT_MAX_NOTE:
                    flat = flat[:DEFAULT_MAX_NOTE]

                done_prefix = "✅ " if (rec.get("done") is True) else ""

                sid = rec.get("scratch_id")
                sid = sid if isinstance(sid, str) else "SP-??????"
                sid_label = sid.ljust(max_id_len)

                lines.append(f"    • [{sid_label}] {done_prefix}{flat}")

        # NOTE: scratchpad_preview (UNBOUND) is a scratchpad-only concept.
        # Frontier previews should never reference scratchpad variables.
        return lines

    def _build_frontier_tree_from_flat(self, frontier: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build nested tree from flat frontier."""
        if not frontier:
            return []

        by_handle: dict[str, dict[str, Any]] = {}
        for entry in frontier:
            node = dict(entry)
            node["children"] = []
            by_handle[node["handle"]] = node

        roots = []
        for handle, node in by_handle.items():
            parent = node.get("parent_handle")
            if parent and parent in by_handle:
                by_handle[parent]["children"].append(node)
            else:
                roots.append(node)

        # Natural sort
        def natural_key(node):
            handle = node.get("handle", "")
            parts = handle.split(".")
            result = []
            for part in parts:
                if part.isdigit():
                    result.append((0, int(part)))
                else:
                    result.append((1, part))
            return result
        
        roots.sort(key=natural_key)
        for node in by_handle.values():
            node["children"].sort(key=natural_key)
        
        # Remove empty children
        for node in by_handle.values():
            if not node["children"]:
                del node["children"]

        return roots

    def _compute_exploration_frontier(
        self,
        session: dict[str, Any],
        frontier_size: int,
        max_depth_per_frontier: int,
    ) -> list[dict[str, Any]]:
        """Compute next frontier for exploration session.
        
        DFS GUIDED modes: Leaf-chunk frontiers (accumulate sibling leaves in DFS order)
        LEGACY modes: Round-robin open parents
        """
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        search_filter = session.get("search_filter")
        exploration_mode = session.get("exploration_mode", "manual")
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        def _collect_hints(handle: str) -> list[str]:
            hints = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_hints = (handles.get(parent, {}) or {}).get("hints") or []
                hints.extend(parent_hints)
                parent = handles.get(parent, {}).get("parent")
            return hints

        def _has_ancestor_hint(handle: str, hint_name: str) -> bool:
            """Return True if any ancestor of handle carries hint_name.

            Used to prune whole preserved/deleted subtrees from frontier generation.
            """
            if not handle or handle == "R":
                return False
            return hint_name in _collect_hints(handle)

        def _nearest_decided_target_ancestor(handle: str) -> str | None:
            """Return nearest ancestor handle that is a decided target (merge/permanent).

            This is purely for preview visibility: helps both human+agent understand why a node
            is showing up in the frontier even though an ancestor section has already been
            marked as a target.
            """
            cur = (handles.get(handle, {}) or {}).get("parent")
            seen: set[str] = set()
            while isinstance(cur, str) and cur and cur not in seen and cur != "R":
                seen.add(cur)
                meta = handles.get(cur, {}) or {}
                hints_here = (meta.get("hints") or [])
                if "SKELETON_MERGE_TARGET" in hints_here or "SKELETON_PERMANENT_TARGET" in hints_here:
                    return cur
                cur = meta.get("parent")
            return None

        frontier = []

        # DFS GUIDED MODES
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:
            from collections import deque

            if frontier_size <= 0:
                return frontier

            leaf_target = max(1, frontier_size)
            leaf_leeway = max(1, min(leaf_target, 10))

            # BFS to collect undecided leaves
            bfs_leaves = []
            queue = deque(["R"])
            visited = set()

            while queue and len(bfs_leaves) < leaf_target + leaf_leeway:
                h = queue.popleft()
                if h in visited:
                    continue
                visited.add(h)

                if h == "R":
                    for ch in handles.get("R", {}).get("children", []) or []:
                        if ch not in visited:
                            queue.append(ch)
                    continue

                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles

                st = state.get(h, {"status": "unseen"})
                is_undecided = st.get("status") in {"unseen", "candidate", "open"}

                if is_leaf and is_undecided:
                    ancestor_hints = _collect_hints(h)
                    if "SKELETON_PRUNED" in ancestor_hints:
                        # Hidden from BFS by Skeleton Walk pruning (delete section), but still present in handles/state.
                        continue
                    if "SKELETON_PERMANENT_PRUNED" in ancestor_hints:
                        # Hidden from BFS by Skeleton Walk permanent pruning (preserve section), but still present in handles/state.
                        continue

                    bfs_leaves.append(h)

                for ch in child_handles:
                    if ch not in visited:
                        queue.append(ch)

            if not bfs_leaves:
                # Optional pseudo-branch inclusion: if no undecided structural leaves remain,
                # surface undecided branches whose descendants are all decided as a "last-mile"
                # frontier, so the agent can explicitly decide them.
                pseudo_leaves: list[str] = []
                for h_all, meta_all in handles.items():
                    if h_all == "R":
                        continue
                    child_handles_all = (meta_all or {}).get("children", []) or []
                    # Only consider structural branches (true leaves already handled above)
                    if not child_handles_all:
                        continue
                    st_all = state.get(h_all, {"status": "unseen"})
                    if st_all.get("status") not in {"unseen", "candidate", "open"}:
                        continue

                    # Check descendants for any undecided node
                    has_undecided_desc = False
                    stack = list(child_handles_all)
                    seen_local: set[str] = set()
                    while stack and not has_undecided_desc:
                        ch = stack.pop()
                        if ch in seen_local:
                            continue
                        seen_local.add(ch)
                        st_ch = state.get(ch, {"status": "unseen"})
                        if st_ch.get("status") in {"unseen", "candidate", "open"}:
                            has_undecided_desc = True
                            break
                        stack.extend((handles.get(ch, {}) or {}).get("children", []) or [])

                    if not has_undecided_desc:
                        pseudo_leaves.append(h_all)

                # If we found pseudo-leaf branches, use them as our frontier; otherwise bail out
                if pseudo_leaves:
                    bfs_leaves = pseudo_leaves
                else:
                    return frontier

            # Chunk by siblings
            selected = []
            selected_set = set()
            total = 0
            idx = 0

            while idx < len(bfs_leaves) and total < leaf_target + leaf_leeway:
                leaf = bfs_leaves[idx]
                idx += 1
                if leaf in selected_set:
                    continue

                parent_handle = handles.get(leaf, {}).get("parent")
                sibling_group = []

                if parent_handle:
                    for ch in handles.get(parent_handle, {}).get("children", []) or []:
                        if ch in selected_set:
                            continue
                        st_ch = state.get(ch, {"status": "unseen"})
                        if st_ch.get("status") in {"finalized", "closed"}:
                            continue
                        ch_children = handles.get(ch, {}).get("children", []) or []
                        if not ch_children:
                            sibling_group.append(ch)
                else:
                    sibling_group.append(leaf)

                if not sibling_group:
                    continue

                if total >= leaf_target and total + len(sibling_group) > leaf_target + leaf_leeway:
                    break

                for ch in sibling_group:
                    if ch in selected_set:
                        continue
                    selected_set.add(ch)
                    selected.append(ch)
                    total += 1

            if not selected:
                return frontier

            # Usability rule (Dan): In NORMAL frontier mode, if any effective leaf is shown under a parent,
            # then show ALL sibling UNDECIDED branch nodes as well (do NOT include decided branches).
            # This provides crucial local context without affecting the leaf budget.
            parents_with_selected_leaf: set[str] = set()
            for leaf_h in selected:
                p = (handles.get(leaf_h, {}) or {}).get("parent")
                if isinstance(p, str):
                    parents_with_selected_leaf.add(p)

            for p in parents_with_selected_leaf:
                for sib in (handles.get(p, {}) or {}).get("children", []) or []:
                    if sib in selected_set:
                        continue
                    sib_children = (handles.get(sib, {}) or {}).get("children", []) or []
                    if not sib_children:
                        continue  # only branches
                    st_sib = state.get(sib, {"status": "unseen"})
                    if st_sib.get("status") in {"finalized", "closed"}:
                        continue  # skip decided branches
                    selected_set.add(sib)
                    selected.append(sib)

            # Collect branch ancestors
            include_branches = True
            branches = []
            if include_branches:
                branch_set = set()
                for leaf_h in selected:
                    ancestor = handles.get(leaf_h, {}).get("parent")
                    while ancestor and ancestor != "R":
                        if ancestor not in branch_set:
                            branch_set.add(ancestor)
                        ancestor = handles.get(ancestor, {}).get("parent")
                branches = sorted(branch_set)

            MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

            # Build branch entries
            if include_branches and branches:
                for h in branches:
                    meta = handles.get(h, {}) or {}
                    st = state.get(h, {"status": "unseen"})
                    child_handles = meta.get("children", []) or []

                    note_full = meta.get("note") or ""
                    if MAX_NOTE_PREVIEW is None:
                        note_preview = note_full
                    else:
                        note_preview = note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]

                    local_hints = meta.get("hints") or []
                    hints_from_ancestors = _collect_hints(h)

                    # Check if has showing descendants
                    has_showing = any(leaf_h.startswith(h + ".") for leaf_h in selected)
                    
                    if exploration_mode == "dfs_guided_bulk" and has_showing:
                        guidance = (
                            "branch: RB=reserve (IN GEM shell; editable summary), "
                            "PB=preserve (ETHER only; NOT in GEM), "
                            "UB|UN=update (IN GEM), AB=auto, "
                            "EF=engulf_showing (IN GEM), PF=preserve_showing (ETHER only; NOT in GEM)"
                        )
                    else:
                        guidance = (
                            "branch: RB=reserve (IN GEM shell; editable summary), "
                            "PB=preserve (ETHER only; NOT in GEM), "
                            "UB|UN=update (IN GEM), AB=auto"
                        )

                    entry = {
                        "handle": h,
                        "parent_handle": meta.get("parent"),
                        "name_preview": meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(child_handles),
                        "depth": meta.get("depth", 0),
                        "status": st.get("status", "candidate"),
                        "is_leaf": False,
                        "is_pseudo_leaf": False,
                        "guidance": guidance,
                        "_frontier_role": "branch_ancestor",
                    }

                    # Visibility: mark decided targets and their descendants
                    local_hints_for_visibility = meta.get("hints") or []
                    tags_tmp: list[str] = []
                    if "SKELETON_MERGE_TARGET" in local_hints_for_visibility or "SKELETON_PERMANENT_TARGET" in local_hints_for_visibility:
                        tags_tmp.append(f"DTARGETSELF:{h}")

                    nearest_target = _nearest_decided_target_ancestor(h)
                    if nearest_target:
                        tags_tmp.append(f"DTARGETDESC:{nearest_target}")

                    if tags_tmp:
                        entry["skeleton_tmp"] = tags_tmp

                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)

            # Build leaf entries
            for h in selected:
                meta = handles.get(h, {}) or {}
                st = state.get(h, {"status": "unseen"})
                child_handles = meta.get("children", []) or []

                note_full = meta.get("note") or ""
                if MAX_NOTE_PREVIEW is None:
                    note_preview = note_full
                else:
                    note_preview = note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]

                local_hints = meta.get("hints") or []
                hints_from_ancestors = _collect_hints(h)

                entry = {
                    "handle": h,
                    "parent_handle": meta.get("parent"),
                    "name_preview": meta.get("name", ""),
                    "note_preview": note_preview,
                    "child_count": len(child_handles),
                    "depth": meta.get("depth", 0),
                    "status": st.get("status", "candidate"),
                    "is_leaf": (not child_handles),
                    "is_pseudo_leaf": bool(child_handles),
                    "guidance": (
                        "leaf: EL=engulf (IN GEM; editable, may be deleted in ETHER), "
                        "PL=preserve (ETHER only; NOT in GEM)"
                    ) if (not child_handles) else (
                        "pseudo-leaf branch: decide this node (branch) now; descendants already decided"
                    ),
                }

                # Visibility: mark decided targets and their descendants
                local_hints_for_visibility = meta.get("hints") or []
                tags_tmp: list[str] = []
                if "SKELETON_MERGE_TARGET" in local_hints_for_visibility or "SKELETON_PERMANENT_TARGET" in local_hints_for_visibility:
                    tags_tmp.append(f"DTARGETSELF:{h}")

                nearest_target = _nearest_decided_target_ancestor(h)
                if nearest_target:
                    tags_tmp.append(f"DTARGETDESC:{nearest_target}")

                if tags_tmp:
                    entry["skeleton_tmp"] = tags_tmp
                if local_hints:
                    entry["hints"] = local_hints
                if hints_from_ancestors:
                    entry["hints_from_ancestors"] = hints_from_ancestors
                
                frontier.append(entry)

            # Natural sort
            def natural_key(entry):
                handle = entry.get("handle", "")
                parts = handle.split(".")
                result = []
                for part in parts:
                    if part.isdigit():
                        result.append((0, int(part)))
                    else:
                        result.append((1, part))
                return result
            
            frontier.sort(key=natural_key)

            # Apply SECONDARY search filter (explicit mode only)
            if search_filter and exploration_mode == "dfs_guided_explicit":
                frontier = self._apply_search_filter_to_frontier(frontier, search_filter, handles)

            return frontier

        # LEGACY MODE: Round-robin open parents
        candidate_parents = [h for h, st in state.items() if st.get("status") == "open"]
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        if not candidate_parents or frontier_size <= 0:
            return frontier

        parent_indices = {h: 0 for h in candidate_parents}

        while len(frontier) < frontier_size:
            made_progress = False

            for parent_handle in candidate_parents:
                meta = handles.get(parent_handle) or {}
                child_handles = meta.get("children", []) or []

                idx = parent_indices.get(parent_handle, 0)
                while idx < len(child_handles) and len(frontier) < frontier_size:
                    child_handle = child_handles[idx]
                    parent_indices[parent_handle] = idx + 1
                    idx += 1

                    child_state = state.get(child_handle, {"status": "unseen"})
                    if child_state.get("status") in {"closed", "finalized"}:
                        continue

                    child_meta = handles.get(child_handle) or {}
                    grandchildren = child_meta.get("children", []) or []

                    is_leaf = not grandchildren
                    guidance = (
                        "leaf: EL=engulf (IN GEM; editable, may be deleted in ETHER), "
                        "PL=preserve (ETHER only; NOT in GEM)"
                    ) if is_leaf else (
                        "branch: OP=open, RB=reserve (IN GEM shell; editable summary)"
                    )

                    note_full = child_meta.get("note") or ""
                    note_preview = note_full if MAX_NOTE_PREVIEW is None else (
                        note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]
                    )

                    local_hints = child_meta.get("hints") or []
                    hints_from_ancestors = _collect_hints(child_handle)

                    entry = {
                        "handle": child_handle,
                        "parent_handle": parent_handle,
                        "name_preview": child_meta.get("name", ""),
                        "note_preview": note_preview,
                        "child_count": len(grandchildren),
                        "depth": child_meta.get("depth", 0),
                        "status": child_state.get("status", "candidate"),
                        "is_leaf": is_leaf,
                        "guidance": guidance,
                    }
                    if local_hints:
                        entry["hints"] = local_hints
                    if hints_from_ancestors:
                        entry["hints_from_ancestors"] = hints_from_ancestors
                    
                    frontier.append(entry)
                    made_progress = True
                    break

                if len(frontier) >= frontier_size:
                    break

            if not made_progress:
                break

        return frontier

    def _apply_search_filter_to_frontier(
        self,
        frontier: list[dict[str, Any]],
        search_filter: dict[str, Any],
        handles: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Apply persistent search filter (SECONDARY - explicit mode only)."""
        import re

        search_text = search_filter.get("search_text", "")
        case_sensitive = search_filter.get("case_sensitive", False)
        whole_word = search_filter.get("whole_word", False)
        use_regex = search_filter.get("regex", False)

        if not search_text:
            return frontier

        filtered = []

        for entry in frontier:
            h = entry.get("handle")
            meta = handles.get(h, {}) or {}
            name = meta.get("name", "")
            note = meta.get("note", "")
            searchable = f"{name}\n{note}"

            matches = False
            if use_regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(search_text, searchable, flags):
                        matches = True
                except re.error:
                    pass
            elif whole_word:
                pattern = r"\b" + re.escape(search_text) + r"\b"
                flags = 0 if case_sensitive else re.IGNORECASE
                if re.search(pattern, searchable, flags):
                    matches = True
            else:
                if case_sensitive:
                    if search_text in searchable:
                        matches = True
                else:
                    if search_text.lower() in searchable.lower():
                        matches = True

            if matches:
                filtered_entry = dict(entry)
                if entry.get("is_leaf"):
                    filtered_entry["guidance"] = "SEARCH MATCH - leaf: EL=engulf, PL=preserve"
                else:
                    filtered_entry["guidance"] = "SEARCH MATCH - branch: RB=reserve, PB=preserve"
                filtered.append(filtered_entry)

        return filtered

    def _compute_search_frontier(
        self,
        session: dict[str, Any],
        search_actions: list[dict[str, Any]],
        scope: str = "undecided",
        max_results: int = 80,
    ) -> list[dict[str, Any]]:
        """Compute frontier from SEARCH results (PRIMARY - bulk mode)."""
        import re

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        editable_mode = bool(session.get("editable", False))
        DEFAULT_NOTE_PREVIEW = 1024

        def _collect_hints(handle: str) -> list[str]:
            hints = []
            parent = handles.get(handle, {}).get("parent")
            while parent:
                parent_hints = (handles.get(parent, {}) or {}).get("hints") or []
                hints.extend(parent_hints)
                parent = handles.get(parent, {}).get("parent")
            return hints

        def _has_ancestor_hint(handle: str, hint_name: str) -> bool:
            """Return True if any ancestor of handle carries hint_name.

            Used to prune whole preserved/deleted subtrees from frontier generation.
            """
            if not handle or handle == "R":
                return False
            return hint_name in _collect_hints(handle)

        def _nearest_decided_target_ancestor(handle: str) -> str | None:
            """Return nearest ancestor handle that is a decided target (merge/permanent).

            This is purely for preview visibility: helps both human+agent understand why a node
            is showing up in the frontier even though an ancestor section has already been
            marked as a target.
            """
            cur = (handles.get(handle, {}) or {}).get("parent")
            seen: set[str] = set()
            while isinstance(cur, str) and cur and cur not in seen and cur != "R":
                seen.add(cur)
                meta = handles.get(cur, {}) or {}
                hints_here = (meta.get("hints") or [])
                if "SKELETON_MERGE_TARGET" in hints_here or "SKELETON_PERMANENT_TARGET" in hints_here:
                    return cur
                cur = meta.get("parent")
            return None

        all_handles = set(handles.keys())
        all_handles.discard("R")
        matches_union: set[str] = set()

        # OR logic across searches (each search_action contributes matches to the union)
        for search_action in search_actions:
            handle_filter = search_action.get("handle", "R")
            search_text = search_action.get("search_text", "")
            case_sensitive = search_action.get("case_sensitive", False)
            whole_word = search_action.get("whole_word", False)
            use_regex = search_action.get("regex", False)

            if not search_text:
                continue

            # Base candidate set for THIS search action
            if handle_filter != "R":
                base = {
                    h for h in all_handles
                    if h == handle_filter or h.startswith(handle_filter + ".")
                }
            else:
                base = set(all_handles)

            # Match within this base
            matching_for_action = set()
            for h in base:
                meta = handles.get(h, {}) or {}
                name = meta.get("name", "")
                note = meta.get("note", "")
                searchable = f"{name}\n{note}"

                if use_regex:
                    try:
                        flags = 0 if case_sensitive else re.IGNORECASE
                        if re.search(search_text, searchable, flags):
                            matching_for_action.add(h)
                    except re.error:
                        pass
                elif whole_word:
                    pattern = r"\b" + re.escape(search_text) + r"\b"
                    flags = 0 if case_sensitive else re.IGNORECASE
                    if re.search(pattern, searchable, flags):
                        matching_for_action.add(h)
                else:
                    if case_sensitive:
                        if search_text in searchable:
                            matching_for_action.add(h)
                    else:
                        if search_text.lower() in searchable.lower():
                            matching_for_action.add(h)

            matches_union |= matching_for_action

        candidates = matches_union

        # Scope filter (undecided only)
        if scope == "undecided":
            UNDECIDED_STATUSES = {"unseen", "candidate", "open"}

            def is_undecided(handle: str) -> bool:
                """Treat missing state entries as 'unseen' (undecided)."""
                st = state.get(handle, {"status": "unseen"})
                return st.get("status") in UNDECIDED_STATUSES

            def has_any_undecided_desc(branch: str) -> bool:
                """Check if branch has ANY undecided descendants (not just search matches)."""
                children = (handles.get(branch, {}) or {}).get("children") or []
                stack = list(children)
                seen: set[str] = set()
                while stack:
                    ch = stack.pop()
                    if ch in seen:
                        continue
                    seen.add(ch)
                    # Check if THIS descendant is undecided (not whether it matches search)
                    if is_undecided(ch):
                        return True
                    stack.extend((handles.get(ch, {}) or {}).get("children") or [])
                return False

            filtered: set[str] = set()
            for h in candidates:
                is_leaf = not (handles.get(h, {}) or {}).get("children")
                if is_leaf:
                    # Leaf: include if undecided
                    if is_undecided(h):
                        filtered.add(h)
                else:
                    # Branch: include if it has ANY undecided descendants OR is itself undecided
                    if is_undecided(h) or has_any_undecided_desc(h):
                        filtered.add(h)
            candidates = filtered

        # Limit matches, include ancestors (including root 'R'), then sort naturally by handle
        matches = sorted(candidates)[:max_results]
        final_handles = set(matches)

        for h in matches:
            ancestor = handles.get(h, {}).get("parent")
            while ancestor:
                final_handles.add(ancestor)
                ancestor = handles.get(ancestor, {}).get("parent")

        def _natural_key_handle(handle: str) -> list[tuple[int, object]]:
            parts = handle.split(".")
            result: list[tuple[int, object]] = []
            for part in parts:
                if part.isdigit():
                    result.append((0, int(part)))
                else:
                    result.append((1, part))
            return result

        # Root 'R' first if present, then others in natural handle order
        if "R" in final_handles:
            others = [h for h in final_handles if h != "R"]
            final_list = ["R"] + sorted(others, key=_natural_key_handle)
        else:
            final_list = sorted(final_handles, key=_natural_key_handle)

        # Build entries
        frontier = []
        MAX_NOTE_PREVIEW = None if editable_mode else DEFAULT_NOTE_PREVIEW

        for h in final_list:
            meta = handles.get(h, {}) or {}
            st = state.get(h, {"status": "unseen"})
            child_handles = meta.get("children", []) or []
            is_leaf = not child_handles

            note_full = meta.get("note") or ""
            note_preview = note_full if MAX_NOTE_PREVIEW is None else (
                note_full if len(note_full) <= MAX_NOTE_PREVIEW else note_full[:MAX_NOTE_PREVIEW]
            )

            local_hints = meta.get("hints") or []
            hints_from_ancestors = _collect_hints(h)

            if h in matches:
                if is_leaf:
                    guidance = (
                        "MATCH - leaf: EL=engulf (IN GEM; editable, may be deleted in ETHER), "
                        "PL=preserve (ETHER only; NOT in GEM)"
                    )
                else:
                    guidance = (
                        "MATCH - branch: RB=reserve (IN GEM shell; editable summary), "
                        "PB=preserve (ETHER only; NOT in GEM), "
                        "EF=engulf_showing (IN GEM), PF=preserve_showing (ETHER only; NOT in GEM)"
                    )
            else:
                guidance = "ancestor (navigational context)"

            entry = {
                "handle": h,
                "parent_handle": meta.get("parent"),
                "name_preview": meta.get("name", ""),
                "note_preview": note_preview,
                "child_count": len(child_handles),
                "depth": meta.get("depth", 0),
                "status": st.get("status", "candidate"),
                "is_leaf": is_leaf,
                "guidance": guidance,
            }
            if local_hints:
                entry["hints"] = local_hints
            if hints_from_ancestors:
                entry["hints_from_ancestors"] = hints_from_ancestors

            frontier.append(entry)

        return frontier

    def nexus_list_exploration_sessions(self, nexus_tag: str | None = None) -> dict[str, Any]:
        """List exploration sessions."""
        import json as json_module
        
        sessions_dir = self._get_explore_sessions_dir()
        sessions_path = Path(sessions_dir)
        
        results = []
        for session_file in sessions_path.glob("*.json"):
            try:
                with open(session_file, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                
                tag = session.get("nexus_tag")
                if nexus_tag and tag != nexus_tag:
                    continue
                
                results.append({
                    "session_id": session.get("session_id"),
                    "nexus_tag": tag,
                    "created_at": session.get("created_at"),
                    "updated_at": session.get("updated_at"),
                    "steps": session.get("steps", 0),
                    "exploration_mode": session.get("exploration_mode"),
                    "editable": session.get("editable", False),
                })
            except Exception:
                continue
        
        results.sort(key=lambda s: s.get("created_at", ""), reverse=True)
        
        return {"success": True, "sessions": results, "total": len(results)}

    async def nexus_resume_exploration(
        self,
        session_id: str | None = None,
        nexus_tag: str | None = None,
        frontier_size: int = 25,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Resume exploration session."""
        import json as json_module
        
        sessions_dir = self._get_explore_sessions_dir()
        
        # Resolve session_id
        if session_id:
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            if not os.path.exists(session_path):
                raise NetworkError(f"Session '{session_id}' not found")
        elif nexus_tag:
            tag_sessions = list(Path(sessions_dir).glob(f"*__{nexus_tag}-*.json"))
            if not tag_sessions:
                raise NetworkError(f"No sessions for '{nexus_tag}'")
            latest = sorted(tag_sessions)[-1]
            session_path = str(latest)
            session_id = latest.stem
        else:
            raise NetworkError("Provide session_id or nexus_tag")
        
        # Load session
        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)
        
        # Check if finalized
        tag = session.get("nexus_tag")
        if tag:
            try:
                run_dir = self._get_nexus_dir(tag)
                phantom = os.path.join(run_dir, "phantom_gem.json")
                if os.path.exists(phantom):
                    full_response = {
                        "success": True,
                        "session_id": session_id,
                        "nexus_tag": tag,
                        "status": "completed",
                        "message": "Already finalized - phantom_gem exists",
                        "phantom_gem": phantom,
                    }
                    self._write_exploration_output(
                        session_id=session_id,
                        phase="resume",
                        payload=full_response,
                        session=session,
                    )
                    return {
                        "success": True,
                        "session_id": session_id,
                        "nexus_tag": tag,
                        "status": "completed",
                        "phantom_gem": phantom,
                    }
            except NetworkError:
                pass
        
        # Compute frontier
        # IMPORTANT: resume should default to the session's saved leaf budget (session['frontier_size']).
        # The 'frontier_size' parameter acts as an explicit override only.
        session_frontier_size = session.get("frontier_size", 25)
        try:
            session_frontier_size = int(session_frontier_size)
        except (TypeError, ValueError):
            session_frontier_size = 25
        if session_frontier_size <= 0:
            session_frontier_size = 25

        effective_frontier_size = frontier_size
        try:
            effective_frontier_size = int(effective_frontier_size)
        except (TypeError, ValueError):
            effective_frontier_size = session_frontier_size
        if effective_frontier_size <= 0:
            effective_frontier_size = session_frontier_size

        # If caller did not explicitly override (i.e., left default=25), prefer session budget.
        # This preserves stable resume behavior for long-running explorations.
        if frontier_size == 25 and session_frontier_size != 25:
            effective_frontier_size = session_frontier_size

        frontier = self._compute_exploration_frontier(session, effective_frontier_size, 1)
        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist flat frontier
        session["last_frontier_flat"] = frontier
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"
        _normalize_scratchpad(session)
        with open(session_path, "w", encoding="utf-8") as f:
            json_module.dump(session, f, indent=2, ensure_ascii=False)
        
        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        
        history = None
        if include_history_summary:
            history = {
                "open": [h for h, st in state.items() if st.get("status") == "open"],
                "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                "steps": session.get("steps", 0),
            }
        
        # Build step guidance (resume should provide the same driver instructions as start/step)
        exploration_mode = session.get("exploration_mode")
        strict = bool(session.get("strict_completeness", False))

        if exploration_mode == "dfs_guided_explicit":
            step_guidance = [
                "🎯 EXPLICIT MODE: Auto-frontier",
                "🪵 = BRANCH",
                "🍃 = LEAF",
                "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER, UL=UPDATE_LEAF_IN_GEM",
                "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                "Lightning: LF=multi-root lightning strike (default 15 nodes per root; large branches show [STRUCT] preview only), MSD=delete section, MSM/MSP=merge/keep, ALS=abandon (per-root/global)",
                "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                UNDO_STEP_GUIDANCE_UR,
                SP_STEP_GUIDANCE,
                SC_STEP_GUIDANCE,
            ]
        elif exploration_mode == "dfs_guided_bulk":
            if strict:
                step_guidance = [
                    "🎯 BULK MODE",
                    "🪵 = BRANCH",
                    "🍃 = LEAF",
                    "🛡️ STRICT - PA disabled",
                    "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER",
                    "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                    "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD, MSM/MSP, ALS",
                    "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                    "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                    UNDO_STEP_GUIDANCE_UR,
                    SP_STEP_GUIDANCE,
                    SC_STEP_GUIDANCE,
                ]
            else:
                step_guidance = [
                    "🎯 BULK MODE",
                    "🪵 = BRANCH",
                    "🍃 = LEAF",
                    "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER",
                    "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                    "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Global: PA=PRESERVE_ALL_REMAINING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD, MSM/MSP, ALS",
                    "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                    "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                    UNDO_STEP_GUIDANCE_UR,
                    SP_STEP_GUIDANCE,
                    SC_STEP_GUIDANCE,
                ]
        else:
            step_guidance = ["🎯 LEGACY MODE: Manual"]

        full_response = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": tag,
            "status": "in_progress",
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "scratchpad_preview": self._build_scratchpad_preview_lines(
                session.get("scratchpad", []),
                handles=session.get("handles", {}) or {},
            ),
            "frontier_preview": frontier_preview,
            "walks": [],
            "skipped_walks": [],
            "decisions_applied": [],
            "history_summary": history,
            "session_meta": {
                "created_at": session.get("created_at"),
                "updated_at": session.get("updated_at"),
                "exploration_mode": session.get("exploration_mode"),
                "editable": session.get("editable"),
                "steps": session.get("steps", 0),
            },
        }

        self._write_exploration_output(
            session_id=session_id,
            phase="resume",
            payload=full_response,
            session=session,
        )

        minimal = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": tag,
            "status": "in_progress",
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "frontier_preview": frontier_preview,
            "scratchpad_preview": self._build_scratchpad_preview_lines(
                session.get("scratchpad", []),
                handles=session.get("handles", {}) or {},
            ),
        }
        return minimal

    async def nexus_start_exploration(
        self,
        nexus_tag: str,
        root_id: str,
        source_mode: str = "scry",
        max_nodes: int = 200000,
        session_hint: str | None = None,
        frontier_size: int = 25,
        max_depth_per_frontier: int = 1,
        editable: bool = False,
        search_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Initialize exploration session."""
        import glob

        logger = _ClientLogger()

        # Validate no existing tag
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        sessions_dir = Path(self._get_explore_sessions_dir())
        
        if base_dir.exists():
            pattern = str(base_dir / f"*__{nexus_tag}")
            if glob.glob(pattern):
                raise NetworkError(f"nexus_tag '{nexus_tag}' already exists")
        
        if sessions_dir.exists():
            pattern = str(sessions_dir / f"*__{nexus_tag}-*.json")
            if glob.glob(pattern):
                raise NetworkError(f"nexus_tag '{nexus_tag}' has existing sessions")

        # Determine mode
        # Default to BULK mode (safer/faster for real exploration work; explicit mode still available via session_hint)
        exploration_mode = "dfs_guided_bulk"
        strict_completeness = False
        
        if session_hint:
            hint = session_hint.strip().lower()
            if "bulk" in hint or "guided_bulk" in hint or "non_strict" in hint:
                exploration_mode = "dfs_guided_bulk"
            if "strict_completeness" in hint or "strict" in hint:
                strict_completeness = True
        
        # Validate search_filter (explicit mode only)
        if search_filter is not None:
            if exploration_mode != "dfs_guided_explicit":
                raise NetworkError("search_filter only for dfs_guided_explicit mode")
            if not isinstance(search_filter, dict) or not search_filter.get("search_text"):
                raise NetworkError("search_filter must have 'search_text'")

        # Fetch tree
        glimpse = await self.workflowy_scry(node_id=root_id, depth=None, size_limit=max_nodes)

        if not glimpse.get("success"):
            raise NetworkError(f"Glimpse failed: {glimpse.get('error')}")

        root_meta = glimpse.get("root") or {"id": root_id, "name": "Root"}
        root_children = glimpse.get("children", []) or []

        root_node = {
            "id": root_meta.get("id", root_id),
            "name": root_meta.get("name", "Root"),
            "note": root_meta.get("note"),
            "parent_id": root_meta.get("parent_id"),
            "children": root_children,
        }

        # Stash original fields for editable
        if editable:
            def _stash(node: dict) -> None:
                if "original_name" not in node:
                    node["original_name"] = node.get("name")
                if "original_note" not in node:
                    node["original_note"] = node.get("note")
                for child in node.get("children") or []:
                    _stash(child)
            _stash(root_node)

        # Assign handles
        handles = {}

        def alpha_handle(index: int) -> str:
            letters = ""
            n = index
            while True:
                n, rem = divmod(n, 26)
                letters = chr(ord("A") + rem) + letters
                if n == 0:
                    break
                n -= 1
            return letters

        def walk(node: dict, handle: str, parent: str | None, depth: int, top: bool = False) -> None:
            children = node.get("children", []) or []
            child_handles = []

            if top:
                for idx, child in enumerate(children):
                    ch = alpha_handle(idx)
                    child_handles.append(ch)
                    walk(child, ch, "R", depth + 1, False)
            else:
                for idx, child in enumerate(children):
                    ch = f"{handle}.{idx + 1}"
                    child_handles.append(ch)
                    walk(child, ch, handle, depth + 1, False)

            handles[handle] = {
                "id": node.get("id"),
                "name": node.get("name", "Untitled"),
                "note": node.get("note"),
                "parent": parent,
                "children": child_handles,
                "depth": depth,
                "hints": [],
            }

        walk(root_node, "R", None, 0, top=True)

        # Ground truth completeness propagation:
        # The exploration session is built from workflowy_scry() (API) which returns a full subtree.
        # Therefore each handle can safely inherit children_status from the source tree.
        # (This is critical so exploration→finalize preserves epistemic completeness.)
        def _index_children_status_by_id(node: dict[str, Any], out: dict[str, str]) -> None:
            if not isinstance(node, dict):
                return
            nid = node.get("id")
            status = node.get("children_status")
            if nid and isinstance(status, str) and status:
                out[str(nid)] = status
            for ch in node.get("children") or []:
                if isinstance(ch, dict):
                    _index_children_status_by_id(ch, out)

        children_status_by_id: dict[str, str] = {}
        _index_children_status_by_id(root_node, children_status_by_id)

        for h, meta in handles.items():
            nid = (meta or {}).get("id")
            if nid and str(nid) in children_status_by_id:
                meta["children_status"] = children_status_by_id[str(nid)]
            else:
                # Fail-closed: if missing, treat as unknown so downstream WEAVE never assumes completeness.
                meta["children_status"] = "unknown"
            handles[h] = meta

        # Initial state
        state = {"R": {"status": "open", "max_depth": None}}
        for ch in handles.get("R", {}).get("children", []) or []:
            state[ch] = {"status": "candidate", "max_depth": None}

        # Create session
        # Persist exploration-wide frontier_size so subsequent steps can reuse it
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        session_id = f"{timestamp}__{nexus_tag}-{uuid.uuid4().hex[:8]}"
        # Ground-truth completeness propagation (critical for WEAVE safety):
        # workflowy_scry() now supports truncation by max_nodes/depth and returns
        # export_root_children_status. Exploration must carry this through to
        # finalize → gem_wrapper → WEAVE.
        export_root_children_status = glimpse.get("export_root_children_status")
        if not isinstance(export_root_children_status, str) or not export_root_children_status:
            export_root_children_status = "unknown"  # fail-closed

        session = {
            "frontier_size": int(frontier_size) if isinstance(frontier_size, int) and frontier_size > 0 else 25,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "root_id": root_node["id"],
            "root_name": root_node.get("name"),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "source_mode": source_mode,
            "exploration_mode": exploration_mode,
            "max_nodes": max_nodes,
            "editable": bool(editable),
            "strict_completeness": strict_completeness,
            "export_root_children_status": export_root_children_status,
            "handles": handles,
            "state": state,
            "scratchpad": [],
            "root_node": root_node,
            "steps": 0,
            "search_filter": search_filter,
            "glimpse_stats": {
                "node_count": glimpse.get("node_count", 0),
                "depth": glimpse.get("depth", 0),
                "_source": glimpse.get("_source", "api"),
            },
        }

        # Compute frontier
        # Use the normalized session-wide frontier_size for initial frontier
        session_frontier_size = session.get("frontier_size", frontier_size)
        frontier = self._compute_exploration_frontier(session, session_frontier_size, max_depth_per_frontier)
        frontier_preview = self._build_frontier_preview_lines(frontier)

        # Persist
        try:
            session_path = os.path.join(sessions_dir, f"{session_id}.json")
            with open(session_path, "w", encoding="utf-8") as f:
                json.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to persist session: {e}") from e

        # Build guidance
        if exploration_mode == "dfs_guided_explicit":
            step_guidance = [
                "🎯 EXPLICIT MODE: Auto-frontier",
                "🪵 = BRANCH",
                "🍃 = LEAF",
                "Leaf: EL=ENGULF_TO_GEM (IN GEM; editable, may be deleted in ETHER), PL=PRESERVE_IN_ETHER (ETHER only; NOT in GEM)",
                "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                "Lightning: LF=multi-root lightning strike (default 15 nodes per root; large branches show [STRUCT] preview only)",
                "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                UNDO_STEP_GUIDANCE_UR,
                SP_STEP_GUIDANCE,
                SC_STEP_GUIDANCE,
            ]
        elif exploration_mode == "dfs_guided_bulk":
            if strict_completeness:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier",
                    "🪵 = BRANCH",
                    "🍃 = LEAF",
                    "🛡️ STRICT COMPLETENESS - PA disabled",
                    "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER, UL=UPDATE_LEAF_IN_GEM",
                    "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM), UB/UN=UPDATE_BRANCH_IN_GEM, AB=AUTO_DECIDE_BRANCH",
                    "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD=delete section, MSM/MSP=merge/keep, ALS=abandon (per-root/global)",
                    "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                    "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                    UNDO_STEP_GUIDANCE_UR,
                    SP_STEP_GUIDANCE,
                    SC_STEP_GUIDANCE,
                ]
            else:
                step_guidance = [
                    "🎯 BULK MODE: Auto-frontier",
                    "🪵 = BRANCH",
                    "🍃 = LEAF",
                    "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER, UL=UPDATE_LEAF_IN_GEM",
                    "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM), UB/UN=UPDATE_BRANCH_IN_GEM, AB=AUTO_DECIDE_BRANCH",
                    "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Global: PA=PRESERVE_ALL_REMAINING_IN_ETHER (ETHER only; NOT in GEM)",
                    "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD=delete section, MSM/MSP=merge/keep, ALS=abandon (per-root/global)",
                    "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                    "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                    UNDO_STEP_GUIDANCE_UR,
                    SP_STEP_GUIDANCE,
                    SC_STEP_GUIDANCE,
                ]
        else:
            step_guidance = ["🎯 LEGACY MODE: Manual"]

        full_response = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "exploration_mode": exploration_mode,
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "root_handle": "R",
            "frontier_preview": frontier_preview,
            "root_summary": {
                "name": root_node.get("name", "Root"),
                "child_count": len(handles.get("R", {}).get("children", []) or []),
            },
            "scratchpad": session.get("scratchpad", []),
            "stats": {
                "total_nodes_indexed": glimpse.get("node_count", 0),
                "truncated": False,
            },
        }

        self._write_exploration_output(
            session_id=session_id,
            phase="start",
            payload=full_response,
            session=session,
        )

        minimal = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "exploration_mode": exploration_mode,
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "step_guidance": step_guidance,
            "frontier_preview": frontier_preview,
        }
        return minimal

    async def nexus_explore_step_v2(
        self,
        session_id: str,
        decisions: list[dict[str, Any]] | None = None,
        global_frontier_limit: int | None = None,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Exploration step with GEMSTORM action vocabulary.

        GUIDED MODES (dfs_guided_explicit / dfs_guided_bulk):
            - decisions: list of { handle, action, ... }
              Actions (canonical names):
                - 'engulf_leaf_into_gem_for_editing' = Bring leaf into GEM
                - 'preserve_leaf_in_ether_untouched' = Preserve leaf in ETHER
                - 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states' = Branch shell
                - 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children' = Preserve branch
            - global_frontier_limit: leaf budget per step (default 80)

        NOTE: The legacy 'walks' concept has been removed. All traversal/peek/lightning/search behavior
        is expressed via decisions (e.g., LF/PD/SX/RF) in guided modes.
        """
        import json as json_module

        logger = _ClientLogger()

        decisions = decisions or []

        # Collect PEEK/LIGHTNING results during this step (used for response/debugging)
        peek_results: list[dict[str, Any]] = []

        # Load session
        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        with open(session_path, "r", encoding="utf-8") as f:
            session = json_module.load(f)

        # Determine effective leaf budget for this step
        session_frontier_size = int(session.get("frontier_size", 25))
        if not isinstance(session_frontier_size, int) or session_frontier_size <= 0:
            session_frontier_size = 25
        effective_limit = global_frontier_limit if isinstance(global_frontier_limit, int) and global_frontier_limit > 0 else session_frontier_size

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}
        _normalize_scratchpad(session)
        editable_mode = bool(session.get("editable", False))
        last_frontier_flat = session.get("last_frontier_flat") or []

        exploration_mode = session.get("exploration_mode", "manual")
        skipped_decisions: list[dict[str, Any]] = []
        
        if exploration_mode in {"dfs_guided_explicit", "dfs_guided_bulk"}:
            # Default: no internal warnings/action reports unless we ran internal step.
            internal_warnings: list[str] = []
            internal_action_reports: list[dict[str, Any]] = []
            include_scratchpad = False

            # Expand 2-letter codes
            if decisions:
                for decision in decisions:
                    act = decision.get("action")
                    if act in EXPLORATION_ACTION_2LETTER:
                        decision["action"] = EXPLORATION_ACTION_2LETTER[act]

            # Separate actions
            # PEEK actions include both explicit peek and lightning flash (both use _peek_frontier)
            peek_actions = [
                d for d in (decisions or [])
                if d.get("action") in {"peek_descendants_as_frontier", "lightning_flash_into_section"}
            ]

            # Scratchpad output toggle
            # NOTE (Dec 2025): SP MUST work in the SAME CALL as LF/PEEK.
            # The SP action itself is processed inside _nexus_explore_step_internal() (it sets
            # session['_include_scratchpad_preview_next'] and session['_sp_filter']).
            # Therefore, we must NOT consume these flags before internal step runs.
            # We detect SP requests up-front (for strict arg validation inside internal step),
            # but we compute include_scratchpad/sp_filter AFTER internal step has persisted changes.
            include_scratchpad_actions = [
                d for d in (decisions or [])
                if d.get("action") == "include_scratchpad_in_step_output"
            ]
            include_scratchpad = False
            sp_filter = None

            # LF FRESH-START SEMANTICS (Dan): Lightning flashes are ephemeral.
            # If there are NO peek/LF actions in this step, we must NOT reuse a stashed _peek_frontier
            # from a prior step.
            if not peek_actions and session.get("_peek_frontier"):
                for key in ("_peek_frontier", "_peek_root_handles", "_peek_max_nodes_by_root"):
                    session.pop(key, None)
                logger.info("Cleared stale peek frontier (fresh-start semantics)")
            search_actions = [d for d in (decisions or []) if d.get("action") == "search_descendants_for_text"]
            resume_actions = [d for d in (decisions or []) if d.get("action") == "resume_guided_frontier"]
            other_decisions = [
                d for d in (decisions or [])
                if d.get("action") not in {"peek_descendants_as_frontier", "search_descendants_for_text", "resume_guided_frontier"}
            ]

            non_search_decisions = (peek_actions or []) + (resume_actions or []) + (other_decisions or [])

            # Pre-compute frontier
            if search_actions:
                correct_frontier_for_bulk = self._compute_search_frontier(
                    session=session,
                    search_actions=search_actions,
                    scope="undecided",
                    max_results=effective_limit,
                )
            elif session.get("_peek_frontier"):
                # Fresh-start semantics: do not reuse stashed peek frontier if it was marked ephemeral-only
                if session.get("_peek_ephemeral_only"):
                    for key in (
                        "_peek_frontier",
                        "_peek_root_handles",
                        "_peek_max_nodes_by_root",
                        "_peek_over_limit_by_root",
                        "_peek_ephemeral_only",
                    ):
                        session.pop(key, None)
                    correct_frontier_for_bulk = self._compute_exploration_frontier(
                        session, frontier_size=effective_limit, max_depth_per_frontier=1
                    )
                    logger.info("Ephemeral STRUCT peek consumed/cleared; returning to DFS")
                else:
                    correct_frontier_for_bulk = session.get("_peek_frontier", [])
                    logger.info(f"Using stashed PEEK frontier ({len(correct_frontier_for_bulk)} entries)")
            elif last_frontier_flat:
                correct_frontier_for_bulk = last_frontier_flat
            else:
                correct_frontier_for_bulk = self._compute_exploration_frontier(
                    session, frontier_size=effective_limit, max_depth_per_frontier=1
                )
            
            # Apply non-search decisions
            if non_search_decisions:
                internal_result = await self._nexus_explore_step_internal(
                    session_id=session_id,
                    actions=non_search_decisions,
                    frontier_size=effective_limit,
                    max_depth_per_frontier=1,
                    _precomputed_frontier=correct_frontier_for_bulk,
                )

                # After internal step runs, it may have set one-shot flags for this SAME call
                # (notably SP: include scratchpad preview in output). We must reload the session
                # BEFORE handling peek_actions early-return, otherwise LF+SP in same call will
                # return a peek frontier without the requested scratchpad_preview.
                try:
                    with open(session_path, "r", encoding="utf-8") as f:
                        session = json_module.load(f)
                except Exception:
                    session = session

                # Persist the session again AFTER internal step so any one-shot flags that were
                # set in-memory inside _nexus_explore_step_internal() (SP filters, include preview)
                # actually make it to disk. Without this, LF+SP can show full scratchpad because
                # _sp_filter was never written.
                try:
                    session["updated_at"] = datetime.utcnow().isoformat() + "Z"
                    with open(session_path, "w", encoding="utf-8") as f:
                        json_module.dump(session, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass

                # New behavior: internal step may return success=False with an action report.
                # In that case, DO NOT compute a new frontier. Return a clear, human-readable
                # failure summary + structured details.
                if internal_result and not internal_result.get("success", True):
                    return {
                        "success": False,
                        "session_id": session_id,
                        "nexus_tag": session.get("nexus_tag"),
                        "status": "in_progress",
                        "exploration_mode": exploration_mode,
                        "error_summary": internal_result.get("error_summary"),
                        "action_reports": internal_result.get("action_reports") or [],
                        "step_guidance": internal_result.get("step_guidance") or [],
                    }

                skipped_decisions = internal_result.get("skipped_decisions", []) or []

                # Propagate internal success warnings (e.g., MSM→EL coercions) upward to the caller.
                # Otherwise they are lost when v2 returns the minimal frontier payload.
                internal_warnings = (internal_result or {}).get("warnings") or []
                internal_action_reports = (internal_result or {}).get("action_reports") or []

                with open(session_path, "r", encoding="utf-8") as f:
                    session = json_module.load(f)
                handles = session.get("handles", {}) or {}
                state = session.get("state", {}) or {}
                root_node = session.get("root_node") or {}
                editable_mode = bool(session.get("editable", False))

                # IMPORTANT (Dec 2025): SP must work in the SAME CALL as LF/PEEK.
                # SP action processing happens inside _nexus_explore_step_internal(), which sets
                # session['_include_scratchpad_preview_next'] and optionally session['_sp_filter'].
                # We must read these AFTER internal step has persisted changes.
                if bool(include_scratchpad_actions) or bool(session.get("_include_scratchpad_preview_next")):
                    include_scratchpad = True

                sp_filter = session.get("_sp_filter")
                if not isinstance(sp_filter, dict):
                    sp_filter = None

                # One-shot semantics: consume these flags after reading them.
                session.pop("_include_scratchpad_preview_next", None)
                session.pop("_sp_filter", None)

                # Persist session again so one-shot flags do not leak across steps.
                try:
                    session["updated_at"] = datetime.utcnow().isoformat() + "Z"
                    with open(session_path, "w", encoding="utf-8") as f:
                        json_module.dump(session, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass

            # Build step guidance
            lf_status_lines: list[str] = []
            if peek_results:
                for pr in peek_results:
                    root_h = pr.get("root_handle")
                    if not root_h:
                        continue
                    is_struct = bool(pr.get("over_limit"))
                    show_dec = bool(pr.get("show_decided_descendants"))
                    if is_struct:
                        suffix = "(decided descendants WILL be shown)" if show_dec else "(decided descendants will NOT be shown)"
                        lf_status_lines.append(
                            f"LF {root_h}: NAVIGATION GUIDE ONLY (STRUCT) — too high for budget; LF deeper or increase budget, or abandon. {suffix}"
                        )
                    else:
                        suffix = "(decided descendants included)" if show_dec else "(decided descendants hidden)"
                        lf_status_lines.append(
                            f"LF {root_h}: FULL PREVIEW (subtree fits within budget). {suffix}"
                        )

            if exploration_mode == "dfs_guided_explicit":
                step_guidance = [
                    "🎯 EXPLICIT MODE: Auto-frontier",
                    *lf_status_lines,
                    "🪵 = BRANCH",
                    "🍃 = LEAF",
                    "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER, UL=UPDATE_LEAF_IN_GEM",
                    "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                    "Lightning: LF=multi-root lightning strike (default 15 nodes per root; large branches show [STRUCT] preview only), MSD=delete section, MSM/MSP=merge/keep, ALS=abandon (per-root/global)",
                    "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                    UNDO_STEP_GUIDANCE_UR,
                    SP_STEP_GUIDANCE,
                    SC_STEP_GUIDANCE,
                ]
            elif exploration_mode == "dfs_guided_bulk":
                strict = session.get("strict_completeness", False)
                if strict:
                    step_guidance = [
                        "🎯 BULK MODE",
                        *lf_status_lines,
                        "🪵 = BRANCH",
                        "🍃 = LEAF",
                        "🛡️ STRICT - PA disabled",
                        "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER",
                        "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                        "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                        "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD, MSM/MSP, ALS",
                        "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                        "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                        UNDO_STEP_GUIDANCE_UR,
                        SP_STEP_GUIDANCE,
                        SC_STEP_GUIDANCE,
                    ]
                else:
                    step_guidance = [
                        "🎯 BULK MODE",
                        *lf_status_lines,
                        "🪵 = BRANCH",
                        "🍃 = LEAF",
                        "Leaf: EL=ENGULF_TO_GEM, PL=PRESERVE_IN_ETHER",
                        "Branch: RB=RESERVE_BRANCH_SHELL_IN_GEM (IN GEM shell; resolved at finalization with child ENGULF/PRESERVE decisions), PB=PRESERVE_BRANCH_IN_ETHER (ETHER only; NOT in GEM)",
                        "Bulk: EF=ENGULF_SHOWING_TO_GEM (IN GEM), PF=PRESERVE_SHOWING_IN_ETHER (ETHER only; NOT in GEM)",
                        "Global: PA=PRESERVE_ALL_REMAINING_IN_ETHER (ETHER only; NOT in GEM)",
                        "Lightning: LF (multi-root, default 15 nodes; [STRUCT] for large branches), MSD, MSM/MSP, ALS",
                        "MSD salvage: MSD may include nodes_to_salvage_for_move=[...handles in LF frontier] to keep/move specific descendants before deleting the section",
                        "Skeleton Walk with Lightning Strikes: BFS across branches, flash (LF) into each (limited, [STRUCT] when large), then for each strike choose MERGE (MSM/MSP, with salvage) or DELETE (MSD, with salvage)",
                        UNDO_STEP_GUIDANCE_UR,
                        SP_STEP_GUIDANCE,
                        SC_STEP_GUIDANCE,
                    ]

            else:
                step_guidance = ["🎯 LEGACY MODE"]

            # Handle PEEK return
            if peek_actions:
                peek_frontier = session.get("_peek_frontier", [])
                if peek_frontier:
                    frontier = peek_frontier
                    frontier_tree = self._build_frontier_tree_from_flat(frontier)
                    frontier_preview = self._build_frontier_preview_lines(frontier)

                    # One-shot structure-only lightning: if there are no real lightning roots,
                    # drop the peek state after returning so MSD/MSM/MSP cannot target it.
                    if not session.get("_peek_root_handles"):
                        for key in ("_peek_frontier", "_peek_max_nodes_by_root"):
                            session.pop(key, None)

                    session["last_frontier_flat"] = frontier
                    session["updated_at"] = datetime.utcnow().isoformat() + "Z"
                    _normalize_scratchpad(session)
                    with open(session_path, "w", encoding="utf-8") as f:
                        json_module.dump(session, f, indent=2, ensure_ascii=False)

                    history_summary = None
                    if include_history_summary:
                        history_summary = {
                            "open": [h for h, st in state.items() if st.get("status") == "open"],
                            "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                            "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                        }

                    # Ensure the step output file always has a stable scratchpad_preview field.
                    # NOTE: In LF-only calls (no SP), 'minimal' may not exist yet in this branch.
                    scratchpad_preview_for_file = None
                    try:
                        scratchpad_preview_for_file = minimal.get("scratchpad_preview")  # type: ignore[name-defined]
                    except Exception:
                        scratchpad_preview_for_file = None

                    full_response = {
                        "success": True,
                        "session_id": session_id,
                        "nexus_tag": session.get("nexus_tag"),
                        "status": "in_progress",
                        "exploration_mode": exploration_mode,
                        "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                        "action_key": EXPLORATION_ACTION_2LETTER,
                        "step_guidance": step_guidance,
                        "frontier_preview": frontier_preview,
                        "decisions_applied": decisions,
                        "skipped_decisions": skipped_decisions,
                        "scratchpad": session.get("scratchpad", []),
                        "history_summary": history_summary,
                        "peek_results": peek_results,
                        "frontier_tree": frontier_tree,
                        # When SP is requested, also persist the rendered preview into the step output file.
                        "scratchpad_preview": scratchpad_preview_for_file,
                        "sp_filter": sp_filter,
                    }

                    self._write_exploration_output(
                        session_id=session_id,
                        phase="step",
                        payload=full_response,
                        session=session,
                    )

                    minimal = {
                        "success": True,
                        "session_id": session_id,
                        "nexus_tag": session.get("nexus_tag"),
                        "status": "in_progress",
                        "exploration_mode": exploration_mode,
                        "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                        "action_key": EXPLORATION_ACTION_2LETTER,
                        "step_guidance": step_guidance,
                        "frontier_preview": frontier_preview,
                    }

                    # IMPORTANT (Dec 2025): SP must work in the SAME CALL as LF/PEEK.
                    # If SP was requested, attach scratchpad_preview to this early-return payload.
                    #
                    # CRITICAL FIX: In the main v2 flow, _sp_filter is explicitly treated as ONE-SHOT.
                    # It is read into a LOCAL variable (sp_filter) and then popped from session and
                    # written back to disk. Therefore, this peek-return path must use the LOCAL sp_filter
                    # computed earlier in this call, NOT re-read session['_sp_filter'] from disk.
                    if include_scratchpad:
                        handles_map = session.get("handles", {}) or {}
                        scratch_entries = session.get("scratchpad", [])

                        try:
                            log_event(f"[SP_DEBUG_RETURN] peek-return using local sp_filter={sp_filter}", component="EXPLORATION")
                        except Exception:
                            pass

                        # Apply optional SP filtering by handle.
                        if sp_filter and isinstance(sp_filter, dict) and sp_filter.get("handles"):
                            target_handles = [h for h in (sp_filter.get("handles") or []) if isinstance(h, str)]
                            include_anc = bool(sp_filter.get("include_ancestors", False))
                            include_desc = bool(sp_filter.get("include_descendants", False))

                            allowed: set[str] = set()

                            def _add_ancestors(h: str) -> None:
                                cur = h
                                seen = set()
                                while isinstance(cur, str) and cur and cur not in seen and cur != "R":
                                    seen.add(cur)
                                    allowed.add(cur)
                                    cur = (handles_map.get(cur) or {}).get("parent")

                            def _add_descendants(h: str) -> None:
                                allowed.add(h)
                                stack = list((handles_map.get(h) or {}).get("children") or [])
                                seen = set()
                                while stack:
                                    ch = stack.pop()
                                    if ch in seen:
                                        continue
                                    seen.add(ch)
                                    allowed.add(ch)
                                    stack.extend((handles_map.get(ch) or {}).get("children") or [])

                            for h in target_handles:
                                if h not in handles_map:
                                    continue
                                allowed.add(h)
                                if include_anc:
                                    _add_ancestors(h)
                                if include_desc:
                                    _add_descendants(h)

                            filtered_scratch = []
                            for e in scratch_entries or []:
                                if not isinstance(e, dict):
                                    continue
                                hh = self._extract_handle_from_scratchpad_entry(e)
                                if hh is None:
                                    # unbound entries excluded from filtered view
                                    continue
                                if hh in allowed:
                                    filtered_scratch.append(e)

                            minimal["scratchpad_preview"] = self._build_scratchpad_preview_lines(
                                filtered_scratch,
                                handles=handles_map,
                            )
                        else:
                            minimal["scratchpad_preview"] = self._build_scratchpad_preview_lines(
                                scratch_entries,
                                handles=handles_map,
                            )

                    return minimal

            # Compute final frontier
            if search_actions:
                frontier = self._compute_search_frontier(
                    session=session, search_actions=search_actions,
                    scope="undecided", max_results=effective_limit
                )
            elif session.get("_peek_frontier"):
                for key in (
                    "_peek_frontier",
                    "_peek_root_handles",
                    "_peek_max_nodes_by_root",
                ):
                    session.pop(key, None)
                logger.info("Peek consumed - returning to DFS")
                frontier = self._compute_exploration_frontier(
                    session, frontier_size=effective_limit, max_depth_per_frontier=1
                )
            else:
                frontier = self._compute_exploration_frontier(
                    session, frontier_size=effective_limit, max_depth_per_frontier=1
                )

            frontier_tree = self._build_frontier_tree_from_flat(frontier)
            frontier_preview = self._build_frontier_preview_lines(frontier)

            session["last_frontier_flat"] = frontier
            session["updated_at"] = datetime.utcnow().isoformat() + "Z"
            _normalize_scratchpad(session)
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)

            history_summary = None
            if include_history_summary:
                history_summary = {
                    "open": [h for h, st in state.items() if st.get("status") == "open"],
                    "finalized": [h for h, st in state.items() if st.get("status") == "finalized"],
                    "closed": [h for h, st in state.items() if st.get("status") == "closed"],
                }

            full_response = {
                "success": True,
                "session_id": session_id,
                "nexus_tag": session.get("nexus_tag"),
                "status": "in_progress",
                "exploration_mode": exploration_mode,
                "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                "action_key": EXPLORATION_ACTION_2LETTER,
                "step_guidance": step_guidance,
                "frontier_preview": frontier_preview,
                "decisions_applied": decisions,
                "skipped_decisions": skipped_decisions,
                "scratchpad": session.get("scratchpad", []),
                "history_summary": history_summary,
                "frontier_tree": frontier_tree,
            }

            self._write_exploration_output(
                session_id=session_id,
                phase="step",
                payload=full_response,
                session=session,
            )

            minimal = {
                "success": True,
                "session_id": session_id,
                "nexus_tag": session.get("nexus_tag"),
                "status": "in_progress",
                "exploration_mode": exploration_mode,
                "action_key_primary_aliases": {"EB": "reserve_branch_for_children"},
                "action_key": EXPLORATION_ACTION_2LETTER,
                "step_guidance": step_guidance,
                "frontier_preview": frontier_preview,
            }

            if include_scratchpad:
                scratch_entries = session.get("scratchpad", [])
                handles_map = session.get("handles", {}) or {}

                # Optional SP filtering by handle
                if sp_filter and isinstance(sp_filter, dict) and sp_filter.get("handles"):
                    target_handles = [h for h in (sp_filter.get("handles") or []) if isinstance(h, str)]
                    include_anc = bool(sp_filter.get("include_ancestors", False))
                    include_desc = bool(sp_filter.get("include_descendants", False))

                    allowed: set[str] = set()

                    def _add_ancestors(h: str) -> None:
                        cur = h
                        seen = set()
                        while isinstance(cur, str) and cur and cur not in seen and cur != "R":
                            seen.add(cur)
                            allowed.add(cur)
                            cur = (handles_map.get(cur) or {}).get("parent")

                    def _add_descendants(h: str) -> None:
                        allowed.add(h)
                        stack = list((handles_map.get(h) or {}).get("children") or [])
                        seen = set()
                        while stack:
                            ch = stack.pop()
                            if ch in seen:
                                continue
                            seen.add(ch)
                            allowed.add(ch)
                            stack.extend((handles_map.get(ch) or {}).get("children") or [])

                    for h in target_handles:
                        if h not in handles_map:
                            continue
                        allowed.add(h)
                        if include_anc:
                            _add_ancestors(h)
                        if include_desc:
                            _add_descendants(h)

                    def _entry_handle(e: dict) -> str | None:
                        # Single-source-of-truth: reuse the SAME handle extraction logic as
                        # _build_scratchpad_preview_lines() so future enhancements stay in sync.
                        # This returns a handle only when it can be tied to exactly one handle.
                        return self._extract_handle_from_scratchpad_entry(e)

                    filtered_scratch = []
                    for e in scratch_entries or []:
                        if not isinstance(e, dict):
                            continue
                        hh = _entry_handle(e)
                        if hh is None:
                            # unbound entries are excluded from filtered view
                            continue
                        if hh in allowed:
                            filtered_scratch.append(e)

                    minimal["scratchpad_preview"] = self._build_scratchpad_preview_lines(
                        filtered_scratch,
                        handles=handles_map,
                    )
                else:
                    minimal["scratchpad_preview"] = self._build_scratchpad_preview_lines(
                        scratch_entries,
                        handles=handles_map,
                    )

            if internal_warnings:
                minimal["warnings"] = internal_warnings
                minimal["action_reports"] = internal_action_reports

            return minimal

    async def nexus_explore_step(
        self,
        session_id: str,
        decisions: list[dict[str, Any]] | None = None,
        global_frontier_limit: int | None = None,
        include_history_summary: bool = True,
    ) -> dict[str, Any]:
        """Exploration step (compat shim).

        NOTE: The legacy 'walks' concept has been removed. All exploration traversal is
        expressed via 'decisions' (LF/PD/SX/RF/etc.).
        """

        # NOTE: This wrapper exists only so server.py can continue to call
        # client.nexus_explore_step(...) without referencing v2 internals.

        return await self.nexus_explore_step_v2(
            session_id=session_id,
            decisions=decisions,
            global_frontier_limit=global_frontier_limit,
            include_history_summary=include_history_summary,
        )

    async def _nexus_explore_step_internal(
        self,
        session_id: str,
        actions: list[dict[str, Any]] | None = None,
        frontier_size: int = 5,
        max_depth_per_frontier: int = 1,
        _precomputed_frontier: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Internal v1-style exploration helper."""
        import json as json_module

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")
        skipped_decisions: list[dict[str, Any]] = []

        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load session: {e}") from e

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        exploration_mode = session.get("exploration_mode", "manual")
        root_node = session.get("root_node") or {}
        _normalize_scratchpad(session)

        # Build node index for editable mode
        node_by_id: dict[str, dict[str, Any]] = {}

        def _index_tree(node: dict[str, Any]) -> None:
            nid = node.get("id")
            if nid:
                node_by_id[nid] = node
            for child in node.get("children", []) or []:
                _index_tree(child)

        if root_node:
            _index_tree(root_node)

        editable_mode = bool(session.get("editable", False))

        def _summarize_descendants(branch_handle: str) -> dict[str, Any]:
            """Summarize descendant decisions."""
            queue: list[str] = list(handles.get(branch_handle, {}).get("children", []) or [])
            descendants: list[str] = []
            while queue:
                h = queue.pop(0)
                descendants.append(h)
                child_handles = handles.get(h, {}).get("children", []) or []
                if child_handles:
                    queue.extend(child_handles)

            if not descendants:
                return {
                    "descendant_count": 0,
                    "has_decided": False,
                    "has_undecided": False,
                    "accepted_leaf_count": 0,
                    "rejected_leaf_count": 0,
                }

            accepted_leaves = 0
            rejected_leaves = 0
            has_decided = False
            has_undecided = False

            for h in descendants:
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")
                if status in {"finalized", "closed"}:
                    has_decided = True
                else:
                    has_undecided = True

                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles
                if not is_leaf:
                    continue

                if status == "finalized":
                    accepted_leaves += 1
                elif status == "closed":
                    rejected_leaves += 1

            return {
                "descendant_count": len(descendants),
                "has_decided": has_decided,
                "has_undecided": has_undecided,
                "accepted_leaf_count": accepted_leaves,
                "rejected_leaf_count": rejected_leaves,
            }

        def _auto_complete_ancestors(start_handle: str) -> None:
            """Auto-complete ancestors when all descendants decided."""
            current = start_handle
            while True:
                parent_handle = (handles.get(current) or {}).get("parent")
                if not parent_handle:
                    break

                parent_entry = state.get(parent_handle, {"status": "unseen", "selection_type": None})
                if parent_entry.get("selection_type") == "subtree":
                    current = parent_handle
                    continue

                summary = _summarize_descendants(parent_handle)
                if summary["descendant_count"] == 0:
                    break
                if summary["has_undecided"]:
                    break

                accepted = summary["accepted_leaf_count"]
                rejected = summary["rejected_leaf_count"]

                parent_entry = state.setdefault(parent_handle, {"status": "unseen", "selection_type": None})

                if accepted > 0:
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "finalized"
                        parent_entry["selection_type"] = "path"
                elif rejected > 0:
                    if parent_entry.get("status") not in {"finalized", "closed"}:
                        parent_entry["status"] = "closed"
                        parent_entry["selection_type"] = None
                else:
                    break

                current = parent_handle

        # Apply actions (best-effort, multi-action reporting)
        actions = actions or []

        # Track per-action outcomes so the caller always knows what succeeded.
        action_reports: list[dict[str, Any]] = []
        any_failures = False
        success_warnings: list[str] = []

        def _report_ok(i: int, act: str, handle: str | None) -> None:
            action_reports.append({"i": i, "action": act, "handle": handle, "status": "ok"})

        def _report_warn(i: int, act: str, handle: str | None, warning: str) -> None:
            action_reports.append(
                {"i": i, "action": act, "handle": handle, "status": "ok_with_warning", "warning": warning}
            )
            # Also capture a top-level warning list so successful steps can surface warnings
            # without forcing callers to inspect action_reports.
            success_warnings.append(f"Step [{i}]: {act} handle={handle} — {warning}")

        def _report_fail(
            i: int,
            act: str,
            handle: str | None,
            error: str,
            *,
            note: str | None = None,
            recommended_action_instead: str | None = None,
            recommended_handle_instead: str | None = None,
        ) -> None:
            nonlocal any_failures
            any_failures = True
            payload: dict[str, Any] = {"i": i, "action": act, "handle": handle, "status": "failed", "error": error}
            if note:
                payload["note"] = note
            if recommended_action_instead:
                payload["recommended_action_instead"] = recommended_action_instead
            if recommended_handle_instead:
                payload["recommended_handle_instead"] = recommended_handle_instead
            action_reports.append(payload)

        def _report_skipped(i: int, act: str, handle: str | None, reason: str) -> None:
            action_reports.append(
                {"i": i, "action": act, "handle": handle, "status": "skipped_due_to_errors", "skip_reason": reason}
            )

        # 2-letter expander + long-name normalizer
        for action in actions:
            act = action.get("action")
            if not act:
                continue
            if act in EXPLORATION_ACTION_2LETTER:
                action["action"] = EXPLORATION_ACTION_2LETTER[act]
                continue

        # Aliases (long-form convenience names)
        ACTION_ALIASES = {
            "reserve_branch_for_children": "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states",
        }
        for action in actions:
            act = action.get("action")
            if not act:
                continue
            if act in ACTION_ALIASES:
                action["action"] = ACTION_ALIASES[act]
        
        # Sort actions
        def action_sort_key(action: dict[str, Any]) -> tuple[int, int]:
            act = action.get("action", "")
            handle = action.get("handle", "")

            if act in {"engulf_leaf_into_gem_for_editing", "engulf_shell_in_gemstorm"}:
                category = 0
            elif act in {"preserve_leaf_in_ether_untouched", "preserve_branch_node_in_ether_untouched__when_no_engulfed_children"}:
                category = 1
            elif act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                category = 3
            elif act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                category = 4
            else:
                category = 2

            depth = -handle.count(".")
            return (category, depth)

        actions.sort(key=action_sort_key)

        # Remember original order for reporting
        for idx, a in enumerate(actions):
            if "__action_index" not in a:
                a["__action_index"] = idx + 1
        
        # Use precomputed frontier
        session["handles"] = handles
        session["state"] = state
        if _precomputed_frontier is not None:
            current_step_frontier = _precomputed_frontier
        else:
            current_step_frontier = self._compute_exploration_frontier(
                session, frontier_size=frontier_size, max_depth_per_frontier=max_depth_per_frontier
            )
        
        peek_results = []

        # ---
        # ACTION ORDERING GUARDRAIL (Dan): Allow MSP/MSM/MSD + LF in the SAME call.
        # We must ensure skeleton decisions apply to the *previous* lightning frontier
        # before any new LF clears/overwrites peek state.
        #
        # Phase 1: apply skeleton mark actions (MSD/MSM/MSP) first
        # Phase 2: apply all remaining actions in their existing (sorted) order
        # ---
        skeleton_mark_actions = {
            "mark_section_for_deletion",
            "mark_section_as_merge_target",
            "mark_section_as_permanent",
        }
        phase1 = [a for a in actions if a.get("action") in skeleton_mark_actions]
        phase2 = [a for a in actions if a.get("action") not in skeleton_mark_actions]
        actions = phase1 + phase2
        
        # Process each action
        # Default behavior: if caller provides a freeform "note" on an action, capture it in scratchpad.
        # Exception: actions where "note" is the payload for updating the node note field.
        NOTE_TO_SCRATCHPAD_EXCLUSIONS = {
            "update_node_and_engulf_in_gemstorm",
            "update_note_and_engulf_in_gemstorm",
            "update_leaf_node_and_engulf_in_gemstorm",
            "update_branch_node_and_engulf_in_gemstorm__descendants_unaffected",
            "update_branch_note_and_engulf_in_gemstorm__descendants_unaffected",
        }

        for action in actions:
            act = action.get("action")
            i = int(action.get("__action_index") or 0) or (len(action_reports) + 1)

            # Auto-capture freeform action notes (unless excluded).
            # We capture:
            # - note
            # - *_note (e.g., salvage_note)
            # - note_* (e.g., note_salvage)
            #
            # This is crucial during live runs: notes become part of the scratchpad minitree.
            if act not in NOTE_TO_SCRATCHPAD_EXCLUSIONS:
                handle_for_note = action.get("handle")

                def _capture_note_field(field: str, val: Any) -> None:
                    if not isinstance(val, str) or not val.strip():
                        return
                    text = val.strip()
                    label = field
                    # Use a single-line prefix for quick scan; note body remains as provided.
                    # We keep the full text as the entry note to preserve multi-line content.
                    if field != "note":
                        text = f"[{field}] {text}"
                    _append_scratchpad_entry(
                        session,
                        {
                            "note": text,
                            "handle": handle_for_note,
                            "origin": "action_note",
                            "action": act,
                            "i": i,
                            "field": label,
                            # scratch_id assigned by _append_scratchpad_entry
                        },
                    )

                for k, v in action.items():
                    if k == "note" or k.endswith("_note") or k.startswith("note_"):
                        _capture_note_field(k, v)

            # If any earlier action failed, we still allow "safe" actions to run,
            # but we SKIP context-dependent actions that would compute or depend on
            # a new visibility slice (LF/PD/SX/RF + skeleton marks + EF/PF).
            # This prevents invisible state changes when we return an error with no frontier.
            #
            # Also avoid double-reporting the SAME step as both failed and skipped.
            if any_failures:
                # If this step already has a report entry, do not add another.
                if any(r.get("i") == i for r in action_reports):
                    continue

                context_dependent = {
                    "peek_descendants_as_frontier",
                    "lightning_flash_into_section",
                    "search_descendants_for_text",
                    "resume_guided_frontier",
                    "mark_section_for_deletion",
                    "mark_section_as_merge_target",
                    "mark_section_as_permanent",
                    "engulf_all_showing_undecided_descendants_into_gem_for_editing",
                    "preserve_all_showing_undecided_descendants_in_ether",
                }
                if act in context_dependent:
                    _report_skipped(i, act, action.get("handle"), "Skipped because a prior action failed (no-frontier error mode)")
                    continue

            # Global actions (no handle required)
            if act in {"set_scratchpad", "append_scratchpad", "preserve_all_remaining_nodes_in_ether_at_finalization", "include_scratchpad_in_step_output", "scratchpad_complete_by_id"}:
                if act == "preserve_all_remaining_nodes_in_ether_at_finalization":
                    if exploration_mode != "dfs_guided_bulk":
                        _report_fail(i, act, None, "preserve_all only in bulk mode")
                        continue

                    strict = session.get("strict_completeness", False)
                    if strict:
                        _report_fail(i, act, None, "PA disabled in strict_completeness mode")
                        continue

                    preserved_count = 0
                    for h in handles:
                        h_state = state.get(h, {})
                        h_status = h_state.get("status")
                        if h_status in ("unseen", "candidate", "open", None):
                            h_summary = _summarize_descendants(h)
                            if h_summary["accepted_leaf_count"] > 0:
                                f_entry = state.setdefault(h, {"status": "unseen", "selection_type": None})
                                if f_entry.get("status") not in {"finalized", "closed"}:
                                    f_entry["status"] = "finalized"
                                    f_entry["selection_type"] = "path"
                            else:
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    logger.info(f"preserve_all: {preserved_count} preserved")
                    _report_ok(i, act, None)
                    continue

                if act in {"set_scratchpad", "append_scratchpad"}:
                    # Strict scratchpad semantics (Dan): do NOT allow implicit clearing.
                    # Require either 'note' or 'content' (content accepted as alias) and treat
                    # missing/empty payload as a step failure with full per-action reporting.

                    allowed_keys = {"action", "note", "content", "handle"}
                    extra_keys = (set(action.keys()) - allowed_keys) - {"__action_index"}
                    if extra_keys:
                        _report_fail(
                            i,
                            act,
                            None,
                            f"{act} has unexpected keys: {sorted(extra_keys)}. Allowed: note|content (+ optional handle)",
                        )
                        continue

                    raw_note = action.get("note")
                    raw_content = action.get("content")

                    payload = None
                    if isinstance(raw_note, str) and raw_note.strip():
                        payload = raw_note.strip()
                    elif isinstance(raw_content, str) and raw_content.strip():
                        payload = raw_content.strip()

                    if not payload:
                        _report_fail(
                            i,
                            act,
                            None,
                            f"{act} requires non-empty 'note' or 'content' (clearing scratchpad is not allowed)",
                        )
                        continue

                    if act == "set_scratchpad":
                        _report_fail(
                            i,
                            act,
                            None,
                            "set_scratchpad is disabled (do not clear scratchpad). Use append_scratchpad.",
                            recommended_action_instead="AS (append_scratchpad)",
                        )
                        continue

                    # append_scratchpad: store as normalized entry with note + optional handle
                    entry: dict[str, Any] = {"note": payload}
                    h = action.get("handle")
                    if isinstance(h, str) and h.strip():
                        entry["handle"] = h.strip()

                    _append_scratchpad_entry(session, entry)
                    _report_ok(i, act, None)
                    continue

                if act == "include_scratchpad_in_step_output":
                    # SP (Scratchpad Preview) action.
                    # Default: include full scratchpad_preview in the step output.
                    # Optional filtering:
                    #   - handle: restrict to scratchpad entries tied to handle
                    #   - include_ancestors: include ancestors of handle
                    #   - include_descendants: include descendants of handle
                    # Multiple SP actions in one call union their filters.

                    allowed_keys = {"action", "handle", "include_ancestors", "include_descendants"}
                    extra_keys = (set(action.keys()) - allowed_keys) - {"__action_index"}
                    if extra_keys:
                        _report_fail(i, act, None, f"SP has unexpected keys: {sorted(extra_keys)}")
                        continue

                    h = action.get("handle")
                    if h is not None and (not isinstance(h, str) or not h.strip()):
                        _report_fail(i, act, None, "SP handle must be a non-empty string when provided")
                        continue

                    if h is not None and h not in handles:
                        _report_fail(i, act, h, f"SP unknown handle: {h}")
                        continue

                    include_anc = bool(action.get("include_ancestors", False))
                    include_desc = bool(action.get("include_descendants", False))

                    # Ensure preview is included
                    session["_include_scratchpad_preview_next"] = True

                    # Accumulate filters
                    if h is not None:
                        filt = session.get("_sp_filter")
                        if not isinstance(filt, dict):
                            filt = {"handles": [], "include_ancestors": False, "include_descendants": False}
                        handles_list = filt.get("handles")
                        if not isinstance(handles_list, list):
                            handles_list = []
                        if h not in handles_list:
                            handles_list.append(h)
                        filt["handles"] = handles_list
                        # OR semantics across SP actions
                        filt["include_ancestors"] = bool(filt.get("include_ancestors")) or include_anc
                        filt["include_descendants"] = bool(filt.get("include_descendants")) or include_desc
                        session["_sp_filter"] = filt

                    # NOTE: (Dec 2025) SP debug instrumentation removed after stabilizing LF+SP filtering.
                    # If needed again, re-add as temporary log_event() calls only (avoid scratchpad spam).

                    _report_ok(i, act, h)
                    continue

                if act == "scratchpad_complete_by_id":
                    # Mark an existing scratchpad entry as done (does not delete it).
                    # Required param: scratch_id (e.g., "SP-000123")
                    raw_id = action.get("scratch_id")
                    if not isinstance(raw_id, str) or not raw_id.strip():
                        _report_fail(i, act, None, "scratchpad_complete_by_id requires scratch_id")
                        continue
                    scratch_id = raw_id.strip()

                    scratch_entries = _normalize_scratchpad(session)
                    matched = False
                    for entry_sp in scratch_entries:
                        if not isinstance(entry_sp, dict):
                            continue
                        if entry_sp.get("scratch_id") == scratch_id:
                            entry_sp["done"] = True
                            entry_sp["done_at"] = datetime.utcnow().isoformat() + "Z"
                            matched = True
                            break

                    if not matched:
                        _report_fail(i, act, None, f"scratch_id not found: {scratch_id}")
                        continue

                    session["scratchpad"] = scratch_entries
                    _report_ok(i, act, None)
                    continue

            # Handle-optional global actions (process before handle validation)
            if act == "abandon_lightning_strike":
                target_root = action.get("handle")
                if target_root and session.get("_peek_frontier"):
                    # Per-root abandon: clear lightning strike ONLY for the specified root
                    peek_roots = session.get("_peek_root_handles") or []
                    if target_root not in peek_roots:
                        _report_fail(
                            i,
                            act,
                            target_root,
                            f"abandon_lightning_strike with handle '{target_root}' requires an active lightning root",
                        )
                        continue

                    current_frontier = session.get("_peek_frontier") or []
                    remaining_frontier = [
                        e
                        for e in current_frontier
                        if e.get("handle") != target_root
                        and not str(e.get("handle", "")).startswith(target_root + ".")
                    ]
                    if remaining_frontier:
                        session["_peek_frontier"] = remaining_frontier
                    else:
                        session.pop("_peek_frontier", None)

                    new_roots = [h for h in peek_roots if h != target_root]
                    if new_roots:
                        session["_peek_root_handles"] = new_roots
                    else:
                        session.pop("_peek_root_handles", None)

                    max_map = session.get("_peek_max_nodes_by_root") or {}
                    if target_root in max_map:
                        max_map.pop(target_root, None)
                        if max_map:
                            session["_peek_max_nodes_by_root"] = max_map
                        else:
                            session.pop("_peek_max_nodes_by_root", None)

                    logger.info(
                        f"abandon_lightning_strike: cleared lightning strike for root {target_root}"
                    )
                    _report_ok(i, act, target_root)
                else:
                    # Global abandon: clear ALL lightning state
                    for key in (
                        "_peek_frontier",
                        "_peek_root_handles",
                        "_peek_max_nodes_by_root",
                    ):
                        session.pop(key, None)
                    logger.info("abandon_lightning_strike: cleared all lightning peek state")
                    _report_ok(i, act, None)
                continue

            if act == "resume_guided_frontier":
                if "_search_frontier" in session:
                    del session["_search_frontier"]
                if "_peek_frontier" in session:
                    for key in (
                        "_peek_frontier",
                        "_peek_root_handles",
                        "_peek_max_nodes_by_root",
                    ):
                        session.pop(key, None)
                    logger.info("resume: cleared peek")
                _report_ok(i, act, action.get("handle"))
                continue

            handle = action.get("handle")
            max_depth = action.get("max_depth")

            # Bulk frontier actions (EF/PF) - apply to the CURRENT STEP FRONTIER (or SEARCH frontier)
            if act in {"engulf_all_showing_undecided_descendants_into_gem_for_editing", "preserve_all_showing_undecided_descendants_in_ether"}:
                if exploration_mode != "dfs_guided_bulk":
                    _report_fail(i, act, handle, f"{act} only in bulk mode")
                    continue

                if handle not in handles:
                    _report_fail(i, act, handle, f"Unknown handle: {handle}")
                    continue

                # Special-case UX (Dan): EF/PF targeted at a LEAF should still "just work".
                # In that situation, EF behaves equivalently to EL (engulf leaf) and PF behaves
                # equivalently to PL (preserve leaf). We emit an explicit warning so callers can
                # see the coercion in the returned JSON.
                target_children = (handles.get(handle, {}) or {}).get("children", []) or []
                is_leaf_target = (len(target_children) == 0)
                if is_leaf_target:
                    st_here = state.get(handle, {"status": "unseen"})
                    if st_here.get("status") in {"finalized", "closed"}:
                        # Already decided: treat as ok (nothing to do), but still warn for clarity.
                        _report_warn(
                            i,
                            act,
                            handle,
                            f"{act} targeted a leaf; node already decided (no-op). This action is equivalent to {'EL' if act == 'engulf_all_showing_undecided_descendants_into_gem_for_editing' else 'PL'} for leaves.",
                        )
                        continue

                    if act == "engulf_all_showing_undecided_descendants_into_gem_for_editing":
                        entry_leaf = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                        entry_leaf["status"] = "finalized"
                        entry_leaf["selection_type"] = "leaf"
                        entry_leaf["max_depth"] = max_depth
                        _auto_complete_ancestors(handle)
                        _report_warn(i, act, handle, "EF targeted a leaf; coerced to EL semantics (engulf leaf).")
                        continue

                    if act == "preserve_all_showing_undecided_descendants_in_ether":
                        entry_leaf = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                        entry_leaf["status"] = "closed"
                        entry_leaf["selection_type"] = None
                        entry_leaf["max_depth"] = None
                        _auto_complete_ancestors(handle)
                        _report_warn(i, act, handle, "PF targeted a leaf; coerced to PL semantics (preserve leaf).")
                        continue

                def _is_desc(fh: str, branch: str) -> bool:
                    cur = fh
                    seen = set()
                    while cur and cur not in seen:
                        if cur == branch:
                            return True
                        seen.add(cur)
                        cur = handles.get(cur, {}).get("parent")
                    return False

                matching = [e for e in current_step_frontier if _is_desc(e["handle"], handle)]

                if not matching:
                    logger.info(f"Bulk on '{handle}': no matches")
                    _report_ok(i, act, handle)
                    continue

                if act == "engulf_all_showing_undecided_descendants_into_gem_for_editing":
                    engulfed_leaves = 0
                    engulfed_branches = 0
                    for entry in matching:
                        fh = entry["handle"]
                        if state.get(fh, {}).get("status") in {"finalized", "closed"}:
                            continue
                        if entry.get("is_leaf"):
                            f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "leaf"
                            f_entry["max_depth"] = max_depth
                            _auto_complete_ancestors(fh)
                            engulfed_leaves += 1
                        else:
                            f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                            f_entry["status"] = "finalized"
                            f_entry["selection_type"] = "subtree"
                            f_entry["max_depth"] = max_depth
                            f_entry["subtree_mode"] = "shell"
                            engulfed_branches += 1
                    logger.info(f"Engulfed {engulfed_leaves} leaves, {engulfed_branches} branches")
                elif act == "preserve_all_showing_undecided_descendants_in_ether":
                    preserved = 0
                    for entry in matching:
                        fh = entry["handle"]
                        if state.get(fh, {}).get("status") in {"finalized", "closed"}:
                            continue
                        f_entry = state.setdefault(fh, {"status": "unseen", "selection_type": None})
                        f_entry["status"] = "closed"
                        _auto_complete_ancestors(fh)
                        preserved += 1
                    logger.info(f"Preserved {preserved} nodes")

                _report_ok(i, act, handle)
                continue

            if handle not in handles:
                _report_fail(i, act, handle, f"Unknown handle: {handle}")
                continue

            if act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm", "update_leaf_node_and_engulf_in_gemstorm", "update_branch_node_and_engulf_in_gemstorm__descendants_unaffected", "update_branch_note_and_engulf_in_gemstorm__descendants_unaffected", "update_tag_and_engulf_in_gemstorm"} and not editable_mode:
                _report_fail(
                    i,
                    act,
                    handle,
                    "Update actions require editable=True",
                )
                continue

            if handle not in state:
                state[handle] = {"status": "unseen", "max_depth": None, "selection_type": None}

            entry = state[handle]
            if "selection_type" not in entry:
                entry["selection_type"] = None

            # PEEK / LIGHTNING FLASH action
            if act in {"peek_descendants_as_frontier", "lightning_flash_into_section"}:
                from collections import deque

                # Distinct defaults for PEEK vs LIGHTNING
                # Accept 'budget' as alias for max_nodes (Dan usability)
                if act == "lightning_flash_into_section":
                    max_nodes = action.get("max_nodes") or action.get("budget") or 15
                else:
                    max_nodes = action.get("max_nodes") or action.get("budget") or 200

                # Guardrail: reject unexpected keys for LF/PEEK to avoid silent no-ops.
                # (We intentionally keep this narrow: only validate when the action is LF/PD.)
                allowed_keys = {
                    "action",
                    "handle",
                    "max_nodes",
                    "budget",  # alias
                    "max_depth",
                    "show_decided_descendants",
                }
                # Internal instrumentation keys should not trip strict LF/PEEK validation.
                extra_keys = (set(action.keys()) - allowed_keys) - {"__action_index"}
                if extra_keys:
                    _report_fail(
                        i,
                        act,
                        handle,
                        f"Unexpected keys for {act} action: {sorted(extra_keys)}",
                    )
                    continue
                try:
                    max_nodes = int(max_nodes)
                except (TypeError, ValueError):
                    max_nodes = 15 if act == "lightning_flash_into_section" else 200
                if max_nodes <= 0:
                    max_nodes = 15 if act == "lightning_flash_into_section" else 200

                # If this is the first peek in this step, clear any previous multi-peek state
                if not peek_results:
                    for key in (
                        "_peek_frontier",
                        "_peek_root_handles",
                        "_peek_max_nodes_by_root",
                    ):
                        session.pop(key, None)
                
                queue = deque([handle])
                visited = set()
                peek_handles = []
                
                while queue and len(peek_handles) < max_nodes:
                    h = queue.popleft()
                    if h in visited:
                        continue
                    visited.add(h)
                    peek_handles.append(h)
                    for ch in handles.get(h, {}).get("children", []) or []:
                        if ch not in visited:
                            queue.append(ch)

                # Hard constraint: only keep the requested section handle and its descendants
                root_handle = handle
                constrained_peek_handles: list[str] = []
                for h in peek_handles:
                    if h == root_handle or h.startswith(root_handle + "."):
                        constrained_peek_handles.append(h)
                peek_handles = constrained_peek_handles

                # Determine if this strike exceeded the node limit (for LIGHTNING only)
                over_limit = (
                    act == "lightning_flash_into_section"
                    and len(peek_handles) >= max_nodes
                    and queue  # still nodes left to visit
                )

                # Decided-descendant visibility is a REQUIRED choice for LF.
                # Rationale (Dan): there is no universally sensible default; forcing explicitness
                # prevents accidental acting-without-seeing.
                show_decided_descendants = action.get("show_decided_descendants")
                if show_decided_descendants is None:
                    _report_fail(
                        i,
                        act,
                        handle,
                        "LF requires explicit show_decided_descendants: true|false",
                    )
                    continue
                show_decided_descendants = bool(show_decided_descendants)

                # Usability rule (Dan): If NOT STRUCT-mode, and we have a complete set of leaf siblings
                # under a parent, also include ALL sibling BRANCHES (no limit for now).
                if act == "lightning_flash_into_section" and not over_limit:
                    peek_set = set(peek_handles)

                    # Find parents for which at least one leaf child is currently included.
                    parents_with_leaf: set[str] = set()
                    for h in peek_handles:
                        meta_h = handles.get(h, {}) or {}
                        child_h = meta_h.get("children", []) or []
                        is_leaf_h = not child_h
                        if not is_leaf_h:
                            continue
                        p = meta_h.get("parent")
                        if isinstance(p, str):
                            parents_with_leaf.add(p)

                    # For those parents, include any sibling branches.
                    for p in parents_with_leaf:
                        for sib in (handles.get(p, {}) or {}).get("children", []) or []:
                            if sib in peek_set:
                                continue
                            sib_children = (handles.get(sib, {}) or {}).get("children", []) or []
                            if not sib_children:
                                continue  # only add BRANCH siblings
                            # Ensure sibling is still within the lightning root subtree.
                            if sib == root_handle or sib.startswith(root_handle + "."):
                                peek_set.add(sib)

                    # Deterministic natural-handle sort
                    def _natural_key_handle(h: str) -> list[tuple[int, object]]:
                        parts = h.split(".")
                        result: list[tuple[int, object]] = []
                        for part in parts:
                            if part.isdigit():
                                result.append((0, int(part)))
                            else:
                                result.append((1, part))
                        return result

                    peek_handles = sorted(peek_set, key=_natural_key_handle)

                peek_frontier = []
                MAX_NOTE = None if editable_mode else 1024

                # If over limit for LIGHTNING, build a structure-only preview
                if over_limit and act == "lightning_flash_into_section":
                    # Build single-child spine from root_handle down
                    spine: list[str] = [root_handle]
                    cur = root_handle
                    while True:
                        children = (handles.get(cur, {}) or {}).get("children", []) or []
                        if len(children) != 1:
                            break
                        next_h = children[0]
                        if len(spine) + 1 > max_nodes:
                            break
                        spine.append(next_h)
                        cur = next_h
                    fan_root = cur
                    fan_children = (handles.get(fan_root, {}) or {}).get("children", []) or []

                    if not show_decided_descendants:
                        fan_children = [
                            ch for ch in fan_children
                            if (state.get(ch, {"status": "unseen"}).get("status") not in {"finalized", "closed"})
                        ]

                    # Nodes to include: entire spine + as many children of fan_root as fit
                    nodes_to_include: list[str] = list(dict.fromkeys(spine))  # preserve order
                    remaining = max_nodes - len(nodes_to_include)
                    if remaining > 0:
                        for ch in fan_children:
                            if remaining <= 0:
                                break
                            nodes_to_include.append(ch)
                            remaining -= 1

                    # STRUCT usability: include the ancestor chain from root (R) down to the lightning root_handle
                    # so the preview has full path context like normal frontiers.
                    ancestor_chain: list[str] = []
                    cur = root_handle
                    seen_chain: set[str] = set()
                    while cur and cur not in seen_chain and cur != "R":
                        seen_chain.add(cur)
                        ancestor_chain.append(cur)
                        cur = (handles.get(cur, {}) or {}).get("parent")
                    ancestor_chain.append("R")
                    ancestor_chain.reverse()

                    nodes_to_include = list(dict.fromkeys(ancestor_chain + nodes_to_include))

                    seen_local: set[str] = set()
                    for h in nodes_to_include:
                        if h in seen_local:
                            continue
                        seen_local.add(h)
                        meta = handles.get(h, {}) or {}
                        st = state.get(h, {"status": "unseen"})
                        child_handles = meta.get("children", []) or []
                        is_leaf = not child_handles
                        note_full = meta.get("note") or ""
                        note_preview = note_full if MAX_NOTE is None else (
                            note_full if len(note_full) <= MAX_NOTE else note_full[:MAX_NOTE]
                        )
                        guidance = (
                        "PEEK - leaf: EL (IN GEM; editable, may be deleted in ETHER), "
                        "PL (ETHER only; NOT in GEM)"
                    ) if is_leaf else (
                        "PEEK - branch: RB (IN GEM shell), PB (ETHER only), EF (IN GEM), PF (ETHER only)"
                    )

                        # Build skeleton tags for this lightning strike
                        skeleton_tags: list[str] = [f"STRUCT:{root_handle}", f"LS:{root_handle}"]
                        if is_leaf:
                            parent_handle = meta.get("parent")
                            if isinstance(parent_handle, str):
                                siblings = (handles.get(parent_handle, {}) or {}).get("children", []) or []
                                try:
                                    idx = siblings.index(h)
                                    total = len(siblings)
                                    # 1-based leaf position among siblings
                                    skeleton_tags.append(f"LEAFSIB:{idx + 1}/{total}")
                                except ValueError:
                                    # If handle not found among siblings (shouldn't happen), skip LEAFSIB
                                    pass

                        entry = {
                            "handle": h,
                            "parent_handle": meta.get("parent"),
                            "name_preview": meta.get("name", ""),
                            "note_preview": note_preview,
                            "child_count": len(child_handles),
                            "depth": meta.get("depth", 0),
                            "status": st.get("status", "candidate"),
                            "is_leaf": is_leaf,
                            "guidance": guidance,
                            "skeleton_tmp": skeleton_tags,
                        }
                        peek_frontier.append(entry)
                else:
                    # Normal PEEK / LIGHTNING frontier
                    # Usability: include full ancestor path from R -> root_handle for context.
                    ancestor_chain: list[str] = []
                    cur = root_handle
                    seen_chain: set[str] = set()
                    while cur and cur not in seen_chain and cur != "R":
                        seen_chain.add(cur)
                        ancestor_chain.append(cur)
                        cur = (handles.get(cur, {}) or {}).get("parent")
                    ancestor_chain.append("R")
                    ancestor_chain.reverse()

                    ordered_handles = list(dict.fromkeys(ancestor_chain + list(peek_handles)))

                    for h in ordered_handles:
                        meta = handles.get(h, {}) or {}
                        st = state.get(h, {"status": "unseen"})
                        child_handles = meta.get("children", []) or []
                        is_leaf = not child_handles
                        note_full = meta.get("note") or ""
                        note_preview = note_full if MAX_NOTE is None else (
                            note_full if len(note_full) <= MAX_NOTE else note_full[:MAX_NOTE]
                        )

                        # Context ancestors should always display as branches.
                        is_context_ancestor = h in ancestor_chain and h != root_handle
                        if is_context_ancestor:
                            is_leaf = False

                        guidance = (
                        "PEEK - leaf: EL (IN GEM; editable, may be deleted in ETHER), "
                        "PL (ETHER only; NOT in GEM)"
                    ) if is_leaf else (
                        "PEEK - branch: RB (IN GEM shell), PB (ETHER only), EF (IN GEM), PF (ETHER only)"
                    )

                        entry = {
                            "handle": h,
                            "parent_handle": meta.get("parent"),
                            "name_preview": meta.get("name", ""),
                            "note_preview": note_preview,
                            "child_count": len(child_handles),
                            "depth": meta.get("depth", 0),
                            "status": st.get("status", "candidate"),
                            "is_leaf": is_leaf,
                            "is_pseudo_leaf": False,
                            "guidance": guidance,
                        }
                        if act == "lightning_flash_into_section":
                            entry["skeleton_tmp"] = [f"LS:{root_handle}"]
                        peek_frontier.append(entry)

                # Merge this root's frontier into aggregated multi-peek frontier
                existing_frontier = session.get("_peek_frontier") or []
                combined_by_handle: dict[str, dict[str, Any]] = {}
                for e in existing_frontier:
                    h_existing = e.get("handle")
                    if h_existing:
                        combined_by_handle[h_existing] = e
                for e in peek_frontier:
                    h_new = e.get("handle")
                    if not h_new:
                        continue
                    existing = combined_by_handle.get(h_new)
                    if existing is not None:
                        # Merge skeleton_tmp annotations if both exist
                        sk_old = existing.get("skeleton_tmp")
                        sk_new = e.get("skeleton_tmp")
                        merged: list[str] = []
                        def _to_list(val: Any) -> list[str]:
                            if not val:
                                return []
                            if isinstance(val, str):
                                return [val]
                            if isinstance(val, list):
                                return [v for v in val if isinstance(v, str)]
                            return []
                        merged = _to_list(sk_old) + _to_list(sk_new)
                        if merged:
                            existing["skeleton_tmp"] = list(dict.fromkeys(merged))
                        combined_by_handle[h_new] = existing
                    else:
                        combined_by_handle[h_new] = e
                session["_peek_frontier"] = list(combined_by_handle.values())

                # Track root handles + per-root max_nodes for LIGHTNING.
                if act == "lightning_flash_into_section":
                    root_handles = set(session.get("_peek_root_handles") or [])
                    root_handles.add(handle)
                    session["_peek_root_handles"] = sorted(root_handles)

                    max_map = session.get("_peek_max_nodes_by_root") or {}
                    max_map[handle] = max_nodes
                    session["_peek_max_nodes_by_root"] = max_map

                    over_map = session.get("_peek_over_limit_by_root") or {}
                    over_map[handle] = bool(over_limit)
                    session["_peek_over_limit_by_root"] = over_map

                    # Fresh-start semantics: if this LF root was STRUCT-truncated, do NOT allow
                    # the lightning state to persist into subsequent steps (agents must re-LF deeper).
                    if bool(over_limit):
                        session["_peek_ephemeral_only"] = True

                peek_results.append({
                    "root_handle": handle,
                    "max_nodes": max_nodes,
                    "nodes_returned": len(peek_frontier),
                    "truncated": len(visited) >= max_nodes,
                    "over_limit": bool(over_limit),
                    "show_decided_descendants": bool(show_decided_descendants),
                    "frontier": peek_frontier,
                })

                _report_ok(i, act, handle)
                continue

            # ADD_HINT action
            if act == "add_hint":
                hint = action.get("hint")
                if not isinstance(hint, str) or not hint.strip():
                    _report_fail(i, act, handle, "add_hint requires non-empty hint")
                    continue
                meta = handles.get(handle) or {}
                existing = meta.get("hints")
                if not isinstance(existing, list):
                    existing = []
                existing.append(hint)
                meta["hints"] = existing
                handles[handle] = meta
                _report_ok(i, act, handle)
                continue

            # SKELETON WALK: section-level delete/merge operations over lightning strike
            if act in {
                "mark_section_for_deletion",
                "mark_section_as_merge_target",
                "mark_section_as_permanent",
            }:
                child_handles_tmp = (handles.get(handle, {}) or {}).get("children", []) or []
                is_leaf_tmp = not child_handles_tmp

                # Special-case (Dan): MSM on a LEAF is often the agent's intent to "take" the leaf.
                # Treat as EL with a warning instead of hard-failing.
                if act == "mark_section_as_merge_target" and is_leaf_tmp:
                    # Apply EL semantics
                    entry_leaf = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                    entry_leaf["status"] = "finalized"
                    entry_leaf["selection_type"] = "leaf"
                    entry_leaf["max_depth"] = max_depth
                    _auto_complete_ancestors(handle)
                    _report_warn(
                        i,
                        act,
                        handle,
                        "MSM requires active lightning on a branch root; target is a leaf. Coerced MSM → EL (engulf leaf).",
                    )
                    continue

                # Guardrail: MSD/MSP on a leaf should NOT suggest lightning on the leaf.
                if act in {"mark_section_for_deletion", "mark_section_as_permanent"} and is_leaf_tmp:
                    _report_fail(
                        i,
                        act,
                        handle,
                        f"{act} requires a lightning-rooted BRANCH handle; got leaf '{handle}'.",
                        recommended_action_instead="EL (engulf_leaf_into_gem_for_editing)",
                        note="If you intended the parent branch, target the parent (e.g., A.7) and LF that branch, then apply MSD/MSP to the branch root.",
                    )
                    continue

                peek_frontier = session.get("_peek_frontier")
                if not peek_frontier:
                    _report_fail(
                        i,
                        act,
                        handle,
                        f"{act} requires active lightning strike rooted at handle '{handle}'",
                        recommended_action_instead="EL (engulf_leaf_into_gem_for_editing)" if act == "mark_section_as_merge_target" else None,
                    )
                    continue

                peek_roots = session.get("_peek_root_handles") or []
                if handle not in peek_roots:
                    _report_fail(
                        i,
                        act,
                        handle,
                        f"{act} requires active lightning strike rooted at handle '{handle}'",
                        recommended_action_instead="EL (engulf_leaf_into_gem_for_editing)" if act == "mark_section_as_merge_target" else None,
                        recommended_handle_instead=(peek_roots[0] if len(peek_roots) == 1 else None),
                    )
                    continue

                # STRUCT gate (Dan): if the strike for this root was STRUCT-truncated, do NOT allow
                # MSP/MSM/MSD decisions. User must LF deeper (or increase budget) to see real nodes.
                over_map = session.get("_peek_over_limit_by_root") or {}
                if bool(over_map.get(handle)):
                    _report_fail(
                        i,
                        act,
                        handle,
                        (
                            f"{act} is not allowed on STRUCT-mode lightning results for root '{handle}'. "
                            "LF deeper (choose a child) or increase budget first."
                        ),
                    )
                    continue

                peek_handles = {e.get("handle") for e in peek_frontier}

                # Optional freeform note provided by caller (very useful during live runs)
                skeleton_note = action.get("note")
                if isinstance(skeleton_note, str) and skeleton_note.strip():
                    _append_scratchpad_entry(
                        session,
                        {
                            "note": skeleton_note.strip(),
                            "handle": handle,
                            "origin": "skeleton_action_note",
                            "action": act,
                        },
                    )

                if act == "mark_section_for_deletion":
                    nodes_to_salvage = action.get("nodes_to_salvage_for_move") or []
                    invalid = [h for h in nodes_to_salvage if h not in peek_handles]
                    if invalid:
                        _report_fail(
                            i,
                            act,
                            handle,
                            f"nodes_to_salvage_for_move not in lightning frontier: {invalid}",
                        )
                        continue

                    # Salvage selected nodes for later move/merge
                    for h_salvage in nodes_to_salvage:
                        if h_salvage not in handles:
                            _report_fail(i, act, handle, f"Unknown salvage handle: {h_salvage}")
                            continue
                        sal_meta = handles.get(h_salvage) or {}
                        sal_children = sal_meta.get("children", []) or []
                        sal_entry = state.setdefault(
                            h_salvage,
                            {"status": "unseen", "selection_type": None},
                        )
                        if not sal_children:
                            sal_entry["status"] = "finalized"
                            sal_entry["selection_type"] = "leaf"
                            sal_entry["max_depth"] = max_depth
                        else:
                            sal_entry["status"] = "finalized"
                            sal_entry["selection_type"] = "subtree"
                            sal_entry["max_depth"] = max_depth
                            sal_entry.pop("subtree_mode", None)
                        _auto_complete_ancestors(h_salvage)

                    # Mark the root section handle as a subtree shell slated for deletion
                    root_entry = state.setdefault(
                        handle,
                        {"status": "unseen", "selection_type": None},
                    )
                    root_entry["status"] = "finalized"
                    root_entry["selection_type"] = "subtree"
                    root_entry["max_depth"] = max_depth
                    root_entry["subtree_mode"] = "shell"

                    # Hint to suppress BFS for this branch
                    root_meta = handles.get(handle) or {}
                    root_hints = root_meta.get("hints") or []
                    root_hints.append("SKELETON_PRUNED")
                    root_meta["hints"] = root_hints
                    handles[handle] = root_meta

                    # Scratchpad log
                    salvage_str = ", ".join(nodes_to_salvage) if nodes_to_salvage else ""
                    line = f"[SKELETON] mark_section_for_deletion: root={handle}, salvaged=[{salvage_str}]"
                    _append_scratchpad_entry(
                        session,
                        {
                            "note": line,
                            "handle": handle,
                            "origin": "skeleton_mark_section_for_deletion",
                            "nodes_to_salvage_for_move": nodes_to_salvage,
                        },
                    )

                else:
                    # mark_section_as_merge_target / mark_section_as_permanent share core semantics
                    target_entry = state.setdefault(
                        handle,
                        {"status": "unseen", "selection_type": None},
                    )
                    target_entry["status"] = "finalized"
                    target_entry["selection_type"] = "subtree"
                    target_entry["max_depth"] = max_depth
                    # Ensure this is a full editable subtree, not a shell
                    target_entry.pop("subtree_mode", None)

                    meta = handles.get(handle) or {}
                    hints = meta.get("hints") or []
                    if act == "mark_section_as_merge_target":
                        hints.append("SKELETON_MERGE_TARGET")
                        scratch_suffix = "merge target"
                    else:  # mark_section_as_permanent
                        # MSP semantics (Dan): "preserve this entire section unchanged and hide its descendants"
                        # Implementation:
                        # 1) Mark this root as a PRESERVED SUBTREE ROOT (closed + selection_type=subtree)
                        # 2) Mark ALL undecided descendants as closed as well
                        # 3) Add a pruning hint so frontier generation skips descendants by default
                        hints.append("SKELETON_PERMANENT_TARGET")
                        hints.append("SKELETON_PERMANENT_PRUNED")
                        scratch_suffix = "permanent target"

                        # Mark the root as a preserved subtree root
                        target_entry["status"] = "closed"
                        target_entry["selection_type"] = "subtree"
                        target_entry["max_depth"] = None

                        # Mark descendants as preserved (closed) so finalization can proceed smoothly.
                        # Only touch undecided nodes (avoid clobbering already-decided/finalized edits).
                        def _mark_descendants_closed(root_h: str) -> int:
                            preserved = 0
                            stack = list((handles.get(root_h, {}) or {}).get("children", []) or [])
                            seen: set[str] = set()
                            while stack:
                                ch = stack.pop()
                                if ch in seen:
                                    continue
                                seen.add(ch)

                                st_ch = state.get(ch)
                                status = (st_ch or {}).get("status") if isinstance(st_ch, dict) else None
                                if status in {None, "unseen", "candidate", "open"}:
                                    st_new = state.setdefault(ch, {"status": "unseen", "selection_type": None})
                                    st_new["status"] = "closed"
                                    st_new["selection_type"] = "subtree" if (handles.get(ch, {}) or {}).get("children") else None
                                    st_new["max_depth"] = None
                                    preserved += 1

                                stack.extend((handles.get(ch, {}) or {}).get("children", []) or [])
                            return preserved

                        preserved_desc_count = _mark_descendants_closed(handle)
                        _append_scratchpad_entry(
                            session,
                            {
                                "note": f"[SKELETON] MSP preserved+pruned descendants: root={handle}, descendants_closed={preserved_desc_count}",
                                "handle": handle,
                                "origin": "skeleton_mark_section_as_permanent",
                            },
                        )

                    meta["hints"] = hints
                    handles[handle] = meta

                    line = f"[SKELETON] {scratch_suffix} = {handle}"
                    origin = "skeleton_mark_section_as_merge_target" if act == "mark_section_as_merge_target" else "skeleton_mark_section_as_permanent"
                    _append_scratchpad_entry(
                        session,
                        {
                            "note": line,
                            "handle": handle,
                            "origin": origin,
                        },
                    )

                _report_ok(i, act, handle)

                # End lightning strike for THIS section only (support multiple active lightning roots)
                current_frontier = session.get("_peek_frontier") or []
                remaining_frontier = [
                    e for e in current_frontier
                    if e.get("handle") != handle
                    and not str(e.get("handle", "")).startswith(handle + ".")
                ]
                if remaining_frontier:
                    session["_peek_frontier"] = remaining_frontier
                else:
                    session.pop("_peek_frontier", None)

                root_handles = session.get("_peek_root_handles") or []
                if handle in root_handles:
                    new_roots = [h for h in root_handles if h != handle]
                    if new_roots:
                        session["_peek_root_handles"] = new_roots
                    else:
                        session.pop("_peek_root_handles", None)

                max_map = session.get("_peek_max_nodes_by_root") or {}
                if handle in max_map:
                    max_map.pop(handle, None)
                    if max_map:
                        session["_peek_max_nodes_by_root"] = max_map
                    else:
                        session.pop("_peek_max_nodes_by_root", None)

                continue

            # UNDO action: reopen previously decided handle back to undecided
            if act == "reopen_node_to_undecided":
                # NOTE: This is session-state-only undo. It does not affect Workflowy ETHER.
                # It is meant for recovering from accidental bulk decisions during long explorations.
                #
                # Default behavior:
                # - If the handle is finalized/closed, set it back to unseen.
                # - Clear decision metadata that can constrain later behavior.
                # - Clear skeleton pruning hints that hide descendants.
                # - Clear auto-completion artifacts on ancestors (best-effort).

                prior_status = entry.get("status")
                if prior_status not in {"finalized", "closed"}:
                    _report_warn(i, act, handle, f"UR is a no-op: node status is '{prior_status}' (already undecided)")
                    continue

                # Reopen this node
                entry["status"] = "unseen"
                entry["selection_type"] = None
                entry["max_depth"] = None
                entry.pop("subtree_mode", None)

                # Clear skeleton-related hints that can prevent the node/subtree from reappearing
                meta = handles.get(handle) or {}
                hints = meta.get("hints") or []
                if isinstance(hints, list) and hints:
                    blocked = {
                        "SKELETON_PRUNED",
                        "SKELETON_PERMANENT_PRUNED",
                        "SKELETON_MERGE_TARGET",
                        "SKELETON_PERMANENT_TARGET",
                    }
                    new_hints = [h for h in hints if h not in blocked]
                    if new_hints != hints:
                        meta["hints"] = new_hints
                        handles[handle] = meta

                # Best-effort: reopen ancestors that were auto-completed solely due to descendant decisions.
                # We only reset ancestors if they are in a decided state.
                cur = (handles.get(handle) or {}).get("parent")
                seen: set[str] = set()
                while isinstance(cur, str) and cur and cur not in seen and cur != "R":
                    seen.add(cur)
                    st_parent = state.get(cur) or {}
                    if st_parent.get("status") in {"finalized", "closed"}:
                        st_parent["status"] = "unseen"
                        st_parent["selection_type"] = None
                        st_parent["max_depth"] = None
                        st_parent.pop("subtree_mode", None)
                    cur = (handles.get(cur) or {}).get("parent")

                _report_ok(i, act, handle)
                continue

            # LEAF actions
            if act == "engulf_leaf_into_gem_for_editing":
                entry["status"] = "finalized"
                entry["selection_type"] = "leaf"
                entry["max_depth"] = max_depth
                _auto_complete_ancestors(handle)
                _report_ok(i, act, handle)
            elif act == "preserve_leaf_in_ether_untouched":
                entry["status"] = "closed"
                entry["selection_type"] = None
                entry["max_depth"] = None
                _auto_complete_ancestors(handle)
                _report_ok(i, act, handle)
            
            # BRANCH actions
            elif act == "flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states":
                summary = _summarize_descendants(handle)
                desc_count = summary["descendant_count"]
                has_undecided = summary["has_undecided"]
                accepted = summary["accepted_leaf_count"]

                if desc_count == 0:
                    _report_fail(i, act, handle, f"{handle} has no descendants - use leaf actions")
                    continue

                if exploration_mode == "dfs_guided_explicit":
                    if has_undecided or accepted > 0:
                        _report_fail(
                            i,
                            act,
                            handle,
                            f"Strict mode: cannot engulf branch '{handle}' with undecided/engulfed descendants",
                            recommended_action_instead="EL/PL on remaining leaves, then RB",
                        )
                        continue

                if exploration_mode == "dfs_guided_bulk":
                    entry["status"] = "finalized"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    entry["subtree_mode"] = "shell"
                    _report_ok(i, act, handle)
                    continue

                # All descendants decided: include as shell
                entry["status"] = "finalized"
                entry["selection_type"] = "subtree"
                entry["max_depth"] = max_depth
                if summary["has_decided"] and not summary["has_undecided"]:
                    if accepted > 0:
                        _auto_complete_ancestors(handle)
                        _report_ok(i, act, handle)
                        continue
                    entry["subtree_mode"] = "shell"
                else:
                    entry["subtree_mode"] = "shell"
                _auto_complete_ancestors(handle)
                _report_ok(i, act, handle)

            elif act == "preserve_branch_node_in_ether_untouched__when_no_engulfed_children":
                summary = _summarize_descendants(handle)
                if summary["descendant_count"] == 0:
                    _report_fail(i, act, handle, f"{handle} has no descendants - use leaf preserve")
                    continue

                if exploration_mode == "dfs_guided_explicit" and summary["has_undecided"]:
                    _report_fail(i, act, handle, "Strict mode: all descendants must be decided first")
                    continue

                if exploration_mode == "dfs_guided_bulk":
                    preserved_count = 0
                    for h in handles:
                        if not h.startswith(handle + "."):
                            continue
                        h_state = state.get(h, {})
                        if h_state.get("status") == "finalized":
                            continue
                        elif h_state.get("status") in ("unseen", "candidate", "open"):
                            h_sum = _summarize_descendants(h)
                            if h_sum["accepted_leaf_count"] == 0:
                                state.setdefault(h, {})["status"] = "closed"
                                state[h]["selection_type"] = "subtree"
                                preserved_count += 1
                    logger.info(f"Smart preserve: {preserved_count} nodes")
                    _report_ok(i, act, handle)
                else:
                    if summary["accepted_leaf_count"] > 0:
                        _report_fail(i, act, handle, f"Cannot preserve '{handle}' - has engulfed leaves")
                        continue
                    entry["status"] = "closed"
                    entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    _report_ok(i, act, handle)
                _auto_complete_ancestors(handle)

            # UPDATE actions (editable mode)
            elif act in {"update_node_and_engulf_in_gemstorm", "update_note_and_engulf_in_gemstorm", "update_leaf_node_and_engulf_in_gemstorm", "update_branch_node_and_engulf_in_gemstorm__descendants_unaffected", "update_branch_note_and_engulf_in_gemstorm__descendants_unaffected"}:
                if not editable_mode:
                    _report_fail(i, act, handle, "Update actions require editable=True")
                    continue
                new_name = action.get("name")
                new_note = action.get("note")
                meta = handles.get(handle) or {}
                node_id = meta.get("id")
                if not node_id:
                    _report_fail(i, act, handle, f"{handle} has no node id")
                    continue
                target = node_by_id.get(node_id)
                if not target:
                    _report_fail(i, act, handle, f"Node {node_id} not in tree")
                    continue
                if new_name:
                    target["name"] = new_name
                    meta["name"] = new_name
                if new_note:
                    target["note"] = new_note
                    meta["note"] = new_note
                handles[handle] = meta
                child_handles = handles.get(handle, {}).get("children", []) or []
                entry = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                if not child_handles:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors(handle)
                else:
                    entry["status"] = "finalized"
                    if entry.get("selection_type") is None:
                        entry["selection_type"] = "subtree"
                    entry["max_depth"] = max_depth
                    _auto_complete_ancestors(handle)
                _report_ok(i, act, handle)

            elif act == "update_tag_and_engulf_in_gemstorm":
                if not editable_mode:
                    _report_fail(i, act, handle, "Update tag requires editable=True")
                    continue
                raw_tag = action.get("tag")
                if not isinstance(raw_tag, str) or not raw_tag.strip():
                    _report_fail(i, act, handle, "Tag required")
                    continue
                tag = raw_tag.strip()
                if not tag.startswith("#"):
                    tag = f"#{tag}"
                meta = handles.get(handle) or {}
                node_id = meta.get("id")
                if not node_id:
                    _report_fail(i, act, handle, f"{handle} has no node id")
                    continue
                target = node_by_id.get(node_id)
                if not target:
                    _report_fail(i, act, handle, f"Node {node_id} not in tree")
                    continue
                current_name = target.get("name") or ""
                if tag not in current_name.split():
                    new_name = f"{current_name} {tag}".strip()
                    target["name"] = new_name
                    meta["name"] = new_name
                    handles[handle] = meta
                child_handles = handles.get(handle, {}).get("children", []) or []
                entry = state.setdefault(handle, {"status": "unseen", "selection_type": None})
                if not child_handles:
                    entry["status"] = "finalized"
                    entry["selection_type"] = "leaf"
                    _auto_complete_ancestors(handle)
                else:
                    entry["status"] = "finalized"
                    if not entry.get("selection_type"):
                        entry["selection_type"] = "subtree"
                    _auto_complete_ancestors(handle)
                _report_ok(i, act, handle)
                continue
            else:
                _report_fail(i, act, handle, f"Unsupported action: {act}")
                continue
        
        # If any failures occurred, we intentionally do NOT compute a new frontier in this call.
        # We also clear ephemeral peek/LF/search state to avoid "invisible flashes".
        if any_failures:
            for key in (
                "_peek_frontier",
                "_peek_root_handles",
                "_peek_max_nodes_by_root",
                "_peek_over_limit_by_root",
                "_peek_ephemeral_only",
                "_search_frontier",
            ):
                session.pop(key, None)

        session["handles"] = handles
        session["state"] = state
        session["steps"] = int(session.get("steps", 0)) + 1
        session["updated_at"] = datetime.utcnow().isoformat() + "Z"
        _normalize_scratchpad(session)

        try:
            with open(session_path, "w", encoding="utf-8") as f:
                json_module.dump(session, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise NetworkError(f"Failed to persist session: {e}") from e

        if any_failures:
            # Provide compact guidance: fix invalid steps, then rerun LF/SEARCH if desired.
            step_guidance = [
                "❌ One or more actions failed. No frontier returned.",
                "Fix the failed action(s) and re-run the step.",
                "Note: LF/peek/search state has been cleared to prevent acting-without-seeing.",
                UNDO_STEP_GUIDANCE_UR,
                SP_STEP_GUIDANCE,
                SC_STEP_GUIDANCE,
            ]
            # Human-readable output (primary) + structured details for inspection.
            # TypingMind renders tool outputs as JSON; embedding the human summary as multiline text
            # avoids painful "\\n" copy/paste workflows.
            error_summary_text = self._format_action_report_plain_text(action_reports)
            try:
                action_report_json = json_module.dumps(action_reports, indent=2, ensure_ascii=False)
            except Exception:
                action_report_json = json_module.dumps(action_reports)

            combined_text = (
                error_summary_text
                + "\n---\n"
                + "ACTION_REPORT (JSON)\n"
                + action_report_json
            )

            return {
                "success": False,
                "session_id": session_id,
                "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
                "action_key": EXPLORATION_ACTION_2LETTER,
                "error_summary": combined_text,
                "action_reports": action_reports,
                "step_guidance": step_guidance,
                "skipped_decisions": skipped_decisions,
            }

        result = {
            "success": True,
            "session_id": session_id,
            "action_key_primary_aliases": {"RB": "reserve_branch_for_children"},
            "action_key": EXPLORATION_ACTION_2LETTER,
            "scratchpad": session.get("scratchpad", []),
            "skipped_decisions": skipped_decisions,
        }
        if success_warnings:
            result["warnings"] = success_warnings
            result["action_reports"] = action_reports
        return result

    async def nexus_finalize_exploration(
        self,
        session_id: str,
        include_terrain: bool = True,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """Finalize exploration into NEXUS artifacts."""
        import json as json_module
        import copy

        logger = _ClientLogger()

        sessions_dir = self._get_explore_sessions_dir()
        session_path = os.path.join(sessions_dir, f"{session_id}.json")

        if not os.path.exists(session_path):
            raise NetworkError(f"Session '{session_id}' not found")

        try:
            with open(session_path, "r", encoding="utf-8") as f:
                session = json_module.load(f)
        except Exception as e:
            raise NetworkError(f"Failed to load session: {e}") from e

        nexus_tag = session.get("nexus_tag")
        if not nexus_tag:
            raise NetworkError(f"Session missing nexus_tag")

        handles = session.get("handles", {}) or {}
        state = session.get("state", {}) or {}
        root_node = session.get("root_node") or {}
        _normalize_scratchpad(session)

        # Build indexes
        node_by_id: dict[str, dict[str, Any]] = {}
        parent_by_id: dict[str, str | None] = {}
        children_by_id: dict[str, list[str]] = {}

        def index_tree(node: dict[str, Any], parent_id: str | None) -> None:
            nid = node.get("id")
            if nid:
                node_by_id[nid] = node
                parent_by_id[nid] = parent_id
                children_by_id.setdefault(nid, [])
            for child in node.get("children", []) or []:
                cid = child.get("id")
                if cid:
                    children_by_id.setdefault(nid, []).append(cid)
                index_tree(child, nid)

        index_tree(root_node, None)

        # Compute preserved IDs
        explicitly_preserved_ids: set[str] = set()
        preserved_subtree_roots: set[str] = set()

        for handle_key, st in state.items():
            if handle_key == "R":
                continue
            status = st.get("status")
            if status != "closed":
                continue
            meta = handles.get(handle_key) or {}
            node_id = meta.get("id")
            if not node_id:
                continue
            child_handles = handles.get(handle_key, {}).get("children", []) or []
            if child_handles and st.get("selection_type") == "subtree":
                preserved_subtree_roots.add(node_id)
            else:
                explicitly_preserved_ids.add(node_id)

        # Expand preserved subtrees
        for root_id in preserved_subtree_roots:
            if root_id not in node_by_id:
                continue
            stack = [root_id]
            while stack:
                cur = stack.pop()
                if cur in explicitly_preserved_ids:
                    continue
                explicitly_preserved_ids.add(cur)
                for child_id in children_by_id.get(cur, []):
                    stack.append(child_id)

        original_ids_seen: set[str] = set(node_by_id.keys())

        # Collect finalized entries
        finalized_entries = []
        for handle, st in state.items():
            if st.get("status") != "finalized":
                continue
            meta = handles.get(handle) or {}
            node_id = meta.get("id")
            if not node_id:
                continue
            selection_type = st.get("selection_type") or "subtree"
            max_depth = st.get("max_depth")
            finalized_entries.append((handle, node_id, selection_type, max_depth))

        # Build needed IDs from finalized entries (may be empty for PF-only runs)
        handle_by_node_id = {}
        for h, meta in handles.items():
            nid = meta.get("id")
            if nid:
                handle_by_node_id[nid] = h

        needed_ids: set[str] = set()
        subtree_shells: set[str] = set()
        true_subtrees: set[str] = set()

        # Capture SKELETON hints so GEM/JEWEL can distinguish delete vs merge targets
        skeleton_delete_ids: set[str] = set()
        skeleton_merge_ids: set[str] = set()
        skeleton_permanent_ids: set[str] = set()
        for handle_key, meta in handles.items():
            nid = (meta or {}).get("id")
            if not nid:
                continue
            hints = (meta or {}).get("hints") or []
            if "SKELETON_PRUNED" in hints:
                skeleton_delete_ids.add(nid)
            if "SKELETON_MERGE_TARGET" in hints:
                skeleton_merge_ids.add(nid)
            if "SKELETON_PERMANENT_TARGET" in hints:
                skeleton_permanent_ids.add(nid)

        for handle, node_id, sel_type, _max_depth in finalized_entries:
            if sel_type != "subtree":
                continue
            st = state.get(handle, {})
            if st.get("subtree_mode") == "shell":
                subtree_shells.add(node_id)
            else:
                true_subtrees.add(node_id)

        # Include full subtrees
        for node_id in true_subtrees:
            if node_id not in node_by_id:
                continue
            stack = [node_id]
            while stack:
                cur = stack.pop()
                if cur in needed_ids or cur in explicitly_preserved_ids:
                    continue
                needed_ids.add(cur)
                for child_id in children_by_id.get(cur, []):
                    stack.append(child_id)

        # Include shells
        for node_id in subtree_shells:
            if node_id in node_by_id:
                needed_ids.add(node_id)

        # Include accepted leaves + ancestors
        accepted_leaf_ids = set()
        for nid in node_by_id.keys():
            if children_by_id.get(nid):
                continue
            handle = handle_by_node_id.get(nid)
            if not handle:
                continue
            st = state.get(handle, {})
            if st.get("status") == "finalized":
                accepted_leaf_ids.add(nid)

        for leaf_id in accepted_leaf_ids:
            cur = leaf_id
            while cur is not None:
                if cur in needed_ids:
                    break
                needed_ids.add(cur)
                cur = parent_by_id.get(cur)

        # PF-only runs: allow degenerate GEM with no children by including just the exploration root
        root_explore_id = root_node.get("id")
        if not needed_ids and root_explore_id:
            needed_ids.add(root_explore_id)

        if not needed_ids:
            raise NetworkError("Empty minimal covering tree")

        # ============================================================================
        # COMPLETENESS CHECK: Ensure every handle is explicitly decided or covered
        # ============================================================================
        # Helper: summarize descendants for completeness validation
        def _summarize_descendants_for_handle(branch_handle: str) -> dict[str, Any]:
            """Summarize descendant decisions for completeness validation.
            
            Mirrors _summarize_descendants from _nexus_explore_step_internal but
            local to nexus_finalize_exploration for validation purposes.
            """
            queue: list[str] = list(handles.get(branch_handle, {}).get("children", []) or [])
            descendants: list[str] = []
            while queue:
                h = queue.pop(0)
                descendants.append(h)
                child_handles = handles.get(h, {}).get("children", []) or []
                if child_handles:
                    queue.extend(child_handles)

            if not descendants:
                return {
                    "descendant_count": 0,
                    "has_decided": False,
                    "has_undecided": False,
                    "accepted_leaf_count": 0,
                    "rejected_leaf_count": 0,
                }

            accepted_leaves = 0
            rejected_leaves = 0
            has_decided = False
            has_undecided = False

            for h in descendants:
                st = state.get(h, {"status": "unseen"})
                status = st.get("status")

                if status in {"finalized", "closed"}:
                    has_decided = True
                else:
                    has_undecided = True

                # Leaf = no children in the cached tree
                child_handles = handles.get(h, {}).get("children", []) or []
                is_leaf = not child_handles
                if not is_leaf:
                    continue

                if status == "finalized":
                    accepted_leaves += 1
                elif status == "closed":
                    rejected_leaves += 1

            return {
                "descendant_count": len(descendants),
                "has_decided": has_decided,
                "has_undecided": has_undecided,
                "accepted_leaf_count": accepted_leaves,
                "rejected_leaf_count": rejected_leaves,
            }

        # Validate completeness: every handle must be decided or covered
        uncovered_handles: list[str] = []

        for handle, meta in handles.items():
            # Skip synthetic root
            if handle == "R":
                continue

            st = state.get(handle, {"status": "unseen"})
            status = st.get("status")
            sel_type = st.get("selection_type")

            # Validate finalized/path elements (must anchor engulfed leaves)
            if status == "finalized" and sel_type == "path":
                summary = _summarize_descendants_for_handle(handle)
                if summary["accepted_leaf_count"] == 0:
                    # Bogus path element - treat as undecided
                    status = "unseen"
                else:
                    # Valid path element
                    continue

            # Normal decided cases
            if status in {"finalized", "closed"}:
                continue

            # Check ancestor chain for subtree coverage
            ancestor_handle = meta.get("parent")
            covered_by_subtree = False
            while ancestor_handle:
                anc_state = state.get(ancestor_handle, {})
                if (
                    anc_state.get("status") in {"finalized", "closed"}
                    and anc_state.get("selection_type") == "subtree"
                ):
                    covered_by_subtree = True
                    break
                ancestor_handle = handles.get(ancestor_handle, {}).get("parent")

            if not covered_by_subtree:
                uncovered_handles.append(handle)

        if uncovered_handles:
            # Build human-readable summary
            details_lines: list[str] = []
            for h in uncovered_handles[:50]:  # cap at 50 to avoid overwhelming output
                meta = handles.get(h, {})
                name = meta.get("name", "Untitled")
                parent_handle = meta.get("parent") or "<root>"
                details_lines.append(f"- {h} (name='{name}', parent='{parent_handle}')")

            more_note = "" if len(uncovered_handles) <= 50 else f"\n…and {len(uncovered_handles) - 50} more handles."

            # Mode-specific hints
            exploration_mode = session.get("exploration_mode", "manual")
            mode_hints = ""
            if exploration_mode == "dfs_guided_bulk":
                mode_hints = (
                    "\n\n💡 BULK MODE OPTIONS:\n"
                    "\n1. For branch nodes you want to add children under:\n"
                    "     {'handle': 'A.4.2', 'action': 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states'}\n"
                    "   (Alias: 'RB' or 'reserve_branch_for_children')\n"
                    "\n2. To preserve all remaining undecided nodes in ETHER:\n"
                    "     {'action': 'preserve_all_remaining_nodes_in_ether_at_finalization'}\n"
                    "   (Alias: 'PA')\n"
                    "   This will:\n"
                    "     • Preserve all non-engulfed descendants in ETHER\n"
                    "     • Structurally include branches with engulfed descendants\n"
                    "     • Then you can finalize successfully"
                )

            raise NetworkError(
                "Incomplete exploration detected during nexus_finalize_exploration.\n\n"
                "You attempted to finalize without accounting for all branches.\n\n"
                "Handles that are neither explicitly decided (engulfed/preserved) nor covered "
                "by an engulfed/preserved subtree decision:\n"
                + "\n".join(details_lines)
                + more_note
                + "\n\nTo proceed safely, either:\n"
                "• Visit these handles and decide with 'engulf_leaf_into_gem_for_editing' / 'preserve_leaf_in_ether_untouched', OR\n"
                "• Use 'flag_branch_node_for_editing_by_engulfment_into_gem__preserve_all_descendant_protection_states' / 'preserve_branch_node_in_ether_untouched__when_no_engulfed_children' on an ancestor branch to cover them."
                + mode_hints
                + "\n\nOnce every handle is accounted for, nexus_finalize_exploration will succeed "
                "and the phantom gem will be a true minimal covering tree."
            )

        # ============================================================================
        # END COMPLETENESS CHECK
        # ============================================================================

        # Ensure connected to root
        if root_explore_id:
            needed_ids.add(root_explore_id)
        for nid in list(needed_ids):
            cur = parent_by_id.get(nid)
            while cur is not None:
                if cur in needed_ids:
                    break
                needed_ids.add(cur)
                cur = parent_by_id.get(cur)

        # Copy pruned tree and project into GEM (S0) and JEWEL (S1) views
        editable_mode = bool(session.get("editable", False))

        def copy_pruned(node: dict[str, Any]) -> dict[str, Any] | None:
            nid = node.get("id")
            if nid not in needed_ids:
                return None
            new_node = {k: v for k, v in node.items() if k != "children"}

            # Preserve shell marker for delete sections
            if nid in subtree_shells:
                new_node["subtree_mode"] = "shell"

            # Persist explicit SKELETON role hints for JEWELSTORM consumers
            if nid in skeleton_delete_ids:
                new_node["skeleton_hint"] = "DELETE_SECTION"
            elif nid in skeleton_merge_ids:
                new_node["skeleton_hint"] = "MERGE_TARGET"
            elif nid in skeleton_permanent_ids:
                new_node["skeleton_hint"] = "PERMANENT_SECTION"

            new_children = []
            for child in node.get("children", []) or []:
                pruned = copy_pruned(child)
                if pruned:
                    new_children.append(pruned)
            new_node["children"] = new_children
            return new_node

        pruned_root = copy_pruned(root_node)
        if not pruned_root:
            raise NetworkError("Root pruned - no nodes available")

        pruned_children = pruned_root.get("children", [])

        def _strip_original_fields(node: dict) -> None:
            node.pop("original_name", None)
            node.pop("original_note", None)
            for child in node.get("children") or []:
                if isinstance(child, dict):
                    _strip_original_fields(child)

        def _normalize_node_key_order_local(node: dict[str, Any]) -> dict[str, Any]:
            """Ensure id / preview_id / jewel_id appear first for readability."""
            front_keys = ["id", "preview_id", "jewel_id"]
            ordered: dict[str, Any] = {}
            for k in front_keys:
                if k in node:
                    ordered[k] = node[k]
            for k, v in node.items():
                if k not in ordered:
                    ordered[k] = v
            children = ordered.get("children")
            if isinstance(children, list):
                new_children: list[Any] = []
                for ch in children:
                    if isinstance(ch, dict):
                        new_children.append(_normalize_node_key_order_local(ch))
                    else:
                        new_children.append(ch)
                ordered["children"] = new_children
            return ordered

        def _build_nodes_from_pruned(
            source_nodes: list[dict[str, Any]],
            use_original_fields: bool,
        ) -> list[dict[str, Any]]:
            """Deep-copy pruned children and normalize name/note + key order.

            - If use_original_fields=True: name/note come from original_name/original_note
              (falling back to current name/note if originals missing).
            - If use_original_fields=False: name/note remain as-is (edited), helpers stripped.
            """
            nodes = copy.deepcopy(source_nodes)

            def _mutate(n: dict[str, Any]) -> None:
                if use_original_fields:
                    # S0 witness: use original_* if present
                    name = n.get("original_name", n.get("name"))
                    note = n.get("original_note", n.get("note"))
                    n["name"] = name
                    n["note"] = note
                # Helpers are never persisted in final NEXUS artifacts
                n.pop("original_name", None)
                n.pop("original_note", None)
                for ch in n.get("children") or []:
                    if isinstance(ch, dict):
                        _mutate(ch)

            for node in nodes:
                if isinstance(node, dict):
                    _mutate(node)

            return [
                _normalize_node_key_order_local(node) if isinstance(node, dict) else node
                for node in nodes
            ]

        # S0 (GEM witness) vs S1 (JEWEL edits)
        if editable_mode:
            # GEM should reflect ORIGINAL values; JEWEL reflects EDITED values
            gem_nodes_s0 = _build_nodes_from_pruned(pruned_children, use_original_fields=True)
            jewel_nodes_s1 = _build_nodes_from_pruned(pruned_children, use_original_fields=False)
        else:
            # Non-edit mode: single tree (current behavior preserved)
            gem_nodes_s0 = _build_nodes_from_pruned(pruned_children, use_original_fields=False)
            jewel_nodes_s1 = []

        # T0 (coarse_terrain) always mirrors S0 (GEM)
        coarse_nodes = gem_nodes_s0

        # Initialize NEXUS run dir
        base_dir = Path(
            r"E:\__daniel347x\__Obsidian\__Inking into Mind\--TypingMind\Projects - All\Projects - Individual\TODO\temp\nexus_runs"
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        try:
            existing = self._get_nexus_dir(nexus_tag)
            run_dir = Path(existing)
        except NetworkError:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            run_dir = base_dir / f"{timestamp}__{nexus_tag}"
            run_dir.mkdir(parents=True, exist_ok=False)

        phantom_path = run_dir / "phantom_gem.json"
        coarse_path = run_dir / "coarse_terrain.json"
        shimmering_path = run_dir / "shimmering_terrain.json"
        phantom_jewel_path = run_dir / "phantom_jewel.json"
        enchanted_path: Path | None = None

        export_root_id = session.get("root_id")
        export_root_name = session.get("root_name") or root_node.get("name", "Root")

        # Compute preview from T0/S0
        exploration_preview = None
        try:
            exploration_preview = self._annotate_preview_ids_and_build_tree(coarse_nodes, "CT")
        except Exception:
            pass

        scratchpad_list = session.get("scratchpad", [])
        try:
            sp_preview = self._build_scratchpad_preview_lines(
                scratchpad_list,
                handles=handles,
            )
        except Exception:
            sp_preview = []

        # S0 wrapper (witness GEM + T0/T1)
        # IMPORTANT (Ground truth semantics): exploration sessions must propagate export_root_children_status
        # derived from their source tree. Do NOT hardcode 'complete'.
        #
        # Current exploration implementation sources its tree from workflowy_scry() (API) and fails
        # before session creation if max_nodes is exceeded; therefore the exploration root is
        # ground-truth complete.
        export_root_children_status = session.get("export_root_children_status")
        if not isinstance(export_root_children_status, str) or not export_root_children_status:
            export_root_children_status = "unknown"  # fail-closed

        gem_wrapper = {
            "export_timestamp": None,
            "export_root_children_status": export_root_children_status,
            "__preview_tree__": exploration_preview,
            "export_root_id": export_root_id,
            "export_root_name": export_root_name,
            "nodes": coarse_nodes,
            "original_ids_seen": sorted(original_ids_seen),
            "explicitly_preserved_ids": sorted(explicitly_preserved_ids),
            # Persist exploration scratchpad so JEWELSTORM can materialize it into new sections later
            "exploration_scratchpad": scratchpad_list,
        }
        if sp_preview:
            gem_wrapper["exploration_scratchpad_preview"] = sp_preview

        with open(phantom_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        with open(coarse_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        with open(shimmering_path, "w", encoding="utf-8") as f:
            json_module.dump(gem_wrapper, f, indent=2, ensure_ascii=False)

        jewel_wrapper = None
        if editable_mode and jewel_nodes_s1:
            # S1 wrapper (edited JEWEL)
            jewel_wrapper = dict(gem_wrapper)
            jewel_wrapper["nodes"] = jewel_nodes_s1

            with open(phantom_jewel_path, "w", encoding="utf-8") as f:
                json_module.dump(jewel_wrapper, f, indent=2, ensure_ascii=False)

            # Auto-ANCHOR_JEWELS to produce enchanted_terrain.json (T2)
            try:
                anchor_result = await self.nexus_anchor_jewels(nexus_tag)
                enchanted_str = anchor_result.get("enchanted_terrain")
                if enchanted_str:
                    enchanted_path = Path(enchanted_str)
            except Exception:
                logger.error("nexus_anchor_jewels failed during finalize", exc_info=True)
                enchanted_path = None

        full_response = {
            "success": True,
            "session_id": session_id,
            "nexus_tag": nexus_tag,
            "coarse_terrain": str(coarse_path),
            "phantom_gem": str(phantom_path),
            "shimmering_terrain": str(shimmering_path),
            "node_count": len(needed_ids),
            "mode": mode,
        }
        if editable_mode:
            full_response["phantom_jewel"] = str(phantom_jewel_path) if jewel_wrapper else None
            full_response["enchanted_terrain"] = str(enchanted_path) if enchanted_path else None

        full_payload_for_file = dict(full_response)
        full_payload_for_file["gem_wrapper"] = gem_wrapper
        if jewel_wrapper is not None:
            full_payload_for_file["jewel_wrapper"] = jewel_wrapper
        self._write_exploration_output(
            session_id=session_id,
            phase="finalize",
            payload=full_payload_for_file,
            session=session,
        )

        # Persist scratchpad snapshot to permanent directory if any entries exist
        if scratchpad_list:
            try:
                scratchpad_dir = self._get_explore_scratchpad_dir()
                scratchpad_path = os.path.join(
                    scratchpad_dir,
                    f"{session_id}.explore_scratchpad.json",
                )

                # Enrich scratchpad entries with node_id where possible
                scratchpad_snapshot: list[dict[str, Any]] = []
                for entry in scratchpad_list:
                    e = dict(entry)
                    h = e.get("handle")
                    if isinstance(h, str):
                        meta = handles.get(h) or {}
                        nid = meta.get("id")
                        if nid:
                            e["node_id"] = nid
                    scratchpad_snapshot.append(e)

                scratchpad_payload: dict[str, Any] = {
                    "nexus_tag": nexus_tag,
                    "session_id": session_id,
                    "created_at": session.get("created_at"),
                    "finalized_at": datetime.utcnow().isoformat() + "Z",
                    "exploration_mode": session.get("exploration_mode"),
                    "editable": bool(session.get("editable")),
                    "scratchpad": scratchpad_snapshot,
                }
                if sp_preview:
                    scratchpad_payload["scratchpad_preview"] = sp_preview

                scratchpad_payload["gem"] = gem_wrapper
                if jewel_wrapper is not None:
                    scratchpad_payload["jewel"] = jewel_wrapper

                with open(scratchpad_path, "w", encoding="utf-8") as f:
                    json_module.dump(scratchpad_payload, f, indent=2, ensure_ascii=False)
            except Exception:
                logger.error("Failed to write exploration scratchpad snapshot", exc_info=True)

        return full_response
