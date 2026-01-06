"""beacon_obtain_code_snippet.py

Beacon-aware helpers for working with source files and NEXUS beacons.

This module currently focuses on the lowest-risk, most immediately useful
capability:

  • **Extracting code/doc snippets around a given beacon id** from a
    single source file (Python, Markdown, SQL), with context lines.

This is deliberately local-only and has **no Workflowy writes** – it only
reads the filesystem and prints snippets. That gives you a usable, beacon-
aware workflow without needing to depend on the installed workflowy_mcp
configuration in this script.

---------------------------------------------------------------------
FUTURE DESIGN (documented for later QUILLSTORMS, not implemented here)
---------------------------------------------------------------------

A future sibling implementation (inside the MCP server/client) will add a
true per-file refresh mode that talks to Workflowy and applies the
AST+beacon + Notes salvage algorithm we discussed:

Per-file beacon-aware refresh of a Workflowy file node from its source file.

Goal:
- Given a Workflowy FILE node UUID that represents a Cartographer-mapped
  source file (Python/Markdown/SQL), and
- Given that the file on disk has been edited (including beacon comments),

Refresh ONLY that one file node in Workflowy by:
  1. Parsing the source file using the same Cartographer logic used by
     `nexus_map_codebase.py` (AST + beacon parsing).
  2. Rebuilding the AST + beacon subtree under that file node in Workflowy.
  3. Salvaging and re-attaching any user-owned Notes[...] subtrees under
     beacons whose `id=...` did not change.

Invariants for that future mode:
- Files remain the source of truth for structure (classes, functions,
  headings, beacons). The Workflowy subtree for a file is a projection +
  manual Notes.
- Manual content must live under nodes whose *name*, after stripping any
  leading emoji and whitespace, begins with "Notes" (case-insensitive).
  All descendants of such a node are treated as user-owned.
- That future logic will live inside the MCP client/server (e.g. a
  WorkFlowyClient method + @mcp.tool wrapper), *not* in this CLI script.

For now, this file implements the safer subset:
  **beacon-based snippet extraction from files**.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Ensure we can import nexus_map_codebase from the same TODO root directory
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    # Local Cartographer implementation (no Workflowy calls)
    import nexus_map_codebase  # type: ignore[import]
except Exception as e:  # noqa: BLE001
    print(
        f"[beacon_obtain_code_snippet] ERROR: Could not import nexus_map_codebase from {_THIS_DIR}: {e}",
        file=sys.stderr,
    )
    nexus_map_codebase = None  # type: ignore[assignment]


@dataclass
class NotesMapping:
    """(Reserved for future per-file refresh mode inside MCP code).

    Mapping from beacon_id -> list of Workflowy node ids for Notes subtrees.
    Not used in this CLI; kept to reflect the planned API shape.
    """

    by_beacon_id: Dict[str, List[str]]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def compute_sha1(path: str) -> str:
    """Compute SHA1 of a file's bytes (hex string).

    Kept because the per-file refresh design uses a hash guard to skip work
    when the source file has not changed.
    """

    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().splitlines()


# ---------------------------------------------------------------------------
# Python beacon snippet extraction
# ---------------------------------------------------------------------------


def _python_find_closing_beacon_span(
    lines: List[str],
    beacons: List[dict],
    target_id: str,
) -> Optional[Tuple[int, int]]:
    """If there are two snippet-less beacons with the same id, treat them as
    an open/close delimiter pair and return a (start_line, end_line) span.

    Semantics (snippet-only, local to this CLI):
    - We scan the parsed beacon list for entries whose id matches target_id.
    - If the first two matches both have no start_snippet/end_snippet, we
      treat the first as the opener and the second as the closer.
    - The content span is:
        (end_of_open_block + 1) .. (close_comment_line - 1)
      where end_of_open_block is the line whose text contains "]" in the
      beacon block that starts at open_comment_line.
    - If that range is empty or inverted, we return None and let the caller
      fall back to existing heuristics.
    """

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1].lstrip()
            # For Python, beacon metadata lines are usually comments starting
            # with '#', but we only care about the first line that contains ']'.
            body = raw.lstrip("#").lstrip() if raw.startswith("#") else raw
            if "]" in body:
                return j
            j += 1
        return int(comment_line) or 1

    tid = target_id.strip()
    if not tid:
        return None

    # Find the first snippet-less opener for this id in the parsed beacons.
    open_b: Optional[dict] = None
    for b in beacons:
        bid = (b.get("id") or "").strip()
        if bid != tid:
            continue
        if b.get("start_snippet") or b.get("end_snippet"):
            continue
        open_b = b
        break

    if not open_b:
        return None

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1].lstrip()
            # For Python, beacon metadata lines are usually comments starting
            # with '#', but we only care about the first line that contains ']'.
            body = raw.lstrip("#").lstrip() if raw.startswith("#") else raw
            if "]" in body:
                return j
            j += 1
        return int(comment_line) or 1

    open_comment = int(open_b.get("comment_line") or 1)
    open_end = _block_end_lineno(open_comment)
    n = len(lines)

    # Scan forward for a @beacon-close[...] block with matching id.
    j = open_end + 1
    while j <= n:
        raw = lines[j - 1].lstrip()
        if raw.startswith("#"):
            body = raw.lstrip("#").lstrip()
        else:
            body = raw

        if "@beacon-close[" not in body:
            j += 1
            continue

        # Collect full close block
        block_lines: List[str] = [body]
        close_comment = j
        k = j + 1
        while k <= n:
            raw2 = lines[k - 1].lstrip()
            if raw2.startswith("#"):
                body2 = raw2.lstrip("#").lstrip()
            else:
                body2 = raw2
            block_lines.append(body2)
            if "]" in body2:
                break
            k += 1

        # Parse fields to get id
        fields: Dict[str, str] = {}
        inner = block_lines[1:-1] if len(block_lines) >= 2 else []
        for raw_line in inner:
            text = raw_line.strip()
            if not text or text.startswith("@beacon-close"):
                continue
            while text and text[-1] in ",]":
                text = text[:-1].rstrip()
            if not text or "=" not in text:
                continue
            key, val = text.split("=", 1)
            key = key.strip()
            val = val.strip()
            if (val.startswith("\"") and val.endswith("\"")) or (
                val.startswith("'") and val.endswith("'")
            ):
                val = val[1:-1]
            fields[key] = val

        close_id = (fields.get("id") or "").strip()
        if close_id == tid:
            core_lo = open_end + 1
            core_hi = close_comment - 1
            if core_lo <= core_hi:
                return core_lo, core_hi

        j = k + 1

    return None


def _python_find_ast_node_for_beacon(
    outline_nodes: List[dict], beacon_id: str
) -> Optional[dict]:
    """Search the Cartographer AST outline tree for an AST beacon with id.

    We rely on nexus_map_codebase.apply_python_beacons behavior:
    - For kind=ast beacons, it appends a block like:

        BEACON (AST)
        id: model:forward@1
        role: model:forward
        slice_labels: model-forward
        kind: ast

      into the node's note.

    We scan all nodes' notes for such a block and match the id.
    """

    def iter_nodes(nodes: Iterable[dict]) -> Iterable[dict]:
        for n in nodes:
            if isinstance(n, dict):
                yield n
                for ch in n.get("children") or []:
                    if isinstance(ch, dict):
                        yield from iter_nodes([ch])

    target = beacon_id.strip()
    for node in iter_nodes(outline_nodes):
        note = node.get("note") or ""
        if "BEACON (AST)" not in note:
            continue
        for line in str(note).splitlines():
            stripped = line.strip()
            if stripped.startswith("id:"):
                val = stripped.split(":", 1)[1].strip()
                if val == target:
                    return node
    return None


def _python_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str]]:
    """Return (start_line, end_line, lines) for a Python beacon id.

    For **AST beacons**, we:
      - Parse the file via Cartographer's parse_file_outline().
      - Locate the AST node whose note carries BEACON (AST) with the id.
      - Use its orig_lineno_start_unused / orig_lineno_end_unused as base
        range, then widen by `context` lines.

    For **SPAN beacons**, v0 uses a simpler heuristic:
      - Use nexus_map_codebase.parse_python_beacon_blocks to find the
        beacon block.
      - Anchor on the first non-empty, non-comment, non-decorator line
        after the beacon block.
      - Return that single line with context before/after.

    This is intentionally simple and safe; it can be upgraded later using
    the richer logic already present in apply_python_beacons.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    # First, try AST beacon path
    try:
        outline_nodes = nexus_map_codebase.parse_file_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python AST for {file_path}: {e}") from e

    node = _python_find_ast_node_for_beacon(outline_nodes, beacon_id)
    if node is not None:
        start = node.get("orig_lineno_start_unused")
        end = node.get("orig_lineno_end_unused")
        if isinstance(start, int) and isinstance(end, int) and start > 0 and end >= start:
            lo = max(1, start - context)
            hi = min(len(lines), end + context)
            return lo, hi, lines

    # Fallback: span-beacon-style heuristic WITH optional closing-delimiter support
    try:
        beacons = nexus_map_codebase.parse_python_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # First, see if there is a closing-delimiter pair for this id
    span = _python_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        core_lo, core_hi = span
        lo = max(1, core_lo - context)
        hi = min(len(lines), core_hi + context)
        return lo, hi, lines

    # Otherwise, fall back to the original single-beacon heuristic
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in Python file {file_path!r}")

    comment_line = chosen.get("comment_line") or 1
    # Anchor on next non-blank, non-comment, non-decorator line after block
    n = len(lines)
    anchor = None
    j = int(comment_line)
    # Advance until the end of the beacon block (first line containing ']')
    while j <= n:
        raw = lines[j - 1]
        stripped = raw.lstrip()
        body = stripped.lstrip("#").lstrip() if stripped.startswith("#") else stripped
        if "]" in body:
            j += 1
            break
        j += 1

    while j <= n:
        raw = lines[j - 1]
        stripped = raw.lstrip()
        if not stripped:
            j += 1
            continue
        if stripped.startswith("#") or stripped.startswith("@"):
            j += 1
            continue
        anchor = j
        break

    if anchor is None:
        anchor = int(comment_line)

    lo = max(1, anchor - context)
    hi = min(len(lines), anchor + context)
    return lo, hi, lines


# ---------------------------------------------------------------------------
# Markdown beacon snippet extraction
# ---------------------------------------------------------------------------


def _markdown_find_closing_beacon_span(
    lines: List[str],
    beacons: List[dict],
    target_id: str,
) -> Optional[Tuple[int, int]]:
    """Closing-delimiter span for Markdown beacons with matching id.

    Similar semantics as the Python helper but adapted to HTML comments:
    - We look for the first two snippet-less beacons with the given id.
    - The opener's block is assumed to end at the first line containing ']'
      in its @beacon[...] comment block.
    - The closer's `comment_line` marks the start of the closing block; the
      content span ends just before that line.
    """

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1]
            if "]" in raw:
                return j
            j += 1
        return int(comment_line) or 1

    tid = target_id.strip()
    if not tid:
        return None

    # Find the first snippet-less opener for this id in the parsed beacons.
    open_b: Optional[dict] = None
    for b in beacons:
        bid = (b.get("id") or "").strip()
        if bid != tid:
            continue
        if b.get("start_snippet") or b.get("end_snippet"):
            continue
        open_b = b
        break

    if not open_b:
        return None

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1]
            if "]" in raw:
                return j
            j += 1
        return int(comment_line) or 1

    open_comment = int(open_b.get("comment_line") or 1)
    open_end = _block_end_lineno(open_comment)
    n = len(lines)

    # Scan forward for a @beacon-close[...] block with matching id.
    j = open_end + 1
    while j <= n:
        raw = lines[j - 1]
        if "@beacon-close[" not in raw:
            j += 1
            continue

        # Collect full close block
        block_lines: List[str] = [raw]
        close_comment = j
        k = j + 1
        while k <= n:
            raw2 = lines[k - 1]
            block_lines.append(raw2)
            if "]" in raw2:
                break
            k += 1

        # Parse fields to get id
        fields: Dict[str, str] = {}
        inner = block_lines[1:-1] if len(block_lines) >= 2 else []
        for raw_line in inner:
            text = raw_line.strip()
            if not text or text.startswith("@beacon-close"):
                continue
            if text.startswith("<!--") or text.startswith("-->"):
                continue
            while text and text[-1] in ",]":
                text = text[:-1].rstrip()
            if not text or "=" not in text:
                continue
            key, val = text.split("=", 1)
            key = key.strip()
            val = val.strip()
            if (val.startswith("\"") and val.endswith("\"")) or (
                val.startswith("'") and val.endswith("'")
            ):
                val = val[1:-1]
            fields[key] = val

        close_id = (fields.get("id") or "").strip()
        if close_id == tid:
            core_lo = open_end + 1
            core_hi = close_comment - 1
            if core_lo <= core_hi:
                return core_lo, core_hi

        j = k + 1

    return None


def _markdown_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str]]:
    """Return (start_line, end_line, lines) for a Markdown beacon id.

    Uses nexus_map_codebase.parse_markdown_beacon_blocks(), which already
    computes span_lineno_start/span_lineno_end for kind=span beacons.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    try:
        beacons = nexus_map_codebase.parse_markdown_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Markdown beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # First, try closing-delimiter pairing for snippet-less beacons
    span = _markdown_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        core_lo, core_hi = span
        lo = max(1, core_lo - context)
        hi = min(len(lines), core_hi + context)
        return lo, hi, lines

    # Otherwise, fall back to the existing span_lineno/anchor behavior
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in Markdown file {file_path!r}")

    start = chosen.get("span_lineno_start")
    end = chosen.get("span_lineno_end")

    # If no explicit span, anchor around comment line
    if not isinstance(start, int) or start <= 0:
        start = int(chosen.get("comment_line") or 1)
        end = start
    if not isinstance(end, int) or end < start:
        end = start

    lo = max(1, start - context)
    hi = min(len(lines), end + context)
    return lo, hi, lines


# ---------------------------------------------------------------------------
# SQL beacon snippet extraction
# ---------------------------------------------------------------------------


def _sql_find_closing_beacon_span(
    lines: List[str],
    beacons: List[dict],
    target_id: str,
) -> Optional[Tuple[int, int]]:
    """Closing-delimiter span for SQL beacons with matching id.

    The semantics mirror the Python/Markdown helpers but adapted to `--` comments.
    """

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1].lstrip()
            if raw.startswith("--"):
                body = raw.lstrip("-").lstrip()
            else:
                body = raw
            if "]" in body:
                return j
            j += 1
        return int(comment_line) or 1

    tid = target_id.strip()
    if not tid:
        return None

    # Find the first snippet-less opener for this id in the parsed beacons.
    open_b: Optional[dict] = None
    for b in beacons:
        bid = (b.get("id") or "").strip()
        if bid != tid:
            continue
        if b.get("start_snippet") or b.get("end_snippet"):
            continue
        open_b = b
        break

    if not open_b:
        return None

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1]
            if "]" in raw:
                return j
            j += 1
        return int(comment_line) or 1

    open_comment = int(open_b.get("comment_line") or 1)
    open_end = _block_end_lineno(open_comment)
    n = len(lines)

    # Scan forward for a @beacon-close[...] block with matching id.
    j = open_end + 1
    while j <= n:
        raw = lines[j - 1]
        if "@beacon-close[" not in raw:
            j += 1
            continue

        # Collect full close block
        block_lines: List[str] = [raw]
        close_comment = j
        k = j + 1
        while k <= n:
            raw2 = lines[k - 1]
            block_lines.append(raw2)
            if "]" in raw2:
                break
            k += 1

        # Parse fields to get id
        fields: Dict[str, str] = {}
        inner = block_lines[1:-1] if len(block_lines) >= 2 else []
        for raw_line in inner:
            text = raw_line.strip()
            if not text or text.startswith("@beacon-close"):
                continue
            if text.startswith("<!--") or text.startswith("-->"):
                continue
            while text and text[-1] in ",]":
                text = text[:-1].rstrip()
            if not text or "=" not in text:
                continue
            key, val = text.split("=", 1)
            key = key.strip()
            val = val.strip()
            if (val.startswith("\"") and val.endswith("\"")) or (
                val.startswith("'") and val.endswith("'")
            ):
                val = val[1:-1]
            fields[key] = val

        close_id = (fields.get("id") or "").strip()
        if close_id == tid:
            core_lo = open_end + 1
            core_hi = close_comment - 1
            if core_lo <= core_hi:
                return core_lo, core_hi

        j = k + 1

    return None


def _sql_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str]]:
    """Return (start_line, end_line, lines) for a SQL beacon id.

    We only have span-style beacons in SQL. For v0 we:
      - Use parse_sql_beacon_blocks() to find the beacon block.
      - Anchor around its comment_line.
      - Return that line with context above/below.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    try:
        beacons = nexus_map_codebase.parse_sql_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse SQL beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # First, try closing-delimiter pairing
    span = _sql_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        core_lo, core_hi = span
        lo = max(1, core_lo - context)
        hi = min(len(lines), core_hi + context)
        return lo, hi, lines

    # Otherwise, fall back to comment-line anchoring
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in SQL file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)
    lo = max(1, comment_line - context)
    hi = min(len(lines), comment_line + context)
    return lo, hi, lines


# ---------------------------------------------------------------------------
# Public CLI: beacon snippet extraction
# ---------------------------------------------------------------------------


def _print_snippet(
    file_path: str,
    beacon_id: str,
    start: int,
    end: int,
    lines: List[str],
) -> None:
    rel = os.path.relpath(file_path, start=os.getcwd())
    print(f"File: {file_path} ({rel})")
    print(f"Beacon id: {beacon_id}")
    print(f"Lines: {start}–{end}\n")
    print("```")
    for lineno in range(start, end + 1):
        if 1 <= lineno <= len(lines):
            print(f"{lineno:5d}: {lines[lineno - 1]}")
    print("```")


# ---------------------------------------------------------------------------
# Shell beacon snippet extraction
# ---------------------------------------------------------------------------


def _sh_find_closing_beacon_span(
    lines: List[str],
    beacons: List[dict],
    target_id: str,
) -> Optional[Tuple[int, int]]:
    """Closing-delimiter span for shell beacons with matching id.

    Uses '#' comment syntax, mirrors the Python/SQL pattern.
    """

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1].lstrip()
            if raw.startswith("#"):
                body = raw.lstrip("#").lstrip()
            else:
                body = raw
            if "]" in body:
                return j
            j += 1
        return int(comment_line) or 1

    tid = target_id.strip()
    if not tid:
        return None

    open_b: Optional[dict] = None
    for b in beacons:
        bid = (b.get("id") or "").strip()
        if bid != tid:
            continue
        if b.get("start_snippet") or b.get("end_snippet"):
            continue
        open_b = b
        break

    if not open_b:
        return None

    open_comment = int(open_b.get("comment_line") or 1)
    open_end = _block_end_lineno(open_comment)
    n = len(lines)

    # Scan forward for @beacon-close[...]
    j = open_end + 1
    while j <= n:
        raw = lines[j - 1].lstrip()
        if raw.startswith("#"):
            body = raw.lstrip("#").lstrip()
        else:
            body = raw

        if "@beacon-close[" not in body:
            j += 1
            continue

        block_lines: List[str] = [body]
        close_comment = j
        k = j + 1
        while k <= n:
            raw2 = lines[k - 1].lstrip()
            if raw2.startswith("#"):
                body2 = raw2.lstrip("#").lstrip()
            else:
                body2 = raw2
            block_lines.append(body2)
            if "]" in body2:
                break
            k += 1

        fields: Dict[str, str] = {}
        inner = block_lines[1:-1] if len(block_lines) >= 2 else []
        for raw_line in inner:
            text = raw_line.strip()
            if not text or text.startswith("@beacon-close"):
                continue
            while text and text[-1] in ",]":
                text = text[:-1].rstrip()
            if not text or "=" not in text:
                continue
            key, val = text.split("=", 1)
            key = key.strip()
            val = val.strip()
            if (val.startswith('"') and val.endswith('"')) or (
                val.startswith("'") and val.endswith("'")
            ):
                val = val[1:-1]
            fields[key] = val

        close_id = (fields.get("id") or "").strip()
        if close_id == tid:
            core_lo = open_end + 1
            core_hi = close_comment - 1
            if core_lo <= core_hi:
                return core_lo, core_hi

        j = k + 1

    return None


def _sh_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str]]:
    """Return (start_line, end_line, lines) for a shell beacon id.

    Shell uses '#' comments (like Python), but has no AST.
    We only support span-style beacons.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    try:
        beacons = nexus_map_codebase.parse_sh_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse shell beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # First, try closing-delimiter pairing
    span = _sh_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        core_lo, core_hi = span
        lo = max(1, core_lo - context)
        hi = min(len(lines), core_hi + context)
        return lo, hi, lines

    # Otherwise, fall back to comment-line anchoring
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in shell file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)
    lo = max(1, comment_line - context)
    hi = min(len(lines), comment_line + context)
    return lo, hi, lines


# ---------------------------------------------------------------------------
# Public CLI: beacon snippet extraction
# ---------------------------------------------------------------------------


def get_snippet_data(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str]]:
    """Return (start_line, end_line, lines) for a given file + beacon id.

    This is the library-friendly entrypoint used by other tools (e.g. MCP)
    that want structured data instead of formatted stdout.
    """
    file_path = os.path.abspath(file_path)
    ext = Path(file_path).suffix.lower()

    if not os.path.isfile(file_path):
        raise RuntimeError(f"Source file not found: {file_path}")

    if ext == ".py":
        start, end, lines = _python_snippet_for_beacon(file_path, beacon_id, context)
    elif ext in {".md", ".markdown"}:
        start, end, lines = _markdown_snippet_for_beacon(file_path, beacon_id, context)
    elif ext == ".sql":
        start, end, lines = _sql_snippet_for_beacon(file_path, beacon_id, context)
    elif ext == ".sh":
        start, end, lines = _sh_snippet_for_beacon(file_path, beacon_id, context)
    else:
        raise RuntimeError(f"Unsupported file extension for beacon snippets: {ext}")

    return start, end, lines


def extract_snippet(file_path: str, beacon_id: str, context: int) -> None:
    """CLI wrapper: resolve snippet and print with line numbers for humans."""
    start, end, lines = get_snippet_data(file_path, beacon_id, context)
    _print_snippet(os.path.abspath(file_path), beacon_id, start, end, lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Beacon-aware helpers for source files: extract snippets around "
            "a given beacon id (Python/Markdown/SQL)."
        )
    )

    parser.add_argument(
        "file",
        help="Path to the source file (Python/Markdown/SQL)",
    )
    parser.add_argument(
        "beacon_id",
        help="Beacon id to locate (e.g. model:forward@1)",
    )
    parser.add_argument(
        "--context",
        type=int,
        default=10,
        help="Number of context lines before/after the beacon (default: 10)",
    )

    args = parser.parse_args(argv)

    try:
        extract_snippet(args.file, args.beacon_id, args.context)
    except Exception as e:  # noqa: BLE001
        print(f"[beacon_obtain_code_snippet] ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
