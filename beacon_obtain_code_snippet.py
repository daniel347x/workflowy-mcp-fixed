"""beacon_obtain_code_snippet.py

Beacon-aware helpers for working with source files and NEXUS beacons.

This module currently focuses on the lowest-risk, most immediately useful
capability:

  • **Extracting code/doc snippets around a given beacon id** from a
    single source file (Python, Markdown, SQL, shell), with context lines.

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


# @beacon[
#   id=carto-js-ts@_python_resolve_ast_node_heuristic,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: Python AST heuristic template.
# Reference for a future JS/TS heuristic helper that will resolve
# JS/TS AST nodes (by node_name + parent_names) to snippets using
# Tree-sitter-based outlines.
def _python_resolve_ast_node_heuristic(
    file_path: str,
    node_name: str,
    parent_names: List[str],
    context: int,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    """Resolve a non-beacon Python AST node to a snippet using AST qualname matching.

    Returns: (start, end, lines, core_start, core_end, beacon_line, metadata)

    Where metadata includes:
    - resolution_strategy: "ast_qualname_exact" | "ast_qualname_prefix" | "ast_simple_name" | "token_search"
    - confidence: 0.0-1.0
    - ambiguity: "none" | "auto_resolved" | "disambiguation_needed"
    - candidates: list of dicts (when ambiguous)
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)
    n = len(lines)

    try:
        outline_nodes = nexus_map_codebase.parse_file_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python AST outline for {file_path}: {e}") from e

    metadata: dict = {
        "resolution_strategy": None,
        "confidence": 0.0,
        "ambiguity": "none",
        "candidates": None,
    }

    def _normalize_core_name(raw: str) -> str:
        name = (raw or "").strip()
        if not name:
            return ""
        # Strip leading non-alphanumeric decoration / emoji.
        i = 0
        while i < len(name) and not (name[i].isalnum() or name[i] in {"_", "$"}):
            i += 1
        name = name[i:].lstrip()
        # Drop trailing hashtag tags (e.g. "#model-forward").
        tokens = name.split()
        while tokens and tokens[-1].startswith("#"):
            tokens.pop()
        return " ".join(tokens).strip()

    def _classify_python_node_name(raw: str) -> Tuple[str | None, str | None]:
        core = _normalize_core_name(raw)
        if not core:
            return None, None
        lower = core.lower()
        # class Foo
        if lower.startswith("class "):
            rest = core[len("class ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "class"
        # async def foo(...)
        if lower.startswith("async def "):
            rest = core[len("async def ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "async_function"
        # def foo(...)
        if lower.startswith("def "):
            rest = core[len("def ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "function"
        # Fallback: treat as function if it looks like name(...)
        if "(" in core and ")" in core:
            simple = core.split("(", 1)[0].strip() or None
            return simple, "function"
        # Constant / attribute style (e.g. CONSTANT_NAME)
        return core.split()[0].strip(), "const"

    def _extract_parent_classes(raw_parents: List[str]) -> List[str]:
        classes: List[str] = []
        for raw in raw_parents:
            core = _normalize_core_name(raw)
            if not core:
                continue
            lower = core.lower()
            if lower.startswith("class "):
                rest = core[len("class ") :].strip()
                cname = rest.split("(", 1)[0].strip()
                if cname:
                    classes.append(cname)
        return classes

    simple_name, expected_type = _classify_python_node_name(node_name)
    parent_classes = _extract_parent_classes(parent_names or [])

    qual_parts: List[str] = []
    if parent_classes:
        qual_parts.extend(parent_classes)
    if simple_name:
        qual_parts.append(simple_name)
    intended_qualname = ".".join(qual_parts) if qual_parts else None

    def _iter_nodes(nodes: Iterable[dict]) -> Iterable[dict]:
        for n in nodes or []:
            if isinstance(n, dict):
                yield n
                for ch in n.get("children") or []:
                    if isinstance(ch, dict):
                        yield from _iter_nodes([ch])

    ast_candidates: List[dict] = []
    for node in _iter_nodes(outline_nodes):
        qual = node.get("ast_qualname")
        if not isinstance(qual, str) or not qual:
            continue
        node_type = node.get("ast_type")
        start = node.get("orig_lineno_start_unused")
        end = node.get("orig_lineno_end_unused") or start
        if not isinstance(start, int) or start <= 0:
            continue
        if not isinstance(end, int) or end < start:
            end = start
        simple = qual.split(".")[-1]
        parents = qual.split(".")[:-1]
        ast_candidates.append(
            {
                "node": node,
                "ast_qualname": qual,
                "ast_type": node_type,
                "simple_name": simple,
                "parent_parts": parents,
                "start": start,
                "end": end,
            }
        )

    def _build_candidates_list(cands: List[dict]) -> List[dict]:
        out: List[dict] = []
        for c in cands:
            out.append(
                {
                    "ast_qualname": c.get("ast_qualname"),
                    "ast_type": c.get("ast_type"),
                    "start_line": c.get("start"),
                    "end_line": c.get("end"),
                }
            )
        return out

    def _make_snippet(
        cand: dict,
        strategy: str,
        confidence: float,
        ambiguity: str,
        all_candidates: List[dict] | None = None,
    ) -> Tuple[int, int, List[str], int, int, int, dict]:
        core_start = int(cand["start"])
        core_end = int(cand["end"])
        start_line = max(1, core_start - context)
        end_line = min(n, core_end + context)
        meta = {
            "resolution_strategy": strategy,
            "confidence": confidence,
            "ambiguity": ambiguity,
            "candidates": _build_candidates_list(all_candidates) if all_candidates else None,
        }
        return start_line, end_line, lines, core_start, core_end, core_start, meta

    # Stage 1: exact ast_qualname match
    if intended_qualname:
        exact = [
            c
            for c in ast_candidates
            if c["ast_qualname"] == intended_qualname
            and (expected_type is None or c.get("ast_type") == expected_type)
        ]
        if exact:
            ambiguity = "none" if len(exact) == 1 else "auto_resolved"
            return _make_snippet(exact[0], "ast_qualname_exact", 1.0, ambiguity, exact)

    # Stage 2: parent class chain prefix + simple name
    if simple_name and parent_classes:
        prefix_matches: List[dict] = []
        for c in ast_candidates:
            if c["simple_name"] != simple_name:
                continue
            parents = c["parent_parts"] or []
            if len(parents) >= len(parent_classes) and parents[-len(parent_classes) :] == parent_classes:
                if expected_type is None or c.get("ast_type") == expected_type:
                    prefix_matches.append(c)
        if prefix_matches:
            ambiguity = "none" if len(prefix_matches) == 1 else "auto_resolved"
            return _make_snippet(prefix_matches[0], "ast_qualname_prefix", 0.9, ambiguity, prefix_matches)

    # Stage 3: simple-name-only match within same ast_type
    if simple_name:
        simple_matches: List[dict] = []
        for c in ast_candidates:
            if c["simple_name"] != simple_name:
                continue
            if expected_type is not None and c.get("ast_type") != expected_type:
                continue
            simple_matches.append(c)
        if simple_matches:
            ambiguity = "none" if len(simple_matches) == 1 else "auto_resolved"
            return _make_snippet(simple_matches[0], "ast_simple_name", 0.7, ambiguity, simple_matches)

    # Final fallback: token search based on node_name
    search_text = _normalize_core_name(node_name)
    if "(" in search_text:
        search_text = search_text.split("(", 1)[0].strip()
    if not search_text:
        raise RuntimeError(
            "Cannot derive search token from node name %r for AST resolution" % (node_name,)
        )

    anchor: int | None = None
    for idx, line in enumerate(lines, start=1):
        if search_text in line:
            anchor = idx
            break

    if anchor is None:
        raise RuntimeError(
            f"Search token {search_text!r} not found in file {file_path!r}"
        )

    core_start = anchor
    core_end = anchor
    start_line = max(1, core_start - context)
    end_line = min(n, core_end + context)
    metadata["resolution_strategy"] = "token_search"
    metadata["confidence"] = 0.4
    metadata["ambiguity"] = "none"
    metadata["candidates"] = None
    return start_line, end_line, lines, core_start, core_end, core_start, metadata


# @beacon[
#   id=carto-js-ts@_js_ts_resolve_ast_node_heuristic,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: JS/TS AST heuristic helper.
# Resolves JS/TS AST nodes (by node_name + parent_names) to snippets using
# Tree-sitter-based outlines produced by parse_js_ts_outline.
def _js_ts_resolve_ast_node_heuristic(
    file_path: str,
    node_name: str,
    parent_names: List[str],
    context: int,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)
    n = len(lines)

    try:
        outline_nodes = nexus_map_codebase.parse_js_ts_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse JS/TS AST outline for {file_path}: {e}") from e

    metadata: dict = {
        "resolution_strategy": None,
        "confidence": 0.0,
        "ambiguity": "none",
        "candidates": None,
    }

    def _normalize_core_name(raw: str) -> str:
        name = (raw or "").strip()
        if not name:
            return ""
        i = 0
        while i < len(name) and not (name[i].isalnum() or name[i] in {"_", "$"}):
            i += 1
        name = name[i:].lstrip()
        tokens = name.split()
        while tokens and tokens[-1].startswith("#"):
            tokens.pop()
        return " ".join(tokens).strip()

    def _classify_js_ts_node_name(raw: str) -> Tuple[str | None, str | None]:
        core = _normalize_core_name(raw)
        if not core:
            return None, None
        lower = core.lower()
        if lower.startswith("class "):
            rest = core[len("class ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "class"
        if lower.startswith("function "):
            rest = core[len("function ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "function"
        if lower.startswith("method "):
            rest = core[len("method ") :].strip()
            simple = rest.split("(", 1)[0].strip() or None
            return simple, "method"
        if "(" in core and ")" in core:
            simple = core.split("(", 1)[0].strip() or None
            return simple, "function"
        return core.split()[0].strip(), "const"

    def _extract_parent_classes(raw_parents: List[str]) -> List[str]:
        classes: List[str] = []
        for raw in raw_parents:
            core = _normalize_core_name(raw)
            if not core:
                continue
            lower = core.lower()
            if lower.startswith("class "):
                rest = core[len("class ") :].strip()
                cname = rest.split("(", 1)[0].strip()
                if cname:
                    classes.append(cname)
        return classes

    simple_name, expected_type = _classify_js_ts_node_name(node_name)
    parent_classes = _extract_parent_classes(parent_names or [])

    qual_parts: List[str] = []
    if parent_classes:
        qual_parts.extend(parent_classes)
    if simple_name:
        qual_parts.append(simple_name)
    intended_qualname = ".".join(qual_parts) if qual_parts else None

    def _iter_nodes(nodes: Iterable[dict]) -> Iterable[dict]:
        for n in nodes or []:
            if isinstance(n, dict):
                yield n
                for ch in n.get("children") or []:
                    if isinstance(ch, dict):
                        yield from _iter_nodes([ch])

    ast_candidates: List[dict] = []
    for node in _iter_nodes(outline_nodes):
        qual = node.get("ast_qualname")
        if not isinstance(qual, str) or not qual:
            continue
        node_type = node.get("ast_type")
        start = node.get("orig_lineno_start_unused")
        end = node.get("orig_lineno_end_unused") or start
        if not isinstance(start, int) or start <= 0:
            continue
        if not isinstance(end, int) or end < start:
            end = start
        simple = qual.split(".")[-1]
        parents = qual.split(".")[:-1]
        ast_candidates.append(
            {
                "node": node,
                "ast_qualname": qual,
                "ast_type": node_type,
                "simple_name": simple,
                "parent_parts": parents,
                "start": start,
                "end": end,
            }
        )

    def _build_candidates_list(cands: List[dict]) -> List[dict]:
        out: List[dict] = []
        for c in cands:
            out.append(
                {
                    "ast_qualname": c.get("ast_qualname"),
                    "ast_type": c.get("ast_type"),
                    "start_line": c.get("start"),
                    "end_line": c.get("end"),
                }
            )
        return out

    def _make_snippet(
        cand: dict,
        strategy: str,
        confidence: float,
        ambiguity: str,
        all_candidates: List[dict] | None = None,
    ) -> Tuple[int, int, List[str], int, int, int, dict]:
        core_start = int(cand["start"])
        core_end = int(cand["end"])
        start_line = max(1, core_start - context)
        end_line = min(n, core_end + context)
        meta = {
            "resolution_strategy": strategy,
            "confidence": confidence,
            "ambiguity": ambiguity,
            "candidates": _build_candidates_list(all_candidates) if all_candidates else None,
        }
        return start_line, end_line, lines, core_start, core_end, core_start, meta

    if intended_qualname:
        exact = [
            c
            for c in ast_candidates
            if c["ast_qualname"] == intended_qualname
            and (expected_type is None or c.get("ast_type") == expected_type)
        ]
        if exact:
            ambiguity = "none" if len(exact) == 1 else "auto_resolved"
            return _make_snippet(exact[0], "ast_qualname_exact", 1.0, ambiguity, exact)

    if simple_name and parent_classes:
        prefix_matches: List[dict] = []
        for c in ast_candidates:
            if c["simple_name"] != simple_name:
                continue
            parents = c["parent_parts"] or []
            if len(parents) >= len(parent_classes) and parents[-len(parent_classes) :] == parent_classes:
                if expected_type is None or c.get("ast_type") == expected_type:
                    prefix_matches.append(c)
        if prefix_matches:
            ambiguity = "none" if len(prefix_matches) == 1 else "auto_resolved"
            return _make_snippet(prefix_matches[0], "ast_qualname_prefix", 0.9, ambiguity, prefix_matches)

    if simple_name:
        simple_matches: List[dict] = []
        for c in ast_candidates:
            if c["simple_name"] != simple_name:
                continue
            if expected_type is not None and c.get("ast_type") != expected_type:
                continue
            simple_matches.append(c)
        if simple_matches:
            ambiguity = "none" if len(simple_matches) == 1 else "auto_resolved"
            return _make_snippet(simple_matches[0], "ast_simple_name", 0.7, ambiguity, simple_matches)

    search_text = _normalize_core_name(node_name)
    if "(" in search_text:
        search_text = search_text.split("(", 1)[0].strip()
    if not search_text:
        raise RuntimeError(
            "Cannot derive search token from node name %r for JS/TS AST resolution" % (node_name,)
        )

    anchor: int | None = None
    for idx, line in enumerate(lines, start=1):
        if search_text in line:
            anchor = idx
            break

    if anchor is None:
        raise RuntimeError(
            f"Search token {search_text!r} not found in file {file_path!r}"
        )

    core_start = anchor
    core_end = anchor
    start_line = max(1, core_start - context)
    end_line = min(n, core_end + context)
    metadata["resolution_strategy"] = "token_search"
    metadata["confidence"] = 0.4
    metadata["ambiguity"] = "none"
    metadata["candidates"] = None
    return start_line, end_line, lines, core_start, core_end, core_start, metadata


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


def _python_comment_block_span(
    lines: List[str],
    comment_line: int,
) -> Optional[Tuple[int, int]]:
    """Return (top, bottom) of the Python comment block around a beacon.

    Semantics are aligned with nexus_map_codebase._extract_python_beacon_context:
    - Start from the beacon's comment_line.
    - Find the end of the @beacon[...] metadata block (first line containing ']').
    - ABOVE: walk upward, skipping blanks, collecting contiguous '#' comment
      lines (excluding the metadata itself).
    - BELOW: from the end of the metadata block, walk downward, skipping
      blanks, collecting contiguous '#' comment lines.

    Returns None if no such non-empty comment lines are found.
    """

    n = len(lines)
    if comment_line <= 0 or comment_line > n:
        return None

    # Determine end of the beacon metadata block
    j = int(comment_line)
    block_end = comment_line
    while j <= n:
        raw = lines[j - 1].lstrip()
        if raw.startswith("#"):
            body = raw.lstrip("#").lstrip()
        else:
            body = raw
        if "]" in body:
            block_end = j
            break
        j += 1

    top: Optional[int] = None
    bottom: Optional[int] = None

    # ABOVE
    j = comment_line - 1
    while j >= 1:
        stripped = lines[j - 1].lstrip()
        if not stripped:
            j -= 1
            continue
        if stripped.startswith("#"):
            body = stripped.lstrip("#").lstrip()
            if body:
                if top is None:
                    top = j
                    bottom = j
                else:
                    top = j
                j -= 1
                continue
        break

    # BELOW
    j = block_end + 1
    while j <= n:
        stripped = lines[j - 1].lstrip()
        if not stripped:
            j += 1
            continue
        if stripped.startswith("#"):
            body = stripped.lstrip("#").lstrip()
            if body:
                if top is None:
                    top = j
                    bottom = j
                else:
                    bottom = j
                j += 1
                continue
        break

    if top is None:
        return None
    if bottom is None:
        bottom = top
    return top, bottom


# @beacon[
#   id=carto-js-ts@_python_snippet_for_beacon,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: Python beacon snippet template.
# Reference for a future JS/TS beacon helper that will return
# (start, end, lines, core_start, core_end, beacon_line) with the
# core region including both the @beacon[...] comment block and the
# associated JS/TS code span.
def _python_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int]:
    """Return (start_line, end_line, lines, core_start, core_end) for a beacon.

    • The **core region** `[core_start, core_end]` always includes:
        - The associated comment block around the @beacon[...] (if any), and
        - The relevant code span (AST body or span region).

    • The returned `[start_line, end_line]` expands that core region by
      `context` lines above and below, but never shrinks it.

    This matches the Cartographer note behavior: comments that are pulled into
    "CONTEXT COMMENTS (PYTHON)" for a beacon are also included in the core
    snippet, with any further `context` lines landing *outside* that block.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    # Parse AST outline once (for AST beacons) and beacon blocks once (for
    # both AST and SPAN beacons).
    try:
        outline_nodes = nexus_map_codebase.parse_file_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python AST for {file_path}: {e}") from e

    try:
        beacons = nexus_map_codebase.parse_python_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # Helper: lookup comment_line for this beacon id (if present)
    def _comment_line_for_id(bid: str) -> Optional[int]:
        bid = bid.strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if isinstance(cl, int) and cl > 0:
                return cl
        return None

    n = len(lines)

    # 1) AST beacon path – decorate the AST node and include its body as core.
    node = _python_find_ast_node_for_beacon(outline_nodes, beacon_id)
    if node is not None:
        start_code = node.get("orig_lineno_start_unused")
        end_code = node.get("orig_lineno_end_unused")
        if isinstance(start_code, int) and isinstance(end_code, int) and start_code > 0 and end_code >= start_code:
            comment_line = _comment_line_for_id(target)
            core_start = start_code
            if comment_line is not None:
                span = _python_comment_block_span(lines, comment_line)
                if span is not None:
                    core_start = span[0]
                else:
                    core_start = comment_line
            core_end = end_code
            start = max(1, core_start - context)
            end = min(n, core_end + context)
            beacon_line = comment_line if comment_line is not None else start_code
            return start, end, lines, core_start, core_end, beacon_line

    # 2) SPAN beacons – use closing-delimiter span if present, otherwise
    #    single-beacon heuristic with anchor line.

    # First, see if there is a closing-delimiter pair for this id
    span = _python_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        inner_start, inner_end = span
        comment_line = _comment_line_for_id(target)
        if comment_line is not None:
            cb_span = _python_comment_block_span(lines, comment_line)
            if cb_span is not None:
                core_start = cb_span[0]
            else:
                core_start = comment_line
        else:
            core_start = inner_start
        core_end = inner_end
        start = max(1, core_start - context)
        end = min(n, core_end + context)
        beacon_line = comment_line if comment_line is not None else inner_start
        return start, end, lines, core_start, core_end, beacon_line

    # Otherwise, fall back to the original single-beacon heuristic
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in Python file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)

    # Anchor on next non-blank, non-comment, non-decorator line after block
    anchor = None
    j = comment_line
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
        anchor = comment_line

    # Core region: comment block (if any) plus the anchor line
    cb_span = _python_comment_block_span(lines, comment_line)
    if cb_span is not None:
        core_start = cb_span[0]
    else:
        core_start = comment_line
    core_end = anchor

    start = max(1, core_start - context)
    end = min(n, core_end + context)
    beacon_line = comment_line
    return start, end, lines, core_start, core_end, beacon_line


def _js_ts_find_closing_beacon_span(
    lines: List[str],
    beacons: List[dict],
    target_id: str,
) -> Optional[Tuple[int, int]]:
    """Open/close span for JS/TS beacons with a matching id.

    This mirrors the Markdown helper: we use the beacon list to locate the
    opener for a given id, then scan the raw lines for a matching
    @beacon-close[...] block. We no longer support the legacy "two snippet-less
    beacons" pairing; the only supported forms are:

        // @beacon[ id=foo@123, ... ]
        ...
        // @beacon-close[ id=foo@123, ]

    or a single opener without a close, which falls back to the
    single-beacon heuristic in _js_ts_snippet_for_beacon.
    """
    tid = (target_id or "").strip()
    if not tid:
        return None

    # Find the opener in the parsed beacons to get its comment_line.
    open_b: Optional[dict] = None
    for b in beacons:
        bid = (b.get("id") or "").strip()
        if bid != tid:
            continue
        # We only care about snippet-less beacons for open/close semantics.
        if b.get("start_snippet") or b.get("end_snippet"):
            continue
        open_b = b
        break

    if not open_b:
        return None

    open_comment = int(open_b.get("comment_line") or 1)

    def _block_end_lineno(comment_line: int) -> int:
        j = max(1, int(comment_line))
        n = len(lines)
        while j <= n:
            raw = lines[j - 1]
            if "]" in raw:
                return j
            j += 1
        return int(comment_line) or 1

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
            text = text.lstrip("/").lstrip("*").lstrip()
            # Drop trailing ',' or '],' or ']'
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
        if close_id != tid:
            j += 1
            continue

        content_start = open_end + 1
        content_end = close_comment - 1
        if content_start > content_end:
            return None
        return content_start, content_end

    return None


# @beacon[
#   id=carto-js-ts@_js_ts_find_ast_node_for_beacon,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: JS/TS AST node finder for beacons.
# Searches recursively through the outline tree for AST nodes with matching beacon IDs.
def _js_ts_find_ast_node_for_beacon(
    outline_nodes: List[dict],
    beacon_id: str,
) -> Optional[dict]:
    """Find AST node decorated by this beacon ID (JS/TS version).
    
    Searches recursively through the outline tree for a node whose note
    contains the beacon metadata block with matching id.
    """
    def _iter_nodes(nodes: List[dict]) -> Iterable[dict]:
        for node in nodes or []:
            if isinstance(node, dict):
                yield node
                for ch in node.get("children") or []:
                    if isinstance(ch, dict):
                        yield from _iter_nodes([ch])
    
    target = beacon_id.strip()
    for node in _iter_nodes(outline_nodes):
        note = node.get("note") or ""
        if not isinstance(note, str):
            continue
        # Look for beacon metadata in note
        if "BEACON (JS/TS AST)" not in note:
            continue
        # Parse id from note
        for line in note.splitlines():
            stripped = line.strip()
            if stripped.startswith("id:"):
                found_id = stripped.split(":", 1)[1].strip()
                if found_id == target:
                    return node
    return None


# @beacon[
#   id=carto-js-ts@_js_ts_snippet_for_beacon,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: JS/TS beacon snippet helper.
# Returns (start, end, lines, core_start, core_end, beacon_line) for a JS/TS beacon.
def _js_ts_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int]:
    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)
    n = len(lines)

    # Parse AST outline once (for AST beacons) and beacon blocks once (for
    # both AST and SPAN beacons).
    try:
        outline_nodes = nexus_map_codebase.parse_js_ts_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse JS/TS AST for {file_path}: {e}") from e

    try:
        beacons = nexus_map_codebase.parse_js_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse JS/TS beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()

    # Helper: lookup comment_line for this beacon id (if present)
    def _comment_line_for_id(bid: str) -> Optional[int]:
        bid = bid.strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if isinstance(cl, int) and cl > 0:
                return cl
        return None

    def _comment_block_span_js(lines: List[str], comment_line: int) -> Tuple[int, int]:
        j = comment_line
        block_end = comment_line
        while j <= len(lines):
            raw = lines[j - 1]
            if "]" in raw:
                block_end = j
                break
            j += 1
        return comment_line, block_end

    # 1) AST beacon path – decorate the AST node and include its body as core.
    node = _js_ts_find_ast_node_for_beacon(outline_nodes, beacon_id)
    if node is not None:
        start_code = node.get("orig_lineno_start_unused")
        end_code = node.get("orig_lineno_end_unused")
        if isinstance(start_code, int) and isinstance(end_code, int) and start_code > 0 and end_code >= start_code:
            comment_line = _comment_line_for_id(target)
            core_start = start_code
            if comment_line is not None:
                span = _comment_block_span_js(lines, comment_line)
                if span is not None:
                    core_start = span[0]
                else:
                    core_start = comment_line
            core_end = end_code
            start = max(1, core_start - context)
            end = min(n, core_end + context)
            beacon_line = comment_line if comment_line is not None else start_code
            return start, end, lines, core_start, core_end, beacon_line

    # 2) SPAN beacons – use closing-delimiter span if present, otherwise
    #    single-beacon heuristic with anchor line.

    # First, see if there is a closing-delimiter pair for this id
    span = _js_ts_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        content_start, content_end = span
        comment_line = _comment_line_for_id(target)
        if comment_line is not None:
            cb_span = _comment_block_span_js(lines, comment_line)
            if cb_span is not None:
                core_start = cb_span[0]
            else:
                core_start = comment_line
        else:
            core_start = content_start
        core_end = content_end
        start = max(1, core_start - context)
        end = min(n, core_end + context)
        beacon_line = comment_line if comment_line is not None else content_start
        return start, end, lines, core_start, core_end, beacon_line

    # Otherwise, fall back to the original single-beacon heuristic
    chosen: Optional[dict] = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in JS/TS file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)
    cb_start, cb_end = _comment_block_span_js(lines, comment_line)

    # Anchor on next non-blank, non-comment line after the beacon block
    anchor = None
    j = cb_end + 1
    while j <= n:
        raw = lines[j - 1]
        stripped = raw.lstrip()
        if not stripped:
            j += 1
            continue
        if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*") or stripped.startswith("*/"):
            j += 1
            continue
        anchor = j
        break

    if anchor is None:
        anchor = comment_line

    # Core region: comment block (if any) plus the anchor line
    core_start = cb_start
    core_end = anchor
    start = max(1, core_start - context)
    end = min(n, core_end + context)
    beacon_line = comment_line
    return start, end, lines, core_start, core_end, beacon_line


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
            # Strip leading SQL comment prefix if present
            if text.startswith("--"):
                text = text[2:].lstrip()
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


def _markdown_comment_block_span(
    lines: List[str],
    comment_line: int,
) -> Optional[Tuple[int, int]]:
    """Return (top, bottom) of the Markdown HTML-comment block around a beacon.

    Mirrors nexus_map_codebase._extract_markdown_beacon_context but returns
    line indices instead of comment text.
    """

    n = len(lines)
    if comment_line <= 0 or comment_line > n:
        return None

    # Determine end of the beacon metadata block (first line containing ']')
    j = int(comment_line)
    block_end = comment_line
    while j <= n:
        raw = lines[j - 1]
        if "]" in raw:
            block_end = j
            break
        j += 1

    top: Optional[int] = None
    bottom: Optional[int] = None

    # ABOVE
    j = comment_line - 1
    while j >= 1:
        stripped = lines[j - 1].strip()
        if not stripped:
            j -= 1
            continue
        if stripped.startswith("<!--") and "@beacon[" not in stripped:
            if top is None:
                top = j
                bottom = j
            else:
                top = j
            j -= 1
            continue
        break

    # BELOW
    j = block_end + 1
    while j <= n:
        stripped = lines[j - 1].strip()
        if not stripped:
            j += 1
            continue
        if stripped.startswith("<!--") and "@beacon[" not in stripped:
            if top is None:
                top = j
                bottom = j
            else:
                bottom = j
            j += 1
            continue
        break

    if top is None:
        return None
    if bottom is None:
        bottom = top
    return top, bottom


def _markdown_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int]:
    """Return (start_line, end_line, lines, core_start, core_end) for a beacon.

    Uses nexus_map_codebase.parse_markdown_beacon_blocks(), which already
    computes span_lineno_start/span_lineno_end for kind=span beacons. The
    core region is expanded to include the nearby HTML comment block that
    serves as beacon context, so those comments are never dropped even when
    `context` is small.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    lines = _read_lines(file_path)

    try:
        beacons = nexus_map_codebase.parse_markdown_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Markdown beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()
    n = len(lines)

    def _comment_line_for_id(bid: str) -> Optional[int]:
        bid = bid.strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if isinstance(cl, int) and cl > 0:
                return cl
        return None

    # First, try closing-delimiter pairing for snippet-less beacons
    span = _markdown_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        inner_start, inner_end = span
        comment_line = _comment_line_for_id(target)
        if comment_line is not None:
            cb_span = _markdown_comment_block_span(lines, comment_line)
            if cb_span is not None:
                core_start = cb_span[0]
            else:
                core_start = comment_line
        else:
            core_start = inner_start
        core_end = inner_end
        start = max(1, core_start - context)
        end = min(n, core_end + context)
        beacon_line = comment_line if comment_line is not None else inner_start
        return start, end, lines, core_start, core_end, beacon_line

    # No closing tag: fall back to single-beacon semantics.
    # Core span is all lines from the opening beacon comment block down to
    # just before the next Markdown heading at ANY level.
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in Markdown file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)

    # Find end of the beacon block (first line containing ']')
    j = comment_line
    block_end = comment_line
    while j <= n:
        raw = lines[j - 1]
        if "]" in raw:
            block_end = j
            break
        j += 1

    # Next heading at ANY level after the beacon block
    next_header: int | None = None
    import re as _re_md_beacon

    for j in range(block_end + 1, n + 1):
        stripped = lines[j - 1].strip()
        # Heading: one or more '#' characters followed by a space
        if _re_md_beacon.match(r"^#{1,32}\s", stripped):
            next_header = j
            break

    core_start = comment_line
    core_end = next_header - 1 if next_header is not None else n
    if core_end < core_start:
        core_end = core_start

    start = max(1, core_start - context)
    end = min(n, core_end + context)
    beacon_line = comment_line
    return start, end, lines, core_start, core_end, beacon_line


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
            # Strip leading SQL comment prefix if present
            if text.startswith("--"):
                text = text[2:].lstrip()
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


def _sql_comment_block_span(
    lines: List[str],
    comment_line: int,
) -> Optional[Tuple[int, int]]:
    """Return (top, bottom) of the SQL `--` comment block around a beacon."""

    n = len(lines)
    if comment_line <= 0 or comment_line > n:
        return None

    # Determine end of the beacon metadata block (first line containing ']')
    j = int(comment_line)
    block_end = comment_line
    while j <= n:
        raw = lines[j - 1].lstrip()
        if raw.startswith("--"):
            body = raw.lstrip("-").lstrip()
        else:
            body = raw
        if "]" in body:
            block_end = j
            break
        j += 1

    top: Optional[int] = None
    bottom: Optional[int] = None

    # ABOVE
    j = comment_line - 1
    while j >= 1:
        raw = lines[j - 1].lstrip()
        if not raw:
            j -= 1
            continue
        if raw.startswith("--") and "@beacon" not in raw:
            if top is None:
                top = j
                bottom = j
            else:
                top = j
            j -= 1
            continue
        break

    # BELOW
    j = block_end + 1
    while j <= n:
        raw = lines[j - 1].lstrip()
        if not raw:
            j += 1
            continue
        if raw.startswith("--") and "@beacon" not in raw:
            if top is None:
                top = j
                bottom = j
            else:
                bottom = j
            j += 1
            continue
        break

    if top is None:
        return None
    if bottom is None:
        bottom = top
    return top, bottom


def _sql_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int]:
    """Return (start_line, end_line, lines, core_start, core_end) for a SQL beacon.

    We only have span-style beacons in SQL. For v1 we:
      - Use parse_sql_beacon_blocks() to find the beacon block.
      - Treat the comment block around the beacon as part of the core region.
      - For open/close delimiter pairs, the core region covers that inner span
        plus the surrounding comment block; otherwise it is comment-centric.
    """

    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_code_snippet could not be imported")

    lines = _read_lines(file_path)

    try:
        beacons = nexus_map_codebase.parse_sql_beacon_blocks(lines)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse SQL beacon blocks in {file_path}: {e}") from e

    target = beacon_id.strip()
    n = len(lines)

    def _comment_line_for_id(bid: str) -> Optional[int]:
        bid = bid.strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if isinstance(cl, int) and cl > 0:
                return cl
        return None

    # First, try closing-delimiter pairing
    span = _sql_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        inner_start, inner_end = span
        comment_line = _comment_line_for_id(target)
        if comment_line is not None:
            cb_span = _sql_comment_block_span(lines, comment_line)
            if cb_span is not None:
                core_start = cb_span[0]
            else:
                core_start = comment_line
        else:
            core_start = inner_start
        core_end = inner_end
        start = max(1, core_start - context)
        end = min(n, core_end + context)
        beacon_line = comment_line if comment_line is not None else inner_start
        return start, end, lines, core_start, core_end, beacon_line

    # No closing delimiter found: default to symmetric context around the
    # opening beacon comment line (context lines above and below).
    chosen = None
    for b in beacons:
        if (b.get("id") or "").strip() == target:
            chosen = b
            break

    if not chosen:
        raise RuntimeError(f"Beacon id {beacon_id!r} not found in SQL file {file_path!r}")

    comment_line = int(chosen.get("comment_line") or 1)
    core_start = comment_line
    core_end = comment_line
    start = max(1, comment_line - context)
    end = min(n, comment_line + context)
    beacon_line = comment_line
    return start, end, lines, core_start, core_end, beacon_line

# ---------------------------------------------------------------------------
# Public CLI: beacon snippet extraction (print helper)
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


def _sh_comment_block_span(
    lines: List[str],
    comment_line: int,
) -> Optional[Tuple[int, int]]:
    """Return (top, bottom) of the shell '#' comment block around a beacon."""

    n = len(lines)
    if comment_line <= 0 or comment_line > n:
        return None

    # Determine end of the beacon metadata block (first line containing ']')
    j = int(comment_line)
    block_end = comment_line
    while j <= n:
        raw = lines[j - 1].lstrip()
        if raw.startswith("#"):
            body = raw.lstrip("#").lstrip()
        else:
            body = raw
        if "]" in body:
            block_end = j
            break
        j += 1

    top: Optional[int] = None
    bottom: Optional[int] = None

    # ABOVE
    j = comment_line - 1
    while j >= 1:
        raw = lines[j - 1].lstrip()
        if not raw:
            j -= 1
            continue
        if raw.startswith("#") and "@beacon" not in raw:
            if top is None:
                top = j
                bottom = j
            else:
                top = j
            j -= 1
            continue
        break

    # BELOW
    j = block_end + 1
    while j <= n:
        raw = lines[j - 1].lstrip()
        if not raw:
            j += 1
            continue
        if raw.startswith("#") and "@beacon" not in raw:
            if top is None:
                top = j
                bottom = j
            else:
                bottom = j
            j += 1
            continue
        break

    if top is None:
        return None
    if bottom is None:
        bottom = top
    return top, bottom


def _sh_snippet_for_beacon(
    file_path: str,
    beacon_id: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int]:
    """Return (start_line, end_line, lines, core_start, core_end) for a shell beacon.

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
    n = len(lines)

    def _comment_line_for_id(bid: str) -> Optional[int]:
        bid = bid.strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if isinstance(cl, int) and cl > 0:
                return cl
        return None

    # First, try closing-delimiter pairing
    span = _sh_find_closing_beacon_span(lines, beacons, target)
    if span is not None:
        inner_start, inner_end = span
        comment_line = _comment_line_for_id(target)
        if comment_line is not None:
            cb_span = _sh_comment_block_span(lines, comment_line)
            if cb_span is not None:
                core_start = cb_span[0]
            else:
                core_start = comment_line
        else:
            core_start = inner_start
        core_end = inner_end
        start = max(1, core_start - context)
        end = min(n, core_end + context)
        beacon_line = comment_line if comment_line is not None else inner_start
        return start, end, lines, core_start, core_end, beacon_line

# @beacon[
#   id=carto-js-ts@get_snippet_for_ast_qualname,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# Phase 3 JS/TS: AST-qualname snippet template.
# Reference for a future get_snippet_for_ast_qualname_js_ts(...)
# that will locate JS/TS AST nodes by Tree-sitter ast_qualname and
# return snippets with the same (start,end,core,beacon_line,metadata)
# contract.
def get_snippet_for_ast_qualname(
    file_path: str,
    ast_qualname: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    """Resolve snippet for a Python AST node identified by its ast_qualname.

    This is the primary resolution path for non-beacon Python AST nodes.

    Contract:
    - If exactly one AST node in the outline has `ast_qualname == ast_qualname`,
      return a snippet spanning its [start,end] lines with the usual context
      padding.
    - If zero or multiple matches, raise RuntimeError to signal an error to
      the caller (MCP tool should surface this, not silently guess).
    """
    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    ast_qualname = (ast_qualname or "").strip()
    if not ast_qualname:
        raise RuntimeError("Empty ast_qualname for AST snippet resolution")

    lines = _read_lines(file_path)
    n = len(lines)

    try:
        outline_nodes = nexus_map_codebase.parse_file_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse Python AST for {file_path}: {e}") from e

    def _iter_nodes(nodes: Iterable[dict]) -> Iterable[dict]:
        for node in nodes or []:
            if isinstance(node, dict):
                yield node
                for ch in node.get("children") or []:
                    if isinstance(ch, dict):
                        yield from _iter_nodes([ch])

    matches: List[dict] = []
    for node in _iter_nodes(outline_nodes):
        qual = node.get("ast_qualname")
        if isinstance(qual, str) and qual == ast_qualname:
            matches.append(node)

    if not matches:
        raise RuntimeError(
            f"AST_QUALNAME {ast_qualname!r} not found in Python file {file_path!r}"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"AST_QUALNAME {ast_qualname!r} is ambiguous in Python file {file_path!r} "
            f"({len(matches)} matches); refresh Cartographer mapping."
        )

    node = matches[0]
    start = node.get("orig_lineno_start_unused")
    end = node.get("orig_lineno_end_unused") or start
    if not isinstance(start, int) or start <= 0:
        raise RuntimeError(
            f"AST node {ast_qualname!r} has invalid start line {start!r} in {file_path!r}"
        )
    if not isinstance(end, int) or end < start:
        end = start

    core_start = start
    core_end = end
    start_line = max(1, core_start - context)
    end_line = min(n, core_end + context)

    metadata = {
        "resolution_strategy": "ast_qualname_exact",
        "confidence": 1.0,
        "ambiguity": "none",
        "candidates": None,
    }
    return start_line, end_line, lines, core_start, core_end, core_start, metadata


# @beacon[
#   id=carto-js-ts@get_snippet_for_ast_qualname_js_ts,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,
# ]
# JS/TS AST-qualname snippet helper.
# Locates JS/TS AST nodes by ast_qualname and returns a snippet
# using the same contract as the Python version.
def get_snippet_for_ast_qualname_js_ts(
    file_path: str,
    ast_qualname: str,
    context: int,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    if nexus_map_codebase is None:
        raise RuntimeError("nexus_map_codebase could not be imported")

    ast_qualname = (ast_qualname or "").strip()
    if not ast_qualname:
        raise RuntimeError("Empty ast_qualname for AST snippet resolution (JS/TS)")

    lines = _read_lines(file_path)
    n = len(lines)

    try:
        outline_nodes = nexus_map_codebase.parse_js_ts_outline(file_path)  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Failed to parse JS/TS AST for {file_path}: {e}") from e

    def _iter_nodes(nodes: Iterable[dict]) -> Iterable[dict]:
        for node in nodes or []:
            if isinstance(node, dict):
                yield node
                for ch in node.get("children") or []:
                    if isinstance(ch, dict):
                        yield from _iter_nodes([ch])

    matches: List[dict] = []
    for node in _iter_nodes(outline_nodes):
        qual = node.get("ast_qualname")
        if isinstance(qual, str) and qual == ast_qualname:
            matches.append(node)

    if not matches:
        raise RuntimeError(
            f"AST_QUALNAME {ast_qualname!r} not found in JS/TS file {file_path!r}"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"AST_QUALNAME {ast_qualname!r} is ambiguous in JS/TS file {file_path!r} "
            f"({len(matches)} matches); refresh Cartographer mapping."
        )

    node = matches[0]
    start = node.get("orig_lineno_start_unused")
    end = node.get("orig_lineno_end_unused") or start
    if not isinstance(start, int) or start <= 0:
        raise RuntimeError(
            f"AST node {ast_qualname!r} has invalid start line {start!r} in {file_path!r}"
        )
    if not isinstance(end, int) or end < start:
        end = start

    core_start = start
    core_end = end
    start_line = max(1, core_start - context)
    end_line = min(n, core_end + context)

    metadata = {
        "resolution_strategy": "js_ts_ast_qualname_exact",
        "confidence": 1.0,
        "ambiguity": "none",
        "candidates": None,
    }
    return start_line, end_line, lines, core_start, core_end, core_start, metadata


# ---------------------------------------------------------------------------
# Public library API: beacon snippet extraction
# ---------------------------------------------------------------------------


# @beacon[
#   id=carto-js-ts@get_snippet_data,
#   role=carto-js-ts,
#   slice_labels=carto-js-ts,carto-js-ts-snippets,nexus-md-header-path,
#   kind=span,
# ]
# Phase 3 JS/TS: central dispatch point for snippet resolution.
# This will be extended so that .js/.ts/.tsx files route to JS/TS-
# specific helpers alongside the existing Python/Markdown/SQL/SH
# paths.
# Likely new helpers (names tentative):
#   - _js_ts_snippet_for_beacon(file_path, beacon_id, context)
#   - _js_ts_resolve_ast_node_heuristic(file_path, node_name, parent_names, context)
#   - get_snippet_for_ast_qualname_js_ts(file_path, ast_qualname, context)
def get_snippet_data(
    file_path: str,
    beacon_id: str,
    context: int,
    node_name: str | None = None,
    parent_names: List[str] | None = None,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    """Return (start_line, end_line, lines, core_start, core_end, beacon_line, metadata).

    - `start_line` / `end_line` include the requested `context` lines around
      the core region.
    - `core_start` / `core_end` mark the minimal span that always contains the
      associated beacon comment block (if any) and the relevant code span.
    - `beacon_line` is the exact line of the @beacon[...] tag itself (for
      editor navigation independent of context/core) *or* the primary anchor
      line for non-beacon AST nodes.
    - `metadata` describes how the snippet was resolved (AST vs token search).

    Callers that only care about the visible window can ignore `core_*`,
    `beacon_line`, and `metadata`. Callers that want to drive an editor to the
    beacon tag line (e.g., Windsurf `-g file:line`) should use `beacon_line`.
    """
    file_path = os.path.abspath(file_path)
    ext = Path(file_path).suffix.lower()

    if not os.path.isfile(file_path):
        raise RuntimeError(f"Source file not found: {file_path}")

    # Non-beacon AST node path: node_name provided → use AST-first
    # resolver. For beacon-based lookups we call this function without
    # node_name, which triggers the original beacon-centric behavior.
    if ext == ".py" and node_name is not None:
        return _python_resolve_ast_node_heuristic(
            file_path,
            node_name,
            parent_names or [],
            context,
        )
    if ext in {".js", ".jsx", ".ts", ".tsx"} and node_name is not None:
        return _js_ts_resolve_ast_node_heuristic(
            file_path,
            node_name,
            parent_names or [],
            context,
        )

    if ext == ".py":
        start, end, lines, core_start, core_end, beacon_line = _python_snippet_for_beacon(
            file_path, beacon_id, context
        )
    elif ext in {".js", ".jsx", ".ts", ".tsx"}:
        start, end, lines, core_start, core_end, beacon_line = _js_ts_snippet_for_beacon(
            file_path, beacon_id, context
        )
    elif ext in {".md", ".markdown"}:
        start, end, lines, core_start, core_end, beacon_line = _markdown_snippet_for_beacon(
            file_path, beacon_id, context
        )
    elif ext == ".sql":
        start, end, lines, core_start, core_end, beacon_line = _sql_snippet_for_beacon(
            file_path, beacon_id, context
        )
    elif ext == ".sh":
        start, end, lines, core_start, core_end, beacon_line = _sh_snippet_for_beacon(
            file_path, beacon_id, context
        )
    else:
        raise RuntimeError(f"Unsupported file extension for beacon snippets: {ext}")

    # Legacy beacon path: wrap in metadata for callers that expect the
    # enriched return shape.
    metadata = {
        "resolution_strategy": "beacon",
        "confidence": 1.0,
        "ambiguity": "none",
        "candidates": None,
    }
    return start, end, lines, core_start, core_end, beacon_line, metadata


def extract_snippet(file_path: str, beacon_id: str, context: int) -> None:
    """CLI wrapper: resolve snippet and print with line numbers for humans."""
    start, end, lines, _core_start, _core_end, _beacon_line, _meta = get_snippet_data(
        file_path, beacon_id, context
    )
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


# @beacon[
#   id=auto-beacon@get_snippet_for_md_path-8iyg,
#   role=get_snippet_for_md_path,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def get_snippet_for_md_path(
    file_path: str,
    md_path_lines: List[str],
    context: int,
) -> Tuple[int, int, List[str], int, int, int, dict]:
    """Resolve snippet for a Markdown heading node identified by its MD_PATH.

    md_path_lines should be the sequence of heading lines recorded in the
    Workflowy note, e.g.::

        MD_PATH:
        # Top
        ## Section
        ### Subsection
        ---

    The caller must strip the leading ``MD_PATH:`` line and terminating ``---``
    separator and pass only the actual heading lines.

    Contract:
    - If exactly one heading in the file has a computed path whose (level,text)
      chain matches ``md_path_lines`` exactly, return a snippet spanning the
      lines from that heading down to just before the next heading of the same
      or higher level, with standard ``context`` padding.
    - If zero or multiple matches, raise RuntimeError so the MCP layer can
      surface a clear error (we do *not* guess).
    """
    import re
    from markdown_it import MarkdownIt
    from nexus_map_codebase import whiten_text_for_header_compare

    # Read the Markdown file once; reuse these lines for snippet construction.
    lines = _read_lines(file_path)
    n = len(lines)

    # ------------------------------------------------------------------
    # 1. Normalize desired MD_PATH using the same whitening semantics
    #    used by Cartographer for header comparison.
    # ------------------------------------------------------------------
    desired: List[tuple[int, str]] = []
    for raw in md_path_lines:
        s = (raw or "").strip()
        if not s:
            continue
        m = re.match(r"^(#{1,32})\s*(.*)$", s)
        if not m:
            raise RuntimeError(f"Invalid MD_PATH component {raw!r} in note")
        level = len(m.group(1))
        text = (m.group(2) or "").strip()
        # Whiten + casefold so comparisons are robust to markup/emoji/spacing.
        norm = whiten_text_for_header_compare(text).casefold()
        desired.append((level, norm))

    if not desired:
        raise RuntimeError("Empty MD_PATH for Markdown snippet resolution")

    target_depth = len(desired)
    target_level = desired[-1][0]

    # ------------------------------------------------------------------
    # 2. Parse the actual Markdown with markdown-it-py to obtain a
    #    heading stream that matches Cartographer's view of structure.
    # ------------------------------------------------------------------
    md_text = "\n".join(lines)
    md = MarkdownIt("commonmark")
    tokens = md.parse(md_text)

    headings: List[dict[str, Any]] = []
    stack: List[tuple[int, str]] = []  # (level, normalized_text)

    for idx, token in enumerate(tokens):
        if token.type != "heading_open":
            continue

        # Derive heading level from <hN> tag; fall back to level 1 on anomalies.
        try:
            level = int(token.tag[1]) if token.tag and len(token.tag) > 1 else 1
        except Exception:  # noqa: BLE001
            level = 1

        # Heading text is carried by the following inline token.
        text = ""
        if idx + 1 < len(tokens) and tokens[idx + 1].type == "inline":
            text = tokens[idx + 1].content or ""
        norm = whiten_text_for_header_compare(text).casefold()

        # Maintain a stack of (level, norm_text) following the same
        # semantics as Cartographer's MD_PATH generation: the nearest
        # preceding heading with a *lower* level is the parent.
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, norm))

        # Compute the starting line for this heading from token.map when
        # available; fall back to 1-based index 1 if mapping is missing.
        start0 = None
        if getattr(token, "map", None):  # type: ignore[attr-defined]
            start0 = token.map[0]  # 0-based
        elif idx + 1 < len(tokens) and getattr(tokens[idx + 1], "map", None):  # type: ignore[attr-defined]
            start0 = tokens[idx + 1].map[0]
        if start0 is None:
            start0 = 0
        start_line = start0 + 1

        headings.append(
            {
                "level": level,
                "path": list(stack),  # copy of current (level, norm_text) chain
                "start_line": start_line,
            }
        )

    if not headings:
        raise RuntimeError(f"No headings found while resolving MD_PATH for {file_path!r}")

    # ------------------------------------------------------------------
    # 3. Match MD_PATH against the normalized heading paths.
    # ------------------------------------------------------------------
    candidates: List[tuple[int, dict[str, Any]]] = []
    for i, h in enumerate(headings):
        path_chain = h.get("path") or []
        if len(path_chain) != target_depth:
            continue
        if all(
            path_chain[j][0] == desired[j][0] and path_chain[j][1] == desired[j][1]
            for j in range(target_depth)
        ):
            candidates.append((i, h))

    if not candidates:
        raise RuntimeError(
            f"MD_PATH {md_path_lines!r} not found in Markdown file {file_path!r}"
        )
    if len(candidates) > 1:
        raise RuntimeError(
            f"MD_PATH {md_path_lines!r} is ambiguous in Markdown file {file_path!r} "
            f"({len(candidates)} matches); refresh Cartographer mapping."
        )

    match_index, match = candidates[0]
    heading_line = int(match.get("start_line") or 1)
    heading_level = int(match.get("level") or target_level)

    # ------------------------------------------------------------------
    # 4. Core span: from this heading down to just before the next
    #    heading of the same or higher level in the AST stream.
    # ------------------------------------------------------------------
    core_start = heading_line
    core_end = n

    for j in range(match_index + 1, len(headings)):
        other = headings[j]
        other_level = int(other.get("level") or 0)
        other_start = int(other.get("start_line") or 0)
        if other_level <= heading_level and other_start > 0:
            core_end = other_start - 1
            break

    if core_end < core_start:
        core_end = core_start

    start_line = max(1, core_start - context)
    end_line = min(n, core_end + context)

    metadata = {
        "resolution_strategy": "md_path_ast",
        "confidence": 1.0,
        "ambiguity": "none",
        "candidates": None,
    }
    return start_line, end_line, lines, core_start, core_end, core_start, metadata
