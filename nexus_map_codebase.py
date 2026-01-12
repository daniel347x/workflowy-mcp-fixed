import ast
import json
import os
import sys
import argparse
import tempfile
import random
import string
from typing import List, Dict, Any, Optional

import re
import unicodedata
from markdown_it import MarkdownIt
import mdformat

try:
    from tree_sitter import Parser
    from tree_sitter_language_pack import get_language, get_parser
    _HAVE_TREE_SITTER = True
except Exception:
    Parser = None  # type: ignore[assignment]
    get_language = None  # type: ignore[assignment]
    get_parser = None  # type: ignore[assignment]
    _HAVE_TREE_SITTER = False

# Force UTF-8 output for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# --- Configuration (Synced) ---
EMOJI_FOLDER = "📂"
EMOJI_FILE = "📄"
EMOJI_PYTHON = "🐍"
EMOJI_MARKDOWN = "📝"
EMOJI_SQL = "🗄️"
EMOJI_SHELL = "🐚"
EMOJI_CLASS = "📦"
EMOJI_FUNC = "ƒ"
EMOJI_ASYNC = "⚡"
EMOJI_CONST = "💎"
EMOJI_JS = "🟨"
EMOJI_TS = "🟦"

# Debug flags (controlled via environment variables)
DEBUG_MD_BEACONS = bool(os.environ.get("CARTOGRAPHER_MD_BEACONS"))


def _get_line_count(path: str) -> Optional[int]:
    """Return number of lines in a text file, or None on error.

    Used to annotate FILE nodes with LINE COUNT in their Workflowy notes.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return None


def format_file_note(path: str, line_count: int | None = None, sha1: str | None = None) -> str:
    """Format the standard FILE-node note header.

    Shared between Cartographer (map_codebase) and per-file beacon refresh
    (refresh_file_node_beacons) so that FILE nodes always expose a consistent
    header block:

        Path: ...
        LINE COUNT: N
        Source-SHA1: ...

    LINE COUNT and Source-SHA1 are optional and omitted when not provided.
    """
    lines: list[str] = [f"Path: {path}"]
    if line_count is not None:
        lines.append(f"LINE COUNT: {line_count}")
    if sha1 is not None:
        lines.append(f"Source-SHA1: {sha1}")
    return "\n".join(lines)


# --- Parsers ---

def tokens_to_nexus_tree(tokens) -> List[Dict[str, Any]]:
    """Convert markdown-it-py token stream to NEXUS hierarchical tree.
    
    Assigns priority values based on document order (100, 200, 300, ...).
    Lower priority = appears first in document.
    
    v2.1: Now handles horizontal rules (hr) and lists (bullet_list, ordered_list).
    
    v2.2: For each Markdown heading node, writes an explicit MD_PATH block into
    the note:

        MD_PATH:
        # Top
        ## Section
        ### Subsection
        ---
        <existing heading content>

    This mirrors Python's AST_QUALNAME header and is consumed by
    get_snippet_for_md_path in beacon_obtain_code_snippet.py.
    """
    root_children: List[Dict[str, Any]] = []
    # Stack of (heading_level, nexus_node)
    stack: List[tuple[int, Dict[str, Any]]] = []
    priority_counter = 100
    # Accumulates paragraphs, lists, hrs, code blocks as Markdown text
    current_content: List[str] = []

    def flush_content() -> None:
        """Flush accumulated content to current node's note field."""
        if current_content and stack:
            note_text = "\n\n".join(current_content)
            current_node = stack[-1][1]
            if "note" in current_node and current_node["note"]:
                current_node["note"] += "\n\n" + note_text
            else:
                current_node["note"] = note_text
            current_content.clear()

    i = 0
    while i < len(tokens):
        token = tokens[i]

        if token.type == "heading_open":
            # Flush any pending content before starting new heading
            flush_content()

            # Start of a heading
            heading_level = int(token.tag[1])  # h1 -> 1, h2 -> 2, etc.

            # Get the heading text from next token (should be inline)
            heading_text = ""
            if i + 1 < len(tokens) and tokens[i + 1].type == "inline":
                heading_text = tokens[i + 1].content

            # Pop stack until we find the correct parent level for this heading
            while stack and stack[-1][0] >= heading_level:
                stack.pop()

            # Build MD_PATH lines from current ancestor stack + this heading
            md_path_lines: List[str] = []
            for lvl, ancestor_node in stack:
                ancestor_name = (ancestor_node.get("name") or "").strip() or "..."
                md_path_lines.append(f"{('#' * lvl)} {ancestor_name}".rstrip())
            this_heading_name = heading_text or "..."
            md_path_lines.append(f"{('#' * heading_level)} {this_heading_name}".rstrip())

            note_lines: List[str] = ["MD_PATH:"]
            note_lines.extend(md_path_lines)
            note_lines.append("---")
            note_text = "\n".join(note_lines)

            # Create NEXUS node
            nexus_node: Dict[str, Any] = {
                "name": this_heading_name,
                "priority": priority_counter,
                "note": note_text,
                "children": [],
            }
            priority_counter += 100

            # Add to parent (or root if stack empty)
            if stack:
                stack[-1][1]["children"].append(nexus_node)
            else:
                root_children.append(nexus_node)

            # Push this heading onto stack
            stack.append((heading_level, nexus_node))

            # Skip the inline and heading_close tokens
            i += 3  # heading_open, inline, heading_close
            continue

        elif token.type == "hr":
            # Horizontal rule - preserve as underscores (mdformat style)
            current_content.append("______________________________________________________________________")
            i += 1
            continue

        elif token.type == "paragraph_open":
            # Paragraph content
            if i + 1 < len(tokens) and tokens[i + 1].type == "inline":
                current_content.append(tokens[i + 1].content)
            i += 3  # paragraph_open, inline, paragraph_close
            continue

        elif token.type == "fence" or token.type == "code_block":
            # Code block
            fence_char = "`"
            fence_count = 3
            if token.type == "fence" and token.markup:
                fence_char = token.markup[0]
                fence_count = len(token.markup)

            fence = fence_char * fence_count
            current_content.append(f"{fence}{token.info}")
            current_content.append(token.content.rstrip())
            current_content.append(fence)
            i += 1
            continue

        elif token.type == "bullet_list_open" or token.type == "ordered_list_open":
            # Start of a list - collect all list items
            list_lines: List[str] = []
            list_depth = 0
            is_ordered = token.type == "ordered_list_open"

            # Advance past the list_open
            i += 1

            # Process list items until we hit the matching list_close
            while i < len(tokens):
                tok = tokens[i]

                if tok.type == "list_item_open":
                    list_depth += 1
                    i += 1
                    continue

                elif tok.type == "list_item_close":
                    list_depth -= 1
                    i += 1
                    continue

                elif tok.type == "paragraph_open":
                    # List item content (paragraph inside list item)
                    if i + 1 < len(tokens) and tokens[i + 1].type == "inline":
                        content = tokens[i + 1].content
                        # Add bullet/number marker
                        marker = "1." if is_ordered else "-"
                        indent = "  " * max(list_depth - 1, 0)
                        list_lines.append(f"{indent}{marker} {content}")
                    i += 3  # paragraph_open, inline, paragraph_close
                    continue

                elif tok.type == "fence" or tok.type == "code_block":
                    # Code block attached to a list item; render it directly after the bullet.
                    fence_char = "`"
                    fence_count = 3
                    if tok.type == "fence" and tok.markup:
                        fence_char = tok.markup[0]
                        fence_count = len(tok.markup)
                    fence = fence_char * fence_count
                    # Blank line before code block for readability
                    list_lines.append("")
                    list_lines.append(f"{fence}{tok.info}")
                    list_lines.append(tok.content.rstrip())
                    list_lines.append(fence)
                    i += 1
                    continue

                elif tok.type == "bullet_list_close" or tok.type == "ordered_list_close":
                    # End of list
                    i += 1
                    break

                elif tok.type == "bullet_list_open" or tok.type == "ordered_list_open":
                    # Nested list - for now, skip (complex case)
                    # TODO: Handle nested lists properly
                    i += 1
                    continue

                else:
                    i += 1
                    continue

            # Add collected list to content
            if list_lines:
                current_content.append("\n".join(list_lines))

            continue

        # Skip other token types we don't handle yet
        i += 1

    # Flush any remaining content
    flush_content()

    return root_children


# @beacon[
#   id=carto-js-ts@parse_markdown_beacon_blocks,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: Markdown beacon parser template.
# Reference for JS/TS block-comment beacons (/* @beacon[...] */),
# which use the same metadata fields and parsing pattern.
def parse_markdown_beacon_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Parse @beacon[...] HTML comment blocks from Markdown source.

    Expected form (we control this schema):

        <!--
        @beacon[
          id=docs:intro@1234,
          role=doc:intro,
          slice_labels=DOC,
          kind=span,
          start_snippet="# Heading text",
          comment=One-line human note,
        ]
        -->

    Returns a list of dicts with keys:
        id, role, slice_labels, kind, start_snippet, end_snippet,
        comment, comment_line, span_lineno_start, span_lineno_end,
        kind_explicit.
    """
    beacons: list[dict[str, Any]] = []
    n = len(lines)
    i = 0

    while i < n:
        line = lines[i]
        if "@beacon[" in line:
            comment_line = i + 1  # 1-based
            block_lines = [line]
            i += 1
            # Collect until a line containing ']' (inclusive)
            while i < n:
                block_lines.append(lines[i])
                if "]" in lines[i]:
                    i += 1
                    break
                i += 1

            # Parse block_lines into key/value pairs (skip first/last structural lines)
            fields: dict[str, str] = {}
            inner_lines = block_lines[1:-1] if len(block_lines) >= 2 else []
            for raw in inner_lines:
                text = raw.strip()
                if not text or text.startswith("@beacon"):
                    continue
                if text.startswith("<!--") or text.startswith("-->"):
                    continue
                # Drop trailing ',' or '],' or ']'
                while text and text[-1] in ",]":
                    text = text[:-1].rstrip()
                if not text or "=" not in text:
                    continue
                key, val = text.split("=", 1)
                key = key.strip()
                val = val.strip()
                # Strip surrounding quotes if present
                if (val.startswith("\"") and val.endswith("\"")) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                fields[key] = val

            kind_raw = fields.get("kind")
            kind = (kind_raw or "span").strip().lower() or "span"
            if kind not in {"ast", "span"}:
                continue

            start_snippet = fields.get("start_snippet")
            end_snippet = fields.get("end_snippet")

            span_start: Optional[int] = None
            span_end: Optional[int] = None

            if kind == "span" and start_snippet:
                norm_snip = normalize_for_match(start_snippet)
                if norm_snip:
                    # Find candidate start lines (global search), but IGNORE
                    # lines that are clearly part of the beacon metadata
                    # itself (the @beacon block and its key/value lines).
                    start_candidates: list[int] = []
                    for idx, src_line in enumerate(lines, start=1):
                        # Skip any line that looks like part of a @beacon
                        # comment block so we never anchor spans to their
                        # own metadata.
                        if "@beacon[" in src_line or "start_snippet=" in src_line or "end_snippet=" in src_line:
                            continue
                        if norm_snip in normalize_for_match(src_line):
                            start_candidates.append(idx)

                    if start_candidates:
                        if len(start_candidates) == 1:
                            span_start = start_candidates[0]
                        else:
                            after = [ln for ln in start_candidates if ln > comment_line]
                            if len(after) == 1:
                                span_start = after[0]
                            else:
                                # Ambiguous; require refinement
                                span_start = None

                    if span_start is not None:
                        if end_snippet:
                            norm_end = normalize_for_match(end_snippet)
                            for idx in range(span_start, len(lines) + 1):
                                if norm_end in normalize_for_match(lines[idx - 1]):
                                    span_end = idx
                                    break
                            if span_end is None:
                                # Explicit end_snippet but no match – leave span unset
                                span_start = None
                        else:
                            # Default: until just before next header (any level)
                            span_end = len(lines)
                            for idx in range(span_start + 1, len(lines) + 1):
                                if re.match(r"^#{1,6}\s", lines[idx - 1]):
                                    span_end = idx - 1
                                    break

            beacon = {
                "id": fields.get("id"),
                "role": fields.get("role"),
                "slice_labels": fields.get("slice_labels"),
                "kind": kind,
                "start_snippet": start_snippet,
                "end_snippet": end_snippet,
                "comment": fields.get("comment"),
                "comment_line": comment_line,
                "span_lineno_start": span_start,
                "span_lineno_end": span_end,
                "kind_explicit": bool(kind_raw),
            }
            beacons.append(beacon)
        else:
            i += 1

    return beacons


def _extract_markdown_beacon_context(lines: list[str], comment_line: int) -> List[str]:
    """Extract nearby HTML comment lines (non-beacon) around a Markdown beacon.

    We look above and below the beacon's comment_line for HTML comments
    (<!-- ... -->) that do not contain @beacon[. Blank lines are skipped.
    """
    context: List[str] = []

    # Above
    j = comment_line - 1
    while j >= 1:
        stripped = lines[j - 1].strip()
        if not stripped:
            j -= 1
            continue
        if stripped.startswith("<!--") and "@beacon[" not in stripped:
            # Skip pure sentinel lines like "<!--" and "-->", but keep
            # single-line comments that actually contain text.
            if stripped != "<!--" and stripped != "-->":
                context.insert(0, stripped)
            j -= 1
            continue
        break

    # Below
    j = comment_line + 1
    while j <= len(lines):
        stripped = lines[j - 1].strip()
        if not stripped:
            j += 1
            continue
        if stripped.startswith("<!--") and "@beacon[" not in stripped:
            if stripped != "<!--" and stripped != "-->":
                context.append(stripped)
            j += 1
            continue
        break

    return context


# @beacon[
#   id=carto-js-ts@apply_markdown_beacons,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: Markdown beacon application template.
# Reference for apply_js_beacons(...) when attaching JS/TS span
# and AST beacons under headings / AST nodes with context comments.
def apply_markdown_beacons(
    lines: list[str],
    root_children: List[Dict[str, Any]],
    beacons: list[dict[str, Any]],
) -> None:
    """Attach Markdown beacons under headings or decorate headings directly.

    V2:
      - Supports both kind=span and kind=ast beacons.
      - kind=ast beacons decorate the nearest heading node's name and note.
      - kind=span beacons remain child nodes under the nearest enclosing
        heading (or at the top level if no heading is found).

    Auto-promotion:
      - For beacons where ``kind`` is *not* explicitly set and the first
        non-blank, non-HTML-comment line after the beacon block is a heading
        (e.g. a line starting with one or more ``#`` followed by a space),
        we auto-upgrade ``kind`` from ``span`` to ``ast``.
    """
    if not beacons or not root_children:
        return

    # Normalize kind and optionally auto-promote some span beacons to AST
    # based on their position relative to the next heading.
    for beacon in beacons:
        kind = (beacon.get("kind") or "span").strip().lower() or "span"
        beacon["kind"] = kind
        if kind != "span":
            continue
        # Only auto-promote when the user did not explicitly specify kind.
        if beacon.get("kind_explicit"):
            continue
        comment_line = beacon.get("comment_line") or 0
        if not isinstance(comment_line, int) or comment_line <= 0:
            continue
        # Find end of the @beacon[...] block (first line containing ']')
        block_end = comment_line
        k = comment_line
        while k <= len(lines):
            if "]" in lines[k - 1]:
                block_end = k
                # If the very next line closes the same HTML comment block,
                # treat that "-->" line as part of the beacon block so that
                # auto-promotion starts scanning AFTER the entire comment.
                if block_end < len(lines) and "-->" in lines[block_end]:
                    block_end += 1
                break
            k += 1
        j = block_end + 1
        in_html_comment = False
        while j <= len(lines):
            raw = lines[j - 1]
            stripped = raw.strip()
            if not stripped:
                j += 1
                continue
            # Track and skip non-beacon HTML comment blocks (multi-line), so
            # we only consider real content lines when deciding whether to
            # auto-promote this beacon to an AST beacon.
            if not in_html_comment and "<!--" in raw and "@beacon[" not in raw:
                in_html_comment = True
            if in_html_comment:
                if "-->" in raw:
                    in_html_comment = False
                j += 1
                continue
            # First significant line after the beacon block: if it's a
            # heading of any level (we accept up to 32 '#' chars for
            # extended Markdown), upgrade this beacon to AST.
            if re.match(r"^#{1,32}\s", stripped):
                beacon["kind"] = "ast"
            break

    def iter_nodes(nodes: List[Dict[str, Any]]):
        for node in nodes:
            yield node
            for ch in node.get("children") or []:
                if isinstance(ch, dict):
                    yield from iter_nodes([ch])

    all_nodes = list(iter_nodes(root_children))

    # Precompute heading positions (line numbers) in the Markdown source so
    # we can attach beacons relative to concrete headings.
    heading_positions: list[tuple[Dict[str, Any], int]] = []

    def _find_heading_lineno(heading_text: str) -> Optional[int]:
        """Find the line number of a Markdown heading whose text matches heading_text.

        We treat any line starting with 1–6 '#' characters followed by optional
        whitespace and then exactly heading_text as a match. This avoids regex
        edge-cases with f-strings and keeps the logic transparent.
        """
        if not heading_text:
            return None
        for idx, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped.startswith("#"):
                if DEBUG_MD_BEACONS:
                    print(
                        f"[MD-HEAD-CHECK] heading={heading_text!r} line={idx} text={stripped!r} (no # prefix)"
                    )
                continue
            # Count leading '#' characters
            hash_count = 0
            for ch in stripped:
                if ch == "#":
                    hash_count += 1
                else:
                    break
            if not (1 <= hash_count <= 6):
                if DEBUG_MD_BEACONS:
                    print(
                        f"[MD-HEAD-CHECK] heading={heading_text!r} line={idx} text={stripped!r} (hash_count={hash_count} out of range)"
                    )
                continue
            rest = stripped[hash_count:].lstrip()
            if DEBUG_MD_BEACONS:
                print(f"[MD-HEAD-CHECK] heading={heading_text!r} line={idx} rest={rest!r}")
            if rest == heading_text:
                if DEBUG_MD_BEACONS:
                    print(f"[MD-HEAD] name={heading_text!r} lineno={idx}")
                return idx
        return None

    for node in all_nodes:
        name = (node.get("name") or "").strip()
        if not name:
            continue
        ln = _find_heading_lineno(name)
        if ln is not None:
            heading_positions.append((node, ln))

    if DEBUG_MD_BEACONS and heading_positions:
        print("[MD-HEAD-LIST] headings:")
        for h_node, h_ln in heading_positions:
            print(f"  - name={h_node.get('name')!r} lineno={h_ln}")

    # PASS 1: AST beacons – decorate heading nodes.
    for beacon in beacons:
        if beacon.get("kind") != "ast":
            continue

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        # Defaults: derive role from id prefix (before '@')
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "md-ast-beacon"
        display_slice = slice_labels or "-"
        tag_suffix = _slice_label_tags(slice_labels or display_slice)

        comment_line = beacon.get("comment_line") or 0
        owner: Optional[Dict[str, Any]] = None

        # Preferred: nearest heading *after* the beacon block.
        if isinstance(comment_line, int) and comment_line > 0 and heading_positions:
            best_node: Optional[Dict[str, Any]] = None
            best_lineno: Optional[int] = None
            for node, h_lineno in heading_positions:
                if h_lineno > comment_line and (best_lineno is None or h_lineno < best_lineno):
                    best_lineno = h_lineno
                    best_node = node
            if best_node is not None:
                owner = best_node

        # Fallback: use start_snippet text if it looks like a header line.
        if owner is None:
            start_snippet = beacon.get("start_snippet") or ""
            header_text = None
            m = (
                re.match(r"^#{1,32}\\s*(.*)", start_snippet.strip())
                if start_snippet
                else None
            )
            if m:
                header_text = m.group(1).strip()

            if header_text:
                norm_target = normalize_for_match(header_text)
                for node in all_nodes:
                    node_name = (node.get("name") or "")
                    node_norm = normalize_for_match(node_name)
                    if node_norm == norm_target or (
                        norm_target and norm_target in node_norm
                    ):
                        owner = node
                        break

        if owner is None:
            # If we couldn't find a heading to decorate, fall back to SPAN
            # semantics by resetting kind; the SPAN pass will handle it.
            beacon["kind"] = "span"
            continue

        name = owner.get("name") or "Untitled"
        if "🔱" not in name:
            name = f"{name} 🔱"
        if tag_suffix:
            name = f"{name} {tag_suffix}"
        owner["name"] = name

        context_lines = _extract_markdown_beacon_context(
            lines, beacon.get("comment_line") or 0
        )
        comment = beacon.get("comment")

        meta_lines = [
            "BEACON (MD AST)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: ast",
        ]
        if comment:
            meta_lines.append(f"comment: {comment}")
        if context_lines:
            meta_lines.append("")
            meta_lines.append("CONTEXT COMMENTS (MD):")
            meta_lines.extend(context_lines)
        # Visual separator between beacon metadata and body text
        meta_lines.append("---")

        note = owner.get("note") or ""
        meta_block = "\n".join(meta_lines)
        if not note:
            owner["note"] = meta_block
        else:
            # Insert beacon metadata block just after the MD_PATH header
            # (the first line that is exactly '---' following 'MD_PATH:').
            lines_note = note.splitlines()
            insert_idx = len(lines_note)
            saw_md_path = False
            for idx, line in enumerate(lines_note):
                if line.strip() == "MD_PATH:":
                    saw_md_path = True
                elif saw_md_path and line.strip() == "---":
                    insert_idx = idx + 1
                    break
            new_lines: list[str] = []
            new_lines.extend(lines_note[:insert_idx])
            # Ensure a blank line before the beacon block when joining
            if insert_idx < len(lines_note) and lines_note[insert_idx].strip():
                new_lines.append("")
            new_lines.append(meta_block)
            # Blank line between beacon block and any existing body text
            if insert_idx < len(lines_note):
                if lines_note[insert_idx].strip():
                    new_lines.append("")
                new_lines.extend(lines_note[insert_idx:])
            owner["note"] = "\n".join(new_lines)

    # PASS 2: SPAN beacons – unchanged behavior, with optional comment support.
    for beacon in beacons:
        if beacon.get("kind") != "span":
            continue

        start_snippet = beacon.get("start_snippet") or ""
        span_start = beacon.get("span_lineno_start")
        span_end = beacon.get("span_lineno_end")
        # Fallback for snippet-less beacons: anchor to the line after the beacon block
        if span_start is None:
            comment_line = beacon.get("comment_line") or 0
            if isinstance(comment_line, int) and comment_line > 0:
                span_start = comment_line + 1

        # Derive heading text from start_snippet if it looks like a Markdown header
        header_text = None
        m = re.match(r"^#{1,6}\\s*(.*)", start_snippet.strip()) if start_snippet else None
        if m:
            header_text = m.group(1).strip()

        # Prefer the nearest preceding heading in the actual Markdown source,
        # using span_lineno_start as the anchor. This makes placement robust even
        # if start_snippet is not a header line.
        owner_header_text = None
        if isinstance(span_start, int):
            for idx in range(span_start, 0, -1):
                line = lines[idx - 1].strip()
                m2 = re.match(r"^#{1,6}\\s*(.*)", line)
                if m2:
                    owner_header_text = m2.group(1).strip()
                    break

        target_header = owner_header_text or header_text

        owner: Optional[Dict[str, Any]] = None

        # Preferred: line-number based nearest-previous-heading resolution.
        if isinstance(span_start, int) and heading_positions:
            best_node = None
            best_lineno = -1
            for node, h_lineno in heading_positions:
                if h_lineno <= span_start and h_lineno > best_lineno:
                    best_lineno = h_lineno
                    best_node = node
            if best_node is not None:
                owner = best_node

        # Fallback: header-text based resolution when line-based match fails.
        if owner is None and target_header:
            norm_target = normalize_for_match(target_header)
            for node in all_nodes:
                node_name = (node.get("name") or "")
                node_norm = normalize_for_match(node_name)
                if node_norm == norm_target or (
                    norm_target and norm_target in node_norm
                ):
                    owner = node
                    break

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        # Defaults: derive role from id prefix (before '@')
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        if DEBUG_MD_BEACONS:
            print(
                "[MD-BEACON] id=%r span_start=%r target_header=%r owner=%r" % (
                    b_id,
                    span_start,
                    target_header,
                    (owner.get("name") if owner else None),
                )
            )

        display_role = role or b_id or "md-span-beacon"
        display_slice = slice_labels or "-"

        tag_suffix = _slice_label_tags(slice_labels or display_slice)
        name = f"🔱 {display_role}"
        if tag_suffix:
            name = f"{name} {tag_suffix}"

        context_lines = _extract_markdown_beacon_context(
            lines, beacon.get("comment_line") or 0
        )
        comment = beacon.get("comment")

        note_lines = [
            "BEACON (MD SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
        if comment:
            note_lines.append(f"comment: {comment}")
        if context_lines:
            note_lines.append("")
            note_lines.append("CONTEXT COMMENTS (MD):")
            note_lines.extend(context_lines)

        span_node = {
            "name": name,
            "note": "\n".join(note_lines),
            "children": [],
        }

        if owner is not None:
            children = owner.get("children") or []
            children.append(span_node)
            owner["children"] = children
        else:
            root_children.append(span_node)


def parse_markdown_structure(file_path: str) -> List[Dict[str, Any]]:
    """Parses Markdown into NEXUS node tree using markdown-it-py.
    
    CRITICAL CHANGES (v2.0):
    1. Runs mdformat FIRST to normalize Markdown (fixes nested code blocks, etc.)
    2. Uses markdown-it-py parser (battle-tested, CommonMark compliant)
    3. Assigns priority values based on document order (preserves ordering)
    4. Converts token stream to hierarchical NEXUS tree
    
    NO JUNK ALLOWED - mdformat enforces opinionated style before parsing.
    """
    try:
        # STEP 1: Format the Markdown with mdformat FIRST (normalize before parsing)
        # This auto-repairs nested code blocks, whitespace, etc.
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        formatted_content = mdformat.text(original_content)
        
        # Optional debug: show formatted Markdown with line numbers
        if DEBUG_MD_BEACONS:
            print(f"[MD-FILE] path={file_path!r}")
            print("[MD-FORMATTED-BEGIN]")
            for idx, line in enumerate(formatted_content.splitlines(), start=1):
                print(f"{idx:4d}: {line!r}")
            print("[MD-FORMATTED-END]")
        
        # STEP 1.5: Write formatted content BACK to source file (opinionated cleanup)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(formatted_content)
        
        lines = formatted_content.splitlines()
        md_beacons = parse_markdown_beacon_blocks(lines)
        
        # STEP 2: Parse formatted Markdown with markdown-it-py
        md = MarkdownIt("commonmark")
        tokens = md.parse(formatted_content)

        if DEBUG_MD_BEACONS:
            print("[MD-TOKENS-BEGIN]")
            for t in tokens:
                # t.map is (start_line, end_line) 0-based; add 1 for human-readable
                line_span = t.map if hasattr(t, 'map') and t.map is not None else None
                if line_span:
                    span_str = f"[{line_span[0]+1},{line_span[1]+1}]"
                else:
                    span_str = "[]"
                print(f"  type={t.type!r} tag={t.tag!r} map={span_str} content={t.content!r}")
            print("[MD-TOKENS-END]")
        
        # STEP 3: Convert token stream to NEXUS hierarchical tree with priorities
        root_children = tokens_to_nexus_tree(tokens)
        if md_beacons:
            apply_markdown_beacons(lines, root_children, md_beacons)
        return root_children
        
    except Exception as e:
        return [{"name": "⚠️ MD Parse Error", "note": str(e), "children": []}]

def normalize_for_match(s: str) -> str:
    """Normalize a snippet or line for matching.

    V1: remove all whitespace characters so that `x=3` matches `x = 3`.
    This intentionally trades a tiny false-positive risk for robustness
    against formatting-only changes.
    """
    if not isinstance(s, str):
        return ""
    return "".join(ch for ch in s if not ch.isspace())


_TAG_LIKE_PATTERN = re.compile(r"<[A-Za-z_/][-A-Za-z0-9_/]*>")


def whiten_text_for_header_compare(text: str | None) -> str:
    """Whiten heading-like text for structural comparison.

    Used by Cartographer/F12/MD_PATH flows to decide whether two headings are
    "semantically the same" while preserving trailing #tags.

    Rules:
    - Strip simple HTML tags like <code>, </code>, <span>, </span> by pattern:
      <[A-Za-z_/][-A-Za-z0-9_/]*>
      (no whitespace or digits at the start, so "a < 5" is untouched).
    - Drop all backticks.
    - Drop emoji / Symbol, Other (So) codepoints.
    - Drop all whitespace.
    - Do NOT strip '#' or words, so tag changes still show up as differences.
    """
    if not isinstance(text, str):
        return ""
    s = text

    # Remove simple tag-like markup such as <code>, </code>, <span>, </span>.
    s = _TAG_LIKE_PATTERN.sub("", s)

    # Drop backticks used for inline code in Markdown.
    s = s.replace("`", "")

    # Normalize a small set of common HTML entities so textual comparisons
    # are robust to &lt;/&gt;/&amp;/&quot; vs their literal forms.
    html_entity_map = {
        "&quot;": '"',
        "&lt;": "<",
        "&gt;": ">",
        "&amp;": "&",
    }
    for ent, ch in html_entity_map.items():
        if ent in s:
            s = s.replace(ent, ch)

    # Drop double quotes entirely (including those produced from &quot;).
    s = s.replace('"', "")

    # Remove emoji / other symbolic decoration.
    s = "".join(ch for ch in s if unicodedata.category(ch) != "So")

    # Collapse and remove whitespace entirely.
    s = re.sub(r"\s+", "", s)
    return s


def _extract_beacon_id_from_note(note: str | None) -> str | None:
    """Extract beacon `id:` field from a node's note, if present.

    We deliberately do not try to distinguish AST vs SPAN kinds here; the
    id is expected to be unique within a single file's Cartographer subtree.
    """
    if not isinstance(note, str) or "BEACON (" not in note:
        return None
    for line in note.splitlines():
        stripped = line.strip()
        if stripped.startswith("id:"):
            val = stripped.split(":", 1)[1].strip()
            return val or None
    return None


def _extract_ast_qualname_from_note(note: str | None) -> str | None:
    """Extract AST_QUALNAME from a node's note, if present."""
    if not isinstance(note, str):
        return None
    for line in note.splitlines():
        stripped = line.strip()
        if stripped.startswith("AST_QUALNAME:"):
            val = stripped.split(":", 1)[1].strip()
            return val or None
    return None


def _extract_md_path_from_note(note: str | None) -> list[str] | None:
    """Extract MD_PATH heading lines from a Markdown node's note, if any.

    Returns the raw heading lines (including leading '#' characters) between
    the 'MD_PATH:' sentinel and the terminating '---' line.
    """
    if not isinstance(note, str):
        return None
    lines = note.splitlines()
    in_block = False
    path_lines: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if not in_block:
            if stripped == "MD_PATH:":
                in_block = True
            continue
        if stripped == "---":
            break
        if stripped:
            # Preserve the heading text as written; callers may whiten it.
            path_lines.append(stripped)
    return path_lines or None


def reconcile_trees_cartographer(source_node: Dict[str, Any], ether_node: Dict[str, Any]) -> None:
    """Reconcile a Cartographer-derived subtree against an existing ETHER tree.

    This variant is used for F12 / per-file Cartographer refresh. It prefers
    identity anchors over raw name matching:

      1. Beacon id (`id:` under BEACON (AST|SPAN)).
      2. AST_QUALNAME (Python AST nodes).
      3. MD_PATH header sequence for Markdown headings (each line whitened via
         ``whiten_text_for_header_compare``).
      4. Fallback: raw name equality.

    For nodes that are matched (i.e., share identity and thus an id), we then
    compare name and note using ``whiten_text_for_header_compare`` and, when
    they are semantically equal, we *restore* the ETHER value into the source
    tree. This lets the downstream WEAVE engine skip spurious updates caused
    purely by Workflowy whitening or formatting differences, while still
    propagating real changes (e.g. new tags or beacon metadata).
    """
    # Seed id on the root when present.
    if isinstance(ether_node, dict) and "id" in ether_node:
        source_node["id"] = ether_node["id"]

    source_children = [c for c in (source_node.get("children") or []) if isinstance(c, dict)]
    ether_children = [c for c in (ether_node.get("children") or []) if isinstance(c, dict)]

    if not source_children or not ether_children:
        return

    # Build Ether maps by identity anchor.
    ether_by_beacon: dict[str, dict[str, Any]] = {}
    ether_by_ast: dict[str, dict[str, Any]] = {}
    ether_by_mdpath: dict[tuple[str, ...], dict[str, Any]] = {}
    ether_by_name: dict[str, dict[str, Any]] = {}

    # CARTO-DEBUG targeting is disabled by default; set this to a real
    # function name when focused instrumentation is needed.
    TARGET_FUNC = "__CARTO_DEBUG_TARGET_UNUSED__"
    import sys as _carto_debug_sys

    for e in ether_children:
        note_e = e.get("note") or ""
        name_e = e.get("name") or ""
        # 1) Beacon id
        b_id = _extract_beacon_id_from_note(note_e)
        if b_id and b_id not in ether_by_beacon:
            ether_by_beacon[b_id] = e
        # 2) AST_QUALNAME
        qual = _extract_ast_qualname_from_note(note_e)
        if qual and qual not in ether_by_ast:
            ether_by_ast[qual] = e
        # 3) MD_PATH
        md_path = _extract_md_path_from_note(note_e)
        if md_path:
            key = tuple(whiten_text_for_header_compare(line) for line in md_path if line.strip())
            if key and key not in ether_by_mdpath:
                ether_by_mdpath[key] = e
        # 4) Raw name fallback
        if isinstance(name_e, str) and name_e not in ether_by_name:
            ether_by_name[name_e] = e

        # Debug: log any ETHER child that looks like our target function
        if (
            isinstance(name_e, str)
            and TARGET_FUNC in name_e
        ) or (
            isinstance(qual, str)
            and qual == TARGET_FUNC
        ):
            print(
                "[CARTO-DEBUG] ether child candidate for TARGET_FUNC:",
                {"name": name_e, "id": e.get("id"), "qual": qual},
                file=_carto_debug_sys.stderr,
                flush=True,
            )

    for s in source_children:
        note_s = s.get("note") or ""
        name_s = s.get("name") or ""
        match: dict[str, Any] | None = None

        b_id_s = _extract_beacon_id_from_note(note_s)
        qual_s = _extract_ast_qualname_from_note(note_s)
        debug_hit = (
            (isinstance(name_s, str) and TARGET_FUNC in name_s)
            or (isinstance(qual_s, str) and qual_s == TARGET_FUNC)
        )

        if debug_hit:
            print("[CARTO-DEBUG] SOURCE child before matching:", file=_carto_debug_sys.stderr, flush=True)
            print(
                "[CARTO-DEBUG]   name_s=", repr(name_s),
                "qual_s=", repr(qual_s),
                "b_id_s=", repr(b_id_s),
                file=_carto_debug_sys.stderr,
                flush=True,
            )
            print(
                "[CARTO-DEBUG]   ether_by_ast keys=",
                list(ether_by_ast.keys()),
                file=_carto_debug_sys.stderr,
                flush=True,
            )
            print(
                "[CARTO-DEBUG]   ether_by_beacon keys=",
                list(ether_by_beacon.keys()),
                file=_carto_debug_sys.stderr,
                flush=True,
            )

        # 1) Beacon id
        if b_id_s and b_id_s in ether_by_beacon:
            match = ether_by_beacon[b_id_s]
            if debug_hit:
                print(
                    "[CARTO-DEBUG]   MATCH via BEACON id:",
                    {"b_id_s": b_id_s, "match_id": match.get("id")},
                    file=_carto_debug_sys.stderr,
                    flush=True,
                )
        else:
            # 2) AST_QUALNAME
            if qual_s and qual_s in ether_by_ast:
                match = ether_by_ast[qual_s]
                if debug_hit:
                    print(
                        "[CARTO-DEBUG]   MATCH via AST_QUALNAME:",
                        {"qual_s": qual_s, "match_id": match.get("id")},
                        file=_carto_debug_sys.stderr,
                        flush=True,
                    )
            else:
                # 3) MD_PATH (whitened per-line comparison)
                md_path_s = _extract_md_path_from_note(note_s)
                if md_path_s:
                    key_s = tuple(
                        whiten_text_for_header_compare(line) for line in md_path_s if line.strip()
                    )
                    if key_s and key_s in ether_by_mdpath:
                        match = ether_by_mdpath[key_s]
                        if debug_hit:
                            print(
                                "[CARTO-DEBUG]   MATCH via MD_PATH:",
                                {"key_s": key_s, "match_id": match.get("id")},
                                file=_carto_debug_sys.stderr,
                                flush=True,
                            )
                # 4) Raw name fallback
                if match is None and isinstance(name_s, str) and name_s in ether_by_name:
                    match = ether_by_name[name_s]
                    if debug_hit:
                        print(
                            "[CARTO-DEBUG]   MATCH via raw name:",
                            {"name_s": name_s, "match_id": match.get("id")},
                            file=_carto_debug_sys.stderr,
                            flush=True,
                        )

        if match is None:
            if debug_hit:
                print("[CARTO-DEBUG]   NO MATCH for source child", file=_carto_debug_sys.stderr, flush=True)
            # No identity match at this level; we do not attempt to force ids.
            continue

        # Seed id from the matched Ether node.
        if "id" in match:
            s["id"] = match["id"]
            if debug_hit:
                print(
                    "[CARTO-DEBUG]   Seeded id from match:",
                    {"seed_id": s["id"]},
                    file=_carto_debug_sys.stderr,
                    flush=True,
                )

        # Whiten-based no-op suppression: if name/note are semantically equal
        # after whitening, preserve the ETHER values so WEAVE sees no change.
        name_e = match.get("name")
        if isinstance(name_s, str) and isinstance(name_e, str):
            if whiten_text_for_header_compare(name_s) == whiten_text_for_header_compare(name_e):
                s["name"] = name_e
        note_e = match.get("note")
        if isinstance(note_s, str) and isinstance(note_e, str):
            if whiten_text_for_header_compare(note_s) == whiten_text_for_header_compare(note_e):
                s["note"] = note_e

        if debug_hit:
            print(
                "[CARTO-DEBUG]   FINAL source child after matching:",
                {"name": s.get("name"), "id": s.get("id"), "note": s.get("note")},
                file=_carto_debug_sys.stderr,
                flush=True,
            )

        # Recurse into children with the same Cartographer-aware logic.
        reconcile_trees_cartographer(s, match)


def _slice_label_tags(slice_labels: str) -> str:
    """Convert a slice_labels string into a space-delimited #tag list.

    Examples:
        "DOC"        -> "#DOC"
        "FP,DA"      -> "#FP #DA"
        "doc-intro"  -> "#doc-intro"

    We split on commas and whitespace to support "FP, DA" or "FP DA" forms.
    """
    if not isinstance(slice_labels, str):
        return ""
    labels = []
    for part in re.split(r"[\s,]+", slice_labels):
        part = part.strip()
        if not part:
            continue
        labels.append(f"#{part}")
    return " ".join(labels)


def _canonicalize_slice_labels(slice_labels: str | None, role: str | None) -> str:
    """Canonicalize slice_labels for tags and display.

    - If slice_labels is empty and role is provided, derive labels from role.
    - Replaces ':', '.', and other non-tag-safe characters with '-' to ensure
      Workflowy tags render correctly (e.g. model:mert:production → model-mert-production,
      Class.method → Class-method).
    - Accepts comma/whitespace separated labels and normalizes each token.
    """
    raw = (slice_labels or "").strip()
    base_role = (role or "").strip()
    tokens: list[str] = []

    if raw:
        for part in re.split(r"[\s,]+", raw):
            part = part.strip()
            if not part:
                continue
            # Replace dots, colons, and other non-tag-safe chars with hyphens.
            sanitized = part.replace(":", "-").replace(".", "-")
            # Also replace slashes, spaces, and other problematic characters.
            sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", sanitized)
            sanitized = sanitized.strip("-")
            if sanitized:
                tokens.append(sanitized)
    elif base_role:
        # Apply same sanitization to role-derived labels.
        sanitized = base_role.replace(":", "-").replace(".", "-")
        sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", sanitized)
        sanitized = sanitized.strip("-")
        if sanitized:
            tokens.append(sanitized)

    return ",".join(tokens)


# @beacon[
#   id=carto-js-ts@parse_python_beacon_blocks,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: Python beacon parser template.
# Reference for parse_js_beacon_blocks(...) (JS/TS @beacon[...] parser)
# so JS/TS beacon metadata matches the existing schema (id, role,
# slice_labels, kind, start_snippet, end_snippet, comment, comment_line).
def parse_python_beacon_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Parse @beacon[...] comment blocks from Python source lines.

    Expected form (we control this schema):

        # @beacon[
        #   id=example@1234,
        #   role=model:forward,
        #   slice_labels=FP,DA,
        #   kind=ast|span,
        #   start_snippet="def forward(",
        #   end_snippet="return y",
        # ]

    Returns a list of dicts with keys:
        id, role, slice_labels, kind, start_snippet, end_snippet, comment_line.
    """
    beacons: list[dict[str, Any]] = []

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("#"):
            body = stripped.lstrip("#").lstrip()
            if body.startswith("@beacon["):
                comment_line = i + 1  # 1-based
                block_lines = [body]
                i += 1
                # Collect until a line containing ']' (inclusive)
                while i < n:
                    next_line = lines[i].lstrip()
                    if next_line.startswith("#"):
                        next_body = next_line.lstrip("#").lstrip()
                    else:
                        next_body = next_line
                    block_lines.append(next_body)
                    if "]" in next_body:
                        i += 1
                        break
                    i += 1

                # Parse block_lines into key/value pairs (skip first/last structural lines)
                fields: dict[str, str] = {}
                inner_lines = block_lines[1:-1] if len(block_lines) >= 2 else []
                for raw in inner_lines:
                    text = raw.strip()
                    if not text or text.startswith("@beacon"):
                        continue
                    # Drop trailing ',' or '],' or ']'
                    while text and text[-1] in ",]":
                        text = text[:-1].rstrip()
                    if not text:
                        continue
                    if "=" not in text:
                        continue
                    key, val = text.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    # Strip surrounding quotes if present
                    if (val.startswith("\"") and val.endswith("\"")) or (
                        val.startswith("'") and val.endswith("'")
                    ):
                        val = val[1:-1]
                    fields[key] = val

                kind = (fields.get("kind") or "span").strip().lower() or "span"
                if kind not in {"ast", "span"}:
                    # Unknown/unsupported kind – skip silently for now
                    continue

                beacon = {
                    "id": fields.get("id"),
                    "role": fields.get("role"),
                    "slice_labels": fields.get("slice_labels"),
                    "kind": kind,
                    "start_snippet": fields.get("start_snippet"),
                    "end_snippet": fields.get("end_snippet"),
                    "comment": fields.get("comment"),
                    "comment_line": comment_line,
                }
                beacons.append(beacon)
                continue
        i += 1

    return beacons


def _iter_ast_outline_nodes(nodes: list[dict[str, Any]]):
    """Yield all AST-backed outline nodes in a pre-order traversal.

    Nodes are expected to carry 'ast_type' when they correspond to
    Python AST elements (class/function/async/const).
    """
    for node in nodes:
        if isinstance(node, dict) and node.get("ast_type"):
            yield node
        children = (node.get("children") or []) if isinstance(node, dict) else []
        for ch in children:
            if isinstance(ch, dict):
                for sub in _iter_ast_outline_nodes([ch]):
                    yield sub


def _find_enclosing_ast_node_for_line(
    nodes: list[dict[str, Any]], start_line: int
) -> Optional[dict[str, Any]]:
    """Find the deepest AST node whose [start,end] span contains start_line."""
    best: Optional[dict[str, Any]] = None
    best_depth = -1

    def _recurse(node: dict[str, Any], depth: int) -> None:
        nonlocal best, best_depth
        if not isinstance(node, dict):
            return
        start = node.get("orig_lineno_start_unused")
        end = node.get("orig_lineno_end_unused")
        ast_type = node.get("ast_type")
        if (
            ast_type
            and isinstance(start, int)
            and isinstance(end, int)
            and start <= start_line <= end
        ):
            if depth > best_depth:
                best = node
                best_depth = depth
        for ch in node.get("children") or []:
            if isinstance(ch, dict):
                _recurse(ch, depth + 1)

    for root in nodes:
        _recurse(root, 0)
    return best


# @beacon[
#   id=carto-js-ts@apply_python_beacons,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: Python beacon attachment template.
# Reference for apply_js_beacons(...), which will decorate JS/TS
# Tree-sitter AST nodes (kind=ast) and attach span children
# (kind=span) in the same way.
def apply_python_beacons(
    file_path: str,
    lines: list[str],
    outline_nodes: list[dict[str, Any]],
) -> None:
    """Apply Python @beacon[...] metadata to an existing AST outline tree.

    - AST beacons (kind=ast): decorate existing AST nodes (no new nodes).
    - SPAN beacons (kind=span): create child nodes under the enclosing AST node
      (or at file-level if no enclosing AST node is found).

    Snippet-less beacons:
    - If kind is span and start_snippet is missing, we:
      1) Find the next non-blank, non-comment, non-decorator line after the
         beacon block, and treat that as the anchor line.
      2) If that line is exactly the start of a class/function/async AST node,
         we auto-upgrade the beacon to kind=ast and decorate that node.
      3) Otherwise, we treat it as a span beacon anchored under the enclosing
         AST node for that line.

    Comment context:
    - For both AST and SPAN beacons, we capture nearby Python comments and
      triple-quoted blocks (treated as комментарий-like) above and below the
      beacon block and append them into the beacon note for Workflowy.
    """
    if not outline_nodes:
        return

    beacons = parse_python_beacon_blocks(lines)
    if not beacons:
        return

    # Precompute AST-backed nodes and a helper for line lookups
    ast_nodes = list(_iter_ast_outline_nodes(outline_nodes))

    # Precompute triple-quoted spans so we can treat them as comment-like.
    triple_spans: List[tuple[int, int]] = []
    in_block = False
    quote_type: Optional[str] = None
    start_line_q: Optional[int] = None

    for idx, raw in enumerate(lines, start=1):
        if not in_block:
            if '"""' in raw or "'''" in raw:
                # Prefer the first occurrence on the line
                dq_index = raw.find('"""') if '"""' in raw else -1
                sq_index = raw.find("'''") if "'''" in raw else -1
                if dq_index != -1 and (sq_index == -1 or dq_index <= sq_index):
                    q = '"""'
                else:
                    q = "'''"
                count = raw.count(q)
                if count >= 2:
                    triple_spans.append((idx, idx))
                else:
                    in_block = True
                    quote_type = q
                    start_line_q = idx
        else:
            if quote_type and quote_type in raw:
                triple_spans.append((start_line_q or idx, idx))
                in_block = False
                quote_type = None
                start_line_q = None

    def _find_triple_span_for_line(line_no: int) -> Optional[tuple[int, int]]:
        for s, e in triple_spans:
            if s <= line_no <= e:
                return (s, e)
        return None

    def _header_line_for_node(node: dict[str, Any]) -> Optional[str]:
        ln = node.get("orig_lineno_start_unused")
        if not isinstance(ln, int) or ln <= 0 or ln > len(lines):
            return None
        return lines[ln - 1]

    def _next_anchor_line_after(line_no: int) -> Optional[int]:
        """Return next non-blank, non-comment, non-decorator line after line_no.

        line_no is 1-based. We skip lines that are:
        - empty/whitespace
        - start with '#'
        - start with '@' (decorators)
        """
        for idx in range(line_no + 1, len(lines) + 1):
            raw = lines[idx - 1]
            stripped = raw.lstrip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                continue
            if stripped.startswith("@"):
                continue
            return idx
        return None

    def _extract_python_beacon_context(comment_line: int) -> List[str]:
        """Extract nearby comment-like lines around a Python beacon.

        Includes:
        - # comments above/below the beacon block (stripped of leading '#')
        - triple-quoted blocks (triple quotes) treated as comment-like

        Skips:
        - blank lines
        - decorator lines starting with '@'
        """
        context: List[str] = []
        seen_spans: set[tuple[int, int]] = set()

        # Determine end of the beacon metadata block
        j = comment_line
        block_end = comment_line
        while j <= len(lines):
            raw = lines[j - 1]
            stripped = raw.lstrip()
            if stripped.startswith("#"):
                body = stripped.lstrip("#").lstrip()
            else:
                body = stripped
            if "]" in body:
                block_end = j
                break
            j += 1

        # Above
        j = comment_line - 1
        while j >= 1:
            raw = lines[j - 1]
            stripped = raw.lstrip()
            if not stripped:
                j -= 1
                continue
            if stripped.startswith("#"):
                body = stripped.lstrip("#").lstrip()
                if body:
                    context.insert(0, body)
                j -= 1
                continue
            span = _find_triple_span_for_line(j)
            if span and span not in seen_spans:
                s, e = span
                for k in range(s, e + 1):
                    context.insert(0, lines[k - 1])
                seen_spans.add(span)
                j = s - 1
                continue
            if stripped.startswith("@"):
                break
            break

        # Below
        j = block_end + 1
        while j <= len(lines):
            raw = lines[j - 1]
            stripped = raw.lstrip()
            if not stripped:
                j += 1
                continue
            if stripped.startswith("#"):
                body = stripped.lstrip("#").lstrip()
                if body:
                    context.append(body)
                j += 1
                continue
            span = _find_triple_span_for_line(j)
            if span and span not in seen_spans:
                s, e = span
                for k in range(s, e + 1):
                    context.append(lines[k - 1])
                seen_spans.add(span)
                j = e + 1
                continue
            if stripped.startswith("@"):
                break
            break

        return context

    # Pre-pass: for snippet-less span beacons, compute anchor lineno and
    # auto-upgrade to AST when the anchor is a clean class/function/async start.
    for beacon in beacons:
        kind = beacon.get("kind")
        start_snippet = beacon.get("start_snippet")
        if kind == "span" and not start_snippet:
            comment_line = beacon.get("comment_line") or 0
            if isinstance(comment_line, int) and comment_line > 0:
                anchor = _next_anchor_line_after(comment_line)
            else:
                anchor = None
            if anchor is None:
                continue
            beacon["_anchor_lineno"] = anchor

            # See if this line is exactly the start of a class/function/async
            # AST node. If so, we treat this beacon as an AST beacon by default.
            ast_candidates: list[dict[str, Any]] = []
            for node in ast_nodes:
                ast_type = node.get("ast_type")
                if ast_type not in {"class", "function", "async_function"}:
                    continue
                ln = node.get("orig_lineno_start_unused")
                if isinstance(ln, int) and ln == anchor:
                    ast_candidates.append(node)

            if len(ast_candidates) == 1:
                beacon["kind"] = "ast"

    # Pass 1: AST beacons
    for beacon in beacons:
        if beacon.get("kind") != "ast":
            continue
        b_id = beacon.get("id") or "(no-id)"
        start_snippet = beacon.get("start_snippet")
        comment_line = beacon.get("comment_line") or 0
        anchor_lineno = beacon.get("_anchor_lineno")

        chosen: Optional[dict[str, Any]] = None

        if start_snippet:
            # Legacy mode: use start_snippet to match header line
            norm_snip = normalize_for_match(start_snippet)
            if not norm_snip:
                continue

            candidates: list[dict[str, Any]] = []
            for node in ast_nodes:
                header = _header_line_for_node(node)
                if header is None:
                    continue
                if norm_snip in normalize_for_match(header):
                    candidates.append(node)

            if not candidates:
                # Beacon is stale or misconfigured – skip
                continue

            if len(candidates) == 1:
                chosen = candidates[0]
            else:
                # Prefer the first candidate whose header is AFTER the beacon
                after = [
                    n
                    for n in candidates
                    if isinstance(n.get("orig_lineno_start_unused"), int)
                    and n["orig_lineno_start_unused"] > comment_line
                ]
                if len(after) == 1:
                    chosen = after[0]
                else:
                    # Ambiguous – skip for now
                    continue
        else:
            # Snippet-less AST beacon: use precomputed anchor lineno
            anchor = beacon.get("_anchor_lineno")
            if not isinstance(anchor, int):
                continue
            ast_candidates = [
                node
                for node in ast_nodes
                if node.get("ast_type") in {"class", "function", "async_function"}
                and isinstance(node.get("orig_lineno_start_unused"), int)
                and node["orig_lineno_start_unused"] == anchor
            ]
            if len(ast_candidates) == 1:
                chosen = ast_candidates[0]
            else:
                continue

        if not chosen:
            continue

        # Decorate chosen AST node
        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        # Defaults: derive role from id prefix (before '@')
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "ast-beacon"
        display_slice = slice_labels or "-"

        tag_suffix = _slice_label_tags(slice_labels or display_slice)
        name = chosen.get("name") or "Untitled"
        if "🔱" not in name:
            name = f"{name} 🔱"
        if tag_suffix:
            name = f"{name} {tag_suffix}"
        chosen["name"] = name

        # Merge beacon metadata into note
        comment = beacon.get("comment")
        meta_lines = [
            "BEACON (AST)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: ast",
        ]
        if comment:
            meta_lines.append(f"comment: {comment}")
        context_lines = _extract_python_beacon_context(comment_line)
        if context_lines:
            meta_lines.append("")
            meta_lines.append("CONTEXT COMMENTS (PYTHON):")
            meta_lines.extend(context_lines)
        note = chosen.get("note") or ""
        meta_block = "\n".join(meta_lines)
        if note:
            chosen["note"] = note + "\n\n" + meta_block
        else:
            chosen["note"] = meta_block

    # Pass 2: SPAN beacons
    for beacon in beacons:
        if beacon.get("kind") != "span":
            continue
        start_snippet = beacon.get("start_snippet")
        comment_line = beacon.get("comment_line") or 0

        start_line: Optional[int] = None

        if start_snippet:
            norm_snip = normalize_for_match(start_snippet)
            if not norm_snip:
                continue

            # Find candidate start lines, but IGNORE lines that are comments
            # (including the @beacon block itself) so we never anchor spans to
            # their own metadata.
            start_candidates: list[int] = []
            for idx, line in enumerate(lines, start=1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                if norm_snip in normalize_for_match(line):
                    start_candidates.append(idx)

            if not start_candidates:
                # No match – skip for now
                continue

            if len(start_candidates) == 1:
                start_line = start_candidates[0]
            else:
                after = [ln for ln in start_candidates if ln > comment_line]
                if len(after) == 1:
                    start_line = after[0]
                else:
                    # Ambiguous span; require beacon refinement
                    continue
        else:
            # Snippet-less span: use precomputed anchor or compute it now
            anchor = beacon.get("_anchor_lineno")
            if isinstance(anchor, int):
                start_line = anchor
            else:
                if isinstance(comment_line, int) and comment_line > 0:
                    start_line = _next_anchor_line_after(comment_line)
                else:
                    start_line = None

        if start_line is None:
            continue

        # Decide enclosing AST node (if any)
        enclosing = _find_enclosing_ast_node_for_line(outline_nodes, start_line)

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        # Defaults: derive role from id prefix (before '@')
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "span-beacon"
        display_slice = slice_labels or "-"

        tag_suffix = _slice_label_tags(slice_labels or display_slice)
        name = f"🔱 {display_role}"
        if tag_suffix:
            name = f"{name} {tag_suffix}"

        context_lines = _extract_python_beacon_context(comment_line)
        comment = beacon.get("comment")

        note_lines = [
            "BEACON (SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
        if comment:
            note_lines.append(f"comment: {comment}")
        if context_lines:
            note_lines.append("")
            note_lines.append("CONTEXT COMMENTS (PYTHON):")
            note_lines.extend(context_lines)

        span_node = {
            "name": name,
            "note": "\n".join(note_lines),
            "children": [],
        }

        if enclosing is not None:
            children = enclosing.get("children") or []
            children.append(span_node)
            enclosing["children"] = children
        else:
            # File-level span: attach directly under top-level outline
            outline_nodes.append(span_node)


def get_docstring(node: ast.AST) -> str:
    """Extract docstring from an AST node if present. (Synced Update)"""
    return ast.get_docstring(node) or ""


def hello_ether():
    """Hello, Ether."""
    print("Hello, Ether!")

def get_function_signature(node: ast.FunctionDef) -> str:
    """Reconstruct function signature for display."""
    args = []
    
    # Handle positional arguments
    for arg in node.args.args:
        arg_str = arg.arg
        if arg.annotation:
            if isinstance(arg.annotation, ast.Name):
                arg_str += f": {arg.annotation.id}"
            elif isinstance(arg.annotation, ast.Subscript):
                try:
                    val = getattr(arg.annotation.value, 'id', '')
                    slice_val = getattr(arg.annotation.slice, 'id', '')
                    if not slice_val and hasattr(arg.annotation.slice, 'value'):
                         if isinstance(arg.annotation.slice, ast.Name):
                             slice_val = arg.annotation.slice.id
                    if val and slice_val:
                        arg_str += f": {val}[{slice_val}]"
                except:
                    pass
            elif isinstance(arg.annotation, ast.Attribute):
                arg_str += f": {arg.annotation.attr}"
        args.append(arg_str)
        
    return f"{node.name}({', '.join(args)})"

# @beacon[
#   id=carto-js-ts@parse_file_outline,
#   slice_labels=carto-js-ts,
# ]
# Phase 1 JS/TS: Python AST outline template.
# Used as the reference shape for the new JS/TS Tree-sitter-based
# outline builder (parse_js_ts_outline) so JS/TS nodes match the
# same NEXUS schema (ast_type, ast_name, ast_qualname, file_path,
# line ranges, priority, children).
def parse_file_outline(file_path: str) -> List[Dict[str, Any]]:
    """Parse a single Python file into a NEXUS outline tree + beacon decorations.

    Steps (V1 Beacon-aware):
    1. Parse the file with `ast.parse` and build an AST-backed outline tree that
       records `ast_type`, `ast_name`, `ast_qualname`, `file_path`, and
       `orig_lineno_start_unused` / `orig_lineno_end_unused` for each node.
    2. Scan the raw source lines for `@beacon[...]` comment blocks.
    3. Attach AST beacons (kind=ast) to existing outline nodes (no new nodes).
    4. Attach SPAN beacons (kind=span) as child nodes under the enclosing AST
       node (or at file-level if no enclosing node is found).

    PRIORITY HANDLING: Existing class/function/const nodes still receive
    sequential priorities (100, 200, 300, ...) based on order in source code.
    Span beacon nodes do not currently participate in the priority sequence.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content, filename=file_path)
        lines = content.splitlines()

        priority_counter = [100]  # Mutable list to share across recursion

        def walk_body(
            body_items: List[ast.AST],
            qual_prefix: Optional[str] = None,
        ) -> List[Dict[str, Any]]:
            results: List[Dict[str, Any]] = []
            for item in body_items:
                if isinstance(item, ast.ClassDef):
                    qual = item.name if not qual_prefix else f"{qual_prefix}.{item.name}"
                    start = getattr(item, "lineno", None)
                    end = getattr(item, "end_lineno", start)
                    # Build note: AST_QUALNAME header, then optional separator and docstring.
                    doc = get_docstring(item)
                    note_lines: list[str] = [f"AST_QUALNAME: {qual}"]
                    if doc:
                        note_lines.append("---")
                        note_lines.append(doc)
                    note_text = "\n".join(note_lines)

                    node = {
                        "name": f"{EMOJI_CLASS} class {item.name}",
                        "priority": priority_counter[0],
                        "note": note_text,
                        "children": walk_body(item.body, qual_prefix=qual),
                        "ast_type": "class",
                        "ast_name": item.name,
                        "ast_qualname": qual,
                        "file_path": file_path,
                        "orig_lineno_start_unused": start,
                        "orig_lineno_end_unused": end,
                    }
                    priority_counter[0] += 100
                    results.append(node)
                elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    prefix = EMOJI_ASYNC if isinstance(item, ast.AsyncFunctionDef) else EMOJI_FUNC
                    sig = get_function_signature(item)
                    qual = item.name if not qual_prefix else f"{qual_prefix}.{item.name}"
                    start = getattr(item, "lineno", None)
                    end = getattr(item, "end_lineno", start)

                    doc = get_docstring(item)
                    note_lines: list[str] = [f"AST_QUALNAME: {qual}"]
                    if doc:
                        note_lines.append("---")
                        note_lines.append(doc)
                    note_text = "\n".join(note_lines)

                    node = {
                        "name": f"{prefix} {sig}",
                        "priority": priority_counter[0],
                        "note": note_text,
                        "children": walk_body(item.body, qual_prefix=qual),
                        "ast_type": "async_function" if isinstance(item, ast.AsyncFunctionDef) else "function",
                        "ast_name": item.name,
                        "ast_qualname": qual,
                        "file_path": file_path,
                        "orig_lineno_start_unused": start,
                        "orig_lineno_end_unused": end,
                    }
                    priority_counter[0] += 100
                    results.append(node)
                elif isinstance(item, ast.Assign):
                    # Optional: Capture top-level constants
                    for target in item.targets:
                        if isinstance(target, ast.Name) and target.id.isupper():
                            start = getattr(item, "lineno", None)
                            end = getattr(item, "end_lineno", start)
                            qual_const = target.id if not qual_prefix else f"{qual_prefix}.{target.id}"
                            note_text = f"AST_QUALNAME: {qual_const}\n---\nConstant"
                            node = {
                                "name": f"{EMOJI_CONST} {target.id}",
                                "priority": priority_counter[0],
                                "note": note_text,
                                "children": [],
                                "ast_type": "const",
                                "ast_name": target.id,
                                "ast_qualname": qual_const,
                                "file_path": file_path,
                                "orig_lineno_start_unused": start,
                                "orig_lineno_end_unused": end,
                            }
                            priority_counter[0] += 100
                            results.append(node)
            return results

        outline_nodes = walk_body(tree.body)
        apply_python_beacons(file_path, lines, outline_nodes)
        return outline_nodes

    except Exception as e:
        return [{
            "name": f"⚠️ Parse Error",
            "note": str(e),
            "children": []
        }]

# @beacon[
#   id=carto-js-ts@parse_js_ts_outline,
#   slice_labels=carto-js-ts,
# ]
# Phase 1 JS/TS: placeholder anchor for the future parse_js_ts_outline(...)
# function. The next agent will define the JS/TS Tree-sitter-based outline
# builder immediately above or below this beacon so Cartographer can
# produce JS/TS AST nodes in the same shape as Python.

def parse_js_ts_outline(file_path: str) -> List[Dict[str, Any]]:
    """Parse a single JavaScript / TypeScript file into a NEXUS outline tree.

    This mirrors parse_file_outline for Python but uses Tree-sitter to
    discover classes, functions, and methods and produces nodes with the
    same schema (ast_type, ast_name, ast_qualname, file_path,
    orig_lineno_start_unused, orig_lineno_end_unused, children, priority).
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in {".js", ".jsx", ".ts", ".tsx"}:
        return [{
            "name": "⚠️ Unsupported JS/TS extension",
            "note": f"File {file_path!r} does not look like a JS/TS source file.",
            "children": [],
        }]

    if not _HAVE_TREE_SITTER:
        return [{
            "name": "⚠️ JS/TS parsing not available",
            "note": (
                "Tree-sitter / tree_sitter_languages could not be imported. "
                "Install 'tree_sitter' and 'tree_sitter_languages' in the Windows "
                "Python environment to enable JS/TS Cartographer support."
            ),
            "children": [],
        }]

    language_name = "javascript"
    if ext in {".ts", ".tsx"}:
        language_name = "typescript"

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source_text = f.read()
        source_bytes = source_text.encode("utf-8")

        parser = Parser()
        language = get_language(language_name)  # type: ignore[operator]
        parser.language = language
        tree = parser.parse(source_bytes)
        root = tree.root_node

        priority_counter = [100]

        def node_text(n) -> str:
            if n is None:
                return ""
            return source_bytes[n.start_byte:n.end_byte].decode("utf-8", errors="ignore")

        def make_note(qual: str, doc: Optional[str] = None) -> str:
            note_lines: List[str] = [f"AST_QUALNAME: {qual}"]
            if doc:
                note_lines.append("---")
                note_lines.append(doc)
            return "\n".join(note_lines)

        def walk(node, qual_prefix: Optional[str] = None) -> List[Dict[str, Any]]:
            results: List[Dict[str, Any]] = []

            for child in node.children:
                t = child.type

                if t == "class_declaration":
                    name_node = child.child_by_field_name("name")
                    class_name = node_text(name_node).strip() or "AnonymousClass"
                    qual = class_name if not qual_prefix else f"{qual_prefix}.{class_name}"
                    start_line = child.start_point[0] + 1
                    end_line = child.end_point[0] + 1

                    # TODO: in future, harvest JSDoc comments into doc
                    note_text = make_note(qual)

                    class_node: Dict[str, Any] = {
                        "name": f"{EMOJI_CLASS} class {class_name}",
                        "priority": priority_counter[0],
                        "note": note_text,
                        "children": [],
                        "ast_type": "class",
                        "ast_name": class_name,
                        "ast_qualname": qual,
                        "file_path": file_path,
                        "orig_lineno_start_unused": start_line,
                        "orig_lineno_end_unused": end_line,
                    }
                    priority_counter[0] += 100

                    body = child.child_by_field_name("body")
                    if body is not None:
                        class_node["children"] = walk(body, qual_prefix=qual)

                    results.append(class_node)
                    continue

                if t == "function_declaration":
                    name_node = child.child_by_field_name("name")
                    func_name = node_text(name_node).strip() or "anonymous"
                    qual = func_name if not qual_prefix else f"{qual_prefix}.{func_name}"
                    start_line = child.start_point[0] + 1
                    end_line = child.end_point[0] + 1

                    signature = func_name
                    params_node = child.child_by_field_name("parameters")
                    if params_node is not None:
                        params_text = node_text(params_node).strip()
                        signature = f"{func_name}{params_text}"

                    note_text = make_note(qual)

                    func_node: Dict[str, Any] = {
                        "name": f"{EMOJI_FUNC} function {signature}",
                        "priority": priority_counter[0],
                        "note": note_text,
                        "children": [],
                        "ast_type": "function",
                        "ast_name": func_name,
                        "ast_qualname": qual,
                        "file_path": file_path,
                        "orig_lineno_start_unused": start_line,
                        "orig_lineno_end_unused": end_line,
                    }
                    priority_counter[0] += 100

                    body = child.child_by_field_name("body")
                    if body is not None:
                        func_node["children"] = walk(body, qual_prefix=qual)

                    results.append(func_node)
                    continue

                if t == "method_definition":
                    name_node = child.child_by_field_name("name")
                    method_name = node_text(name_node).strip() or "anonymous"
                    qual = method_name if not qual_prefix else f"{qual_prefix}.{method_name}"
                    start_line = child.start_point[0] + 1
                    end_line = child.end_point[0] + 1

                    params_node = child.child_by_field_name("parameters")
                    signature = method_name
                    if params_node is not None:
                        params_text = node_text(params_node).strip()
                        signature = f"{method_name}{params_text}"

                    note_text = make_note(qual)

                    method_node: Dict[str, Any] = {
                        "name": f"{EMOJI_FUNC} method {signature}",
                        "priority": priority_counter[0],
                        "note": note_text,
                        "children": [],
                        "ast_type": "method",
                        "ast_name": method_name,
                        "ast_qualname": qual,
                        "file_path": file_path,
                        "orig_lineno_start_unused": start_line,
                        "orig_lineno_end_unused": end_line,
                    }
                    priority_counter[0] += 100

                    body = child.child_by_field_name("body")
                    if body is not None:
                        method_node["children"] = walk(body, qual_prefix=qual)

                    results.append(method_node)
                    continue

                # Recurse into other nodes to discover nested declarations
                if child.children:
                    results.extend(walk(child, qual_prefix=qual_prefix))

            return results

        outline_nodes = walk(root)

        # Phase 2: JS/TS beacon parsing + attachment
        lines = source_text.splitlines()
        js_beacons = parse_js_beacon_blocks(lines)
        apply_js_beacons(file_path, lines, outline_nodes, js_beacons)
        return outline_nodes

    except Exception as e:  # pragma: no cover - safety net
        return [{
            "name": "⚠️ Parse Error (JS/TS)",
            "note": str(e),
            "children": [],
        }]


# @beacon[
#   id=carto-js-ts@map_codebase,
#   slice_labels=carto-js-ts,
# ]
# Phase 1 JS/TS: directory + single-file dispatcher. This will be extended
# to call the JS/TS outline builder (parse_js_ts_outline) for .js/.ts/.tsx
# files alongside parse_file_outline for Python.
def map_codebase(root_path: str, include_exts: List[str] = None, exclude_patterns: List[str] = None) -> Dict[str, Any]:
    """
    Wraps the inner file parsing into a directory walker.
    Returns a single root NEXUS node representing the codebase tree.
    """
    
    def should_include(name: str) -> bool:
        if exclude_patterns:
            for pat in exclude_patterns:
                if pat in name: return False
        
        # If include list is provided, must match extension
        if include_exts:
            _, ext = os.path.splitext(name)
            if ext.lower() not in include_exts:
                # Unless it's a folder? Usually filtering applies to files.
                # Let's say we always include folders unless excluded by pattern.
                return False
        return True

    def scan_dir(current_path: str) -> List[Dict[str, Any]]:
        nodes = []
        try:
            items = sorted(os.listdir(current_path))
            for item in items:
                full_path = os.path.join(current_path, item)
                
                # Ignore hidden/system dirs
                if item.startswith('.') or item == "__pycache__":
                    continue
                
                if exclude_patterns and any(pat in item for pat in exclude_patterns):
                    continue
                
                if os.path.isdir(full_path):
                    children = scan_dir(full_path)
                    if children:  # Only add non-empty directories
                        folder_note = format_file_note(full_path)
                        nodes.append({
                            "name": f"{EMOJI_FOLDER} {item}",
                            "note": folder_note,
                            "children": children,
                        })
                
                elif os.path.isfile(full_path):
                    # Extension Check for files only
                    _, ext = os.path.splitext(item)
                    ext = ext.lower()
                    
                    if include_exts and ext not in include_exts:
                        continue
                        
                    # Dispatch
                    line_count: Optional[int] = None
                    if ext == '.py':
                        icon = EMOJI_PYTHON
                        children = parse_file_outline(full_path)
                        line_count = _get_line_count(full_path)
                    elif ext == '.md':
                        icon = EMOJI_MARKDOWN
                        children = parse_markdown_structure(full_path)
                        line_count = _get_line_count(full_path)
                    elif ext in {'.js', '.jsx'}:
                        icon = EMOJI_JS
                        children = parse_js_ts_outline(full_path)
                        line_count = _get_line_count(full_path)
                    elif ext in {'.ts', '.tsx'}:
                        icon = EMOJI_TS
                        children = parse_js_ts_outline(full_path)
                        line_count = _get_line_count(full_path)
                    elif ext == '.sql':
                        icon = EMOJI_SQL
                        with open(full_path, 'r', encoding='utf-8') as f:
                            sql_lines = f.read().splitlines()
                        line_count = len(sql_lines)
                        children: List[Dict[str, Any]] = []
                        sql_beacons = parse_sql_beacon_blocks(sql_lines)
                        apply_sql_beacons(sql_lines, children, sql_beacons)
                    elif ext == '.sh':
                        icon = EMOJI_SHELL
                        with open(full_path, 'r', encoding='utf-8') as f:
                            sh_lines = f.read().splitlines()
                        line_count = len(sh_lines)
                        children = []
                        sh_beacons = parse_sh_beacon_blocks(sh_lines)
                        apply_sh_beacons(sh_lines, children, sh_beacons)
                    else:
                        icon = EMOJI_FILE
                        children = []  # Generic file = Leaf node
                        line_count = _get_line_count(full_path)

                    note = format_file_note(full_path, line_count=line_count)

                    nodes.append({
                        "name": f"{icon} {item}",
                        "note": note,
                        "children": children
                    })
                    
        except Exception as e:
            print(f"Error scanning {current_path}: {e}")
            
        return nodes

    root_name = os.path.basename(root_path) or "Source Code"
    if os.path.isfile(root_path):
        # Single file mode dispatch
        _, ext = os.path.splitext(root_path)
        ext = ext.lower()
        if ext == '.py':
            children = parse_file_outline(root_path)
            lc = _get_line_count(root_path)
            note = format_file_note(root_path, line_count=lc)
            return {
                "name": f"{EMOJI_PYTHON} {os.path.basename(root_path)}",
                "note": note,
                "children": children
            }
        elif ext == '.md':
            children = parse_markdown_structure(root_path)
            lc = _get_line_count(root_path)
            note = format_file_note(root_path, line_count=lc)
            return {
                "name": f"{EMOJI_MARKDOWN} {os.path.basename(root_path)}",
                "note": note,
                "children": children
            }
        elif ext in ('.js', '.jsx'):
            children = parse_js_ts_outline(root_path)
            lc = _get_line_count(root_path)
            note = format_file_note(root_path, line_count=lc)
            return {
                "name": f"{EMOJI_JS} {os.path.basename(root_path)}",
                "note": note,
                "children": children,
            }
        elif ext in ('.ts', '.tsx'):
            children = parse_js_ts_outline(root_path)
            lc = _get_line_count(root_path)
            note = format_file_note(root_path, line_count=lc)
            return {
                "name": f"{EMOJI_TS} {os.path.basename(root_path)}",
                "note": note,
                "children": children,
            }
        elif ext == '.sql':
            with open(root_path, 'r', encoding='utf-8') as f:
                sql_lines = f.read().splitlines()
            children: List[Dict[str, Any]] = []
            sql_beacons = parse_sql_beacon_blocks(sql_lines)
            apply_sql_beacons(sql_lines, children, sql_beacons)
            note = format_file_note(root_path, line_count=len(sql_lines))
            return {
                "name": f"{EMOJI_SQL} {os.path.basename(root_path)}",
                "note": note,
                "children": children,
            }
        elif ext == '.sh':
            with open(root_path, 'r', encoding='utf-8') as f:
                sh_lines = f.read().splitlines()
            children: List[Dict[str, Any]] = []
            sh_beacons = parse_sh_beacon_blocks(sh_lines)
            apply_sh_beacons(sh_lines, children, sh_beacons)
            note = format_file_note(root_path, line_count=len(sh_lines))
            return {
                "name": f"{EMOJI_SHELL} {os.path.basename(root_path)}",
                "note": note,
                "children": children,
            }
        else:
             lc = _get_line_count(root_path)
             note = format_file_note(root_path, line_count=lc)
             return {
                "name": f"{EMOJI_FILE} {os.path.basename(root_path)}",
                "note": note,
                "children": []
            }
    
    # Directory mode
    return {
        "name": f"🗺️ Source Map: {root_name}",
        "note": f"Root: {root_path}",
        "children": scan_dir(root_path)
    }

def parse_sql_beacon_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Parse @beacon[...] comment blocks from SQL source lines.

    Syntax (mirrors Python/Markdown beacons):

        -- @beacon[
        --   id=segment@001,
        --   role=query:fraud_filter,   # optional
        --   slice_labels=QUERY-FRAUD,  # optional
        --   kind=span,                 # optional, defaults to span
        -- ]

    All metadata fields are optional except id; defaults are applied later.
    """
    beacons: list[dict[str, Any]] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("--"):
            body = stripped.lstrip("-").lstrip()
            if body.startswith("@beacon["):
                comment_line = i + 1  # 1-based
                block_lines = [body]
                i += 1
                # Collect until a line containing ']' (inclusive)
                while i < n:
                    next_line = lines[i].lstrip()
                    if next_line.startswith("--"):
                        next_body = next_line.lstrip("-").lstrip()
                    else:
                        next_body = next_line
                    block_lines.append(next_body)
                    if "]" in next_body:
                        i += 1
                        break
                    i += 1

                # Parse block_lines into key/value pairs (skip first/last structural lines)
                fields: dict[str, str] = {}
                inner_lines = block_lines[1:-1] if len(block_lines) >= 2 else []
                for raw in inner_lines:
                    text = raw.strip()
                    if not text or text.startswith("@beacon"):
                        continue
                    # Drop trailing ',' or '],' or ']'
                    while text and text[-1] in ",]":
                        text = text[:-1].rstrip()
                    if not text:
                        continue
                    if "=" not in text:
                        continue
                    key, val = text.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    # Strip surrounding quotes if present
                    if (val.startswith("\"") and val.endswith("\"")) or (
                        val.startswith("'") and val.endswith("'")
                    ):
                        val = val[1:-1]
                    fields[key] = val

                kind = (fields.get("kind") or "span").strip().lower() or "span"
                if kind not in {"ast", "span"}:
                    # Unknown/unsupported kind – skip silently for now
                    continue

                beacon = {
                    "id": fields.get("id"),
                    "role": fields.get("role"),
                    "slice_labels": fields.get("slice_labels"),
                    "kind": kind,
                    "start_snippet": fields.get("start_snippet"),
                    "end_snippet": fields.get("end_snippet"),
                    "comment": fields.get("comment"),
                    "comment_line": comment_line,
                }
                beacons.append(beacon)
                continue
        i += 1

    return beacons


# @beacon[
#   id=carto-js-ts@parse_js_beacon_blocks,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: JS/TS beacon parser.
# Supports both line-comment (//) and block-comment (/* ... */) forms
# with the same metadata schema as Python/Markdown beacons.
def parse_js_beacon_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Parse @beacon[...] comment blocks from JS/TS source lines.

    Supported forms (we control this schema):

        // @beacon[
        //   id=feature@1234,
        //   role=glimpse:refresh,
        //   slice_labels=glimpse-extension,
        //   kind=ast|span,
        //   start_snippet="function handleRefresh(",
        //   end_snippet="}",
        //   comment=One-line human note,
        // ]

        /* @beacon[
           id=feature@1234,
           slice_labels=glimpse-extension,
           kind=ast,
         ] */

    Returns a list of dicts with keys:
        id, role, slice_labels, kind, start_snippet, end_snippet,
        comment, comment_line, kind_explicit.
    """
    beacons: list[dict[str, Any]] = []
    n = len(lines)
    i = 0

    def _strip_js_comment_sugar(text: str) -> str:
        t = text.lstrip()
        # Leading JS comment prefixes
        if t.startswith("//"):
            t = t[2:].lstrip()
        if t.startswith("/*"):
            t = t[2:].lstrip()
        if t.startswith("*/"):
            t = t[2:].lstrip()
        if t.startswith("*"):
            t = t[1:].lstrip()
        # Trailing block-comment closer
        if t.endswith("*/"):
            t = t[:-2].rstrip()
        return t

    while i < n:
        line = lines[i]
        if "@beacon[" in line:
            comment_line = i + 1  # 1-based
            block_lines = [line]
            i += 1
            # Collect until a line containing ']' (inclusive)
            while i < n:
                block_lines.append(lines[i])
                if "]" in lines[i]:
                    i += 1
                    break
                i += 1

            # Parse block_lines into key/value pairs (skip first/last structural lines)
            fields: dict[str, str] = {}
            inner_lines = block_lines[1:-1] if len(block_lines) >= 2 else []
            for raw in inner_lines:
                text = raw.strip()
                if not text:
                    continue
                text = _strip_js_comment_sugar(text)
                if not text or text.startswith("@beacon"):
                    continue
                # Drop trailing ',' or '],' or ']'
                while text and text[-1] in ",]":
                    text = text[:-1].rstrip()
                if not text or "=" not in text:
                    continue
                key, val = text.split("=", 1)
                key = key.strip()
                val = val.strip()
                # Strip surrounding quotes if present
                if (val.startswith("\"") and val.endswith("\"")) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                fields[key] = val

            kind_raw = fields.get("kind")
            kind = (kind_raw or "span").strip().lower() or "span"
            if kind not in {"ast", "span"}:
                continue

            beacon = {
                "id": fields.get("id"),
                "role": fields.get("role"),
                "slice_labels": fields.get("slice_labels"),
                "kind": kind,
                "start_snippet": fields.get("start_snippet"),
                "end_snippet": fields.get("end_snippet"),
                "comment": fields.get("comment"),
                "comment_line": comment_line,
                "kind_explicit": bool(kind_raw),
            }
            beacons.append(beacon)
        else:
            i += 1

    return beacons


# @beacon[
#   id=carto-js-ts@parse_sh_beacon_blocks,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: shell beacon parser example.
# Another line-comment @beacon[...] parser useful for cross-checking
# JS/TS behavior.
def parse_sh_beacon_blocks(lines: list[str]) -> list[dict[str, Any]]:
    """Parse @beacon[...] blocks from shell scripts using '#'-style comments.

    Syntax mirrors the Python beacon form but is language-agnostic:

        # @beacon[
        #   id=segment@001,
        #   role=my:role,             # optional
        #   slice_labels=TAG1,TAG2,   # optional
        #   kind=span,                # optional, defaults to span
        # ]
    """
    beacons: list[dict[str, Any]] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("#"):
            body = stripped.lstrip("#").lstrip()
            if body.startswith("@beacon["):
                comment_line = i + 1
                block_lines = [body]
                i += 1
                while i < n:
                    next_line = lines[i].lstrip()
                    if next_line.startswith("#"):
                        next_body = next_line.lstrip("#").lstrip()
                    else:
                        next_body = next_line
                    block_lines.append(next_body)
                    if "]" in next_body:
                        i += 1
                        break
                    i += 1

                fields: dict[str, str] = {}
                inner_lines = block_lines[1:-1] if len(block_lines) >= 2 else []
                for raw in inner_lines:
                    text = raw.strip()
                    if not text or text.startswith("@beacon"):
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

                kind = (fields.get("kind") or "span").strip().lower() or "span"
                if kind not in {"ast", "span"}:
                    continue

                beacon = {
                    "id": fields.get("id"),
                    "role": fields.get("role"),
                    "slice_labels": fields.get("slice_labels"),
                    "kind": kind,
                    "start_snippet": fields.get("start_snippet"),
                    "end_snippet": fields.get("end_snippet"),
                    "comment": fields.get("comment"),
                    "comment_line": comment_line,
                }
                beacons.append(beacon)
                continue
        i += 1

    return beacons


def _extract_sql_beacon_context(lines: list[str], comment_line: int) -> List[str]:
    """Extract nearby SQL comments (non-beacon) around a SQL beacon.

    Includes:
    - Lines starting with '--' above/below the beacon block (excluding @beacon lines)

    Skips:
    - blank lines
    - the beacon metadata lines themselves
    """
    context: List[str] = []

    # Determine end of the beacon metadata block (similar to parse_sql_beacon_blocks)
    j = comment_line
    block_end = comment_line
    while j <= len(lines):
        raw = lines[j - 1].lstrip()
        if raw.startswith("--"):
            body = raw.lstrip("-").lstrip()
        else:
            body = raw
        if "]" in body:
            block_end = j
            break
        j += 1

    # Above
    j = comment_line - 1
    while j >= 1:
        raw = lines[j - 1].lstrip()
        if not raw:
            j -= 1
            continue
        if raw.startswith("--") and "@beacon" not in raw:
            context.insert(0, raw.lstrip("-").lstrip())
            j -= 1
            continue
        break

    # Below
    j = block_end + 1
    while j <= len(lines):
        raw = lines[j - 1].lstrip()
        if not raw:
            j += 1
            continue
        if raw.startswith("--") and "@beacon" not in raw:
            context.append(raw.lstrip("-").lstrip())
            j += 1
            continue
        break

    return context


# @beacon[
#   id=carto-js-ts@apply_sql_beacons,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: SQL beacon attachment example.
# Secondary template for attaching span-only beacons as children
# under a FILE node for non-AST languages.
def apply_sql_beacons(
    lines: list[str],
    file_children: List[Dict[str, Any]],
    beacons: list[dict[str, Any]],
) -> None:
    """Attach SQL span beacons as child nodes under the file node.

    For now, SQL beacons are purely location-centric annotations on the file.
    We do not compute or store explicit line ranges; future tools can re-scan
    the file to recover spans if needed.
    """
    if not beacons:
        return

    for beacon in beacons:
        if beacon.get("kind") != "span":
            continue

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()

        # Defaults: derive role from id prefix (before '@')
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "sql-span-beacon"
        display_slice = slice_labels or "-"

        tag_suffix = _slice_label_tags(slice_labels or display_slice)
        name = f"🔱 {display_role}"
        if tag_suffix:
            name = f"{name} {tag_suffix}"

        context_lines = _extract_sql_beacon_context(lines, beacon.get("comment_line") or 0)
        comment = beacon.get("comment")

        note_lines = [
            "BEACON (SQL SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
        if comment:
            note_lines.append(f"comment: {comment}")
        if context_lines:
            note_lines.append("")
            note_lines.append("CONTEXT COMMENTS (SQL):")
            note_lines.extend(context_lines)

        span_node = {
            "name": name,
            "note": "\n".join(note_lines),
            "children": [],
        }

        file_children.append(span_node)


def _extract_sh_beacon_context(lines: list[str], comment_line: int) -> List[str]:
    """Extract nearby '#' comments (non-beacon) around a shell beacon.

    Includes lines starting with '#' above/below the block (excluding @beacon).
    """
    context: List[str] = []

    j = comment_line
    block_end = comment_line
    while j <= len(lines):
        raw = lines[j - 1].lstrip()
        if raw.startswith("#"):
            body = raw.lstrip("#").lstrip()
        else:
            body = raw
        if "]" in body:
            block_end = j
            break
        j += 1

    # Above
    j = comment_line - 1
    while j >= 1:
        raw = lines[j - 1].lstrip()
        if not raw:
            j -= 1
            continue
        if raw.startswith("#") and "@beacon" not in raw:
            context.insert(0, raw.lstrip("#").lstrip())
            j -= 1
            continue
        break

    # Below
    j = block_end + 1
    while j <= len(lines):
        raw = lines[j - 1].lstrip()
        if not raw:
            j += 1
            continue
        if raw.startswith("#") and "@beacon" not in raw:
            context.append(raw.lstrip("#").lstrip())
            j += 1
            continue
        break

    return context


# @beacon[
#   id=carto-js-ts@apply_sh_beacons,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: shell beacon attachment example.
# Secondary template similar to SQL for span-only beacons.
def apply_sh_beacons(
    lines: list[str],
    file_children: List[Dict[str, Any]],
    beacons: list[dict[str, Any]],
) -> None:
    """Attach shell span beacons as child nodes under the file node.

    Behavior mirrors SQL: purely location-centric annotations; explicit spans
    are left to snippet tools.
    """
    if not beacons:
        return

    for beacon in beacons:
        if beacon.get("kind") != "span":
            continue

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()

        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "sh-span-beacon"
        display_slice = slice_labels or "-"

        tag_suffix = _slice_label_tags(slice_labels or display_slice)
        name = f"🔱 {display_role}"
        if tag_suffix:
            name = f"{name} {tag_suffix}"

        context_lines = _extract_sh_beacon_context(lines, beacon.get("comment_line") or 0)
        comment = beacon.get("comment")

        note_lines = [
            "BEACON (SH SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
        if comment:
            note_lines.append(f"comment: {comment}")
        if context_lines:
            note_lines.append("")
            note_lines.append("CONTEXT COMMENTS (SH):")
            note_lines.extend(context_lines)

        span_node = {
            "name": name,
            "note": "\n".join(note_lines),
            "children": [],
        }

        file_children.append(span_node)


# @beacon[
#   id=carto-js-ts@apply_js_beacons,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Phase 2 JS/TS: JS/TS beacon attachment.
# Decorates JS/TS AST nodes (kind=ast) and attaches span children
# (kind=span) similarly to Python beacons.
def apply_js_beacons(
    file_path: str,
    lines: list[str],
    outline_nodes: List[Dict[str, Any]],
    beacons: list[dict[str, Any]],
) -> None:
    if not outline_nodes or not beacons:
        return

    ast_nodes = list(_iter_ast_outline_nodes(outline_nodes))

    n = len(lines)

    def _js_next_anchor_line_after(line_no: int) -> Optional[int]:
        for idx in range(line_no + 1, n + 1):
            raw = lines[idx - 1]
            stripped = raw.lstrip()
            if not stripped:
                continue
            if stripped.startswith("//"):
                continue
            if stripped.startswith("/*") or stripped.startswith("*") or stripped.startswith("*/"):
                continue
            return idx
        return None

    def _header_line_for_node(node: dict[str, Any]) -> Optional[str]:
        ln = node.get("orig_lineno_start_unused")
        if not isinstance(ln, int) or ln <= 0 or ln > len(lines):
            return None
        return lines[ln - 1]

    def _extract_js_beacon_context(comment_line: int) -> List[str]:
        context: List[str] = []
        j = comment_line
        block_end = comment_line
        # Find end of beacon block (first line containing ']')
        while j <= n:
            raw = lines[j - 1]
            if "]" in raw:
                block_end = j
                break
            j += 1

        # Above
        j = comment_line - 1
        while j >= 1:
            raw = lines[j - 1].lstrip()
            if not raw:
                j -= 1
                continue
            if raw.startswith("//") and "@beacon" not in raw:
                context.insert(0, raw[2:].lstrip())
                j -= 1
                continue
            if (raw.startswith("/*") or raw.startswith("*") or raw.startswith("*/")) and "@beacon" not in raw:
                cleaned = raw
                for prefix in ("/*", "*/", "*"):
                    if cleaned.startswith(prefix):
                        cleaned = cleaned[len(prefix):].lstrip()
                context.insert(0, cleaned)
                j -= 1
                continue
            break

        # Below
        j = block_end + 1
        while j <= n:
            raw = lines[j - 1].lstrip()
            if not raw:
                j += 1
                continue
            if raw.startswith("//") and "@beacon" not in raw:
                context.append(raw[2:].lstrip())
                j += 1
                continue
            if (raw.startswith("/*") or raw.startswith("*") or raw.startswith("*/")) and "@beacon" not in raw:
                cleaned = raw
                for prefix in ("/*", "*/", "*"):
                    if cleaned.startswith(prefix):
                        cleaned = cleaned[len(prefix):].lstrip()
                context.append(cleaned)
                j += 1
                continue
            break

        return context

    # Pre-pass: for snippet-less beacons (span OR ast), compute anchor lineno.
    # For span beacons, auto-upgrade to AST when anchor is a class/function/method.
    # For ast beacons without start_snippet, we still need _anchor_lineno for matching.
    for beacon in beacons:
        kind = beacon.get("kind")
        start_snippet = beacon.get("start_snippet")
        b_id = beacon.get("id") or "(no-id)"
        if kind in {"span", "ast"} and not start_snippet:
            comment_line = beacon.get("comment_line") or 0
            if isinstance(comment_line, int) and comment_line > 0:
                anchor = _js_next_anchor_line_after(comment_line)
            else:
                anchor = None
            if anchor is None:
                continue
            beacon["_anchor_lineno"] = anchor

            ast_candidates: list[dict[str, Any]] = []
            for node in ast_nodes:
                ast_type = node.get("ast_type")
                if ast_type not in {"class", "function", "method"}:
                    continue
                ln = node.get("orig_lineno_start_unused")
                if isinstance(ln, int) and ln == anchor:
                    ast_candidates.append(node)
            if len(ast_candidates) == 1:
                beacon["kind"] = "ast"

    # Pass 1: AST beacons
    for beacon in beacons:
        if beacon.get("kind") != "ast":
            continue
        b_id = beacon.get("id") or "(no-id)"
        start_snippet = beacon.get("start_snippet")
        comment_line = beacon.get("comment_line") or 0
        anchor_lineno = beacon.get("_anchor_lineno")
        print(f"[CARTOGRAPHER] apply_js_beacons Pass1: id={b_id} start_snippet={start_snippet!r} _anchor_lineno={anchor_lineno}", file=sys.stderr)

        chosen: Optional[dict[str, Any]] = None

        if start_snippet:
            norm_snip = normalize_for_match(start_snippet)
            if not norm_snip:
                continue

            candidates: list[dict[str, Any]] = []
            for node in ast_nodes:
                header = _header_line_for_node(node)
                if header is None:
                    continue
                if norm_snip in normalize_for_match(header):
                    candidates.append(node)

            if not candidates:
                continue

            if len(candidates) == 1:
                chosen = candidates[0]
            else:
                after = [
                    n
                    for n in candidates
                    if isinstance(n.get("orig_lineno_start_unused"), int)
                    and n["orig_lineno_start_unused"] > comment_line
                ]
                if len(after) == 1:
                    chosen = after[0]
                else:
                    continue
        else:
            anchor = beacon.get("_anchor_lineno")
            if not isinstance(anchor, int):
                continue
            ast_candidates = [
                node
                for node in ast_nodes
                if node.get("ast_type") in {"class", "function", "method"}
                and isinstance(node.get("orig_lineno_start_unused"), int)
                and node["orig_lineno_start_unused"] == anchor
            ]
            if len(ast_candidates) == 1:
                chosen = ast_candidates[0]
            else:
                continue

        if not chosen:
            continue

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "js-ast-beacon"
        display_slice = slice_labels or "-"
        tag_suffix = _slice_label_tags(slice_labels or display_slice)

        name = chosen.get("name") or "Untitled"
        if "🔱" not in name:
            name = f"{name} 🔱"
        if tag_suffix:
            name = f"{name} {tag_suffix}"
        chosen["name"] = name

        comment = beacon.get("comment")
        meta_lines = [
            "BEACON (JS/TS AST)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: ast",
        ]
        if comment:
            meta_lines.append(f"comment: {comment}")
        context_lines = _extract_js_beacon_context(comment_line)
        if context_lines:
            meta_lines.append("")
            meta_lines.append("CONTEXT COMMENTS (JS/TS):")
            meta_lines.extend(context_lines)
        note = chosen.get("note") or ""
        meta_block = "\n".join(meta_lines)
        if note:
            chosen["note"] = note + "\n\n" + meta_block
        else:
            chosen["note"] = meta_block

    # Pass 2: SPAN beacons
    for beacon in beacons:
        if beacon.get("kind") != "span":
            continue
        start_snippet = beacon.get("start_snippet")
        comment_line = beacon.get("comment_line") or 0

        start_line: Optional[int] = None

        if start_snippet:
            norm_snip = normalize_for_match(start_snippet)
            if not norm_snip:
                continue
            start_candidates: list[int] = []
            for idx, line in enumerate(lines, start=1):
                stripped = line.lstrip()
                if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*") or stripped.startswith("*/"):
                    continue
                if norm_snip in normalize_for_match(line):
                    start_candidates.append(idx)
            if not start_candidates:
                continue
            if len(start_candidates) == 1:
                start_line = start_candidates[0]
            else:
                after = [ln for ln in start_candidates if ln > comment_line]
                if len(after) == 1:
                    start_line = after[0]
                else:
                    continue
        else:
            anchor = beacon.get("_anchor_lineno")
            if isinstance(anchor, int):
                start_line = anchor
            else:
                if isinstance(comment_line, int) and comment_line > 0:
                    start_line = _js_next_anchor_line_after(comment_line)
                else:
                    start_line = None

        if start_line is None:
            continue

        enclosing = _find_enclosing_ast_node_for_line(outline_nodes, start_line)

        b_id = (beacon.get("id") or "").strip()
        role = (beacon.get("role") or "").strip()
        raw_slice_labels = (beacon.get("slice_labels") or "").strip()
        if not role and b_id and "@" in b_id:
            role = b_id.split("@", 1)[0]
        slice_labels = _canonicalize_slice_labels(raw_slice_labels, role)

        display_role = role or b_id or "js-span-beacon"
        display_slice = slice_labels or "-"
        tag_suffix = _slice_label_tags(slice_labels or display_slice)

        name = f"🔱 {display_role}"
        if tag_suffix:
            name = f"{name} {tag_suffix}"

        context_lines = _extract_js_beacon_context(comment_line)
        comment = beacon.get("comment")

        note_lines = [
            "BEACON (JS/TS SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
        if comment:
            note_lines.append(f"comment: {comment}")
        if context_lines:
            note_lines.append("")
            note_lines.append("CONTEXT COMMENTS (JS/TS):")
            note_lines.extend(context_lines)

        span_node = {
            "name": name,
            "note": "\n".join(note_lines),
            "children": [],
        }

        if enclosing is not None:
            children = enclosing.get("children") or []
            children.append(span_node)
            enclosing["children"] = children
        else:
            outline_nodes.append(span_node)


# @beacon[
#   id=carto-js-ts@split_name_and_tags,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Utility: split a Workflowy node name into base text and trailing #tags.
# Used by update_beacon_from_node_* helpers to derive slice_labels from
# name suffixes while keeping the canonical AST/beacon node name clean.
def split_name_and_tags(raw_name: str) -> tuple[str, list[str]]:
    name = (raw_name or "").strip()
    if not name:
        return "", []
    tokens = name.split()
    tags: list[str] = []
    # Walk from the end, collect tokens that look like tags (start with '#').
    while tokens and tokens[-1].startswith("#"):
        tags.insert(0, tokens.pop())
    base = " ".join(tokens).strip()
    return base, tags


def _generate_auto_beacon_hash() -> str:
    """Generate a short random hash for auto-beacon collision resistance.
    
    Returns a 4-character lowercase alphanumeric string.
    """
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=4))


# @beacon[
#   id=carto-js-ts@update_beacon_from_node_python,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Python: update/create/delete a beacon for a single AST/beacon node
# based on Workflowy name/note fields.
#
# This is the Cartographer-side helper invoked by the MCP server when F12
# is pressed on a Python AST/beacon node. It does not talk to Workflowy
# directly; instead it focuses on:
#   • interpreting Workflowy name/note as desired beacon metadata
#   • computing canonical slice_labels from tags + note
#   • deciding whether to update, create, or delete @beacon[...] blocks
#   • returning a small summary of what should be reflected back into
#     the local cache.
def update_beacon_from_node_python(
    file_path: str,
    name: str,
    note: str,
) -> Dict[str, Any]:
    """Update/create/delete Python @beacon[...] blocks for a single node.

    This helper performs on-disk edits ONLY. It does not touch Workflowy; the
    MCP client is responsible for updating the local /nodes-export cache name
    (and possibly note) to reflect the final state returned here.

    Policy:
    - If note contains BEACON metadata with an id: → update existing block.
    - Else if AST_QUALNAME present and tags present → create a new beacon
      anchored to that AST node, derived from tags.
    - Else if there is a beacon on disk for this AST_QUALNAME/id but the note
      has no BEACON block → delete the beacon.
    """
    base_name, tags = split_name_and_tags(name)
    beacon_id = _extract_beacon_id_from_note(note)
    ast_qualname = _extract_ast_qualname_from_note(note)

    result: Dict[str, Any] = {
        "language": "python",
        "file_path": file_path,
        "base_name": base_name,
        "tags": tags,
        "beacon_id": beacon_id,
        "ast_qualname": ast_qualname,
        "operation": "noop",
    }

    # Load file lines once.
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:  # noqa: BLE001
        result["error"] = f"failed_to_read_file: {e}"
        return result

    beacons = parse_python_beacon_blocks(lines)

    # Helper: locate beacon block by id and return (start_idx,end_idx,comment_line).
    def _find_beacon_block_by_id(bid: str) -> Optional[tuple[int, int, int]]:
        bid = (bid or "").strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if not isinstance(cl, int) or cl <= 0:
                continue
            # Walk forward to find the ']' line; include all lines that start
            # with '#' and belong to this metadata block.
            start_idx = cl - 1
            j = cl
            end_idx = start_idx
            while j <= len(lines):
                raw = lines[j - 1]
                stripped = raw.lstrip()
                if stripped.startswith("#"):
                    body = stripped.lstrip("#").lstrip()
                else:
                    body = stripped
                if "]" in body:
                    end_idx = j - 1
                    break
                j += 1
            return start_idx, end_idx, cl
        return None

    # Helper: rebuild a beacon block for a given id/role/slice_labels/comment.
    def _build_beacon_block(
        bid: str,
        role: str,
        slice_labels: str,
        kind: str,
        comment_text: Optional[str],
    ) -> list[str]:
        meta_lines = [
            "# @beacon[",
            f"#   id={bid},",
        ]
        if role:
            meta_lines.append(f"#   role={role},")
        if slice_labels:
            meta_lines.append(f"#   slice_labels={slice_labels},")
        if kind:
            meta_lines.append(f"#   kind={kind},")
        if comment_text:
            meta_lines.append(f"#   comment={comment_text},")
        meta_lines.append("# ]")
        return meta_lines

    # Case 1: beacon_id present in note (update existing or create from metadata).
    if beacon_id:
        # Derive role/slice_labels/comment from note.
        role_val: str | None = None
        slice_val: str | None = None
        comment_val: str | None = None
        for line in (note or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("role:"):
                role_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("slice_labels:"):
                slice_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("comment:"):
                comment_val = stripped.split(":", 1)[1].strip()

        # Merge slice_labels from tags + note role/slice_labels using existing
        # canonicalization logic.
        role_display = role_val or (beacon_id.split("@", 1)[0] if "@" in beacon_id else "")
        slice_labels_canon = _canonicalize_slice_labels(slice_val, role_display)
        # Also pull in tags from the name as extra labels.
        if tags:
            extra = [t.lstrip("#") for t in tags]
            existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
            merged = existing + [e for e in extra if e not in existing]
            slice_labels_canon = ",".join(merged)

        block_span = _find_beacon_block_by_id(beacon_id)
        if block_span is not None:
            # Case 1a: UPDATE existing beacon on disk.
            start_idx, end_idx, _cl = block_span
            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:start_idx] + new_block + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "updated_beacon",
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result
        else:
            # Case 1b: CREATE beacon from note metadata (beacon_id in note, not on disk).
            # Use AST_QUALNAME to find insertion point.
            target_line: Optional[int] = None
            if ast_qualname:
                try:
                    outline_nodes = parse_js_ts_outline(file_path)
                except Exception:
                    outline_nodes = []

                if outline_nodes:
                    def _iter_nodes(nodes_list: list[dict[str, Any]]):
                        for n in nodes_list:
                            if isinstance(n, dict):
                                yield n
                                for ch in n.get("children") or []:
                                    if isinstance(ch, dict):
                                        yield from _iter_nodes([ch])

                    for n in _iter_nodes(outline_nodes):
                        if n.get("ast_qualname") == ast_qualname:
                            ln = n.get("orig_lineno_start_unused")
                            if isinstance(ln, int) and ln > 0:
                                target_line = ln
                                break

            if not isinstance(target_line, int) or target_line <= 0 or target_line > len(lines):
                insert_idx = 0
            else:
                insert_idx = target_line - 1

            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "created_beacon",
                    "beacon_id": beacon_id,
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result

    # Case 1c: DELETE auto-beacon when note has no BEACON metadata and no tags.
    # This covers the common Python F12 path where F12-per-node originally created
    # an auto-beacon@<AST_QUALNAME>-<hash> block and the user later deletes the tags and
    # BEACON block from the Workflowy note (leaving only AST_QUALNAME).
    if ast_qualname and not tags:
        # Scan for any auto-beacon whose id starts with 'auto-beacon@{ast_qualname}-'.
        # The hash suffix varies, so we use a prefix match.
        block_span = None
        matched_beacon_id = None
        prefix = f"auto-beacon@{ast_qualname}-"
        for b in beacons:
            bid = (b.get("id") or "").strip()
            if bid.startswith(prefix):
                matched_beacon_id = bid
                block_span = _find_beacon_block_by_id(bid)
                break
        if block_span is not None:
            start_idx, end_idx, _cl = block_span
            new_lines = lines[:start_idx] + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "deleted_beacon",
                    "beacon_id": matched_beacon_id,
                    "role": ast_qualname,
                    "slice_labels": "",
                }
            )
            return result

    # Case 2: no beacon_id, but AST_QUALNAME + tags → create a new beacon.
    if ast_qualname and tags:
        # Build a synthetic id with a hash suffix for collision resistance.
        # The role and slice_labels are canonicalized separately so tags are Workflowy-safe.
        hash_suffix = _generate_auto_beacon_hash()
        simple_id = f"auto-beacon@{ast_qualname}-{hash_suffix}"
        role_display = ast_qualname
        slice_labels_canon = _canonicalize_slice_labels(None, role_display)
        extra = [t.lstrip("#") for t in tags]
        existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
        merged = existing + [e for e in extra if e not in existing]
        slice_labels_canon = ",".join(merged)

        # Insert above the AST node header line (best-effort): use
        # parse_file_outline to discover the node and its start line.
        try:
            outline_nodes = parse_file_outline(file_path)
        except Exception:
            outline_nodes = []

        target_line: Optional[int] = None
        if outline_nodes:
            # Walk outline to find node with matching AST_QUALNAME.
            def _iter_nodes(nodes_list: list[dict[str, Any]]):
                for n in nodes_list:
                    if isinstance(n, dict):
                        yield n
                        for ch in n.get("children") or []:
                            if isinstance(ch, dict):
                                yield from _iter_nodes([ch])

            for n in _iter_nodes(outline_nodes):
                if n.get("ast_qualname") == ast_qualname:
                    ln = n.get("orig_lineno_start_unused")
                    if isinstance(ln, int) and ln > 0:
                        target_line = ln
                        break

        if not isinstance(target_line, int) or target_line <= 0 or target_line > len(lines):
            # Fallback: prepend at top of file.
            insert_idx = 0
        else:
            insert_idx = target_line - 1

        new_block = _build_beacon_block(
            bid=simple_id,
            role=role_display,
            slice_labels=slice_labels_canon,
            kind="ast",
            comment_text=None,
        )
        new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(new_lines) + "\n")
        except Exception as e:  # noqa: BLE001
            result["error"] = f"failed_to_write_file: {e}"
            return result

        result.update(
            {
                "operation": "created_beacon",
                "beacon_id": simple_id,
                "role": role_display,
                "slice_labels": slice_labels_canon,
            }
        )
        return result

    # Otherwise, noop for now.
    return result


# @beacon[
#   id=carto-js-ts@update_beacon_from_node_js_ts,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# JS/TS: update/create/delete a beacon for a single AST/beacon node based
# on Workflowy name/note fields. Mirrors the Python helper but uses
# JS/TS-specific outline + beacon parsing.
def update_beacon_from_node_js_ts(
    file_path: str,
    name: str,
    note: str,
) -> Dict[str, Any]:
    """Update/create/delete JS/TS @beacon[...] blocks for a single node.

    Mirrors the Python helper but uses JS/TS beacons and Tree-sitter outlines.
    """
    base_name, tags = split_name_and_tags(name)
    beacon_id = _extract_beacon_id_from_note(note)
    ast_qualname = _extract_ast_qualname_from_note(note)

    result: Dict[str, Any] = {
        "language": "js_ts",
        "file_path": file_path,
        "base_name": base_name,
        "tags": tags,
        "beacon_id": beacon_id,
        "ast_qualname": ast_qualname,
        "operation": "noop",
    }

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:  # noqa: BLE001
        result["error"] = f"failed_to_read_file: {e}"
        return result

    beacons = parse_js_beacon_blocks(lines)

    def _find_beacon_block_by_id(bid: str) -> Optional[tuple[int, int, int]]:
        bid = (bid or "").strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if not isinstance(cl, int) or cl <= 0:
                continue
            start_idx = cl - 1
            j = cl
            end_idx = start_idx
            while j <= len(lines):
                raw = lines[j - 1]
                if "]" in raw:
                    end_idx = j - 1
                    break
                j += 1
            return start_idx, end_idx, cl
        return None

    def _build_beacon_block(
        bid: str,
        role: str,
        slice_labels: str,
        kind: str,
        comment_text: Optional[str],
    ) -> list[str]:
        meta_lines = [
            "// @beacon[",
            f"//   id={bid},",
        ]
        if role:
            meta_lines.append(f"//   role={role},")
        if slice_labels:
            meta_lines.append(f"//   slice_labels={slice_labels},")
        if kind:
            meta_lines.append(f"//   kind={kind},")
        if comment_text:
            meta_lines.append(f"//   comment={comment_text},")
        meta_lines.append("// ]")
        return meta_lines

    # Case 1: beacon_id present in note (update existing or create from metadata).
    if beacon_id:
        role_val: str | None = None
        slice_val: str | None = None
        comment_val: str | None = None
        for line in (note or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("role:"):
                role_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("slice_labels:"):
                slice_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("comment:"):
                comment_val = stripped.split(":", 1)[1].strip()

        role_display = role_val or (beacon_id.split("@", 1)[0] if "@" in beacon_id else "")
        slice_labels_canon = _canonicalize_slice_labels(slice_val, role_display)
        if tags:
            extra = [t.lstrip("#") for t in tags]
            existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
            merged = existing + [e for e in extra if e not in existing]
            slice_labels_canon = ",".join(merged)

        block_span = _find_beacon_block_by_id(beacon_id)
        
        if block_span is not None:
            # Case 1a: UPDATE existing beacon on disk.
            start_idx, end_idx, _cl = block_span
            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:start_idx] + new_block + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "updated_beacon",
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result
        else:
            # Case 1b: CREATE beacon from note metadata (beacon_id in note, not on disk).
            # Use AST_QUALNAME to find insertion point.
            target_line: Optional[int] = None
            if ast_qualname:
                try:
                    outline_nodes = parse_js_ts_outline(file_path)
                except Exception:
                    outline_nodes = []

                if outline_nodes:
                    def _iter_nodes(nodes_list: list[dict[str, Any]]):
                        for n in nodes_list:
                            if isinstance(n, dict):
                                yield n
                                for ch in n.get("children") or []:
                                    if isinstance(ch, dict):
                                        yield from _iter_nodes([ch])

                    for n in _iter_nodes(outline_nodes):
                        if n.get("ast_qualname") == ast_qualname:
                            ln = n.get("orig_lineno_start_unused")
                            if isinstance(ln, int) and ln > 0:
                                target_line = ln
                                break

            if not isinstance(target_line, int) or target_line <= 0 or target_line > len(lines):
                insert_idx = 0
            else:
                insert_idx = target_line - 1

            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "created_beacon",
                    "beacon_id": beacon_id,
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result

    # Case 1c: DELETE auto-beacon when note has no BEACON metadata and no tags.
    # This covers the common JS/TS F12 path where F12-per-node originally created
    # an auto-beacon@<AST_QUALNAME>-<hash> block and the user later deletes the tags and
    # BEACON block from the Workflowy note (leaving only AST_QUALNAME).
    if ast_qualname and not tags:
        # Scan for any auto-beacon whose id starts with 'auto-beacon@{ast_qualname}-'.
        # The hash suffix varies, so we use a prefix match.
        block_span = None
        matched_beacon_id = None
        prefix = f"auto-beacon@{ast_qualname}-"
        for b in beacons:
            bid = (b.get("id") or "").strip()
            if bid.startswith(prefix):
                matched_beacon_id = bid
                block_span = _find_beacon_block_by_id(bid)
                break
        if block_span is not None:
            start_idx, end_idx, _cl = block_span
            new_lines = lines[:start_idx] + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "deleted_beacon",
                    "beacon_id": matched_beacon_id,
                    "role": ast_qualname,
                    "slice_labels": "",
                }
            )
            return result

    # Case 2: create from AST_QUALNAME + tags.
    if ast_qualname and tags:
        # Build a synthetic id with a hash suffix for collision resistance.
        # The role and slice_labels are canonicalized separately so tags are Workflowy-safe.
        hash_suffix = _generate_auto_beacon_hash()
        simple_id = f"auto-beacon@{ast_qualname}-{hash_suffix}"
        role_display = ast_qualname
        slice_labels_canon = _canonicalize_slice_labels(None, role_display)
        extra = [t.lstrip("#") for t in tags]
        existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
        merged = existing + [e for e in extra if e not in existing]
        slice_labels_canon = ",".join(merged)

        target_line: Optional[int] = None
        try:
            outline_nodes = parse_js_ts_outline(file_path)
        except Exception:
            outline_nodes = []

        if outline_nodes:
            def _iter_nodes(nodes_list: list[dict[str, Any]]):
                for n in nodes_list:
                    if isinstance(n, dict):
                        yield n
                        for ch in n.get("children") or []:
                            if isinstance(ch, dict):
                                yield from _iter_nodes([ch])

            for n in _iter_nodes(outline_nodes):
                if n.get("ast_qualname") == ast_qualname:
                    ln = n.get("orig_lineno_start_unused")
                    if isinstance(ln, int) and ln > 0:
                        target_line = ln
                        break

        if not isinstance(target_line, int) or target_line <= 0 or target_line > len(lines):
            insert_idx = 0
        else:
            insert_idx = target_line - 1

        new_block = _build_beacon_block(
            bid=simple_id,
            role=role_display,
            slice_labels=slice_labels_canon,
            kind="ast",
            comment_text=None,
        )
        new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(new_lines) + "\n")
        except Exception as e:  # noqa: BLE001
            result["error"] = f"failed_to_write_file: {e}"
            return result

        result.update(
            {
                "operation": "created_beacon",
                "beacon_id": simple_id,
                "role": role_display,
                "slice_labels": slice_labels_canon,
            }
        )
        return result

    return result


# @beacon[
#   id=carto-js-ts@update_beacon_from_node_markdown,
#   slice_labels=carto-js-ts,carto-js-ts-beacons,
# ]
# Markdown: update/create/delete a beacon for a single heading/beacon node
# based on Workflowy name/note fields. Uses MD_PATH to locate the heading
# and mirrors the Python/JS/TS behavior for slice_labels and comments.
def update_beacon_from_node_markdown(
    file_path: str,
    name: str,
    note: str,
) -> Dict[str, Any]:
    """Update/create/delete Markdown beacons for a single heading/beacon node.

    Uses MD_PATH to locate the heading and follows the same slice_labels
    + tags semantics as the Python/JS/TS helpers.
    """
    base_name, tags = split_name_and_tags(name)
    beacon_id = _extract_beacon_id_from_note(note)
    md_path = _extract_md_path_from_note(note)

    result: Dict[str, Any] = {
        "language": "markdown",
        "file_path": file_path,
        "base_name": base_name,
        "tags": tags,
        "beacon_id": beacon_id,
        "md_path": md_path,
        "operation": "noop",
    }

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:  # noqa: BLE001
        result["error"] = f"failed_to_read_file: {e}"
        return result

    beacons = parse_markdown_beacon_blocks(lines)

    def _find_beacon_block_by_id(bid: str) -> Optional[tuple[int, int, int]]:
        bid = (bid or "").strip()
        if not bid:
            return None
        for b in beacons:
            if (b.get("id") or "").strip() != bid:
                continue
            cl = b.get("comment_line")
            if not isinstance(cl, int) or cl <= 0:
                continue
            # For Markdown, comment_line refers to the line containing '@beacon['.
            # We treat the block as everything from the first '<!--' before it
            # through the matching '-->' after it.
            start_idx = cl - 1
            # Walk backward to include the opening '<!--' if present.
            j = cl - 1
            while j >= 1:
                raw = lines[j - 1]
                if "<!--" in raw:
                    start_idx = j - 1
                    break
                if raw.strip():
                    break
                j -= 1
            # Walk forward to include the closing '-->'.
            j = cl
            end_idx = cl - 1
            while j <= len(lines):
                raw = lines[j - 1]
                end_idx = j - 1
                if "-->" in raw:
                    break
                j += 1
            return start_idx, end_idx, cl
        return None

    def _build_beacon_block(
        bid: str,
        role: str,
        slice_labels: str,
        kind: str,
        comment_text: Optional[str],
    ) -> list[str]:
        meta_lines = [
            "<!--",
            "@beacon[",
            f"  id={bid},",
        ]
        if role:
            meta_lines.append(f"  role={role},")
        if slice_labels:
            meta_lines.append(f"  slice_labels={slice_labels},")
        if kind:
            meta_lines.append(f"  kind={kind},")
        if comment_text:
            meta_lines.append(f"  comment={comment_text},")
        meta_lines.append(" ]")
        meta_lines.append("-->")
        return meta_lines

    # Case 1: beacon_id present in note (update existing or create from metadata).
    if beacon_id:
        role_val: str | None = None
        slice_val: str | None = None
        comment_val: str | None = None
        for line in (note or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("role:"):
                role_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("slice_labels:"):
                slice_val = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("comment:"):
                comment_val = stripped.split(":", 1)[1].strip()

        role_display = role_val or (beacon_id.split("@", 1)[0] if "@" in beacon_id else "")
        slice_labels_canon = _canonicalize_slice_labels(slice_val, role_display)
        if tags:
            extra = [t.lstrip("#") for t in tags]
            existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
            merged = existing + [e for e in extra if e not in existing]
            slice_labels_canon = ",".join(merged)

        block_span = _find_beacon_block_by_id(beacon_id)
        
        if block_span is not None:
            # Case 1a: UPDATE existing beacon on disk.
            start_idx, end_idx, _cl = block_span
            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:start_idx] + new_block + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "updated_beacon",
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result
        else:
            # Case 1b: CREATE beacon from note metadata (beacon_id in note, not on disk).
            # For Markdown, use MD_PATH to find insertion point (or top-of-file).
            target_line: Optional[int] = None
            if md_path:
                # Best-effort: insert at top-of-file for now.
                # A later refinement can locate the heading via md_path.
                insert_idx = 0
            elif ast_qualname:
                # Fallback: treat as generic file insert.
                insert_idx = 0
            else:
                insert_idx = 0

            new_block = _build_beacon_block(
                bid=beacon_id,
                role=role_display or "",
                slice_labels=slice_labels_canon,
                kind="ast",
                comment_text=comment_val,
            )
            new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "created_beacon",
                    "beacon_id": beacon_id,
                    "role": role_display,
                    "slice_labels": slice_labels_canon,
                    "comment": comment_val,
                }
            )
            return result

    # Case 1c: DELETE auto-beacon when note has no BEACON metadata and no tags.
    # This covers the common Markdown F12 path where F12-per-node originally created
    # an auto-beacon@<heading> block and the user later deletes the tags and
    # BEACON block from the Workflowy note (leaving only MD_PATH).
    if md_path and not tags:
        # Try to find any auto-beacon for this MD_PATH (with any hash suffix).
        sanitized_base = (md_path[0] if md_path else "md").replace(".", "-").replace(":", "-")
        sanitized_base = re.sub(r"[^A-Za-z0-9_-]+", "-", sanitized_base).strip("-")
        block_span = None
        matched_beacon_id = None
        for b in beacons:
            bid = (b.get("id") or "").strip()
            if bid.startswith(f"auto-beacon@{sanitized_base}-"):
                matched_beacon_id = bid
                block_span = _find_beacon_block_by_id(bid)
                break
        if block_span is not None:
            start_idx, end_idx, _cl = block_span
            new_lines = lines[:start_idx] + lines[end_idx + 1 :]
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(new_lines) + "\n")
            except Exception as e:  # noqa: BLE001
                result["error"] = f"failed_to_write_file: {e}"
                return result

            result.update(
                {
                    "operation": "deleted_beacon",
                    "beacon_id": matched_beacon_id,
                    "role": (md_path[0] if md_path else "md"),
                    "slice_labels": "",
                }
            )
            return result

    # Case 2: create from MD_PATH + tags.
    if md_path and tags:
        # Build a synthetic id with a hash suffix for collision resistance.
        # The role and slice_labels are canonicalized separately so tags are Workflowy-safe.
        heading = md_path[0] if md_path else "md"
        hash_suffix = _generate_auto_beacon_hash()
        simple_id = f"auto-beacon@{heading}-{hash_suffix}"
        role_display = heading
        slice_labels_canon = _canonicalize_slice_labels(None, role_display)
        extra = [t.lstrip("#") for t in tags]
        existing = [x for x in (slice_labels_canon.split(",") if slice_labels_canon else []) if x]
        merged = existing + [e for e in extra if e not in existing]
        slice_labels_canon = ",".join(merged)

        # Best-effort insertion: prepend at top of file for now. A later pass
        # can refine this to insert near the MD_PATH heading.
        insert_idx = 0
        new_block = _build_beacon_block(
            bid=simple_id,
            role=role_display,
            slice_labels=slice_labels_canon,
            kind="ast",
            comment_text=None,
        )
        new_lines = lines[:insert_idx] + new_block + lines[insert_idx:]
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(new_lines) + "\n")
        except Exception as e:  # noqa: BLE001
            result["error"] = f"failed_to_write_file: {e}"
            return result

        result.update(
            {
                "operation": "created_beacon",
                "beacon_id": simple_id,
                "role": role_display,
                "slice_labels": slice_labels_canon,
            }
        )
        return result

    return result


def reconcile_trees(source_node: Dict[str, Any], ether_node: Dict[str, Any]) -> None:
    """
    Recursively match source nodes with ether nodes to preserve UUIDs.
    Modifies source_node in-place to add 'id' fields from ether_node.
    """
    # 1. Match self (if not root wrapper)
    if "id" in ether_node:
        source_node["id"] = ether_node["id"]
        
    # 2. Build map of Ether children by Name
    ether_children_map = {}
    if "children" in ether_node:
        for child in ether_node["children"]:
            # We match by Name. 
            # Limitation: If multiple siblings have same name, behavior is undefined (first match).
            # Workflowy allows duplicates, but code structures usually don't have duplicate classes/funcs in same scope.
            ether_children_map[child["name"]] = child
            
    # 3. Iterate Source children and try to match
    if "children" in source_node:
        for source_child in source_node["children"]:
            name = source_child["name"]
            if name in ether_children_map:
                # Match found! Recurse.
                reconcile_trees(source_child, ether_children_map[name])
                # Remove from map to handle duplicates/prevent re-matching? 
                # For now, let's leave it. Standard code shouldn't have dups.

def load_existing_map(file_path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Handle NEXUS wrapper or raw node
            if "nodes" in data and isinstance(data["nodes"], list):
                # Wrapper format. Return the first root node if matches context?
                # Actually, usually we map to a single root.
                if len(data["nodes"]) > 0:
                    return data["nodes"][0]
            elif "name" in data:
                # Single node
                return data
            elif isinstance(data, list) and len(data) > 0:
                 return data[0]
    except Exception as e:
        print(f"⚠️ Failed to load existing map: {e}")
    return None


def validate_nonempty_names(node: Dict[str, Any], path: str = "root") -> Optional[str]:
    """Ensure all nodes in a NEXUS tree have non-empty, non-whitespace names.

    This enforces the same ETHER invariant used by the Workflowy MCP: any
    prospective node that will be woven into Workflowy must have a valid name.
    """
    name = node.get("name")
    if not isinstance(name, str) or not name.strip():
        return f"{path} has empty or whitespace-only 'name'"

    children = node.get("children") or []
    for idx, child in enumerate(children):
        child_label = child.get("name") or f"child[{idx}]"
        err = validate_nonempty_names(child, f"{path} -> {child_label}")
        if err:
            return err
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map source code to NEXUS JSON.")
    parser.add_argument("path", help="Root directory to map")
    parser.add_argument("--parent-id", "-p", help="Target Workflowy Parent ID (for weaving)", default="00000000-0000-0000-0000-000000000000")
    parser.add_argument("--existing-map", "-e", help="Existing NEXUS JSON map (from Scry) for synchronization", default=None)
    parser.add_argument("--include", "-i", nargs="+", help="Whitelist of file extensions (e.g. .py .md .js)")
    parser.add_argument("--exclude", "-x", nargs="+", help="Blacklist of filename patterns (e.g. test_ venv)")
    parser.add_argument(
        "--nexus-tag",
        "-t",
        required=True,
        help="NEXUS tag; enchanted_terrain.json will be written under temp/nexus_runs/<timestamp>__<tag>/"
    )
    
    args = parser.parse_args()
    
    abs_path = os.path.abspath(args.path)
    print(f"🗺️ Mapping codebase at: {abs_path}")
    
    # Normalize extensions
    includes = [ext.lower() if ext.startswith('.') else f'.{ext.lower()}' for ext in args.include] if args.include else None
    
    nexus_tree = map_codebase(abs_path, include_exts=includes, exclude_patterns=args.exclude)

    # Validate newly generated tree: no blank or whitespace-only names
    err = validate_nonempty_names(nexus_tree)
    if err:
        print("❌ Generated codebase map contains invalid node names:")
        print(err)
        raise SystemExit(1)
    
    # Reconciliation Step
    if args.existing_map:
        print(f"🔄 Reconciling with existing map: {args.existing_map}")
        ether_root = load_existing_map(args.existing_map)
        if ether_root:
            # Ensure existing map also respects the non-empty-name invariant
            err_existing = validate_nonempty_names(ether_root)
            if err_existing:
                print("❌ Existing map contains invalid node names:")
                print(err_existing)
                raise SystemExit(1)

            # We assume nexus_tree matches the ether_root in structure (Root -> Children)
            # If names differ (e.g. root name change), we might fail to match top level,
            # but usually we care about children.
            # Use Cartographer-aware reconciliation so beacon/AST/MD_PATH anchors
            # drive ID preservation, with name-based matching as a fallback.
            reconcile_trees_cartographer(nexus_tree, ether_root)
            print("✅ Reconciliation complete (Cartographer-aware). UUIDs preserved.")
        else:
             print("⚠️ Existing map loaded but empty/invalid.")

    # Decide output location (always NEXUS run directory for this tag)
    # TIMESTAMP PREFIX: YYYY-MM-DD_HH-MM-SS__<tag>
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    timestamped_tag = f"{timestamp}__{args.nexus_tag}"
    
    project_root = os.path.dirname(os.path.abspath(__file__))
    nexus_runs_root = os.path.join(project_root, "temp", "nexus_runs")
    tag_dir = os.path.join(nexus_runs_root, timestamped_tag)
    os.makedirs(tag_dir, exist_ok=True)
    output_path = os.path.join(tag_dir, "enchanted_terrain.json")

    # Wrap in NEXUS metadata wrapper (enchanted terrain T2-style header)
    import datetime
    
    wrapper = {
        "export_root_id": args.parent_id,
        "export_root_name": nexus_tree["name"],
        "export_timestamp": datetime.datetime.now().isoformat(),
        "export_root_children_status": "complete",
        "nodes": [nexus_tree],
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(wrapper, f, indent=2)
        
    print(f"✅ Codebase map generated at: {output_path}")
