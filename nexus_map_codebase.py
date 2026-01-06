import ast
import json
import os
import sys
import argparse
import tempfile
from typing import List, Dict, Any, Optional

import re
from markdown_it import MarkdownIt
import mdformat

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

# Debug flags (controlled via environment variables)
DEBUG_MD_BEACONS = bool(os.environ.get("CARTOGRAPHER_MD_BEACONS"))

# --- Parsers ---

def tokens_to_nexus_tree(tokens) -> List[Dict[str, Any]]:
    """Convert markdown-it-py token stream to NEXUS hierarchical tree.
    
    Assigns priority values based on document order (100, 200, 300, ...).
    Lower priority = appears first in document.
    
    v2.1: Now handles horizontal rules (hr) and lists (bullet_list, ordered_list).
    """
    root_children = []
    stack = []  # Stack of (heading_level, nexus_node)
    priority_counter = 100
    current_content = []  # Accumulates paragraphs, lists, hrs, code blocks as Markdown text
    
    def flush_content():
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
            
            # Create NEXUS node
            nexus_node = {
                "name": heading_text or "...",
                "priority": priority_counter,
                "children": []
            }
            priority_counter += 100
            
            # Pop stack until we find the correct parent level
            while stack and stack[-1][0] >= heading_level:
                stack.pop()
            
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
            list_lines = []
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
        ]
        -->

    Returns a list of dicts with keys:
        id, role, slice_labels, kind, start_snippet, end_snippet,
        comment_line, span_lineno_start, span_lineno_end.
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

            kind = (fields.get("kind") or "span").strip().lower() or "span"
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
                                if re.match(r"^#{1,6}\\s", lines[idx - 1]):
                                    span_end = idx - 1
                                    break

            beacon = {
                "id": fields.get("id"),
                "role": fields.get("role"),
                "slice_labels": fields.get("slice_labels"),
                "kind": kind,
                "start_snippet": start_snippet,
                "end_snippet": end_snippet,
                "comment_line": comment_line,
                "span_lineno_start": span_start,
                "span_lineno_end": span_end,
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
            context.append(stripped)
            j += 1
            continue
        break

    return context


def apply_markdown_beacons(
    lines: list[str],
    root_children: List[Dict[str, Any]],
    beacons: list[dict[str, Any]],
) -> None:
    """Attach Markdown span beacons as child nodes under headings.

    V1: we support only kind=span beacons. The `start_snippet` is typically a
    Markdown heading line (e.g. "# Title" or "## Files"). We map the beacon to
    the heading node whose text matches the heading text parsed from
    `start_snippet`. If no such heading is found, we attach the span node at the
    top level.
    """
    if not beacons or not root_children:
        return

    def iter_nodes(nodes: List[Dict[str, Any]]):
        for node in nodes:
            yield node
            for ch in node.get("children") or []:
                if isinstance(ch, dict):
                    yield from iter_nodes([ch])

    all_nodes = list(iter_nodes(root_children))

    # Precompute heading positions (line numbers) in the Markdown source so
    # we can attach span beacons to the nearest preceding heading even when
    # the @beacon[...] block itself lives far away in the file.
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
                    print(f"[MD-HEAD-CHECK] heading={heading_text!r} line={idx} text={stripped!r} (no # prefix)")
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
                    print(f"[MD-HEAD-CHECK] heading={heading_text!r} line={idx} text={stripped!r} (hash_count={hash_count} out of range)")
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
            best_node: Optional[Dict[str, Any]] = None
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
                if node_norm == norm_target or (norm_target and norm_target in node_norm):
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

        context_lines = _extract_markdown_beacon_context(lines, beacon.get("comment_line") or 0)

        note_lines = [
            "BEACON (MD SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
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
    - Replaces ':' with '-' in every label to match tag convention
      (e.g. model:mert:production → model-mert-production).
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
            tokens.append(part.replace(":", "-"))
    elif base_role:
        tokens.append(base_role.replace(":", "-"))

    return ",".join(tokens)


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
        start_snippet = beacon.get("start_snippet")
        comment_line = beacon.get("comment_line") or 0

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
        meta_lines = [
            "BEACON (AST)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: ast",
        ]
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

        note_lines = [
            "BEACON (SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
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
                    node = {
                        "name": f"{EMOJI_CLASS} class {item.name}",
                        "priority": priority_counter[0],
                        "note": get_docstring(item),
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
                    node = {
                        "name": f"{prefix} {sig}",
                        "priority": priority_counter[0],
                        "note": get_docstring(item),
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
                            node = {
                                "name": f"{EMOJI_CONST} {target.id}",
                                "priority": priority_counter[0],
                                "note": "Constant",
                                "children": [],
                                "ast_type": "const",
                                "ast_name": target.id,
                                "ast_qualname": target.id if not qual_prefix else f"{qual_prefix}.{target.id}",
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
                    if children: # Only add non-empty directories
                        nodes.append({
                            "name": f"{EMOJI_FOLDER} {item}",
                            "children": children
                        })
                
                elif os.path.isfile(full_path):
                    # Extension Check for files only
                    _, ext = os.path.splitext(item)
                    ext = ext.lower()
                    
                    if include_exts and ext not in include_exts:
                        continue
                        
                    # Dispatch
                    if ext == '.py':
                        icon = EMOJI_PYTHON
                        children = parse_file_outline(full_path)
                    elif ext == '.md':
                        icon = EMOJI_MARKDOWN
                        children = parse_markdown_structure(full_path)
                    elif ext == '.sql':
                        icon = EMOJI_SQL
                        with open(full_path, 'r', encoding='utf-8') as f:
                            sql_lines = f.read().splitlines()
                        children: List[Dict[str, Any]] = []
                        sql_beacons = parse_sql_beacon_blocks(sql_lines)
                        apply_sql_beacons(sql_lines, children, sql_beacons)
                    elif ext == '.sh':
                        icon = EMOJI_SHELL
                        with open(full_path, 'r', encoding='utf-8') as f:
                            sh_lines = f.read().splitlines()
                        children = []
                        sh_beacons = parse_sh_beacon_blocks(sh_lines)
                        apply_sh_beacons(sh_lines, children, sh_beacons)
                    else:
                        icon = EMOJI_FILE
                        children = [] # Generic file = Leaf node
                    
                    nodes.append({
                        "name": f"{icon} {item}",
                        "note": f"Path: {full_path}",
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
            return {
                "name": f"{EMOJI_PYTHON} {os.path.basename(root_path)}",
                "note": f"Path: {root_path}",
                "children": parse_file_outline(root_path)
            }
        elif ext == '.md':
            return {
                "name": f"{EMOJI_MARKDOWN} {os.path.basename(root_path)}",
                "note": f"Path: {root_path}",
                "children": parse_markdown_structure(root_path)
            }
        elif ext == '.sql':
            with open(root_path, 'r', encoding='utf-8') as f:
                sql_lines = f.read().splitlines()
            children: List[Dict[str, Any]] = []
            sql_beacons = parse_sql_beacon_blocks(sql_lines)
            apply_sql_beacons(sql_lines, children, sql_beacons)
            return {
                "name": f"{EMOJI_SQL} {os.path.basename(root_path)}",
                "note": f"Path: {root_path}",
                "children": children,
            }
        elif ext == '.sh':
            with open(root_path, 'r', encoding='utf-8') as f:
                sh_lines = f.read().splitlines()
            children: List[Dict[str, Any]] = []
            sh_beacons = parse_sh_beacon_blocks(sh_lines)
            apply_sh_beacons(sh_lines, children, sh_beacons)
            return {
                "name": f"{EMOJI_SHELL} {os.path.basename(root_path)}",
                "note": f"Path: {root_path}",
                "children": children,
            }
        else:
             return {
                "name": f"{EMOJI_FILE} {os.path.basename(root_path)}",
                "note": f"Path: {root_path}",
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
                    "comment_line": comment_line,
                }
                beacons.append(beacon)
                continue
        i += 1

    return beacons


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

        note_lines = [
            "BEACON (SQL SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
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

        note_lines = [
            "BEACON (SH SPAN)",
            f"id: {b_id}",
            f"role: {role}",
            f"slice_labels: {slice_labels}",
            "kind: span",
        ]
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
            reconcile_trees(nexus_tree, ether_root)
            print("✅ Reconciliation complete. UUIDs preserved.")
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
