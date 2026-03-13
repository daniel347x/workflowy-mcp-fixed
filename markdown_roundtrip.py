import json
import sys
import os
import argparse
from typing import Dict, Any, List, Optional
from markdown_it import MarkdownIt

# Force UTF-8 output for Windows console
if sys.platform == 'win32':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

"""
markdown_roundtrip.py (v2.0 - markdown-it-py + mdformat)

Purpose:
    Convert a Workflowy NEXUS JSON export back to Markdown using battle-tested
    markdown-it-py parser + mdformat's MDRenderer.

Key improvements over v1 (hand-rolled parser):
    - Properly handles nested code blocks (4+ backticks)
    - Preserves HTML entities correctly
    - CommonMark spec-compliant
    - No manual regex parsing - relies on proven libraries
    - Handles complex escaping automatically

Input format:
    NEXUS JSON export from workflowy_scry() / nexus_scry():
    {
        "export_root_id": "uuid",
        "export_root_name": "Node Name",
        "export_timestamp": timestamp,
        "nodes": [ ... NEXUS node tree ... ]
    }

Algorithm:
    1. Load NEXUS JSON
    2. Convert NEXUS tree → markdown-it token stream
    3. Render token stream → Markdown via MDRenderer
    4. Convert HTML entities back to Markdown syntax (<b> → **)

This produces a clean Markdown file that can be re-imported via Cartographer.
"""


# @beacon[
#   id=auto-beacon@load_nexus_root-sycb,
#   role=load_nexus_root,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def load_nexus_root(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the root node from NEXUS JSON export format.
    
    VALIDATION: Enforces single-child requirement for round-trip.
    Multiple children indicate stale CARTOGRAPHER runs at same target UUID.
    
    (Test comment for MCP validation.)
    """
    # NEXUS export format (from workflowy_scry / nexus_scry)
    if "export_root_id" in data and "nodes" in data:
        nodes = data.get("nodes") or []
        if not nodes:
            raise ValueError("NEXUS export contains no nodes")
        
        if len(nodes) > 1:
            raise ValueError(
                f"❌ GOTCHA DETECTED: SCRY contains {len(nodes)} root nodes (expected 1).\n"
                f"This indicates multiple CARTOGRAPHER runs targeted the same UUID.\n"
                f"Root nodes found: {[n.get('name', 'NO_NAME')[:50] for n in nodes]}\n\n"
                f"ACTION REQUIRED:\n"
                f"  1. Manually delete stale nodes from Workflowy, OR\n"
                f"  2. SCRY from a deeper child node that has only one version\n\n"
                f"Refusing to proceed - cannot determine which version is current."
            )
        
        return nodes[0]  # First (and only) node is the root
    
    # Fallback: direct node tree
    if "name" in data:
        return data
    
    raise ValueError("Unrecognized JSON format - expected NEXUS export or direct node tree")


# @beacon[
#   id=auto-beacon@detach_yaml_frontmatter_child-7x3q,
#   role=detach_yaml_frontmatter_child,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def detach_yaml_frontmatter_child(node: Dict[str, Any]) -> tuple[str | None, Dict[str, Any]]:
    """Return (frontmatter_text, node_without_frontmatter_child).

    If the node has an immediate child named ``⚙️ YAML Frontmatter``, we remove
    that child from the returned node copy and return its note text separately so
    callers can prepend it verbatim outside the markdown-it/mdformat pipeline.
    """
    if not isinstance(node, dict):
        return None, node

    children = node.get("children") or []
    if not isinstance(children, list):
        return None, node

    frontmatter_text: str | None = None
    kept_children: list[Dict[str, Any]] = []
    found = False

    for child in children:
        if isinstance(child, dict) and (child.get("name") or "").strip() == "⚙️ YAML Frontmatter":
            if frontmatter_text is None:
                frontmatter_text = str(child.get("note") or "")
            found = True
            continue
        kept_children.append(child)

    if not found:
        return None, node

    node_copy = dict(node)
    node_copy["children"] = kept_children
    return frontmatter_text, node_copy


# @beacon[
#   id=auto-beacon@_extract_md_path_lines_from_note-z2m4,
#   role=_extract_md_path_lines_from_note,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _extract_md_path_lines_from_note(note: str | None) -> list[str]:
    """Extract raw MD_PATH heading lines from a Workflowy note."""
    if not isinstance(note, str):
        return []
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
            path_lines.append(stripped)
    return path_lines


# @beacon[
#   id=auto-beacon@_heading_name_from_note_or_name-g7v1,
#   role=_heading_name_from_note_or_name,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _heading_name_from_note_or_name(note: str | None, fallback_name: str) -> str:
    """Prefer the canonical heading text from MD_PATH over the decorated node name."""
    import re

    md_path_lines = _extract_md_path_lines_from_note(note)
    if md_path_lines:
        last = md_path_lines[-1].strip()
        m = re.match(r"^#{1,32}\s*(.*)$", last)
        if m:
            text = (m.group(1) or "").strip()
            if text:
                return text
        if last:
            return last
    return (fallback_name or "").strip()


# @beacon[
#   id=auto-beacon@_strip_md_path_block-7s8k,
#   role=_strip_md_path_block,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _strip_md_path_block(note: str | None) -> str:
    """Remove the leading MD_PATH block from a Workflowy Markdown note."""
    if not isinstance(note, str):
        return ""
    note_lines = note.splitlines()
    if note_lines and note_lines[0].strip() == "MD_PATH:":
        cut = 1
        while cut < len(note_lines):
            if note_lines[cut].strip() == "---":
                cut += 1
                break
            cut += 1
        while cut < len(note_lines) and not note_lines[cut].strip():
            cut += 1
        return "\n".join(note_lines[cut:])
    return note


# @beacon[
#   id=auto-beacon@_strip_md_ast_beacon_block-1c9p,
#   role=_strip_md_ast_beacon_block,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _strip_md_ast_beacon_block(note_text: str | None) -> str:
    """Remove the BEACON (MD AST) metadata block from note text before emission."""
    if not isinstance(note_text, str):
        return ""

    lines = note_text.splitlines()
    start_idx: Optional[int] = None
    for idx, raw in enumerate(lines):
        if raw.strip().startswith("BEACON (MD AST)"):
            start_idx = idx
            break

    if start_idx is None:
        cleaned = lines
    else:
        end_idx = start_idx + 1
        while end_idx < len(lines):
            if lines[end_idx].strip() == "---":
                end_idx += 1
                break
            end_idx += 1
        while end_idx < len(lines) and not lines[end_idx].strip():
            end_idx += 1
        cleaned = lines[:start_idx] + lines[end_idx:]

    while cleaned and not cleaned[0].strip():
        cleaned.pop(0)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    return "\n".join(cleaned)


# @beacon[
#   id=auto-beacon@_is_markdown_span_beacon_node-s4q6,
#   role=_is_markdown_span_beacon_node,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _is_markdown_span_beacon_node(node: Dict[str, Any]) -> bool:
    """Return True for Workflowy child nodes that represent Markdown SPAN beacons.

    Executive decision (Dan): reverse Markdown round-trip currently ignores
    Markdown SPAN beacons entirely. Markdown structure should be modeled using
    headings/content blocks, not exported as synthetic SPAN beacon headings.
    """
    note = str(node.get("note") or "")
    return "BEACON (MD SPAN)" in note


# @beacon[
#   id=auto-beacon@_collect_markdown_ast_beacon_nodes-w8d3,
#   role=_collect_markdown_ast_beacon_nodes,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _collect_markdown_ast_beacon_nodes(root_node: Dict[str, Any]) -> list[tuple[str, str, list[str]]]:
    """Collect Markdown AST beacon-bearing nodes from a Workflowy subtree."""
    collected: list[tuple[str, str, list[str]]] = []

    def walk(node: Dict[str, Any]) -> None:
        if not isinstance(node, dict):
            return
        if _is_markdown_span_beacon_node(node):
            return

        note = str(node.get("note") or "")
        md_path_lines = _extract_md_path_lines_from_note(note)
        if "BEACON (MD AST)" in note and md_path_lines:
            collected.append((str(node.get("name") or ""), note, md_path_lines))

        for child in node.get("children") or []:
            if isinstance(child, dict):
                walk(child)

    walk(root_node)
    collected.sort(key=lambda item: (len(item[2]), item[2]))
    return collected


# @beacon[
#   id=auto-beacon@reapply_markdown_ast_beacons-4n6x,
#   role=reapply_markdown_ast_beacons,
#   slice_labels=nexus-md-header-path,f9-f12-handlers,
#   kind=ast,
# ]
def reapply_markdown_ast_beacons(file_path: str, root_node: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Rehydrate Markdown AST beacons onto a regenerated Markdown file.

    Strategy:
      1. Export beacon-free Markdown content from Workflowy.
      2. Walk the Workflowy subtree for nodes carrying BEACON (MD AST) metadata.
      3. Recreate/update the corresponding HTML comment beacon blocks on disk
         via Cartographer's existing update_beacon_from_node_markdown helper.

    Markdown SPAN beacons are intentionally ignored.
    """
    import importlib

    try:
        cartographer = importlib.import_module("nexus_map_codebase")
    except Exception:
        here = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.join(here, "MCP_Servers", "workflowy_mcp")
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.insert(0, candidate)
        cartographer = importlib.import_module("nexus_map_codebase")

    helper = getattr(cartographer, "update_beacon_from_node_markdown", None)
    if helper is None:
        raise RuntimeError("update_beacon_from_node_markdown helper not available")

    results: list[Dict[str, Any]] = []
    for name, note, _md_path in _collect_markdown_ast_beacon_nodes(root_node):
        result = helper(file_path, name, note)
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected markdown beacon helper result for {name!r}: {result!r}")
        if result.get("error"):
            raise RuntimeError(
                f"Failed to reapply Markdown AST beacon for {name!r}: {result.get('error')}"
            )
        op = str(result.get("operation") or "")
        if op not in {"created_beacon", "updated_beacon", "noop"}:
            raise RuntimeError(
                f"Unexpected markdown beacon operation for {name!r}: {op!r}"
            )
        results.append(result)

    return results


# @beacon[
#   id=auto-beacon@nexus_to_tokens-chbp,
#   role=nexus_to_tokens,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def nexus_to_tokens(node: Dict[str, Any], depth: int = 0) -> List[str]:
    """Convert NEXUS node tree to Markdown text while preserving MD semantics.

    CRITICAL:
    - Sort children by priority (lower first) to preserve document order.
    - Use MD_PATH as the canonical source of heading text, so Workflowy-only
      AST-beacon decorations (🔱 and trailing #tags) do not leak into headings.
    - Omit Markdown SPAN beacon child nodes entirely from reverse export.
    - Strip BEACON (MD AST) metadata blocks from note bodies; these are
      rehydrated separately as HTML comment beacon blocks after file write.
    """
    lines: List[str] = []
    raw_name = (node.get("name") or "").strip()
    note = node.get("note") or ""
    children = [child for child in (node.get("children") or []) if isinstance(child, dict)]

    # Sort children by priority (ascending) - lower values appear first.
    children_sorted = sorted(children, key=lambda c: c.get("priority", 999999))

    if depth >= 1 and raw_name == "⚙️ YAML Frontmatter":
        lines.append("---")
        if note:
            lines.append(str(note))
        lines.append("---")
        lines.append("")
        return lines

    if depth == 0:
        frontmatter_children = [
            child for child in children_sorted
            if (child.get("name") or "").strip() == "⚙️ YAML Frontmatter"
        ]
        normal_children = [
            child for child in children_sorted
            if (child.get("name") or "").strip() != "⚙️ YAML Frontmatter"
            and not _is_markdown_span_beacon_node(child)
        ]

        for child in frontmatter_children:
            lines.extend(nexus_to_tokens(child, depth + 1))
        for child in normal_children:
            lines.extend(nexus_to_tokens(child, depth + 1))
        return lines

    # depth >= 1: emit heading
    level = min(depth, 6)
    heading_name = _heading_name_from_note_or_name(note, raw_name)
    heading = f"{'#' * level} {heading_name}" if heading_name else f"{'#' * level}"
    lines.append(heading)
    lines.append("")

    note_body = _strip_md_ast_beacon_block(_strip_md_path_block(note))
    if note_body:
        lines.append(note_body)
        lines.append("")

    for child in children_sorted:
        if _is_markdown_span_beacon_node(child):
            continue
        lines.extend(nexus_to_tokens(child, depth + 1))

    return lines


# @beacon[
#   id=auto-beacon@clean_html_entities-rh2t,
#   role=clean_html_entities,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def clean_html_entities(markdown_text: str) -> str:
    """Convert HTML entities back to Markdown syntax.
    
    The forward mapper (nexus_map_codebase.py) converts Markdown **bold**
    to HTML <b>bold</b> tags. This reverses that conversion.
    
    Also handles common HTML entities that should be Markdown.
    """
    import html
    import re
    
    # Decode HTML entities first (&amp; → &, &lt; → <, etc.)
    text = html.unescape(markdown_text)
    
    # Convert <b>text</b> → **text** (bold)
    text = re.sub(r'<b>(.*?)</b>', r'**\1**', text, flags=re.DOTALL)
    
    # Convert <i>text</i> → *text* (italic)
    text = re.sub(r'<i>(.*?)</i>', r'*\1*', text, flags=re.DOTALL)
    
    # Convert <em>text</em> → *text* (emphasis)
    text = re.sub(r'<em>(.*?)</em>', r'*\1*', text, flags=re.DOTALL)
    
    # Convert <strong>text</strong> → **text** (strong)
    text = re.sub(r'<strong>(.*?)</strong>', r'**\1**', text, flags=re.DOTALL)
    
    return text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert NEXUS JSON to normalized Markdown (round-trip test)"
    )
    parser.add_argument(
        "json_path",
        help="Path to NEXUS JSON file (enchanted_terrain.json or phantom_gem.json)"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Output Markdown file path"
    )
    
    args = parser.parse_args()
    
    nexus_path = args.json_path
    output_path = args.output

    # Load NEXUS JSON
    with open(nexus_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    root = load_nexus_root(data)
    frontmatter_text, root_for_render = detach_yaml_frontmatter_child(root)
    
    print(f"\n{'='*60}")
    print(f"MARKDOWN ROUND-TRIP v2.1 (preserve raw Workflowy Markdown)")
    print(f"Root node: {root.get('name', 'NO_NAME')}")
    print(f"Root children: {len(root.get('children', []))}")
    print(f"{'='*60}\n")

    # Convert NEXUS tree → Markdown text (excluding YAML frontmatter child).
    raw_markdown = "\n".join(nexus_to_tokens(root_for_render, depth=0))

    # Parse once for validation/debug visibility, but DO NOT render back through
    # mdformat's MDRenderer. MDRenderer normalizes ordered-list markers to "1."
    # for every item, which destroys explicit numbering preserved in Workflowy
    # notes (e.g. 1/2/3/4 collapses to 1/1/1/1).
    md = MarkdownIt("commonmark")
    tokens = md.parse(raw_markdown)

    print(f"Parsed {len(tokens)} tokens from NEXUS tree")

    # Preserve the raw Workflowy-authored Markdown structure verbatim, while
    # still cleaning HTML entities/tags that may have come from Workflowy.
    final_markdown = clean_html_entities(raw_markdown)
    if frontmatter_text is not None:
        frontmatter_clean = clean_html_entities(frontmatter_text).strip("\n")
        body_clean = final_markdown.lstrip("\n")
        final_markdown = f"---\n{frontmatter_clean}\n---\n\n{body_clean}"
    
    # Write output
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_markdown)

    beacon_results = reapply_markdown_ast_beacons(output_path, root_for_render)

    print(f"\n{'='*60}")
    print(f"ROUND-TRIP COMPLETE")
    print(f"Output file: {output_path}")
    print(f"Output size: {len(final_markdown)} chars")
    print(f"Markdown AST beacons reapplied: {len(beacon_results)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
