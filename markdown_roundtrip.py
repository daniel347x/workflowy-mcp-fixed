import json
import sys
import os
import argparse
from typing import Dict, Any, List
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
#   id=auto-beacon@nexus_to_tokens-chbp,
#   role=nexus_to_tokens,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def nexus_to_tokens(node: Dict[str, Any], depth: int = 0) -> List[str]:
    """Convert NEXUS node tree to markdown-it-py compatible Markdown text.
    
    We'll generate Markdown text, then let markdown-it-py parse it to tokens.
    This ensures proper handling of code blocks, escaping, etc.
    
    CRITICAL: Sorts children by priority field before emitting.
    This preserves original document order when round-tripping.
    
    Returns a list of Markdown lines.
    """
    lines = []
    name = (node.get("name") or "").strip()
    note = node.get("note") or ""
    children = node.get("children") or []

    if isinstance(note, str):
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
            note = "\n".join(note_lines[cut:])
    
    # Sort children by priority (ascending) - lower values appear first
    # This is Workflowy's native sort order and preserves document structure
    children_sorted = sorted(children, key=lambda c: c.get("priority", 999999))
    
    if depth >= 1 and name == "⚙️ YAML Frontmatter":
        lines.append("---")
        if note:
            lines.append(note)
        lines.append("---")
        lines.append("")
        return lines

    if depth == 0:
        # Root: do not emit note; only emit children at depth 1.
        # Special case: YAML frontmatter must always render FIRST in the file,
        # regardless of Workflowy priority/order drift.
        frontmatter_children = [
            child for child in children_sorted
            if (child.get("name") or "").strip() == "⚙️ YAML Frontmatter"
        ]
        normal_children = [
            child for child in children_sorted
            if (child.get("name") or "").strip() != "⚙️ YAML Frontmatter"
        ]

        for child in frontmatter_children:
            lines.extend(nexus_to_tokens(child, depth + 1))
        for child in normal_children:
            lines.extend(nexus_to_tokens(child, depth + 1))
        return lines
    
    # depth >= 1: emit heading
    level = min(depth, 6)  # Clamp to max 6 heading levels
    heading = f"{'#' * level} {name}" if name else f"{'#' * level}"
    lines.append(heading)
    lines.append("")  # Blank line after heading
    
    # Emit note content
    if note:
        lines.append(note)
        lines.append("")  # Blank line after note
    
    # Recurse into children (SORTED by priority)
    for child in children_sorted:
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

    print(f"\n{'='*60}")
    print(f"ROUND-TRIP COMPLETE")
    print(f"Output file: {output_path}")
    print(f"Output size: {len(final_markdown)} chars")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
