import json
import re
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

    Looks for YAML frontmatter in two places, in priority order:
      1. An immediate child node named ``⚙️ YAML Frontmatter`` (the canonical
         representation produced by ``parse_markdown_structure``). If found,
         the child is removed from the returned node copy and its note text
         is returned separately.
      2. (Fallback safety net, Dan 2026-04-30) The root node's own ``note``
         field, when it begins with a ``---`` fence followed by a closing
         ``---``. This catches the case where Workflowy lost the frontmatter
         child node (e.g. agent edits, F12 refresh quirks) but the YAML
         content survived inside the file root's note. Without this fallback,
         ``nexus_to_tokens`` would emit the YAML body as a regular Markdown
         heading, mangling the frontmatter (the root cause of the F12+3 99-file
         corruption observed Apr 2026).

    Returns (None, node) only when no frontmatter is detected by either path.
    """
    import sys

    if not isinstance(node, dict):
        return None, node

    children = node.get("children") or []
    if not isinstance(children, list):
        children = []

    frontmatter_text: str | None = None
    kept_children: list[Dict[str, Any]] = []
    found_child = False

    for child in children:
        if isinstance(child, dict) and (child.get("name") or "").strip() == "⚙️ YAML Frontmatter":
            if frontmatter_text is None:
                frontmatter_text = str(child.get("note") or "")
            found_child = True
            continue
        kept_children.append(child)

    if found_child:
        node_copy = dict(node)
        node_copy["children"] = kept_children
        return frontmatter_text, node_copy

    # Fallback: probe the root node's own note for a `---`-fenced YAML block.
    root_note = node.get("note") or ""
    if isinstance(root_note, str) and root_note:
        note_lines = root_note.splitlines()
        if note_lines and note_lines[0].strip() == "---":
            for idx in range(1, len(note_lines)):
                if note_lines[idx].strip() == "---":
                    fm_lines = note_lines[1:idx]
                    remaining_note_lines = note_lines[idx + 1 :]
                    fm_text = "\n".join(fm_lines).strip("\n") or None
                    print(
                        "[MD-ROUNDTRIP] detach_yaml_frontmatter_child: "
                        "recovered frontmatter from root-node note (no "
                        "'⚙️ YAML Frontmatter' child found). This indicates "
                        "the Workflowy frontmatter child went missing; "
                        "falling back to root-note YAML to avoid mangling.",
                        file=sys.stderr,
                    )
                    node_copy = dict(node)
                    # Strip the YAML block from the note so it isn't emitted twice.
                    node_copy["note"] = "\n".join(remaining_note_lines).lstrip("\n")
                    return fm_text, node_copy
            # First line was '---' but no closing '---' was found; treat as
            # normal text (don't risk mangling).

    return None, node


# @beacon[
#   id=auto-beacon@_extract_md_path_lines_from_note-z2m4,
#   role=_extract_md_path_lines_from_note,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _extract_md_path_lines_from_note(note: str | None) -> list[str]:
    """Extract raw MD_PATH heading lines from a Workflowy note.

    Bug #3 hardening (Dan, 2026-05-01): the MD_PATH section of a Workflowy
    Markdown note is supposed to contain only Markdown heading lines
    (lines starting with 1-6 '#' characters followed by whitespace and
    heading text). However, an earlier corruption mode could leave
    BEACON metadata fields ('id: ...', 'role: ...', 'slice_labels: ...',
    'kind: ast') accidentally interleaved inside the MD_PATH block when
    the closing '---' separator was missing or misplaced. To make F12+3
    phase [6]/[7] resilient against such cache poisoning, this extractor
    now skips any line inside the MD_PATH block that does NOT match the
    Markdown heading shape '^#{1,32}\s+'. The block still terminates on
    the first '---' as before. This is layer (1) of the defense-in-depth
    fix; layers (2)/(3) live in nexus_to_tokens and the parse-time
    MD_PATH builder.
    """
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
        if not stripped:
            continue
        # Layer (1) filter: only accept actual Markdown heading lines.
        # Drops stray BEACON metadata fields that may have leaked into
        # the MD_PATH block due to historical corruption.
        if not re.match(r"^#{1,32}\s+\S", stripped):
            continue
        path_lines.append(stripped)
    return path_lines


# @beacon[
#   id=auto-beacon@_heading_name_from_note_or_name-g7v1,
#   role=_heading_name_from_note_or_name,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _heading_name_from_note_or_name(note: str | None, fallback_name: str) -> str:
    """Return the heading text to emit for a Workflowy heading node.

    Policy (Dan, 2026-04-30):
      Prefer the *Workflowy node name* (with decoration stripped) so that
      heading rename in Workflowy is reflected in the regenerated Markdown.
      The MD_PATH metadata block is treated as a stale snapshot; it gets
      refreshed on the next F12 file-refresh after F12+3 anyway.

      Decoration that gets stripped from raw_name:
        - Trailing ' 🔱' / '🔱' (trident marker added by apply_markdown_beacons).
        - Trailing '#tag' tokens (slice-label tags appended to beaconed names).

      If the decoration-stripped node name is empty (shouldn't happen, but
      guard anyway), fall back to the MD_PATH last-segment text. If both
      are empty, return the raw fallback_name.

    History:
      Previously this function preferred MD_PATH last-segment text over the
      node name on the rationale that decorated node names (🔱 + #tags)
      should not leak into headings. That made heading text effectively
      read-only in Workflowy (renaming a node had no effect on the emitted
      Markdown heading). Stripping the decoration explicitly fixes both
      problems.
    """
    import re as _re_local  # local alias to avoid shadowing module-level re

    raw = (fallback_name or "").strip()
    md_path_lines = _extract_md_path_lines_from_note(note)

    # Layer (2) heading-name guard (Dan, 2026-05-01, Bug #3 hardening):
    # Reject a fallback_name that smells like a leaked BEACON metadata
    # block. Such a name (e.g., "BEACON (MD AST)\nid: ...\nrole: ...\n...")
    # would otherwise be flattened to a single line via raw.split()/join(),
    # producing a corrupt heading like
    # '## BEACON (MD AST) id: ... role: ... slice_labels: ... kind: ast'
    # on disk, which is the original Bug #3 corruption shape.
    #
    # Heuristics for "smelly":
    #   - Contains an internal newline, OR
    #   - Starts with the BEACON header marker (any language), OR
    #   - Contains two or more recognizable metadata-field prefixes
    #     ('id:', 'role:', 'slice_labels:', 'kind:').
    def _name_is_meta_block(candidate: str) -> bool:
        if not candidate:
            return False
        if "\n" in candidate or "\r" in candidate:
            return True
        head = candidate.lstrip()
        if head.startswith("BEACON ("):
            return True
        meta_markers = ("id:", "role:", "slice_labels:", "kind:")
        hits = sum(1 for m in meta_markers if m in candidate)
        if hits >= 2:
            return True
        return False

    if _name_is_meta_block(raw):
        # Discard the poisoned name and fall through to MD_PATH fallback.
        raw = ""

    # Strip Workflowy decorations from the node name to get the canonical
    # heading text the user typed.
    if raw:
        # Strip trailing '#tag' tokens (whitespace-separated).
        tokens = raw.split()
        while tokens and tokens[-1].startswith("#"):
            tokens.pop()
        # Strip trailing '🔱' marker (also handle stray whitespace before it).
        while tokens and tokens[-1].strip() == "🔱":
            tokens.pop()
        # Also strip a '🔱' that's glued to the last token without whitespace.
        if tokens:
            last = tokens[-1]
            if last.endswith("🔱"):
                tokens[-1] = last[:-1].rstrip()
                if not tokens[-1]:
                    tokens.pop()
        canonical_name = " ".join(tokens).strip()
        if canonical_name:
            return canonical_name

    # Fallback: MD_PATH last-segment text (legacy behavior).
    if md_path_lines:
        last = md_path_lines[-1].strip()
        m = _re_local.match(r"^#{1,32}\s*(.*)$", last)
        if m:
            text = (m.group(1) or "").strip()
            # Same meta-block guard on the MD_PATH-derived fallback text:
            # if MD_PATH itself was poisoned at some point, layer (1) should
            # have already filtered the offending lines, but belt-and-suspenders.
            if text and not _name_is_meta_block(text):
                return text
        if last and not _name_is_meta_block(last):
            return last

    return raw


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
#   id=auto-beacon@_load_notes_name_predicate-r6s2,
#   role=_load_notes_name_predicate,
#   slice_labels=nexus-md-header-path,ra-notes,ra-notes-salvage,
#   kind=ast,
# ]
def _load_notes_name_predicate():
    """Load the canonical NOTES/subtree classifier from workflowy_mcp.

    IMPORTANT: the salvageable/Notes emoji set must remain defined in exactly
    one place. We therefore import the canonical `_is_notes_name` helper rather
    than duplicating any emoji list in markdown_roundtrip.py.
    """
    import importlib

    module_name = "workflowy_mcp.client.api_client_nexus"
    try:
        module = importlib.import_module(module_name)
        predicate = getattr(module, "_is_notes_name", None)
        if callable(predicate):
            return predicate
    except Exception:
        pass

    here = os.path.dirname(os.path.abspath(__file__))
    candidate_parents = [
        os.path.join(here, "src"),
        os.path.join(here, "MCP_Servers"),
    ]
    for candidate in candidate_parents:
        if not os.path.isdir(os.path.join(candidate, "workflowy_mcp")):
            continue
        if candidate not in sys.path:
            sys.path.insert(0, candidate)
        try:
            module = importlib.import_module(module_name)
            predicate = getattr(module, "_is_notes_name", None)
            if callable(predicate):
                return predicate
        except Exception:
            continue

    return None


_NOTES_NAME_PREDICATE = _load_notes_name_predicate()


# @beacon[
#   id=auto-beacon@_is_persistent_notes_subtree-v9k4,
#   role=_is_persistent_notes_subtree,
#   slice_labels=nexus-md-header-path,ra-notes,ra-notes-salvage,
#   kind=ast,
# ]
def _is_persistent_notes_subtree(node: Dict[str, Any]) -> bool:
    """Return True for salvageable/persistent Notes-style Workflowy subtrees."""
    predicate = _NOTES_NAME_PREDICATE
    if not callable(predicate):
        return False
    raw_name = str(node.get("name") or "")
    note = str(node.get("note") or "")
    try:
        return bool(predicate(raw_name, note))
    except Exception:
        return False


# @beacon[
#   id=auto-beacon@_collect_markdown_ast_beacon_nodes-w8d3,
#   role=_collect_markdown_ast_beacon_nodes,
#   slice_labels=nexus-md-header-path,
#   kind=ast,
# ]
def _collect_markdown_ast_beacon_nodes(root_node: Dict[str, Any]) -> list[tuple[str, str, list[str]]]:
    """Collect Markdown heading nodes whose *current name tags* require a beacon.

    Returns a list of (name, note, md_path_lines) tuples. The returned ``note``
    has its ``MD_PATH:`` block REBUILT from the live tree's heading hierarchy
    rather than copied from the cached note.

    Source-of-truth policy for F12+3 Phase [7]
    ------------------------------------------
    The current Workflowy heading name is the source of truth for whether a
    Markdown AST beacon should exist at all:

      - If the current name has trailing ``#tags`` -> emit/reapply a beacon.
      - If the current name has no trailing ``#tags`` -> do NOT emit a beacon,
        even if the cached note still contains stale ``BEACON (MD AST)`` text.

    This makes Phase [7] the single clean materialization pass from the live
    tree state to the final on-disk Markdown file. Existing BEACON metadata in
    the note is still passed through to ``update_beacon_from_node_markdown``
    when present so that stable fields such as id/comment/kind/show_span can be
    preserved. But stale note metadata is no longer treated as evidence that a
    beacon should continue to exist after the user removed all name tags.

    Why we rebuild MD_PATH from the live tree
    -----------------------------------------
    Phase [7]'s downstream helper ``update_beacon_from_node_markdown`` calls
    ``_find_markdown_heading_insert_idx`` against the JUST-EMITTED on-disk file.
    Cached MD_PATH blocks can be stale after Workflowy heading renames, while
    phase [6] emitted headings from the live node names. Therefore we synthesize
    each candidate's MD_PATH from the same live ancestor chain used for emission,
    so insertion paths match disk reality byte-for-byte.
    """
    collected: list[tuple[str, str, list[str]]] = []

    def _name_has_trailing_tags(raw_name: str) -> bool:
        tokens = (raw_name or "").strip().split()
        return bool(tokens and tokens[-1].startswith("#"))

    def walk(node: Dict[str, Any], depth: int, ancestor_chain: list[str]) -> None:
        """Recurse, accumulating disk-emitted heading text at each depth."""
        if not isinstance(node, dict):
            return
        if _is_markdown_span_beacon_node(node) or _is_persistent_notes_subtree(node):
            return

        raw_name = (node.get("name") or "").strip()
        note = node.get("note") or ""

        # YAML Frontmatter at depth >=1 is emitted as ---/---/blank, not a
        # heading. Skip its descendants from the heading-tree walk.
        if depth >= 1 and raw_name == "⚙️ YAML Frontmatter":
            return

        # Build the next chain. Depth 0 (FILE root) does NOT contribute.
        if depth >= 1:
            own_heading = _heading_name_from_note_or_name(note, raw_name)
            new_chain = list(ancestor_chain)
            new_chain.append(own_heading or raw_name)
        else:
            new_chain = ancestor_chain

        # Phase [7] should emit a Markdown AST beacon iff the live Workflowy
        # heading name currently has trailing #tags. Existing cached BEACON
        # metadata is used only as stable metadata for that write, not as the
        # existence condition.
        if depth >= 1 and _name_has_trailing_tags(raw_name) and new_chain:
            md_path_lines: list[str] = []
            for idx, ancestor_text in enumerate(new_chain):
                level = min(idx + 1, 6)
                md_path_lines.append(f"{'#' * level} {ancestor_text}")
            rewritten_note = _rewrite_md_path_block_in_note(str(note), md_path_lines)
            collected.append((str(node.get("name") or ""), rewritten_note, md_path_lines))

        for child in node.get("children") or []:
            if isinstance(child, dict):
                walk(child, depth + 1, new_chain)

    walk(root_node, 0, [])
    collected.sort(key=lambda item: (len(item[2]), item[2]))
    return collected

def _rewrite_md_path_block_in_note(note: str, md_path_lines: list[str]) -> str:
    """Replace the ``MD_PATH:`` block in a Workflowy note with a fresh one.

    If the note has no existing ``MD_PATH:`` block, prepend a new one. The
    BEACON block and any body text after the MD_PATH block are preserved.
    """
    if not isinstance(note, str):
        note = ""

    new_md_path_block_lines = ["MD_PATH:"]
    new_md_path_block_lines.extend(md_path_lines)
    new_md_path_block_lines.append("---")

    note_lines = note.splitlines() if note else []

    # Find the existing MD_PATH: block (from "MD_PATH:" line to the next "---").
    md_path_start: int | None = None
    md_path_end: int | None = None
    for idx, line in enumerate(note_lines):
        if line.strip() == "MD_PATH:":
            md_path_start = idx
            for j in range(idx + 1, len(note_lines)):
                if note_lines[j].strip() == "---":
                    md_path_end = j
                    break
            break

    if md_path_start is not None and md_path_end is not None:
        # Replace existing block in place.
        rewritten = (
            note_lines[:md_path_start]
            + new_md_path_block_lines
            + note_lines[md_path_end + 1:]
        )
    else:
        # No MD_PATH block found; prepend.
        rewritten = new_md_path_block_lines + note_lines

    return "\n".join(rewritten)


# @beacon[
#   id=auto-beacon@reapply_markdown_ast_beacons-4n6x,
#   role=reapply_markdown_ast_beacons,
#   slice_labels=nexus-md-header-path,f9-f12-handlers,ra-reconcile,ra-bulk-visible-apply,carto-js-ts-beacons,
#   kind=ast,
#   comment=F12+3 phase 7 disk-write rehydrator. Walks Workflowy subtree for heading nodes whose current names carry trailing #tags and materializes exactly those as HTML-comment beacon blocks via update_beacon_from_node_markdown. Runs AFTER nexus_to_tokens emits beacon-free Markdown (phase 6).,
# ]
def reapply_markdown_ast_beacons(file_path: str, root_node: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Rehydrate Markdown AST beacons onto a regenerated Markdown file.

    Strategy:
      1. Export beacon-free Markdown content from Workflowy.
      2. Walk the Workflowy subtree for heading nodes whose current names have
         trailing #tags. Existing BEACON (MD AST) note metadata is used only to
         preserve stable fields (id/comment/kind/show_span), not as the beacon
         existence condition.
      3. Create/update the corresponding HTML comment beacon blocks on disk via
         Cartographer's existing update_beacon_from_node_markdown helper.

    Heading nodes without trailing #tags intentionally emit no beacon, even if
    their cached note still contains stale BEACON (MD AST) metadata. Markdown
    SPAN beacons are intentionally ignored.
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
            and not _is_persistent_notes_subtree(child)
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
        if _is_markdown_span_beacon_node(child) or _is_persistent_notes_subtree(child):
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
