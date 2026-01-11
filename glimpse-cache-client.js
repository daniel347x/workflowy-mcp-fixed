/**
 * Workflowy GLIMPSE Cache - Standalone WebSocket Client
 * 
 * Injected directly into Workflowy desktop app (Electron).
 * No Chrome extension APIs - pure WebSocket + DOM extraction.
 */

(function() {
  'use strict';
  
  const GLIMPSE_VERSION = '3.11.0';
  console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Standalone client initializing...`);
  
  let ws = null;
  let reconnectInterval = null;
  let isConnecting = false;
  
  const WS_URL = 'ws://localhost:8765';
  const RECONNECT_DELAY = 3000; // 3 seconds

  // ------------------------------------------------------------
  // 🔎 NEXUS-ROOT EXPANSION HELPERS (#nexus-- tags)
  // ------------------------------------------------------------

  // REMOVED: Obsolete expandSubtreeSafeById with isExpanded guard.
  // The working version (below) unconditionally calls WF.expandItem(item)
  // to handle tag-filtered views where isExpanded=true but children are
  // only partially visible.

  // Extract Workflowy UUID from a DOM node representing a bullet row
  function extractItemIdFromDomNode(node) {
    if (!node) return null;
    const id = node.getAttribute('projectid');
    if (id) return id;
    // Fallback: in case of short IDs, try WF.shortIdToId
    const shortId = node.getAttribute('data-projectid');
    if (shortId && WF.shortIdToId) {
      try {
        return WF.shortIdToId(shortId);
      } catch (e) {
        console.warn('[GLIMPSE Cache] Failed to convert shortId to UUID:', e);
      }
    }
    return null;
  }

  // Check if a node name contains any token that starts with "#nexus--" (for expansion)
  function nameHasNexusExpandTag(nameText) {
    if (!nameText) return false;
    const parts = nameText.split(/\s+/);
    return parts.some(p => p.startsWith('#nexus--'));
  }

  // Check if a node name contains the collapse tag "#collapse-nexus" (for full subtree reset)
  function nameHasNexusCollapseTag(nameText) {
    if (!nameText) return false;
    const parts = nameText.split(/\s+/);
    return parts.some(p => p === '#collapse-nexus');
  }

  // [DEPRECATED] kept for backwards compatibility if referenced elsewhere
  // prior nameHasNexusTag behavior: expansion only
  function nameHasNexusTag(nameText) {
    return nameHasNexusExpandTag(nameText);
  }

  // Robust check: does this row have any Workflowy tag whose value starts with "#nexus--"?
  // We look both at the plain text (for older structures) and at the .contentTag
  // elements Workflowy uses for clickable tags (data-val="#tag").
  function rowHasNexusExpandTag(row) {
    if (!row) return false;

    // First: fall back to the plain-text name check (historical behavior).
    const nameText = extractNodeName(row);
    if (nameHasNexusExpandTag(nameText)) {
      return true;
    }

    // Second: inspect tag widgets rendered in the name container.
    // Workflowy renders tags roughly as:
    //   <span class="contentTag" data-val="#nexus--cartographer">#<span class="contentTagText">nexus--cartographer</span>...</span>
    try {
      const tagEls = row.querySelectorAll(':scope > .name .contentTag');
      for (const tagEl of tagEls) {
        const dataVal = (tagEl.getAttribute('data-val') || '').trim();
        const textVal = (tagEl.textContent || '').trim();
        if (dataVal.startsWith('#nexus--') || textVal.startsWith('#nexus--')) {
          return true;
        }
      }
    } catch (e) {
      // Non-fatal: if DOM structure changes, we simply fall back to text-only behavior.
    }

    return false;
  }

  // Find all visible nodes whose name contains a #nexus-- tag (expansion)
  function findVisibleNexusExpandNodes() {
    const results = [];
    const rows = document.querySelectorAll('div[projectid]');

    rows.forEach(row => {
      if (!rowHasNexusExpandTag(row)) return;

      const id = extractItemIdFromDomNode(row);
      if (!id) return;

      const name = extractNodeName(row);
      results.push({ id, name, el: row });
    });

    return results;
  }

  // Find all visible nodes whose name contains the collapse tag #collapse-nexus
  function findVisibleNexusCollapseNodes() {
    const results = [];
    const rows = document.querySelectorAll('div[projectid]');

    rows.forEach(row => {
      const name = extractNodeName(row);
      if (!nameHasNexusCollapseTag(name)) return;

      const id = extractItemIdFromDomNode(row);
      if (!id) return;

      results.push({ id, name, el: row });
    });

    return results;
  }

  // Prune nested NEXUS-tagged nodes so we only operate on highest-level roots
  // among the currently visible set.
  function pruneNestedNexusRoots(nodes) {
    if (!nodes || nodes.length === 0) return [];

    const byId = new Map();
    const keep = new Set();

    // Index by id and initialize as "keep"
    nodes.forEach(node => {
      byId.set(node.id, node);
      keep.add(node.id);
    });

    // For each candidate node, walk up its ancestor chain; if it finds another
    // candidate ancestor, mark this node as nested and drop it from the keep set.
    nodes.forEach(node => {
      let current = node.el.parentElement && node.el.parentElement.closest('div[projectid]');
      while (current) {
        const ancestorId = current.getAttribute('projectid');
        if (ancestorId && byId.has(ancestorId)) {
          // This node is nested under another #nexus-- root; drop it
          keep.delete(node.id);
          break;
        }
        current = current.parentElement && current.parentElement.closest('div[projectid]');
      }
    });

    const pruned = nodes.filter(node => keep.has(node.id));
    console.log(
      `[GLIMPSE Cache v${GLIMPSE_VERSION}] GLIMPSE-EXPAND: Found ${nodes.length} #nexus-- nodes, pruned to ${pruned.length} top-level roots.`
    );
    return pruned;
  }

  const DEFAULT_NEXUS_EXPAND_OPTIONS = {
    maxDepth: 50,
    maxNodes: 4000,
    log: true,
  };

  // Expand all visible top-level #nexus-- roots
  function expandAllVisibleNexusRoots(options) {
    const candidates = findVisibleNexusExpandNodes();
    const roots = pruneNestedNexusRoots(candidates);

    if (!roots.length) {
      console.log('[GLIMPSE Cache] GLIMPSE-EXPAND: No visible #nexus-- roots found.');
      return;
    }

    const mergedOptions = Object.assign({}, DEFAULT_NEXUS_EXPAND_OPTIONS, options || {});
    console.log('[GLIMPSE Cache] GLIMPSE-EXPAND: Expanding #nexus-- roots:', roots.map(r => r.name));

    roots.forEach(root => {
      expandSubtreeSafeById(root.id, mergedOptions);
    });
  }

  // Collapse an entire subtree (bottom-up) under a given UUID
  function collapseSubtreeById(id, {
    maxDepth = 50,
    maxNodes = 4000,
    log = true,
  } = {}) {
    const root = WF.getItemById(id);
    if (!root) {
      console.warn(`[GLIMPSE Cache v${GLIMPSE_VERSION}] GLIMPSE-COLLAPSE: No item found for id ${id}`);
      return;
    }

    const queue = [{ item: root, depth: 0 }];
    const collected = [];
    let count = 0;

    // Collect nodes in breadth-first order up to bounds
    while (queue.length && count < maxNodes) {
      const { item, depth } = queue.shift();
      collected.push(item);
      count++;

      if (depth >= maxDepth) continue;

      const children = item.getChildren();
      for (const child of children) {
        queue.push({ item: child, depth: depth + 1 });
      }
    }

    // Collapse from deepest to shallowest
    for (let i = collected.length - 1; i >= 0; i--) {
      const it = collected[i];
      if (it.data.isExpanded) {
        WF.collapseItem(it);
      }
    }

    if (log) {
      console.log(
        `[GLIMPSE Cache v${GLIMPSE_VERSION}] GLIMPSE-COLLAPSE: Collapsed ${collected.length} nodes (maxNodes=${maxNodes}, maxDepth=${maxDepth}) under "${root.getNameInPlainText()}"`
      );
    }
  }

  // Collapse all visible top-level #collapse-nexus roots (full subtree reset)
  function collapseAllVisibleNexusRoots(options) {
    const candidates = findVisibleNexusCollapseNodes();
    const roots = pruneNestedNexusRoots(candidates);

    if (!roots.length) {
      console.log('[GLIMPSE Cache] GLIMPSE-COLLAPSE: No visible #nexus-- roots found.');
      return;
    }

    const mergedOptions = Object.assign({}, DEFAULT_NEXUS_EXPAND_OPTIONS, options || {});
    console.log('[GLIMPSE Cache] GLIMPSE-COLLAPSE: Collapsing #collapse-nexus roots:', roots.map(r => r.name));

    roots.forEach(root => {
      collapseSubtreeById(root.id, mergedOptions);
    });
  }

  // Expand an entire subtree (top-down) under a given UUID
  function expandSubtreeSafeById(id, {
    maxDepth = 6,
    maxNodes = 2000,
    log = true,
  } = {}) {
    const root = WF.getItemById(id);
    if (!root) {
      console.warn(`[GLIMPSE Cache v${GLIMPSE_VERSION}] GLIMPSE-EXPAND: No item found for id ${id}`);
      return;
    }

    const queue = [{ item: root, depth: 0 }];
    let count = 0;

    while (queue.length && count < maxNodes) {
      const { item, depth } = queue.shift();

      // Always call expandItem – even if Workflowy thinks the node is already
      // expanded, this guarantees we don't miss partially expanded branches.
      WF.expandItem(item);

      count++;
      if (depth >= maxDepth) continue;

      const children = item.getChildren();
      for (const child of children) {
        queue.push({ item: child, depth: depth + 1 });
      }
    }

    if (log) {
      console.log(
        `[GLIMPSE Cache v${GLIMPSE_VERSION}] GLIMPSE-EXPAND: Expanded ${count} nodes (maxNodes=${maxNodes}, maxDepth=${maxDepth}) from "${root.getNameInPlainText()}"`
      );
    }
  }

  // Allow external callers (e.g., MCP or DevTools) to trigger expansion or collapse
  window.GlimpseExpandNexusRoots = function(options) {
    expandAllVisibleNexusRoots(options);
  };
  window.GlimpseCollapseNexusRoots = function(options) {
    collapseAllVisibleNexusRoots(options);
  };

  // ------------------------------------------------------------
  // Existing GLIMPSE logic below
  // ------------------------------------------------------------
  
  /**
   * Detect if Workflowy search is active
   */
  function isSearchActive() {
    // Check for search results by looking for nodes with 'matches' class
    const matchedNodes = document.querySelectorAll('div[projectid].matches');
    return matchedNodes.length > 0;
  }
  
  /**
   * Extract complete node tree from DOM
   * 
   * Behavior:
   * - If Workflowy search is active: extract only matching nodes + their ancestor paths
   * - If no search: extract based on expansion state (existing behavior)
   */
  // @beacon[
  //   id=extract-dom-tree@glimpse-ext,
  //   slice_labels=f9-f12-handlers,glimpse-core,
  //   kind=span,
  //   comment=Main DOM extraction - converts visible Workflowy tree to JSON for MCP server,
  // ]
  function extractDOMTree(nodeId) {
    console.log('[GLIMPSE Cache] 🔍 Extracting DOM tree for node:', nodeId);
    
    try {
      const rootElement = document.querySelector(`div[projectid="${nodeId}"]`);
      
      if (!rootElement) {
        console.warn('[GLIMPSE Cache] ❌ Node not found in DOM:', nodeId);
        return {
          success: false,
          error: `Node ${nodeId} not found in DOM (may be collapsed or not loaded)`
        };
      }
      
      const rootName = extractNodeNameHtml(rootElement);
      const rootNote = extractNodeNoteHtml(rootElement);
      
      // Check if search is active
      const searchActive = isSearchActive();
      console.log(`[GLIMPSE Cache] Search active: ${searchActive}`);
      
      const children = searchActive ? 
        extractSearchResults(rootElement, true) : 
        extractChildren(rootElement, true);
      
      const nodeCount = 1 + countNodesRecursive(children);
      const depth = calculateDepth(children);
      
      console.log(`[GLIMPSE Cache] ✅ Extracted ${nodeCount} nodes, depth ${depth}`);
      console.log('[GLIMPSE Cache] 📊 TREE STRUCTURE:');
      printTree(children, 0);
      
      return {
        success: true,
        root: {
          id: nodeId,
          name: rootName,
          note: rootNote,
          parent_id: rootElement.parentElement?.closest('div[projectid]')?.getAttribute('projectid') || null
        },
        children: children,
        node_count: nodeCount,
        depth: depth,
        _source: searchActive ? 'search_results' : 'expansion'
      };
      
    } catch (error) {
      console.error('[GLIMPSE Cache] Error during extraction:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  function extractNodeName(element) {
    // @beacon[
    //   id=extract-node-name@glimpse-ext,
    //   slice_labels=glimpse-core,
    //   kind=span,
    //   comment=Extract node name text from Workflowy DOM element,
    // ]
    // SPAN DEMO START: lines inside function, wrapped by span beacon
    const spanDemoData = [
      'SPAN_DEMO_01',
      'SPAN_DEMO_02',
      'SPAN_DEMO_03',
      'SPAN_DEMO_04',
      'SPAN_DEMO_05',
      'SPAN_DEMO_06',
      'SPAN_DEMO_07',
      'SPAN_DEMO_08',
      'SPAN_DEMO_09',
      'SPAN_DEMO_10',
      'SPAN_DEMO_11',
      'SPAN_DEMO_12',
      'SPAN_DEMO_13',
      'SPAN_DEMO_14',
      'SPAN_DEMO_15',
      'SPAN_DEMO_16',
      'SPAN_DEMO_17',
      'SPAN_DEMO_18',
      'SPAN_DEMO_19',
      'SPAN_DEMO_20',
      'SPAN_DEMO_21',
      'SPAN_DEMO_22',
      'SPAN_DEMO_23',
      'SPAN_DEMO_24',
      'SPAN_DEMO_25',
    ];
    void spanDemoData;
    // @beacon-close[
    //   id=extract-node-name@glimpse-ext,
    // ]
    // SPAN DEMO END
    const nameContainer = element.querySelector(':scope > .name > .content > .innerContentContainer');
    return nameContainer ? nameContainer.textContent.trim() : 'Untitled';
  }
  
  // @beacon[
  //   id=extract-node-note@glimpse-ext,
  //   slice_labels=glimpse-core,
  //   kind=span,
  //   comment=Extract node note text from Workflowy DOM element,
  // ]
  function extractNodeNote(element) {
    const noteContainer = element.querySelector(':scope > .notes > .content > .innerContentContainer');
    if (!noteContainer || !noteContainer.textContent.trim()) {
      return null;
    }
    return noteContainer.textContent;
  }

  // Dewhiten HTML entities: &lt; &gt; &amp; → < > &
  function dewhitenText(text) {
    if (!text || typeof text !== 'string') return text;
    return text.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  }

  // Markup-preserving variants for GLIMPSE JSON (do not strip HTML tags).
  function extractNodeNameHtml(element) {
    const nameContainer = element.querySelector(':scope > .name > .content > .innerContentContainer');
    if (!nameContainer) {
      return 'Untitled';
    }
    // Preserve semantics of "empty = Untitled" while returning innerHTML
    const text = nameContainer.textContent.trim();
    if (!text) {
      return 'Untitled';
    }
    return dewhitenText(nameContainer.innerHTML);
  }

  function extractNodeNoteHtml(element) {
    const noteContainer = element.querySelector(':scope > .notes > .content > .innerContentContainer');
    if (!noteContainer) {
      return null;
    }
    if (!noteContainer.textContent.trim()) {
      return null;
    }
    // Return raw HTML so formatted notes (bold/italic/code/color spans) round-trip.
    return dewhitenText(noteContainer.innerHTML);
  }
  
  /**
   * Extract search results from DOM (when Workflowy search is active)
   * 
   * Returns hierarchical structure containing only nodes that match the search,
   * plus their ancestor path elements (for context and proper nesting).
   */
  function extractSearchResults(rootElement, useHtml = false) {
    const results = [];
    const processedIds = new Set();
    
    // Find all matching nodes under this root
    const matchedElements = rootElement.querySelectorAll('div[projectid].matches, div[projectid].terminalMatch');
    
    console.log(`[GLIMPSE Cache] Found ${matchedElements.length} search result nodes`);
    
    // For each matched node, build its path from root and collect all ancestors
    matchedElements.forEach(matchedEl => {
      const matchedId = matchedEl.getAttribute('projectid');
      if (!matchedId || processedIds.has(matchedId)) return;
      
      // Build path from root to this matched node
      const path = [];
      let current = matchedEl;
      
      while (current && current !== rootElement) {
        const id = current.getAttribute('projectid');
        if (id) {
          path.unshift({
            id: id,
            element: current,
            isMatch: current.classList.contains('matches') || current.classList.contains('terminalMatch')
          });
        }
        current = current.parentElement?.closest('div[projectid]');
      }
      
      // Insert this path into the results tree
      let currentLevel = results;
      let currentParentId = rootElement.getAttribute('projectid');
      
      path.forEach((pathNode, depth) => {
        if (processedIds.has(pathNode.id)) {
          // Already added - find it and descend
          const existing = currentLevel.find(n => n.id === pathNode.id);
          if (existing) {
            currentLevel = existing.children;
            currentParentId = existing.id;
          }
          return;
        }
        
        processedIds.add(pathNode.id);
        
        const node = {
          id: pathNode.id,
          name: useHtml ? extractNodeNameHtml(pathNode.element) : extractNodeName(pathNode.element),
          note: useHtml ? extractNodeNoteHtml(pathNode.element) : extractNodeNote(pathNode.element),
          parent_id: currentParentId,
          children: [],
          has_hidden_children: !pathNode.isMatch // Path elements might have hidden children
        };
        
        currentLevel.push(node);
        currentLevel = node.children;
        currentParentId = node.id;
      });
    });
    
    return results;
  }
  
  function extractChildren(parentElement, useHtml = false) {
    const children = [];
    const childrenContainer = parentElement.querySelector(':scope > .children');
    
    if (!childrenContainer) {
      return children;
    }
    
    const childElements = childrenContainer.querySelectorAll(':scope > div[projectid]');
    
    childElements.forEach(childElement => {
      const childId = childElement.getAttribute('projectid');
      const childName = useHtml ? extractNodeNameHtml(childElement) : extractNodeName(childElement);
      const childNote = useHtml ? extractNodeNoteHtml(childElement) : extractNodeNote(childElement);
      
      // Detect hidden children metadata for NEXUS safety
      const isCollapsed = childElement.classList.contains('collapsed');
      const expandButton = childElement.querySelector('a[data-handbook="expand.toggle"] svg');
      const hasExpandIcon = expandButton !== null;
      const grandchildrenContainer = childElement.querySelector(':scope > .children');
      const visibleGrandchildCount = grandchildrenContainer ? 
        grandchildrenContainer.querySelectorAll(':scope > div[projectid]').length : 0;
      const hasHiddenChildren = isCollapsed || (hasExpandIcon && visibleGrandchildCount === 0);
      
      const grandchildren = grandchildrenContainer ? extractChildren(childElement, useHtml) : [];
      
      children.push({
        id: childId,
        name: childName,
        note: childNote,
        parent_id: parentElement.getAttribute('projectid'),
        children: grandchildren,
        has_hidden_children: hasHiddenChildren
      });
    });
    
    return children;
  }
  
  function countNodesRecursive(nodes) {
    let count = nodes.length;
    nodes.forEach(node => {
      if (node.children && node.children.length > 0) {
        count += countNodesRecursive(node.children);
      }
    });
    return count;
  }
  
  function calculateDepth(nodes, currentDepth = 1) {
    if (!nodes || nodes.length === 0) {
      return currentDepth - 1;
    }
    
    let maxDepth = currentDepth;
    nodes.forEach(node => {
      if (node.children && node.children.length > 0) {
        const childDepth = calculateDepth(node.children, currentDepth + 1);
        maxDepth = Math.max(maxDepth, childDepth);
      }
    });
    
    return maxDepth;
  }
  
  function printTree(nodes, level) {
    const indent = '    '.repeat(level);
    nodes.forEach(node => {
      // Determine bullet emoji based on whether node has children
      const bullet = (node.children && node.children.length > 0) ? '◉' : '●';
      console.log(`${indent}${bullet} ${node.name}`);
      if (node.children && node.children.length > 0) {
        printTree(node.children, level + 1);
      }
    });
  }
  
  /**
   * Connect to Python MCP WebSocket server
   */
  function connectWebSocket() {
    if (isConnecting || (ws && ws.readyState === WebSocket.OPEN)) {
      return;
    }
    
    isConnecting = true;
    console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Connecting to`, WS_URL);
    
    try {
      ws = new WebSocket(WS_URL);
      
      ws.onopen = () => {
        console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ✅ Connected to Python MCP server`);
        isConnecting = false;
        
        if (reconnectInterval) {
          clearInterval(reconnectInterval);
          reconnectInterval = null;
        }
        
        // Send initial ping to keep connection alive
        ws.send(JSON.stringify({action: 'ping'}));
        console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Sent initial ping`);
      };
      
      // @beacon[
      //   id=ws-onmessage@glimpse-ext,
      //   slice_labels=f9-f12-handlers,
      //   kind=span,
      //   comment=WebSocket message handler - dispatches MCP server requests to client handlers,
      // ]
      ws.onmessage = (event) => {
        try {
          const request = JSON.parse(event.data);
          console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] 📩 Request from Python:`, request.action);
          
          if (request.action === 'extract_dom') {
            const result = extractDOMTree(request.node_id);
            ws.send(JSON.stringify(result));
            console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ✅ Sent response:`, result.node_count || 0, 'nodes');
          } else if (request.action === 'uuid_path_result') {
            handleUuidPathResult(request);
          } else if (request.action === 'refresh_nodes_export_cache_result') {
            handleRefreshCacheResult(request);
          } else if (request.action === 'expand_nexus_roots') {
            // Agent-triggered: expand all visible #nexus-- roots
            const opts = request.options || {};
            expandAllVisibleNexusRoots(opts);
            ws.send(JSON.stringify({
              success: true,
              action: 'expand_nexus_roots_result',
              expanded: true
            }));
          }
          
        } catch (error) {
          console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Error handling message:`, error);
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              success: false,
              error: error.message
            }));
          }
        }
      };
      
      ws.onerror = (error) => {
        console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] WebSocket error:`, error);
        isConnecting = false;
      };
      
      ws.onclose = () => {
        console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ⚠️ Disconnected from Python MCP server`);
        ws = null;
        isConnecting = false;
        
        if (!reconnectInterval) {
          reconnectInterval = setInterval(connectWebSocket, RECONNECT_DELAY);
        }
      };
      
    } catch (error) {
      console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Failed to create WebSocket:`, error);
      isConnecting = false;
      
      if (!reconnectInterval) {
        reconnectInterval = setInterval(connectWebSocket, RECONNECT_DELAY);
      }
    }
  }
  
  /**
   * UUID Hover Helper - copy Workflowy node UUIDs without DevTools
   * 
   * Algorithm (v3.9.0+):
   * - Tracks cursor position on mousemove
   * - F2 keyup → copy all UUIDs in path (instant)
   * - F3 keyup → copy leaf UUID only (instant)
   * - Mousemove hides tooltip
   * - No park timer needed (F2/F3 press is explicit intent)
   */
  let lastMousePos = null;
  let uuidTooltipEl = null;

  // UUID Navigator widget state
  let uuidNavigatorEl = null;
  let uuidNavigatorInputEl = null;
  let uuidNavigatorOutputEl = null;
  let uuidNavigatorToggleEl = null;
  let uuidNavigatorExpanded = false;
  // Tracks whether the next uuid_path_result should be rendered in FULL
  // (verbose) mode rather than compact mode.
  let uuidNavigatorFullMode = false;

  function ensureUuidTooltipElement() {
    if (uuidTooltipEl) return uuidTooltipEl;
    const el = document.createElement('div');
    el.id = 'glimpse-uuid-tooltip';
    el.style.position = 'absolute';
    el.style.zIndex = '9999';
    el.style.padding = '4px 8px';
    el.style.background = 'rgba(0, 0, 0, 0.85)';
    el.style.color = '#fff';
    el.style.fontSize = '11px';
    el.style.borderRadius = '4px';
    el.style.pointerEvents = 'none';
    el.style.maxWidth = '800px';
    el.style.maxHeight = '80vh'; // Use viewport height (allows up to 80% of screen)
    el.style.overflowY = 'auto';
    el.style.whiteSpace = 'pre-wrap';
    el.style.wordWrap = 'break-word';
    el.style.overflow = 'hidden';
    el.style.textOverflow = 'ellipsis';
    el.style.fontFamily = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
    el.style.boxShadow = '0 2px 6px rgba(0,0,0,0.4)';
    el.style.display = 'none';
    document.body.appendChild(el);
    uuidTooltipEl = el;
    return el;
  }

  function hideUuidTooltip() {
    if (uuidTooltipEl) {
      uuidTooltipEl.style.display = 'none';
    }
  }

  function showUuidTooltip(targetEl, uuid, copied, verboseMode) {
    const el = ensureUuidTooltipElement();
    const rect = targetEl.getBoundingClientRect();
    
    // Hide any existing tooltip first to prevent ghost positioning
    hideUuidTooltip();
    
    // Build hierarchical path (same as clipboard format)
    const pathData = [];
    let current = targetEl;
    while (current) {
      const name = extractNodeName(current) || 'Untitled';
      const nodeUuid = current.getAttribute('projectid');
      pathData.unshift({name, uuid: nodeUuid});
      current = current.parentElement && current.parentElement.closest('div[projectid]');
    }
    
    // Format tooltip to match clipboard (with inline backticks for ancestors)
    const lines = [];
    pathData.forEach((node, depth) => {
      const prefix = '#'.repeat(depth + 1);
      lines.push(`${prefix} ${node.name}`);
      if (verboseMode) {
        lines.push(`\`${node.uuid}\``);
        lines.push('');
      }
    });
    
    // Always show final UUID with arrow indicator
    if (verboseMode) {
      lines.push('--> Use Leaf UUID:');
      lines.push(`\`${uuid}\``);
    } else {
      lines.push('');
      lines.push(`→ \`${uuid}\``);
    }
    
    if (copied) {
      lines.push('');
      const mode = verboseMode ? ' (verbose)' : '';
      lines.push(`✓ Copied${mode}`);
    }
    
    el.textContent = lines.join('\n');
    
    // Position tooltip below cursor, accounting for scroll offset
    const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
    const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
    el.style.top = `${top}px`;
    el.style.left = `${left}px`;
    el.style.display = 'block';
    clearTimeout(el._hideTimer);
    el._hideTimer = setTimeout(() => {
      hideUuidTooltip();
    }, 10000);
  }

  function showUuidTooltipText(targetEl, text) {
    const el = ensureUuidTooltipElement();
    const rect = targetEl.getBoundingClientRect();
    
    // Hide any existing tooltip first to prevent ghost positioning
    hideUuidTooltip();
    
    // Add "Copied" indicator
    const displayLines = text.split('\n');
    displayLines.push('');
    displayLines.push('✓ Copied (from server)');
    
    // Render with blue bullets (same logic as Navigator widget)
    el.textContent = '';
    el.innerHTML = '';
    
    displayLines.forEach((line) => {
      const lineDiv = document.createElement('div');
      
      // Add hanging indent styling (matches UUID Explorer)
      lineDiv.style.whiteSpace = 'pre-wrap';
      lineDiv.style.paddingLeft = '12px';
      lineDiv.style.textIndent = '-12px';
      
      // Preserve blank lines by giving them minimal content
      if (line.trim().length === 0) {
        lineDiv.innerHTML = '&nbsp;';  // Non-breaking space prevents collapse
      } else {
        // Check for header with bullet: "# ⦿ Name" or "## • Name"
        const headerMatch = line.match(/^(#+)(\s+)([⦿•])(\s+)(.*)$/);
        if (headerMatch) {
          const hashes = headerMatch[1];
          const bullet = headerMatch[3];
          const rest = headerMatch[5];
          
          const hashesNode = document.createTextNode(hashes + ' ');
          const bulletSpan = document.createElement('span');
          bulletSpan.textContent = bullet;
          bulletSpan.style.color = '#66b3ff';
          const spaceNode = document.createTextNode(' ');
          const restNode = document.createTextNode(rest);
          
          lineDiv.appendChild(hashesNode);
          lineDiv.appendChild(bulletSpan);
          lineDiv.appendChild(spaceNode);
          lineDiv.appendChild(restNode);
        } else {
          // Check for arrow lines: "→ Use Leaf UUID:" or "→ `uuid`"
          const arrowMatch = line.match(/^(\s*)(→)(\s*)(.*)$/);
          if (arrowMatch) {
            const leading = arrowMatch[1] || '';
            const arrow = arrowMatch[2];
            const between = arrowMatch[3] || ' ';
            const tail = arrowMatch[4] || '';
            
            if (leading) {
              lineDiv.appendChild(document.createTextNode(leading));
            }
            const arrowSpan = document.createElement('span');
            arrowSpan.textContent = arrow;
            arrowSpan.style.color = '#66b3ff';
            lineDiv.appendChild(arrowSpan);
            lineDiv.appendChild(document.createTextNode(between + tail));
          } else {
            // Plain text line (UUID, checkmark, etc.)
            lineDiv.textContent = line;
          }
        }
      }
      
      el.appendChild(lineDiv);
    });
    
    // Position tooltip below cursor, accounting for scroll offset
    const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
    const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
    el.style.top = `${top}px`;
    el.style.left = `${left}px`;
    el.style.display = 'block';
    clearTimeout(el._hideTimer);
    el._hideTimer = setTimeout(() => {
      hideUuidTooltip();
    }, 10000);
  }

  function copyUuidForElement(el, includeAllUuids) {
    const uuid = el.getAttribute('projectid');
    if (!uuid) return;

    // Use WebSocket to resolve full path from server if available
    if (ws && ws.readyState === WebSocket.OPEN) {
      isHoverRequest = true;
      isHoverFullMode = includeAllUuids; // F2=true, F3=false
      lastHoverTargetEl = el;
      
      const payload = {
        action: 'resolve_uuid_path',
        target_uuid: uuid,
        format: includeAllUuids ? 'f2' : 'f3'
      };
      ws.send(JSON.stringify(payload));
      
      // Show immediate feedback
      const elTooltip = ensureUuidTooltipElement();
      // Position...
      const rect = el.getBoundingClientRect();
      const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
      const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
      elTooltip.style.top = `${top}px`;
      elTooltip.style.left = `${left}px`;
      elTooltip.textContent = 'Resolving full path from server...';
      elTooltip.style.display = 'block';
      return;
    }
    
    // Fallback to DOM traversal if WebSocket unavailable
    // Build path from root to current node
    const pathData = [];
    let current = el;
    while (current) {
      const name = extractNodeName(current) || 'Untitled';
      const nodeUuid = current.getAttribute('projectid');
      pathData.unshift({name, uuid: nodeUuid});
      current = current.parentElement && current.parentElement.closest('div[projectid]');
    }
    
    // Format as Markdown
    const lines = [];
    pathData.forEach((node, depth) => {
      const prefix = '#'.repeat(depth + 1);
      lines.push(`${prefix} ${node.name}`);
      if (includeAllUuids) {
        lines.push(`\`${node.uuid}\``);
        lines.push('');
      }
    });
    
    // Always show final UUID with arrow indicator
    if (includeAllUuids) {
      lines.push('--> Use Leaf UUID:');
      lines.push(`\`${uuid}\``);
    } else {
      lines.push('');
      lines.push(`→ \`${uuid}\``);
    }
    
    const copyText = lines.join('\n');
    
    const finish = (copied) => {
      showUuidTooltip(el, uuid, copied, includeAllUuids);
    };
    
    // Modern Clipboard API requires document focus - try to ensure it
    if (navigator.clipboard && navigator.clipboard.writeText) {
      if (document.hasFocus && !document.hasFocus()) {
        window.focus();
      }
      
      navigator.clipboard.writeText(copyText).then(
        () => finish(true),
        (err) => fallbackCopy(copyText, finish)
      );
    } else {
      fallbackCopy(copyText, finish);
    }
  }

  // Build Markdown path from DOM hierarchy for a given element
  function buildMarkdownPathFromDom(el) {
    if (!el) return null;
    const uuid = el.getAttribute('projectid');
    if (!uuid) return null;

    const pathData = [];
    let current = el;
    while (current) {
      const name = extractNodeName(current) || 'Untitled';
      const nodeUuid = current.getAttribute('projectid');
      pathData.unshift({ name, uuid: nodeUuid });
      current = current.parentElement && current.parentElement.closest('div[projectid]');
    }

    const lines = [];
    pathData.forEach((node, depth) => {
      const prefix = '#'.repeat(depth + 1);
      lines.push(`${prefix} ${node.name}`);
    });
    lines.push('');
    lines.push(`→ \`${uuid}\``);

    return lines.join('\n');
  }

  // Build verbose Markdown path (with UUIDs under each node) from DOM hierarchy
  function buildFullMarkdownPathFromDom(el) {
    if (!el) return null;
    const uuid = el.getAttribute('projectid');
    if (!uuid) return null;

    const pathData = [];
    let current = el;
    while (current) {
      const name = extractNodeName(current) || 'Untitled';
      const nodeUuid = current.getAttribute('projectid');
      pathData.unshift({ name, uuid: nodeUuid });
      current = current.parentElement && current.parentElement.closest('div[projectid]');
    }

    const lines = [];
    pathData.forEach((node, depth) => {
      const prefix = '#'.repeat(depth + 1);
      lines.push(`${prefix} ${node.name}`);
      lines.push(`\`${node.uuid}\``);
      lines.push('');
    });

    lines.push('--> Use Leaf UUID:');
    lines.push(`\`${uuid}\``);

    return lines.join('\n');
  }
  
  function fallbackCopy(text, callback) {
    try {
      const tmp = document.createElement('textarea');
      tmp.value = text;
      tmp.style.position = 'fixed';
      tmp.style.opacity = '0';
      tmp.style.pointerEvents = 'none';
      document.body.appendChild(tmp);
      tmp.select();
      tmp.focus(); // Explicitly focus the textarea
      const success = document.execCommand('copy');
      document.body.removeChild(tmp);
      callback(success);
    } catch (err) {
      console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Fallback copy failed:`, err);
      callback(false);
    }
  }

  // UUID validation helper
  function isValidUuid(str) {
    if (!str) return false;
    const trimmed = str.trim();
    // UUID format: 8-4-4-4-12 hex digits, or virtual_<uuid>_...
    // Also accept virtual UUIDs (server will parse them)
    if (trimmed.startsWith('virtual_')) return true;
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(trimmed);
  }
  
  // Extract UUID from pasted text (handles markdown with UUIDs)
  function extractUuidFromText(text) {
    if (!text) return null;
    
    // If already a valid UUID, return as-is
    if (isValidUuid(text)) return text.trim();
    
    // FIRST: Check for F2 "→ Use Leaf UUID:" pattern (explicit leaf UUID marker)
    const leafUuidMatch = text.match(/→\s*Use Leaf UUID:\s*\n?\s*`?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|virtual_[0-9a-f-_]+)`?/i);
    if (leafUuidMatch && leafUuidMatch[1]) {
      return leafUuidMatch[1];
    }
    
    // FALLBACK: Try to extract UUID from markdown patterns:
    // "→ `uuid`" or just "`uuid`" (works for F3 output)
    const uuidPattern = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|virtual_[0-9a-f-_]+/gi;
    const matches = text.match(uuidPattern);
    
    if (matches && matches.length > 0) {
      // Return last match (typically the leaf UUID in F3 format)
      return matches[matches.length - 1];
    }
    
    return null;
  }

  // UUID Navigator widget

  function generateRandomHash(length = 6) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';

    if (window.crypto && window.crypto.getRandomValues) {
      const arr = new Uint32Array(length);
      window.crypto.getRandomValues(arr);
      for (let i = 0; i < length; i++) {
        result += chars[arr[i] % chars.length];
      }
    } else {
      for (let i = 0; i < length; i++) {
        result += chars[Math.floor(Math.random() * chars.length)];
      }
    }

    return result;
  }

  // UUID Navigator widget
  function initializeUuidNavigator() {
    if (uuidNavigatorEl) return;

    const container = document.createElement('div');
    container.id = 'glimpse-uuid-navigator';
    container.style.position = 'fixed';
    container.style.right = '16px';
    container.style.bottom = '16px';
    container.style.zIndex = '9999';
    container.style.background = 'rgba(0, 0, 0, 0.8)';
    container.style.color = '#fff';
    container.style.padding = '8px';
    container.style.borderRadius = '6px';
    container.style.fontSize = '11px';
    container.style.fontFamily = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
    container.style.width = '760px';
    container.style.maxWidth = '760px';
    container.style.boxShadow = '0 2px 8px rgba(0,0,0,0.5)';

    const header = document.createElement('div');
    header.style.display = 'flex';
    header.style.alignItems = 'center';
    header.style.justifyContent = 'space-between';
    header.style.marginBottom = '4px';

    const title = document.createElement('span');
    title.textContent = 'UUID Explorer';

    const toggle = document.createElement('button');
    toggle.textContent = '▾';
    toggle.style.background = 'transparent';
    toggle.style.border = 'none';
    toggle.style.color = '#fff';
    toggle.style.cursor = 'pointer';
    toggle.style.fontSize = '11px';

    toggle.addEventListener('click', () => {
      uuidNavigatorExpanded = !uuidNavigatorExpanded;
      toggle.textContent = uuidNavigatorExpanded ? '▾' : '▸';
      if (uuidNavigatorOutputEl) {
        uuidNavigatorOutputEl.style.display = uuidNavigatorExpanded ? 'block' : 'none';
      }
    });

    header.appendChild(title);
    header.appendChild(toggle);

    const row = document.createElement('div');
    row.style.display = 'flex';
    row.style.gap = '4px';
    row.style.marginBottom = '4px';

    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = 'Paste UUID...';
    input.style.flex = '1 1 auto';
    input.style.fontSize = '11px';
    input.style.padding = '2px 4px';

    const goBtn = document.createElement('button');
    goBtn.textContent = 'Go';
    goBtn.style.fontSize = '11px';
    goBtn.addEventListener('click', () => {
      const value = input.value.trim();
      if (!value) return;
      
      if (!isValidUuid(value)) {
        if (uuidNavigatorOutputEl) {
          uuidNavigatorOutputEl.style.display = 'block';
          uuidNavigatorOutputEl.textContent = 'Invalid UUID format';
        }
        uuidNavigatorExpanded = true;
        if (uuidNavigatorToggleEl) {
          uuidNavigatorToggleEl.textContent = '▾';
        }
        return;
      }
      
      uuidNavigatorFullMode = false;
      requestUuidPath(value);
    });

    const goFullBtn = document.createElement('button');
    goFullBtn.textContent = 'Go (full)';
    goFullBtn.style.fontSize = '11px';
    goFullBtn.addEventListener('click', () => {
      const value = input.value.trim();
      if (!value) return;
      
      if (!isValidUuid(value)) {
        if (uuidNavigatorOutputEl) {
          uuidNavigatorOutputEl.style.display = 'block';
          uuidNavigatorOutputEl.textContent = 'Invalid UUID format';
        }
        uuidNavigatorExpanded = true;
        if (uuidNavigatorToggleEl) {
          uuidNavigatorToggleEl.textContent = '▾';
        }
        return;
      }
      
      uuidNavigatorFullMode = true;
      requestUuidPath(value);
    });

    const pasteBtn = document.createElement('button');
    pasteBtn.textContent = 'Paste';
    pasteBtn.style.fontSize = '11px';
    pasteBtn.addEventListener('click', async () => {
      try {
        const text = await navigator.clipboard.readText();
        const extracted = extractUuidFromText(text);
        
        if (!extracted) {
          if (uuidNavigatorOutputEl) {
            uuidNavigatorOutputEl.style.display = 'block';
            uuidNavigatorOutputEl.textContent = 'Invalid UUID pasted from clipboard';
          }
          uuidNavigatorExpanded = true;
          if (uuidNavigatorToggleEl) {
            uuidNavigatorToggleEl.textContent = '▾';
          }
          return;
        }
        
        input.value = extracted;
        uuidNavigatorFullMode = false;
        requestUuidPath(extracted);
      } catch (err) {
        console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Paste failed:`, err);
      }
    });

    const copyBtn = document.createElement('button');
    copyBtn.textContent = 'Copy';
    copyBtn.style.fontSize = '11px';
    copyBtn.addEventListener('click', () => {
      if (!uuidNavigatorOutputEl) return;

      let text = '';
      if (uuidNavigatorOutputEl.childNodes && uuidNavigatorOutputEl.childNodes.length > 0) {
        // We rendered one logical line per div with a hanging indent; rebuild the
        // clipboard text as those lines joined with real newlines so the path
        // structure is preserved outside the widget.
        text = Array.from(uuidNavigatorOutputEl.childNodes)
          .map((node) => node.textContent)
          .join('\n');
      } else if (uuidNavigatorOutputEl.textContent) {
        text = uuidNavigatorOutputEl.textContent;
      }

      if (!text) return;

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).catch((err) => {
          console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] UUID Navigator copy failed:`, err);
          fallbackCopy(text, () => {});
        });
      } else {
        fallbackCopy(text, () => {});
      }
    });

    const copyUuidBtn = document.createElement('button');
    copyUuidBtn.textContent = 'Copy UUID';
    copyUuidBtn.style.fontSize = '11px';
    copyUuidBtn.addEventListener('click', () => {
      const value = input.value.trim();
      if (!value) return;
      
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(value).catch((err) => {
          console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Copy UUID failed:`, err);
          fallbackCopy(value, () => {});
        });
      } else {
        fallbackCopy(value, () => {});
      }
    });
    
    const clearBtn = document.createElement('button');
    clearBtn.textContent = 'X';
    clearBtn.style.fontSize = '11px';
    clearBtn.addEventListener('click', () => {
      input.value = '';
      if (uuidNavigatorOutputEl) {
        uuidNavigatorOutputEl.textContent = '';
      }
    });

    const hashBtn = document.createElement('button');
    hashBtn.textContent = 'hash';
    hashBtn.style.fontSize = '11px';
    hashBtn.addEventListener('click', () => {
      const h = generateRandomHash(6);

      if (uuidNavigatorInputEl) {
        uuidNavigatorInputEl.value = h;
      }

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(h).catch((err) => {
          console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Copy hash failed:`, err);
          fallbackCopy(h, () => {});
        });
      } else {
        fallbackCopy(h, () => {});
      }
    });

    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const value = input.value.trim();
        if (value) {
          uuidNavigatorFullMode = false;
          requestUuidPath(value);
        }
      }
    });

    input.addEventListener('paste', (e) => {
      const clipboard = e.clipboardData || window.clipboardData;
      if (!clipboard) return;
      const data = clipboard.getData('text');
      if (!data) return;
      
      e.preventDefault();
      const extracted = extractUuidFromText(data);
      
      if (!extracted) {
        input.value = data.trim();
        if (uuidNavigatorOutputEl) {
          uuidNavigatorOutputEl.style.display = 'block';
          uuidNavigatorOutputEl.textContent = 'Invalid UUID format';
        }
        uuidNavigatorExpanded = true;
        if (uuidNavigatorToggleEl) {
          uuidNavigatorToggleEl.textContent = '▾';
        }
        return;
      }
      
      input.value = extracted;
      uuidNavigatorFullMode = false;
      requestUuidPath(extracted);
    });

    row.appendChild(input);
    row.appendChild(goBtn);
    row.appendChild(goFullBtn);
    row.appendChild(pasteBtn);
    row.appendChild(copyBtn);
    row.appendChild(copyUuidBtn);
    row.appendChild(clearBtn);
    row.appendChild(hashBtn);

    const refreshBtn = document.createElement('button');
    refreshBtn.textContent = 'Refresh cache';
    refreshBtn.style.background = 'transparent';
    refreshBtn.style.border = 'none';
    refreshBtn.style.color = '#ccc';
    refreshBtn.style.cursor = 'pointer';
    refreshBtn.style.fontSize = '10px';
    refreshBtn.style.padding = '0';
    refreshBtn.style.margin = '0 0 4px 0';
    refreshBtn.addEventListener('click', () => {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        if (uuidNavigatorOutputEl) {
          uuidNavigatorOutputEl.style.display = 'block';
          uuidNavigatorOutputEl.textContent = 'WebSocket not connected (cannot refresh cache).';
        }
        return;
      }
      const payload = { action: 'refresh_nodes_export_cache' };
      console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Sending refresh_nodes_export_cache request`);
      ws.send(JSON.stringify(payload));
      if (uuidNavigatorOutputEl) {
        uuidNavigatorOutputEl.style.display = 'block';
        uuidNavigatorOutputEl.textContent = 'Refreshing /nodes-export cache...';
      }
    });

    const output = document.createElement('pre');
    output.style.margin = '0';
    output.style.marginTop = '4px';
    output.style.maxHeight = '160px';
    output.style.overflowY = 'auto';
    output.style.whiteSpace = 'pre-wrap';
    output.style.wordWrap = 'break-word';
    output.style.wordBreak = 'break-word';
    output.style.paddingLeft = '4px';
    // Ensure the rendered path text is manually selectable for partial copy
    output.style.userSelect = 'text';
    output.style.webkitUserSelect = 'text';
    output.style.MozUserSelect = 'text';
    output.style.display = 'none';

    container.appendChild(header);
    container.appendChild(row);
    container.appendChild(refreshBtn);
    container.appendChild(output);

    document.body.appendChild(container);

    uuidNavigatorEl = container;
    uuidNavigatorInputEl = input;
    uuidNavigatorOutputEl = output;
    uuidNavigatorToggleEl = toggle;
  }

  function requestUuidPath(targetUuid) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      if (uuidNavigatorOutputEl) {
        uuidNavigatorOutputEl.style.display = 'block';
        uuidNavigatorOutputEl.textContent = 'WebSocket not connected (GLIMPSE server unavailable).';
      }
      uuidNavigatorExpanded = true;
      if (uuidNavigatorToggleEl) {
        uuidNavigatorToggleEl.textContent = '▾';
      }
      return;
    }

    const payload = {
      action: 'resolve_uuid_path',
      target_uuid: targetUuid,
      format: uuidNavigatorFullMode ? 'f2' : 'f3'
    };

    ws.send(JSON.stringify(payload));

    if (uuidNavigatorOutputEl) {
      uuidNavigatorOutputEl.style.display = 'block';
      uuidNavigatorOutputEl.textContent = `Resolving path for ${targetUuid}...`;
    }
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }
  }

  // Render a given Markdown path into the UUID Navigator output area
  function renderUuidNavigatorPath(text) {
    if (!uuidNavigatorOutputEl) return;

    uuidNavigatorOutputEl.style.display = 'block';
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }

    const markdown = text || '(No path information returned)';

    // Render each logical line with a hanging indent so the start of the line
    // is visually outset relative to its own word-wrapped continuation.
    uuidNavigatorOutputEl.textContent = '';
    const lines = markdown.split('\n');

    // Determine how many leading header lines we have (before the first blank
    // line). The last header line is treated as the leaf; earlier ones are
    // treated as ancestors ("has children").
    let headerCount = 0;
    for (let i = 0; i < lines.length; i++) {
      const raw = lines[i];
      if (raw.trim().length === 0) break;
      if (!raw.match(/^(#+)(\s+)(.*)$/)) break;
      headerCount++;
    }

    lines.forEach((line, idx) => {
      const lineEl = document.createElement('div');
      lineEl.style.whiteSpace = 'pre-wrap';
      lineEl.style.paddingLeft = '12px';
      lineEl.style.textIndent = '-12px';

      if (line.trim().length === 0) {
        // Preserve visual blank lines in the widget by giving the div minimal
        // content; clipboard reconstruction still inserts a real '\n' for
        // this logical line.
        lineEl.textContent = ' ';
      } else {
        let display = line;
        const m = display.match(/^(#+)(\s+)(.*)$/);
        if (m) {
          const hashes = m[1];
          const rest = m[3];
          
          // Check if bullet already present in server markdown
          // Server now sends: "# ⦿ Work" or "# • Leaf"
          const bulletMatch = rest.match(/^([⦿•])(\s+)(.*)$/);
          
          if (bulletMatch) {
            // Bullet already present - just colorize it
            const bullet = bulletMatch[1];
            const restAfterBullet = bulletMatch[3];
            
            lineEl.textContent = '';
            const hashesNode = document.createTextNode(hashes + ' ');
            const bulletSpan = document.createElement('span');
            bulletSpan.textContent = bullet;
            bulletSpan.style.color = '#66b3ff';
            const spaceAfter = document.createTextNode(' ');
            const restNode = document.createTextNode(restAfterBullet);
            lineEl.appendChild(hashesNode);
            lineEl.appendChild(bulletSpan);
            lineEl.appendChild(spaceAfter);
            lineEl.appendChild(restNode);
          } else {
            // No bullet in server markdown (shouldn't happen with new server, but fallback)
            lineEl.textContent = line;
          }
        } else {
          // Special case: color the leading arrow on the UUID line, but keep
          // the rest of the text in the default color.
          const arrowMatch = display.match(/^(\s*)→(\s*)(.*)$/);
          if (arrowMatch) {
            const leading = arrowMatch[1] || '';
            const between = arrowMatch[2] || ' ';
            const tail = arrowMatch[3] || '';
            lineEl.textContent = '';
            if (leading) {
              lineEl.appendChild(document.createTextNode(leading));
            }
            const arrowSpan = document.createElement('span');
            arrowSpan.textContent = '→';
            arrowSpan.style.color = '#66b3ff';
            lineEl.appendChild(arrowSpan);
            lineEl.appendChild(document.createTextNode(between + tail));
          } else {
            lineEl.textContent = display;
          }
        }
      }

      uuidNavigatorOutputEl.appendChild(lineEl);
    });
  }

  // Show a FULL (verbose) path for a UUID now uses server-resolved path
  // (message.path) via uuidNavigatorFullMode; no DOM-based ancestry.
  // The actual rendering is handled in handleUuidPathResult.

  // Determine whether the last request came from F2/F3 keypress
  // so we can show tooltip instead of Navigator.
  let isHoverRequest = false;
  // If F2 (verbose) was pressed, we want the FULL path mode.
  let isHoverFullMode = false;
  // Store the target element for tooltip positioning when async response returns
  let lastHoverTargetEl = null;

  function handleUuidPathResult(message) {
    if (isHoverRequest) {
      isHoverRequest = false;
      // Handle result for F2/F3 hover request -> show tooltip + copy to clipboard
      if (!message.success) {
        console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] F2/F3 path resolution failed:`, message.error);
        return;
      }

      // Use server-generated markdown directly (already has bullets, HTML decoding, etc.)
      let markdown = message.markdown || '';
      const targetUuid = message.target_uuid || message.uuid;

      // Fallback to DOM only if server didn't provide markdown
      if ((!markdown || !markdown.trim()) && targetUuid && lastHoverTargetEl) {
        const domMarkdown = buildMarkdownPathFromDom(lastHoverTargetEl);
        if (domMarkdown) {
          markdown = domMarkdown;
        }
      }

      if (!markdown) return;

      // Copy to clipboard
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(markdown).catch(err => {
          console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Clipboard write failed:`, err);
          fallbackCopy(markdown, () => {});
        });
      } else {
        fallbackCopy(markdown, () => {});
      }

      // Show tooltip
      if (lastHoverTargetEl) {
        // Reuse existing showUuidTooltip logic but pass pre-formatted text
        // We'll modify showUuidTooltip to accept text directly.
        showUuidTooltipText(lastHoverTargetEl, markdown);
      }
      return;
    }

    if (!uuidNavigatorOutputEl) return;

    uuidNavigatorOutputEl.style.display = 'block';
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }

    if (!message.success) {
      uuidNavigatorOutputEl.textContent = `Error: ${message.error || 'Unknown error'}`;
      return;
    }

    let markdown = message.markdown || '';
    const targetUuid = message.target_uuid || message.uuid;

    // COMPACT MODE (or fallback if server didn't already format it fully):
    // rely on the MCP server's resolved markdown path. Only if that is
    // empty do we attempt a DOM fallback.
    // NOTE: The server now handles full/compact formatting internally
    // based on the 'format' param (f2/f3). So we just render what we get.
    if ((!markdown || !markdown.trim()) && targetUuid) {
      const el = document.querySelector(`div[projectid="${targetUuid}"]`);
      if (el) {
        const domMarkdown = buildMarkdownPathFromDom(el);
        if (domMarkdown) {
          markdown = domMarkdown;
        }
      }
    }

    uuidNavigatorFullMode = false;
    renderUuidNavigatorPath(markdown || '(No path information returned)');
  }

  function handleRefreshCacheResult(message) {
    console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] refresh_nodes_export_cache_result:`, message);
    if (!uuidNavigatorOutputEl) return;

    uuidNavigatorOutputEl.style.display = 'block';
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }

    if (!message.success) {
      uuidNavigatorOutputEl.textContent = `Cache refresh failed: ${message.error || 'Unknown error'}`;
      return;
    }

    const count = message.node_count != null ? message.node_count : 'unknown';
    uuidNavigatorOutputEl.textContent = `Cache refreshed successfully. nodes=${count}`;
  }

  // Removed: timer-based scheduling (replaced with Ctrl-keyup check)

  function initializeUuidHoverHelper() {
    // Track cursor position on every mousemove
    document.addEventListener('mousemove', (event) => {
      lastMousePos = {x: event.clientX, y: event.clientY};
      hideUuidTooltip(); // Hide tooltip when cursor moves
    });

    // F2, F3, F4, F8, F9, F10, and F12 keyup handlers
    document.addEventListener('keyup', (event) => {
      // F2 keyup: all UUIDs in path (instant)
      if (event.key === 'F2') {
        event.preventDefault();
        
        if (!lastMousePos) {
          return;
        }
        
        const el = document.elementFromPoint(lastMousePos.x, lastMousePos.y);
        const projectEl = el && el.closest('div[projectid]');
        
        if (projectEl) {
          copyUuidForElement(projectEl, true); // true = all UUIDs in path
        }
      }
      
      // F3 keyup: leaf UUID only (instant)
      if (event.key === 'F3') {
        event.preventDefault();
        
        if (!lastMousePos) {
          return;
        }
        
        const el = document.elementFromPoint(lastMousePos.x, lastMousePos.y);
        const projectEl = el && el.closest('div[projectid]');
        
        if (projectEl) {
          copyUuidForElement(projectEl, false); // false = leaf UUID only
        }
      }
      
      // F4 keyup: just the UUID, nothing else (no path, no formatting)
      if (event.key === 'F4') {
        event.preventDefault();
        
        if (!lastMousePos) {
          return;
        }
        
        const el = document.elementFromPoint(lastMousePos.x, lastMousePos.y);
        const projectEl = el && el.closest('div[projectid]');
        
        if (projectEl) {
          let uuid = projectEl.getAttribute('projectid');
          if (!uuid) return;
          
          // If virtual UUID, extract the real one
          if (uuid.startsWith('virtual_')) {
            const parts = uuid.split('_');
            if (parts.length >= 2) {
              uuid = parts[1]; // First UUID is the actual node
            }
          }
          
          // Copy to clipboard
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(uuid).catch(err => {
              fallbackCopy(uuid, () => {});
            });
          } else {
            fallbackCopy(uuid, () => {});
          }
          
          // Show simple tooltip with just the UUID
          const elTooltip = ensureUuidTooltipElement();
          const rect = projectEl.getBoundingClientRect();
          const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
          const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
          elTooltip.style.top = `${top}px`;
          elTooltip.style.left = `${left}px`;
          elTooltip.innerHTML = '';
          elTooltip.textContent = `${uuid}\n\n✓ Copied`;
          elTooltip.style.display = 'block';
          clearTimeout(elTooltip._hideTimer);
          elTooltip._hideTimer = setTimeout(() => {
            hideUuidTooltip();
          }, 10000);
        }
      }

      // F8 keyup: expand all visible #nexus-- roots
      if (event.key === 'F8') {
        event.preventDefault();
        expandAllVisibleNexusRoots(DEFAULT_NEXUS_EXPAND_OPTIONS);
      }

      // F10 keyup: collapse all visible #nexus-- roots (full subtree reset)
      if (event.key === 'F10') {
        event.preventDefault();
        collapseAllVisibleNexusRoots(DEFAULT_NEXUS_EXPAND_OPTIONS);
      }

      // @beacon[
      //   id=f9-handler@glimpse-ext,
      //   slice_labels=f9-f12-handlers,
      //   kind=span,
      //   comment=F9 keyup handler - opens code file in WindSurf via MCP beacon resolution,
      // ]
      // F9 keyup: open associated code file (beacon-aware) in WindSurf
      // Uses beacon_line from MCP server response for precise navigation.
      if (event.key === 'F9') {
        event.preventDefault();

        if (!lastMousePos) {
          return;
        }

        const el = document.elementFromPoint(lastMousePos.x, lastMousePos.y);
        const projectEl = el && el.closest('div[projectid]');

        if (!projectEl) {
          return;
        }

        // Require an active WebSocket connection; otherwise, we cannot resolve
        // beacons or file paths via the MCP server.
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          const tooltip = ensureUuidTooltipElement();
          const rect = projectEl.getBoundingClientRect();
          const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
          const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
          tooltip.style.top = `${top}px`;
          tooltip.style.left = `${left}px`;
          tooltip.textContent = 'GLIMPSE server unavailable (cannot open in WindSurf).';
          tooltip.style.display = 'block';
          clearTimeout(tooltip._hideTimer);
          tooltip._hideTimer = setTimeout(() => {
            hideUuidTooltip();
          }, 5000);
          return;
        }

        const uuid = projectEl.getAttribute('projectid');
        if (!uuid) {
          return;
        }

        const name = extractNodeName(projectEl) || '';
        const payload = {
          action: 'open_node_in_windsurf',
          node_id: uuid,
          node_name: name,
        };
        ws.send(JSON.stringify(payload));

        // Immediate feedback while the server resolves the beacon/file/line.
        const tooltip = ensureUuidTooltipElement();
        const rect = projectEl.getBoundingClientRect();
        const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
        const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
        tooltip.style.top = `${top}px`;
        tooltip.style.left = `${left}px`;
        tooltip.textContent = 'Opening in WindSurf...';
        tooltip.style.display = 'block';
        clearTimeout(tooltip._hideTimer);
        tooltip._hideTimer = setTimeout(() => {
          hideUuidTooltip();
        }, 5000);
      }

      // @beacon[
      //   id=f12-handler@glimpse-ext,
      //   slice_labels=f9-f12-handlers,
      //   kind=span,
      //   comment=F12 keyup handler - refreshes Cartographer FILE/FOLDER from source via MCP,
      // ]
      // F12 keyup: refresh Cartographer FILE node or FOLDER subtree in Workflowy from source
      if (event.key === 'F12') {
        event.preventDefault();

        if (!lastMousePos) {
          return;
        }

        const el = document.elementFromPoint(lastMousePos.x, lastMousePos.y);
        const projectEl = el && el.closest('div[projectid]');

        if (!projectEl) {
          return;
        }

        // Require an active WebSocket connection
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          const tooltip = ensureUuidTooltipElement();
          const rect = projectEl.getBoundingClientRect();
          const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
          const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
          tooltip.style.top = `${top}px`;
          tooltip.style.left = `${left}px`;
          tooltip.textContent = 'GLIMPSE server unavailable (cannot refresh node).';
          tooltip.style.display = 'block';
          clearTimeout(tooltip._hideTimer);
          tooltip._hideTimer = setTimeout(() => {
            hideUuidTooltip();
          }, 5000);
          return;
        }

        const uuid = projectEl.getAttribute('projectid');
        if (!uuid) {
          return;
        }

        const name = extractNodeName(projectEl) || '';
        const note = extractNodeNote(projectEl) || '';
        const isFolder = name.trim().startsWith('📂');
        const payload = {
          action: isFolder ? 'refresh_folder_node' : 'refresh_file_node',
          node_id: uuid,
          node_name: name,
          node_note: note,
        };
        ws.send(JSON.stringify(payload));

        // Immediate feedback while the server refreshes the node or subtree.
        const tooltip = ensureUuidTooltipElement();
        const rect = projectEl.getBoundingClientRect();
        const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
        const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
        tooltip.style.top = `${top}px`;
        tooltip.style.left = `${left}px`;
        tooltip.textContent = isFolder
          ? 'Refreshing folder subtree in Workflowy...'
          : 'Refreshing file node in Workflowy...';
        tooltip.style.display = 'block';
        clearTimeout(tooltip._hideTimer);
        tooltip._hideTimer = setTimeout(() => {
          hideUuidTooltip();
        }, 10000);
      }
    });
  }

  function initializeMutationNotifications() {
    const pending = new Set();
    let debounceTimer = null;
    const DEBOUNCE_MS = 800;

    function scheduleNotify(uuid) {
      if (!uuid) return;
      pending.add(uuid);
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          pending.clear();
          return;
        }
        const ids = Array.from(pending);
        pending.clear();

        // Build a richer payload so the MCP server (and its stderr logs) can
        // include both the UUID and the current node name.
        const nodes = ids.map((id) => {
          const el = document.querySelector(`div[projectid="${id}"]`);
          const name = el ? extractNodeName(el) : null;
          return { id, name };
        });

        console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] notify_node_mutated (batch):`, nodes);
        ws.send(JSON.stringify({
          action: 'notify_node_mutated',
          node_ids: ids,
          nodes: nodes,
          reason: 'dom_edit'
        }));
      }, DEBOUNCE_MS);
    }

    document.addEventListener('input', (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (!target.matches('div.content[contenteditable="true"]')) return;

      const projectEl = target.closest('div[projectid]');
      if (!projectEl) return;
      const uuid = projectEl.getAttribute('projectid');
      scheduleNotify(uuid);
    }, true);

    // NOTE:
    // We intentionally do NOT send additional notifications on 'blur' events.
    // The debounced 'input' handler above is sufficient to catch name/note
    // edits, and the MutationObserver below handles structural changes
    // (moves/creates/deletes). A separate blur-based notifier caused noisy
    // duplicate "dirty" logs without adding meaningful signal.

    // Observe structural changes to .children containers for moves/creates/deletes
    try {
      const observer = new MutationObserver((mutations) => {
        const collapseToggled = new Set(); // projectid strings
        const touchedParents = new Set();  // projectid strings

        // Pass 1: track project nodes whose 'collapsed' class flipped in this batch
        for (const m of mutations) {
          if (m.type !== 'attributes') continue;
          if (m.attributeName !== 'class') continue;
          const target = m.target;
          if (!(target instanceof HTMLElement)) continue;
          if (!target.matches('div[projectid]')) continue;

          const oldClass = m.oldValue || '';
          const hadCollapsed = oldClass.includes('collapsed');
          const hasCollapsed = target.classList.contains('collapsed');
          if (hadCollapsed !== hasCollapsed) {
            const pid = target.getAttribute('projectid');
            if (pid) collapseToggled.add(pid);
          }
        }

        // Pass 2: for .children childList changes, ignore those associated with collapse/expand
        for (const m of mutations) {
          if (m.type !== 'childList') continue;
          const target = m.target;
          if (!(target instanceof HTMLElement)) continue;
          if (!target.matches('div.children')) continue;

          const projectEl = target.closest('div[projectid]');
          if (!projectEl) continue;
          const pid = projectEl.getAttribute('projectid');
          if (!pid) continue;

          // If this parent just toggled collapsed in this batch, treat as UI-only change
          if (collapseToggled.has(pid)) continue;

          // Only treat as structural change if real node children were added/removed
          const changedNode = [...m.addedNodes, ...m.removedNodes].some(
            n => n instanceof HTMLElement && n.matches('div[projectid]')
          );
          if (!changedNode) continue;

          touchedParents.add(pid);
        }

        for (const pid of touchedParents) {
          scheduleNotify(pid);
        }
      });

      observer.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['class'],
        attributeOldValue: true
      });
    } catch (err) {
      console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] MutationObserver init failed:`, err);
    }
  }

  // Wait for DOM to be ready, then connect and initialize helpers
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      console.log('[GLIMPSE Cache] DOM ready, connecting...');
      connectWebSocket();
      initializeUuidHoverHelper();
      initializeUuidNavigator();
      initializeMutationNotifications();
    });
  } else {
    console.log('[GLIMPSE Cache] DOM already ready, connecting...');
    connectWebSocket();
    initializeUuidHoverHelper();
    initializeUuidNavigator();
    initializeMutationNotifications();
  }
  
  console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ✅ Standalone client loaded`);
  
})();
