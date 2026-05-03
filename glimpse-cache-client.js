/**
 * Workflowy GLIMPSE Cache - Standalone WebSocket Client
 * 
 * Injected directly into Workflowy desktop app (Electron).
 * No Chrome extension APIs - pure WebSocket + DOM extraction.
 */

(function() {
  'use strict';
  
  // @beacon[
  //   id=glimpse-loading@standalone-client-config,
  //   role=standalone client websocket/config defaults,
  //   slice_labels=nexus--glimpse-extension,nexus--config,nexus-loading-flow,
  //   kind=span,
  //   show_span=true,
  //   comment=Standalone Electron client bootstrap constants: version log, websocket endpoint, reconnect timing, and feature flags,
  // ]
  const GLIMPSE_VERSION = '3.14.0';
  console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Standalone client initializing...`);
  
  let ws = null;
  let reconnectInterval = null;
  let isConnecting = false;
  
  const WS_URL = 'ws://localhost:8765';
  const RECONNECT_DELAY = 3000; // 3 seconds
  const F12_POPUP_ENABLED = true;
  const F12_BULK_VISIBLE_APPLY_ENABLED = true;
  // @beacon-close[
  //   id=glimpse-loading@standalone-client-config,
  // ]

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
// @beacon[
//   id=extract-node-name@23224,
//   role=extract-node-name,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
  function nameHasNexusCollapseTag(nameText) {
    if (!nameText) return false;
    const parts = nameText.split(/\s+/);
    return parts.some(p => p === '#collapse-nexus');
  }

  // [DEPRECATED] kept for backwards compatibility if referenced elsewhere
  // prior nameHasNexusTag behavior: expansion only
  // @beacon[
  //   id=extract-node-name@222246,
  //   role=extract-node-name,
  //   slice_labels=glimpse-core,
  //   kind=ast,
  // ]
  function nameHasNexusTag(nameText) {
    return nameHasNexusExpandTag(nameText);
  }

  // Robust check: does this row have any Workflowy tag whose value starts with "#nexus--"?
  // We look both at the plain text (for older structures) and at the .contentTag
  // elements Workflowy uses for clickable tags (data-val="#tag")...
// @beacon[
//   id=extract-node-name@22225,
//   role=extract-node-name,
//   slice_labels=glimpse-core,
//   kind=ast,
//   comment=foo,
// ]
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
  // @beacon[
  //   id=auto-beacon@findVisibleNexusExpandNodes-rlf8,
  //   role=findVisibleNexusExpandNodes,
  //   slice_labels=glimpse-core,
  //   kind=ast,
  // ]
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
// @beacon[
//   id=auto-beacon@pruneNestedNexusRoots,
//   role=pruneNestedNexusRoots,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
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
// @beacon[
//   id=auto-beacon@expandAllVisibleNexusRoots,
//   role=expandAllVisibleNexusRoots,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
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
//   role=extract-dom-tree,
//   slice_labels=f9-f12-handlers,glimpse-core,
//   kind=ast,
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

  function extractVisibleActionTree(nodeId) {
    console.log('[GLIMPSE Cache] 🏷️ Extracting visible action tree for node:', nodeId);

    try {
      const rootElement = document.querySelector(`div[projectid="${nodeId}"]`);

      if (!rootElement) {
        console.warn('[GLIMPSE Cache] ❌ Action root not found in DOM:', nodeId);
        return {
          success: false,
          error: `Node ${nodeId} not found in DOM (may be collapsed or not loaded)`
        };
      }

      const rootName = extractNodeName(rootElement);
      const rootNote = extractNodeNote(rootElement);
      const searchActive = isSearchActive();
      const children = searchActive ?
        extractSearchResults(rootElement, false) :
        extractChildren(rootElement, false);

      const nodeCount = 1 + countNodesRecursive(children);
      const depth = calculateDepth(children);

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
      console.error('[GLIMPSE Cache] Error during visible action tree extraction:', error);
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
//   role=extract-node-note,
//   slice_labels=glimpse-core,
//   kind=ast,
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
  // @beacon[
  //   id=auto-beacon@dewhitenText-ol8k,
  //   role=dewhitenText,
  //   slice_labels=glimpse-core,
  //   kind=ast,
  // ]
  function dewhitenText(text) {
    if (!text || typeof text !== 'string') return text;
    return text.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  }

  // Markup-preserving variants for GLIMPSE JSON (do not strip HTML tags).
// @beacon[
//   id=auto-beacon@extractNodeNameHtml,
//   role=extractNodeNameHtml,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
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

  // @beacon[
  //   id=auto-beacon@extractNodeNoteHtml,
  //   role=extractNodeNoteHtml,
  //   slice_labels=glimpse-core,
  //   kind=ast,
  // ]
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
  // @beacon[
  //   id=auto-beacon@extractSearchResults-v534,
  //   role=extractSearchResults,
  //   slice_labels=glimpse-core,
  //   kind=ast,
  // ]
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
  
// @beacon[
//   id=auto-beacon@extractChildren,
//   role=extractChildren,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
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
  
// @beacon[
//   id=auto-beacon@countNodesRecursive,
//   role=countNodesRecursive,
//   slice_labels=glimpse-core,
//   kind=ast,
// ]
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
  // @beacon[
  //   id=auto-beacon@connectWebSocket-fh7i,
   //   role=connectWebSocket,
  //   slice_labels=ra-websocket,nexus--config,nexus-loading-flow,
  //   kind=ast,
  // ]
  function connectWebSocket() {
    if (isConnecting || (ws && ws.readyState === WebSocket.OPEN)) {
      return;
    }
    
    isConnecting = true;
    console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] Connecting to`, WS_URL);
    
    try {
      ws = new WebSocket(WS_URL);
      // Expose WebSocket globally for UI helpers (e.g., CARTO CANCEL button).
      try {
        // eslint-disable-next-line no-undef
        window.ws = ws;
      } catch (e) {
        // In case window is not available for some reason, ignore.
      }
      
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

        // @beacon[
        //   id=carto-jobs@ws-open-polling,
        //   role=Start polling for CARTO_REFRESH job status via carto_list_jobs,
        //   slice_labels=ra-carto-jobs,nexus--config,
        //   kind=span,
        //   comment=Start polling for CARTO_REFRESH job status via carto_list_jobs,
        // ]
        // Start polling for CARTO_REFRESH job status once per second (best-effort)
        if (!cartoJobsInterval) {
          cartoJobsInterval = setInterval(() => {
            try {
              if (!ws || ws.readyState !== WebSocket.OPEN) {
                return;
              }
              ws.send(JSON.stringify({ action: 'carto_list_jobs' }));
            } catch (err) {
              console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] carto_list_jobs poll failed:`, err);
            }
          }, 1000);
        }
        // @beacon-close[
        //   id=carto-jobs@ws-open-polling,
        // ]
      };
      
      // @beacon[
      //   id=ws-onmessage@glimpse-ext,
      //   role=WebSocket message handler - dispatches MCP server requests to client handlers,
      //   slice_labels=f9-f12-handlers,ra-websocket,
      //   kind=span,
      //   comment=WebSocket message handler - dispatches MCP server requests to client handlers,
      // ]
      ws.onmessage = (event) => {
        try {
          const request = JSON.parse(event.data);
          if (request.action !== 'carto_list_jobs_result' && request.action !== 'carto_cancel_job_result') {
            console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] 📩 Request from Python:`, request.action);
          }
          
          if (request.action === 'extract_dom') {
            const result = extractDOMTree(request.node_id);
            ws.send(JSON.stringify(result));
            console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ✅ Sent response:`, result.node_count || 0, 'nodes');
          } else if (request.action === 'uuid_path_result') {
            handleUuidPathResult(request);
          } else if (request.action === 'refresh_nodes_export_cache_status') {
            handleRefreshCacheStatus(request);
          } else if (request.action === 'refresh_nodes_export_cache_result') {
            handleRefreshCacheResult(request);
          } else if (request.action === 'bulk_apply_visible_nodes_result') {
            handleBulkApplyVisibleResult(request);
          } else if (request.action === 'expand_nexus_roots') {
            // Agent-triggered: expand all visible #nexus-- roots
            const opts = request.options || {};
            expandAllVisibleNexusRoots(opts);
            ws.send(JSON.stringify({
              success: true,
              action: 'expand_nexus_roots_result',
              expanded: true
            }));
          } else if (request.action === 'carto_list_jobs_result') {
            // @beacon[
            //   id=carto-jobs@ws-carto-result-branch,
            //   slice_labels=ra-carto-jobs,
            //   kind=span,
            //   role=Dispatch CARTO_REFRESH job list updates to handleCartoJobsResult,
            //   comment=Dispatch CARTO_REFRESH job list updates to handleCartoJobsResult,
            // ]
            handleCartoJobsResult(request);
            // @beacon-close[
            //   id=carto-jobs@ws-carto-result-branch,
            // ]
          } else if (request.action === 'carto_cancel_job_result') {
            if (!request.success) {
              console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] carto_cancel_job_result error:`, request.error);
            }
            // No further action required; the next carto_list_jobs poll will update the UI.
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
      // @beacon-close[
      //   id=ws-onmessage@glimpse-ext,
      // ]
      
      ws.onerror = (error) => {
        console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] WebSocket error:`, error);
        isConnecting = false;
      };
      
      ws.onclose = () => {
        console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] ⚠️ Disconnected from Python MCP server`);
        ws = null;
        try {
          // eslint-disable-next-line no-undef
          window.ws = null;
        } catch (e) {
          // Ignore if window not available.
        }
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
  // Container for async CARTO_REFRESH job summaries (F12)
  let cartoJobsEl = null;
  // Polling interval handle for CARTO job status
  let cartoJobsInterval = null;
  // Tracks whether the next uuid_path_result should be rendered in FULL
  // (verbose) mode rather than compact mode.
  let uuidNavigatorFullMode = false;
  // F12 action popup state
  let f12PopupEl = null;
  let f12PopupState = null;

  // @beacon[
  //   id=auto-beacon@ensureUuidTooltipElement-pm6x,
  //   role=ensureUuidTooltipElement,
  //   slice_labels=nexus--glimpse-extension,
  //   kind=ast,
  // ]
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

  function ensureF12ActionPopup() {
    if (f12PopupEl) return f12PopupEl;
    const el = document.createElement('div');
    el.id = 'glimpse-f12-popup';
    el.style.position = 'absolute';
    el.style.zIndex = '10000';
    el.style.minWidth = '280px';
    el.style.maxWidth = '420px';
    el.style.padding = '8px 10px';
    el.style.background = 'rgba(17, 17, 17, 0.96)';
    el.style.color = '#fff';
    el.style.fontSize = '12px';
    el.style.lineHeight = '1.4';
    el.style.border = '1px solid rgba(102, 179, 255, 0.35)';
    el.style.borderRadius = '8px';
    el.style.boxShadow = '0 8px 24px rgba(0, 0, 0, 0.45)';
    el.style.pointerEvents = 'auto';
    el.style.display = 'none';
    el.style.fontFamily = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
    document.body.appendChild(el);
    f12PopupEl = el;
    return el;
  }

  function hideF12ActionPopup() {
    if (f12PopupEl) {
      f12PopupEl.style.display = 'none';
    }
    f12PopupState = null;
  }

  // @beacon[
  //   id=glimpse-path@noteHasPathOrRootHeader,
  //   role=noteHasPathOrRootHeader,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Detect whether a Workflowy note carries Path:/Root: Cartographer headers,
  // ]
  function noteHasPathOrRootHeader(noteText) {
    return String(noteText || '')
      .split('\n')
      .some((line) => {
        const stripped = line.trim();
        return stripped.startsWith('Path:') || stripped.startsWith('Root:');
      });
  }

  // @beacon[
  //   id=glimpse-path@getCartographerHeaderInfo,
  //   role=getCartographerHeaderInfo,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Parse the first Path:/Root: header and LINE COUNT metadata used by extension-side F12 routing,
  // ]
  function getCartographerHeaderInfo(noteText) {
    const lines = String(noteText || '').split('\n');
    let firstHeader = '';
    let hasLineCount = false;

    lines.forEach((line) => {
      const stripped = line.trim();
      if (!stripped) return;
      if (!firstHeader && (stripped.startsWith('Path:') || stripped.startsWith('Root:'))) {
        firstHeader = stripped;
      }
      if (stripped.startsWith('LINE COUNT:')) {
        hasLineCount = true;
      }
    });

    return { firstHeader, hasLineCount };
  }

  // @beacon[
  //   id=glimpse-path@pathHeaderLooksLikeFile,
  //   role=pathHeaderLooksLikeFile,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Infer file-vs-folder semantics from a Path: header and visible node name for F12 action routing,
  // ]
  function pathHeaderLooksLikeFile(firstHeader, nameText) {
    const header = String(firstHeader || '').trim();
    if (!header.startsWith('Path:')) return false;

    const pathValue = header.slice('Path:'.length).trim().replace(/^['"]|['"]$/g, '');
    if (!pathValue || pathValue === '.' || pathValue.endsWith('/') || pathValue.endsWith('\\')) {
      return false;
    }

    const lowerPath = pathValue.toLowerCase();
    const lowerName = String(nameText || '').trim().toLowerCase();

    // Manual file nodes may not yet have a LINE COUNT header because they were
    // created directly in Workflowy before any Cartographer refresh. If the
    // Path: value (or the visible node name) clearly ends with a filename
    // extension, treat the target as a FILE node so F12 can offer file actions
    // such as Markdown roundtrip generation.
    if (/(^|[\\/])[^\\/]+\.[^\\/.\s]+$/.test(lowerPath)) return true;
    if (/\.[a-z0-9_-]+$/.test(lowerName)) return true;
    return false;
  }

  // @beacon[
  //   id=glimpse-path@classifyF12Target,
  //   role=classifyF12Target,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Classify a hovered Workflowy node as folder/file/node from Path:/Root: and Cartographer note headers,
  // ]
  function classifyF12Target(nameText, noteText) {
    const note = String(noteText || '');
    const { firstHeader, hasLineCount } = getCartographerHeaderInfo(note);

    if (firstHeader.startsWith('Root:')) return 'folder';
    if (firstHeader.startsWith('Path:')) {
      return (hasLineCount || pathHeaderLooksLikeFile(firstHeader, nameText)) ? 'file' : 'folder';
    }
    if (note.includes('AST_QUALNAME:') || note.includes('BEACON (') || note.includes('MD_PATH:')) {
      return 'node';
    }
    return 'unsupported';
  }

  function showTransientHoverText(targetEl, text, timeoutMs = 6000) {
    if (!targetEl) return;
    const tooltip = ensureUuidTooltipElement();
    const rect = targetEl.getBoundingClientRect();
    const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 20;
    const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 16;
    tooltip.style.top = `${top}px`;
    tooltip.style.left = `${left}px`;
    tooltip.textContent = text;
    tooltip.style.display = 'block';
    clearTimeout(tooltip._hideTimer);
    tooltip._hideTimer = setTimeout(() => {
      hideUuidTooltip();
    }, timeoutMs);
  }

  // @beacon[
  //   id=glimpse-path@dispatchF12RefreshAction,
  //   role=dispatchF12RefreshAction,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Route extension-side F12 actions to file vs folder refresh requests based on classified Path:/Root: semantics,
  // ]
  function dispatchF12RefreshAction(state, cartoAsync) {
    if (!state || !state.uuid || !ws || ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    const payload = {
      action: state.targetType === 'folder' ? 'refresh_folder_node' : 'refresh_file_node',
      node_id: state.uuid,
      node_name: state.name,
      node_note: state.note,
      carto_async: !!cartoAsync,
    };
    ws.send(JSON.stringify(payload));

    let message = 'Starting refresh...';
    if (state.targetType === 'folder') {
      message = 'Starting async CARTO_REFRESH for folder subtree...';
    } else if (state.targetType === 'file') {
      message = 'Starting async CARTO_REFRESH for file node...';
    } else if (cartoAsync) {
      message = 'Starting async CARTO_REFRESH for containing file...';
    } else {
      message = 'Running fast node beacon update + visual file refresh...';
    }
    showTransientHoverText(state.projectEl, message, cartoAsync ? 10000 : 6000);
    return true;
  }

  function dispatchF12BulkApplyAction(state) {
    if (!state || !state.uuid || !ws || ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    const visibleTree = extractVisibleActionTree(state.uuid);
    if (!visibleTree || !visibleTree.success) {
      const errorText = visibleTree && visibleTree.error
        ? visibleTree.error
        : 'Failed to extract visible subtree for batch apply.';
      showTransientHoverText(state.projectEl, errorText, 6000);
      return false;
    }

    const payload = {
      action: 'bulk_apply_visible_nodes',
      root_uuid: state.uuid,
      root_name: state.name,
      root_note: state.note,
      root_mode: state.targetType,
      visible_tree: visibleTree,
    };
    ws.send(JSON.stringify(payload));

    const message = state.targetType === 'folder'
      ? 'Starting bulk visible beacon/tag apply for folder subtree...'
      : 'Starting bulk visible beacon/tag apply for file...';
    showTransientHoverText(state.projectEl, message, 10000);
    return true;
  }

  // @beacon[
  //   id=glimpse-path@findEnclosingFileFromNode,
  //   role=findEnclosingFileFromNode,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Walk DOM ancestors from an AST/beacon node to find the enclosing FILE node by Path:/Root: header,
  // ]
  function findEnclosingFileFromNode(state) {
    if (!state || !state.projectEl) return null;

    // Walk up DOM ancestors looking for a node whose note has a Path: or Root: header.
    // The first such ancestor is treated as the enclosing FILE/FOLDER target. This is
    // the same heuristic used server-side in update_beacon_from_node and lets the user
    // trigger the file-level bulk apply from any descendant AST/beacon node WITHOUT
    // scrolling the file node back into view (a long-standing UX trap that causes the
    // file refresh to delete tags on nodes that scrolled OFF the bottom of the viewport).
    let current = state.projectEl.parentElement
      && state.projectEl.parentElement.closest('div[projectid]');
    while (current) {
      const ancestorUuid = current.getAttribute('projectid');
      if (!ancestorUuid) {
        current = current.parentElement
          && current.parentElement.closest('div[projectid]');
        continue;
      }

      const ancestorName = extractNodeName(current);
      const ancestorNote = extractNodeNote(current);
      const ancestorType = classifyF12Target(ancestorName, ancestorNote);

      if (ancestorType === 'file' || ancestorType === 'folder') {
        return {
          uuid: ancestorUuid,
          name: ancestorName,
          note: ancestorNote,
          targetType: ancestorType,
          projectEl: current,
        };
      }

      current = current.parentElement
        && current.parentElement.closest('div[projectid]');
    }
    return null;
  }

  // @beacon[
  //   id=glimpse-path@dispatchF12BulkApplyForContainingFile,
  //   role=dispatchF12BulkApplyForContainingFile,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Run F12+2 bulk visible apply targeting the AST node's enclosing FILE - safe to invoke when the file node is scrolled out of view,
  // ]
  function dispatchF12BulkApplyForContainingFile(state) {
    if (!state || !state.uuid || !ws || ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    const fileState = findEnclosingFileFromNode(state);
    if (!fileState) {
      showTransientHoverText(
        state.projectEl,
        'Could not locate enclosing FILE/FOLDER ancestor (Path:/Root: header).',
        6000,
      );
      return false;
    }

    // Use the SAME bulk-apply dispatcher rooted at the FILE node. Because Workflowy
    // keeps ancestor DOM elements rendered regardless of scroll position, the
    // extractVisibleActionTree call inside dispatchF12BulkApplyAction successfully
    // captures the entire currently-visible subtree under the file -- the same payload
    // you would get if you scrolled up and triggered F12+2 directly on the file node.
    return dispatchF12BulkApplyAction(fileState);
  }

  // @beacon[
  //   id=glimpse-path@getF12ActionOptions,
  //   role=getF12ActionOptions,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Select available F12 actions from extension-side file/folder/node classification derived from Cartographer headers,
  // ]
  function getF12ActionOptions(state) {
    if (!state) return [];

    if (state.targetType === 'folder') {
      return [
        {
          key: '1',
          label: 'Async folder refresh',
          detail: 'Detached CARTO_REFRESH over this subtree',
          disabled: false,
          onSelect: () => dispatchF12RefreshAction(state, true),
        },
        {
          key: '2',
          label: 'Bulk visible beacon/tag apply',
          detail: 'Apply current visible Workflowy metadata in one batch',
          disabled: !F12_BULK_VISIBLE_APPLY_ENABLED,
          onSelect: () => dispatchF12BulkApplyAction(state),
        },
      ];
    }

    if (state.targetType === 'file') {
      const { firstHeader } = getCartographerHeaderInfo(state.note);
      const loweredHeader = String(firstHeader || '').toLowerCase();
      const loweredName = String(state.name || '').toLowerCase();
      const isMarkdownFile =
        loweredHeader.endsWith('.md') ||
        loweredHeader.endsWith('.markdown') ||
        loweredName.includes('.md') ||
        loweredName.includes('.markdown');

      return [
        {
          key: '1',
          label: 'Async file refresh',
          detail: 'Detached CARTO_REFRESH for this file',
          disabled: false,
          onSelect: () => dispatchF12RefreshAction(state, true),
        },
        {
          key: '2',
          label: 'Bulk visible beacon/tag apply',
          detail: 'Apply current visible Workflowy metadata in one batch',
          disabled: !F12_BULK_VISIBLE_APPLY_ENABLED,
          onSelect: () => dispatchF12BulkApplyAction(state),
        },
        {
          key: '3',
          label: 'Generate Markdown roundtrip file',
          detail: isMarkdownFile
            ? 'Overwrites local file with Markdown roundtrip of this subtree'
            : 'Markdown file nodes only',
          disabled: !isMarkdownFile,
          onSelect: () => {
            ws.send(JSON.stringify({ action: 'generate_markdown_file', node_id: state.uuid }));
            showTransientHoverText(state.projectEl, 'Generating Markdown roundtrip...', 4000);
          },
        },
      ];
    }

    if (state.targetType === 'node') {
      return [
        {
          key: '1',
          label: 'Fast node beacon update',
          detail: 'Sync per-node update + visual file refresh',
          disabled: false,
          onSelect: () => dispatchF12RefreshAction(state, false),
        },
        {
          key: '2',
          label: 'Async containing-file refresh',
          detail: 'Detached CARTO_REFRESH after node update',
          disabled: false,
          onSelect: () => dispatchF12RefreshAction(state, true),
        },
        {
          key: '3',
          label: 'Bulk visible beacon/tag apply (containing file)',
          detail: 'Run F12+2 against enclosing FILE without needing to scroll back to it',
          disabled: !F12_BULK_VISIBLE_APPLY_ENABLED,
          onSelect: () => dispatchF12BulkApplyForContainingFile(state),
        },
      ];
    }

    return [];
  }

  function executeF12PopupAction(selectionKey) {
    if (!f12PopupState || !Array.isArray(f12PopupState.actions)) {
      return false;
    }

    const action = f12PopupState.actions.find((opt) => opt.key === selectionKey);
    if (!action) {
      return false;
    }

    const state = f12PopupState;
    hideF12ActionPopup();

    if (action.disabled) {
      showTransientHoverText(state.projectEl, `${action.label} — coming soon.`, 4000);
      return true;
    }

    try {
      action.onSelect();
    } catch (err) {
      console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] F12 popup action failed:`, err);
      const msg = err && err.message ? err.message : String(err);
      showTransientHoverText(state.projectEl, `F12 action failed: ${msg}`, 6000);
    }
    return true;
  }

  // @beacon[
  //   id=glimpse-path@showF12ActionPopup,
  //   role=showF12ActionPopup,
  //   slice_labels=f9-f12-handlers,nexus--glimpse-extension,nexus-path-resolution-logic,
  //   kind=ast,
  //   comment=Top-level F12 popup entrypoint that classifies the current node using Path:/Root: semantics before presenting actions,
  // ]
  function showF12ActionPopup(projectEl) {
    const uuid = projectEl && projectEl.getAttribute('projectid');
    if (!projectEl || !uuid) return false;

    const name = extractNodeName(projectEl) || '';
    const note = extractNodeNote(projectEl) || '';
    const targetType = classifyF12Target(name, note);

    if (targetType === 'unsupported') {
      showTransientHoverText(projectEl, 'No F12 actions available for this node.', 5000);
      return false;
    }

    hideUuidTooltip();

    const popup = ensureF12ActionPopup();
    popup.innerHTML = '';

    const state = {
      projectEl,
      uuid,
      name,
      note,
      targetType,
      actions: [],
    };
    state.actions = getF12ActionOptions(state);
    f12PopupState = state;

    const title = document.createElement('div');
    title.textContent =
      targetType === 'folder'
        ? 'F12 · Folder actions'
        : targetType === 'file'
          ? 'F12 · File actions'
          : 'F12 · Node actions';
    title.style.fontWeight = '600';
    title.style.marginBottom = '4px';

    const subtitle = document.createElement('div');
    subtitle.textContent = name || 'Untitled';
    subtitle.style.fontSize = '11px';
    subtitle.style.color = '#9ecbff';
    subtitle.style.marginBottom = '8px';
    subtitle.style.whiteSpace = 'nowrap';
    subtitle.style.overflow = 'hidden';
    subtitle.style.textOverflow = 'ellipsis';

    popup.appendChild(title);
    popup.appendChild(subtitle);

    state.actions.forEach((action) => {
      const row = document.createElement('div');
      row.style.display = 'flex';
      row.style.alignItems = 'flex-start';
      row.style.gap = '8px';
      row.style.padding = '6px 4px';
      row.style.borderRadius = '6px';
      row.style.marginBottom = '4px';
      row.style.cursor = action.disabled ? 'default' : 'pointer';
      row.style.opacity = action.disabled ? '0.55' : '1';
      row.style.background = action.disabled ? 'transparent' : 'rgba(255, 255, 255, 0.03)';

      if (!action.disabled) {
        row.addEventListener('mouseenter', () => {
          row.style.background = 'rgba(102, 179, 255, 0.12)';
        });
        row.addEventListener('mouseleave', () => {
          row.style.background = 'rgba(255, 255, 255, 0.03)';
        });
      }

      row.addEventListener('click', (evt) => {
        evt.stopPropagation();
        executeF12PopupAction(action.key);
      });

      const key = document.createElement('div');
      key.textContent = action.key;
      key.style.minWidth = '16px';
      key.style.fontWeight = '700';
      key.style.color = action.disabled ? '#888' : '#66b3ff';

      const body = document.createElement('div');
      body.style.display = 'flex';
      body.style.flexDirection = 'column';

      const label = document.createElement('div');
      label.textContent = action.label;
      label.style.fontSize = '12px';

      const detail = document.createElement('div');
      detail.textContent = action.detail;
      detail.style.fontSize = '10px';
      detail.style.color = '#aaa';

      body.appendChild(label);
      body.appendChild(detail);
      row.appendChild(key);
      row.appendChild(body);
      popup.appendChild(row);
    });

    const footer = document.createElement('div');
    footer.textContent = 'Press 1 / 2 / 3 or click · ESC cancels';
    footer.style.marginTop = '6px';
    footer.style.fontSize = '10px';
    footer.style.color = '#999';
    popup.appendChild(footer);

    const rect = projectEl.getBoundingClientRect();
    const top = window.scrollY + (lastMousePos ? lastMousePos.y : rect.bottom) + 18;
    const left = window.scrollX + (lastMousePos ? lastMousePos.x : rect.left) + 12;
    popup.style.top = `${top}px`;
    popup.style.left = `${left}px`;
    popup.style.display = 'block';
    return true;
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
  // @beacon[
  //   id=auto-beacon@initializeUuidNavigator-i8gn,
  //   role=initializeUuidNavigator,
  //   slice_labels=ra-websocket,
  //   kind=ast,
  // ]
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

    // @beacon[
    //   id=carto-jobs@uuid-widget-ui,
    //   slice_labels=ra-carto-jobs,
    //   kind=span,
    //   comment=CARTO_REFRESH job list UI (header + rows) in UUID Explorer widget,
    // ]
    const jobsHeader = document.createElement('div');
    jobsHeader.style.marginTop = '4px';
    jobsHeader.style.fontSize = '10px';
    jobsHeader.style.color = '#aaa';
    jobsHeader.textContent = 'CARTO jobs (async F12 / batch)';

    const jobsContainer = document.createElement('div');
    jobsContainer.id = 'glimpse-carto-jobs';
    jobsContainer.style.marginTop = '2px';
    jobsContainer.style.fontSize = '10px';
    jobsContainer.style.color = '#ccc';
    jobsContainer.style.maxHeight = '80px';
    jobsContainer.style.overflowY = 'auto';
    jobsContainer.textContent = 'No active CARTO jobs.';

    container.appendChild(header);
    container.appendChild(row);
    container.appendChild(refreshBtn);
    container.appendChild(jobsHeader);
    container.appendChild(jobsContainer);
    container.appendChild(output);

    document.body.appendChild(container);

    uuidNavigatorEl = container;
    uuidNavigatorInputEl = input;
    uuidNavigatorOutputEl = output;
    uuidNavigatorToggleEl = toggle;
    cartoJobsEl = jobsContainer;
    // @beacon-close[
    //   id=carto-jobs@uuid-widget-ui,
    // ]
  }

  // @beacon[
  //   id=auto-beacon@requestUuidPath-vj86,
  //   role=requestUuidPath,
  //   slice_labels=ra-websocket,
  //   kind=ast,
  // ]
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

  // @beacon[
  //   id=auto-beacon@handleUuidPathResult-96tu,
  //   role=handleUuidPathResult,
  //   slice_labels=ra-websocket,f9-f12-handlers,nexus--glimpse-extension,
  //   kind=ast,
  //   comment=F2/F3 hover + UUID-Navigator click result handler. Receives {action:'uuid_path_result'} from server after _resolve_uuid_path_and_respond walks ancestor chain via /nodes-export cache. Not directly part of F12+3 timeout pipeline, but listed alongside other ws.onmessage dispatch handlers for completeness in the GUI handler beacon slice.,
  // ]
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

  // @beacon[
  //   id=auto-beacon@handleRefreshCacheStatus-fxm12,
  //   role=handleRefreshCacheStatus,
  //   slice_labels=ra-websocket,f9-f12-handlers,nexus--glimpse-extension,ra-workflowy-cache,
  //   kind=ast,
  //   comment=Cache-refresh STATUS handler (queued/running). Receives {action:'refresh_nodes_export_cache_status'} pushes from server during F12+3 step [1] preflight + step [4] quiescence wait. UI lifecycle: shows 'Refreshing /nodes-export cache...' in widget. NOTE: previously had a misplaced beacon block (id=...handleRefreshCacheResult-ppk6) anchored here — that beacon was for handleRefreshCacheResult below; corrected May 2026.,
  // ]
  function handleRefreshCacheStatus(message) {
    console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] refresh_nodes_export_cache_status:`, message);
    if (!uuidNavigatorOutputEl) return;

    uuidNavigatorOutputEl.style.display = 'block';
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }

    const status = String(message.status || '').toLowerCase();
    if (status === 'queued') {
      uuidNavigatorOutputEl.textContent = 'Refreshing /nodes-export cache (queued)...';
      return;
    }
    if (status === 'running') {
      uuidNavigatorOutputEl.textContent = 'Refreshing /nodes-export cache...';
      return;
    }

    uuidNavigatorOutputEl.textContent = `Cache refresh status: ${message.status || 'unknown'}`;
  }

  // @beacon[
  //   id=auto-beacon@handleRefreshCacheResult-ppk6,
  //   role=handleRefreshCacheResult,
  //   slice_labels=ra-websocket,f9-f12-handlers,nexus--glimpse-extension,ra-workflowy-cache,
  //   kind=ast,
  //   comment=Cache-refresh RESULT handler (final completion). Receives {action:'refresh_nodes_export_cache_result'} after server-side cache settles. UI lifecycle: 'Cache refreshed successfully. nodes=N'. HYPOTHESIS (unverified): if this never fires after F12+3, the widget would show 'in progress' indefinitely — worth checking against the F12+3 timeout symptom Dan reports.,
  // ]
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

  // @beacon[
  //   id=auto-beacon@handleBulkApplyVisibleResult-bvr1,
  //   role=handleBulkApplyVisibleResult,
  //   slice_labels=ra-websocket,f9-f12-handlers,ra-bulk-visible-apply,nexus--glimpse-extension,ra-carto-jobs,
  //   kind=ast,
  //   comment=Bulk-apply RESULT handler. Receives {action:'bulk_apply_visible_nodes_result'} after _start_carto_bulk_visible_apply_job returns its job_id. UI lifecycle: 'Bulk apply queued: <job_id>'. This is the QUEUE acknowledgment only — actual completion would be reported separately via handleCartoJobsResult polling. HYPOTHESIS (unverified): the persistent 'in progress' widget state during F12+3 timeout may originate from continuing carto_list_jobs polls finding the job in 'running' state, not from this handler.,
  // ]
  function handleBulkApplyVisibleResult(message) {
    console.log(`[GLIMPSE Cache v${GLIMPSE_VERSION}] bulk_apply_visible_nodes_result:`, message);
    if (!uuidNavigatorOutputEl) return;

    uuidNavigatorOutputEl.style.display = 'block';
    uuidNavigatorExpanded = true;
    if (uuidNavigatorToggleEl) {
      uuidNavigatorToggleEl.textContent = '▾';
    }

    if (!message.success) {
      uuidNavigatorOutputEl.textContent = `Bulk apply failed: ${message.error || 'Unknown error'}`;
      return;
    }

    if (message.job_id) {
      uuidNavigatorOutputEl.textContent = `Bulk apply queued: ${message.job_id}`;
      return;
    }

    uuidNavigatorOutputEl.textContent = message.message || 'Bulk apply request accepted.';
  }

  // Render CARTO_REFRESH jobs into the UUID widget (best-effort)
  // @beacon[
  //   id=auto-beacon@handleCartoJobsResult-cjr1,
  //   role=handleCartoJobsResult,
  //   slice_labels=ra-websocket,f9-f12-handlers,ra-bulk-visible-apply,ra-carto-jobs,nexus--glimpse-extension,
  //   kind=ast,
  //   comment=CARTO jobs polling RESULT handler. Renders active CARTO_REFRESH + CARTO_BULK_APPLY jobs in the widget with status/progress (X/Y files, phase, node counters). HYPOTHESIS (unverified, worth checking): this is a candidate UI source for the F12+3 'still running' false positive — if MCP appears to hang and the JSON job file isn't transitioned to status='completed', this handler would keep rendering the job as active until MCP restarts; the falsely-reports-failure symptom on MCP restart could originate here when a subsequent poll sees a 'failed' or absent job.,
  // ]
  function handleCartoJobsResult(message) {
    if (!cartoJobsEl) return;

    if (!message.success) {
      cartoJobsEl.textContent = `Job listing failed: ${message.error || 'Unknown error'}`;
      return;
    }

    const jobs = Array.isArray(message.jobs) ? message.jobs : [];
    // VISIBILITY POLICY (May 2026 expanded):
    //   - `completed` and `cancelled`: hide (these are the "clean exit" states).
    //   - `failed`: keep visible (red status persists until cleared).
    //   - `completed_with_warnings`: keep visible (yellow ⚠️ persists until cleared).
    // Both `failed` and `completed_with_warnings` are also exempt from the
    // server-side GC sweep, so they will persist on disk and remain renderable
    // here until the user manually cleans them up.
    const active = jobs.filter((j) => {
      if (!j || typeof j !== 'object') return false;
      const status = String(j.status || 'unknown').toLowerCase();
      return status !== 'completed' && status !== 'cancelled';
    });

    if (active.length === 0) {
      cartoJobsEl.textContent = 'No active CARTO jobs.';
      return;
    }

    cartoJobsEl.textContent = '';
    active.forEach((job) => {
      const mode = job.mode || 'file';
      const kind = String(job.kind || '');
      const rootUuid = job.root_uuid || '';
      const progress = job.progress || {};
      const total = progress.total_files || 0;
      const completed = progress.completed_files || 0;

      const phase = String(progress.current_phase || '');
      const nodesCreated = Number(progress.nodes_created || 0);
      const nodesUpdated = Number(progress.nodes_updated || 0);
      const nodesMoved = Number(progress.nodes_moved || 0);
      const nodesDeleted = Number(progress.nodes_deleted || 0);
      const currentFileUuid = String(progress.current_file || '');

      // Try to resolve root name via DOM; fall back to UUID
      let label = rootUuid;
      if (rootUuid) {
        const el = document.querySelector(`div[projectid="${rootUuid}"]`);
        if (el) {
          const name = extractNodeName(el) || '';
          const prefix = kind === 'CARTO_BULK_APPLY'
            ? '🏷️'
            : mode === 'folder' ? '📂' : '📄';
          label = name ? `${prefix} ${name}` : `${prefix} ${rootUuid}`;
        }
      }

      const line = document.createElement('div');
      line.style.whiteSpace = 'nowrap';
      line.style.textOverflow = 'ellipsis';
      line.style.overflow = 'hidden';
      line.style.display = 'flex';
      line.style.alignItems = 'center';

      const status = String(job.status || 'unknown');
      let progressText = '';

      // Show X/Y whenever there is meaningful multi-file work; avoid noisy 1/1.
      if (total && total > 1) {
        progressText = ` (${completed}/${total})`;
      }

      // Extra progress info (phase + node counters) when available.
      const extraBits = [];
      if (phase && phase !== 'refresh') {
        extraBits.push(`phase=${phase}`);
      }

      if (nodesCreated || nodesUpdated || nodesMoved || nodesDeleted) {
        extraBits.push(`nodes:+${nodesCreated} ~${nodesUpdated} ↻${nodesMoved} -${nodesDeleted}`);
      }

      // When server reports which file is being processed, show it (best-effort).
      if (currentFileUuid) {
        let fileLabel = currentFileUuid;
        const el = document.querySelector(`div[projectid="${currentFileUuid}"]`);
        if (el) {
          const name = extractNodeName(el) || '';
          fileLabel = name ? name : currentFileUuid;
        }
        extraBits.push(`file=${fileLabel}`);
      }

      if (extraBits.length > 0) {
        progressText += ` | ${extraBits.join(' | ')}`;
      }

      // STATUS RENDERING (May 2026 expanded):
      //   - `failed` -> red
      //   - `completed_with_warnings` -> yellow/orange ⚠️ with warning count
      //   - everything else -> default
      const labelSpan = document.createElement('span');
      const statusLower = status.toLowerCase();
      let statusBadge = `[${status}]`;
      if (statusLower === 'completed_with_warnings') {
        const warnCount = Number(job.warning_count || 0);
        statusBadge = warnCount > 0
          ? `⚠️ [completed_with_warnings: ${warnCount}]`
          : '⚠️ [completed_with_warnings]';
      }
      labelSpan.textContent = `${statusBadge} ${label}${progressText}`;
      if (statusLower === 'failed') {
        labelSpan.style.color = '#f88';
      } else if (statusLower === 'completed_with_warnings') {
        labelSpan.style.color = '#fc6'; // yellow/orange for warnings
      }
      labelSpan.style.flex = '1';
      labelSpan.style.overflow = 'hidden';
      labelSpan.style.textOverflow = 'ellipsis';

      const canCancelStatus = status.toLowerCase();
      // Terminal states (no point in offering a CANCEL button):
      //   completed, cancelled, failed, completed_with_warnings
      const cancellable = (
        canCancelStatus !== 'completed'
        && canCancelStatus !== 'failed'
        && canCancelStatus !== 'cancelled'
        && canCancelStatus !== 'completed_with_warnings'
      );

      line.appendChild(labelSpan);

      if (cancellable && window.ws && window.ws.readyState === WebSocket.OPEN) {
        const cancelSpan = document.createElement('span');
        cancelSpan.textContent = 'CANCEL';
        cancelSpan.style.marginLeft = '8px';
        cancelSpan.style.cursor = 'pointer';
        cancelSpan.style.color = '#f88';
        cancelSpan.style.flexShrink = '0';
        cancelSpan.title = 'Cancel this CARTO job';

        cancelSpan.addEventListener('click', (event) => {
          event.stopPropagation();
          try {
            window.ws.send(
              JSON.stringify({
                action: 'carto_cancel_job',
                job_id: job.job_id,
              }),
            );
          } catch (err) {
            console.error(`[GLIMPSE Cache v${GLIMPSE_VERSION}] carto_cancel_job send failed:`, err);
          }
        });

        line.appendChild(cancelSpan);
      }

      cartoJobsEl.appendChild(line);
    });
  }

  // Removed: timer-based scheduling (replaced with Ctrl-keyup check)

  // @beacon[
  //   id=auto-beacon@initializeUuidHoverHelper-n238,
  //   role=initializeUuidHoverHelper,
  //   slice_labels=ra-websocket,
  //   kind=ast,
  // ]
  function initializeUuidHoverHelper() {
    // Track cursor position on every mousemove
    document.addEventListener('mousemove', (event) => {
      lastMousePos = {x: event.clientX, y: event.clientY};
      hideUuidTooltip(); // Hide tooltip when cursor moves
    });

    // When the F12 popup is open, intercept action keys on keydown so
    // Workflowy's contenteditable does not also type the digit into the node.
    document.addEventListener('keydown', (event) => {
      if (!f12PopupState) {
        return;
      }

      if (event.key === 'Escape') {
        event.preventDefault();
        event.stopPropagation();
        if (typeof event.stopImmediatePropagation === 'function') {
          event.stopImmediatePropagation();
        }
        hideF12ActionPopup();
        return;
      }

      if (/^[1-9]$/.test(event.key)) {
        const handled = executeF12PopupAction(event.key);
        if (handled) {
          event.preventDefault();
          event.stopPropagation();
          if (typeof event.stopImmediatePropagation === 'function') {
            event.stopImmediatePropagation();
          }
        }
      }
    }, true);

    // F2, F3, F4, F8, F9, F10, and F12 keyup handlers
    document.addEventListener('keyup', (event) => {
      if (f12PopupState) {
        if (event.key === 'Escape') {
          event.preventDefault();
          hideF12ActionPopup();
          return;
        }
        if (/^[1-9]$/.test(event.key)) {
          const handled = executeF12PopupAction(event.key);
          if (handled) {
            event.preventDefault();
            return;
          }
        }
      }

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
      // @beacon-close[
      //   id=f9-handler@glimpse-ext,
      // ]

      // F12 keyup: open action popup for Cartographer refresh/update operations
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

        if (F12_POPUP_ENABLED) {
          showF12ActionPopup(projectEl);
          return;
        }

        const uuid = projectEl.getAttribute('projectid');
        if (!uuid) {
          return;
        }

        const name = extractNodeName(projectEl) || '';
        const note = extractNodeNote(projectEl) || '';
        dispatchF12RefreshAction({
          projectEl,
          uuid,
          name,
          note,
          targetType: classifyF12Target(name, note),
        }, true);
      }
    });

    document.addEventListener('mousedown', (event) => {
      if (!f12PopupEl || f12PopupEl.style.display !== 'block') {
        return;
      }
      if (event.target instanceof Node && f12PopupEl.contains(event.target)) {
        return;
      }
      hideF12ActionPopup();
    }, true);
  }

  // @beacon[
  //   id=auto-beacon@initializeMutationNotifications-twd1,
  //   role=initializeMutationNotifications,
  //   slice_labels=ra-websocket,
  //   kind=ast,
  // ]
  function initializeMutationNotifications() {
    const pending = new Set();
    let debounceTimer = null;
    const DEBOUNCE_MS = 800;

    // @beacon[
    //   id=auto-beacon@initializeMutationNotifications.scheduleNotify-tmwo,
    //   role=initializeMutationNotifications.scheduleNotify,
    //   slice_labels=ra-websocket,
    //   kind=ast,
    // ]
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

  // @beacon[
  //   id=glimpse-loading@standalone-dom-bootstrap,
  //   role=standalone client DOM-ready bootstrap,
  //   slice_labels=nexus--glimpse-extension,nexus-loading-flow,
  //   kind=span,
  //   show_span=true,
  //   comment=Main standalone-client entrypoint: wait for DOM readiness, then initialize websocket connectivity plus UUID/F12/mutation helpers,
  // ]
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
  // @beacon-close[
  //   id=glimpse-loading@standalone-dom-bootstrap,
  // ]
  
})();
