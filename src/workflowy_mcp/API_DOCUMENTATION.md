# Workflowy API Documentation

**Source:** https://beta.workflowy.com/api-reference/
**Base URL:** `https://workflowy.com`
**API Version:** v1
**Authentication:** Bearer token in Authorization header

**🔑 API Key:** `564291ef7bf21807f6ea80c7777bfe2309c8f052`

---

## Authentication

All requests require Bearer token authentication:

```bash
-H "Authorization: Bearer <YOUR_API_KEY>"
```

---

## Endpoints

### 1. Create Node

**POST** `/api/v1/nodes`

Create a new node in your WorkFlowy outline.

**Parameters:**
- `parent_id` (string, optional) - The parent node identifier. Can be:
  - A node UUID
  - A target key (e.g., "inbox", "home")
  - `None` to create a top-level node
- `name` (string, required) - The text content of the new node
- `note` (string, optional) - Additional note content for the node
- `layoutMode` (string, optional) - The display mode of the node
- `position` (string, optional) - Where to place the new node: `"top"` or `"bottom"` (default: `"top"`)

**Request Example:**
```bash
curl -X POST https://workflowy.com/api/v1/nodes \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <YOUR_API_KEY>" \
  -d '{
    "parent_id": "inbox",
    "name": "Hello API",
    "position": "top"
  }' | jq
```

**Response Example:**
```json
{
  "item_id": "5b401959-4740-4e1a-905a-62a961daa8c9"
}
```

---

### 2. Update Node

**POST** `/api/v1/nodes/:id`

Update an existing node's content.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to update
- `name` (string, optional) - The text content of the node
- `note` (string, optional) - The note content of the node
- `layoutMode` (string, optional) - The display mode of the node

**Request Example:**
```bash
curl -X POST https://workflowy.com/api/v1/nodes/5b401959-4740-4e1a-905a-62a961daa8c9 \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <YOUR_API_KEY>" \
  -d '{
    "name": "Updated node title"
  }' | jq
```

**Response Example:**
```json
{
  "status": "ok"
}
```

---

### 3. Get Node

**GET** `/api/v1/nodes/:id`

Retrieve a specific node by its ID.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to retrieve

**Request Example:**
```bash
curl -X GET https://workflowy.com/api/v1/nodes/6ed4b9ca-256c-bf2e-bd70-d8754237b505 \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "node": {
    "id": "6ed4b9ca-256c-bf2e-bd70-d8754237b505",
    "name": "This is a test outline for API examples",
    "note": null,
    "priority": 200,
    "data": {
      "layoutMode": "bullets"
    },
    "createdAt": 1753120779,
    "modifiedAt": 1753120850,
    "completedAt": null
  }
}
```

---

### 4. List Nodes

**GET** `/api/v1/nodes`

List nodes with optional parent filtering.

**Parameters:**
- `parent_id` (string, optional, query param) - The parent node identifier. Can be:
  - A node UUID
  - A target key (e.g., "inbox")
  - Omit to list top-level nodes

**Request Example:**
```bash
curl -G https://workflowy.com/api/v1/nodes \
  -H "Authorization: Bearer <YOUR_API_KEY>" \
  -d "parent_id=inbox" | jq
```

**Response Example:**
```json
{
  "nodes": [
    {
      "id": "ee1ac4c4-775e-1983-ae98-a8eeb92b1aca",
      "name": "Bullet A",
      "note": null,
      "priority": 100,
      "data": {
        "layoutMode": "bullets"
      },
      "createdAt": 1753120787,
      "modifiedAt": 1753120815,
      "completedAt": null
    }
  ]
}
```

**Notes:**
- The nodes are returned unordered
- Sort them yourself based on the `priority` field
- Lower priority values appear first (priority 100 before priority 200)

---

### 5. Delete Node

**DELETE** `/api/v1/nodes/:id`

Permanently delete a node and all its children.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to delete

**Request Example:**
```bash
curl -X DELETE https://workflowy.com/api/v1/nodes/ee1ac4c4-775e-1983-ae98-a8eeb92b1aca \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "status": "ok"
}
```

**⚠️ Warning:** This permanently deletes the node. Cannot be undone.

---

### 6. Move Node

**POST** `/api/v1/nodes/:id/move`

Move a node to a new parent location.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to move
- `parent_id` (string, optional) - The new parent node identifier
- `position` (string, optional) - Where to place the node: `"top"` or `"bottom"` (default: `"top"`)

**Request Example:**
```bash
curl -X POST https://workflowy.com/api/v1/nodes/ee1ac4c4-775e-1983-ae98-a8eeb92b1aca/move \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <YOUR_API_KEY>" \
  -d '{
    "parent_id": "inbox",
    "position": "top"
  }' | jq
```

**Response Example:**
```json
{
  "status": "ok"
}
```

---

### 7. Complete Node

**POST** `/api/v1/nodes/:id/complete`

Mark a node as completed.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to complete

**Request Example:**
```bash
curl -X POST https://workflowy.com/api/v1/nodes/ee1ac4c4-775e-1983-ae98-a8eeb92b1aca/complete \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "status": "ok"
}
```

---

### 8. Uncomplete Node

**POST** `/api/v1/nodes/:id/uncomplete`

Mark a node as not completed.

**Parameters:**
- `id` (string, required, in URL) - The identifier of the node to uncomplete

**Request Example:**
```bash
curl -X POST https://workflowy.com/api/v1/nodes/ee1ac4c4-775e-1983-ae98-a8eeb92b1aca/uncomplete \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "status": "ok"
}
```

---

### 9. Export All Nodes (⭐ NEW)

**GET** `/api/v1/nodes-export`

Export all nodes in your WorkFlowy account as a flat list.

**Parameters:** None

**Request Example:**
```bash
curl https://workflowy.com/api/v1/nodes-export \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "nodes": [
    {
      "id": "ee1ac4c4-775e-1983-ae98-a8eeb92b1aca",
      "name": "Top Level Item",
      "note": "This is a note",
      "parent_id": null,
      "priority": 100,
      "completed": false,
      "data": {
        "layoutMode": "bullets"
      },
      "createdAt": 1753120787,
      "modifiedAt": 1753120815,
      "completedAt": null
    },
    {
      "id": "5b401959-4740-4e1a-905a-62a961daa8c9",
      "name": "Child Item",
      "note": null,
      "parent_id": "ee1ac4c4-775e-1983-ae98-a8eeb92b1aca",
      "priority": 100,
      "completed": false,
      "data": {
        "layoutMode": "bullets"
      },
      "createdAt": 1753120800,
      "modifiedAt": 1753120820,
      "completedAt": null
    }
  ]
}
```

**Key Features:**
- Returns **all nodes** in your WorkFlowy account as a **flat list**
- Each node includes its `parent_id` field to reconstruct the hierarchy
- Root-level nodes have `parent_id: null`
- **⚠️ Rate Limit:** 1 request per minute

**Use Cases:**
- Full backup of your WorkFlowy data
- Rebuilding node hierarchy programmatically
- Searching across all nodes
- Data migration or analysis

---

### 10. List Targets

**GET** `/api/v1/targets`

List all available target keys (shortcuts and system targets).

**Parameters:** None

**Request Example:**
```bash
curl https://workflowy.com/api/v1/targets \
  -H "Authorization: Bearer <YOUR_API_KEY>" | jq
```

**Response Example:**
```json
{
  "targets": [
    {
      "key": "home",
      "type": "shortcut",
      "name": "My Home Page"
    },
    {
      "key": "inbox",
      "type": "system",
      "name": "Inbox"
    }
  ]
}
```

**Notes:**
- Target keys can be used as `parent_id` values in create/move operations
- System targets: `"inbox"` (always available)
- User can create custom shortcut targets in WorkFlowy UI

---

## Rate Limits

**Default:** Not specified for most endpoints
**Export Endpoint:** 1 request per minute

The MCP server includes adaptive rate limiting (10 req/s initial, adjusts between 1-100 req/s based on API responses).

---

## Error Responses

**401 Unauthorized:**
```json
{
  "error": "Invalid API key or unauthorized access"
}
```

**404 Not Found:**
```json
{
  "error": "Resource not found"
}
```

**429 Rate Limit:**
```json
{
  "error": "Rate limit exceeded",
  "retry_after": 60
}
```

**5xx Server Error:**
```json
{
  "error": "Server error: 500"
}
```

---

## Data Types Reference

### Node Structure

```typescript
{
  id: string;              // UUID of the node
  name: string;            // Text content
  note: string | null;     // Note content (optional)
  parent_id: string | null; // Parent UUID (null for root nodes)
  priority: number;        // Ordering priority (lower = first)
  completed: boolean;      // Completion status
  data: {
    layoutMode: "bullets" | "todo" | "h1" | "h2" | "h3"
  };
  createdAt: number;       // Unix timestamp
  modifiedAt: number;      // Unix timestamp
  completedAt: number | null; // Unix timestamp (null if not completed)
}
```

### Target Structure

```typescript
{
  key: string;       // Target identifier (e.g., "inbox", "home")
  type: "system" | "shortcut"; // Target type
  name: string;      // Display name
}
```

---

## Testing the Export Endpoint

**Manual curl test command (API key included):**

```bash
curl https://workflowy.com/api/v1/nodes-export \
  -H "Authorization: Bearer 564291ef7bf21807f6ea80c7777bfe2309c8f052" | jq > export_test.json
```

**PowerShell version (tested and working):**

```powershell
$headers = @{'Authorization' = 'Bearer 564291ef7bf21807f6ea80c7777bfe2309c8f052'}
Invoke-RestMethod -Uri 'https://workflowy.com/api/v1/nodes-export' -Headers $headers | ConvertTo-Json -Depth 10
```

**✅ Verified:** This endpoint successfully returns all nodes in your Workflowy account as a flat list.

This will:
1. Export all nodes from your WorkFlowy account
2. Pretty-print JSON with jq
3. Save to `export_test.json` for inspection

**Verify:**
- Check response structure matches documentation
- Confirm `parent_id` values link nodes correctly
- Verify rate limit header (if present)
