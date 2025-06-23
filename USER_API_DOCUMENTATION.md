# User Management API

This document describes the REST API endpoints for managing users stored in Redis within the lucid-ai-routing-service.

## Overview

The User Management API provides comprehensive endpoints to retrieve, search, and manage user data stored in Redis. Users are stored with a composite ID format: `{tenantId}:{workspaceId}:{slackUserId}`.

## Base URL

```
/api/users
```

## Endpoints

### 1. Get All Users

**GET** `/api/users`

Retrieves all users stored in Redis.

**Response:**
```json
[
  {
    "id": "tenant1:workspace1:U123456789",
    "tenantId": "tenant1",
    "workspaceId": "workspace1",
    "slackUserId": "U123456789",
    "name": "John Doe",
    "displayName": "John",
    "email": "john@example.com",
    "title": "Software Engineer",
    "firstName": "John",
    "lastName": "Doe",
    "messageCount": 42,
    "isActive": true,
    "createdAt": "2025-06-23T10:00:00",
    "updatedAt": "2025-06-23T15:30:00",
    "lastSeenAt": "2025-06-23T15:30:00"
  }
]
```

### 2. Get All Users (DTO Format)

**GET** `/api/users/dto`

Retrieves all users in simplified DTO format (excluding sensitive fields).

**Response:**
Same as above but with only essential fields.

### 3. Get Users by Tenant

**GET** `/api/users/tenant/{tenantId}`

Retrieves all users for a specific tenant.

**Parameters:**
- `tenantId` (path): The tenant identifier

### 4. Get Users by Workspace

**GET** `/api/users/workspace/{tenantId}/{workspaceId}`

Retrieves all users for a specific workspace.

**Parameters:**
- `tenantId` (path): The tenant identifier
- `workspaceId` (path): The workspace identifier

### 5. Get Users by Workspace (DTO Format)

**GET** `/api/users/dto/workspace/{tenantId}/{workspaceId}`

Retrieves workspace users in DTO format.

### 6. Get User by ID

**GET** `/api/users/id/{userId}`

Retrieves a specific user by their composite ID.

**Parameters:**
- `userId` (path): The composite user ID (`tenantId:workspaceId:slackUserId`)

**Response:**
```json
{
  "id": "tenant1:workspace1:U123456789",
  "tenantId": "tenant1",
  "workspaceId": "workspace1",
  "slackUserId": "U123456789",
  "name": "John Doe",
  "email": "john@example.com",
  "messageCount": 42,
  "isActive": true
}
```

### 7. Get Users by Slack ID

**GET** `/api/users/slack/{slackUserId}`

Retrieves all users with a specific Slack user ID across all tenants/workspaces.

**Parameters:**
- `slackUserId` (path): The Slack user identifier

### 8. Search Users

**GET** `/api/users/search`

Search users by name or email with optional filtering.

**Parameters:**
- `query` (query): Search term (searches name, displayName, email, firstName, lastName)
- `tenantId` (query, optional): Filter by tenant ID
- `workspaceId` (query, optional): Filter by workspace ID

**Example:**
```
GET /api/users/search?query=john&tenantId=tenant1
```

### 9. Get Active Users

**GET** `/api/users/active/{tenantId}/{workspaceId}`

Retrieves only active users for a specific workspace.

**Parameters:**
- `tenantId` (path): The tenant identifier
- `workspaceId` (path): The workspace identifier

### 10. Get User Statistics

**GET** `/api/users/stats/{tenantId}/{workspaceId}`

Retrieves user statistics for a workspace.

**Parameters:**
- `tenantId` (path): The tenant identifier
- `workspaceId` (path): The workspace identifier

**Response:**
```json
{
  "totalUsers": 150,
  "activeUsers": 142,
  "totalMessages": 5847
}
```

### 11. Health Check

**GET** `/api/users/health`

Check Redis connectivity and return basic system statistics.

**Response:**
```json
{
  "status": "UP",
  "timestamp": "2025-06-23T15:30:00",
  "totalUsers": 150,
  "activeUsers": 142,
  "totalMessages": 5847,
  "usersByTenant": {
    "tenant1": 75,
    "tenant2": 75
  },
  "redisConnection": "OK"
}
```

## User Data Model

### Full User Entity

The complete User entity stored in Redis includes:

```java
{
  "id": "String",                    // Composite ID: tenantId:workspaceId:slackUserId
  "tenantId": "String",              // Tenant identifier
  "workspaceId": "String",           // Workspace/Team identifier
  "slackUserId": "String",           // Original Slack user ID
  "teamId": "String",                // Team ID
  "name": "String",                  // Full name
  "emailConfirmed": "Boolean",       // Email confirmation status
  "displayName": "String",           // Display name
  "displayNameNormalized": "String", // Normalized display name
  "realNameNormalized": "String",    // Normalized real name
  "email": "String",                 // Email address
  "title": "String",                 // Job title
  "phone": "String",                 // Phone number
  "firstName": "String",             // First name
  "lastName": "String",              // Last name
  "pronouns": "String",              // Preferred pronouns
  "statusText": "String",            // Status message
  "avatarHash": "String",            // Avatar hash
  "imageOriginal": "String",         // Original image URL
  "image24": "String",               // 24px image URL
  "image32": "String",               // 32px image URL
  "image48": "String",               // 48px image URL
  "image72": "String",               // 72px image URL
  "image192": "String",              // 192px image URL
  "image512": "String",              // 512px image URL
  "image1024": "String",             // 1024px image URL
  "teamName": "String",              // Team name
  "slackUpdatedAt": "Long",          // Slack update timestamp
  "createdAt": "LocalDateTime",      // Creation timestamp
  "updatedAt": "LocalDateTime",      // Last update timestamp
  "lastSeenAt": "LocalDateTime",     // Last activity timestamp
  "messageCount": "Long",            // Number of messages
  "isActive": "Boolean",             // Active status
  "metadata": "String"               // Additional metadata
}
```

### DTO Response Format

The DTO format includes only essential fields for API responses:

```java
{
  "id": "String",
  "tenantId": "String",
  "workspaceId": "String",
  "slackUserId": "String",
  "name": "String",
  "displayName": "String",
  "email": "String",
  "title": "String",
  "firstName": "String",
  "lastName": "String",
  "avatarHash": "String",
  "image24": "String",
  "image48": "String",
  "teamName": "String",
  "createdAt": "LocalDateTime",
  "updatedAt": "LocalDateTime",
  "lastSeenAt": "LocalDateTime",
  "messageCount": "Long",
  "isActive": "Boolean"
}
```

## Error Responses

### 404 Not Found
```json
{
  "timestamp": "2025-06-23T15:30:00",
  "status": 404,
  "error": "Not Found",
  "path": "/api/users/id/nonexistent"
}
```

### 500 Internal Server Error
```json
{
  "timestamp": "2025-06-23T15:30:00",
  "status": 500,
  "error": "Internal Server Error",
  "path": "/api/users"
}
```

### 503 Service Unavailable (Redis Down)
```json
{
  "status": "DOWN",
  "error": "Connection refused",
  "timestamp": 1703347800000
}
```

## Usage Examples

### cURL Examples

#### Get all users
```bash
curl -X GET "http://localhost:8083/api/users"
```

#### Get users for a workspace
```bash
curl -X GET "http://localhost:8083/api/users/workspace/tenant1/workspace1"
```

#### Search users
```bash
curl -X GET "http://localhost:8083/api/users/search?query=john&tenantId=tenant1"
```

#### Get user statistics
```bash
curl -X GET "http://localhost:8083/api/users/stats/tenant1/workspace1"
```

#### Health check
```bash
curl -X GET "http://localhost:8083/api/users/health"
```

### JavaScript/Fetch Examples

```javascript
// Get all users
const users = await fetch('/api/users').then(r => r.json());

// Get workspace users
const workspaceUsers = await fetch('/api/users/workspace/tenant1/workspace1')
  .then(r => r.json());

// Search users
const searchResults = await fetch('/api/users/search?query=john&tenantId=tenant1')
  .then(r => r.json());

// Get user stats
const stats = await fetch('/api/users/stats/tenant1/workspace1')
  .then(r => r.json());
```

## Notes

- All endpoints return JSON responses
- Timestamps are in ISO 8601 format
- User IDs follow the format: `{tenantId}:{workspaceId}:{slackUserId}`
- Search is case-insensitive and searches across multiple fields
- The API supports both full entity and DTO response formats
- Health check endpoint can be used for monitoring Redis connectivity
