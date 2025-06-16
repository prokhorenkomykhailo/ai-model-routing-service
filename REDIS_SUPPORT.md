# Redis Support in AI Routing Service

This document describes the Redis integration added to the AI Routing Service for managing conversation history.

## Overview

Redis is used to store and retrieve conversation messages by tenant, workspace, channel, and thread. This allows the service to maintain context for AI processing and retrieve historical messages when needed.

## Features

1. **Save Messages**: Automatically saves incoming messages from RabbitMQ to Redis.
2. **Retrieve Messages**: Get messages by tenant, workspace, channel, and thread.
3. **Retrieve Recent Messages**: Get the most recent N messages from a conversation.
4. **Delete Oldest Messages**: Delete the oldest N messages from a conversation.
5. **Automatic Trimming**: Automatically trims conversations to prevent them from growing too large.

## Configuration

Redis configuration is set in `application.yml`:

```yaml
spring:
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      password: ${REDIS_PASSWORD:}
      timeout: 2000
      database: 0

redis:
  conversation:
    max-messages: ${REDIS_MAX_MESSAGES:100}
    ttl-seconds: ${REDIS_TTL_SECONDS:604800} # 7 days
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| REDIS_HOST | Redis server hostname | localhost |
| REDIS_PORT | Redis server port | 6379 |
| REDIS_PASSWORD | Redis server password | (empty) |
| REDIS_MAX_MESSAGES | Maximum messages per conversation | 100 |
| REDIS_TTL_SECONDS | Time-to-live for messages in seconds | 604800 (7 days) |

## Usage

The Redis functionality is encapsulated in the `ConversationHistoryService`. This service is used by the `IngestionMessageListenerService` to automatically save incoming messages to Redis.

### Key Structure

Keys in Redis are structured as follows:
- `tenantId:workspaceId:channelId:threadTs:messageTs`

This allows for efficient retrieval and management of messages by any combination of these identifiers.

## Limitations

- Redis is currently configured without persistence. Consider enabling persistence for production deployments.
- The maximum number of messages per conversation is configurable but defaults to 100.
- Message TTL is configurable but defaults to 7 days.
