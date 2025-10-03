# AI Routing Service Documentation

This directory contains documentation for the AI Routing Service, which handles AI model routing, user management, and workspace coordination.

## 📚 Documentation

### Implementation Guides
- **[AI_PROGRESS_REPORTING_IMPLEMENTATION.md](./AI_PROGRESS_REPORTING_IMPLEMENTATION.md)** - AI task progress tracking implementation
- **[README-MESSAGE-SCHEDULER-CONFIG.md](./README-MESSAGE-SCHEDULER-CONFIG.md)** - Message scheduler configuration guide
- **[overlap-strategy-examples.md](./overlap-strategy-examples.md)** - Message overlap handling strategies

### Troubleshooting
- **[LOMBOK_COMPILATION_ISSUE.md](./LOMBOK_COMPILATION_ISSUE.md)** - Lombok configuration and compilation fixes

## 🎯 Service Overview

The AI Routing Service is responsible for:

1. **User Management**
   - Store user information from ingestion events
   - Cross-platform user identification (Slack, Gmail)
   - Redis-based user caching
   - User ID format: `tenant:workspace:userId`

2. **AI Model Routing**
   - Route messages to appropriate AI models
   - Task queue management
   - Progress reporting

3. **Workspace Management**
   - Workspace/team coordination
   - Cross-platform workspace mapping

## 🏗️ Architecture

### Key Components

#### UserService
- **Purpose**: User lifecycle management
- **Storage**: Redis cache
- **Key Features**:
  - Unified user DTO handling (IngestionUserDTO)
  - Cross-platform identification via workspaceId
  - TTL-based cache management

#### MessageService
- **Purpose**: Message processing and routing
- **Features**:
  - Extract workspace context from messages
  - Route to appropriate AI models
  - Handle both Slack and Gmail messages

#### WorkspaceService
- **Purpose**: Workspace/team management
- **Features**:
  - Workspace metadata storage
  - Team member tracking
  - Cross-platform workspace mapping

### Data Flow

```
Kafka (lucid-ingestion-messages)
  ↓
Consumer (IngestionEventListener)
  ↓
UserService.createOrUpdateUser()
  ↓
Redis (user cache)
  ↓
MessageService.processMessage()
  ↓
AI Model Router
  ↓
Kafka (lucid-ai-tasks)
```

## 🔑 Key Features

### Unified Message Handling (v1.1.0)
- **IngestionMessageDTO** support for all platforms
- **workspaceId** extraction for user identification
- Eliminated ClassCastException errors

### User Storage Format
```
Key: tenant:{workspaceId}:{userId}
Examples:
  - Slack: tenant:T1234567890:U1234567890
  - Gmail: tenant:example.com:gmail-sub-123
```

### Progress Reporting
- Real-time AI task progress updates
- Webhook notifications
- Progress percentage tracking

## 📝 Configuration

### Key Properties

```yaml
# Redis Configuration
spring:
  data:
    redis:
      host: ${REDIS_HOST}
      port: ${REDIS_PORT}
      ttl: 3600 # User cache TTL (seconds)

# Kafka Configuration
spring:
  kafka:
    consumer:
      topics:
        ingestion-messages: lucid-ingestion-messages
    producer:
      topics:
        ai-tasks: lucid-ai-tasks
        ai-responses: ai-responses

# AI Model Configuration
ai:
  routing:
    default-model: gpt-4
    retry-attempts: 3
```

## 🔄 Message Scheduler

The message scheduler handles:
- Batch message processing
- Priority-based routing
- Rate limiting
- Overlap detection strategies

See **[README-MESSAGE-SCHEDULER-CONFIG.md](./README-MESSAGE-SCHEDULER-CONFIG.md)** for detailed configuration.

## 🧪 Testing

### Run Unit Tests
```bash
./mvnw clean test
```

### Run Integration Tests
```bash
./mvnw clean verify
```

### Test User Storage
```bash
# Check Redis for stored users
redis-cli -h <redis-host> KEYS "tenant:*"
redis-cli -h <redis-host> GET "tenant:example.com:user123"
```

## 🐛 Common Issues

### Lombok Compilation Errors
See **[LOMBOK_COMPILATION_ISSUE.md](./LOMBOK_COMPILATION_ISSUE.md)** for Maven configuration fixes.

### ClassCastException with User DTOs
**Fixed in v1.1.0**: Now uses unified IngestionUserDTO instead of platform-specific casts.

### NullPointerException on workspaceId
**Fixed in v1.1.0**: Added `getBestWorkspaceId()` helper method with fallback logic.

## 📊 Monitoring

### Health Check
```bash
curl http://localhost:8083/actuator/health
```

### Metrics
- User cache hit/miss ratio
- Message processing throughput
- AI model response times

### Logs
```bash
# View user storage logs
docker compose logs ai-routing-service | grep "user"

# View message routing logs
docker compose logs ai-routing-service | grep "routing"
```

## 🔗 Related Documentation

- **lucid-backend2/docs/UNIFIED_MESSAGE_DTO_IMPLEMENTATION.md** - Unified message architecture
- **lucid-backend2/docs/UNIFIED_INGESTION_DTO_IMPLEMENTATION.md** - User DTO standardization

## 🚀 Deployment

See **lucy-deployment/docs/** for deployment guides and operational procedures.

## 📞 Support

For service-specific issues:
1. Check this documentation
2. Review service logs
3. Consult main architecture documentation
