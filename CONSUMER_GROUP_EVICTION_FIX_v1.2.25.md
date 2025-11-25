# AI Routing Service v1.2.25 - Consumer Group Eviction Fix

## Issue
**Error**: `CommitFailedException: Offset commit cannot be completed since the consumer is not part of an active group for auto partition assignment; it is likely that the consumer was kicked out of the group.`

**Root Cause**: The `consumerFactory()` used by `IngestionConsumer` was missing critical Kafka timeout configurations, causing the consumer to be kicked out of the consumer group when processing took longer than the default `max.poll.interval.ms` (5 minutes).

## Solution

### Changes Made

#### 1. Added Timeout Configurations to `consumerFactory()`
**File**: `src/main/java/com/lucid/automation/airouting/config/KafkaConfig.java`

Added the following consumer timeout configurations:
```java
// CRITICAL: Timeout configurations to prevent consumer group eviction
configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1800000);  // 30 minutes
configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);     // 5 minutes
configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 90000);   // 1.5 minutes
configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);           // Batch size
```

**Rationale**:
- `max.poll.interval.ms = 30 minutes`: Allows consumer adequate time to process messages before being considered dead
- `session.timeout.ms = 5 minutes`: Balanced stability setting for detecting dead consumers
- `heartbeat.interval.ms = 1.5 minutes`: Must be < session_timeout/3 per Kafka requirements
- `max.poll.records = 50`: Reasonable batch size for ingestion processing

#### 2. Created Missing Container Factory Bean
Added `kafkaListenerContainerFactory` bean that was referenced by `IngestionConsumer`:
```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, IngestionEventDTO> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, IngestionEventDTO> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setCommonErrorHandler(defaultErrorHandler());
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    return factory;
}
```

## Configuration Consistency

All consumer factories now have consistent timeout configurations:

| Consumer Factory | max.poll.interval.ms | session.timeout.ms | heartbeat.interval.ms | max.poll.records |
|------------------|---------------------|-------------------|----------------------|-----------------|
| consumerFactory() | 30 min | 5 min | 1.5 min | 50 |
| aiMessageConsumerFactory() | 30 min | 5 min | 1.5 min | 10 |
| genericObjectConsumerFactory() | 30 min | 5 min | 1.5 min | (default) |

## Deployment

### Build & Deploy
```bash
# Build
cd lucid-backend2/lucid-ai-routing-service
mvn clean compile package -Pdocker -DskipTests

# Build Docker image
cd ..
docker build -f lucid-ai-routing-service/Dockerfile -t docker.x51.vn/lucy/ai-routing-service:1.2.25 .

# Push to registry
docker push docker.x51.vn/lucy/ai-routing-service:1.2.25

# Deploy to production
ssh ubuntu@10.113.213.30 "cd lucid && git pull && docker compose pull ai-routing-service && docker compose up -d ai-routing-service"
```

### Verification
```bash
# Check health
curl http://10.113.213.30:8083/actuator/health

# Verify timeout configuration in logs
docker logs ai-routing-service 2>&1 | grep "max.poll.interval.ms"
# Should show: max.poll.interval.ms = 1800000

# Monitor for CommitFailedException
docker logs ai-routing-service -f 2>&1 | grep -i "CommitFailedException"
# Should not appear
```

## Impact

**Before**:
- Consumer kicked out after 5 minutes (default timeout)
- `CommitFailedException` errors in logs
- Message processing interrupted
- Service required manual restart

**After**:
- Consumer has 30 minutes to process messages
- Proper heartbeat mechanism maintains consumer group membership
- No more unexpected evictions
- Consistent timeout configuration across all consumers

## Testing

Monitor the service for the following success criteria:
1. ✅ No `CommitFailedException` errors in logs
2. ✅ Consumer remains in consumer group during long processing
3. ✅ Service health remains UP
4. ✅ Messages processed successfully without interruption

## Related Issues

This fix addresses the consumer group eviction issue that was causing:
- Offset commit failures
- Message reprocessing
- Service instability during high load

This complements the earlier v1.2.24 fix that addressed infinite message reprocessing.

---
**Version**: 1.2.25
**Date**: 2025-11-16
**Author**: vudu
**Status**: ✅ Deployed to Production
