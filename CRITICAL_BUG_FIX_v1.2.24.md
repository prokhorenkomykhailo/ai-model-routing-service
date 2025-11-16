# AI Routing Service v1.2.24 - Critical Bug Fix

**Date:** November 16, 2025
**Author:** vudu
**Version:** 1.2.24
**Previous Version:** 1.2.23

## Critical Issues Fixed

### 🔴 Issue #1: Infinite Message Reprocessing Loop
**Problem:** Same 170 messages processed repeatedly every ~20 minutes, causing:
- Excessive AI API costs (same messages processed 20+ times)
- Duplicate topic creation attempts
- Redis churn (delete/reload same messages)
- Kafka consumer instability

**Root Cause:** Messages deleted from Redis BEFORE AI processing completes
- `cleanupBatchMessages()` called immediately after sending to AI (~instant)
- AI processing takes ~20 minutes to complete
- `PostProcessingConsumer` tries to mark messages as processed (~20 min later)
- Messages already deleted from Redis → "No Messages Found" warning
- Messages never marked `isProcessed=true`
- Next scheduler cycle (15 min) → loads same messages again
- **INFINITE LOOP**

**Fix:**
```java
// SlidingWindowService.java line ~668
// BEFORE:
cleanupBatchMessages(currentBatch, overlapSize, batchNumber);

// AFTER (commented out):
// NOTE: Cleanup moved to PostProcessingConsumer after messages are marked as processed
// This prevents premature deletion before AI processing completes (~20 minutes)
// cleanupBatchMessages(currentBatch, overlapSize, batchNumber);
```

### 🔴 Issue #2: Messages Not Marked After Processing
**Problem:** No cleanup after successful AI processing and message marking

**Fix:** Added cleanup in `PostProcessingConsumer` after successful marking
```java
// PostProcessingConsumer.java line ~471
// Mark messages as processed
slidingWindowService.markMessagesAsProcessedByIds(originalMessageIds, tenantId, deemergeUserId);

// NEW: Clean up marked messages after successful processing
slidingWindowService.cleanupMessagesByIds(originalMessageIds, tenantId, deemergeUserId);
```

**New Method Added:**
```java
// SlidingWindowService.java - new public method
public void cleanupMessagesByIds(List<String> messageIds, String tenantId, String deemergeUserId)
```

### 🟡 Issue #3: Kafka Consumer Timeout
**Problem:** AI processing takes ~20 minutes, exceeds `max.poll.interval.ms` (15 minutes)
- Consumer kicked out: `❌ [ACK-ERROR] consumer is not part of an active group`
- Kafka offset not committed
- Same message reprocessed on recovery

**Fix:** Increased timeout from 15 to 30 minutes
```java
// KafkaConfig.java line ~101 and ~222
// BEFORE:
configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 900000);  // 15 minutes

// AFTER:
configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1800000);  // 30 minutes
```

## Changes Summary

### Modified Files
1. **SlidingWindowService.java**
   - Commented out premature `cleanupBatchMessages()` call (line ~668)
   - Added new public method `cleanupMessagesByIds()` (line ~917)

2. **PostProcessingConsumer.java**
   - Added cleanup call after successful marking (line ~473)
   - Uses new `cleanupMessagesByIds()` method

3. **KafkaConfig.java**
   - Increased `MAX_POLL_INTERVAL_MS_CONFIG` from 900000 to 1800000 (both consumer factories)
   - Updated comments to reflect 30-minute timeout

### Version Updates
- **pom.xml**: 1.2.23 → 1.2.24
- **lucid-backend2/docker-compose.yml**: Updated image tag to 1.2.24
- **lucy-deployment/docker-compose.yml**: Updated image tag to 1.2.24

## Expected Outcomes

### ✅ Positive Impact
1. **No more infinite reprocessing loops**
   - Messages marked as processed after AI completion
   - Same messages won't be reloaded by scheduler

2. **Reduced AI API costs**
   - Each message processed exactly once (unless AI fails)
   - No duplicate enrichment requests

3. **Kafka consumer stability**
   - 30-minute timeout accommodates 20-minute AI processing
   - No more consumer kick-outs
   - Proper offset commits

4. **Clean Redis state**
   - Messages deleted only after successful processing
   - No orphaned unprocessed messages

### 📊 Monitoring Points
After deployment, verify:
- [ ] "No Messages Found" warnings disappear from logs
- [ ] Same message IDs not reprocessed multiple times
- [ ] Kafka consumer stays in group (no kick-out errors)
- [ ] AI API call volume decreases
- [ ] Topic creation rate normalizes
- [ ] Messages properly marked `isProcessed=true` in Redis

### ⚠️ Known Side Effects
- **Redis memory usage may increase slightly** (messages retained until marking completes)
  - Trade-off: Correctness vs. memory optimization
  - Acceptable since messages deleted after ~20 minutes instead of instantly

## Deployment Instructions

### Build & Push
```bash
# From lucid-backend2 root
cd lucid-ai-routing-service
mvn clean compile package -Pdocker -DskipTests

# Tag and push
docker tag lucid-backend2-ai-routing-service:latest docker.x51.vn/lucy/lucid-ai-routing-service:1.2.24
docker push docker.x51.vn/lucy/lucid-ai-routing-service:1.2.24
```

### Deploy to Production
```bash
# On production server
cd ~/lucid
git pull origin master  # Pull version update from lucy-deployment
docker compose pull ai-routing-service
docker compose up -d ai-routing-service

# Verify deployment
docker compose logs -f ai-routing-service | head -100
curl http://localhost:8083/actuator/health
```

### Post-Deployment Verification
```bash
# Monitor for 30+ minutes to observe full cycle
docker compose logs -f ai-routing-service | grep -E "MESSAGE-PROCESSING|MESSAGE-CLEANUP|No Messages Found"

# Should see:
# ✅ "Marking N messages as processed" (without subsequent "No Messages Found")
# ✅ "Successfully cleaned up N messages after AI processing"
# ❌ NO MORE "No Messages Found" warnings
# ❌ NO MORE repeated processing of same message IDs
```

## Rollback Plan
If issues occur:
```bash
# Rollback to v1.2.23
docker tag docker.x51.vn/lucy/lucid-ai-routing-service:1.2.23 docker.x51.vn/lucy/lucid-ai-routing-service:latest
docker compose up -d ai-routing-service
```

## Technical Notes

### Message Lifecycle (After Fix)
1. **Scheduler** loads unprocessed messages (`isProcessed=false`)
2. **SlidingWindowService** sends batch to AI via Kafka
3. **Messages remain in Redis** (NOT deleted)
4. **AI processes** for ~20 minutes
5. **PostProcessingConsumer** receives AI response
6. **Mark as processed** via `markMessagesAsProcessedByIds()`
7. **Cleanup** via `cleanupMessagesByIds()` (soft delete)
8. **Next cycle** → messages not loaded (already processed)

### Why This Fix Works
- **Sequential guarantee**: Mark → Then cleanup
- **Retry safety**: If marking fails, messages remain for retry
- **No race conditions**: Cleanup only after successful marking
- **Kafka stability**: 30-minute timeout accommodates processing time

## Related Documentation
- Original bug discovery: Conversation log Nov 16, 2025
- Kafka credentials: `docs/KAFKA_CREDENTIALS_UPDATE.md`
- Build guide: `docs/BUILD_GUIDE.md`
- Agent guide: `docs/AGENTS.md`

---
**Status:** Ready for deployment ✅
**Risk Level:** Low (fixes critical bug, minimal side effects)
**Testing:** Required monitoring for 30+ minutes post-deployment
