# AI LLM Progress Tracking in AI Routing Service

**Document Version:** 1.0
**Last Updated:** October 13, 2025
**Service:** lucid-ai-routing-service v1.1.x
**Author:** AI Analysis

---

## Table of Contents

1. [Overview](#overview)
2. [Progress Tracking Architecture](#progress-tracking-architecture)
3. [Data Models](#data-models)
4. [Progress Flow](#progress-flow)
5. [Pipeline Stages](#pipeline-stages)
6. [Kafka Integration](#kafka-integration)
7. [Redis Storage](#redis-storage)
8. [API Endpoints](#api-endpoints)
9. [Current Limitations](#current-limitations)
10. [Comparison with Ingestion Services](#comparison-with-ingestion-services)

---

## Overview

The AI Routing Service tracks progress for AI enrichment tasks (LLM operations) through a **Redis-based job tracking system**. Unlike the ingestion services (Slack/Gmail) which publish detailed progress events to Kafka's `ingestion-progress` topic, the AI Routing Service currently has:

- ✅ **Redis-based job storage** (`EnrichmentJob` entity)
- ✅ **Pipeline stage tracking** (STARTED → TRANSFORMED → VALIDATED → ENRICHED → COMPLETED)
- ✅ **Progress percentage calculation** (0% → 100%)
- ✅ **ETA and time-left estimation**
- ✅ **Kafka publishing capability** (integrated but limited usage)
- ❌ **No comprehensive progress event publishing** to `ingestion-progress` topic
- ❌ **No coverage tracking** (distinct days, temporal ranges)
- ❌ **No per-scope statistics** (per-conversation stats)

---

## Progress Tracking Architecture

### Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                      AI Routing Service                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐         ┌─────────────────────────┐         │
│  │ AIMessageProducer │────────→│ Kafka Topic: ai-enrich  │         │
│  └──────────────────┘         └─────────────────────────┘         │
│           │                                                         │
│           ├──→ Creates EnrichmentJob (Redis)                       │
│           │    - jobId, parentId, status: PENDING                  │
│           │    - progress: 0.0, estimatedTimeLeft: 0               │
│           │                                                         │
│  ┌────────▼─────────────────────────────────────┐                 │
│  │    Post-Processing Pipeline Orchestrator      │                 │
│  │  (processes AI responses from LLM providers)  │                 │
│  └───────────────┬──────────────────────────────┘                 │
│                  │                                                  │
│                  ├──→ EnrichmentJobProgressService                 │
│                  │    - updateProgress(jobId, stage, message)      │
│                  │    - markJobFailed(jobId, errorMessage)         │
│                  │                                                  │
│  ┌───────────────▼────────────────────────────────┐               │
│  │     EnrichmentJobProgressService                │               │
│  │  ┌──────────────────────────────────────────┐  │               │
│  │  │ Pipeline Stages (Progress %)              │  │               │
│  │  ├──────────────────────────────────────────┤  │               │
│  │  │ STARTED           →   0%                  │  │               │
│  │  │ TRANSFORMED       →  20%                  │  │               │
│  │  │ VALIDATED         →  40%                  │  │               │
│  │  │ ENRICHED          →  70%                  │  │               │
│  │  │ RESPONSE_SENT     →  90%                  │  │               │
│  │  │ COMPLETED         → 100%                  │  │               │
│  │  │ FAILED            →  -1% (error state)    │  │               │
│  │  └──────────────────────────────────────────┘  │               │
│  └───────────────┬────────────────────────────────┘               │
│                  │                                                  │
│                  ├──→ Update EnrichmentJob in Redis                │
│                  │    - progress, status, estimatedTimeLeft        │
│                  │                                                  │
│                  └──→ JobService.publishJobProgress()              │
│                       - Publishes to ingestion-progress topic      │
│                       - Format: IngestionStatusEvent DTO           │
│                                                                     │
│  ┌─────────────────────────────────────────────────────┐          │
│  │             Redis Storage                            │          │
│  │  ┌────────────────────────────────────────────┐     │          │
│  │  │  EnrichmentJob (TTL: 60 minutes)           │     │          │
│  │  │  - id, parentId, status, type              │     │          │
│  │  │  - progress (0.0-1.0), estimatedTimeLeft   │     │          │
│  │  │  - startTime, endTime, durationMs          │     │          │
│  │  │  - userId, tenantId, tenantSchema          │     │          │
│  │  └────────────────────────────────────────────┘     │          │
│  └─────────────────────────────────────────────────────┘          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Models

### EnrichmentJob (Redis Entity)

**Location:** `com.lucid.automation.airouting.model.EnrichmentJob`

```java
@RedisHash("enrichmentJob")
public class EnrichmentJob implements Serializable {
    @Id
    private String id;                          // Job ID (e.g., "job-abc123")

    @Indexed
    private String parentId;                    // Parent job ID for hierarchical tracking

    private String status;                      // PENDING, PROCESSING, COMPLETED, FAILED
    private String type;                        // Task type (e.g., ENRICH_CONVERSATION)
    private String result;                      // Final result or error message

    // Timestamps
    private Long createdAt;                     // Unix timestamp (ms)
    private Long updatedAt;                     // Unix timestamp (ms)

    // Progress tracking
    private Double progress;                    // 0.0 to 1.0 (0% to 100%)
    private Long durationMs;                    // Total duration in milliseconds
    private String startTime;                   // ISO-8601 string
    private String endTime;                     // ISO-8601 string or null
    private String estimatedCompletionTime;     // ISO-8601 string or null
    private Long estimatedTimeLeft;             // Milliseconds remaining

    // Multi-tenant tracking
    @Indexed
    private String userId;
    @Indexed
    private String tenantId;
    private String tenantSchema;

    // TTL (Time To Live) - Auto-cleanup after 60 minutes
    @TimeToLive(unit = TimeUnit.SECONDS)
    @Builder.Default
    private Long ttl = 3600L;                   // 60 minutes
}
```

### Pipeline Stages

**Location:** `EnrichmentJobProgressService.PipelineStage` enum

| Stage | Progress % | Status | Description |
|-------|-----------|--------|-------------|
| `STARTED` | 0% | PROCESSING | Job created, pipeline initiated |
| `TRANSFORMED` | 20% | PROCESSING | Input data transformed for LLM |
| `VALIDATED` | 40% | PROCESSING | Input validated, ready for AI |
| `ENRICHED` | 70% | PROCESSING | LLM processing complete |
| `RESPONSE_SENT` | 90% | PROCESSING | Response sent to data-storage |
| `COMPLETED` | 100% | COMPLETED | Job finished successfully |
| `FAILED` | -1% | FAILED | Job failed with error |

---

## Progress Flow

### 1. Job Creation Flow

```java
// AIMessageProducer.scheduleAiProcessing()
String jobId = IdUtil.generateId("job-");

// 1. Publish AI task to Kafka (ai-enrich topic)
AIMessage aiMessage = AIMessage.builder()
    .messageId(messageId)
    .taskType(taskType)
    .jobId(jobId)
    .parentId(parentId)
    .build();
kafkaTemplate.send(topic, aiMessage).get();

// 2. Create EnrichmentJob in Redis
EnrichmentJob job = EnrichmentJob.builder()
    .id(jobId)
    .parentId(parentId)
    .status("PENDING")
    .type(taskType.name())
    .progress(0.0)
    .createdAt(System.currentTimeMillis())
    .build();
enrichmentJobService.create(job);
```

### 2. Progress Update Flow

```java
// EnrichmentJobProgressService.updateProgress()
void updateProgress(String jobId, PipelineStage stage, String message) {
    EnrichmentJob job = enrichmentJobRepository.findById(jobId);

    // 1. Update progress percentage
    double progressPercentage = stage.getProgressPercentage();
    job.setProgress(progressPercentage / 100.0);

    // 2. Calculate ETA and time left
    if (progressPercentage > 0 && progressPercentage < 100) {
        long elapsedMs = System.currentTimeMillis() - job.getCreatedAt();
        long estimatedTotalMs = (long) (elapsedMs / (progressPercentage / 100.0));
        long estimatedTimeLeftMs = estimatedTotalMs - elapsedMs;

        job.setEstimatedTimeLeft(Math.max(0, estimatedTimeLeftMs));
        job.setEstimatedCompletionTime(
            Instant.ofEpochMilli(updatedAt + estimatedTimeLeftMs).toString()
        );
    }

    // 3. Update status based on stage
    switch (stage) {
        case STARTED -> job.setStatus("PROCESSING");
        case COMPLETED -> {
            job.setStatus("COMPLETED");
            job.setEndTime(Instant.now().toString());
            job.setDurationMs(updatedAt - createdAt);
        }
        case FAILED -> job.setStatus("FAILED");
    }

    // 4. Save to Redis
    enrichmentJobRepository.save(job);

    // 5. Publish to Kafka (ingestion-progress topic)
    publishProgressToKafka(job, stage);
}
```

### 3. Kafka Publishing Flow

```java
// EnrichmentJobProgressService.publishProgressToKafka()
private void publishProgressToKafka(EnrichmentJob job, PipelineStage stage) {
    // Calculate time left in seconds
    Integer timeLeftEta = job.getEstimatedTimeLeft() != null ?
        Math.max(0, (int) (job.getEstimatedTimeLeft() / 1000)) : null;

    // Map pipeline stage to progress stage
    String progressStage = mapPipelineStageToProgressStage(stage);
    // STARTED → "starting"
    // TRANSFORMED/VALIDATED → "processing"
    // ENRICHED → "enriching"
    // RESPONSE_SENT → "finalizing"
    // COMPLETED → "completed"
    // FAILED → "failed"

    // Publish to Kafka using JobService
    jobService.publishJobProgress(
        job.getId(),                    // jobId
        job.getParentId(),              // parentId (never null)
        "CREATION",                     // jobType (AI jobs are CREATION type)
        job.getTenantId(),
        job.getTenantSchema(),
        progressStage,                  // stage
        (int) stage.getProgressPercentage(),  // percent
        timeLeftEta,                    // timeLeftEta
        null                            // topics (not implemented for AI jobs)
    );
}
```

---

## Pipeline Stages

### Stage Progression Example

**Scenario:** Enriching a Slack conversation with 100 messages

```
Time  | Stage          | Progress | Status      | Time Left | Description
------|----------------|----------|-------------|-----------|-------------
00:00 | STARTED        | 0%       | PROCESSING  | ~5min     | Job created, queued for LLM
00:30 | TRANSFORMED    | 20%      | PROCESSING  | ~2min     | Messages formatted for Gemini
01:00 | VALIDATED      | 40%      | PROCESSING  | ~1.5min   | Input validated, sent to LLM
02:00 | ENRICHED       | 70%      | PROCESSING  | ~50sec    | LLM returned enriched data
02:30 | RESPONSE_SENT  | 90%      | PROCESSING  | ~15sec    | Response published to Kafka
02:45 | COMPLETED      | 100%     | COMPLETED   | 0sec      | Job finished
```

### ETA Calculation

```java
// Calculation logic (same as ingestion services)
long elapsedMs = currentTime - startTime;
long estimatedTotalMs = (long) (elapsedMs / (progressPercentage / 100.0));
long remainingMs = estimatedTotalMs - elapsedMs;

// Example: 40% complete after 1 minute
// elapsedMs = 60,000ms
// estimatedTotalMs = 60,000 / 0.40 = 150,000ms (2.5 minutes)
// remainingMs = 150,000 - 60,000 = 90,000ms (1.5 minutes)
```

---

## Kafka Integration

### Topics Used

1. **Input Topics (Consumed):**
   - `lucid-ingestion-messages` - Raw messages from Slack/Gmail ingestion
   - `ai-categorize` - Categorization tasks
   - `ai-summarize` - Summarization tasks
   - `ai-enrich` - General enrichment tasks

2. **Output Topics (Produced):**
   - `ai.responses.queue` - AI responses to be stored
   - `pre.ai.responses.queue` - Pre-processed responses
   - `ingestion-progress` - **Progress events (via JobService)**
   - `ai-token-consumption` - Token usage metrics

### Progress Event Format

**Published via:** `JobService.publishJobProgress()`

```java
// IngestionStatusEvent DTO structure (from lucid-common-dtos)
{
  "job_id": "job-abc123def456",
  "parent_id": "parent-xyz789",
  "job_type": "CREATION",              // AI jobs are CREATION type
  "tenant_id": "e5a45508-...",
  "tenant_schema": "tenant_e5a45508",
  "stage": "enriching",                // starting, processing, enriching, finalizing, completed, failed
  "status": "IN_PROGRESS",             // PENDING, IN_PROGRESS, COMPLETED, FAILED
  "percent_complete": 70.0,

  // Performance metrics
  "progress_metrics": {
    "messages_processed": null,        // Not tracked for AI jobs
    "conversations_processed": null,
    "attachments_processed": null,
    "bytes_processed": null,
    "channels_processed": null,
    "teams_processed": null
  },

  "performance": {
    "started_at": "2025-10-13T06:00:00Z",
    "last_update_at": "2025-10-13T06:02:00Z",
    "eta_at": "2025-10-13T06:03:30Z",
    "duration_ms": 120000,
    "time_left_eta_seconds": 90
  },

  // Not populated for AI jobs
  "coverage": null,
  "failures": null,
  "service_specific": null
}
```

### Current Limitations

❌ **AI jobs do NOT populate:**
- `progress_metrics` fields (messages_processed, etc.)
- `coverage` data (ingestedFromTs, ingestedToTs, distinctDays)
- `service_specific` metadata
- Per-scope statistics

✅ **AI jobs DO populate:**
- Basic job info (job_id, parent_id, tenant_id)
- Stage and status
- Progress percentage
- Performance timing (started_at, eta_at, time_left_eta_seconds)

---

## Redis Storage

### Configuration

```yaml
# application.yml
redis:
  conversation:
    max-messages: 100
    ttl-seconds: 604800              # 7 days
  enrichment-job:
    ttl-seconds: 3600                # 60 minutes (1 hour)
```

### Automatic Cleanup

- **EnrichmentJob TTL:** 60 minutes (3600 seconds)
- Jobs are automatically deleted from Redis after 1 hour
- Prevents memory buildup for completed/failed jobs
- Configured via `@TimeToLive` annotation on `EnrichmentJob.ttl` field

### Storage Keys

```
Redis Key Pattern: enrichmentJob:{jobId}
Example: enrichmentJob:job-abc123def456

Indexed Fields:
- parentId (supports hierarchical queries)
- userId (query all jobs for a user)
- tenantId (query all jobs for a tenant)
```

---

## API Endpoints

### Job Status Endpoints

**Location:** `EnrichmentJobService` (internal service, not exposed via REST API)

```java
// Get job by ID
EnrichmentJob getById(String id);

// Get all jobs for a user
Iterable<EnrichmentJob> getAllJobs(String userId);

// Get latest job by user ID
EnrichmentJob getLatestJobByUserId(String userId);

// Get latest job by user ID and tenant ID
EnrichmentJob getLatestJobByUserIdAndTenantId(String userId, String tenantId);
```

### Progress Service Methods

**Location:** `EnrichmentJobProgressService`

```java
// Update job progress
void updateProgress(String jobId, PipelineStage stage, String message);

// Mark job as failed
void markJobFailed(String jobId, String errorMessage);

// Initialize new job (deprecated - use version with parentId)
@Deprecated
void initializeJob(String jobId, String userId, String tenantId,
                   String tenantSchema, String taskType);

// Initialize new job with parent tracking
void initializeJob(String jobId, String parentId, String userId,
                   String tenantId, String tenantSchema, String taskType);
```

---

## Current Limitations

### Missing Features (Compared to Ingestion Services)

1. **No Coverage Tracking:**
   - No `ingestedFromTs` / `ingestedToTs` fields
   - No `distinctDaysTotal` / `distinctDaysNew` metrics
   - No temporal range tracking

2. **No Progress Metrics:**
   - `messages_processed` always null
   - `conversations_processed` always null
   - `attachments_processed` always null
   - `bytes_processed` always null
   - `channels_processed` always null
   - `teams_processed` always null

3. **No Per-Scope Statistics:**
   - No per-conversation breakdowns
   - No per-channel statistics
   - `perScope` array always empty

4. **No Service-Specific Metadata:**
   - No AI model information
   - No token consumption details
   - No LLM provider info in progress events
   - `service_specific` map always null

5. **No Failure Details:**
   - `failures` section always null
   - Error details only in `EnrichmentJob.result` field
   - No structured error reporting in progress events

### Why These Limitations Exist

**Different Use Cases:**
- Ingestion services track **volume metrics** (messages, channels, teams)
- AI routing tracks **processing stages** (transformation, validation, enrichment)
- Ingestion services have clear **temporal ranges** (fromTime → toTime)
- AI jobs have **variable durations** based on LLM response times

**Redis vs Database:**
- Ingestion jobs stored in **PostgreSQL** (persistent, queryable)
- Enrichment jobs stored in **Redis** (temporary, 60-minute TTL)
- Redis optimized for **speed**, not long-term analysis

---

## Comparison with Ingestion Services

### Slack/Gmail Ingestion Services

**Progress Tracking:**
```java
// IngestionStatusEventBuilder.java
IngestionStatusEvent buildEvent(IngestionJob job) {
    return IngestionStatusEvent.builder()
        .jobId(job.getJobId())
        .parentId(job.getParentId())
        .progressMetrics(ProgressMetrics.builder()
            .messagesProcessed(job.getMessagesCrawled())
            .conversationsProcessed(job.getConversationsProcessed())
            .attachmentsProcessed(job.getAttachmentsProcessed())
            .bytesProcessed(job.getBytesProcessed())
            .channelsProcessed(job.getChannelsProcessed())
            .teamsProcessed(job.getTeamsProcessed())
            .build())
        .coverage(CoverageInfo.builder()
            .ingestedFromTs(job.getIngestedFromTs())
            .ingestedToTs(job.getIngestedToTs())
            .distinctDaysTotal(job.getDistinctDaysTotal())
            .distinctDaysNew(job.getDistinctDaysNew())
            .perScope(parsePerScopeStats(job))
            .build())
        .performance(PerformanceMetrics.builder()
            .startedAt(job.getStartTime())
            .etaAt(calculateEtaInstant(job))
            .timeLeftEtaSeconds(calculateTimeLeft(job))
            .build())
        .build();
}
```

### AI Routing Service

**Progress Tracking:**
```java
// EnrichmentJobProgressService.java
void publishProgressToKafka(EnrichmentJob job, PipelineStage stage) {
    jobService.publishJobProgress(
        job.getId(),
        job.getParentId(),
        "CREATION",
        job.getTenantId(),
        job.getTenantSchema(),
        mapStageToProgressStage(stage),
        (int) stage.getProgressPercentage(),
        calculateTimeLeft(job),
        null  // No topics array
    );
}
```

### Key Differences

| Feature | Ingestion Services | AI Routing Service |
|---------|-------------------|-------------------|
| **Storage** | PostgreSQL (persistent) | Redis (60min TTL) |
| **Progress Metrics** | ✅ All fields populated | ❌ All fields null |
| **Coverage Tracking** | ✅ Full temporal tracking | ❌ Not implemented |
| **Per-Scope Stats** | ✅ Per-channel breakdown | ❌ Not implemented |
| **Service Metadata** | ✅ Platform-specific data | ❌ Not implemented |
| **Failure Details** | ✅ Structured error info | ⚠️ Basic error message only |
| **ETA Calculation** | ✅ Based on progress % | ✅ Based on pipeline stage |
| **Kafka Publishing** | ✅ Comprehensive events | ⚠️ Basic events only |
| **Job Hierarchy** | ✅ Parent-child tracking | ✅ Parent-child tracking |

---

## Recommendations for Enhancement

### Phase 1: Metrics Population

**Add AI-specific metrics to progress events:**

```java
// Proposed: AIProgressMetrics (new DTO)
public class AIProgressMetrics {
    private Integer tokensConsumed;           // Total tokens used
    private Integer tokensRemaining;          // Tokens left in quota
    private Integer batchesProcessed;         // Number of batches completed
    private Integer batchesTotal;             // Total batches to process
    private String aiProvider;                // "gemini", "openai"
    private String aiModel;                   // "gemini-2.0-flash"
    private Long inferenceTimeMs;             // Time spent in LLM
    private Integer retryCount;               // Number of retries
}
```

### Phase 2: Coverage Tracking

**Track temporal coverage for AI enrichment:**

```java
// Add to EnrichmentJob
private Instant enrichedFromTs;              // Earliest message enriched
private Instant enrichedToTs;                // Latest message enriched
private Integer messagesEnriched;            // Count of messages enriched
private Integer conversationsEnriched;       // Count of conversations enriched
```

### Phase 3: Service-Specific Metadata

**Add AI context to progress events:**

```java
// Proposed metadata structure
"service_specific": {
    "ai_provider": "gemini",
    "ai_model": "gemini-2.0-flash",
    "task_type": "ENRICH_CONVERSATION",
    "tokens_consumed": 15420,
    "tokens_quota_remaining": 84580,
    "sliding_window_batch": 3,
    "sliding_window_total": 5,
    "inference_time_ms": 2340,
    "retry_count": 0,
    "cache_hit": false
}
```

### Phase 4: Unified Progress Interface

**Create consistent progress API across all services:**

```java
// Proposed: ProgressPublisher interface
public interface ProgressPublisher {
    void publishProgress(String jobId, String parentId,
                        ProgressSnapshot snapshot);
}

// Implemented by:
// - SlackProgressPublisher
// - GmailProgressPublisher
// - AIProgressPublisher (NEW)
```

---

## Conclusion

The AI Routing Service has a **functional but basic** progress tracking system:

✅ **What Works:**
- Pipeline stage tracking (0% → 100%)
- ETA and time-left calculation
- Redis-based job storage with auto-cleanup
- Kafka progress publishing (basic)
- Parent-child job relationships

❌ **What's Missing:**
- AI-specific progress metrics (tokens, batches, inference time)
- Coverage tracking (temporal ranges)
- Per-scope statistics
- Service-specific metadata in progress events
- Comprehensive failure details

**Recommendation:** Implement **Phase 1** (AI-specific metrics) to align with ingestion services and provide better visibility into LLM processing progress for data-storage-service and frontend consumers.

---

**End of Document**
