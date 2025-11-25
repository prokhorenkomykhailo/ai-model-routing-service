package com.lucid.automation.airouting.publisher;

import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.common.dto.messaging.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Publisher for AI enrichment job progress events to Kafka.
 * Transforms EnrichmentJob entities into IngestionStatusEvent DTOs with AI-specific metrics.
 *
 * @author vudu
 * @since 1.2.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AIProgressPublisher {

    private static final String AI_ROUTING_SERVICE = "AI_ROUTING";
    private static final String EVENT_VERSION = "1.0.0";
    private static final String INGESTION_PROGRESS_TOPIC = "ingestion-progress";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes AI progress event to Kafka ingestion-progress topic
     *
     * @param job The enrichment job to publish progress for
     * @param stage The current pipeline stage
     * @param statusType The type of status update (PROGRESS, FINAL, etc.)
     */
    public void publishProgress(EnrichmentJob job, String stage, String statusType) {
        if (job == null) {
            log.warn("❌ [AI-PROGRESS] Cannot publish progress: job is null");
            return;
        }

        if (job.getTenantId() == null || job.getTenantSchema() == null) {
            log.warn("❌ [AI-PROGRESS] Cannot publish progress for job {}: missing tenantId or tenantSchema", job.getId());
            return;
        }

        try {
            IngestionStatusEvent event = buildProgressEvent(job, stage, statusType);

            // Use tenantId as partition key for ordered processing
            String partitionKey = job.getTenantId();

            log.info("📤 [AI-PROGRESS] Publishing progress: jobId={}, stage={}, percent={}%, type={}",
                    job.getId(), stage, event.getStatus().getPercentComplete(), statusType);

            CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(INGESTION_PROGRESS_TOPIC, partitionKey, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("✅ [AI-PROGRESS] Published to partition {} offset {} for jobId={}",
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            job.getId());
                } else {
                    log.error("❌ [AI-PROGRESS] Failed to publish for jobId={}: {}",
                            job.getId(), ex.getMessage(), ex);
                }
            });

        } catch (Exception e) {
            log.error("❌ [AI-PROGRESS] Error building/publishing progress for jobId={}: {}",
                    job.getId(), e.getMessage(), e);
        }
    }

    /**
     * Builds comprehensive IngestionStatusEvent from EnrichmentJob
     */
    private IngestionStatusEvent buildProgressEvent(EnrichmentJob job, String stage, String statusType) {
        return IngestionStatusEvent.builder()
                .eventVersion(EVENT_VERSION)
                .emittedAt(Instant.now())
                .producerService(AI_ROUTING_SERVICE)
                .statusType(statusType)
                .job(buildJobDetails(job))
                .status(buildStatusDetails(job, stage))
                .build();
    }

    /**
     * Builds JobDetails section
     */
    private JobDetails buildJobDetails(EnrichmentJob job) {
        // Ensure parentId is never null - use jobId as parent if not set
        String parentId = job.getParentId() != null && !job.getParentId().isEmpty()
                ? job.getParentId()
                : job.getId();

        return JobDetails.builder()
                .jobId(job.getId())
                .parentId(parentId)
                .jobType("CREATION")  // AI enrichment jobs are CREATION type
                .tenantId(job.getTenantId())
                .tenantSchema(job.getTenantSchema())
                .userId(job.getUserId())
                .triggerSource(JobDetails.TriggerSource.API)
                .build();
    }

    /**
     * Builds StatusDetails section with all metrics
     */
    private StatusDetails buildStatusDetails(EnrichmentJob job, String stage) {
        return StatusDetails.builder()
                .stage(stage)
                .status(mapJobStatusToEventStatus(job.getStatus()))
                .percentComplete(calculatePercentComplete(job))
                .progressMetrics(buildProgressMetrics(job))
                .performance(buildPerformanceMetrics(job))
                .serviceSpecific(buildServiceSpecificMetadata(job))
                .build();
    }

    /**
     * Builds progress metrics for AI enrichment
     */
    private ProgressMetrics buildProgressMetrics(EnrichmentJob job) {
        return ProgressMetrics.builder()
                .messagesProcessed(job.getMessagesEnriched() != null ? job.getMessagesEnriched().longValue() : 0L)
                .conversationsProcessed(job.getConversationsEnriched() != null ? job.getConversationsEnriched().longValue() : 0L)
                .attachmentsProcessed(0L)  // Not applicable for AI enrichment
                .bytesProcessed(0L)        // Not applicable for AI enrichment
                .channelsProcessed(0)      // Not applicable for AI enrichment
                .teamsProcessed(0)         // Not applicable for AI enrichment
                .build();
    }

    /**
     * Builds performance metrics with timing information
     */
    private PerformanceMetrics buildPerformanceMetrics(EnrichmentJob job) {
        Instant startedAt = parseTimestampSafely(job.getStartTime());
        Instant lastUpdateAt = job.getUpdatedAt() != null ? Instant.ofEpochMilli(job.getUpdatedAt()) : Instant.now();
        Instant etaAt = calculateEtaInstant(job);
        Integer timeLeftEtaSeconds = calculateTimeLeftSeconds(job);

        return PerformanceMetrics.builder()
                .startedAt(startedAt)
                .lastUpdateAt(lastUpdateAt)
                .etaAt(etaAt)
                .durationMs(job.getDurationMs())
                .timeLeftEtaSeconds(timeLeftEtaSeconds)
                .build();
    }

    /**
     * Safely parses timestamp strings, handling both microsecond and nanosecond precision.
     * Java's Instant.parse() only supports up to 6 decimal places (microseconds), but the database
     * may store 9 decimal places (nanoseconds). This method truncates to 6 digits before parsing.
     *
     * @param timestamp The timestamp string to parse (may be null)
     * @return Parsed Instant or null if parsing fails or input is null
     */
    private Instant parseTimestampSafely(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }

        try {
            // Check if timestamp has more than 6 decimal places (nanosecond precision)
            // Format: 2025-10-13T08:45:03.731860498
            //                                 ^ index 19 (start of decimal)
            int decimalIndex = timestamp.lastIndexOf('.');
            if (decimalIndex > 0) {
                // Find the end of the decimal portion (before Z or T timezone indicators)
                int endIndex = timestamp.length();
                if (timestamp.contains("Z")) {
                    endIndex = timestamp.indexOf('Z');
                } else if (timestamp.lastIndexOf('T') > decimalIndex) {
                    // Handle timezone offsets like +00:00
                    endIndex = timestamp.lastIndexOf('T');
                }

                int decimalPlaces = endIndex - decimalIndex - 1;

                // If more than 6 decimal places, truncate to microseconds
                if (decimalPlaces > 6) {
                    String truncated = timestamp.substring(0, decimalIndex + 7); // Keep 6 decimal places
                    if (timestamp.endsWith("Z")) {
                        truncated += "Z";
                    }
                    return Instant.parse(truncated);
                }
            }

            // Standard parsing for properly formatted timestamps
            return Instant.parse(timestamp);

        } catch (Exception e) {
            log.warn("⚠️ [AI-PROGRESS] Failed to parse timestamp '{}': {}", timestamp, e.getMessage());
            return null;
        }
    }

    /**
     * Builds AI-specific metadata
     */
    private Map<String, Object> buildServiceSpecificMetadata(EnrichmentJob job) {
        Map<String, Object> metadata = new HashMap<>();

        // AI provider and model
        if (job.getAiProvider() != null) {
            metadata.put("ai_provider", job.getAiProvider());
        }
        if (job.getAiModel() != null) {
            metadata.put("ai_model", job.getAiModel());
        }

        // Task type
        if (job.getType() != null) {
            metadata.put("task_type", job.getType());
        }

        // Batch processing info
        if (job.getBatchesProcessed() != null) {
            metadata.put("batches_processed", job.getBatchesProcessed());
        }
        if (job.getBatchesTotal() != null) {
            metadata.put("batches_total", job.getBatchesTotal());
        }

        // Token consumption
        if (job.getTokensConsumed() != null && job.getTokensConsumed() > 0) {
            metadata.put("tokens_consumed", job.getTokensConsumed());
        }

        // Inference timing
        if (job.getInferenceTimeMs() != null && job.getInferenceTimeMs() > 0) {
            metadata.put("inference_time_ms", job.getInferenceTimeMs());
        }

        // Retry count
        if (job.getRetryCount() != null && job.getRetryCount() > 0) {
            metadata.put("retry_count", job.getRetryCount());
        }

        // Pipeline stage (inferred from progress)
        metadata.put("pipeline_stage", inferPipelineStage(job));

        return metadata.isEmpty() ? null : metadata;
    }

    /**
     * Calculates ETA instant with 3-tier fallback (same as ingestion services)
     */
    private Instant calculateEtaInstant(EnrichmentJob job) {
        // Tier 1: Use existing estimatedCompletionTime if present
        if (job.getEstimatedCompletionTime() != null && !job.getEstimatedCompletionTime().isEmpty()) {
            Instant parsed = parseTimestampSafely(job.getEstimatedCompletionTime());
            if (parsed != null) {
                return parsed;
            }
        }

        // Tier 2: Calculate from progress if > 0%
        if (job.getProgress() != null && job.getProgress() > 0 && job.getProgress() < 1.0
                && job.getCreatedAt() != null && job.getUpdatedAt() != null) {
            long elapsedMs = job.getUpdatedAt() - job.getCreatedAt();
            long estimatedTotalMs = (long) (elapsedMs / job.getProgress());
            long remainingMs = estimatedTotalMs - elapsedMs;
            return Instant.ofEpochMilli(job.getUpdatedAt() + Math.max(0, remainingMs));
        }

        // Tier 3: Default to 5 minutes from now for 0% progress
        return Instant.now().plusSeconds(300);
    }

    /**
     * Calculates time left in seconds
     */
    private Integer calculateTimeLeftSeconds(EnrichmentJob job) {
        if (job.getEstimatedTimeLeft() != null && job.getEstimatedTimeLeft() > 0) {
            return Math.max(0, (int) (job.getEstimatedTimeLeft() / 1000));
        }

        // Calculate from progress if available
        if (job.getProgress() != null && job.getProgress() > 0 && job.getProgress() < 1.0
                && job.getCreatedAt() != null && job.getUpdatedAt() != null) {
            long elapsedMs = job.getUpdatedAt() - job.getCreatedAt();
            long estimatedTotalMs = (long) (elapsedMs / job.getProgress());
            long remainingMs = estimatedTotalMs - elapsedMs;
            return Math.max(0, (int) (remainingMs / 1000));
        }

        // Default: 5 minutes for new jobs
        return 300;
    }

    /**
     * Maps job progress to percent complete
     */
    private Double calculatePercentComplete(EnrichmentJob job) {
        if (job.getProgress() == null) {
            return 0.0;
        }
        // Progress is stored as 0.0-1.0, convert to 0-100
        return Math.max(0.0, Math.min(100.0, job.getProgress() * 100.0));
    }

    /**
     * Maps job status to event status
     */
    private String mapJobStatusToEventStatus(String jobStatus) {
        if (jobStatus == null) {
            return StatusDetails.Status.PROCESSING;
        }

        return switch (jobStatus.toUpperCase()) {
            case "COMPLETED" -> StatusDetails.Status.COMPLETED;
            case "FAILED" -> StatusDetails.Status.FAILED;
            case "CANCELLED" -> StatusDetails.Status.CANCELLED;
            default -> StatusDetails.Status.PROCESSING;
        };
    }

    /**
     * Infers pipeline stage from progress percentage
     */
    private String inferPipelineStage(EnrichmentJob job) {
        if (job.getProgress() == null) {
            return "STARTED";
        }

        double progress = job.getProgress() * 100.0;

        if (progress < 10) return "STARTED";
        if (progress < 30) return "TRANSFORMED";
        if (progress < 50) return "VALIDATED";
        if (progress < 80) return "ENRICHED";
        if (progress < 100) return "RESPONSE_SENT";
        return "COMPLETED";
    }
}
