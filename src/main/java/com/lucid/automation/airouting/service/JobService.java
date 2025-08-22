package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.ProgressJobStatusDTO;
import com.lucid.automation.airouting.config.KafkaTopicProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for publishing job progress updates to Kafka for AI routing operations
 * This service publishes ProgressJobStatusDTO messages to the ingestion-progress topic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicProperties kafkaTopicProperties;

    /**
     * Publishes a job progress update to the ingestion-progress Kafka topic
     *
     * @param jobId The unique job identifier
     * @param parentId The parent job identifier (for hierarchical job tracking)
     * @param type The job type ("INGESTION" or "CREATION")
     * @param tenantId The tenant identifier
     * @param tenantSchema The tenant database schema
     * @param stage The current stage of the job (e.g., "starting", "processing", "topic_creation", "completed")
     * @param percent Progress percentage (0-100)
     * @param timeLeftEta Estimated time left in seconds (optional)
     * @param topics List of topics being processed (optional)
     */
    public void publishJobProgress(String jobId, String parentId, String type, String tenantId, String tenantSchema, String stage,
                                  Integer percent, Integer timeLeftEta, List<ProgressJobStatusDTO.TopicProgress> topics) {
        try {
            ProgressJobStatusDTO progressUpdate = ProgressJobStatusDTO.builder()
                    .jobId(jobId)
                    .parentId(parentId)
                    .type(type)
                    .tenantId(tenantId)
                    .tenantSchema(tenantSchema)
                    .stage(stage)
                    .percent(percent)
                    .topics(topics)
                    .timestamp(Instant.now().toString())
                    .timeLeftEta(timeLeftEta)
                    .build();

            publishJobProgress(progressUpdate);

        } catch (Exception e) {
            log.error("Failed to create and publish job progress for jobId={}, type={}, stage={}: {}",
                     jobId, type, stage, e.getMessage(), e);
        }
    }

    /**
     * Publishes a job progress update to the ingestion-progress Kafka topic
     *
     * @param progressUpdate The progress update DTO to publish
     */
    public void publishJobProgress(ProgressJobStatusDTO progressUpdate) {
        try {
            String topic = kafkaTopicProperties.getIngestionProgress();

            // Use tenant_id as the partition key to ensure tenant-level ordering
            String key = progressUpdate.getTenantId();

            log.info("Publishing job progress for jobId={}, stage={}, percent={}% to topic={} with key={}",
                    progressUpdate.getJobId(), progressUpdate.getStage(), progressUpdate.getPercent(), topic, key);

            // Send message asynchronously
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, progressUpdate);

            // Add callback for success/failure handling
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Successfully published job progress for jobId={} to partition {} at offset {}",
                            progressUpdate.getJobId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish job progress for jobId={}: {}",
                             progressUpdate.getJobId(), ex.getMessage(), ex);
                }
            });

        } catch (Exception e) {
            log.error("Failed to publish job progress for jobId={}: {}",
                     progressUpdate.getJobId(), e.getMessage(), e);
        }
    }

    /**
     * Convenience method to publish job start event
     */
    public void publishJobStarted(String jobId, String parentId, String tenantId, String tenantSchema) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "starting", 0, null, null);
    }

    /**
     * Convenience method to publish job completion event
     */
    public void publishJobCompleted(String jobId, String parentId, String tenantId, String tenantSchema) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "completed", 100, null, null);
    }

    /**
     * Convenience method to publish job failed event
     */
    public void publishJobFailed(String jobId, String parentId, String tenantId, String tenantSchema, String errorMessage) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "failed", -1, null, null);
    }

    /**
     * Convenience method to publish AI processing progress
     */
    public void publishProcessingProgress(String jobId, String parentId, String tenantId, String tenantSchema,
                                         Integer percent, Integer timeLeftEta) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "processing", percent, timeLeftEta, null);
    }

    /**
     * Convenience method to publish topic creation progress with topic details
     */
    public void publishTopicCreationProgress(String jobId, String parentId, String tenantId, String tenantSchema,
                                           Integer percent, List<ProgressJobStatusDTO.TopicProgress> topics) {
        publishJobProgress(jobId, parentId, "CREATION", tenantId, tenantSchema, "topic_creation", percent, null, topics);
    }

    /**
     * Convenience method to publish enrichment progress
     */
    public void publishEnrichmentProgress(String jobId, String parentId, String tenantId, String tenantSchema,
                                         Integer percent, Integer timeLeftEta) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "enriching", percent, timeLeftEta, null);
    }

    /**
     * Convenience method to publish categorization progress
     */
    public void publishCategorizationProgress(String jobId, String parentId, String tenantId, String tenantSchema,
                                            Integer percent, Integer timeLeftEta) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "categorizing", percent, timeLeftEta, null);
    }

    /**
     * Convenience method to publish summarization progress
     */
    public void publishSummarizationProgress(String jobId, String parentId, String tenantId, String tenantSchema,
                                           Integer percent, Integer timeLeftEta) {
        publishJobProgress(jobId, parentId, "INGESTION", tenantId, tenantSchema, "summarizing", percent, timeLeftEta, null);
    }
}
