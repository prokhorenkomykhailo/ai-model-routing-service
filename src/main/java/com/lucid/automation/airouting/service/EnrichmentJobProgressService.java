package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.airouting.repository.EnrichmentJobRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

/**
 * Service for tracking and updating enrichment job progress through pipeline stages.
 */
@Service
@RequiredArgsConstructor
public class EnrichmentJobProgressService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentJobProgressService.class);

    private final EnrichmentJobRepository enrichmentJobRepository;
    private final JobService jobService;

    /**
     * Pipeline stage progress percentages
     */
    public enum PipelineStage {
        STARTED(0.0),
        TRANSFORMED(20.0),
        VALIDATED(40.0),
        ENRICHED(70.0),
        RESPONSE_SENT(90.0),
        COMPLETED(100.0),
        FAILED(-1.0);

        private final double progressPercentage;

        PipelineStage(double progressPercentage) {
            this.progressPercentage = progressPercentage;
        }

        public double getProgressPercentage() {
            return progressPercentage;
        }
    }

    /**
     * Updates the progress of an enrichment job by jobId.
     *
     * @param jobId The job ID to update
     * @param stage The current pipeline stage
     * @param message Optional descriptive message about the current stage
     */
    public void updateProgress(String jobId, PipelineStage stage, String message) {
        if (jobId == null || jobId.trim().isEmpty()) {
            logger.warn("Cannot update progress: jobId is null or empty");
            return;
        }

        try {
            Optional<EnrichmentJob> jobOptional = enrichmentJobRepository.findById(jobId);

            if (jobOptional.isPresent()) {
                EnrichmentJob job = jobOptional.get();

                // Update progress fields
                double progressPercentage = stage.getProgressPercentage();
                job.setProgress(progressPercentage / 100.0); // Convert to 0.0-1.0 range
                job.setUpdatedAt(Instant.now().toEpochMilli());

                // Calculate estimated completion time and time left
                if (job.getCreatedAt() != null && progressPercentage > 0 && progressPercentage < 100) {
                    long elapsedMs = job.getUpdatedAt() - job.getCreatedAt();
                    long estimatedTotalMs = (long) (elapsedMs / (progressPercentage / 100.0));
                    long estimatedTimeLeftMs = estimatedTotalMs - elapsedMs;

                    job.setEstimatedTimeLeft(Math.max(0, estimatedTimeLeftMs));
                    job.setEstimatedCompletionTime(
                        Instant.ofEpochMilli(job.getUpdatedAt() + estimatedTimeLeftMs).toString()
                    );
                }

                // Update status based on stage
                switch (stage) {
                    case STARTED -> job.setStatus("PROCESSING");
                    case FAILED -> job.setStatus("FAILED");
                    case COMPLETED -> {
                        job.setStatus("COMPLETED");
                        job.setEndTime(Instant.now().toString());
                        job.setDurationMs(job.getUpdatedAt() - job.getCreatedAt());
                        job.setEstimatedTimeLeft(0L);
                    }
                    default -> job.setStatus("PROCESSING");
                }

                // Save the updated job
                enrichmentJobRepository.save(job);

                // Publish progress to Kafka
                publishProgressToKafka(job, stage);

                logger.debug("Updated job progress: jobId={}, stage={}, progress={}%, status={}, estimatedTimeLeft={}ms",
                           jobId, stage.name(), stage.getProgressPercentage(), job.getStatus(), job.getEstimatedTimeLeft());

                if (message != null && !message.trim().isEmpty()) {
                    logger.debug("Progress message for jobId={}: {}", jobId, message);
                }

            } else {
                logger.warn("Job not found for progress update: jobId={}", jobId);
            }

        } catch (Exception e) {
            logger.error("Failed to update job progress: jobId={}, stage={}, error={}",
                        jobId, stage.name(), e.getMessage(), e);
        }
    }

    /**
     * Marks a job as failed with error details.
     *
     * @param jobId The job ID to mark as failed
     * @param errorMessage The error message to record
     */
    public void markJobFailed(String jobId, String errorMessage) {
        if (jobId == null || jobId.trim().isEmpty()) {
            logger.warn("Cannot mark job as failed: jobId is null or empty");
            return;
        }

        try {
            Optional<EnrichmentJob> jobOptional = enrichmentJobRepository.findById(jobId);

            if (jobOptional.isPresent()) {
                EnrichmentJob job = jobOptional.get();

                job.setStatus("FAILED");
                job.setProgress(-1.0); // Indicate failure
                job.setUpdatedAt(Instant.now().toEpochMilli());
                job.setEndTime(Instant.now().toString());
                job.setResult(errorMessage);
                job.setDurationMs(job.getUpdatedAt() - job.getCreatedAt());

                enrichmentJobRepository.save(job);

                // Publish failure to Kafka
                if (job.getTenantId() != null && job.getTenantSchema() != null) {
                    jobService.publishJobFailed(job.getId(), job.getParentId(), job.getTenantId(), job.getTenantSchema(), errorMessage);
                }

                logger.warn("Marked job as failed: jobId={}, error={}", jobId, errorMessage);

            } else {
                logger.warn("Job not found for failure marking: jobId={}", jobId);
            }

        } catch (Exception e) {
            logger.error("Failed to mark job as failed: jobId={}, error={}",
                        jobId, e.getMessage(), e);
        }
    }

    /**
     * Initializes or creates a job for tracking if it doesn't exist.
     *
     * @deprecated Use initializeJob(String, String, String, String, String, String) instead for proper parent tracking
     * @param jobId The job ID
     * @param userId The user ID
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @param taskType The task type being processed
     */
    @Deprecated
    public void initializeJob(String jobId, String userId, String tenantId, String tenantSchema, String taskType) {
        // Call the new method with null parentId for backward compatibility
        initializeJob(jobId, null, userId, tenantId, tenantSchema, taskType);
    }

    /**
     * Initializes or creates a job for tracking if it doesn't exist.
     *
     * @param jobId The job ID
     * @param parentId The parent job ID (for hierarchical job relationships)
     * @param userId The user ID
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @param taskType The task type being processed
     */
    public void initializeJob(String jobId, String parentId, String userId, String tenantId, String tenantSchema, String taskType) {
        if (jobId == null || jobId.trim().isEmpty()) {
            logger.warn("Cannot initialize job: jobId is null or empty");
            return;
        }

        try {
            Optional<EnrichmentJob> existingJob = enrichmentJobRepository.findById(jobId);

            if (existingJob.isEmpty()) {
                EnrichmentJob newJob = EnrichmentJob.builder()
                        .id(jobId)
                        .parentId(parentId) // Set parentId for hierarchical job tracking
                        .status("PROCESSING")
                        .type(taskType != null ? taskType : "UNKNOWN")
                        .progress(0.0)
                        .createdAt(Instant.now().toEpochMilli())
                        .updatedAt(Instant.now().toEpochMilli())
                        .startTime(Instant.now().toString())
                        .userId(userId)
                        .tenantId(tenantId)
                        .tenantSchema(tenantSchema)
                        .build();

                enrichmentJobRepository.save(newJob);

                logger.debug("Initialized new enrichment job: jobId={}, parentId={}, type={}, userId={}, tenantId={}",
                           jobId, parentId, taskType, userId, tenantId);
            }

        } catch (Exception e) {
            logger.error("Failed to initialize job: jobId={}, parentId={}, error={}", jobId, parentId, e.getMessage(), e);
        }
    }

    /**
     * Publishes job progress to Kafka using the JobService
     */
    private void publishProgressToKafka(EnrichmentJob job, PipelineStage stage) {
        try {
            if (job.getTenantId() == null || job.getTenantSchema() == null) {
                logger.warn("Cannot publish progress to Kafka: job {} missing tenantId or tenantSchema", job.getId());
                return;
            }

            // Calculate estimated time left in seconds
            Integer timeLeftEta = null;
            if (job.getEstimatedTimeLeft() != null && job.getEstimatedTimeLeft() > 0) {
                timeLeftEta = Math.max(0, (int) (job.getEstimatedTimeLeft() / 1000));
            }

            // Map pipeline stage to progress stage
            String progressStage = mapPipelineStageToProgressStage(stage);

            // Convert progress to percentage
            int percent = (int) Math.max(0, stage.getProgressPercentage());

            // Ensure parentId is not null - use jobId as parentId if no parent exists (makes it a parent job)
            String parentId = job.getParentId() != null ? job.getParentId() : job.getId();

            // Publish to Kafka
            jobService.publishJobProgress(
                job.getId(),
                parentId, // parentId - never null, uses jobId if no parent exists
                "CREATION", // AI enrichment jobs are CREATION type for AI-generated content
                job.getTenantId(),
                job.getTenantSchema(),
                progressStage,
                percent,
                timeLeftEta,
                null // topics will be added when implementing topic creation progress
            );

        } catch (Exception e) {
            logger.warn("Failed to publish job progress to Kafka for jobId={}: {}", job.getId(), e.getMessage());
        }
    }

    /**
     * Maps pipeline stages to progress reporting stages
     */
    private String mapPipelineStageToProgressStage(PipelineStage stage) {
        return switch (stage) {
            case STARTED -> "starting";
            case TRANSFORMED -> "processing";
            case VALIDATED -> "processing";
            case ENRICHED -> "enriching";
            case RESPONSE_SENT -> "finalizing";
            case COMPLETED -> "completed";
            case FAILED -> "failed";
        };
    }
}
