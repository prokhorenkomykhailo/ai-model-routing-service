package com.lucid.automation.airouting.consumer;

import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineFactory;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineResult;
import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import com.lucid.automation.airouting.pipeline.postprocessing.step.PipelineStep;
import com.lucid.automation.airouting.service.SlidingWindowService;
import com.lucid.automation.airouting.config.EnhancedErrorHandler;
import com.lucid.automation.airouting.config.KafkaRetryProperties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer service for post-processing AI responses from Kafka using pipeline architecture.
 *
 * SCRUM-345: Enhanced with exponential backoff retry strategy:
 * - Retryable errors (connection, timeout): NO ACK → Kafka retries with exponential backoff
 * - Non-retryable errors (validation, deserialization): ACK → sent to DLQ
 * - Success: ACK → message processed successfully
 *
 * This ensures every message in ai-enrich topic is processed successfully or logged in DLQ.
 *
 * @author vudu (SCRUM-345 enhancement)
 */
@Service
public class PostProcessingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PostProcessingConsumer.class);

    // Add post-processing counters
    private final AtomicLong totalPostProcessingRequests = new AtomicLong(0);
    private final AtomicLong successfulPostProcessing = new AtomicLong(0);
    private final AtomicLong failedPostProcessing = new AtomicLong(0);
    private final AtomicLong retriedPostProcessing = new AtomicLong(0);
    private final AtomicLong dlqPostProcessing = new AtomicLong(0);
    private final AtomicLong totalPostProcessingTime = new AtomicLong(0);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PostProcessingPipelineOrchestrator pipelineOrchestrator;
    private final PostProcessingPipelineFactory pipelineFactory;
    private final PipelineConfiguration pipelineConfiguration;
    private final SlidingWindowService slidingWindowService;
    private final EnhancedErrorHandler enhancedErrorHandler;
    private final KafkaRetryProperties kafkaRetryProperties;

    @Value("${kafka.topics.ai-responses:ai.responses.queue}")
    private String aiResponsesTopic;

    @Value("${kafka.topics.pre-ai-responses:pre.ai.responses.queue}")
    private String preAiResponsesTopic;

    public PostProcessingConsumer(
            KafkaTemplate<String, Object> kafkaTemplate,
            PostProcessingPipelineOrchestrator pipelineOrchestrator,
            PostProcessingPipelineFactory pipelineFactory,
            PipelineConfiguration pipelineConfiguration,
            SlidingWindowService slidingWindowService,
            EnhancedErrorHandler enhancedErrorHandler,
            KafkaRetryProperties kafkaRetryProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.pipelineFactory = pipelineFactory;
        this.pipelineConfiguration = pipelineConfiguration;
        this.slidingWindowService = slidingWindowService;
        this.enhancedErrorHandler = enhancedErrorHandler;
        this.kafkaRetryProperties = kafkaRetryProperties;

        logger.info("🚀 === POST-PROCESSING CONSUMER INITIALIZED (SCRUM-345 ENHANCED) ===");
        logger.info("🎯 PostProcessingConsumer initialized with pipeline architecture");
        logger.info("📤 Final output topic: {}", aiResponsesTopic);
        logger.info("⚙️ Pipeline orchestrator: {}", pipelineOrchestrator.getClass().getSimpleName());
        logger.info("✅ Pipeline enabled: {}", pipelineConfiguration.isEnabled());
        logger.info("🔄 Continue on failure: {}", pipelineConfiguration.isContinueOnFailure());
        logger.info("⏱️ Max execution time: {}ms", pipelineConfiguration.getMaxExecutionTimeMs());
        logger.info("🔄 Retry enabled: {}", kafkaRetryProperties.isEnabled());
        logger.info("� Max retries: {} | Initial backoff: {}ms | Multiplier: {} | Max backoff: {}ms",
            kafkaRetryProperties.getMaxAttempts(),
            kafkaRetryProperties.getInitialBackoffMs(),
            kafkaRetryProperties.getBackoffMultiplier(),
            kafkaRetryProperties.getMaxBackoffMs());
        logger.info("❌ DLQ enabled: {} | Retention: {} days",
            kafkaRetryProperties.isDlqEnabled(),
            kafkaRetryProperties.getDlqRetentionDays());
        logger.info("�🚀 ============================================================");
    }

    /**
     * Consume pre-AI responses for further processing using pipeline architecture.
     *
     * SCRUM-345: Implements intelligent retry/DLQ logic:
     * - Success: ACK message
     * - Retryable error: NO ACK → exponential backoff retry
     * - Non-retryable error: ACK + send to DLQ
     */
    @KafkaListener(topics = "${kafka.topics.pre-ai-responses:pre.ai.responses.queue}",
                   containerFactory = "genericObjectListenerContainerFactory")
    public void handlePreAiResponses(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment) {
        long postProcessingStartTime = System.currentTimeMillis();
        totalPostProcessingRequests.incrementAndGet();

        Object messageResponse = record.value();
        String topic = record.topic();

        logger.info("📋 [POST-PROCESSING] Received pre-AI response | Topic: {} | Partition: {} | Offset: {} | Key: {}",
            topic, record.partition(), record.offset(), record.key());
        logger.debug("🔍 [POST-PROCESSING-DEBUG] Method entry | Thread: {} | Acknowledgment: {}", 
            Thread.currentThread().getName(), acknowledgment != null ? "provided" : "NULL");

        // Extract the actual payload from ConsumerRecord if needed
        Object payload = messageResponse;
        if (messageResponse instanceof ConsumerRecord) {
            ConsumerRecord<?, ?> consumerRecord = (ConsumerRecord<?, ?>) messageResponse;
            payload = consumerRecord.value();
            logger.debug("🔍 Extracted payload from nested ConsumerRecord - Type: {}",
                    payload != null ? payload.getClass().getSimpleName() : "null");
        }

        PostProcessingContext context = null;
        try {
            // Validate payload
            if (payload == null) {
                handleNullPayload(record, acknowledgment, postProcessingStartTime);
                return;
            }

            if (!(payload instanceof Map<?, ?>)) {
                handleInvalidPayload(record, payload, acknowledgment, postProcessingStartTime);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> responseMap = (Map<String, Object>) payload;

            // Create processing context
            context = new PostProcessingContext(responseMap);

            // Execute the pipeline
            PostProcessingPipelineResult pipelineResult = executePipeline(context);

            // Handle pipeline result
            if (pipelineResult.isOverallSuccess()) {
                EnrichmentResponse enrichmentResponse = context.getEnrichmentResponse();
                if (enrichmentResponse != null) {
                    // Create lightweight version to avoid Kafka message size issues
                    EnrichmentResponse lightweightResponse = createLightweightResponse(enrichmentResponse);
                    sendToFinalAiResponsesTopic(lightweightResponse);

                    // ✅ CRITICAL: Mark original messages as processed ONLY after successful AI processing
                    // This ensures proper message lifecycle management and prevents duplicate processing
                    markOriginalMessagesAsProcessed(context);

                    successfulPostProcessing.incrementAndGet();
                    long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
                    totalPostProcessingTime.addAndGet(postProcessingTime);

                    logger.info("✅ [POST-PROCESSING-SUCCESS] Successfully processed pre-AI response in {}ms | Key: {} | 📊 Requests: {} | Success: {} | Retried: {} | DLQ: {}",
                        postProcessingTime, record.key(), totalPostProcessingRequests.get(),
                        successfulPostProcessing.get(), retriedPostProcessing.get(), dlqPostProcessing.get());

                    // ✅ ACK only after successful processing
                    try {
                        if (acknowledgment != null) {
                            logger.info("🔔 [ACK] About to acknowledge message | Offset: {} | Topic: {} | Partition: {}", 
                                record.offset(), record.topic(), record.partition());
                            acknowledgment.acknowledge();
                            logger.info("✅ [ACK-SUCCESS] Message acknowledged successfully | Offset: {} | Consumer should advance to offset {}", 
                                record.offset(), record.offset() + 1);
                        } else {
                            logger.error("❌ [ACK-NULL] Cannot acknowledge - Acknowledgment parameter is NULL | Offset: {}", 
                                record.offset());
                        }
                    } catch (Exception ackException) {
                        logger.error("❌ [ACK-ERROR] Failed to acknowledge message | Offset: {} | Error: {}", 
                            record.offset(), ackException.getMessage(), ackException);
                        throw ackException;
                    }
                    
                    logger.info("🏁 [POST-PROCESSING-COMPLETE] Method execution finished for offset {}", record.offset());
                } else {
                    handlePipelineNullResponse(context, record, acknowledgment, postProcessingStartTime);
                }
            } else {
                // Pipeline failed - check if error is retryable
                handlePipelineFailure(pipelineResult, context, record, acknowledgment, postProcessingStartTime);
            }

        } catch (Exception e) {
            // SCRUM-345: Check if exception is retryable
            boolean retryable = enhancedErrorHandler.isRetryableException(e);

            if (retryable) {
                // ❌ DO NOT ACK - let Kafka rebalance and retry with exponential backoff
                retriedPostProcessing.incrementAndGet();
                long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
                logger.error("🔄 [RETRY-SCHEDULED] Retryable error during post-processing after {}ms, NO ACK | Exception: {} | 📊 Requests: {} | Success: {} | Retried: {} | DLQ: {}",
                    postProcessingTime, e.getClass().getSimpleName(),
                    totalPostProcessingRequests.get(), successfulPostProcessing.get(),
                    retriedPostProcessing.get(), dlqPostProcessing.get(), e);
                // DO NOT call acknowledgment.acknowledge() - let it timeout and retry
            } else {
                // Non-retryable: ACK and send to DLQ
                dlqPostProcessing.incrementAndGet();
                long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
                logger.error("❌ [DLQ-DISPATCH] Non-retryable error during post-processing after {}ms | Exception: {} | 📊 Requests: {} | Success: {} | Retried: {} | DLQ: {}",
                    postProcessingTime, e.getClass().getSimpleName(),
                    totalPostProcessingRequests.get(), successfulPostProcessing.get(),
                    retriedPostProcessing.get(), dlqPostProcessing.get(), e);

                try {
                    // Send to DLQ if enabled
                    if (kafkaRetryProperties.isDlqEnabled()) {
                        sendToDLQ(record, e);
                    }
                    // ACK after sending to DLQ
                    if (acknowledgment != null) {
                        acknowledgment.acknowledge();
                    }
                } catch (Exception dlqError) {
                    logger.error("❌ [DLQ-ERROR] Failed to send to DLQ: {}", dlqError.getMessage());
                    // Still acknowledge to prevent infinite retry loop
                    if (acknowledgment != null) {
                        acknowledgment.acknowledge();
                    }
                }
            }
        }
    }

    /**
     * Handle null payload error
     */
    private void handleNullPayload(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment,
                                   long postProcessingStartTime) {
        failedPostProcessing.incrementAndGet();
        long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
        totalPostProcessingTime.addAndGet(postProcessingTime);
        logger.error("❌ [POST-PROCESSING-NULL] Received NULL payload from topic: {} | 📊 Requests: {} | Success: {} | Failed: {} | Retried: {} | DLQ: {} | Time: {}ms",
            record.topic(), totalPostProcessingRequests.get(), successfulPostProcessing.get(),
            failedPostProcessing.get(), retriedPostProcessing.get(), dlqPostProcessing.get(), postProcessingTime);

        // Null payload is non-retryable validation error
        dlqPostProcessing.incrementAndGet();
        try {
            if (kafkaRetryProperties.isDlqEnabled()) {
                sendToDLQ(record, new IllegalArgumentException("Null payload"));
            }
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] Failed to send null payload to DLQ: {}", e.getMessage());
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }

    /**
     * Handle invalid payload error
     */
    private void handleInvalidPayload(ConsumerRecord<String, Object> record,
                                      Object payload,
                                      Acknowledgment acknowledgment,
                                      long postProcessingStartTime) {
        failedPostProcessing.incrementAndGet();
        dlqPostProcessing.incrementAndGet();
        long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
        totalPostProcessingTime.addAndGet(postProcessingTime);
        logger.error("❌ [POST-PROCESSING-INVALID] Invalid pre-AI response format: expected Map, got {} | 📊 Requests: {} | Success: {} | Failed: {} | Retried: {} | DLQ: {} | Time: {}ms",
            payload.getClass().getSimpleName(), totalPostProcessingRequests.get(),
            successfulPostProcessing.get(), failedPostProcessing.get(),
            retriedPostProcessing.get(), dlqPostProcessing.get(), postProcessingTime);

        // Invalid format is non-retryable
        try {
            if (kafkaRetryProperties.isDlqEnabled()) {
                sendToDLQ(record, new IllegalArgumentException("Invalid payload format, expected Map"));
            }
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] Failed to send invalid payload to DLQ: {}", e.getMessage());
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }

    /**
     * Handle pipeline null response error
     */
    private void handlePipelineNullResponse(PostProcessingContext context,
                                            ConsumerRecord<String, Object> record,
                                            Acknowledgment acknowledgment,
                                            long postProcessingStartTime) {
        failedPostProcessing.incrementAndGet();
        dlqPostProcessing.incrementAndGet();
        long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;
        totalPostProcessingTime.addAndGet(postProcessingTime);
        logger.error("❌ [POST-PROCESSING-NULL-RESPONSE] Pipeline succeeded but enrichment response is null | 📊 Requests: {} | Success: {} | Failed: {} | Retried: {} | DLQ: {} | Time: {}ms",
            totalPostProcessingRequests.get(), successfulPostProcessing.get(),
            failedPostProcessing.get(), retriedPostProcessing.get(), dlqPostProcessing.get(), postProcessingTime);

        // Null response is non-retryable
        try {
            sendErrorResponse(context, "Pipeline succeeded but response is null");
            if (kafkaRetryProperties.isDlqEnabled()) {
                sendToDLQ(record, new RuntimeException("Pipeline null response"));
            }
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] Failed to send null response to DLQ: {}", e.getMessage());
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }

    /**
     * Handle pipeline failure with retry logic
     */
    private void handlePipelineFailure(PostProcessingPipelineResult pipelineResult,
                                       PostProcessingContext context,
                                       ConsumerRecord<String, Object> record,
                                       Acknowledgment acknowledgment,
                                       long postProcessingStartTime) {
        failedPostProcessing.incrementAndGet();
        long postProcessingTime = System.currentTimeMillis() - postProcessingStartTime;

        // Check if the pipeline failure is due to a retryable error
        Exception pipelineException = new RuntimeException("Pipeline failed: " + pipelineResult.getErrorMessage());
        boolean retryable = enhancedErrorHandler.isRetryableException(pipelineException);

        if (retryable) {
            // ❌ DO NOT ACK - let Kafka retry
            retriedPostProcessing.incrementAndGet();
            logger.error("🔄 [RETRY-SCHEDULED] Retryable pipeline failure after {}ms: {}, NO ACK | 📊 Requests: {} | Success: {} | Retried: {} | DLQ: {}",
                postProcessingTime, pipelineResult.getErrorMessage(),
                totalPostProcessingRequests.get(), successfulPostProcessing.get(),
                retriedPostProcessing.get(), dlqPostProcessing.get());
        } else {
            // Non-retryable: ACK and send to DLQ
            dlqPostProcessing.incrementAndGet();
            logger.error("❌ [DLQ-DISPATCH] Non-retryable pipeline failure after {}ms: {} | 📊 Requests: {} | Success: {} | Retried: {} | DLQ: {}",
                postProcessingTime, pipelineResult.getErrorMessage(),
                totalPostProcessingRequests.get(), successfulPostProcessing.get(),
                retriedPostProcessing.get(), dlqPostProcessing.get());

            try {
                sendErrorResponse(context, pipelineResult.getErrorMessage());
                if (kafkaRetryProperties.isDlqEnabled()) {
                    sendToDLQ(record, pipelineException);
                }
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
            } catch (Exception e) {
                logger.error("❌ [DLQ-ERROR] Failed to send pipeline failure to DLQ: {}", e.getMessage());
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
            }
        }
    }

    /**
     * Execute the processing pipeline
     */
    private PostProcessingPipelineResult executePipeline(PostProcessingContext context) {
        try {
            // Check if pipeline is enabled
            if (!pipelineConfiguration.isEnabled()) {
                logger.warn("⚠️ Pipeline is disabled, skipping processing");
                return PostProcessingPipelineResult.failure("Pipeline is disabled");
            }

            // Create the appropriate pipeline based on configuration
            List<PipelineStep> pipelineSteps = createPipelineSteps();

            // Execute the pipeline
            return pipelineOrchestrator.execute(context, pipelineSteps);

        } catch (Exception e) {
            logger.error("❌ Error creating or executing pipeline: {}", e.getMessage(), e);
            return PostProcessingPipelineResult.failure("Pipeline execution error: " + e.getMessage());
        }
    }

    /**
     * Create pipeline steps - simplified to only use standard pipeline
     */
    private List<PipelineStep> createPipelineSteps() {
        // Always use standard pipeline for simplicity
        return pipelineFactory.createStandardPipeline();
    }

    /**
     * Marks original messages as processed after successful AI processing.
     * Extracts message IDs from the context and uses SlidingWindowService to update their status.
     *
     * @param context PostProcessingContext containing original message IDs and tenant information
     */
    private void markOriginalMessagesAsProcessed(PostProcessingContext context) {
        try {
            // Extract original message IDs from the AI request context
            Map<String, Object> rawResponse = context.getRawResponseMap();
            if (rawResponse == null) {
                logger.warn("⚠️ [MESSAGE-PROCESSING] Cannot mark messages as processed: no raw response data");
                return;
            }

            // Check if the context contains the original message IDs
            // These should have been added by MessageEnrichmentScheduler when creating the AI request
            Object originalMessageIdsObj = rawResponse.get("originalMessageIds");
            if (originalMessageIdsObj == null) {
                // Try to get it from the nested context within the response
                Object contextObj = rawResponse.get("context");
                if (contextObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> contextMap = (Map<String, Object>) contextObj;
                    originalMessageIdsObj = contextMap.get("originalMessageIds");
                }
            }

            if (originalMessageIdsObj == null) {
                logger.warn("⚠️ [MESSAGE-PROCESSING] Cannot mark messages as processed: no original message IDs found in context or response");
                return;
            }

            @SuppressWarnings("unchecked")
            List<String> originalMessageIds = (List<String>) originalMessageIdsObj;

            if (originalMessageIds.isEmpty()) {
                logger.warn("⚠️ [MESSAGE-PROCESSING] Cannot mark messages as processed: empty original message IDs list");
                return;
            }

            String tenantId = context.getTenantId();
            String deemergeUserId = context.getDeemergeUserId();

            if (tenantId == null || deemergeUserId == null) {
                logger.warn("⚠️ [MESSAGE-PROCESSING] Cannot mark messages as processed: missing tenant ID or user ID");
                return;
            }

            logger.info("✅ [MESSAGE-PROCESSING] Marking {} original messages as processed after successful AI response | Tenant: {} | User: {}",
                       originalMessageIds.size(), tenantId, deemergeUserId);

            // Use SlidingWindowService to mark the original messages as processed
            slidingWindowService.markMessagesAsProcessedByIds(originalMessageIds, tenantId, deemergeUserId);

        } catch (Exception e) {
            logger.error("❌ [MESSAGE-PROCESSING] Failed to mark original messages as processed: {}", e.getMessage(), e);
            // Don't throw - this is a status update issue, not a critical pipeline failure
        }
    }

    /**
     * Send processed message to the final ai-responses topic synchronously with timeout
     */
    private void sendToFinalAiResponsesTopic(Object parsedAiResponse) {
        try {
            logger.info("📤 [FORWARD] Attempting to send enrichment response to topic: {}", aiResponsesTopic);

            // Send synchronously with 30-second timeout to prevent indefinite blocking
            kafkaTemplate.send(aiResponsesTopic, parsedAiResponse)
                .get(30, java.util.concurrent.TimeUnit.SECONDS);

            logger.info("✅ [FORWARD-SUCCESS] Successfully forwarded message to final ai-responses topic: {}", aiResponsesTopic);
        } catch (java.util.concurrent.TimeoutException e) {
            logger.error("⏱️ [FORWARD-TIMEOUT] Timeout after 30s sending to topic: {}", aiResponsesTopic, e);
            throw new RuntimeException("Timeout sending message to final ai-responses topic after 30s", e);
        } catch (Exception e) {
            logger.error("❌ [FORWARD-ERROR] Failed to send message to final ai-responses topic: {}, error type: {}, message: {}",
                        aiResponsesTopic, e.getClass().getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to final ai-responses topic", e);
        }
    }

    /**
     * Send error response to the final topic
     */
    private void sendErrorResponse(PostProcessingContext context, String errorMessage) {
        try {
            EnrichmentResponse errorResponse = createErrorResponse(context, errorMessage);
            sendToFinalAiResponsesTopic(errorResponse);
            logger.info("⚠️ Sent error response to final topic due to processing failure");
        } catch (Exception e) {
            logger.error("❌ Failed to send error response: {}", e.getMessage(), e);
        }
    }

    /**
     * Create an error EnrichmentResponse
     */
    private EnrichmentResponse createErrorResponse(PostProcessingContext context, String errorMessage) {
        EnrichmentResponse response = new EnrichmentResponse();

        // Set basic fields from context if available
        if (context != null) {
            response.setMessageId(context.getMessageId());
            response.setCorrelationId(context.getCorrelationId());
            response.setTaskType(context.getTaskType());
            response.setTenantId(context.getTenantId());
            response.setTenantSchema(context.getTenantSchema());
            response.setUserId(context.getUserId());
            response.setDeemergeUserId(context.getDeemergeUserId());
            response.setDeemergeUserName(context.getDeemergeUserName());
            response.setTeamId(context.getTeamId());
        }

        response.setSuccess(false);
        response.setStatus("error");
        response.setErrorMessage(errorMessage);
        response.setProcessedAt(List.of(
            LocalDateTime.now().getYear(),
            LocalDateTime.now().getMonthValue(),
            LocalDateTime.now().getDayOfMonth(),
            LocalDateTime.now().getHour(),
            LocalDateTime.now().getMinute(),
            LocalDateTime.now().getSecond()
        ));

        return response;
    }

    /**
     * Create a lightweight version of EnrichmentResponse to avoid Kafka message size issues.
     * This method removes or summarizes large data fields like full message and participant lists.
     */
    private EnrichmentResponse createLightweightResponse(EnrichmentResponse originalResponse) {
        if (originalResponse == null) {
            return null;
        }

        EnrichmentResponse lightweightResponse = new EnrichmentResponse();

        // Copy all basic fields (these are small)
        lightweightResponse.setMessageId(originalResponse.getMessageId());
        lightweightResponse.setCorrelationId(originalResponse.getCorrelationId());
        lightweightResponse.setConversationId(originalResponse.getConversationId());
        lightweightResponse.setTaskType(originalResponse.getTaskType());
        lightweightResponse.setSuccess(originalResponse.isSuccess());
        lightweightResponse.setStatus(originalResponse.getStatus());
        lightweightResponse.setProviderId(originalResponse.getProviderId());
        lightweightResponse.setErrorMessage(originalResponse.getErrorMessage());
        lightweightResponse.setConfidence(originalResponse.getConfidence());
        lightweightResponse.setProcessedAt(originalResponse.getProcessedAt());
        lightweightResponse.setProcessingTimeMs(originalResponse.getProcessingTimeMs());
        lightweightResponse.setTenantId(originalResponse.getTenantId());
        lightweightResponse.setTenantSchema(originalResponse.getTenantSchema());
        lightweightResponse.setUserId(originalResponse.getUserId());
        lightweightResponse.setDeemergeUserId(originalResponse.getDeemergeUserId());
        lightweightResponse.setDeemergeUserName(originalResponse.getDeemergeUserName());
        lightweightResponse.setTeamId(originalResponse.getTeamId());

        // Handle metadata - copy but limit size
        if (originalResponse.getMetadata() != null) {
            Map<String, Object> lightweightMetadata = new java.util.HashMap<>();
            originalResponse.getMetadata().forEach((key, value) -> {
                // Only include small metadata fields, skip large collections
                if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                    lightweightMetadata.put(key, value);
                } else if (value instanceof Map || value instanceof List) {
                    // Add summary info instead of full data
                    if (value instanceof List) {
                        lightweightMetadata.put(key + "_count", ((List<?>) value).size());
                    } else if (value instanceof Map) {
                        lightweightMetadata.put(key + "_keys", ((Map<?, ?>) value).keySet().size());
                    }
                }
            });
            lightweightResponse.setMetadata(lightweightMetadata);
        }

        // Handle ConversationEnrichment result - create summary version
        if (originalResponse.getResult() != null) {
            lightweightResponse.setResult(createLightweightConversationEnrichment(originalResponse.getResult()));
        }

        logger.debug("💡 Created lightweight response for messageId={}, optimized for Kafka",
                    originalResponse.getMessageId());

        return lightweightResponse;
    }

    /**
     * Create a lightweight version of ConversationEnrichment by removing large data and keeping only summaries
     */
    private com.lucid.automation.common.dto.enrichment.ConversationEnrichment createLightweightConversationEnrichment(
            com.lucid.automation.common.dto.enrichment.ConversationEnrichment original) {

        if (original == null) {
            return null;
        }

        // Create lightweight metadata with counts instead of full data
        Map<String, Object> lightweightMetadata = new java.util.HashMap<>();
        if (original.metadata() != null) {
            lightweightMetadata.putAll(original.metadata());
        }

        // Add summary counts instead of full data
        if (original.messages() != null) {
            lightweightMetadata.put("messages_count", original.messages().size());
        }
        if (original.participants() != null) {
            lightweightMetadata.put("participants_count", original.participants().size());
        }
        if (original.topics() != null) {
            lightweightMetadata.put("topics_count", original.topics().size());
        }

        // Return only topics and metadata, exclude large message/participant lists
        return new com.lucid.automation.common.dto.enrichment.ConversationEnrichment(
            original.topics(), // Keep topics as they're usually small
            List.of(), // Empty participants list
            List.of(), // Empty messages list
            lightweightMetadata
        );
    }

    /**
     * Send message to Dead Letter Queue for non-retryable errors.
     * SCRUM-345: DLQ topic = original topic + "-dlq" suffix
     *
     * @param record the ConsumerRecord that failed
     * @param error the exception that caused the failure
     */
    private void sendToDLQ(ConsumerRecord<String, Object> record, Exception error) {
        try {
            String dlqTopic = record.topic() + kafkaRetryProperties.getDlqTopicSuffix();

            // Create DLQ message with error context
            Map<String, Object> dlqMessage = new java.util.HashMap<>();
            dlqMessage.put("originalTopic", record.topic());
            dlqMessage.put("originalPartition", record.partition());
            dlqMessage.put("originalOffset", record.offset());
            dlqMessage.put("originalKey", record.key());
            dlqMessage.put("originalValue", record.value());
            dlqMessage.put("errorType", error.getClass().getSimpleName());
            dlqMessage.put("errorMessage", error.getMessage());
            dlqMessage.put("failedAt", LocalDateTime.now());
            dlqMessage.put("retryAttempts", 0); // Will be incremented by DLQ consumer

            kafkaTemplate.send(dlqTopic, dlqMessage).get();
            logger.info("📤 [DLQ] Successfully sent message to DLQ topic: {} (from: {})", dlqTopic, record.topic());
        } catch (Exception dlqError) {
            logger.error("❌ [DLQ-SEND-ERROR] Failed to send message to DLQ: {}", dlqError.getMessage(), dlqError);
            throw new RuntimeException("Failed to send message to DLQ", dlqError);
        }
    }
}
