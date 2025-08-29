package com.lucid.automation.airouting.consumer;

import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineFactory;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingPipelineResult;
import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import com.lucid.automation.airouting.pipeline.postprocessing.step.PipelineStep;

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

/**
 * Consumer service for post-processing AI responses from Kafka using pipeline architecture.
 * This replaces the monolithic PostProcessingConsumer with a modular, pipeline-based approach.
 *
 * @author AI Assistant
 */
@Service
public class PostProcessingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PostProcessingConsumer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PostProcessingPipelineOrchestrator pipelineOrchestrator;
    private final PostProcessingPipelineFactory pipelineFactory;
    private final PipelineConfiguration pipelineConfiguration;

    @Value("${kafka.topics.ai-responses:ai.responses.queue}")
    private String aiResponsesTopic;

    public PostProcessingConsumer(
            KafkaTemplate<String, Object> kafkaTemplate,
            PostProcessingPipelineOrchestrator pipelineOrchestrator,
            PostProcessingPipelineFactory pipelineFactory,
            PipelineConfiguration pipelineConfiguration) {
        this.kafkaTemplate = kafkaTemplate;
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.pipelineFactory = pipelineFactory;
        this.pipelineConfiguration = pipelineConfiguration;

        logger.info("🚀 === POST-PROCESSING CONSUMER INITIALIZED ===");
        logger.info("🎯 PostProcessingConsumer initialized with pipeline architecture");
        logger.info("📤 Final output topic: {}", aiResponsesTopic);
        logger.info("⚙️ Pipeline orchestrator: {}", pipelineOrchestrator.getClass().getSimpleName());
        logger.info("✅ Pipeline enabled: {}", pipelineConfiguration.isEnabled());
        logger.info("🔄 Continue on failure: {}", pipelineConfiguration.isContinueOnFailure());
        logger.info("⏱️ Max execution time: {}ms", pipelineConfiguration.getMaxExecutionTimeMs());
        logger.info("🚀 ============================================");
    }

    /**
     * Consume pre-AI responses for further processing using pipeline architecture
     * Handles both direct payload and ConsumerRecord objects
     */
    @KafkaListener(topics = "${kafka.topics.pre-ai-responses:pre.ai.responses.queue}",
                   containerFactory = "genericObjectListenerContainerFactory")
    public void handlePreAiResponses(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment) {
        Object messageResponse = record.value();
        String topic = record.topic();

        logger.info("🎯 POST-PROCESSING-CONSUMER: Received message from topic: {}, partition: {}, offset: {}, key: {}",
                topic, record.partition(), record.offset(), record.key());

        // Extract the actual payload from ConsumerRecord if needed
        Object payload = messageResponse;
        if (messageResponse instanceof ConsumerRecord) {
            ConsumerRecord<?, ?> consumerRecord = (ConsumerRecord<?, ?>) messageResponse;
            payload = consumerRecord.value();
            logger.info("🔍 CONSUMER_RECORD_DEBUG: Extracted payload from ConsumerRecord - Type: {}, Topic: {}, Partition: {}, Offset: {}, Key: {}",
                    payload != null ? payload.getClass().getName() : "null",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key());

            if (payload != null) {
                logger.info("🔍 PAYLOAD_DEBUG: Payload content: {}",
                        payload.toString().length() > 300 ? payload.toString().substring(0, 300) + "... (truncated)" : payload.toString());
            }
        }

        if (payload == null) {
            logger.error("❌ POST-PROCESSING-ERROR: Received NULL payload from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }

        PostProcessingContext context = null;
        try {
            // Validate that the payload is a Map
            if (!(payload instanceof Map<?, ?>)) {
                logger.error("❌ Invalid pre-AI response format: expected Map, got {}",
                           payload.getClass().getSimpleName());
                acknowledgment.acknowledge();
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
                    logger.info("🎯 Successfully processed pre-AI response using pipeline and forwarded to final topic");
                } else {
                    logger.error("❌ Pipeline succeeded but enrichment response is null");
                }
            } else {
                logger.error("❌ Pipeline execution failed: {}", pipelineResult.getErrorMessage());
                // Send error response if needed
                sendErrorResponse(context, pipelineResult.getErrorMessage());
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("❌ Failed to process pre-AI response from topic: {}, error: {}", topic, e.getMessage(), e);

            // Send error response if context is available
            if (context != null) {
                sendErrorResponse(context, "Unexpected error: " + e.getMessage());
            }

            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
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
     * Send processed message to the final ai-responses topic
     */
    private void sendToFinalAiResponsesTopic(Object parsedAiResponse) {
        try {
            kafkaTemplate.send(aiResponsesTopic, parsedAiResponse);
            logger.info("✅ Successfully forwarded message to final ai-responses topic: {}", aiResponsesTopic);
        } catch (Exception e) {
            logger.error("❌ Failed to send message to final ai-responses topic: {}, error: {}",
                        aiResponsesTopic, e.getMessage(), e);
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

        logger.debug("💡 Created lightweight response for messageId={}, removing large data collections",
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
}
