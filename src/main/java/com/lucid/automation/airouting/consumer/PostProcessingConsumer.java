package com.lucid.automation.airouting.consumer;

import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.airouting.pipeline.PostProcessingPipelineFactory;
import com.lucid.automation.airouting.pipeline.PostProcessingPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.PostProcessingPipelineResult;
import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import com.lucid.automation.airouting.pipeline.context.PostProcessingContext;
import com.lucid.automation.airouting.pipeline.step.PipelineStep;

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
        
        logger.info("=== POST-PROCESSING CONSUMER INITIALIZED ===");
        logger.info("PostProcessingConsumer initialized with pipeline architecture");
        logger.info("Final output topic: {}", aiResponsesTopic);
        logger.info("Pipeline orchestrator: {}", pipelineOrchestrator.getClass().getSimpleName());
        logger.info("Pipeline enabled: {}", pipelineConfiguration.isEnabled());
        logger.info("Pipeline type: {}", pipelineConfiguration.getType());
        logger.info("Continue on failure: {}", pipelineConfiguration.isContinueOnFailure());
        logger.info("Max execution time: {}ms", pipelineConfiguration.getMaxExecutionTimeMs());
        logger.info("============================================");
    }
    
    /**
     * Consume pre-AI responses for further processing using pipeline architecture
     */
    @KafkaListener(topics = "${kafka.topics.pre-ai-responses:pre.ai.responses.queue}", 
                   containerFactory = "genericObjectListenerContainerFactory")
    public void handlePreAiResponses(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment) {
        Object messageResponse = record.value();
        String topic = record.topic();
        
        if (messageResponse == null) {
            logger.error("=== POST-PROCESSING-ERROR === Received NULL message from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }

        PostProcessingContext context = null;
        try {
            // Validate that the message is a Map
            if (!(messageResponse instanceof Map<?, ?>)) {
                logger.error("Invalid pre-AI response format: expected Map, got {}", 
                           messageResponse.getClass().getSimpleName());
                acknowledgment.acknowledge();
                return;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> responseMap = (Map<String, Object>) messageResponse;
            
            // Create processing context
            context = new PostProcessingContext(responseMap);
            
            // Execute the pipeline
            PostProcessingPipelineResult pipelineResult = executePipeline(context);
            
            // Handle pipeline result
            if (pipelineResult.isOverallSuccess()) {
                EnrichmentResponse enrichmentResponse = context.getEnrichmentResponse();
                if (enrichmentResponse != null) {
                    sendToFinalAiResponsesTopic(enrichmentResponse);
                    logger.info("Successfully processed pre-AI response using pipeline and forwarded to final topic");
                } else {
                    logger.error("Pipeline succeeded but enrichment response is null");
                }
            } else {
                logger.error("Pipeline execution failed: {}", pipelineResult.getErrorMessage());
                // Send error response if needed
                sendErrorResponse(context, pipelineResult.getErrorMessage());
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            logger.error("Failed to process pre-AI response from topic: {}, error: {}", topic, e.getMessage(), e);
            
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
                logger.warn("Pipeline is disabled, skipping processing");
                return PostProcessingPipelineResult.failure("Pipeline is disabled");
            }
            
            // Create the appropriate pipeline based on configuration
            List<PipelineStep> pipelineSteps = createPipelineSteps();
            
            // Execute the pipeline
            return pipelineOrchestrator.execute(context, pipelineSteps);
            
        } catch (Exception e) {
            logger.error("Error creating or executing pipeline: {}", e.getMessage(), e);
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
            logger.info("Successfully forwarded message to final ai-responses topic: {}", aiResponsesTopic);
        } catch (Exception e) {
            logger.error("Failed to send message to final ai-responses topic: {}, error: {}", 
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
            logger.info("Sent error response to final topic due to processing failure");
        } catch (Exception e) {
            logger.error("Failed to send error response: {}", e.getMessage(), e);
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
}
