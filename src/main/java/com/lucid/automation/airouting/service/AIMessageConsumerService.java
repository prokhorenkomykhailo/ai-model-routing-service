package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RabbitMQConfig;
import com.lucid.automation.airouting.model.AIResponse;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.model.message.AIMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * Service that listens to RabbitMQ messages and routes them to appropriate AI operations
 */
@Service
public class AIMessageConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessageConsumerService.class);
    
    private final AIRoutingService aiRoutingService;
    private final MessageConverterService messageConverter;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQConfig rabbitMQConfig;
    
    public AIMessageConsumerService(AIRoutingService aiRoutingService,
                                  MessageConverterService messageConverter,
                                  RabbitTemplate rabbitTemplate,
                                  RabbitMQConfig rabbitMQConfig) {
        this.aiRoutingService = aiRoutingService;
        this.messageConverter = messageConverter;
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitMQConfig = rabbitMQConfig;
    }
    
    /**
     * Listen for categorization requests
     */
    @RabbitListener(
        queues = "#{rabbitMQConfig.getAiCategorizeQueue()}",
        concurrency = "1",
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void handleCategorizationRequest(AIMessage message) {
        logger.info("Received categorization request: messageId={}, tenantId={}", 
                   message.getMessageId(), message.getTenantId());
        
        if (message.getTaskType() != AITaskType.CATEGORIZE) {
            logger.warn("Invalid task type for categorization queue: {}", message.getTaskType());
            sendErrorResponse(message, "Invalid task type for categorization queue");
            return;
        }
        
        processAIRequest(message);
    }
    
    /**
     * Listen for summarization requests
     */
    @RabbitListener(
        queues = "#{rabbitMQConfig.getAiSummarizeQueue()}",
        concurrency = "1", 
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void handleSummarizationRequest(AIMessage message) {
        logger.info("Received summarization request: messageId={}, tenantId={}", 
                   message.getMessageId(), message.getTenantId());
        
        if (message.getTaskType() != AITaskType.SUMMARIZE) {
            logger.warn("Invalid task type for summarization queue: {}", message.getTaskType());
            sendErrorResponse(message, "Invalid task type for summarization queue");
            return;
        }
        
        processAIRequest(message);
    }
    
    /**
     * Listen for enrichment requests (conversation, message, participant analysis, etc.)
     */
    @RabbitListener(
        queues = "#{rabbitMQConfig.getAiEnrichQueue()}",
        concurrency = "1",
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void handleEnrichmentRequest(AIMessage message) {
        logger.info("Received enrichment request: messageId={}, taskType={}, tenantId={}", 
                   message.getMessageId(), message.getTaskType(), message.getTenantId());
        
        // Validate that it's an enrichment-related task
        if (!isEnrichmentTask(message.getTaskType())) {
            logger.warn("Invalid task type for enrichment queue: {}", message.getTaskType());
            sendErrorResponse(message, "Invalid task type for enrichment queue");
            return;
        }
        
        processAIRequest(message);
    }
    
    private void processAIRequest(AIMessage aiMessage) {
        long startTime = System.currentTimeMillis();
        
        try {
            var aiRequest = messageConverter.convertToAIRequest(aiMessage);
            AIResponse response = aiRoutingService.processRequest(aiRequest);
            
            handleSuccessfulResponse(aiMessage, response, startTime);
            
        } catch (Exception e) {
            logger.error("Failed to convert or process AI message: messageId={}, error={}", 
                        aiMessage.getMessageId(), e.getMessage(), e);
            handleFailedResponse(aiMessage, e, startTime);
        }
    }

    private void handleSuccessfulResponse(AIMessage aiMessage, AIResponse response, long startTime) {
        long processingTime = System.currentTimeMillis() - startTime;
        
        AIMessageResponse messageResponse = response.isSuccess() 
            ? createSuccessResponse(aiMessage, response)
            : createErrorResponse(aiMessage, response);
            
        messageResponse.setProcessingTimeMs(processingTime);
        messageResponse.setMetadata(response.getMetadata());
        
        sendResponse(messageResponse, aiMessage.getReplyTopic());
        
        logger.info("Successfully processed AI request: messageId={}, taskType={}, time={}ms", 
                   aiMessage.getMessageId(), aiMessage.getTaskType(), processingTime);
    }

    private void handleFailedResponse(AIMessage aiMessage, Exception exception, long startTime) {
        long processingTime = System.currentTimeMillis() - startTime;
        
        logger.error("Failed to process AI request: messageId={}, taskType={}, error={}", 
                   aiMessage.getMessageId(), aiMessage.getTaskType(), exception.getMessage(), exception);
        
        AIMessageResponse errorResponse = AIMessageResponse.error(
            aiMessage.getMessageId(),
            aiMessage.getCorrelationId(),
            aiMessage.getConversationId(),
            aiMessage.getTaskType(),
            "Processing failed: " + exception.getMessage()
        );
        errorResponse.setProcessingTimeMs(processingTime);
        
        sendResponse(errorResponse, aiMessage.getReplyTopic());
    }

    private AIMessageResponse createSuccessResponse(AIMessage aiMessage, AIResponse response) {
        return AIMessageResponse.success(
            aiMessage.getMessageId(),
            aiMessage.getCorrelationId(),
            aiMessage.getConversationId(),
            response.getTaskType(),
            response.getResult(),
            response.getProviderId(),
            response.getConfidence()
        );
    }

    private AIMessageResponse createErrorResponse(AIMessage aiMessage, AIResponse response) {
        return AIMessageResponse.error(
            aiMessage.getMessageId(),
            aiMessage.getCorrelationId(),
            aiMessage.getConversationId(),
            response.getTaskType(),
            response.getErrorMessage()
        );
    }
    
    private void sendErrorResponse(AIMessage aiMessage, String errorMessage) {
        AIMessageResponse errorResponse = AIMessageResponse.error(
            aiMessage.getMessageId(),
            aiMessage.getCorrelationId(),
            aiMessage.getConversationId(),
            aiMessage.getTaskType(),
            errorMessage
        );
        sendResponse(errorResponse, aiMessage.getReplyTopic());
    }
    
    private void sendResponse(AIMessageResponse response, String replyTopic) {
        try {
            String targetTopic = replyTopic != null ? replyTopic : rabbitMQConfig.getResponsesRoutingKey();
            
            rabbitTemplate.convertAndSend(
                rabbitMQConfig.getAiResponsesExchange(),
                targetTopic,
                response
            );
            
            logger.debug("Sent response: messageId={}, success={}, topic={}", 
                        response.getMessageId(), response.isSuccess(), targetTopic);
            
        } catch (Exception e) {
            logger.error("Failed to send response: messageId={}, error={}", 
                        response.getMessageId(), e.getMessage(), e);
        }
    }
    
    private boolean isEnrichmentTask(AITaskType taskType) {
        return taskType == AITaskType.ENRICH_CONVERSATION ||
               taskType == AITaskType.ENRICH_MESSAGE ||
               taskType == AITaskType.ANALYZE_PARTICIPANT ||
               taskType == AITaskType.ASSESS_URGENCY ||
               taskType == AITaskType.GENERATE_TOPIC ||
               taskType == AITaskType.EXTRACT_ENTITIES ||
               taskType == AITaskType.SENTIMENT_ANALYSIS;
    }
}
