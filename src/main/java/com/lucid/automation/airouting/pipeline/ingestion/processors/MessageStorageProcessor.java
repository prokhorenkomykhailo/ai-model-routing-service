package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.pipeline.context.MessageProcessingContext;
import com.lucid.automation.airouting.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stores ingestion messages to Redis.
 * This is a critical processor - if it fails, the pipeline should stop.
 * 
 * @author AI Assistant
 */
@Component
public class MessageStorageProcessor implements MessageProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageStorageProcessor.class);
    
    private final MessageService messageService;
    
    public MessageStorageProcessor(MessageService messageService) {
        this.messageService = messageService;
    }
    
    @Override
    public ProcessingResult process(MessageProcessingContext context) {
        var ingestionEvent = context.getIngestionEvent();
        
        logger.debug("Storing message for messageId: {}, tenantId: {}", 
            ingestionEvent.getMessage().getTs(), ingestionEvent.getTenantId());
        
        try {
            // Call the message service to store the message
            // Note: We need to check if it's storeMessage or saveMessage
            boolean stored = messageService.storeMessage(ingestionEvent);
            
            if (!stored) {
                String errorMsg = "Failed to store message to Redis for messageId: " + ingestionEvent.getMessage().getTs();
                logger.error(errorMsg);
                return ProcessingResult.failure(getProcessorName(), errorMsg);
            }
            
            logger.debug("Message stored successfully for messageId: {}", ingestionEvent.getMessage().getTs());
            
            // Store the processing result in context for other processors
            context.setProcessingData("messageStored", true);
            context.setProcessingData("messageId", ingestionEvent.getMessage().getTs());
            
            return ProcessingResult.success(getProcessorName());
            
        } catch (Exception e) {
            String errorMsg = "Exception while storing message: " + e.getMessage();
            logger.error(errorMsg, e);
            return ProcessingResult.failure(getProcessorName(), e);
        }
    }
    
    @Override
    public String getProcessorName() {
        return "MessageStorageProcessor";
    }
    
    @Override
    public int getOrder() {
        return 20; // Second processor, after validation
    }
}
