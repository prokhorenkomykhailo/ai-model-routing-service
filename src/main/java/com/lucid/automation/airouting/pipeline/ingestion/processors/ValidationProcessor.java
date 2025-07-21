package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.pipeline.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Validates incoming ingestion messages for required fields and data integrity.
 * This is the first processor in the ingestion pipeline.
 * 
 * @author AI Assistant
 */
@Component
public class ValidationProcessor implements MessageProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(ValidationProcessor.class);
    
    @Override
    public ProcessingResult process(MessageProcessingContext context) {
        var ingestionEvent = context.getIngestionEvent();
        
        logger.debug("Validating ingestion event for tenantId: {}", ingestionEvent.getTenantId());
        
        // Check if message exists
        if (ingestionEvent.getMessage() == null) {
            return ProcessingResult.failure(getProcessorName(), "Message is null");
        }
        
        // Check if message timestamp exists
        if (ingestionEvent.getMessage().getTs() == null) {
            return ProcessingResult.failure(getProcessorName(), "Message timestamp is null");
        }
        
        // Validate timestamp format and future check
        try {
            String tsStr = ingestionEvent.getMessage().getTs();
            double timestamp = Double.parseDouble(tsStr);
            if (timestamp > System.currentTimeMillis() / 1000.0 + 86400) {
                logger.warn("Message timestamp in future: {} - will skip", timestamp);
                return ProcessingResult.failure(getProcessorName(), "Message timestamp is in the future");
            }
        } catch (NumberFormatException e) {
            logger.warn("Invalid timestamp format: {} - continuing", ingestionEvent.getMessage().getTs());
            // Don't fail for invalid timestamp format, just log warning
        }
        
        // Check tenant ID
        if (ingestionEvent.getTenantId() == null || ingestionEvent.getTenantId().trim().isEmpty()) {
            return ProcessingResult.failure(getProcessorName(), "Tenant ID is null or empty");
        }
        
        logger.debug("Validation completed successfully for messageId: {}", ingestionEvent.getMessage().getTs());
        return ProcessingResult.success(getProcessorName());
    }
    
    @Override
    public String getProcessorName() {
        return "ValidationProcessor";
    }
    
    @Override
    public int getOrder() {
        return 10; // First processor
    }
}
