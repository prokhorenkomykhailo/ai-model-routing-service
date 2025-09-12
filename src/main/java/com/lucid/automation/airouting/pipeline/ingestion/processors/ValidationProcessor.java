package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionProcessingContext;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.util.TimestampUtil;
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
    public ProcessingResult process(IngestionProcessingContext context) {
        var ingestionEvent = context.getIngestionEvent();

        logger.debug("Validating ingestion event for tenantId: {}", context.getTenantId());

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

            // Use TimestampUtil to handle both timestamp formats
            TimestampUtil.ParsedTimestamp parsed = TimestampUtil.parseTimestamp(tsStr);

            if (!parsed.isValid()) {
                logger.warn("❌ [VALIDATION] Invalid timestamp format: '{}' - Error: {} - continuing with processing",
                    tsStr, parsed.getErrorMessage());
                // Don't fail for invalid timestamp format, just log warning
            } else {
                // Check if timestamp is too far in the future (24 hours threshold)
                if (TimestampUtil.isTimestampInFuture(tsStr, 86400)) {
                    logger.warn("⏰ [VALIDATION] Message timestamp too far in future: '{}' (format: {}) - will skip",
                        tsStr, TimestampUtil.getTimestampFormatDescription(tsStr));
                    return ProcessingResult.failure(getProcessorName(), "Message timestamp is in the future");
                }

                logger.debug("✅ [VALIDATION] Message timestamp validated: '{}' (format: {})",
                    tsStr, TimestampUtil.getTimestampFormatDescription(tsStr));
            }
        } catch (Exception e) {
            logger.warn("⚠️ [VALIDATION] Unexpected error validating timestamp: '{}' - Error: {} - continuing with processing",
                ingestionEvent.getMessage().getTs(), e.getMessage());
            // Continue processing even with validation errors
        }

        // Check tenant ID
        if (context.getTenantId() == null || context.getTenantId().trim().isEmpty()) {
            return ProcessingResult.failure(getProcessorName(), "Tenant ID is null or empty");
        }

        logger.debug("Validation completed successfully for messageId: {}", context.getMessageId());
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
