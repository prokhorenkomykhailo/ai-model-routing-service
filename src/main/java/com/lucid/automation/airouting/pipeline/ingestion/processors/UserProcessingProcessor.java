package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionProcessingContext;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Processes user information from ingestion messages.
 * Creates or updates user records based on the ingestion event.
 *
 * @author AI Assistant
 */
@Component
public class UserProcessingProcessor implements MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(UserProcessingProcessor.class);

    private final UserService userService;

    public UserProcessingProcessor(UserService userService) {
        this.userService = userService;
    }

    @Override
    public ProcessingResult process(IngestionProcessingContext context) {
        var ingestionEvent = context.getIngestionEvent();

        logger.debug("Processing user information for messageId: {}, tenantId: {}",
            ingestionEvent.getMessage().getTs(), ingestionEvent.getTenantId());

        try {
            // Process user information
            User user = userService.createOrUpdateUser(ingestionEvent);

            if (user == null) {
                String userId = ingestionEvent.getUser() != null ? ingestionEvent.getUser().getSlackUserId() : "null";
                // This is not a critical failure, so we continue
                context.setUserProcessed(false);
                context.setProcessingData("userWarning", "User service returned null");
            } else {
                context.setUserProcessed(true);
                context.setProcessedUser(user);
            }

            return ProcessingResult.success(getProcessorName());

        } catch (Exception e) {
            String errorMsg = "Exception while processing user: " + e.getMessage();
            logger.error(errorMsg, e);

            // User processing failure is not critical, so we continue
            context.setUserProcessed(false);
            context.setProcessingData("userError", errorMsg);

            return ProcessingResult.success(getProcessorName()); // Return success to continue pipeline
        }
    }

    @Override
    public String getProcessorName() {
        return "UserProcessingProcessor";
    }

    @Override
    public int getOrder() {
        return 30; // Third processor, after message storage
    }
}
