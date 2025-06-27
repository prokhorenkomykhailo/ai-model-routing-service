package com.lucid.automation.airouting.service;

import com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.model.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Service that listens to the Kafka topic and processes incoming messages
 */
@Service
public class IngestionListenerService {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestionListenerService.class);
    
    private final MessageService messageService;
    private final WorkspaceService workspaceService;
    private final UserService userService;
    
    public IngestionListenerService(MessageService messageService, WorkspaceService workspaceService, UserService userService) {
        this.messageService = messageService;
        this.workspaceService = workspaceService;
        this.userService = userService;
        
        // Debug logging to verify services are injected
        logger.info("=== IngestionMessageListenerService INITIALIZED ===");
        logger.info("MessageService: {}", messageService != null ? messageService.getClass().getSimpleName() : "NULL");
        logger.info("WorkspaceService: {}", workspaceService != null ? workspaceService.getClass().getSimpleName() : "NULL");
        logger.info("UserService: {}", userService != null ? userService.getClass().getSimpleName() : "NULL");
        logger.info("=== Service injection check complete ===");
    }
    
    /**
     * Listens to the Kafka topic and processes incoming ingestion messages
     */
    @KafkaListener(
        topics = "${kafka.topics.ingestion-messages}",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "spring.json.use.type.headers=false",
            "spring.json.value.default.type=com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO"
        }
    )
    public void processIngestionMessage(@Payload IngestionEventDTO ingestionEventDto,
                                      Acknowledgment acknowledgment,
                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                      @Header(KafkaHeaders.OFFSET) long offset) {
        
        // Handle case where deserialization failed and ingestionEvent is null
        if (ingestionEventDto == null) {
            logger.error("Received null message from Kafka topic - DESERIALIZATION FAILURE detected!");
            logger.error("This usually indicates malformed JSON or incompatible message format.");
            logger.error("Partition={}, offset={}", partition, offset);
            logger.error("Skipping this message and continuing with next message...");
            acknowledgment.acknowledge();
            return;
        }
        
        logger.info("=== PROCESSING KAFKA MESSAGE ===");
        logger.info("Partition: {}, Offset: {}", partition, offset);
        logger.info("TenantId: {}", ingestionEventDto.getTenantId());
        logger.info("TenantSchema: {}", ingestionEventDto.getTenantSchema());
        logger.info("DeemergeUserId: {}", ingestionEventDto.getDeemergeUserId());

        
        // Log message data with detailed validation
        if (ingestionEventDto.getMessage() != null) {
            logger.info("Message Details:");
            logger.info("  - MessageId (ts): {}", ingestionEventDto.getMessage().getTs());
            logger.info("  - ChannelId: {}", ingestionEventDto.getMessage().getChannelId());
            logger.info("  - ChannelName: {}", ingestionEventDto.getMessage().getChannelName());
            logger.info("  - TeamId: {}", ingestionEventDto.getMessage().getTeamId());
            logger.info("  - UserId: {}", ingestionEventDto.getMessage().getUser());
            logger.info("  - Text: {}", ingestionEventDto.getMessage().getText());
            logger.info("  - Type: {}", ingestionEventDto.getMessage().getType());
            logger.info("  - ThreadTs: {}", ingestionEventDto.getMessage().getThreadTs());
            logger.info("  - IngestedAt (raw): {}", ingestionEventDto.getMessage().getIngestedAt());
            
            // Check for potential timestamp issues
            if (ingestionEventDto.getMessage().getTs() != null) {
                try {
                    double timestamp = Double.parseDouble(ingestionEventDto.getMessage().getTs());
                    logger.info("  - Timestamp parsed as double: {}", timestamp);
                    if (timestamp > System.currentTimeMillis() / 1000.0 + 86400) { // More than 1 day in future
                        logger.warn("  - WARNING: Timestamp appears to be in the future!");
                    }
                } catch (NumberFormatException e) {
                    logger.warn("  - WARNING: Timestamp is not a valid number: {}", e.getMessage());
                }
            }
        } else {
            logger.error("MESSAGE DATA IS NULL!");
        }
        
        // Log user data information with more details
        if (ingestionEventDto.getUser() != null) {
            logger.info("User Details:");
            logger.info("  - SlackUserId: {}", ingestionEventDto.getUser().getSlackUserId());
            logger.info("  - Name: {}", ingestionEventDto.getUser().getName());
            logger.info("  - DisplayName: '{}'", ingestionEventDto.getUser().getDisplayName());
            logger.info("  - Email: {}", ingestionEventDto.getUser().getEmail());
            logger.info("  - ID: {}", ingestionEventDto.getUser().getId());
            logger.info("  - TeamId: {}", ingestionEventDto.getUser().getTeamId());
        } else {
            logger.warn("USER DATA IS NULL!");
        }
        
        logger.info("IngestedAt: {}", ingestionEventDto.getIngestedAt());
        
        if (ingestionEventDto.getMessage() == null) {
            logger.warn("Received message with null message data, skipping processing");
            acknowledgment.acknowledge();
            return;
        }
        
        // Add basic validation for message data
        if (ingestionEventDto.getMessage().getTs() == null) {
            logger.warn("Message timestamp is null, skipping processing");
            acknowledgment.acknowledge();
            return;
        }
        
        // Check for reasonable timestamp values (should be in seconds since epoch)
        try {
            String tsStr = ingestionEventDto.getMessage().getTs();
            double timestamp = Double.parseDouble(tsStr);
            if (timestamp > System.currentTimeMillis() / 1000.0 + 86400) { // More than 1 day in future
                logger.warn("Message timestamp appears to be in the future: {}, skipping processing", timestamp);
                acknowledgment.acknowledge();
                return;
            }
        } catch (NumberFormatException e) {
            logger.warn("Invalid timestamp format: {}, continuing with processing", ingestionEventDto.getMessage().getTs());
        }

        try {
            logger.info("=== STARTING MESSAGE PROCESSING ===");
            logger.info("Step 1: Processing message: messageId={}, tenantId={}", 
                ingestionEventDto.getMessage().getTs(), ingestionEventDto.getTenantId());
            
            boolean processingSuccessful = true;
            
            // Save message to Redis for conversation history
            logger.info("Step 2: Attempting to save message to Redis...");
            logger.debug("  - Calling messageService.storeMessage()");
            boolean stored = messageService.storeMessage(ingestionEventDto);
            
            if (stored) {
                logger.info("Step 2: SUCCESS - Message saved to Redis");
                logger.info("  - MessageId: {}", ingestionEventDto.getMessage().getTs());
                logger.info("  - SlackUserId: {}", 
                    ingestionEventDto.getUser() != null ? ingestionEventDto.getUser().getSlackUserId() : "null");
            } else {
                logger.error("Step 2: FAILED - Could not save message to Redis");
                logger.error("  - MessageId: {}", ingestionEventDto.getMessage().getTs());
                logger.error("  - This indicates Redis connectivity or data issues");
                processingSuccessful = false;
            }
            
            // Store/update user information in Redis
            logger.info("Step 3: Attempting to store/update user information...");
            logger.debug("  - Calling userService.createOrUpdateUser()");
            var user = userService.createOrUpdateUser(ingestionEventDto);
            
            if (user != null) {
                logger.info("Step 3: SUCCESS - User information stored/updated");
                logger.info("  - UserId: {}", user.getId());
                logger.info("  - SlackUserId: {}", user.getSlackUserId());
                logger.info("  - Name: {}", user.getName());
                logger.info("  - MessageCount: {}", user.getMessageCount());
            } else {
                logger.warn("Step 3: WARNING - User service returned null");
                logger.warn("  - This might happen if user data is missing or invalid");
                logger.warn("  - SlackUserId: {}", 
                    ingestionEventDto.getUser() != null ? ingestionEventDto.getUser().getSlackUserId() : "null");
                // Don't mark as failed for user update issues - this is not critical for message processing
            }
            // The deemergeUserId is now part of the IngestionEventDTO and will be set on the Workspace model by WorkspaceService.
            Workspace workspace = workspaceService.createOrUpdateWorkspace(ingestionEventDto);
            
            if (workspace != null) {
                logger.info("Step 4: SUCCESS - Workspace updated");
                logger.info("  - WorkspaceId: {}", workspace.getId());
                logger.info("  - TenantId: {}", workspace.getTenantId());
            } else {
                logger.warn("Step 4: WARNING - Workspace service returned null");
                logger.warn("  - TenantId: {}", ingestionEventDto.getTenantId());
                logger.warn("  - This might be expected behavior in some cases");
                // Don't mark as failed for workspace update issues
            }
            
            // Only acknowledge if both critical operations succeeded
            if (processingSuccessful) {
                logger.info("Step 5: Processing completed successfully");
                logger.info("  - MessageId: {}", ingestionEventDto.getMessage().getTs());
                
                // Acknowledge the message after successful processing
                logger.info("Step 6: Acknowledging message...");
                acknowledgment.acknowledge();
                logger.info("=== MESSAGE PROCESSING COMPLETE ===");
            } else {
                logger.error("Step 5: Processing FAILED - Critical operations unsuccessful");
                logger.error("  - MessageId: {}", ingestionEventDto.getMessage().getTs());
                logger.error("  - Message will NOT be acknowledged to allow retry");
                logger.error("=== MESSAGE PROCESSING FAILED - NO ACK ===");
                // Do NOT acknowledge - let Kafka retry
                throw new RuntimeException("Critical processing step failed - message not acknowledged");
            }
            
        } catch (Exception e) {
            logger.error("=== EXCEPTION DURING MESSAGE PROCESSING ===");
            logger.error("Exception Type: {}", e.getClass().getSimpleName());
            logger.error("Exception Message: {}", e.getMessage());
            logger.error("MessageId: {}", 
                ingestionEventDto.getMessage() != null ? ingestionEventDto.getMessage().getTs() : "unknown");
            logger.error("TenantId: {}", ingestionEventDto.getTenantId());
            logger.error("Partition: {}, Offset: {}", partition, offset);
            
            // Try to determine which step failed
            String exceptionMsg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            String className = e.getClass().getSimpleName();
            
            if (exceptionMsg.contains("redis") || exceptionMsg.contains("store") || 
                className.contains("Redis") || className.contains("Jedis")) {
                logger.error("LIKELY FAILURE POINT: Step 2 - Redis storage");
            } else if (exceptionMsg.contains("user") || className.contains("User")) {
                logger.error("LIKELY FAILURE POINT: Step 3 - User processing");
            } else if (exceptionMsg.contains("workspace") || className.contains("Workspace")) {
                logger.error("LIKELY FAILURE POINT: Step 4 - Workspace processing");
            } else if (exceptionMsg.contains("sql") || exceptionMsg.contains("database") || 
                       className.contains("SQL") || className.contains("DataAccess")) {
                logger.error("LIKELY FAILURE POINT: Database operation");
            } else {
                logger.error("LIKELY FAILURE POINT: Unknown - General processing error");
            }
            
            // Log the full stack trace for debugging
            logger.error("Full Exception Stack Trace:", e);
            
            // Check for common issues
            if (e instanceof NullPointerException) {
                logger.error("NULL POINTER EXCEPTION - Check for null values in message data");
            } else if (e instanceof IllegalArgumentException) {
                logger.error("ILLEGAL ARGUMENT EXCEPTION - Check for invalid data values");
            } else if (e instanceof ClassCastException) {
                logger.error("CLASS CAST EXCEPTION - Check for type mismatches");
            }
            
            // Determine if this is a retryable error
            boolean isRetryable = isRetryableException(e);
            
            if (isRetryable) {
                logger.info("Exception is RETRYABLE - message will NOT be acknowledged");
                logger.info("Kafka will retry this message according to retry policy");
                logger.error("=== RETRYABLE EXCEPTION - NO ACK ===");
                // Do NOT acknowledge - let Kafka retry
                throw e; // Re-throw to trigger retry
            } else {
                logger.info("Exception is NOT RETRYABLE - acknowledging message to prevent infinite retries");
                acknowledgment.acknowledge();
                logger.error("=== NON-RETRYABLE EXCEPTION - ACKNOWLEDGED ===");
            }
        }
    }
    
    /**
     * Determines if an exception is retryable or should be skipped
     * 
     * @param exception The exception to evaluate
     * @return true if the exception is retryable, false if it should be skipped
     */
    private boolean isRetryableException(Exception exception) {
        // Network/connectivity issues - should retry
        if (exception.getMessage() != null) {
            String msg = exception.getMessage().toLowerCase();
            if (msg.contains("connection") || msg.contains("timeout") || 
                msg.contains("network") || msg.contains("redis") ||
                msg.contains("unable to connect") || msg.contains("connection refused")) {
                return true;
            }
        }
        
        // Class name based checks
        String className = exception.getClass().getSimpleName().toLowerCase();
        if (className.contains("connection") || className.contains("timeout") ||
            className.contains("redis") || className.contains("jedis")) {
            return true;
        }
        
        // Data format issues - should not retry
        if (exception instanceof IllegalArgumentException ||
            exception instanceof ClassCastException ||
            exception instanceof NullPointerException) {
            return false;
        }
        
        // Serialization/deserialization issues - should not retry
        if (className.contains("serialization") || className.contains("json") ||
            className.contains("parse") || className.contains("mapping")) {
            return false;
        }
        
        // Default to retryable for unknown exceptions
        return true;
    }
}
