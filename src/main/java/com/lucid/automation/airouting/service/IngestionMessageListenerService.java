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
public class IngestionMessageListenerService {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestionMessageListenerService.class);
    
    private final MessageService messageService;
    private final WorkspaceService workspaceService;
    
    public IngestionMessageListenerService(MessageService messageService, WorkspaceService workspaceService) {
        this.messageService = messageService;
        this.workspaceService = workspaceService;
        
        // Debug logging to verify services are injected
        logger.info("=== IngestionMessageListenerService INITIALIZED ===");
        logger.info("MessageService: {}", messageService != null ? messageService.getClass().getSimpleName() : "NULL");
        logger.info("WorkspaceService: {}", workspaceService != null ? workspaceService.getClass().getSimpleName() : "NULL");
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
    public void processIngestionMessage(@Payload IngestionEventDTO ingestionEvent,
                                      Acknowledgment acknowledgment,
                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                      @Header(KafkaHeaders.OFFSET) long offset) {
        
        // Handle case where deserialization failed and ingestionEvent is null
        if (ingestionEvent == null) {
            logger.error("Received null message from Kafka topic - DESERIALIZATION FAILURE detected!");
            logger.error("This usually indicates malformed JSON or incompatible message format.");
            logger.error("Partition={}, offset={}", partition, offset);
            logger.error("Skipping this message and continuing with next message...");
            acknowledgment.acknowledge();
            return;
        }
        
        logger.info("=== PROCESSING KAFKA MESSAGE ===");
        logger.info("Partition: {}, Offset: {}", partition, offset);
        logger.info("TenantId: {}", ingestionEvent.getTenantId());
        logger.info("TenantSchema: {}", ingestionEvent.getTenantSchema());
        
        // Log message data with detailed validation
        if (ingestionEvent.getMessage() != null) {
            logger.info("Message Details:");
            logger.info("  - MessageId (ts): {}", ingestionEvent.getMessage().getTs());
            logger.info("  - ChannelId: {}", ingestionEvent.getMessage().getChannelId());
            logger.info("  - TeamId: {}", ingestionEvent.getMessage().getTeamId());
            logger.info("  - UserId: {}", ingestionEvent.getMessage().getUser());
            logger.info("  - Text: {}", ingestionEvent.getMessage().getText());
            logger.info("  - Type: {}", ingestionEvent.getMessage().getType());
            logger.info("  - ThreadTs: {}", ingestionEvent.getMessage().getThreadTs());
            logger.info("  - IngestedAt (raw): {}", ingestionEvent.getMessage().getIngestedAt());
            
            // Check for potential timestamp issues
            if (ingestionEvent.getMessage().getTs() != null) {
                try {
                    double timestamp = Double.parseDouble(ingestionEvent.getMessage().getTs());
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
        if (ingestionEvent.getUser() != null) {
            logger.info("User Details:");
            logger.info("  - SlackUserId: {}", ingestionEvent.getUser().getSlackUserId());
            logger.info("  - Name: {}", ingestionEvent.getUser().getName());
            logger.info("  - DisplayName: '{}'", ingestionEvent.getUser().getDisplayName());
            logger.info("  - Email: {}", ingestionEvent.getUser().getEmail());
            logger.info("  - ID: {}", ingestionEvent.getUser().getId());
            logger.info("  - TeamId: {}", ingestionEvent.getUser().getTeamId());
        } else {
            logger.warn("USER DATA IS NULL!");
        }
        
        logger.info("IngestedAt: {}", ingestionEvent.getIngestedAt());
        
        if (ingestionEvent.getMessage() == null) {
            logger.warn("Received message with null message data, skipping processing");
            acknowledgment.acknowledge();
            return;
        }
        
        // Add basic validation for message data
        if (ingestionEvent.getMessage().getTs() == null) {
            logger.warn("Message timestamp is null, skipping processing");
            acknowledgment.acknowledge();
            return;
        }
        
        // Check for reasonable timestamp values (should be in seconds since epoch)
        try {
            String tsStr = ingestionEvent.getMessage().getTs();
            double timestamp = Double.parseDouble(tsStr);
            if (timestamp > System.currentTimeMillis() / 1000.0 + 86400) { // More than 1 day in future
                logger.warn("Message timestamp appears to be in the future: {}, skipping processing", timestamp);
                acknowledgment.acknowledge();
                return;
            }
        } catch (NumberFormatException e) {
            logger.warn("Invalid timestamp format: {}, continuing with processing", ingestionEvent.getMessage().getTs());
        }

        try {
            logger.info("=== STARTING MESSAGE PROCESSING ===");
            logger.info("Step 1: Processing message: messageId={}, tenantId={}", 
                ingestionEvent.getMessage().getTs(), ingestionEvent.getTenantId());
            
            // Save message to Redis for conversation history
            logger.info("Step 2: Attempting to save message to Redis...");
            logger.debug("  - Calling messageService.storeMessage()");
            boolean stored = messageService.storeMessage(ingestionEvent);
            
            if (stored) {
                logger.info("Step 2: SUCCESS - Message saved to Redis");
                logger.info("  - MessageId: {}", ingestionEvent.getMessage().getTs());
                logger.info("  - SlackUserId: {}", 
                    ingestionEvent.getUser() != null ? ingestionEvent.getUser().getSlackUserId() : "null");
            } else {
                logger.error("Step 2: FAILED - Could not save message to Redis");
                logger.error("  - MessageId: {}", ingestionEvent.getMessage().getTs());
                logger.error("  - This might indicate Redis connectivity issues");
            }
            
            // Create or update workspace data
            logger.info("Step 3: Attempting to create/update workspace...");
            logger.debug("  - Calling workspaceService.createOrUpdateWorkspace()");
            Workspace workspace = workspaceService.createOrUpdateWorkspace(ingestionEvent);
            
            if (workspace != null) {
                logger.info("Step 3: SUCCESS - Workspace updated");
                logger.info("  - WorkspaceId: {}", workspace.getId());
                logger.info("  - TenantId: {}", workspace.getTenantId());
            } else {
                logger.warn("Step 3: WARNING - Workspace service returned null");
                logger.warn("  - TenantId: {}", ingestionEvent.getTenantId());
                logger.warn("  - This might be expected behavior in some cases");
            }
            
            logger.info("Step 4: Processing completed successfully");
            logger.info("  - MessageId: {}", ingestionEvent.getMessage().getTs());
            
            // Acknowledge the message after successful processing
            logger.info("Step 5: Acknowledging message...");
            acknowledgment.acknowledge();
            
            logger.info("=== MESSAGE PROCESSING COMPLETE ===");
            
        } catch (Exception e) {
            logger.error("=== EXCEPTION DURING MESSAGE PROCESSING ===");
            logger.error("Exception Type: {}", e.getClass().getSimpleName());
            logger.error("Exception Message: {}", e.getMessage());
            logger.error("MessageId: {}", 
                ingestionEvent.getMessage() != null ? ingestionEvent.getMessage().getTs() : "unknown");
            logger.error("TenantId: {}", ingestionEvent.getTenantId());
            logger.error("Partition: {}, Offset: {}", partition, offset);
            
            // Try to determine which step failed
            String exceptionMsg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            String className = e.getClass().getSimpleName();
            
            if (exceptionMsg.contains("redis") || exceptionMsg.contains("store") || 
                className.contains("Redis") || className.contains("Jedis")) {
                logger.error("LIKELY FAILURE POINT: Step 2 - Redis storage");
            } else if (exceptionMsg.contains("workspace") || className.contains("Workspace")) {
                logger.error("LIKELY FAILURE POINT: Step 3 - Workspace processing");
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
            
            // For now, acknowledge even on error to prevent message reprocessing
            logger.info("Acknowledging failed message to prevent infinite reprocessing...");
            acknowledgment.acknowledge();
            logger.error("=== EXCEPTION HANDLING COMPLETE ===");
        }
    }
}
