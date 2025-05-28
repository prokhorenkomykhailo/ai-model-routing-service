package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.client.DataStorageServiceClient;
import com.lucid.automation.airouting.dto.APIResponse;
import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import com.lucid.automation.airouting.dto.TenantDTO;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.request.ConversationEnrichmentRequest;
import com.lucid.automation.airouting.service.AIMessagePublisherService;
import com.lucid.automation.airouting.service.TenantService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduler for enriching messages from data storage service.
 * This scheduler fetches all group IDs, loads messages for each group,
 * and publishes them to the ai.enrich.queue for AI processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "ai.enrichment.scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class MessageEnrichmentScheduler {

    private final DataStorageServiceClient dataStorageServiceClient;
    private final AIMessagePublisherService aiMessagePublisherService;
    private final TenantService tenantService;

    @Value("${ai.enrichment.scheduler.batch-size:50}")
    private int batchSize;

    @Value("${ai.enrichment.scheduler.max-retries:3}")
    private int maxRetries;

    @Value("${ai.enrichment.scheduler.default-tenant-id:default}")
    private String defaultTenantId;

    @Value("${ai.enrichment.scheduler.default-tenant-schema:public}")
    private String defaultTenantSchema;

    /**
     * Scheduled method that runs based on the cron expression in application.yml.
     * First gets all tenants from auth-service, then processes messages for each tenant.
     */
    @Scheduled(cron = "${ai.enrichment.scheduler.cron:0 */5 * * * ?}")
    public void processMessageEnrichment() {
        log.info("Starting scheduled message enrichment process");
        
        try {
            // Step 1: Get all tenants from auth-service
            List<TenantDTO> tenants = tenantService.getAllTenants();
            log.info("Found {} tenants to process for enrichment", tenants.size());

            if (tenants.isEmpty()) {
                log.info("No tenants found, skipping enrichment cycle");
                return;
            }

            int processedTenants = 0;
            int totalGroups = 0;
            int totalMessages = 0;

            // Step 2: Process each tenant
            for (TenantDTO tenant : tenants) {
                try {
                    log.info("Processing tenant: {} ({})", tenant.getName(), tenant.getTenantId());
                    int[] results = processTenant(tenant);
                    int tenantGroups = results[0];
                    int tenantMessages = results[1];
                    
                    totalGroups += tenantGroups;
                    totalMessages += tenantMessages;
                    processedTenants++;
                    
                    log.debug("Processed {} groups with {} messages for tenant: {}", 
                             tenantGroups, tenantMessages, tenant.getName());
                } catch (Exception e) {
                    log.error("Failed to process tenant: {} ({}). Error: {}", 
                             tenant.getName(), tenant.getTenantId(), e.getMessage(), e);
                    // Continue processing other tenants even if one fails
                }
            }

            log.info("Completed scheduled message enrichment process. " +
                    "Processed {} tenants with {} total groups and {} total messages", 
                    processedTenants, totalGroups, totalMessages);

        } catch (Exception e) {
            log.error("Error during scheduled message enrichment process: {}", e.getMessage(), e);
        }
    }

    /**
     * Processes messages for a specific tenant.
     * Gets all group IDs for the tenant, then processes messages for each group.
     * 
     * @param tenant the tenant to process
     * @return array with [totalGroups, totalMessages] processed
     */
    private int[] processTenant(TenantDTO tenant) {
        try {
            String tenantId = tenant.getTenantId().toString();
            String tenantSchema = tenant.getSchemaName();
            
            // Get all group IDs for this tenant
            APIResponse<List<String>> groupIdsResponse = dataStorageServiceClient.getAllGroupIds(tenantId, tenantSchema);
            
            if (!groupIdsResponse.isSuccess() || groupIdsResponse.getData() == null) {
                log.warn("Failed to retrieve group IDs for tenant {}: {}", tenant.getName(), groupIdsResponse.getMessage());
                return new int[]{0, 0};
            }
            
            List<String> groupIds = groupIdsResponse.getData();
            
            if (groupIds.isEmpty()) {
                log.debug("No groups found for tenant: {}", tenant.getName());
                return new int[]{0, 0};
            }

            log.debug("Found {} groups for tenant: {}", groupIds.size(), tenant.getName());

            int totalMessages = 0;
            int processedGroups = 0;

            // Process each group
            for (String groupId : groupIds) {
                try {
                    int messagesProcessed = processMessagesForGroup(groupId, tenantId, tenantSchema);
                    totalMessages += messagesProcessed;
                    processedGroups++;
                } catch (Exception e) {
                    log.error("Failed to process group {} for tenant {}: {}", groupId, tenant.getName(), e.getMessage(), e);
                    // Continue processing other groups
                }
            }

            return new int[]{processedGroups, totalMessages};

        } catch (Exception e) {
            log.error("Error processing tenant: {}. Error: {}", tenant.getName(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Processes messages for a specific group ID within a tenant using batch conversation enrichment.
     * 
     * @param groupId the group ID to process
     * @param tenantId the tenant ID
     * @param tenantSchema the tenant schema
     * @return the number of messages processed
     */
    private int processMessagesForGroup(String groupId, String tenantId, String tenantSchema) {
        try {
            // Get all messages for this group ID within the tenant
            APIResponse<List<MessageWithContentDTO>> messagesResponse = dataStorageServiceClient.getAllMessagesByGroupId(
                    groupId, tenantId, tenantSchema);
            
            if (!messagesResponse.isSuccess() || messagesResponse.getData() == null) {
                log.warn("Failed to retrieve messages for group ID {} in tenant {}: {}", 
                        groupId, tenantId, messagesResponse.getMessage());
                return 0;
            }
            
            List<MessageWithContentDTO> messages = messagesResponse.getData();
            
            if (messages.isEmpty()) {
                log.debug("No messages found for group ID: {} in tenant: {}", groupId, tenantId);
                return 0;
            }

            log.debug("Found {} messages for group ID: {} in tenant: {}", messages.size(), groupId, tenantId);

            // Filter out messages that already have enrichment data
            List<MessageWithContentDTO> unenrichedMessages = messages.stream()
                    .filter(message -> !hasEnrichmentData(message))
                    .toList();

            if (unenrichedMessages.isEmpty()) {
                log.debug("All messages in group {} already have enrichment data, skipping", groupId);
                return 0;
            }

            log.debug("Processing {} unenriched messages for group ID: {} in tenant: {}", 
                     unenrichedMessages.size(), groupId, tenantId);

            // Convert messages to SlackMessage objects
            List<SlackMessage> slackMessages = convertToSlackMessages(unenrichedMessages);

            // Extract participants from messages
            List<SlackParticipant> participants = extractParticipants(unenrichedMessages);
            
            // Use senderId from first message as userId if available, otherwise use a default
            String userId = unenrichedMessages.get(0).getSenderId() != null ? 
                          unenrichedMessages.get(0).getSenderId() : "scheduler";

            // Create context with tenant information for the response handler
            Map<String, Object> context = new HashMap<>();
            context.put("tenantId", tenantId);
            context.put("tenantSchema", tenantSchema);

            // Create conversation enrichment request using the request object
            ConversationEnrichmentRequest request = new ConversationEnrichmentRequest();
            request.setConversationId(groupId);
            request.setMessages(slackMessages);
            request.setParticipants(participants);
            request.setTenantId(tenantId);
            request.setPreferredProvider(null); // use default
            request.setReplyTopic("ai.enrich.conversation.response");
            request.setContext(context);

            // Publish conversation enrichment request using the request object
            aiMessagePublisherService.publishConversationEnrichmentRequest(request, userId);

            log.info("Published conversation enrichment request for {} messages in group: {} for tenant: {}", 
                     unenrichedMessages.size(), groupId, tenantId);

            return unenrichedMessages.size();

        } catch (Exception e) {
            log.error("Error processing messages for group ID: {} in tenant: {}. Error: {}", 
                     groupId, tenantId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Checks if a message already has enrichment data.
     * 
     * @param messageDto the message to check
     * @return true if the message has enrichment data, false otherwise
     */
    private boolean hasEnrichmentData(MessageWithContentDTO messageDto) {
        // Check if any of the enrichment fields are populated
        return messageDto.getCategory() != null || 
               messageDto.getSentiment() != null || 
               messageDto.getConfidenceScore() != null ||
               (messageDto.getEnrichmentData() != null && !messageDto.getEnrichmentData().isEmpty());
    }

    /**
     * Converts MessageWithContentDTO objects to SlackMessage objects.
     * 
     * @param messages the list of message DTOs to convert
     * @return list of SlackMessage objects
     */
    private List<SlackMessage> convertToSlackMessages(List<MessageWithContentDTO> messages) {
        return messages.stream()
                .map(this::convertToSlackMessage)
                .toList();
    }

    /**
     * Converts a single MessageWithContentDTO to SlackMessage.
     * 
     * @param messageDto the message DTO to convert
     * @return SlackMessage object
     */
    private SlackMessage convertToSlackMessage(MessageWithContentDTO messageDto) {
        // Log the conversion for debugging
        log.debug("Converting message with ID {} to SlackMessage", messageDto.getMessageId());
        log.debug("Message content: {}", messageDto.getContent());
        log.debug("Message timestamp: {}", messageDto.getMessageTimestamp());
        log.debug("Message sender ID: {}", messageDto.getSenderId());
        log.debug("Message channel ID: {}", messageDto.getChannelId());
        

        SlackMessage slackMessage = new SlackMessage();
        slackMessage.setTs(messageDto.getMessageTimestamp() != null ? 
                          messageDto.getMessageTimestamp().toString() : null);
        slackMessage.setText(messageDto.getContent());
        slackMessage.setUser(messageDto.getSenderId());
        slackMessage.setChannel(messageDto.getChannelId());
        slackMessage.setType("message");
        
        // Convert timestamp to epoch seconds if available
        if (messageDto.getMessageTimestamp() != null) {
            long epochSeconds = messageDto.getMessageTimestamp().toEpochSecond(ZoneOffset.UTC);
            slackMessage.setTs(String.valueOf(epochSeconds) + ".000000");
        }
        
        return slackMessage;
    }

    /**
     * Extracts unique participants from a list of messages.
     * 
     * @param messages the list of messages to extract participants from
     * @return list of unique SlackParticipant objects
     */
    private List<SlackParticipant> extractParticipants(List<MessageWithContentDTO> messages) {
        return messages.stream()
                .filter(message -> message.getSenderId() != null)
                .map(message -> {
                    SlackParticipant participant = new SlackParticipant();
                    participant.setId(message.getSenderId());
                    participant.setName(message.getSenderName() != null ? 
                                      message.getSenderName() : message.getSenderId());
                    participant.setRealName(message.getSenderName());
                    return participant;
                })
                .distinct()
                .toList();
    }
}
