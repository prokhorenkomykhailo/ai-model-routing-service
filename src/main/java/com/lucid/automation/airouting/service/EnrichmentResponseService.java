package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.DataStorageServiceClient;
import com.lucid.automation.airouting.dto.APIResponse;
import com.lucid.automation.airouting.dto.EnrichedMessageDTO;
import com.lucid.automation.airouting.dto.StoredDataDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Service for handling enrichment responses from RabbitMQ and storing them using the
 * data-storage-service
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EnrichmentResponseService {

    private final DataStorageServiceClient dataStorageServiceClient;

    /**
     * Listener for conversation enrichment responses
     * 
     * @param enrichedMessage the enriched message received from RabbitMQ
     */
    @RabbitListener(queues = "${rabbitmq.queue.ai-enrich-conversation-response}")
    public void handleConversationEnrichmentResponse(Map<String, Object> messageMap) {
        try {
            log.debug("Received enriched message response: {}", messageMap);
            
            // Extract tenant info from the message
            String tenantId = extractTenantId(messageMap);
            String tenantSchema = extractTenantSchema(messageMap);
            
            // Extract and convert the enriched message data
            EnrichedMessageDTO enrichedMessage = convertToEnrichedMessageDTO(messageMap);
            
            if (enrichedMessage == null || enrichedMessage.getGroupId() == null) {
                log.error("Invalid enriched message format or missing group ID");
                return;
            }
            
            log.info("Processing enriched message for group ID: {}", enrichedMessage.getGroupId());
            
            // Additional metadata 
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("source", "ai-enrichment");
            metadata.put("processingTimestamp", System.currentTimeMillis());
            
            // Save the enriched message to data-storage-service
            APIResponse<StoredDataDTO> response = dataStorageServiceClient.saveEnrichedMessage(
                    enrichedMessage, tenantId, tenantSchema, metadata);
            
            if (response.isSuccess()) {
                log.info("Successfully saved enriched message for group ID: {}, stored data ID: {}", 
                        enrichedMessage.getGroupId(), response.getData().getId());
            } else {
                log.error("Failed to save enriched message for group ID: {}, error: {}", 
                        enrichedMessage.getGroupId(), response.getMessage());
            }
        } catch (Exception e) {
            log.error("Error handling enriched message response: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Extract tenant ID from the message
     */
    private String extractTenantId(Map<String, Object> messageMap) {
        if (messageMap.containsKey("tenantId")) {
            return messageMap.get("tenantId").toString();
        }
        return "default"; // Fallback to default tenant
    }
    
    /**
     * Extract tenant schema from the message
     */
    private String extractTenantSchema(Map<String, Object> messageMap) {
        if (messageMap.containsKey("tenantSchema")) {
            return messageMap.get("tenantSchema").toString();
        }
        return "public"; // Fallback to public schema
    }
    
    /**
     * Convert the message map to EnrichedMessageDTO
     */
    @SuppressWarnings("unchecked")
    private EnrichedMessageDTO convertToEnrichedMessageDTO(Map<String, Object> messageMap) {
        try {
            if (messageMap.containsKey("result")) {
                Map<String, Object> result = (Map<String, Object>) messageMap.get("result");
                
                // Build the EnrichedMessageDTO from the result map
                EnrichedMessageDTO dto = new EnrichedMessageDTO();
                
                // Set group ID
                if (messageMap.containsKey("conversationId")) {
                    dto.setGroupId(messageMap.get("conversationId").toString());
                }
                
                // Set people involved
                if (result.containsKey("peopleInvolved")) {
                    dto.setPeopleInvolved((java.util.List<String>) result.get("peopleInvolved"));
                }
                
                // Set topic
                if (result.containsKey("topic")) {
                    Map<String, Object> topicMap = (Map<String, Object>) result.get("topic");
                    EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
                    
                    if (topicMap.containsKey("name")) {
                        topic.setName(topicMap.get("name").toString());
                    }
                    
                    if (topicMap.containsKey("summary")) {
                        topic.setSummary(topicMap.get("summary").toString());
                    }
                    
                    if (topicMap.containsKey("category")) {
                        topic.setCategory((java.util.List<String>) topicMap.get("category"));
                    }
                    
                    if (topicMap.containsKey("subCategory")) {
                        topic.setSubCategory((java.util.List<String>) topicMap.get("subCategory"));
                    }
                    
                    if (topicMap.containsKey("keyPoints")) {
                        topic.setKeyPoints((java.util.List<String>) topicMap.get("keyPoints"));
                    }
                    
                    if (topicMap.containsKey("priority")) {
                        topic.setPriority(topicMap.get("priority").toString());
                    }
                    
                    if (topicMap.containsKey("keywords")) {
                        topic.setKeywords((java.util.List<String>) topicMap.get("keywords"));
                    }
                    
                    dto.setTopic(topic);
                }
                
                // Set conversations
                if (result.containsKey("conversations")) {
                    java.util.List<Map<String, Object>> conversationMaps = 
                            (java.util.List<Map<String, Object>>) result.get("conversations");
                    
                    java.util.List<EnrichedMessageDTO.ConversationDTO> conversations = new java.util.ArrayList<>();
                    
                    for (Map<String, Object> convMap : conversationMaps) {
                        EnrichedMessageDTO.ConversationDTO conv = new EnrichedMessageDTO.ConversationDTO();
                        
                        if (convMap.containsKey("text")) {
                            conv.setText(convMap.get("text").toString());
                        }
                        
                        if (convMap.containsKey("relevance")) {
                            conv.setRelevance(convMap.get("relevance").toString());
                        }
                        
                        conversations.add(conv);
                    }
                    
                    dto.setConversations(conversations);
                }
                
                return dto;
            }
        } catch (Exception e) {
            log.error("Error converting message map to EnrichedMessageDTO: {}", e.getMessage(), e);
        }
        
        return null;
    }
}
