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
     * @param messageMap the enriched message received from RabbitMQ
     */
    @RabbitListener(
        queues = "${rabbitmq.queue.ai-enrich-conversation-response}",
        concurrency = "1",
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void handleConversationEnrichmentResponse(Map<String, Object> messageMap) {
        try {
            log.info("Received enriched message response: {}", messageMap);
            log.debug("Message map keys: {}", messageMap.keySet());
            
            // Extract tenant info from the message
            String tenantId = extractTenantId(messageMap);
            String tenantSchema = extractTenantSchema(messageMap);
            
            // Extract and convert the enriched message data
            EnrichedMessageDTO enrichedMessage = convertToEnrichedMessageDTO(messageMap);
            
            if (enrichedMessage == null || enrichedMessage.getGroupId() == null) {
                log.error("Invalid enriched message format or missing group ID");
                return;
            }
            
            // Validate required fields before sending to data-storage-service
            if (!validateEnrichedMessage(enrichedMessage)) {
                log.error("Enriched message validation failed for group ID: {}", enrichedMessage.getGroupId());
                return;
            }
            
            log.info("DEBUG-ENRICHMENT: Processing enriched message for group ID: {}", enrichedMessage.getGroupId());
            log.debug("DEBUG-ENRICHMENT: Enriched message details - conversations: {}, peopleInvolved: {}, topic: {}", 
                    enrichedMessage.getConversations() != null ? enrichedMessage.getConversations().size() : "null",
                    enrichedMessage.getPeopleInvolved() != null ? enrichedMessage.getPeopleInvolved().size() : "null",
                    enrichedMessage.getTopic() != null ? enrichedMessage.getTopic().getName() : "null");
            
            // Additional metadata 
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("source", "ai-enrichment");
            metadata.put("processingTimestamp", System.currentTimeMillis());
            
            log.debug("DEBUG-ENRICHMENT: Calling data-storage-service with tenantId: {}, tenantSchema: {}, metadata keys: {}", 
                    tenantId, tenantSchema, metadata.keySet());
            
            // Save the enriched message to data-storage-service
            APIResponse<StoredDataDTO> response;
            try {
                response = dataStorageServiceClient.saveEnrichedMessage(
                        enrichedMessage, tenantId, tenantSchema, metadata);
                log.debug("DEBUG-ENRICHMENT: Received response from data-storage-service - success: {}, message: {}", 
                        response.isSuccess(), response.getMessage());
            } catch (Exception clientException) {
                log.error("DEBUG-ENRICHMENT: Exception calling data-storage-service client for group ID: {} - Exception type: {}, Message: {}", 
                        enrichedMessage.getGroupId(), clientException.getClass().getSimpleName(), clientException.getMessage(), clientException);
                throw clientException;
            }
            
            if (response.isSuccess()) {
                log.info("DEBUG-ENRICHMENT: Successfully saved enriched message for group ID: {}, stored data ID: {}", 
                        enrichedMessage.getGroupId(), response.getData().getId());
            } else {
                log.error("DEBUG-ENRICHMENT: Failed to save enriched message for group ID: {}, error: {}. " +
                        "This might be due to validation issues. Check the enriched message format.",
                        enrichedMessage.getGroupId(), response.getMessage());
                
                // Log the enriched message details for debugging
                log.debug("DEBUG-ENRICHMENT: Failed enriched message details: conversations={}, peopleInvolved={}, topic={}",
                        enrichedMessage.getConversations(),
                        enrichedMessage.getPeopleInvolved(),
                        enrichedMessage.getTopic());
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
                
                // Check if result is not null
                if (result == null) {
                    log.error("Result map is null in message: {}", messageMap);
                    return null;
                }
                
                // Build the EnrichedMessageDTO from the result map
                EnrichedMessageDTO dto = new EnrichedMessageDTO();
                
                // Set group ID - try multiple possible field names
                String groupId = null;
                if (messageMap.containsKey("conversationId")) {
                    groupId = messageMap.get("conversationId").toString();
                } else if (messageMap.containsKey("correlationId")) {
                    groupId = messageMap.get("correlationId").toString();
                } else if (messageMap.containsKey("messageId")) {
                    groupId = messageMap.get("messageId").toString();
                }
                
                if (groupId != null) {
                    dto.setGroupId(groupId);
                } else {
                    log.warn("No suitable group ID field found in message: {}", messageMap.keySet());
                }
                
                // Set people involved - try metadata.peopleInvolved first (new format), then result.peopleInvolved (old format)
                Object peopleInvolvedSource = null;
                
                // Check if metadata exists and has peopleInvolved (new format from Gemini)
                if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
                    Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
                    if (metadata.containsKey("peopleInvolved")) {
                        peopleInvolvedSource = metadata.get("peopleInvolved");
                    }
                }
                
                // Fallback to result.peopleInvolved if metadata.peopleInvolved not found (old format)
                if (peopleInvolvedSource == null && result.containsKey("peopleInvolved")) {
                    peopleInvolvedSource = result.get("peopleInvolved");
                }
                
                if (peopleInvolvedSource instanceof java.util.List) {
                    dto.setPeopleInvolved((java.util.List<String>) peopleInvolvedSource);
                } else if (peopleInvolvedSource != null) {
                    log.warn("PeopleInvolved field is not a List, found type: {}", 
                            peopleInvolvedSource.getClass().getSimpleName());
                    // Set empty list as fallback
                    dto.setPeopleInvolved(new java.util.ArrayList<>());
                } else {
                    // No peopleInvolved found, set empty list
                    dto.setPeopleInvolved(new java.util.ArrayList<>());
                }
                
                // Set topic - try metadata.topic first (new format), then result.topic (old format)
                Map<String, Object> topicSource = null;
                
                // Check if metadata exists and has topic (new format from Gemini)
                if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
                    Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
                    if (metadata.containsKey("topic") && metadata.get("topic") instanceof Map) {
                        topicSource = (Map<String, Object>) metadata.get("topic");
                    }
                }
                
                // Fallback to result.topic if metadata.topic not found (old format)
                if (topicSource == null && result.containsKey("topic") && result.get("topic") instanceof Map) {
                    topicSource = (Map<String, Object>) result.get("topic");
                }
                
                if (topicSource != null) {
                    EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
                
                    if (topicSource.containsKey("name")) {
                        topic.setName(topicSource.get("name").toString());
                    }
                    
                    if (topicSource.containsKey("summary")) {
                        topic.setSummary(topicSource.get("summary").toString());
                    }
                    
                    if (topicSource.containsKey("category")) {
                        Object categoryObj = topicSource.get("category");
                        if (categoryObj instanceof java.util.List) {
                            topic.setCategory((java.util.List<String>) categoryObj);
                        } else {
                            log.warn("Topic category field is not a List, found type: {}", 
                                    categoryObj != null ? categoryObj.getClass().getSimpleName() : "null");
                        }
                    }
                    
                    if (topicSource.containsKey("subCategory") || topicSource.containsKey("sub-category")) {
                        Object subCategoryObj = topicSource.containsKey("subCategory") ? 
                                topicSource.get("subCategory") : topicSource.get("sub-category");
                        if (subCategoryObj instanceof java.util.List) {
                            topic.setSubCategory((java.util.List<String>) subCategoryObj);
                        } else {
                            log.warn("Topic subCategory field is not a List, found type: {}", 
                                    subCategoryObj != null ? subCategoryObj.getClass().getSimpleName() : "null");
                        }
                    }
                    
                    if (topicSource.containsKey("keyPoints")) {
                        Object keyPointsObj = topicSource.get("keyPoints");
                        if (keyPointsObj instanceof java.util.List) {
                            topic.setKeyPoints((java.util.List<String>) keyPointsObj);
                        } else {
                            log.warn("Topic keyPoints field is not a List, found type: {}", 
                                    keyPointsObj != null ? keyPointsObj.getClass().getSimpleName() : "null");
                        }
                    }
                    
                    if (topicSource.containsKey("priority")) {
                        topic.setPriority(topicSource.get("priority").toString());
                    }
                    
                    if (topicSource.containsKey("keywords")) {
                        Object keywordsObj = topicSource.get("keywords");
                        if (keywordsObj instanceof java.util.List) {
                            topic.setKeywords((java.util.List<String>) keywordsObj);
                        } else {
                            log.warn("Topic keywords field is not a List, found type: {}", 
                                    keywordsObj != null ? keywordsObj.getClass().getSimpleName() : "null");
                        }
                    }
                    
                    dto.setTopic(topic);
                } else {
                    // Create a basic topic from string fields if available
                    EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
                    
                    if (result.containsKey("topic") && result.get("topic") instanceof String) {
                        topic.setName(result.get("topic").toString());
                    }
                    
                    if (result.containsKey("summary") && result.get("summary") instanceof String) {
                        topic.setSummary(result.get("summary").toString());
                    }
                    
                    dto.setTopic(topic);
                    
                    log.warn("Topic data not found in expected format, created basic topic from string fields");
                }
                
                // Set conversations - try metadata.conversations first (new format), then result.conversations (old format)
                Object conversationsSource = null;
                
                // Check if metadata exists and has conversations (new format from Gemini)
                if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
                    Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
                    if (metadata.containsKey("conversations")) {
                        conversationsSource = metadata.get("conversations");
                    }
                }
                
                // Fallback to result.conversations if metadata.conversations not found (old format)
                if (conversationsSource == null && result.containsKey("conversations")) {
                    conversationsSource = result.get("conversations");
                }
                
                if (conversationsSource instanceof java.util.List) {
                    try {
                        java.util.List<Map<String, Object>> conversationMaps = 
                                (java.util.List<Map<String, Object>>) conversationsSource;
                        
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
                    } catch (ClassCastException e) {
                        log.warn("Conversations field is a List but contains unexpected types: {}", e.getMessage());
                        // Set empty list as fallback
                        dto.setConversations(new java.util.ArrayList<>());
                    }
                } else if (conversationsSource != null) {
                    log.warn("Conversations field is not a List, found type: {}", 
                            conversationsSource.getClass().getSimpleName());
                    // Set empty list as fallback
                    dto.setConversations(new java.util.ArrayList<>());
                } else {
                    // No conversations found, set empty list
                    dto.setConversations(new java.util.ArrayList<>());
                }
                
                return dto;
            }
        } catch (Exception e) {
            log.error("Error converting message map to EnrichedMessageDTO: {}", e.getMessage(), e);
        }
        
        return null;
    }
    
    /**
     * Validate enriched message and ensure required fields are present
     */
    private boolean validateEnrichedMessage(EnrichedMessageDTO enrichedMessage) {
        boolean isValid = true;
        
        // Ensure conversations is not null - provide empty list if null
        if (enrichedMessage.getConversations() == null) {
            log.warn("Conversations field is null, setting to empty list for group ID: {}", enrichedMessage.getGroupId());
            enrichedMessage.setConversations(new java.util.ArrayList<>());
        }
        
        // Ensure peopleInvolved is not null - provide empty list if null
        if (enrichedMessage.getPeopleInvolved() == null) {
            log.warn("PeopleInvolved field is null, setting to empty list for group ID: {}", enrichedMessage.getGroupId());
            enrichedMessage.setPeopleInvolved(new java.util.ArrayList<>());
        }
        
        // Ensure topic is not null
        if (enrichedMessage.getTopic() == null) {
            log.error("Topic field is null for group ID: {}", enrichedMessage.getGroupId());
            
            // Create a basic topic with group ID as name if completely missing
            EnrichedMessageDTO.TopicDTO basicTopic = new EnrichedMessageDTO.TopicDTO();
            basicTopic.setName("Conversation-" + enrichedMessage.getGroupId());
            basicTopic.setSummary("Topic information not available");
            enrichedMessage.setTopic(basicTopic);
            
            log.warn("Created basic topic for group ID: {}", enrichedMessage.getGroupId());
        }
        
        // Ensure topic name is not null or empty
        if (enrichedMessage.getTopic().getName() == null || enrichedMessage.getTopic().getName().trim().isEmpty()) {
            log.warn("Topic name is null or empty for group ID: {}, setting default name", enrichedMessage.getGroupId());
            enrichedMessage.getTopic().setName("Conversation-" + enrichedMessage.getGroupId());
        }
        
        return isValid;
    }
}
