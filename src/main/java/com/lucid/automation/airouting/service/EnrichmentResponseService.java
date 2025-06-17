// package com.lucid.automation.airouting.service;

// import java.time.Instant;
// import java.time.LocalDateTime;
// import java.time.ZoneId;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.stream.Collectors;

// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// import org.springframework.amqp.rabbit.annotation.RabbitListener;
// import org.springframework.stereotype.Service;

// import com.lucid.automation.airouting.client.DataStorageServiceClient;
// import com.lucid.automation.airouting.dto.APIResponse;
// import com.lucid.automation.airouting.dto.EnrichedMessageDTO;
// import com.lucid.automation.airouting.dto.StoredDataDTO;
// import com.lucid.automation.airouting.dto.TopicDTO;
// import com.lucid.automation.airouting.model.message.EnrichmentResponse;

// /**
//  * Service for handling enrichment responses from RabbitMQ and storing them using the
//  * data-storage-service
//  */
// @Slf4j
// @Service
// @RequiredArgsConstructor
// public class EnrichmentResponseService {

//     private final DataStorageServiceClient dataStorageServiceClient;
//     private final ObjectMapper objectMapper;

//     /**
//      * Listener for conversation enrichment responses
//      * 
//      * @param enrichmentResponse the enriched message received from RabbitMQ
//      */
//     @RabbitListener(
//         queues = "${rabbitmq.queue.ai-responses:ai.responses.queue}",
//         concurrency = "1",
//         containerFactory = "rabbitListenerContainerFactory"
//     )
//     public void handleConversationEnrichmentResponse(EnrichmentResponse enrichmentResponse) {
//         try {
//             log.info("Received enriched message response: {}", enrichmentResponse);
            
//             if (!enrichmentResponse.isSuccess()) {
//                 log.error("Received failed enrichment response: {}", enrichmentResponse.getErrorMessage());
//                 return;
//             }

//             String tenantId = extractTenantId(enrichmentResponse);
//             String tenantSchema = extractTenantSchema(enrichmentResponse);

//             EnrichedMessageDTO enrichedMessage = convertToEnrichedMessageDTO(enrichmentResponse);
//             if (!isValidEnrichedMessage(enrichedMessage)) return;

//             Map<String, Object> metadata = extractAndPrepareMetadata(enrichmentResponse);
//             log.debug("DEBUG-ENRICHMENT: Calling data-storage-service with tenantId: {}, tenantSchema: {}, metadata keys: {}",
//                     tenantId, tenantSchema, metadata.keySet());

//             APIResponse<StoredDataDTO> response = saveEnrichedMessage(enrichedMessage, tenantId, tenantSchema, metadata);
//             handlePersistenceResult(response, enrichedMessage, metadata, tenantId, tenantSchema);
//         } catch (Exception e) {
//             log.error("Error handling enriched message response: {}", e.getMessage(), e);
//         }
//     }

//     private boolean isValidEnrichedMessage(EnrichedMessageDTO enrichedMessage) {
//         if (enrichedMessage == null || enrichedMessage.getGroupId() == null) {
//             log.error("Invalid enriched message format or missing group ID");
//             return false;
//         }
//         if (!validateEnrichedMessage(enrichedMessage)) {
//             log.error("Enriched message validation failed for group ID: {}", enrichedMessage.getGroupId());
//             return false;
//         }
//         log.info("DEBUG-ENRICHMENT: Processing enriched message for group ID: {}", enrichedMessage.getGroupId());
//         log.debug("DEBUG-ENRICHMENT: Enriched message details - conversations: {}, peopleInvolved: {}, topic: {}",
//                 enrichedMessage.getConversations() != null ? enrichedMessage.getConversations().size() : "null",
//                 enrichedMessage.getPeopleInvolved() != null ? enrichedMessage.getPeopleInvolved().size() : "null",
//                 enrichedMessage.getTopic() != null ? enrichedMessage.getTopic().getName() : "null");
//         return true;
//     }

//     private APIResponse<StoredDataDTO> saveEnrichedMessage(
//             EnrichedMessageDTO enrichedMessage,
//             String tenantId,
//             String tenantSchema,
//             Map<String, Object> metadata
//     ) {
//         try {
//             APIResponse<StoredDataDTO> response = dataStorageServiceClient.saveEnrichedMessage(
//                     enrichedMessage, tenantId, tenantSchema, metadata);
//             log.debug("DEBUG-ENRICHMENT: Received response from data-storage-service - success: {}, message: {}",
//                     response.isSuccess(), response.getMessage());
//             return response;
//         } catch (Exception clientException) {
//             log.error("DEBUG-ENRICHMENT: Exception calling data-storage-service client for group ID: {} - Exception type: {}, Message: {}",
//                     enrichedMessage.getGroupId(), clientException.getClass().getSimpleName(), clientException.getMessage(), clientException);
//             throw clientException;
//         }
//     }

//     private void handlePersistenceResult(
//             APIResponse<StoredDataDTO> response,
//             EnrichedMessageDTO enrichedMessage,
//             Map<String, Object> metadata,
//             String tenantId,
//             String tenantSchema
//     ) {
//         if (response.isSuccess()) {
//             log.info("DEBUG-ENRICHMENT: Successfully saved enriched message for group ID: {}, stored data ID: {}",
//                     enrichedMessage.getGroupId(), response.getData().getId());
//             persistTopic(metadata, enrichedMessage, tenantId, tenantSchema);
//         } else {
//             log.error("DEBUG-ENRICHMENT: Failed to save enriched message for group ID: {}, error: {}. " +
//                             "This might be due to validation issues. Check the enriched message format.",
//                     enrichedMessage.getGroupId(), response.getMessage());
//             log.debug("DEBUG-ENRICHMENT: Failed enriched message details: conversations={}, peopleInvolved={}, topic={}",
//                     enrichedMessage.getConversations(),
//                     enrichedMessage.getPeopleInvolved(),
//                     enrichedMessage.getTopic());
//         }
//     }    private void persistTopic(
//             Map<String, Object> metadata,
//             EnrichedMessageDTO enrichedMessage,
//             String tenantId,
//             String tenantSchema) {
//         try {
//             String topicId = null;
//             String topicName = null;
//             if (metadata.containsKey("topicId")) {
//                 topicId = metadata.get("topicId").toString();
//             }
            
//             if (metadata.containsKey("topic")) {
//                 topicName = metadata.get("topic").toString();
//             }
//             if (topicId == null && enrichedMessage.getTopic() != null && enrichedMessage.getTopic().getName() != null) {
//                 topicId = enrichedMessage.getTopic().getName();
//             }
//             if (topicName == null && enrichedMessage.getTopic() != null && enrichedMessage.getTopic().getName() != null) {
//                 topicName = enrichedMessage.getTopic().getName();
//             }
            
//             if (topicId != null && topicName != null) {
//                 TopicDTO.TopicDTOBuilder topicBuilder = TopicDTO.builder()
//                         .id(topicId)
//                         .name(topicName);
                
//                 // Populate enrichment fields from EnrichedMessageDTO.TopicDTO
//                 if (enrichedMessage.getTopic() != null) {
//                     EnrichedMessageDTO.TopicDTO enrichedTopic = enrichedMessage.getTopic();
                    
//                     // Set summary
//                     if (enrichedTopic.getSummary() != null && !enrichedTopic.getSummary().trim().isEmpty()) {
//                         topicBuilder.summary(enrichedTopic.getSummary());
//                     }
                    
//                     // Convert List<String> category to single String (use first category or join with comma)
//                     if (enrichedTopic.getCategory() != null && !enrichedTopic.getCategory().isEmpty()) {
//                         String categoryStr = enrichedTopic.getCategory().stream()
//                                 .filter(cat -> cat != null && !cat.trim().isEmpty())
//                                 .collect(Collectors.joining(", "));
//                         if (!categoryStr.isEmpty()) {
//                             topicBuilder.category(categoryStr);
//                         }
//                     }
                    
//                     // Combine keyPoints and keywords for suggested actions
//                     StringBuilder suggestedActions = new StringBuilder();
//                     if (enrichedTopic.getKeyPoints() != null && !enrichedTopic.getKeyPoints().isEmpty()) {
//                         suggestedActions.append("Key Points: ")
//                                 .append(enrichedTopic.getKeyPoints().stream()
//                                         .filter(point -> point != null && !point.trim().isEmpty())
//                                         .collect(Collectors.joining("; ")));
//                     }
//                     if (enrichedTopic.getKeywords() != null && !enrichedTopic.getKeywords().isEmpty()) {
//                         if (suggestedActions.length() > 0) {
//                             suggestedActions.append(" | ");
//                         }
//                         suggestedActions.append("Keywords: ")
//                                 .append(enrichedTopic.getKeywords().stream()
//                                         .filter(keyword -> keyword != null && !keyword.trim().isEmpty())
//                                         .collect(Collectors.joining(", ")));
//                     }
//                     if (suggestedActions.length() > 0) {
//                         topicBuilder.suggestedActions(suggestedActions.toString());
//                     }
//                 }
                
//                 // Map peopleInvolved to participants JSON string
//                 if (enrichedMessage.getPeopleInvolved() != null && !enrichedMessage.getPeopleInvolved().isEmpty()) {
//                     try {
//                         String participantsJson = objectMapper.writeValueAsString(enrichedMessage.getPeopleInvolved());
//                         topicBuilder.participants(participantsJson);
//                     } catch (JsonProcessingException e) {
//                         log.warn("Failed to serialize participants to JSON for topic: {}", topicId, e);
//                         // Fallback to simple string join
//                         String participantsStr = enrichedMessage.getPeopleInvolved().stream()
//                                 .filter(person -> person != null && !person.trim().isEmpty())
//                                 .collect(Collectors.joining(", "));
//                         topicBuilder.participants(participantsStr);
//                     }
//                 }
                
//                 // Derive start and end times from metadata timestamps
//                 if (metadata.containsKey("processingTimestamp")) {
//                     try {
//                         Object timestamp = metadata.get("processingTimestamp");
//                         LocalDateTime dateTime = null;
                        
//                         if (timestamp instanceof Long) {
//                             dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) timestamp), ZoneId.systemDefault());
//                         } else if (timestamp instanceof String) {
//                             // Try to parse as milliseconds
//                             try {
//                                 long timestampLong = Long.parseLong((String) timestamp);
//                                 dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampLong), ZoneId.systemDefault());
//                             } catch (NumberFormatException e) {
//                                 log.debug("Could not parse timestamp as long: {}", timestamp);
//                             }
//                         }
                        
//                         if (dateTime != null) {
//                             // Use processing timestamp as both start and end time (can be refined later)
//                             topicBuilder.startTime(dateTime);
//                             topicBuilder.endTime(dateTime);
//                         }
//                     } catch (Exception e) {
//                         log.debug("Failed to parse processingTimestamp for topic: {}", topicId, e);
//                     }
//                 }
                
//                 TopicDTO topicDTO = topicBuilder.build();
//                 dataStorageServiceClient.createTopic(topicDTO, tenantId, tenantSchema);
//                 log.info("Persisted enriched topic to datastorage-service: topicId={}, topic={}, summary={}, category={}, participants={}", 
//                         topicId, topicName, topicDTO.getSummary(), topicDTO.getCategory(), 
//                         topicDTO.getParticipants() != null ? topicDTO.getParticipants().length() + " chars" : "null");
//             } else {
//                 log.warn("Could not persist topic: topicId or topic name missing (topicId={}, topic={})", topicId, topicName);
//             }
//         } catch (Exception ex) {
//             log.error("Failed to persist topic to datastorage-service", ex);
//         }
//     }
    
//     /**
//      * Extract tenant ID from the message
//      */
//     private String extractTenantId(Map<String, Object> messageMap) {
//         if (messageMap.containsKey("tenantId")) {
//             return messageMap.get("tenantId").toString();
//         }
//         return "default"; // Fallback to default tenant
//     }
    
//     /**
//      * Extract tenant schema from the message
//      */
//     private String extractTenantSchema(Map<String, Object> messageMap) {
//         if (messageMap.containsKey("tenantSchema")) {
//             return messageMap.get("tenantSchema").toString();
//         }
//         return "public"; // Fallback to public schema
//     }
    
//     /**
//      * Convert the message map to EnrichedMessageDTO
//      */
//     @SuppressWarnings("unchecked")
//     private EnrichedMessageDTO convertToEnrichedMessageDTO(Map<String, Object> messageMap) {
//         try {
//             if (messageMap.containsKey("result")) {
//                 Map<String, Object> result = (Map<String, Object>) messageMap.get("result");
                
//                 // Check if result is not null
//                 if (result == null) {
//                     log.error("Result map is null in message: {}", messageMap);
//                     return null;
//                 }
                
//                 // Build the EnrichedMessageDTO from the result map
//                 EnrichedMessageDTO dto = new EnrichedMessageDTO();
                
//                 // Set group ID - try multiple possible field names
//                 String groupId = null;
//                 if (messageMap.containsKey("conversationId")) {
//                     groupId = messageMap.get("conversationId").toString();
//                 } else if (messageMap.containsKey("correlationId")) {
//                     groupId = messageMap.get("correlationId").toString();
//                 } else if (messageMap.containsKey("messageId")) {
//                     groupId = messageMap.get("messageId").toString();
//                 }
                
//                 if (groupId != null) {
//                     dto.setGroupId(groupId);
//                 } else {
//                     log.warn("No suitable group ID field found in message: {}", messageMap.keySet());
//                 }
                
//                 // Set people involved - try metadata.peopleInvolved first (new format), then result.peopleInvolved (old format)
//                 Object peopleInvolvedSource = null;
                
//                 // Check if metadata exists and has peopleInvolved (new format from Gemini)
//                 if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
//                     Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
//                     if (metadata.containsKey("peopleInvolved")) {
//                         peopleInvolvedSource = metadata.get("peopleInvolved");
//                     }
//                 }
                
//                 // Fallback to result.peopleInvolved if metadata.peopleInvolved not found (old format)
//                 if (peopleInvolvedSource == null && result.containsKey("peopleInvolved")) {
//                     peopleInvolvedSource = result.get("peopleInvolved");
//                 }
                
//                 if (peopleInvolvedSource instanceof java.util.List) {
//                     dto.setPeopleInvolved((java.util.List<String>) peopleInvolvedSource);
//                 } else if (peopleInvolvedSource != null) {
//                     log.warn("PeopleInvolved field is not a List, found type: {}", 
//                             peopleInvolvedSource.getClass().getSimpleName());
//                     // Set empty list as fallback
//                     dto.setPeopleInvolved(new java.util.ArrayList<>());
//                 } else {
//                     // No peopleInvolved found, set empty list
//                     dto.setPeopleInvolved(new java.util.ArrayList<>());
//                 }
                
//                 // Set topic - try metadata.topic first (new format), then result.topic (old format)
//                 Map<String, Object> topicSource = null;
                
//                 // Check if metadata exists and has topic (new format from Gemini)
//                 if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
//                     Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
//                     if (metadata.containsKey("topic") && metadata.get("topic") instanceof Map) {
//                         topicSource = (Map<String, Object>) metadata.get("topic");
//                     }
//                 }
                
//                 // Fallback to result.topic if metadata.topic not found (old format)
//                 if (topicSource == null && result.containsKey("topic") && result.get("topic") instanceof Map) {
//                     topicSource = (Map<String, Object>) result.get("topic");
//                 }
                
//                 if (topicSource != null) {
//                     EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
                
//                     if (topicSource.containsKey("name")) {
//                         topic.setName(topicSource.get("name").toString());
//                     }
                    
//                     if (topicSource.containsKey("summary")) {
//                         topic.setSummary(topicSource.get("summary").toString());
//                     }
                    
//                     if (topicSource.containsKey("category")) {
//                         Object categoryObj = topicSource.get("category");
//                         if (categoryObj instanceof java.util.List) {
//                             topic.setCategory((java.util.List<String>) categoryObj);
//                         } else {
//                             log.warn("Topic category field is not a List, found type: {}", 
//                                     categoryObj != null ? categoryObj.getClass().getSimpleName() : "null");
//                         }
//                     }
                    
//                     if (topicSource.containsKey("subCategory") || topicSource.containsKey("sub-category")) {
//                         Object subCategoryObj = topicSource.containsKey("subCategory") ? 
//                                 topicSource.get("subCategory") : topicSource.get("sub-category");
//                         if (subCategoryObj instanceof java.util.List) {
//                             topic.setSubCategory((java.util.List<String>) subCategoryObj);
//                         } else {
//                             log.warn("Topic subCategory field is not a List, found type: {}", 
//                                     subCategoryObj != null ? subCategoryObj.getClass().getSimpleName() : "null");
//                         }
//                     }
                    
//                     if (topicSource.containsKey("keyPoints")) {
//                         Object keyPointsObj = topicSource.get("keyPoints");
//                         if (keyPointsObj instanceof java.util.List) {
//                             topic.setKeyPoints((java.util.List<String>) keyPointsObj);
//                         } else {
//                             log.warn("Topic keyPoints field is not a List, found type: {}", 
//                                     keyPointsObj != null ? keyPointsObj.getClass().getSimpleName() : "null");
//                         }
//                     }
                    
//                     if (topicSource.containsKey("priority")) {
//                         topic.setPriority(topicSource.get("priority").toString());
//                     }
                    
//                     if (topicSource.containsKey("keywords")) {
//                         Object keywordsObj = topicSource.get("keywords");
//                         if (keywordsObj instanceof java.util.List) {
//                             topic.setKeywords((java.util.List<String>) keywordsObj);
//                         } else {
//                             log.warn("Topic keywords field is not a List, found type: {}", 
//                                     keywordsObj != null ? keywordsObj.getClass().getSimpleName() : "null");
//                         }
//                     }
                    
//                     dto.setTopic(topic);
//                 } else {
//                     // Create a basic topic from string fields if available
//                     EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
                    
//                     if (result.containsKey("topic") && result.get("topic") instanceof String) {
//                         topic.setName(result.get("topic").toString());
//                     }
                    
//                     if (result.containsKey("summary") && result.get("summary") instanceof String) {
//                         topic.setSummary(result.get("summary").toString());
//                     }
                    
//                     dto.setTopic(topic);
                    
//                     log.warn("Topic data not found in expected format, created basic topic from string fields");
//                 }
                
//                 // Set conversations - try metadata.conversations first (new format), then result.conversations (old format)
//                 Object conversationsSource = null;
                
//                 // Check if metadata exists and has conversations (new format from Gemini)
//                 if (result.containsKey("metadata") && result.get("metadata") instanceof Map) {
//                     Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
//                     if (metadata.containsKey("conversations")) {
//                         conversationsSource = metadata.get("conversations");
//                     }
//                 }
                
//                 // Fallback to result.conversations if metadata.conversations not found (old format)
//                 if (conversationsSource == null && result.containsKey("conversations")) {
//                     conversationsSource = result.get("conversations");
//                 }
                
//                 if (conversationsSource instanceof java.util.List) {
//                     try {
//                         java.util.List<Map<String, Object>> conversationMaps = 
//                                 (java.util.List<Map<String, Object>>) conversationsSource;
                        
//                         java.util.List<EnrichedMessageDTO.ConversationDTO> conversations = new java.util.ArrayList<>();
                        
//                         for (Map<String, Object> convMap : conversationMaps) {
//                             EnrichedMessageDTO.ConversationDTO conv = new EnrichedMessageDTO.ConversationDTO();
                            
//                             if (convMap.containsKey("text")) {
//                                 conv.setText(convMap.get("text").toString());
//                             }
                            
//                             if (convMap.containsKey("relevance")) {
//                                 conv.setRelevance(convMap.get("relevance").toString());
//                             }
                            
//                             conversations.add(conv);
//                         }
                        
//                         dto.setConversations(conversations);
//                     } catch (ClassCastException e) {
//                         log.warn("Conversations field is a List but contains unexpected types: {}", e.getMessage());
//                         // Set empty list as fallback
//                         dto.setConversations(new java.util.ArrayList<>());
//                     }
//                 } else if (conversationsSource != null) {
//                     log.warn("Conversations field is not a List, found type: {}", 
//                             conversationsSource.getClass().getSimpleName());
//                     // Set empty list as fallback
//                     dto.setConversations(new java.util.ArrayList<>());
//                 } else {
//                     // No conversations found, set empty list
//                     dto.setConversations(new java.util.ArrayList<>());
//                 }
                
//                 return dto;
//             }
//         } catch (Exception e) {
//             log.error("Error converting message map to EnrichedMessageDTO: {}", e.getMessage(), e);
//         }
        
//         return null;
//     }
    
//     /**
//      * Validate enriched message and ensure required fields are present
//      */
//     private boolean validateEnrichedMessage(EnrichedMessageDTO enrichedMessage) {
//         boolean isValid = true;
        
//         // Ensure conversations is not null - provide empty list if null
//         if (enrichedMessage.getConversations() == null) {
//             log.warn("Conversations field is null, setting to empty list for group ID: {}", enrichedMessage.getGroupId());
//             enrichedMessage.setConversations(new java.util.ArrayList<>());
//         }
        
//         // Ensure peopleInvolved is not null - provide empty list if null
//         if (enrichedMessage.getPeopleInvolved() == null) {
//             log.warn("PeopleInvolved field is null, setting to empty list for group ID: {}", enrichedMessage.getGroupId());
//             enrichedMessage.setPeopleInvolved(new java.util.ArrayList<>());
//         }
        
//         // Ensure topic is not null
//         if (enrichedMessage.getTopic() == null) {
//             log.error("Topic field is null for group ID: {}", enrichedMessage.getGroupId());
            
//             // Create a basic topic with group ID as name if completely missing
//             EnrichedMessageDTO.TopicDTO basicTopic = new EnrichedMessageDTO.TopicDTO();
//             basicTopic.setName("Conversation-" + enrichedMessage.getGroupId());
//             basicTopic.setSummary("Topic information not available");
//             enrichedMessage.setTopic(basicTopic);
            
//             log.warn("Created basic topic for group ID: {}", enrichedMessage.getGroupId());
//         }
        
//         // Ensure topic name is not null or empty
//         if (enrichedMessage.getTopic().getName() == null || enrichedMessage.getTopic().getName().trim().isEmpty()) {
//             log.warn("Topic name is null or empty for group ID: {}, setting default name", enrichedMessage.getGroupId());
//             enrichedMessage.getTopic().setName("Conversation-" + enrichedMessage.getGroupId());
//         }
        
//         return isValid;
//     }

//     /**
//      * Extract tenant ID from the enrichment response
//      */
//     private String extractTenantId(EnrichmentResponse enrichmentResponse) {
//         if (enrichmentResponse.getMetadata() != null && enrichmentResponse.getMetadata().containsKey("tenantId")) {
//             return enrichmentResponse.getMetadata().get("tenantId").toString();
//         }
//         return "default"; // Fallback to default tenant
//     }
    
//     /**
//      * Extract tenant schema from the enrichment response
//      */
//     private String extractTenantSchema(EnrichmentResponse enrichmentResponse) {
//         if (enrichmentResponse.getMetadata() != null && enrichmentResponse.getMetadata().containsKey("tenantSchema")) {
//             return enrichmentResponse.getMetadata().get("tenantSchema").toString();
//         }
//         return "public"; // Fallback to public schema
//     }

//     private Map<String, Object> extractAndPrepareMetadata(EnrichmentResponse enrichmentResponse) {
//         Map<String, Object> metadata = new HashMap<>();
//         if (enrichmentResponse.getMetadata() != null) {
//             metadata.putAll(enrichmentResponse.getMetadata());
//         }
//         metadata.putIfAbsent("source", "ai-enrichment");
//         metadata.putIfAbsent("processingTimestamp", System.currentTimeMillis());
//         metadata.putIfAbsent("messageId", enrichmentResponse.getMessageId());
//         metadata.putIfAbsent("correlationId", enrichmentResponse.getCorrelationId());
//         metadata.putIfAbsent("conversationId", enrichmentResponse.getConversationId());
//         metadata.putIfAbsent("taskType", enrichmentResponse.getTaskType());
//         metadata.putIfAbsent("providerId", enrichmentResponse.getProviderId());
//         metadata.putIfAbsent("confidence", enrichmentResponse.getConfidence());
//         metadata.putIfAbsent("processedAt", enrichmentResponse.getProcessedAt());
//         metadata.putIfAbsent("processingTimeMs", enrichmentResponse.getProcessingTimeMs());
//         return metadata;
//     }

//     /**
//      * Convert the EnrichmentResponse to EnrichedMessageDTO
//      */
//     private EnrichedMessageDTO convertToEnrichedMessageDTO(EnrichmentResponse enrichmentResponse) {
//         try {
//             if (enrichmentResponse.getResult() == null) {
//                 log.error("Result is null in enrichment response: {}", enrichmentResponse);
//                 return null;
//             }

//             EnrichedMessageDTO dto = new EnrichedMessageDTO();
            
//             // Set group ID from various fields
//             String groupId = enrichmentResponse.getConversationId();
//             if (groupId == null) {
//                 groupId = enrichmentResponse.getCorrelationId();
//             }
//             if (groupId == null) {
//                 groupId = enrichmentResponse.getMessageId();
//             }
            
//             if (groupId != null) {
//                 dto.setGroupId(groupId);
//             } else {
//                 log.warn("No suitable group ID field found in enrichment response");
//             }

//             // Convert ConversationEnrichment to EnrichedMessageDTO
//             // For now, we'll need to adapt the structure based on the available data
//             // This is a simplified conversion - you may need to adjust based on your specific needs
            
//             // Set people involved - extract from topics if available
//             if (enrichmentResponse.getResult().topics() != null && !enrichmentResponse.getResult().topics().isEmpty()) {
//                 // Get people involved from the first topic (or combine from all topics)
//                 var peopleInvolved = enrichmentResponse.getResult().topics().get(0).peopleInvolved();
//                 dto.setPeopleInvolved(peopleInvolved != null ? peopleInvolved : new java.util.ArrayList<>());
                
//                 // Create topic from the first TopicEnrichment
//                 var topicEnrichment = enrichmentResponse.getResult().topics().get(0);
//                 EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
//                 topic.setName(topicEnrichment.title());
//                 topic.setSummary(topicEnrichment.summary());
//                 topic.setCategory(java.util.List.of(topicEnrichment.category()));
//                 // Add more fields as needed
//                 dto.setTopic(topic);
//             } else {
//                 dto.setPeopleInvolved(new java.util.ArrayList<>());
//                 // Create a basic topic
//                 EnrichedMessageDTO.TopicDTO topic = new EnrichedMessageDTO.TopicDTO();
//                 topic.setName("Unknown Topic");
//                 dto.setTopic(topic);
//             }

//             // Set conversations - convert from messages if available
//             if (enrichmentResponse.getResult().messages() != null) {
//                 var conversations = enrichmentResponse.getResult().messages().stream()
//                     .map(messageEnrichment -> {
//                         EnrichedMessageDTO.ConversationDTO conversation = new EnrichedMessageDTO.ConversationDTO();
//                         // MessageEnrichment doesn't have content, using category as text placeholder
//                         conversation.setText("Category: " + messageEnrichment.category() + 
//                                            ", Intent: " + messageEnrichment.intent() + 
//                                            ", Sentiment: " + messageEnrichment.sentiment());
//                         conversation.setRelevance("high"); // Default relevance
//                         return conversation;
//                     })
//                     .collect(java.util.stream.Collectors.toList());
//                 dto.setConversations(conversations);
//             } else {
//                 dto.setConversations(new java.util.ArrayList<>());
//             }

//             return dto;
//         } catch (Exception e) {
//             log.error("Error converting enrichment response to EnrichedMessageDTO: {}", e.getMessage(), e);
//             return null;
//         }
//     }
// }
