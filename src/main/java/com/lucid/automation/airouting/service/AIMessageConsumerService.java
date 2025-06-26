package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.lucid.automation.airouting.dto.ConversationEnrichment;
import com.lucid.automation.airouting.dto.SuggestedReply;
import com.lucid.automation.airouting.dto.SummaryPerPerson;
import com.lucid.automation.airouting.dto.TopicEnrichment;
import com.lucid.automation.airouting.dto.UserDTO;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import com.lucid.automation.airouting.provider.ProviderUtils;
import com.lucid.automation.airouting.util.JsonUtils;
import com.lucid.automation.airouting.util.TextUtils;
import com.lucid.automation.airouting.dto.ForwardInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.dto.UrgencyLevel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Service for consuming AI processing requests from Kafka and processing them
 */
@Service
public class AIMessageConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIMessageConsumerService.class);
    
    private final AIProviderFactory providerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topics.pre-ai-responses:pre.ai.responses.queue}")
    private String preAiResponsesTopic;
    
    @Value("${kafka.topics.ai-responses:ai.responses.queue}")
    private String aiResponsesTopic;

    private final UserService userService;
    private final ObjectMapper objectMapper;

    
    public AIMessageConsumerService(AIProviderFactory providerFactory, UserService userService, ObjectMapper objectMapper,
                                  KafkaTemplate<String, Object> kafkaTemplate) {
        this.providerFactory = providerFactory;
        this.userService = userService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        
        // Log the guarantee about ai-enrich processing
        logger.info("=== AI-ENRICH PROCESSING GUARANTEE ===");
        logger.info("AIMessageConsumerService initialized");
        logger.info("GUARANTEE: ai-enrich topic will ALWAYS be processed from the beginning");
        logger.info("Mechanisms ensuring this:");
        logger.info("1. aiMessageConsumerFactory with auto-offset-reset=earliest");
        logger.info("2. KafkaOffsetResetService resets offsets on startup");
        logger.info("3. Dedicated consumer group: {}-ai-enrich", "ai-routing-service-group");
        logger.info("======================================");
    }

    /**
     * Consume enrichment requests (conversation, message, participant analysis, etc.)
     * GUARANTEE: This consumer ALWAYS processes ai-enrich topic from the beginning
     * due to the dedicated aiMessageConsumerFactory configuration with auto-offset-reset=earliest
     * and KafkaOffsetResetService that resets offsets on startup.
     */
    @KafkaListener(topics = "${kafka.topics.ai-enrich:ai-enrich}", containerFactory = "aiMessageListenerContainerFactory")
    public void handleEnrichmentRequest(@Payload AIMessage messageRequest,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      Acknowledgment acknowledgment) {
        if (messageRequest == null) {
            logger.error("=== HANDLE-ENRICHMENT-ERROR === Received NULL message from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }
                   
        logger.info("[X] Received enrichment request: messageId={}, taskType={}, tenantId={}", 
                   messageRequest.getMessageId(), messageRequest.getTaskType(), messageRequest.getTenantId());
        
        // Set default replyTopic if not specified
        if (messageRequest.getReplyTopic() == null || messageRequest.getReplyTopic().trim().isEmpty()) {
            logger.info("No reply topic specified for messageId={}, setting default ai-responses topic", 
                       messageRequest.getMessageId());
            messageRequest.setReplyTopic(preAiResponsesTopic);
        }
        
        try {            
            // Handle null or empty preferred provider
            String preferredProvider = messageRequest.getPreferredProvider();
            if (preferredProvider == null || preferredProvider.trim().isEmpty()) {
                logger.warn("No preferred provider specified for messageId={}, using default provider", messageRequest.getMessageId());
                preferredProvider = null; // This will trigger default provider selection
            }
            
            AIProvider provider = preferredProvider != null ? 
                providerFactory.getProvider(preferredProvider) : 
                providerFactory.getDefaultProvider();
                
            if (provider == null) {
                throw new RuntimeException("No AI provider available for processing enrichment request");
            }
                        
            Object result = processEnrichmentTask(provider, messageRequest); // String
            sendResponse(messageRequest, result, messageRequest.getTaskType().toString().toLowerCase());
            acknowledgment.acknowledge();            
        } catch (Exception e) {
            e.printStackTrace(); // Print full stack trace to console
            sendErrorResponse(messageRequest, messageRequest.getTaskType().toString().toLowerCase(), e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    private Object processEnrichmentTask(AIProvider provider, AIMessage message) {
        AITaskType taskType = message.getTaskType();
        
        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                yield provider.enrichConversation(message.getMessages());
            }
            default -> throw new IllegalArgumentException("Unsupported enrichment task type: " + taskType);
        };
    }
    
    private void sendResponse(AIMessage originalMessage, Object result, String taskType) {
        String replyTopic = originalMessage.getReplyTopic();
        if (replyTopic == null || replyTopic.trim().isEmpty()) {
            logger.error("CRITICAL: Reply topic is null/empty even after setting default! messageId={}", originalMessage.getMessageId());
            return;
        }

        try {
            Map<String, Object> response = new HashMap<>();
            response.put("messageId", originalMessage.getMessageId());
            response.put("correlationId", originalMessage.getCorrelationId());
            response.put("taskType", taskType);
            response.put("status", "success");
            response.put("result", result);
            response.put("processedAt", LocalDateTime.now());
            response.put("tenantId", originalMessage.getTenantId());
            response.put("tenantSchema", originalMessage.getTenantSchema());
            response.put("userId", originalMessage.getUserId());
            
            kafkaTemplate.send(replyTopic, response);
        } catch (Exception e) {
            logger.error("Failed to send response to reply topic: messageId={}, replyTopic={}, error={}", originalMessage.getMessageId(), replyTopic, e.getMessage(), e);
        }
    }
    
    private void sendErrorResponse(AIMessage originalMessage, String taskType, String errorMessage) {
        String replyTopic = originalMessage.getReplyTopic();
        
        // The replyTopic should always be set by now (in handleEnrichmentRequest)
        if (replyTopic == null || replyTopic.trim().isEmpty()) {
            logger.error("CRITICAL: Reply topic is null/empty even after setting default for error response! messageId={}", 
                        originalMessage.getMessageId());
            return;
        }
        
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("messageId", originalMessage.getMessageId());
            response.put("correlationId", originalMessage.getCorrelationId());
            response.put("taskType", taskType);
            response.put("status", "error");
            response.put("error", errorMessage);
            response.put("processedAt", LocalDateTime.now());
            response.put("tenantId", originalMessage.getTenantId());
            response.put("tenantSchema", originalMessage.getTenantSchema());
            response.put("userId", originalMessage.getUserId());
            
            kafkaTemplate.send(replyTopic, response);
            
            // Log whether this was sent to the default ai-responses topic
            if (preAiResponsesTopic.equals(replyTopic)) {
                logger.info("ERROR: Sent error response to ai-responses topic: messageId={}, topic={}", 
                           originalMessage.getMessageId(), replyTopic);
            } else {
                logger.info("Sent error response to reply topic: messageId={}, replyTopic={}", 
                           originalMessage.getMessageId(), replyTopic);
            }
            
        } catch (Exception e) {
            logger.error("Failed to send error response to reply topic: messageId={}, replyTopic={}, error={}", 
                        originalMessage.getMessageId(), replyTopic, e.getMessage(), e);
        }
    }
    
    /**
     * Consume pre-AI responses for further processing
     * This consumer processes messages from the pre-ai-responses topic that need additional AI processing
     */
    @KafkaListener(topics = "${kafka.topics.pre-ai-responses:pre.ai.responses.queue}", containerFactory = "genericObjectListenerContainerFactory")
    public void handlePreAiResponses(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment) {
        Object messageResponse = record.value();
        String topic = record.topic();
        
        if (messageResponse == null) {
            logger.error("=== HANDLE-PRE-AI-RESPONSES-ERROR === Received NULL message from topic: {}", topic);
            acknowledgment.acknowledge();
            return;
        }

        try {
            // Validate that the message is a Map
            if (!(messageResponse instanceof Map<?, ?>)) {
                logger.error("Invalid pre-AI response format: expected Map, got {}", messageResponse.getClass().getSimpleName());
                acknowledgment.acknowledge();
                return;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> responseMap = (Map<String, Object>) messageResponse;
            
            Map<String, Object> parsedResponse = processPreAiResponse(responseMap);
            sendToFinalAiResponsesTopic(parsedResponse);
            acknowledgment.acknowledge();
            logger.info("Successfully processed pre-AI response and forwarded to final topic");
            
        } catch (Exception e) {
            logger.error("Failed to process pre-AI response from topic: {}, error: {}", topic, e.getMessage(), e);
            e.printStackTrace(); // Print full stack trace to console
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Process pre-AI response message
     * This method can be extended to perform additional AI processing, validation, or enrichment
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> processPreAiResponse(Map<String, Object> messageResponse) {
        logger.info("Processing pre-AI response: {}", messageResponse);
        if (!(messageResponse instanceof Map<?, ?>)) {
            logger.error("Invalid pre-AI response format: expected Map, got {}", messageResponse.getClass().getSimpleName());
            return Map.of("error", "Invalid pre-AI response format");
        }
        
        Map<String, Object> responseMap = (Map<String, Object>) messageResponse;
        
        // Extract necessary fields from the response
        String messageId = (String) responseMap.get("messageId");
        String correlationId = (String) responseMap.get("correlationId");
        String taskType = (String) responseMap.get("taskType");
        String tenantId = (String) responseMap.get("tenantId");
        String tenantSchema = (String) responseMap.get("tenantSchema");
        String userId = (String) responseMap.get("userId");
        
        // extract result as a String
        Map<String, Object> aiResultMap = (Map<String, Object>) responseMap.get("result");

        // Convert the request objects from LinkedHashMap to SlackMessage objects
        List<Map<String, Object>> requestMapList = (List<Map<String, Object>>) aiResultMap.get("request");
        List<SlackMessage> requestMessages;
        try {
            requestMessages = convertToSlackMessages(requestMapList);
        } catch (Exception e) {
            logger.error("Failed to convert request maps to SlackMessage objects: {}", e.getMessage());
            requestMessages = new ArrayList<>();
        }
        String responseResult = (String) aiResultMap.get("response");
        ConversationEnrichment parsedResult = parseConversationEnrichmentResponse(responseResult, requestMessages);

        // Handle processedAt field - it could be an array or a string
        Object processedAtObj = responseMap.get("processedAt");
        LocalDateTime processedAtTime;
        if (processedAtObj instanceof List) {
            // Handle array format: [2025, 6, 25, 10, 7, 25, 754055364]
            List<Integer> timeArray = (List<Integer>) processedAtObj;
            if (timeArray.size() >= 6) {
                processedAtTime = LocalDateTime.of(
                    timeArray.get(0), // year
                    timeArray.get(1), // month
                    timeArray.get(2), // day
                    timeArray.get(3), // hour
                    timeArray.get(4), // minute
                    timeArray.get(5)  // second
                    // Note: nanoseconds (7th element) are ignored for simplicity
                );
            } else {
                processedAtTime = LocalDateTime.now();
            }
        } else if (processedAtObj instanceof String) {
            // Handle string format
            processedAtTime = LocalDateTime.parse((String) processedAtObj);
        } else {
            processedAtTime = LocalDateTime.now();
        }

        // Create a new response map with the processed data
        Map<String, Object> processedResponse = new HashMap<>();
        processedResponse.put("messageId", messageId);
        processedResponse.put("correlationId", correlationId);
        processedResponse.put("taskType", taskType);
        processedResponse.put("status", "success");
        processedResponse.put("result", parsedResult);
        processedResponse.put("processedAt", processedAtTime);
        processedResponse.put("tenantId", tenantId);
        processedResponse.put("tenantSchema", tenantSchema);
        processedResponse.put("userId", userId);
        // Log the processed response
        return processedResponse;
    }
    
    /**
     * Send processed message to the final ai-responses topic
     */
    private void sendToFinalAiResponsesTopic(Object parsedAiResponse) {
        try {
            // Send to the final ai-responses topic
            kafkaTemplate.send(aiResponsesTopic, parsedAiResponse);
            logger.info("Successfully forwarded message to final ai-responses topic: {}", aiResponsesTopic);
        } catch (Exception e) {
            logger.error("Failed to send message to final ai-responses topic: {}, error: {}", aiResponsesTopic, e.getMessage(), e);
        }
    }

    private ConversationEnrichment parseConversationEnrichmentResponse(String response, List<SlackMessage> messages) {
        try {
            String cleanedResponse = JsonUtils.cleanJsonResponse(response);
            logger.info("GEMINI-PARSE: Cleaned response:\n{}", cleanedResponse);

            if (cleanedResponse.trim().startsWith("[")) {
                return parseTopicsFromText(cleanedResponse, messages);
            }

            return new ConversationEnrichment(
                List.of(), // No topics
                List.of(), // No action items
                List.of(), // No messages
                null // No urgency level
            );
        } catch (Exception e) {
            logger.error("GEMINI-PARSE: Failed to parse conversation enrichment response. Error: {}", e.getMessage(), e);
            return getDefaultConversationEnrichment();
        }
    }

    private ConversationEnrichment parseTopicsFromText(String topicsText, List<SlackMessage> messages) 
        throws JsonMappingException, JsonProcessingException {

        List<?> topicsArray = objectMapper.readValue(topicsText, List.class);
        List<TopicEnrichment> topics = new ArrayList<>();
        for (int i = 0; i < topicsArray.size(); i++) {
            Object topicObj = topicsArray.get(i);            
            if (topicObj instanceof Map<?, ?> topicMap) {
                TopicEnrichment topic = parseTopicFromMap(topicMap, messages);
                topics.add(topic);
                logger.info("GEMINI-PARSE: Successfully parsed topic {}: '{}'", i, topic.title());
            }
        }
        return new ConversationEnrichment(topics, List.of(), List.of(), null);
    }

    
    private String extractStringValue(Map<?, ?> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value instanceof String str ? str : defaultValue;
    }
    
    private Integer extractIntegerValue(Map<?, ?> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value instanceof Integer intValue) {
            return intValue;
        } else if (value instanceof String strValue) {
            try {
                return Integer.parseInt(strValue);
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse integer value '{}' for key '{}', using default: {}", strValue, key, defaultValue);
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    private ConversationEnrichment getDefaultConversationEnrichment() {
        TopicEnrichment defaultTopic = new TopicEnrichment(
            "General Discussion",
            "No summary available", 
            "No detailed summary available",
            "No action suggested",
            null, // clientOrSupplier
            null, // deadline
            UrgencyLevel.LOW,
            "General", // category
            null, // subCategory
            null, // startTime
            null, // endTime
            null, // periodStartDate
            null, // periodEndDate
            null, // latestMessageDate
            List.of(), // peopleInvolved
            List.of(), // summaryPerPerson
            Map.of(), // lastMessageDatePerPerson
            List.of(), // suggestedReplies
            null // suggestedForwardRecipient
        );
        
        return new ConversationEnrichment(
            List.of(defaultTopic),
            List.of(),
            List.of(),
            Map.of("error", "Failed to analyze conversation")
        );
    }
    
    private TopicEnrichment parseTopicFromMap(Map<?, ?> topicMap, List<SlackMessage> messages) {
        Map<String, UserDTO> userInfos = messages.stream().filter(msg -> {
                String key = msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername();
                return key != null && !key.trim().isEmpty();
            }).collect(Collectors.toMap(
                msg -> msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername(),
                msg -> new UserDTO(msg.getSlackUserId(), msg.getUsername(), msg.getDisplayName(), msg.getImage72()),
                (existing, replacement) -> existing // Keep existing if duplicate
            ));

        String tenantId = messages.isEmpty() ? null : messages.get(0).getTenantId();
        String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();
        updateUserInformation(userInfos, tenantId, workspaceId);

        String title = extractStringValue(topicMap, "title", "Untitled Topic");
        String shortSummary = extractStringValue(topicMap, "shortSummary", "No summary available");
        shortSummary = TextUtils.replaceSlackMentions(shortSummary, userInfos);
        
        String fullSummary = extractStringValue(topicMap, "fullSummary", "No detailed summary available");
        fullSummary = TextUtils.replaceSlackMentions(fullSummary, userInfos);

        String suggestedAction = extractStringValue(topicMap, "suggestedAction", "No action suggested");
        suggestedAction = TextUtils.replaceSlackMentions(suggestedAction, userInfos);

        String clientOrSupplier = extractStringValue(topicMap, "clientOrSupplier", null);
        String deadlineStr = extractStringValue(topicMap, "deadline", null);
        LocalDateTime deadline = parseDeadline(deadlineStr);
        String urgencyStr = extractStringValue(topicMap, "urgency", "Low");
        String category = extractStringValue(topicMap, "category", "General");
        String subCategory = extractStringValue(topicMap, "subCategory", null);
        String periodStartDate = extractStringValue(topicMap, "periodStartDate", null);
        String periodEndDate = extractStringValue(topicMap, "periodEndDate", null);
        String latestMessageDate = extractStringValue(topicMap, "latestMessageDate", null);

        // Update periodStartDate to first message date if null
        if (periodStartDate == null && !messages.isEmpty()) {
            periodStartDate = messages.get(0).getTimestamp().toString();
        }
        // Update periodEndDate to last message date if null
        if (periodEndDate == null && !messages.isEmpty()) {
            periodEndDate = messages.get(messages.size() - 1).getTimestamp().toString();
        }
        
        UrgencyLevel urgency = mapStringToUrgency(urgencyStr);
        
        LocalDateTime startTime = extractDateTime(topicMap, "startTime");
        LocalDateTime endTime = extractDateTime(topicMap, "endTime");
                
        List<UserDTO> peopleInvolved = extractPeopleInvolved(topicMap, tenantId, workspaceId);
        List<SummaryPerPerson> summaryPerPerson = extractSummaryPerPerson(topicMap, tenantId, workspaceId, messages);
        Map<String, String> lastMessageDatePerPerson = extractStringMap(topicMap, "lastMessageDatePerPerson");
        List<SuggestedReply> suggestedReplies = ProviderUtils.extractSuggestedReplies(topicMap);
        ForwardInfo suggestedForwardRecipient = extractForwardInfo(topicMap);
        
        return new TopicEnrichment(title, shortSummary, fullSummary, suggestedAction, 
                                 clientOrSupplier, deadline, urgency, category, subCategory,
                                 startTime, endTime, periodStartDate, periodEndDate, latestMessageDate,
                                 peopleInvolved, summaryPerPerson, lastMessageDatePerPerson, 
                                 suggestedReplies, suggestedForwardRecipient);
    }
    

    private void updateUserInformation(Map<String, UserDTO> userInfos, String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) return;
        
        if (tenantId != null && workspaceId != null) {
            userInfos.forEach((slackId, user) -> {
                try {
                    // Only call userService if slackId is not null
                    if (slackId != null && !slackId.trim().isEmpty()) {
                        User updatedUser = userService.getUser(tenantId, workspaceId, slackId).orElse(null);
                        if (updatedUser != null) {
                            UserDTO updatedUserDTO = new UserDTO(
                                updatedUser.getId(),
                                updatedUser.getName(),
                                updatedUser.getDisplayName(),
                                updatedUser.getImage72()
                            );
                            userInfos.put(slackId, updatedUserDTO);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to update user info for slackId {}: {}", slackId, e.getMessage());
                }
            });
        } else {
            logger.warn("Cannot update user info: tenantId={}, workspaceId={}", tenantId, workspaceId);
        }
    }

    private UrgencyLevel mapStringToUrgency(String urgencyStr) {
        if (urgencyStr == null) return UrgencyLevel.LOW;
        
        return switch (urgencyStr.toUpperCase()) {
            case "CRITICAL" -> UrgencyLevel.CRITICAL;
            case "HIGH" -> UrgencyLevel.HIGH;
            case "MEDIUM" -> UrgencyLevel.MEDIUM;
            default -> UrgencyLevel.LOW;
        };
    }
    
    private List<String> extractStringList(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value instanceof List<?> list) {
            return list.stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .toList();
        }
        return List.of();
    }
    
    private Map<String, String> extractStringMap(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value instanceof Map<?, ?> innerMap) {
            Map<String, String> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : innerMap.entrySet()) {
                if (entry.getKey() instanceof String k && entry.getValue() instanceof String v) {
                    result.put(k, v);
                }
            }
            return result;
        }
        return Map.of();
    }
    
    private ForwardInfo extractForwardInfo(Map<?, ?> topicMap) {
        Object forwardObj = topicMap.get("forward");
        if (forwardObj instanceof Map<?, ?> forwardMap) {
            String channel = extractStringValue(forwardMap, "channel", null);
            String to = extractStringValue(forwardMap, "to", null);
            String subject = extractStringValue(forwardMap, "subject", null);
            String body = extractStringValue(forwardMap, "body", null);
            
            return new ForwardInfo(channel, to, subject, body);
        }
        return null;
    }

    private List<UserDTO> extractPeopleInvolved(Map<?, ?> topicMap, String tenantId, String workspaceId) {
        Object value = topicMap.get("peopleInvolved");
        if (value instanceof List<?> list) {
            List<UserDTO> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String userId) {
                    // Process only string user IDs like "U08SABCH6R3"
                    UserDTO enrichedUser = enrichUserDTO(userId, tenantId, workspaceId);
                    // Add user only if ID is valid (not null and not N/A)
                    if (enrichedUser.id() != null && !enrichedUser.id().equals("N/A")) {
                        result.add(enrichedUser);
                    }
                } else {
                    logger.warn("Skipping non-string item in peopleInvolved: {} (type: {})", 
                               item, item != null ? item.getClass().getSimpleName() : "null");
                }
            }
            return result;
        }
        logger.debug("peopleInvolved is not a List, returning empty list. Value type: {}", 
                    value != null ? value.getClass().getSimpleName() : "null");
        return List.of();
    }
    
    /**
     * Enriches UserDTO with data from Redis
     */
    private UserDTO enrichUserDTO(String userId, String tenantId, String workspaceId) {
        try {            
            // Get user details from Redis - add null checks
            if (tenantId != null && workspaceId != null && userId != null && !userId.trim().isEmpty()) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    String displayName = user.getDisplayName();
                    displayName = displayName != null ? displayName : user.getName();
                    return new UserDTO(
                        userId,
                        user.getName(),
                        displayName,
                        user.getImageOriginal()
                    );
                }
            }
            logger.warn("Cannot enrich user data due to null/empty parameters: tenantId={}, workspaceId={}, userId={}", 
                       tenantId, workspaceId, userId);
            return new UserDTO(userId, "N/A", "N/A", null);
        } catch (Exception e) {
            logger.warn("Failed to enrich UserDTO for user {}: {}", userId, e.getMessage());
            // Return original data on error
            return new UserDTO(userId, "N/A", "N/A", null);
        }
    }
    
    /**
     * Extract DateTime from map with ISO format support
     */
    private LocalDateTime extractDateTime(Map<?, ?> map, String key) {
        String dateTimeStr = extractStringValue(map, key, null);
        if (dateTimeStr == null || dateTimeStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Try ISO format first (e.g., "2025-06-18T10:30:00Z")
            if (dateTimeStr.endsWith("Z")) {
                dateTimeStr = dateTimeStr.substring(0, dateTimeStr.length() - 1);
            }
            return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (Exception e) {
            try {
                // Try without time part (just date)
                return LocalDateTime.parse(dateTimeStr + "T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception e2) {
                // If all parsing fails, return null
                return null;
            }
        }
    }
    
    private List<SummaryPerPerson> extractSummaryPerPerson(Map<?, ?> topicMap, String tenantId, String workspaceId, List<SlackMessage> messages) {
        Object summaryPerPersonObj = topicMap.get("summaryPerPerson");
        
        // Handle new format: simple map of userId -> summary
        if (summaryPerPersonObj instanceof Map<?, ?> summaryMap) {
            List<SummaryPerPerson> summaries = new ArrayList<>();
            
            for (Map.Entry<?, ?> entry : summaryMap.entrySet()) {
                String userId = String.valueOf(entry.getKey());
                String summary = String.valueOf(entry.getValue());
                
                // Enrich with user data from Redis
                SummaryPerPerson enrichedSummary = enrichSummaryWithUserData(
                    userId, summary, tenantId, workspaceId, messages
                );
                summaries.add(enrichedSummary);
            }
            return summaries;
        } else if (summaryPerPersonObj instanceof List<?> summaryList) {
            List<SummaryPerPerson> summaries = new ArrayList<>();
            
            for (Object summaryObj : summaryList) {
                if (summaryObj instanceof Map<?, ?> summaryObjMap) {
                    String id = extractStringValue(summaryObjMap, "id", null);
                    String username = extractStringValue(summaryObjMap, "username", null);
                    String displayName = extractStringValue(summaryObjMap, "displayName", null);
                    String imageUrl = extractStringValue(summaryObjMap, "imageUrl", null);
                    String summary = extractStringValue(summaryObjMap, "summary", "No summary available");
                    Integer messageCount = extractIntegerValue(summaryObjMap, "messageCount", 0);
                    LocalDateTime firstMessageDate = extractDateTime(summaryObjMap, "firstMessageDate");
                    LocalDateTime lastMessageDate = extractDateTime(summaryObjMap, "lastMessageDate");
                    List<String> keyContributions = extractStringList(summaryObjMap, "keyContributions");
                    List<String> actionItems = extractStringList(summaryObjMap, "actionItems");
                    // check if username is empty or null, fallback to id
                    if (username == null || username.trim().isEmpty()) {
                        username = id;
                    }
                    // check if displayName is empty or null, fallback to username
                    if (displayName == null || displayName.trim().isEmpty()) {
                        displayName = username;
                    }
                    SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
                        id, username, displayName, imageUrl, summary,
                        messageCount, firstMessageDate, lastMessageDate,
                        keyContributions, actionItems
                    );
                    summaries.add(summaryPerPerson);
                }
            }
            return summaries;
        }
        
        return List.of();
    }
    
    /**
     * Enriches a basic SummaryPerPerson with user data from Redis and message statistics
     */
    private SummaryPerPerson enrichSummaryWithUserData(String userId, String summary, String tenantId, String workspaceId, List<SlackMessage> messages) {
        try {
            // Get user details from Redis
            User user = null;
            if (tenantId != null && workspaceId != null) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                user = userOptional.orElse(null);
            }
            // Calculate message statistics for this user
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> userId.equals(msg.getUserId()))
                .toList();
            int messageCount = userMessages.size();
            LocalDateTime firstMessageDate = userMessages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .min(LocalDateTime::compareTo)
                .orElse(null);
            LocalDateTime lastMessageDate = userMessages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(null);
            // Extract user details from Redis or fall back to basic info
            String username = user != null ? user.getName() : null;
            if (username == null || username.trim().isEmpty()) {
                username = userId; // Fallback to userId if username is not available
            }
            String displayName = user != null ? user.getDisplayName() : null;
            // check if displayName is empty or null, fallback to username
            if (displayName == null || displayName.trim().isEmpty()) {
                displayName = username;
            }
            String imageUrl = user != null ? user.getImageOriginal() : null;
            return new SummaryPerPerson(
                userId,
                username,
                displayName,
                imageUrl,
                summary,
                messageCount,
                firstMessageDate,
                lastMessageDate,
                List.of(), // keyContributions - could be enhanced later
                List.of()  // actionItems - could be enhanced later
            );
        } catch (Exception e) {
            logger.warn("Failed to enrich summary for user {}: {}", userId, e.getMessage());
            // Return basic summary without enrichment
            return new SummaryPerPerson(
                userId,
                userId,  // username
                userId,  // displayName
                null,  // imageUrl
                summary,
                0,     // messageCount
                null,  // firstMessageDate
                null,  // lastMessageDate
                List.of(),
                List.of()
            );
        }
    }
    
    /**
     * Parse deadline string into LocalDateTime
     * Supports various date formats commonly used in AI responses
     */
    private LocalDateTime parseDeadline(String deadlineStr) {
        if (deadlineStr == null || deadlineStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Try ISO date-time format first
            if (deadlineStr.contains("T")) {
                return LocalDateTime.parse(deadlineStr);
            }
            
            // Try date-only format (assume end of day)
            if (deadlineStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return LocalDate.parse(deadlineStr).atTime(23, 59, 59);
            }
            
            // Try other common formats
            DateTimeFormatter[] formatters = {
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd")
            };
            
            for (DateTimeFormatter formatter : formatters) {
                try {
                    if (formatter.toString().contains("HH")) {
                        return LocalDateTime.parse(deadlineStr, formatter);
                    } else {
                        return LocalDate.parse(deadlineStr, formatter).atTime(23, 59, 59);
                    }
                } catch (DateTimeParseException ignored) {
                    // Try next formatter
                }
            }
            
            logger.warn("Could not parse deadline string: {}", deadlineStr);
            return null;
        } catch (Exception e) {
            logger.warn("Error parsing deadline string '{}': {}", deadlineStr, e.getMessage());
            return null;
        }
    }
    
    /**
     * Convert a list of LinkedHashMap objects to SlackMessage objects
     */
    private List<SlackMessage> convertToSlackMessages(List<Map<String, Object>> requestMapList) {
        if (requestMapList == null) {
            return new ArrayList<>();
        }
        
        return requestMapList.stream()
            .map(this::convertMapToSlackMessage)
            .collect(Collectors.toList());
    }
    
    /**
     * Convert a single Map to SlackMessage object
     */
    private SlackMessage convertMapToSlackMessage(Map<String, Object> map) {
        try {
            // Use ObjectMapper to convert Map to SlackMessage
            return objectMapper.convertValue(map, SlackMessage.class);
        } catch (Exception e) {
            logger.warn("Failed to convert map to SlackMessage: {}, error: {}", map, e.getMessage());
            // Return a basic SlackMessage with minimal data
            SlackMessage message = new SlackMessage();
            message.setId((String) map.get("id"));
            message.setContent((String) map.get("content"));
            message.setUserId((String) map.get("userId"));
            return message;
        }
    }
}