package com.lucid.automation.airouting.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.enrichment.ConversationEnrichment;
import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.common.dto.enrichment.ForwardInfo;
import com.lucid.automation.common.dto.enrichment.SourceDTO;
import com.lucid.automation.common.dto.enrichment.SuggestedReply;
import com.lucid.automation.common.dto.enrichment.SummaryPerPerson;
import com.lucid.automation.common.dto.enrichment.TopicEnrichment;
import com.lucid.automation.common.dto.enrichment.UrgencyLevel;
import com.lucid.automation.common.dto.enrichment.EnrichmentUserDTO;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.model.Channel;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.service.ChannelService;
import com.lucid.automation.airouting.util.TextUtils;
import com.lucid.automation.airouting.util.json.JsonCleaner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
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
 * Consumer service for post-processing AI responses from Kafka
 * Handles processing of pre-AI responses and converts them to final AI responses
 * 
 * PERMALINK HANDLING IMPROVEMENTS:
 * - Added defensive permaLink handling in ObjectMapper conversion
 * - Enhanced fallback conversion to always preserve permaLink values
 * - Added comprehensive logging for permaLink status tracking
 * - Implemented fallback permalink generation when original is missing
 * - Added support for array-based timestamp conversion in fallback scenarios
 * 
 * @author AI Assistant
 */
@Service
public class PostProcessingConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(PostProcessingConsumer.class);
    
    // Constants
    private static final String DEFAULT_TOPIC_TITLE = "General Discussion";
    private static final String DEFAULT_SUMMARY = "No summary available";
    private static final String DEFAULT_DETAILED_SUMMARY = "No detailed summary available";
    private static final String DEFAULT_ACTION = "No action suggested";
    private static final String DEFAULT_CATEGORY = "General";
    private static final String DEFAULT_URGENCY = "Low";
    private static final String UNKNOWN_USER = "Unknown User";
    private static final String ERROR_INVALID_FORMAT = "Invalid pre-AI response format";
    private static final String ERROR_RESULT_NULL = "Result map is null in pre-AI response";
    private static final String ERROR_FAILED_CONVERSATION_ANALYSIS = "Failed to analyze conversation";
    private static final String NOT_AVAILABLE = "N/A";
    private static final String SUCCESS_STATUS = "success";
    private static final String UNTITLED_TOPIC = "Untitled Topic";
    private static final String DEFAULT_SUMMARY_FOR_PERSON = "No summary available";
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final UserService userService;
    private final ChannelService channelService;
    private final ObjectMapper objectMapper;
    
    @Value("${kafka.topics.ai-responses:ai.responses.queue}")
    private String aiResponsesTopic;
    
    public PostProcessingConsumer(UserService userService, ChannelService channelService, ObjectMapper objectMapper,
                                 KafkaTemplate<String, Object> kafkaTemplate) {
        this.userService = userService;
        this.channelService = channelService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        
        logger.info("=== POST-PROCESSING CONSUMER INITIALIZED ===");
        logger.info("PostProcessingConsumer initialized for processing pre-AI responses");
        logger.info("Final output topic: {}", aiResponsesTopic);
        logger.info("============================================");
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
            
            EnrichmentResponse parsedResponse = processPreAiResponse(responseMap);
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
    private EnrichmentResponse processPreAiResponse(Map<String, Object> messageResponse) {
        logger.info("Processing pre-AI response with keys: {}", messageResponse.keySet());
        if (!(messageResponse instanceof Map<?, ?>)) {
            logger.error("Invalid pre-AI response format: expected Map, got {}", messageResponse.getClass().getSimpleName());
            return createErrorResponse(ERROR_INVALID_FORMAT);
        }
        
        Map<String, Object> responseMap = (Map<String, Object>) messageResponse;
        
        // Extract necessary fields from the response
        String messageId = (String) responseMap.get("messageId");
        String correlationId = (String) responseMap.get("correlationId");
        String taskType = (String) responseMap.get("taskType");
        String tenantId = (String) responseMap.get("tenantId");
        String tenantSchema = (String) responseMap.get("tenantSchema");
        String userId = (String) responseMap.get("userId");
        String deemergeUserId = (String) responseMap.get("deemergeUserId");
        String deemergeUserName = (String) responseMap.get("deemergeUserName");
        String teamId = (String) responseMap.get("teamId");
        
        logger.info("Extracted fields - messageId: {}, correlationId: {}, taskType: {}, tenantId: {}, teamId: {}, deemergeUserId: {}, deemergeUserName: {}", 
                   messageId, correlationId, taskType, tenantId, teamId, deemergeUserId, deemergeUserName);
        
        // extract result as a String
        Map<String, Object> aiResultMap = (Map<String, Object>) responseMap.get("result");
        if (aiResultMap == null) {
            logger.error("Result map is null in pre-AI response: {}", responseMap);
            return createErrorResponse(ERROR_RESULT_NULL);
        }

        // Convert the request objects from LinkedHashMap to SlackMessage objects
        List<Map<String, Object>> requestMapList = (List<Map<String, Object>>) aiResultMap.get("request");
        List<SlackMessage> requestMessages;
        try {
            requestMessages = convertToSlackMessages(requestMapList);
            // Log permaLink status for debugging
            long messagesWithPermaLink = requestMessages.stream()
                .mapToLong(msg -> msg.getPermaLink() != null && !msg.getPermaLink().trim().isEmpty() ? 1 : 0)
                .sum();
            logger.info("Converted {} request messages, {} have permaLink", requestMessages.size(), messagesWithPermaLink);
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

        // Create a new response object with the processed data
        EnrichmentResponse enrichmentResponse = new EnrichmentResponse();
        enrichmentResponse.setMessageId(messageId);
        enrichmentResponse.setCorrelationId(correlationId);
        enrichmentResponse.setTaskType(taskType);
        enrichmentResponse.setSuccess(true);
        enrichmentResponse.setStatus(SUCCESS_STATUS);
        enrichmentResponse.setResult(parsedResult);
        enrichmentResponse.setProcessedAt(List.of(
                processedAtTime.getYear(),
                processedAtTime.getMonthValue(),
                processedAtTime.getDayOfMonth(),
                processedAtTime.getHour(),
                processedAtTime.getMinute(),
                processedAtTime.getSecond()
        ));
        enrichmentResponse.setTenantId(tenantId);
        enrichmentResponse.setTenantSchema(tenantSchema);
        enrichmentResponse.setUserId(userId);
        enrichmentResponse.setDeemergeUserId(deemergeUserId);
        enrichmentResponse.setDeemergeUserName(deemergeUserName);
        enrichmentResponse.setTeamId(teamId);
        return enrichmentResponse;
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

    /**
     * Create an error EnrichmentResponse
     */
    private EnrichmentResponse createErrorResponse(String errorMessage) {
        EnrichmentResponse response = new EnrichmentResponse();
        response.setSuccess(false);
        response.setStatus("error");
        response.setErrorMessage(errorMessage);
        response.setProcessedAt(List.of(
                LocalDateTime.now().getYear(),
                LocalDateTime.now().getMonthValue(),
                LocalDateTime.now().getDayOfMonth(),
                LocalDateTime.now().getHour(),
                LocalDateTime.now().getMinute(),
                LocalDateTime.now().getSecond()
        ));
        return response;
    }

    private ConversationEnrichment parseConversationEnrichmentResponse(String response, List<SlackMessage> messages) {
        try {
            String cleanedResponse = JsonCleaner.cleanJsonResponse(response);
            logger.info("[==>>> GEMINI-PARSE]: Cleaned response:\n{}", cleanedResponse);

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
            DEFAULT_TOPIC_TITLE,
            DEFAULT_SUMMARY, 
            DEFAULT_DETAILED_SUMMARY,
            DEFAULT_ACTION,
            null, // clientOrSupplier
            null, // deadline
            UrgencyLevel.LOW,
            DEFAULT_CATEGORY, // category
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
            Map.of("error", ERROR_FAILED_CONVERSATION_ANALYSIS)
        );
    }
    
    private TopicEnrichment parseTopicFromMap(Map<?, ?> topicMap, List<SlackMessage> messages) {
        Map<String, EnrichmentUserDTO> userInfos = messages.stream().filter(msg -> {
                String key = msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername();
                return key != null && !key.trim().isEmpty();
            }).collect(Collectors.toMap(
                msg -> msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername(),
                msg -> new EnrichmentUserDTO(msg.getSlackUserId(), msg.getUsername(), getBestDisplayNameFromSlackMessage(msg), msg.getImage72()),
                (existing, replacement) -> existing // Keep existing if duplicate
            ));

        String tenantId = messages.isEmpty() ? null : messages.get(0).getTenantId();
        String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();
        updateUserInformation(userInfos, tenantId, workspaceId);

        String title = extractStringValue(topicMap, "title", UNTITLED_TOPIC);
        String shortSummary = extractStringValue(topicMap, "shortSummary", DEFAULT_SUMMARY);
        shortSummary = TextUtils.replaceSlackMentions(shortSummary, userInfos);
        
        String fullSummary = extractStringValue(topicMap, "fullSummary", DEFAULT_DETAILED_SUMMARY);
        fullSummary = TextUtils.replaceSlackMentions(fullSummary, userInfos);

        String suggestedAction = extractStringValue(topicMap, "suggestedAction", DEFAULT_ACTION);
        suggestedAction = TextUtils.replaceSlackMentions(suggestedAction, userInfos);

        String clientOrSupplier = extractStringValue(topicMap, "clientOrSupplier", null);
        String deadlineStr = extractStringValue(topicMap, "deadline", null);
        LocalDateTime deadline = parseDeadline(deadlineStr);
        String urgencyStr = extractStringValue(topicMap, "urgency", DEFAULT_URGENCY);
        String category = extractStringValue(topicMap, "category", DEFAULT_CATEGORY);
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
                
        List<EnrichmentUserDTO> peopleInvolved = extractPeopleInvolved(topicMap, tenantId, workspaceId);
        List<SummaryPerPerson> summaryPerPerson = extractSummaryPerPerson(topicMap, tenantId, workspaceId, messages);
        Map<String, String> lastMessageDatePerPerson = extractStringMap(topicMap, "lastMessageDatePerPerson");
        List<SuggestedReply> suggestedReplies = extractSuggestedRepliesWithChannelLookup(topicMap, tenantId, workspaceId);
        ForwardInfo suggestedForwardRecipient = extractForwardInfo(topicMap);
        
        // Log permaLink status
        logger.debug("Processing {} messages, permaLink status: {}", 
                    messages.size(), 
                    messages.stream().map(msg -> msg.getId() + ":" + (msg.getPermaLink() != null ? "present" : "null")).collect(Collectors.joining(", ")));
        
        return new TopicEnrichment(title, shortSummary, fullSummary, suggestedAction, 
                                 clientOrSupplier, deadline, urgency, category, subCategory,
                                 startTime, endTime, periodStartDate, periodEndDate, latestMessageDate,
                                 peopleInvolved, summaryPerPerson, lastMessageDatePerPerson, 
                                 suggestedReplies, suggestedForwardRecipient);
    }
    

    /**
     * Get the best available display name for a user
     * Priority: displayName -> displayNameNormalized -> realNameNormalized -> name -> fallback
     */
    private String getBestDisplayName(User user, String fallback) {
        if (user == null) {
            logger.debug("getBestDisplayName: User is null, using fallback: {}", fallback);
            return fallback != null ? fallback : UNKNOWN_USER;
        }
        
        // Try displayName first
        if (user.getDisplayName() != null && !user.getDisplayName().trim().isEmpty()) {
            logger.debug("getBestDisplayName: Using displayName: {}", user.getDisplayName());
            return user.getDisplayName();
        }
        
        // Try displayNameNormalized
        if (user.getDisplayNameNormalized() != null && !user.getDisplayNameNormalized().trim().isEmpty()) {
            logger.debug("getBestDisplayName: Using displayNameNormalized: {}", user.getDisplayNameNormalized());
            return user.getDisplayNameNormalized();
        }
        
        // Try realNameNormalized
        if (user.getRealNameNormalized() != null && !user.getRealNameNormalized().trim().isEmpty()) {
            logger.debug("getBestDisplayName: Using realNameNormalized: {}", user.getRealNameNormalized());
            return user.getRealNameNormalized();
        }
        
        // Try name
        if (user.getName() != null && !user.getName().trim().isEmpty()) {
            logger.debug("getBestDisplayName: Using name: {}", user.getName());
            return user.getName();
        }
        
        // Use fallback
        String result = fallback != null ? fallback : UNKNOWN_USER;
        logger.debug("getBestDisplayName: All user name fields are null/empty. Using fallback: {}", result);
        logger.debug("getBestDisplayName: User debug info - displayName: '{}', displayNameNormalized: '{}', realNameNormalized: '{}', name: '{}'", 
                    user.getDisplayName(), user.getDisplayNameNormalized(), user.getRealNameNormalized(), user.getName());
        return result;
    }

    /**
     * Get the best available display name from SlackMessage data
     * Priority: displayName -> username -> slackUserId
     */
    private String getBestDisplayNameFromSlackMessage(SlackMessage msg) {
        if (msg == null) {
            return UNKNOWN_USER;
        }
        
        // Try displayName first
        if (msg.getDisplayName() != null && !msg.getDisplayName().trim().isEmpty()) {
            return msg.getDisplayName();
        }
        
        // Try username
        if (msg.getUsername() != null && !msg.getUsername().trim().isEmpty()) {
            return msg.getUsername();
        }
        
        // Use slackUserId as last resort
        if (msg.getSlackUserId() != null && !msg.getSlackUserId().trim().isEmpty()) {
            return msg.getSlackUserId();
        }
        
        return UNKNOWN_USER;
    }

    private void updateUserInformation(Map<String, EnrichmentUserDTO> userInfos, String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) return;
        
        if (tenantId != null && workspaceId != null) {
            userInfos.forEach((slackId, user) -> {
                try {
                    // Only call userService if slackId is not null
                    if (slackId != null && !slackId.trim().isEmpty()) {
                        User updatedUser = userService.getUser(tenantId, workspaceId, slackId).orElse(null);
                        if (updatedUser != null) {
                            EnrichmentUserDTO updatedUserDTO = new EnrichmentUserDTO(
                                updatedUser.getId(),
                                updatedUser.getName(),
                                getBestDisplayName(updatedUser, updatedUser.getName()),
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

    /**
     * Create a short text representation of a message for source reference
     */
    private String createShortTextFromMessage(SlackMessage msg) {
        if (msg == null) {
            return "Unknown message";
        }
        
        String content = msg.getText() != null ? msg.getText() : msg.getContent();
        String username = msg.getUsername() != null ? msg.getUsername() : 
                         (msg.getUserId() != null ? msg.getUserId() : UNKNOWN_USER);
        
        if (content == null || content.trim().isEmpty()) {
            return username + ": (empty message)";
        }
        
        // Truncate content to a reasonable length for short text
        String truncatedContent = content.length() > 100 ? 
            content.substring(0, 97) + "..." : content;
            
        // Remove newlines and extra spaces
        truncatedContent = truncatedContent.replaceAll("\\s+", " ").trim();
        
        return username + ": " + truncatedContent;
    }

    /**
     * Extract sources from user's messages to create SourceDTO list
     * Each user message becomes a source entry with fallback handling for permaLink
     */
    private List<SourceDTO> extractUserSources(String userId, List<SlackMessage> messages) {
        if (messages == null || messages.isEmpty() || userId == null) {
            return List.of();
        }
        
        return messages.stream()
            .filter(msg -> userId.equals(msg.getUserId()))
            .map(msg -> {
                // Get permalink with fallback handling
                String permalink = msg.getPermaLink();
                
                // Fallback to a constructed URL if permaLink is null or empty
                if (permalink == null || permalink.trim().isEmpty()) {
                    // Try to construct a basic permalink if we have the necessary data
                    if (msg.getTeamId() != null && msg.getChannelId() != null && msg.getTs() != null) {
                        permalink = String.format("slack://team=%s/channel=%s/message=%s", 
                                                msg.getTeamId(), msg.getChannelId(), msg.getTs());
                        logger.debug("Generated fallback permalink for message {}: {}", msg.getId(), permalink);
                    } else {
                        // Last resort fallback
                        permalink = "deemerge.ai";
                        logger.warn("Using default permalink fallback for message {} - teamId: {}, channelId: {}, ts: {}", 
                                  msg.getId(), msg.getTeamId(), msg.getChannelId(), msg.getTs());
                    }
                }
                
                // Create a short text from the message content
                String shortText = createShortTextFromMessage(msg);
                
                return new SourceDTO(permalink, shortText, msg.getSource());
            })
            .distinct() // Remove duplicates based on permaLink
            .collect(Collectors.toList());
    }

    /**
     * Extract suggested replies from topic map with channel lookup functionality.
     * If channelId is null but channelName is provided, attempts to resolve channelId from database.
     */
    private List<SuggestedReply> extractSuggestedRepliesWithChannelLookup(Map<?, ?> topicMap, String tenantId, String workspaceId) {
        Object suggestedRepliesObj = topicMap.get("suggestedReplies");
        if (suggestedRepliesObj instanceof List<?> repliesList) {
            List<SuggestedReply> replies = new ArrayList<>();

            for (Object replyObj : repliesList) {
                if (replyObj instanceof Map<?, ?> replyMap) {
                    String tone = extractStringValue(replyMap, "tone", null);
                    String replyMethod = extractStringValue(replyMap, "replyMethod", null);
                    String recipientHandle = extractStringValue(replyMap, "recipientHandle", null);
                    String channelName = extractStringValue(replyMap, "channelName", null);
                    String channelId = extractStringValue(replyMap, "channelId", null);
                    String threadId = extractStringValue(replyMap, "threadId", null);
                    String to = extractStringValue(replyMap, "to", null);
                    List<String> cc = extractStringList(replyMap, "cc");
                    String subject = extractStringValue(replyMap, "subject", null);
                    String messageBody = extractStringValue(replyMap, "messageBody", "");

                    // Handle channelId resolution if null but channelName is provided
                    if (channelId == null && channelName != null && !channelName.trim().isEmpty()) {
                        channelId = channelName.trim();
                    }
                    if (channelId != null) {
                        // Attempt to resolve channelName from channelId
                        channelName = resolveChannelNameFromId(channelId);
                    } else if (channelId == null || channelId.trim().isEmpty()) {
                        logger.warn("Channel ID is still null or empty after resolution attempt for channelName: {}", channelName);
                        continue; // Skip this reply if we cannot resolve channelId
                        
                    }

                    SuggestedReply suggestedReply = new SuggestedReply(
                        tone, replyMethod, recipientHandle, channelName, channelId,
                        threadId, to, cc, subject, messageBody
                    );
                    replies.add(suggestedReply);
                }
            }
            return replies;
        }
        return List.of();
    }

    /**
     * Resolves channelId from channelName by querying the database
     */
    /**
     * Resolves channelName from channelId by querying the database
     */
    private String resolveChannelNameFromId(String channelId) {
        try {
            if (channelId == null || channelId.trim().isEmpty()) {
                logger.warn("Cannot resolve channelName: channelId is null or empty");
                return null;
            }
            // Find channel by channelId
            Optional<Channel> channelOptional = channelService.findByChannelId(channelId.trim());
            
            if (channelOptional.isPresent()) {
                Channel channel = channelOptional.get();
                String channelName = channel.getChannelName();
                return channelName;
            } else {
                return channelId; // Return original channelId as fallback
            }
            
        } catch (Exception e) {
            return channelId; // Return original channelId as fallback
        }
    }

    private List<EnrichmentUserDTO> extractPeopleInvolved(Map<?, ?> topicMap, String tenantId, String workspaceId) {
        Object value = topicMap.get("peopleInvolved");
        if (value instanceof List<?> list) {
            List<EnrichmentUserDTO> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String userId) {
                    // Process only string user IDs like "U08SABCH6R3"
                    EnrichmentUserDTO enrichedUser = enrichUserDTO(userId, tenantId, workspaceId);
                    // Add user only if ID is valid (not null and not N/A)
                    if (enrichedUser.id() != null && !enrichedUser.id().equals(NOT_AVAILABLE)) {
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
    private EnrichmentUserDTO enrichUserDTO(String userId, String tenantId, String workspaceId) {
        try {            
            // Get user details from Redis - add null checks
            if (tenantId != null && workspaceId != null && userId != null && !userId.trim().isEmpty()) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    // Use userId as final fallback if all user name fields are empty
                    String displayName = getBestDisplayName(user, userId);
                    String userName = user.getName() != null && !user.getName().trim().isEmpty() ? user.getName() : userId;
                    logger.debug("enrichUserDTO: Successfully enriched user {} with displayName: '{}', userName: '{}'", 
                                userId, displayName, userName);
                    return new EnrichmentUserDTO(
                        userId,
                        userName,
                        displayName,
                        user.getImageOriginal()
                    );
                }
            }
            logger.warn("Cannot enrich user data due to null/empty parameters: tenantId={}, workspaceId={}, userId={}", 
                       tenantId, workspaceId, userId);
            return new EnrichmentUserDTO(userId, userId, userId, null); // Use userId for all name fields as fallback
        } catch (Exception e) {
            logger.warn("Failed to enrich UserDTO for user {}: {}", userId, e.getMessage());
            // Return original data on error with userId as fallback for all name fields
            return new EnrichmentUserDTO(userId, userId, userId, null);
        }
    }
    
    /**
     * Extract DateTime from map with ISO format support
     */
    private LocalDateTime extractDateTime(Map<?, ?> map, String key) {
        Object dateTimeObj = map.get(key);
        
        // Handle array format [year, month, day, hour, minute, second]
        if (dateTimeObj instanceof List<?> dateList) {
            try {
                if (dateList.size() >= 6) {
                    int year = ((Number) dateList.get(0)).intValue();
                    int month = ((Number) dateList.get(1)).intValue();
                    int day = ((Number) dateList.get(2)).intValue();
                    int hour = ((Number) dateList.get(3)).intValue();
                    int minute = ((Number) dateList.get(4)).intValue();
                    int second = ((Number) dateList.get(5)).intValue();
                    
                    LocalDateTime result = LocalDateTime.of(year, month, day, hour, minute, second);
                    logger.debug("SUMMARY_DEBUG: Parsed date array {} for key '{}' to: {}", dateList, key, result);
                    return result;
                }
            } catch (Exception e) {
                logger.warn("Failed to parse date array {} for key '{}': {}", dateList, key, e.getMessage());
                return null;
            }
        }
        
        // Handle string format (existing logic)
        String dateTimeStr = extractStringValue(map, key, null);
        if (dateTimeStr == null || dateTimeStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Try ISO format first (e.g., "2025-06-18T10:30:00Z")
            if (dateTimeStr.endsWith("Z")) {
                dateTimeStr = dateTimeStr.substring(0, dateTimeStr.length() - 1);
            }
            LocalDateTime result = LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            logger.debug("SUMMARY_DEBUG: Parsed date string '{}' for key '{}' to: {}", dateTimeStr, key, result);
            return result;
        } catch (Exception e) {
            try {
                // Try without time part (just date)
                LocalDateTime result = LocalDateTime.parse(dateTimeStr + "T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                logger.debug("SUMMARY_DEBUG: Parsed date string '{}' (with default time) for key '{}' to: {}", dateTimeStr, key, result);
                return result;
            } catch (Exception e2) {
                // If all parsing fails, return null
                logger.warn("Failed to parse date string '{}' for key '{}': {}", dateTimeStr, key, e2.getMessage());
                return null;
            }
        }
    }
    
    /**
     * Enhance displayName for an existing user with enriched data from Redis
     */
    private String enhanceDisplayName(String userId, String existingDisplayName, String existingUsername, String tenantId, String workspaceId) {
        try {
            if (tenantId != null && workspaceId != null && userId != null && !userId.trim().isEmpty()) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    return getBestDisplayName(user, existingDisplayName);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to enhance displayName for user {}: {}", userId, e.getMessage());
        }
        
        // Fallback to existing logic if Redis lookup fails
        if (existingDisplayName == null || existingDisplayName.trim().isEmpty()) {
            return existingUsername != null ? existingUsername : userId;
        }
        return existingDisplayName;
    }

    private List<SummaryPerPerson> extractSummaryPerPerson(Map<?, ?> topicMap, String tenantId, String workspaceId, List<SlackMessage> messages) {
        Object summaryPerPersonObj = topicMap.get("summaryPerPerson");
        logger.debug("SUMMARY_DEBUG: extractSummaryPerPerson called with summaryPerPersonObj type: {}, value: {}", 
                    summaryPerPersonObj != null ? summaryPerPersonObj.getClass().getSimpleName() : "null", summaryPerPersonObj);
        
        // Handle new format: simple map of userId -> summary
        if (summaryPerPersonObj instanceof Map<?, ?> summaryMap) {
            logger.info("SUMMARY_DEBUG: Processing summaryPerPerson as Map with {} entries", summaryMap.size());
            List<SummaryPerPerson> summaries = new ArrayList<>();
            
            for (Map.Entry<?, ?> entry : summaryMap.entrySet()) {
                String userId = String.valueOf(entry.getKey());
                String summary = String.valueOf(entry.getValue());
                
                // Enrich with user data from Redis
                SummaryPerPerson enrichedSummary = enrichSummaryWithUserData(
                    userId, summary, tenantId, workspaceId, messages
                );
                summaries.add(enrichedSummary);
                logger.debug("SUMMARY_DEBUG: Added enriched summary for user {}: {}", userId, enrichedSummary);
            }
            return summaries;
        } else if (summaryPerPersonObj instanceof List<?> summaryList) {
            logger.info("SUMMARY_DEBUG: Processing summaryPerPerson as List with {} entries", summaryList.size());
            List<SummaryPerPerson> summaries = new ArrayList<>();
            
            for (Object summaryObj : summaryList) {
                if (summaryObj instanceof Map<?, ?> summaryObjMap) {
                    String id = extractStringValue(summaryObjMap, "id", null);
                    String username = extractStringValue(summaryObjMap, "username", null);
                    String displayName = extractStringValue(summaryObjMap, "displayName", null);
                    String imageUrl = extractStringValue(summaryObjMap, "imageUrl", null);
                    String summary = extractStringValue(summaryObjMap, "summary", DEFAULT_SUMMARY_FOR_PERSON);
                    Integer messageCount = extractIntegerValue(summaryObjMap, "messageCount", 0);
                    LocalDateTime firstMessageDate = extractDateTime(summaryObjMap, "firstMessageDate");
                    LocalDateTime lastMessageDate = extractDateTime(summaryObjMap, "lastMessageDate");
                    List<String> keyContributions = extractStringList(summaryObjMap, "keyContributions");
                    List<String> actionItems = extractStringList(summaryObjMap, "actionItems");
                    
                    // Extract sources from user's messages
                    List<SourceDTO> sources = extractUserSources(id, messages);
                    
                    logger.debug("SUMMARY_DEBUG: Processing user {} - firstDate: {}, lastDate: {}", 
                               id, firstMessageDate, lastMessageDate);
                    
                    // check if username is empty or null, fallback to id
                    if (username == null || username.trim().isEmpty()) {
                        username = id;
                    }
                    // Enhance displayName with user data from Redis
                    displayName = enhanceDisplayName(id, displayName, username, tenantId, workspaceId);
                    SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
                        id, username, displayName, imageUrl, summary,
                        messageCount, firstMessageDate, lastMessageDate,
                        keyContributions, actionItems, sources
                    );
                    summaries.add(summaryPerPerson);
                    logger.debug("SUMMARY_DEBUG: Created SummaryPerPerson for user {}: {}", id, summaryPerPerson);
                }
            }
            logger.info("SUMMARY_DEBUG: Returning {} summaries from List processing", summaries.size());
            return summaries;
        }
        
        logger.warn("SUMMARY_DEBUG: summaryPerPerson is not Map or List, returning empty list");
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
            String displayName = getBestDisplayName(user, username);
            String imageUrl = user != null ? user.getImageOriginal() : null;
            
            // Extract sources from user's messages
            List<SourceDTO> sources = extractUserSources(userId, messages);
            
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
                List.of(),  // actionItems - could be enhanced later
                sources
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
                List.of(), // keyContributions
                List.of(), // actionItems
                List.of()  // sources
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
            .map(this::ensureChannelNameResolved)  // Ensure channelName is resolved for all messages
            .collect(Collectors.toList());
    }
    
    /**
     * Ensure channelName is resolved for a SlackMessage, attempting resolution if missing
     */
    private SlackMessage ensureChannelNameResolved(SlackMessage message) {
        if ((message.getChannelName() == null || message.getChannelName().trim().isEmpty()) &&
            message.getChannelId() != null && !message.getChannelId().trim().isEmpty() &&
            message.getTenantId() != null && message.getWorkspaceId() != null) {
            
            String resolvedChannelName = resolveChannelNameFromId(
                message.getChannelId()
            );
            
            if (resolvedChannelName != null && !resolvedChannelName.trim().isEmpty()) {
                message.setChannelName(resolvedChannelName);
                logger.debug("Successfully resolved channelName '{}' for channelId '{}' in message post-processing", 
                           resolvedChannelName, message.getChannelId());
            } else {
                logger.warn("Failed to resolve channelName for channelId '{}' in tenantId '{}', workspaceId '{}'", 
                          message.getChannelId(), message.getTenantId(), message.getWorkspaceId());
            }
        }
        return message;
    }
    
    /**
     * Convert a single Map to SlackMessage object
     */
    private SlackMessage convertMapToSlackMessage(Map<String, Object> map) {
        try {
            // Use ObjectMapper to convert Map to SlackMessage
            SlackMessage message = objectMapper.convertValue(map, SlackMessage.class);
            
            // Defensive check: ensure permaLink is preserved even if ObjectMapper misses it
            if (message.getPermaLink() == null && map.containsKey("permaLink")) {
                String permaLink = (String) map.get("permaLink");
                message.setPermaLink(permaLink);
                logger.debug("Defensively set permaLink for message {}: {}", message.getId(), permaLink);
            }
            
            return message;
        } catch (Exception e) {
            logger.warn("Failed to convert map to SlackMessage: {}, error: {}", map, e.getMessage());
            // Return a basic SlackMessage with minimal data including permaLink
            SlackMessage message = new SlackMessage();
            message.setId((String) map.get("id"));
            message.setContent((String) map.get("content"));
            message.setUserId((String) map.get("userId"));
            message.setChannelId((String) map.get("channelId"));
            
            String channelName = (String) map.get("channelName");
            String channelId = (String) map.get("channelId");
            String tenantId = (String) map.get("tenantId");
            String workspaceId = (String) map.get("workspaceId");
            
            // Try to resolve channel name if missing but channel ID is present
            if ((channelName == null || channelName.trim().isEmpty()) && 
                channelId != null && !channelId.trim().isEmpty() &&
                tenantId != null && workspaceId != null) {
                
                channelName = resolveChannelNameFromId(channelId);
                logger.debug("Resolved channelName '{}' for channelId '{}' in fallback conversion", channelName, channelId);
            }
            
            message.setChannelName(channelName);  // ← Preserve or resolve channelName!
            message.setUsername((String) map.get("username"));
            message.setTeamId((String) map.get("teamId"));
            message.setTenantId(tenantId);
            message.setWorkspaceId(workspaceId);
            
            // Always set permaLink in fallback conversion
            String permaLink = (String) map.get("permaLink");
            message.setPermaLink(permaLink);
            String source = (String) map.get("source");
            message.setSource(source != null ? source : "slack");

            // Set additional required fields for complete SlackMessage
            message.setSlackUserId((String) map.get("slackUserId"));
            message.setDisplayName((String) map.get("displayName"));
            message.setText((String) map.get("text"));
            message.setTs((String) map.get("ts"));
            message.setMessageTs((String) map.get("messageTs"));
            message.setThreadTs((String) map.get("threadTs"));
            
            // Handle timestamp conversion with array support
            Object timestampObj = map.get("timestamp");
            if (timestampObj instanceof List<?> timestampArray) {
                try {
                    if (timestampArray.size() >= 6) {
                        int year = ((Number) timestampArray.get(0)).intValue();
                        int month = ((Number) timestampArray.get(1)).intValue();
                        int day = ((Number) timestampArray.get(2)).intValue();
                        int hour = ((Number) timestampArray.get(3)).intValue();
                        int minute = ((Number) timestampArray.get(4)).intValue();
                        int second = ((Number) timestampArray.get(5)).intValue();
                        
                        message.setTimestamp(LocalDateTime.of(year, month, day, hour, minute, second));
                        logger.debug("Fallback: Parsed timestamp array for message {}: {}", message.getId(), message.getTimestamp());
                    }
                } catch (Exception tsException) {
                    logger.debug("Failed to parse timestamp array: {}", timestampArray);
                }
            } else if (timestampObj instanceof String) {
                try {
                    message.setTimestamp(LocalDateTime.parse((String) timestampObj));
                } catch (Exception tsException) {
                    logger.debug("Failed to parse timestamp: {}", timestampObj);
                }
            }
            
            return message;
        }
    }
}
