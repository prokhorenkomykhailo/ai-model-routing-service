package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.ProviderUtils;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.dto.CategoryResult;
import com.lucid.automation.airouting.dto.SummaryResult;
import com.lucid.automation.airouting.dto.SentimentResult;
import com.lucid.automation.airouting.dto.UrgencyLevel;
import com.lucid.automation.airouting.dto.MessageEnrichment;
import com.lucid.automation.airouting.dto.ParticipantInsight;
import com.lucid.automation.airouting.dto.ConversationEnrichment;
import com.lucid.automation.airouting.dto.TopicEnrichment;
import com.lucid.automation.airouting.dto.UserDTO;
import com.lucid.automation.airouting.dto.SummaryPerPerson;
import com.lucid.automation.airouting.dto.SuggestedReply;
import com.lucid.automation.airouting.dto.ForwardInfo;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.util.PromptLoader;
import com.lucid.automation.airouting.util.TextUtils;
import com.lucid.automation.airouting.util.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Component("geminiProvider")
public class GeminiProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiProvider.class);
    private static final String UNCATEGORIZED = "Uncategorized";
    private static final String messagePlaceholder = "##messages##";
    
    @Value("${ai.providers.gemini.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.gemini.endpoint:https://generativelanguage.googleapis.com/v1/models}")
    private String apiEndpoint;
    
    // @Value("${ai.providers.gemini.model:gemini-2.0-flash}")
    @Value("${ai.providers.gemini.model:gemini-2.5-pro}")
    private String model;
    
    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;
    
    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;
    
    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private final PromptLoader promptLoader;
    private final UserService userService;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;
    
    public GeminiProvider(ObjectMapper objectMapper, 
                         PromptLoader promptLoader, UserService userService) {
        this.objectMapper = objectMapper;
        this.promptLoader = promptLoader;
        this.userService = userService;
        
        // Try to initialize the client, but handle gracefully if API key is not available
        Client tempClient = null;
        boolean clientAvailable = false;
        
        try {
            // Check if GOOGLE_API_KEY environment variable is set before initializing client
            String googleApiKey = System.getenv("GOOGLE_API_KEY");
            if (googleApiKey != null && !googleApiKey.trim().isEmpty()) {
                tempClient = new Client();
                clientAvailable = true;
                logger.info("Gemini client initialized successfully");
            } else {
                logger.warn("GOOGLE_API_KEY not set, Gemini provider will be unavailable");
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize Gemini client: {}", e.getMessage());
        }
        
        this.geminiClient = tempClient;
        this.isClientAvailable = clientAvailable;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        return new CategoryResult(UNCATEGORIZED, 0.0);
    }
    
    @Override
    public SummaryResult summarize(String content) {
        return new SummaryResult("Summary unavailable", "No content provided");
    }
    
    @Override
    public Map<String, Object> enrichConversation(List<SlackMessage> messages) {
        String debugId = "ENRICH-CONV-" + System.currentTimeMillis();
        logger.info("GEMINI-ENRICH [{}]: Starting conversation enrichment", debugId);
        try {
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-ENRICH [{}]: No messages provided, returning default enrichment", debugId);
                
                // return getDefaultConversationEnrichment();
                return Map.of(
                    "response", "No messages provided for enrichment",
                    "request", messages
                );
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-ENRICH [{}]: Gemini client not available, returning default enrichment", debugId);
                // return getDefaultConversationEnrichment();
                return Map.of(
                    "response", "Gemini client not available",
                    "request", messages
                );
            }
            
            logger.info("GEMINI-ENRICH [{}]: Formatting conversation for analysis", debugId);
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText);
            System.out.println("GEMINI-ENRICH [" + debugId + "]: Built conversation enrichment prompt: \n" + prompt);
            String response = callGeminiAPI(prompt, "conversation-enrichment", debugId);
            return Map.of(
                "response", response,
                "request", messages
            );
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-ENRICH [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Invalid input for conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (RuntimeException e) {
            logger.error("GEMINI-ENRICH [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "API error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        } catch (Exception e) {
            logger.error("GEMINI-ENRICH [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return Map.of(
                "response", "Unexpected error during conversation enrichment: " + e.getMessage(),
                "request", messages
            );
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        return new ParticipantInsight(0.0, "Neutral", 0);
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        return UrgencyLevel.LOW;
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        return "General Discussion";
    }
    
    @Override
    public List<String> extractEntities(String content) {
        return List.of();
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        return new SentimentResult("Neutral", 0.0, 0.0);
    }
    
    @Override
    public String getProviderId() {
        return "geminiProvider";
    }
    
    @Override
    public boolean isAvailable() {
        return isClientAvailable;
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    // Private helper methods
    private String callGeminiAPI(String prompt, String operation, String debugId) {
        try {
            if (geminiClient == null) {
                logger.error("GEMINI-API [{}]: Client is not available - API key not configured", debugId);
                throw new RuntimeException("Gemini client is not available - API key not configured");
            }

            long startTime = System.currentTimeMillis();
            GenerateContentResponse response = geminiClient.models.generateContent(
                model, 
                prompt, 
                null
            );
            
            long duration = System.currentTimeMillis() - startTime;
            String responseText = response.text();
            logger.info("GEMINI-API [{}]: {} operation completed in {}ms, response: \n\n: {}", debugId, operation, duration, responseText);
            if (responseText == null || responseText.trim().isEmpty()) {
                logger.warn("GEMINI-API [{}]: Received empty or null response from Gemini API", debugId);
                throw new RuntimeException("Received empty response from Gemini API");
            }
            return responseText;            
        } catch (Exception e) {
            logger.error("GEMINI-API [{}]: Error calling Gemini API for {} operation: {}", debugId, operation, e.getMessage(), e);
            throw new RuntimeException("Failed to call Gemini API: " + e.getMessage(), e);
        }
    }
    
    private String buildConversationEnrichmentPrompt(String conversationText) {
        String template = promptLoader.loadPromptTemplate("conversation-enrichment");
        if (template.contains(messagePlaceholder)) {
            return template.replace(messagePlaceholder, messagePlaceholder + "\n" +conversationText);
        } else {
            return template + "\n" + conversationText;
        }
    }
    
    private ConversationEnrichment parseConversationEnrichmentResponse(String response, List<SlackMessage> messages) {
        try {
            String cleanedResponse = JsonUtils.cleanJsonResponse(response);
            logger.info("GEMINI-PARSE: Cleaned response:\n{}", cleanedResponse);

            if (cleanedResponse.trim().startsWith("[")) {
                return parseTopicsFromText(cleanedResponse, messages);
            }

            List<ParticipantInsight> participantsInsights = new ArrayList<>();
            return new ConversationEnrichment(
                List.of(), // No topics
                participantsInsights, // Participant insights
                messages.stream()
                    .map(msg -> new MessageEnrichment("General Error parsing AI response", 0.0, "Unknown", List.of(), 0.0))
                    .toList(), // Default message enrichments

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
    
    // Utility methods
    private String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(this::formatMessageForAnalysis)
            .collect(Collectors.joining("\n"));
    }
    
    private String formatMessageForAnalysis(SlackMessage msg) {
        //  {
        //   “role”: “user”,
        //   “content”: “...“,
        //   “author”: “Benoit”,
        //   “timestamp”: ...,
        //   “channel_id”: “...“,
        //   “thread_ts”: ...
        //  },

        try {
            Map<String, Object> messageMap = new LinkedHashMap<>();
            messageMap.put("ROLE", safeString("user"));
            
            // Message content (prioritize content over text)
            String content = msg.getContent();
            if (content == null || content.trim().isEmpty()) {
                content = msg.getText();
            }
            messageMap.put("CONTENT", safeString(content));
            messageMap.put("USER_ID", safeString(msg.getSlackUserId()));
            messageMap.put("AUTHOR", safeString(msg.getDisplayName()));

            messageMap.put("TIMESTAMP", msg.getTimestamp());
            // Channel and thread context
            messageMap.put("CHANNEL_ID", safeString(msg.getChannelId()));

            if (msg.getThreadTs() != null) {
                messageMap.put("THREAD_TS", msg.getThreadTs());
            }            
            return objectMapper.writeValueAsString(messageMap);
        } catch (Exception e) {
            logger.warn("Failed to format message as JSON: {}", e.getMessage());
            return "{}";
        }
    }
    
    private String safeString(String value) {
        return value != null ? value : "N/A";
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
                if (item instanceof Map<?, ?> userMap) {
                    // New format: user object
                    String id = extractStringValue(userMap, "id", null);
                    // Enrich with data from Redis if available
                    UserDTO enrichedUser = enrichUserDTO(id, tenantId, workspaceId);
                    // check if user ID is not null and not N/A
                    if (enrichedUser.id() != null && !enrichedUser.id().equals("N/A")) {
                        result.add(enrichedUser);
                    }
                } else if (item instanceof String userString) {
                    // First try to parse as a UserDTO
                    if (userString.startsWith("{") && userString.endsWith("}")) {
                        try {
                            UserDTO user = objectMapper.readValue(userString, UserDTO.class);
                            // Enrich with Redis data
                            UserDTO enrichedUser = enrichUserDTO(user.id(), tenantId, workspaceId);
                            // check if user ID is not null and not N/A
                            if (enrichedUser.id() != null && !enrichedUser.id().equals("N/A")) {
                                result.add(enrichedUser);
                            }
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to parse user string as UserDTO: {}", userString, e);
                            // Fallback to old format handling
                            UserDTO basicUser = UserDTO.fromString(userString);
                            // Try to enrich with Redis data
                            UserDTO enrichedUser = enrichUserDTO(basicUser.id(), tenantId, workspaceId);
                            // check if user ID is not null and not N/A
                            if (enrichedUser.id() != null && !enrichedUser.id().equals("N/A")) {
                                result.add(enrichedUser);
                            }
                        }
                    }
                }
            }
            return result;
        }
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
                    return new UserDTO(
                        userId,
                        user.getName(),
                        user.getDisplayName(),
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
            String displayName = user != null ? user.getDisplayName() : null;
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
                null,  // username
                null,  // displayName
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
}
