package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

@Component("langchainProvider")
public class LangChainProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(LangChainProvider.class);
    
    @Value("${ai.providers.langchain.endpoint:http://localhost:8000}")
    private String apiEndpoint;
    
    @Value("${ai.providers.langchain.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.langchain.timeout:30000}")
    private int timeoutMs;
    
    @Value("${ai.providers.langchain.model:gpt-3.5-turbo}")
    private String model;
    
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private double lastConfidence = 0.0;
    
    public LangChainProvider(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        try {
            logger.debug("Categorizing content with LangChain: length={}", content.length());
            
            Map<String, Object> request = Map.of(
                "task", "categorize",
                "content", content,
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/categorize", request);
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("Categorization result: category={}, confidence={}", 
                        result.category(), result.confidence());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to categorize with LangChain", e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        try {
            logger.debug("Summarizing content with LangChain: length={}", content.length());
            
            Map<String, Object> request = Map.of(
                "task", "summarize",
                "content", content,
                "model", model,
                "max_length", 200
            );
            
            Map<String, Object> response = callLangChainAPI("/summarize", request);
            return parseSummaryResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to summarize with LangChain", e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, List<SlackParticipant> participants) {
        try {
            logger.info("Enriching conversation with LangChain: {} messages, {} participants", 
                       messages.size(), participants != null ? participants.size() : 0);
            
            Map<String, Object> conversationData = prepareConversationData(messages, participants);
            Map<String, Object> request = Map.of(
                "task", "enrich_conversation",
                "conversation", conversationData,
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/enrich", request);
            return parseConversationEnrichmentResponse(response, messages, participants);
            
        } catch (Exception e) {
            logger.error("Failed to enrich conversation with LangChain", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        try {
            Map<String, Object> request = Map.of(
                "task", "enrich_message",
                "content", content,
                "context", context != null ? context : Map.of(),
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/enrich/message", request);
            return parseMessageEnrichmentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to enrich message with LangChain", e);
            return getDefaultMessageEnrichment();
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        try {
            List<SlackMessage> participantMessages = messages.stream()
                .filter(msg -> msg.getUserId().equals(participant.getId()))
                .collect(Collectors.toList());
                
            Map<String, Object> request = Map.of(
                "task", "analyze_participant",
                "participant", participantToMap(participant),
                "messages", participantMessages.stream().map(this::messageToMap).collect(Collectors.toList()),
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/analyze/participant", request);
            return parseParticipantInsightResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze participant with LangChain", e);
            return getDefaultParticipantInsight();
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        try {
            Map<String, Object> request = Map.of(
                "task", "assess_urgency",
                "messages", messages.stream().map(this::messageToMap).collect(Collectors.toList()),
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/analyze/urgency", request);
            return parseUrgencyLevelResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to assess urgency with LangChain", e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        try {
            Map<String, Object> request = Map.of(
                "task", "generate_topic",
                "messages", messages.stream().map(this::messageToMap).collect(Collectors.toList()),
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/generate/topic", request);
            return parseTopicGenerationResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to generate topic with LangChain", e);
            return "General Discussion";
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        try {
            Map<String, Object> request = Map.of(
                "task", "extract_entities",
                "content", content,
                "model", model,
                "entity_types", List.of("PERSON", "ORG", "GPE", "PRODUCT", "EVENT")
            );
            
            Map<String, Object> response = callLangChainAPI("/extract/entities", request);
            return parseEntityExtractionResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to extract entities with LangChain", e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        try {
            Map<String, Object> request = Map.of(
                "task", "analyze_sentiment",
                "content", content,
                "model", model
            );
            
            Map<String, Object> response = callLangChainAPI("/analyze/sentiment", request);
            return parseSentimentResultResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze sentiment with LangChain", e);
            return new SentimentResult("neutral", 0.5, 0.5);
        }
    }
    
    @Override
    public boolean isAvailable() {
        try {
            Map<String, Object> request = Map.of("ping", "health_check");
            Map<String, Object> response = callLangChainAPI("/health", request);
            return response != null && "ok".equals(response.get("status"));
        } catch (Exception e) {
            logger.warn("LangChain health check failed", e);
            return false;
        }
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    @Override
    public String getProviderId() {
        return "langchain";
    }
    
    private Map<String, Object> callLangChainAPI(String endpoint, Map<String, Object> requestBody) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        if (apiKey != null && !apiKey.trim().isEmpty()) {
            headers.set("Authorization", "Bearer " + apiKey);
        }
        
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);
        String url = apiEndpoint + endpoint;
        
        logger.debug("Calling LangChain API: {}", url);
        
        ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
        
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("LangChain API call failed with status: " + response.getStatusCode());
        }
        
        return objectMapper.readValue(response.getBody(), Map.class);
    }
    
    // Data preparation methods
    private Map<String, Object> prepareConversationData(List<SlackMessage> messages, List<SlackParticipant> participants) {
        Map<String, Object> data = new HashMap<>();
        data.put("messages", messages.stream().map(this::messageToMap).collect(Collectors.toList()));
        
        if (participants != null) {
            data.put("participants", participants.stream().map(this::participantToMap).collect(Collectors.toList()));
        }
        
        return data;
    }
    
    private Map<String, Object> messageToMap(SlackMessage message) {
        Map<String, Object> map = new HashMap<>();
        map.put("text", message.getContent());
        map.put("userId", message.getUserId());
        map.put("userName", message.getUsername());
        map.put("timestamp", message.getTimestamp());
        map.put("channelId", message.getChannelId());
        if (message.getThreadId() != null) {
            map.put("threadTs", message.getThreadId());
        }
        return map;
    }
    
    private Map<String, Object> participantToMap(SlackParticipant participant) {
        Map<String, Object> map = new HashMap<>();
        map.put("userId", participant.getId());
        map.put("displayName", participant.getDisplayName());
        map.put("email", participant.getEmail());
        map.put("role", participant.getRole());
        return map;
    }
    
    // Response parsing methods
    private CategoryResult parseCategorizationResponse(Map<String, Object> response) {
        try {
            String category = (String) response.getOrDefault("category", "Uncategorized");
            Object confidenceObj = response.get("confidence");
            double confidence = confidenceObj instanceof Number ? ((Number) confidenceObj).doubleValue() : 0.5;
            
            return new CategoryResult(category, confidence);
        } catch (Exception e) {
            logger.warn("Failed to parse categorization response: {}", response, e);
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    private SummaryResult parseSummaryResponse(Map<String, Object> response) {
        try {
            String summary = (String) response.getOrDefault("summary", "Summary unavailable");
            Object keyPointsObj = response.get("key_points");
            
            String keyPoints = "";
            if (keyPointsObj instanceof List) {
                List<String> points = (List<String>) keyPointsObj;
                keyPoints = String.join("; ", points);
            } else if (keyPointsObj instanceof String) {
                keyPoints = (String) keyPointsObj;
            }
            
            return new SummaryResult(summary, keyPoints);
        } catch (Exception e) {
            logger.warn("Failed to parse summary response: {}", response, e);
            return new SummaryResult("Summary unavailable", "Error parsing response");
        }
    }
    
    private ConversationEnrichment parseConversationEnrichmentResponse(Map<String, Object> response, 
            List<SlackMessage> messages, List<SlackParticipant> participants) {
        try {
            String topic = (String) response.getOrDefault("topic", "General Discussion");
            String summary = (String) response.getOrDefault("summary", "No summary available");
            
            // Parse urgency level
            String urgencyStr = (String) response.getOrDefault("urgency", "low");
            UrgencyLevel urgency = parseUrgencyLevel(urgencyStr);
            
            // Create placeholder participant insights and message enrichments
            List<ParticipantInsight> participantInsights = participants != null ? 
                participants.stream().map(this::createDefaultParticipantInsight).collect(Collectors.toList()) :
                List.of();
            
            List<MessageEnrichment> messageEnrichments = messages.stream()
                .map(this::createDefaultMessageEnrichment).collect(Collectors.toList());
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("processed_at", System.currentTimeMillis());
            
            return new ConversationEnrichment(topic, summary, urgency, participantInsights, messageEnrichments, metadata);
            
        } catch (Exception e) {
            logger.warn("Failed to parse conversation enrichment response: {}", response, e);
            return getDefaultConversationEnrichment();
        }
    }
    
    private MessageEnrichment parseMessageEnrichmentResponse(Map<String, Object> response) {
        try {
            String category = (String) response.getOrDefault("category", "general");
            Object sentimentObj = response.get("sentiment");
            double sentiment = sentimentObj instanceof Number ? ((Number) sentimentObj).doubleValue() : 0.5;
            String intent = (String) response.getOrDefault("intent", "information");
            List<String> entities = extractListFromResponse(response, "entities");
            Object confidenceObj = response.get("confidence");
            double confidence = confidenceObj instanceof Number ? ((Number) confidenceObj).doubleValue() : 0.5;
            
            return new MessageEnrichment(category, sentiment, intent, entities, confidence);
        } catch (Exception e) {
            logger.warn("Failed to parse message enrichment response: {}", response, e);
            return getDefaultMessageEnrichment();
        }
    }
    

    
    // Helper methods
    @SuppressWarnings("unchecked")
    private List<String> extractListFromResponse(Map<String, Object> response, String key) {
        Object value = response.get(key);
        if (value instanceof List) {
            return (List<String>) value;
        } else if (value instanceof String) {
            return List.of((String) value);
        }
        return List.of();
    }
    
    // Default response methods
    private UrgencyLevel parseUrgencyLevel(String urgencyStr) {
        if (urgencyStr == null) return UrgencyLevel.LOW;
        
        return switch (urgencyStr.toLowerCase()) {
            case "high", "urgent" -> UrgencyLevel.HIGH;
            case "critical", "emergency" -> UrgencyLevel.CRITICAL;
            case "medium", "moderate" -> UrgencyLevel.MEDIUM;
            default -> UrgencyLevel.LOW;
        };
    }
    
    private ParticipantInsight createDefaultParticipantInsight(SlackParticipant participant) {
        return new ParticipantInsight(
            participant.getRole() != null ? participant.getRole() : "member",
            0.5,
            "neutral",
            0
        );
    }
    
    private MessageEnrichment createDefaultMessageEnrichment(SlackMessage message) {
        return new MessageEnrichment("general", 0.5, "information", List.of(), 0.5);
    }
    
    private ParticipantInsight parseParticipantInsightResponse(Map<String, Object> response) {
        try {
            String role = (String) response.getOrDefault("role", "member");
            Object engagementObj = response.get("engagement");
            double engagement = engagementObj instanceof Number ? ((Number) engagementObj).doubleValue() : 0.5;
            String sentiment = (String) response.getOrDefault("sentiment", "neutral");
            Object messageCountObj = response.get("message_count");
            int messageCount = messageCountObj instanceof Number ? ((Number) messageCountObj).intValue() : 0;
            
            return new ParticipantInsight(role, engagement, sentiment, messageCount);
        } catch (Exception e) {
            logger.warn("Failed to parse participant insight response: {}", response, e);
            return new ParticipantInsight("member", 0.5, "neutral", 0);
        }
    }
    
    private UrgencyLevel parseUrgencyLevelResponse(Map<String, Object> response) {
        try {
            String urgency = (String) response.getOrDefault("urgency", "low");
            return parseUrgencyLevel(urgency);
        } catch (Exception e) {
            logger.warn("Failed to parse urgency level response: {}", response, e);
            return UrgencyLevel.LOW;
        }
    }
    
    private String parseTopicGenerationResponse(Map<String, Object> response) {
        try {
            String topic = (String) response.getOrDefault("topic", "General Discussion");
            return topic;
        } catch (Exception e) {
            logger.warn("Failed to parse topic generation response: {}", response, e);
            return "General Discussion";
        }
    }
    
    private SentimentResult parseSentimentResultResponse(Map<String, Object> response) {
        try {
            String sentiment = (String) response.getOrDefault("sentiment", "neutral");
            Object scoreObj = response.get("score");
            double score = scoreObj instanceof Number ? ((Number) scoreObj).doubleValue() : 0.5;
            Object confidenceObj = response.get("confidence");
            double confidence = confidenceObj instanceof Number ? ((Number) confidenceObj).doubleValue() : 0.5;
            
            return new SentimentResult(sentiment, score, confidence);
        } catch (Exception e) {
            logger.warn("Failed to parse sentiment result response: {}", response, e);
            return new SentimentResult("neutral", 0.5, 0.5);
        }
    }
    
    private List<String> parseEntityExtractionResponse(Map<String, Object> response) {
        try {
            return extractListFromResponse(response, "entities");
        } catch (Exception e) {
            logger.warn("Failed to parse entity extraction response: {}", response, e);
            return List.of();
        }
    }
    
    private ConversationEnrichment getDefaultConversationEnrichment() {
        return new ConversationEnrichment(
            "General Discussion", 
            "Analysis unavailable", 
            UrgencyLevel.LOW, 
            List.of(), 
            List.of(),
            Map.of()
        );
    }
    
    private MessageEnrichment getDefaultMessageEnrichment() {
        return new MessageEnrichment("general", 0.5, "information", List.of(), 0.5);
    }
    
    private ParticipantInsight getDefaultParticipantInsight() {
        return new ParticipantInsight("member", 0.5, "neutral", 0);
    }
}
