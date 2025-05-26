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

@Component("geminiProvider")
public class GeminiProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiProvider.class);
    
    @Value("${ai.providers.gemini.api-key:}")
    private String apiKey;
    
    @Value("${ai.providers.gemini.endpoint:https://generativelanguage.googleapis.com/v1/models}")
    private String apiEndpoint;
    
    @Value("${ai.providers.gemini.model:gemini-pro}")
    private String model;
    
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private double lastConfidence = 0.0;
    
    public GeminiProvider(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public CategoryResult categorize(String content) {
        try {
            logger.debug("Categorizing content with Gemini: length={}", content.length());
            
            String prompt = buildCategorizationPrompt(content);
            String response = callGeminiAPI(prompt);
            
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("Categorization result: category={}, confidence={}", 
                        result.category(), result.confidence());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to categorize with Gemini", e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        try {
            logger.debug("Summarizing content with Gemini: length={}", content.length());
            
            String prompt = buildSummarizationPrompt(content);
            String response = callGeminiAPI(prompt);
            
            return parseSummaryResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to summarize with Gemini", e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, List<SlackParticipant> participants) {
        try {
            logger.info("Enriching conversation with Gemini: {} messages, {} participants", 
                       messages.size(), participants != null ? participants.size() : 0);
            
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants);
            String response = callGeminiAPI(prompt);
            
            return parseConversationEnrichmentResponse(response, messages, participants);
            
        } catch (Exception e) {
            logger.error("Failed to enrich conversation with Gemini", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        try {
            String prompt = buildMessageEnrichmentPrompt(content, context);
            String response = callGeminiAPI(prompt);
            return parseMessageEnrichmentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to enrich message with Gemini", e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        try {
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> participant.getId().equals(msg.getUserId()))
                .collect(Collectors.toList());
                
            String prompt = buildParticipantAnalysisPrompt(participant, userMessages);
            String response = callGeminiAPI(prompt);
            return parseParticipantAnalysisResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze participant with Gemini", e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages);
            String prompt = buildUrgencyAssessmentPrompt(conversationText);
            String response = callGeminiAPI(prompt);
            return parseUrgencyResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to assess urgency with Gemini", e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        try {
            String conversationText = formatConversationForAnalysis(messages.stream().limit(10).collect(Collectors.toList()));
            String prompt = buildTopicGenerationPrompt(conversationText);
            String response = callGeminiAPI(prompt);
            return parseTopicResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to generate topic with Gemini", e);
            return "General Discussion";
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        try {
            String prompt = buildEntityExtractionPrompt(content);
            String response = callGeminiAPI(prompt);
            return parseEntitiesResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to extract entities with Gemini", e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        try {
            String prompt = buildSentimentAnalysisPrompt(content);
            String response = callGeminiAPI(prompt);
            return parseSentimentResponse(response);
            
        } catch (Exception e) {
            logger.error("Failed to analyze sentiment with Gemini", e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        }
    }
    
    @Override
    public String getProviderId() {
        return "gemini";
    }
    
    @Override
    public boolean isAvailable() {
        try {
            if (apiKey == null || apiKey.trim().isEmpty()) {
                logger.warn("Gemini API key not configured");
                return false;
            }
            
            // Simple test call
            String testPrompt = "Hello";
            callGeminiAPI(testPrompt);
            return true;
            
        } catch (Exception e) {
            logger.warn("Gemini provider unavailable: {}", e.getMessage());
            return false;
        }
    }
    
    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }
    
    // Private helper methods
    private String callGeminiAPI(String prompt) {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new RuntimeException("Gemini API key not configured");
        }
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        Map<String, Object> requestBody = Map.of(
            "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
            "generationConfig", Map.of(
                "temperature", 0.1,
                "maxOutputTokens", 2048,
                "topP", 0.8,
                "topK", 10
            )
        );
        
        try {
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);
            String url = apiEndpoint + "/" + model + ":generateContent?key=" + apiKey;
            
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                return extractTextFromGeminiResponse(response.getBody());
            } else {
                throw new RuntimeException("Gemini API call failed with status: " + response.getStatusCode());
            }
            
        } catch (Exception e) {
            logger.error("Error calling Gemini API", e);
            throw new RuntimeException("Failed to call Gemini API: " + e.getMessage(), e);
        }
    }
    
    private String extractTextFromGeminiResponse(String responseBody) {
        try {
            Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> candidates = (List<Map<String, Object>>) responseMap.get("candidates");
            
            if (candidates != null && !candidates.isEmpty()) {
                Map<String, Object> candidate = candidates.get(0);
                
                @SuppressWarnings("unchecked")
                Map<String, Object> content = (Map<String, Object>) candidate.get("content");
                
                if (content != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> parts = (List<Map<String, Object>>) content.get("parts");
                    
                    if (parts != null && !parts.isEmpty()) {
                        return (String) parts.get(0).get("text");
                    }
                }
            }
            
            throw new RuntimeException("No text found in Gemini response");
            
        } catch (Exception e) {
            logger.error("Failed to parse Gemini response: {}", responseBody, e);
            throw new RuntimeException("Failed to parse Gemini response", e);
        }
    }
    
    // Prompt building methods
    private String buildCategorizationPrompt(String content) {
        return String.format("""
            Categorize this message into one of these categories:
            - Customer Inquiry
            - Technical Support
            - Billing Issue
            - Feature Request
            - Complaint
            - General Discussion
            - Urgent Issue
            
            Message: "%s"
            
            Respond with only the category name followed by a confidence score (0.0-1.0).
            Format: "Category|confidence"
            Example: "Customer Inquiry|0.85"
            """, content);
    }
    
    private String buildSummarizationPrompt(String content) {
        return String.format("""
            Summarize the following content in 2-3 sentences. Focus on key points and main ideas.
            
            Content: "%s"
            
            Summary:
            """, content);
    }
    
    private String buildConversationEnrichmentPrompt(String conversationText, List<SlackParticipant> participants) {
        StringBuilder participantInfo = new StringBuilder();
        if (participants != null) {
            participantInfo.append("Participants: ");
            participants.forEach(p -> participantInfo.append(p.getUsername()).append(" "));
        }
        
        return String.format("""
            Analyze this Slack conversation and provide comprehensive enrichment.
            %s
            
            Conversation:
            %s
            
            Provide analysis in JSON format with these fields:
            {
                "topic": "main discussion topic",
                "summary": "comprehensive summary",
                "urgency": "LOW|MEDIUM|HIGH|CRITICAL",
                "sentiment": "overall conversation sentiment",
                "actionItems": ["item1", "item2"],
                "keyInsights": ["insight1", "insight2"]
            }
            """, participantInfo.toString(), conversationText);
    }
    
    private String buildMessageEnrichmentPrompt(String content, Map<String, Object> context) {
        return String.format("""
            Analyze this message and provide enrichment data:
            
            Message: "%s"
            Context: %s
            
            Provide response in format:
            Category: [category]
            Sentiment: [sentiment_score between -1 and 1]
            Intent: [user intent]
            Entities: [comma-separated entities]
            Confidence: [confidence_score between 0 and 1]
            """, content, context != null ? context.toString() : "None");
    }
    
    private String buildParticipantAnalysisPrompt(SlackParticipant participant, List<SlackMessage> messages) {
        String messagesText = messages.stream()
            .map(SlackMessage::getContent)
            .collect(Collectors.joining("\n"));
            
        return String.format("""
            Analyze this participant's communication patterns:
            
            Participant: %s
            Messages (%d total):
            %s
            
            Provide analysis in format:
            Role: [participant role in conversation]
            Engagement: [engagement level 0.0-1.0]
            Sentiment: [dominant sentiment]
            """, participant.getUsername(), messages.size(), messagesText);
    }
    
    private String buildUrgencyAssessmentPrompt(String conversationText) {
        return String.format("""
            Assess the urgency level of this conversation.
            Consider factors like: time sensitivity, impact level, escalation words, customer satisfaction.
            
            Conversation:
            %s
            
            Respond with only one of: LOW, MEDIUM, HIGH, CRITICAL
            """, conversationText);
    }
    
    private String buildTopicGenerationPrompt(String conversationText) {
        return String.format("""
            Generate a concise topic/title for this conversation (max 5 words):
            
            Conversation:
            %s
            
            Topic:
            """, conversationText);
    }
    
    private String buildEntityExtractionPrompt(String content) {
        return String.format("""
            Extract key entities from this text. Include names, organizations, products, locations, etc.
            
            Text: "%s"
            
            Respond with comma-separated entities:
            """, content);
    }
    
    private String buildSentimentAnalysisPrompt(String content) {
        return String.format("""
            Analyze the sentiment of this text.
            
            Text: "%s"
            
            Respond in format:
            Sentiment: [Positive|Negative|Neutral]
            Score: [numeric score between -1 and 1]
            Confidence: [confidence between 0 and 1]
            """, content);
    }
    
    // Response parsing methods
    private CategoryResult parseCategorizationResponse(String response) {
        try {
            String[] parts = response.trim().split("\\|");
            if (parts.length >= 2) {
                String category = parts[0].trim();
                double confidence = Double.parseDouble(parts[1].trim());
                return new CategoryResult(category, confidence);
            } else {
                return new CategoryResult(response.trim(), 0.8);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse categorization response: {}", response, e);
            return new CategoryResult("General Discussion", 0.5);
        }
    }
    
    private SummaryResult parseSummaryResponse(String response) {
        return new SummaryResult(response.trim(), "Generated by Gemini");
    }
    
    private ConversationEnrichment parseConversationEnrichmentResponse(String response, 
            List<SlackMessage> messages, List<SlackParticipant> participants) {
        
        try {
            // Try to parse JSON response
            Map<String, Object> analysis = objectMapper.readValue(response, Map.class);
            
            String topic = (String) analysis.getOrDefault("topic", "General Discussion");
            String summary = (String) analysis.getOrDefault("summary", "No summary available");
            String urgencyStr = (String) analysis.getOrDefault("urgency", "LOW");
            UrgencyLevel urgency = UrgencyLevel.valueOf(urgencyStr);
            
            // Create participant insights
            List<ParticipantInsight> participantInsights = participants != null ? 
                participants.stream()
                    .map(p -> analyzeParticipant(p, messages))
                    .collect(Collectors.toList()) : 
                List.of();
            
            // Create message enrichments
            List<MessageEnrichment> messageEnrichments = messages.stream()
                .map(msg -> enrichMessage(msg.getContent(), Map.of()))
                .collect(Collectors.toList());
            
            return new ConversationEnrichment(topic, summary, urgency, 
                                            participantInsights, messageEnrichments, analysis);
                                            
        } catch (Exception e) {
            logger.warn("Failed to parse conversation enrichment response, using fallback", e);
            return getDefaultConversationEnrichment();
        }
    }
    
    private MessageEnrichment parseMessageEnrichmentResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String category = "General";
            double sentiment = 0.0;
            String intent = "Unknown";
            List<String> entities = List.of();
            double confidence = 0.5;
            
            for (String line : lines) {
                if (line.startsWith("Category:")) {
                    category = line.substring(9).trim();
                } else if (line.startsWith("Sentiment:")) {
                    sentiment = Double.parseDouble(line.substring(10).trim());
                } else if (line.startsWith("Intent:")) {
                    intent = line.substring(7).trim();
                } else if (line.startsWith("Entities:")) {
                    String entitiesStr = line.substring(9).trim();
                    entities = Arrays.asList(entitiesStr.split(",\\s*"));
                } else if (line.startsWith("Confidence:")) {
                    confidence = Double.parseDouble(line.substring(11).trim());
                }
            }
            
            return new MessageEnrichment(category, sentiment, intent, entities, confidence);
            
        } catch (Exception e) {
            logger.warn("Failed to parse message enrichment response: {}", response, e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    private ParticipantInsight parseParticipantAnalysisResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String role = "Participant";
            double engagement = 0.5;
            String sentiment = "Neutral";
            
            for (String line : lines) {
                if (line.startsWith("Role:")) {
                    role = line.substring(5).trim();
                } else if (line.startsWith("Engagement:")) {
                    engagement = Double.parseDouble(line.substring(11).trim());
                } else if (line.startsWith("Sentiment:")) {
                    sentiment = line.substring(10).trim();
                }
            }
            
            return new ParticipantInsight(role, engagement, sentiment, 0);
            
        } catch (Exception e) {
            logger.warn("Failed to parse participant analysis response: {}", response, e);
            return new ParticipantInsight("Participant", 0.5, "Neutral", 0);
        }
    }
    
    private UrgencyLevel parseUrgencyResponse(String response) {
        try {
            String urgencyStr = response.trim().toUpperCase();
            return UrgencyLevel.valueOf(urgencyStr);
        } catch (Exception e) {
            logger.warn("Failed to parse urgency response: {}", response, e);
            return UrgencyLevel.LOW;
        }
    }
    
    private String parseTopicResponse(String response) {
        return response.trim();
    }
    
    private List<String> parseEntitiesResponse(String response) {
        try {
            return Arrays.asList(response.trim().split(",\\s*"));
        } catch (Exception e) {
            logger.warn("Failed to parse entities response: {}", response, e);
            return List.of();
        }
    }
    
    private SentimentResult parseSentimentResponse(String response) {
        try {
            String[] lines = response.split("\n");
            String sentiment = "Neutral";
            double score = 0.0;
            double confidence = 0.5;
            
            for (String line : lines) {
                if (line.startsWith("Sentiment:")) {
                    sentiment = line.substring(10).trim();
                } else if (line.startsWith("Score:")) {
                    score = Double.parseDouble(line.substring(6).trim());
                } else if (line.startsWith("Confidence:")) {
                    confidence = Double.parseDouble(line.substring(11).trim());
                }
            }
            
            return new SentimentResult(sentiment, score, confidence);
            
        } catch (Exception e) {
            logger.warn("Failed to parse sentiment response: {}", response, e);
            return new SentimentResult("Neutral", 0.0, 0.5);
        }
    }
    
    // Utility methods
    private String formatConversationForAnalysis(List<SlackMessage> messages) {
        return messages.stream()
            .map(msg -> String.format("[%s] %s: %s", 
                msg.getTimestamp(), msg.getUsername(), msg.getContent()))
            .collect(Collectors.joining("\n"));
    }
    
    private ConversationEnrichment getDefaultConversationEnrichment() {
        return new ConversationEnrichment(
            "General Discussion",
            "No summary available",
            UrgencyLevel.LOW,
            List.of(),
            List.of(),
            Map.of("error", "Failed to analyze conversation")
        );
    }
}
