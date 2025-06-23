package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Component("geminiProvider")
public class GeminiProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(GeminiProvider.class);
    
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
        String debugId = "CATEGORIZE-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting categorization - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for categorization", debugId);
                this.lastConfidence = 0.0;
                return new CategoryResult("Uncategorized", 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for categorization", debugId);
                this.lastConfidence = 0.0;
                return new CategoryResult("Uncategorized", 0.0);
            }
            
            String prompt = buildCategorizationPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built categorization prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "categorization", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for categorization", debugId);
            
            CategoryResult result = parseCategorizationResponse(response);
            this.lastConfidence = result.confidence();
            
            logger.debug("GEMINI-DEBUG [{}]: Categorization successful - category: {}, confidence: {}", 
                        debugId, result.category(), result.confidence());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during categorization: {}", debugId, e.getMessage(), e);
            this.lastConfidence = 0.0;
            return new CategoryResult("Uncategorized", 0.0);
        }
    }
    
    @Override
    public SummaryResult summarize(String content) {
        String debugId = "SUMMARIZE-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting summarization - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for summarization", debugId);
                return new SummaryResult("Summary unavailable", "No content provided");
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for summarization", debugId);
                return new SummaryResult("Summary unavailable", "AI service unavailable");
            }
            
            String prompt = buildSummarizationPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built summarization prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "summarization", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for summarization", debugId);
            
            SummaryResult result = parseSummaryResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Summarization successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Invalid input: " + e.getMessage());
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Service error occurred");
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during summarization: {}", debugId, e.getMessage(), e);
            return new SummaryResult("Summary unavailable", "Error occurred");
        }
    }
    
    /**
     * Enhanced enrichConversation method that accepts available categories as input parameter
     * 
     * @param messages The list of Slack messages to analyze
     * @param participants The list of participants in the conversation
     * @param availableCategories The list of available categories to use for categorization.
     *                           If null or empty, will attempt to fetch from data storage service,
     *                           and fallback to default categories if that fails.
     * @return ConversationEnrichment object containing the analysis results
     */
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, 
                                                   List<SlackParticipant> participants, 
                                                   List<String> availableCategories) {
        String debugId = "ENRICH-CONV-" + System.currentTimeMillis();
        logger.info("GEMINI-ENRICH [{}]: Starting conversation enrichment", debugId);
        try {
            if (messages != null) {
                for (int i = 0; i < Math.min(messages.size(), 5); i++) {
                    SlackMessage msg = messages.get(i);
                    logger.debug("GEMINI-ENRICH [{}]: Message {}: {} chars from user {} at {}", 
                                debugId, i+1, 
                                msg.getContent() != null ? msg.getContent().length() : 0,
                                msg.getUsername(),
                                msg.getTimestamp());
                }
                if (messages.size() > 5) {
                    logger.debug("GEMINI-ENRICH [{}]: ... and {} more messages", debugId, messages.size() - 5);
                }
            }
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-ENRICH [{}]: No messages provided, returning default enrichment", debugId);
                return getDefaultConversationEnrichment();
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-ENRICH [{}]: Gemini client not available, returning default enrichment", debugId);
                return getDefaultConversationEnrichment();
            }
            
            logger.info("GEMINI-ENRICH [{}]: Formatting conversation for analysis", debugId);
            String conversationText = formatConversationForAnalysis(messages);
            // logger.info("GEMINI-ENRICH [{}]: Formatted conversation text \n\n: {}", debugId, conversationText);
            
            List<String> categoriesToUse = List.of();
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants, categoriesToUse);
            // logger.info("GEMINI-ENRICH [{}]: Prompt built, \n\n: {}", debugId, prompt);

            String response = callGeminiAPI(prompt, "conversation-enrichment", debugId);
            logger.info("GEMINI-ENRICH [{}]: Received response from Gemini API, length: {}", debugId, response);
            ConversationEnrichment result = parseConversationEnrichmentResponse(response, messages, participants);
            
            // Log the parsed result details
            logger.info("GEMINI-ENRICH [{}]: Result summary - {} topics, {} participant insights, {} message enrichments", 
                       debugId, 
                       result.topics() != null ? result.topics().size() : 0,
                       result.participants() != null ? result.participants().size() : 0,
                       result.messages() != null ? result.messages().size() : 0);
            
            if (result.topics() != null) {
                for (int i = 0; i < result.topics().size(); i++) {
                    TopicEnrichment topic = result.topics().get(i);
                    logger.info("GEMINI-ENRICH [{}]: Topic {}: '{}' (urgency: {}, category: {})", 
                               debugId, i+1, topic.title(), topic.urgency(), topic.category());
                    logger.debug("GEMINI-ENRICH [{}]: Topic {} summary: {}", debugId, i+1, topic.shortSummary());
                }
            }
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-ENRICH [{}]: Invalid input for conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        } catch (RuntimeException e) {
            logger.error("GEMINI-ENRICH [{}]: API error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        } catch (Exception e) {
            logger.error("GEMINI-ENRICH [{}]: Unexpected error during conversation enrichment: {}", debugId, e.getMessage(), e);
            return getDefaultConversationEnrichment();
        }
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        String debugId = "ENRICH-MSG-" + System.currentTimeMillis();
        try {
            int contextSize = context != null ? context.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting message enrichment - content length: {}, context size: {}", 
                        debugId, content != null ? content.length() : 0, contextSize);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for message enrichment", debugId);
                return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for message enrichment", debugId);
                return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
            }
            
            String prompt = buildMessageEnrichmentPrompt(content, context);
            logger.debug("GEMINI-DEBUG [{}]: Built message enrichment prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "message-enrichment", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for message enrichment", debugId);
            
            MessageEnrichment result = parseMessageEnrichmentResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Message enrichment successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during message enrichment: {}", debugId, e.getMessage(), e);
            return new MessageEnrichment("General", 0.0, "Unknown", List.of(), 0.0);
        }
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        String debugId = "ANALYZE-PARTICIPANT-" + System.currentTimeMillis();
        try {
            String participantId = participant != null ? participant.getId() : "null";
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting participant analysis - participant: {}, total messages: {}", 
                        debugId, participantId, messagesCount);
            
            // Input validation
            if (participant == null) {
                logger.warn("GEMINI-DEBUG [{}]: Null participant provided for analysis", debugId);
                return new ParticipantInsight(0.0, "Neutral", 0);
            }
            
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for participant analysis", debugId);
                return new ParticipantInsight(0.0, "Neutral", 0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for participant analysis", debugId);
                return new ParticipantInsight(0.0, "Neutral", 0);
            }
            
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> participant.getId().equals(msg.getUserId()))
                .collect(Collectors.toList());
                
            logger.debug("GEMINI-DEBUG [{}]: Filtered to {} messages from participant {}", 
                        debugId, userMessages.size(), participantId);
            
            String prompt = buildParticipantAnalysisPrompt(participant, userMessages);
            logger.debug("GEMINI-DEBUG [{}]: Built participant analysis prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "participant-analysis", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for participant analysis", debugId);
            
            ParticipantInsight result = parseParticipantAnalysisResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Participant analysis successful", debugId);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight(0.0, "Neutral", 0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight(0.0, "Neutral", 0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight(0.0, "Neutral", 0);
        }
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        String debugId = "ASSESS-URGENCY-" + System.currentTimeMillis();
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting urgency assessment - {} messages", debugId, messagesCount);
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for urgency assessment", debugId);
                return UrgencyLevel.LOW;
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for urgency assessment", debugId);
                return UrgencyLevel.LOW;
            }
            
            String conversationText = formatConversationForAnalysis(messages);
            logger.debug("GEMINI-DEBUG [{}]: Formatted conversation for urgency analysis, length: {}", debugId, conversationText.length());
            
            String prompt = buildUrgencyAssessmentPrompt(conversationText);
            logger.debug("GEMINI-DEBUG [{}]: Built urgency assessment prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "urgency-assessment", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for urgency assessment", debugId);
            
            UrgencyLevel result = parseUrgencyResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Urgency assessment successful - level: {}", debugId, result);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during urgency assessment: {}", debugId, e.getMessage(), e);
            return UrgencyLevel.LOW;
        }
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        String debugId = "GENERATE-TOPIC-" + System.currentTimeMillis();
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            logger.debug("GEMINI-DEBUG [{}]: Starting topic generation - {} messages", debugId, messagesCount);
            
            // Input validation
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for topic generation", debugId);
                return "General Discussion";
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for topic generation", debugId);
                return "General Discussion";
            }
            
            String conversationText = formatConversationForAnalysis(messages.stream().limit(10).collect(Collectors.toList()));
            logger.debug("GEMINI-DEBUG [{}]: Formatted conversation for topic generation, length: {}", debugId, conversationText.length());
            
            String prompt = buildTopicGenerationPrompt(conversationText);
            logger.debug("GEMINI-DEBUG [{}]: Built topic generation prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "topic-generation", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for topic generation", debugId);
            
            String result = parseTopicResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Topic generation successful - topic: {}", debugId, result);
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during topic generation: {}", debugId, e.getMessage(), e);
            return "General Discussion";
        }
    }
    
    @Override
    public List<String> extractEntities(String content) {
        String debugId = "EXTRACT-ENTITIES-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting entity extraction - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for entity extraction", debugId);
                return List.of();
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for entity extraction", debugId);
                return List.of();
            }
            
            String prompt = buildEntityExtractionPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built entity extraction prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "entity-extraction", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for entity extraction", debugId);
            
            List<String> result = parseEntitiesResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Entity extraction successful - found {} entities", debugId, result.size());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during entity extraction: {}", debugId, e.getMessage(), e);
            return List.of();
        }
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        String debugId = "ANALYZE-SENTIMENT-" + System.currentTimeMillis();
        try {
            logger.debug("GEMINI-DEBUG [{}]: Starting sentiment analysis - content length: {}", debugId, content != null ? content.length() : 0);
            
            // Input validation
            if (content == null || content.trim().isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: Empty or null content provided for sentiment analysis", debugId);
                return new SentimentResult("Neutral", 0.0, 0.0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for sentiment analysis", debugId);
                return new SentimentResult("Neutral", 0.0, 0.0);
            }
            
            String prompt = buildSentimentAnalysisPrompt(content);
            logger.debug("GEMINI-DEBUG [{}]: Built sentiment analysis prompt, length: {}", debugId, prompt.length());
            
            String response = callGeminiAPI(prompt, "sentiment-analysis", debugId);
            logger.debug("GEMINI-DEBUG [{}]: Received API response for sentiment analysis", debugId);
            
            SentimentResult result = parseSentimentResponse(response);
            logger.debug("GEMINI-DEBUG [{}]: Sentiment analysis successful - sentiment: {}, confidence: {}", 
                        debugId, result.sentiment(), result.confidence());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            logger.error("GEMINI-DEBUG [{}]: Invalid input for sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during sentiment analysis: {}", debugId, e.getMessage(), e);
            return new SentimentResult("Neutral", 0.0, 0.0);
        }
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
            
            logger.info("GEMINI-API [{}]: Starting {} operation with prompt prompt:\n\n {}", debugId, operation, prompt);
            long startTime = System.currentTimeMillis();
            GenerateContentResponse response = geminiClient.models.generateContent(
                model, 
                prompt, 
                null
            );
            
            long duration = System.currentTimeMillis() - startTime;
            String responseText = response.text();
            
            logger.info("GEMINI-API [{}]: {} operation completed in {}ms, response: \n\n: {}", 
                       debugId, operation, duration, responseText);
            
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
    
    // Prompt building methods
    private String buildCategorizationPrompt(String content) {
        String template = promptLoader.loadPromptTemplate("categorization");
        return String.format(template, content);
    }
    
    private String buildSummarizationPrompt(String content) {
        String template = promptLoader.loadPromptTemplate("summarization");
        return String.format(template, content);
    }
    
    private String buildConversationEnrichmentPrompt(String conversationText, 
                                                    List<SlackParticipant> participants, 
                                                    List<String> availableCategories) {
        // Build participant names list
        List<String> participantNames = new ArrayList<>();
        if (participants != null) {
            participantNames = participants.stream()
                .map(SlackParticipant::getUsername)
                .toList();
        }
        
        String template = promptLoader.loadPromptTemplate("conversation-enrichment");
        return String.format(template, 
            conversationText,
            participantNames.isEmpty() ? "[]" : participantNames.toString()
        );
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
                double confidence = parseNumericValue(parts[1].trim(), 0.8);
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
            logger.info("GEMINI-PARSE: Starting to parse conversation enrichment response");            
            String cleanedResponse = JsonUtils.cleanJsonResponse(response);
            logger.info("GEMINI-PARSE: Cleaned response:\n{}", cleanedResponse);
            
            // Check if the cleaned response is a JSON array (starts with '[')
            if (cleanedResponse.trim().startsWith("[")) {
                return parseTopicsFromText(cleanedResponse, messages);
            } else {
                logger.info("GEMINI-PARSE: Response is not a JSON array, parsing as object: {}", cleanedResponse);
            }
            // build default conversation enrichment if no topics found
            logger.info("GEMINI-PARSE: No topics found in response, building default conversation enrichment");
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
            logger.error("GEMINI-PARSE: Original response (first 500 chars): {}", 
                        response.length() > 500 ? response.substring(0, 500) + "..." : response);
            logger.error("GEMINI-PARSE: Exception details: {}", e.toString());
            return getDefaultConversationEnrichment();
        }
    }

    private ConversationEnrichment parseTopicsFromText(String topicsText, List<SlackMessage> messages) throws JsonMappingException, JsonProcessingException {
        logger.info("GEMINI-PARSE: Detected JSON array format - parsing as array of topics");

        // Parse as array of topics directly
        List<?> topicsArray = objectMapper.readValue(topicsText, List.class);
        logger.info("GEMINI-PARSE: Successfully parsed {} topics from array", topicsArray.size());
        List<TopicEnrichment> topics = new ArrayList<>();
        for (int i = 0; i < topicsArray.size(); i++) {
            Object topicObj = topicsArray.get(i);
            logger.debug("GEMINI-PARSE: Processing topic {} of type: {}", i, topicObj.getClass().getSimpleName());
            
            if (topicObj instanceof Map<?, ?> topicMap) {
                logger.debug("GEMINI-PARSE: Topic {} map keys: {}", i, topicMap.keySet());
                TopicEnrichment topic = parseTopicFromMap(topicMap, messages);
                topics.add(topic);
                logger.info("GEMINI-PARSE: Successfully parsed topic {}: '{}'", i, topic.title());
            } else {
                logger.warn("GEMINI-PARSE: Topic {} is not a Map, skipping. Type: {}", i, topicObj.getClass());
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
                    sentiment = parseNumericValue(line.substring(10).trim(), 0.0);
                } else if (line.startsWith("Intent:")) {
                    intent = line.substring(7).trim();
                } else if (line.startsWith("Entities:")) {
                    String entitiesStr = line.substring(9).trim();
                    entities = Arrays.asList(entitiesStr.split(",\\s*"));
                } else if (line.startsWith("Confidence:")) {
                    confidence = parseNumericValue(line.substring(11).trim(), 0.5);
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
            double engagement = 0.5;
            String sentiment = "Neutral";
            
            for (String line : lines) {
                if (line.startsWith("Engagement:")) {
                    String engagementStr = line.substring(11).trim();
                    engagement = parseEngagementValue(engagementStr);
                } else if (line.startsWith("Sentiment:")) {
                    sentiment = line.substring(10).trim();
                }
            }
            
            return new ParticipantInsight(engagement, sentiment, 0);
            
        } catch (Exception e) {
            logger.warn("Failed to parse participant analysis response: {}", response, e);
            return new ParticipantInsight(0.5, "Neutral", 0);
        }
    }
    
    /**
     * Extracts numeric engagement value from a string that may contain descriptive text.
     * Handles formats like:
     * - "0.2"
     * - "0.2 (Very low. One message only...)"
     * - "0.8 - High engagement"
     */
    private double parseEngagementValue(String engagementStr) {
        try {
            // First, try to parse directly in case it's just a number
            return Double.parseDouble(engagementStr);
        } catch (NumberFormatException e) {
            // If that fails, extract the first numeric value from the string
            try {
                // Use regex to find first decimal number (including integers)
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([0-9]*\\.?[0-9]+)");
                java.util.regex.Matcher matcher = pattern.matcher(engagementStr);
                
                if (matcher.find()) {
                    double value = Double.parseDouble(matcher.group(1));
                    // Ensure the value is within valid range [0.0, 1.0]
                    return Math.max(0.0, Math.min(1.0, value));
                } else {
                    logger.warn("No numeric value found in engagement string: {}", engagementStr);
                    return 0.5; // Default fallback
                }
            } catch (Exception parseException) {
                logger.warn("Failed to extract numeric value from engagement string: {}", engagementStr, parseException);
                return 0.5; // Default fallback
            }
        }
    }
    
    /**
     * Extracts numeric value from a string that may contain descriptive text.
     * Handles formats like:
     * - "0.8"
     * - "0.8 (High confidence)"
     * - "0.2 - Low value with description"
     */
    private double parseNumericValue(String valueStr, double defaultValue) {
        try {
            // First, try to parse directly in case it's just a number
            return Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
            // If that fails, extract the first numeric value from the string
            try {
                // Use regex to find first decimal number (including integers)
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("([0-9]*\\.?[0-9]+)");
                java.util.regex.Matcher matcher = pattern.matcher(valueStr);
                
                if (matcher.find()) {
                    double value = Double.parseDouble(matcher.group(1));
                    return value;
                } else {
                    logger.warn("No numeric value found in string: {}", valueStr);
                    return defaultValue; // Default fallback
                }
            } catch (Exception parseException) {
                logger.warn("Failed to extract numeric value from string: {}", valueStr, parseException);
                return defaultValue; // Default fallback
            }
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
                    score = parseNumericValue(line.substring(6).trim(), 0.0);
                } else if (line.startsWith("Confidence:")) {
                    confidence = parseNumericValue(line.substring(11).trim(), 0.5);
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
            .map(this::formatMessageForAnalysis)
            .collect(Collectors.joining("\n\n"));
    }
    
    private String formatMessageForAnalysis(SlackMessage msg) {
        try {
            Map<String, Object> messageMap = new LinkedHashMap<>();
            
            // Basic message info
            messageMap.put("MESSAGE_ID", safeString(msg.getId()));
            messageMap.put("TIMESTAMP", msg.getTimestamp());
            String userId = msg.getSlackUserId();
            if (userId == null) {
                userId = msg.getUsername(); // Fallback to username if userId is null
            }
            messageMap.put("USER_ID", safeString(userId));
            
            // Channel and thread context
            messageMap.put("CHANNEL", safeString(msg.getChannelId()));
            if (msg.getThreadTs() != null) {
                messageMap.put("THREAD", msg.getThreadTs());
            }
            
            // Message type and metadata
            if (msg.getMessageType() != null) {
                messageMap.put("TYPE", msg.getMessageType());
            }
            if (msg.getSubtype() != null) {
                messageMap.put("SUBTYPE", msg.getSubtype());
            }
            
            // Message content (prioritize content over text)
            String content = msg.getContent();
            if (content == null || content.trim().isEmpty()) {
                content = msg.getText();
            }
            messageMap.put("CONTENT", safeString(content));            
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
    
    private List<TopicEnrichment> extractTopicsFromResponse(Map<String, Object> analysis, List<SlackMessage> messages) {
        try {
            if (analysis.containsKey("topics") && analysis.get("topics") instanceof List<?> topicsList) {
                List<TopicEnrichment> topics = new ArrayList<>();
                
                for (Object topicObj : topicsList) {
                    if (topicObj instanceof Map<?, ?> topicMap) {
                        TopicEnrichment topic = parseTopicFromMap(topicMap, messages);
                        topics.add(topic);
                    }
                }
                
                return topics;
            } else {
                logger.warn("No topics array found in response, returning empty list");
                return List.of();
            }
        } catch (Exception e) {
            logger.error("Failed to extract topics from response", e);
            return List.of();
        }
    }
    
    private TopicEnrichment parseTopicFromMap(Map<?, ?> topicMap, List<SlackMessage> messages) {
        // extract userInfo from messages for user enrichment
        Map<String, UserDTO> userInfos = messages.stream()
            .collect(Collectors.toMap(
                msg -> msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername(),
                msg -> new UserDTO(msg.getSlackUserId(), msg.getUsername(), msg.getDisplayName(), msg.getImage72()),
                (existing, replacement) -> existing // Keep existing if duplicate
            ));

        String title = extractStringValue(topicMap, "title", "Untitled Topic");
        String shortSummary = extractStringValue(topicMap, "shortSummary", "No summary available");
        shortSummary = TextUtils.replaceSlackMentions(shortSummary, userInfos);
        
        String fullSummary = extractStringValue(topicMap, "fullSummary", "No detailed summary available");
        fullSummary = TextUtils.replaceSlackMentions(fullSummary, userInfos);
        
        String suggestedAction = extractStringValue(topicMap, "suggestedAction", "No action suggested");
        suggestedAction = TextUtils.replaceSlackMentions(suggestedAction, userInfos);

        String clientOrSupplier = extractStringValue(topicMap, "clientOrSupplier", null);
        String deadline = extractStringValue(topicMap, "deadline", null);
        String urgencyStr = extractStringValue(topicMap, "urgency", "Low");
        String category = extractStringValue(topicMap, "category", "General");
        String subCategory = extractStringValue(topicMap, "subCategory", null);
        String periodStartDate = extractStringValue(topicMap, "periodStartDate", null);
        String periodEndDate = extractStringValue(topicMap, "periodEndDate", null);
        String latestMessageDate = extractStringValue(topicMap, "latestMessageDate", null);
        
        UrgencyLevel urgency = mapStringToUrgency(urgencyStr);
        
        LocalDateTime startTime = extractDateTime(topicMap, "startTime");
        LocalDateTime endTime = extractDateTime(topicMap, "endTime");
        
        // Extract tenant and workspace info from messages for user enrichment
        String tenantId = messages.isEmpty() ? null : messages.get(0).getTenantId();
        String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();
        
        List<UserDTO> peopleInvolved = extractPeopleInvolved(topicMap, tenantId, workspaceId);
        List<SummaryPerPerson> summaryPerPerson = extractSummaryPerPerson(topicMap, tenantId, workspaceId, messages);
        Map<String, String> lastMessageDatePerPerson = extractStringMap(topicMap, "lastMessageDatePerPerson");
        List<SuggestedReply> suggestedReplies = extractSuggestedReplies(topicMap);
        ForwardInfo suggestedForwardRecipient = extractForwardInfo(topicMap);
        
        return new TopicEnrichment(title, shortSummary, fullSummary, suggestedAction, 
                                 clientOrSupplier, deadline, urgency, category, subCategory,
                                 startTime, endTime, periodStartDate, periodEndDate, latestMessageDate,
                                 peopleInvolved, summaryPerPerson, lastMessageDatePerPerson, 
                                 suggestedReplies, suggestedForwardRecipient);
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
     * Extract people involved with backward compatibility and user enrichment from Redis
     * Handles both old format (array of strings) and new format (array of user objects)
    *  "peopleInvolved": [
        {
          "id": "U08SABCH6R3",
          "username": "Benoit",
          "displayName": "Benoit",
          "imageUrl": null
        },
        {
          "id": "U08SY01U88L",
          "username": "User B",
          "displayName": "User B",
          "imageUrl": null
        },
        {
          "id": "U08SA5URCHL",
          "username": "User D",
          "displayName": "User D",
          "imageUrl": null
        },
        {
          "id": "U08SA5RGSDC",
          "username": "User A",
          "displayName": "User A",
          "imageUrl": null
        },
        {
          "id": "U08S36A8A15",
          "username": "User C",
          "displayName": "User C",
          "imageUrl": null
        }
      ],
     */
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
            // Get user details from Redis
            if (tenantId != null && workspaceId != null && userId != null) {
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
            logger.warn("Tenant ID or Workspace ID is null, cannot enrich user data for userId: {}", userId);
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
        }
        
        // Handle legacy format: array of full objects (for backward compatibility)
        else if (summaryPerPersonObj instanceof List<?> summaryList) {
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
                .collect(Collectors.toList());
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
    
    private List<SuggestedReply> extractSuggestedReplies(Map<?, ?> topicMap) {
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
}
