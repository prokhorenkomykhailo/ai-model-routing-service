package com.lucid.automation.airouting.provider.impl;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.client.DataStorageServiceClient;
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
import com.lucid.automation.airouting.util.PromptLoader;

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
    
    @Value("${ai.providers.gemini.model:gemini-2.0-flash}")
    //@Value("${ai.providers.gemini.model:gemini-2.5-pro}")
    private String model;
    
    @Value("${app.tenant.default-id:default}")
    private String defaultTenantId;
    
    @Value("${app.tenant.default-schema:public}")
    private String defaultTenantSchema;
    
    private final Client geminiClient;
    private final ObjectMapper objectMapper;
    private final DataStorageServiceClient dataStorageServiceClient;
    private final PromptLoader promptLoader;
    private double lastConfidence = 0.0;
    private final boolean isClientAvailable;
    
    public GeminiProvider(ObjectMapper objectMapper, DataStorageServiceClient dataStorageServiceClient, 
                         PromptLoader promptLoader) {
        this.objectMapper = objectMapper;
        this.dataStorageServiceClient = dataStorageServiceClient;
        this.promptLoader = promptLoader;
        
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
        try {
            int messagesCount = messages != null ? messages.size() : 0;
            int participantsCount = participants != null ? participants.size() : 0;
            int categoriesCount = availableCategories != null ? availableCategories.size() : 0;
            
            logger.info("GEMINI-ENRICH [{}]: Starting conversation enrichment", debugId);
            logger.info("GEMINI-ENRICH [{}]: Input summary - {} messages, {} participants, {} categories", 
                       debugId, messagesCount, participantsCount, categoriesCount);
            
            if (messages != null) {
                logger.debug("GEMINI-ENRICH [{}]: Message details:", debugId);
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
            
            if (participants != null) {
                logger.debug("GEMINI-ENRICH [{}]: Participants: {}", debugId, 
                            participants.stream().map(SlackParticipant::getUsername).toList());
            }
            
            if (availableCategories != null) {
                logger.debug("GEMINI-ENRICH [{}]: Available categories: {}", debugId, availableCategories);
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
            logger.info("GEMINI-ENRICH [{}]: Formatted conversation text length: {}", debugId, conversationText.length());
            logger.debug("GEMINI-ENRICH [{}]: Formatted conversation text (first 1000 chars):\n{}", 
                        debugId, conversationText.length() > 1000 ? 
                        conversationText.substring(0, 1000) + "\n... [truncated]" : conversationText);
            
            List<String> categoriesToUse = List.of();
            logger.info("GEMINI-ENRICH [{}]: Building enrichment prompt", debugId);
            String prompt = buildConversationEnrichmentPrompt(conversationText, participants, categoriesToUse);
            logger.info("GEMINI-ENRICH [{}]: Prompt built, length: {}", debugId, prompt.length());
            logger.debug("GEMINI-ENRICH [{}]: Full prompt being sent:\n{}", debugId, prompt);
            
            logger.info("GEMINI-ENRICH [{}]: Calling Gemini API for conversation enrichment", debugId);
            String response = callGeminiAPI(prompt, "conversation-enrichment", debugId);
            logger.info("GEMINI-ENRICH [{}]: Received response from Gemini API, length: {}", debugId, response.length());
            logger.debug("GEMINI-ENRICH [{}]: Raw response received:\n{}", debugId, response);
            
            logger.info("GEMINI-ENRICH [{}]: Parsing conversation enrichment response", debugId);
            ConversationEnrichment result = parseConversationEnrichmentResponse(response, messages, participants);
            
            // Log the parsed result details
            logger.info("GEMINI-ENRICH [{}]: Conversation enrichment completed successfully", debugId);
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
                return new ParticipantInsight("Unknown Participant", 0.0, "Neutral", 0);
            }
            
            if (messages == null || messages.isEmpty()) {
                logger.warn("GEMINI-DEBUG [{}]: No messages provided for participant analysis", debugId);
                return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
            }
            
            if (!isClientAvailable) {
                logger.warn("GEMINI-DEBUG [{}]: Client not available for participant analysis", debugId);
                return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
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
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        } catch (RuntimeException e) {
            logger.error("GEMINI-DEBUG [{}]: API error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
        } catch (Exception e) {
            logger.error("GEMINI-DEBUG [{}]: Unexpected error during participant analysis: {}", debugId, e.getMessage(), e);
            return new ParticipantInsight("Participant", 0.0, "Neutral", 0);
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
            
            logger.info("GEMINI-API [{}]: Starting {} operation with prompt length: {}", debugId, operation, prompt.length());
            logger.debug("GEMINI-API [{}]: Full prompt being sent:\n{}", debugId, prompt);
            
            long startTime = System.currentTimeMillis();
            
            GenerateContentResponse response = geminiClient.models.generateContent(
                model, 
                prompt, 
                null
            );
            
            long duration = System.currentTimeMillis() - startTime;
            String responseText = response.text();
            
            logger.info("GEMINI-API [{}]: {} operation completed in {}ms, response length: {}", 
                       debugId, operation, duration, responseText != null ? responseText.length() : 0);
            logger.debug("GEMINI-API [{}]: Full raw response received:\n{}", debugId, responseText);
            
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
            logger.debug("GEMINI-PARSE: Original response before cleaning (length: {}):\n{}", 
                        response.length(), response);
            
            String cleanedResponse = cleanJsonResponse(response);
            logger.info("GEMINI-PARSE: Response cleaned, length: {} -> {}", 
                       response.length(), cleanedResponse.length());
            logger.debug("GEMINI-PARSE: Cleaned response:\n{}", cleanedResponse);
            
            // Check if the cleaned response is a JSON array (starts with '[')
            if (cleanedResponse.trim().startsWith("[")) {
                logger.info("GEMINI-PARSE: Detected JSON array format - parsing as array of topics");
                
                // Parse as array of topics directly
                List<?> topicsArray = objectMapper.readValue(cleanedResponse, List.class);
                logger.info("GEMINI-PARSE: Successfully parsed {} topics from array", topicsArray.size());
                
                List<TopicEnrichment> topics = new ArrayList<>();
                
                for (int i = 0; i < topicsArray.size(); i++) {
                    Object topicObj = topicsArray.get(i);
                    logger.debug("GEMINI-PARSE: Processing topic {} of type: {}", i, topicObj.getClass().getSimpleName());
                    
                    if (topicObj instanceof Map<?, ?> topicMap) {
                        logger.debug("GEMINI-PARSE: Topic {} map keys: {}", i, topicMap.keySet());
                        TopicEnrichment topic = parseTopicFromMap(topicMap);
                        topics.add(topic);
                        logger.info("GEMINI-PARSE: Successfully parsed topic {}: '{}'", i, topic.title());
                    } else {
                        logger.warn("GEMINI-PARSE: Topic {} is not a Map, skipping. Type: {}", i, topicObj.getClass());
                    }
                }
                
                // logger.info("GEMINI-PARSE: Parsed {} topics successfully", topics.size());
                
                // List<ParticipantInsight> participantInsights = participants != null ? 
                //     participants.stream()
                //         .map(p -> analyzeParticipant(p, messages))
                //         .toList() : 
                //     List.of();
                
                // List<MessageEnrichment> messageEnrichments = messages.stream()
                //     .map(msg -> enrichMessage(msg.getContent(), Map.of()))
                //     .toList();
                
                // // Create an empty analysis map since we're parsing the topics directly
                // Map<String, Object> analysis = Map.of("topics", topics);
                
                // logger.info("GEMINI-PARSE: Conversation enrichment parsing completed successfully with {} topics, {} participant insights, {} message enrichments", 
                //            topics.size(), participantInsights.size(), messageEnrichments.size());
                
                // return new ConversationEnrichment(topics, participantInsights, messageEnrichments, analysis);
                return new ConversationEnrichment(topics, List.of(), List.of(), null);
            } else {
                logger.info("GEMINI-PARSE: Detected JSON object format - parsing as analysis map");
                
                // Original parsing logic for Map-based responses
                @SuppressWarnings("unchecked")
                Map<String, Object> analysis = objectMapper.readValue(cleanedResponse, Map.class);
                
                logger.info("GEMINI-PARSE: Successfully parsed analysis map with keys: {}", analysis.keySet());
                logger.debug("GEMINI-PARSE: Analysis map contents: {}", analysis);
                
                List<TopicEnrichment> topics = extractTopicsFromResponse(analysis);
                logger.info("GEMINI-PARSE: Extracted {} topics from analysis map", topics.size());
                
                List<ParticipantInsight> participantInsights = participants != null ? 
                    participants.stream()
                        .map(p -> analyzeParticipant(p, messages))
                        .toList() : 
                    List.of();
                
                List<MessageEnrichment> messageEnrichments = messages.stream()
                    .map(msg -> enrichMessage(msg.getContent(), Map.of()))
                    .toList();
                
                logger.info("GEMINI-PARSE: Conversation enrichment parsing completed successfully with {} topics, {} participant insights, {} message enrichments", 
                           topics.size(), participantInsights.size(), messageEnrichments.size());
                
                return new ConversationEnrichment(topics, participantInsights, messageEnrichments, analysis);
            }
                                            
        } catch (Exception e) {
            logger.error("GEMINI-PARSE: Failed to parse conversation enrichment response. Error: {}", e.getMessage(), e);
            logger.error("GEMINI-PARSE: Original response (first 500 chars): {}", 
                        response.length() > 500 ? response.substring(0, 500) + "..." : response);
            logger.error("GEMINI-PARSE: Exception details: {}", e.toString());
            return getDefaultConversationEnrichment();
        }
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
            String role = "Participant";
            double engagement = 0.5;
            String sentiment = "Neutral";
            
            for (String line : lines) {
                if (line.startsWith("Role:")) {
                    role = line.substring(5).trim();
                } else if (line.startsWith("Engagement:")) {
                    String engagementStr = line.substring(11).trim();
                    engagement = parseEngagementValue(engagementStr);
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
        StringBuilder formatted = new StringBuilder();
        
        // Basic message info
        formatted.append(String.format("MESSAGE ID: %s\n", safeString(msg.getId())));
        formatted.append(String.format("TIMESTAMP: %s\n", msg.getTimestamp()));
        formatted.append(String.format("USER: %s", safeString(msg.getUsername())));
        
        // User profile info if available
        if (msg.getDisplayName() != null && !msg.getDisplayName().equals(msg.getUsername())) {
            formatted.append(String.format(" (Display: %s)", msg.getDisplayName()));
        }
        if (msg.getEmail() != null) {
            formatted.append(String.format(" <%s>", msg.getEmail()));
        }
        if (msg.getTitle() != null) {
            formatted.append(String.format(" [%s]", msg.getTitle()));
        }
        formatted.append("\n");
        
        // Channel and thread context
        formatted.append(String.format("CHANNEL: %s\n", safeString(msg.getChannelId())));
        if (msg.getThreadTs() != null) {
            formatted.append(String.format("THREAD: %s\n", msg.getThreadTs()));
        }
        
        // Message type and metadata
        if (msg.getMessageType() != null) {
            formatted.append(String.format("TYPE: %s\n", msg.getMessageType()));
        }
        if (msg.getSubtype() != null) {
            formatted.append(String.format("SUBTYPE: %s\n", msg.getSubtype()));
        }
        
        // Message content (prioritize content over text)
        String content = msg.getContent();
        if (content == null || content.trim().isEmpty()) {
            content = msg.getText();
        }
        formatted.append(String.format("CONTENT: %s\n", safeString(content)));
        
        // Attachments and files info
        if (msg.getFiles() != null && !msg.getFiles().isEmpty()) {
            formatted.append(String.format("FILES: %d attached\n", msg.getFiles().size()));
        }
        if (msg.getAttachments() != null && !msg.getAttachments().isEmpty()) {
            formatted.append(String.format("ATTACHMENTS: %d attached\n", msg.getAttachments().size()));
        }
        
        // Reactions and replies
        if (msg.getReactions() != null && !msg.getReactions().isEmpty()) {
            formatted.append(String.format("REACTIONS: %d reactions\n", msg.getReactions().size()));
        }
        if (msg.getReplyCount() > 0) {
            formatted.append(String.format("REPLIES: %d replies\n", msg.getReplyCount()));
        }
        
        // Tenant and workspace context
        if (msg.getTenantId() != null) {
            formatted.append(String.format("TENANT: %s\n", msg.getTenantId()));
        }
        if (msg.getWorkspaceId() != null) {
            formatted.append(String.format("WORKSPACE: %s\n", msg.getWorkspaceId()));
        }
        
        // Additional metadata
        if (msg.getMetadata() != null && !msg.getMetadata().isEmpty()) {
            formatted.append("METADATA: ").append(msg.getMetadata().toString()).append("\n");
        }
        
        return formatted.toString().trim();
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
    
    /**
     * Cleans JSON response by removing markdown code block formatting if present.
     * Handles responses that start with ```json or ``` and end with ```
     */
    private String cleanJsonResponse(String response) {
        if (response == null || response.trim().isEmpty()) {
            logger.warn("GEMINI-CLEAN: Received null or empty response to clean");
            return response;
        }
        
        String trimmed = response.trim();
        logger.debug("GEMINI-CLEAN: Starting to clean response of length: {}", trimmed.length());
        
        // Check if response contains markdown code blocks with ```json
        if (trimmed.contains("```json")) {
            logger.debug("GEMINI-CLEAN: Found ```json markdown block");
            int jsonStart = trimmed.indexOf("```json");
            if (jsonStart >= 0) {
                // Find the first newline after ```json
                int firstNewline = trimmed.indexOf('\n', jsonStart);
                if (firstNewline > 0) {
                    // Extract content after ```json
                    String jsonContent = trimmed.substring(firstNewline + 1);
                    
                    // Find the closing ```
                    int closingIndex = jsonContent.indexOf("```");
                    if (closingIndex > 0) {
                        jsonContent = jsonContent.substring(0, closingIndex);
                    }
                    
                    logger.debug("GEMINI-CLEAN: Extracted JSON from markdown code block, length: {}", jsonContent.length());
                    return jsonContent.trim();
                }
            }
        }
        
        // Check if response is wrapped in markdown code blocks
        if (trimmed.startsWith("```")) {
            logger.debug("GEMINI-CLEAN: Found generic markdown code block");
            // Find the first newline after the opening ```
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline > 0) {
                // Remove the opening ``` line
                trimmed = trimmed.substring(firstNewline + 1);
            }
            
            // Remove closing ``` if present
            if (trimmed.endsWith("```")) {
                trimmed = trimmed.substring(0, trimmed.length() - 3).trim();
            }
            logger.debug("GEMINI-CLEAN: Removed markdown formatting, new length: {}", trimmed.length());
        }
        
        // Look for JSON content by finding the first { or [
        int jsonStart = -1;
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '{' || c == '[') {
                jsonStart = i;
                break;
            }
        }
        
        if (jsonStart > 0) {
            // Found JSON content after some text, extract from that point
            trimmed = trimmed.substring(jsonStart);
            logger.debug("GEMINI-CLEAN: Extracted JSON content starting from position {}, new length: {}", jsonStart, trimmed.length());
        } else if (jsonStart == -1) {
            // No JSON structure found, log the response for debugging
            logger.warn("GEMINI-CLEAN: No JSON structure found in response (length: {}): {}", 
                       trimmed.length(), 
                       trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed);
            return trimmed; // Return as-is and let the parsing fail gracefully
        }
        
        // Find the last } or ] to handle any trailing text
        int jsonEnd = -1;
        for (int i = trimmed.length() - 1; i >= 0; i--) {
            char c = trimmed.charAt(i);
            if (c == '}' || c == ']') {
                jsonEnd = i;
                break;
            }
        }
        
        if (jsonEnd > 0 && jsonEnd < trimmed.length() - 1) {
            // Found trailing text after JSON, remove it
            trimmed = trimmed.substring(0, jsonEnd + 1);
            logger.debug("GEMINI-CLEAN: Removed trailing text after JSON, final length: {}", trimmed.length());
        }
        
        logger.debug("GEMINI-CLEAN: Cleaning completed, final response length: {}", trimmed.length());
        return trimmed.trim();
    }
    
    private List<TopicEnrichment> extractTopicsFromResponse(Map<String, Object> analysis) {
        try {
            if (analysis.containsKey("topics") && analysis.get("topics") instanceof List<?> topicsList) {
                List<TopicEnrichment> topics = new ArrayList<>();
                
                for (Object topicObj : topicsList) {
                    if (topicObj instanceof Map<?, ?> topicMap) {
                        TopicEnrichment topic = parseTopicFromMap(topicMap);
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
    
    private TopicEnrichment parseTopicFromMap(Map<?, ?> topicMap) {
        String title = extractStringValue(topicMap, "title", "Untitled Topic");
        String shortSummary = extractStringValue(topicMap, "shortSummary", "No summary available");
        String fullSummary = extractStringValue(topicMap, "fullSummary", "No detailed summary available");
        String suggestedAction = extractStringValue(topicMap, "suggestedAction", "No action suggested");
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
        
        List<UserDTO> peopleInvolved = extractPeopleInvolved(topicMap);
        List<SummaryPerPerson> summaryPerPerson = extractSummaryPerPerson(topicMap);
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
     * Extract people involved with backward compatibility
     * Handles both old format (array of strings) and new format (array of user objects)
     */
    private List<UserDTO> extractPeopleInvolved(Map<?, ?> map) {
        Object value = map.get("peopleInvolved");
        if (value instanceof List<?> list) {
            List<UserDTO> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> userMap) {
                    // New format: user object
                    String id = extractStringValue(userMap, "id", null);
                    String username = extractStringValue(userMap, "username", null);
                    String displayName = extractStringValue(userMap, "displayName", null);
                    String imageUrl = extractStringValue(userMap, "imageUrl", null);
                    result.add(new UserDTO(id, username, displayName, imageUrl));
                } else if (item instanceof String userString) {
                    // Old format: just a string (user ID/name)
                    result.add(UserDTO.fromString(userString));
                }
            }
            return result;
        }
        return List.of();
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
    
    private List<SummaryPerPerson> extractSummaryPerPerson(Map<?, ?> topicMap) {
        Object summaryPerPersonObj = topicMap.get("summaryPerPerson");
        
        // Handle new format: simple map of userId -> summary
        if (summaryPerPersonObj instanceof Map<?, ?> summaryMap) {
            List<SummaryPerPerson> summaries = new ArrayList<>();
            
            for (Map.Entry<?, ?> entry : summaryMap.entrySet()) {
                String userId = String.valueOf(entry.getKey());
                String summary = String.valueOf(entry.getValue());
                
                // Create minimal SummaryPerPerson object - will be enhanced with full user data in post-processing
                SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
                    userId,           // id
                    null,            // username - to be filled in post-processing
                    null,            // displayName - to be filled in post-processing  
                    null,            // imageUrl - to be filled in post-processing
                    summary,         // summary - from LLM response
                    null,            // role - to be filled in post-processing
                    0,               // messageCount - to be calculated in post-processing
                    null,            // firstMessageDate - to be calculated in post-processing
                    null,            // lastMessageDate - to be calculated in post-processing
                    List.of(),       // keyContributions - to be calculated in post-processing
                    List.of()        // actionItems - to be calculated in post-processing
                );
                summaries.add(summaryPerPerson);
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
                    String role = extractStringValue(summaryObjMap, "role", null);
                    Integer messageCount = extractIntegerValue(summaryObjMap, "messageCount", 0);
                    LocalDateTime firstMessageDate = extractDateTime(summaryObjMap, "firstMessageDate");
                    LocalDateTime lastMessageDate = extractDateTime(summaryObjMap, "lastMessageDate");
                    List<String> keyContributions = extractStringList(summaryObjMap, "keyContributions");
                    List<String> actionItems = extractStringList(summaryObjMap, "actionItems");
                    
                    SummaryPerPerson summaryPerPerson = new SummaryPerPerson(
                        id, username, displayName, imageUrl, summary, role,
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
