package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.model.request.*;
import com.lucid.automation.airouting.service.AIMessagePublisherService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for publishing AI requests to RabbitMQ
 */
@RestController
@RequestMapping("/ai/queue")
@CrossOrigin(origins = "*")
@Tag(name = "AI Queue Controller", description = "Endpoints for publishing AI requests to message queues")
public class AIQueueController {
    
    private static final Logger logger = LoggerFactory.getLogger(AIQueueController.class);
    
    private final AIMessagePublisherService publisherService;
    
    @Autowired
    public AIQueueController(AIMessagePublisherService publisherService) {
        this.publisherService = publisherService;
    }
    
    @PostMapping("/categorize")
    @Operation(summary = "Queue categorization request", description = "Publish a categorization request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueCategorization(@Valid @RequestBody CategoryRequest request) {
        logger.info("Queueing categorization request for content length: {}, tenantId: {}", 
                   request.getContent().length(), request.getTenantId());
        
        String messageId = publisherService.publishCategorizationRequest(
            request.getContent(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "CATEGORIZE"
        ));
    }
    
    @PostMapping("/summarize")
    @Operation(summary = "Queue summarization request", description = "Publish a summarization request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueSummarization(@Valid @RequestBody SummaryRequest request) {
        logger.info("Queueing summarization request for content length: {}, tenantId: {}", 
                   request.getContent().length(), request.getTenantId());
        
        String messageId = publisherService.publishSummarizationRequest(
            request.getContent(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "SUMMARIZE"
        ));
    }
    
    @PostMapping("/enrich-conversation")
    @Operation(summary = "Queue conversation enrichment request", description = "Publish a conversation enrichment request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueConversationEnrichment(@Valid @RequestBody ConversationEnrichmentRequest request) {
        logger.info("Queueing conversation enrichment request for {} messages, tenantId: {}", 
                   request.getMessages() != null ? request.getMessages().size() : 0, request.getTenantId());
        
        String messageId = publisherService.publishConversationEnrichmentRequest(
            request.getConversationId(),
            request.getMessages(),
            request.getParticipants(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "ENRICH_CONVERSATION"
        ));
    }
    
    @PostMapping("/enrich-message")
    @Operation(summary = "Queue message enrichment request", description = "Publish a message enrichment request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueMessageEnrichment(@Valid @RequestBody MessageEnrichmentRequest request) {
        logger.info("Queueing message enrichment request for content length: {}, tenantId: {}", 
                   request.getContent().length(), request.getTenantId());
        
        String messageId = publisherService.publishMessageEnrichmentRequest(
            request.getContent(),
            request.getContext(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "ENRICH_MESSAGE"
        ));
    }
    
    @PostMapping("/analyze-participant")
    @Operation(summary = "Queue participant analysis request", description = "Publish a participant analysis request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueParticipantAnalysis(@Valid @RequestBody ParticipantAnalysisRequest request) {
        logger.info("Queueing participant analysis request for participant: {}, tenantId: {}", 
                   request.getParticipant() != null ? request.getParticipant().getId() : "unknown", 
                   request.getTenantId());
        
        String messageId = publisherService.publishParticipantAnalysisRequest(
            request.getParticipant(),
            request.getMessages(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "ANALYZE_PARTICIPANT"
        ));
    }
    
    @PostMapping("/assess-urgency")
    @Operation(summary = "Queue urgency assessment request", description = "Publish an urgency assessment request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueUrgencyAssessment(@Valid @RequestBody UrgencyAssessmentRequest request) {
        logger.info("Queueing urgency assessment request for {} messages, tenantId: {}", 
                   request.getMessages() != null ? request.getMessages().size() : 0, request.getTenantId());
        
        String messageId = publisherService.publishUrgencyAssessmentRequest(
            request.getMessages(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "ASSESS_URGENCY"
        ));
    }
    
    @PostMapping("/generate-topic")
    @Operation(summary = "Queue topic generation request", description = "Publish a topic generation request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueTopicGeneration(@Valid @RequestBody TopicGenerationRequest request) {
        logger.info("Queueing topic generation request for {} messages, tenantId: {}", 
                   request.getMessages() != null ? request.getMessages().size() : 0, request.getTenantId());
        
        String messageId = publisherService.publishTopicGenerationRequest(
            request.getMessages(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "GENERATE_TOPIC"
        ));
    }
    
    @PostMapping("/extract-entities")
    @Operation(summary = "Queue entity extraction request", description = "Publish an entity extraction request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueEntityExtraction(@Valid @RequestBody EntityExtractionRequest request) {
        logger.info("Queueing entity extraction request for content length: {}, tenantId: {}", 
                   request.getContent().length(), request.getTenantId());
        
        String messageId = publisherService.publishEntityExtractionRequest(
            request.getContent(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "EXTRACT_ENTITIES"
        ));
    }
    
    @PostMapping("/sentiment-analysis")
    @Operation(summary = "Queue sentiment analysis request", description = "Publish a sentiment analysis request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueSentimentAnalysis(@Valid @RequestBody SentimentAnalysisRequest request) {
        logger.info("Queueing sentiment analysis request for content length: {}, tenantId: {}", 
                   request.getContent().length(), request.getTenantId());
        
        String messageId = publisherService.publishSentimentAnalysisRequest(
            request.getContent(),
            request.getTenantId(),
            getCurrentUserId(),
            request.getPreferredProvider(),
            request.getReplyTopic()
        );
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "SENTIMENT_ANALYSIS"
        ));
    }
    
    /**
     * Get current user ID from security context
     * TODO: Implement proper security context extraction
     */
    private String getCurrentUserId() {
        // For now, return a placeholder. In a real implementation,
        // this would extract the user ID from the JWT token or security context
        return "system-user";
    }
}
