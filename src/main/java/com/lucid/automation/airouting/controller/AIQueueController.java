package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.model.request.*;
import com.lucid.automation.airouting.service.AIMessagePublisherService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    
    public AIQueueController(AIMessagePublisherService publisherService) {
        this.publisherService = publisherService;
    }
    
    @PostMapping("/categorize")
    @Operation(summary = "Queue categorization request", description = "Publish a categorization request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueCategorization(
            @Valid @RequestBody CategoryRequest request,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Queueing categorization request for content length: {}, tenantId: {}", 
                   request.getContent().length(), tenantId);
        
        String messageId = publisherService.publishCategorizationRequest(
            request.getContent(),
            tenantId,
            tenantSchema,
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
    public ResponseEntity<Map<String, String>> queueSummarization(
            @Valid @RequestBody SummaryRequest request,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Queueing summarization request for content length: {}, tenantId: {}", 
                   request.getContent().length(), tenantId);
        
        String messageId = publisherService.publishSummarizationRequest(
            request.getContent(),
            tenantId,
            tenantSchema,
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
    public ResponseEntity<Map<String, String>> queueConversationEnrichment(
            @Valid @RequestBody ConversationEnrichmentRequest request,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Queueing conversation enrichment request for {} messages, tenantId: {}", 
                   request.getMessages() != null ? request.getMessages().size() : 0, tenantId);
        
        // Update the request object with header values
        request.setTenantId(tenantId);
        request.setTenantSchema(tenantSchema);
        
        String messageId = publisherService.publishConversationEnrichmentRequest(request, getCurrentUserId());
        
        return ResponseEntity.ok(Map.of(
            "messageId", messageId,
            "status", "queued",
            "taskType", "ENRICH_CONVERSATION"
        ));
    }
    
    @PostMapping("/enrich-message")
    @Operation(summary = "Queue message enrichment request", description = "Publish a message enrichment request to the message queue")
    @ApiResponse(responseCode = "200", description = "Request queued successfully")
    public ResponseEntity<Map<String, String>> queueMessageEnrichment(
            @Valid @RequestBody MessageEnrichmentRequest request,
            @RequestHeader("X-Tenant-ID") String tenantId,
            @RequestHeader("X-Tenant-Schema") String tenantSchema) {
        logger.info("Queueing message enrichment request for content length: {}, tenantId: {}", 
                   request.getContent().length(), tenantId);
        
        String messageId = publisherService.publishMessageEnrichmentRequest(
            request.getContent(),
            request.getContext(),
            tenantId,
            tenantSchema,
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
