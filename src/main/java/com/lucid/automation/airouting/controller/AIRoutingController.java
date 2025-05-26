package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.model.*;
import com.lucid.automation.airouting.model.request.*;
import com.lucid.automation.airouting.model.response.*;
import com.lucid.automation.airouting.service.AIRoutingService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/ai")
@CrossOrigin(origins = "*")
public class AIRoutingController {
    
    private static final Logger logger = LoggerFactory.getLogger(AIRoutingController.class);
    
    private final AIRoutingService routingService;
    
    public AIRoutingController(AIRoutingService routingService) {
        this.routingService = routingService;
    }
    
    @PostMapping("/categorize")
    public CompletableFuture<ResponseEntity<AIResponse>> categorize(@Valid @RequestBody CategoryRequest request) {
        logger.info("Received categorization request for content length: {}", request.getContent().length());
        
        AIRequest aiRequest = new AIRequest(AITaskType.CATEGORIZE, request.getContent());
        aiRequest.setTenantId(request.getTenantId());
        aiRequest.setPreferredProvider(request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(response -> ResponseEntity.ok(response));
    }
    
    @PostMapping("/summarize")
    public CompletableFuture<ResponseEntity<AIResponse>> summarize(@Valid @RequestBody SummaryRequest request) {
        logger.info("Received summarization request for content length: {}", request.getContent().length());
        
        AIRequest aiRequest = new AIRequest(AITaskType.SUMMARIZE, request.getContent());
        aiRequest.setTenantId(request.getTenantId());
        aiRequest.setPreferredProvider(request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(response -> ResponseEntity.ok(response));
    }
    
    @PostMapping("/enrich-conversation")
    public CompletableFuture<ResponseEntity<AIResponse>> enrichConversation(
            @Valid @RequestBody ConversationEnrichmentRequest request) {
        
        logger.info("Received conversation enrichment request for {} messages", 
                   request.getMessages().size());
        
        AIRequest aiRequest = new AIRequest(AITaskType.ENRICH_CONVERSATION, "");
        aiRequest.setConversationId(request.getConversationId());
        aiRequest.setMessages(request.getMessages());
        aiRequest.setParticipants(request.getParticipants());
        aiRequest.setContext(request.getContext());
        aiRequest.setTenantId(request.getTenantId());
        aiRequest.setPreferredProvider(request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(response -> ResponseEntity.ok(response));
    }
    
    @PostMapping("/batch")
    public CompletableFuture<ResponseEntity<List<AIResponse>>> processBatch(
            @Valid @RequestBody List<AIRequest> requests) {
        
        logger.info("Received batch request with {} items", requests.size());
        
        List<CompletableFuture<AIResponse>> futures = requests.stream()
                .map(routingService::processRequest)
                .toList();
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .toList())
                .thenApply(ResponseEntity::ok);
    }
    
    @GetMapping("/health")
    public ResponseEntity<HealthStatus> health() {
        HealthStatus status = new HealthStatus();
        status.setStatus("UP");
        status.setTimestamp(java.time.LocalDateTime.now());
        
        return ResponseEntity.ok(status);
    }
}