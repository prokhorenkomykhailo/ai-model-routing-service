package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.model.*;
import com.lucid.automation.airouting.model.request.*;
import com.lucid.automation.airouting.model.response.*;
import com.lucid.automation.airouting.security.CustomUserDetails;
import com.lucid.automation.airouting.service.AIRoutingService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/ai")
@CrossOrigin(origins = "*")
public class AIRoutingController {
    
    private static final Logger logger = LoggerFactory.getLogger(AIRoutingController.class);
    
    private final AIRoutingService routingService;
    
    public AIRoutingController(AIRoutingService routingService) {
        this.routingService = routingService;
    }
    
    @PostMapping("/categorize")
    public CompletableFuture<ResponseEntity<AIResponse>> categorize(@Valid @RequestBody CategoryRequest request) {
        String tenantInfo = getTenantInfoFromAuth();
        logger.info("Received categorization request for content length: {} from tenant: {}", 
                   request.getContent().length(), tenantInfo);
        
        AIRequest aiRequest = new AIRequest(AITaskType.CATEGORIZE, request.getContent());
        enrichRequestWithAuthInfo(aiRequest, request.getTenantId(), request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(ResponseEntity::ok);
    }
    
    @PostMapping("/summarize")
    public CompletableFuture<ResponseEntity<AIResponse>> summarize(@Valid @RequestBody SummaryRequest request) {
        String tenantInfo = getTenantInfoFromAuth();
        logger.info("Received summarization request for content length: {} from tenant: {}", 
                   request.getContent().length(), tenantInfo);
        
        AIRequest aiRequest = new AIRequest(AITaskType.SUMMARIZE, request.getContent());
        enrichRequestWithAuthInfo(aiRequest, request.getTenantId(), request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(ResponseEntity::ok);
    }
    
    @PostMapping("/enrich-conversation")
    public CompletableFuture<ResponseEntity<AIResponse>> enrichConversation(
            @Valid @RequestBody ConversationEnrichmentRequest request) {
        
        String tenantInfo = getTenantInfoFromAuth();
        logger.info("Received conversation enrichment request for {} messages from tenant: {}", 
                   request.getMessages().size(), tenantInfo);
        
        AIRequest aiRequest = new AIRequest(AITaskType.ENRICH_CONVERSATION, "");
        aiRequest.setConversationId(request.getConversationId());
        aiRequest.setMessages(request.getMessages());
        aiRequest.setParticipants(request.getParticipants());
        aiRequest.setContext(request.getContext());
        enrichRequestWithAuthInfo(aiRequest, request.getTenantId(), request.getPreferredProvider());
        
        return routingService.processRequest(aiRequest)
                .thenApply(ResponseEntity::ok);
    }
    
    @PostMapping("/batch")
    public CompletableFuture<ResponseEntity<List<AIResponse>>> processBatch(
            @Valid @RequestBody List<AIRequest> requests) {
        
        String tenantInfo = getTenantInfoFromAuth();
        logger.info("Received batch request with {} items from tenant: {}", requests.size(), tenantInfo);
        
        // Enrich each request with auth info
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof CustomUserDetails userDetails) {
            requests.forEach(request -> {
                if (request.getTenantId() == null && userDetails.getTenantId() != null) {
                    request.setTenantId(userDetails.getTenantId());
                }
                if (userDetails.getUserId() != null) {
                    request.setUserId(userDetails.getUserId());
                }
            });
        }
        
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
    
    /**
     * Helper method to extract tenant information from authenticated user context.
     */
    private String getTenantInfoFromAuth() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof CustomUserDetails userDetails) {
            return userDetails.getTenantId();
        }
        return "unknown";
    }
    
    /**
     * Helper method to enrich AI request with authentication context information.
     */
    private void enrichRequestWithAuthInfo(AIRequest aiRequest, String requestTenantId, String preferredProvider) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof CustomUserDetails userDetails) {
            // Use tenant from auth context, fallback to request if not available
            String tenantId = userDetails.getTenantId() != null ? userDetails.getTenantId() : requestTenantId;
            aiRequest.setTenantId(tenantId);
            
            // Set user context for audit purposes
            aiRequest.setUserId(userDetails.getUserId());
        } else {
            // Fallback to request tenant if auth context is not available
            aiRequest.setTenantId(requestTenantId);
        }
        
        aiRequest.setPreferredProvider(preferredProvider);
    }
}