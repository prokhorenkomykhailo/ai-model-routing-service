package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RoutingConfig;
import com.lucid.automation.airouting.model.*;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
public class AIRoutingService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIRoutingService.class);
    
    private final AIProviderFactory providerFactory;
    private final RoutingConfig routingConfig;
    private final AIRequestAuditService auditService;
    private final Executor taskExecutor;
    
    public AIRoutingService(AIProviderFactory providerFactory, 
                          RoutingConfig routingConfig,
                          AIRequestAuditService auditService,
                          Executor taskExecutor) {
        this.providerFactory = providerFactory;
        this.routingConfig = routingConfig;
        this.auditService = auditService;
        this.taskExecutor = taskExecutor;
    }
    
    public CompletableFuture<AIResponse> processRequest(AIRequest request) {
        String requestId = UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();
        
        logger.info("Processing AI request: id={}, taskType={}, provider={}", 
                   requestId, request.getTaskType(), request.getPreferredProvider());
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Audit request start
                auditService.logRequestStart(requestId, request);
                
                // Select provider
                AIProvider provider = selectProvider(request);
                
                // Process the request based on task type
                Object result = processRequestByType(request, provider);
                
                long processingTime = System.currentTimeMillis() - startTime;
                
                // Create success response
                AIResponse response = AIResponse.success(
                    requestId, 
                    request.getTaskType(), 
                    result, 
                    provider.getProviderId(),
                    provider.getLastConfidence()
                );
                response.setProcessingTimeMs(processingTime);
                
                // Audit success
                auditService.logRequestSuccess(requestId, response);
                
                logger.info("AI request completed: id={}, provider={}, time={}ms", 
                           requestId, provider.getProviderId(), processingTime);
                
                return response;
                
            } catch (Exception e) {
                long processingTime = System.currentTimeMillis() - startTime;
                
                logger.error("AI request failed: id={}, error={}", requestId, e.getMessage(), e);
                
                // Create error response
                AIResponse response = AIResponse.error(
                    requestId, 
                    request.getTaskType(), 
                    e.getMessage()
                );
                response.setProcessingTimeMs(processingTime);
                
                // Audit failure
                auditService.logRequestFailure(requestId, response, e);
                
                return response;
            }
        }, taskExecutor);
    }
    
    private AIProvider selectProvider(AIRequest request) {
        // If a preferred provider is specified, try to use it
        if (request.getPreferredProvider() != null && !request.getPreferredProvider().trim().isEmpty()) {
            AIProvider preferredProvider = providerFactory.getProvider(request.getPreferredProvider());
            if (preferredProvider != null && preferredProvider.isAvailable()) {
                logger.debug("Using preferred provider: {}", request.getPreferredProvider());
                return preferredProvider;
            } else {
                logger.warn("Preferred provider '{}' not available, falling back to routing config", 
                           request.getPreferredProvider());
            }
        }
        
        // Use routing configuration to select provider for task type
        String providerId = routingConfig.getProviderForTask(request.getTaskType());
        AIProvider provider = providerFactory.getProvider(providerId);
        
        if (provider != null && provider.isAvailable()) {
            logger.debug("Using configured provider: {} for task: {}", providerId, request.getTaskType());
            return provider;
        }
        
        // Fall back to default provider
        logger.warn("Configured provider '{}' not available, using default", providerId);
        return providerFactory.getDefaultProvider();
    }
    
    private Object processRequestByType(AIRequest request, AIProvider provider) {
        return switch (request.getTaskType()) {
            case CATEGORIZE -> provider.categorize(request.getContent());
            
            case SUMMARIZE -> provider.summarize(request.getContent());
            
            case ENRICH_CONVERSATION -> {
                if (request.getMessages() == null || request.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                yield provider.enrichConversation(request.getMessages(), request.getParticipants());
            }
            
            case ENRICH_MESSAGE -> {
                if (request.getContent() == null || request.getContent().trim().isEmpty()) {
                    throw new IllegalArgumentException("Content is required for message enrichment");
                }
                yield provider.enrichMessage(request.getContent(), request.getContext());
            }
            
            case ANALYZE_PARTICIPANT -> {
                if (request.getParticipants() == null || request.getParticipants().isEmpty()) {
                    throw new IllegalArgumentException("Participant is required for participant analysis");
                }
                if (request.getMessages() == null) {
                    throw new IllegalArgumentException("Messages are required for participant analysis");
                }
                yield provider.analyzeParticipant(request.getParticipants().get(0), request.getMessages());
            }
            
            case ASSESS_URGENCY -> {
                if (request.getMessages() == null || request.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for urgency assessment");
                }
                yield provider.assessUrgency(request.getMessages());
            }
            
            case GENERATE_TOPIC -> {
                if (request.getMessages() == null || request.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for topic generation");
                }
                yield provider.generateTopic(request.getMessages());
            }
            
            case EXTRACT_ENTITIES -> {
                if (request.getContent() == null || request.getContent().trim().isEmpty()) {
                    throw new IllegalArgumentException("Content is required for entity extraction");
                }
                yield provider.extractEntities(request.getContent());
            }
            
            case SENTIMENT_ANALYSIS -> {
                if (request.getContent() == null || request.getContent().trim().isEmpty()) {
                    throw new IllegalArgumentException("Content is required for sentiment analysis");
                }
                yield provider.analyzeSentiment(request.getContent());
            }
            
            default -> throw new UnsupportedOperationException("Task type not supported: " + request.getTaskType());
        };
    }
    
    public Map<String, Boolean> getProviderHealthStatus() {
        return providerFactory.getAllProviders().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().isAvailable()
                ));
    }
    
    public boolean isServiceHealthy() {
        return providerFactory.getAllProviders().values().stream()
                .anyMatch(AIProvider::isAvailable);
    }
}