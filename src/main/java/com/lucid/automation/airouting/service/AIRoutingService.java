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

@Service
public class AIRoutingService {
    
    private static final Logger logger = LoggerFactory.getLogger(AIRoutingService.class);
    
    private final AIProviderFactory providerFactory;
    private final RoutingConfig routingConfig;
    private final AIRequestAuditService auditService;
    
    public AIRoutingService(AIProviderFactory providerFactory, 
                          RoutingConfig routingConfig,
                          AIRequestAuditService auditService) {
        this.providerFactory = providerFactory;
        this.routingConfig = routingConfig;
        this.auditService = auditService;
    }
    
    public AIResponse processRequest(AIRequest request) {
        String requestId = generateRequestId();
        long startTime = System.currentTimeMillis();
        
        logRequestStart(requestId, request);
        auditService.logRequestStart(requestId, request);
        
        try {
            AIProvider selectedProvider = selectProvider(request);
            Object processingResult = processRequestByType(request, selectedProvider);
            
            return buildSuccessResponse(requestId, request, processingResult, selectedProvider, startTime);
            
        } catch (Exception processingError) {
            return buildErrorResponse(requestId, request, processingError, startTime);
        }
    }
    
    private String generateRequestId() {
        return UUID.randomUUID().toString();
    }
    
    private void logRequestStart(String requestId, AIRequest request) {
        logger.info("Processing AI request: id={}, taskType={}, provider={}", 
                   requestId, request.getTaskType(), request.getPreferredProvider());
    }
    
    private AIResponse buildSuccessResponse(String requestId, AIRequest request, Object result, 
                                          AIProvider provider, long startTime) {
        long processingTime = calculateProcessingTime(startTime);
        
        AIResponse response = AIResponse.success(
            requestId, 
            request.getTaskType(), 
            result, 
            provider.getProviderId(),
            provider.getLastConfidence()
        );
        response.setProcessingTimeMs(processingTime);
        
        auditService.logRequestSuccess(requestId, response);
        logRequestCompletion(requestId, provider, processingTime);
        
        return response;
    }
    
    private AIResponse buildErrorResponse(String requestId, AIRequest request, Exception error, long startTime) {
        long processingTime = calculateProcessingTime(startTime);
        
        logger.error("AI request failed: id={}, error={}", requestId, error.getMessage(), error);
        
        AIResponse response = AIResponse.error(requestId, request.getTaskType(), error.getMessage());
        response.setProcessingTimeMs(processingTime);
        
        auditService.logRequestFailure(requestId, response, error);
        
        return response;
    }
    
    private long calculateProcessingTime(long startTime) {
        return System.currentTimeMillis() - startTime;
    }
    
    private void logRequestCompletion(String requestId, AIProvider provider, long processingTime) {
        logger.info("AI request completed: id={}, provider={}, time={}ms", 
                   requestId, provider.getProviderId(), processingTime);
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