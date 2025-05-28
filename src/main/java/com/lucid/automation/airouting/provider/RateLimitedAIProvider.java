package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.exception.AIProviderRateLimitException;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bandwidth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate-limited wrapper for AIProvider implementations.
 * Provides rate limiting, fallback handling, and monitoring capabilities.
 */
public class RateLimitedAIProvider implements AIProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(RateLimitedAIProvider.class);
    
    private final AIProvider primaryProvider;
    private final AIProvider fallbackProvider;
    private final Bucket rateLimitBucket;
    private final String providerId;
    private final AtomicLong requestCount = new AtomicLong(0);
    private final AtomicLong rateLimitedCount = new AtomicLong(0);
    private final AtomicLong fallbackCount = new AtomicLong(0);
    
    /**
     * Creates a rate-limited AI provider with the specified rate limit.
     * 
     * @param primaryProvider the primary AI provider
     * @param fallbackProvider the fallback AI provider (can be null)
     * @param requestsPerMinute maximum requests allowed per minute
     */
    public RateLimitedAIProvider(AIProvider primaryProvider, AIProvider fallbackProvider, int requestsPerMinute) {
        this.primaryProvider = primaryProvider;
        this.fallbackProvider = fallbackProvider;
        this.providerId = primaryProvider.getProviderId() + "-rate-limited";
        
        // Create rate limiting bucket
        Bandwidth limit = Bandwidth.builder()
                .capacity(requestsPerMinute)
                .refillIntervally(requestsPerMinute, Duration.ofMinutes(1))
                .build();
        this.rateLimitBucket = Bucket.builder()
                .addLimit(limit)
                .build();
    }
    
    /**
     * Executes a request with rate limiting and fallback handling.
     */
    private <T> T executeWithRateLimit(String operation, RateLimitedOperation<T> primaryOperation, 
                                      RateLimitedOperation<T> fallbackOperation) {
        requestCount.incrementAndGet();
        
        // Check rate limit
        if (!rateLimitBucket.tryConsume(1)) {
            rateLimitedCount.incrementAndGet();
            logger.warn("Rate limit exceeded for provider: {}, operation: {}", 
                       primaryProvider.getProviderId(), operation);
            
            // Try fallback if available
            if (fallbackProvider != null && fallbackOperation != null) {
                fallbackCount.incrementAndGet();
                logger.info("Using fallback provider: {} for operation: {}", 
                           fallbackProvider.getProviderId(), operation);
                try {
                    return fallbackOperation.execute();
                } catch (RuntimeException e) {
                    logger.error("Fallback provider also failed for operation: {}", operation, e);
                    throw new AIProviderRateLimitException(
                        primaryProvider.getProviderId(), 60, e);
                }
            } else {
                throw new AIProviderRateLimitException(
                    primaryProvider.getProviderId(), 60);
            }
        }
        
        // Execute primary operation
        try {
            return primaryOperation.execute();
        } catch (RuntimeException e) {
            logger.error("Primary provider failed for operation: {}", operation, e);
            
            // Try fallback on error if available
            if (fallbackProvider != null && fallbackOperation != null) {
                fallbackCount.incrementAndGet();
                logger.info("Primary provider failed, using fallback provider: {} for operation: {}", 
                           fallbackProvider.getProviderId(), operation);
                try {
                    return fallbackOperation.execute();
                } catch (RuntimeException fallbackException) {
                    logger.error("Fallback provider also failed for operation: {}", operation, fallbackException);
                    // Throw original exception
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }
    
    @Override
    public CategoryResult categorize(String content) {
        return executeWithRateLimit("categorize",
            () -> primaryProvider.categorize(content),
            fallbackProvider != null ? () -> fallbackProvider.categorize(content) : null
        );
    }
    
    @Override
    public SummaryResult summarize(String content) {
        return executeWithRateLimit("summarize",
            () -> primaryProvider.summarize(content),
            fallbackProvider != null ? () -> fallbackProvider.summarize(content) : null
        );
    }
    
    @Override
    public ConversationEnrichment enrichConversation(List<SlackMessage> messages, List<SlackParticipant> participants) {
        return executeWithRateLimit("enrichConversation",
            () -> primaryProvider.enrichConversation(messages, participants),
            fallbackProvider != null ? () -> fallbackProvider.enrichConversation(messages, participants) : null
        );
    }
    
    @Override
    public MessageEnrichment enrichMessage(String content, Map<String, Object> context) {
        return executeWithRateLimit("enrichMessage",
            () -> primaryProvider.enrichMessage(content, context),
            fallbackProvider != null ? () -> fallbackProvider.enrichMessage(content, context) : null
        );
    }
    
    @Override
    public ParticipantInsight analyzeParticipant(SlackParticipant participant, List<SlackMessage> messages) {
        return executeWithRateLimit("analyzeParticipant",
            () -> primaryProvider.analyzeParticipant(participant, messages),
            fallbackProvider != null ? () -> fallbackProvider.analyzeParticipant(participant, messages) : null
        );
    }
    
    @Override
    public UrgencyLevel assessUrgency(List<SlackMessage> messages) {
        return executeWithRateLimit("assessUrgency",
            () -> primaryProvider.assessUrgency(messages),
            fallbackProvider != null ? () -> fallbackProvider.assessUrgency(messages) : null
        );
    }
    
    @Override
    public String generateTopic(List<SlackMessage> messages) {
        return executeWithRateLimit("generateTopic",
            () -> primaryProvider.generateTopic(messages),
            fallbackProvider != null ? () -> fallbackProvider.generateTopic(messages) : null
        );
    }
    
    @Override
    public List<String> extractEntities(String content) {
        return executeWithRateLimit("extractEntities",
            () -> primaryProvider.extractEntities(content),
            fallbackProvider != null ? () -> fallbackProvider.extractEntities(content) : null
        );
    }
    
    @Override
    public SentimentResult analyzeSentiment(String content) {
        return executeWithRateLimit("analyzeSentiment",
            () -> primaryProvider.analyzeSentiment(content),
            fallbackProvider != null ? () -> fallbackProvider.analyzeSentiment(content) : null
        );
    }
    
    @Override
    public String getProviderId() {
        return providerId;
    }
    
    @Override
    public boolean isAvailable() {
        boolean primaryAvailable = primaryProvider.isAvailable();
        boolean fallbackAvailable = fallbackProvider != null && fallbackProvider.isAvailable();
        
        // Consider available if either primary or fallback is available
        return primaryAvailable || fallbackAvailable;
    }
    
    @Override
    public double getLastConfidence() {
        return primaryProvider.getLastConfidence();
    }
    
    /**
     * Get rate limiting statistics
     */
    public RateLimitStats getRateLimitStats() {
        return new RateLimitStats(
            requestCount.get(),
            rateLimitedCount.get(),
            fallbackCount.get(),
            rateLimitBucket.getAvailableTokens(),
            primaryProvider.getProviderId(),
            fallbackProvider != null ? fallbackProvider.getProviderId() : null
        );
    }
    
    /**
     * Get the underlying primary provider
     */
    public AIProvider getPrimaryProvider() {
        return primaryProvider;
    }
    
    /**
     * Get the fallback provider
     */
    public AIProvider getFallbackProvider() {
        return fallbackProvider;
    }
    
    /**
     * Functional interface for rate-limited operations
     */
    @FunctionalInterface
    private interface RateLimitedOperation<T> {
        T execute();
    }
    
    /**
     * Rate limiting statistics
     */
    public record RateLimitStats(
        long totalRequests,
        long rateLimitedRequests,
        long fallbackRequests,
        long availableTokens,
        String primaryProviderId,
        String fallbackProviderId
    ) {}
}
