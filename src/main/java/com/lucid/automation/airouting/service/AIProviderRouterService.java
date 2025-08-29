package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RoutingConfig;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import com.lucid.automation.common.dto.TokenQuotaResponseDTO;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * AI Provider routing service with intelligent failover and load balancing
 */
@Service
public class AIProviderRouterService {

    private static final Logger logger = LoggerFactory.getLogger(AIProviderRouterService.class);

    private final AIProviderFactory providerFactory;
    private final AIProviderHealthChecker healthChecker;
    private final EnhancedTokenAvailabilityService enhancedTokenAvailabilityService;
    private final RoutingConfig routingConfig;
    private final Tracer tracer;

    // Metrics
    private final Timer routingLatencyTimer;
    private final Counter providerFailoverCounter;
    private final Counter providerSelectionCounter;

    // Provider priority configuration
    private final Map<String, Integer> providerPriorities;

    public AIProviderRouterService(AIProviderFactory providerFactory,
                                 AIProviderHealthChecker healthChecker,
                                 EnhancedTokenAvailabilityService enhancedTokenAvailabilityService,
                                 RoutingConfig routingConfig,
                                 MeterRegistry meterRegistry,
                                 Tracer tracer) {
        this.providerFactory = providerFactory;
        this.healthChecker = healthChecker;
        this.enhancedTokenAvailabilityService = enhancedTokenAvailabilityService;
        this.routingConfig = routingConfig;
        this.tracer = tracer;

        // Initialize metrics
        this.routingLatencyTimer = Timer.builder("ai.provider.routing.latency")
            .description("Time taken to select and route to a provider")
            .register(meterRegistry);

        this.providerFailoverCounter = Counter.builder("ai.provider.failover.count")
            .description("Number of provider failovers")
            .register(meterRegistry);

        this.providerSelectionCounter = Counter.builder("ai.provider.selection.count")
            .description("Number of provider selections")
            .register(meterRegistry);

        // Initialize provider priorities (can be made configurable)
        this.providerPriorities = new HashMap<>();
        this.providerPriorities.put("geminiProvider", 1);
        this.providerPriorities.put("openaiProvider", 2);
    }

    /**
     * Select the best available provider for a task
     */
    public AIProvider selectProvider(AITaskType taskType, String tenantId) {
        return selectProvider(taskType, tenantId, null);
    }

    /**
     * Select the best available provider with preferred provider hint
     */
    public AIProvider selectProvider(AITaskType taskType, String tenantId, String preferredProviderId) {
        Span span = tracer.spanBuilder("ai.provider.selection")
            .setAttribute("task_type", taskType.toString())
            .setAttribute("tenant_id", tenantId)
            .setAttribute("preferred_provider", preferredProviderId != null ? preferredProviderId : "none")
            .startSpan();

        Timer.Sample sample = Timer.start();

        try {
            providerSelectionCounter.increment();

            // Step 1: Try preferred provider if specified
            if (preferredProviderId != null && !preferredProviderId.trim().isEmpty()) {
                AIProvider preferredProvider = providerFactory.getProvider(preferredProviderId);
                if (isProviderAvailable(preferredProvider, tenantId)) {
                    logger.debug("Selected preferred provider {} for task {} and tenant {}",
                               preferredProviderId, taskType, tenantId);
                    span.setAttribute("selected_provider", preferredProviderId);
                    span.setAttribute("selection_reason", "preferred");
                    return preferredProvider;
                } else {
                    logger.warn("Preferred provider {} unavailable, falling back for task {} and tenant {}",
                              preferredProviderId, taskType, tenantId);
                    providerFailoverCounter.increment();
                    span.setAttribute("preferred_provider_available", false);
                }
            }

            // Step 2: Try task-specific provider
            String taskProvider = routingConfig.getProviderForTask(taskType);
            if (taskProvider != null) {
                AIProvider provider = providerFactory.getProvider(taskProvider);
                if (isProviderAvailable(provider, tenantId)) {
                    logger.debug("Selected task-specific provider {} for task {} and tenant {}",
                               taskProvider, taskType, tenantId);
                    span.setAttribute("selected_provider", taskProvider);
                    span.setAttribute("selection_reason", "task_specific");
                    return provider;
                } else {
                    logger.warn("Task-specific provider {} unavailable, falling back for task {} and tenant {}",
                              taskProvider, taskType, tenantId);
                    providerFailoverCounter.increment();
                }
            }

            // Step 3: Try default provider
            String defaultProvider = routingConfig.getDefaultProvider();
            if (defaultProvider != null) {
                AIProvider provider = providerFactory.getProvider(defaultProvider);
                if (isProviderAvailable(provider, tenantId)) {
                    logger.debug("Selected default provider {} for task {} and tenant {}",
                               defaultProvider, taskType, tenantId);
                    span.setAttribute("selected_provider", defaultProvider);
                    span.setAttribute("selection_reason", "default");
                    return provider;
                } else {
                    logger.warn("Default provider {} unavailable, falling back for task {} and tenant {}",
                              defaultProvider, taskType, tenantId);
                    providerFailoverCounter.increment();
                }
            }

            // Step 4: Find any available provider by priority
            List<AIProvider> availableProviders = findAvailableProvidersByPriority(tenantId);
            if (!availableProviders.isEmpty()) {
                AIProvider provider = availableProviders.get(0);
                logger.warn("Using fallback provider {} for task {} and tenant {}",
                          provider.getProviderId(), taskType, tenantId);
                providerFailoverCounter.increment();
                span.setAttribute("selected_provider", provider.getProviderId());
                span.setAttribute("selection_reason", "fallback");
                return provider;
            }

            // Step 5: No providers available
            logger.error("No available providers found for task {} and tenant {}", taskType, tenantId);
            span.setAttribute("error", true);
            span.setAttribute("error_reason", "no_providers_available");
            throw new NoAvailableProviderException(
                String.format("No AI providers available for task %s and tenant %s", taskType, tenantId));

        } finally {
            sample.stop(routingLatencyTimer);
            span.end();
        }
    }

    /**
     * Check if a provider is available for a tenant
     */
    private boolean isProviderAvailable(AIProvider provider, String tenantId) {
        if (provider == null) {
            return false;
        }

        // Check provider basic availability
        if (!provider.isAvailable()) {
            logger.debug("Provider {} reports as unavailable", provider.getProviderId());
            return false;
        }

        // Check provider health status
        if (!healthChecker.isProviderHealthy(provider.getProviderId())) {
            logger.debug("Provider {} failed health check", provider.getProviderId());
            return false;
        }

        // Check tenant token availability with detailed quota information
        try {
            TokenQuotaResponseDTO tokenQuota = enhancedTokenAvailabilityService.getTokenQuota(tenantId);
            if (!tokenQuota.isAvailable()) {
                logger.debug("No tokens available for tenant {} on provider {}: {}",
                           tenantId, provider.getProviderId(), tokenQuota.getMessage());
                return false;
            }

            // Log quota information for monitoring
            if (tokenQuota.getUsagePercentage() > 80.0) {
                logger.warn("High token usage for tenant {}: {}% used ({}/{} tokens)",
                          tenantId, String.format("%.1f", tokenQuota.getUsagePercentage()),
                          tokenQuota.getUsedTokens(), tokenQuota.getTotalTokens());
            }

        } catch (Exception e) {
            logger.error("Error checking token availability for tenant {} on provider {}: {}",
                       tenantId, provider.getProviderId(), e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Find all available providers sorted by priority
     */
    private List<AIProvider> findAvailableProvidersByPriority(String tenantId) {
        return providerFactory.getAllProviders().values().stream()
            .filter(provider -> isProviderAvailable(provider, tenantId))
            .sorted(Comparator.comparingInt(provider ->
                providerPriorities.getOrDefault(provider.getProviderId(), Integer.MAX_VALUE)))
            .collect(Collectors.toList());
    }

    /**
     * Get provider statistics for monitoring
     */
    public ProviderStats getProviderStats() {
        Map<String, AIProvider> allProviders = providerFactory.getAllProviders();
        Map<String, Boolean> providerHealths = new HashMap<>();

        for (String providerId : allProviders.keySet()) {
            providerHealths.put(providerId, healthChecker.isProviderHealthy(providerId));
        }

        return new ProviderStats(
            allProviders.size(),
            (int) providerHealths.values().stream().filter(h -> h).count(),
            providerHealths
        );
    }

    /**
     * Select provider with estimated token usage validation
     * This method performs pre-validation to ensure the tenant has sufficient tokens
     * for the estimated operation before selecting a provider.
     *
     * @param taskType The AI task type
     * @param tenantId The tenant ID
     * @param estimatedTokens Estimated token usage for the operation
     * @param preferredProvider Optional preferred provider ID
     * @return Selected AI provider
     * @throws NoAvailableProviderException if no provider has sufficient tokens
     */
    public AIProvider selectProviderWithTokenValidation(AITaskType taskType,
                                                      String tenantId,
                                                      long estimatedTokens,
                                                      String preferredProvider) {
        Timer.Sample sample = Timer.start();
        Span span = tracer.spanBuilder("ai.provider.selection.with.validation")
                    .setAttribute("task_type", taskType.toString())
                    .setAttribute("tenant_id", tenantId)
                    .setAttribute("estimated_tokens", estimatedTokens)
                    .setAttribute("preferred_provider", preferredProvider != null ? preferredProvider : "none")
                    .startSpan();

        try {
            providerSelectionCounter.increment();

            logger.debug("Selecting provider for task {} and tenant {} with estimated tokens: {}",
                        taskType, tenantId, estimatedTokens);

            // First check if tenant has sufficient tokens overall
            try {
                if (!enhancedTokenAvailabilityService.hasSufficientTokens(tenantId, estimatedTokens)) {
                    TokenQuotaResponseDTO quota = enhancedTokenAvailabilityService.getTokenQuota(tenantId);
                    logger.warn("Insufficient tokens for tenant {} (required: {}, available: {}): {}",
                              tenantId, estimatedTokens, quota.getRemainingTokens(), quota.getMessage());
                    span.setAttribute("error", true);
                    span.setAttribute("error_reason", "insufficient_tokens");
                    throw new InsufficientTokensException(
                        String.format("Insufficient tokens for tenant %s: required %d, available %d",
                                     tenantId, estimatedTokens, quota.getRemainingTokens()));
                }
            } catch (InsufficientTokensException e) {
                throw e; // Re-throw our custom exception
            } catch (Exception e) {
                logger.warn("Error checking token sufficiency for tenant {}: {}", tenantId, e.getMessage());
                // Continue with regular provider selection if token estimation fails
            }

            // Use regular provider selection logic
            return selectProvider(taskType, tenantId, preferredProvider);

        } finally {
            sample.stop(routingLatencyTimer);
            span.end();
        }
    }

    /**
     * Custom exception for no available providers
     */
    public static class NoAvailableProviderException extends RuntimeException {
        public NoAvailableProviderException(String message) {
            super(message);
        }
    }

    /**
     * Custom exception for insufficient token quota
     */
    public static class InsufficientTokensException extends RuntimeException {
        public InsufficientTokensException(String message) {
            super(message);
        }
    }

    /**
     * Provider statistics data class
     */
    public static class ProviderStats {
        private final int totalProviders;
        private final int healthyProviders;
        private final Map<String, Boolean> providerHealths;

        public ProviderStats(int totalProviders, int healthyProviders, Map<String, Boolean> providerHealths) {
            this.totalProviders = totalProviders;
            this.healthyProviders = healthyProviders;
            this.providerHealths = Map.copyOf(providerHealths);
        }

        public int getTotalProviders() { return totalProviders; }
        public int getHealthyProviders() { return healthyProviders; }
        public Map<String, Boolean> getProviderHealths() { return providerHealths; }

        @Override
        public String toString() {
            return String.format("ProviderStats{total=%d, healthy=%d, healths=%s}",
                               totalProviders, healthyProviders, providerHealths);
        }
    }
}
