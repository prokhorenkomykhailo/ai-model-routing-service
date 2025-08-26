package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduled health checker for AI providers with metrics and health indicator support
 */
@Component
public class AIProviderHealthChecker implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(AIProviderHealthChecker.class);

    private final AIProviderFactory providerFactory;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    // Health state tracking
    private final Map<String, ProviderHealthStatus> providerHealthCache = new ConcurrentHashMap<>();
    private final AtomicInteger totalProvidersUp = new AtomicInteger(0);
    private final Counter healthCheckCounter;
    private final Counter healthCheckErrorCounter;

    public AIProviderHealthChecker(AIProviderFactory providerFactory,
                                 MeterRegistry meterRegistry,
                                 Tracer tracer) {
        this.providerFactory = providerFactory;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;

        // Initialize metrics
        this.healthCheckCounter = Counter.builder("ai.provider.healthcheck.total")
            .description("Total number of provider health checks performed")
            .register(meterRegistry);

        this.healthCheckErrorCounter = Counter.builder("ai.provider.healthcheck.errors")
            .description("Number of provider health check errors")
            .register(meterRegistry);

        // Register gauge for provider count
        meterRegistry.gauge("ai.provider.up.count", this, AIProviderHealthChecker::getTotalProvidersUp);
    }

    /**
     * Scheduled health check - runs every 30 seconds
     */
    @Scheduled(fixedRate = 30000)
    public void performHealthChecks() {
        Span span = tracer.spanBuilder("ai.provider.health_check").startSpan();
        try {
            logger.debug("Starting scheduled health checks for AI providers");

            Map<String, AIProvider> allProviders = providerFactory.getAllProviders();
            int healthyCount = 0;

            for (Map.Entry<String, AIProvider> entry : allProviders.entrySet()) {
                String providerId = entry.getKey();
                AIProvider provider = entry.getValue();

                try {
                    boolean healthy = performHealthCheck(provider);
                    updateProviderHealth(providerId, healthy, null);

                    if (healthy) {
                        healthyCount++;
                    }

                    // Record per-provider metrics - will be handled by getTotalProvidersUp
                    // Individual provider health is available via health endpoint

                    healthCheckCounter.increment();

                } catch (Exception e) {
                    logger.warn("Health check failed for provider {}: {}", providerId, e.getMessage());
                    updateProviderHealth(providerId, false, e.getMessage());
                    healthCheckErrorCounter.increment();
                }
            }

            totalProvidersUp.set(healthyCount);
            logger.debug("Health check completed. {}/{} providers healthy", healthyCount, allProviders.size());

        } finally {
            span.end();
        }
    }

    /**
     * Perform health check for a specific provider
     * This is a lightweight check that only verifies provider availability without consuming tokens
     */
    private boolean performHealthCheck(AIProvider provider) {
        // Only check if the provider is available (configured and ready)
        // Avoid making actual API calls to preserve tokens and reduce latency
        boolean available = provider.isAvailable();

        logger.debug("Health check for provider {}: available={}", provider.getProviderId(), available);

        return available;
    }

    /**
     * Get health status for a specific provider
     */
    public boolean isProviderHealthy(String providerId) {
        ProviderHealthStatus status = providerHealthCache.get(providerId);
        return status != null && status.isHealthy();
    }

    /**
     * Get all provider health statuses
     */
    public Map<String, ProviderHealthStatus> getAllProviderHealthStatuses() {
        return Map.copyOf(providerHealthCache);
    }

    /**
     * Update provider health status
     */
    private void updateProviderHealth(String providerId, boolean healthy, String errorMessage) {
        ProviderHealthStatus status = new ProviderHealthStatus(
            providerId, healthy, LocalDateTime.now(), errorMessage
        );
        providerHealthCache.put(providerId, status);
    }

    /**
     * Get total number of healthy providers (for metrics)
     */
    public int getTotalProvidersUp() {
        return totalProvidersUp.get();
    }

    /**
     * Spring Boot Actuator health indicator implementation
     */
    @Override
    public Health health() {
        Map<String, AIProvider> allProviders = providerFactory.getAllProviders();
        int totalProviders = allProviders.size();
        int healthyProviders = getTotalProvidersUp();

        Health.Builder builder = healthyProviders > 0 ? Health.up() : Health.down();

        builder.withDetail("totalProviders", totalProviders)
               .withDetail("healthyProviders", healthyProviders)
               .withDetail("lastChecked", LocalDateTime.now());

        // Add individual provider statuses
        for (Map.Entry<String, ProviderHealthStatus> entry : providerHealthCache.entrySet()) {
            ProviderHealthStatus status = entry.getValue();
            builder.withDetail("provider." + entry.getKey(), Map.of(
                "healthy", status.isHealthy(),
                "lastChecked", status.getLastChecked(),
                "error", status.getErrorMessage() != null ? status.getErrorMessage() : "none"
            ));
        }

        return builder.build();
    }

    /**
     * Provider health status data class
     */
    public static class ProviderHealthStatus {
        private final String providerId;
        private final boolean healthy;
        private final LocalDateTime lastChecked;
        private final String errorMessage;

        public ProviderHealthStatus(String providerId, boolean healthy, LocalDateTime lastChecked, String errorMessage) {
            this.providerId = providerId;
            this.healthy = healthy;
            this.lastChecked = lastChecked;
            this.errorMessage = errorMessage;
        }

        public String getProviderId() { return providerId; }
        public boolean isHealthy() { return healthy; }
        public LocalDateTime getLastChecked() { return lastChecked; }
        public String getErrorMessage() { return errorMessage; }

        @Override
        public String toString() {
            return String.format("ProviderHealthStatus{providerId='%s', healthy=%s, lastChecked=%s, error='%s'}",
                               providerId, healthy, lastChecked, errorMessage);
        }
    }
}
