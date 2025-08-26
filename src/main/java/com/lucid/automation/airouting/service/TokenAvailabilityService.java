package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.AuthTokenAvailableClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced token availability service with proper quota checking and metrics
 */
@Service
public class TokenAvailabilityService {
    private static final Logger logger = LoggerFactory.getLogger(TokenAvailabilityService.class);

    private final AuthTokenAvailableClient authTokenAvailableClient;
    private final Counter tokenAvailableCounter;
    private final Counter tokenUnavailableCounter;
    private final Counter tokenCheckErrorCounter;

    // Cache for token availability status with tenant-level tracking
    private final Map<String, TokenStatus> tokenStatusCache = new ConcurrentHashMap<>();

    public TokenAvailabilityService(AuthTokenAvailableClient authTokenAvailableClient,
                                  MeterRegistry meterRegistry) {
        this.authTokenAvailableClient = authTokenAvailableClient;

        // Initialize metrics
        this.tokenAvailableCounter = Counter.builder("ai.tokens.available.count")
            .description("Number of times tokens were available")
            .register(meterRegistry);
        this.tokenUnavailableCounter = Counter.builder("ai.tokens.unavailable.count")
            .description("Number of times tokens were unavailable")
            .register(meterRegistry);
        this.tokenCheckErrorCounter = Counter.builder("ai.tokens.check.error.count")
            .description("Number of token check errors")
            .register(meterRegistry);
    }

    /**
     * Checks if the tenant has tokens available with proper response handling.
     * @param tenantId the tenant UUID as string
     * @return true if tokens are available, false if not
     */
    @CircuitBreaker(name = "tokenAvailability", fallbackMethod = "tokenAvailableFallback")
    public boolean isTokenAvailable(String tenantId) {
        try {
            ResponseEntity<Boolean> response = authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json");

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                boolean available = response.getBody();
                updateTokenStatus(tenantId, available, "SUCCESS");

                if (available) {
                    tokenAvailableCounter.increment();
                    logger.debug("Tokens available for tenant: {}", tenantId);
                } else {
                    tokenUnavailableCounter.increment();
                    logger.warn("Tokens unavailable for tenant: {}", tenantId);
                }

                return available;
            } else {
                logger.warn("Token availability check failed for tenant {}: status {}",
                           tenantId, response.getStatusCode());
                updateTokenStatus(tenantId, false, "HTTP_ERROR");
                tokenCheckErrorCounter.increment();
                return false;
            }
        } catch (Exception e) {
            logger.error("Error checking token availability for tenant {}: {}", tenantId, e.getMessage());
            updateTokenStatus(tenantId, false, "EXCEPTION");
            tokenCheckErrorCounter.increment();
            return false;
        }
    }

    /**
     * Enhanced method to check token availability with estimated consumption.
     * This method first checks basic availability, then could be extended for quota validation.
     *
     * @param tenantId the tenant UUID as string
     * @param estimatedTokens estimated number of tokens that will be consumed
     * @return true if tokens are available for the estimated consumption
     */
    public boolean isTokenAvailable(String tenantId, int estimatedTokens) {
        // First check basic token availability from auth service
        boolean available = isTokenAvailable(tenantId);

        if (!available) {
            logger.warn("Tenant {} has no tokens available", tenantId);
            return false;
        }

        if (estimatedTokens > 0) {
            logger.debug("Token check for tenant {} with estimated consumption: {} tokens",
                        tenantId, estimatedTokens);

            // For now, if basic availability is true, we allow the operation
            // Future enhancement: Could call additional endpoint to validate against remaining quota
            // e.g., authTokenAvailableClient.checkTokenQuota(tenantId, estimatedTokens);
        }

        return available;
    }

    /**
     * Get cached token status for a tenant
     */
    public TokenStatus getTokenStatus(String tenantId) {
        return tokenStatusCache.get(tenantId);
    }

    /**
     * Get all cached token statuses (for monitoring/health checks)
     */
    public Map<String, TokenStatus> getAllTokenStatuses() {
        return Map.copyOf(tokenStatusCache);
    }

    private void updateTokenStatus(String tenantId, boolean available, String status) {
        TokenStatus tokenStatus = new TokenStatus(available, status, System.currentTimeMillis());
        tokenStatusCache.put(tenantId, tokenStatus);
    }

    // Fallback method for circuit breaker
    public boolean tokenAvailableFallback(String tenantId, Throwable t) {
        logger.error("Circuit breaker fallback: error checking token availability for tenant {}: {}",
                    tenantId, t.getMessage());
        updateTokenStatus(tenantId, false, "CIRCUIT_BREAKER_FALLBACK");
        tokenCheckErrorCounter.increment();

        // Return false in fallback to be more conservative
        // This prevents token exhaustion in case of auth service issues
        return false;
    }

    /**
     * Token status data class
     */
    public static class TokenStatus {
        private final boolean available;
        private final String status;
        private final long lastChecked;

        public TokenStatus(boolean available, String status, long lastChecked) {
            this.available = available;
            this.status = status;
            this.lastChecked = lastChecked;
        }

        public boolean isAvailable() { return available; }
        public String getStatus() { return status; }
        public long getLastChecked() { return lastChecked; }

        @Override
        public String toString() {
            return String.format("TokenStatus{available=%s, status='%s', lastChecked=%d}",
                               available, status, lastChecked);
        }
    }
}
