package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.EnhancedAuthTokenClient;
import com.lucid.automation.common.dto.TokenQuotaResponseDTO;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Enhanced token availability service with detailed quota information
 * Provides both simple availability checks and comprehensive quota details
 */
@Service
public class EnhancedTokenAvailabilityService {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedTokenAvailabilityService.class);

    private final EnhancedAuthTokenClient authTokenClient;

    // Simple in-memory cache with timestamps
    private final ConcurrentMap<String, TokenQuotaResponseDTO> quotaCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> cacheTimestamps = new ConcurrentHashMap<>();

    // Cache TTL in milliseconds
    private static final long QUOTA_CACHE_TTL = 120_000; // 2 minutes

    // Metrics
    private final Timer tokenCheckTimer;
    private final Counter tokenCheckCounter;
    private final Counter quotaCheckCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;

    public EnhancedTokenAvailabilityService(EnhancedAuthTokenClient authTokenClient) {
        this.authTokenClient = authTokenClient;

        // Initialize metrics
        this.tokenCheckTimer = Timer.builder("token.check.duration")
                .description("Duration of token availability checks")
                .register(Metrics.globalRegistry);

        this.tokenCheckCounter = Counter.builder("token.check.requests")
                .description("Number of token availability checks")
                .register(Metrics.globalRegistry);

        this.quotaCheckCounter = Counter.builder("token.quota.requests")
                .description("Number of token quota information requests")
                .register(Metrics.globalRegistry);

        this.cacheHitCounter = Counter.builder("token.cache.hits")
                .description("Number of cache hits")
                .register(Metrics.globalRegistry);

        this.cacheMissCounter = Counter.builder("token.cache.misses")
                .description("Number of cache misses")
                .register(Metrics.globalRegistry);
    }

    /**
    /**
     * Get detailed token quota information for the tenant
     *
     * @param tenantId The tenant ID to check
     * @return TokenQuotaResponseDTO with comprehensive quota information
     */
    @CircuitBreaker(name = "token-quota", fallbackMethod = "fallbackTokenQuota")
    public TokenQuotaResponseDTO getTokenQuota(String tenantId) {
        Timer.Sample sample = Timer.start();
        try {
            quotaCheckCounter.increment();

            // Check cache first
            TokenQuotaResponseDTO cached = getCachedQuota(tenantId);
            if (cached != null) {
                cacheHitCounter.increment();
                return cached;
            }

            cacheMissCounter.increment();

            // Call auth service
            TokenQuotaResponseDTO quota = authTokenClient.getTokenQuota(tenantId);
            if (quota != null) {
                cacheQuota(tenantId, quota);
            }

            logger.debug("Token quota for tenant {}: available={}, used={}, total={}",
                        tenantId, quota != null ? quota.isAvailable() : null,
                        quota != null ? quota.getUsedTokens() : null,
                        quota != null ? quota.getTotalTokens() : null);

            return quota;

        } catch (Exception e) {
            logger.error("Error getting token quota for tenant {}: {}", tenantId, e.getMessage(), e);
            throw e;
        } finally {
            sample.stop(tokenCheckTimer);
        }
    }

    /**
     * Check if tenant has sufficient tokens for a specific operation
     *
     * @param tenantId The tenant ID to check
     * @param requiredTokens Number of tokens required
     * @return true if tenant has sufficient tokens, false otherwise
     */
    @CircuitBreaker(name = "token-sufficiency", fallbackMethod = "fallbackTokenSufficiency")
    public boolean hasSufficientTokens(String tenantId, long requiredTokens) {
        try {
            tokenCheckCounter.increment();

            // For sufficiency checks, we prefer fresh quota data
            TokenQuotaResponseDTO quota = getTokenQuota(tenantId);
            if (quota != null) {
                boolean sufficient = quota.getRemainingTokens() >= requiredTokens;
                logger.debug("Token sufficiency for tenant {}: required={}, remaining={}, sufficient={}",
                           tenantId, requiredTokens, quota.getRemainingTokens(), sufficient);
                return sufficient;
            }

            // Fallback to quota check for availability
            TokenQuotaResponseDTO fallbackQuota = getTokenQuota(tenantId);
            return fallbackQuota != null && fallbackQuota.isAvailable();

        } catch (Exception e) {
            logger.error("Error checking token sufficiency for tenant {}: {}", tenantId, e.getMessage(), e);
            return false;
        }
    }

    // Cache helper methods
    private TokenQuotaResponseDTO getCachedQuota(String tenantId) {
        Long timestamp = cacheTimestamps.get("quota:" + tenantId);
        if (timestamp != null && (System.currentTimeMillis() - timestamp) < QUOTA_CACHE_TTL) {
            return quotaCache.get(tenantId);
        }
        return null;
    }

    private void cacheQuota(String tenantId, TokenQuotaResponseDTO quota) {
        quotaCache.put(tenantId, quota);
        cacheTimestamps.put("quota:" + tenantId, System.currentTimeMillis());
    }

    /**
     * Fallback method for token quota when auth service is down
     */
    public TokenQuotaResponseDTO fallbackTokenQuota(String tenantId, Exception ex) {
        logger.warn("Token quota check failed for tenant {}, using fallback: {}", tenantId, ex.getMessage());

        // Check if we have recent cached data
        TokenQuotaResponseDTO cached = quotaCache.get(tenantId);
        if (cached != null) {
            logger.info("Using cached token quota for tenant {}", tenantId);
            return cached;
        }

        // Conservative fallback - return unknown status
        return TokenQuotaResponseDTO.builder()
                .available(true)
                .totalTokens(0)
                .usedTokens(0)
                .remainingTokens(0)
                .usagePercentage(0.0)
                .planType("UNKNOWN")
                .subscriptionStatus("UNKNOWN")
                .timestamp(Instant.now())
                .message("Token quota unavailable - auth service error")
                .build();
    }

    /**
     * Fallback method for token sufficiency when auth service is down
     */
    public boolean fallbackTokenSufficiency(String tenantId, long requiredTokens, Exception ex) {
        logger.warn("Token sufficiency check failed for tenant {}, using fallback: {}", tenantId, ex.getMessage());

        // Try to use cached quota data first
        TokenQuotaResponseDTO cached = quotaCache.get(tenantId);
        if (cached != null) {
            boolean sufficient = cached.getRemainingTokens() >= requiredTokens;
            logger.info("Using cached token sufficiency for tenant {}: {}", tenantId, sufficient);
            return sufficient;
        }

        // Conservative fallback - allow operation with warning
        logger.warn("No cached quota data available for tenant {}, allowing operation with warning", tenantId);
        return true;
    }

    /**
     * Clear cache for a specific tenant (useful for testing or immediate updates)
     */
    public void clearCache(String tenantId) {
        quotaCache.remove(tenantId);
        cacheTimestamps.remove("quota:" + tenantId);
        logger.debug("Cleared token cache for tenant: {}", tenantId);
    }

    /**
     * Get cache statistics for monitoring
     */
    public String getCacheStats() {
        return String.format("QuotaCache: %d entries",
                           quotaCache.size());
    }
}
