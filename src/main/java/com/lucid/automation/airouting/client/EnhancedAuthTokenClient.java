package com.lucid.automation.airouting.client;

import com.lucid.automation.common.dto.TokenQuotaResponseDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Enhanced Feign client for calling auth-service token-related endpoints
 * Provides detailed quota information and token sufficiency checks
 */
@FeignClient(name = "auth-service", configuration = FeignClientConfig.class)
public interface EnhancedAuthTokenClient {

    /**
     * Get detailed token quota information including usage, limits, and percentages
     *
     * @param tenantId The tenant ID to check
     * @return TokenQuotaResponseDTO with comprehensive quota information
     */
    @GetMapping("/internal/tenants/{tenantId}/token-quota")
    TokenQuotaResponseDTO getTokenQuota(@PathVariable("tenantId") String tenantId);

    /**
     * Check if tenant has sufficient tokens for a specific operation
     *
     * @param tenantId The tenant ID to check
     * @param requiredTokens Number of tokens required for the operation
     * @return true if tenant has sufficient tokens, false otherwise
     */
    @GetMapping("/internal/tenants/{tenantId}/token-sufficient")
    Boolean hasSufficientTokens(@PathVariable("tenantId") String tenantId,
                               @RequestParam("requiredTokens") long requiredTokens);
}
