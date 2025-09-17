package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when a tenant has exhausted their token quota for AI provider services.
 * This exception provides specific information about quota exhaustion to enable
 * better error messaging and potential fallback to alternative providers.
 *
 * @author vudu
 */
public class TokenQuotaExhaustedException extends AIProviderException {

    private final String tenantId;

    public TokenQuotaExhaustedException(String tenantId, String providerId) {
        super("Token quota exhausted for tenant " + tenantId, providerId);
        this.tenantId = tenantId;
    }

    public TokenQuotaExhaustedException(String tenantId, String providerId, Throwable cause) {
        super("Token quota exhausted for tenant " + tenantId, providerId, cause);
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }
}