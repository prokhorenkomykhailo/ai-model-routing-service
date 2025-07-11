package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when an AI provider has exceeded its rate limit.
 * This exception triggers fallback behavior to alternative providers.
 */
public class AIProviderRateLimitException extends RuntimeException {
    
    private final String providerName;
    private final long retryAfterSeconds;
    
    public AIProviderRateLimitException(String providerName, long retryAfterSeconds) {
        super(String.format("Rate limit exceeded for provider '%s'. Retry after %d seconds.", 
              providerName, retryAfterSeconds));
        this.providerName = providerName;
        this.retryAfterSeconds = retryAfterSeconds;
    }
    
    public AIProviderRateLimitException(String providerName, long retryAfterSeconds, Throwable cause) {
        super(String.format("Rate limit exceeded for provider '%s'. Retry after %d seconds.", 
              providerName, retryAfterSeconds), cause);
        this.providerName = providerName;
        this.retryAfterSeconds = retryAfterSeconds;
    }
    
    public String getProviderName() {
        return providerName;
    }
    
    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
