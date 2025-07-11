package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when AI provider operations fail.
 */
public class AIProviderException extends RuntimeException {
    
    private final String providerId;
    
    public AIProviderException(String message, String providerId) {
        super(message);
        this.providerId = providerId;
    }
    
    public AIProviderException(String message, String providerId, Throwable cause) {
        super(message, cause);
        this.providerId = providerId;
    }
    
    public String getProviderId() {
        return providerId;
    }
}
