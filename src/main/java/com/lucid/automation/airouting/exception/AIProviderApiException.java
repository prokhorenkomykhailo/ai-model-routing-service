package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when AI provider API calls fail.
 */
public class AIProviderApiException extends AIProviderException {
    
    public AIProviderApiException(String message, String providerId) {
        super(message, providerId);
    }
    
    public AIProviderApiException(String message, String providerId, Throwable cause) {
        super(message, providerId, cause);
    }
}
