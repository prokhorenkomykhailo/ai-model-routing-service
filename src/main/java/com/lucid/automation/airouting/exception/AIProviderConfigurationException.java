package com.lucid.automation.airouting.exception;

/**
 * Exception thrown when AI provider configuration is invalid or missing.
 */
public class AIProviderConfigurationException extends AIProviderException {
    
    public AIProviderConfigurationException(String message, String providerId) {
        super(message, providerId);
    }
    
    public AIProviderConfigurationException(String message, String providerId, Throwable cause) {
        super(message, providerId, cause);
    }
}
