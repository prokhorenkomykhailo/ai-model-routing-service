package com.lucid.automation.airouting.dto;

import java.time.LocalDateTime;

/**
 * DTO for AI debug logging messages
 */
public class AIDebugLog {
    private String provider;
    private String operation;
    private String debugId;
    private String promptLength;
    private String rawResponse;
    private LocalDateTime timestamp;
    
    public AIDebugLog() {
        this.timestamp = LocalDateTime.now();
    }
    
    public AIDebugLog(String provider, String operation, String debugId, String promptLength, String rawResponse) {
        this();
        this.provider = provider;
        this.operation = operation;
        this.debugId = debugId;
        this.promptLength = promptLength;
        this.rawResponse = rawResponse;
    }
    
    // Getters and setters
    public String getProvider() {
        return provider;
    }
    
    public void setProvider(String provider) {
        this.provider = provider;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public void setOperation(String operation) {
        this.operation = operation;
    }
    
    public String getDebugId() {
        return debugId;
    }
    
    public void setDebugId(String debugId) {
        this.debugId = debugId;
    }
    
    public String getPromptLength() {
        return promptLength;
    }
    
    public void setPromptLength(String promptLength) {
        this.promptLength = promptLength;
    }
    
    public String getRawResponse() {
        return rawResponse;
    }
    
    public void setRawResponse(String rawResponse) {
        this.rawResponse = rawResponse;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "AIDebugLog{" +
                "provider='" + provider + '\'' +
                ", operation='" + operation + '\'' +
                ", debugId='" + debugId + '\'' +
                ", promptLength='" + promptLength + '\'' +
                ", rawResponse='" + (rawResponse != null ? rawResponse.substring(0, Math.min(100, rawResponse.length())) + "..." : "null") + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
