package com.lucid.automation.airouting.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.lucid.automation.airouting.provider.AIProvider;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AIResponse {
    private String requestId;
    private AITaskType taskType;
    private boolean success;
    private Object result;
    private String providerId;
    private String errorMessage;
    private double confidence;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime processedAt;
    
    private long processingTimeMs;
    private Map<String, Object> metadata;
    
    // Constructors
    public AIResponse() {
        this.processedAt = LocalDateTime.now();
    }
    
    public static AIResponse success(String requestId, AITaskType taskType, Object result,
                                   String providerId, double confidence) {
        AIResponse response = new AIResponse();
        response.requestId = requestId;
        response.taskType = taskType;
        response.success = true;
        response.result = result;
        response.providerId = providerId;
        response.confidence = confidence;
        
        // Extract metadata from ConversationEnrichment if present
        if (result instanceof AIProvider.ConversationEnrichment enrichment) {
            if (enrichment.metadata() != null && !enrichment.metadata().isEmpty()) {
                response.metadata = new HashMap<>(enrichment.metadata());
            }
            // Add topic info to metadata
            if (response.metadata == null) {
                response.metadata = new HashMap<>();
            }
            // Get the first topic title if available
            if (!enrichment.topics().isEmpty()) {
                response.metadata.put("topic", enrichment.topics().get(0).title());
            }
        }

        return response;
    }
    
    public static AIResponse error(String requestId, AITaskType taskType, String errorMessage) {
        AIResponse response = new AIResponse();
        response.requestId = requestId;
        response.taskType = taskType;
        response.success = false;
        response.errorMessage = errorMessage;
        response.confidence = 0.0;
        return response;
    }
    
    // Getters and Setters
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    
    public AITaskType getTaskType() { return taskType; }
    public void setTaskType(AITaskType taskType) { this.taskType = taskType; }
    
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    
    public Object getResult() { return result; }
    public void setResult(Object result) { this.result = result; }
    
    public String getProviderId() { return providerId; }
    public void setProviderId(String providerId) { this.providerId = providerId; }
    
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    
    public double getConfidence() { return confidence; }
    public void setConfidence(double confidence) { this.confidence = confidence; }
    
    public LocalDateTime getProcessedAt() { return processedAt; }
    public void setProcessedAt(LocalDateTime processedAt) { this.processedAt = processedAt; }
    
    public long getProcessingTimeMs() { return processingTimeMs; }
    public void setProcessingTimeMs(long processingTimeMs) { this.processingTimeMs = processingTimeMs; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
}
