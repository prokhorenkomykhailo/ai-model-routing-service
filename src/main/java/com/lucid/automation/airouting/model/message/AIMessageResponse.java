package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lucid.automation.airouting.model.AITaskType;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Response message model for RabbitMQ AI processing results
 */
public class AIMessageResponse {
    
    @JsonProperty("messageId")
    private String messageId;
    
    @JsonProperty("correlationId")
    private String correlationId;
    
    @JsonProperty("taskType")
    private AITaskType taskType;
    
    @JsonProperty("success")
    private boolean success;
    
    @JsonProperty("result")
    private Object result;
    
    @JsonProperty("providerId")
    private String providerId;
    
    @JsonProperty("errorMessage")
    private String errorMessage;
    
    @JsonProperty("confidence")
    private double confidence;
    
    @JsonProperty("processedAt")
    private LocalDateTime processedAt;
    
    @JsonProperty("processingTimeMs")
    private long processingTimeMs;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    // Constructors
    public AIMessageResponse() {
        this.processedAt = LocalDateTime.now();
    }
    
    public static AIMessageResponse success(String messageId, String correlationId, 
                                          AITaskType taskType, Object result, 
                                          String providerId, double confidence) {
        AIMessageResponse response = new AIMessageResponse();
        response.messageId = messageId;
        response.correlationId = correlationId;
        response.taskType = taskType;
        response.success = true;
        response.result = result;
        response.providerId = providerId;
        response.confidence = confidence;
        return response;
    }
    
    public static AIMessageResponse error(String messageId, String correlationId, 
                                        AITaskType taskType, String errorMessage) {
        AIMessageResponse response = new AIMessageResponse();
        response.messageId = messageId;
        response.correlationId = correlationId;
        response.taskType = taskType;
        response.success = false;
        response.errorMessage = errorMessage;
        response.confidence = 0.0;
        return response;
    }
    
    // Getters and Setters
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    
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
    
    @Override
    public String toString() {
        return "AIMessageResponse{" +
                "messageId='" + messageId + '\'' +
                ", taskType=" + taskType +
                ", success=" + success +
                ", providerId='" + providerId + '\'' +
                ", processedAt=" + processedAt +
                '}';
    }
}
