package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.lucid.automation.airouting.dto.ConversationEnrichment;
import com.lucid.automation.airouting.model.AITaskType;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Response message model for RabbitMQ AI processing results
 */
public class EnrichmentResponse {
    
    @JsonProperty("messageId")
    private String messageId;
    
    @JsonProperty("correlationId")
    private String correlationId;
    
    @JsonProperty("conversationId")
    private String conversationId;
    
    @JsonProperty("taskType")
    private AITaskType taskType;
    
    @JsonProperty("success")
    private boolean success;
    
    @JsonProperty("result")
    private ConversationEnrichment result;
    
    @JsonProperty("providerId")
    private String providerId;
    
    @JsonProperty("errorMessage")
    private String errorMessage;
    
    @JsonProperty("confidence")
    private double confidence;
    
    @JsonProperty("processedAt")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime processedAt;
    
    @JsonProperty("processingTimeMs")
    private long processingTimeMs;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    // Constructors
    public EnrichmentResponse() {
        this.processedAt = LocalDateTime.now();
    }
    
    public static EnrichmentResponse success(String messageId, String correlationId, String conversationId,
                                          AITaskType taskType, ConversationEnrichment result, 
                                          String providerId, double confidence) {
        EnrichmentResponse response = new EnrichmentResponse();
        response.messageId = messageId;
        response.correlationId = correlationId;
        response.conversationId = conversationId;
        response.taskType = taskType;
        response.success = true;
        response.result = result;
        response.providerId = providerId;
        response.confidence = confidence;
        response.metadata = Map.of(); // Initialize with empty metadata
        response.processedAt = LocalDateTime.now();
        response.processingTimeMs = 0; // Default to 0, can be set later
        return response;
    }
    
    public static EnrichmentResponse error(String messageId, String correlationId, String conversationId,
                                        AITaskType taskType, String errorMessage) {
        EnrichmentResponse response = new EnrichmentResponse();
        response.messageId = messageId;
        response.correlationId = correlationId;
        response.conversationId = conversationId;
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
    
    public String getConversationId() { return conversationId; }
    public void setConversationId(String conversationId) { this.conversationId = conversationId; }
    
    public AITaskType getTaskType() { return taskType; }
    public void setTaskType(AITaskType taskType) { this.taskType = taskType; }
    
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    
    public ConversationEnrichment getResult() { return result; }
    public void setResult(ConversationEnrichment result) { this.result = result; }
    
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
