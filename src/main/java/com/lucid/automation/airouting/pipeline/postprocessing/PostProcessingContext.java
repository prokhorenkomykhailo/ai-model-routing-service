package com.lucid.automation.airouting.pipeline.postprocessing;

import com.lucid.automation.common.dto.enrichment.ConversationEnrichment;
import com.lucid.automation.common.dto.enrichment.EnrichmentResponse;
import com.lucid.automation.airouting.model.SlackMessage;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Context object that holds all data needed during post-processing pipeline execution.
 * This context is passed through each pipeline step and can be modified by steps.
 */
public class PostProcessingContext {
    
    // Input data
    private Map<String, Object> rawResponseMap;
    private String messageId;
    private String correlationId;
    private String taskType;
    private String tenantId;
    private String tenantSchema;
    private String userId;
    private String deemergeUserId;
    private String deemergeUserName;
    private String teamId;
    private LocalDateTime processedAt;
    
    // Intermediate processing data
    private Map<String, Object> aiResultMap;
    private String responseResult;
    private List<SlackMessage> requestMessages;
    private ConversationEnrichment conversationEnrichment;
    
    // Output data
    private EnrichmentResponse enrichmentResponse;
    
    // Processing metadata
    private Map<String, Object> metadata;
    private boolean processingFailed = false;
    private String errorMessage;
    private Exception lastException;
    
    public PostProcessingContext() {
        this.metadata = new HashMap<>();
    }
    
    public PostProcessingContext(Map<String, Object> rawResponseMap) {
        this();
        this.rawResponseMap = rawResponseMap;
    }
    
    // Getters and setters
    public Map<String, Object> getRawResponseMap() {
        return rawResponseMap;
    }
    
    public void setRawResponseMap(Map<String, Object> rawResponseMap) {
        this.rawResponseMap = rawResponseMap;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getTaskType() {
        return taskType;
    }
    
    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }
    
    public String getTenantId() {
        return tenantId;
    }
    
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
    
    public String getTenantSchema() {
        return tenantSchema;
    }
    
    public void setTenantSchema(String tenantSchema) {
        this.tenantSchema = tenantSchema;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getDeemergeUserId() {
        return deemergeUserId;
    }
    
    public void setDeemergeUserId(String deemergeUserId) {
        this.deemergeUserId = deemergeUserId;
    }
    
    public String getDeemergeUserName() {
        return deemergeUserName;
    }
    
    public void setDeemergeUserName(String deemergeUserName) {
        this.deemergeUserName = deemergeUserName;
    }
    
    public String getTeamId() {
        return teamId;
    }
    
    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }
    
    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
    
    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }
    
    public Map<String, Object> getAiResultMap() {
        return aiResultMap;
    }
    
    public void setAiResultMap(Map<String, Object> aiResultMap) {
        this.aiResultMap = aiResultMap;
    }
    
    public String getResponseResult() {
        return responseResult;
    }
    
    public void setResponseResult(String responseResult) {
        this.responseResult = responseResult;
    }
    
    public List<SlackMessage> getRequestMessages() {
        return requestMessages;
    }
    
    public void setRequestMessages(List<SlackMessage> requestMessages) {
        this.requestMessages = requestMessages;
    }
    
    public ConversationEnrichment getConversationEnrichment() {
        return conversationEnrichment;
    }
    
    public void setConversationEnrichment(ConversationEnrichment conversationEnrichment) {
        this.conversationEnrichment = conversationEnrichment;
    }
    
    public EnrichmentResponse getEnrichmentResponse() {
        return enrichmentResponse;
    }
    
    public void setEnrichmentResponse(EnrichmentResponse enrichmentResponse) {
        this.enrichmentResponse = enrichmentResponse;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
    
    public void addMetadata(String key, Object value) {
        this.metadata.put(key, value);
    }
    
    public Object getMetadata(String key) {
        return this.metadata.get(key);
    }
    
    public boolean isProcessingFailed() {
        return processingFailed;
    }
    
    public void setProcessingFailed(boolean processingFailed) {
        this.processingFailed = processingFailed;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.processingFailed = true;
    }
    
    public Exception getLastException() {
        return lastException;
    }
    
    public void setLastException(Exception lastException) {
        this.lastException = lastException;
        this.processingFailed = true;
    }
    
    /**
     * Mark the context as failed with error message and exception
     */
    public void markAsFailed(String errorMessage, Exception exception) {
        this.processingFailed = true;
        this.errorMessage = errorMessage;
        this.lastException = exception;
    }
    
    /**
     * Check if context has all required input data for processing
     */
    public boolean hasValidInput() {
        return rawResponseMap != null && !rawResponseMap.isEmpty();
    }
    
    @Override
    public String toString() {
        return "PostProcessingContext{" +
                "messageId='" + messageId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", taskType='" + taskType + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", teamId='" + teamId + '\'' +
                ", processingFailed=" + processingFailed +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
