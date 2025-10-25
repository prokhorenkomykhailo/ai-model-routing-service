package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data Transfer Object representing a message in the Dead Letter Queue (DLQ).
 * Contains the original message content, error details, and metadata for audit and manual intervention.
 *
 * @author vudu
 * @since 1.2.6
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DLQMessageDTO {

    @JsonProperty("original_topic")
    private String originalTopic;

    @JsonProperty("original_value")
    private Object originalValue;

    @JsonProperty("original_partition")
    private Integer originalPartition;

    @JsonProperty("original_offset")
    private Long originalOffset;

    @JsonProperty("original_key")
    private String originalKey;

    @JsonProperty("error_type")
    private String errorType;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("error_stack_trace")
    private String errorStackTrace;

    @JsonProperty("failed_at")
    private String failedAt;

    @JsonProperty("retry_attempts")
    private Integer retryAttempts;

    @JsonProperty("processing_time_ms")
    private Long processingTimeMs;

    @JsonProperty("consumer_name")
    private String consumerName;

    @JsonProperty("dlq_received_at")
    private String dlqReceivedAt;

    @JsonProperty("investigation_status")
    private String investigationStatus; // "pending", "reviewed", "resolved", "archived"

    @JsonProperty("investigation_notes")
    private String investigationNotes;

    // Constructors
    public DLQMessageDTO() {}

    public DLQMessageDTO(String originalTopic, Object originalValue, String errorType,
                        String errorMessage, String failedAt, Integer retryAttempts) {
        this.originalTopic = originalTopic;
        this.originalValue = originalValue;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.failedAt = failedAt;
        this.retryAttempts = retryAttempts;
    }

    // Getters and Setters
    public String getOriginalTopic() {
        return originalTopic;
    }

    public void setOriginalTopic(String originalTopic) {
        this.originalTopic = originalTopic;
    }

    public Object getOriginalValue() {
        return originalValue;
    }

    public void setOriginalValue(Object originalValue) {
        this.originalValue = originalValue;
    }

    public Integer getOriginalPartition() {
        return originalPartition;
    }

    public void setOriginalPartition(Integer originalPartition) {
        this.originalPartition = originalPartition;
    }

    public Long getOriginalOffset() {
        return originalOffset;
    }

    public void setOriginalOffset(Long originalOffset) {
        this.originalOffset = originalOffset;
    }

    public String getOriginalKey() {
        return originalKey;
    }

    public void setOriginalKey(String originalKey) {
        this.originalKey = originalKey;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorStackTrace() {
        return errorStackTrace;
    }

    public void setErrorStackTrace(String errorStackTrace) {
        this.errorStackTrace = errorStackTrace;
    }

    public String getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(String failedAt) {
        this.failedAt = failedAt;
    }

    public Integer getRetryAttempts() {
        return retryAttempts;
    }

    public void setRetryAttempts(Integer retryAttempts) {
        this.retryAttempts = retryAttempts;
    }

    public Long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(Long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public String getDlqReceivedAt() {
        return dlqReceivedAt;
    }

    public void setDlqReceivedAt(String dlqReceivedAt) {
        this.dlqReceivedAt = dlqReceivedAt;
    }

    public String getInvestigationStatus() {
        return investigationStatus;
    }

    public void setInvestigationStatus(String investigationStatus) {
        this.investigationStatus = investigationStatus;
    }

    public String getInvestigationNotes() {
        return investigationNotes;
    }

    public void setInvestigationNotes(String investigationNotes) {
        this.investigationNotes = investigationNotes;
    }
}
