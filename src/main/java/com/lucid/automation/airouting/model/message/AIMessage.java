package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Base message model for RabbitMQ AI processing requests
 */
public class AIMessage {
    
    @NotNull
    @JsonProperty("messageId")
    private String messageId;
    
    @NotNull
    @JsonProperty("taskType")
    private AITaskType taskType;
    
    @NotBlank
    @JsonProperty("content")
    private String content;
    
    @JsonProperty("tenantId")
    private String tenantId;
    
    @JsonProperty("tenantSchema")
    private String tenantSchema;
    
    @JsonProperty("conversationId")
    private String conversationId;
    
    @JsonProperty("messages")
    private List<SlackMessage> messages;
    
    @JsonProperty("participants")
    private List<SlackParticipantData> participants;
    
    @JsonProperty("context")
    private Map<String, Object> context;
    
    @JsonProperty("preferredProvider")
    private String preferredProvider;
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("priority")
    private MessagePriority priority = MessagePriority.NORMAL;
    
    @JsonProperty("requestedAt")
    private LocalDateTime requestedAt;
    
    @JsonProperty("replyTopic")
    private String replyTopic;
    
    @JsonProperty("correlationId")
    private String correlationId;
    
    public enum MessagePriority {
        LOW, NORMAL, HIGH, URGENT
    }
    
    // Constructors
    public AIMessage() {
        this.requestedAt = LocalDateTime.now();
    }
    
    public AIMessage(String messageId, AITaskType taskType, String content) {
        this();
        this.messageId = messageId;
        this.taskType = taskType;
        this.content = content;
    }
    
    // Getters and Setters
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    
    public AITaskType getTaskType() { return taskType; }
    public void setTaskType(AITaskType taskType) { this.taskType = taskType; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getTenantSchema() { return tenantSchema; }
    public void setTenantSchema(String tenantSchema) { this.tenantSchema = tenantSchema; }
    
    public String getConversationId() { return conversationId; }
    public void setConversationId(String conversationId) { this.conversationId = conversationId; }
    
    public List<SlackMessage> getMessages() { return messages; }
    public void setMessages(List<SlackMessage> messages) { this.messages = messages; }
    
    public List<SlackParticipantData> getParticipants() { return participants; }
    public void setParticipants(List<SlackParticipantData> participants) { this.participants = participants; }
    
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public MessagePriority getPriority() { return priority; }
    public void setPriority(MessagePriority priority) { this.priority = priority; }
    
    public LocalDateTime getRequestedAt() { return requestedAt; }
    public void setRequestedAt(LocalDateTime requestedAt) { this.requestedAt = requestedAt; }
    
    public String getReplyTopic() { return replyTopic; }
    public void setReplyTopic(String replyTopic) { this.replyTopic = replyTopic; }
    
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    
    @Override
    public String toString() {
        return "AIMessage{" +
                "messageId='" + messageId + '\'' +
                ", taskType=" + taskType +
                ", tenantId='" + tenantId + '\'' +
                ", priority=" + priority +
                ", requestedAt=" + requestedAt +
                '}';
    }
}
