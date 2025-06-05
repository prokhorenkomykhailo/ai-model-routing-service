package com.lucid.automation.airouting.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class AIRequest {
    @NotNull
    private AITaskType taskType;
    
    @NotBlank
    private String content;
    
    private String conversationId;
    private List<SlackMessage> messages;
    private List<SlackParticipant> participants;
    private Map<String, Object> context;
    private String tenantId;
    private String preferredProvider;
    private String userId;
    private List<String> availableCategories;
    
    // Constructors
    public AIRequest() {}
    
    public AIRequest(AITaskType taskType, String content) {
        this.taskType = taskType;
        this.content = content;
    }
    
    // Getters and Setters
    public AITaskType getTaskType() { return taskType; }
    public void setTaskType(AITaskType taskType) { this.taskType = taskType; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public String getConversationId() { return conversationId; }
    public void setConversationId(String conversationId) { this.conversationId = conversationId; }
    
    public List<SlackMessage> getMessages() { return messages; }
    public void setMessages(List<SlackMessage> messages) { this.messages = messages; }
    
    public List<SlackParticipant> getParticipants() { return participants; }
    public void setParticipants(List<SlackParticipant> participants) { this.participants = participants; }
    
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public List<String> getAvailableCategories() { return availableCategories; }
    public void setAvailableCategories(List<String> availableCategories) { this.availableCategories = availableCategories; }
}
