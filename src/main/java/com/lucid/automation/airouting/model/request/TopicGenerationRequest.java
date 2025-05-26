package com.lucid.automation.airouting.model.request;

import com.lucid.automation.airouting.model.SlackMessage;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public class TopicGenerationRequest {
    @NotEmpty
    private List<SlackMessage> messages;
    private String tenantId;
    private String preferredProvider;
    private String replyTopic;
    
    // Constructors
    public TopicGenerationRequest() {}
    
    public TopicGenerationRequest(List<SlackMessage> messages) {
        this.messages = messages;
    }
    
    // Getters and Setters
    public List<SlackMessage> getMessages() { return messages; }
    public void setMessages(List<SlackMessage> messages) { this.messages = messages; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
    
    public String getReplyTopic() { return replyTopic; }
    public void setReplyTopic(String replyTopic) { this.replyTopic = replyTopic; }
}
