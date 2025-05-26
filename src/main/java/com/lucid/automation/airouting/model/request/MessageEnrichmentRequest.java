package com.lucid.automation.airouting.model.request;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public class MessageEnrichmentRequest {
    @NotBlank
    private String content;
    private Map<String, Object> context;
    private String tenantId;
    private String preferredProvider;
    private String replyTopic;
    
    // Constructors
    public MessageEnrichmentRequest() {}
    
    public MessageEnrichmentRequest(String content) {
        this.content = content;
    }
    
    // Getters and Setters
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
    
    public String getReplyTopic() { return replyTopic; }
    public void setReplyTopic(String replyTopic) { this.replyTopic = replyTopic; }
}
