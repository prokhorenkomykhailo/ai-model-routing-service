package com.lucid.automation.airouting.model.request;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public class ConversationEnrichmentRequest {
    @NotNull
    private String conversationId;

    @NotEmpty
    private List<SlackMessage> messages;

    private List<SlackParticipant> participants;
    private Map<String, Object> context;
    private String tenantId;
    private String tenantSchema;
    private String preferredProvider;
    private String replyTopic;

    // Constructors
    public ConversationEnrichmentRequest() {
    }

    public ConversationEnrichmentRequest(String conversationId, List<SlackMessage> messages) {
        this.conversationId = conversationId;
        this.messages = messages;
    }

    // Getters and Setters
    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    public List<SlackMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<SlackMessage> messages) {
        this.messages = messages;
    }

    public List<SlackParticipant> getParticipants() {
        return participants;
    }

    public void setParticipants(List<SlackParticipant> participants) {
        this.participants = participants;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void setContext(Map<String, Object> context) {
        this.context = context;
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

    public String getPreferredProvider() {
        return preferredProvider;
    }

    public void setPreferredProvider(String preferredProvider) {
        this.preferredProvider = preferredProvider;
    }

    public String getReplyTopic() {
        return replyTopic;
    }

    public void setReplyTopic(String replyTopic) {
        this.replyTopic = replyTopic;
    }
}
