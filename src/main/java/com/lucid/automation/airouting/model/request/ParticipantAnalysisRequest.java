package com.lucid.automation.airouting.model.request;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class ParticipantAnalysisRequest {
    @NotNull
    private SlackParticipant participant;
    private List<SlackMessage> messages;
    private String tenantId;
    private String preferredProvider;
    private String replyTopic;
    
    // Constructors
    public ParticipantAnalysisRequest() {}
    
    public ParticipantAnalysisRequest(SlackParticipant participant, List<SlackMessage> messages) {
        this.participant = participant;
        this.messages = messages;
    }
    
    // Getters and Setters
    public SlackParticipant getParticipant() { return participant; }
    public void setParticipant(SlackParticipant participant) { this.participant = participant; }
    
    public List<SlackMessage> getMessages() { return messages; }
    public void setMessages(List<SlackMessage> messages) { this.messages = messages; }
    
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    
    public String getPreferredProvider() { return preferredProvider; }
    public void setPreferredProvider(String preferredProvider) { this.preferredProvider = preferredProvider; }
    
    public String getReplyTopic() { return replyTopic; }
    public void setReplyTopic(String replyTopic) { this.replyTopic = replyTopic; }
}
