package com.lucid.automation.airouting.dto.topic.suggestion;

import java.util.Map;

public class TopicActionSuggestionResponse {

    private String topicId;
    private TopicActionType actionType;
    private String suggestedSubject;
    private String suggestedText;
    private String providerId;
    private String promptVersion;
    private boolean topicContextFound;
    private Map<String, Object> metadata;

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public TopicActionType getActionType() {
        return actionType;
    }

    public void setActionType(TopicActionType actionType) {
        this.actionType = actionType;
    }

    public String getSuggestedSubject() {
        return suggestedSubject;
    }

    public void setSuggestedSubject(String suggestedSubject) {
        this.suggestedSubject = suggestedSubject;
    }

    public String getSuggestedText() {
        return suggestedText;
    }

    public void setSuggestedText(String suggestedText) {
        this.suggestedText = suggestedText;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getPromptVersion() {
        return promptVersion;
    }

    public void setPromptVersion(String promptVersion) {
        this.promptVersion = promptVersion;
    }

    public boolean isTopicContextFound() {
        return topicContextFound;
    }

    public void setTopicContextFound(boolean topicContextFound) {
        this.topicContextFound = topicContextFound;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}

