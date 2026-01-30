package com.lucid.automation.airouting.dto.topic;

import java.util.ArrayList;
import java.util.List;

public class TopicMetadata {

    private String title;
    private String summary;
    private String externalParty;
    private List<String> participants = new ArrayList<>();
    private List<TopicActionItem> actionItems = new ArrayList<>();
    private String urgency;
    private String deadline;
    private String status;
    private String channel;
    private List<String> tags = new ArrayList<>();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getExternalParty() {
        return externalParty;
    }

    public void setExternalParty(String externalParty) {
        this.externalParty = externalParty;
    }

    public List<String> getParticipants() {
        return participants;
    }

    public void setParticipants(List<String> participants) {
        this.participants = participants;
    }

    public List<TopicActionItem> getActionItems() {
        return actionItems;
    }

    public void setActionItems(List<TopicActionItem> actionItems) {
        this.actionItems = actionItems;
    }

    public String getUrgency() {
        return urgency;
    }

    public void setUrgency(String urgency) {
        this.urgency = urgency;
    }

    public String getDeadline() {
        return deadline;
    }

    public void setDeadline(String deadline) {
        this.deadline = deadline;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}

