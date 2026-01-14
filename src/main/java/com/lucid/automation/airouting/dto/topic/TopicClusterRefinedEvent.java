package com.lucid.automation.airouting.dto.topic;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TopicClusterRefinedEvent {
    private String eventId;
    private String workspaceId;
    private String batchId;
    private String providerId;
    private String promptVersion;
    private Instant createdAt;
    private int clusterCount;
    private List<TopicClusterRefined> clusters = new ArrayList<>();
    private Object metadata;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
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

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public int getClusterCount() {
        return clusterCount;
    }

    public void setClusterCount(int clusterCount) {
        this.clusterCount = clusterCount;
    }

    public List<TopicClusterRefined> getClusters() {
        return clusters;
    }

    public void setClusters(List<TopicClusterRefined> clusters) {
        this.clusters = clusters;
    }

    public Object getMetadata() {
        return metadata;
    }

    public void setMetadata(Object metadata) {
        this.metadata = metadata;
    }
}
