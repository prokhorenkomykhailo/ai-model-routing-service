package com.lucid.automation.airouting.dto;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Data Transfer Object for workspace statistics
 */
public class WorkspaceStats {
    private final String workspaceId;
    private final String tenantId;
    private final String tenantSchema;
    private final String name;
    private final Long totalMessages;
    private final Long totalChannels;
    private final Long totalThreads;
    private final Instant firstMessageAt;
    private final Instant lastMessageAt;
    private final Set<String> channelIds;
    private final Instant generatedAt;
    
    public WorkspaceStats(String workspaceId, String tenantId, String tenantSchema, String name, 
                         Long totalMessages, Long totalChannels, Long totalThreads,
                         Instant firstMessageAt, Instant lastMessageAt, Set<String> channelIds) {
        this.workspaceId = workspaceId;
        this.tenantId = tenantId;
        this.tenantSchema = tenantSchema;
        this.name = name;
        this.totalMessages = totalMessages != null ? totalMessages : 0L;
        this.totalChannels = totalChannels != null ? totalChannels : 0L;
        this.totalThreads = totalThreads != null ? totalThreads : 0L;
        this.firstMessageAt = firstMessageAt;
        this.lastMessageAt = lastMessageAt;
        this.channelIds = channelIds != null ? new HashSet<>(channelIds) : new HashSet<>();
        this.generatedAt = Instant.now();
    }
    
    // Getters
    public String getWorkspaceId() { return workspaceId; }
    public String getTenantId() { return tenantId; }
    public String getTenantSchema() { return tenantSchema; }
    public String getName() { return name; }
    public Long getTotalMessages() { return totalMessages; }
    public Long getTotalChannels() { return totalChannels; }
    public Long getTotalThreads() { return totalThreads; }
    public Instant getFirstMessageAt() { return firstMessageAt; }
    public Instant getLastMessageAt() { return lastMessageAt; }
    public Set<String> getChannelIds() { return new HashSet<>(channelIds); }
    public Instant getGeneratedAt() { return generatedAt; }
    
    public Duration getActivityDuration() {
        if (firstMessageAt != null && lastMessageAt != null) {
            return Duration.between(firstMessageAt, lastMessageAt);
        }
        return Duration.ZERO;
    }
    
    public double getMessagesPerDay() {
        Duration duration = getActivityDuration();
        long days = duration.toDays();
        if (days > 0) {
            return totalMessages.doubleValue() / days;
        }
        return totalMessages.doubleValue();
    }
    
    public double getMessagesPerHour() {
        Duration duration = getActivityDuration();
        long hours = duration.toHours();
        if (hours > 0) {
            return totalMessages.doubleValue() / hours;
        }
        return totalMessages.doubleValue();
    }
    
    public boolean isActive() {
        if (lastMessageAt == null) {
            return false;
        }
        // Consider workspace active if it had messages in the last 30 days
        return lastMessageAt.isAfter(Instant.now().minus(Duration.ofDays(30)));
    }
    
    @Override
    public String toString() {
        return String.format("WorkspaceStats{workspaceId='%s', name='%s', messages=%d, channels=%d, threads=%d, active=%s}", 
                           workspaceId, name, totalMessages, totalChannels, totalThreads, isActive());
    }
}
