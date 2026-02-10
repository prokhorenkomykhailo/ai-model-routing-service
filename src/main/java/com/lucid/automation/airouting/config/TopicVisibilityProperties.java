package com.lucid.automation.airouting.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
@ConfigurationProperties(prefix = "topic.visibility")
public class TopicVisibilityProperties {

    private String metadataTopic = "ai-topic-metadata";
    private String visibilityTopic = "topic-visibility";
    private String dlqTopic = "topic-visibility-dlq";
    private String consumerGroup = "ai-service-group-topic-visibility";

    private boolean strictChannelMembership = false;

    private String userChannelsJson = "";

    private String identityAliasesJson = "";

    private String rule = "owns_open_action_item_and_member_of_channel";

    /**
     * Emit a QA-friendly summary JSON snapshot per batch under {@link #reportDir}.
     */
    private boolean batchReportEnabled = true;

    /**
     * Directory for Step 5 local reports (relative to service working dir).
     */
    private String reportDir = "logs";

    /**
     * Cap how many user keys we include in per-topic logs.
     */
    private int maxUsersToLogPerTopic = 10;

    /**
     * How long to keep a user's visibility set for aggregation (Step 5 cache).
     */
    private long ttlSeconds = 3600;

    /**
     * Safety cap to avoid unbounded per-user topic lists.
     */
    private int maxTopicsPerUser = 5000;

    public String getMetadataTopic() {
        return metadataTopic;
    }

    public void setMetadataTopic(String metadataTopic) {
        this.metadataTopic = metadataTopic;
    }

    public String getVisibilityTopic() {
        return visibilityTopic;
    }

    public void setVisibilityTopic(String visibilityTopic) {
        this.visibilityTopic = visibilityTopic;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }

    public void setDlqTopic(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public boolean isStrictChannelMembership() {
        return strictChannelMembership;
    }

    public void setStrictChannelMembership(boolean strictChannelMembership) {
        this.strictChannelMembership = strictChannelMembership;
    }

    public String getUserChannelsJson() {
        return userChannelsJson;
    }

    public void setUserChannelsJson(String userChannelsJson) {
        this.userChannelsJson = userChannelsJson;
    }

    public String getIdentityAliasesJson() {
        return identityAliasesJson;
    }

    public void setIdentityAliasesJson(String identityAliasesJson) {
        this.identityAliasesJson = identityAliasesJson;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public boolean isBatchReportEnabled() {
        return batchReportEnabled;
    }

    public void setBatchReportEnabled(boolean batchReportEnabled) {
        this.batchReportEnabled = batchReportEnabled;
    }

    public String getReportDir() {
        return reportDir;
    }

    public void setReportDir(String reportDir) {
        this.reportDir = reportDir;
    }

    public int getMaxUsersToLogPerTopic() {
        return maxUsersToLogPerTopic;
    }

    public void setMaxUsersToLogPerTopic(int maxUsersToLogPerTopic) {
        this.maxUsersToLogPerTopic = maxUsersToLogPerTopic;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    public void setTtlSeconds(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public int getMaxTopicsPerUser() {
        return maxTopicsPerUser;
    }

    public void setMaxTopicsPerUser(int maxTopicsPerUser) {
        this.maxTopicsPerUser = maxTopicsPerUser;
    }

    public Map<String, java.util.List<String>> parseUserChannels(ObjectMapper mapper) {
        if (!StringUtils.hasText(userChannelsJson)) {
            return Collections.emptyMap();
        }
        try {
            return mapper.readValue(userChannelsJson, new TypeReference<>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public Map<String, String> parseIdentityAliases(ObjectMapper mapper) {
        if (!StringUtils.hasText(identityAliasesJson)) {
            return Collections.emptyMap();
        }
        try {
            return mapper.readValue(identityAliasesJson, new TypeReference<>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}
