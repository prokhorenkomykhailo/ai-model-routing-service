package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "topic.metadata")
public class TopicMetadataProperties {

    private String refinedTopic = "ai-topic-refined";
    private String metadataTopic = "ai-topic-metadata";
    private String dlqTopic = "ai-topic-metadata-dlq";

    private String tenantId = "";

    private String promptName = "topic_metadata/v2/topic_metadata";
    private String promptVersion = "v2";

    private int maxMessages = 60;
    private int maxMessageCharacters = 600;
    private int maxPromptCharacters = 180_000;

    private int minTagsFallback = 5;
    private int maxTagsFallback = 10;

    private boolean repairEmptyActionItemsEnabled = true;
    private int repairEmptyActionItemsMaxAttempts = 1;

    private boolean csvFallbackEnabled = false;
    private String csvPath = "";

    public String getRefinedTopic() {
        return refinedTopic;
    }

    public void setRefinedTopic(String refinedTopic) {
        this.refinedTopic = refinedTopic;
    }

    public String getMetadataTopic() {
        return metadataTopic;
    }

    public void setMetadataTopic(String metadataTopic) {
        this.metadataTopic = metadataTopic;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }

    public void setDlqTopic(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getPromptName() {
        return promptName;
    }

    public void setPromptName(String promptName) {
        this.promptName = promptName;
    }

    public String getPromptVersion() {
        return promptVersion;
    }

    public void setPromptVersion(String promptVersion) {
        this.promptVersion = promptVersion;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public int getMaxMessageCharacters() {
        return maxMessageCharacters;
    }

    public void setMaxMessageCharacters(int maxMessageCharacters) {
        this.maxMessageCharacters = maxMessageCharacters;
    }

    public int getMaxPromptCharacters() {
        return maxPromptCharacters;
    }

    public void setMaxPromptCharacters(int maxPromptCharacters) {
        this.maxPromptCharacters = maxPromptCharacters;
    }

    public int getMinTagsFallback() {
        return minTagsFallback;
    }

    public void setMinTagsFallback(int minTagsFallback) {
        this.minTagsFallback = minTagsFallback;
    }

    public int getMaxTagsFallback() {
        return maxTagsFallback;
    }

    public void setMaxTagsFallback(int maxTagsFallback) {
        this.maxTagsFallback = maxTagsFallback;
    }

    public boolean isRepairEmptyActionItemsEnabled() {
        return repairEmptyActionItemsEnabled;
    }

    public void setRepairEmptyActionItemsEnabled(boolean repairEmptyActionItemsEnabled) {
        this.repairEmptyActionItemsEnabled = repairEmptyActionItemsEnabled;
    }

    public int getRepairEmptyActionItemsMaxAttempts() {
        return repairEmptyActionItemsMaxAttempts;
    }

    public void setRepairEmptyActionItemsMaxAttempts(int repairEmptyActionItemsMaxAttempts) {
        this.repairEmptyActionItemsMaxAttempts = repairEmptyActionItemsMaxAttempts;
    }

    public boolean isCsvFallbackEnabled() {
        return csvFallbackEnabled;
    }

    public void setCsvFallbackEnabled(boolean csvFallbackEnabled) {
        this.csvFallbackEnabled = csvFallbackEnabled;
    }

    public String getCsvPath() {
        return csvPath;
    }

    public void setCsvPath(String csvPath) {
        this.csvPath = csvPath;
    }
}
