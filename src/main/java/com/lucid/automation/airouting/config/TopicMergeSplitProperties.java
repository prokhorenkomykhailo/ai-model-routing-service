package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for Step 2 (merge/split refinement).
 */
@Component
@ConfigurationProperties(prefix = "topic.merge-split")
public class TopicMergeSplitProperties {

    /**
     * Draft input topic (from Step 1).
     */
    private String draftTopic = "ai-topic-drafts";

    /**
     * Refined output topic (to Step 3).
     */
    private String refinedTopic = "ai-topic-refined";

    /**
     * DLQ topic for malformed/unprocessable draft events.
     */
    private String dlqTopic = "ai-topic-drafts-dlq";

    /**
     * Similarity threshold for merging clusters.
     */
    private double similarityThreshold = 0.85;

    /**
     * If enabled, split oversized clusters into smaller chunks after merge.
     */
    private boolean splitEnabled = true;

    /**
     * If a cluster has more than this many messages, it is considered oversized.
     */
    private int maxMessagesPerCluster = 40;

    /**
     * Split oversized clusters into chunks of this size.
     */
    private int splitChunkSize = 25;

    public String getDraftTopic() {
        return draftTopic;
    }

    public void setDraftTopic(String draftTopic) {
        this.draftTopic = draftTopic;
    }

    public String getRefinedTopic() {
        return refinedTopic;
    }

    public void setRefinedTopic(String refinedTopic) {
        this.refinedTopic = refinedTopic;
    }

    public String getDlqTopic() {
        return dlqTopic;
    }

    public void setDlqTopic(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public void setSimilarityThreshold(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    public boolean isSplitEnabled() {
        return splitEnabled;
    }

    public void setSplitEnabled(boolean splitEnabled) {
        this.splitEnabled = splitEnabled;
    }

    public int getMaxMessagesPerCluster() {
        return maxMessagesPerCluster;
    }

    public void setMaxMessagesPerCluster(int maxMessagesPerCluster) {
        this.maxMessagesPerCluster = maxMessagesPerCluster;
    }

    public int getSplitChunkSize() {
        return splitChunkSize;
    }

    public void setSplitChunkSize(int splitChunkSize) {
        this.splitChunkSize = splitChunkSize;
    }
}
