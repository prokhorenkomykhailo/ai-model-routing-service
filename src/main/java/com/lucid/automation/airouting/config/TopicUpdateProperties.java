package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "topic.update")
public class TopicUpdateProperties {

    private boolean enabled = false;

    private int searchTopK = 5;

    private double similarityThreshold = 0.85;

    private boolean requireSameChannel = true;

    private String updatePromptName = "topic_update/v1/topic_update";

    private boolean belongsCheckEnabled = true;

    private String belongsCheckModelHint = "";

    private String promptVersion = "v1";

    /**
     * If enabled, Step 6 will attempt to compute richer rejection reasons for top-k candidates
     * by loading candidate topic metadata (channel, etc.) from pgvector. Otherwise, non-selected
     * candidates are marked as rejected due to lower score.
     */
    private boolean debugCandidateEvaluationEnabled = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getSearchTopK() {
        return searchTopK;
    }

    public void setSearchTopK(int searchTopK) {
        this.searchTopK = searchTopK;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public void setSimilarityThreshold(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    public boolean isRequireSameChannel() {
        return requireSameChannel;
    }

    public void setRequireSameChannel(boolean requireSameChannel) {
        this.requireSameChannel = requireSameChannel;
    }

    public String getUpdatePromptName() {
        return updatePromptName;
    }

    public void setUpdatePromptName(String updatePromptName) {
        this.updatePromptName = updatePromptName;
    }

    public boolean isBelongsCheckEnabled() {
        return belongsCheckEnabled;
    }

    public void setBelongsCheckEnabled(boolean belongsCheckEnabled) {
        this.belongsCheckEnabled = belongsCheckEnabled;
    }

    public String getBelongsCheckModelHint() {
        return belongsCheckModelHint;
    }

    public void setBelongsCheckModelHint(String belongsCheckModelHint) {
        this.belongsCheckModelHint = belongsCheckModelHint;
    }

    public String getPromptVersion() {
        return promptVersion;
    }

    public void setPromptVersion(String promptVersion) {
        this.promptVersion = promptVersion;
    }

    public boolean isDebugCandidateEvaluationEnabled() {
        return debugCandidateEvaluationEnabled;
    }

    public void setDebugCandidateEvaluationEnabled(boolean debugCandidateEvaluationEnabled) {
        this.debugCandidateEvaluationEnabled = debugCandidateEvaluationEnabled;
    }
}
