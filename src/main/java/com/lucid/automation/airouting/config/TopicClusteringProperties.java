package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for the topic clustering (Step 1) pipeline.
 * Values can be overridden via application.yml using the prefix "topic.clustering".
 */
@Component
@ConfigurationProperties(prefix = "topic.clustering")
public class TopicClusteringProperties {

    /**
     * Prompt template name stored under resources/prompts.
     */
    private String promptName = "topic_clustering/v1/topic_clustering";

    /**
     * Human-readable prompt version for observability/versioning.
     */
    private String promptVersion = "v1";

    /**
     * Max messages to include in a single LLM prompt.
     */
    private int maxMessages = 200;

    /**
     * Max characters per message to prevent overlong prompts.
     */
    private int maxMessageCharacters = 300;

    /**
     * Max total characters of a prompt.
     */
    private int maxPromptCharacters = 200_000;

    /**
     * Kafka topic used to publish draft clusters.
     */
    private String draftTopic = "ai-topic-drafts";

    /**
     * Optional provider hint (bean name/providerId) to force a provider for Step 1.
     * Example: "geminiProvider", "openaiProvider", "huggingfaceProvider".
     */
    private String providerHint = "";

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

    public String getDraftTopic() {
        return draftTopic;
    }

    public void setDraftTopic(String draftTopic) {
        this.draftTopic = draftTopic;
    }

    public String getProviderHint() {
        return providerHint;
    }

    public void setProviderHint(String providerHint) {
        this.providerHint = providerHint;
    }
}
