package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "topic.action-suggestion")
public class TopicActionSuggestionProperties {

    private boolean enabled = true;
    private String promptName = "topic_action_suggestion/v1/topic_action_suggestion";
    private String promptVersion = "v1";
    private String providerHint = "";
    private String defaultTone = "professional";
    private String defaultLanguage = "English";
    private int maxContextChars = 12000;
    private int maxResponseChars = 4000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
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

    public String getProviderHint() {
        return providerHint;
    }

    public void setProviderHint(String providerHint) {
        this.providerHint = providerHint;
    }

    public String getDefaultTone() {
        return defaultTone;
    }

    public void setDefaultTone(String defaultTone) {
        this.defaultTone = defaultTone;
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public void setDefaultLanguage(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
    }

    public int getMaxContextChars() {
        return maxContextChars;
    }

    public void setMaxContextChars(int maxContextChars) {
        this.maxContextChars = maxContextChars;
    }

    public int getMaxResponseChars() {
        return maxResponseChars;
    }

    public void setMaxResponseChars(int maxResponseChars) {
        this.maxResponseChars = maxResponseChars;
    }
}

