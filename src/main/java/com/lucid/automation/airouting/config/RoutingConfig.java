package com.lucid.automation.airouting.config;

import com.lucid.automation.airouting.model.AITaskType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.HashMap;

@Configuration
@ConfigurationProperties(prefix = "ai.routing")
public class RoutingConfig {
    
    private Map<String, String> taskProviders = new HashMap<>();
    private String defaultProvider = "geminiProvider";
    private Map<String, ProviderConfig> providers = new HashMap<>();
    private Map<String, Integer> providerPriorities = new HashMap<>();
    
    public String getProviderForTask(AITaskType taskType) {
        return taskProviders.getOrDefault(taskType.name().toLowerCase(), defaultProvider);
    }
    
    // Getters and Setters
    public Map<String, String> getTaskProviders() { return taskProviders; }
    public void setTaskProviders(Map<String, String> taskProviders) { this.taskProviders = taskProviders; }
    
    public String getDefaultProvider() { return defaultProvider; }
    public void setDefaultProvider(String defaultProvider) { this.defaultProvider = defaultProvider; }
    
    public Map<String, ProviderConfig> getProviders() { return providers; }
    public void setProviders(Map<String, ProviderConfig> providers) { this.providers = providers; }

    public Map<String, Integer> getProviderPriorities() {
        return providerPriorities;
    }

    public void setProviderPriorities(Map<String, Integer> providerPriorities) {
        this.providerPriorities = providerPriorities;
    }
    
    public static class ProviderConfig {
        private String type;
        private String apiKey;
        private String endpoint;
        private int timeout = 30000;
        private boolean enabled = true;
        
        // Getters and Setters
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
        
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        
        public int getTimeout() { return timeout; }
        public void setTimeout(int timeout) { this.timeout = timeout; }
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }
}
