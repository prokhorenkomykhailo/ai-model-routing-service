package com.lucid.automation.airouting.provider;

import com.lucid.automation.airouting.config.RoutingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AIProviderFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(AIProviderFactory.class);
    
    private final Map<String, AIProvider> providers = new ConcurrentHashMap<>();
    private final RoutingConfig routingConfig;
    
    public AIProviderFactory(RoutingConfig routingConfig,
                           Map<String, AIProvider> availableProviders) {
        this.routingConfig = routingConfig;
        
        // Register all available providers
        availableProviders.forEach((name, provider) -> {
            providers.put(name, provider);
            logger.info("Registered AI provider: {}", name);
        });
    }
    
    public AIProvider getProvider(String providerId) {
        AIProvider provider = providers.get(providerId);
        if (provider == null) {
            logger.warn("Provider '{}' not found, available providers: {}", 
                       providerId, providers.keySet());
        }
        return provider;
    }
    
    public AIProvider getDefaultProvider() {
        String defaultProviderId = routingConfig.getDefaultProvider();
        AIProvider defaultProvider = providers.get(defaultProviderId);
        
        if (defaultProvider == null || !defaultProvider.isAvailable()) {
            // Find any available provider as last resort
            for (AIProvider provider : providers.values()) {
                if (provider.isAvailable()) {
                    logger.warn("Default provider '{}' unavailable, using fallback: {}", 
                               defaultProviderId, provider.getProviderId());
                    return provider;
                }
            }
            throw new RuntimeException("No AI providers available");
        }
        
        return defaultProvider;
    }
    
    public Map<String, AIProvider> getAllProviders() {
        return Map.copyOf(providers);
    }
    
    public boolean isProviderAvailable(String providerId) {
        AIProvider provider = providers.get(providerId);
        return provider != null && provider.isAvailable();
    }
}
