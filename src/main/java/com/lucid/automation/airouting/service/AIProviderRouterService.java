package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.RoutingConfig;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import com.lucid.automation.airouting.exception.TokenQuotaExhaustedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * AI Provider routing service with failover support
 * @author vudu
 */
@Service
public class AIProviderRouterService {

    private static final Logger logger = LoggerFactory.getLogger(AIProviderRouterService.class);

    private final AIProviderFactory providerFactory;
    private final RoutingConfig routingConfig;

    // Provider priority configuration
    private final Map<String, Integer> providerPriorities;

    public AIProviderRouterService(AIProviderFactory providerFactory,
                                 RoutingConfig routingConfig) {
        this.providerFactory = providerFactory;
        this.routingConfig = routingConfig;

        // Initialize provider priorities (configurable via ai.routing.provider-priorities)
        this.providerPriorities = new HashMap<>(Optional.ofNullable(routingConfig.getProviderPriorities()).orElseGet(HashMap::new));
        if (this.providerPriorities.isEmpty()) {
            this.providerPriorities.put("geminiProvider", 1);
            this.providerPriorities.put("openaiProvider", 2);
            this.providerPriorities.put("huggingfaceProvider", 3);
        }
    }

    /**
     * Select the best available provider for a task
     */
    public AIProvider selectProvider(AITaskType taskType, String tenantId) {
        return selectProvider(taskType, tenantId, null);
    }

    /**
     * Select the best available provider with preferred provider hint
     */
    public AIProvider selectProvider(AITaskType taskType, String tenantId, String preferredProviderId) {
        String debugId = "ROUTER-" + System.currentTimeMillis();
        logger.info("ROUTER [{}]: Selecting provider for task: {}, tenant: {}, preferred: {}",
                   debugId, taskType, tenantId, preferredProviderId);

        try {
            // Step 1: Try preferred provider if specified
            if (preferredProviderId != null && !preferredProviderId.trim().isEmpty()) {
                AIProvider preferredProvider = providerFactory.getProvider(preferredProviderId);
                if (isProviderAvailableForTenant(preferredProvider, tenantId)) {
                    logger.info("ROUTER [{}]: Using preferred provider: {}", debugId, preferredProviderId);
                    return preferredProvider;
                } else {
                    logger.warn("ROUTER [{}]: Preferred provider '{}' not available for tenant {}, falling back",
                               debugId, preferredProviderId, tenantId);
                }
            }

            // Step 2: Try task-specific provider
            String taskProvider = routingConfig.getProviderForTask(taskType);
            if (taskProvider != null) {
                AIProvider provider = providerFactory.getProvider(taskProvider);
                if (isProviderAvailableForTenant(provider, tenantId)) {
                    logger.info("ROUTER [{}]: Using task-specific provider: {} for task: {}",
                               debugId, taskProvider, taskType);
                    return provider;
                } else {
                    logger.warn("ROUTER [{}]: Task-specific provider '{}' not available for task: {} and tenant {}",
                               debugId, taskProvider, taskType, tenantId);
                }
            }

            // Step 3: Try default provider
            String defaultProvider = routingConfig.getDefaultProvider();
            if (defaultProvider != null) {
                AIProvider provider = providerFactory.getProvider(defaultProvider);
                if (isProviderAvailableForTenant(provider, tenantId)) {
                    logger.info("ROUTER [{}]: Using default provider: {}", debugId, defaultProvider);
                    return provider;
                } else {
                    logger.warn("ROUTER [{}]: Default provider '{}' not available for tenant {}", debugId, defaultProvider, tenantId);
                }
            }

            // Step 4: Find any available provider by priority
            List<AIProvider> availableProviders = findAvailableProvidersByPriorityForTenant(tenantId);
            if (!availableProviders.isEmpty()) {
                AIProvider provider = availableProviders.get(0);
                logger.warn("ROUTER [{}]: Using fallback provider: {}", debugId, provider.getProviderId());
                return provider;
            }

            // Step 5: Check if the issue is token quota exhaustion across all providers
            boolean allProvidersExist = providerFactory.getAllProviders().values().stream()
                .anyMatch(this::isProviderAvailable);

            if (allProvidersExist) {
                // Providers exist but no tokens available for this tenant
                logger.error("ROUTER [{}]: Token quota exhausted for tenant {} across all available providers", debugId, tenantId);
                throw new RuntimeException("Token quota exhausted for tenant " + tenantId +
                                         ". Please upgrade your plan or wait for quota renewal.");
            } else {
                // No providers available at all
                logger.error("ROUTER [{}]: No available providers found for task {} and tenant {}", debugId, taskType, tenantId);
                throw new RuntimeException("AI service is currently unavailable: No AI providers available for task " + taskType + " and tenant " + tenantId);
            }

        } catch (TokenQuotaExhaustedException e) {
            logger.warn("ROUTER [{}]: Token quota exhausted for tenant {} with provider {}",
                       debugId, e.getTenantId(), e.getProviderId());
            throw new RuntimeException("Token quota exhausted for tenant " + e.getTenantId() +
                                     ". Please upgrade your plan or wait for quota renewal.");
        } catch (Exception e) {
            logger.error("ROUTER [{}]: Error selecting provider for task {} and tenant {}: {}",
                        debugId, taskType, tenantId, e.getMessage(), e);
            throw new RuntimeException("AI service is currently unavailable: " + e.getMessage());
        }
    }

    /**
     * Check if a provider is available
     */
    private boolean isProviderAvailable(AIProvider provider) {
        if (provider == null) {
            return false;
        }

        // Check provider basic availability
        if (!provider.isAvailable()) {
            logger.debug("Provider {} reports as unavailable", provider.getProviderId());
            return false;
        }

        return true;
    }

    /**
     * Check if a provider is available for a specific tenant (includes token availability)
     */
    private boolean isProviderAvailableForTenant(AIProvider provider, String tenantId) {
        if (!isProviderAvailable(provider)) {
            return false;
        }

        // Use reflection to check token availability method from the provider
        try {
            // Call the protected method using reflection to check token availability
            java.lang.reflect.Method method = AIProvider.class.getDeclaredMethod("isTokenAvailableForTenant", String.class);
            method.setAccessible(true);
            boolean hasTokens = (Boolean) method.invoke(provider, tenantId);

            if (!hasTokens) {
                logger.debug("Provider {} has no tokens available for tenant {}", provider.getProviderId(), tenantId);
                return false;
            }

            return true;
        } catch (Exception e) {
            logger.warn("Could not check token availability for provider {} and tenant {}: {}",
                       provider.getProviderId(), tenantId, e.getMessage());
            // Fall back to basic availability check
            return true;
        }
    }

    /**
     * Find all available providers for a specific tenant sorted by priority
     */
    private List<AIProvider> findAvailableProvidersByPriorityForTenant(String tenantId) {
        return providerFactory.getAllProviders().values().stream()
            .filter(provider -> isProviderAvailableForTenant(provider, tenantId))
            .sorted(Comparator.comparingInt(provider ->
                providerPriorities.getOrDefault(provider.getProviderId(), Integer.MAX_VALUE)))
            .collect(Collectors.toList());
    }

    /**
     * Check if any providers are available
     */
    public boolean hasAvailableProviders() {
        return providerFactory.getAllProviders().values().stream()
            .anyMatch(this::isProviderAvailable);
    }

    /**
     * Get status of all providers
     */
    public String getProviderStatus() {
        StringBuilder status = new StringBuilder("Provider Status:\n");
        providerFactory.getAllProviders().forEach((name, provider) -> {
            status.append(String.format("- %s: %s\n", name, provider.isAvailable() ? "AVAILABLE" : "UNAVAILABLE"));
        });
        return status.toString();
    }

    /**
     * Get detailed provider statistics
     */
    public ProviderStats getProviderStats() {
        Map<String, Boolean> providerStatuses = new HashMap<>();
        int totalProviders = 0;
        int availableProviders = 0;

        for (Map.Entry<String, AIProvider> entry : providerFactory.getAllProviders().entrySet()) {
            String providerId = entry.getKey();
            AIProvider provider = entry.getValue();
            boolean isAvailable = provider.isAvailable();

            providerStatuses.put(providerId, isAvailable);
            totalProviders++;
            if (isAvailable) {
                availableProviders++;
            }
        }

        return new ProviderStats(totalProviders, availableProviders, providerStatuses);
    }

    /**
     * Data class for provider statistics
     */
    public static class ProviderStats {
        private final int totalProviders;
        private final int availableProviders;
        private final Map<String, Boolean> providerStatuses;

        public ProviderStats(int totalProviders, int availableProviders, Map<String, Boolean> providerStatuses) {
            this.totalProviders = totalProviders;
            this.availableProviders = availableProviders;
            this.providerStatuses = Map.copyOf(providerStatuses);
        }

        public int getTotalProviders() {
            return totalProviders;
        }

        public int getAvailableProviders() {
            return availableProviders;
        }

        public Map<String, Boolean> getProviderStatuses() {
            return providerStatuses;
        }

        public boolean isHealthy() {
            return availableProviders > 0;
        }

        public double getAvailabilityRatio() {
            return totalProviders > 0 ? (double) availableProviders / totalProviders : 0.0;
        }
    }
}
