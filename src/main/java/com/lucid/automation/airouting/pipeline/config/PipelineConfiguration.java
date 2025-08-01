package com.lucid.automation.airouting.pipeline.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Comprehensive configuration properties for pipeline processing.
 * This class maps both basic pipeline settings and detailed post-processing configuration.
 * Supports both 'pipeline.*' and 'lucid.post-processing.pipeline.*' configuration prefixes.
 */
@ConfigurationProperties(prefix = "lucid.post-processing.pipeline")
public class PipelineConfiguration {

    /**
     * Whether to enable the pipeline (default: true)
     */
    private boolean enabled = true;

    /**
     * Pipeline type: STANDARD, COMPREHENSIVE, MINIMAL, CUSTOM
     */
    private PipelineType type = PipelineType.STANDARD;

    /**
     * Whether to continue processing if a step fails (default: false)
     */
    private boolean continueOnFailure = false;

    /**
     * Maximum execution time for the entire pipeline in milliseconds (default: 30000)
     */
    private long maxExecutionTimeMs = 30000;

    /**
     * Whether to enable detailed logging (default: false)
     */
    private boolean detailedLogging = false;

    /**
     * Whether to enable metrics collection (default: true)
     */
    private boolean metricsEnabled = true;

    /**
     * Custom steps configuration (used when type is CUSTOM)
     */
    private List<String> customSteps;

    /**
     * Batch processing size (default: 50)
     */
    private int batchSize = 50;

    /**
     * Maximum retry attempts (default: 3)
     */
    private int maxRetries = 3;

    /**
     * Default tenant ID (default: "default-tenant")
     */
    private String defaultTenantId = "default-tenant";

    /**
     * Default tenant schema (default: "public")
     */
    private String defaultTenantSchema = "public";

    /**
     * Thread pool size for parallel processing (default: 10)
     */
    private int threadPoolSize = 10;

    /**
     * Queue capacity for task queuing (default: 100)
     */
    private int queueCapacity = 100;

    /**
     * Step-specific configurations
     */
    private StepConfiguration steps = new StepConfiguration();

    /**
     * Pipeline type enumeration
     */
    public enum PipelineType {
        STANDARD, COMPREHENSIVE, MINIMAL, CUSTOM
    }

    /**
     * Configuration for individual pipeline steps
     */
    public static class StepConfiguration {
        private StepConfig contextValidation = new StepConfig();
        private StepConfig tenantMapping = new StepConfig();
        private StepConfig userEnrichment = new StepConfig();
        private StepConfig channelEnrichment = new StepConfig();
        private StepConfig topicEnrichment = new StepConfig();
        private StepConfig sentimentAnalysis = new StepConfig();
        private StepConfig responseFormatting = new StepConfig();
        private StepConfig auditLogging = new StepConfig();

        // Getters and setters
        public StepConfig getContextValidation() { return contextValidation; }
        public void setContextValidation(StepConfig contextValidation) { this.contextValidation = contextValidation; }

        public StepConfig getTenantMapping() { return tenantMapping; }
        public void setTenantMapping(StepConfig tenantMapping) { this.tenantMapping = tenantMapping; }

        public StepConfig getUserEnrichment() { return userEnrichment; }
        public void setUserEnrichment(StepConfig userEnrichment) { this.userEnrichment = userEnrichment; }

        public StepConfig getChannelEnrichment() { return channelEnrichment; }
        public void setChannelEnrichment(StepConfig channelEnrichment) { this.channelEnrichment = channelEnrichment; }

        public StepConfig getTopicEnrichment() { return topicEnrichment; }
        public void setTopicEnrichment(StepConfig topicEnrichment) { this.topicEnrichment = topicEnrichment; }

        public StepConfig getSentimentAnalysis() { return sentimentAnalysis; }
        public void setSentimentAnalysis(StepConfig sentimentAnalysis) { this.sentimentAnalysis = sentimentAnalysis; }

        public StepConfig getResponseFormatting() { return responseFormatting; }
        public void setResponseFormatting(StepConfig responseFormatting) { this.responseFormatting = responseFormatting; }

        public StepConfig getAuditLogging() { return auditLogging; }
        public void setAuditLogging(StepConfig auditLogging) { this.auditLogging = auditLogging; }
    }

    /**
     * Configuration for individual step behavior
     */
    public static class StepConfig {
        private boolean enabled = true;
        private long timeoutMs = 10000;
        private boolean strictMode = false;
        private String provider;
        private int batchSize = 10;
        private int retryAttempts = 3;
        private long retryDelayMs = 1000;
        private boolean useCache = true;
        private long cacheTtlSeconds = 300;
        private boolean includeMetadata = true;
        private boolean aiEnabled = false;
        private double confidenceThreshold = 0.7;
        private int maxItems = 5;
        private String logLevel = "INFO";

        // Getters and setters
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public long getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }

        public boolean isStrictMode() { return strictMode; }
        public void setStrictMode(boolean strictMode) { this.strictMode = strictMode; }

        public String getProvider() { return provider; }
        public void setProvider(String provider) { this.provider = provider; }

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

        public int getRetryAttempts() { return retryAttempts; }
        public void setRetryAttempts(int retryAttempts) { this.retryAttempts = retryAttempts; }

        public long getRetryDelayMs() { return retryDelayMs; }
        public void setRetryDelayMs(long retryDelayMs) { this.retryDelayMs = retryDelayMs; }

        public boolean isUseCache() { return useCache; }
        public void setUseCache(boolean useCache) { this.useCache = useCache; }

        public long getCacheTtlSeconds() { return cacheTtlSeconds; }
        public void setCacheTtlSeconds(long cacheTtlSeconds) { this.cacheTtlSeconds = cacheTtlSeconds; }

        public boolean isIncludeMetadata() { return includeMetadata; }
        public void setIncludeMetadata(boolean includeMetadata) { this.includeMetadata = includeMetadata; }

        public boolean isAiEnabled() { return aiEnabled; }
        public void setAiEnabled(boolean aiEnabled) { this.aiEnabled = aiEnabled; }

        public double getConfidenceThreshold() { return confidenceThreshold; }
        public void setConfidenceThreshold(double confidenceThreshold) { this.confidenceThreshold = confidenceThreshold; }

        public int getMaxItems() { return maxItems; }
        public void setMaxItems(int maxItems) { this.maxItems = maxItems; }

        public String getLogLevel() { return logLevel; }
        public void setLogLevel(String logLevel) { this.logLevel = logLevel; }
    }

    // Main configuration getters and setters
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public PipelineType getType() { return type; }
    public void setType(PipelineType type) { this.type = type; }

    public boolean isContinueOnFailure() { return continueOnFailure; }
    public void setContinueOnFailure(boolean continueOnFailure) { this.continueOnFailure = continueOnFailure; }

    public long getMaxExecutionTimeMs() { return maxExecutionTimeMs; }
    public void setMaxExecutionTimeMs(long maxExecutionTimeMs) { this.maxExecutionTimeMs = maxExecutionTimeMs; }

    public boolean isDetailedLogging() { return detailedLogging; }
    public void setDetailedLogging(boolean detailedLogging) { this.detailedLogging = detailedLogging; }

    public boolean isMetricsEnabled() { return metricsEnabled; }
    public void setMetricsEnabled(boolean metricsEnabled) { this.metricsEnabled = metricsEnabled; }

    public List<String> getCustomSteps() { return customSteps; }
    public void setCustomSteps(List<String> customSteps) { this.customSteps = customSteps; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public String getDefaultTenantId() { return defaultTenantId; }
    public void setDefaultTenantId(String defaultTenantId) { this.defaultTenantId = defaultTenantId; }

    public String getDefaultTenantSchema() { return defaultTenantSchema; }
    public void setDefaultTenantSchema(String defaultTenantSchema) { this.defaultTenantSchema = defaultTenantSchema; }

    public int getThreadPoolSize() { return threadPoolSize; }
    public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }

    public StepConfiguration getSteps() { return steps; }
    public void setSteps(StepConfiguration steps) { this.steps = steps; }
}
