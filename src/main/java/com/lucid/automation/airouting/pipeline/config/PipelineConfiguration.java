package com.lucid.automation.airouting.pipeline.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for pipeline processing.
 */
@Component
@ConfigurationProperties(prefix = "pipeline")
public class PipelineConfiguration {
    
    /**
     * Whether to enable the pipeline (default: true)
     */
    private boolean enabled = true;
    
    /**
     * Whether to continue processing if a step fails (default: true)
     */
    private boolean continueOnFailure = true;
    
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
    
    // Getters and setters
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isContinueOnFailure() {
        return continueOnFailure;
    }
    
    public void setContinueOnFailure(boolean continueOnFailure) {
        this.continueOnFailure = continueOnFailure;
    }
    
    public long getMaxExecutionTimeMs() {
        return maxExecutionTimeMs;
    }
    
    public void setMaxExecutionTimeMs(long maxExecutionTimeMs) {
        this.maxExecutionTimeMs = maxExecutionTimeMs;
    }
    
    public boolean isDetailedLogging() {
        return detailedLogging;
    }
    
    public void setDetailedLogging(boolean detailedLogging) {
        this.detailedLogging = detailedLogging;
    }
    
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }
    
    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }
}
