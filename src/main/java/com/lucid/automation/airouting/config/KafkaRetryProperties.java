package com.lucid.automation.airouting.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Kafka retry configuration properties bound to application.yml.
 * Provides centralized configuration for retry behavior, DLQ settings, and metrics.
 *
 * @author vudu
 * @since 1.2.6
 */
@Component
@ConfigurationProperties(prefix = "kafka.retry")
public class KafkaRetryProperties {

    /**
     * Enable/disable retry logic for Kafka listeners
     */
    private boolean enabled = true;

    /**
     * Initial backoff duration in milliseconds (1000ms = 1 second)
     */
    private int initialBackoffMs = 1000;

    /**
     * Backoff multiplier for exponential backoff (2.0 = double each retry)
     */
    private double backoffMultiplier = 2.0;

    /**
     * Maximum backoff duration in milliseconds (31000ms = 31 seconds)
     */
    private int maxBackoffMs = 31000;

    /**
     * Maximum number of retry attempts before sending to DLQ (5 attempts)
     */
    private int maxAttempts = 5;

    /**
     * Enable/disable DLQ (Dead Letter Queue) for non-retryable failures
     */
    private boolean dlqEnabled = true;

    /**
     * DLQ message retention in days (for audit purposes)
     */
    private int dlqRetentionDays = 30;

    /**
     * Suffix appended to topic name to create DLQ topic (default: "-dlq")
     */
    private String dlqTopicSuffix = "-dlq";

    /**
     * Enable/disable metrics collection for retries and DLQ
     */
    private boolean metricsEnabled = true;

    /**
     * Enable/disable detailed audit logging of DLQ messages
     */
    private boolean auditingEnabled = true;

    /**
     * Enable/disable pause/resume consumer when errors occur
     */
    private boolean pauseResumeEnabled = false;

    /**
     * Timeout in milliseconds for pause/resume operations
     */
    private int pauseResumeTimeoutMs = 5000;

    // Getters and setters

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public void setInitialBackoffMs(int initialBackoffMs) {
        this.initialBackoffMs = initialBackoffMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public int getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public void setMaxBackoffMs(int maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public boolean isDlqEnabled() {
        return dlqEnabled;
    }

    public void setDlqEnabled(boolean dlqEnabled) {
        this.dlqEnabled = dlqEnabled;
    }

    public int getDlqRetentionDays() {
        return dlqRetentionDays;
    }

    public void setDlqRetentionDays(int dlqRetentionDays) {
        this.dlqRetentionDays = dlqRetentionDays;
    }

    public String getDlqTopicSuffix() {
        return dlqTopicSuffix;
    }

    public void setDlqTopicSuffix(String dlqTopicSuffix) {
        this.dlqTopicSuffix = dlqTopicSuffix;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    public boolean isAuditingEnabled() {
        return auditingEnabled;
    }

    public void setAuditingEnabled(boolean auditingEnabled) {
        this.auditingEnabled = auditingEnabled;
    }

    public boolean isPauseResumeEnabled() {
        return pauseResumeEnabled;
    }

    public void setPauseResumeEnabled(boolean pauseResumeEnabled) {
        this.pauseResumeEnabled = pauseResumeEnabled;
    }

    public int getPauseResumeTimeoutMs() {
        return pauseResumeTimeoutMs;
    }

    public void setPauseResumeTimeoutMs(int pauseResumeTimeoutMs) {
        this.pauseResumeTimeoutMs = pauseResumeTimeoutMs;
    }
}
