package com.lucid.automation.airouting.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for Anonymization Service integration.
 * Controls PII masking/unmasking for secure LLM processing.
 *
 * @author vudu
 */
@Data
@Component
@ConfigurationProperties(prefix = "anonymization.service")
public class AnonymizationProperties {

    /**
     * Anonymization service base URL
     * Default: http://anonymization-service:8089 (Docker container name)
     */
    private String url = "http://anonymization-service:8089";

    /**
     * Enable/disable anonymization integration
     * If false, PII will NOT be masked (UNSAFE - use only for testing)
     * Default: true
     */
    private Boolean enabled = true;

    /**
     * Request timeout in milliseconds
     * Default: 5000ms (5 seconds)
     */
    private Long timeout = 5000L;

    /**
     * Retry configuration for anonymization calls
     */
    private Retry retry = new Retry();

    @Data
    public static class Retry {
        /**
         * Maximum number of retry attempts
         * Default: 3
         */
        private Integer maxAttempts = 3;

        /**
         * Backoff duration between retries in milliseconds
         * Default: 2000ms (2 seconds)
         */
        private Long backoff = 2000L;
    }
}
