package com.lucid.automation.airouting.config;

import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class to enable pipeline configuration properties
 */
@Configuration
@EnableConfigurationProperties(PipelineConfiguration.class)
public class PipelineAutoConfiguration {
    // Configuration is handled by @EnableConfigurationProperties
}
