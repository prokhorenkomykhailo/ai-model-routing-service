package com.lucid.automation.airouting.pipeline;

import com.lucid.automation.airouting.pipeline.config.PipelineConfiguration;
import com.lucid.automation.airouting.pipeline.config.PipelineType;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify pipeline configuration is properly loaded
 */
@SpringBootTest(properties = {
    "lucid.post-processing.pipeline.enabled=true",
    "lucid.post-processing.pipeline.type=STANDARD",
    "lucid.post-processing.pipeline.continueOnFailure=false",
    "lucid.post-processing.pipeline.maxExecutionTimeMs=30000",
    "lucid.post-processing.pipeline.detailedLogging=false",
    "lucid.post-processing.pipeline.metricsEnabled=true"
})
@SpringJUnitConfig
public class PipelineConfigurationIntegrationTest {

    @TestConfiguration
    static class TestConfig {
        @Bean
        public PipelineConfiguration pipelineConfiguration() {
            return new PipelineConfiguration();
        }
    }

    @Autowired
    private PipelineConfiguration pipelineConfiguration;

    @Test
    void testPipelineConfigurationIsLoaded() {
        assertNotNull(pipelineConfiguration);
        assertTrue(pipelineConfiguration.isEnabled());
        assertEquals(PipelineType.STANDARD, pipelineConfiguration.getType());
        assertFalse(pipelineConfiguration.isContinueOnFailure());
        assertEquals(30000, pipelineConfiguration.getMaxExecutionTimeMs());
        assertFalse(pipelineConfiguration.isDetailedLogging());
        assertTrue(pipelineConfiguration.isMetricsEnabled());
    }

    @Test
    void testDefaultValues() {
        PipelineConfiguration defaultConfig = new PipelineConfiguration();
        assertTrue(defaultConfig.isEnabled());
        assertEquals(PipelineType.STANDARD, defaultConfig.getType());
        assertFalse(defaultConfig.isContinueOnFailure());
        assertEquals(30000, defaultConfig.getMaxExecutionTimeMs());
        assertFalse(defaultConfig.isDetailedLogging());
        assertTrue(defaultConfig.isMetricsEnabled());
    }
}
