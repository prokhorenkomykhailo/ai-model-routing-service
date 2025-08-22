package com.lucid.automation.airouting.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for Kafka topics used by the AI routing service
 */
@Data
@Component
@ConfigurationProperties(prefix = "kafka.topics")
public class KafkaTopicProperties {

    /**
     * Topic name for AI categorization
     */
    private String aiCategorize = "ai-categorize";

    /**
     * Topic name for AI summarization
     */
    private String aiSummarize = "ai-summarize";

    /**
     * Topic name for AI enrichment
     */
    private String aiEnrich = "ai-enrich";

    /**
     * Topic name for ingestion progress updates
     */
    private String ingestionProgress = "ingestion-progress";

    // Add more topic configurations as needed in the future
}
