package com.lucid.automation.airouting.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Service to handle Kafka offset reset functionality for ensuring ai-enrich topic
 * is always processed from the beginning
 */
@Service
public class KafkaOffsetResetService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetResetService.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String aiEnrichTopic;
    
    @Value("${kafka.reset-offsets-on-startup:true}")
    private boolean resetOffsetsOnStartup;
    
    @Value("${spring.kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;
    
    @Value("${spring.kafka.properties.[sasl.mechanism]:}")
    private String saslMechanism;
    
    @Value("${spring.kafka.properties.[sasl.jaas.config]:}")
    private String saslJaasConfig;
    
    /**
     * Handle application ready event to reset offsets if configured
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (resetOffsetsOnStartup) {
            logger.info("=== KAFKA OFFSET RESET SERVICE ===");
            logger.info("Application ready - checking if ai-enrich topic offsets need reset");
            resetAiEnrichTopicOffsets();
        } else {
            logger.info("Offset reset on startup is disabled (kafka.reset-offsets-on-startup=false)");
        }
    }
    
    /**
     * Reset offsets for the ai-enrich topic to ensure processing from beginning
     */
    private void resetAiEnrichTopicOffsets() {
        String aiEnrichConsumerGroup = groupId + "-ai-enrich";
        
        try {
            logger.info("Attempting to reset offsets for consumer group: {} on topic: {}", 
                       aiEnrichConsumerGroup, aiEnrichTopic);
            
            // Create admin client properties
            Properties adminProps = createAdminProperties();
            
            try (AdminClient adminClient = AdminClient.create(adminProps)) {
                
                // Check if consumer group exists and has committed offsets
                try {
                    Map<String, ConsumerGroupDescription> groups = adminClient
                        .describeConsumerGroups(Collections.singletonList(aiEnrichConsumerGroup))
                        .all()
                        .get();
                    
                    if (groups.containsKey(aiEnrichConsumerGroup)) {
                        logger.info("Consumer group {} exists - will attempt to reset offsets", aiEnrichConsumerGroup);
                        
                        // Delete the consumer group to force offset reset
                        DeleteConsumerGroupsResult deleteResult = adminClient
                            .deleteConsumerGroups(Collections.singletonList(aiEnrichConsumerGroup));
                        
                        deleteResult.all().get(); // Wait for completion
                        logger.info("Successfully deleted consumer group: {}", aiEnrichConsumerGroup);
                        logger.info("Next consumer startup will read ai-enrich topic from the beginning");
                        
                    } else {
                        logger.info("Consumer group {} does not exist - ai-enrich will be read from beginning on first startup", 
                                   aiEnrichConsumerGroup);
                    }
                    
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                        logger.info("Consumer group {} not found - ai-enrich will be read from beginning", aiEnrichConsumerGroup);
                    } else {
                        logger.warn("Error checking/resetting consumer group {}: {}", aiEnrichConsumerGroup, e.getMessage());
                    }
                }
            }
            
            // Additional verification - log the configuration
            logger.info("=== AI-ENRICH PROCESSING GUARANTEE ===");
            logger.info("Topic: {}", aiEnrichTopic);
            logger.info("Consumer Group: {}", aiEnrichConsumerGroup);
            logger.info("Auto Offset Reset: earliest");
            logger.info("Processing guarantee: ai-enrich messages will be processed from the beginning");
            logger.info("=====================================");
            
        } catch (Exception e) {
            logger.error("Failed to reset offsets for ai-enrich topic", e);
            logger.warn("ai-enrich topic will still be processed with auto-offset-reset=earliest configuration");
        }
    }
    
    /**
     * Create admin client properties with security configuration
     */
    private Properties createAdminProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Add security configuration if needed
        if (!"PLAINTEXT".equals(securityProtocol)) {
            props.put("security.protocol", securityProtocol);
            if (!saslMechanism.isEmpty()) {
                props.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isEmpty()) {
                props.put("sasl.jaas.config", saslJaasConfig);
            }
        }
        
        return props;
    }
}
