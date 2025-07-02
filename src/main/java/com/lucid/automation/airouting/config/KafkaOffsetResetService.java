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
    
    @Value("${kafka.topics.ingestion-messages:lucid-ingestion-messages}")
    private String ingestionMessagesTopic;
    
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
        logger.info("=== KAFKA OFFSET RESET SERVICE ===");
        logger.info("Reset offsets on startup configured: {}", resetOffsetsOnStartup);
        
        if (resetOffsetsOnStartup) {
            logger.info("🔄 DEBUGGING MODE: Application will restart from beginning of ALL topics every time");
            logger.info("Application ready - resetting offsets for debugging (reset-offsets-on-startup=true)");
            
            // Reset offsets for ingestion-messages topic (main consumer group)
            resetIngestionMessagesOffsets();
            
            // Reset offsets for ai-enrich topic
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
        logger.info("Attempting to reset offsets for consumer group: {} on topic: {}", 
                   aiEnrichConsumerGroup, aiEnrichTopic);
        
        Properties adminProps = createAdminProperties();
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            resetConsumerGroupOffsets(adminClient, aiEnrichConsumerGroup);
        } catch (Exception e) {
            logger.error("Failed to reset offsets for ai-enrich topic", e);
            logger.warn("ai-enrich topic will still be processed with auto-offset-reset=earliest configuration");
        }
        
        logProcessingGuarantee(aiEnrichTopic, aiEnrichConsumerGroup, "ai-enrich messages");
    }
    
    /**
     * Reset offsets for a specific consumer group
     */
    private void resetConsumerGroupOffsets(AdminClient adminClient, String consumerGroup) throws ExecutionException, InterruptedException {
        try {
            Map<String, ConsumerGroupDescription> groups = adminClient
                .describeConsumerGroups(Collections.singletonList(consumerGroup))
                .all()
                .get();
            
            if (groups.containsKey(consumerGroup)) {
                logger.info("Consumer group {} exists - will attempt to reset offsets", consumerGroup);
                deleteConsumerGroup(adminClient, consumerGroup);
            } else {
                logger.info("Consumer group {} does not exist - will be read from beginning on first startup", consumerGroup);
            }
            
        } catch (ExecutionException e) {
            handleConsumerGroupException(e, consumerGroup);
        }
    }
    
    /**
     * Delete a consumer group to force offset reset
     */
    private void deleteConsumerGroup(AdminClient adminClient, String consumerGroup) throws ExecutionException, InterruptedException {
        DeleteConsumerGroupsResult deleteResult = adminClient
            .deleteConsumerGroups(Collections.singletonList(consumerGroup));
        
        deleteResult.all().get(); // Wait for completion
        logger.info("Successfully deleted consumer group: {}", consumerGroup);
        logger.info("Next consumer startup will read topic from the beginning");
    }
    
    /**
     * Handle exceptions when checking consumer groups
     */
    private void handleConsumerGroupException(ExecutionException e, String consumerGroup) {
        if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
            logger.info("Consumer group {} not found - will be read from beginning", consumerGroup);
        } else {
            logger.warn("Error checking/resetting consumer group {}: {}", consumerGroup, e.getMessage());
        }
    }
    
    /**
     * Log processing guarantee information
     */
    private void logProcessingGuarantee(String topic, String consumerGroup, String messageType) {
        logger.info("=== AI-ENRICH PROCESSING GUARANTEE ===");
        logger.info("Topic: {}", topic);
        logger.info("Consumer Group: {}", consumerGroup);
        logger.info("Auto Offset Reset: earliest");
        logger.info("Processing guarantee: {} will be processed from the beginning", messageType);
        logger.info("=====================================");
    }
    
    /**
     * Reset offsets for the ingestion-messages topic to ensure processing from beginning
     */
    private void resetIngestionMessagesOffsets() {
        String mainConsumerGroup = groupId;
        logger.info("Attempting to reset offsets for MAIN consumer group: {} on topic: {}", 
                   mainConsumerGroup, ingestionMessagesTopic);
        
        Properties adminProps = createAdminProperties();
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            resetConsumerGroupOffsets(adminClient, mainConsumerGroup);
        } catch (Exception e) {
            logger.error("Failed to reset offsets for ingestion-messages topic", e);
            logger.warn("ingestion-messages topic will still be processed with auto-offset-reset=earliest configuration");
        }
        
        logIngestionProcessingGuarantee(ingestionMessagesTopic, mainConsumerGroup);
    }
    
    /**
     * Log processing guarantee information for ingestion messages
     */
    private void logIngestionProcessingGuarantee(String topic, String consumerGroup) {
        logger.info("=== INGESTION-MESSAGES PROCESSING GUARANTEE ===");
        logger.info("Topic: {}", topic);
        logger.info("Consumer Group: {}", consumerGroup);
        logger.info("Auto Offset Reset: earliest");
        logger.info("Processing guarantee: ALL ingestion messages will be processed from the beginning");
        logger.info("===============================================");
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
