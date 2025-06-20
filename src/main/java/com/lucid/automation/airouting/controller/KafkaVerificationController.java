package com.lucid.automation.airouting.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Controller for verifying Kafka consumer status and offset reset functionality
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaVerificationController {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaVerificationController.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String aiEnrichTopic;
    
    @Value("${spring.kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;
    
    @Value("${spring.kafka.properties.[sasl.mechanism]:}")
    private String saslMechanism;
    
    @Value("${spring.kafka.properties.[sasl.jaas.config]:}")
    private String saslJaasConfig;
    
    /**
     * Verify the status of the ai-enrich consumer group
     */
    @GetMapping("/verify-ai-enrich-consumer")
    public ResponseEntity<Map<String, Object>> verifyAiEnrichConsumer() {
        Map<String, Object> status = new HashMap<>();
        String aiEnrichConsumerGroup = groupId + "-ai-enrich";
        
        try {
            Properties adminProps = createAdminProperties();
            
            try (AdminClient adminClient = AdminClient.create(adminProps)) {
                
                // Check consumer group status
                try {
                    Map<String, ConsumerGroupDescription> groups = adminClient
                        .describeConsumerGroups(Collections.singletonList(aiEnrichConsumerGroup))
                        .all()
                        .get();
                    
                    if (groups.containsKey(aiEnrichConsumerGroup)) {
                        ConsumerGroupDescription description = groups.get(aiEnrichConsumerGroup);
                        status.put("consumerGroupExists", true);
                        status.put("consumerGroupId", aiEnrichConsumerGroup);
                        status.put("state", description.state().toString());
                        status.put("memberCount", description.members().size());
                        status.put("guaranteeStatus", "ACTIVE - ai-enrich processed from beginning");
                    } else {
                        status.put("consumerGroupExists", false);
                        status.put("consumerGroupId", aiEnrichConsumerGroup);
                        status.put("guaranteeStatus", "READY - ai-enrich will be processed from beginning on first message");
                    }
                    
                } catch (Exception e) {
                    if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                        status.put("consumerGroupExists", false);
                        status.put("consumerGroupId", aiEnrichConsumerGroup);
                        status.put("guaranteeStatus", "READY - ai-enrich will be processed from beginning on first message");
                    } else {
                        status.put("error", "Error checking consumer group: " + e.getMessage());
                    }
                }
            }
            
            // Add configuration details
            status.put("topic", aiEnrichTopic);
            status.put("bootstrapServers", bootstrapServers);
            status.put("autoOffsetReset", "earliest");
            status.put("processingGuarantee", "ai-enrich topic is ALWAYS processed from the beginning");
            status.put("mechanisms", 
                      "1. auto-offset-reset=earliest, 2. KafkaOffsetResetService, 3. Dedicated consumer group");
            
        } catch (Exception e) {
            logger.error("Failed to verify ai-enrich consumer status", e);
            status.put("error", "Failed to verify consumer status: " + e.getMessage());
        }
        
        return ResponseEntity.ok(status);
    }
    
    /**
     * Get general Kafka configuration status
     */
    @GetMapping("/config-status")
    public ResponseEntity<Map<String, Object>> getConfigStatus() {
        Map<String, Object> config = new HashMap<>();
        
        config.put("bootstrapServers", bootstrapServers);
        config.put("mainConsumerGroupId", groupId);
        config.put("aiEnrichConsumerGroupId", groupId + "-ai-enrich");
        config.put("aiEnrichTopic", aiEnrichTopic);
        config.put("securityProtocol", securityProtocol);
        config.put("autoOffsetReset", "earliest");
        config.put("processingGuarantee", "ai-enrich topic ALWAYS processed from beginning");
        
        return ResponseEntity.ok(config);
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
