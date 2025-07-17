package com.lucid.automation.airouting.config;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.model.message.AIMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka error handling configuration that properly handles deserialization errors
 */
@Configuration
public class KafkaErrorHandlingConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${spring.kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;
    
    @Value("${spring.kafka.properties.[sasl.mechanism]:}")
    private String saslMechanism;
    
    @Value("${spring.kafka.properties.[sasl.jaas.config]:}")
    private String saslJaasConfig;
    
    @Bean
    public ConsumerFactory<String, IngestionEventDTO> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Add security configuration if needed
        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (!saslMechanism.isEmpty()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isEmpty()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }
        
        // Configure ErrorHandlingDeserializer properly
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // JsonDeserializer specific configuration
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put("spring.json.use.type.headers", false);
        configProps.put("spring.json.value.default.type", IngestionEventDTO.class.getName());
        
        // Additional JSON configuration for better error handling
        configProps.put(JsonDeserializer.TYPE_MAPPINGS, 
            "com.lucid.automation.common.dto.messaging.IngestionEventDTO:" + IngestionEventDTO.class.getName());
        configProps.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, true);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConsumerFactory<String, AIMessage> aiMessageConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-ai-enrich");
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // CONFIGURATION: Start from latest offset - Skip historical messages
        // This ensures the consumer doesn't get kicked out of the group during processing
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000); // 10 seconds
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000); // 3 seconds
        
        logger.info("=== AI-ENRICH CONSUMER CONFIG ===");
        logger.info("Using group ID: {}", groupId + "-ai-enrich");
        logger.info("AUTO_OFFSET_RESET: latest");
        logger.info("This ensures ai-enrich topic is processed from the latest offset when no committed offsets exist");
        logger.info("================================");
        
        // Add security configuration if needed
        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (!saslMechanism.isEmpty()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isEmpty()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }
        
        // Configure ErrorHandlingDeserializer properly for AIMessage
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // JsonDeserializer specific configuration for AIMessage
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put("spring.json.use.type.headers", false);
        configProps.put("spring.json.fail.on.unknown.properties", false);
        configProps.put("spring.json.value.default.type", AIMessage.class.getName());
        
        // Type mappings for AIMessage
        configProps.put(JsonDeserializer.TYPE_MAPPINGS, 
            "com.lucid.automation.airouting.model.message.AIMessage:" + AIMessage.class.getName());
        configProps.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, true);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
    
    @Bean
    public ConsumerFactory<String, Object> genericObjectConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-pre-ai-responses");
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Add security configuration if needed
        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (!saslMechanism.isEmpty()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isEmpty()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }
        
        // Configure ErrorHandlingDeserializer properly for generic objects
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // JsonDeserializer specific configuration for generic objects
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put("spring.json.use.type.headers", false);
        configProps.put("spring.json.fail.on.unknown.properties", false);
        // Set default type to LinkedHashMap for generic object deserialization
        configProps.put("spring.json.value.default.type", "java.util.LinkedHashMap");
        configProps.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, true);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {
                logger.error("=== KAFKA ERROR HANDLER TRIGGERED ===");
                logger.error("Topic: {}, partition: {}, offset: {}, key: {}", 
                    record.topic(), record.partition(), record.offset(), record.key());
                logger.error("Exception type: {}", exception.getClass().getSimpleName());
                logger.error("Exception message: {}", exception.getMessage());
                
                // Log the full stack trace for debugging
                logger.error("Full exception details:", exception);
                
                // Log the raw value to help debug deserialization issues
                if (record.value() != null) {
                    String valueStr = record.value().toString();
                    if (valueStr.length() > 500) {
                        logger.warn("Raw message value that failed (first 500 chars): {}", valueStr.substring(0, 500));
                    } else {
                        logger.warn("Raw message value that failed: {}", valueStr);
                    }
                } else {
                    logger.warn("Record value is null");
                }
                
                logger.info("Message skipped, continuing with next message...");
                logger.error("=== END KAFKA ERROR HANDLER ===");
            },
            new FixedBackOff(0L, 0L) // No retries, just skip
        );
        
        // Configure the error handler to handle deserialization exceptions
        errorHandler.addNotRetryableExceptions(
            org.apache.kafka.common.errors.SerializationException.class,
            org.apache.kafka.common.errors.RecordDeserializationException.class,
            com.fasterxml.jackson.core.JsonParseException.class,
            com.fasterxml.jackson.databind.JsonMappingException.class
        );
        
        return errorHandler;
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, IngestionEventDTO> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, IngestionEventDTO> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        
        // Configure manual acknowledgment mode
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AIMessage> aiMessageListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AIMessage> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(aiMessageConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        
        // Configure manual acknowledgment mode
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> genericObjectListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(genericObjectConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        
        // Configure manual acknowledgment mode
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
}
