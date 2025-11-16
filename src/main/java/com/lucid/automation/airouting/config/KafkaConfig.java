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

import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka error handling configuration that properly handles deserialization errors
 */
@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

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

        // BEST PRACTICES: Optimized for AI processing workloads
        // Max Poll Interval: Time consumer can spend processing before being kicked out
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1800000);  // 30 minutes (AI enrichment takes ~20 min)

        // Session Timeout: How long consumer can be silent before being considered dead
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);    // 5 minutes (reasonable for stability)

        // Heartbeat Interval: Must be < session_timeout/3 (Kafka requirement)
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 90000);  // 1.5 minutes (300s/3 = 100s max)

        // OPTIMIZATION: Reduce batch size for faster processing cycles
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);          // Process 10 records at a time
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);            // Don't wait for data accumulation
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);        // Max 500ms wait for data

        logger.info("=== AI-ENRICH CONSUMER CONFIG (BEST PRACTICES) ===");
        logger.info("Using group ID: {}", groupId + "-ai-enrich");
        logger.info("AUTO_OFFSET_RESET: latest");
        logger.info("SESSION_TIMEOUT: 5m (balanced stability vs responsiveness)");
        logger.info("MAX_POLL_INTERVAL: 15m (sufficient for AI processing)");
        logger.info("HEARTBEAT_INTERVAL: 1.5m (< session_timeout/3)");
        logger.info("MAX_POLL_RECORDS: 10 (small batches for consistent processing)");
        logger.info("Configuration optimized for AI workload characteristics");
        logger.info("====================================================");

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
            SerializationException.class,
            RecordDeserializationException.class,
            JsonParseException.class,
            JsonMappingException.class
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AIMessage> aiMessageListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AIMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(aiMessageConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());

        // Configure manual acknowledgment mode
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
    @Bean
    public ConsumerFactory<String, Object> genericObjectConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-generic-object");
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // CRITICAL: Large message support for AI enrichment responses (up to 10 MB)
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760); // 10 MB per partition
        configProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 10485760); // 10 MB total fetch size

        // Timeout settings for large message processing
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1800000); // 30 minutes (AI enrichment takes ~20 min)
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000); // 5 minutes
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 90000); // 1.5 minutes
        configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000); // 40 seconds

        // Security configuration
        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (!saslMechanism.isEmpty()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isEmpty()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }

        // Deserializer configuration
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // JsonDeserializer configuration for generic Object
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put("spring.json.use.type.headers", false);
        configProps.put("spring.json.value.default.type", "java.lang.Object");
        configProps.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, true);

        return new DefaultKafkaConsumerFactory<>(configProps);
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
