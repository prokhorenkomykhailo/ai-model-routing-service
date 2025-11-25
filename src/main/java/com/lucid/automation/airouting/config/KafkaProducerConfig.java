package com.lucid.automation.airouting.config;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for AI routing service
 *
 * CRITICAL: Must disable Spring Boot auto-configuration for Kafka producer
 * to ensure our 10MB max.request.size is applied.
 */
@Configuration
@Data
public class KafkaProducerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.[sasl.mechanism]:}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.[sasl.jaas.config]:}")
    private String saslJaasConfig;

    /**
     * Producer factory for Kafka messages
     *
     * @Primary annotation ensures this bean takes precedence over Spring Boot's auto-configured producer
     */
    @Bean
    @Primary
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Add security configuration if needed
        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (saslMechanism != null && !saslMechanism.isEmpty()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }

        // Producer performance and reliability settings
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64 MB buffer
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // CRITICAL: Large message support - handles AI enrichment responses up to 10 MB
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760); // 10 MB max request size
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // Compress large payloads

        // Timeout settings for large message transmission
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000); // 60 seconds
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 120 seconds

        logger.info("🚀 [KAFKA-PRODUCER-CONFIG] Initializing custom ProducerFactory");
        logger.info("📊 MAX_REQUEST_SIZE: {} bytes (10 MB)", configProps.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
        logger.info("📦 BUFFER_MEMORY: {} bytes (64 MB)", configProps.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        logger.info("🗜️ COMPRESSION_TYPE: {}", configProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        logger.info("⏱️ REQUEST_TIMEOUT: {}ms", configProps.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        logger.info("⏱️ DELIVERY_TIMEOUT: {}ms", configProps.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Kafka template for sending messages
     *
     * @Primary annotation ensures this bean takes precedence over Spring Boot's auto-configured template
     */
    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        logger.info("✅ [KAFKA-TEMPLATE] KafkaTemplate initialized with custom ProducerFactory (10MB max request size)");
        return template;
    }
}
