package com.lucid.automation.airouting.config;

import com.lucid.automation.airouting.dto.topic.TopicClusterDraftEvent;
import java.util.HashMap;
import java.util.Map;
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

@Configuration
public class TopicClusterDraftKafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(TopicClusterDraftKafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.[sasl.mechanism]:}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.[sasl.jaas.config]:}")
    private String saslJaasConfig;

    @Bean
    public ConsumerFactory<String, TopicClusterDraftEvent> topicClusterDraftConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ai-service-group-merge-split");
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        if (!"PLAINTEXT".equals(securityProtocol)) {
            configProps.put("security.protocol", securityProtocol);
            if (!saslMechanism.isBlank()) {
                configProps.put("sasl.mechanism", saslMechanism);
            }
            if (!saslJaasConfig.isBlank()) {
                configProps.put("sasl.jaas.config", saslJaasConfig);
            }
        }

        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.lucid.automation.airouting.dto.topic");
        configProps.put("spring.json.use.type.headers", false);
        configProps.put("spring.json.fail.on.unknown.properties", false);
        configProps.put("spring.json.value.default.type", TopicClusterDraftEvent.class.getName());
        configProps.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, true);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean(name = "topicClusterDraftKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, TopicClusterDraftEvent> topicClusterDraftKafkaListenerContainerFactory(
        ConsumerFactory<String, TopicClusterDraftEvent> topicClusterDraftConsumerFactory,
        DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TopicClusterDraftEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(topicClusterDraftConsumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

        logger.info("✅ [KAFKA-CONFIG] Topic draft listener factory initialized");
        return factory;
    }
}

