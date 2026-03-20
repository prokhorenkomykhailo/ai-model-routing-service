package com.lucid.automation.airouting.config;

import com.lucid.automation.common.dto.topic.TopicClusterRefinedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TopicMetadataKafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataKafkaConfig.class);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TopicClusterRefinedEvent> refinedTopicListenerContainerFactory(
        KafkaProperties kafkaProperties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TopicClusterRefinedEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(refinedTopicConsumerFactory(kafkaProperties));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        logger.info("✅ [KAFKA-CONFIG] Refined topic listener factory initialized");
        return factory;
    }

    private ConsumerFactory<String, TopicClusterRefinedEvent> refinedTopicConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.lucid.automation.common.dto.*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TopicClusterRefinedEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }
}

