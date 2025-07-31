package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.model.message.AIMessage;

import lombok.RequiredArgsConstructor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSelector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Configuration
@RequiredArgsConstructor
public class AIMessageIntegrationConfig {

    // Logger removed (was unused)

    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String aiEnrichTopic;

    private final ConsumerFactory<String, AIMessage> aiMessageListenerContainerFactory;

    @Bean
    public MessageChannel aiEnrichInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow aiEnrichKafkaListenerFlow() {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(aiMessageListenerContainerFactory, aiEnrichTopic)
                        .id("aiEnrichKafkaListenerAdapter"))
                .channel(aiEnrichInputChannel())
                .handle((payload, headers) -> {
                    return payload;
                })
                .handle((payload, headers) -> {
                    Acknowledgment acknowledgment = (Acknowledgment) headers.get("kafka_acknowledgment");
                    if (acknowledgment != null) {
                        acknowledgment.acknowledge();
                    }
                    return null;
                })
                .get();
    }
}
