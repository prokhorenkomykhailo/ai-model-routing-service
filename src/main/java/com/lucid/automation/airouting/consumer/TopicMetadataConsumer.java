package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.config.TopicMetadataProperties;
import com.lucid.automation.airouting.dto.topic.TopicClusterRefinedEvent;
import com.lucid.automation.airouting.service.TopicMetadataService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TopicMetadataConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataConsumer.class);
    private final TopicMetadataService service;
    private final TopicMetadataProperties properties;

    public TopicMetadataConsumer(TopicMetadataService service, TopicMetadataProperties properties) {
        this.service = service;
        this.properties = properties;
    }

    @KafkaListener(
        topics = "${topic.metadata.refined-topic:ai-topic-refined}",
        groupId = "${topic.metadata.consumer-group:ai-service-group-topic-metadata}",
        containerFactory = "refinedTopicListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TopicClusterRefinedEvent> record, Acknowledgment ack) {
        TopicClusterRefinedEvent event = record.value();
        if (event == null) {
            ack.acknowledge();
            return;
        }
        logger.info("Step3 consuming refined batch {} workspace {}", event.getBatchId(), event.getWorkspaceId());
        service.processRefinedEvent(event);
        ack.acknowledge();
    }
}

