package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import com.lucid.automation.airouting.service.TopicEmbeddingStoreService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TopicEmbeddingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TopicEmbeddingConsumer.class);

    private final TopicEmbeddingStoreService service;

    public TopicEmbeddingConsumer(TopicEmbeddingStoreService service) {
        this.service = service;
    }

    @KafkaListener(
        topics = "${topic.embedding.metadata-topic:ai-topic-metadata}",
        groupId = "${topic.embedding.consumer-group:ai-service-group-topic-embeddings}",
        containerFactory = "topicMetadataListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TopicMetadataEvent> record, Acknowledgment ack) {
        TopicMetadataEvent event = record.value();
        if (event == null) {
            ack.acknowledge();
            return;
        }
        logger.info("Step4 consuming metadata batch {} workspace {}", event.getBatchId(), event.getWorkspaceId());
        service.upsertFromMetadataEvent(event);
        ack.acknowledge();
    }
}

