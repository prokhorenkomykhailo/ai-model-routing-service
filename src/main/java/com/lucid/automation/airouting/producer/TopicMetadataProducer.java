package com.lucid.automation.airouting.producer;

import com.lucid.automation.airouting.config.TopicMetadataProperties;
import com.lucid.automation.airouting.dto.topic.TopicMetadataDlqEvent;
import com.lucid.automation.common.dto.topic.TopicMetadataEvent;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TopicMetadataProducer {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicMetadataProperties properties;

    public TopicMetadataProducer(KafkaTemplate<String, Object> kafkaTemplate, TopicMetadataProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publish(TopicMetadataEvent event) {
        kafkaTemplate.send(properties.getMetadataTopic(), event.getWorkspaceId(), event);
        logger.info("📤 Published topic metadata event {} to {}", event.getEventId(), properties.getMetadataTopic());
    }

    public void publishDlq(String payloadJson, String reason, String workspaceId, String batchId) {
        TopicMetadataDlqEvent dlqEvent = new TopicMetadataDlqEvent();
        dlqEvent.setEventId(UUID.randomUUID().toString());
        dlqEvent.setCreatedAt(Instant.now());
        dlqEvent.setWorkspaceId(workspaceId);
        dlqEvent.setBatchId(batchId);
        dlqEvent.setReason(reason);
        dlqEvent.setPayload(payloadJson);

        kafkaTemplate.send(properties.getDlqTopic(), workspaceId, dlqEvent);
        logger.warn("🚨 Sent metadata payload to DLQ {} because {}", properties.getDlqTopic(), reason);
    }
}

