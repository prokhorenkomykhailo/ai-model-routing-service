package com.lucid.automation.airouting.producer;

import com.lucid.automation.airouting.config.TopicMergeSplitProperties;
import com.lucid.automation.airouting.dto.topic.TopicDraftDlqEvent;
import com.lucid.automation.common.dto.topic.TopicClusterRefinedEvent;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TopicRefinedProducer {

    private static final Logger logger = LoggerFactory.getLogger(TopicRefinedProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicMergeSplitProperties properties;

    public TopicRefinedProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                TopicMergeSplitProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publishRefined(TopicClusterRefinedEvent event) {
        kafkaTemplate.send(properties.getRefinedTopic(), event.getWorkspaceId(), event);
        logger.info("📤 Published refined topic event {} to {}", event.getEventId(), properties.getRefinedTopic());
    }

    public void publishDlq(String payloadJson, String reason, String workspaceId, String batchId) {
        TopicDraftDlqEvent dlqEvent = new TopicDraftDlqEvent();
        dlqEvent.setEventId(UUID.randomUUID().toString());
        dlqEvent.setCreatedAt(Instant.now());
        dlqEvent.setWorkspaceId(workspaceId);
        dlqEvent.setBatchId(batchId);
        dlqEvent.setReason(reason);
        dlqEvent.setPayload(payloadJson);

        kafkaTemplate.send(properties.getDlqTopic(), workspaceId, dlqEvent);
        logger.warn("🚨 Sent draft payload to DLQ {} because {}", properties.getDlqTopic(), reason);
    }
}
