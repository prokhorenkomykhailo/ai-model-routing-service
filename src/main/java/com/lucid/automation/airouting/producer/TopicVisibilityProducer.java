package com.lucid.automation.airouting.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.airouting.dto.topic.TopicVisibilityDlqEvent;
import com.lucid.automation.airouting.dto.topic.TopicVisibilityEvent;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TopicVisibilityProducer {

    private static final Logger logger = LoggerFactory.getLogger(TopicVisibilityProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicVisibilityProperties properties;
    private final ObjectMapper objectMapper;

    public TopicVisibilityProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                  TopicVisibilityProperties properties,
                                  ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public void publish(TopicVisibilityEvent event) {
        if (event == null) {
            return;
        }
        kafkaTemplate.send(properties.getVisibilityTopic(), event.getUserId(), event);
        logger.info("📤 Published topic visibility event {} userId={} workspaceId={} topics={}",
            event.getEventId(), event.getUserId(), event.getWorkspaceId(),
            event.getTopicIds() != null ? event.getTopicIds().size() : 0);
    }

    public void sendDlq(String workspaceId, String batchId, String reason, Object payload) {
        TopicVisibilityDlqEvent dlq = new TopicVisibilityDlqEvent();
        dlq.setEventId(UUID.randomUUID().toString());
        dlq.setCreatedAt(Instant.now());
        dlq.setWorkspaceId(workspaceId);
        dlq.setBatchId(batchId);
        dlq.setReason(reason);
        try {
            dlq.setPayload(objectMapper.writeValueAsString(payload));
        } catch (Exception e) {
            dlq.setPayload(String.valueOf(payload));
        }
        kafkaTemplate.send(properties.getDlqTopic(), dlq.getEventId(), dlq);
        logger.warn("🚨 Sent visibility payload to DLQ {} because {}", properties.getDlqTopic(), reason);
    }
}

