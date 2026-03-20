package com.lucid.automation.airouting.producer;

import com.lucid.automation.airouting.config.TopicClusteringProperties;
import com.lucid.automation.common.dto.topic.TopicClusterDraftEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TopicDraftProducer {

    private static final Logger logger = LoggerFactory.getLogger(TopicDraftProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicClusteringProperties properties;

    public TopicDraftProducer(KafkaTemplate<String, Object> kafkaTemplate,
                              TopicClusteringProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publishDraft(TopicClusterDraftEvent event) {
        if (event == null) {
            return;
        }
        kafkaTemplate.send(properties.getDraftTopic(), event.getBatchId(), event);
        logger.info("📤 Published topic draft event {} to topic {}", event.getEventId(), properties.getDraftTopic());
    }
}
