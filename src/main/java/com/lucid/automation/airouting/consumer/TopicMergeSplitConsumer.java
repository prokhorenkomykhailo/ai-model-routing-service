package com.lucid.automation.airouting.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.topic.TopicClusterDraftEvent;
import com.lucid.automation.common.dto.topic.TopicClusterRefinedEvent;
import com.lucid.automation.common.dto.topic.TopicClusterRefined;
import com.lucid.automation.airouting.producer.TopicRefinedProducer;
import com.lucid.automation.airouting.service.TopicMergeSplitService;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka listener for Step 2: consumes ai-topic-drafts and emits refined events.
 */
@Component
public class TopicMergeSplitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TopicMergeSplitConsumer.class);

    private final TopicMergeSplitService mergeSplitService;
    private final TopicRefinedProducer producer;
    private final ObjectMapper objectMapper;

    public TopicMergeSplitConsumer(TopicMergeSplitService mergeSplitService,
                                   TopicRefinedProducer producer,
                                   ObjectMapper objectMapper) {
        this.mergeSplitService = mergeSplitService;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${topic.merge-split.draft-topic:ai-topic-drafts}",
        groupId = "ai-service-group-merge-split",
        containerFactory = "topicClusterDraftKafkaListenerContainerFactory")
    public void handleDraft(@Payload TopicClusterDraftEvent event,
                            ConsumerRecord<String, TopicClusterDraftEvent> record,
                            Acknowledgment acknowledgment) {
        try {
            logger.info("Step2 consuming draft batch {} workspace {}", event.getBatchId(), event.getWorkspaceId());
            List<TopicClusterRefined> refinedClusters =
                mergeSplitService.refine(event.getClusters());

            TopicClusterRefinedEvent refinedEvent = new TopicClusterRefinedEvent();
            refinedEvent.setEventId(UUID.randomUUID().toString());
            refinedEvent.setWorkspaceId(event.getWorkspaceId());
            refinedEvent.setBatchId(event.getBatchId());
            refinedEvent.setProviderId(event.getProviderId());
            refinedEvent.setPromptVersion(event.getPromptVersion());
            refinedEvent.setCreatedAt(Instant.now());
            refinedEvent.setClusterCount(refinedClusters.size());
            refinedEvent.setMetadata(event.getMetadata());
            refinedEvent.setClusters(refinedClusters);

            producer.publishRefined(refinedEvent);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            logger.error("Step2 failed for record {}: {}", record.offset(), e.getMessage(), e);
            try {
                producer.publishDlq(objectMapper.writeValueAsString(event), e.getMessage(), event.getWorkspaceId(), event.getBatchId());
            } catch (Exception ignored) {
                // best effort
            }
            acknowledgment.acknowledge();
        }
    }
}
