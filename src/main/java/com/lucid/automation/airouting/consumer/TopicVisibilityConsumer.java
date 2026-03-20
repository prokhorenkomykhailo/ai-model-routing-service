package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.common.dto.topic.TopicMetadataEvent;
import com.lucid.automation.common.dto.topic.TopicVisibilityEvent;
import com.lucid.automation.airouting.producer.TopicVisibilityProducer;
import com.lucid.automation.airouting.service.TopicVisibilityBatchReportService;
import com.lucid.automation.airouting.service.TopicVisibilityCacheService;
import com.lucid.automation.airouting.service.TopicVisibilityService;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TopicVisibilityConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TopicVisibilityConsumer.class);

    private final TopicVisibilityService service;
    private final TopicVisibilityProducer producer;
    private final TopicVisibilityProperties properties;
    private final TopicVisibilityCacheService cacheService;
    private final TopicVisibilityBatchReportService reportService;

    public TopicVisibilityConsumer(TopicVisibilityService service,
                                  TopicVisibilityProducer producer,
                                  TopicVisibilityProperties properties,
                                  TopicVisibilityCacheService cacheService,
                                  TopicVisibilityBatchReportService reportService) {
        this.service = service;
        this.producer = producer;
        this.properties = properties;
        this.cacheService = cacheService;
        this.reportService = reportService;
    }

    @KafkaListener(
        topics = "${topic.visibility.metadata-topic:ai-topic-metadata}",
        groupId = "${topic.visibility.consumer-group:ai-service-group-topic-visibility}",
        containerFactory = "topicVisibilityMetadataListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TopicMetadataEvent> record, Acknowledgment ack) {
        TopicMetadataEvent event = record.value();
        if (event == null) {
            ack.acknowledge();
            return;
        }
        try {
            TopicVisibilityService.VisibilityEvaluation evaluation = service.evaluate(event);
            List<TopicVisibilityService.UserTopicVisibility> matches = evaluation.matches();
            Set<String> touchedUsers = new LinkedHashSet<>();
            for (TopicVisibilityService.UserTopicVisibility m : matches) {
                cacheService.update(m.workspaceId(), m.userId(), m.topicId(), m.batchId(), m.channel());
                touchedUsers.add(m.userId());
            }
            int emitted = 0;
            for (String userId : touchedUsers) {
                TopicVisibilityEvent snapshot = cacheService.snapshotEvent(event.getWorkspaceId(), userId);
                if (snapshot != null && snapshot.getTopicIds() != null && !snapshot.getTopicIds().isEmpty()) {
                    snapshot.setTenantId(event.getTenantId());
                    snapshot.setTenantSchema(event.getTenantSchema());
                    producer.publish(snapshot);
                    emitted++;
                }
            }
            if (matches.isEmpty()) {
                logger.warn("Step5 topicId={} workspace={} skipReason={} candidateOwners={}",
                    event.getTopicId(), event.getWorkspaceId(), evaluation.skipReason(), evaluation.candidateOwnerKeys());
            } else {
                List<String> usersForLog = touchedUsers.stream()
                    .limit(Math.max(0, properties.getMaxUsersToLogPerTopic()))
                    .toList();
                logger.info("Step5 topicId={} workspace={} visibleUsers={} users={} emittedSnapshots={}",
                    event.getTopicId(), event.getWorkspaceId(), touchedUsers.size(), usersForLog, emitted);
            }

            reportService.update(event, evaluation);
            reportService.writeSnapshot(event.getWorkspaceId(), event.getBatchId());
        } catch (Exception e) {
            producer.sendDlq(event.getWorkspaceId(), event.getBatchId(), "step5_processing_error", event);
            logger.error("Step5 failed for topicId={} batch={}: {}", event.getTopicId(), event.getBatchId(), e.getMessage());
        } finally {
            ack.acknowledge();
        }
    }
}
