package com.lucid.automation.airouting.producer;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.EnrichmentJob;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.model.message.SlackParticipantData;
import com.lucid.automation.airouting.service.EnrichmentJobService;
import com.lucid.automation.airouting.service.MessageConverterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import com.lucid.automation.airouting.util.IdUtil;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Producer service for publishing AI processing requests to Kafka
 *
 * @author AI Assistant
 */
@Service
public class AIMessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(AIMessageProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessageConverterService messageConverter;

    @Value("${kafka.topics.ai-categorize:ai-categorize}")
    private String categorizeTopic;

    @Value("${kafka.topics.ai-summarize:ai-summarize}")
    private String summarizeTopic;

    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String enrichTopic;

    private final EnrichmentJobService enrichmentJobService;

    /**
     * Constructor with dependency injection
     *
     * @param kafkaTemplate Kafka template for sending messages
     * @param messageConverter Service for converting messages
     * @param enrichmentJobService Service for managing enrichment jobs
     */
    public AIMessageProducer(KafkaTemplate<String, Object> kafkaTemplate,
                                   MessageConverterService messageConverter,
                                   EnrichmentJobService enrichmentJobService) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageConverter = messageConverter;
        this.enrichmentJobService = enrichmentJobService;
    }


    /**
     * Generic method to publish AI requests
     */
    public String scheduleAiProcessing(AITaskType taskType, String content, String tenantId, String tenantSchema,
                                 String userId, String conversationId, List<SlackMessage> messages,
                                 List<SlackParticipant> participants, Map<String, Object> context,
                                 String preferredProvider, String replyTopic, String parentId) {

        String messageId = IdUtil.generateId("msg-");
        String jobId = IdUtil.generateId("job-");

        try {
            // Convert participants if present
            List<SlackParticipantData> participantData = null;
            if (participants != null && !participants.isEmpty()) {
                participantData = participants.stream()
                        .map(messageConverter::convertToParticipantData)
                        .collect(Collectors.toList());
            }

            // Build AI message using Lombok builder
            AIMessage aiMessage = AIMessage.builder()
                    .messageId(messageId)
                    .taskType(taskType)
                    .content(content != null ? content : "")
                    .tenantId(tenantId)
                    .tenantSchema(tenantSchema)
                    .userId(userId)
                    .conversationId(conversationId)
                    .context(context)
                    .preferredProvider(preferredProvider)
                    .replyTopic(replyTopic)
                    .jobId(jobId)
                    .parentId(parentId)
                    .priority(determinePriority(taskType))
                    .messages(messages)
                    .participants(participantData)
                    .build();

            // Determine topic and publish
            String topic = getTopicForTaskType(taskType);

            kafkaTemplate.send(topic, aiMessage);

            // Create EnrichmentJob after publishing (moved from scheduler)
            EnrichmentJob job = EnrichmentJob.builder()
                    .id(jobId) // Use jobId as job ID
                    .parentId(parentId)
                    .status("PENDING")
                    .type(taskType.name())
                    .result(null)
                    .createdAt(System.currentTimeMillis())
                    .updatedAt(System.currentTimeMillis())
                    .progress(0.0)
                    .durationMs(0L)
                    .startTime(LocalDateTime.now(ZoneOffset.UTC).toString())
                    .endTime(null)
                    .estimatedCompletionTime(null)
                    .estimatedTimeLeft(0L)
                    .userId(userId)
                    .tenantId(tenantId)
                    .tenantSchema(tenantSchema)
                    .build();
            enrichmentJobService.create(job);
            logger.info("Published AI request: messageId={}, taskType={}, tenantId={}, topic={}",
                       messageId, taskType, tenantId, topic);

            return jobId; // Return job ID for tracking

        } catch (Exception e) {
            logger.error("Failed to publish AI request: taskType={}, tenantId={}, error={}",
                        taskType, tenantId, e.getMessage(), e);
            throw new RuntimeException("Failed to publish AI request", e);
        }
    }


        /**
     * Determine message priority based on task type
     */
    private AIMessage.MessagePriority determinePriority(AITaskType taskType) {
        return switch (taskType) {
            case ASSESS_URGENCY -> AIMessage.MessagePriority.HIGH;
            case CATEGORIZE -> AIMessage.MessagePriority.NORMAL;
            case SENTIMENT_ANALYSIS -> AIMessage.MessagePriority.NORMAL;
            case SUMMARIZE -> AIMessage.MessagePriority.LOW;
            case ENRICH_CONVERSATION, ENRICH_MESSAGE -> AIMessage.MessagePriority.LOW;
            case ANALYZE_PARTICIPANT -> AIMessage.MessagePriority.LOW;
            case GENERATE_TOPIC -> AIMessage.MessagePriority.LOW;
            case EXTRACT_ENTITIES -> AIMessage.MessagePriority.NORMAL;
        };
    }

    /**
     * Get Kafka topic based on task type
     */
    private String getTopicForTaskType(AITaskType taskType) {
        return switch (taskType) {
            case CATEGORIZE -> categorizeTopic;
            case SUMMARIZE -> summarizeTopic;
            case ENRICH_CONVERSATION, ENRICH_MESSAGE, ANALYZE_PARTICIPANT,
                 ASSESS_URGENCY, GENERATE_TOPIC, EXTRACT_ENTITIES, SENTIMENT_ANALYSIS -> enrichTopic;
        };
    }
}
