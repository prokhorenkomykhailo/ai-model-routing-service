package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.provider.AIProviderFactory;
import com.lucid.automation.airouting.service.EnrichmentJobProgressService;

import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class AIMessageIntegrationConfig {

    private static final Logger logger = LoggerFactory.getLogger(AIMessageIntegrationConfig.class);

    @Value("${kafka.topics.ai-enrich:ai-enrich}")
    private String aiEnrichTopic;

    @Value("${kafka.topics.pre-ai-responses:pre.ai.responses.queue}")
    private String preAiResponsesTopic;

    private final ConsumerFactory<String, AIMessage> aiMessageListenerContainerFactory;
    private final AIProviderFactory providerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EnrichmentJobProgressService progressService;

    @Bean
    public MessageChannel aiEnrichInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel aiEnrichTransformChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel aiEnrichProcessChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel aiEnrichResponseChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow aiEnrichKafkaListenerFlow() {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(aiMessageListenerContainerFactory, aiEnrichTopic)
                        .id("aiEnrichKafkaListenerAdapter")
                        // Ensure acknowledgment header is propagated through the flow
                        .configureListenerContainer(spec ->
                            spec.ackMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE)
                        ))
                .channel(aiEnrichInputChannel())
                .transform(Message.class, message -> {
                    // Transform while preserving headers including acknowledgment
                    AIMessage transformedPayload = transformAIMessage((AIMessage) message.getPayload());
                    return org.springframework.messaging.support.MessageBuilder
                            .withPayload(transformedPayload)
                            .copyHeaders(message.getHeaders())
                            .build();
                })
                .channel(aiEnrichTransformChannel())
                .handle(Message.class, (message, headers) -> {
                    AIMessage aiMessage = (AIMessage) message.getPayload();
                    validateAIMessage(aiMessage);
                    return message; // Return message with preserved headers
                })
                .channel(aiEnrichProcessChannel())
                .handle(Message.class, (message, headers) -> {
                    // propagate enriched message with headers
                    return enrichAIMessage((AIMessage) message.getPayload(), message.getHeaders());
                })
                .channel(aiEnrichResponseChannel())
                .handle(Message.class, (message, headers) -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> response = (Map<String, Object>) message.getPayload();
                    sendAIResponse(response, message.getHeaders());
                    return message;
                })
                .handle(Message.class, (message, headers) -> {
                    acknowledgeMessage(message.getHeaders());
                    return null;
                })
                .get();
    }

    /**
     * Transform component: Validates and prepares the incoming AI message
     */
    public AIMessage transformAIMessage(AIMessage aiMessage) {
        try {
            if (aiMessage == null) {
                logger.error("=== AI-TRANSFORM-ERROR === Received NULL message");
                throw new MessagingException("Received null AIMessage");
            }

            // Initialize job progress tracking - ensure we have a valid jobId
            String jobId = aiMessage.getJobId();
            if (jobId != null && !jobId.trim().isEmpty()) {
                progressService.initializeJob(
                    jobId,
                    aiMessage.getParentId(), // Use parentId from AIMessage for hierarchical job tracking
                    aiMessage.getUserId(),
                    aiMessage.getTenantId(),
                    aiMessage.getTenantSchema(),
                    aiMessage.getTaskType() != null ? aiMessage.getTaskType().toString() : "UNKNOWN"
                );
                progressService.updateProgress(jobId,
                    EnrichmentJobProgressService.PipelineStage.STARTED,
                    "AI message transformation started");
            } else {
                logger.debug("No jobId provided for message {}, skipping progress tracking", aiMessage.getMessageId());
            }

            // Set default reply topic if not specified
            if (aiMessage.getReplyTopic() == null || aiMessage.getReplyTopic().trim().isEmpty()) {
                logger.info("No reply topic specified for messageId={}, setting default", aiMessage.getMessageId());
                aiMessage.setReplyTopic(preAiResponsesTopic);
            }

            // Set default provider if not specified
            String preferredProvider = aiMessage.getPreferredProvider();
            if (preferredProvider == null || preferredProvider.trim().isEmpty()) {
                logger.warn("No preferred provider specified for messageId={}, will use default", aiMessage.getMessageId());
                aiMessage.setPreferredProvider(null); // Will trigger default provider selection
            }

            // Update progress for transformation completion
            jobId = aiMessage.getJobId();
            progressService.updateProgress(jobId,
                EnrichmentJobProgressService.PipelineStage.TRANSFORMED,
                "AI message transformation completed successfully");

            logger.debug("AI message transformed successfully: messageId={}, taskType={}",
                       aiMessage.getMessageId(), aiMessage.getTaskType());

            return aiMessage;

        } catch (Exception e) {
            // Mark job as failed if jobId is available
            String jobId = (aiMessage != null) ? aiMessage.getJobId() : null;
            if (jobId != null && !jobId.trim().isEmpty()) {
                progressService.markJobFailed(jobId,
                    "Transformation failed: " + e.getMessage());
            }

            logger.error("Error during AI message transformation: {}", e.getMessage(), e);
            throw new MessagingException("Failed to transform AI message", e);
        }
    }

    /**
     * Handler component: Validates the transformed message and task type
     */
    public void validateAIMessage(AIMessage aiMessage) {
        try {
            // Validate task type
            AITaskType taskType = aiMessage.getTaskType();
            if (taskType == null) {
                throw new IllegalArgumentException("Task type is required for enrichment processing");
            }

            // Validate task-specific requirements
            switch (taskType) {
                case ENRICH_CONVERSATION -> {
                    if (aiMessage.getMessages() == null || aiMessage.getMessages().isEmpty()) {
                        throw new IllegalArgumentException("Messages are required for conversation enrichment");
                    }
                }
                default -> {
                    // Other task types may have different validation requirements
                    logger.debug("Validating task type: {}", taskType);
                }
            }

            // Update progress for validation completion
            String jobId = aiMessage.getJobId();
            progressService.updateProgress(jobId,
                EnrichmentJobProgressService.PipelineStage.VALIDATED,
                "AI message validation completed successfully for task type: " + taskType);


            logger.debug("AI message validation successful: messageId={}, taskType={}",
                       aiMessage.getMessageId(), taskType);

        } catch (Exception e) {
            // Mark job as failed if jobId is available
            String jobId = aiMessage.getJobId();
            if (jobId != null && !jobId.trim().isEmpty()) {
                progressService.markJobFailed(jobId,
                    "Validation failed: " + e.getMessage());
            }

            logger.error("Error during AI message validation: {}", e.getMessage(), e);
            throw new MessagingException("Failed to validate AI message", e);
        }
    }

    /**
     * Enrich component: Processes the AI message using the appropriate provider
     */
    public org.springframework.messaging.Message<?> enrichAIMessage(AIMessage aiMessage, MessageHeaders headers) {
        try {
            String preferredProvider = aiMessage.getPreferredProvider();
            AIProvider provider = preferredProvider != null ?
                providerFactory.getProvider(preferredProvider) :
                providerFactory.getDefaultProvider();

            if (provider == null) {
                throw new RuntimeException("No AI provider available for processing enrichment request");
            }

            logger.info("Processing enrichment with provider: {} for messageId={}",
                      provider.getProviderId(), aiMessage.getMessageId());

            Object result = processEnrichmentTask(provider, aiMessage);

            logger.info("AI enrichment completed successfully: messageId={}, provider={}",
                      aiMessage.getMessageId(), provider.getProviderId());

            Map<String, Object> response = createSuccessResponse(aiMessage, result);

            // Update progress for enrichment completion
            String jobId = aiMessage.getJobId();
            progressService.updateProgress(jobId,
                EnrichmentJobProgressService.PipelineStage.ENRICHED,
                "AI enrichment completed successfully using provider: " + provider.getProviderId());


            return org.springframework.messaging.support.MessageBuilder
                .withPayload(response)
                .copyHeaders(headers)
                .setHeader("ai.reply.topic", aiMessage.getReplyTopic())
                .setHeader("ai.status", "success")
                .setHeader("ai.provider", provider.getProviderId())
                .setHeader("ai.job.id", aiMessage.getJobId()) // Include jobId in headers
                .build();

        } catch (Exception e) {
            logger.error("Error during AI message enrichment: {}", e.getMessage(), e);

            // Mark job as failed if jobId is available
            String jobId = aiMessage.getJobId();
            if (jobId != null && !jobId.trim().isEmpty()) {
                progressService.markJobFailed(jobId,
                    "Enrichment failed: " + e.getMessage());
            }

            Map<String, Object> errorResponse = createErrorResponse(aiMessage, e.getMessage());

            return org.springframework.messaging.support.MessageBuilder
                .withPayload(errorResponse)
                .copyHeaders(headers)
                .setHeader("ai.reply.topic", aiMessage.getReplyTopic())
                .setHeader("ai.status", "error")
                .setHeader("ai.error", e.getMessage())
                .setHeader("ai.job.id", aiMessage.getJobId()) // Include jobId in headers
                .build();
        }
    }

    /**
     * Handler component: Sends the response to the appropriate Kafka topic
     */
    public void sendAIResponse(Map<String, Object> response, MessageHeaders headers) {
        String jobId = null;
        try {
            String replyTopic = (String) headers.get("ai.reply.topic");
            jobId = (String) headers.get("ai.job.id");

            if (replyTopic == null || replyTopic.trim().isEmpty()) {
                logger.error("CRITICAL: Reply topic is null/empty!");
                throw new RuntimeException("Reply topic is null or empty");
            }

            // Send response to Kafka
            kafkaTemplate.send(replyTopic, response);

            // Update progress for response sent
            progressService.updateProgress(jobId,
                EnrichmentJobProgressService.PipelineStage.RESPONSE_SENT,
                "Response sent successfully to topic: " + replyTopic);


            String status = (String) headers.get("ai.status");
            if ("error".equals(status)) {
                logger.error("Sent error response to topic: replyTopic={}, error={}",
                                    replyTopic,
                                    headers.get("ai.error"));
            } else {
                logger.info("Sent success response to topic: replyTopic={}",
                           replyTopic);
            }

        } catch (Exception e) {
            // Mark job as failed if jobId is available
            if (jobId != null && !jobId.trim().isEmpty()) {
                progressService.markJobFailed(jobId,
                    "Failed to send response: " + e.getMessage());
            }

            logger.error("Error during AI message response handling: {}", e.getMessage(), e);
            throw new MessagingException("Failed to send AI response", e);
        }
    }

    /**
     * Handler component: Acknowledges the Kafka message
     */
    public void acknowledgeMessage(MessageHeaders headers) {
        String jobId = null;
        try {
            jobId = (String) headers.get("ai.job.id");
            // Mark job as completed
            progressService.updateProgress(jobId,
                EnrichmentJobProgressService.PipelineStage.COMPLETED,
                "AI enrichment pipeline completed successfully");


            // Use the correct KafkaHeaders constant for acknowledgment
            Acknowledgment acknowledgment = (Acknowledgment) headers.get(KafkaHeaders.ACKNOWLEDGMENT);
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("✅ [ACK-SUCCESS] Kafka message acknowledged successfully for jobId={}", jobId);
            } else {
                // Log all available header keys for debugging
                logger.warn("⚠️ [ACK-MISSING] No acknowledgment found in message headers for jobId={}. Available headers: {}",
                           jobId, headers.keySet());
            }
        } catch (Exception e) {
            // Don't mark job as failed here since acknowledgment issues are usually not critical
            // for the business logic, but log for monitoring
            logger.error("❌ [ACK-ERROR] Error during message acknowledgment: {}", e.getMessage(), e);

            if (jobId != null && !jobId.trim().isEmpty()) {
                logger.warn("⚠️ [ACK-WARN] Job {} completed processing but acknowledgment failed", jobId);
            }
        }
    }

    /**
     * Private helper method to process enrichment tasks
     */
    private Object processEnrichmentTask(AIProvider provider, AIMessage message) {
        AITaskType taskType = message.getTaskType();
        if (taskType == null) {
            throw new IllegalArgumentException("Task type is required for enrichment processing");
        }

        return switch (taskType) {
            case ENRICH_CONVERSATION -> {
                if (message.getMessages() == null || message.getMessages().isEmpty()) {
                    throw new IllegalArgumentException("Messages are required for conversation enrichment");
                }
                yield provider.enrichConversation(message);
            }
            default -> throw new IllegalArgumentException("Unsupported enrichment task type: " + taskType);
        };
    }

    /**
     * Private helper method to create success response
     */
    private Map<String, Object> createSuccessResponse(AIMessage originalMessage, Object result) {
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", originalMessage.getMessageId());
        response.put("jobId", originalMessage.getJobId());
        response.put("taskType", originalMessage.getTaskType().toString().toLowerCase());
        response.put("status", "success");
        response.put("result", result);
        response.put("processedAt", LocalDateTime.now());
        response.put("tenantId", originalMessage.getTenantId());
        response.put("tenantSchema", originalMessage.getTenantSchema());
        response.put("userId", originalMessage.getUserId());

        // Extract context information
        Map<String, Object> context = originalMessage.getContext();
        Object deemergeUserIdObj = context != null ? context.get("deemergeUserId") : null;
        Object deemergeUserNameObj = context != null ? context.get("deemergeUserName") : null;
        response.put("deemergeUserId", deemergeUserIdObj != null ? deemergeUserIdObj.toString() : "NoId");
        response.put("deemergeUserName", deemergeUserNameObj != null ? deemergeUserNameObj.toString() : "NoUser");
        response.put("teamId", context != null && context.get("teamId") != null ? context.get("teamId").toString() : "");

        return response;
    }

    /**
     * Private helper method to create error response
     */
    private Map<String, Object> createErrorResponse(AIMessage originalMessage, String errorMessage) {
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", originalMessage.getMessageId());
        response.put("jobId", originalMessage.getJobId());
        response.put("taskType", originalMessage.getTaskType().toString().toLowerCase());
        response.put("status", "error");
        response.put("error", errorMessage);
        response.put("processedAt", LocalDateTime.now());
        response.put("tenantId", originalMessage.getTenantId());
        response.put("tenantSchema", originalMessage.getTenantSchema());
        response.put("userId", originalMessage.getUserId());

        return response;
    }
}
