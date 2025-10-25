package com.lucid.automation.airouting.consumer;

import com.lucid.automation.airouting.dto.DLQMessageDTO;
import com.lucid.automation.airouting.service.DLQMetricsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer service for processing messages from Dead Letter Queue (DLQ) topics.
 * Listens to all DLQ topics and performs audit logging, metrics collection, and alerting.
 *
 * Message Flow:
 * 1. Message fails in PostProcessingConsumer/IngestionConsumer
 * 2. Non-retryable error → sendToDLQ() → lucid-*-dlq topic
 * 3. DLQConsumer receives → logs to audit table → publishes metrics
 * 4. Operations team reviews audit logs for investigation
 *
 * @author vudu
 * @since 1.2.6
 */
@Service
public class DLQConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DLQConsumer.class);

    // DLQ metrics
    private final AtomicLong totalDlqMessagesReceived = new AtomicLong(0);
    private final AtomicLong dlqMessagesProcessed = new AtomicLong(0);
    private final AtomicLong dlqProcessingErrors = new AtomicLong(0);

    private final DLQMetricsPublisher metricsPublisher;

    public DLQConsumer(DLQMetricsPublisher metricsPublisher) {
        this.metricsPublisher = metricsPublisher;
        logger.info("✅ [INIT] DLQConsumer initialized with metrics publisher");
    }

    /**
     * Listens to all pre.ai.responses DLQ messages (from PostProcessingConsumer failures).
     * Topics pattern allows future expansion to multiple DLQ topics.
     *
     * @param dlqMessage the DLQ message containing original message and error details
     * @param topic the topic name (e.g., pre.ai.responses.queue-dlq)
     * @param partition the partition the message came from
     * @param offset the message offset
     * @param acknowledgment Kafka acknowledgment for manual commit
     */
    @KafkaListener(
        topics = "pre.ai.responses.queue-dlq",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "spring.json.use.type.headers=false",
            "spring.json.value.default.type=com.lucid.automation.airouting.dto.DLQMessageDTO"
        }
    )
    public void processPreAiResponsesDLQ(@Payload DLQMessageDTO dlqMessage,
                                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                        @Header(KafkaHeaders.OFFSET) long offset,
                                        Acknowledgment acknowledgment) {

        totalDlqMessagesReceived.incrementAndGet();

        try {
            logger.warn("📥 [DLQ-RECEIVED] PreAiResponses DLQ message | Partition: {} | Offset: {} | ErrorType: {} | Message: {} | Received: {}",
                partition, offset, dlqMessage.getErrorType(),
                dlqMessage.getErrorMessage(), totalDlqMessagesReceived.get());

            // Record metrics
            metricsPublisher.recordDLQMessageReceived(dlqMessage, "pre.ai.responses");
            metricsPublisher.recordErrorTypeFrequency("pre.ai.responses", dlqMessage.getErrorType());
            if (dlqMessage.getRetryAttempts() != null) {
                metricsPublisher.recordRetryAttempts("pre.ai.responses", dlqMessage.getRetryAttempts());
            }

            // Audit logging (future: write to audit table)
            auditDLQMessage(dlqMessage, "pre.ai.responses", partition, offset);

            // ACK after successful processing
            dlqMessagesProcessed.incrementAndGet();
            metricsPublisher.recordDLQMessageProcessed("pre.ai.responses");
            acknowledgment.acknowledge();

            logger.info("✅ [DLQ-PROCESSED] PreAiResponses DLQ message logged | Processed: {} | Errors: {}",
                dlqMessagesProcessed.get(), dlqProcessingErrors.get());

        } catch (Exception e) {
            dlqProcessingErrors.incrementAndGet();
            logger.error("❌ [DLQ-ERROR] Failed to process PreAiResponses DLQ message | Error: {} | Errors: {}",
                e.getMessage(), dlqProcessingErrors.get(), e);

            // On error, still acknowledge to prevent infinite retry loop
            acknowledgment.acknowledge();
        }
    }

    /**
     * Listens to all ingestion DLQ messages (from IngestionConsumer failures).
     *
     * @param dlqMessage the DLQ message containing original message and error details
     * @param topic the topic name (e.g., lucid-ingestion-messages-dlq)
     * @param partition the partition the message came from
     * @param offset the message offset
     * @param acknowledgment Kafka acknowledgment for manual commit
     */
    @KafkaListener(
        topics = "lucid-ingestion-messages-dlq",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "spring.json.use.type.headers=false",
            "spring.json.value.default.type=com.lucid.automation.airouting.dto.DLQMessageDTO"
        }
    )
    public void processIngestionDLQ(@Payload DLQMessageDTO dlqMessage,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   Acknowledgment acknowledgment) {

        totalDlqMessagesReceived.incrementAndGet();

        try {
            logger.warn("📥 [DLQ-RECEIVED] Ingestion DLQ message | Partition: {} | Offset: {} | ErrorType: {} | Message: {} | Received: {}",
                partition, offset, dlqMessage.getErrorType(),
                dlqMessage.getErrorMessage(), totalDlqMessagesReceived.get());

            // Record metrics
            metricsPublisher.recordDLQMessageReceived(dlqMessage, "lucid-ingestion-messages");
            metricsPublisher.recordErrorTypeFrequency("lucid-ingestion-messages", dlqMessage.getErrorType());
            if (dlqMessage.getRetryAttempts() != null) {
                metricsPublisher.recordRetryAttempts("lucid-ingestion-messages", dlqMessage.getRetryAttempts());
            }

            // Audit logging (future: write to audit table)
            auditDLQMessage(dlqMessage, "lucid-ingestion-messages", partition, offset);

            // ACK after successful processing
            dlqMessagesProcessed.incrementAndGet();
            metricsPublisher.recordDLQMessageProcessed("lucid-ingestion-messages");
            acknowledgment.acknowledge();

            logger.info("✅ [DLQ-PROCESSED] Ingestion DLQ message logged | Processed: {} | Errors: {}",
                dlqMessagesProcessed.get(), dlqProcessingErrors.get());

        } catch (Exception e) {
            dlqProcessingErrors.incrementAndGet();
            logger.error("❌ [DLQ-ERROR] Failed to process Ingestion DLQ message | Error: {} | Errors: {}",
                e.getMessage(), dlqProcessingErrors.get(), e);

            // On error, still acknowledge to prevent infinite retry loop
            acknowledgment.acknowledge();
        }
    }

    /**
     * Performs audit logging of DLQ messages for investigation and compliance.
     * Future: Write to audit table in PostgreSQL for persistent audit trail.
     *
     * @param dlqMessage the DLQ message
     * @param originalTopic the original topic name
     * @param partition the Kafka partition
     * @param offset the Kafka offset
     */
    private void auditDLQMessage(DLQMessageDTO dlqMessage, String originalTopic,
                                int partition, long offset) {
        try {
            // Log audit entry with full context
            logger.info("📋 [AUDIT] DLQ Message Audit Entry | OriginalTopic: {} | ErrorType: {} | Partition: {} | Offset: {} | FailedAt: {} | StackTrace: {}",
                originalTopic,
                dlqMessage.getErrorType(),
                partition,
                offset,
                dlqMessage.getFailedAt(),
                dlqMessage.getErrorStackTrace());

            // Future: Write to audit table
            // auditRepository.saveAuditEntry(dlqMessage, originalTopic, partition, offset);

        } catch (Exception e) {
            logger.error("⚠️ [AUDIT-ERROR] Failed to create audit entry: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns total DLQ messages received since service startup.
     *
     * @return total DLQ messages received
     */
    public long getTotalDlqMessagesReceived() {
        return totalDlqMessagesReceived.get();
    }    /**
     * Returns total DLQ messages successfully processed.
     *
     * @return total DLQ messages processed
     */
    public long getDlqMessagesProcessed() {
        return dlqMessagesProcessed.get();
    }

    /**
     * Returns total errors during DLQ message processing.
     *
     * @return total DLQ processing errors
     */
    public long getDlqProcessingErrors() {
        return dlqProcessingErrors.get();
    }
}
