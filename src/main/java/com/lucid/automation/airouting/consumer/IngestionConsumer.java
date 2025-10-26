package com.lucid.automation.airouting.consumer;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.util.TimestampUtil;
import com.lucid.automation.airouting.config.EnhancedErrorHandler;
import com.lucid.automation.airouting.config.KafkaRetryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer service for processing ingestion messages from Kafka
 * Now uses the pipeline architecture for modular processing.
 * Implements intelligent retry/DLQ strategy with exponential backoff.
 *
 * @author vudu
 */
@Service
public class IngestionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(IngestionConsumer.class);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    // Add counters for statistics
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong successfulMessages = new AtomicLong(0);
    private final AtomicLong failedMessages = new AtomicLong(0);
    private final AtomicLong newMessages = new AtomicLong(0);
    private final AtomicLong duplicateMessages = new AtomicLong(0);

    // New metrics for retry/DLQ tracking
    private final AtomicLong retriedIngestion = new AtomicLong(0);
    private final AtomicLong dlqIngestion = new AtomicLong(0);

    private final IngestionPipelineOrchestrator pipelineOrchestrator;
    private final EnhancedErrorHandler enhancedErrorHandler;
    private final KafkaRetryProperties kafkaRetryProperties;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public IngestionConsumer(IngestionPipelineOrchestrator pipelineOrchestrator,
                            EnhancedErrorHandler enhancedErrorHandler,
                            KafkaRetryProperties kafkaRetryProperties,
                            KafkaTemplate<String, Object> kafkaTemplate) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.enhancedErrorHandler = enhancedErrorHandler;
        this.kafkaRetryProperties = kafkaRetryProperties;
        this.kafkaTemplate = kafkaTemplate;
        logger.info("✅ [INIT] IngestionConsumer initialized with enhanced retry/DLQ strategy");
    }

    @KafkaListener(
        topics = "${kafka.topics.ingestion-messages}",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "spring.json.use.type.headers=false",
            "spring.json.value.default.type=com.lucid.automation.common.dto.messaging.IngestionEventDTO"
        }
    )
    public void processIngestionMessage(@Payload IngestionEventDTO ingestionEventDto,
                                      Acknowledgment acknowledgment,
                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                      @Header(KafkaHeaders.OFFSET) long offset) {

        long startTime = System.currentTimeMillis();
        totalMessagesProcessed.incrementAndGet();

        if (ingestionEventDto == null) {
            failedMessages.incrementAndGet();
            dlqIngestion.incrementAndGet();
            logger.error("📊 [INGESTION-NULL] Received null message | Partition: {} | Offset: {} | STATS - Processed: {} | ✅ Success: {} | ❌ Failed: {} | 🆕 New: {} | 🔄 Duplicates: {}",
                partition, offset, totalMessagesProcessed.get(), successfulMessages.get(), failedMessages.get(),
                newMessages.get(), duplicateMessages.get());

            // Send to DLQ for monitoring - create minimal error context
            sendNullMessageToDLQ(partition, offset, "Null message received from Kafka");
            acknowledgment.acknowledge();
            return;
        }

        if (ingestionEventDto.getMessage() == null || ingestionEventDto.getMessage().getTs() == null) {
            failedMessages.incrementAndGet();
            dlqIngestion.incrementAndGet();
            logger.warn("⚠️ [INGESTION-INVALID] Invalid message data - skipping (messageId=null or ts=null) | STATS - Processed: {} | ✅ Success: {} | ❌ Failed: {} | 🆕 New: {} | 🔄 Duplicates: {}",
                totalMessagesProcessed.get(), successfulMessages.get(), failedMessages.get(),
                newMessages.get(), duplicateMessages.get());

            // Send to DLQ for monitoring
            sendToDLQ(ingestionEventDto, new IllegalArgumentException("Message or timestamp is null"), 0, 0);
            acknowledgment.acknowledge();
            return;
        }

        logger.info("📨 [INGESTION] Processing message: tenantId={}, messageId={}, ts={}",
            ingestionEventDto.getTenantId(),
            ingestionEventDto.getMessage().getTs(),
            ingestionEventDto.getMessage().getTs());

        validateTimestamp(ingestionEventDto, acknowledgment);

        try {
            ProcessingResult processingResult = pipelineOrchestrator.processMessage(ingestionEventDto);
            long processingTime = System.currentTimeMillis() - startTime;

            if (processingResult.isSuccess()) {
                successfulMessages.incrementAndGet();
                newMessages.incrementAndGet();
                logger.info("✅ [INGESTION-SUCCESS] Message processed in {}ms: {} | 📊 RUNNING TOTALS - Processed: {} | Success: {} | Failed: {} | New: {} | Duplicates: {}",
                    processingTime,
                    ingestionEventDto.getMessage().getTs(),
                    totalMessagesProcessed.get(),
                    successfulMessages.get(),
                    failedMessages.get(),
                    newMessages.get(),
                    duplicateMessages.get());
                acknowledgment.acknowledge();
            } else {
                // Pipeline failed - treat as retryable for transient failures
                handlePipelineFailure(ingestionEventDto, processingResult, acknowledgment, processingTime);
            }

        } catch (Exception e) {
            failedMessages.incrementAndGet();
            long processingTime = System.currentTimeMillis() - startTime;
            logger.error("🚨 [INGESTION-EXCEPTION] Exception processing message after {}ms: {} | Error: {} | 📊 TOTALS - Processed: {} | Success: {} | Failed: {} | New: {} | Duplicates: {}",
                processingTime,
                ingestionEventDto.getMessage().getTs(),
                e.getMessage(),
                totalMessagesProcessed.get(),
                successfulMessages.get(),
                failedMessages.get(),
                newMessages.get(),
                duplicateMessages.get());

            // Classify error and decide retry strategy
            if (enhancedErrorHandler.isRetryableException(e)) {
                // Retryable: NO ACK issued - Kafka session timeout triggers rebalance
                retriedIngestion.incrementAndGet();
                logger.info("🔄 [RETRY-STRATEGY] Retryable exception - NOT acknowledging message (will be retried via rebalance)");
                throw e;
            } else {
                // Non-retryable: ACK + DLQ dispatch
                dlqIngestion.incrementAndGet();
                logger.info("🚫 [DLQ-STRATEGY] Non-retryable exception - acknowledging and sending to DLQ");
                sendToDLQ(ingestionEventDto, e, 0, processingTime);
                acknowledgment.acknowledge();
            }
        }
    }

    private void validateTimestamp(IngestionEventDTO ingestionEventDto, Acknowledgment acknowledgment) {
        try {
            String tsStr = ingestionEventDto.getMessage().getTs();

            // Use TimestampUtil to handle both timestamp formats
            TimestampUtil.ParsedTimestamp parsed = TimestampUtil.parseTimestamp(tsStr);

            if (!parsed.isValid()) {
                logger.warn("❌ [TIMESTAMP-INVALID] Invalid timestamp format: '{}' - Error: {} - continuing with processing",
                    tsStr, parsed.getErrorMessage());
                return;
            }

            // Check if timestamp is too far in the future (24 hours threshold)
            if (TimestampUtil.isTimestampInFuture(tsStr, 86400)) {
                logger.warn("⏰ [TIMESTAMP-FUTURE] Message timestamp too far in future: '{}' (format: {}) - skipping message",
                    tsStr, TimestampUtil.getTimestampFormatDescription(tsStr));

                // Send to DLQ for monitoring - future timestamp messages
                failedMessages.incrementAndGet();
                dlqIngestion.incrementAndGet();
                sendToDLQ(ingestionEventDto,
                    new IllegalArgumentException("Message timestamp is too far in future (>24h): " + tsStr),
                    0, 0);
                acknowledgment.acknowledge();
                return;
            }

            logger.debug("✅ [TIMESTAMP-VALID] Message timestamp validated: '{}' (format: {})",
                tsStr, TimestampUtil.getTimestampFormatDescription(tsStr));

        } catch (Exception e) {
            logger.warn("⚠️ [TIMESTAMP-ERROR] Unexpected error validating timestamp: '{}' - Error: {} - continuing with processing",
                ingestionEventDto.getMessage().getTs(), e.getMessage());
        }
    }

    /**
     * Handles pipeline processing failures with intelligent retry/DLQ classification.
     *
     * @param ingestionEventDto the ingestion event
     * @param result the pipeline processing result
     * @param acknowledgment Kafka acknowledgment
     * @param processingTime processing duration in ms
     */
    private void handlePipelineFailure(IngestionEventDTO ingestionEventDto, ProcessingResult result,
                                      Acknowledgment acknowledgment, long processingTime) {
        failedMessages.incrementAndGet();

        String errorMsg = result.getErrorMessage();
        logger.error("❌ [INGESTION-FAILED] Message processing failed after {}ms: {} | Error: {} | 📊 RUNNING TOTALS - Processed: {} | Success: {} | Failed: {} | New: {} | Duplicates: {}",
            processingTime,
            ingestionEventDto.getMessage().getTs(),
            errorMsg,
            totalMessagesProcessed.get(),
            successfulMessages.get(),
            failedMessages.get(),
            newMessages.get(),
            duplicateMessages.get());

        // Treat as retryable for transient pipeline failures
        RuntimeException pipelineException = new RuntimeException("Pipeline processing failed: " + errorMsg);
        if (enhancedErrorHandler.isRetryableException(pipelineException)) {
            retriedIngestion.incrementAndGet();
            logger.info("🔄 [PIPELINE-RETRY] Pipeline failure classified as retryable - NOT acknowledging");
            throw pipelineException;
        } else {
            dlqIngestion.incrementAndGet();
            logger.info("🚫 [PIPELINE-DLQ] Pipeline failure classified as non-retryable - DLQ dispatch");
            sendToDLQ(ingestionEventDto, pipelineException, 0, processingTime);
            acknowledgment.acknowledge();
        }
    }

    /**
     * Sends a failed message to the DLQ topic for audit and manual intervention.
     * Creates a structured error message with full context.
     *
     * GUARANTEED DELIVERY: DLQ is ALWAYS enabled for monitoring, even if config says disabled.
     *
     * @param ingestionEventDto the original ingestion event
     * @param exception the exception that caused the failure
     * @param retryAttempts number of retry attempts made
     * @param processingTime processing duration in ms
     */
    private void sendToDLQ(IngestionEventDTO ingestionEventDto, Exception exception,
                          int retryAttempts, long processingTime) {
        try {
            String dlqTopic = "lucid-ingestion-messages" + kafkaRetryProperties.getDlqTopicSuffix();

            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("originalTopic", "lucid-ingestion-messages");
            dlqMessage.put("originalValue", ingestionEventDto);
            dlqMessage.put("errorType", exception.getClass().getSimpleName());
            dlqMessage.put("errorMessage", exception.getMessage());
            dlqMessage.put("errorStackTrace", getStackTraceString(exception));
            dlqMessage.put("failedAt", LocalDateTime.now().format(ISO_FORMATTER));
            dlqMessage.put("retryAttempts", retryAttempts);
            dlqMessage.put("processingTimeMs", processingTime);

            // Add message identifiers for tracking
            if (ingestionEventDto != null) {
                dlqMessage.put("tenantId", ingestionEventDto.getTenantId());
                if (ingestionEventDto.getMessage() != null) {
                    dlqMessage.put("messageTs", ingestionEventDto.getMessage().getTs());
                    dlqMessage.put("channelId", ingestionEventDto.getMessage().getChannelId());
                    dlqMessage.put("teamId", ingestionEventDto.getMessage().getTeamId());
                }
            }

            kafkaTemplate.send(dlqTopic, dlqMessage);
            logger.info("✅ [DLQ-SENT] Failed message sent to DLQ topic: {} | ErrorType: {} | Attempts: {} | TenantId: {}",
                dlqTopic, exception.getClass().getSimpleName(), retryAttempts,
                ingestionEventDto != null ? ingestionEventDto.getTenantId() : "null");

        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] CRITICAL: Failed to send message to DLQ - MESSAGE LOST: {}", e.getMessage(), e);
            // Log full message details for recovery
            logger.error("❌ [DLQ-ERROR] Lost message details: tenantId={}, messageTs={}, error={}",
                ingestionEventDto != null ? ingestionEventDto.getTenantId() : "null",
                ingestionEventDto != null && ingestionEventDto.getMessage() != null ? ingestionEventDto.getMessage().getTs() : "null",
                exception.getMessage());
        }
    }

    /**
     * Sends a null message error to DLQ with partition/offset context.
     * Used when the message payload itself is null and we can't extract details.
     *
     * @param partition Kafka partition
     * @param offset Kafka offset
     * @param errorMessage Error description
     */
    private void sendNullMessageToDLQ(int partition, long offset, String errorMessage) {
        try {
            String dlqTopic = "lucid-ingestion-messages" + kafkaRetryProperties.getDlqTopicSuffix();

            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("originalTopic", "lucid-ingestion-messages");
            dlqMessage.put("originalValue", null);
            dlqMessage.put("errorType", "NullMessageException");
            dlqMessage.put("errorMessage", errorMessage);
            dlqMessage.put("failedAt", LocalDateTime.now().format(ISO_FORMATTER));
            dlqMessage.put("partition", partition);
            dlqMessage.put("offset", offset);
            dlqMessage.put("retryAttempts", 0);
            dlqMessage.put("processingTimeMs", 0);

            kafkaTemplate.send(dlqTopic, dlqMessage);
            logger.info("✅ [DLQ-SENT] Null message sent to DLQ topic: {} | Partition: {} | Offset: {}",
                dlqTopic, partition, offset);

        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] CRITICAL: Failed to send null message to DLQ - MONITORING BLIND SPOT | Partition: {} | Offset: {} | Error: {}",
                partition, offset, e.getMessage(), e);
        }
    }

    /**
     * Extracts stack trace from exception as string for DLQ logging.
     *
     * @param exception The exception
     * @return Stack trace as string (first 10 lines)
     */
    private String getStackTraceString(Exception exception) {
        if (exception == null) return "No stack trace";

        StringBuilder sb = new StringBuilder();
        sb.append(exception.getClass().getName()).append(": ").append(exception.getMessage()).append("\n");

        StackTraceElement[] elements = exception.getStackTrace();
        int limit = Math.min(elements.length, 10); // First 10 lines only
        for (int i = 0; i < limit; i++) {
            sb.append("\tat ").append(elements[i].toString()).append("\n");
        }

        if (elements.length > 10) {
            sb.append("\t... ").append(elements.length - 10).append(" more\n");
        }

        return sb.toString();
    }
}
