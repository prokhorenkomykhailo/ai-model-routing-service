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
    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    public IngestionConsumer(IngestionPipelineOrchestrator pipelineOrchestrator,
                            EnhancedErrorHandler enhancedErrorHandler,
                            KafkaRetryProperties kafkaRetryProperties,
                            KafkaTemplate<String, Map<String, Object>> kafkaTemplate) {
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
            logger.error("📊 [INGESTION-NULL] Received null message | Partition: {} | Offset: {} | STATS - Processed: {} | ✅ Success: {} | ❌ Failed: {} | 🆕 New: {} | 🔄 Duplicates: {}",
                partition, offset, totalMessagesProcessed.get(), successfulMessages.get(), failedMessages.get(),
                newMessages.get(), duplicateMessages.get());
            acknowledgment.acknowledge();
            return;
        }

        if (ingestionEventDto.getMessage() == null || ingestionEventDto.getMessage().getTs() == null) {
            failedMessages.incrementAndGet();
            logger.warn("⚠️ [INGESTION-INVALID] Invalid message data - skipping (messageId=null or ts=null) | STATS - Processed: {} | ✅ Success: {} | ❌ Failed: {} | 🆕 New: {} | 🔄 Duplicates: {}",
                totalMessagesProcessed.get(), successfulMessages.get(), failedMessages.get(),
                newMessages.get(), duplicateMessages.get());
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
     * @param ingestionEventDto the original ingestion event
     * @param exception the exception that caused the failure
     * @param retryAttempts number of retry attempts made
     * @param processingTime processing duration in ms
     */
    private void sendToDLQ(IngestionEventDTO ingestionEventDto, Exception exception,
                          int retryAttempts, long processingTime) {
        if (!kafkaRetryProperties.isDlqEnabled()) {
            logger.warn("⚠️ [DLQ-DISABLED] DLQ is disabled - skipping DLQ dispatch");
            return;
        }

        try {
            String dlqTopic = "lucid-ingestion-messages" + kafkaRetryProperties.getDlqTopicSuffix();

            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("originalTopic", "lucid-ingestion-messages");
            dlqMessage.put("originalValue", ingestionEventDto);
            dlqMessage.put("errorType", exception.getClass().getSimpleName());
            dlqMessage.put("errorMessage", exception.getMessage());
            dlqMessage.put("failedAt", LocalDateTime.now().format(ISO_FORMATTER));
            dlqMessage.put("retryAttempts", retryAttempts);
            dlqMessage.put("processingTimeMs", processingTime);

            kafkaTemplate.send(dlqTopic, dlqMessage);
            logger.info("✅ [DLQ-SENT] Failed message sent to DLQ topic: {} | ErrorType: {} | Attempts: {}",
                dlqTopic, exception.getClass().getSimpleName(), retryAttempts);

        } catch (Exception e) {
            logger.error("❌ [DLQ-ERROR] Failed to send message to DLQ: {}", e.getMessage(), e);
        }
    }
}
