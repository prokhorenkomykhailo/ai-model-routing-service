package com.lucid.automation.airouting.consumer;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionPipelineOrchestrator;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
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
 * Consumer service for processing ingestion messages from Kafka
 * Now uses the pipeline architecture for modular processing.
 *
 * @author AI Assistant
 */
@Service
public class IngestionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(IngestionConsumer.class);

    // Add counters for statistics
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong successfulMessages = new AtomicLong(0);
    private final AtomicLong failedMessages = new AtomicLong(0);
    private final AtomicLong newMessages = new AtomicLong(0);
    private final AtomicLong duplicateMessages = new AtomicLong(0);

    private final IngestionPipelineOrchestrator pipelineOrchestrator;

    public IngestionConsumer(IngestionPipelineOrchestrator pipelineOrchestrator) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        logger.info("IngestionConsumer initialized with pipeline architecture");
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
                newMessages.incrementAndGet(); // Assume new if processed successfully
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
                failedMessages.incrementAndGet();
                logger.error("❌ [INGESTION-FAILED] Message processing failed after {}ms: {} | Error: {} | 📊 RUNNING TOTALS - Processed: {} | Success: {} | Failed: {} | New: {} | Duplicates: {}",
                    processingTime,
                    ingestionEventDto.getMessage().getTs(),
                    processingResult.getErrorMessage(),
                    totalMessagesProcessed.get(),
                    successfulMessages.get(),
                    failedMessages.get(),
                    newMessages.get(),
                    duplicateMessages.get());
                throw new RuntimeException("Pipeline processing failed: " + processingResult.getErrorMessage());
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

            if (isRetryableException(e)) {
                logger.info("Retryable exception - not acknowledging message");
                throw e;
            } else {
                logger.info("Non-retryable exception - acknowledging message");
                acknowledgment.acknowledge();
            }
        }
    }

    private void validateTimestamp(IngestionEventDTO ingestionEventDto, Acknowledgment acknowledgment) {
        try {
            String tsStr = ingestionEventDto.getMessage().getTs();
            double timestamp = Double.parseDouble(tsStr);
            if (timestamp > System.currentTimeMillis() / 1000.0 + 86400) {
                logger.warn("Message timestamp in future: {} - skipping", timestamp);
                acknowledgment.acknowledge();
                return;
            }
        } catch (NumberFormatException e) {
            logger.warn("Invalid timestamp format: {} - continuing", ingestionEventDto.getMessage().getTs());
        }
    }

    private boolean isRetryableException(Exception exception) {
        if (exception.getMessage() != null) {
            String msg = exception.getMessage().toLowerCase();
            if (msg.contains("connection") || msg.contains("timeout") ||
                msg.contains("network") || msg.contains("redis") ||
                msg.contains("unable to connect") || msg.contains("connection refused")) {
                return true;
            }
        }

        String className = exception.getClass().getSimpleName().toLowerCase();
        if (className.contains("connection") || className.contains("timeout") ||
            className.contains("redis") || className.contains("jedis")) {
            return true;
        }

        if (exception instanceof IllegalArgumentException ||
            exception instanceof ClassCastException ||
            exception instanceof NullPointerException) {
            return false;
        }

        if (className.contains("serialization") || className.contains("json") ||
            className.contains("parse") || className.contains("mapping")) {
            return false;
        }

        return true;
    }
}
