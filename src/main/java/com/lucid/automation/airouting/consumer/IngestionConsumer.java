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

/**
 * Consumer service for processing ingestion messages from Kafka
 * Now uses the pipeline architecture for modular processing.
 *
 * @author AI Assistant
 */
@Service
public class IngestionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(IngestionConsumer.class);

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

        if (ingestionEventDto == null) {
            logger.error("Received null message from Kafka - deserialization failure (partition={}, offset={})", partition, offset);
            acknowledgment.acknowledge();
            return;
        }

        if (ingestionEventDto.getMessage() == null || ingestionEventDto.getMessage().getTs() == null) {
            logger.warn("Invalid message data - skipping (messageId=null or ts=null)");
            acknowledgment.acknowledge();
            return;
        }

        validateTimestamp(ingestionEventDto, acknowledgment);

        try {
            ProcessingResult processingResult = pipelineOrchestrator.processMessage(ingestionEventDto);

            if (processingResult.isSuccess()) {
                acknowledgment.acknowledge();
            } else {
                logger.error("Message processing failed: messageId={}, error={}",
                    ingestionEventDto.getMessage().getTs(),
                    processingResult.getErrorMessage());
                throw new RuntimeException("Pipeline processing failed: " + processingResult.getErrorMessage());
            }

        } catch (Exception e) {
            logger.error("Exception processing message {}: {}",
                ingestionEventDto.getMessage().getTs(), e.getMessage(), e);

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
