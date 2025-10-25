package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.DLQMessageDTO;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Service for publishing DLQ metrics to Micrometer for Prometheus monitoring.
 * Tracks DLQ message counts, error types, and processing performance.
 *
 * Exposed Metrics:
 * - dlq.messages.received - Counter by topic and error_type
 * - dlq.messages.processed - Counter by topic
 * - dlq.processing.errors - Counter for DLQ processing failures
 * - dlq.message.processing.time - Timer for DLQ message processing duration
 *
 * @author vudu
 * @since 1.2.6
 */
@Service
public class DLQMetricsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(DLQMetricsPublisher.class);

    private final MeterRegistry meterRegistry;

    public DLQMetricsPublisher(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records a DLQ message received event with full classification.
     * Increments counters by topic and error type for detailed monitoring.
     *
     * @param dlqMessage the DLQ message
     * @param originalTopic the original source topic
     */
    public void recordDLQMessageReceived(DLQMessageDTO dlqMessage, String originalTopic) {
        try {
            // Counter: total DLQ messages by topic and error type
            Counter.builder("dlq.messages.received")
                .tag("topic", originalTopic)
                .tag("error_type", dlqMessage.getErrorType() != null ? dlqMessage.getErrorType() : "unknown")
                .tag("consumer", dlqMessage.getConsumerName() != null ? dlqMessage.getConsumerName() : "unknown")
                .description("Count of messages received in DLQ")
                .register(meterRegistry)
                .increment();

            logger.debug("📊 [METRICS-RECORDED] DLQ message received | Topic: {} | ErrorType: {}",
                originalTopic, dlqMessage.getErrorType());

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record DLQ message received metric: {}", e.getMessage());
        }
    }

    /**
     * Records a DLQ message successfully processed event.
     *
     * @param originalTopic the original source topic
     */
    public void recordDLQMessageProcessed(String originalTopic) {
        try {
            // Counter: total processed DLQ messages by topic
            Counter.builder("dlq.messages.processed")
                .tag("topic", originalTopic)
                .description("Count of DLQ messages successfully processed")
                .register(meterRegistry)
                .increment();

            logger.debug("📊 [METRICS-RECORDED] DLQ message processed | Topic: {}", originalTopic);

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record DLQ message processed metric: {}", e.getMessage());
        }
    }

    /**
     * Records a DLQ message processing error.
     *
     * @param originalTopic the original source topic
     */
    public void recordDLQProcessingError(String originalTopic) {
        try {
            // Counter: DLQ processing errors by topic
            Counter.builder("dlq.processing.errors")
                .tag("topic", originalTopic)
                .description("Count of errors during DLQ message processing")
                .register(meterRegistry)
                .increment();

            logger.debug("📊 [METRICS-RECORDED] DLQ processing error | Topic: {}", originalTopic);

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record DLQ processing error metric: {}", e.getMessage());
        }
    }

    /**
     * Records DLQ message processing duration.
     *
     * @param originalTopic the original source topic
     * @param durationMs the processing duration in milliseconds
     */
    public void recordDLQMessageProcessingTime(String originalTopic, long durationMs) {
        try {
            // Timer: DLQ message processing time by topic
            Timer.builder("dlq.message.processing.time")
                .tag("topic", originalTopic)
                .description("Duration to process a DLQ message")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);

            logger.debug("📊 [METRICS-RECORDED] DLQ processing time | Topic: {} | Duration: {}ms",
                originalTopic, durationMs);

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record DLQ processing time metric: {}", e.getMessage());
        }
    }

    /**
     * Records error type frequency for root cause analysis.
     *
     * @param originalTopic the original source topic
     * @param errorType the exception class name
     */
    public void recordErrorTypeFrequency(String originalTopic, String errorType) {
        try {
            // Counter: errors by type for RCA
            Counter.builder("dlq.error.type.frequency")
                .tag("topic", originalTopic)
                .tag("error_type", errorType != null ? errorType : "unknown")
                .description("Frequency of specific error types in DLQ")
                .register(meterRegistry)
                .increment();

            logger.debug("📊 [METRICS-RECORDED] Error type frequency | Topic: {} | ErrorType: {}",
                originalTopic, errorType);

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record error type frequency: {}", e.getMessage());
        }
    }

    /**
     * Records retry attempts for messages that were retried before DLQ.
     *
     * @param originalTopic the original source topic
     * @param retryCount number of retries before DLQ
     */
    public void recordRetryAttempts(String originalTopic, int retryCount) {
        try {
            // Gauge: average retry attempts per DLQ message
            if (retryCount > 0) {
                Counter.builder("dlq.retry.attempts")
                    .tag("topic", originalTopic)
                    .tag("retry_count", String.valueOf(retryCount))
                    .description("Count of DLQ messages by retry attempts before failure")
                    .register(meterRegistry)
                    .increment();

                logger.debug("📊 [METRICS-RECORDED] Retry attempts | Topic: {} | Retries: {}",
                    originalTopic, retryCount);
            }

        } catch (Exception e) {
            logger.error("⚠️ [METRICS-ERROR] Failed to record retry attempts: {}", e.getMessage());
        }
    }
}
