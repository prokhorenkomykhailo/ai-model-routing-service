package com.lucid.automation.airouting.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enhanced error handler for Kafka listeners implementing exponential backoff strategy.
 * Classifies exceptions as retryable or non-retryable and computes backoff timing.
 *
 * Retry Strategy:
 * - Retryable errors (connection, timeout): NO ACK → Kafka session timeout → rebalance processes
 * - Non-retryable errors (JSON parse, validation): ACK + DLQ dispatch
 * - Backoff timing: exponential (1s → 2s → 4s → 8s → 16s) with max 31s
 *
 * @author vudu
 * @since 1.2.6
 */
@Component
public class EnhancedErrorHandler implements ConsumerAwareListenerErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedErrorHandler.class);

    // Configuration constants
    private static final int INITIAL_BACKOFF_MS = 1000;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final int MAX_BACKOFF_MS = 31000;
    private static final int MAX_RETRIES = 5;

    /**
     * Classifies an exception as retryable or non-retryable by examining
     * the exception type and cause chain.
     *
     * @param exception the exception to classify
     * @return true if retryable (connection/timeout issues), false if non-retryable (validation/parse errors)
     */
    public boolean isRetryableException(Throwable exception) {
        if (exception == null) {
            return false;
        }

        // Check exception class hierarchy for serialization/parse errors (non-retryable)
        String exceptionClassName = exception.getClass().getName().toLowerCase();
        if (exceptionClassName.contains("serialization") ||
            exceptionClassName.contains("jsonparse") ||
            exceptionClassName.contains("mappingexception") ||
            exceptionClassName.contains("parse")) {
            return false;
        }

        // Check message for keywords indicating validation errors (non-retryable)
        if (exception.getMessage() != null) {
            String msg = exception.getMessage().toLowerCase();
            if (msg.contains("validation") ||
                msg.contains("illegal argument") ||
                msg.contains("cannot deserialize")) {
                return false;
            }
        }

        // Check exception type for transient connection/network errors (retryable)
        if (exception instanceof java.net.ConnectException ||
            exception instanceof java.net.SocketTimeoutException ||
            exception instanceof java.io.IOException ||
            exception instanceof java.util.concurrent.TimeoutException) {
            return true;
        }

        // Check message for connection/timeout keywords (retryable)
        if (exception.getMessage() != null) {
            String msg = exception.getMessage().toLowerCase();
            if (msg.contains("connection") ||
                msg.contains("timeout") ||
                msg.contains("network") ||
                msg.contains("unable to connect") ||
                msg.contains("connection refused") ||
                msg.contains("reset")) {
                return true;
            }
        }

        // Check cause chain for retryable exceptions
        Throwable cause = exception.getCause();
        while (cause != null) {
            if (isRetryableException(cause)) {
                return true;
            }
            cause = cause.getCause();
        }

        // Default to non-retryable for unknown exceptions
        return false;
    }

    /**
     * Calculates exponential backoff duration in milliseconds.
     * Formula: min(1000 * 2^attemptNumber, 31000)
     *
     * @param attemptNumber the retry attempt number (0-based)
     * @return backoff duration in milliseconds
     */
    public long calculateBackoffMs(int attemptNumber) {
        // Ensure non-negative attempt number
        if (attemptNumber < 0) {
            return INITIAL_BACKOFF_MS;
        }

        // Calculate exponential backoff
        long backoffMs = (long) (INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, attemptNumber));

        // Cap at maximum backoff
        return Math.min(backoffMs, MAX_BACKOFF_MS);
    }

    /**
     * Determines whether to retry based on exception type and retry count.
     *
     * @param exception the exception that occurred
     * @param retryCount the number of retries already attempted
     * @return true if should retry, false if should give up
     */
    public boolean shouldRetry(Exception exception, int retryCount) {
        // Don't retry if max attempts exceeded
        if (retryCount >= MAX_RETRIES) {
            logger.debug("❌ [RETRY-EXHAUSTED] Max retries ({}) exhausted for exception: {}",
                MAX_RETRIES, exception.getMessage());
            return false;
        }

        // Only retry if exception is classified as retryable
        boolean retryable = isRetryableException(exception);
        if (!retryable) {
            logger.debug("❌ [NON-RETRYABLE] Exception classified as non-retryable: {}",
                exception.getMessage());
        }

        return retryable;
    }

    /**
     * Returns the maximum number of retry attempts allowed.
     *
     * @return max retry attempts (5)
     */
    public int getMaxRetries() {
        return MAX_RETRIES;
    }

    /**
     * Returns the initial backoff duration in milliseconds.
     *
     * @return initial backoff (1000ms)
     */
    public int getInitialBackoffMs() {
        return INITIAL_BACKOFF_MS;
    }

    /**
     * Returns the backoff multiplier for exponential calculation.
     *
     * @return backoff multiplier (2.0)
     */
    public double getBackoffMultiplier() {
        return BACKOFF_MULTIPLIER;
    }

    /**
     * Returns the maximum backoff duration in milliseconds.
     *
     * @return max backoff (31000ms)
     */
    public int getMaxBackoffMs() {
        return MAX_BACKOFF_MS;
    }

    /**
     * Implements ConsumerAwareListenerErrorHandler for Spring Kafka integration.
     * This method is called when a listener throws an exception.
     *
     * Note: Actual retry/DLQ logic is handled by consumers using this class.
     * This implementation provides the classification and timing calculations.
     *
     * @param message the message that caused the exception
     * @param exception the exception that occurred
     * @param consumer the Kafka consumer
     * @return null (error handling logic is in consumers)
     */
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        logger.warn("🚨 [ERROR-HANDLER] Kafka listener error | Error: {}", exception.getMessage());
        return null;
    }
}
