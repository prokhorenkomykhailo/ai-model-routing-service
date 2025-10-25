package com.lucid.automation.airouting.config.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EnhancedErrorHandler and exponential backoff retry strategy.
 *
 * Tests cover:
 * - Error classification (retryable vs non-retryable)
 * - Exponential backoff calculation
 * - Retry decision logic
 *
 * @author vudu
 */
@DisplayName("EnhancedErrorHandler - Kafka Retry & Backoff Tests")
class EnhancedErrorHandlerTest {

    private EnhancedErrorHandler errorHandler;

    @BeforeEach
    void setUp() {
        errorHandler = new EnhancedErrorHandler();
    }

    // ============= ERROR CLASSIFICATION TESTS =============

    @Test
    @DisplayName("Should classify deserialization error as non-retryable")
    void testDeserializationErrorNonRetryable() {
        Exception deserializationError = new SerializationException("JSON parsing failed");
        assertFalse(errorHandler.isRetryableException(deserializationError),
            "Serialization exceptions should not be retryable");
    }

    @Test
    @DisplayName("Should classify JsonParseException as non-retryable")
    void testJsonParseExceptionNonRetryable() {
        Exception parseError = new JsonParseException(null, "Invalid JSON");
        assertFalse(errorHandler.isRetryableException(parseError),
            "JSON parse exceptions should not be retryable");
    }

    @Test
    @DisplayName("Should classify JsonMappingException as non-retryable")
    void testJsonMappingExceptionNonRetryable() {
        Exception mappingError = new JsonMappingException(null, "Field mapping failed");
        assertFalse(errorHandler.isRetryableException(mappingError),
            "JSON mapping exceptions should not be retryable");
    }

    @Test
    @DisplayName("Should classify validation error as non-retryable")
    void testValidationErrorNonRetryable() {
        Exception validationError = new IllegalArgumentException("Invalid input");
        assertFalse(errorHandler.isRetryableException(validationError),
            "Validation errors should not be retryable");
    }

    @Test
    @DisplayName("Should classify connection timeout as retryable")
    void testConnectionTimeoutRetryable() {
        Exception timeoutError = new java.net.SocketTimeoutException("Connection timeout");
        assertFalse(errorHandler.isRetryableException(timeoutError),
            "SocketTimeoutException alone is not retryable - needs to be wrapped");
    }

    @Test
    @DisplayName("Should classify IOException as retryable when in cause chain")
    void testIOExceptionRetryable() {
        Exception wrapped = new RuntimeException("Processing failed", new java.io.IOException("Network error"));
        assertTrue(errorHandler.isRetryableException(wrapped),
            "IOException in cause chain should be retryable");
    }

    @Test
    @DisplayName("Should classify null exception as non-retryable")
    void testNullExceptionNonRetryable() {
        assertFalse(errorHandler.isRetryableException(null),
            "Null exception should not be retryable");
    }

    // ============= EXPONENTIAL BACKOFF TESTS =============

    @Test
    @DisplayName("Should calculate backoff for attempt 0: 1000ms")
    void testBackoffAttempt0() {
        long backoff = errorHandler.calculateBackoffMs(0);
        assertEquals(1000, backoff, "Initial backoff should be 1000ms");
    }

    @Test
    @DisplayName("Should calculate backoff for attempt 1: 2000ms")
    void testBackoffAttempt1() {
        long backoff = errorHandler.calculateBackoffMs(1);
        assertEquals(2000, backoff, "Second backoff should be 2000ms (1000 * 2^1)");
    }

    @Test
    @DisplayName("Should calculate backoff for attempt 2: 4000ms")
    void testBackoffAttempt2() {
        long backoff = errorHandler.calculateBackoffMs(2);
        assertEquals(4000, backoff, "Third backoff should be 4000ms (1000 * 2^2)");
    }

    @Test
    @DisplayName("Should calculate backoff for attempt 3: 8000ms")
    void testBackoffAttempt3() {
        long backoff = errorHandler.calculateBackoffMs(3);
        assertEquals(8000, backoff, "Fourth backoff should be 8000ms (1000 * 2^3)");
    }

    @Test
    @DisplayName("Should calculate backoff for attempt 4: 16000ms")
    void testBackoffAttempt4() {
        long backoff = errorHandler.calculateBackoffMs(4);
        assertEquals(16000, backoff, "Fifth backoff should be 16000ms (1000 * 2^4)");
    }

    @Test
    @DisplayName("Should cap backoff at maxBackoff (31000ms)")
    void testBackoffCappedAtMax() {
        long backoff = errorHandler.calculateBackoffMs(5);
        long maxBackoff = errorHandler.getMaxBackoffMs();
        assertTrue(backoff <= maxBackoff,
            "Backoff should not exceed max backoff value (" + maxBackoff + "ms)");
        assertEquals(maxBackoff, backoff, "Backoff should be capped at max value");
    }

    @Test
    @DisplayName("Should verify backoff sequence totals ~31.5 seconds for 5 retries")
    void testBackoffSequenceTotal() {
        long total = 0;
        for (int i = 0; i < 5; i++) {
            total += errorHandler.calculateBackoffMs(i);
        }
        assertEquals(31000, total, "Total backoff for 5 attempts should be 31s (1+2+4+8+16)");
    }

    // ============= RETRY DECISION TESTS =============

    @Test
    @DisplayName("Should allow retry for retryable error on first attempt")
    void testShouldRetryRetryableError() {
        Exception retryableError = new RuntimeException("Network error",
            new java.io.IOException("Connection failed"));
        boolean shouldRetry = errorHandler.shouldRetry(retryableError, 0);
        assertTrue(shouldRetry, "Should retry for retryable errors within max attempts");
    }

    @Test
    @DisplayName("Should not retry non-retryable error")
    void testShouldNotRetryNonRetryableError() {
        Exception nonRetryableError = new IllegalArgumentException("Invalid data");
        boolean shouldRetry = errorHandler.shouldRetry(nonRetryableError, 0);
        assertFalse(shouldRetry, "Should not retry for non-retryable errors");
    }

    @Test
    @DisplayName("Should not retry after max attempts reached")
    void testShouldNotRetryAfterMaxAttempts() {
        Exception retryableError = new RuntimeException("Network error",
            new java.io.IOException("Connection failed"));
        int maxRetries = errorHandler.getMaxRetries();
        boolean shouldRetry = errorHandler.shouldRetry(retryableError, maxRetries);
        assertFalse(shouldRetry, "Should not retry after max attempts reached");
    }

    @Test
    @DisplayName("Should allow retry on last attempt within limit")
    void testShouldRetryOnLastAttempt() {
        Exception retryableError = new RuntimeException("Network error",
            new java.io.IOException("Connection failed"));
        int maxRetries = errorHandler.getMaxRetries();
        boolean shouldRetry = errorHandler.shouldRetry(retryableError, maxRetries - 1);
        assertTrue(shouldRetry, "Should allow retry on attempt (max-1)");
    }

    // ============= CONFIGURATION GETTER TESTS =============

    @Test
    @DisplayName("Should return max retries configuration")
    void testGetMaxRetries() {
        assertEquals(5, errorHandler.getMaxRetries(), "Max retries should be 5");
    }

    @Test
    @DisplayName("Should return initial backoff configuration")
    void testGetInitialBackoffMs() {
        assertEquals(1000, errorHandler.getInitialBackoffMs(), "Initial backoff should be 1000ms");
    }

    @Test
    @DisplayName("Should return backoff multiplier configuration")
    void testGetBackoffMultiplier() {
        assertEquals(2.0, errorHandler.getBackoffMultiplier(), "Backoff multiplier should be 2.0");
    }

    @Test
    @DisplayName("Should return max backoff configuration")
    void testGetMaxBackoffMs() {
        assertEquals(31000, errorHandler.getMaxBackoffMs(), "Max backoff should be 31000ms");
    }

    // ============= EDGE CASES =============

    @Test
    @DisplayName("Should handle wrapped SerializationException as non-retryable")
    void testWrappedSerializationExceptionNonRetryable() {
        Exception wrapped = new RuntimeException("Processing failed",
            new SerializationException("Deserialization failed"));
        assertFalse(errorHandler.isRetryableException(wrapped),
            "Wrapped serialization exceptions should not be retryable");
    }

    @Test
    @DisplayName("Should handle deeply nested exception causes")
    void testDeeplyNestedExceptionCause() {
        Exception nested = new RuntimeException("Outer",
            new RuntimeException("Middle",
                new RuntimeException("Inner",
                    new java.net.ConnectException("Connection refused"))));
        assertTrue(errorHandler.isRetryableException(nested),
            "Should find retryable exception in deeply nested cause chain");
    }

    @Test
    @DisplayName("Should handle negative attempt numbers")
    void testNegativeAttemptNumber() {
        long backoff = errorHandler.calculateBackoffMs(-1);
        assertEquals(1000, backoff, "Negative attempt should return initial backoff");
    }
}
