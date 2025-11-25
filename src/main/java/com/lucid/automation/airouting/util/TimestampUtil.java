package com.lucid.automation.airouting.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Utility class for handling different timestamp formats in the AI routing service.
 * Supports both millisecond timestamps (e.g., 1757316803000) and Slack format timestamps (e.g., 1757596600.850079).
 */
public class TimestampUtil {

    private static final Logger logger = LoggerFactory.getLogger(TimestampUtil.class);

    // Constants for timestamp validation
    private static final String TIMESTAMP_SEPARATOR = "\\.";
    private static final int TIMESTAMP_EPOCH_INDEX = 0;
    private static final long MIN_VALID_EPOCH_SECONDS = 946684800L; // January 1, 2000 00:00:00 UTC
    private static final long MAX_VALID_EPOCH_SECONDS = 4102444800L; // January 1, 2100 00:00:00 UTC

    /**
     * Represents the result of parsing a timestamp string
     */
    public static class ParsedTimestamp {
        private final long epochSeconds;
        private final long nanoSeconds;
        private final TimestampFormat format;
        private final boolean valid;
        private final String errorMessage;

        private ParsedTimestamp(long epochSeconds, long nanoSeconds, TimestampFormat format, boolean valid, String errorMessage) {
            this.epochSeconds = epochSeconds;
            this.nanoSeconds = nanoSeconds;
            this.format = format;
            this.valid = valid;
            this.errorMessage = errorMessage;
        }

        public static ParsedTimestamp success(long epochSeconds, long nanoSeconds, TimestampFormat format) {
            return new ParsedTimestamp(epochSeconds, nanoSeconds, format, true, null);
        }

        public static ParsedTimestamp failure(String errorMessage) {
            return new ParsedTimestamp(0, 0, TimestampFormat.UNKNOWN, false, errorMessage);
        }

        public boolean isValid() { return valid; }
        public long getEpochSeconds() { return epochSeconds; }
        public long getNanoSeconds() { return nanoSeconds; }
        public TimestampFormat getFormat() { return format; }
        public String getErrorMessage() { return errorMessage; }

        public LocalDateTime toLocalDateTime() {
            if (!valid) {
                throw new IllegalStateException("Cannot convert invalid timestamp to LocalDateTime");
            }
            return LocalDateTime.ofEpochSecond(epochSeconds, (int) nanoSeconds, ZoneOffset.UTC);
        }

        public Instant toInstant() {
            if (!valid) {
                throw new IllegalStateException("Cannot convert invalid timestamp to Instant");
            }
            return Instant.ofEpochSecond(epochSeconds, nanoSeconds);
        }
    }

    /**
     * Enumeration of supported timestamp formats
     */
    public enum TimestampFormat {
        SLACK_FORMAT,    // e.g., "1757596600.850079" (seconds.microseconds)
        MILLISECONDS,    // e.g., "1757316803000" (milliseconds since epoch)
        UNKNOWN
    }

    /**
     * Parses a timestamp string and returns a ParsedTimestamp result.
     * Supports both Slack format (seconds.microseconds) and millisecond timestamps.
     *
     * @param timestampStr The timestamp string to parse
     * @return ParsedTimestamp containing the parsed result and metadata
     */
    public static ParsedTimestamp parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            return ParsedTimestamp.failure("Timestamp string is null or empty");
        }

        String trimmed = timestampStr.trim();

        try {
            // Check if it contains a decimal point (Slack format)
            if (trimmed.contains(".")) {
                return parseSlackTimestamp(trimmed);
            } else {
                return parseMillisecondTimestamp(trimmed);
            }
        } catch (Exception e) {
            logger.warn("Error parsing timestamp '{}': {}", timestampStr, e.getMessage());
            return ParsedTimestamp.failure("Parse error: " + e.getMessage());
        }
    }

    /**
     * Parses a Slack format timestamp (e.g., "1757596600.850079")
     */
    private static ParsedTimestamp parseSlackTimestamp(String timestampStr) {
        try {
            String[] parts = timestampStr.split(TIMESTAMP_SEPARATOR);
            if (parts.length != 2) {
                return ParsedTimestamp.failure("Invalid Slack timestamp format - expected 'seconds.microseconds'");
            }

            long epochSeconds = Long.parseLong(parts[TIMESTAMP_EPOCH_INDEX]);
            if (!isValidEpochSeconds(epochSeconds)) {
                return ParsedTimestamp.failure("Epoch seconds out of valid range: " + epochSeconds);
            }

            // Parse microseconds and convert to nanoseconds
            String microsecondsStr = parts[1];
            // Pad or truncate to 6 digits (microseconds)
            if (microsecondsStr.length() > 6) {
                microsecondsStr = microsecondsStr.substring(0, 6);
            } else {
                microsecondsStr = String.format("%-6s", microsecondsStr).replace(' ', '0');
            }

            long microseconds = Long.parseLong(microsecondsStr);
            long nanoSeconds = microseconds * 1000; // Convert microseconds to nanoseconds

            return ParsedTimestamp.success(epochSeconds, nanoSeconds, TimestampFormat.SLACK_FORMAT);

        } catch (NumberFormatException e) {
            return ParsedTimestamp.failure("Invalid number format in Slack timestamp: " + e.getMessage());
        }
    }

    /**
     * Parses a millisecond timestamp (e.g., "1757316803000")
     */
    private static ParsedTimestamp parseMillisecondTimestamp(String timestampStr) {
        try {
            long timestampMillis = Long.parseLong(timestampStr);

            // Convert milliseconds to seconds and nanoseconds
            long epochSeconds = timestampMillis / 1000;
            long remainingMillis = timestampMillis % 1000;
            long nanoSeconds = remainingMillis * 1_000_000; // Convert remaining milliseconds to nanoseconds

            if (!isValidEpochSeconds(epochSeconds)) {
                return ParsedTimestamp.failure("Epoch seconds out of valid range: " + epochSeconds);
            }

            return ParsedTimestamp.success(epochSeconds, nanoSeconds, TimestampFormat.MILLISECONDS);

        } catch (NumberFormatException e) {
            return ParsedTimestamp.failure("Invalid number format in millisecond timestamp: " + e.getMessage());
        }
    }

    /**
     * Validates if the epoch seconds are within a reasonable range
     */
    private static boolean isValidEpochSeconds(long epochSeconds) {
        return epochSeconds >= MIN_VALID_EPOCH_SECONDS && epochSeconds <= MAX_VALID_EPOCH_SECONDS;
    }

    /**
     * Parses a timestamp string and returns epoch seconds for filtering operations.
     * Returns the current time if parsing fails.
     *
     * @param timestampStr The timestamp string to parse
     * @return Epoch seconds, or current time if parsing fails
     */
    public static long parseToEpochSeconds(String timestampStr) {
        ParsedTimestamp parsed = parseTimestamp(timestampStr);
        if (parsed.isValid()) {
            return parsed.getEpochSeconds();
        } else {
            logger.warn("Failed to parse timestamp '{}', using current time: {}", timestampStr, parsed.getErrorMessage());
            return System.currentTimeMillis() / 1000;
        }
    }

    /**
     * Parses a timestamp string and returns a LocalDateTime.
     * Returns null if parsing fails.
     *
     * @param timestampStr The timestamp string to parse
     * @return LocalDateTime or null if parsing fails
     */
    public static LocalDateTime parseToLocalDateTime(String timestampStr) {
        ParsedTimestamp parsed = parseTimestamp(timestampStr);
        if (parsed.isValid()) {
            return parsed.toLocalDateTime();
        } else {
            logger.warn("Failed to parse timestamp '{}' to LocalDateTime: {}", timestampStr, parsed.getErrorMessage());
            return null;
        }
    }

    /**
     * Checks if a timestamp is in the future by more than the specified threshold.
     *
     * @param timestampStr The timestamp string to check
     * @param thresholdSeconds Maximum allowed seconds in the future
     * @return true if timestamp is too far in the future
     */
    public static boolean isTimestampInFuture(String timestampStr, long thresholdSeconds) {
        ParsedTimestamp parsed = parseTimestamp(timestampStr);
        if (!parsed.isValid()) {
            return false; // If we can't parse it, don't consider it invalid due to future date
        }

        long currentEpochSeconds = System.currentTimeMillis() / 1000;
        return parsed.getEpochSeconds() > currentEpochSeconds + thresholdSeconds;
    }

    /**
     * Validates if a timestamp string is in a supported format and within valid range.
     *
     * @param timestampStr The timestamp string to validate
     * @return true if the timestamp is valid
     */
    public static boolean isValidTimestamp(String timestampStr) {
        return parseTimestamp(timestampStr).isValid();
    }

    /**
     * Gets a human-readable description of the timestamp format.
     *
     * @param timestampStr The timestamp string to analyze
     * @return Format description
     */
    public static String getTimestampFormatDescription(String timestampStr) {
        ParsedTimestamp parsed = parseTimestamp(timestampStr);
        if (!parsed.isValid()) {
            return "Invalid: " + parsed.getErrorMessage();
        }

        switch (parsed.getFormat()) {
            case SLACK_FORMAT:
                return "Slack format (seconds.microseconds)";
            case MILLISECONDS:
                return "Milliseconds since epoch";
            default:
                return "Unknown format";
        }
    }
}