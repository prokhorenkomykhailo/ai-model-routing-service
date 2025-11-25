package com.lucid.automation.airouting.util;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TimestampUtil
 */
class TimestampUtilTest {

    @Test
    void testParseSlackTimestamp() {
        // Test Slack format: 1757596600.850079
        String slackTimestamp = "1757596600.850079";

        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(slackTimestamp);

        assertTrue(result.isValid());
        assertEquals(TimestampUtil.TimestampFormat.SLACK_FORMAT, result.getFormat());
        assertEquals(1757596600L, result.getEpochSeconds());
        assertEquals(850079000L, result.getNanoSeconds()); // 850079 microseconds = 850079000 nanoseconds
    }

    @Test
    void testParseMillisecondTimestamp() {
        // Test millisecond format: 1757316803000
        String millisTimestamp = "1757316803000";

        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(millisTimestamp);

        assertTrue(result.isValid());
        assertEquals(TimestampUtil.TimestampFormat.MILLISECONDS, result.getFormat());
        assertEquals(1757316803L, result.getEpochSeconds());
        assertEquals(0L, result.getNanoSeconds());
    }

    @Test
    void testParseMillisecondTimestampWithRemainder() {
        // Test millisecond format with non-zero remainder: 1757316803123
        String millisTimestamp = "1757316803123";

        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(millisTimestamp);

        assertTrue(result.isValid());
        assertEquals(TimestampUtil.TimestampFormat.MILLISECONDS, result.getFormat());
        assertEquals(1757316803L, result.getEpochSeconds());
        assertEquals(123000000L, result.getNanoSeconds()); // 123 milliseconds = 123000000 nanoseconds
    }

    @Test
    void testParseInvalidTimestamp() {
        String invalidTimestamp = "not-a-timestamp";

        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(invalidTimestamp);

        assertFalse(result.isValid());
        assertNotNull(result.getErrorMessage());
    }

    @Test
    void testParseNullTimestamp() {
        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(null);

        assertFalse(result.isValid());
        assertEquals("Timestamp string is null or empty", result.getErrorMessage());
    }

    @Test
    void testParseEmptyTimestamp() {
        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp("");

        assertFalse(result.isValid());
        assertEquals("Timestamp string is null or empty", result.getErrorMessage());
    }

    @Test
    void testParseToEpochSeconds() {
        // Test both formats
        assertEquals(1757596600L, TimestampUtil.parseToEpochSeconds("1757596600.850079"));
        assertEquals(1757316803L, TimestampUtil.parseToEpochSeconds("1757316803000"));

        // Test invalid timestamp returns current time (approximately)
        long currentTime = System.currentTimeMillis() / 1000;
        long parsedTime = TimestampUtil.parseToEpochSeconds("invalid");
        assertTrue(Math.abs(parsedTime - currentTime) < 10); // Within 10 seconds
    }

    @Test
    void testParseToLocalDateTime() {
        // Test Slack format
        LocalDateTime slackDateTime = TimestampUtil.parseToLocalDateTime("1757596600.850079");
        assertNotNull(slackDateTime);
        assertEquals(LocalDateTime.ofEpochSecond(1757596600L, 850079000, ZoneOffset.UTC), slackDateTime);

        // Test millisecond format
        LocalDateTime millisDateTime = TimestampUtil.parseToLocalDateTime("1757316803000");
        assertNotNull(millisDateTime);
        assertEquals(LocalDateTime.ofEpochSecond(1757316803L, 0, ZoneOffset.UTC), millisDateTime);

        // Test invalid timestamp returns null
        assertNull(TimestampUtil.parseToLocalDateTime("invalid"));
    }

    @Test
    void testIsTimestampInFuture() {
        // Test with a timestamp that's exactly 2 days in the future (within valid range)
        // Use millisecond format since that's one of our supported formats
        long currentTimeMillis = System.currentTimeMillis();
        long twoDaysFromNowMillis = currentTimeMillis + (2 * 24 * 60 * 60 * 1000L); // 2 days in future (milliseconds)
        String futureTwoDaysStr = String.valueOf(twoDaysFromNowMillis);

        assertTrue(TimestampUtil.isTimestampInFuture(futureTwoDaysStr, 86400)); // 1 day threshold, so 2 days should be future

        // Test timestamp that's only slightly in future (within threshold)
        long oneHourFromNowMillis = currentTimeMillis + (3600 * 1000L); // 1 hour in future (milliseconds)
        String nearFutureTimestampStr = String.valueOf(oneHourFromNowMillis);

        assertFalse(TimestampUtil.isTimestampInFuture(nearFutureTimestampStr, 86400)); // 24 hours threshold

        // Test past timestamp
        long pastTimestampMillis = currentTimeMillis - (3600 * 1000L); // 1 hour ago (milliseconds)
        String pastTimestampStr = String.valueOf(pastTimestampMillis);

        assertFalse(TimestampUtil.isTimestampInFuture(pastTimestampStr, 86400));

        // Test invalid timestamp (should return false)
        assertFalse(TimestampUtil.isTimestampInFuture("invalid", 86400));
    }

    @Test
    void testIsValidTimestamp() {
        assertTrue(TimestampUtil.isValidTimestamp("1757596600.850079"));
        assertTrue(TimestampUtil.isValidTimestamp("1757316803000"));
        assertFalse(TimestampUtil.isValidTimestamp("invalid"));
        assertFalse(TimestampUtil.isValidTimestamp(null));
        assertFalse(TimestampUtil.isValidTimestamp(""));
    }

    @Test
    void testGetTimestampFormatDescription() {
        assertEquals("Slack format (seconds.microseconds)",
                    TimestampUtil.getTimestampFormatDescription("1757596600.850079"));
        assertEquals("Milliseconds since epoch",
                    TimestampUtil.getTimestampFormatDescription("1757316803000"));
        assertTrue(TimestampUtil.getTimestampFormatDescription("invalid").startsWith("Invalid:"));
    }

    @Test
    void testSlackTimestampWithVariableMicroseconds() {
        // Test with different microsecond precision
        String timestamp1 = "1757596600.1"; // 1 digit
        String timestamp2 = "1757596600.12"; // 2 digits
        String timestamp3 = "1757596600.123456"; // 6 digits
        String timestamp4 = "1757596600.1234567890"; // More than 6 digits

        TimestampUtil.ParsedTimestamp result1 = TimestampUtil.parseTimestamp(timestamp1);
        TimestampUtil.ParsedTimestamp result2 = TimestampUtil.parseTimestamp(timestamp2);
        TimestampUtil.ParsedTimestamp result3 = TimestampUtil.parseTimestamp(timestamp3);
        TimestampUtil.ParsedTimestamp result4 = TimestampUtil.parseTimestamp(timestamp4);

        assertTrue(result1.isValid());
        assertTrue(result2.isValid());
        assertTrue(result3.isValid());
        assertTrue(result4.isValid());

        // Check nanoseconds are correctly padded/truncated
        assertEquals(100000000L, result1.getNanoSeconds()); // 100000 microseconds
        assertEquals(120000000L, result2.getNanoSeconds()); // 120000 microseconds
        assertEquals(123456000L, result3.getNanoSeconds()); // 123456 microseconds
        assertEquals(123456000L, result4.getNanoSeconds()); // Truncated to 123456 microseconds
    }

    @Test
    void testTimestampRangeValidation() {
        // Test timestamp too old (before year 2000)
        String oldTimestamp = "946684799"; // December 31, 1999 23:59:59 UTC
        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(oldTimestamp);
        assertFalse(result.isValid());
        assertTrue(result.getErrorMessage().contains("out of valid range"));

        // Test timestamp too far in future (after year 2100)
        // Use millisecond format: 4102444801000 milliseconds = 4102444801 seconds = January 1, 2100 00:00:01 UTC
        String futureTimestamp = "4102444801000"; // January 1, 2100 00:00:01 UTC in milliseconds
        TimestampUtil.ParsedTimestamp result2 = TimestampUtil.parseTimestamp(futureTimestamp);
        assertFalse(result2.isValid());
        assertTrue(result2.getErrorMessage().contains("out of valid range"));

        // Test valid timestamp (current era) in milliseconds
        String validTimestamp = "1609459200000"; // January 1, 2021 00:00:00 UTC in milliseconds
        TimestampUtil.ParsedTimestamp result3 = TimestampUtil.parseTimestamp(validTimestamp);
        assertTrue(result3.isValid());

        // Test the specific timestamp formats from the user's example
        // 1757316803000 - this is milliseconds, should be valid when parsed as milliseconds
        String millisTimestamp = "1757316803000";
        TimestampUtil.ParsedTimestamp result4 = TimestampUtil.parseTimestamp(millisTimestamp);
        assertTrue(result4.isValid(), "Millisecond timestamp should be valid: " + result4.getErrorMessage());
        assertEquals(TimestampUtil.TimestampFormat.MILLISECONDS, result4.getFormat());

        // 1757596600.850079 - this is Slack format, should be valid
        String slackTimestamp = "1757596600.850079";
        TimestampUtil.ParsedTimestamp result5 = TimestampUtil.parseTimestamp(slackTimestamp);
        assertTrue(result5.isValid(), "Slack timestamp should be valid: " + result5.getErrorMessage());
        assertEquals(TimestampUtil.TimestampFormat.SLACK_FORMAT, result5.getFormat());
    }
}