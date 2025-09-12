package com.lucid.automation.airouting.util;

import org.junit.jupiter.api.Test;

/**
 * Debug test for TimestampUtil
 */
class TimestampDebugTest {

    @Test
    void debugTimestampIssues() {
        System.out.println("=== Debugging Timestamp Issues ===");

        // Current time
        long currentEpochSeconds = System.currentTimeMillis() / 1000;
        System.out.println("Current epoch seconds: " + currentEpochSeconds);

        // Test the exact failing scenarios
        System.out.println("\n--- Testing Future Detection from Failing Test ---");
        long twoDaysFromNow = currentEpochSeconds + (2 * 24 * 60 * 60); // 2 days in future
        String futureTwoDaysStr = String.valueOf(twoDaysFromNow);

        System.out.println("Current time: " + currentEpochSeconds);
        System.out.println("Two days from now: " + twoDaysFromNow);
        System.out.println("Threshold: 86400 (1 day)");
        System.out.println("Difference: " + (twoDaysFromNow - currentEpochSeconds) + " seconds");
        System.out.println("Should be future: " + (twoDaysFromNow > currentEpochSeconds + 86400));

        boolean actualResult = TimestampUtil.isTimestampInFuture(futureTwoDaysStr, 86400);
        System.out.println("isTimestampInFuture result: " + actualResult);

        // Debug the parsing
        TimestampUtil.ParsedTimestamp parsed = TimestampUtil.parseTimestamp(futureTwoDaysStr);
        System.out.println("Parsed timestamp - Valid: " + parsed.isValid());
        System.out.println("Parsed timestamp - Epoch seconds: " + parsed.getEpochSeconds());
        System.out.println("Calculation: " + parsed.getEpochSeconds() + " > " + (currentEpochSeconds + 86400) + " = " +
                          (parsed.getEpochSeconds() > currentEpochSeconds + 86400));

        System.out.println("\n--- Testing Range Validation ---");
        String futureRangeTimestamp = "4102444801"; // January 1, 2100 00:00:01 UTC
        TimestampUtil.ParsedTimestamp rangeResult = TimestampUtil.parseTimestamp(futureRangeTimestamp);
        System.out.println("Range test timestamp: " + futureRangeTimestamp);
        System.out.println("Valid: " + rangeResult.isValid());
        System.out.println("Error message: " + rangeResult.getErrorMessage());
        System.out.println("MAX_VALID_EPOCH_SECONDS: " + 4102444800L);
        System.out.println("Test value: " + 4102444801L);
        System.out.println("Is over max: " + (4102444801L > 4102444800L));
    }
}