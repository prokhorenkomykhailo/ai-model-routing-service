package com.lucid.automation.airouting.util;

/**
 * Simple debug class to test timestamp parsing
 */
public class TimestampDebug {
    public static void main(String[] args) {
        // Debug range constants
        System.out.println("MIN_VALID_EPOCH_SECONDS: 946684800 (Jan 1, 2000)");
        System.out.println("MAX_VALID_EPOCH_SECONDS: 4102444800 (Jan 1, 2100)");

        // Current time
        long currentTime = System.currentTimeMillis() / 1000;
        System.out.println("Current time: " + currentTime);

        // Test 2 days in future
        long twoDaysFromNow = currentTime + (2 * 24 * 60 * 60);
        System.out.println("Two days from now: " + twoDaysFromNow);
        System.out.println("Is " + twoDaysFromNow + " >= 946684800? " + (twoDaysFromNow >= 946684800L));
        System.out.println("Is " + twoDaysFromNow + " <= 4102444800? " + (twoDaysFromNow <= 4102444800L));

        // Test parsing
        TimestampUtil.ParsedTimestamp result = TimestampUtil.parseTimestamp(String.valueOf(twoDaysFromNow));
        System.out.println("Parse result valid: " + result.isValid());
        System.out.println("Parse result error: " + result.getErrorMessage());

        // Test future check
        boolean isFuture = TimestampUtil.isTimestampInFuture(String.valueOf(twoDaysFromNow), 86400);
        System.out.println("Is future (threshold 86400): " + isFuture);
        System.out.println("Should be true because " + twoDaysFromNow + " > " + (currentTime + 86400) + " = " + (twoDaysFromNow > currentTime + 86400));

        // Test the specific timestamps from comments
        System.out.println("\nTesting specific timestamps:");

        // Millisecond timestamp
        String millisTs = "1757316803000";
        TimestampUtil.ParsedTimestamp millisResult = TimestampUtil.parseTimestamp(millisTs);
        System.out.println("Millis timestamp (" + millisTs + "): valid=" + millisResult.isValid() +
                          ", error=" + millisResult.getErrorMessage() +
                          ", format=" + millisResult.getFormat());

        // Slack timestamp
        String slackTs = "1757596600.850079";
        TimestampUtil.ParsedTimestamp slackResult = TimestampUtil.parseTimestamp(slackTs);
        System.out.println("Slack timestamp (" + slackTs + "): valid=" + slackResult.isValid() +
                          ", error=" + slackResult.getErrorMessage() +
                          ", format=" + slackResult.getFormat());
    }
}