package com.lucid.automation.airouting.logging;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.regex.Pattern;

/**
 * Custom Logback converter for sanitizing PII (Personally Identifiable Information) in log messages.
 * This converter removes or masks sensitive information like email addresses, phone numbers, etc.
 */
public class PiiSanitizingConverter extends ClassicConverter {

    // Pattern for email addresses
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
        "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b"
    );

    // Pattern for phone numbers (simple pattern for US format)
    private static final Pattern PHONE_PATTERN = Pattern.compile(
        "\\b(?:\\+?1[-.]?)?\\(?[0-9]{3}\\)?[-.]?[0-9]{3}[-.]?[0-9]{4}\\b"
    );

    // Pattern for potential SSN (XXX-XX-XXXX format)
    private static final Pattern SSN_PATTERN = Pattern.compile(
        "\\b\\d{3}-\\d{2}-\\d{4}\\b"
    );

    // Pattern for credit card numbers (simple 16-digit pattern)
    private static final Pattern CREDIT_CARD_PATTERN = Pattern.compile(
        "\\b\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}[\\s-]?\\d{4}\\b"
    );

    @Override
    public String convert(ILoggingEvent event) {
        String message = event.getFormattedMessage();

        if (message == null) {
            return null;
        }

        return sanitizeMessage(message);
    }

    /**
     * Sanitizes the message by masking or removing PII data.
     *
     * @param message The original message
     * @return The sanitized message
     */
    private String sanitizeMessage(String message) {
        // Replace email addresses with [EMAIL]
        message = EMAIL_PATTERN.matcher(message).replaceAll("[EMAIL]");

        // Replace phone numbers with [PHONE]
        message = PHONE_PATTERN.matcher(message).replaceAll("[PHONE]");

        // Replace SSN with [SSN]
        message = SSN_PATTERN.matcher(message).replaceAll("[SSN]");

        // Replace credit card numbers with [CARD]
        message = CREDIT_CARD_PATTERN.matcher(message).replaceAll("[CARD]");

        return message;
    }
}
