package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service for counting tokens in messages to ensure proper AI processing limits.
 * This service provides accurate token counting for different types of content
 * to prevent exceeding AI provider token limits.
 */
@Service
public class TokenCountingService {

    private static final Logger logger = LoggerFactory.getLogger(TokenCountingService.class);

    // Approximate token counts per character for different content types
    // Based on GPT-4 tokenization patterns where 1 token ≈ 0.75 words ≈ 4 characters
    private static final double TOKENS_PER_CHARACTER = 0.25;
    private static final double STRUCTURED_DATA_MULTIPLIER = 0.8; // Structured data is more token-efficient
    private static final double SLACK_FORMATTING_OVERHEAD = 1.2; // Slack markup adds overhead

    @Value("${sliding.window.token.counting.estimation.method:character_based}")
    private String estimationMethod;

    @Value("${sliding.window.token.counting.safety.margin:0.1}")
    private double safetyMargin;

    /**
     * Counts the total tokens for a list of messages.
     * This includes message content, user information, and metadata.
     *
     * @param messages List of messages to count tokens for
     * @return Estimated total token count
     */
    public int countTokensForMessages(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }

        int totalTokens = 0;
        for (Message message : messages) {
            totalTokens += countTokensForMessage(message);
        }

        // Apply safety margin to account for tokenization variations
        int adjustedTokens = (int) Math.ceil(totalTokens * (1 + safetyMargin));

        logger.debug("Counted tokens for {} messages: {} raw tokens, {} with safety margin",
                    messages.size(), totalTokens, adjustedTokens);

        return adjustedTokens;
    }

    /**
     * Counts tokens for a single message including all its content and metadata.
     *
     * @param message Message to count tokens for
     * @return Estimated token count for the message
     */
    public int countTokensForMessage(Message message) {
        if (message == null) {
            return 0;
        }

        int tokens = 0;

        // Message content (main text)
        tokens += countTokensForText(message.getText());

        // User information (formatted for AI context)
        tokens += countTokensForUserInfo(message);

        // Message metadata
        tokens += countTokensForMetadata(message);

        // Channel and thread context
        tokens += countTokensForContext(message);

        return tokens;
    }

    /**
     * Counts tokens for text content using character-based estimation.
     *
     * @param text Text to count tokens for
     * @return Estimated token count
     */
    public int countTokensForText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }

        // Clean the text and apply Slack formatting considerations
        String cleanText = cleanSlackFormatting(text);
        int baseTokens = (int) Math.ceil(cleanText.length() * TOKENS_PER_CHARACTER);

        // Apply Slack formatting overhead if original text had markup
        if (hasSlackFormatting(text)) {
            baseTokens = (int) Math.ceil(baseTokens * SLACK_FORMATTING_OVERHEAD);
        }

        return Math.max(1, baseTokens); // Minimum 1 token for non-empty text
    }

    /**
     * Counts tokens for user information that gets included in AI context.
     *
     * @param message Message containing user information
     * @return Estimated token count for user info
     */
    private int countTokensForUserInfo(Message message) {
        int tokens = 0;

        // User identification
        tokens += countTokensForText(message.getUsername());
        tokens += countTokensForText(message.getDisplayName());
        tokens += countTokensForText(message.getName());

        // User role/title information
        tokens += countTokensForText(message.getTitle());

        // Email (if included in context)
        if (message.getEmail() != null && !message.getEmail().isEmpty()) {
            tokens += countTokensForText(message.getEmail());
        }

        // Apply structured data multiplier (user info is structured)
        return (int) Math.ceil(tokens * STRUCTURED_DATA_MULTIPLIER);
    }

    /**
     * Counts tokens for message metadata.
     *
     * @param message Message containing metadata
     * @return Estimated token count for metadata
     */
    private int countTokensForMetadata(Message message) {
        int tokens = 0;

        // Message type and subtype
        tokens += countTokensForText(message.getMessageType());
        tokens += countTokensForText(message.getSubtype());

        // Timestamps (formatted for AI)
        if (message.getMessageTs() != null) {
            tokens += 2; // Timestamp representation
        }

        // Source information
        tokens += countTokensForText(message.getSource());

        // Additional metadata if present
        if (message.getMetadata() != null && !message.getMetadata().isEmpty()) {
            // Approximate token count for metadata map
            tokens += message.getMetadata().size() * 3; // Key-value pairs
        }

        return (int) Math.ceil(tokens * STRUCTURED_DATA_MULTIPLIER);
    }

    /**
     * Counts tokens for message context (channel, thread info).
     *
     * @param message Message containing context information
     * @return Estimated token count for context
     */
    private int countTokensForContext(Message message) {
        int tokens = 0;

        // Channel information
        tokens += countTokensForText(message.getChannelName());
        tokens += countTokensForText(message.getChannelId());

        // Thread context
        if (message.getThreadTs() != null && !message.getThreadTs().isEmpty()) {
            tokens += 3; // Thread indicator
        }

        // Workspace context
        tokens += countTokensForText(message.getTeamName());

        return (int) Math.ceil(tokens * STRUCTURED_DATA_MULTIPLIER);
    }

    /**
     * Cleans Slack formatting from text for more accurate token counting.
     *
     * @param text Text potentially containing Slack formatting
     * @return Cleaned text
     */
    private String cleanSlackFormatting(String text) {
        if (text == null) {
            return "";
        }

        // Remove common Slack formatting but keep the content
        return text
                .replaceAll("<@[^>]+>", "@user")           // User mentions
                .replaceAll("<#[^>]+>", "#channel")        // Channel mentions
                .replaceAll("<![^>]+>", "")                // Special mentions
                .replaceAll("<http[^>]+\\|([^>]+)>", "$1") // Links with text
                .replaceAll("<http[^>]+>", "link")         // Plain links
                .replaceAll("```[^`]*```", "code_block")   // Code blocks
                .replaceAll("`[^`]+`", "code")             // Inline code
                .replaceAll("\\*([^*]+)\\*", "$1")         // Bold
                .replaceAll("_([^_]+)_", "$1")             // Italic
                .replaceAll("~([^~]+)~", "$1")             // Strikethrough
                .trim();
    }

    /**
     * Checks if text contains Slack formatting.
     *
     * @param text Text to check
     * @return true if text contains Slack formatting
     */
    private boolean hasSlackFormatting(String text) {
        if (text == null) {
            return false;
        }

        return text.contains("<@") || text.contains("<#") || text.contains("```") ||
               text.contains("`") || text.contains("*") || text.contains("_") ||
               text.contains("~") || text.contains("<http");
    }

    /**
     * Estimates if adding a message would exceed the token limit.
     *
     * @param currentTokens Current token count
     * @param messageToAdd Message to potentially add
     * @param tokenLimit Maximum token limit
     * @return true if adding the message would exceed the limit
     */
    public boolean wouldExceedLimit(int currentTokens, Message messageToAdd, int tokenLimit) {
        int additionalTokens = countTokensForMessage(messageToAdd);
        return (currentTokens + additionalTokens) > tokenLimit;
    }

    /**
     * Calculates how many tokens are remaining before hitting the limit.
     *
     * @param currentTokens Current token count
     * @param tokenLimit Maximum token limit
     * @return Remaining tokens
     */
    public int getRemainingTokens(int currentTokens, int tokenLimit) {
        return Math.max(0, tokenLimit - currentTokens);
    }

    /**
     * Gets an estimated token count for a batch of messages without detailed processing.
     * Useful for quick estimates.
     *
     * @param messages List of messages
     * @return Quick estimated token count
     */
    public int getQuickEstimate(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }

        int totalChars = 0;
        for (Message message : messages) {
            if (message.getText() != null) {
                totalChars += message.getText().length();
            }
            // Add overhead for metadata
            totalChars += 100; // Approximate overhead per message
        }

        return (int) Math.ceil(totalChars * TOKENS_PER_CHARACTER * (1 + safetyMargin));
    }
}
