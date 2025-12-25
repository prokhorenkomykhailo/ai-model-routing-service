package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.TopicClusteringProperties;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.util.PromptLoader;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Builds the Step 1 prompt for corpus-wide clustering and enforces basic limits.
 */
@Component
public class TopicClusteringPromptBuilder {

    private static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final PromptLoader promptLoader;
    private final TopicClusteringProperties properties;

    public TopicClusteringPromptBuilder(PromptLoader promptLoader, TopicClusteringProperties properties) {
        this.promptLoader = promptLoader;
        this.properties = properties;
    }

    public PromptPayload buildPrompt(List<SlackMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return new PromptPayload("", List.of());
        }

        AtomicInteger counter = new AtomicInteger(0);
        List<SlackMessage> bounded = messages.stream()
            .limit(properties.getMaxMessages())
            .toList();

        List<String> formattedMessages = new ArrayList<>();
        for (SlackMessage message : bounded) {
            int idx = counter.incrementAndGet();
            String channel = StringUtils.hasText(message.getChannelName())
                ? message.getChannelName()
                : StringUtils.hasText(message.getChannelId()) ? message.getChannelId() : "unknown";
            String user = StringUtils.hasText(message.getDisplayName())
                ? message.getDisplayName()
                : StringUtils.hasText(message.getUsername()) ? message.getUsername() : "unknown";
            String thread = StringUtils.hasText(message.getThreadTs())
                ? message.getThreadTs()
                : StringUtils.hasText(message.getThreadId()) ? message.getThreadId() : "None";
            String text = truncate(message.getText(), properties.getMaxMessageCharacters());
            String ts = message.getTimestamp() != null ? TS_FORMATTER.format(message.getTimestamp()) : "";

            formattedMessages.add(String.format(
                "ID: %d | Channel: %s | User: %s | Thread: %s | Time: %s | Text: %s",
                idx, channel, user, thread, ts, text
            ));
        }

        String template = promptLoader.loadPromptTemplate(properties.getPromptName());
        String promptBody = template.replace("{{messages}}", formattedMessages.stream().collect(Collectors.joining("\n")));

        // Enforce total prompt size
        if (promptBody.length() > properties.getMaxPromptCharacters()) {
            promptBody = promptBody.substring(0, properties.getMaxPromptCharacters());
        }

        return new PromptPayload(promptBody, bounded);
    }

    private String truncate(String text, int maxChars) {
        if (text == null) {
            return "";
        }
        return text.length() <= maxChars ? text : text.substring(0, maxChars) + "...";
    }

    public record PromptPayload(String prompt, List<SlackMessage> messagesUsed) {}
}
