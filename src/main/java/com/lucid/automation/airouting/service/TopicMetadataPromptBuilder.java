package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicMetadataProperties;
import com.lucid.automation.airouting.dto.topic.TopicClusterRefined;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.util.PromptLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class TopicMetadataPromptBuilder {

    private final PromptLoader promptLoader;
    private final TopicMetadataProperties properties;
    private final ObjectMapper objectMapper;

    public TopicMetadataPromptBuilder(PromptLoader promptLoader,
                                     TopicMetadataProperties properties,
                                     ObjectMapper objectMapper) {
        this.promptLoader = promptLoader;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public String buildPrompt(TopicClusterRefined cluster, List<Message> messages) {
        if (cluster == null) {
            return "";
        }

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("cluster_id", cluster.getClusterId());
        clusterInfo.put("draft_title", cluster.getDraftTitle());
        clusterInfo.put("channel", cluster.getChannel());
        clusterInfo.put("thread_id", cluster.getThreadId());
        clusterInfo.put("participants", cluster.getParticipants());
        clusterInfo.put("message_ids", cluster.getMessageIds());

        String clusterInfoJson;
        try {
            clusterInfoJson = objectMapper.writeValueAsString(clusterInfo);
        } catch (Exception e) {
            clusterInfoJson = "{}";
        }

        List<String> formatted = new ArrayList<>();
        if (messages != null) {
            int limit = Math.min(messages.size(), properties.getMaxMessages());
            for (int i = 0; i < limit; i++) {
                Message m = messages.get(i);
                if (m == null) continue;
                String text = truncate(m.getText(), properties.getMaxMessageCharacters());
                String user = StringUtils.hasText(m.getDisplayName()) ? m.getDisplayName()
                    : StringUtils.hasText(m.getUsername()) ? m.getUsername() : "";
                formatted.add(String.format(
                    "ID: %s | Channel: %s | User: %s | Thread: %s | Text: %s",
                    safe(m.getId()),
                    safe(m.getChannelName()),
                    safe(user),
                    safe(m.getThreadTs()),
                    text
                ));
            }
        }

        String template = promptLoader.loadPromptTemplate(properties.getPromptName());
        String prompt = template
            .replace("{{cluster_info_json}}", clusterInfoJson)
            .replace("{{messages}}", String.join("\n", formatted));

        if (prompt.length() > properties.getMaxPromptCharacters()) {
            prompt = prompt.substring(0, properties.getMaxPromptCharacters());
        }
        return prompt;
    }

    private String truncate(String text, int maxChars) {
        if (text == null) {
            return "";
        }
        return text.length() <= maxChars ? text : text.substring(0, maxChars) + "...";
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }
}

