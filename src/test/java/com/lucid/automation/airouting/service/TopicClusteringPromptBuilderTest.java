package com.lucid.automation.airouting.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.lucid.automation.airouting.config.TopicClusteringProperties;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.util.PromptLoader;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TopicClusteringPromptBuilderTest {

    @Mock
    private PromptLoader promptLoader;

    private TopicClusteringProperties properties;
    private TopicClusteringPromptBuilder builder;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        properties = new TopicClusteringProperties();
        properties.setMaxMessages(2);
        properties.setMaxMessageCharacters(50);
        properties.setMaxPromptCharacters(500);
        properties.setPromptName("topic_clustering/v1/topic_clustering");

        when(promptLoader.loadPromptTemplate(anyString()))
            .thenReturn("Messages to analyze:\n{{messages}}\nEND");

        builder = new TopicClusteringPromptBuilder(promptLoader, properties);
    }

    @Test
    void buildPrompt_includesFormattedMessagesAndTemplate() {
        SlackMessage first = new SlackMessage();
        first.setChannelName("#general");
        first.setDisplayName("alice");
        first.setThreadTs("thread_1");
        first.setText("First message body about a project.");
        first.setTimestamp(LocalDateTime.now());

        SlackMessage second = new SlackMessage();
        second.setChannelName("#random");
        second.setDisplayName("bob");
        second.setThreadTs("thread_2");
        second.setText("Second message body with some more details.");
        second.setTimestamp(LocalDateTime.now());

        TopicClusteringPromptBuilder.PromptPayload payload =
            builder.buildPrompt(List.of(first, second));

        assertThat(payload).isNotNull();
        assertThat(payload.prompt()).contains("Messages to analyze:");
        assertThat(payload.prompt()).contains("Channel: #general | User: alice | Thread: thread_1 |");
        assertThat(payload.prompt()).contains("Channel: #random | User: bob | Thread: thread_2 |");
        assertThat(payload.prompt()).contains("END");
        assertThat(payload.messagesUsed()).hasSize(2);
    }
}
