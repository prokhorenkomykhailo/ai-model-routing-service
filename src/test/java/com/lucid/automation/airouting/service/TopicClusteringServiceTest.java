package com.lucid.automation.airouting.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicClusteringProperties;
import com.lucid.automation.common.dto.topic.TopicClusterDraftEvent;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.producer.TopicDraftProducer;
import com.lucid.automation.airouting.provider.AIProvider;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.util.StringUtils;

class TopicClusteringServiceTest {

    @Mock
    private TopicClusteringPromptBuilder promptBuilder;
    @Mock
    private AIProviderRouterService providerRouterService;
    @Mock
    private TopicDraftProducer topicDraftProducer;
    @Mock
    private TopicDraftCacheService topicDraftCacheService;
    @Mock
    private AIProvider aiProvider;
    private TopicClusteringProperties properties;

    private TopicClusteringService service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        properties = new TopicClusteringProperties();
        service = new TopicClusteringService(
            promptBuilder,
            providerRouterService,
            topicDraftProducer,
            topicDraftCacheService,
            new ObjectMapper(),
            properties
        );
    }

    @Test
    void processBatch_emitsDraftEvent_whenProviderReturnsClusters() {
        SlackMessage msg = new SlackMessage();
        msg.setId("msg-1");
        msg.setChannelName("#general");
        msg.setDisplayName("alice");
        msg.setThreadTs("thread-1");
        msg.setText("Message body");

        when(promptBuilder.buildPrompt(any()))
            .thenReturn(new TopicClusteringPromptBuilder.PromptPayload("prompt-body", List.of(msg)));

        when(providerRouterService.selectProvider(AITaskType.GENERATE_TOPIC, "tenant-1"))
            .thenReturn(aiProvider);
        when(aiProvider.getProviderId()).thenReturn("geminiProvider");
        when(aiProvider.processTextQuery("prompt-body", "user-1", "tenant-1"))
            .thenReturn("""
                {
                  "clusters": [
                    {
                      "cluster_id": "cluster_001",
                      "message_ids": [1],
                      "draft_title": "Title",
                      "participants": ["alice"],
                      "channel": "#general",
                      "thread_id": "thread-1"
                    }
                  ]
                }
                """);

        Workspace workspace = new Workspace();
        workspace.setId("ws-1");
        workspace.setName("Test Workspace");
        workspace.setTenantId("tenant-1");
        workspace.setDeemergeUserId("user-1");

        service.processBatch(workspace, List.of(msg), Map.of("requestedAt", Instant.now()), 1);

        ArgumentCaptor<TopicClusterDraftEvent> eventCaptor = ArgumentCaptor.forClass(TopicClusterDraftEvent.class);
        verify(topicDraftProducer).publishDraft(eventCaptor.capture());
        verify(topicDraftCacheService).cacheDraft(eventCaptor.getValue());

        TopicClusterDraftEvent event = eventCaptor.getValue();
        assertThat(event.getWorkspaceId()).isEqualTo("ws-1");
        assertThat(event.getBatchId()).isEqualTo("ws-1:batch_1");
        assertThat(event.getClusterCount()).isEqualTo(1);
        assertThat(event.getClusters()).hasSize(1);
        assertThat(event.getClusters().get(0).getClusterId()).isEqualTo("cluster_001");
        assertThat(event.getClusters().get(0).getMessageIds()).containsExactly("msg-1");
        assertThat(StringUtils.hasText(event.getEventId())).isTrue();
    }

    @Test
    void processBatch_parsesFencedJsonAndTrailingCommas() {
        SlackMessage msg = new SlackMessage();
        msg.setId("msg-42");
        msg.setChannelName("#alerts");
        msg.setDisplayName("bob");
        msg.setThreadTs("thread-xyz");
        msg.setText("Something happened");

        when(promptBuilder.buildPrompt(any()))
            .thenReturn(new TopicClusteringPromptBuilder.PromptPayload("prompt-body", List.of(msg)));

        when(providerRouterService.selectProvider(AITaskType.GENERATE_TOPIC, "tenant-1"))
            .thenReturn(aiProvider);
        when(aiProvider.getProviderId()).thenReturn("geminiProvider");
        when(aiProvider.processTextQuery("prompt-body", "user-1", "tenant-1"))
            .thenReturn("""
                ```json
                {
                  "clusters": [
                    {
                      "cluster_id": "cluster_abc",
                      "message_ids": [1,],
                      "draft_title": "Alert cluster",
                      "participants": ["bob",],
                      "channel": "#alerts",
                      "thread_id": "thread-xyz",
                    },
                  ]
                }
                ```
                """);

        Workspace workspace = new Workspace();
        workspace.setId("ws-1");
        workspace.setName("Test Workspace");
        workspace.setTenantId("tenant-1");
        workspace.setDeemergeUserId("user-1");

        service.processBatch(workspace, List.of(msg), Map.of(), 1);

        ArgumentCaptor<TopicClusterDraftEvent> eventCaptor = ArgumentCaptor.forClass(TopicClusterDraftEvent.class);
        verify(topicDraftProducer).publishDraft(eventCaptor.capture());

        TopicClusterDraftEvent event = eventCaptor.getValue();
        assertThat(event.getClusterCount()).isEqualTo(1);
        assertThat(event.getClusters().get(0).getClusterId()).isEqualTo("cluster_abc");
        assertThat(event.getClusters().get(0).getMessageIds()).containsExactly("msg-42");
    }
}
