package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicActionSuggestionProperties;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionRequest;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionType;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.util.PromptLoader;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopicActionSuggestionServiceTest {

    @Test
    void suggest_returnsReplySuggestion_withTopicContext() {
        TopicActionSuggestionProperties props = new TopicActionSuggestionProperties();
        props.setEnabled(true);
        props.setPromptName("topic_action_suggestion/v1/topic_action_suggestion");
        props.setPromptVersion("v1");

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        embeddingProps.getPgvector().setSchema("public");
        embeddingProps.getPgvector().setTable("topics_embeddings");

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("geminiProvider");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"suggested_subject\":\"\",\"suggested_text\":\"Thanks for the update. I will share final confirmation by EOD.\"}");
        when(router.selectProvider(eq(AITaskType.TEXT_QUERY), anyString(), any())).thenReturn(provider);

        PromptLoader promptLoader = mock(PromptLoader.class);
        when(promptLoader.loadPromptTemplate(anyString())).thenReturn("{{action_type}} {{topic_context_json}}");

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(startsWith("SELECT topic_metadata::text"),
            any(org.springframework.jdbc.core.RowMapper.class),
            eq("topic-1"), eq("ws-1")))
            .thenReturn(List.of("{\"title\":\"Shipment confirmation\",\"summary\":\"Customer waiting confirmation\"}"));

        TopicActionSuggestionService service = new TopicActionSuggestionService(
            props,
            embeddingProps,
            router,
            promptLoader,
            new ObjectMapper(),
            Optional.of(jdbc)
        );

        TopicActionSuggestionRequest req = new TopicActionSuggestionRequest();
        req.setTenantId("tenant-1");
        req.setWorkspaceId("ws-1");
        req.setTopicId("topic-1");
        req.setActionType(TopicActionType.REPLY);
        req.setUserDisplayName("Benoit");
        req.setRecipient("Client");

        var result = service.suggest(req);
        Assertions.assertEquals(TopicActionType.REPLY, result.getActionType());
        Assertions.assertEquals("geminiProvider", result.getProviderId());
        Assertions.assertTrue(result.isTopicContextFound());
        Assertions.assertTrue(result.getSuggestedText().contains("EOD"));
    }

    @Test
    void suggest_returnsForwardFallbackSubject_whenProviderReturnsPlainText() {
        TopicActionSuggestionProperties props = new TopicActionSuggestionProperties();
        props.setEnabled(true);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("openaiProvider");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("Please review this and advise next steps.");
        when(router.selectProvider(eq(AITaskType.TEXT_QUERY), anyString(), any())).thenReturn(provider);

        PromptLoader promptLoader = mock(PromptLoader.class);
        when(promptLoader.loadPromptTemplate(anyString())).thenReturn("{{action_type}}");

        TopicActionSuggestionService service = new TopicActionSuggestionService(
            props,
            embeddingProps,
            router,
            promptLoader,
            new ObjectMapper(),
            Optional.empty()
        );

        TopicActionSuggestionRequest req = new TopicActionSuggestionRequest();
        req.setTenantId("tenant-1");
        req.setTopicId("topic-x");
        req.setActionType(TopicActionType.FORWARD);

        var result = service.suggest(req);
        Assertions.assertEquals("Fwd: Topic update", result.getSuggestedSubject());
        Assertions.assertTrue(result.getSuggestedText().contains("Please review"));
    }
}

