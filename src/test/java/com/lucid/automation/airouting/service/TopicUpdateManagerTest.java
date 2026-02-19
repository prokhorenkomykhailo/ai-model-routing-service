package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.config.TopicUpdateProperties;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.producer.TopicMetadataProducer;
import com.lucid.automation.airouting.util.PromptLoader;
import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.IngestionMessageDTO;
import com.lucid.automation.common.dto.messaging.IngestionUserDTO;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.*;

public class TopicUpdateManagerTest {

    @Test
    void handleNewMessage_createsNewTopic_whenNoCandidates() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(false);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenReturn(List.of());

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("test");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T\",\"summary\":\"S\",\"external_party\":null,\"participants\":[\"A\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\"]}");
        when(router.selectProvider(any(AITaskType.class), anyString(), any())).thenReturn(provider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("""
existing_topic_json:
{{existing_topic_json}}
new_messages:
{{new_messages}}
""");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.empty(),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");

        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("hello");
        ev.setMessage(msg);

        IngestionUserDTO user = new IngestionUserDTO();
        user.setDisplayName("Devon");
        user.setSlackUserId("U1");
        ev.setUser(user);

        mgr.handleNewMessage(ev);

        ArgumentCaptor<TopicMetadataEvent> cap = ArgumentCaptor.forClass(TopicMetadataEvent.class);
        verify(producer, times(1)).publish(cap.capture());
        Assertions.assertNotNull(cap.getValue().getTopicId());
        Assertions.assertEquals("ws1", cap.getValue().getWorkspaceId());
        Assertions.assertEquals("#c", cap.getValue().getTopic().getChannel());
    }

    @Test
    void handleNewMessage_updatesExistingTopic_whenCandidateAboveThreshold_andIncludesBelongsCheckMetadata() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(true);
        props.setSimilarityThreshold(0.75);
        props.setSearchTopK(5);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        embeddingProps.getPgvector().setSchema("public");
        embeddingProps.getPgvector().setTable("topics_embeddings");

        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenReturn(List.of(new TopicEmbeddingStoreService.ScoredTopic("topic-123", 0.9)));

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // Version query
        when(jdbc.query(startsWith("SELECT topic_metadata->>'version'"),
            org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<String>>any(),
            eq("topic-123")))
            .thenReturn(List.of("3"));
        // Metadata fetch query
        when(jdbc.query(startsWith("SELECT topic_metadata::text"),
            org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<String>>any(),
            eq("topic-123")))
            .thenReturn(List.of("{\"title\":\"T\",\"summary\":\"S\",\"channel\":\"#c\",\"participants\":[\"Devon (U1)\"],\"actionItems\":[],\"tags\":[\"t\"],\"version\":3}"));

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider metaProvider = mock(AIProvider.class);
        when(metaProvider.getProviderId()).thenReturn("geminiProvider");
        when(metaProvider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T2\",\"summary\":\"S2\",\"external_party\":null,\"participants\":[\"Devon (U1)\",\"Sam (U2)\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\",\"design\"]}");

        AIProvider belongsProvider = mock(AIProvider.class);
        when(belongsProvider.getProviderId()).thenReturn("geminiProvider");
        when(belongsProvider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"belongs\":true,\"confidence\":0.83,\"explanation\":\"same workstream\"}");

        when(router.selectProvider(eq(AITaskType.GENERATE_TOPIC), anyString(), any())).thenReturn(metaProvider);
        when(router.selectProvider(eq(AITaskType.TEXT_QUERY), anyString(), any())).thenReturn(belongsProvider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("""
existing_topic_json:
{{existing_topic_json}}
new_messages:
{{new_messages}}
""");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.of(jdbc),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");

        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("design concepts");
        ev.setMessage(msg);

        IngestionUserDTO user = new IngestionUserDTO();
        user.setDisplayName("Sam");
        user.setSlackUserId("U2");
        ev.setUser(user);

        mgr.handleNewMessage(ev);

        ArgumentCaptor<TopicMetadataEvent> cap = ArgumentCaptor.forClass(TopicMetadataEvent.class);
        verify(producer, times(1)).publish(cap.capture());
        Assertions.assertEquals("topic-123", cap.getValue().getTopicId());
        Assertions.assertNotNull(cap.getValue().getMetadata());
        @SuppressWarnings("unchecked")
        var meta = (java.util.Map<String, Object>) cap.getValue().getMetadata();
        Assertions.assertEquals("6", String.valueOf(meta.get("step")));
        Assertions.assertEquals("update", String.valueOf(meta.get("action")));
        Assertions.assertEquals("semantic_similarity_path", String.valueOf(meta.get("decisionPathCategory")));
        Assertions.assertEquals("0.75", String.valueOf(meta.get("appliedSimilarityThreshold")));
        Assertions.assertEquals(4, Integer.parseInt(String.valueOf(meta.get("topicVersion"))));
        Assertions.assertEquals("llm", String.valueOf(meta.get("metadataGenerationMode")));

        @SuppressWarnings("unchecked")
        var bc = (java.util.Map<String, Object>) meta.get("belongsCheck");
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(bc.get("performed"))));
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(bc.get("belongs"))));

        @SuppressWarnings("unchecked")
        var ss = (java.util.Map<String, Object>) meta.get("similaritySearch");
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(ss.get("executed"))));
        Assertions.assertEquals("5", String.valueOf(ss.get("topK")));
        Assertions.assertEquals("0.75", String.valueOf(ss.get("threshold")));
        @SuppressWarnings("unchecked")
        var candidates = (java.util.List<java.util.Map<String, Object>>) ss.get("candidates");
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("selected"))));
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("accepted"))));
        Assertions.assertEquals("accepted_selected_best_score", String.valueOf(candidates.get(0).get("acceptedReason")));
        @SuppressWarnings("unchecked")
        var retrieved = (java.util.List<java.util.Map<String, Object>>) ss.get("retrievedCandidates");
        Assertions.assertEquals(1, retrieved.size());
    }

    @Test
    void handleNewMessage_createsNewTopic_whenBelowThreshold_andEmitsCandidateScores() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(false);
        props.setSimilarityThreshold(0.75);
        props.setSearchTopK(5);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenReturn(List.of(new TopicEmbeddingStoreService.ScoredTopic("topic-abc", 0.72)));

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("test");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T\",\"summary\":\"S\",\"external_party\":null,\"participants\":[\"A\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\"]}");
        when(router.selectProvider(any(AITaskType.class), anyString(), any())).thenReturn(provider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("""
existing_topic_json:
{{existing_topic_json}}
new_messages:
{{new_messages}}
""");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.empty(),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");

        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("hello");
        ev.setMessage(msg);

        IngestionUserDTO user = new IngestionUserDTO();
        user.setDisplayName("Devon");
        user.setSlackUserId("U1");
        ev.setUser(user);

        mgr.handleNewMessage(ev);

        ArgumentCaptor<TopicMetadataEvent> cap = ArgumentCaptor.forClass(TopicMetadataEvent.class);
        verify(producer, times(1)).publish(cap.capture());
        @SuppressWarnings("unchecked")
        var meta = (java.util.Map<String, Object>) cap.getValue().getMetadata();
        Assertions.assertEquals("create", String.valueOf(meta.get("action")));
        Assertions.assertEquals("below_similarity_threshold", String.valueOf(meta.get("reason")));
        @SuppressWarnings("unchecked")
        var ss = (java.util.Map<String, Object>) meta.get("similaritySearch");
        @SuppressWarnings("unchecked")
        var candidates = (java.util.List<java.util.Map<String, Object>>) ss.get("candidates");
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals("topic-abc", String.valueOf(candidates.get(0).get("topicId")));
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("selected"))));
        Assertions.assertEquals(false, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("accepted"))));
        Assertions.assertEquals("rejected_below_threshold", String.valueOf(candidates.get(0).get("rejectedReason")));
        @SuppressWarnings("unchecked")
        var retrieved = (java.util.List<java.util.Map<String, Object>>) ss.get("retrievedCandidates");
        Assertions.assertEquals(1, retrieved.size());
    }

    @Test
    void handleNewMessage_createsNewTopic_whenVectorSearchThrows_andIncludesError() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(false);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenThrow(new RuntimeException("pgvector unreachable"));

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("test");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T\",\"summary\":\"S\",\"external_party\":null,\"participants\":[\"A\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\"]}");
        when(router.selectProvider(any(AITaskType.class), anyString(), any())).thenReturn(provider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("{{existing_topic_json}} {{new_messages}}");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.empty(),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");
        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("hello");
        ev.setMessage(msg);
        ev.setUser(new IngestionUserDTO());

        mgr.handleNewMessage(ev);

        ArgumentCaptor<TopicMetadataEvent> cap = ArgumentCaptor.forClass(TopicMetadataEvent.class);
        verify(producer, times(1)).publish(cap.capture());
        @SuppressWarnings("unchecked")
        var meta = (java.util.Map<String, Object>) cap.getValue().getMetadata();
        Assertions.assertEquals("vector_search_error", String.valueOf(meta.get("reason")));
        @SuppressWarnings("unchecked")
        var ss = (java.util.Map<String, Object>) meta.get("similaritySearch");
        Assertions.assertTrue(String.valueOf(ss.get("error")).contains("pgvector"));
        @SuppressWarnings("unchecked")
        var retrieved = (java.util.List<java.util.Map<String, Object>>) ss.get("retrievedCandidates");
        Assertions.assertEquals(0, retrieved.size());
    }

    @Test
    void handleNewMessage_sendsDlq_whenPublishThrows() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(false);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenReturn(List.of());

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);
        doThrow(new RuntimeException("kafka down")).when(producer).publish(any(TopicMetadataEvent.class));

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn("test");
        when(provider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T\",\"summary\":\"S\",\"external_party\":null,\"participants\":[\"A\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\"]}");
        when(router.selectProvider(any(AITaskType.class), anyString(), any())).thenReturn(provider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("{{existing_topic_json}} {{new_messages}}");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.empty(),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");
        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("hello");
        ev.setMessage(msg);
        ev.setUser(new IngestionUserDTO());

        mgr.handleNewMessage(ev);

        verify(producer, times(1)).publishDlq(anyString(), startsWith("step6_publish_error:"), eq("ws1"), anyString());
    }

    @Test
    void handleNewMessage_createsNewTopic_whenBelongsCheckBlocks_andEmitsCandidateRejection() {
        TopicUpdateProperties props = new TopicUpdateProperties();
        props.setEnabled(true);
        props.setBelongsCheckEnabled(true);
        props.setSimilarityThreshold(0.75);
        props.setSearchTopK(5);

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        embeddingProps.getPgvector().setSchema("public");
        embeddingProps.getPgvector().setTable("topics_embeddings");

        TopicEmbeddingStoreService embeddingStore = mock(TopicEmbeddingStoreService.class);
        when(embeddingStore.search(anyString(), any(com.lucid.automation.airouting.dto.topic.TopicMetadata.class), anyInt()))
            .thenReturn(List.of(new TopicEmbeddingStoreService.ScoredTopic("topic-123", 0.9)));

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(startsWith("SELECT topic_metadata->>'version'"),
            org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<String>>any(),
            anyString()))
            .thenReturn(List.of());
        when(jdbc.query(startsWith("SELECT topic_metadata::text"),
            org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<String>>any(),
            eq("topic-123")))
            .thenReturn(List.of("{\"title\":\"T\",\"summary\":\"S\",\"channel\":\"#c\",\"participants\":[\"Devon (U1)\"],\"actionItems\":[],\"tags\":[\"t\"],\"version\":1}"));

        TopicMetadataProducer producer = mock(TopicMetadataProducer.class);

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider metaProvider = mock(AIProvider.class);
        when(metaProvider.getProviderId()).thenReturn("geminiProvider");
        when(metaProvider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"title\":\"T2\",\"summary\":\"S2\",\"external_party\":null,\"participants\":[\"Devon (U1)\"],\"action_items\":[{\"task\":\"x\",\"owner\":\"A\",\"status\":\"pending\",\"priority\":\"medium\"}],\"urgency\":\"medium\",\"deadline\":null,\"status\":\"active\",\"channel\":\"#c\",\"tags\":[\"t\"]}");

        AIProvider belongsProvider = mock(AIProvider.class);
        when(belongsProvider.getProviderId()).thenReturn("geminiProvider");
        when(belongsProvider.processTextQuery(anyString(), anyString(), anyString()))
            .thenReturn("{\"belongs\":false,\"confidence\":0.6,\"explanation\":\"different workstream\"}");

        when(router.selectProvider(eq(AITaskType.GENERATE_TOPIC), anyString(), any())).thenReturn(metaProvider);
        when(router.selectProvider(eq(AITaskType.TEXT_QUERY), anyString(), any())).thenReturn(belongsProvider);

        PromptLoader loader = mock(PromptLoader.class);
        when(loader.loadPromptTemplate(anyString())).thenReturn("{{existing_topic_json}} {{new_messages}}");

        TopicUpdateManager mgr = new TopicUpdateManager(
            props,
            embeddingStore,
            producer,
            router,
            loader,
            new ObjectMapper(),
            Optional.of(jdbc),
            embeddingProps
        );

        IngestionEventDTO ev = new IngestionEventDTO();
        ev.setTenantId("tenant-1");
        IngestionMessageDTO msg = new IngestionMessageDTO();
        msg.setWorkspaceId("ws1");
        msg.setChannelId("C1");
        msg.setChannelName("#c");
        msg.setTs("123");
        msg.setText("something unrelated");
        ev.setMessage(msg);
        IngestionUserDTO user = new IngestionUserDTO();
        user.setDisplayName("Sam");
        user.setSlackUserId("U2");
        ev.setUser(user);

        mgr.handleNewMessage(ev);

        ArgumentCaptor<TopicMetadataEvent> cap = ArgumentCaptor.forClass(TopicMetadataEvent.class);
        verify(producer, times(1)).publish(cap.capture());
        @SuppressWarnings("unchecked")
        var meta = (java.util.Map<String, Object>) cap.getValue().getMetadata();
        Assertions.assertEquals("create", String.valueOf(meta.get("action")));
        Assertions.assertEquals("belongs_check_negative", String.valueOf(meta.get("reason")));
        Assertions.assertEquals("semantic_similarity_belongs_blocked", String.valueOf(meta.get("decisionPathCategory")));
        @SuppressWarnings("unchecked")
        var ss = (java.util.Map<String, Object>) meta.get("similaritySearch");
        @SuppressWarnings("unchecked")
        var candidates = (java.util.List<java.util.Map<String, Object>>) ss.get("candidates");
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals(true, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("selected"))));
        Assertions.assertEquals(false, Boolean.parseBoolean(String.valueOf(candidates.get(0).get("accepted"))));
        Assertions.assertEquals("rejected_belongs_check_blocked", String.valueOf(candidates.get(0).get("rejectedReason")));
    }
}
