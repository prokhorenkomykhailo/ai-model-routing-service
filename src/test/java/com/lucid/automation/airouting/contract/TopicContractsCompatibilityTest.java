package com.lucid.automation.airouting.contract;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.topic.TopicActionItem;
import com.lucid.automation.common.dto.topic.TopicClusterDraft;
import com.lucid.automation.common.dto.topic.TopicClusterDraftEvent;
import com.lucid.automation.common.dto.topic.TopicClusterRefined;
import com.lucid.automation.common.dto.topic.TopicClusterRefinedEvent;
import com.lucid.automation.common.dto.topic.TopicMetadata;
import com.lucid.automation.common.dto.topic.TopicMetadataEvent;
import com.lucid.automation.common.dto.topic.TopicVisibilityEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopicContractsCompatibilityTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void draftEventSchemaHasRequiredFields() throws Exception {
        TopicClusterDraft cluster = new TopicClusterDraft();
        cluster.setClusterId("cluster_001");
        cluster.setDraftTitle("Campaign planning");
        cluster.setChannel("#campaign-briefs");
        cluster.setThreadId("thread_001");
        cluster.setMessageIds(List.of("m1", "m2"));
        cluster.setParticipants(List.of("Devon", "Sam"));

        TopicClusterDraftEvent event = new TopicClusterDraftEvent();
        event.setEventId("evt-1");
        event.setWorkspaceId("ws-1");
        event.setBatchId("ws-1:batch_1");
        event.setProviderId("geminiProvider");
        event.setPromptVersion("v1");
        event.setCreatedAt(Instant.now());
        event.setClusterCount(1);
        event.setClusters(List.of(cluster));

        Map<String, Object> json = toMap(event);
        assertHas(json, "eventId", "workspaceId", "batchId", "providerId", "promptVersion", "createdAt", "clusterCount", "clusters");

        List<Map<String, Object>> clusters = (List<Map<String, Object>>) json.get("clusters");
        Assertions.assertEquals(1, clusters.size());
        assertHas(clusters.get(0), "clusterId", "messageIds", "draftTitle", "participants", "channel", "threadId");
    }

    @Test
    void refinedEventSchemaHasRequiredFields() throws Exception {
        TopicClusterRefined cluster = new TopicClusterRefined();
        cluster.setClusterId("cluster_001");
        cluster.setDraftTitle("Campaign planning");
        cluster.setChannel("#campaign-briefs");
        cluster.setThreadId("thread_001");
        cluster.setMessageIds(List.of("m1", "m2"));
        cluster.setParticipants(List.of("Devon", "Sam"));
        cluster.setSimilarityScore(0.91);

        TopicClusterRefinedEvent event = new TopicClusterRefinedEvent();
        event.setEventId("evt-2");
        event.setWorkspaceId("ws-1");
        event.setBatchId("ws-1:batch_1");
        event.setProviderId("geminiProvider");
        event.setPromptVersion("v1");
        event.setCreatedAt(Instant.now());
        event.setClusterCount(1);
        event.setClusters(List.of(cluster));

        Map<String, Object> json = toMap(event);
        assertHas(json, "eventId", "workspaceId", "batchId", "providerId", "promptVersion", "createdAt", "clusterCount", "clusters");

        List<Map<String, Object>> clusters = (List<Map<String, Object>>) json.get("clusters");
        Assertions.assertEquals(1, clusters.size());
        assertHas(clusters.get(0), "clusterId", "draftTitle", "channel", "threadId", "participants", "messageIds", "similarityScore");
    }

    @Test
    void metadataEventSchemaHasRequiredFields() throws Exception {
        TopicActionItem actionItem = new TopicActionItem();
        actionItem.setTask("Send timeline");
        actionItem.setOwner("Devon");
        actionItem.setOwnerUserId("U001");
        actionItem.setStatus("pending");
        actionItem.setPriority("high");

        TopicMetadata topic = new TopicMetadata();
        topic.setTitle("EcoBloom kickoff");
        topic.setSummary("Kickoff with actions and owners.");
        topic.setExternalParty("EcoBloom");
        topic.setParticipants(List.of("Devon", "Sam"));
        topic.setActionItems(List.of(actionItem));
        topic.setUrgency("high");
        topic.setDeadline("2026-03-20");
        topic.setStatus("open");
        topic.setChannel("#campaign-briefs");
        topic.setTags(List.of("campaign", "timeline"));

        TopicMetadataEvent event = new TopicMetadataEvent();
        event.setEventId("evt-3");
        event.setCreatedAt(Instant.now());
        event.setWorkspaceId("ws-1");
        event.setBatchId("ws-1:batch_1");
        event.setClusterId("cluster_001");
        event.setTopicId("topic_001");
        event.setProviderId("geminiProvider");
        event.setPromptVersion("v3");
        event.setMessageIds(List.of("m1", "m2"));
        event.setTopic(topic);

        Map<String, Object> json = toMap(event);
        assertHas(json, "eventId", "createdAt", "workspaceId", "batchId", "clusterId", "topicId", "providerId", "promptVersion", "messageIds", "topic");

        Map<String, Object> topicJson = (Map<String, Object>) json.get("topic");
        assertHas(topicJson, "title", "summary", "externalParty", "participants", "actionItems", "urgency", "status", "channel", "tags");

        List<Map<String, Object>> actionItems = (List<Map<String, Object>>) topicJson.get("actionItems");
        Assertions.assertEquals(1, actionItems.size());
        assertHas(actionItems.get(0), "task", "owner", "ownerUserId", "status", "priority");
    }

    @Test
    void step6MetadataCarriesObservabilityFields() throws Exception {
        Map<String, Object> candidate1 = new HashMap<>();
        candidate1.put("topicId", "topic_1");
        candidate1.put("score", 0.91);
        candidate1.put("selected", true);
        candidate1.put("selectionReason", "best_score_and_channel_match");

        Map<String, Object> candidate2 = new HashMap<>();
        candidate2.put("topicId", "topic_2");
        candidate2.put("score", 0.74);
        candidate2.put("selected", false);
        candidate2.put("selectionReason", "rejected_below_threshold");

        Map<String, Object> similaritySearch = new HashMap<>();
        similaritySearch.put("executed", true);
        similaritySearch.put("topK", 5);
        similaritySearch.put("threshold", 0.85);
        similaritySearch.put("candidates", List.of(candidate1, candidate2));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("step", "step6");
        metadata.put("action", "update");
        metadata.put("decisionPath", "semantic_similarity_path");
        metadata.put("decisionPathCategory", "semantic_similarity_path");
        metadata.put("similarityThreshold", 0.85);
        metadata.put("similaritySearch", similaritySearch);
        metadata.put("metadataRegeneratedOnUpdate", true);
        metadata.put("embeddingRecalculatedAfterUpdate", true);
        metadata.put("embeddingPersistedAfterUpdate", true);

        TopicMetadataEvent event = new TopicMetadataEvent();
        event.setEventId("evt-step6");
        event.setCreatedAt(Instant.now());
        event.setWorkspaceId("ws-1");
        event.setBatchId("step6:ws-1:m1");
        event.setClusterId("cluster_step6");
        event.setTopicId("topic_1");
        event.setProviderId("geminiProvider");
        event.setPromptVersion("v1");
        event.setMessageIds(List.of("m1"));
        event.setMetadata(metadata);

        Map<String, Object> json = toMap(event);
        Map<String, Object> metaJson = (Map<String, Object>) json.get("metadata");
        assertHas(metaJson,
            "step",
            "action",
            "decisionPath",
            "decisionPathCategory",
            "similarityThreshold",
            "similaritySearch",
            "metadataRegeneratedOnUpdate",
            "embeddingRecalculatedAfterUpdate",
            "embeddingPersistedAfterUpdate");

        Map<String, Object> similarityJson = (Map<String, Object>) metaJson.get("similaritySearch");
        assertHas(similarityJson, "executed", "topK", "threshold", "candidates");

        List<Map<String, Object>> candidates = (List<Map<String, Object>>) similarityJson.get("candidates");
        Assertions.assertEquals(2, candidates.size());
        assertHas(candidates.get(0), "topicId", "score", "selected", "selectionReason");
    }

    @Test
    void visibilityEventSchemaHasRequiredFields() throws Exception {
        TopicVisibilityEvent event = new TopicVisibilityEvent();
        event.setEventId("evt-4");
        event.setCreatedAt(Instant.now());
        event.setWorkspaceId("ws-1");
        event.setUserId("U001");
        event.setTopicIds(List.of("topic_1", "topic_2"));
        event.setBatchIds(List.of("ws-1:batch_1"));
        event.setChannels(List.of("#campaign-briefs"));
        event.setRule("OWNER_OPEN_ACTION_AND_CHANNEL_MEMBERSHIP");

        Map<String, Object> json = toMap(event);
        assertHas(json, "eventId", "createdAt", "workspaceId", "userId", "topicIds", "batchIds", "channels", "rule");
    }

    private Map<String, Object> toMap(Object value) throws Exception {
        String json = objectMapper.writeValueAsString(value);
        return objectMapper.readValue(json, new TypeReference<>() {});
    }

    private void assertHas(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Assertions.assertTrue(map.containsKey(key), "Missing key: " + key);
        }
    }
}
