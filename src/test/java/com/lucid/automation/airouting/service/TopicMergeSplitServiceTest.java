package com.lucid.automation.airouting.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.lucid.automation.airouting.config.TopicMergeSplitProperties;
import com.lucid.automation.airouting.dto.topic.TopicClusterDraft;
import com.lucid.automation.airouting.dto.topic.TopicClusterRefined;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopicMergeSplitServiceTest {

    private final TopicMergeSplitService service = new TopicMergeSplitService(new TopicMergeSplitProperties());

    @Test
    void mergesDuplicatesInSameChannel() {
        TopicClusterDraft a = draft("c1", "#general", List.of("u1"), List.of("m1", "m2"));
        TopicClusterDraft b = draft("c2", "#general", List.of("u1"), List.of("m3"));

        List<TopicClusterRefined> refined = service.refine(List.of(a, b));

        assertThat(refined).hasSize(1);
        assertThat(refined.get(0).getMessageIds()).containsExactlyInAnyOrder("m1", "m2", "m3");
    }

    @Test
    void keepsDistinctChannelsSeparate() {
        TopicClusterDraft a = draft("c1", "#general", List.of("u1"), List.of("m1"));
        TopicClusterDraft b = draft("c2", "#random", List.of("u1"), List.of("m2"));

        List<TopicClusterRefined> refined = service.refine(List.of(a, b));

        assertThat(refined).hasSize(2);
    }

    private TopicClusterDraft draft(String id, String channel, List<String> users, List<String> messageIds) {
        TopicClusterDraft d = new TopicClusterDraft();
        d.setClusterId(id);
        d.setChannel(channel);
        d.setParticipants(users);
        d.setMessageIds(messageIds);
        return d;
    }

    @Test
    void splitsOversizedClustersIntoChunks() {
        TopicMergeSplitProperties props = new TopicMergeSplitProperties();
        props.setSplitEnabled(true);
        props.setMaxMessagesPerCluster(3);
        props.setSplitChunkSize(2);

        TopicMergeSplitService localService = new TopicMergeSplitService(props);

        TopicClusterDraft big = draft("c1", "#general", List.of("u1"), List.of("m1", "m2", "m3", "m4", "m5"));
        List<TopicClusterRefined> refined = localService.refine(List.of(big));

        // 5 messages split into chunks of 2 => 3 clusters
        assertThat(refined).hasSize(3);
        assertThat(refined.stream().mapToInt(c -> c.getMessageIds().size()).sum()).isEqualTo(5);
    }
}
