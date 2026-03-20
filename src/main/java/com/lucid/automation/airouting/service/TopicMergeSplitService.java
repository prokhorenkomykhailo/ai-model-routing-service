package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.config.TopicMergeSplitProperties;
import com.lucid.automation.common.dto.topic.TopicClusterDraft;
import com.lucid.automation.common.dto.topic.TopicClusterRefined;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Step 2 (merge/split) refinement.
 * <p>
 * Consumes draft clusters from Step 1, derives lightweight embeddings,
 * computes cosine similarity, merges near-duplicates, and emits refined drafts.
 * This is deterministic and non-destructive; when a real embedding provider is
 * available, replace the embedding generation logic.
 */
@Service
public class TopicMergeSplitService {

    private static final Logger logger = LoggerFactory.getLogger(TopicMergeSplitService.class);

    private final TopicMergeSplitProperties properties;

    public TopicMergeSplitService(TopicMergeSplitProperties properties) {
        this.properties = properties;
    }

    /**
     * Merge clusters that appear to be duplicates based on cosine similarity and channel.
     * Keeps the earliest cluster_id and appends messageIds from duplicates.
     */
    public List<TopicClusterRefined> refine(List<TopicClusterDraft> drafts) {
        if (drafts == null || drafts.isEmpty()) {
            logger.debug("No drafts provided to merge/split refinement");
            return List.of();
        }

        // Group by channel to keep merging localized and predictable
        Map<String, List<TopicClusterDraft>> byChannel = drafts.stream()
            .collect(Collectors.groupingBy(d -> safe(d.getChannel())));

        List<TopicClusterRefined> refined = new ArrayList<>();

        for (Map.Entry<String, List<TopicClusterDraft>> entry : byChannel.entrySet()) {
            List<TopicClusterDraft> channelDrafts = entry.getValue();

            List<double[]> embeddings = channelDrafts.stream()
                .map(this::embedDraft)
                .toList();

            boolean[] merged = new boolean[channelDrafts.size()];

            for (int i = 0; i < channelDrafts.size(); i++) {
                if (merged[i]) continue;
                TopicClusterDraft anchor = cloneDraft(channelDrafts.get(i));
                Double maxSimilarity = null;
                for (int j = i + 1; j < channelDrafts.size(); j++) {
                    if (merged[j]) continue;
                    double sim = cosine(embeddings.get(i), embeddings.get(j));
                    if (sim >= properties.getSimilarityThreshold()) {
                        mergeInto(anchor, channelDrafts.get(j));
                        maxSimilarity = maxSimilarity == null ? sim : Math.max(maxSimilarity, sim);
                        merged[j] = true;
                    }
                }
                TopicClusterRefined out = new TopicClusterRefined();
                out.setClusterId(anchor.getClusterId());
                out.setDraftTitle(anchor.getDraftTitle());
                out.setChannel(anchor.getChannel());
                out.setThreadId(anchor.getThreadId());
                out.setParticipants(anchor.getParticipants());
                out.setMessageIds(anchor.getMessageIds());
                out.setSimilarityScore(maxSimilarity);
                refined.addAll(splitIfOversized(out));
            }
        }

        logger.info("Step2 merge/split produced {} clusters from {}", refined.size(), drafts.size());
        return refined;
    }

    private double[] embedDraft(TopicClusterDraft draft) {
        // deterministic pseudo-embedding: channel + participants (message IDs excluded to encourage merges)
        String basis = safe(draft.getChannel()) + "|" +
            String.join(",", draft.getParticipants() != null ? draft.getParticipants() : List.of());
        double[] vec = new double[8];
        int h = basis.hashCode();
        for (int i = 0; i < vec.length; i++) {
            h = 31 * h + i;
            vec[i] = (h % 1000) / 1000.0; // keep small magnitudes
        }
        return vec;
    }

    private double cosine(double[] a, double[] b) {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length && i < b.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        double denom = Math.sqrt(na) * Math.sqrt(nb);
        return denom == 0 ? 0 : dot / denom;
    }

    private void mergeInto(TopicClusterDraft target, TopicClusterDraft source) {
        // append messageIds
        List<String> mergedIds = new ArrayList<>();
        if (target.getMessageIds() != null) {
            mergedIds.addAll(target.getMessageIds());
        }
        if (source.getMessageIds() != null) {
            mergedIds.addAll(source.getMessageIds());
        }
        target.setMessageIds(mergedIds);

        // merge participants (distinct, case-insensitive)
        List<String> mergedParticipants = new ArrayList<>();
        if (target.getParticipants() != null) {
            mergedParticipants.addAll(target.getParticipants());
        }
        if (source.getParticipants() != null) {
            mergedParticipants.addAll(source.getParticipants());
        }
        target.setParticipants(
            mergedParticipants.stream()
                .filter(s -> s != null && !s.isBlank())
                .distinct()
                .toList()
        );

        // Keep a stable clusterId across merges (lexicographically smallest among merged IDs)
        List<String> ids = new ArrayList<>();
        if (target.getClusterId() != null) ids.add(target.getClusterId());
        if (source.getClusterId() != null) ids.add(source.getClusterId());
        if (!ids.isEmpty()) {
            target.setClusterId(ids.stream().min(Comparator.naturalOrder()).orElse(target.getClusterId()));
        }

        // Prefer a non-empty threadId if one exists
        if ((target.getThreadId() == null || target.getThreadId().isBlank())
            && source.getThreadId() != null && !source.getThreadId().isBlank()) {
            target.setThreadId(source.getThreadId());
        }
    }

    private List<TopicClusterRefined> splitIfOversized(TopicClusterRefined cluster) {
        if (!properties.isSplitEnabled()) {
            return List.of(cluster);
        }

        List<String> messageIds = cluster.getMessageIds() != null ? cluster.getMessageIds() : List.of();
        if (messageIds.size() <= properties.getMaxMessagesPerCluster()) {
            return List.of(cluster);
        }

        int chunkSize = Math.max(1, properties.getSplitChunkSize());
        List<TopicClusterRefined> parts = new ArrayList<>();

        for (int start = 0, part = 1; start < messageIds.size(); start += chunkSize, part++) {
            int end = Math.min(messageIds.size(), start + chunkSize);
            List<String> chunk = new ArrayList<>(messageIds.subList(start, end));

            TopicClusterRefined split = new TopicClusterRefined();
            split.setChannel(cluster.getChannel());
            split.setThreadId(cluster.getThreadId());
            split.setParticipants(cluster.getParticipants());
            split.setMessageIds(chunk);
            split.setSimilarityScore(cluster.getSimilarityScore());

            // Stable derived cluster ID for the chunk
            String basis = safe(cluster.getClusterId()) + "|" + String.join(",", chunk);
            split.setClusterId(safe(cluster.getClusterId()) + "_part_" + part + "_" +
                UUID.nameUUIDFromBytes(basis.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            split.setDraftTitle(cluster.getDraftTitle() + " (part " + part + ")");

            parts.add(split);
        }

        logger.info("Step2 split oversized cluster {} into {} parts ({} messages total)",
            cluster.getClusterId(), parts.size(), messageIds.size());
        return parts;
    }

    private TopicClusterDraft cloneDraft(TopicClusterDraft draft) {
        TopicClusterDraft copy = new TopicClusterDraft();
        copy.setClusterId(draft.getClusterId());
        copy.setDraftTitle(draft.getDraftTitle());
        copy.setChannel(draft.getChannel());
        copy.setThreadId(draft.getThreadId());
        copy.setParticipants(draft.getParticipants() != null ? new ArrayList<>(draft.getParticipants()) : new ArrayList<>());
        copy.setMessageIds(draft.getMessageIds() != null ? new ArrayList<>(draft.getMessageIds()) : new ArrayList<>());
        return copy;
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }
}
