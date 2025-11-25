package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for generating message statistics and aggregations
 * @author vudu
 */
@Service
public class MessageStatisticsService {

    private static final Logger logger = LoggerFactory.getLogger(MessageStatisticsService.class);

    private final MessageRepository messageRepository;

    @Value("${statistics.max-messages:50000}")
    private int maxMessagesPerRequest;

    @Value("${statistics.max-breakdown-items:100}")
    private int maxBreakdownItems;

    @Value("${statistics.retention-window-days:7}")
    private int retentionWindowDays;

    public MessageStatisticsService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    /**
     * Generate statistics for a tenant
     *
     * @param request The statistics request
     * @return MessageStatisticsDTO with aggregated data
     */
    public MessageStatisticsDTO generateStatistics(MessageStatisticsRequestDTO request) {
        long startTime = System.currentTimeMillis();

        logger.info("🔍 [STATISTICS] Generating for tenant: {} | From: {} | To: {} | Breakdowns: {}",
                request.getTenantId(), request.getFrom(), request.getTo(), request.getIncludeBreakdowns());

        // Validate request
        validateRequest(request);

        // Fetch messages for tenant
        List<Message> messages = fetchMessages(request.getTenantId());

        // Apply temporal filtering if specified
        if (request.getFrom() != null || request.getTo() != null) {
            messages = applyTemporalFilter(messages, request.getFrom(), request.getTo());
        }

        // Check if we need to truncate for defensive limits
        boolean truncated = false;
        String dataCompleteness = "complete";

        if (messages.size() > maxMessagesPerRequest) {
            logger.warn("⚠️ [STATISTICS] Tenant {} has {} messages, truncating to {}",
                    request.getTenantId(), messages.size(), maxMessagesPerRequest);
            messages = messages.subList(0, maxMessagesPerRequest);
            truncated = true;
            dataCompleteness = "partial";
        }

        // Calculate totals
        StatisticsTotalsDTO totals = calculateTotals(messages);

        // Calculate breakdowns (if requested)
        StatisticsBreakdownsDTO breakdowns = null;
        if (Boolean.TRUE.equals(request.getIncludeBreakdowns())) {
            breakdowns = calculateBreakdowns(messages);
        }

        // Build metadata
        StatisticsMetadataDTO metadata = StatisticsMetadataDTO.builder()
                .generatedAt(Instant.now().toEpochMilli())
                .retentionWindowDays(retentionWindowDays)
                .dataCompleteness(dataCompleteness)
                .queriedFrom(request.getFrom())
                .queriedTo(request.getTo())
                .truncated(truncated)
                .build();

        long duration = System.currentTimeMillis() - startTime;
        logger.info("✅ [STATISTICS] Generated for tenant {} | Messages: {} | Duration: {}ms",
                request.getTenantId(), totals.getMessageCount(), duration);

        return MessageStatisticsDTO.builder()
                .tenantId(request.getTenantId())
                .totals(totals)
                .breakdowns(breakdowns)
                .metadata(metadata)
                .build();
    }

    /**
     * Validate the statistics request
     */
    private void validateRequest(MessageStatisticsRequestDTO request) {
        if (request.getTenantId() == null || request.getTenantId().trim().isEmpty()) {
            throw new IllegalArgumentException("tenantId is required");
        }

        if (request.getFrom() != null && request.getTo() != null && request.getFrom() > request.getTo()) {
            throw new IllegalArgumentException("'from' timestamp must be before 'to' timestamp");
        }
    }

    /**
     * Fetch all messages for a tenant
     */
    private List<Message> fetchMessages(String tenantId) {
        try {
            List<Message> messages = messageRepository.findAllByTenantId(tenantId);
            logger.debug("🔍 [STATISTICS] Fetched {} messages for tenant {}", messages.size(), tenantId);
            return messages;
        } catch (Exception e) {
            logger.error("❌ [STATISTICS] Failed to fetch messages for tenant {}: {}", tenantId, e.getMessage());
            throw new RuntimeException("Failed to fetch messages from Redis", e);
        }
    }

    /**
     * Apply temporal filtering to messages
     */
    private List<Message> applyTemporalFilter(List<Message> messages, Long from, Long to) {
        return messages.stream()
                .filter(message -> {
                    Long ingestedAt = message.getIngestedAt();
                    if (ingestedAt == null) return false;

                    if (from != null && ingestedAt < from) return false;
                    if (to != null && ingestedAt > to) return false;

                    return true;
                })
                .collect(Collectors.toList());
    }

    /**
     * Calculate totals from messages
     */
    private StatisticsTotalsDTO calculateTotals(List<Message> messages) {
        long messageCount = messages.size();

        long processedCount = messages.stream()
                .filter(msg -> Boolean.TRUE.equals(msg.getIsProcessed()))
                .count();

        long unprocessedCount = messageCount - processedCount;

        long threadedCount = messages.stream()
                .filter(msg -> msg.getThreadTs() != null &&
                              msg.getMessageTs() != null &&
                              !msg.getThreadTs().equals(msg.getMessageTs()))
                .count();

        long uniqueUsers = messages.stream()
                .map(Message::getUniqueUserId)
                .filter(Objects::nonNull)
                .distinct()
                .count();

        return StatisticsTotalsDTO.builder()
                .messageCount(messageCount)
                .processedCount(processedCount)
                .unprocessedCount(unprocessedCount)
                .threadedCount(threadedCount)
                .uniqueUsers(uniqueUsers)
                .build();
    }

    /**
     * Calculate breakdowns by workspace, channel, and source
     */
    private StatisticsBreakdownsDTO calculateBreakdowns(List<Message> messages) {
        // Breakdown by workspace
        List<BreakdownItemDTO> byWorkspace = calculateWorkspaceBreakdown(messages);

        // Breakdown by channel
        List<BreakdownItemDTO> byChannel = calculateChannelBreakdown(messages);

        // Breakdown by source
        List<BreakdownItemDTO> bySource = calculateSourceBreakdown(messages);

        return StatisticsBreakdownsDTO.builder()
                .byWorkspace(byWorkspace)
                .byChannel(byChannel)
                .bySource(bySource)
                .build();
    }

    /**
     * Calculate workspace breakdown with processed counts
     */
    private List<BreakdownItemDTO> calculateWorkspaceBreakdown(List<Message> messages) {
        Map<String, Long> workspaceCounts = messages.stream()
                .filter(msg -> msg.getWorkspaceId() != null)
                .collect(Collectors.groupingBy(Message::getWorkspaceId, Collectors.counting()));

        Map<String, Long> workspaceProcessedCounts = messages.stream()
                .filter(msg -> msg.getWorkspaceId() != null && Boolean.TRUE.equals(msg.getIsProcessed()))
                .collect(Collectors.groupingBy(Message::getWorkspaceId, Collectors.counting()));

        return workspaceCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(maxBreakdownItems)
                .map(entry -> BreakdownItemDTO.builder()
                        .id(entry.getKey())
                        .name(null) // Workspace name not stored in Message
                        .count(entry.getValue())
                        .processedCount(workspaceProcessedCounts.getOrDefault(entry.getKey(), 0L))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Calculate channel breakdown
     */
    private List<BreakdownItemDTO> calculateChannelBreakdown(List<Message> messages) {
        Map<String, ChannelInfo> channelMap = new HashMap<>();

        messages.stream()
                .filter(msg -> msg.getChannelId() != null)
                .forEach(msg -> {
                    String channelId = msg.getChannelId();
                    ChannelInfo info = channelMap.getOrDefault(channelId, new ChannelInfo(channelId, msg.getChannelName()));
                    info.count++;
                    channelMap.put(channelId, info);
                });

        return channelMap.values().stream()
                .sorted(Comparator.comparing(ChannelInfo::getCount).reversed())
                .limit(maxBreakdownItems)
                .map(info -> BreakdownItemDTO.builder()
                        .id(info.channelId)
                        .name(info.channelName)
                        .count(info.count)
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Calculate source breakdown
     */
    private List<BreakdownItemDTO> calculateSourceBreakdown(List<Message> messages) {
        Map<String, Long> sourceCounts = messages.stream()
                .filter(msg -> msg.getSource() != null)
                .collect(Collectors.groupingBy(Message::getSource, Collectors.counting()));

        return sourceCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .map(entry -> BreakdownItemDTO.builder()
                        .id(null)
                        .name(entry.getKey())
                        .count(entry.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Helper class to track channel info
     */
    private static class ChannelInfo {
        String channelId;
        String channelName;
        long count;

        ChannelInfo(String channelId, String channelName) {
            this.channelId = channelId;
            this.channelName = channelName;
            this.count = 0;
        }

        long getCount() {
            return count;
        }
    }
}
