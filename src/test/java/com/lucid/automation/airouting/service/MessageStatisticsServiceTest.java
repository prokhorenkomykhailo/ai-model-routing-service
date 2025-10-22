package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for MessageStatisticsService
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageStatisticsService Tests")
class MessageStatisticsServiceTest {

    @Mock
    private MessageRepository messageRepository;

    private MessageStatisticsService statisticsService;

    @BeforeEach
    void setUp() {
        statisticsService = new MessageStatisticsService(messageRepository);

        // Set config values via reflection
        ReflectionTestUtils.setField(statisticsService, "maxMessagesPerRequest", 50000);
        ReflectionTestUtils.setField(statisticsService, "maxBreakdownItems", 100);
        ReflectionTestUtils.setField(statisticsService, "retentionWindowDays", 7);
    }

    @Test
    @DisplayName("Should throw exception when tenantId is null")
    void shouldThrowExceptionWhenTenantIdIsNull() {
        // Given
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(null)
                .build();

        // When/Then
        assertThatThrownBy(() -> statisticsService.generateStatistics(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId is required");
    }

    @Test
    @DisplayName("Should throw exception when tenantId is empty")
    void shouldThrowExceptionWhenTenantIdIsEmpty() {
        // Given
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId("   ")
                .build();

        // When/Then
        assertThatThrownBy(() -> statisticsService.generateStatistics(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenantId is required");
    }

    @Test
    @DisplayName("Should throw exception when from is after to")
    void shouldThrowExceptionWhenFromIsAfterTo() {
        // Given
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId("tenant-1")
                .from(2000L)
                .to(1000L)
                .build();

        // When/Then
        assertThatThrownBy(() -> statisticsService.generateStatistics(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'from' timestamp must be before 'to' timestamp");
    }

    @Test
    @DisplayName("Should return zeroed statistics for empty tenant")
    void shouldReturnZeroedStatisticsForEmptyTenant() {
        // Given
        String tenantId = "tenant-empty";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .build();

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(new ArrayList<>());

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getTenantId()).isEqualTo(tenantId);
        assertThat(result.getTotals().getMessageCount()).isZero();
        assertThat(result.getTotals().getProcessedCount()).isZero();
        assertThat(result.getTotals().getUnprocessedCount()).isZero();
        assertThat(result.getTotals().getThreadedCount()).isZero();
        assertThat(result.getTotals().getUniqueUsers()).isZero();
        assertThat(result.getMetadata().getDataCompleteness()).isEqualTo("complete");
        assertThat(result.getMetadata().getTruncated()).isFalse();
    }

    @Test
    @DisplayName("Should calculate correct totals for single message")
    void shouldCalculateCorrectTotalsForSingleMessage() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getTotals().getMessageCount()).isEqualTo(1);
        assertThat(result.getTotals().getProcessedCount()).isEqualTo(1);
        assertThat(result.getTotals().getUnprocessedCount()).isZero();
        assertThat(result.getTotals().getThreadedCount()).isZero(); // Same threadTs and messageTs
        assertThat(result.getTotals().getUniqueUsers()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should calculate correct totals for multiple messages")
    void shouldCalculateCorrectTotalsForMultipleMessages() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L),
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", true, "ts1", "ts2", "slack", 2000L), // threaded
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", false, "ts3", "ts3", "gmail", 3000L),
                createMessage("msg-4", "workspace-2", "channel-2", "user-3", false, "ts4", "ts4", "gmail", 4000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getTotals().getMessageCount()).isEqualTo(4);
        assertThat(result.getTotals().getProcessedCount()).isEqualTo(2);
        assertThat(result.getTotals().getUnprocessedCount()).isEqualTo(2);
        assertThat(result.getTotals().getThreadedCount()).isEqualTo(1); // msg-2 is threaded
        assertThat(result.getTotals().getUniqueUsers()).isEqualTo(3); // user-1, user-2, user-3
    }

    @Test
    @DisplayName("Should apply temporal filtering correctly")
    void shouldApplyTemporalFilteringCorrectly() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .from(2000L)
                .to(3500L)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L), // before range
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", true, "ts2", "ts2", "slack", 2500L), // in range
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", false, "ts3", "ts3", "gmail", 3000L), // in range
                createMessage("msg-4", "workspace-2", "channel-2", "user-3", false, "ts4", "ts4", "gmail", 4000L)  // after range
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getTotals().getMessageCount()).isEqualTo(2); // Only msg-2 and msg-3
        assertThat(result.getMetadata().getQueriedFrom()).isEqualTo(2000L);
        assertThat(result.getMetadata().getQueriedTo()).isEqualTo(3500L);
    }

    @Test
    @DisplayName("Should calculate workspace breakdown correctly")
    void shouldCalculateWorkspaceBreakdownCorrectly() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .includeBreakdowns(true)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L),
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", false, "ts2", "ts2", "slack", 2000L),
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", true, "ts3", "ts3", "gmail", 3000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getBreakdowns()).isNotNull();
        assertThat(result.getBreakdowns().getByWorkspace()).hasSize(2);

        BreakdownItemDTO workspace1 = result.getBreakdowns().getByWorkspace().get(0);
        assertThat(workspace1.getId()).isEqualTo("workspace-1");
        assertThat(workspace1.getCount()).isEqualTo(2);
        assertThat(workspace1.getProcessedCount()).isEqualTo(1);

        BreakdownItemDTO workspace2 = result.getBreakdowns().getByWorkspace().get(1);
        assertThat(workspace2.getId()).isEqualTo("workspace-2");
        assertThat(workspace2.getCount()).isEqualTo(1);
        assertThat(workspace2.getProcessedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should calculate channel breakdown correctly")
    void shouldCalculateChannelBreakdownCorrectly() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .includeBreakdowns(true)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L),
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", false, "ts2", "ts2", "slack", 2000L),
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", true, "ts3", "ts3", "gmail", 3000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getBreakdowns().getByChannel()).hasSize(2);

        BreakdownItemDTO channel1 = result.getBreakdowns().getByChannel().get(0);
        assertThat(channel1.getId()).isEqualTo("channel-1");
        assertThat(channel1.getName()).isEqualTo("General");
        assertThat(channel1.getCount()).isEqualTo(2);

        BreakdownItemDTO channel2 = result.getBreakdowns().getByChannel().get(1);
        assertThat(channel2.getId()).isEqualTo("channel-2");
        assertThat(channel2.getName()).isEqualTo("Engineering");
        assertThat(channel2.getCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should calculate source breakdown correctly")
    void shouldCalculateSourceBreakdownCorrectly() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .includeBreakdowns(true)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L),
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", false, "ts2", "ts2", "slack", 2000L),
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", true, "ts3", "ts3", "gmail", 3000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getBreakdowns().getBySource()).hasSize(2);

        BreakdownItemDTO slackSource = result.getBreakdowns().getBySource().get(0);
        assertThat(slackSource.getName()).isEqualTo("slack");
        assertThat(slackSource.getCount()).isEqualTo(2);

        BreakdownItemDTO gmailSource = result.getBreakdowns().getBySource().get(1);
        assertThat(gmailSource.getName()).isEqualTo("gmail");
        assertThat(gmailSource.getCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should not include breakdowns when includeBreakdowns is false")
    void shouldNotIncludeBreakdownsWhenFlagIsFalse() {
        // Given
        String tenantId = "tenant-1";
        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .includeBreakdowns(false)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getBreakdowns()).isNull();
    }

    @Test
    @DisplayName("Should mark as truncated when exceeding max messages")
    void shouldMarkAsTruncatedWhenExceedingMaxMessages() {
        // Given
        String tenantId = "tenant-large";
        ReflectionTestUtils.setField(statisticsService, "maxMessagesPerRequest", 3);

        MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .build();

        List<Message> messages = List.of(
                createMessage("msg-1", "workspace-1", "channel-1", "user-1", true, "ts1", "ts1", "slack", 1000L),
                createMessage("msg-2", "workspace-1", "channel-1", "user-2", false, "ts2", "ts2", "slack", 2000L),
                createMessage("msg-3", "workspace-2", "channel-2", "user-1", true, "ts3", "ts3", "gmail", 3000L),
                createMessage("msg-4", "workspace-2", "channel-2", "user-3", false, "ts4", "ts4", "gmail", 4000L),
                createMessage("msg-5", "workspace-2", "channel-2", "user-3", false, "ts5", "ts5", "gmail", 5000L)
        );

        when(messageRepository.findAllByTenantId(tenantId)).thenReturn(messages);

        // When
        MessageStatisticsDTO result = statisticsService.generateStatistics(request);

        // Then
        assertThat(result.getTotals().getMessageCount()).isEqualTo(3); // Truncated to max
        assertThat(result.getMetadata().getTruncated()).isTrue();
        assertThat(result.getMetadata().getDataCompleteness()).isEqualTo("partial");
    }

    // Helper method to create test messages
    private Message createMessage(String id, String workspaceId, String channelId, String uniqueUserId,
                                   boolean isProcessed, String threadTs, String messageTs, String source, Long ingestedAt) {
        return Message.builder()
                .id(id)
                .tenantId("tenant-1")
                .workspaceId(workspaceId)
                .channelId(channelId)
                .channelName(channelId.equals("channel-1") ? "General" : "Engineering")
                .uniqueUserId(uniqueUserId)
                .isProcessed(isProcessed)
                .threadTs(threadTs)
                .messageTs(messageTs)
                .source(source)
                .ingestedAt(ingestedAt)
                .build();
    }
}
