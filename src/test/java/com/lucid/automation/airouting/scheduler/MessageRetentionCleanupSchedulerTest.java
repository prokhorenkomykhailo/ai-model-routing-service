package com.lucid.automation.airouting.scheduler;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import com.lucid.automation.airouting.service.MessageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MessageRetentionCleanupScheduler
 * Tests SCRUM-355 implementation
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageRetentionCleanupScheduler Tests")
class MessageRetentionCleanupSchedulerTest {

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private MessageService messageService;

    private MessageRetentionCleanupScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new MessageRetentionCleanupScheduler(messageRepository, messageService);

        // Set config values via reflection (only values injected via @Value)
        ReflectionTestUtils.setField(scheduler, "batchSize", 100);
        ReflectionTestUtils.setField(scheduler, "jobName", "message-retention-cleanup");
    }

    // ==================== SCHEDULER EXECUTION TESTS ====================

    @Test
    @DisplayName("Should process expired messages successfully")
    void shouldProcessExpiredMessagesSuccessfully() {
        // Given
        Long now = System.currentTimeMillis();
        Long expired = now - (35L * 24 * 60 * 60 * 1000); // 35 days ago

        List<Message> expiredMessages = List.of(
                createExpiredMessage("msg-1", expired),
                createExpiredMessage("msg-2", expired),
                createExpiredMessage("msg-3", expired)
        );

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(expiredMessages);

        doNothing().when(messageService).hardDeleteExpired(anyString());

        // When
        scheduler.runRetentionCleanup();

        // Then
        verify(messageRepository, times(1)).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class));
        verify(messageService, times(3)).hardDeleteExpired(anyString());
        verify(messageService).hardDeleteExpired("msg-1");
        verify(messageService).hardDeleteExpired("msg-2");
        verify(messageService).hardDeleteExpired("msg-3");
    }

    @Test
    @DisplayName("Should handle empty result set gracefully")
    void shouldHandleEmptyResultSetGracefully() {
        // Given
        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(new ArrayList<>());

        // When
        scheduler.runRetentionCleanup();

        // Then
        verify(messageRepository, times(1)).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class));
        verify(messageService, never()).hardDeleteExpired(anyString());
    }

    @Test
    @DisplayName("Should use correct batch size in query")
    void shouldUseCorrectBatchSizeInQuery() {
        // Given
        int customBatchSize = 50;
        ReflectionTestUtils.setField(scheduler, "batchSize", customBatchSize);

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(new ArrayList<>());

        // When
        scheduler.runRetentionCleanup();

        // Then
        ArgumentCaptor<PageRequest> pageRequestCaptor = ArgumentCaptor.forClass(PageRequest.class);
        verify(messageRepository).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), pageRequestCaptor.capture());

        PageRequest capturedPageRequest = pageRequestCaptor.getValue();
        assertThat(capturedPageRequest.getPageSize()).isEqualTo(customBatchSize);
        assertThat(capturedPageRequest.getPageNumber()).isZero();
    }

    @Test
    @DisplayName("Should query with current timestamp")
    void shouldQueryWithCurrentTimestamp() {
        // Given
        Long before = System.currentTimeMillis();

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(new ArrayList<>());

        // When
        scheduler.runRetentionCleanup();

        Long after = System.currentTimeMillis();

        // Then
        ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
        verify(messageRepository).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), timestampCaptor.capture(), any(PageRequest.class));

        Long capturedTimestamp = timestampCaptor.getValue();
        assertThat(capturedTimestamp).isBetween(before, after);
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("Should continue processing after single message failure")
    void shouldContinueProcessingAfterSingleMessageFailure() {
        // Given
        Long expired = System.currentTimeMillis() - (35L * 24 * 60 * 60 * 1000);

        List<Message> expiredMessages = List.of(
                createExpiredMessage("msg-1", expired),
                createExpiredMessage("msg-2", expired),
                createExpiredMessage("msg-3", expired)
        );

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(expiredMessages);

        // Simulate failure on msg-2
        doNothing().when(messageService).hardDeleteExpired("msg-1");
        doThrow(new RuntimeException("Database error")).when(messageService).hardDeleteExpired("msg-2");
        doNothing().when(messageService).hardDeleteExpired("msg-3");

        // When
        scheduler.runRetentionCleanup();

        // Then - should process all messages despite error on msg-2
        verify(messageService, times(1)).hardDeleteExpired("msg-1");
        verify(messageService, times(1)).hardDeleteExpired("msg-2");
        verify(messageService, times(1)).hardDeleteExpired("msg-3");
    }

    @Test
    @DisplayName("Should handle repository query exception")
    void shouldHandleRepositoryQueryException() {
        // Given
        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenThrow(new RuntimeException("Redis connection failed"));

        // When/Then - should not throw exception, just log error
        assertThatCode(() -> scheduler.runRetentionCleanup())
                .doesNotThrowAnyException();

        verify(messageRepository, times(1)).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class));
        verify(messageService, never()).hardDeleteExpired(anyString());
    }

    @Test
    @DisplayName("Should handle null message ID gracefully")
    void shouldHandleNullMessageIdGracefully() {
        // Given
        Message messageWithNullId = Message.builder()
                .id(null)
                .isDeleted(true)
                .retentionExpiry(System.currentTimeMillis() - (35L * 24 * 60 * 60 * 1000))
                .build();

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(List.of(messageWithNullId));

        // When/Then - should skip null ID message without crashing
        assertThatCode(() -> scheduler.runRetentionCleanup())
                .doesNotThrowAnyException();

        // Should still attempt to delete (service will handle null validation)
        verify(messageService, times(1)).hardDeleteExpired(null);
    }

    // ==================== BATCH PROCESSING TESTS ====================

    @Test
    @DisplayName("Should process large batch correctly")
    void shouldProcessLargeBatchCorrectly() {
        // Given
        int batchSize = 100;
        ReflectionTestUtils.setField(scheduler, "batchSize", batchSize);

        Long expired = System.currentTimeMillis() - (35L * 24 * 60 * 60 * 1000);
        List<Message> largeExpiredList = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            largeExpiredList.add(createExpiredMessage("msg-" + i, expired));
        }

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(largeExpiredList);

        doNothing().when(messageService).hardDeleteExpired(anyString());

        // When
        scheduler.runRetentionCleanup();

        // Then
        verify(messageRepository, times(1)).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class));
        verify(messageService, times(batchSize)).hardDeleteExpired(anyString());
    }

    @Test
    @DisplayName("Should process small batch correctly")
    void shouldProcessSmallBatchCorrectly() {
        // Given
        Long expired = System.currentTimeMillis() - (35L * 24 * 60 * 60 * 1000);
        List<Message> smallExpiredList = List.of(
                createExpiredMessage("msg-1", expired)
        );

        when(messageRepository.findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class)))
                .thenReturn(smallExpiredList);

        doNothing().when(messageService).hardDeleteExpired(anyString());

        // When
        scheduler.runRetentionCleanup();

        // Then
        verify(messageRepository, times(1)).findByIsDeletedAndRetentionExpiryLessThan(
                eq(true), anyLong(), any(PageRequest.class));
        verify(messageService, times(1)).hardDeleteExpired("msg-1");
    }

    // ==================== CONFIGURATION TESTS ====================

    @Test
    @DisplayName("Should have correct configuration values")
    void shouldHaveCorrectConfigurationValues() {
        // Given/When
        Integer batchSize = (Integer) ReflectionTestUtils.getField(scheduler, "batchSize");
        String jobName = (String) ReflectionTestUtils.getField(scheduler, "jobName");

        // Then
        assertThat(batchSize).isEqualTo(100);
        assertThat(jobName).isEqualTo("message-retention-cleanup");
    }

    // ==================== HELPER METHODS ====================

    private Message createExpiredMessage(String id, Long retentionExpiry) {
        return Message.builder()
                .id(id)
                .tenantId("tenant-1")
                .text("Expired message " + id)
                .isDeleted(true)
                .deletedAt(retentionExpiry - (30L * 24 * 60 * 60 * 1000)) // 30 days before expiry
                .deletedBy("user-admin")
                .deletionReason("Test deletion")
                .retentionExpiry(retentionExpiry)
                .build();
    }
}
