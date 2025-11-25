package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MessageService soft delete functionality
 * Tests SCRUM-352, SCRUM-353 implementations
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageService Soft Delete Tests")
class MessageSoftDeleteServiceTest {

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private ChannelService channelService;

    private MessageService messageService;

    @BeforeEach
    void setUp() {
        messageService = new MessageService(messageRepository, channelService);
    }

    // ==================== SOFT DELETE TESTS ====================

    @Test
    @DisplayName("Should soft delete message successfully")
    void shouldSoftDeleteMessageSuccessfully() {
        // Given
        String messageId = "msg-123";
        String deletedBy = "user-admin";
        String reason = "User requested deletion";

        Message existingMessage = Message.builder()
                .id(messageId)
                .tenantId("tenant-1")
                .text("Test message")
                .isDeleted(false)
                .build();

        when(messageRepository.findById(messageId)).thenReturn(Optional.of(existingMessage));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        messageService.softDeleteById(messageId, deletedBy, reason);

        // Then
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository, times(1)).save(messageCaptor.capture());

        Message savedMessage = messageCaptor.getValue();
        assertThat(savedMessage.getIsDeleted()).isTrue();
        assertThat(savedMessage.getDeletedBy()).isEqualTo(deletedBy);
        assertThat(savedMessage.getDeletionReason()).isEqualTo(reason);
        assertThat(savedMessage.getDeletedAt()).isNotNull();
        assertThat(savedMessage.getRetentionExpiry()).isNotNull();

        // Verify retention expiry is approximately 30 days from deletion (in milliseconds)
        long expectedRetentionMs = 30L * 24 * 60 * 60 * 1000;
        long actualRetentionDuration = savedMessage.getRetentionExpiry() - savedMessage.getDeletedAt();
        assertThat(actualRetentionDuration).isCloseTo(expectedRetentionMs, within(1000L)); // Within 1 second tolerance
    }

    @Test
    @DisplayName("Should handle message not found gracefully")
    void shouldHandleMessageNotFoundGracefully() {
        // Given
        String messageId = "msg-nonexistent";
        when(messageRepository.findById(messageId)).thenReturn(Optional.empty());

        // When/Then - should not throw exception, just log warning
        assertThatCode(() -> messageService.softDeleteById(messageId, "user-admin", "Test"))
                .doesNotThrowAnyException();

        verify(messageRepository, times(1)).findById(messageId);
        verify(messageRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should handle null message ID gracefully")
    void shouldHandleNullMessageIdGracefully() {
        // When/Then - should not throw exception, just log warning
        assertThatCode(() -> messageService.softDeleteById(null, "user-admin", "Test"))
                .doesNotThrowAnyException();

        verify(messageRepository, never()).findById(any());
        verify(messageRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should use default values when deletedBy and reason are null")
    void shouldUseDefaultValuesWhenParametersAreNull() {
        // Given
        String messageId = "msg-123";
        Message existingMessage = Message.builder()
                .id(messageId)
                .isDeleted(false)
                .build();

        when(messageRepository.findById(messageId)).thenReturn(Optional.of(existingMessage));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        messageService.softDeleteById(messageId, null, null);

        // Then
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(messageCaptor.capture());

        Message savedMessage = messageCaptor.getValue();
        assertThat(savedMessage.getDeletionReason()).isEqualTo("MANUAL");
        assertThat(savedMessage.getDeletedBy()).isEqualTo("SYSTEM");
    }

    // ==================== HARD DELETE TESTS ====================

    @Test
    @DisplayName("Should hard delete message successfully")
    void shouldHardDeleteMessageSuccessfully() {
        // Given
        String messageId = "msg-expired";
        doNothing().when(messageRepository).deleteById(messageId);

        // When
        messageService.hardDeleteExpired(messageId);

        // Then
        verify(messageRepository, times(1)).deleteById(messageId);
    }

    @Test
    @DisplayName("Should handle null ID in hard delete")
    void shouldHandleNullIdInHardDelete() {
        // When/Then - should not throw exception
        assertThatCode(() -> messageService.hardDeleteExpired(null))
                .doesNotThrowAnyException();

        verify(messageRepository, never()).deleteById(any());
    }

    @Test
    @DisplayName("Should propagate exceptions from repository")
    void shouldPropagateExceptionsFromRepository() {
        // Given
        String messageId = "msg-error";
        doThrow(new RuntimeException("Redis connection failed"))
                .when(messageRepository).deleteById(messageId);

        // When/Then
        assertThatThrownBy(() -> messageService.hardDeleteExpired(messageId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Redis connection failed");

        verify(messageRepository, times(1)).deleteById(messageId);
    }

    // ==================== RESTORE TESTS ====================

    @Test
    @DisplayName("Should restore deleted message successfully")
    void shouldRestoreDeletedMessageSuccessfully() {
        // Given
        String messageId = "msg-restore";
        Long deletedAt = System.currentTimeMillis() - (5L * 24 * 60 * 60 * 1000); // 5 days ago

        Message deletedMessage = Message.builder()
                .id(messageId)
                .isDeleted(true)
                .deletedAt(deletedAt)
                .deletedBy("user-admin")
                .deletionReason("Test deletion")
                .retentionExpiry(deletedAt + (30L * 24 * 60 * 60 * 1000))
                .build();

        when(messageRepository.findById(messageId)).thenReturn(Optional.of(deletedMessage));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        messageService.restoreDeletedMessage(messageId);

        // Then
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(messageCaptor.capture());

        Message restoredMessage = messageCaptor.getValue();
        assertThat(restoredMessage.getIsDeleted()).isFalse();
        assertThat(restoredMessage.getDeletedAt()).isNull();
        assertThat(restoredMessage.getDeletedBy()).isNull();
        assertThat(restoredMessage.getDeletionReason()).isNull();
        assertThat(restoredMessage.getRetentionExpiry()).isNull();
    }

    @Test
    @DisplayName("Should handle restore of non-existent message")
    void shouldHandleRestoreOfNonExistentMessage() {
        // Given
        String messageId = "msg-nonexistent";
        when(messageRepository.findById(messageId)).thenReturn(Optional.empty());

        // When/Then - should not throw exception
        assertThatCode(() -> messageService.restoreDeletedMessage(messageId))
                .doesNotThrowAnyException();

        verify(messageRepository, times(1)).findById(messageId);
        verify(messageRepository, never()).save(any());
    }

    @Test
    @DisplayName("Should handle null ID in restore")
    void shouldHandleNullIdInRestore() {
        // When/Then - should not throw exception
        assertThatCode(() -> messageService.restoreDeletedMessage(null))
                .doesNotThrowAnyException();

        verify(messageRepository, never()).findById(any());
        verify(messageRepository, never()).save(any());
    }
}
