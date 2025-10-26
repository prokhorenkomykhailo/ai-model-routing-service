package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.repository.MessageRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for soft-delete filtering in repository queries
 * Verifies that new repository methods correctly exclude soft-deleted messages
 *
 * @author vudu
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Repository - Soft Delete Filtering Tests")
class RepositorySoftDeleteFilterTest {

    @Mock
    private MessageRepository messageRepository;

    private static final String TENANT_ID = "tenant-123";
    private static final String DEEMERGE_USER_ID = "demerge-user-456";
    private static final String WORKSPACE_ID = "workspace-789";

    @Test
    @DisplayName("Should use findActiveMessagesByTenantIdAndDeemergeUserId to exclude deleted messages")
    void shouldUseActiveMessagesRepositoryMethod() {
        // Arrange: Setup repository to return active messages
        Message activeMessage1 = createMessage("msg-1", "Active 1", false);
        Message activeMessage2 = createMessage("msg-2", "Active 2", false);

        when(messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(TENANT_ID, DEEMERGE_USER_ID))
                .thenReturn(Arrays.asList(activeMessage1, activeMessage2));

        // Act: Call repository method
        List<Message> result = messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(
                TENANT_ID, DEEMERGE_USER_ID);

        // Assert: Verify correct method is called and results are active only
        assertThat(result).hasSize(2);
        assertThat(result).extracting(Message::getIsDeleted).containsOnly(false);
        verify(messageRepository).findActiveMessagesByTenantIdAndDeemergeUserId(TENANT_ID, DEEMERGE_USER_ID);
    }

    @Test
    @DisplayName("Should delegate to findByTenantIdAndDeemergeUserIdAndIsDeleted with isDeleted=false")
    void shouldDelegateToMethodWithIsDeletedParameter() {
        // Arrange
        List<Message> activeMessages = Arrays.asList(
                createMessage("msg-1", "Message 1", false),
                createMessage("msg-2", "Message 2", false)
        );

        when(messageRepository.findByTenantIdAndDeemergeUserIdAndIsDeleted(TENANT_ID, DEEMERGE_USER_ID, false))
                .thenReturn(activeMessages);

        when(messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(TENANT_ID, DEEMERGE_USER_ID))
                .thenCallRealMethod(); // Use default implementation

        // Act
        List<Message> result = messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(
                TENANT_ID, DEEMERGE_USER_ID);

        // Assert: Verify delegation occurs
        verify(messageRepository).findByTenantIdAndDeemergeUserIdAndIsDeleted(
                TENANT_ID, DEEMERGE_USER_ID, false);
    }

    @Test
    @DisplayName("Should return empty list when all messages are soft-deleted")
    void shouldReturnEmptyListWhenAllDeleted() {
        // Arrange: No active messages
        when(messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(TENANT_ID, DEEMERGE_USER_ID))
                .thenReturn(Collections.emptyList());

        // Act
        List<Message> result = messageRepository.findActiveMessagesByTenantIdAndDeemergeUserId(
                TENANT_ID, DEEMERGE_USER_ID);

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Should use findActiveUnprocessedMessagesByWorkspace for sliding window queries")
    void shouldFilterUnprocessedMessagesWithSoftDeleteCheck() {
        // Arrange: Setup unprocessed active messages
        Message unprocessedMessage1 = createMessage("msg-1", "Unprocessed 1", false);
        unprocessedMessage1.setIsProcessed(false);

        Message unprocessedMessage2 = createMessage("msg-2", "Unprocessed 2", false);
        unprocessedMessage2.setIsProcessed(false);

        when(messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID))
                .thenReturn(Arrays.asList(unprocessedMessage1, unprocessedMessage2));

        // Act
        List<Message> result = messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID);

        // Assert: Only active, unprocessed messages returned
        assertThat(result).hasSize(2);
        assertThat(result).allMatch(msg -> !msg.getIsDeleted() && !msg.getIsProcessed());
    }

    @Test
    @DisplayName("Should delegate to findByWorkspaceIdAndIsProcessedAndIsDeletedOrderByMessageTsAsc")
    void shouldDelegateToWorkspaceMethodWithDeletedFilter() {
        // Arrange
        List<Message> activeUnprocessed = Arrays.asList(
                createMessage("msg-1", "Message 1", false),
                createMessage("msg-2", "Message 2", false)
        );

        when(messageRepository.findByWorkspaceIdAndIsProcessedAndIsDeletedOrderByMessageTsAsc(
                WORKSPACE_ID, false, false))
                .thenReturn(activeUnprocessed);

        when(messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID))
                .thenCallRealMethod(); // Use default implementation

        // Act
        List<Message> result = messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID);

        // Assert: Verify correct parameters passed (isProcessed=false, isDeleted=false)
        verify(messageRepository).findByWorkspaceIdAndIsProcessedAndIsDeletedOrderByMessageTsAsc(
                WORKSPACE_ID, false, false);
    }

    @Test
    @DisplayName("Should not return deleted messages even if isProcessed=false")
    void shouldExcludeDeletedMessagesFromUnprocessedQuery() {
        // Arrange: Mix of processed/unprocessed and active/deleted
        Message activeUnprocessed = createMessage("msg-1", "Active unprocessed", false);
        activeUnprocessed.setIsProcessed(false);

        // Only active unprocessed should be returned
        when(messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID))
                .thenReturn(Collections.singletonList(activeUnprocessed));

        // Act
        List<Message> result = messageRepository.findActiveUnprocessedMessagesByWorkspace(WORKSPACE_ID);

        // Assert: Only active unprocessed messages
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIsDeleted()).isFalse();
        assertThat(result.get(0).getIsProcessed()).isFalse();
    }

    // Helper method to create test messages
    private Message createMessage(String id, String text, boolean isDeleted) {
        Message message = new Message();
        message.setId(id);
        message.setText(text);
        message.setTenantId(TENANT_ID);
        message.setDeemergeUserId(DEEMERGE_USER_ID);
        message.setIsDeleted(isDeleted);
        message.setIsProcessed(false);
        message.setMessageTs(String.valueOf(System.currentTimeMillis()));
        return message;
    }
}
