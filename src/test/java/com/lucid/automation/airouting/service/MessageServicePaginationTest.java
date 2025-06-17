package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.lucid.automation.airouting.repository.MessageRepository;

/**
 * Test class for MessageService paginated query methods
 */
@ExtendWith(MockitoExtension.class)
class MessageServicePaginationTest {

    @Mock
    private MessageRepository messageRepository;

    @InjectMocks
    private MessageService messageService;

    @Test
    @DisplayName("Should get first 1000 messages for workspace")
    void shouldGetFirst1000MessagesForWorkspace() {
        // Arrange
        String workspaceId = "workspace-123";
        List<Message> mockMessages = Arrays.asList(
                createMockMessage("msg1", workspaceId),
                createMockMessage("msg2", workspaceId)
        );
        
        Pageable expectedPageable = PageRequest.of(0, 1000, Sort.by(Sort.Direction.ASC, "messageTs"));
        when(messageRepository.findByWorkspaceId(eq(workspaceId), eq(expectedPageable)))
                .thenReturn(mockMessages);

        // Act
        List<Message> result = messageService.getFirst1000MessagesForWorkspace(workspaceId);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(mockMessages, result);
    }

    @Test
    @DisplayName("Should return empty list for invalid workspace ID")
    void shouldReturnEmptyListForInvalidWorkspaceId() {
        // Act
        List<Message> result = messageService.getFirst1000MessagesForWorkspace("");

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Should get first N messages for workspace with custom limit")
    void shouldGetFirstNMessagesForWorkspaceWithCustomLimit() {
        // Arrange
        String workspaceId = "workspace-123";
        int customLimit = 500;
        List<Message> mockMessages = Arrays.asList(createMockMessage("msg1", workspaceId));
        
        Pageable expectedPageable = PageRequest.of(0, customLimit, Sort.by(Sort.Direction.ASC, "messageTs"));
        when(messageRepository.findByWorkspaceId(eq(workspaceId), eq(expectedPageable)))
                .thenReturn(mockMessages);

        // Act
        List<Message> result = messageService.getFirstMessagesForWorkspace(workspaceId, customLimit);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(mockMessages, result);
    }

    private Message createMockMessage(String messageId, String workspaceId) {
        return Message.builder()
                .id(messageId)
                .workspaceId(workspaceId)
                .tenantId("tenant-123")
                .channelId("channel-123")
                .messageTs("1234567890")
                .text("Test message")
                .build();
    }
}
