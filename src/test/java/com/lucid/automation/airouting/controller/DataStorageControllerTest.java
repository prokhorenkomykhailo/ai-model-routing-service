package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.MessageWithContentDTO;
import com.lucid.automation.airouting.service.DataStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DataStorageController
 */
@ExtendWith(MockitoExtension.class)
class DataStorageControllerTest {

    @Mock
    private DataStorageService dataStorageService;

    @InjectMocks
    private DataStorageController dataStorageController;

    private static final String TENANT_ID = "tenant-123";
    private static final String TENANT_SCHEMA = "tenant_schema";
    private static final String GROUP_ID = "group-456";

    @BeforeEach
    void setUp() {
        // Mock setup is handled by @Mock annotations
    }

    @Test
    void getAllGroupIds_ShouldReturnListOfGroupIds() {
        // Arrange
        List<String> expectedGroupIds = Arrays.asList("group1", "group2", "group3");
        when(dataStorageService.getAllGroupIds(TENANT_ID, TENANT_SCHEMA))
                .thenReturn(expectedGroupIds);

        // Act
        ResponseEntity<List<String>> response = dataStorageController
                .getAllGroupIds(TENANT_ID, TENANT_SCHEMA);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(expectedGroupIds, response.getBody());
        verify(dataStorageService, times(1)).getAllGroupIds(TENANT_ID, TENANT_SCHEMA);
    }

    @Test
    void getAllMessagesByGroupId_ShouldReturnListOfMessages() {
        // Arrange
        MessageWithContentDTO message1 = MessageWithContentDTO.builder()
                .id(UUID.randomUUID())
                .groupId(GROUP_ID)
                .content("Test message 1")
                .messageTimestamp(LocalDateTime.now())
                .build();
        
        MessageWithContentDTO message2 = MessageWithContentDTO.builder()
                .id(UUID.randomUUID())
                .groupId(GROUP_ID)
                .content("Test message 2")
                .messageTimestamp(LocalDateTime.now())
                .build();

        List<MessageWithContentDTO> expectedMessages = Arrays.asList(message1, message2);
        when(dataStorageService.getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA))
                .thenReturn(expectedMessages);

        // Act
        ResponseEntity<List<MessageWithContentDTO>> response = dataStorageController
                .getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(expectedMessages, response.getBody());
        assertEquals(2, response.getBody().size());
        verify(dataStorageService, times(1))
                .getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA);
    }

    @Test
    void getAllGroupIds_ShouldHandleEmptyList() {
        // Arrange
        List<String> emptyGroupIds = Arrays.asList();
        when(dataStorageService.getAllGroupIds(TENANT_ID, TENANT_SCHEMA))
                .thenReturn(emptyGroupIds);

        // Act
        ResponseEntity<List<String>> response = dataStorageController
                .getAllGroupIds(TENANT_ID, TENANT_SCHEMA);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().isEmpty());
        verify(dataStorageService, times(1)).getAllGroupIds(TENANT_ID, TENANT_SCHEMA);
    }

    @Test
    void getAllMessagesByGroupId_ShouldHandleEmptyList() {
        // Arrange
        List<MessageWithContentDTO> emptyMessages = Arrays.asList();
        when(dataStorageService.getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA))
                .thenReturn(emptyMessages);

        // Act
        ResponseEntity<List<MessageWithContentDTO>> response = dataStorageController
                .getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().isEmpty());
        verify(dataStorageService, times(1))
                .getAllMessagesByGroupId(GROUP_ID, TENANT_ID, TENANT_SCHEMA);
    }
}
