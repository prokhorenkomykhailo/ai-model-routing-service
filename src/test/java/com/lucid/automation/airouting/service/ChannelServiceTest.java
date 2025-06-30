package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Channel;
import com.lucid.automation.airouting.repository.ChannelRepository;
import com.lucid.automation.slackingestion.dto.messaging.IngestionEventDTO;
import com.lucid.automation.slackingestion.dto.messaging.SlackMessageDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ChannelService
 * 
 * @author AI Assistant
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ChannelService Tests")
class ChannelServiceTest {
    
    @Mock
    private ChannelRepository channelRepository;
    
    @InjectMocks
    private ChannelService channelService;
    
    private Channel testChannel;
    private IngestionEventDTO testIngestionEvent;
    
    @BeforeEach
    void setUp() {
        testChannel = Channel.builder()
            .channelId("C1234567890")
            .channelSrc("slack")
            .channelName("general")
            .tenantId("tenant-123")
            .workspaceId("T1234567890")
            .channelType("channel")
            .isPrivate(false)
            .topic("General discussions")
            .purpose("Company-wide communication")
            .build();
        
        SlackMessageDTO slackMessage = SlackMessageDTO.builder()
            .channelId("C1234567890")
            .channelName("general")
            .teamId("T1234567890")
            .topic("General discussions")
            .purpose("Company-wide communication")
            .build();
        
        testIngestionEvent = IngestionEventDTO.builder()
            .tenantId("tenant-123")
            .message(slackMessage)
            .build();
    }
    
    @Test
    @DisplayName("Should save channel successfully")
    void shouldSaveChannelSuccessfully() {
        // Given
        when(channelRepository.save(any(Channel.class))).thenReturn(testChannel);
        
        // When
        Channel result = channelService.saveChannel(testChannel);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getChannelId()).isEqualTo("C1234567890");
        assertThat(result.getChannelName()).isEqualTo("general");
        verify(channelRepository).save(testChannel);
    }
    
    @Test
    @DisplayName("Should return null when saving null channel")
    void shouldReturnNullWhenSavingNullChannel() {
        // When
        Channel result = channelService.saveChannel(null);
        
        // Then
        assertThat(result).isNull();
        verify(channelRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Should find channel by ID")
    void shouldFindChannelById() {
        // Given
        when(channelRepository.findByChannelId("C1234567890")).thenReturn(Optional.of(testChannel));
        
        // When
        Optional<Channel> result = channelService.findByChannelId("C1234567890");
        
        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getChannelId()).isEqualTo("C1234567890");
        verify(channelRepository).findByChannelId("C1234567890");
    }
    
    @Test
    @DisplayName("Should return empty when channel not found")
    void shouldReturnEmptyWhenChannelNotFound() {
        // Given
        when(channelRepository.findByChannelId("NONEXISTENT")).thenReturn(Optional.empty());
        
        // When
        Optional<Channel> result = channelService.findByChannelId("NONEXISTENT");
        
        // Then
        assertThat(result).isEmpty();
        verify(channelRepository).findByChannelId("NONEXISTENT");
    }
    
    @Test
    @DisplayName("Should find channels by source")
    void shouldFindChannelsBySource() {
        // Given
        when(channelRepository.findByChannelSrc("slack")).thenReturn(List.of(testChannel));
        
        // When
        List<Channel> result = channelService.findByChannelSrc("slack");
        
        // Then
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getChannelSrc()).isEqualTo("slack");
        verify(channelRepository).findByChannelSrc("slack");
    }
    
    @Test
    @DisplayName("Should check if channel exists")
    void shouldCheckIfChannelExists() {
        // Given
        when(channelRepository.existsByChannelId("C1234567890")).thenReturn(true);
        
        // When
        boolean result = channelService.channelExists("C1234567890");
        
        // Then
        assertThat(result).isTrue();
        verify(channelRepository).existsByChannelId("C1234567890");
    }
    
    @Test
    @DisplayName("Should create new channel from ingestion event")
    void shouldCreateNewChannelFromIngestionEvent() {
        // Given
        when(channelRepository.findByChannelId("C1234567890")).thenReturn(Optional.empty());
        when(channelRepository.save(any(Channel.class))).thenReturn(testChannel);
        
        // When
        Channel result = channelService.createOrUpdateChannelFromEvent(testIngestionEvent);
        
        // Then
        assertThat(result).isNotNull();
        assertThat(result.getChannelId()).isEqualTo("C1234567890");
        assertThat(result.getChannelSrc()).isEqualTo("slack");
        verify(channelRepository).findByChannelId("C1234567890");
        verify(channelRepository).save(any(Channel.class));
    }
    
    @Test
    @DisplayName("Should update existing channel from ingestion event")
    void shouldUpdateExistingChannelFromIngestionEvent() {
        // Given
        when(channelRepository.findByChannelId("C1234567890")).thenReturn(Optional.of(testChannel));
        when(channelRepository.save(any(Channel.class))).thenReturn(testChannel);
        
        // When
        Channel result = channelService.createOrUpdateChannelFromEvent(testIngestionEvent);
        
        // Then
        assertThat(result).isNotNull();
        verify(channelRepository).findByChannelId("C1234567890");
        verify(channelRepository).save(any(Channel.class));
    }
    
    @Test
    @DisplayName("Should return null for invalid ingestion event")
    void shouldReturnNullForInvalidIngestionEvent() {
        // When
        Channel result = channelService.createOrUpdateChannelFromEvent(null);
        
        // Then
        assertThat(result).isNull();
        verify(channelRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Should delete channel by ID")
    void shouldDeleteChannelById() {
        // When
        channelService.deleteById("C1234567890");
        
        // Then
        verify(channelRepository).deleteById("C1234567890");
    }
    
    @Test
    @DisplayName("Should handle empty channel ID gracefully")
    void shouldHandleEmptyChannelIdGracefully() {
        // When
        Optional<Channel> result = channelService.findByChannelId("");
        
        // Then
        assertThat(result).isEmpty();
        verify(channelRepository, never()).findByChannelId(anyString());
    }
}
