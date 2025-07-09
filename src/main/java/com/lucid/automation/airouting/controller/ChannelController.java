package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.ChannelResponseDTO;
import com.lucid.automation.airouting.model.Channel;
import com.lucid.automation.airouting.service.ChannelService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

/**
 * REST controller for channel management
 * 
 * @author AI Assistant
 */
@RestController
@RequestMapping("/api/channels")
@CrossOrigin(origins = "*")
@Tag(name = "Channel Management", description = "CRUD operations for channel management")
public class ChannelController {
    
    private static final Logger logger = LoggerFactory.getLogger(ChannelController.class);
    
    private final ChannelService channelService;
    
    public ChannelController(ChannelService channelService) {
        this.channelService = channelService;
    }
    
    @GetMapping
    @Operation(summary = "Get all channels", description = "Retrieves all channels")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channels retrieved successfully")
    })
    public ResponseEntity<List<ChannelResponseDTO>> getAllChannels() {
        logger.debug("Retrieving all channels");
        
        List<Channel> channels = channelService.findAll();
        List<ChannelResponseDTO> responseDtos = channels.stream()
            .map(this::convertToResponseDTO)
            .toList();
        
        logger.info("Retrieved {} channels", responseDtos.size());
        return ResponseEntity.ok(responseDtos);
    }

    @DeleteMapping("/{channelId}")
    @Operation(summary = "Delete channel", description = "Deletes a channel by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Channel deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Channel not found"),
        @ApiResponse(responseCode = "400", description = "Invalid channel ID")
    })
    public ResponseEntity<Void> deleteChannel(
            @Parameter(description = "Channel ID") @PathVariable String channelId) {
        
        logger.info("Deleting channel with ID: {}", channelId);
        
        if (channelId == null || channelId.trim().isEmpty()) {
            logger.warn("Invalid channel ID provided: {}", channelId);
            return ResponseEntity.badRequest().build();
        }
        
        try {
            Optional<Channel> existingChannel = channelService.findByChannelId(channelId);
            
            if (existingChannel.isEmpty()) {
                logger.warn("Channel not found with ID: {}", channelId);
                return ResponseEntity.notFound().build();
            }
            
            channelService.deleteById(channelId);
            logger.info("Successfully deleted channel with ID: {}", channelId);
            return ResponseEntity.noContent().build();
            
        } catch (Exception e) {
            logger.error("Failed to delete channel {}: {}", channelId, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Converts Channel entity to ChannelResponseDTO
     * 
     * @param channel The channel entity
     * @return ChannelResponseDTO
     */
    private ChannelResponseDTO convertToResponseDTO(Channel channel) {
        return ChannelResponseDTO.builder()
            .channelId(channel.getChannelId())
            .channelSrc(channel.getChannelSrc())
            .channelName(channel.getChannelName())
            .tenantId(channel.getTenantId())
            .workspaceId(channel.getWorkspaceId())
            .channelType(channel.getChannelType())
            .isPrivate(channel.getIsPrivate())
            .topic(channel.getTopic())
            .purpose(channel.getPurpose())
            .createdAt(channel.getCreatedAt())
            .updatedAt(channel.getUpdatedAt())
            .compositeKey(channel.getCompositeKey())
            .build();
    }
}