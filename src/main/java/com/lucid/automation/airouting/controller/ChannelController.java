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
    
    @GetMapping("/{channelId}")
    @Operation(summary = "Get channel by ID", description = "Retrieves a specific channel by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channel found"),
        @ApiResponse(responseCode = "404", description = "Channel not found"),
        @ApiResponse(responseCode = "400", description = "Invalid channel ID")
    })
    public ResponseEntity<ChannelResponseDTO> getChannelById(
            @Parameter(description = "Channel ID") @PathVariable String channelId) {
        
        logger.debug("Retrieving channel with ID: {}", channelId);
        
        if (channelId == null || channelId.trim().isEmpty()) {
            logger.warn("Invalid channel ID provided: {}", channelId);
            return ResponseEntity.badRequest().build();
        }
        
        Optional<Channel> channel = channelService.findByChannelId(channelId);
        
        if (channel.isPresent()) {
            ChannelResponseDTO responseDto = convertToResponseDTO(channel.get());
            return ResponseEntity.ok(responseDto);
        } else {
            logger.warn("Channel not found with ID: {}", channelId);
            return ResponseEntity.notFound().build();
        }
    }
    
    @GetMapping("/by-source/{channelSrc}")
    @Operation(summary = "Get channels by source", description = "Retrieves channels by their source (slack, email, etc.)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channels found"),
        @ApiResponse(responseCode = "400", description = "Invalid channel source")
    })
    public ResponseEntity<List<ChannelResponseDTO>> getChannelsBySource(
            @Parameter(description = "Channel source (slack, email, etc.)") @PathVariable String channelSrc) {
        
        logger.debug("Retrieving channels with source: {}", channelSrc);
        
        if (channelSrc == null || channelSrc.trim().isEmpty()) {
            logger.warn("Invalid channel source provided: {}", channelSrc);
            return ResponseEntity.badRequest().build();
        }
        
        List<Channel> channels = channelService.findByChannelSrc(channelSrc);
        List<ChannelResponseDTO> responseDtos = channels.stream()
            .map(this::convertToResponseDTO)
            .toList();
        return ResponseEntity.ok(responseDtos);
    }
    
    @GetMapping("/by-tenant/{tenantId}")
    @Operation(summary = "Get channels by tenant", description = "Retrieves channels for a specific tenant")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channels found"),
        @ApiResponse(responseCode = "400", description = "Invalid tenant ID")
    })
    public ResponseEntity<List<ChannelResponseDTO>> getChannelsByTenant(
            @Parameter(description = "Tenant ID") @PathVariable String tenantId) {
        
        logger.debug("Retrieving channels for tenant: {}", tenantId);
        
        if (tenantId == null || tenantId.trim().isEmpty()) {
            logger.warn("Invalid tenant ID provided: {}", tenantId);
            return ResponseEntity.badRequest().build();
        }
        
        List<Channel> channels = channelService.findByTenantId(tenantId);
        List<ChannelResponseDTO> responseDtos = channels.stream()
            .map(this::convertToResponseDTO)
            .toList();
        return ResponseEntity.ok(responseDtos);
    }
    
    @GetMapping("/by-workspace/{workspaceId}")
    @Operation(summary = "Get channels by workspace", description = "Retrieves channels for a specific workspace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channels found"),
        @ApiResponse(responseCode = "400", description = "Invalid workspace ID")
    })
    public ResponseEntity<List<ChannelResponseDTO>> getChannelsByWorkspace(
            @Parameter(description = "Workspace ID") @PathVariable String workspaceId) {
        
        logger.debug("Retrieving channels for workspace: {}", workspaceId);
        
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            logger.warn("Invalid workspace ID provided: {}", workspaceId);
            return ResponseEntity.badRequest().build();
        }
        
        List<Channel> channels = channelService.findByWorkspaceId(workspaceId);
        List<ChannelResponseDTO> responseDtos = channels.stream()
            .map(this::convertToResponseDTO)
            .toList();
        return ResponseEntity.ok(responseDtos);
    }
    
    @PostMapping
    @Operation(summary = "Create or update channel", description = "Creates a new channel or updates an existing one")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Channel created/updated successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid channel data")
    })
    public ResponseEntity<ChannelResponseDTO> createOrUpdateChannel(@RequestBody Channel channel) {
        logger.info("Creating/updating channel: {}", channel.getChannelId());
        
        if (channel == null || channel.getChannelId() == null || channel.getChannelId().trim().isEmpty()) {
            logger.warn("Invalid channel data provided");
            return ResponseEntity.badRequest().build();
        }
        
        Channel savedChannel = channelService.saveChannel(channel);
        
        if (savedChannel != null) {
            ChannelResponseDTO responseDto = convertToResponseDTO(savedChannel);
            return ResponseEntity.ok(responseDto);
        } else {
            logger.error("Failed to save channel: {}", channel.getChannelId());
            return ResponseEntity.internalServerError().build();
        }
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
    
    @GetMapping("/{channelId}/exists")
    @Operation(summary = "Check if channel exists", description = "Checks if a channel exists by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Check completed"),
        @ApiResponse(responseCode = "400", description = "Invalid channel ID")
    })
    public ResponseEntity<Boolean> channelExists(
            @Parameter(description = "Channel ID") @PathVariable String channelId) {
        
        logger.debug("Checking if channel exists: {}", channelId);
        
        if (channelId == null || channelId.trim().isEmpty()) {
            logger.warn("Invalid channel ID provided: {}", channelId);
            return ResponseEntity.badRequest().build();
        }
        
        boolean exists = channelService.channelExists(channelId);
        return ResponseEntity.ok(exists);
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
