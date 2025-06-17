package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.service.MessageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/messages")
@CrossOrigin(origins = "*")
@Tag(name = "Message Management", description = "CRUD operations for message management")
public class MessageController {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);
    
    private final MessageService messageService;
    
    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }
    
    @PostMapping
    @Operation(summary = "Create a new message", description = "Creates a new message in the system")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Message created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request data"),
        @ApiResponse(responseCode = "409", description = "Message already exists")
    })
    public ResponseEntity<MessageResponseDTO> createMessage(@Valid @RequestBody CreateMessageDTO createDto) {
        logger.info("Creating new message for tenant: {}, workspace: {}, channel: {}", 
                   createDto.getTenantId(), createDto.getWorkspaceId(), createDto.getChannelId());
        
        try {
            // Convert DTO to Message entity
            Message message = convertToMessage(createDto);
            
            // Check if message already exists
            Optional<Message> existingMessage = messageService.findById(message.getId());
            if (existingMessage.isPresent()) {
                logger.warn("Message with ID {} already exists", message.getId());
                return ResponseEntity.status(HttpStatus.CONFLICT).build();
            }
            
            // Store the message
            Message savedMessage = messageService.storeMessage(message);
            
            // Convert to response DTO
            MessageResponseDTO responseDto = convertToResponseDTO(savedMessage);
            
            logger.info("Successfully created message with ID: {}", savedMessage.getId());
            return ResponseEntity.status(HttpStatus.CREATED).body(responseDto);
            
        } catch (Exception e) {
            logger.error("Error creating message", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/{id}")
    @Operation(summary = "Get message by ID", description = "Retrieves a specific message by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Message found"),
        @ApiResponse(responseCode = "404", description = "Message not found")
    })
    public ResponseEntity<MessageResponseDTO> getMessageById(
            @Parameter(description = "Message ID") @PathVariable String id) {
        
        logger.debug("Retrieving message with ID: {}", id);
        
        Optional<Message> message = messageService.findById(id);
        
        if (message.isPresent()) {
            MessageResponseDTO responseDto = convertToResponseDTO(message.get());
            return ResponseEntity.ok(responseDto);
        } else {
            logger.warn("Message not found with ID: {}", id);
            return ResponseEntity.notFound().build();
        }
    }
    
    @PutMapping("/{id}")
    @Operation(summary = "Update message", description = "Updates an existing message")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Message updated successfully"),
        @ApiResponse(responseCode = "404", description = "Message not found"),
        @ApiResponse(responseCode = "400", description = "Invalid request data")
    })
    public ResponseEntity<MessageResponseDTO> updateMessage(
            @Parameter(description = "Message ID") @PathVariable String id,
            @Valid @RequestBody UpdateMessageDTO updateDto) {
        
        logger.info("Updating message with ID: {}", id);
        
        try {
            Optional<Message> existingMessage = messageService.findById(id);
            
            if (existingMessage.isEmpty()) {
                logger.warn("Message not found with ID: {}", id);
                return ResponseEntity.notFound().build();
            }
            
            // Update the message
            Message message = existingMessage.get();
            updateMessageFromDTO(message, updateDto);
            
            Message updatedMessage = messageService.storeMessage(message);
            MessageResponseDTO responseDto = convertToResponseDTO(updatedMessage);
            
            logger.info("Successfully updated message with ID: {}", id);
            return ResponseEntity.ok(responseDto);
            
        } catch (Exception e) {
            logger.error("Error updating message with ID: {}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @DeleteMapping("/{id}")
    @Operation(summary = "Delete message", description = "Deletes a message by its ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Message deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Message not found")
    })
    public ResponseEntity<Void> deleteMessage(@Parameter(description = "Message ID") @PathVariable String id) {
        logger.info("Deleting message with ID: {}", id);
        
        try {
            Optional<Message> existingMessage = messageService.findById(id);
            
            if (existingMessage.isEmpty()) {
                logger.warn("Message not found with ID: {}", id);
                return ResponseEntity.notFound().build();
            }
            
            messageService.deleteById(id);
            logger.info("Successfully deleted message with ID: {}", id);
            return ResponseEntity.noContent().build();
            
        } catch (Exception e) {
            logger.error("Error deleting message with ID: {}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping
    @Operation(summary = "Search messages", description = "Search and filter messages with pagination")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Messages retrieved successfully")
    })
    public ResponseEntity<Page<MessageResponseDTO>> searchMessages(
            @Parameter(description = "Tenant ID") @RequestParam(required = false) String tenantId,
            @Parameter(description = "Workspace ID") @RequestParam(required = false) String workspaceId,
            @Parameter(description = "Channel ID") @RequestParam(required = false) String channelId,
            @Parameter(description = "Thread timestamp") @RequestParam(required = false) String threadTs,
            @Parameter(description = "User ID") @RequestParam(required = false) String userId,
            @Parameter(description = "Message type") @RequestParam(required = false) String messageType,
            @Parameter(description = "Message subtype") @RequestParam(required = false) String subtype,
            @Parameter(description = "Start time (epoch milliseconds)") @RequestParam(required = false) Long startTime,
            @Parameter(description = "End time (epoch milliseconds)") @RequestParam(required = false) Long endTime,
            @Parameter(description = "Text contains") @RequestParam(required = false) String textContains,
            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Sort field") @RequestParam(defaultValue = "ingestedAt") String sortBy,
            @Parameter(description = "Sort direction (ASC/DESC)") @RequestParam(defaultValue = "DESC") String sortDirection) {
        
        logger.debug("Searching messages with filters - tenant: {}, workspace: {}, channel: {}", 
                    tenantId, workspaceId, channelId);
        
        try {
            MessageSearchDTO searchDto = MessageSearchDTO.builder()
                    .tenantId(tenantId)
                    .workspaceId(workspaceId)
                    .channelId(channelId)
                    .threadTs(threadTs)
                    .userId(userId)
                    .messageType(messageType)
                    .subtype(subtype)
                    .startTime(startTime)
                    .endTime(endTime)
                    .textContains(textContains)
                    .page(page)
                    .size(size)
                    .sortBy(sortBy)
                    .sortDirection(sortDirection)
                    .build();
            
            Page<Message> messages = searchMessages(searchDto);
            
            // Convert to response DTOs
            Page<MessageResponseDTO> responsePage = messages.map(this::convertToResponseDTO);
            
            logger.debug("Found {} messages matching search criteria", messages.getTotalElements());
            return ResponseEntity.ok(responsePage);
            
        } catch (Exception e) {
            logger.error("Error searching messages", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/optimized/tenant-workspace")
    @Operation(summary = "Get messages by tenant and workspace (optimized)", 
               description = "Uses composite index for optimal performance")
    public ResponseEntity<Page<MessageResponseDTO>> getMessagesByTenantAndWorkspace(
            @Parameter(description = "Tenant ID", required = true) @RequestParam String tenantId,
            @Parameter(description = "Workspace ID", required = true) @RequestParam String workspaceId,
            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "1000") int size) {
        
        logger.debug("Getting messages for tenant: {} and workspace: {} (optimized)", tenantId, workspaceId);
        
        try {
            Page<Message> messages = messageService.getFirstMessagesByTenantAndWorkspaceOptimized(tenantId, workspaceId, size);
            Page<MessageResponseDTO> responsePage = messages.map(this::convertToResponseDTO);
            
            return ResponseEntity.ok(responsePage);
            
        } catch (Exception e) {
            logger.error("Error getting messages by tenant and workspace", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/optimized/workspace-channel-thread")
    @Operation(summary = "Get messages by workspace, channel and thread (optimized)", 
               description = "Uses composite index for optimal performance")
    public ResponseEntity<Page<MessageResponseDTO>> getMessagesByWorkspaceChannelThread(
            @Parameter(description = "Workspace ID", required = true) @RequestParam String workspaceId,
            @Parameter(description = "Channel ID", required = true) @RequestParam String channelId,
            @Parameter(description = "Thread timestamp", required = true) @RequestParam String threadTs,
            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "1000") int size) {
        
        logger.debug("Getting messages for workspace: {}, channel: {}, thread: {} (optimized)", 
                    workspaceId, channelId, threadTs);
        
        try {
            Page<Message> messages = messageService.getFirstMessagesByWorkspaceChannelThreadOptimized(
                    workspaceId, channelId, threadTs, size);
            Page<MessageResponseDTO> responsePage = messages.map(this::convertToResponseDTO);
            
            return ResponseEntity.ok(responsePage);
            
        } catch (Exception e) {
            logger.error("Error getting messages by workspace, channel and thread", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/count")
    @Operation(summary = "Count messages", description = "Count messages matching the given criteria")
    public ResponseEntity<Long> countMessages(
            @Parameter(description = "Tenant ID") @RequestParam(required = false) String tenantId,
            @Parameter(description = "Workspace ID") @RequestParam(required = false) String workspaceId,
            @Parameter(description = "Channel ID") @RequestParam(required = false) String channelId,
            @Parameter(description = "Thread timestamp") @RequestParam(required = false) String threadTs) {
        
        try {
            long count;
            
            // Use optimized methods when possible
            if (tenantId != null && workspaceId != null && channelId == null && threadTs == null) {
                count = messageService.countMessagesByTenantAndWorkspaceOptimized(tenantId, workspaceId);
            } else if (workspaceId != null && channelId != null && threadTs != null) {
                count = messageService.countMessagesByWorkspaceChannelThreadOptimized(workspaceId, channelId, threadTs);
            } else if (tenantId != null && workspaceId != null && channelId != null && threadTs != null) {
                count = messageService.getMessageCount(tenantId, workspaceId, channelId, threadTs);
            } else {
                // Fall back to repository method or return error for unsupported combinations
                logger.warn("Unsupported count combination - tenant: {}, workspace: {}, channel: {}, thread: {}", 
                           tenantId, workspaceId, channelId, threadTs);
                return ResponseEntity.badRequest().build();
            }
            
            return ResponseEntity.ok(count);
            
        } catch (Exception e) {
            logger.error("Error counting messages", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    // Helper methods
    
    private Message convertToMessage(CreateMessageDTO dto) {
        String threadTs = StringUtils.hasText(dto.getThreadTs()) ? dto.getThreadTs() : dto.getMessageTs();
        
        // Generate unique ID
        String id = String.join(":", 
                dto.getTenantId(), 
                dto.getWorkspaceId(), 
                dto.getChannelId(), 
                threadTs, 
                dto.getMessageTs());
        
        Message message = Message.builder()
                .id(id)
                .tenantId(dto.getTenantId())
                .workspaceId(dto.getWorkspaceId())
                .channelId(dto.getChannelId())
                .threadTs(threadTs)
                .messageTs(dto.getMessageTs())
                .userId(dto.getUserId())
                .username(dto.getUsername())
                .text(dto.getText())
                .messageType(dto.getMessageType())
                .subtype(dto.getSubtype())
                .metadata(dto.getMetadata())
                .ingestedAt(Instant.now().toEpochMilli())
                .build();
                
        // Update composite indexes
        message.updateCompositeIndexes();
        
        return message;
    }
    
    private void updateMessageFromDTO(Message message, UpdateMessageDTO dto) {
        if (StringUtils.hasText(dto.getUsername())) {
            message.setUsername(dto.getUsername());
        }
        if (StringUtils.hasText(dto.getText())) {
            message.setText(dto.getText());
        }
        if (StringUtils.hasText(dto.getMessageType())) {
            message.setMessageType(dto.getMessageType());
        }
        if (StringUtils.hasText(dto.getSubtype())) {
            message.setSubtype(dto.getSubtype());
        }
        if (dto.getMetadata() != null) {
            message.setMetadata(dto.getMetadata());
        }
    }
    
    private MessageResponseDTO convertToResponseDTO(Message message) {
        return MessageResponseDTO.builder()
                .id(message.getId())
                .tenantId(message.getTenantId())
                .workspaceId(message.getWorkspaceId())
                .channelId(message.getChannelId())
                .threadTs(message.getThreadTs())
                .messageTs(message.getMessageTs())
                .userId(message.getUserId())
                .username(message.getUsername())
                .text(message.getText())
                .messageType(message.getMessageType())
                .subtype(message.getSubtype())
                .metadata(message.getMetadata())
                .ingestedAt(message.getIngestedAt())
                .build();
    }
    
    private Page<Message> searchMessages(MessageSearchDTO searchDto) {
        // Create pageable
        Sort.Direction direction = "ASC".equalsIgnoreCase(searchDto.getSortDirection()) 
                ? Sort.Direction.ASC : Sort.Direction.DESC;
        Pageable pageable = PageRequest.of(searchDto.getPage(), searchDto.getSize(), 
                Sort.by(direction, searchDto.getSortBy()));
        
        // Use optimized methods when possible
        if (searchDto.getTenantId() != null && searchDto.getWorkspaceId() != null && 
            searchDto.getChannelId() == null && searchDto.getThreadTs() == null) {
            
            String compositeKey = searchDto.getTenantId() + ":" + searchDto.getWorkspaceId();
            return messageService.findByTenantWorkspaceIndex(compositeKey, pageable);
        }
        
        if (searchDto.getWorkspaceId() != null && searchDto.getChannelId() != null && 
            searchDto.getThreadTs() != null) {
            
            String compositeKey = searchDto.getWorkspaceId() + ":" + searchDto.getChannelId() + ":" + searchDto.getThreadTs();
            return messageService.findByWorkspaceChannelThreadIndex(compositeKey, pageable);
        }
        
        // Support individual field searches
        if (searchDto.getTenantId() != null && searchDto.getWorkspaceId() == null && 
            searchDto.getChannelId() == null && searchDto.getThreadTs() == null) {
            
            logger.debug("Searching by tenantId only: {}", searchDto.getTenantId());
            return messageService.findMessagesByTenantId(searchDto.getTenantId(), pageable);
        }
        
        if (searchDto.getWorkspaceId() != null && searchDto.getTenantId() == null && 
            searchDto.getChannelId() == null && searchDto.getThreadTs() == null) {
            
            logger.debug("Searching by workspaceId only: {}", searchDto.getWorkspaceId());
            return messageService.findMessagesByWorkspaceId(searchDto.getWorkspaceId(), pageable);
        }
        
        if (searchDto.getChannelId() != null && searchDto.getTenantId() == null && 
            searchDto.getWorkspaceId() == null && searchDto.getThreadTs() == null) {
            
            logger.debug("Searching by channelId only: {}", searchDto.getChannelId());
            return messageService.findMessagesByChannelId(searchDto.getChannelId(), pageable);
        }
        
        if (searchDto.getThreadTs() != null && searchDto.getTenantId() == null && 
            searchDto.getWorkspaceId() == null && searchDto.getChannelId() == null) {
            
            logger.debug("Searching by threadTs only: {}", searchDto.getThreadTs());
            return messageService.findMessagesByThreadTs(searchDto.getThreadTs(), pageable);
        }
        
        // Support search by workspaceId only
        if (searchDto.getWorkspaceId() != null && searchDto.getTenantId() == null && 
            searchDto.getChannelId() == null && searchDto.getThreadTs() == null &&
            searchDto.getUserId() == null && searchDto.getMessageType() == null &&
            searchDto.getSubtype() == null && searchDto.getTextContains() == null &&
            searchDto.getStartTime() == null && searchDto.getEndTime() == null) {
            
            logger.debug("Returning messages for workspace: {}", searchDto.getWorkspaceId());
            return messageService.findMessagesByWorkspaceId(searchDto.getWorkspaceId(), pageable);
        }
        
        // If no specific filters are provided, return all messages with pagination
        if (searchDto.getTenantId() == null && searchDto.getWorkspaceId() == null && 
            searchDto.getChannelId() == null && searchDto.getThreadTs() == null &&
            searchDto.getUserId() == null && searchDto.getMessageType() == null &&
            searchDto.getSubtype() == null && searchDto.getTextContains() == null &&
            searchDto.getStartTime() == null && searchDto.getEndTime() == null) {
            
            logger.debug("Returning all messages with pagination");
            return messageService.findAllMessages(pageable);
        }
        
        // For complex searches, we would need to implement additional repository methods
        // For now, return empty page for unsupported combinations
        logger.warn("Complex search not yet implemented for criteria: {}", searchDto);
        return Page.empty(pageable);
    }
}
