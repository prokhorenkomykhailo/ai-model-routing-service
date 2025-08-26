package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.audit.Audit;
import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.service.MessageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

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

    @GetMapping("/{id}")
    @Audit(action = "AI_MESSAGE_GET", description = "User retrieved message")
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

    @DeleteMapping("/{id}")
    @Audit(action = "AI_MESSAGE_DELETE", description = "User deleted message")
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

    @DeleteMapping
    @Audit(action = "AI_MESSAGES_DELETE_ALL", description = "User deleted all messages")
    @Operation(summary = "Delete all messages", description = "Deletes all messages from the system")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "All messages deleted successfully"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> deleteAllMessages() {
        logger.warn("⚠️ Request to delete ALL messages received");

        try {
            messageService.deleteAllMessages();
            logger.info("✅ Successfully deleted all messages");
            return ResponseEntity.noContent().build();

        } catch (Exception e) {
            logger.error("❌ Error deleting all messages", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping
    @Audit(action = "AI_MESSAGES_SEARCH", description = "User searched messages")
    @Operation(summary = "Search messages", description = "Search and filter messages with pagination")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Messages retrieved successfully")
    })
    public ResponseEntity<Page<MessageResponseDTO>> searchMessages(
            @Parameter(description = "Tenant ID") @RequestParam(required = false) String tenantId,
            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Sort field") @RequestParam(defaultValue = "ingestedAt") String sortBy,
            @Parameter(description = "Sort direction (ASC/DESC)") @RequestParam(defaultValue = "DESC") String sortDirection) {


        try {
            MessageSearchDTO searchDto = MessageSearchDTO.builder()
                    .tenantId(tenantId)
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

    // Helper methods

    private MessageResponseDTO convertToResponseDTO(Message message) {
        // log all fields for debugging
        logger.debug("Converting message to response DTO: {}", message);
        return MessageResponseDTO.builder()
                .id(message.getId())
                .source(message.getSource())
                .tenantId(message.getTenantId())
                .workspaceId(message.getWorkspaceId())
                .channelId(message.getChannelId())
                .threadTs(message.getThreadTs())
                .messageTs(message.getMessageTs())
                .userId(message.getUserId())
                .deemergeUserId(message.getDeemergeUserId())
                .channelName(message.getChannelName())
                .username(message.getUsername())
                .text(message.getText())
                .messageType(message.getMessageType())
                .subtype(message.getSubtype())
                .permaLink(message.getPermaLink())
                .metadata(message.getMetadata())
                .ingestedAt(message.getIngestedAt())
                .isProcessed(message.getIsProcessed())
                .build();
    }

    private Page<Message> searchMessages(MessageSearchDTO searchDto) {
        // Create pageable
        Sort.Direction direction = "ASC".equalsIgnoreCase(searchDto.getSortDirection())
                ? Sort.Direction.ASC : Sort.Direction.DESC;
        Pageable pageable = PageRequest.of(searchDto.getPage(), searchDto.getSize(),
                Sort.by(direction, searchDto.getSortBy()));

        // Support individual field searches
        if (searchDto.getTenantId() != null) {

            logger.debug("Searching by tenantId only: {}", searchDto.getTenantId());
            return messageService.findMessagesByTenantId(searchDto.getTenantId(), pageable);
        }

        logger.debug("Returning all messages with pagination");
        return messageService.findAllMessages(pageable);
    }
}
