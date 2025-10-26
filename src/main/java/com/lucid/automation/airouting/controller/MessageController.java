package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.audit.Audit;
import com.lucid.automation.airouting.dto.*;
import com.lucid.automation.airouting.model.Message;
import com.lucid.automation.airouting.service.MessageService;
import com.lucid.automation.airouting.service.MessageStatisticsService;
import com.lucid.automation.common.dto.response.APIResponse;
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

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/messages")
@Tag(name = "Message Management", description = "CRUD operations for message management")
public class MessageController {

    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

    private final MessageService messageService;
    private final MessageStatisticsService statisticsService;

    public MessageController(MessageService messageService, MessageStatisticsService statisticsService) {
        this.messageService = messageService;
        this.statisticsService = statisticsService;
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
    @Operation(summary = "Delete message", description = "Soft deletes a message by its ID. The message will be marked as deleted and retained for 30 days before permanent removal.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Message marked as deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Message not found")
    })
    public ResponseEntity<APIResponse<Void>> deleteMessage(
            @Parameter(description = "Message ID") @PathVariable String id,
            @RequestHeader(value = "X-User-ID", required = false) String userId) {
        logger.info("🗑️ [SOFT-DELETE] Soft deleting message with ID: {} by user: {}", id, userId);

        try {
            Optional<Message> existingMessage = messageService.findById(id);

            if (existingMessage.isEmpty()) {
                logger.warn("⚠️ [SOFT-DELETE] Message not found with ID: {}", id);
                return ResponseEntity.status(404)
                        .body(APIResponse.<Void>builder()
                                .success(false)
                                .message("Message not found")
                                .build());
            }

            // Use "UNKNOWN" if userId not provided
            String deletedBy = (userId != null && !userId.isBlank()) ? userId : "UNKNOWN";
            messageService.softDeleteById(id, deletedBy, "MANUAL");
            
            logger.info("✅ [SOFT-DELETE] Message marked as deleted with ID: {} by user: {}. Will be removed after 30 days.", id, deletedBy);
            return ResponseEntity.ok(APIResponse.<Void>builder()
                    .success(true)
                    .message("Message marked as deleted. It will be permanently removed after 30 days.")
                    .build());

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

    @GetMapping("/statistics")
    @Audit(action = "AI_MESSAGES_STATISTICS", description = "User requested message statistics")
    @Operation(summary = "Get tenant message statistics",
               description = "Returns aggregated metrics for tenant messages in Redis")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Statistics generated successfully"),
        @ApiResponse(responseCode = "400", description = "Missing required tenantId parameter"),
        @ApiResponse(responseCode = "401", description = "Unauthorized"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<MessageStatisticsDTO> getMessageStatistics(
            @Parameter(description = "Tenant ID (required)")
            @RequestParam(required = true) String tenantId,

            @Parameter(description = "Start timestamp (epoch millis, optional)")
            @RequestParam(required = false) Long from,

            @Parameter(description = "End timestamp (epoch millis, optional)")
            @RequestParam(required = false) Long to,

            @Parameter(description = "Include detailed breakdowns (default: true)")
            @RequestParam(required = false, defaultValue = "true") Boolean includeBreakdowns) {

        long startTime = System.currentTimeMillis();

        try {
            MessageStatisticsRequestDTO request = MessageStatisticsRequestDTO.builder()
                .tenantId(tenantId)
                .from(from)
                .to(to)
                .includeBreakdowns(includeBreakdowns)
                .build();

            MessageStatisticsDTO statistics = statisticsService.generateStatistics(request);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("✅ [STATISTICS] Generated for tenant {} | Messages: {} | Duration: {}ms",
                tenantId, statistics.getTotals().getMessageCount(), duration);

            return ResponseEntity.ok(statistics);

        } catch (IllegalArgumentException e) {
            logger.warn("⚠️ [STATISTICS] Invalid request: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("❌ [STATISTICS] Failed for tenant {}: {}", tenantId, e.getMessage(), e);
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

    // ============================================================================
    // ADMIN ENDPOINTS FOR SOFT-DELETED MESSAGES
    // ============================================================================

    @GetMapping("/deleted")
    @Audit(action = "AI_MESSAGES_DELETED_LIST", description = "Admin viewed deleted messages")
    @Operation(summary = "List deleted messages (Admin)", 
               description = "Retrieves all soft-deleted messages with pagination. Messages are retained for 30 days before permanent deletion.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Deleted messages retrieved successfully"),
        @ApiResponse(responseCode = "401", description = "Unauthorized - Admin access required")
    })
    public ResponseEntity<Page<MessageResponseDTO>> getDeletedMessages(
            @Parameter(description = "Tenant ID (optional filter)") @RequestParam(required = false) String tenantId,
            @Parameter(description = "Page number (0-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size) {

        logger.info("🔍 [ADMIN] Request to view deleted messages - tenantId: {}, page: {}, size: {}", 
                   tenantId, page, size);

        try {
            Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "deletedAt"));
            
            Page<Message> deletedMessages;
            if (tenantId != null && !tenantId.isBlank()) {
                deletedMessages = messageService.findDeletedMessagesByTenantId(tenantId, pageable);
            } else {
                deletedMessages = messageService.findDeletedMessages(pageable);
            }

            // Convert to response DTOs
            Page<MessageResponseDTO> responsePage = deletedMessages.map(this::convertToResponseDTO);

            logger.info("✅ [ADMIN] Retrieved {} deleted messages (page {}/{})",
                       deletedMessages.getTotalElements(), page + 1, deletedMessages.getTotalPages());

            return ResponseEntity.ok(responsePage);

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error retrieving deleted messages: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/deleted/{id}")
    @Audit(action = "AI_MESSAGE_DELETED_GET", description = "Admin viewed specific deleted message")
    @Operation(summary = "Get specific deleted message (Admin)", 
               description = "Retrieves a specific soft-deleted message by ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Deleted message found"),
        @ApiResponse(responseCode = "404", description = "Message not found or not deleted"),
        @ApiResponse(responseCode = "401", description = "Unauthorized - Admin access required")
    })
    public ResponseEntity<MessageResponseDTO> getDeletedMessageById(
            @Parameter(description = "Message ID") @PathVariable String id) {

        logger.info("🔍 [ADMIN] Request to view deleted message: {}", id);

        try {
            Optional<Message> message = messageService.findById(id);

            if (message.isEmpty()) {
                logger.warn("⚠️ [ADMIN] Message not found: {}", id);
                return ResponseEntity.notFound().build();
            }

            Message msg = message.get();
            if (!Boolean.TRUE.equals(msg.getIsDeleted())) {
                logger.warn("⚠️ [ADMIN] Message {} exists but is not deleted", id);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }

            MessageResponseDTO responseDto = convertToResponseDTO(msg);
            logger.info("✅ [ADMIN] Retrieved deleted message: {} | Deleted by: {} | Reason: {}",
                       id, msg.getDeletedBy(), msg.getDeletionReason());

            return ResponseEntity.ok(responseDto);

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error retrieving deleted message {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/{id}/restore")
    @Audit(action = "AI_MESSAGE_RESTORE", description = "Admin restored deleted message")
    @Operation(summary = "Restore deleted message (Admin)", 
               description = "Restores a soft-deleted message, making it active again")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Message restored successfully"),
        @ApiResponse(responseCode = "404", description = "Message not found"),
        @ApiResponse(responseCode = "400", description = "Message is not deleted"),
        @ApiResponse(responseCode = "401", description = "Unauthorized - Admin access required")
    })
    public ResponseEntity<APIResponse<MessageResponseDTO>> restoreDeletedMessage(
            @Parameter(description = "Message ID") @PathVariable String id) {

        logger.info("🔄 [ADMIN] Request to restore deleted message: {}", id);

        try {
            Optional<Message> existingMessage = messageService.findById(id);

            if (existingMessage.isEmpty()) {
                logger.warn("⚠️ [ADMIN] Cannot restore - message not found: {}", id);
                return ResponseEntity.status(404)
                        .body(APIResponse.<MessageResponseDTO>builder()
                                .success(false)
                                .message("Message not found")
                                .build());
            }

            Message message = existingMessage.get();
            if (!Boolean.TRUE.equals(message.getIsDeleted())) {
                logger.warn("⚠️ [ADMIN] Cannot restore - message {} is not deleted", id);
                return ResponseEntity.status(400)
                        .body(APIResponse.<MessageResponseDTO>builder()
                                .success(false)
                                .message("Message is not deleted")
                                .build());
            }

            // Restore the message
            messageService.restoreDeletedMessage(id);
            
            // Fetch restored message
            Message restoredMessage = messageService.findById(id).orElseThrow();
            MessageResponseDTO responseDto = convertToResponseDTO(restoredMessage);

            logger.info("✅ [ADMIN] Successfully restored message: {} | Was deleted by: {} | Reason: {}",
                       id, message.getDeletedBy(), message.getDeletionReason());

            return ResponseEntity.ok(APIResponse.<MessageResponseDTO>builder()
                    .success(true)
                    .message("Message restored successfully")
                    .data(responseDto)
                    .build());

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error restoring message {}: {}", id, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(APIResponse.<MessageResponseDTO>builder()
                            .success(false)
                            .message("Error restoring message: " + e.getMessage())
                            .build());
        }
    }

    @GetMapping("/deleted/stats")
    @Audit(action = "AI_MESSAGES_DELETED_STATS", description = "Admin viewed deletion statistics")
    @Operation(summary = "Get deletion statistics (Admin)", 
               description = "Returns statistics about deleted vs active messages")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully"),
        @ApiResponse(responseCode = "401", description = "Unauthorized - Admin access required")
    })
    public ResponseEntity<Map<String, Object>> getDeletionStatistics() {
        logger.info("📊 [ADMIN] Request to view deletion statistics");

        try {
            Map<String, Object> stats = messageService.getDeletionStatistics();

            logger.info("✅ [ADMIN] Retrieved deletion statistics: {} total, {} active, {} deleted",
                       stats.get("totalMessages"), stats.get("activeMessages"), stats.get("deletedMessages"));

            return ResponseEntity.ok(stats);

        } catch (Exception e) {
            logger.error("❌ [ADMIN] Error retrieving deletion statistics: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
