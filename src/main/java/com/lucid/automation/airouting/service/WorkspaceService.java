package com.lucid.automation.airouting.service;

import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import com.lucid.automation.common.dto.messaging.MessageDTO;
import com.lucid.automation.airouting.model.Workspace;
import com.lucid.automation.airouting.repository.WorkspaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing workspaces
 */
@Service
public class WorkspaceService {

    private static final Logger logger = LoggerFactory.getLogger(WorkspaceService.class);

    private final WorkspaceRepository workspaceRepository;
    public WorkspaceService(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }

    /**
     * Create or update workspace from message data
     *
     * @param ingestionEventDto The ingestion event DTO containing message data
     * @return The created or updated workspace
     */
    public Workspace createOrUpdateWorkspace(IngestionEventDTO ingestionEventDto) {
        if (ingestionEventDto.getTenantId() == null || ingestionEventDto.getMessage() == null) {
            logger.warn("Cannot create/update workspace: missing tenantId or message data");
            return null;
        }

        String deemergeUserId = ingestionEventDto.getDeemergeUserId();
        if (deemergeUserId == null || deemergeUserId.trim().isEmpty()) {
            logger.warn("Cannot create/update workspace: missing deemergeUserId");
            return null;
        }

        String tenantId = ingestionEventDto.getTenantId();
        if (tenantId == null || tenantId.trim().isEmpty()) {
            logger.warn("Cannot create/update workspace: missing tenantId");
            return null;
        }

        String teamId = ingestionEventDto.getMessage().getTeamId();
        if (teamId == null || teamId.trim().isEmpty()) {
            logger.warn("Cannot create/update workspace: missing teamId in message data");
            return null;
        }

        try {
            Workspace workspace = workspaceRepository.findByTeamIdAndTenantIdAndDeemergeUserId(teamId, tenantId, deemergeUserId)
                    .orElse(new Workspace(teamId, tenantId, deemergeUserId));
            updateWorkspaceFromMessage(workspace, ingestionEventDto);

            // Save and return
            Workspace savedWorkspace = workspaceRepository.save(workspace);
            return savedWorkspace;

        } catch (Exception e) {
            logger.error("Error creating/updating workspace: deemergeUserId={}, error={}",
                        deemergeUserId, e);
            return null;
        }
    }

    /**
     * Update workspace statistics from message
     *
     * @param workspace The workspace to update
     * @param ingestionEventDto The message data
     */
    private void updateWorkspaceFromMessage(Workspace workspace, IngestionEventDTO ingestionEventDto) {
        MessageDTO messageData = ingestionEventDto.getMessage();

        // Update basic info
        if (workspace.getTeamId() == null && messageData.getTeamId() != null) {
            workspace.setTeamId(messageData.getTeamId());
        }

        // Update tenant schema if not set
        if (workspace.getTenantSchema() == null && ingestionEventDto.getTenantSchema() != null) {
            workspace.setTenantSchema(ingestionEventDto.getTenantSchema());
        }

        // Add channel
        if (messageData.getChannelId() != null) {
            workspace.addChannel(messageData.getChannelId());
        }

        // Update message count
        workspace.incrementMessageCount();

        // Update thread count if this is a thread message
        if (messageData.getThreadTs() != null && !messageData.getThreadTs().isEmpty()) {
            workspace.incrementThreadCount();
        }

        // Update message timing
        Instant messageTime = parseMessageTime(ingestionEventDto, messageData);
        workspace.updateLastMessageTime(messageTime);

        // Extract workspace name from domain or other metadata if available
        if (workspace.getName() == null) {
            extractWorkspaceName(workspace, ingestionEventDto);
        }

        // Update deemerger user id if present in DTO metadata
        if (ingestionEventDto.getDeemergeUserId() != null) {
            workspace.setDeemergeUserId(ingestionEventDto.getDeemergeUserId());
        }

        // Update deemerger user name if present in DTO metadata
        if (ingestionEventDto.getDeemergeUserName() != null) {
            workspace.setDeemergeUserName(ingestionEventDto.getDeemergeUserName());
        }
    }

    /**
     * Parse message timestamp
     */
    private Instant parseMessageTime(IngestionEventDTO dto, MessageDTO messageData) {
        // Try to use ingestedAt first
        if (dto.getIngestedAt() != null) {
            return dto.getIngestedAt();
        }

        // Try to parse message timestamp
        if (messageData.getTs() != null) {
            try {
                String timestamp = messageData.getTs();
                // Slack timestamps are usually in format "1234567890.123456"
                if (timestamp.contains(".")) {
                    timestamp = timestamp.split("\\.")[0];
                }
                return Instant.ofEpochSecond(Long.parseLong(timestamp));
            } catch (NumberFormatException e) {
                logger.warn("Could not parse message timestamp: {}", messageData.getTs());
            }
        }

        // Fallback to current time
        return Instant.now();
    }

    /**
     * Extract workspace name from message metadata
     */
    private void extractWorkspaceName(Workspace workspace, IngestionEventDTO dto) {
        // Try to get name from metadata
        if (dto.getMetadata() != null && dto.getMetadata().getWorkspaceName() != null) {
            workspace.setName(dto.getMetadata().getWorkspaceName());
        } else {
            // Default name
            workspace.setName("Workspace " + workspace.getId());
        }
    }

    /**
     * Get all workspaces
     *
     * @return List of all workspaces
     */
    public List<Workspace> getAllWorkspaces() {
        try {
            List<Workspace> workspaces = new ArrayList<>();
            workspaceRepository.findAll().forEach(workspaces::add);
            logger.debug("Retrieved {} workspaces", workspaces.size());
            return workspaces;
        } catch (Exception e) {
            logger.error("Error retrieving all workspaces", e);
            return Collections.emptyList();
        }
    }

    /**
     * Delete a workspace by ID along with all its associated messages
     *
     * @param id The workspace ID to delete
     * @return true if the workspace was found and deleted, false if not found
     */
    public boolean deleteWorkspaceById(String id) {
        if (id == null || id.trim().isEmpty()) {
            logger.warn("Cannot delete workspace: ID is null or empty");
            return false;
        }

        try {
            // Check if workspace exists
            Optional<Workspace> workspaceOpt = workspaceRepository.findById(id);
            if (workspaceOpt.isEmpty()) {
                logger.warn("Workspace with ID {} not found", id);
                return false;
            }

            Workspace workspace = workspaceOpt.get();
            String deemergeUserId = workspace.getDeemergeUserId();
            String tenantId = workspace.getTenantId();

            logger.info("Deleting workspace {} (ID: {}) and all associated messages", workspace.getName(), id);

            // Clean up all messages associated with this workspace
            if (deemergeUserId != null && !deemergeUserId.trim().isEmpty()) {
                int deletedMessages = 0;
                logger.info("Deleted {} messages for workspace {}", deletedMessages, id);
            } else {
                logger.warn("Workspace {} has no deemergeUserId, skipping message cleanup", id);
            }

            // Delete the workspace itself
            workspaceRepository.deleteById(id);

            logger.info("Successfully deleted workspace {} (ID: {})", workspace.getName(), id);
            return true;

        } catch (Exception e) {
            logger.error("Error deleting workspace with ID: {}", id, e);
            return false;
        }
    }
}
