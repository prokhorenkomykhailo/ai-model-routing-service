package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.Channel;
import com.lucid.automation.airouting.repository.ChannelRepository;
import com.lucid.automation.common.dto.messaging.IngestionEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service for managing channels in Redis
 *
 * @author vudu
 */
@Service
public class ChannelService {

    private static final Logger logger = LoggerFactory.getLogger(ChannelService.class);

    private final ChannelRepository channelRepository;

    public ChannelService(ChannelRepository channelRepository) {
        this.channelRepository = channelRepository;
    }

    /**
     * Save or update channel information
     *
     * @param channel The channel to save
     * @return Saved channel
     */
    public Channel saveChannel(Channel channel) {
        if (channel == null) {
            logger.warn("⚠️ Cannot save null channel");
            return null;
        }

        try {
            channel.updateTimestamps();
            Channel savedChannel = channelRepository.save(channel);
            return savedChannel;
        } catch (Exception e) {
            logger.error("❌ Failed to save channel {}: {}",
                channel.getChannelId(), e.getMessage());
            return null;
        }
    }

    /**
     * Find channel by channel ID
     *
     * @param channelId The channel ID
     * @return Optional containing channel if found
     */
    public Optional<Channel> findByChannelId(String channelId) {
        if (channelId == null || channelId.trim().isEmpty()) {
            logger.warn("⚠️ Cannot find channel with null or empty ID");
            return Optional.empty();
        }

        try {
            return channelRepository.findByChannelId(channelId.trim());
        } catch (Exception e) {
            logger.error("❌ Failed to find channel {}: {}", channelId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Find all channels
     *
     * @return List of all channels
     */
    public List<Channel> findAll() {
        try {
            List<Channel> channels = (List<Channel>) channelRepository.findAll();
            logger.debug("Found {} channels", channels.size());
            return channels;
        } catch (Exception e) {
            logger.error("❌ Failed to find all channels: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Find all channels by a list of IDs
     *
     * @param channelIds List of channel IDs
     * @return List of found channels
     */
    public List<Channel> findAllById(Iterable<String> channelIds) {
        if (channelIds == null) {
            return List.of();
        }
        try {
            List<Channel> channels = new java.util.ArrayList<>();
            channelRepository.findAllById(channelIds).forEach(channels::add);
            return channels;
        } catch (Exception e) {
            logger.error("❌ Failed to batch find channels: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Find channels by name (case-insensitive)
     *
     * @param channelName The channel name
     * @return List of channels with matching name
     */
    public List<Channel> findByChannelNameIgnoreCase(String channelName) {
        if (channelName == null || channelName.trim().isEmpty()) {
            logger.warn("⚠️ Cannot find channels with null or empty name");
            return List.of();
        }

        try {
            return channelRepository.findByChannelNameIgnoreCase(channelName.trim());
        } catch (Exception e) {
            logger.error("❌ Failed to find channels by name {}: {}", channelName, e.getMessage());
            return List.of();
        }
    }

    /**
     * Create or update channel from ingestion event
     *
     * @param ingestionEventDto The ingestion event containing channel info
     * @return Created or updated channel
     */
    public Channel createOrUpdateChannelFromEvent(IngestionEventDTO ingestionEventDto) {
        if (isInvalidIngestionEvent(ingestionEventDto)) {
            // Provide detailed diagnostic information about WHY the event is invalid
            String channelId = (ingestionEventDto != null && ingestionEventDto.getMessage() != null)
                ? ingestionEventDto.getMessage().getChannelId() : "null";
            String messageTs = (ingestionEventDto != null && ingestionEventDto.getMessage() != null)
                ? ingestionEventDto.getMessage().getTs() : "null";
            String tenantId = (ingestionEventDto != null) ? ingestionEventDto.getTenantId() : "null";

            logger.warn("⚠️ [CHANNEL-INVALID] Cannot create channel from invalid ingestion event | " +
                    "Reason: missing or empty channelId | channelId={} | messageTs={} | tenantId={}",
                    channelId, messageTs, tenantId);
            return null;
        }

        try {
            var message = ingestionEventDto.getMessage();
            String channelId = resolveChannelId(ingestionEventDto);

            // Check if channel already exists
            Optional<Channel> existingChannel = findByChannelId(channelId);

            Channel channel;
            if (existingChannel.isPresent()) {
                channel = existingChannel.get();
                updateChannelFromEvent(channel, ingestionEventDto);
            } else {
                channel = createChannelFromEvent(ingestionEventDto);
            }

            return saveChannel(channel);

        } catch (Exception e) {
            logger.error("❌ Failed to create/update channel from event: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Resolve channelId with Gmail fallback logic.
     * For Gmail messages without channelId, use clientMsgId as fallback.
     *
     * @param dto The ingestion event DTO
     * @return Resolved channelId, or null if unavailable
     * @author vudu
     */
    private String resolveChannelId(IngestionEventDTO dto) {
        if (dto == null || dto.getMessage() == null) {
            return null;
        }

        var message = dto.getMessage();
        String channelId = message.getChannelId();

        // If channelId exists and is not empty, use it
        if (channelId != null && !channelId.trim().isEmpty()) {
            return channelId.trim();
        }

        // Gmail fallback: use clientMsgId if channelId is missing
        String sourceType = dto.getSourceType();
        String messageSource = message.getSource();

        if (isGmailMessage(sourceType, messageSource)) {
            String clientMsgId = message.getClientMsgId();
            if (clientMsgId != null && !clientMsgId.trim().isEmpty()) {
                logger.info("📧 [GMAIL-FALLBACK] Using clientMsgId as channelId for Gmail message | " +
                        "clientMsgId={} | sourceType={} | messageSource={}",
                        clientMsgId, sourceType, messageSource);
                return clientMsgId.trim();
            }
        }

        return null;
    }

    /**
     * Check if the message is from Gmail platform.
     *
     * @param sourceType Event-level source type
     * @param messageSource Message-level source
     * @return true if Gmail message
     */
    private boolean isGmailMessage(String sourceType, String messageSource) {
        return ("GMAIL".equalsIgnoreCase(sourceType) || "Gmail".equalsIgnoreCase(messageSource));
    }

    private boolean isInvalidIngestionEvent(IngestionEventDTO dto) {
        if (dto == null || dto.getMessage() == null) {
            return true;
        }

        // Use resolveChannelId to apply Gmail fallback logic
        String resolvedChannelId = resolveChannelId(dto);
        return resolvedChannelId == null || resolvedChannelId.trim().isEmpty();
    }

    private Channel createChannelFromEvent(IngestionEventDTO dto) {
        var message = dto.getMessage();
        String channelId = resolveChannelId(dto);

        // Determine channel source based on sourceType or message.source
        String channelSrc = "slack"; // Default
        if (isGmailMessage(dto.getSourceType(), message.getSource())) {
            channelSrc = "gmail";
        }

        return Channel.builder()
            .channelId(channelId)
            .channelSrc(channelSrc)
            .channelName(message.getChannelName())
            .tenantId(dto.getTenantId())
            .workspaceId(message.getTeamId() != null ? message.getTeamId() : message.getWorkspaceId())
            .topic(message.getTopic())
            .purpose(message.getPurpose())
            .isPrivate(false) // Default value, can be enhanced later
            .channelType("channel") // Default type
            .build();
    }

    private void updateChannelFromEvent(Channel channel, IngestionEventDTO dto) {
        var message = dto.getMessage();

        // Update fields that might have changed
        if (message.getChannelName() != null && !message.getChannelName().trim().isEmpty()) {
            channel.setChannelName(message.getChannelName().trim());
        }

        if (message.getTopic() != null && !message.getTopic().trim().isEmpty()) {
            channel.setTopic(message.getTopic().trim());
        }

        if (message.getPurpose() != null && !message.getPurpose().trim().isEmpty()) {
            channel.setPurpose(message.getPurpose().trim());
        }
    }

    /**
     * Delete channel by ID
     *
     * @param channelId The channel ID to delete
     */
    public void deleteById(String channelId) {
        if (channelId == null || channelId.trim().isEmpty()) {
            logger.warn("⚠️ Cannot delete channel with null or empty ID");
            return;
        }

        try {
            channelRepository.deleteById(channelId.trim());
            logger.info("✅ Channel deleted: {}", channelId);
        } catch (Exception e) {
            logger.error("❌ Failed to delete channel {}: {}", channelId, e.getMessage());
            throw e;
        }
    }
}
