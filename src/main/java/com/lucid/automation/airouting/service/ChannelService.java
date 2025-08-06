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
 * @author AI Assistant
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
            logger.warn("⚠️ Cannot create channel from invalid ingestion event");
            return null;
        }

        try {
            var message = ingestionEventDto.getMessage();
            String channelId = message.getChannelId();

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

    private boolean isInvalidIngestionEvent(IngestionEventDTO dto) {
        return dto == null ||
               dto.getMessage() == null ||
               dto.getMessage().getChannelId() == null ||
               dto.getMessage().getChannelId().trim().isEmpty();
    }

    private Channel createChannelFromEvent(IngestionEventDTO dto) {
        var message = dto.getMessage();

        return Channel.builder()
            .channelId(message.getChannelId())
            .channelSrc("slack") // Default to slack, can be parameterized later
            .channelName(message.getChannelName())
            .tenantId(dto.getTenantId())
            .workspaceId(message.getTeamId())
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
