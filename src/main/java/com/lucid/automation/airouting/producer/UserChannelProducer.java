package com.lucid.automation.airouting.producer;

import com.lucid.automation.common.dto.event.UserChannelEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Producer service for publishing user-channel relationship events to Kafka
 * 
 * @author AI Assistant
 */
@Service
public class UserChannelProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(UserChannelProducer.class);
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topics.user-channel:user-channel-topic}")
    private String userChannelTopic;
    
    public UserChannelProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Publishes a user-channel relationship event to Kafka
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema
     * @param userId The user ID who triggered the event
     * @param channelId The channel ID
     * @param channelName The channel name
     * @param slackUsers List of Slack users in the channel
     * @param eventType The event type (e.g., "MESSAGE_RELATED_USERS")
     * @return The message ID
     */
    public String publishUserChannelEvent(String tenantId, String tenantSchema, String userId, 
                                        String channelId, String channelName, 
                                        List<UserChannelEventDTO.SlackUserInfo> slackUsers,
                                        String eventType) {
        
        String messageId = UUID.randomUUID().toString();
        
        try {
            UserChannelEventDTO event = UserChannelEventDTO.builder()
                .tenantId(tenantId)
                .tenantSchema(tenantSchema)
                .userId(userId)
                .channelId(channelId)
                .channelName(channelName)
                .slackUsers(slackUsers)
                .timestamp(LocalDateTime.now())
                .eventType(eventType)
                .build();
            
            logger.debug("Publishing user-channel event to topic '{}' with messageId: {}, " +
                        "channelId: {}, usersCount: {}, eventType: {}", 
                        userChannelTopic, messageId, channelId, 
                        slackUsers != null ? slackUsers.size() : 0, eventType);
            
            kafkaTemplate.send(userChannelTopic, messageId, event)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to publish user-channel event with messageId: {} to topic: {}", 
                                   messageId, userChannelTopic, throwable);
                    } else {
                        logger.info("Successfully published user-channel event with messageId: {} to topic: {}", 
                                  messageId, userChannelTopic);
                    }
                });
            
            return messageId;
            
        } catch (Exception e) {
            logger.error("Error creating user-channel event for messageId: {}", messageId, e);
            throw new RuntimeException("Failed to publish user-channel event", e);
        }
    }
    
    /**
     * Convenience method for publishing related users from a message
     * 
     * @param tenantId The tenant ID
     * @param tenantSchema The tenant schema  
     * @param userId The user ID who sent the message
     * @param channelId The channel ID
     * @param channelName The channel name
     * @param relatedUsers List of related/mentioned users
     * @return The message ID
     */
    public String publishRelatedUsersEvent(String tenantId, String tenantSchema, String userId,
                                         String channelId, String channelName, 
                                         List<UserChannelEventDTO.SlackUserInfo> relatedUsers) {
        return publishUserChannelEvent(tenantId, tenantSchema, userId, channelId, channelName, 
                                     relatedUsers, "MESSAGE_RELATED_USERS");
    }
}
