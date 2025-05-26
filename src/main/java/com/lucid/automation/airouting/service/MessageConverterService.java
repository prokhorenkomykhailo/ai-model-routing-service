package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.AIRequest;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.model.message.SlackMessageData;
import com.lucid.automation.airouting.model.message.SlackParticipantData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for converting between RabbitMQ message formats and AI request formats
 */
@Service
public class MessageConverterService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageConverterService.class);
    
    /**
     * Convert an AIMessage from RabbitMQ to an AIRequest for processing
     */
    public AIRequest convertToAIRequest(AIMessage aiMessage) {
        logger.debug("Converting AIMessage to AIRequest: messageId={}, taskType={}", 
                    aiMessage.getMessageId(), aiMessage.getTaskType());
        
        AIRequest request = new AIRequest(aiMessage.getTaskType(), aiMessage.getContent());
        
        // Set basic fields
        request.setTenantId(aiMessage.getTenantId());
        request.setConversationId(aiMessage.getConversationId());
        request.setContext(aiMessage.getContext());
        request.setPreferredProvider(aiMessage.getPreferredProvider());
        request.setUserId(aiMessage.getUserId());
        
        // Convert messages if present
        if (aiMessage.getMessages() != null && !aiMessage.getMessages().isEmpty()) {
            List<SlackMessage> slackMessages = aiMessage.getMessages().stream()
                    .map(this::convertToSlackMessage)
                    .collect(Collectors.toList());
            request.setMessages(slackMessages);
        }
        
        // Convert participants if present
        if (aiMessage.getParticipants() != null && !aiMessage.getParticipants().isEmpty()) {
            List<SlackParticipant> slackParticipants = aiMessage.getParticipants().stream()
                    .map(this::convertToSlackParticipant)
                    .collect(Collectors.toList());
            request.setParticipants(slackParticipants);
        }
        
        logger.debug("Successfully converted AIMessage to AIRequest");
        return request;
    }
    
    /**
     * Convert SlackMessageData to SlackMessage
     */
    private SlackMessage convertToSlackMessage(SlackMessageData messageData) {
        SlackMessage message = new SlackMessage();
        message.setId(messageData.getId());
        message.setUserId(messageData.getUserId());
        message.setContent(messageData.getContent());
        message.setTimestamp(messageData.getTimestamp());
        message.setChannelId(messageData.getChannelId());
        message.setThreadTs(messageData.getThreadTs());
        
        // Set default values for fields not in message data
        message.setType("message");
        message.setSubtype("");
        message.setUser(messageData.getUserId());
        message.setTs(messageData.getTimestamp() != null ? 
                      String.valueOf(messageData.getTimestamp().toString()) : "");
        message.setChannel(messageData.getChannelId());
        message.setText(messageData.getContent());
        message.setReplyCount(0);
        message.setThreadTs(messageData.getThreadTs());
        message.setReplies(Collections.emptyList());
        message.setReactions(Collections.emptyList());
        message.setFiles(Collections.emptyList());
        message.setAttachments(Collections.emptyList());
        
        return message;
    }
    
    /**
     * Convert SlackParticipantData to SlackParticipant
     */
    private SlackParticipant convertToSlackParticipant(SlackParticipantData participantData) {
        SlackParticipant participant = new SlackParticipant();
        participant.setId(participantData.getId());
        participant.setName(participantData.getName());
        participant.setEmail(participantData.getEmail());
        participant.setRole(participantData.getRole());
        
        // Set default values for fields not in participant data
        participant.setDisplayName(participantData.getName());
        participant.setRealName(participantData.getName());
        participant.setActive(true);
        participant.setBot(false);
        participant.setDeleted(false);
        participant.setTimeZone("UTC");
        participant.setTimeZoneOffset(0);
        
        return participant;
    }
    
    /**
     * Convert SlackMessage to SlackMessageData for lightweight transport
     */
    public SlackMessageData convertToMessageData(SlackMessage message) {
        SlackMessageData data = new SlackMessageData();
        data.setId(message.getId());
        data.setUserId(message.getUserId());
        data.setContent(message.getContent());
        data.setTimestamp(message.getTimestamp());
        data.setChannelId(message.getChannelId());
        data.setThreadTs(message.getThreadTs());
        return data;
    }
    
    /**
     * Convert SlackParticipant to SlackParticipantData for lightweight transport
     */
    public SlackParticipantData convertToParticipantData(SlackParticipant participant) {
        SlackParticipantData data = new SlackParticipantData();
        data.setId(participant.getId());
        data.setName(participant.getName());
        data.setEmail(participant.getEmail());
        data.setRole(participant.getRole());
        return data;
    }
}
