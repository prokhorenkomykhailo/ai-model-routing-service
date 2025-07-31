package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.message.SlackParticipantData;
import org.springframework.stereotype.Service;

/**
 * Service for converting between RabbitMQ message formats and AI request formats
 */
@Service
public class MessageConverterService {

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
