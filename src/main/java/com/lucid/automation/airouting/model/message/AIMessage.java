package com.lucid.automation.airouting.model.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Base message model for RabbitMQ AI processing requests
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AIMessage {

    @NotNull
    @JsonProperty("messageId")
    private String messageId;

    @NotNull
    @JsonProperty("taskType")
    private AITaskType taskType;

    @NotBlank
    @JsonProperty("content")
    private String content;

    @JsonProperty("tenantId")
    private String tenantId;

    @JsonProperty("tenantSchema")
    private String tenantSchema;

    @JsonProperty("conversationId")
    private String conversationId;

    @JsonProperty("messages")
    private List<SlackMessage> messages;

    @JsonProperty("participants")
    private List<SlackParticipantData> participants;

    @JsonProperty("context")
    private Map<String, Object> context;

    @JsonProperty("preferredProvider")
    private String preferredProvider;

    @JsonProperty("userId")
    private String userId;

    @JsonProperty("priority")
    @Builder.Default
    private MessagePriority priority = MessagePriority.NORMAL;

    @JsonProperty("requestedAt")
    private LocalDateTime requestedAt;

    @JsonProperty("replyTopic")
    private String replyTopic;

    @JsonProperty("jobId")
    private String jobId;

    public enum MessagePriority {
        LOW, NORMAL, HIGH, URGENT
    }

    // Lombok will generate constructors, getters, setters, builder, and toString
    // If you need to customize default values, use @Builder.Default
}
