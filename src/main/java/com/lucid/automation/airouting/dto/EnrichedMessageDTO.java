package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedMessageDTO {
    
    @NotBlank(message = "Group ID is required")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String groupId;
    
    @NotNull(message = "Topic is required")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TopicDTO topic;
    
    @NotNull(message = "Conversations are required")
    private List<ConversationDTO> conversations;
    
    @NotNull(message = "People involved are required")
    private List<String> peopleInvolved;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TopicDTO {
        
        @NotBlank(message = "Topic name is required")
        private String name;
        
        private String summary;
        
        private List<String> category;
        
        private List<String> subCategory;
        
        private List<String> keyPoints;
        
        private String priority;
        
        private List<String> keywords;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConversationDTO {
        
        @NotBlank(message = "Conversation text is required")
        private String text;
        
        private String relevance;
    }
}
