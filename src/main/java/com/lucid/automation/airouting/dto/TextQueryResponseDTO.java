package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Response DTO for text query operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Response payload for text query processing")
public class TextQueryResponseDTO {
    
    @Schema(description = "The AI-generated response to the query", example = "The weather today is sunny with a temperature of 25°C.")
    private String response;
    
    @Schema(description = "The original query that was processed", example = "What is the weather like today?")
    private String originalQuery;
    
    @Schema(description = "AI provider that processed the query", example = "geminiProvider")
    private String providerId;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    @Schema(description = "Timestamp when the response was generated")
    private LocalDateTime timestamp;
}
