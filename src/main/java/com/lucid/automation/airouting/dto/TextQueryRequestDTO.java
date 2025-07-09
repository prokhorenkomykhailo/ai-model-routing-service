package com.lucid.automation.airouting.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Request DTO for text query operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Request payload for text query processing")
public class TextQueryRequestDTO {
    
    @NotBlank(message = "Query text is required")
    @Size(min = 1, max = 10000, message = "Query text must be between 1 and 10000 characters")
    @Schema(description = "The text query to process", example = "What is the weather like today?", required = true)
    private String query;
}
