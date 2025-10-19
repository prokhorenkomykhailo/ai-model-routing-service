package com.lucid.automation.airouting.dto.anonymization;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Response DTO for PII masking from anonymization service.
 * Matches Python contract from lucid-anonimized-service/app/api/masker.py
 *
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MaskResponse {

    /**
     * Text with PII replaced by tokens (e.g., "Contact {{email_123}} at {{phone_456}}")
     */
    @JsonProperty("masked_text")
    private String maskedText;

    /**
     * List of detected PII entities with details
     */
    @JsonProperty("detected_entities")
    private List<Map<String, Object>> detectedEntities;

    /**
     * Number of tokens created
     */
    @JsonProperty("token_count")
    private Integer tokenCount;

    /**
     * Number of distinct entities found
     */
    @JsonProperty("entities_found")
    private Integer entitiesFound;

    /**
     * Unique request identifier for tracking
     */
    @JsonProperty("request_id")
    private String requestId;
}
