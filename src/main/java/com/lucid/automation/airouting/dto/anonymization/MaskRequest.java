package com.lucid.automation.airouting.dto.anonymization;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Request DTO for PII masking via anonymization service.
 * Matches Python contract from lucid-anonimized-service/app/api/masker.py
 *
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MaskRequest {

    /**
     * Text containing PII to mask
     */
    @JsonProperty("text")
    private String text;

    /**
     * Tenant ID for token vault isolation (optional for service-to-service calls)
     */
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Specific entity types to detect (optional)
     * Examples: EMAIL, PERSON, PHONE_NUMBER, etc.
     */
    @JsonProperty("entity_types")
    private List<String> entityTypes;

    /**
     * Minimum confidence score for entity detection (0.0 to 1.0)
     * Default: 0.5
     */
    @JsonProperty("confidence_threshold")
    @Builder.Default
    private Double confidenceThreshold = 0.5;

    /**
     * Additional context metadata (optional)
     */
    @JsonProperty("context_metadata")
    private Map<String, Object> contextMetadata;
}
