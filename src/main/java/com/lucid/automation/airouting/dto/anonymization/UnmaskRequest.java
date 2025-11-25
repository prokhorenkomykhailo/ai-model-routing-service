package com.lucid.automation.airouting.dto.anonymization;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Request DTO for text unmasking (re-enrichment) via anonymization service.
 * Matches Python contract from lucid-anonimized-service/app/api/unmasker.py
 *
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UnmaskRequest {

    /**
     * Text containing tokens to unmask (e.g., "Topic: {{user_123}} discussed {{organization_45}}")
     */
    @JsonProperty("masked_text")
    private String maskedText;

    /**
     * Tenant ID for token vault access (optional for service-to-service calls)
     */
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Reason for unmasking (required for audit trail)
     * Example: "AI topic re-enrichment for UI display"
     */
    @JsonProperty("justification")
    private String justification;

    /**
     * Additional context metadata (optional)
     */
    @JsonProperty("context_metadata")
    private Map<String, Object> contextMetadata;
}
