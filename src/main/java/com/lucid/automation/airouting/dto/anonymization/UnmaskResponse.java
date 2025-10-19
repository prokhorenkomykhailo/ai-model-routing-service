package com.lucid.automation.airouting.dto.anonymization;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for text unmasking (re-enrichment) from anonymization service.
 * Matches Python contract from lucid-anonimized-service/app/api/unmasker.py
 *
 * @author vudu
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UnmaskResponse {

    /**
     * Text with tokens replaced by original PII values
     * Example: "Topic: John Smith discussed Acme Corp"
     */
    @JsonProperty("unmasked_text")
    private String unmaskedText;

    /**
     * Number of tokens found and processed
     */
    @JsonProperty("tokens_processed")
    private Integer tokensProcessed;

    /**
     * Number of tokens successfully unmasked
     */
    @JsonProperty("tokens_retrieved")
    private Integer tokensRetrieved;

    /**
     * Unique request identifier for tracking
     */
    @JsonProperty("request_id")
    private String requestId;
}
