package com.lucid.automation.airouting.client;

import com.lucid.automation.airouting.dto.anonymization.MaskRequest;
import com.lucid.automation.airouting.dto.anonymization.MaskResponse;
import com.lucid.automation.airouting.dto.anonymization.UnmaskRequest;
import com.lucid.automation.airouting.dto.anonymization.UnmaskResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Feign client for Anonymization Service.
 * Provides PII masking and unmasking capabilities for secure LLM processing.
 *
 * Service contract: lucid-anonimized-service (Python FastAPI, port 8089)
 * - Masking endpoint: POST /api/v1/mask/text
 * - Unmasking endpoint: POST /api/v1/unmask/text
 *
 * @author vudu
 */
@FeignClient(
    name = "anonymization-service",
    url = "${anonymization.service.url:http://anonymization-service:8089}",
    configuration = FeignClientConfig.class
)
public interface AnonymizationClient {

    /**
     * Mask PII in text by replacing with secure tokens.
     * This MUST be called before sending any content to LLM to ensure compliance.
     *
     * Example:
     * Input:  "Contact john@example.com at +1-555-1234"
     * Output: "Contact {{email_123}} at {{phone_456}}"
     *
     * @param request masking request with text and optional parameters
     * @return masked text with PII replaced by deterministic placeholders
     */
    @PostMapping("/api/v1/mask/text")
    MaskResponse maskText(@RequestBody MaskRequest request);

    /**
     * Unmask tokens in text by replacing with original PII values.
     * This should be called after LLM response to restore original context.
     *
     * Example:
     * Input:  "Topic: {{user_123}} discussed {{organization_45}}"
     * Output: "Topic: John Smith discussed Acme Corp"
     *
     * @param request unmasking request with masked text and justification
     * @return unmasked text with original PII values restored
     */
    @PostMapping("/api/v1/unmask/text")
    UnmaskResponse unmaskText(@RequestBody UnmaskRequest request);
}
