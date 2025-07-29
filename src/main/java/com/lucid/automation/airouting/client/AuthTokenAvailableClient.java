package com.lucid.automation.airouting.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.http.ResponseEntity;

/**
 * Feign client for calling token-available endpoint from auth-service
 */
@FeignClient(name = "auth-service", configuration = FeignClientConfig.class)
public interface AuthTokenAvailableClient {

    @GetMapping("/internal/tenants/{tenantId}/token-available")
    ResponseEntity<Boolean> getTenantTokenAvailable(
            @PathVariable("tenantId") String tenantId,
            @RequestHeader(value = "accept", defaultValue = "application/json") String acceptHeader
    );
}
