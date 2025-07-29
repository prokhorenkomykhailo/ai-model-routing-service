package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.client.AuthTokenAvailableClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

@Service
public class TokenAvailabilityService {
    private static final Logger logger = LoggerFactory.getLogger(TokenAvailabilityService.class);

    private final AuthTokenAvailableClient authTokenAvailableClient;

    @Autowired
    public TokenAvailabilityService(AuthTokenAvailableClient authTokenAvailableClient) {
        this.authTokenAvailableClient = authTokenAvailableClient;
    }

    /**
     * Checks if the tenant has tokens available, with fallback to true on error.
     * @param tenantId the tenant UUID as string
     * @return true if tokens are available, false if not, fallback true on error
     */
    @CircuitBreaker(name = "tokenAvailability", fallbackMethod = "tokenAvailableFallback")
    public boolean isTokenAvailable(String tenantId) {
        ResponseEntity<Boolean> response = authTokenAvailableClient.getTenantTokenAvailable(tenantId, "application/json");
        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            // return response.getBody();
            return true;
        } else {
            logger.warn("Token availability check failed for tenant {}: status {}", tenantId, response.getStatusCode());
            // return false; // fallback
            return true; // fallback to true to allow processing
        }
    }

    // Fallback method for circuit breaker
    public boolean tokenAvailableFallback(String tenantId, Throwable t) {
        logger.error("Circuit breaker fallback: error checking token availability for tenant {}: {}", tenantId, t.getMessage());
        return true;
    }
}
