package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.AIRequest;
import com.lucid.automation.airouting.model.AIResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class AIRequestAuditService {

    private static final Logger logger = LoggerFactory.getLogger(AIRequestAuditService.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AI_AUDIT");

    private final ConcurrentMap<String, RequestAudit> activeRequests = new ConcurrentHashMap<>();

    public void logRequestStart(String requestId, AIRequest request) {
        RequestAudit audit = new RequestAudit(
            requestId,
            request.getTaskType().name(),
            request.getPreferredProvider(),
            request.getTenantId(),
            OffsetDateTime.now(ZoneOffset.UTC)
        );

        activeRequests.put(requestId, audit);

        auditLogger.info("REQUEST_START: id={}, task={}, preferred_provider={}, tenant={}",
                        requestId, request.getTaskType(), request.getPreferredProvider(), request.getTenantId());

        logger.debug("Started tracking request: {}", requestId);
    }

    public void logRequestSuccess(String requestId, AIResponse response) {
        RequestAudit audit = activeRequests.remove(requestId);

        if (audit != null) {
            audit.setCompletedAt(OffsetDateTime.now(ZoneOffset.UTC));
            audit.setSuccess(true);
            audit.setProviderId(response.getProviderId());
            audit.setConfidence(response.getConfidence());
            audit.setProcessingTimeMs(response.getProcessingTimeMs());
        }

        auditLogger.info("REQUEST_SUCCESS: id={}, provider={}, confidence={}, time={}ms",
                        requestId, response.getProviderId(), response.getConfidence(),
                        response.getProcessingTimeMs());

        logger.debug("Request completed successfully: {}", requestId);
    }

    public void logRequestFailure(String requestId, AIResponse response, Exception error) {
        RequestAudit audit = activeRequests.remove(requestId);

        if (audit != null) {
            audit.setCompletedAt(OffsetDateTime.now(ZoneOffset.UTC));
            audit.setSuccess(false);
            audit.setErrorMessage(error.getMessage());
            audit.setProcessingTimeMs(response.getProcessingTimeMs());
        }

        auditLogger.error("REQUEST_FAILURE: id={}, error={}, time={}ms",
                         requestId, error.getMessage(), response.getProcessingTimeMs());

        logger.debug("Request failed: {}", requestId, error);
    }

    public RequestAudit getActiveRequest(String requestId) {
        return activeRequests.get(requestId);
    }

    public int getActiveRequestCount() {
        return activeRequests.size();
    }

    public void cleanupStaleRequests(int maxAgeMinutes) {
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(maxAgeMinutes);

        activeRequests.entrySet().removeIf(entry -> {
            RequestAudit audit = entry.getValue();
            if (audit.getStartedAt().isBefore(cutoff)) {
                auditLogger.warn("REQUEST_TIMEOUT: id={}, started={}",
                               entry.getKey(), audit.getStartedAt());
                return true;
            }
            return false;
        });
    }

    public static class RequestAudit {
        private final String requestId;
        private final String taskType;
        private final String preferredProvider;
        private final String tenantId;
        private final OffsetDateTime startedAt;

        private OffsetDateTime completedAt;
        private boolean success;
        private String providerId;
        private double confidence;
        private long processingTimeMs;
        private String errorMessage;

        public RequestAudit(String requestId, String taskType, String preferredProvider,
                          String tenantId, OffsetDateTime startedAt) {
            this.requestId = requestId;
            this.taskType = taskType;
            this.preferredProvider = preferredProvider;
            this.tenantId = tenantId;
            this.startedAt = startedAt;
        }

        // Getters and setters
        public String getRequestId() { return requestId; }
        public String getTaskType() { return taskType; }
        public String getPreferredProvider() { return preferredProvider; }
        public String getTenantId() { return tenantId; }
        public OffsetDateTime getStartedAt() { return startedAt; }

        public OffsetDateTime getCompletedAt() { return completedAt; }
        public void setCompletedAt(OffsetDateTime completedAt) { this.completedAt = completedAt; }

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getProviderId() { return providerId; }
        public void setProviderId(String providerId) { this.providerId = providerId; }

        public double getConfidence() { return confidence; }
        public void setConfidence(double confidence) { this.confidence = confidence; }

        public long getProcessingTimeMs() { return processingTimeMs; }
        public void setProcessingTimeMs(long processingTimeMs) { this.processingTimeMs = processingTimeMs; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }
}