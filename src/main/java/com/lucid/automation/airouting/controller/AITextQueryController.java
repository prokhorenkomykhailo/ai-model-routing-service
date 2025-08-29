package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.audit.Audit;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.service.AIProviderRouterService;
import com.lucid.automation.common.dto.response.APIResponse;
import com.lucid.automation.common.dto.ai.TextQueryRequestDTO;
import com.lucid.automation.common.dto.ai.TextQueryResponseDTO;
import com.lucid.automation.airouting.provider.AIProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.time.LocalDateTime;

/**
 * Internal controller for AI text query operations with enhanced routing and failover
 * This controller is intended for internal service-to-service communication
 */
@RestController
@RequestMapping("/api/internal/ai")
@CrossOrigin(origins = "*")
@Validated
@Tag(name = "AI Text Query (Internal)", description = "Internal API for AI text query processing")
public class AITextQueryController {

    private static final Logger logger = LoggerFactory.getLogger(AITextQueryController.class);

    private final AIProviderRouterService providerRouter;

    public AITextQueryController(AIProviderRouterService providerRouter) {
        this.providerRouter = providerRouter;
        logger.info("AI-CONTROLLER: Initialized with enhanced provider routing");
    }

    @PostMapping(value = "/text-query")
    @Audit(action = "AI_TEXT_QUERY", description = "AI processed text query")
    @Operation(
        summary = "Process text query with AI",
        description = "Submit a text query to be processed by the AI provider with automatic failover support"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Query processed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request payload"),
        @ApiResponse(responseCode = "503", description = "AI service unavailable"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<APIResponse<TextQueryResponseDTO>> processTextQuery(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader(value = "X-Tenant-Schema", required = false) String tenantSchema,
            @RequestHeader(value = "X-Preferred-Provider", required = false) String preferredProvider,
            @Parameter(description = "Text query request payload", required = true)
            @Valid @RequestBody TextQueryRequestDTO request) {

        String debugId = "AI-CONTROLLER-" + System.currentTimeMillis();
        logger.info("AI-CONTROLLER [{}]: Received text query request from UserId: [{}], TenantId: [{}], TenantSchema: [{}], PreferredProvider: [{}]",
                   debugId, userId, tenantId, tenantSchema, preferredProvider);

        try {
            // Select the best available provider using the router
            AIProvider selectedProvider = providerRouter.selectProvider(AITaskType.TEXT_QUERY, tenantId, preferredProvider);

            logger.info("AI-CONTROLLER [{}]: Selected provider: {} for tenant: {}",
                       debugId, selectedProvider.getProviderId(), tenantId);

            // Process the query using the selected provider
            String aiResponse = selectedProvider.processTextQuery(request.getQuery(), userId, tenantId);

            // Build response DTO
            TextQueryResponseDTO response = TextQueryResponseDTO.builder()
                    .response(aiResponse)
                    .originalQuery(request.getQuery())
                    .providerId(selectedProvider.getProviderId())
                    .timestamp(LocalDateTime.now())
                    .build();

            logger.info("AI-CONTROLLER [{}]: Successfully processed text query with provider: {}",
                       debugId, selectedProvider.getProviderId());

            return ResponseEntity.ok(
                APIResponse.success("Query processed successfully", response)
            );

        } catch (AIProviderRouterService.NoAvailableProviderException e) {
            logger.error("AI-CONTROLLER [{}]: No available providers: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(APIResponse.error("AI service is currently unavailable: " + e.getMessage()));

        } catch (AIProviderRouterService.InsufficientTokensException e) {
            logger.error("AI-CONTROLLER [{}]: Insufficient tokens: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.PAYMENT_REQUIRED)
                .body(APIResponse.error("Insufficient tokens: " + e.getMessage()));

        } catch (IllegalArgumentException e) {
            logger.error("AI-CONTROLLER [{}]: Invalid request: {}", debugId, e.getMessage());
            return ResponseEntity.badRequest()
                .body(APIResponse.error("Invalid request: " + e.getMessage()));

        } catch (RuntimeException e) {
            logger.error("AI-CONTROLLER [{}]: Service error: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(APIResponse.error("Service error: " + e.getMessage()));

        } catch (Exception e) {
            logger.error("AI-CONTROLLER [{}]: Unexpected error: {}", debugId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(APIResponse.error("Unexpected error occurred"));
        }
    }

    /**
     * Enhanced endpoint with token estimation and validation
     */
    @PostMapping(value = "/text-query-validated")
    @Audit(action = "AI_TEXT_QUERY_VALIDATED", description = "AI processed text query with token validation")
    @Operation(
        summary = "Process text query with token validation",
        description = "Submit a text query with pre-validation of token availability for estimated usage"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Query processed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request payload"),
        @ApiResponse(responseCode = "402", description = "Insufficient tokens for operation"),
        @ApiResponse(responseCode = "503", description = "AI service unavailable"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<APIResponse<TextQueryResponseDTO>> processTextQueryWithValidation(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestHeader(value = "X-Tenant-Schema", required = false) String tenantSchema,
            @RequestHeader(value = "X-Preferred-Provider", required = false) String preferredProvider,
            @RequestHeader(value = "X-Estimated-Tokens", defaultValue = "1000") String estimatedTokensHeader,
            @Parameter(description = "Text query request payload", required = true)
            @Valid @RequestBody TextQueryRequestDTO request) {

        String debugId = "AI-CONTROLLER-VALIDATED-" + System.currentTimeMillis();
        long estimatedTokens = Long.parseLong(estimatedTokensHeader);

        logger.info("AI-CONTROLLER [{}]: Received validated text query request from UserId: [{}], TenantId: [{}], EstimatedTokens: [{}]",
                   debugId, userId, tenantId, estimatedTokens);

        try {
            // Use the enhanced provider selection with token validation
            AIProvider selectedProvider = providerRouter.selectProviderWithTokenValidation(
                AITaskType.TEXT_QUERY, tenantId, estimatedTokens, preferredProvider);

            logger.info("AI-CONTROLLER [{}]: Selected provider: {} for tenant: {} with token validation",
                       debugId, selectedProvider.getProviderId(), tenantId);

            // Process the query using the selected provider
            String aiResponse = selectedProvider.processTextQuery(request.getQuery(), userId, tenantId);

            // Build response DTO
            TextQueryResponseDTO response = TextQueryResponseDTO.builder()
                    .response(aiResponse)
                    .originalQuery(request.getQuery())
                    .providerId(selectedProvider.getProviderId())
                    .timestamp(LocalDateTime.now())
                    .build();

            logger.info("AI-CONTROLLER [{}]: Successfully processed validated text query with provider: {}",
                       debugId, selectedProvider.getProviderId());

            return ResponseEntity.ok(
                APIResponse.success("Query processed successfully with token validation", response)
            );

        } catch (AIProviderRouterService.InsufficientTokensException e) {
            logger.error("AI-CONTROLLER [{}]: Insufficient tokens for operation: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.PAYMENT_REQUIRED)
                .body(APIResponse.error("Insufficient tokens for operation: " + e.getMessage()));

        } catch (AIProviderRouterService.NoAvailableProviderException e) {
            logger.error("AI-CONTROLLER [{}]: No available providers: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(APIResponse.error("AI service is currently unavailable: " + e.getMessage()));

        } catch (NumberFormatException e) {
            logger.error("AI-CONTROLLER [{}]: Invalid estimated tokens header: {}", debugId, estimatedTokensHeader);
            return ResponseEntity.badRequest()
                .body(APIResponse.error("Invalid estimated tokens value: " + estimatedTokensHeader));

        } catch (IllegalArgumentException e) {
            logger.error("AI-CONTROLLER [{}]: Invalid request: {}", debugId, e.getMessage());
            return ResponseEntity.badRequest()
                .body(APIResponse.error("Invalid request: " + e.getMessage()));

        } catch (RuntimeException e) {
            logger.error("AI-CONTROLLER [{}]: Service error: {}", debugId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(APIResponse.error("Service error: " + e.getMessage()));

        } catch (Exception e) {
            logger.error("AI-CONTROLLER [{}]: Unexpected error: {}", debugId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(APIResponse.error("Unexpected error occurred"));
        }
    }

    /**
     * Health endpoint to check provider status
     */
    @GetMapping(value = "/providers/status")
    @Operation(
        summary = "Get AI providers status",
        description = "Get the current status of all AI providers"
    )
    public ResponseEntity<APIResponse<AIProviderRouterService.ProviderStats>> getProviderStatus() {
        try {
            AIProviderRouterService.ProviderStats stats = providerRouter.getProviderStats();
            return ResponseEntity.ok(APIResponse.success("Provider status retrieved", stats));
        } catch (Exception e) {
            logger.error("Error retrieving provider status: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(APIResponse.error("Failed to retrieve provider status"));
        }
    }
}
