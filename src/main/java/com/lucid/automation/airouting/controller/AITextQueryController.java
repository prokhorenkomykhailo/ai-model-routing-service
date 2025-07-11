package com.lucid.automation.airouting.controller;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.time.LocalDateTime;

/**
 * Internal controller for AI text query operations
 * This controller is intended for internal service-to-service communication
 */
@RestController
@RequestMapping("/api/internal/ai")
@CrossOrigin(origins = "*")
@Validated
@Tag(name = "AI Text Query (Internal)", description = "Internal API for AI text query processing")
public class AITextQueryController {
    
    private static final Logger logger = LoggerFactory.getLogger(AITextQueryController.class);
    
    private final AIProvider aiProvider;
    
    @Value("${ai.routing.default-provider:geminiProvider}")
    private String defaultProviderName;
    
    public AITextQueryController(ApplicationContext applicationContext, 
                                @Value("${ai.routing.default-provider:geminiProvider}") String defaultProviderName) {
        this.defaultProviderName = defaultProviderName;
        this.aiProvider = applicationContext.getBean(defaultProviderName, AIProvider.class);
        logger.info("AI-CONTROLLER: Initialized with provider: {}", defaultProviderName);
    }
    
    @PostMapping("/text-query")
    @Operation(
        summary = "Process text query with AI",
        description = "Submit a text query to be processed by the AI provider and receive a response"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Query processed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request payload"),
        @ApiResponse(responseCode = "503", description = "AI service unavailable"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<APIResponse<TextQueryResponseDTO>> processTextQuery(
            @Parameter(description = "Text query request payload", required = true)
            @Valid @RequestBody TextQueryRequestDTO request) {
        
        String debugId = "AI-CONTROLLER-" + System.currentTimeMillis();
        logger.info("AI-CONTROLLER [{}]: Received text query request, using provider: {}", debugId, aiProvider.getProviderId());
        
        try {
            // Check if AI provider is available
            if (!aiProvider.isAvailable()) {
                logger.warn("AI-CONTROLLER [{}]: AI provider is not available", debugId);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(APIResponse.error("AI service is currently unavailable"));
            }
            
            // Process the query using the configured provider
            String aiResponse = aiProvider.processTextQuery(request.getQuery());
            
            // Build response DTO
            TextQueryResponseDTO response = TextQueryResponseDTO.builder()
                    .response(aiResponse)
                    .originalQuery(request.getQuery())
                    .providerId(aiProvider.getProviderId())
                    .timestamp(LocalDateTime.now())
                    .build();
            
            logger.info("AI-CONTROLLER [{}]: Successfully processed text query with provider: {}", 
                       debugId, aiProvider.getProviderId());
            
            return ResponseEntity.ok(
                APIResponse.success("Query processed successfully", response)
            );
            
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
}
