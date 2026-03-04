package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionRequest;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionResponse;
import com.lucid.automation.airouting.service.TopicActionSuggestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
@Tag(name = "Topic Action Suggestion", description = "Generate AI suggestion text for reply/forward actions")
public class TopicActionSuggestionController {

    private static final Logger logger = LoggerFactory.getLogger(TopicActionSuggestionController.class);

    private final TopicActionSuggestionService suggestionService;

    public TopicActionSuggestionController(TopicActionSuggestionService suggestionService) {
        this.suggestionService = suggestionService;
    }

    @PostMapping("/topic-action-suggestion")
    @Operation(
        summary = "Generate suggested text for Reply/Forward",
        description = "Returns AI-generated text suggestion based on topic context and user action."
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Suggestion generated"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "500", description = "Failed to generate suggestion")
    })
    public ResponseEntity<?> suggest(@RequestBody TopicActionSuggestionRequest request) {
        try {
            TopicActionSuggestionResponse response = suggestionService.suggest(request);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to generate topic action suggestion: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to generate suggestion");
        }
    }
}

