package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicActionSuggestionProperties;
import com.lucid.automation.airouting.config.TopicEmbeddingProperties;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionRequest;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionSuggestionResponse;
import com.lucid.automation.airouting.dto.topic.suggestion.TopicActionType;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.provider.AIProvider;
import com.lucid.automation.airouting.util.PromptLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopicActionSuggestion20CaseMatrixTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void runTwentyCases_andWriteReport() throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        int passed = 0;

        for (int i = 1; i <= 20; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("caseId", i);
            row.put("name", caseName(i));
            try {
                runCase(i, row);
                row.put("status", "pass");
                passed++;
            } catch (Exception e) {
                row.put("status", "fail");
                row.put("error", e.getMessage());
            }
            rows.add(row);
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("generatedAt", Instant.now().toString());
        report.put("suite", "topic_action_suggestion_20_case_matrix");
        report.put("totalCases", rows.size());
        report.put("passedCases", passed);
        report.put("failedCases", rows.size() - passed);
        report.put("cases", rows);

        Path out = Path.of("/home/maksym/sgh_works/lucid/logs/topic_action_suggestion_20cases_report.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(report), StandardCharsets.UTF_8);

        Assertions.assertEquals(20, rows.size());
        Assertions.assertEquals(20, passed, "Some matrix cases failed. Check " + out);
    }

    private void runCase(int i, Map<String, Object> row) {
        TopicActionSuggestionProperties props = new TopicActionSuggestionProperties();
        props.setEnabled(i != 16);
        props.setPromptName("topic_action_suggestion/v1/topic_action_suggestion");
        props.setPromptVersion("v1");
        props.setDefaultTone("professional");
        props.setDefaultLanguage("English");
        if (i == 15) {
            props.setMaxResponseChars(32);
        }

        TopicEmbeddingProperties embeddingProps = new TopicEmbeddingProperties();
        embeddingProps.getPgvector().setSchema("public");
        embeddingProps.getPgvector().setTable("topics_embeddings");

        AIProviderRouterService router = mock(AIProviderRouterService.class);
        AIProvider provider = mock(AIProvider.class);
        when(provider.getProviderId()).thenReturn(i % 2 == 0 ? "openaiProvider" : "geminiProvider");
        when(router.selectProvider(eq(AITaskType.TEXT_QUERY), anyString(), any())).thenReturn(provider);

        PromptLoader promptLoader = mock(PromptLoader.class);
        when(promptLoader.loadPromptTemplate(anyString())).thenReturn(
            "{\"action\":\"{{action_type}}\",\"tone\":\"{{tone}}\",\"lang\":\"{{language}}\",\"ctx\":{{topic_context_json}}}"
        );

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(startsWith("SELECT topic_metadata::text"),
            any(org.springframework.jdbc.core.RowMapper.class),
            anyString(),
            anyString()))
            .thenReturn(List.of("{\"title\":\"Shipping Confirmation\",\"summary\":\"Client waiting on shipping date\"}"));
        when(jdbc.query(startsWith("SELECT topic_metadata::text"),
            any(org.springframework.jdbc.core.RowMapper.class),
            anyString()))
            .thenReturn(List.of("{\"title\":\"Fallback Topic\",\"summary\":\"General context\"}"));

        TopicActionSuggestionService service = new TopicActionSuggestionService(
            props,
            embeddingProps,
            router,
            promptLoader,
            objectMapper,
            Optional.of(jdbc)
        );

        TopicActionSuggestionRequest req = baseRequest(i);
        configureCaseSpecificMocks(i, provider, jdbc);

        if (i == 16) {
            Assertions.assertThrows(IllegalStateException.class, () -> service.suggest(req));
            row.put("expected", "feature disabled error");
            return;
        }
        if (i == 17) {
            req.setTenantId(null);
            Assertions.assertThrows(IllegalArgumentException.class, () -> service.suggest(req));
            row.put("expected", "missing tenantId error");
            return;
        }
        if (i == 18) {
            req.setActionType(null);
            Assertions.assertThrows(IllegalArgumentException.class, () -> service.suggest(req));
            row.put("expected", "missing actionType error");
            return;
        }

        TopicActionSuggestionResponse res = service.suggest(req);
        row.put("provider", res.getProviderId());
        row.put("actionType", String.valueOf(res.getActionType()));
        row.put("topicContextFound", res.isTopicContextFound());
        row.put("subject", res.getSuggestedSubject());
        row.put("text", res.getSuggestedText());

        Assertions.assertNotNull(res.getMetadata());
        Assertions.assertNotNull(res.getSuggestedText());

        if (req.getActionType() == TopicActionType.FORWARD) {
            Assertions.assertTrue(res.getSuggestedSubject() == null || !res.getSuggestedSubject().isBlank());
        }
        if (i == 15) {
            Assertions.assertTrue(res.getSuggestedText().length() <= 32);
        }
    }

    private TopicActionSuggestionRequest baseRequest(int i) {
        TopicActionSuggestionRequest req = new TopicActionSuggestionRequest();
        req.setTenantId("tenant-1");
        req.setWorkspaceId(i == 10 ? null : "ws-1");
        req.setTopicId(i == 7 ? null : "topic-1");
        req.setActionType((i % 3 == 0 || i == 7 || i == 11 || i == 12 || i == 20) ? TopicActionType.FORWARD : TopicActionType.REPLY);
        req.setUserDisplayName("Benoit");
        req.setRecipient(i % 2 == 0 ? "Client" : "Ops");
        req.setAdditionalInstruction(i == 14 ? "Make it very concise." : "");
        req.setLatestMessage("Can you confirm the shipping date and pricing?");
        if (i == 14) {
            req.setTone("friendly");
            req.setLanguage("French");
        }
        return req;
    }

    private void configureCaseSpecificMocks(int i, AIProvider provider, JdbcTemplate jdbc) {
        String jsonSnake = "{\"suggested_subject\":\"Status update\",\"suggested_text\":\"Thank you. We confirm shipment tomorrow.\"}";
        String jsonCamel = "{\"suggestedSubject\":\"Quick Follow-up\",\"suggestedText\":\"Forwarding for your review and decision.\"}";
        String plain = "Please review and advise next steps.";
        String fenced = "```json\n{\"suggested_subject\":\"Fwd Subject\",\"suggested_text\":\"Forwarding for action.\"}\n```";
        String malformed = "{'suggested_subject':'Bad Json', 'suggested_text':'Still parsable',}";
        String longPlain = "This is a very long plain-text suggestion that should be clipped when maxResponseChars is low.";

        String response = switch (i) {
            case 1 -> jsonSnake;
            case 2 -> jsonCamel;
            case 3 -> plain;
            case 4 -> jsonSnake;
            case 5 -> "{\"suggested_text\":\"Please handle this thread and send final confirmation.\"}";
            case 6 -> "{\"suggested_subject\":\"Fwd: Action Needed\",\"suggested_text\":\"Forwarding to you for immediate action.\"}";
            case 7 -> "{\"suggested_text\":\"Forwarding this item for awareness.\"}";
            case 8 -> jsonSnake;
            case 9 -> jsonSnake;
            case 10 -> jsonSnake;
            case 11 -> fenced;
            case 12 -> malformed;
            case 13 -> jsonSnake;
            case 14 -> jsonSnake;
            case 15 -> longPlain;
            case 19 -> "";
            case 20 -> "{\"suggested_text\":\"Forwarding with context for your review.\"}";
            default -> jsonSnake;
        };
        when(provider.processTextQuery(anyString(), anyString(), anyString())).thenReturn(response);

        if (i == 8) {
            when(jdbc.query(startsWith("SELECT topic_metadata::text"),
                any(org.springframework.jdbc.core.RowMapper.class),
                anyString(),
                anyString())).thenReturn(List.of());
        }
        if (i == 20) {
            when(jdbc.query(startsWith("SELECT topic_metadata::text"),
                any(org.springframework.jdbc.core.RowMapper.class),
                anyString(),
                anyString())).thenThrow(new RuntimeException("db down"));
        }
    }

    private String caseName(int i) {
        return switch (i) {
            case 1 -> "reply_json_snake_case";
            case 2 -> "reply_json_camel_case";
            case 3 -> "reply_plain_text_fallback";
            case 4 -> "reply_with_topic_context";
            case 5 -> "forward_subject_fallback_from_title";
            case 6 -> "forward_with_model_subject";
            case 7 -> "forward_without_topic_id";
            case 8 -> "context_missing_in_db";
            case 9 -> "context_found_with_workspace";
            case 10 -> "context_found_without_workspace";
            case 11 -> "fenced_json_response";
            case 12 -> "lenient_json_parse_single_quotes";
            case 13 -> "default_tone_and_language";
            case 14 -> "custom_tone_and_language";
            case 15 -> "plain_text_clipped_to_max";
            case 16 -> "feature_disabled_rejected";
            case 17 -> "missing_tenant_rejected";
            case 18 -> "missing_action_type_rejected";
            case 19 -> "empty_model_response_safe_fallback";
            case 20 -> "db_failure_still_generates_text";
            default -> "case_" + i;
        };
    }
}
