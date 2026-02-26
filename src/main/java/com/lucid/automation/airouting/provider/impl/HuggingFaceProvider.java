package com.lucid.automation.airouting.provider.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.exception.TokenQuotaExhaustedException;
import com.lucid.automation.airouting.model.message.AIMessage;
import com.lucid.automation.airouting.provider.AIProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Hugging Face Inference API provider.
 *
 * Supports hosted models and custom endpoints that accept the HF "inputs" payload.
 * This is intended for rapid experimentation with open-source models (e.g. via HF)
 * without changing Steps 1/3/6 code.
 */
@Component("huggingfaceProvider")
public class HuggingFaceProvider extends AIProvider {

    private static final Logger logger = LoggerFactory.getLogger(HuggingFaceProvider.class);

    @Value("${ai.providers.huggingface.api-key:}")
    private String apiKey;

    @Value("${ai.providers.huggingface.endpoint:https://api-inference.huggingface.co/models}")
    private String endpoint;

    @Value("${ai.providers.huggingface.model:}")
    private String model;

    @Value("${ai.providers.huggingface.timeout:60000}")
    private int timeoutMs;

    @Value("${ai.providers.huggingface.enabled:false}")
    private boolean enabled;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(20))
        .build();
    private double lastConfidence = 0.0;

    @Override
    public String getProviderId() {
        return "huggingfaceProvider";
    }

    @Override
    public boolean isAvailable() {
        return enabled && StringUtils.hasText(resolveApiKey()) && StringUtils.hasText(resolveEndpoint());
    }

    @Override
    public double getLastConfidence() {
        return lastConfidence;
    }

    @Override
    protected Map<String, Object> doEnrichConversation(AIMessage request, String tenantId, String deemergeUserId, String deemergeUserName, String debugId) {
        return Map.of("response", "huggingfaceProvider does not support conversation enrichment in this service yet");
    }

    @Override
    protected String doProcessTextQuery(String maskedQuery, String userId, String tenantId, String debugId) {
        if (!isAvailable()) {
            return "HuggingFace provider not available";
        }

        // Token quota check (uses shared tenant token mechanism)
        if (!isTokenAvailableForTenant(tenantId)) {
            throw new TokenQuotaExhaustedException(tenantId, getProviderId());
        }

        String url = resolveEndpoint();
        try {
            long start = System.currentTimeMillis();
            String body = objectMapper.writeValueAsString(Map.of("inputs", maskedQuery));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Authorization", "Bearer " + resolveApiKey())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long duration = System.currentTimeMillis() - start;

            if (response.statusCode() / 100 != 2) {
                logger.warn("HF-API [{}]: non-2xx status={} url={} bodyLen={}", debugId, response.statusCode(), url, response.body() != null ? response.body().length() : 0);
                throw new RuntimeException("HuggingFace API error status=" + response.statusCode());
            }

            String output = extractText(response.body());
            if (!StringUtils.hasText(output)) {
                throw new RuntimeException("Empty response from HuggingFace API");
            }

            // HF does not return token usage reliably; estimate and mark as estimated.
            int inputTokensEst = estimateTokens(maskedQuery);
            int outputTokensEst = estimateTokens(output);
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("endpoint", url);
            metadata.put("model", model);
            metadata.put("actualTokens", false);
            sendTokenConsumption("text-query", userId, tenantId, inputTokensEst, outputTokensEst, inputTokensEst + outputTokensEst, null, metadata);

            logger.info("✅ HF-API [{}]: text-query completed | ⏱️ {}ms | estIn={} estOut={} | respChars={}",
                debugId, duration, inputTokensEst, outputTokensEst, output.length());
            return output;
        } catch (Exception e) {
            logger.error("🚨 HF-API [{}]: Error calling HuggingFace API url={}: {}", debugId, url, e.getMessage(), e);
            throw new RuntimeException("Failed to call HuggingFace API: " + e.getMessage(), e);
        }
    }

    private String resolveApiKey() {
        if (StringUtils.hasText(apiKey)) {
            return apiKey.trim();
        }
        String env = System.getenv("HUGGINGFACE_API_KEY");
        if (StringUtils.hasText(env)) {
            return env.trim();
        }
        String envAlt = System.getenv("HF_API_KEY");
        if (StringUtils.hasText(envAlt)) {
            return envAlt.trim();
        }
        return null;
    }

    private String resolveEndpoint() {
        String base = endpoint != null ? endpoint.trim() : "";
        if (!StringUtils.hasText(base)) {
            return null;
        }
        if (StringUtils.hasText(model)) {
            String m = model.trim();
            if (base.endsWith("/models")) {
                return base + "/" + m;
            }
            // If endpoint already looks like a full URL, keep it.
            if (base.contains("{model}")) {
                return base.replace("{model}", m);
            }
        }
        return base;
    }

    private String extractText(String json) {
        if (!StringUtils.hasText(json)) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(json);
            // Common HF hosted response: [{"generated_text":"..."}]
            if (node.isArray() && node.size() > 0) {
                JsonNode first = node.get(0);
                if (first.isObject()) {
                    JsonNode gt = first.get("generated_text");
                    if (gt != null && gt.isTextual()) {
                        return gt.asText();
                    }
                }
            }
            // Some endpoints: {"generated_text":"..."}
            if (node.isObject()) {
                JsonNode gt = node.get("generated_text");
                if (gt != null && gt.isTextual()) {
                    return gt.asText();
                }
                // Text-generation-inference style: {"choices":[{"text":"..."}]}
                JsonNode choices = node.get("choices");
                if (choices != null && choices.isArray() && choices.size() > 0) {
                    JsonNode first = choices.get(0);
                    JsonNode text = first.get("text");
                    if (text != null && text.isTextual()) {
                        return text.asText();
                    }
                    JsonNode message = first.get("message");
                    if (message != null && message.isObject()) {
                        JsonNode content = message.get("content");
                        if (content != null && content.isTextual()) {
                            return content.asText();
                        }
                    }
                }
            }
        } catch (Exception ignored) {
            // If response is not JSON, return raw.
        }
        return json;
    }

    private int estimateTokens(String text) {
        if (!StringUtils.hasText(text)) {
            return 0;
        }
        return Math.max(1, text.length() / 4);
    }
}
