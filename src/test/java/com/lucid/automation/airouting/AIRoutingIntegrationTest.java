package com.lucid.automation.airouting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.model.AITaskType;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.SlackParticipant;
import com.lucid.automation.airouting.model.request.CategoryRequest;
import com.lucid.automation.airouting.model.request.ConversationEnrichmentRequest;
import com.lucid.automation.airouting.model.request.SummaryRequest;
import com.lucid.automation.airouting.model.response.HealthStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureTestMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureTestMvc
@ActiveProfiles("test")
public class AIRoutingIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/ai/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.service").value("lucid-ai-routing-service"))
                .andExpect(jsonPath("$.providersStatus").exists());
    }

    @Test
    public void testCategorizeEndpoint() throws Exception {
        CategoryRequest request = new CategoryRequest();
        request.setText("This is an urgent bug report that needs immediate attention");
        request.setCategories(Arrays.asList("bug", "feature", "support", "urgent"));

        mockMvc.perform(post("/ai/categorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.taskType").value("CATEGORIZE"))
                .andExpect(jsonPath("$.result").exists())
                .andExpect(jsonPath("$.provider").exists())
                .andExpect(jsonPath("$.processingTimeMs").exists());
    }

    @Test
    public void testSummarizeEndpoint() throws Exception {
        SummaryRequest request = new SummaryRequest();
        request.setText("This is a long conversation about implementing a new feature. " +
                       "The team discussed various approaches, including using microservices " +
                       "architecture and implementing proper authentication. After much debate, " +
                       "they decided to proceed with Spring Boot implementation.");
        request.setMaxLength(100);

        mockMvc.perform(post("/ai/summarize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.taskType").value("SUMMARIZE"))
                .andExpect(jsonPath("$.result").exists())
                .andExpect(jsonPath("$.provider").exists());
    }

    @Test
    public void testConversationEnrichmentEndpoint() throws Exception {
        // Create test participants
        SlackParticipant participant1 = new SlackParticipant();
        participant1.setUserId("U001");
        participant1.setUsername("john.doe");
        participant1.setDisplayName("John Doe");
        participant1.setIsBot(false);

        SlackParticipant participant2 = new SlackParticipant();
        participant2.setUserId("U002");
        participant2.setUsername("jane.smith");
        participant2.setDisplayName("Jane Smith");
        participant2.setIsBot(false);

        // Create test messages
        SlackMessage message1 = new SlackMessage();
        message1.setMessageId("M001");
        message1.setText("Hey team, we need to discuss the new feature implementation");
        message1.setUser(participant1);
        message1.setTimestamp(Instant.now().minusSeconds(3600));

        SlackMessage message2 = new SlackMessage();
        message2.setMessageId("M002");
        message2.setText("I agree, let's schedule a meeting for tomorrow");
        message2.setUser(participant2);
        message2.setTimestamp(Instant.now().minusSeconds(3000));

        ConversationEnrichmentRequest request = new ConversationEnrichmentRequest();
        request.setMessages(Arrays.asList(message1, message2));
        request.setParticipants(Arrays.asList(participant1, participant2));

        mockMvc.perform(post("/ai/enrich-conversation")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.taskType").value("ENRICH_CONVERSATION"))
                .andExpect(jsonPath("$.result").exists())
                .andExpect(jsonPath("$.provider").exists());
    }

    @Test
    public void testBatchProcessingEndpoint() throws Exception {
        CategoryRequest categoryRequest = new CategoryRequest();
        categoryRequest.setText("Bug report");
        categoryRequest.setCategories(Arrays.asList("bug", "feature"));

        SummaryRequest summaryRequest = new SummaryRequest();
        summaryRequest.setText("Long text to summarize...");
        summaryRequest.setMaxLength(50);

        List<Object> requests = Arrays.asList(categoryRequest, summaryRequest);

        mockMvc.perform(post("/ai/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requests)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    public void testInvalidRequestReturns400() throws Exception {
        mockMvc.perform(post("/ai/categorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void testTaskTypesEndpoint() throws Exception {
        mockMvc.perform(get("/ai/task-types"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(AITaskType.values().length));
    }

    @Test
    public void testProvidersEndpoint() throws Exception {
        mockMvc.perform(get("/ai/providers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }
}