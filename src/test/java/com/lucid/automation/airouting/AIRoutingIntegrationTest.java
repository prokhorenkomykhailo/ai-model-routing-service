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
@ActiveProfiles("test")
public class AIRoutingIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;
}