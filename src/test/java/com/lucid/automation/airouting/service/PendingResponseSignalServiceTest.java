package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.common.dto.topic.TopicMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PendingResponseSignalServiceTest {

    private final PendingResponseSignalService service = new PendingResponseSignalService();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void fromLlmNode_parsesPendingResponse_whenDetected() throws Exception {
        String json = """
            {
              "title": "Invoice Follow-up",
              "pending_response": {
                "detected": true,
                "type": "waiting_on_other_party",
                "requester": "Devon (U001)",
                "requester_user_id": "U001",
                "assignee": "Sam (U002)",
                "assignee_user_id": "U002",
                "request_text": "Can you send the latest invoice?",
                "reason": "Invoice still not delivered.",
                "suggested_action": "Follow up with Sam for invoice delivery.",
                "evidence": "request in thread, no response"
              }
            }
            """;
        var signal = service.fromLlmNode(objectMapper.readTree(json));
        Assertions.assertTrue(signal.isPresent());
        Assertions.assertEquals("waiting_on_other_party", signal.get().type());
        Assertions.assertEquals("Sam (U002)", signal.get().assignee());
        Assertions.assertEquals("U002", signal.get().assigneeUserId());
    }

    @Test
    void fromLlmNode_returnsEmpty_whenSignalMissing() throws Exception {
        String json = """
            {
              "title": "No Pending Case",
              "summary": "Everything resolved",
              "pending_response": {
                "detected": false
              }
            }
            """;
        var signal = service.fromLlmNode(objectMapper.readTree(json));
        Assertions.assertTrue(signal.isEmpty());
    }

    @Test
    void applyToTopic_addsActionAndTags_fromSignal() {
        TopicMetadata topic = new TopicMetadata();
        topic.setTitle("Invoice Follow-up");

        PendingResponseSignalService.PendingResponseSignal signal =
            new PendingResponseSignalService.PendingResponseSignal(
                "waiting_on_current_user",
                "Ops (U099)",
                "U099",
                "Devon (U001)",
                "U001",
                "When can you confirm shipping date?",
                "Ops is waiting for shipping confirmation.",
                "Respond to Ops with shipping date confirmation.",
                "message without reply"
            );

        TopicMetadata updated = service.applyToTopic(topic, signal, "Devon (U001)");
        Assertions.assertNotNull(updated.getActionItems());
        Assertions.assertFalse(updated.getActionItems().isEmpty());
        Assertions.assertTrue(updated.getTags().contains("pending-response"));
        Assertions.assertTrue(updated.getTags().contains("awaiting-user-response"));
        Assertions.assertTrue(updated.getParticipants().contains("Devon (U001)"));
        Assertions.assertTrue(updated.getParticipants().contains("Ops (U099)"));
    }

    @Test
    void fromLlmNode_supportsCamelCaseKey() throws Exception {
        String json = """
            {
              "pendingResponse": {
                "detected": true,
                "type": "waiting_on_other_party",
                "requester": "Devon",
                "assignee": "Finance",
                "suggestedAction": "Follow up with Finance."
              }
            }
            """;
        var signal = service.fromLlmNode(objectMapper.readTree(json));
        Assertions.assertTrue(signal.isPresent());
        Assertions.assertEquals("Finance", signal.get().assignee());
    }
}
