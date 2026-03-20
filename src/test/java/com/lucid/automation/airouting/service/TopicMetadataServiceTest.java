package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicMetadataProperties;
import com.lucid.automation.common.dto.topic.TopicActionItem;
import com.lucid.automation.common.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.producer.TopicMetadataProducer;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

class TopicMetadataServiceTest {

    @Test
    void parseMetadata_parsesUxFields_andDueDateAlias() throws Exception {
        TopicMetadataService service = new TopicMetadataService(
            mock(TopicMetadataPromptBuilder.class),
            mock(AIProviderRouterService.class),
            mock(TopicMetadataProducer.class),
            mock(MessageService.class),
            mock(PendingResponseSignalService.class),
            new ObjectMapper(),
            new TopicMetadataProperties()
        );

        String json = """
{
  "title": "Follow up on BENEL PO LEG-260101 shipping and pricing",
  "external_party": "BENEL Industries Corp.",
  "priority": "high",
  "due_date": "2026-01-13",
  "reason": "Customer is waiting for confirmation.",
  "suggested_action": "Confirm shipping date and pricing and reply.",
  "situation": "BENEL sent a PO and requested confirmation.",
  "impact": "Delay may impact fulfillment.",
  "proposed_solution": "Validate pricing and confirm date.",
  "decision_needed": "Confirm details and respond.",
  "participants": ["Ling", "Earl Yap"],
  "action_items": [{"task":"Confirm shipping date","owner":"Ling","due_date":"2026-01-13","status":"pending","priority":"high"}],
  "urgency": "high",
  "status": "active",
  "channel": "#sales-orders",
  "tags": ["benel","po","shipping","pricing","follow-up"]
}
""";

        Method parse = TopicMetadataService.class.getDeclaredMethod("parseMetadata", String.class);
        parse.setAccessible(true);
        TopicMetadata meta = (TopicMetadata) parse.invoke(service, json);

        Assertions.assertNotNull(meta);
        Assertions.assertEquals("Follow up on BENEL PO LEG-260101 shipping and pricing", meta.getTitle());
        Assertions.assertEquals("BENEL Industries Corp.", meta.getExternalParty());
        Assertions.assertEquals("high", meta.getPriority());
        Assertions.assertEquals("2026-01-13", meta.getDeadline());
        Assertions.assertEquals("Customer is waiting for confirmation.", meta.getReason());
        Assertions.assertEquals("Confirm shipping date and pricing and reply.", meta.getSuggestedAction());
        Assertions.assertEquals("BENEL sent a PO and requested confirmation.", meta.getSituation());
        Assertions.assertEquals("Delay may impact fulfillment.", meta.getImpact());
        Assertions.assertEquals("Validate pricing and confirm date.", meta.getProposedSolution());
        Assertions.assertEquals("Confirm details and respond.", meta.getDecisionNeeded());
        Assertions.assertEquals(List.of("Ling", "Earl Yap"), meta.getParticipants());
        Assertions.assertEquals(1, meta.getActionItems().size());
        Assertions.assertEquals("#sales-orders", meta.getChannel());
        Assertions.assertFalse(meta.getTags().isEmpty());
    }

    @Test
    void applyUxFallbacks_derivesMissingFields_fromExistingMetadata() throws Exception {
        TopicMetadataService service = new TopicMetadataService(
            mock(TopicMetadataPromptBuilder.class),
            mock(AIProviderRouterService.class),
            mock(TopicMetadataProducer.class),
            mock(MessageService.class),
            mock(PendingResponseSignalService.class),
            new ObjectMapper(),
            new TopicMetadataProperties()
        );

        TopicMetadata meta = new TopicMetadata();
        meta.setTitle("EcoBloom Summer Campaign Kickoff");
        meta.setSummary("Kickoff meeting scheduled tomorrow. Prepare initial ideas.");
        meta.setUrgency("high");
        meta.setParticipants(List.of("Devon"));
        meta.setTags(List.of("ecobloom"));

        TopicActionItem ai = new TopicActionItem();
        ai.setTask("Prepare initial ideas for kickoff meeting");
        ai.setOwner("Devon");
        ai.setStatus("pending");
        ai.setPriority("high");
        meta.setActionItems(List.of(ai));

        Method apply = TopicMetadataService.class.getDeclaredMethod("applyUxFallbacks", TopicMetadata.class);
        apply.setAccessible(true);
        apply.invoke(service, meta);

        Assertions.assertEquals("high", meta.getPriority());
        Assertions.assertTrue(meta.getSuggestedAction().contains("Prepare initial ideas"));
        Assertions.assertTrue(meta.getReason() != null && !meta.getReason().isBlank());
        Assertions.assertTrue(meta.getSituation() != null && !meta.getSituation().isBlank());
        Assertions.assertTrue(meta.getImpact() != null && !meta.getImpact().isBlank());
        Assertions.assertTrue(meta.getProposedSolution() != null && !meta.getProposedSolution().isBlank());
        Assertions.assertTrue(meta.getDecisionNeeded() != null && !meta.getDecisionNeeded().isBlank());
    }
}
