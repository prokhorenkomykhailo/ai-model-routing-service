package com.lucid.automation.airouting.provider;

import com.lucid.automation.common.dto.enrichment.SuggestedReply;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ProviderUtilsTest {

    @Test
    public void testExtractSuggestedReplies_RealDataFromMessage() {
        // Exact data structure from your message
        Map<String, Object> topicMap = Map.of(
            "suggestedReplies", List.of(
                Map.of(
                    "tone", "pro",
                    "replyMethod", "slack",
                    "recipientHandle", "@U08SA5URCHL",
                    "channelName", "C08SK9QANV8",
                    "threadId", "1751552100.507299",
                    "messageBody", "Vu, please provide an ETA for UI availability. Confirm prompt testing will support API-pulled messages without manual entry as discussed."
                ),
                Map.of(
                    "tone", "formal",
                    "replyMethod", "slack",
                    "recipientHandle", "@U08SA5URCHL",
                    "channelName", "C08SK9QANV8",
                    "threadId", "1751552100.507299",
                    "messageBody", "Dear Vu, could you please provide an estimated timeframe for the UI to be fully operational? Additionally, I would appreciate confirmation that the prompt testing functionality will accommodate API-pulled messages, negating the need for manual data entry, in line with our previous discussion."
                ),
                Map.of(
                    "tone", "friendly",
                    "replyMethod", "slack",
                    "recipientHandle", "@U08SA5URCHL",
                    "channelName", "C08SK9QANV8",
                    "threadId", "1751552100.507299",
                    "messageBody", "Hey Vu, any update on when the UI will be ready? Just want to make sure the prompt testing is set up to pull messages directly via API. Let me know!"
                )
            )
        );

        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);

        assertNotNull(result);
        assertEquals(3, result.size());

        // Verify all three replies are correctly parsed
        SuggestedReply proReply = result.get(0);
        assertEquals("pro", proReply.tone());
        assertEquals("slack", proReply.replyMethod());
        assertEquals("@U08SA5URCHL", proReply.recipientHandle());
        assertEquals("C08SK9QANV8", proReply.channelName());
        assertEquals("1751552100.507299", proReply.threadId());
        assertEquals("Vu, please provide an ETA for UI availability. Confirm prompt testing will support API-pulled messages without manual entry as discussed.", proReply.messageBody());

        SuggestedReply formalReply = result.get(1);
        assertEquals("formal", formalReply.tone());
        assertEquals("slack", formalReply.replyMethod());
        assertEquals("@U08SA5URCHL", formalReply.recipientHandle());
        assertEquals("C08SK9QANV8", formalReply.channelName());
        assertEquals("1751552100.507299", formalReply.threadId());
        assertTrue(formalReply.messageBody().startsWith("Dear Vu"));

        SuggestedReply friendlyReply = result.get(2);
        assertEquals("friendly", friendlyReply.tone());
        assertEquals("slack", friendlyReply.replyMethod());
        assertEquals("@U08SA5URCHL", friendlyReply.recipientHandle());
        assertEquals("C08SK9QANV8", friendlyReply.channelName());
        assertEquals("1751552100.507299", friendlyReply.threadId());
        assertTrue(friendlyReply.messageBody().startsWith("Hey Vu"));
    }

    @Test
    public void testExtractSuggestedReplies_EmptyList() {
        Map<String, Object> topicMap = Map.of("suggestedReplies", List.of());
        
        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExtractSuggestedReplies_NoSuggestedReplies() {
        Map<String, Object> topicMap = Map.of();
        
        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExtractSuggestedReplies_InvalidDataType() {
        Map<String, Object> topicMap = Map.of("suggestedReplies", "not a list");
        
        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExtractSuggestedReplies_MissingFields() {
        // Test with missing optional fields
        Map<String, Object> topicMap = Map.of(
            "suggestedReplies", List.of(
                Map.of(
                    "tone", "pro",
                    "messageBody", "Just the message body"
                    // Missing: replyMethod, recipientHandle, channelName, threadId, etc.
                )
            )
        );

        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);

        assertNotNull(result);
        assertEquals(1, result.size());

        SuggestedReply reply = result.get(0);
        assertEquals("pro", reply.tone());
        assertNull(reply.replyMethod());
        assertNull(reply.recipientHandle());
        assertNull(reply.channelName());
        assertNull(reply.channelId());
        assertNull(reply.threadId());
        assertNull(reply.to());
        assertNotNull(reply.cc());
        assertTrue(reply.cc().isEmpty());
        assertNull(reply.subject());
        assertEquals("Just the message body", reply.messageBody());
    }

    @Test
    public void testExtractSuggestedReplies_NullMessageBody() {
        // Test with null messageBody - should default to empty string
        Map<String, Object> topicMap = Map.of(
            "suggestedReplies", List.of(
                Map.of("tone", "pro")
                // messageBody is missing
            )
        );

        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("", result.get(0).messageBody()); // Should default to empty string
    }

    @Test
    public void testExtractSuggestedReplies_EmailData() {
        Map<String, Object> topicMap = Map.of(
            "suggestedReplies", List.of(
                Map.of(
                    "tone", "formal",
                    "replyMethod", "email",
                    "to", "user@example.com",
                    "cc", List.of("manager@example.com", "team@example.com"),
                    "subject", "Test Subject",
                    "messageBody", "Test email body"
                )
            )
        );

        List<SuggestedReply> result = ProviderUtils.extractSuggestedReplies(topicMap);

        assertNotNull(result);
        assertEquals(1, result.size());

        SuggestedReply reply = result.get(0);
        assertEquals("formal", reply.tone());
        assertEquals("email", reply.replyMethod());
        assertNull(reply.recipientHandle()); // Not applicable for email
        assertNull(reply.channelName()); // Not applicable for email
        assertEquals("user@example.com", reply.to());
        assertEquals(List.of("manager@example.com", "team@example.com"), reply.cc());
        assertEquals("Test Subject", reply.subject());
        assertEquals("Test email body", reply.messageBody());
    }
}
