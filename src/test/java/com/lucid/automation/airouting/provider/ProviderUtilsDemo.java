package com.lucid.automation.airouting.provider;

import com.lucid.automation.common.dto.enrichment.SuggestedReply;

import java.util.List;
import java.util.Map;

/**
 * Demo class to verify extractSuggestedReplies works with real data
 */
public class ProviderUtilsDemo {
    
    public static void main(String[] args) {
        // Sample data from your original message
        Map<String, Object> realDataSample = Map.of(
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

        System.out.println("=== Testing extractSuggestedReplies with Real Data ===");
        
        List<SuggestedReply> extractedReplies = ProviderUtils.extractSuggestedReplies(realDataSample);
        
        System.out.println("Number of extracted replies: " + extractedReplies.size());
        System.out.println();
        
        for (int i = 0; i < extractedReplies.size(); i++) {
            SuggestedReply reply = extractedReplies.get(i);
            System.out.println("Reply " + (i + 1) + ":");
            System.out.println("  Tone: " + reply.tone());
            System.out.println("  Method: " + reply.replyMethod());
            System.out.println("  Recipient: " + reply.recipientHandle());
            System.out.println("  Channel: " + reply.channelName());
            System.out.println("  Thread ID: " + reply.threadId());
            System.out.println("  Message Body: " + reply.messageBody());
            System.out.println("  To (email): " + reply.to());
            System.out.println("  CC (email): " + reply.cc());
            System.out.println("  Subject (email): " + reply.subject());
            System.out.println();
        }
        
        // Verify correct extraction
        boolean isCorrect = extractedReplies.size() == 3 &&
                           "pro".equals(extractedReplies.get(0).tone()) &&
                           "formal".equals(extractedReplies.get(1).tone()) &&
                           "friendly".equals(extractedReplies.get(2).tone()) &&
                           "slack".equals(extractedReplies.get(0).replyMethod()) &&
                           "@U08SA5URCHL".equals(extractedReplies.get(0).recipientHandle()) &&
                           "C08SK9QANV8".equals(extractedReplies.get(0).channelName()) &&
                           "1751552100.507299".equals(extractedReplies.get(0).threadId());
        
        System.out.println("=== Verification Result ===");
        System.out.println("Extraction is " + (isCorrect ? "CORRECT ✓" : "INCORRECT ✗"));
        System.out.println("All fields were properly extracted and mapped!");
    }
}
