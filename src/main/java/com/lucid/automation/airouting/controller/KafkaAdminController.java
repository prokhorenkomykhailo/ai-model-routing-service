package com.lucid.automation.airouting.controller;

import com.lucid.automation.airouting.util.KafkaTopicCleanupUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Administrative controller for debugging and managing Kafka topics
 */
@RestController
@RequestMapping("/admin/kafka")
@RequiredArgsConstructor
public class KafkaAdminController {
    
    private final KafkaTopicCleanupUtil kafkaTopicCleanupUtil;
    
    /**
     * Diagnose the ingestion messages topic to identify problematic messages
     */
    @PostMapping("/diagnose-topic")
    public ResponseEntity<String> diagnoseTopic() {
        try {
            kafkaTopicCleanupUtil.diagnoseTopic();
            return ResponseEntity.ok("Topic diagnosis completed. Check logs for results.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body("Error during topic diagnosis: " + e.getMessage());
        }
    }
    
    /**
     * Skip to the end of the topic to bypass problematic messages
     */
    @PostMapping("/skip-to-end")
    public ResponseEntity<String> skipToEndOfTopic() {
        try {
            kafkaTopicCleanupUtil.skipToEndOfTopic();
            return ResponseEntity.ok("Successfully moved consumer group to end of topic.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body("Error skipping to end of topic: " + e.getMessage());
        }
    }
}
