package com.lucid.automation.airouting.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Utility to help diagnose and clean up problematic messages in Kafka topics
 */
@Component
public class KafkaTopicCleanupUtil {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicCleanupUtil.class);
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${kafka.topics.ingestion-messages}")
    private String ingestionMessagesTopic;
    
    /**
     * Read the first few messages from the topic to diagnose issues
     */
    public void diagnoseTopic() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "diagnostic-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(ingestionMessagesTopic, 0);
            consumer.assign(Collections.singletonList(partition));
            consumer.seekToBeginning(Collections.singletonList(partition));
            
            logger.info("Starting diagnostic read of topic: {}", ingestionMessagesTopic);
            
            int messageCount = 0;
            int maxMessages = 10; // Only read first 10 messages for diagnosis
            
            while (messageCount < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    logger.info("No more messages found in topic");
                    break;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    logger.info("Message {}: partition={}, offset={}, key={}", 
                        messageCount, record.partition(), record.offset(), record.key());
                    
                    String value = record.value();
                    if (value != null && value.length() > 100) {
                        logger.info("Value (first 100 chars): {}", value.substring(0, 100));
                    } else {
                        logger.info("Value: {}", value);
                    }
                    
                    // Check if this looks like the problematic message
                    if (value != null && value.trim().startsWith("TEsts")) {
                        logger.error("FOUND PROBLEMATIC MESSAGE at partition={}, offset={}", 
                            record.partition(), record.offset());
                        logger.error("Problematic value: {}", value);
                    }
                    
                    logger.info("---");
                    
                    if (messageCount >= maxMessages) {
                        break;
                    }
                }
            }
            
            logger.info("Diagnostic read complete. Read {} messages", messageCount);
            
        } catch (Exception e) {
            logger.error("Error during topic diagnosis", e);
        }
    }
    
    /**
     * Skip to the end of the topic to bypass problematic messages
     * This effectively moves the consumer group past any problematic messages
     */
    public void skipToEndOfTopic() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ai-routing-service-group"); // Use the actual consumer group
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(ingestionMessagesTopic, 0);
            consumer.assign(Collections.singletonList(partition));
            
            // Seek to end
            consumer.seekToEnd(Collections.singletonList(partition));
            
            // Commit the offset to move the consumer group past problematic messages
            consumer.commitSync();
            
            logger.info("Successfully moved consumer group to end of topic: {}", ingestionMessagesTopic);
            
        } catch (Exception e) {
            logger.error("Error skipping to end of topic", e);
        }
    }
}
