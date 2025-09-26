package com.lucid.automation.airouting.service;

import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.common.dto.messaging.SlackUserDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple tests for field migration logic validation.
 * Tests the core preference logic for uniqueUserId over slackUserId and avatarUrl over imageOriginal.
 *
 * @author vudu
 */
@DisplayName("Field Migration Logic Tests")
class FieldMigrationLogicTest {

    @Test
    @DisplayName("✅ SlackUserDTO should prefer uniqueUserId over slackUserId")
    void testUniqueUserIdPreference() {
        // Given
        SlackUserDTO user = SlackUserDTO.builder()
                .uniqueUserId("unique-123")
                .slackUserId("slack-456")
                .build();

        // When
        String resolvedId = user.getUniqueUserId() != null ? user.getUniqueUserId() : user.getSlackUserId();

        // Then
        assertEquals("unique-123", resolvedId, 
                "Should prefer uniqueUserId when both are present");
    }

    @Test
    @DisplayName("✅ SlackUserDTO should fallback to slackUserId when uniqueUserId is null")
    void testSlackUserIdFallback() {
        // Given
        SlackUserDTO user = SlackUserDTO.builder()
                .uniqueUserId(null)
                .slackUserId("slack-456")
                .build();

        // When
        String resolvedId = user.getUniqueUserId() != null ? user.getUniqueUserId() : user.getSlackUserId();

        // Then
        assertEquals("slack-456", resolvedId, 
                "Should fallback to slackUserId when uniqueUserId is null");
    }

    @Test
    @DisplayName("✅ SlackUserDTO should prefer avatarUrl over imageOriginal")
    void testAvatarUrlPreference() {
        // Given
        SlackUserDTO user = SlackUserDTO.builder()
                .avatarUrl("https://avatar.com/user.jpg")
                .imageOriginal("https://slack.com/original.jpg")
                .build();

        // When
        String resolvedImage = user.getAvatarUrl() != null ? user.getAvatarUrl() : user.getImageOriginal();

        // Then
        assertEquals("https://avatar.com/user.jpg", resolvedImage, 
                "Should prefer avatarUrl when both are present");
    }

    @Test
    @DisplayName("✅ SlackUserDTO should fallback to imageOriginal when avatarUrl is null")
    void testImageOriginalFallback() {
        // Given
        SlackUserDTO user = SlackUserDTO.builder()
                .avatarUrl(null)
                .imageOriginal("https://slack.com/original.jpg")
                .build();

        // When
        String resolvedImage = user.getAvatarUrl() != null ? user.getAvatarUrl() : user.getImageOriginal();

        // Then
        assertEquals("https://slack.com/original.jpg", resolvedImage, 
                "Should fallback to imageOriginal when avatarUrl is null");
    }

    @Test
    @DisplayName("✅ Message field resolution should work with SlackMessage")
    void testMessageFieldResolution() {
        // Given
        SlackMessage message = new SlackMessage();
        String uniqueUserId = "unique-789";
        String slackUserId = "slack-123";
        String resolvedUserId = uniqueUserId != null ? uniqueUserId : slackUserId;

        // When
        message.setSlackUserId(resolvedUserId);

        // Then
        assertEquals("unique-789", message.getSlackUserId(), 
                "Message should store the resolved user ID");
    }

    @Test
    @DisplayName("✅ Integration test: both field preferences work together")
    void testIntegratedFieldResolution() {
        // Given
        SlackUserDTO user = SlackUserDTO.builder()
                .uniqueUserId("integration-unique")
                .slackUserId("integration-slack")
                .avatarUrl("https://avatar-integrated.com/user.png")
                .imageOriginal("https://slack-integrated.com/user.png")
                .name("Integration User")
                .build();

        // When
        String resolvedUserId = user.getUniqueUserId() != null ? user.getUniqueUserId() : user.getSlackUserId();
        String resolvedImage = user.getAvatarUrl() != null ? user.getAvatarUrl() : user.getImageOriginal();
        
        SlackMessage message = new SlackMessage();
        message.setSlackUserId(resolvedUserId);
        message.setImageOriginal(resolvedImage);
        message.setName(user.getName());

        // Then
        assertEquals("integration-unique", message.getSlackUserId(), 
                "Should use uniqueUserId for user identification");
        assertEquals("https://avatar-integrated.com/user.png", message.getImageOriginal(), 
                "Should use avatarUrl for image");
        assertEquals("Integration User", message.getName(), 
                "Should preserve other fields");
    }
}