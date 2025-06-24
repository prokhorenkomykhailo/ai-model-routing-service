package com.lucid.automation.airouting.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.automation.airouting.dto.UserDTO;

public class TextUtils {
    
    // Private constructor to prevent instantiation
    private TextUtils() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    private static final Logger logger = LoggerFactory.getLogger(TextUtils.class);
    
    /**
     * Replaces Slack mentions (e.g., <@U12345>) in the input text with the display name or username from userInfos.
     * @param text The input text containing Slack mentions.
     * @param userInfos Map of userId to UserDTO.
     * @return The text with Slack mentions replaced.
     */
    public static String replaceSlackMentions(String text, Map<String, UserDTO> userInfos) {
        if (text == null) return null;
        if (userInfos == null || userInfos.isEmpty()) return text;

        for (String key : userInfos.keySet()) {

            // Skip null / empty keys
            if (key == null || key.isBlank()) {
                logger.warn("Found null or empty key in userInfos map, skipping");
                continue;
            }

            try {
                //   (?<=^|\\s)     – phía trước phải là đầu dòng hoặc khoảng trắng
                //   (<@ID>|ID)     – chính mention (ID bọc <> hoặc trần)
                //   (?=[\\s.,!?]|$)– phía sau là khoảng trắng, dấu câu, hoặc kết thúc chuỗi
                String mentionPattern =
                        "(?<=^|\\s)(<@" + Pattern.quote(key) + ">|" + Pattern.quote(key) + ")(?=[\\s.,!?]|$)";
                Pattern pattern = Pattern.compile(mentionPattern);
                Matcher matcher = pattern.matcher(text);

                UserDTO user   = userInfos.get(key);
                String name;
                if (user != null) {
                    if (user.displayName() != null) {
                        name = user.displayName();
                    } else if (user.username() != null) {
                        name = user.username();
                    } else {
                        name = key;
                    }
                } else {
                    name = key;
                }

                // Thay thế bằng " name " (một dấu cách hai bên)
                text = matcher.replaceAll(" " + name + " ");

            } catch (Exception e) {
                logger.warn("Failed to process mention for key: {}, error: {}", key, e.getMessage());
            }
        }

        // Gộp các cụm >=2 dấu cách thành 1, rồi trim hai đầu
        text = text.replaceAll("\\s{2,}", " ").trim();
        return text;
    }
}
