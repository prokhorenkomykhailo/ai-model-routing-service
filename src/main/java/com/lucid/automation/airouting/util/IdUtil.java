package com.lucid.automation.airouting.util;

import java.util.UUID;

public class IdUtil {
    /**
     * Generates a random ID string with the given prefix.
     * @param prefix the prefix to prepend to the ID
     * @return the generated ID string
     */
    public static String generateId(String prefix) {
        return prefix + UUID.randomUUID().toString();
    }
}
