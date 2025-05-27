package com.lucid.automation.airouting.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Generic API response wrapper for data-storage-service responses
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class APIResponse<T> {
    private boolean success;
    private String message;
    private T data;
    private String error;
    
    public static <T> APIResponse<T> success(String message, T data) {
        return APIResponse.<T>builder()
                .success(true)
                .message(message)
                .data(data)
                .build();
    }
    
    public static <T> APIResponse<T> error(String error) {
        return APIResponse.<T>builder()
                .success(false)
                .error(error)
                .build();
    }
}
