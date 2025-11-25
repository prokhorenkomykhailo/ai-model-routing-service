package com.lucid.automation.airouting.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import feign.Logger;
import feign.RequestInterceptor;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Configuration for Feign clients
 */
@Configuration
public class FeignClientConfig {

    @Value("${spring.application.name:ai-routing-service}")
    private String serviceName;

    /**
     * Set Feign client logging level
     *
     * @return the logging level
     */
    @Bean
    Logger.Level feignLoggerLevel() {
        return Logger.Level.FULL;
    }

    /**
     * Add authorization header to requests
     * This interceptor will add the JWT token from the current request to the outgoing Feign request
     *
     * @return the request interceptor
     */
    @Bean
    public RequestInterceptor authRequestInterceptor() {
        return requestTemplate -> {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes != null) {
                HttpServletRequest request = attributes.getRequest();
                String authHeader = request.getHeader("Authorization");
                if (authHeader != null && authHeader.startsWith("Bearer ")) {
                    // Add Authorization header with the JWT token to the Feign request
                    requestTemplate.header("Authorization", authHeader);
                }
            }
        };
    }

    /**
     * Add internal service headers for service-to-service communication
     * These headers are required by the gateway's InternalApiBlockFilter
     * to allow internal service-to-service communication.
     *
     * @return the request interceptor for internal service headers
     */
    @Bean
    public RequestInterceptor internalServiceHeaderInterceptor() {
        return requestTemplate -> {
            // Add headers required by InternalApiBlockFilter
            requestTemplate.header("X-Internal-Service", "true");
            requestTemplate.header("X-Service-Name", serviceName);
        };
    }
}
