package com.lucid.automation.airouting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
public class AIRoutingServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(AIRoutingServiceApplication.class, args);
    }
}
