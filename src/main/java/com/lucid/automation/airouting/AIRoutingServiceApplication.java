package com.lucid.automation.airouting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableDiscoveryClient
@EnableConfigurationProperties
@EnableAsync
@EnableFeignClients
public class AIRoutingServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(AIRoutingServiceApplication.class, args);
    }
}
