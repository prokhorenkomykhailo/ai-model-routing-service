package com.lucid.automation.airouting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableDiscoveryClient
@EnableConfigurationProperties
@EnableAsync
@EnableScheduling
@EnableFeignClients
@EnableRedisRepositories
public class AIRoutingServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(AIRoutingServiceApplication.class, args);
    }
}
