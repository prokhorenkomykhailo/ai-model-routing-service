package com.lucid.automation.airouting.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Value("${spring.application.name}")
    private String applicationName;
    
    @Value("${server.port:8083}")
    private String serverPort;
    
    @Value("${springdoc.swagger-ui.gateway-uri:/}")
    private String gatewayUri;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
            .addServersItem(new Server()
                    .url(gatewayUri)
                    .description("AI Routing Service API"))
            .info(new Info()
                    .title("Lucid AI Routing Service API")
                    .description("Spring Boot REST API for AI model routing and orchestration in the Lucid platform. " +
                               "This service handles routing requests to various AI providers including OpenAI, Gemini, and LangChain " +
                               "for tasks such as summarization, categorization, conversation enrichment, and more.")
                    .version("1.0.0")
                    .contact(new Contact()
                            .name("Lucid Team")
                            .url("https://www.lucid.com")
                            .email("support@lucid.com"))
                    .license(new License()
                            .name("Apache 2.0")
                            .url("https://www.apache.org/licenses/LICENSE-2.0")))
            .components(new Components()
                    .addSecuritySchemes("bearerAuth",
                            new SecurityScheme()
                                    .type(SecurityScheme.Type.HTTP)
                                    .scheme("bearer")
                                    .bearerFormat("JWT")
                                    .description("JWT token authentication")))
            .addSecurityItem(new SecurityRequirement().addList("bearerAuth"));
    }
}
