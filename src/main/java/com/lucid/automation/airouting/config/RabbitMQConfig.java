package com.lucid.automation.airouting.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration for AI message processing
 */
@Configuration
@EnableRabbit
public class RabbitMQConfig {
    
    // Exchange names
    @Value("${rabbitmq.exchange.ai-requests:ai.requests}")
    private String aiRequestsExchange;
    
    @Value("${rabbitmq.exchange.ai-responses:ai.responses}")
    private String aiResponsesExchange;
    
    // Queue names
    @Value("${rabbitmq.queue.ai-requests:ai.requests.queue}")
    private String aiRequestsQueue;
    
    @Value("${rabbitmq.queue.ai-categorize:ai.categorize.queue}")
    private String aiCategorizeQueue;
    
    @Value("${rabbitmq.queue.ai-summarize:ai.summarize.queue}")
    private String aiSummarizeQueue;
    
    @Value("${rabbitmq.queue.ai-enrich:ai.enrich.queue}")
    private String aiEnrichQueue;
    
    @Value("${rabbitmq.queue.ai-responses:ai.responses.queue}")
    private String aiResponsesQueue;
    
    @Value("${rabbitmq.queue.ai-enrich-conversation-response:ai.enrich.conversation.response.queue}")
    private String aiEnrichConversationResponseQueue;
    
    // Routing keys
    @Value("${rabbitmq.routing.categorize:ai.categorize}")
    private String categorizeRoutingKey;
    
    @Value("${rabbitmq.routing.summarize:ai.summarize}")
    private String summarizeRoutingKey;
    
    @Value("${rabbitmq.routing.enrich:ai.enrich}")
    private String enrichRoutingKey;
    
    @Value("${rabbitmq.routing.responses:ai.responses}")
    private String responsesRoutingKey;
    
    @Value("${rabbitmq.routing.enrich-conversation-response:ai.enrich.conversation.response}")
    private String enrichConversationResponseRoutingKey;
    
    // Message converter
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }
    
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter());
        factory.setConcurrentConsumers(3);
        factory.setMaxConcurrentConsumers(10);
        return factory;
    }
    
    // Exchanges
    @Bean
    public TopicExchange aiRequestsExchange() {
        return new TopicExchange(aiRequestsExchange, true, false);
    }
    
    @Bean
    public TopicExchange aiResponsesExchange() {
        return new TopicExchange(aiResponsesExchange, true, false);
    }
    
    // Queues
    @Bean
    public Queue aiRequestsQueue() {
        return QueueBuilder.durable(aiRequestsQueue)
                .withArgument("x-dead-letter-exchange", aiRequestsExchange + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead-letter")
                .build();
    }
    
    @Bean
    public Queue aiCategorizeQueue() {
        return QueueBuilder.durable(aiCategorizeQueue)
                .withArgument("x-dead-letter-exchange", aiRequestsExchange + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead-letter")
                .build();
    }
    
    @Bean
    public Queue aiSummarizeQueue() {
        return QueueBuilder.durable(aiSummarizeQueue)
                .withArgument("x-dead-letter-exchange", aiRequestsExchange + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead-letter")
                .build();
    }
    
    @Bean
    public Queue aiEnrichQueue() {
        return QueueBuilder.durable(aiEnrichQueue)
                .withArgument("x-dead-letter-exchange", aiRequestsExchange + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead-letter")
                .build();
    }
    
    @Bean
    public Queue aiResponsesQueue() {
        return QueueBuilder.durable(aiResponsesQueue).build();
    }
    
    @Bean
    public Queue aiEnrichConversationResponseQueue() {
        return QueueBuilder.durable(aiEnrichConversationResponseQueue).build();
    }
    
    // Dead letter queue
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(aiRequestsExchange + ".dlq").build();
    }
    
    @Bean
    public TopicExchange deadLetterExchange() {
        return new TopicExchange(aiRequestsExchange + ".dlx", true, false);
    }
    
    // Bindings
    @Bean
    public Binding categorizeBinding() {
        return BindingBuilder.bind(aiCategorizeQueue())
                .to(aiRequestsExchange())
                .with(categorizeRoutingKey);
    }
    
    @Bean
    public Binding summarizeBinding() {
        return BindingBuilder.bind(aiSummarizeQueue())
                .to(aiRequestsExchange())
                .with(summarizeRoutingKey);
    }
    
    @Bean
    public Binding enrichBinding() {
        return BindingBuilder.bind(aiEnrichQueue())
                .to(aiRequestsExchange())
                .with(enrichRoutingKey);
    }
    
    @Bean
    public Binding responsesBinding() {
        return BindingBuilder.bind(aiResponsesQueue())
                .to(aiResponsesExchange())
                .with(responsesRoutingKey);
    }
    
    @Bean
    public Binding enrichConversationResponseBinding() {
        return BindingBuilder.bind(aiEnrichConversationResponseQueue())
                .to(aiResponsesExchange())
                .with(enrichConversationResponseRoutingKey);
    }
    
    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with("dead-letter");
    }
    
    // Getters for queue names (used by listeners)
    public String getAiCategorizeQueue() { return aiCategorizeQueue; }
    public String getAiSummarizeQueue() { return aiSummarizeQueue; }
    public String getAiEnrichQueue() { return aiEnrichQueue; }
    public String getAiResponsesQueue() { return aiResponsesQueue; }
    public String getAiEnrichConversationResponseQueue() { return aiEnrichConversationResponseQueue; }
    public String getAiResponsesExchange() { return aiResponsesExchange; }
    public String getResponsesRoutingKey() { return responsesRoutingKey; }
    public String getEnrichConversationResponseRoutingKey() { return enrichConversationResponseRoutingKey; }
}
