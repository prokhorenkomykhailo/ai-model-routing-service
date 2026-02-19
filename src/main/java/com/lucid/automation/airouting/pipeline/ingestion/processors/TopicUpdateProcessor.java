package com.lucid.automation.airouting.pipeline.ingestion.processors;

import com.lucid.automation.airouting.config.TopicUpdateProperties;
import com.lucid.automation.airouting.pipeline.ProcessingResult;
import com.lucid.automation.airouting.pipeline.ingestion.IngestionProcessingContext;
import com.lucid.automation.airouting.pipeline.ingestion.MessageProcessor;
import com.lucid.automation.airouting.service.TopicUpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TopicUpdateProcessor implements MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TopicUpdateProcessor.class);

    private final TopicUpdateProperties properties;
    private final TopicUpdateManager manager;

    public TopicUpdateProcessor(TopicUpdateProperties properties, TopicUpdateManager manager) {
        this.properties = properties;
        this.manager = manager;
    }

    @Override
    public ProcessingResult process(IngestionProcessingContext context) {
        if (!properties.isEnabled()) {
            return ProcessingResult.success(getProcessorName());
        }
        try {
            manager.handleNewMessage(context.getIngestionEvent());
            return ProcessingResult.success(getProcessorName());
        } catch (Exception e) {
            // Best-effort: don't block ingestion if topic update fails.
            logger.warn("Step6 topic update failed for messageId={}: {}", context.getMessageId(), e.getMessage());
            return ProcessingResult.success(getProcessorName());
        }
    }

    @Override
    public String getProcessorName() {
        return "TopicUpdateProcessor";
    }

    @Override
    public int getOrder() {
        return 45;
    }
}
