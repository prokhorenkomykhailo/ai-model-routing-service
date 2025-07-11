# Message Enrichment Prompt

Analyze this message and provide enrichment data:

Message: "%s"
Context: %s

Provide response in format:
Category: [category]
Sentiment: [sentiment_score between -1 and 1]
Intent: [user intent]
Entities: [comma-separated entities]
Confidence: [confidence_score between 0 and 1]
