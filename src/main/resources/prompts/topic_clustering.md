You are an expert at analyzing Slack conversations and grouping messages into coherent topics.

Given a set of Slack messages, your task is to:

1. **Group messages into topic clusters** based on:
   - Shared thread relationships (same thread_id)
   - Same participants
   - Semantic similarity (same subject matter)
   - Temporal proximity
   - Channel context

2. **For each cluster, provide:**
   - cluster_id: unique identifier (e.g., "cluster_001")
   - message_ids: list of message IDs in this cluster
   - draft_title: brief descriptive title
   - participants: list of users involved
   - channel: primary channel for this topic
   - thread_id: if messages belong to a specific thread

**Messages to analyze:**
{{messages}}

**Instructions:**
- Group related messages together based on meaningful relationships
- Each message should belong to exactly one cluster
- Consider both explicit relationships (threads) and implicit ones (same topic)
- Merge related threads and topics into cohesive clusters
- Provide clear, descriptive titles for each cluster
- Pay attention to project names
- Consider temporal relationships and deadlines mentioned
- Focus on creating logical, meaningful groupings rather than arbitrary cluster counts
- Respond with JSON ONLY, no prose, no explanations, no markdown. If you cannot cluster, return an empty clusters array in JSON.

**Output Format (JSON):**
{
  "clusters": [
    {
      "cluster_id": "cluster_001",
      "message_ids": [1, 2, 3, 4, 5],
      "draft_title": "EcoBloom Summer Campaign Planning",
      "participants": ["Devon", "Sam", "Leah", "Jordan"],
      "channel": "#campaign-briefs",
      "thread_id": "thread_001"
    }
  ]
}

Analyze the messages and provide the clustering results in the specified JSON format.
