You are an expert at analyzing Slack conversations and producing topic-level metadata.

Given a topic cluster and its messages, return STRICT JSON ONLY (no markdown, no extra text) with:

- title: short, descriptive (use any explicit project/client/vendor name if present)
- summary: 2-4 sentences, concrete details (decisions, deadlines, numbers)
- external_party: name of the outside organization/person this topic is about (client/service company/supplier/vendor). If none, null.
- participants: list of participants (names from messages). MUST be non-empty; if unsure, include message authors and/or action item owners.
- action_items: list of objects: {task, owner, due_date, status, priority}
- urgency: one of [low, medium, high]
- deadline: main deadline in YYYY-MM-DD if present, else null
- status: one of [pending, in_progress, active, completed]
- channel: the primary channel for the topic
- tags: 5-10 short tags. MUST be non-empty; include external_party and a few keywords from the title/summary.

Cluster info (JSON):
{{cluster_info_json}}

Messages:
{{messages}}

Output JSON schema example:
{
  "title": "EcoBloom Summer Campaign Planning",
  "summary": "...",
  "external_party": "EcoBloom",
  "participants": ["Devon", "Sam"],
  "action_items": [{"task":"...","owner":"@devon","due_date":"2025-07-10","status":"pending","priority":"high"}],
  "urgency": "high",
  "deadline": "2025-07-28",
  "status": "active",
  "channel": "#campaign-briefs",
  "tags": ["ecobloom","campaign","planning"]
}
