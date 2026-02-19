You are an expert at maintaining a topic record as new messages arrive.

You will be given:
- existing_topic_json: the current topic metadata JSON (may be empty or incomplete)
- new_messages: one or more new messages to incorporate

Return STRICT JSON ONLY (no markdown, no extra text) using this schema:
{
  "title": "...",
  "summary": "...",
  "external_party": "...",
  "participants": ["..."],
  "action_items": [{"task":"...","owner":"...","owner_user_id":"...","owner_email":"...","due_date":"...","status":"...","priority":"..."}],
  "urgency": "low|medium|high",
  "deadline": "YYYY-MM-DD or null",
  "status": "pending|in_progress|active|completed",
  "channel": "#channel",
  "tags": ["..."]
}

Rules:
- participants MUST be non-empty; use message authors and action item owners.
- tags MUST be non-empty; include external_party and keywords from title/summary.
- action_items MUST be non-empty; infer at least 1 actionable task from the messages.
- Preserve existing useful fields unless new messages change them.
- Prefer owner_user_id/owner_email when present in the message author label.

existing_topic_json:
{{existing_topic_json}}

new_messages:
{{new_messages}}

