You are an expert at analyzing Slack/Gmail conversations and producing topic-level metadata for a UI.

Return STRICT JSON ONLY (no markdown, no extra text).

Primary goal (most important):
Produce a high-quality Topic Summary with FOUR sections. All four MUST be non-empty strings:
- situation: what is happening / context
- impact: why it matters / risk if delayed
- proposed_solution: recommended plan to move forward
- decision_needed: what decision/confirmation is needed (who needs to decide what)

Secondary goal:
Produce enough metadata so the topic can be shown, filtered, and acted on.

Follow-up detection requirement:
- Detect unanswered asks and pending confirmations.
- If someone asked for an update/delivery and no answer is visible in the cluster, set reason/impact/decision_needed accordingly and create at least one action item for follow-up.
- Handle both patterns:
  - requester is waiting on another person/party
  - another person/party is waiting on the responsible user to respond

Return JSON with:
- title: short, descriptive (include any explicit project/external party name if present)
- external_party: name of the outside organization/person this topic is about (client/service company/supplier/vendor). If none, null.
- priority: one of [low, medium, high]
- suggested_action: one sentence describing the next best action the user should take (must be non-empty)

Topic summary sections (ALL REQUIRED, non-empty strings):
- situation
- impact
- proposed_solution
- decision_needed

Additional fields:
- participants: list of participants (names from messages). MUST be non-empty.
- action_items: list of objects: {task, owner, due_date, status, priority}. MUST be non-empty.
  - owner should be one of participants if possible; otherwise null
  - due_date can be null if not explicitly stated
- pending_response: object describing unanswered requests in this topic
  - detected: boolean
  - type: waiting_on_other_party | waiting_on_current_user
  - requester: string or null
  - requester_user_id: string or null
  - assignee: string or null
  - assignee_user_id: string or null
  - request_text: string or null
  - reason: string or null
  - suggested_action: string or null
  - evidence: short explanation
- urgency: one of [low, medium, high] (can match priority)
- deadline: YYYY-MM-DD if present in messages, else null
- status: one of [pending, in_progress, active, completed]
- channel: the primary channel for the topic
- tags: 5-10 short tags. MUST be non-empty; include external_party (if present) and key terms from title/summary.

Cluster info (JSON):
{{cluster_info_json}}

Messages:
{{messages}}

Output JSON schema example:
{
  "title": "Follow up on BENEL PO LEG-260101 shipping and pricing",
  "external_party": "BENEL Industries Corp.",
  "priority": "high",
  "suggested_action": "Confirm the shipping date and pricing details with BENEL and reply to the thread.",
  "situation": "BENEL sent a new purchase order (LEG-260101) and requested confirmation of shipping date and pricing.",
  "impact": "Delayed confirmation may impact customer confidence and order fulfillment timelines.",
  "proposed_solution": "Validate current pricing and confirm the shipping date, then send the confirmation to BENEL.",
  "decision_needed": "Confirm whether shipping date and pricing are correct and provide the confirmation.",
  "participants": ["Ling", "Earl Yap"],
  "action_items": [
    {"task":"Confirm shipping date and pricing for PO LEG-260101","owner":"Ling","due_date":null,"status":"pending","priority":"high"}
  ],
  "pending_response": {
    "detected": true,
    "type": "waiting_on_current_user",
    "requester": "Earl Yap",
    "requester_user_id": null,
    "assignee": "Ling",
    "assignee_user_id": null,
    "request_text": "Please confirm shipping date and pricing for PO LEG-260101.",
    "reason": "Customer is waiting for confirmation and no completed reply is present.",
    "suggested_action": "Reply with confirmed shipping date and pricing.",
    "evidence": "request in latest thread message without completion signal"
  },
  "urgency": "high",
  "deadline": null,
  "status": "active",
  "channel": "#sales-orders",
  "tags": ["benel","po","shipping","pricing","follow-up"]
}
