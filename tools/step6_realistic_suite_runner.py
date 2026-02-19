#!/usr/bin/env python3
"""
Step 6 realistic test suite runner.

Goals:
  - Use *only* real users/channels/threads/text from Synthetic_Slack_Messages.csv for the main cases.
  - Add a few explicit edge cases (new channel/user, empty text, long message) clearly labeled.
  - Produce a client-readable JSON + Markdown report explaining: create vs update, target topicId, and what changed.

This runner assumes:
  - ai-model-routing-service is running and consuming STEP6_INPUT_TOPIC
  - it publishes Step 6 TopicMetadataEvent to STEP6_OUTPUT_TOPIC
  - Step 4 upserts topic_metadata into pgvector (optional, used here only if credentials are present)
"""

import csv
import json
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


KAFKA_CONTAINER = os.environ.get("LUCID_KAFKA_CONTAINER", "lucid-kafka-1")
BOOTSTRAP = os.environ.get("LUCID_KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.environ.get("STEP6_INPUT_TOPIC", "lucid-ingestion-messages-step6test")
TOPIC_OUT = os.environ.get("STEP6_OUTPUT_TOPIC", "ai-topic-metadata-step6test")

CSV_PATH = os.environ.get(
    "STEP6_CSV_PATH",
    "/home/maksym/sgh_works/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv",
)

WAIT_AFTER_PRODUCE_S = float(os.environ.get("STEP6_WAIT_AFTER_PRODUCE_S", "4"))
WAIT_BETWEEN_CREATE_UPDATE_S = float(os.environ.get("STEP6_WAIT_BETWEEN_CREATE_UPDATE_S", "12"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: List[str], *, input_text: Optional[str] = None, timeout_s: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        input=(input_text.encode("utf-8") if input_text is not None else None),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_s,
        check=False,
    )


def docker_exec(args: List[str], *, input_text: Optional[str] = None, timeout_s: int = 60) -> subprocess.CompletedProcess:
    return run(["docker", "exec", "-i", KAFKA_CONTAINER, *args], input_text=input_text, timeout_s=timeout_s)


def topic_message_count(topic: str) -> int:
    proc = docker_exec(
        ["kafka-run-class", "kafka.tools.GetOffsetShell", "--bootstrap-server", BOOTSTRAP, "--topic", topic],
        timeout_s=15,
    )
    out = proc.stdout.decode("utf-8", errors="replace").strip().splitlines()
    total = 0
    for line in out:
        parts = line.strip().split(":")
        if len(parts) >= 3 and parts[-1].isdigit():
            total += int(parts[-1])
    return total


def produce_events(events: List[Dict[str, Any]]) -> Tuple[int, str]:
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    proc = docker_exec(
        ["kafka-console-producer", "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC_IN],
        input_text=payload,
        timeout_s=45,
    )
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


def consume_step6_events_from_beginning(tenant_id: str, expected: int, *, timeout_s: int = 90) -> List[Dict[str, Any]]:
    max_messages = max(topic_message_count(TOPIC_OUT), expected)
    proc = docker_exec(
        [
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC_OUT,
            "--from-beginning",
            "--max-messages",
            str(max_messages),
        ],
        timeout_s=timeout_s,
    )
    stdout = proc.stdout.decode("utf-8", errors="replace").splitlines()
    consumed: List[Dict[str, Any]] = []
    for line in stdout:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue
        meta = evt.get("metadata")
        if not (isinstance(meta, dict) and str(meta.get("step")) == "6"):
            continue
        mids = evt.get("messageIds")
        if not (isinstance(mids, list) and mids and str(mids[0]).startswith(tenant_id)):
            continue
        consumed.append(evt)
        if len(consumed) >= expected:
            break
    return consumed


def safe_text(text: str, limit: int = 280) -> str:
    t = (text or "").replace("\n", " ").strip()
    return t if len(t) <= limit else (t[:limit] + "…")


def make_channel_id(channel_name: str) -> str:
    return "C_" + channel_name.lstrip("#").replace("-", "_").upper()


def message_id(tenant_id: str, workspace_id: str, channel_id: str, thread_ts: Optional[str], ts: str) -> str:
    thread_part = thread_ts.strip() if thread_ts and thread_ts.strip() else ts.strip()
    return f"{tenant_id.strip()}:{workspace_id.strip()}:{(channel_id or '').strip()}:{thread_part}:{ts.strip()}"


@dataclass
class CsvRow:
    channel: str
    user_name: str
    user_id: str
    thread_id: Optional[str]
    text: str


def load_n_rows(path: str, *, channel: str, thread_id: str, n: int) -> List[CsvRow]:
    rows: List[CsvRow] = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("channel") or "") != channel:
                continue
            tid = row.get("thread_id") or None
            if tid == "None":
                tid = None
            if tid != thread_id:
                continue
            rows.append(
                CsvRow(
                    channel=channel,
                    user_name=row.get("user_name") or "",
                    user_id=row.get("user_id") or "",
                    thread_id=tid,
                    text=row.get("text") or "",
                )
            )
            if len(rows) >= n:
                break
    if len(rows) < n:
        raise RuntimeError(f"Need {n} rows for channel={channel} thread_id={thread_id}, got {len(rows)}")
    return rows


@dataclass
class InputMessage:
    name: str
    tenant_id: str
    workspace_id: str
    channel_id: str
    channel_name: str
    thread_ts: Optional[str]
    ts: str
    text: str
    user: Dict[str, Any]

    def expected_message_id(self) -> str:
        return message_id(self.tenant_id, self.workspace_id, self.channel_id, self.thread_ts, self.ts)

    def to_event(self) -> Dict[str, Any]:
        msg: Dict[str, Any] = {
            "id": self.name,
            "ts": self.ts,
            "text": self.text,
            "source": "SLACK",
            "workspaceId": self.workspace_id,
            "channelId": self.channel_id,
            "channelName": self.channel_name,
            "threadTs": self.thread_ts,
        }
        return {"sourceType": "SLACK", "tenantId": self.tenant_id, "ingestedAt": utc_now_iso(), "message": msg, "user": self.user}


def summarize_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    meta = evt.get("metadata") if isinstance(evt.get("metadata"), dict) else {}
    topic = evt.get("topic") if isinstance(evt.get("topic"), dict) else {}
    return {
        "eventId": evt.get("eventId"),
        "topicId": evt.get("topicId"),
        "action": meta.get("action"),
        "reason": meta.get("reason"),
        "similarityScore": meta.get("similarityScore"),
        "title": topic.get("title"),
        "summary": safe_text(topic.get("summary", ""), 320),
        "participants": topic.get("participants") if isinstance(topic.get("participants"), list) else [],
        "actionItemsCount": len(topic.get("actionItems") or []) if isinstance(topic.get("actionItems"), list) else 0,
        "tags": topic.get("tags") if isinstance(topic.get("tags"), list) else [],
        "channel": topic.get("channel"),
    }


def load_pgvector_conn_from_env() -> Optional[Dict[str, str]]:
    url = os.environ.get("TOPIC_EMBEDDING_PGVECTOR_JDBC_URL", "")
    username = os.environ.get("TOPIC_EMBEDDING_PGVECTOR_USERNAME", "")
    password = os.environ.get("TOPIC_EMBEDDING_PGVECTOR_PASSWORD", "")
    if not (url and username and password):
        return None
    # Example: jdbc:postgresql://host:6543/postgres?sslmode=require
    m = re.match(r"^jdbc:postgresql://([^:/]+):(\d+)/([^?]+)\?(.*)$", url.strip())
    if not m:
        return None
    host, port, db, qs = m.group(1), m.group(2), m.group(3), m.group(4)
    ssl = "require" if "sslmode=require" in qs else "prefer"
    return {
        "conn": f"postgresql://{username}:{password}@{host}:{port}/{db}?sslmode={ssl}",
        "schema": os.environ.get("TOPIC_EMBEDDING_PGVECTOR_SCHEMA", "public"),
        "table": os.environ.get("TOPIC_EMBEDDING_PGVECTOR_TABLE", "topics_embeddings"),
    }


def fetch_topic_metadata_from_pgvector(topic_id: str, *, timeout_s: float = 20.0) -> Optional[Dict[str, Any]]:
    cfg = load_pgvector_conn_from_env()
    if not cfg:
        return None
    deadline = time.time() + timeout_s
    while True:
        sql = f"SELECT topic_metadata FROM {cfg['schema']}.{cfg['table']} WHERE topic_id::text = '{topic_id}';"
        proc = subprocess.run(
            ["psql", cfg["conn"], "-P", "pager=off", "-t", "-A", "-c", sql],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=30,
        )
        if proc.returncode == 0:
            raw = proc.stdout.strip()
            if raw:
                try:
                    return json.loads(raw)
                except Exception:
                    pass
        if time.time() >= deadline:
            return None
        time.sleep(2.0)


def diff_counts(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not before or not after:
        return {}
    def _len(v):
        return len(v) if isinstance(v, list) else 0
    return {
        "participants": {"before": _len(before.get("participants")), "after": _len(after.get("participants"))},
        "actionItems": {"before": _len(before.get("actionItems")), "after": _len(after.get("actionItems"))},
        "tags": {"before": _len(before.get("tags")), "after": _len(after.get("tags"))},
    }


def diff_sets(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]], key: str) -> Dict[str, Any]:
    if not before or not after:
        return {}
    b = set(before.get(key) or []) if isinstance(before.get(key), list) else set()
    a = set(after.get(key) or []) if isinstance(after.get(key), list) else set()
    return {"added": sorted(a - b), "removed": sorted(b - a)}


def diff_message_ids(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not before or not after:
        return {}
    b = before.get("messageIds") if isinstance(before.get("messageIds"), list) else []
    a = after.get("messageIds") if isinstance(after.get("messageIds"), list) else []
    bset, aset = set(map(str, b)), set(map(str, a))
    return {"beforeCount": len(b), "afterCount": len(a), "addedCount": len(aset - bset)}


def diff_topic_fields(prev_evt: Optional[Dict[str, Any]], curr_evt: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not prev_evt or not curr_evt:
        return {}
    p = prev_evt.get("topic") if isinstance(prev_evt.get("topic"), dict) else {}
    c = curr_evt.get("topic") if isinstance(curr_evt.get("topic"), dict) else {}
    prev_participants = set(p.get("participants") or []) if isinstance(p.get("participants"), list) else set()
    curr_participants = set(c.get("participants") or []) if isinstance(c.get("participants"), list) else set()
    prev_tags = set(p.get("tags") or []) if isinstance(p.get("tags"), list) else set()
    curr_tags = set(c.get("tags") or []) if isinstance(c.get("tags"), list) else set()

    def _tasks(topic_dict: Dict[str, Any]) -> set:
        items = topic_dict.get("actionItems")
        if not isinstance(items, list):
            return set()
        tasks = []
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("task"), str) and it.get("task").strip():
                tasks.append(it["task"].strip())
        return set(tasks)

    prev_tasks = _tasks(p)
    curr_tasks = _tasks(c)
    return {
        "participantsAdded": sorted(curr_participants - prev_participants),
        "tagsAdded": sorted(curr_tags - prev_tags),
        "actionItemsAdded": sorted(curr_tasks - prev_tasks),
        "counts": {
            "participants": {"before": len(prev_participants), "after": len(curr_participants)},
            "actionItems": {"before": len(prev_tasks), "after": len(curr_tasks)},
            "tags": {"before": len(prev_tags), "after": len(curr_tags)},
        },
    }


def main() -> int:
    run_id = f"step6_suite_{int(time.time())}"
    tenant_id = str(uuid.uuid4())
    base_workspace_id = os.environ.get("STEP6_WORKSPACE_ID", f"ws-{run_id}")
    base_ts = time.time()

    # Main realistic case: messages in the same thread_001 to show create + updates.
    t1 = load_n_rows(CSV_PATH, channel="#campaign-briefs", thread_id="thread_001", n=4)
    ws1 = f"{base_workspace_id}-case1"
    msgs_case1 = [
        InputMessage(
            name="case1_seed_create",
            tenant_id=tenant_id,
            workspace_id=ws1,
            channel_id=make_channel_id(t1[0].channel),
            channel_name=t1[0].channel,
            thread_ts=t1[0].thread_id,
            ts=f"{base_ts + 0.0:.6f}",
            text=t1[0].text.strip(),
            user={"displayName": t1[0].user_name, "slackUserId": t1[0].user_id},
        ),
        InputMessage(
            name="case1_followup_update_1",
            tenant_id=tenant_id,
            workspace_id=ws1,
            channel_id=make_channel_id(t1[1].channel),
            channel_name=t1[1].channel,
            thread_ts=t1[1].thread_id,
            ts=f"{base_ts + 2.0:.6f}",
            text=t1[1].text.strip(),
            user={"displayName": t1[1].user_name, "slackUserId": t1[1].user_id},
        ),
        InputMessage(
            name="case1_followup_update_2",
            tenant_id=tenant_id,
            workspace_id=ws1,
            channel_id=make_channel_id(t1[2].channel),
            channel_name=t1[2].channel,
            thread_ts=t1[2].thread_id,
            ts=f"{base_ts + 4.0:.6f}",
            text=t1[2].text.strip(),
            user={"displayName": t1[2].user_name, "slackUserId": t1[2].user_id},
        ),
        # Same channel, no threadTs: should try embedding match against the existing topic (not the same-thread fast path).
        InputMessage(
            name="case1_semantic_update_no_thread",
            tenant_id=tenant_id,
            workspace_id=ws1,
            channel_id=make_channel_id(t1[3].channel),
            channel_name=t1[3].channel,
            thread_ts=None,
            ts=f"{base_ts + 18.0:.6f}",
            text=t1[3].text.strip(),
            user={"displayName": t1[3].user_name, "slackUserId": t1[3].user_id},
        ),
    ]

    # Realistic create: a different thread in another channel.
    ws2 = f"{base_workspace_id}-case2"
    t2 = load_n_rows(CSV_PATH, channel="#client-communications", thread_id="thread_004", n=1)[0]
    msg_case2 = InputMessage(
        name="case2_create_new_thread",
        tenant_id=tenant_id,
        workspace_id=ws2,
        channel_id=make_channel_id(t2.channel),
        channel_name=t2.channel,
        thread_ts=t2.thread_id,
        ts=f"{base_ts + 20.0:.6f}",
        text=t2.text.strip(),
        user={"displayName": t2.user_name, "slackUserId": t2.user_id},
    )

    # Realistic create in an existing channel but unrelated text: should create a new topic (low similarity).
    ws5 = f"{base_workspace_id}-case5"
    msg_case5 = InputMessage(
        name="case5_realistic_unrelated_same_channel_create",
        tenant_id=tenant_id,
        workspace_id=ws5,
        channel_id=make_channel_id("#campaign-briefs"),
        channel_name="#campaign-briefs",
        thread_ts=None,
        ts=f"{base_ts + 24.0:.6f}",
        text="Heads-up: the staging website is down with a 502. Can someone restart the gateway and confirm when it’s back?",
        user={"displayName": "Leah", "slackUserId": "U003"},
    )

    # Edge: new channel (explicitly labeled).
    ws3 = f"{base_workspace_id}-case3"
    msg_case3 = InputMessage(
        name="case3_edge_new_channel_create",
        tenant_id=tenant_id,
        workspace_id=ws3,
        channel_id="C_NEW_CHANNEL",
        channel_name="#new-channel",
        thread_ts=None,
        ts=f"{base_ts + 30.0:.6f}",
        text="New channel kickoff: align on owners and next steps for vendor onboarding.",
        user={"displayName": "Devon", "slackUserId": "U001"},
    )

    # Edge: empty text (should be skipped, no output).
    ws4 = f"{base_workspace_id}-case4"
    msg_case4 = InputMessage(
        name="case4_edge_empty_text",
        tenant_id=tenant_id,
        workspace_id=ws4,
        channel_id=make_channel_id("#campaign-briefs"),
        channel_name="#campaign-briefs",
        thread_ts=None,
        ts=f"{base_ts + 40.0:.6f}",
        text="",
        user={"displayName": "Sam", "slackUserId": "U002"},
    )

    # Edge: new user in an existing channel with missing slackUserId (should still create a topic).
    ws6 = f"{base_workspace_id}-case6"
    msg_case6 = InputMessage(
        name="case6_edge_new_user_missing_slack_id",
        tenant_id=tenant_id,
        workspace_id=ws6,
        channel_id=make_channel_id("#project-updates"),
        channel_name="#project-updates",
        thread_ts=None,
        ts=f"{base_ts + 35.0:.6f}",
        text="Hi team, I’m joining the FitFusion rebrand stream — can someone share the latest timeline and owners?",
        user={"displayName": "New Joiner", "slackUserId": None},
    )

    # Edge: very long message (exercise prompt sizing + metadata extraction).
    ws7 = f"{base_workspace_id}-case7"
    long_text = (
        "Update on the GreenScape sustainability report: "
        + ("We need to reconcile the draft metrics with stakeholder feedback and confirm the narrative structure. " * 20).strip()
    )
    msg_case7 = InputMessage(
        name="case7_edge_long_message",
        tenant_id=tenant_id,
        workspace_id=ws7,
        channel_id=make_channel_id("#client-communications"),
        channel_name="#client-communications",
        thread_ts=None,
        ts=f"{base_ts + 44.0:.6f}",
        text=long_text,
        user={"displayName": "Devon", "slackUserId": "U001"},
    )

    scenarios = [
        {"id": "case1_realistic_create_then_updates", "workspaceId": ws1, "inputs": msgs_case1, "expected": 4, "type": "realistic"},
        {"id": "case2_realistic_create_other_channel", "workspaceId": ws2, "inputs": [msg_case2], "expected": 1, "type": "realistic"},
        {"id": "case5_realistic_unrelated_same_channel", "workspaceId": ws5, "inputs": [msg_case5], "expected": 1, "type": "realistic"},
        {"id": "case3_edge_new_channel", "workspaceId": ws3, "inputs": [msg_case3], "expected": 1, "type": "edge"},
        {"id": "case4_edge_empty_text_skipped", "workspaceId": ws4, "inputs": [msg_case4], "expected": 0, "type": "edge"},
        {"id": "case6_edge_new_user_missing_slack_id", "workspaceId": ws6, "inputs": [msg_case6], "expected": 1, "type": "edge"},
        {"id": "case7_edge_long_message", "workspaceId": ws7, "inputs": [msg_case7], "expected": 1, "type": "edge"},
    ]

    expected_total = sum(s["expected"] for s in scenarios)
    print(f"RunId: {run_id}")
    print(f"TenantId: {tenant_id}")
    print(f"CSV: {CSV_PATH}")
    print(f"Kafka: in={TOPIC_IN} out={TOPIC_OUT}")
    print(f"Expected Step6 events: {expected_total}")

    # Produce sequentially; wait between messages in scenario 1 so Step4 has time to upsert between updates.
    prod_err = ""
    for s in scenarios:
        for idx, msg in enumerate(s["inputs"]):
            rc, err = produce_events([msg.to_event()])
            prod_err += err
            if rc != 0:
                raise SystemExit(f"Failed to produce {msg.name}: {err}")
            if s["id"] == "case1_realistic_create_then_updates" and idx < len(s["inputs"]) - 1:
                time.sleep(WAIT_BETWEEN_CREATE_UPDATE_S)
            else:
                time.sleep(WAIT_AFTER_PRODUCE_S)

    # Snapshot topic and filter by tenant.
    time.sleep(3.0)
    consumed = consume_step6_events_from_beginning(tenant_id, expected_total, timeout_s=120)

    by_mid: Dict[str, Dict[str, Any]] = {}
    for evt in consumed:
        mids = evt.get("messageIds")
        if isinstance(mids, list) and mids:
            by_mid[str(mids[0])] = evt

    create_count = 0
    update_count = 0
    missing = 0

    scenario_reports: List[Dict[str, Any]] = []
    for s in scenarios:
        cases: List[Dict[str, Any]] = []
        last_topic_id: Optional[str] = None
        before_meta: Optional[Dict[str, Any]] = None
        prev_event_for_topic: Optional[Dict[str, Any]] = None
        for i, msg in enumerate(s["inputs"]):
            mid = msg.expected_message_id()
            evt = by_mid.get(mid)
            got = evt is not None
            step6 = summarize_event(evt) if evt else None
            if step6 and step6.get("action") == "create":
                create_count += 1
            if step6 and step6.get("action") == "update":
                update_count += 1
            if not got and s["expected"] > 0 and msg.text.strip():
                missing += 1

            topic_id = step6.get("topicId") if step6 else None
            after_meta = fetch_topic_metadata_from_pgvector(topic_id) if topic_id else None

            entry: Dict[str, Any] = {
                "name": msg.name,
                "fromTo": {"from": f"{msg.user.get('displayName')} ({msg.user.get('slackUserId')})", "to": msg.channel_name},
                "message": {
                    "workspaceId": msg.workspace_id,
                    "channelName": msg.channel_name,
                    "threadTs": msg.thread_ts,
                    "ts": msg.ts,
                    "text": msg.text,
                },
                "expectedMessageId": mid,
                "gotEvent": got,
                "step6": step6,
                "pgvectorRowFound": bool(after_meta) if topic_id else False,
            }

            # Include a compact diff for update cases (based on Step 6 outputs, so it's readable even without pgvector).
            if step6 and step6.get("action") == "create":
                last_topic_id = topic_id
                before_meta = after_meta
                prev_event_for_topic = evt
            if step6 and step6.get("action") == "update" and last_topic_id == topic_id:
                entry["topicDiff"] = {
                    "topicId": topic_id,
                    "fromStep6Event": diff_topic_fields(prev_event_for_topic, evt),
                    "fromPgvectorRow": {
                        "counts": diff_counts(before_meta, after_meta),
                        "participants": diff_sets(before_meta, after_meta, "participants"),
                        "tags": diff_sets(before_meta, after_meta, "tags"),
                        "messageIds": diff_message_ids(before_meta, after_meta),
                    },
                }
                before_meta = after_meta
                prev_event_for_topic = evt
            cases.append(entry)

        scenario_reports.append(
            {
                "scenarioId": s["id"],
                "type": s["type"],
                "workspaceId": s["workspaceId"],
                "expectedStep6Events": s["expected"],
                "cases": cases,
            }
        )

    report = {
        "runId": run_id,
        "startedAt": utc_now_iso(),
        "tenantId": tenant_id,
        "baseWorkspaceId": base_workspace_id,
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "inputs": {
            "totalMessagesSent": sum(len(s["inputs"]) for s in scenarios),
            "csvPath": CSV_PATH,
            "timing": {"waitAfterProduceSeconds": WAIT_AFTER_PRODUCE_S, "waitBetweenCreateUpdateSeconds": WAIT_BETWEEN_CREATE_UPDATE_S},
        },
        "outputs": {"observedStep6Events": len(consumed), "create": create_count, "update": update_count, "missing": missing},
        "scenarios": scenario_reports,
        "debug": {"producerStderr": (prod_err or "")[:2000]},
        "notes": {
            "what_update_means": "Update regenerates topic metadata (Step 3 prompt) for an existing topicId and re-publishes it as action=update.",
            "why_some_scores_null": "If update happened via same-thread matching (reason=same_thread), similarityScore may be null.",
        },
    }

    logs_dir = Path(__file__).resolve().parent.parent.parent / "deliverables" / "step6_delivery" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_json = logs_dir / f"{run_id}_report.json"
    out_md = logs_dir / f"{run_id}_summary.md"
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    # Markdown summary (client-readable)
    lines: List[str] = [
        f"# Step 6 realistic suite report ({run_id})",
        "",
        f"- Kafka input: `{TOPIC_IN}`",
        f"- Kafka output: `{TOPIC_OUT}`",
        f"- Observed: create={create_count}, update={update_count}, missing={missing}",
        "",
        "## Scenarios",
    ]
    for s in scenario_reports:
        lines.append(f"### {s['scenarioId']} ({s['type']})")
        for c in s["cases"]:
            msg = c["message"]
            who = c["fromTo"]["from"]
            ch = msg["channelName"]
            txt = safe_text(msg["text"], 140)
            step6 = c.get("step6") or {}
            action = step6.get("action")
            reason = step6.get("reason")
            tid = step6.get("topicId")
            lines.append(f"- {who} in {ch}: {txt}")
            if action:
                lines.append(f"  - Step6: `{action}` topicId=`{tid}` reason=`{reason}`")
                if c.get("topicDiff"):
                    d = c["topicDiff"].get("fromStep6Event") or {}
                    counts = d.get("counts") or {}
                    if counts:
                        lines.append(f"  - Counts: {counts}")
                    if d.get("participantsAdded"):
                        lines.append(f"  - Added participants: {d['participantsAdded']}")
                    if d.get("tagsAdded"):
                        lines.append(f"  - Added tags: {d['tagsAdded']}")
                    if d.get("actionItemsAdded"):
                        lines.append(f"  - Added action items: {d['actionItemsAdded']}")
            else:
                lines.append("  - Step6: (no output event)")
        lines.append("")

    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(f"Wrote JSON: {out_json}")
    print(f"Wrote MD:   {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
