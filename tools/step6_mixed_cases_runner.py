#!/usr/bin/env python3
"""
Step 6 mixed test runner.

Creates a small, client-readable report that mixes:
  - realistic Slack messages from the synthetic CSV dataset
  - edge cases (new user / new channel / empty text)

Important:
  - Uses isolated workspaces per scenario to keep results deterministic.
  - Consumes Step 6 output events from Kafka (topic-metadata) and writes a JSON report.
"""

import csv
import json
import os
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

WAIT_FOR_EMBEDDINGS_S = float(os.environ.get("STEP6_WAIT_FOR_EMBEDDINGS_S", "5"))


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


def produce_events(events: List[Dict[str, Any]]) -> Tuple[int, str]:
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    cmd = ["kafka-console-producer", "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC_IN]
    proc = docker_exec(cmd, input_text=payload, timeout_s=30)
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


def wait_for_group_ready(group_id: str, topic: str, timeout_s: float = 8.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = docker_exec(
            ["kafka-consumer-groups", "--bootstrap-server", BOOTSTRAP, "--group", group_id, "--describe"],
            timeout_s=5,
        )
        out = proc.stdout.decode("utf-8", errors="replace")
        if proc.returncode == 0 and topic in out:
            return
        time.sleep(0.4)


def safe_text(text: str, limit: int = 320) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text if len(text) <= limit else (text[:limit] + "…")


def message_id(tenant_id: str, workspace_id: str, channel_id: str, thread_ts: Optional[str], ts: str) -> str:
    thread_part = thread_ts.strip() if thread_ts and thread_ts.strip() else ts.strip()
    return f"{tenant_id.strip()}:{workspace_id.strip()}:{(channel_id or '').strip()}:{thread_part}:{ts.strip()}"


def summarize_step6_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    meta = evt.get("metadata") if isinstance(evt.get("metadata"), dict) else {}
    topic = evt.get("topic") if isinstance(evt.get("topic"), dict) else {}
    return {
        "eventId": evt.get("eventId"),
        "topicId": evt.get("topicId"),
        "action": meta.get("action"),
        "reason": meta.get("reason"),
        "similarityScore": meta.get("similarityScore"),
        "title": topic.get("title"),
        "summary": safe_text(topic.get("summary", ""), 260),
        "externalParty": topic.get("externalParty") or topic.get("external_party"),
        "participants": topic.get("participants") if isinstance(topic.get("participants"), list) else [],
        "actionItemsCount": len(topic.get("actionItems") or []) if isinstance(topic.get("actionItems"), list) else 0,
        "channel": topic.get("channel"),
        "tags": topic.get("tags") if isinstance(topic.get("tags"), list) else [],
    }


@dataclass
class CsvRow:
    channel: str
    user_name: str
    user_id: str
    thread_id: Optional[str]
    text: str


def load_two_rows_same_thread(path: str, *, channel: str, thread_id: str) -> Tuple[CsvRow, CsvRow]:
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
            if len(rows) >= 2:
                break
    if len(rows) < 2:
        raise RuntimeError(f"Could not find 2 rows for channel={channel} thread_id={thread_id}")
    return rows[0], rows[1]


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


def make_channel_id(channel_name: str) -> str:
    return "C_" + channel_name.lstrip("#").replace("-", "_").upper()


def topic_message_count(topic: str) -> int:
    proc = docker_exec(
        ["kafka-run-class", "kafka.tools.GetOffsetShell", "--bootstrap-server", BOOTSTRAP, "--topic", topic],
        timeout_s=15,
    )
    out = proc.stdout.decode("utf-8", errors="replace").strip().splitlines()
    total = 0
    for line in out:
        # format: topic:partition:offset
        parts = line.strip().split(":")
        if len(parts) >= 3 and parts[-1].isdigit():
            total += int(parts[-1])
    return total


def consume_step6_events_from_beginning(tenant_id: str, expected: int, *, timeout_s: int = 60) -> List[Dict[str, Any]]:
    """
    Consume the whole topic from the beginning and filter by tenantId+step=6.

    This is intentionally simple and robust for local demos/tests where the topic size is small,
    and avoids consumer-group timing/offset issues that can miss events.
    """
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


def main() -> int:
    run_id = f"step6_mixed_{int(time.time())}"
    tenant_id = str(uuid.uuid4())
    base_workspace_id = os.environ.get("STEP6_WORKSPACE_ID", f"ws-{run_id}")
    group_id = f"ai-service-group-{run_id}"
    base_ts = time.time()

    # Scenario 1: realistic update (same thread, 2 different messages from CSV)
    s1a, s1b = load_two_rows_same_thread(CSV_PATH, channel="#campaign-briefs", thread_id="thread_001")
    ws1 = f"{base_workspace_id}-case1"
    m1 = InputMessage(
        name="case1_create",
        tenant_id=tenant_id,
        workspace_id=ws1,
        channel_id=make_channel_id(s1a.channel),
        channel_name=s1a.channel,
        thread_ts=s1a.thread_id,
        ts=f"{base_ts + 0.0:.6f}",
        text=s1a.text.strip(),
        user={"displayName": s1a.user_name, "slackUserId": s1a.user_id},
    )
    m2 = InputMessage(
        name="case1_update",
        tenant_id=tenant_id,
        workspace_id=ws1,
        channel_id=make_channel_id(s1b.channel),
        channel_name=s1b.channel,
        thread_ts=s1b.thread_id,
        ts=f"{base_ts + 2.0:.6f}",
        text=s1b.text.strip(),
        user={"displayName": s1b.user_name, "slackUserId": s1b.user_id},
    )

    # Scenario 2: new channel (create only)
    ws2 = f"{base_workspace_id}-case2"
    m3 = InputMessage(
        name="case2_new_channel_create",
        tenant_id=tenant_id,
        workspace_id=ws2,
        channel_id="C_NEW_CHANNEL",
        channel_name="#new-channel",
        thread_ts=None,
        ts=f"{base_ts + 10.0:.6f}",
        text="New channel kickoff: let's align on owners and next steps for vendor onboarding.",
        user={"displayName": "Devon", "slackUserId": "U001"},
    )

    # Scenario 3: new user in an existing channel (create only)
    ws3 = f"{base_workspace_id}-case3"
    m4 = InputMessage(
        name="case3_new_user_create",
        tenant_id=tenant_id,
        workspace_id=ws3,
        channel_id=make_channel_id("#project-updates"),
        channel_name="#project-updates",
        thread_ts="thread_007",
        ts=f"{base_ts + 20.0:.6f}",
        text="Joining late: can someone confirm the latest decision on FitFusion tagline so I can update the deck?",
        user={"displayName": "Avery", "slackUserId": "U999"},
    )

    # Scenario 4: empty text (expect no Step6 output)
    ws4 = f"{base_workspace_id}-case4"
    m5 = InputMessage(
        name="case4_empty_text",
        tenant_id=tenant_id,
        workspace_id=ws4,
        channel_id=make_channel_id("#client-communications"),
        channel_name="#client-communications",
        thread_ts="thread_004",
        ts=f"{base_ts + 30.0:.6f}",
        text="",
        user={"displayName": "Sam", "slackUserId": "U002"},
    )

    scenarios = [
        {"id": "case1_update_existing_topic", "workspaceId": ws1, "inputs": [m1, m2], "expectedStep6Events": 2},
        {"id": "case2_new_channel_create", "workspaceId": ws2, "inputs": [m3], "expectedStep6Events": 1},
        {"id": "case3_new_user_create", "workspaceId": ws3, "inputs": [m4], "expectedStep6Events": 1},
        {"id": "case4_empty_text_skipped", "workspaceId": ws4, "inputs": [m5], "expectedStep6Events": 0},
    ]

    expected_total = sum(s["expectedStep6Events"] for s in scenarios)
    print(f"RunId: {run_id}")
    print(f"TenantId: {tenant_id}")
    print(f"CSV: {CSV_PATH}")
    print(f"Kafka: in={TOPIC_IN} out={TOPIC_OUT}")
    print(f"Expected Step6 events: {expected_total}")

    # Produce messages (sequential, with a small wait between create/update for scenario 1).
    prod_err_all = ""
    for s in scenarios:
        msgs: List[InputMessage] = s["inputs"]
        for idx, msg in enumerate(msgs):
            rc, err = produce_events([msg.to_event()])
            prod_err_all += err
            if rc != 0:
                try:
                    consumer.terminate()
                except Exception:
                    pass
                raise SystemExit(f"Failed to produce {msg.name}: {err}")
            # only wait between create->update in case 1
            if s["id"] == "case1_update_existing_topic" and idx == 0:
                time.sleep(WAIT_FOR_EMBEDDINGS_S)

    # Give the service a moment to publish to Kafka before we snapshot the topic.
    time.sleep(3.0)
    consumed = consume_step6_events_from_beginning(tenant_id, expected_total, timeout_s=90)

    by_msgid: Dict[str, Dict[str, Any]] = {}
    for evt in consumed:
        mids = evt.get("messageIds")
        if isinstance(mids, list) and mids:
            by_msgid[str(mids[0])] = evt

    scenario_reports: List[Dict[str, Any]] = []
    create_count = 0
    update_count = 0
    missing = 0

    for s in scenarios:
        case_entries: List[Dict[str, Any]] = []
        for msg in s["inputs"]:
            mid = msg.expected_message_id()
            evt = by_msgid.get(mid)
            got = evt is not None
            if not got and s["expectedStep6Events"] > 0 and msg.text.strip():
                missing += 1
            step6 = summarize_step6_event(evt) if evt else None
            if step6 and step6.get("action") == "create":
                create_count += 1
            if step6 and step6.get("action") == "update":
                update_count += 1
            case_entries.append(
                {
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
                }
            )

        scenario_reports.append(
            {
                "scenarioId": s["id"],
                "workspaceId": s["workspaceId"],
                "expectedStep6Events": s["expectedStep6Events"],
                "cases": case_entries,
            }
        )

    report = {
        "runId": run_id,
        "startedAt": utc_now_iso(),
        "tenantId": tenant_id,
        "baseWorkspaceId": base_workspace_id,
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "consumerGroup": group_id,
        "inputs": {"csvPath": CSV_PATH, "waitForEmbeddingsSeconds": WAIT_FOR_EMBEDDINGS_S},
        "outputs": {"observedStep6Events": len(consumed), "create": create_count, "update": update_count, "missing": missing},
        "scenarios": scenario_reports,
        "debug": {"producerStderr": (prod_err_all or "")[:2000]},
    }

    logs_dir = Path(__file__).resolve().parent.parent.parent / "deliverables" / "step6_delivery" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"{run_id}_report.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    # Also write a short markdown summary.
    md_lines = [
        f"# Step 6 mixed test report ({run_id})",
        "",
        f"- CSV source: `{CSV_PATH}`",
        f"- Kafka input: `{TOPIC_IN}`",
        f"- Kafka output: `{TOPIC_OUT}`",
        f"- Observed: create={create_count}, update={update_count}, missing={missing}",
        "",
        "## Scenarios",
    ]
    for s in scenario_reports:
        md_lines.append(f"### {s['scenarioId']} ({s['workspaceId']})")
        for c in s["cases"]:
            step6 = c.get("step6") or {}
            msg = c["message"]
            who = c["fromTo"]["from"]
            ch = msg["channelName"]
            txt = safe_text(msg["text"], 140)
            action = step6.get("action")
            reason = step6.get("reason")
            score = step6.get("similarityScore")
            tid = step6.get("topicId")
            md_lines.append(f"- {who} in {ch}: {txt}")
            if action:
                md_lines.append(f"  - Step6: `{action}` topicId=`{tid}` score=`{score}` reason=`{reason}`")
            else:
                md_lines.append("  - Step6: (no output event)")
        md_lines.append("")
    md_path = logs_dir / f"{run_id}_summary.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"Wrote JSON: {out_path}")
    print(f"Wrote MD:   {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
