#!/usr/bin/env python3
"""
Step 6 update demo runner (CSV-backed).

Uses real channel/user/thread/text rows from Synthetic_Slack_Messages.csv and
forces an update by sending the same message text twice in the same workspace/channel.
"""

import csv
import json
import os
import select
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


KAFKA_CONTAINER = os.environ.get("LUCID_KAFKA_CONTAINER", "lucid-kafka-1")
BOOTSTRAP = os.environ.get("LUCID_KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.environ.get("STEP6_INPUT_TOPIC", "lucid-ingestion-messages")
TOPIC_OUT = os.environ.get("STEP6_OUTPUT_TOPIC", "ai-topic-metadata")

CSV_PATH = os.environ.get(
    "STEP6_CSV_PATH",
    "/home/maksym/sgh_works/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv",
)

WAIT_FOR_EMBEDDINGS_S = float(os.environ.get("STEP6_WAIT_FOR_EMBEDDINGS_S", "12"))


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


def message_id(tenant_id: str, workspace_id: str, channel_id: str, thread_ts: Optional[str], ts: str) -> str:
    thread_part = thread_ts.strip() if thread_ts and thread_ts.strip() else ts.strip()
    return f"{tenant_id.strip()}:{workspace_id.strip()}:{(channel_id or '').strip()}:{thread_part}:{ts.strip()}"


def safe_text(text: str, limit: int = 500) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text if len(text) <= limit else (text[:limit] + "…")


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


def produce_events(events: List[Dict[str, Any]]) -> Tuple[int, str]:
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    cmd = ["kafka-console-producer", "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC_IN]
    proc = docker_exec(cmd, input_text=payload, timeout_s=30)
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


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
        "actionItems": topic.get("actionItems") if isinstance(topic.get("actionItems"), list) else [],
        "channel": topic.get("channel"),
        "tags": topic.get("tags") if isinstance(topic.get("tags"), list) else [],
    }


@dataclass
class CsvRow:
    channel: str
    user_name: str
    user_id: str
    timestamp: str
    thread_id: Optional[str]
    text: str


def load_seed_rows(path: str) -> List[CsvRow]:
    wanted_channels = {"#campaign-briefs", "#client-communications", "#project-updates"}
    by_channel: Dict[str, CsvRow] = {}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ch = row.get("channel") or ""
            if ch not in wanted_channels or ch in by_channel:
                continue
            by_channel[ch] = CsvRow(
                channel=ch,
                user_name=row.get("user_name") or "",
                user_id=row.get("user_id") or "",
                timestamp=row.get("timestamp") or "",
                thread_id=(row.get("thread_id") or None) if (row.get("thread_id") or "") != "None" else None,
                text=row.get("text") or "",
            )
            if len(by_channel) == 3:
                break
    if len(by_channel) < 3:
        raise RuntimeError(f"CSV did not contain all expected channels: {wanted_channels}")
    return [by_channel[c] for c in sorted(by_channel.keys())]


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


def build_cases_from_csv(rows: List[CsvRow], tenant_id: str, base_workspace_id: str) -> Tuple[List[InputMessage], List[InputMessage]]:
    base_ts = time.time()
    initial: List[InputMessage] = []
    followup: List[InputMessage] = []
    for i, row in enumerate(rows):
        user = {"displayName": row.user_name, "slackUserId": row.user_id}
        text = row.text.strip()
        channel_id = "C_" + row.channel.lstrip("#").replace("-", "_").upper()
        workspace_id = f"{base_workspace_id}-s{i+1}"
        initial.append(
            InputMessage(
                name=f"csv_create_{i+1}",
                tenant_id=tenant_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                channel_name=row.channel,
                thread_ts=row.thread_id,
                ts=f"{base_ts + i * 5.0:.6f}",
                text=text,
                user=user,
            )
        )
        followup.append(
            InputMessage(
                name=f"csv_update_{i+1}",
                tenant_id=tenant_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                channel_name=row.channel,
                thread_ts=row.thread_id,
                ts=f"{base_ts + i * 5.0 + 2.0:.6f}",
                text=text,
                user=user,
            )
        )
    return initial, followup


def main() -> int:
    run_id = f"step6_update_csv_{int(time.time())}"
    tenant_id = str(uuid.uuid4())
    base_workspace_id = os.environ.get("STEP6_WORKSPACE_ID", f"ws-{run_id}")
    group_id = f"ai-service-group-{run_id}"

    rows = load_seed_rows(CSV_PATH)
    initial, followup = build_cases_from_csv(rows, tenant_id, base_workspace_id)
    all_inputs = initial + followup
    expected = len(all_inputs)

    print(f"RunId: {run_id}")
    print(f"CSV: {CSV_PATH}")
    print(f"Base workspaceId: {base_workspace_id}")
    print(f"Expected Step 6 events: {expected} (create={len(initial)} then update={len(followup)})")
    sys.stdout.flush()

    consumer_cmd = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC_OUT,
        "--group",
        group_id,
        "--consumer-property",
        "auto.offset.reset=latest",
    ]
    consumer = subprocess.Popen(consumer_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    wait_for_group_ready(group_id, TOPIC_OUT, timeout_s=10.0)

    prod_err = ""
    prod_err2 = ""
    for create_msg, update_msg in zip(initial, followup):
        rc, err = produce_events([create_msg.to_event()])
        prod_err += err
        if rc != 0:
            consumer.terminate()
            print("ERROR: failed to produce initial message", file=sys.stderr)
            print(err, file=sys.stderr)
            return 2

        time.sleep(WAIT_FOR_EMBEDDINGS_S)

        rc2, err2 = produce_events([update_msg.to_event()])
        prod_err2 += err2
        if rc2 != 0:
            consumer.terminate()
            print("ERROR: failed to produce follow-up message", file=sys.stderr)
            print(err2, file=sys.stderr)
            return 2

    consumed: List[Dict[str, Any]] = []
    deadline = time.time() + 90.0
    while time.time() < deadline:
        if consumer.poll() is not None:
            break
        if consumer.stdout is None:
            break
        rlist, _, _ = select.select([consumer.stdout], [], [], 0.25)
        if not rlist:
            continue
        line = consumer.stdout.readline()
        if not line:
            continue
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

    try:
        consumer.terminate()
        consumer.wait(timeout=5)
    except Exception:
        try:
            consumer.kill()
        except Exception:
            pass

    by_msgid: Dict[str, Dict[str, Any]] = {}
    for evt in consumed:
        mids = evt.get("messageIds")
        if isinstance(mids, list) and mids:
            by_msgid[str(mids[0])] = evt

    cases: List[Dict[str, Any]] = []
    create_count = 0
    update_count = 0
    missing = 0

    for msg in all_inputs:
        mid = msg.expected_message_id()
        evt = by_msgid.get(mid)
        if not evt:
            missing += 1
        step6 = summarize_step6_event(evt) if evt else None
        if step6 and step6.get("action") == "create":
            create_count += 1
        if step6 and step6.get("action") == "update":
            update_count += 1
        cases.append(
            {
                "name": msg.name,
                "phase": "initial_create" if msg in initial else "followup_update",
                "fromTo": {"from": f"{msg.user.get('displayName')} ({msg.user.get('slackUserId')})", "to": msg.channel_name},
                "message": {
                    "workspaceId": msg.workspace_id,
                    "channelName": msg.channel_name,
                    "threadTs": msg.thread_ts,
                    "ts": msg.ts,
                    "text": msg.text,
                },
                "user": msg.user,
                "expectedMessageId": mid,
                "gotEvent": evt is not None,
                "step6": step6,
            }
        )

    report = {
        "runId": run_id,
        "startedAt": utc_now_iso(),
        "tenantId": tenant_id,
        "baseWorkspaceId": base_workspace_id,
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "consumerGroup": group_id,
        "inputs": {"total": len(all_inputs), "initialCreates": len(initial), "followupUpdates": len(followup), "csvPath": CSV_PATH},
        "outputs": {"observedStep6Events": len(consumed), "create": create_count, "update": update_count, "missing": missing},
        "notes": {
            "why_text_repeated": "Follow-up messages reuse the same CSV message text to make the update path deterministic in demo.",
            "channels_users_source": "All channel/user/thread/text values are taken from the CSV dataset.",
        },
        "cases": cases,
        "debug": {"producerStderr": (prod_err or "")[:2000], "producerStderr2": (prod_err2 or "")[:2000]},
    }

    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"{run_id}_report.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote report: {out_path}")
    print(f"Observed step6 events: {len(consumed)} | create={create_count} update={update_count} missing={missing}")
    return 0 if missing == 0 and update_count > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
