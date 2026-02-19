#!/usr/bin/env python3
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

CSV_PATH_ENV = os.environ.get("STEP6_CSV_PATH")
CSV_LIMIT_ENV = os.environ.get("STEP6_CSV_LIMIT")


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
    if text is None:
        return ""
    text = str(text)
    return text if len(text) <= limit else (text[:limit] + "…")


def detect_csv_path() -> str:
    candidates = [
        CSV_PATH_ENV,
        "/home/maksym/sgh_works/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv",
        "/home/maksym/vertex-set/deemerge/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv",
    ]
    for c in candidates:
        if not c:
            continue
        p = Path(c)
        if p.exists() and p.is_file():
            return str(p)
    raise FileNotFoundError("Synthetic Slack CSV not found; set STEP6_CSV_PATH")


def produce_events(events: List[Dict[str, Any]]) -> Tuple[int, str]:
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    cmd = [
        "kafka-console-producer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC_IN,
    ]
    proc = docker_exec(cmd, input_text=payload, timeout_s=45)
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


def summarize_step6_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    meta = evt.get("metadata") if isinstance(evt.get("metadata"), dict) else {}
    topic = evt.get("topic") if isinstance(evt.get("topic"), dict) else {}
    action_items = topic.get("actionItems") if isinstance(topic.get("actionItems"), list) else []
    participants = topic.get("participants") if isinstance(topic.get("participants"), list) else []
    return {
        "topicId": evt.get("topicId"),
        "action": meta.get("action"),
        "reason": meta.get("reason"),
        "similarityScore": meta.get("similarityScore"),
        "title": topic.get("title"),
        "summary": safe_text(topic.get("summary", ""), 300),
        "participants": participants[:10],
        "actionItemsCount": len(action_items),
    }


@dataclass
class TestInput:
    name: str
    source_type: str
    tenant_id: str
    workspace_id: str
    channel_id: str
    channel_name: str
    thread_ts: Optional[str]
    ts: str
    text: str
    user: Dict[str, Any]
    extra: Dict[str, Any]

    def expected_message_id(self) -> str:
        return message_id(self.tenant_id, self.workspace_id, self.channel_id, self.thread_ts, self.ts)

    def to_event(self) -> Dict[str, Any]:
        message: Dict[str, Any] = {
            "id": self.extra.get("id") or self.name,
            "ts": self.ts,
            "text": self.text,
            "source": self.source_type,
            "workspaceId": self.workspace_id,
            "channelId": self.channel_id,
            "channelName": self.channel_name,
            "threadTs": self.thread_ts,
        }
        message.update({k: v for k, v in self.extra.items() if k not in {"id"}})
        return {
            "sourceType": self.source_type,
            "tenantId": self.tenant_id,
            "ingestedAt": utc_now_iso(),
            "message": message,
            "user": self.user,
        }

    def friendly_from_to(self) -> Dict[str, Any]:
        if self.source_type.upper() == "GMAIL":
            return {
                "from": self.extra.get("fromEmail"),
                "to": self.extra.get("toEmail"),
                "subject": self.extra.get("subject"),
            }
        return {
            "from": self.user.get("displayName") or self.user.get("slackUserId") or self.user.get("uniqueUserId"),
            "to": self.channel_name or self.channel_id,
        }


def load_csv_inputs(csv_path: str, *, limit: Optional[int], tenant_id: str, workspace_id: str) -> List[TestInput]:
    out: List[TestInput] = []
    base_ts = time.time()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if limit is not None and idx >= limit:
                break
            text = row.get("text", "") or ""
            channel = row.get("channel", "") or "unknown"
            user_name = row.get("user_name", "") or "unknown"
            user_id = row.get("user_id", "") or f"UCSV{idx+1:04d}"
            thread_id = row.get("thread_id", "")
            thread_ts = None if thread_id in ("None", "", None) else str(thread_id)
            ts = f"{base_ts + (idx * 0.001):.6f}"
            out.append(
                TestInput(
                    name=f"csv_row_{idx+1:03d}",
                    source_type="SLACK",
                    tenant_id=tenant_id,
                    workspace_id=workspace_id,
                    channel_id=channel,
                    channel_name=channel,
                    thread_ts=thread_ts,
                    ts=ts,
                    text=text,
                    user={"displayName": user_name, "slackUserId": user_id},
                    extra={"csv": {"row": idx + 1, "user_id": user_id, "user_name": user_name, "channel": channel}},
                )
            )
    return out


def build_edge_inputs(tenant_id: str, workspace_id: str) -> List[TestInput]:
    base_ts = time.time()
    user_slack = {"displayName": "Edge Tester", "slackUserId": "UEDGE0001"}
    long_text = " ".join(["This is a long realistic message about planning, blockers, timelines, and next steps."] * 250)

    return [
        TestInput(
            name="edge_slack_normal",
            source_type="SLACK",
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            channel_id="CEDGE001",
            channel_name="#ops",
            thread_ts=None,
            ts=f"{base_ts:.6f}",
            text="Can you confirm the shipping date for the next release? We need an update by Friday.",
            user=user_slack,
            extra={"id": "edge-m1"},
        ),
        TestInput(
            name="edge_slack_missing_channelName",
            source_type="SLACK",
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            channel_id="CEDGE002",
            channel_name="",
            thread_ts=None,
            ts=f"{base_ts + 1:.6f}",
            text="We need approval on the contract terms before we proceed.",
            user=user_slack,
            extra={"id": "edge-m2"},
        ),
        TestInput(
            name="edge_slack_unicode",
            source_type="SLACK",
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            channel_id="CEDGE003",
            channel_name="#incidents",
            thread_ts=None,
            ts=f"{base_ts + 2:.6f}",
            text="Prod is failing again 😅 — can someone check the logs? Also: café résumé naïve 漢字",
            user=user_slack,
            extra={"id": "edge-m3"},
        ),
        TestInput(
            name="edge_slack_multiline",
            source_type="SLACK",
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            channel_id="CEDGE004",
            channel_name="#builds",
            thread_ts=None,
            ts=f"{base_ts + 3:.6f}",
            text="Update:\n- build is red\n- tests flaky\nNext: retry pipeline and report back.",
            user=user_slack,
            extra={"id": "edge-m4"},
        ),
        TestInput(
            name="edge_slack_very_long",
            source_type="SLACK",
            tenant_id=tenant_id,
            workspace_id=workspace_id,
            channel_id="CEDGE005",
            channel_name="#longform",
            thread_ts=None,
            ts=f"{base_ts + 4:.6f}",
            text=long_text,
            user=user_slack,
            extra={"id": "edge-m5"},
        ),
        TestInput(
            name="edge_gmail_from_to",
            source_type="GMAIL",
            tenant_id=tenant_id,
            workspace_id="gmail-default-workspace",
            channel_id="email:inbox",
            channel_name="inbox",
            thread_ts="gmail-thread-001",
            ts=f"{base_ts + 5:.6f}",
            text="Subject: Contract renewal\nBody: Please confirm the renewal terms and send the updated invoice.",
            user={"displayName": "Edge Tester", "uniqueUserId": "edge.tester@example.com"},
            extra={"id": "gmail-edge-1", "fromEmail": "finance@external.com", "toEmail": "edge.tester@example.com", "subject": "Contract renewal"},
        ),
    ]


def main() -> int:
    run_id = f"step6_full_{int(time.time())}"
    group_id = f"ai-service-group-{run_id}"
    started_at = utc_now_iso()

    tenant_id = str(uuid.uuid4())
    workspace_id = f"ws-{run_id}"

    csv_path = detect_csv_path()
    limit: Optional[int] = 200
    if CSV_LIMIT_ENV and CSV_LIMIT_ENV.strip():
        try:
            limit = int(CSV_LIMIT_ENV.strip())
        except Exception:
            limit = None

    edge_inputs = build_edge_inputs(tenant_id, workspace_id)
    csv_inputs = load_csv_inputs(csv_path, limit=limit, tenant_id=tenant_id, workspace_id=workspace_id)
    all_inputs = edge_inputs + csv_inputs

    expected_outputs = sum(1 for ti in all_inputs if (ti.text or "").strip())

    print(f"RunId: {run_id}")
    print(f"TenantId: {tenant_id}")
    print(f"WorkspaceId: {workspace_id}")
    print(f"CSV: {csv_path} (rows={len(csv_inputs)})")
    print(f"Inputs: total={len(all_inputs)} (edge={len(edge_inputs)} + csv={len(csv_inputs)}) expectedStep6Events≈{expected_outputs}")
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
    consumer = subprocess.Popen(
        consumer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    wait_for_group_ready(group_id, TOPIC_OUT, timeout_s=10.0)

    events_to_send = [ti.to_event() for ti in all_inputs]
    rc, prod_err = produce_events(events_to_send)
    if rc != 0:
        consumer.terminate()
        print("ERROR: failed to produce ingestion events", file=sys.stderr)
        print(prod_err, file=sys.stderr)
        return 2

    consumed: List[Dict[str, Any]] = []
    raw_out_lines: List[str] = []
    max_wait_s = max(120.0, min(900.0, expected_outputs * 3.0))
    deadline = time.time() + max_wait_s
    idle_timeout_s = 25.0
    last_step6_time = time.time()
    while time.time() < deadline:
        if consumer.stdout is None:
            break
        rlist, _, _ = select.select([consumer.stdout], [], [], 0.25)
        if consumer.poll() is not None:
            break
        if time.time() - last_step6_time > idle_timeout_s and consumed:
            break
        if not rlist:
            continue
        line = consumer.stdout.readline()
        if not line:
            continue
        raw_out_lines.append(line)
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue
        meta = evt.get("metadata")
        if isinstance(meta, dict) and str(meta.get("step")) == "6":
            mids = evt.get("messageIds")
            if not (isinstance(mids, list) and mids and str(mids[0]).startswith(tenant_id)):
                continue
            consumed.append(evt)
            last_step6_time = time.time()
            if len(consumed) >= expected_outputs:
                break

    try:
        consumer.terminate()
        consumer.wait(timeout=5)
    except Exception:
        try:
            consumer.kill()
        except Exception:
            pass
    raw_err = ""
    try:
        if consumer.stderr is not None:
            raw_err = consumer.stderr.read()
    except Exception:
        raw_err = ""

    by_msgid: Dict[str, Dict[str, Any]] = {}
    for evt in consumed:
        mids = evt.get("messageIds")
        if isinstance(mids, list) and mids:
            by_msgid[str(mids[0])] = evt

    results: List[Dict[str, Any]] = []
    creates = 0
    updates = 0
    no_output = 0
    for ti in all_inputs:
        exp_mid = ti.expected_message_id()
        evt = by_msgid.get(exp_mid)
        got = evt is not None
        if not (ti.text or "").strip():
            expect = False
        else:
            expect = True
        if not got:
            no_output += 1
        else:
            action = ((evt.get("metadata") or {}).get("action")) if isinstance(evt.get("metadata"), dict) else None
            if action == "create":
                creates += 1
            elif action == "update":
                updates += 1
        results.append(
            {
                "name": ti.name,
                "sourceType": ti.source_type,
                "fromTo": ti.friendly_from_to(),
                "message": {
                    "workspaceId": ti.workspace_id,
                    "channelId": ti.channel_id,
                    "channelName": ti.channel_name,
                    "threadTs": ti.thread_ts,
                    "ts": ti.ts,
                    "text": safe_text(ti.text, 800),
                },
                "user": ti.user,
                "expectedMessageId": exp_mid,
                "expectEvent": expect,
                "gotEvent": got,
                "step6": summarize_step6_event(evt) if evt else None,
            }
        )

    report = {
        "runId": run_id,
        "startedAt": started_at,
        "completedAt": utc_now_iso(),
        "tenantId": tenant_id,
        "workspaceId": workspace_id,
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "consumerGroup": group_id,
        "inputs": {
            "edgeCount": len(edge_inputs),
            "csvPath": csv_path,
            "csvCount": len(csv_inputs),
            "total": len(all_inputs),
        },
        "outputs": {
            "expectedStep6Events": expected_outputs,
            "observedStep6Events": len(consumed),
            "creates": creates,
            "updates": updates,
            "noOutput": no_output,
            "maxWaitSeconds": max_wait_s,
            "idleTimeoutSeconds": idle_timeout_s,
        },
        "notes": [
            "Slack messages do not have email-style From/To; for Slack, 'from' is the user and 'to' is the channel.",
            "Step 6 output events are read from ai-topic-metadata where metadata.step == '6'.",
            "Each output is matched to an input via the deterministic messageId (TopicMetadataEvent.messageIds[0]).",
        ],
        "cases": results,
        "debug": {
            "producerStderr": prod_err.strip()[:4000],
            "consumerStderr": raw_err.strip()[:4000],
        },
    }

    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"{run_id}_report.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Wrote report: {out_path}")
    print(
        f"Inputs: {len(all_inputs)} (edge={len(edge_inputs)}, csv={len(csv_inputs)}) | "
        f"Observed step6 events: {len(consumed)} | create={creates} update={updates} noOutput={no_output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
