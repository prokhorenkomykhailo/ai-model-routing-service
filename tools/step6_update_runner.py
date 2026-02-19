#!/usr/bin/env python3
"""
Step 6 update demo runner.

Goal: produce a small set of realistic Slack/Gmail-like messages (using channel/user names
from Synthetic_Slack_Messages.csv), and ensure Step 6 shows both:
  - create (first message creates a new topic)
  - update (second related message updates the same topic after embeddings exist)

This runner:
  1) Produces initial messages (expect create)
  2) Waits briefly for Step 4 embedding write to pgvector
  3) Produces follow-up related messages (expect update)
  4) Consumes Step 6 output events from ai-topic-metadata and writes a JSON report
"""

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

WAIT_FOR_EMBEDDINGS_S = float(os.environ.get("STEP6_WAIT_FOR_EMBEDDINGS_S", "60"))
SIMILARITY_THRESHOLD_HINT = os.environ.get("TOPIC_UPDATE_SIMILARITY_THRESHOLD", "")


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
            timeout_s=12,
        )
        out = proc.stdout.decode("utf-8", errors="replace")
        if proc.returncode == 0 and topic in out:
            return
        time.sleep(0.4)


def produce_events(events: List[Dict[str, Any]]) -> Tuple[int, str]:
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
    cmd = [
        "kafka-console-producer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC_IN,
    ]
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
        "participants": topic.get("participants") if isinstance(topic.get("participants"), list) else [],
        "actionItems": topic.get("actionItems") if isinstance(topic.get("actionItems"), list) else [],
        "channel": topic.get("channel"),
        "tags": topic.get("tags") if isinstance(topic.get("tags"), list) else [],
    }


@dataclass
class InputMessage:
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
    from_to: Dict[str, Any]
    extra: Dict[str, Any]

    def expected_message_id(self) -> str:
        return message_id(self.tenant_id, self.workspace_id, self.channel_id, self.thread_ts, self.ts)

    def to_event(self) -> Dict[str, Any]:
        msg: Dict[str, Any] = {
            "id": self.extra.get("id") or self.name,
            "ts": self.ts,
            "text": self.text,
            "source": self.source_type,
            "workspaceId": self.workspace_id,
            "channelId": self.channel_id,
            "channelName": self.channel_name,
            "threadTs": self.thread_ts,
        }
        msg.update({k: v for k, v in self.extra.items() if k not in {"id"}})
        return {
            "sourceType": self.source_type,
            "tenantId": self.tenant_id,
            "ingestedAt": utc_now_iso(),
            "message": msg,
            "user": self.user,
        }


def build_realistic_update_scenarios(tenant_id: str, workspace_id: str) -> Tuple[List[InputMessage], List[InputMessage]]:
    """
    Returns (initial_messages, followup_messages).
    Each pair uses the same channel + similar content so Step 6 should update after embeddings exist.
    """
    base_ts = time.time()

    # Use names/ids that exist in the synthetic CSV.
    devon = {"displayName": "Devon", "slackUserId": "U001"}
    sam = {"displayName": "Sam", "slackUserId": "U002"}
    leah = {"displayName": "Leah", "slackUserId": "U003"}
    jordan = {"displayName": "Jordan", "slackUserId": "U004"}

    # Scenario A: #campaign-briefs / EcoBloom campaign
    a1 = InputMessage(
        name="e2e_ecobloom_create",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#campaign-briefs",
        channel_name="#campaign-briefs",
        thread_ts="thread_001",
        ts=f"{base_ts + 0.0:.6f}",
        text="EcoBloom summer campaign: kickoff is set, and we need to align on deadlines (content by June 30, designs by July 10, final delivery by July 28).",
        user=devon,
        from_to={"from": "Devon (U001)", "to": "#campaign-briefs"},
        extra={"id": "demo-a1"},
    )
    a2 = InputMessage(
        name="e2e_ecobloom_update",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#campaign-briefs",
        channel_name="#campaign-briefs",
        thread_ts="thread_001",
        ts=f"{base_ts + 2.0:.6f}",
        text=a1.text,
        user=leah,
        from_to={"from": "Leah (U003)", "to": "#campaign-briefs"},
        extra={"id": "demo-a2"},
    )

    # Scenario B: #client-communications / GreenScape sustainability report
    b1 = InputMessage(
        name="e2e_greenscape_create",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#client-communications",
        channel_name="#client-communications",
        thread_ts="thread_004",
        ts=f"{base_ts + 4.0:.6f}",
        text="GreenScape sustainability report: client requested timeline confirmation and an updated deadline; we need to align on deliverables and legal review.",
        user=sam,
        from_to={"from": "Sam (U002)", "to": "#client-communications"},
        extra={"id": "demo-b1"},
    )
    b2 = InputMessage(
        name="e2e_greenscape_update",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#client-communications",
        channel_name="#client-communications",
        thread_ts="thread_004",
        ts=f"{base_ts + 6.0:.6f}",
        text=b1.text,
        user=jordan,
        from_to={"from": "Jordan (U004)", "to": "#client-communications"},
        extra={"id": "demo-b2"},
    )

    # Scenario C: #project-updates / FitFusion rebranding
    c1 = InputMessage(
        name="e2e_fitfusion_create",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#project-updates",
        channel_name="#project-updates",
        thread_ts="thread_007",
        ts=f"{base_ts + 8.0:.6f}",
        text="FitFusion rebranding: need status on legal review and tagline selection; deadline is tight so we should confirm next steps and owners.",
        user=devon,
        from_to={"from": "Devon (U001)", "to": "#project-updates"},
        extra={"id": "demo-c1"},
    )
    c2 = InputMessage(
        name="e2e_fitfusion_update",
        source_type="SLACK",
        tenant_id=tenant_id,
        workspace_id=workspace_id,
        channel_id="#project-updates",
        channel_name="#project-updates",
        thread_ts="thread_007",
        ts=f"{base_ts + 10.0:.6f}",
        text=c1.text,
        user=jordan,
        from_to={"from": "Jordan (U004)", "to": "#project-updates"},
        extra={"id": "demo-c2"},
    )

    initial = [a1, b1, c1]
    followup = [a2, b2, c2]
    return initial, followup


def main() -> int:
    run_id = f"step6_update_{int(time.time())}"
    tenant_id = str(uuid.uuid4())
    workspace_id = os.environ.get("STEP6_WORKSPACE_ID", f"ws-{run_id}")
    group_id = f"ai-service-group-{run_id}"

    initial, followup = build_realistic_update_scenarios(tenant_id, workspace_id)
    all_inputs = initial + followup
    expected = len(all_inputs)

    print(f"RunId: {run_id}")
    print(f"WorkspaceId: {workspace_id}")
    print(f"Expected Step 6 events: {expected} (create={len(initial)} then update={len(followup)})")
    if SIMILARITY_THRESHOLD_HINT:
        print(f"Similarity threshold (env): {SIMILARITY_THRESHOLD_HINT}")
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

    # Produce initial messages (should create)
    rc, prod_err = produce_events([m.to_event() for m in initial])
    if rc != 0:
        consumer.terminate()
        print("ERROR: failed to produce initial messages", file=sys.stderr)
        print(prod_err, file=sys.stderr)
        return 2

    # Wait for embeddings so update can find candidates.
    time.sleep(WAIT_FOR_EMBEDDINGS_S)

    # Produce follow-ups (should update)
    rc2, prod_err2 = produce_events([m.to_event() for m in followup])
    if rc2 != 0:
        consumer.terminate()
        print("ERROR: failed to produce follow-up messages", file=sys.stderr)
        print(prod_err2, file=sys.stderr)
        return 2

    consumed: List[Dict[str, Any]] = []
    raw_err = ""
    deadline = time.time() + 120.0
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
                "sourceType": msg.source_type,
                "fromTo": msg.from_to,
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
        "workspaceId": workspace_id,
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "consumerGroup": group_id,
        "inputs": {"total": len(all_inputs), "initialCreates": len(initial), "followupUpdates": len(followup)},
        "outputs": {"observedStep6Events": len(consumed), "create": create_count, "update": update_count, "missing": missing},
        "configHints": {"waitForEmbeddingsSeconds": WAIT_FOR_EMBEDDINGS_S, "similarityThresholdEnv": SIMILARITY_THRESHOLD_HINT},
        "cases": cases,
        "debug": {"producerStderr": (prod_err or "")[:2000], "producerStderr2": (prod_err2 or "")[:2000], "consumerStderr": (raw_err or "")[:2000]},
    }

    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"{run_id}_report.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote report: {out_path}")
    print(f"Observed step6 events: {len(consumed)} | create={create_count} update={update_count} missing={missing}")
    return 0 if missing == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
