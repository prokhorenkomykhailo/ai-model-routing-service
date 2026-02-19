#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time
import select
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


KAFKA_CONTAINER = os.environ.get("LUCID_KAFKA_CONTAINER", "lucid-kafka-1")
BOOTSTRAP = os.environ.get("LUCID_KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.environ.get("STEP6_INPUT_TOPIC", "lucid-ingestion-messages")
TOPIC_OUT = os.environ.get("STEP6_OUTPUT_TOPIC", "ai-topic-metadata")


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


@dataclass
class Case:
    name: str
    tenant_id: str
    source_type: str
    message: Dict[str, Any]
    user: Optional[Dict[str, Any]]
    expect_event: bool
    notes: str

    def expected_message_id(self) -> Optional[str]:
        ts = self.message.get("ts")
        if not isinstance(ts, str) or not ts.strip():
            return None
        workspace_id = (self.message.get("workspaceId") or self.message.get("teamId") or "")
        if not isinstance(workspace_id, str) or not workspace_id.strip():
            if self.source_type.upper() == "GMAIL":
                workspace_id = "gmail-default-workspace"
            else:
                workspace_id = "default-workspace"
        channel_id = self.message.get("channelId") or ""
        thread_ts = self.message.get("threadTs")
        return message_id(self.tenant_id, workspace_id, str(channel_id), str(thread_ts) if thread_ts is not None else None, ts)


def make_event(case: Case) -> Dict[str, Any]:
    event: Dict[str, Any] = {
        "sourceType": case.source_type,
        "tenantId": case.tenant_id,
        "ingestedAt": utc_now_iso(),
        "message": case.message,
    }
    if case.user is not None:
        event["user"] = case.user
    return event


def consume_step6_events(timeout_ms: int, group_id: str) -> Tuple[List[Dict[str, Any]], str, str]:
    cmd = [
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC_OUT,
        "--group",
        group_id,
        "--consumer-property",
        "auto.offset.reset=latest",
        "--timeout-ms",
        str(timeout_ms),
    ]
    proc = docker_exec(cmd, timeout_s=max(10, int(timeout_ms / 1000) + 10))
    out = proc.stdout.decode("utf-8", errors="replace")
    err = proc.stderr.decode("utf-8", errors="replace")
    events: List[Dict[str, Any]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue
        meta = evt.get("metadata")
        if isinstance(meta, dict) and str(meta.get("step")) == "6":
            events.append(evt)
    return events, out, err


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

def wait_for_group_ready(group_id: str, topic: str, timeout_s: float = 8.0) -> None:
    """
    Best-effort wait until Kafka reports the consumer group exists and is assigned to the topic.
    This reduces the chance we miss the first few produced messages when using auto.offset.reset=latest.
    """
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


def summarize_topic(evt: Dict[str, Any]) -> Dict[str, Any]:
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
        "participantsCount": len(participants),
        "actionItemsCount": len(action_items),
    }


def main() -> int:
    run_id = f"step6_edge_{int(time.time())}"
    group_id = f"ai-service-group-{run_id}"

    tenant_id = str(uuid.uuid4())
    ws_new = f"ws-{run_id}"

    base_user = {"displayName": "Edge Tester", "slackUserId": "UEDGE0001"}

    long_text = " ".join(["This is a long realistic message about planning, blockers, timelines, and next steps."] * 250)

    cases: List[Case] = [
        Case(
            name="slack_normal_create",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m1",
                "ts": f"{time.time():.6f}",
                "text": "Can you confirm the shipping date for the next release? We need an update by Friday.",
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE001",
                "channelName": "#ops",
                "threadTs": None,
            },
            user=base_user,
            expect_event=True,
            notes="Baseline: valid Slack message should create a new topic (no existing embeddings in new workspace).",
        ),
        Case(
            name="slack_missing_workspaceId_uses_teamId",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m2",
                "ts": f"{time.time() + 1:.6f}",
                "text": "Reminder: please send the invoice for January. It is overdue and blocking payment.",
                "source": "SLACK",
                "teamId": ws_new,
                "channelId": "CEDGE002",
                "channelName": "#finance",
            },
            user=base_user,
            expect_event=True,
            notes="WorkspaceId omitted; service should fall back to teamId as workspace.",
        ),
        Case(
            name="slack_missing_channelName",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m3",
                "ts": f"{time.time() + 2:.6f}",
                "text": "We need approval on the contract terms before we proceed.",
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE003",
            },
            user=base_user,
            expect_event=True,
            notes="ChannelName missing; topic should still be created (but channel-based constraints cannot apply).",
        ),
        Case(
            name="slack_unicode_emoji",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m4",
                "ts": f"{time.time() + 3:.6f}",
                "text": "Prod is failing again 😅 — can someone check the logs? Also: café résumé naïve 漢字",
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE004",
                "channelName": "#incidents",
            },
            user=base_user,
            expect_event=True,
            notes="Non-ASCII content should not break JSON parsing or embeddings.",
        ),
        Case(
            name="slack_multiline_text",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m5",
                "ts": f"{time.time() + 4:.6f}",
                "text": "Update:\n- build is red\n- tests flaky\nNext: retry pipeline and report back.",
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE005",
                "channelName": "#builds",
            },
            user=base_user,
            expect_event=True,
            notes="Multi-line text should be accepted and produce a topic.",
        ),
        Case(
            name="slack_empty_text_no_output",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m6",
                "ts": f"{time.time() + 5:.6f}",
                "text": "",
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE006",
                "channelName": "#empty",
            },
            user=base_user,
            expect_event=False,
            notes="Empty text should be ignored by Step 6 (no topic created/updated).",
        ),
        Case(
            name="slack_very_long_text",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m7",
                "ts": f"{time.time() + 6:.6f}",
                "text": long_text,
                "source": "SLACK",
                "workspaceId": ws_new,
                "channelId": "CEDGE007",
                "channelName": "#longform",
            },
            user=base_user,
            expect_event=True,
            notes="Very long text should still produce output (embedding may fall back deterministically).",
        ),
        Case(
            name="gmail_message_default_workspace",
            tenant_id=tenant_id,
            source_type="GMAIL",
            message={
                "id": "gmail-1",
                "ts": f"{time.time() + 7:.6f}",
                "text": "Subject: Contract renewal\nBody: Please confirm the renewal terms and send the updated invoice.",
                "source": "GMAIL",
                "type": "EMAIL",
                "threadTs": "gmail-thread-001",
                "channelId": "email:inbox",
                "channelName": "inbox",
            },
            user={"displayName": "Edge Tester", "uniqueUserId": "edge.tester@example.com"},
            expect_event=True,
            notes="Gmail messages should map to gmail-default-workspace when workspaceId/teamId not present.",
        ),
        Case(
            name="update_existing_ws_gemini_project_updates",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m8",
                "ts": f"{time.time() + 8:.6f}",
                "text": "FitFusion rebranding: can we confirm legal review status and finalize the tagline selection this week?",
                "source": "SLACK",
                "workspaceId": "ws-gemini",
                "channelId": "C-PROJ-UPDATES",
                "channelName": "#project-updates",
                "threadTs": None,
            },
            user=base_user,
            expect_event=True,
            notes="Should update an existing ws-gemini topic if embeddings exist and similarity >= threshold; otherwise creates.",
        ),
        Case(
            name="channel_mismatch_should_create_if_candidate",
            tenant_id=tenant_id,
            source_type="SLACK",
            message={
                "id": "m9",
                "ts": f"{time.time() + 9:.6f}",
                "text": "FitFusion rebranding: legal review + tagline selection — please advise next steps.",
                "source": "SLACK",
                "workspaceId": "ws-gemini",
                "channelId": "C-RANDOM",
                "channelName": "#random",
                "threadTs": None,
            },
            user=base_user,
            expect_event=True,
            notes="If a candidate topic is found but channel differs, Step 6 should create with reason=channel_mismatch.",
        ),
    ]

    events_to_send = [make_event(c) for c in cases]

    # Start consumer FIRST (auto.offset.reset=latest) so we only receive events produced during this run.
    expected_outputs = sum(1 for c in cases if c.expect_event)
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
    wait_for_group_ready(group_id, TOPIC_OUT, timeout_s=8.0)

    rc, prod_err = produce_events(events_to_send)
    if rc != 0:
        consumer.terminate()
        print("ERROR: failed to produce ingestion events", file=sys.stderr)
        print(prod_err, file=sys.stderr)
        return 2

    consumed: List[Dict[str, Any]] = []
    raw_out_lines: List[str] = []
    deadline = time.time() + 75.0
    while time.time() < deadline:
        if consumer.stdout is None:
            break
        rlist, _, _ = select.select([consumer.stdout], [], [], 0.25)
        if not rlist:
            if consumer.poll() is not None:
                break
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
            consumed.append(evt)
            if len(consumed) >= expected_outputs:
                break

    # Stop consumer and capture any stderr for debugging
    try:
        consumer.terminate()
        consumer.wait(timeout=5)
    except Exception:
        try:
            consumer.kill()
        except Exception:
            pass
    raw_out = "".join(raw_out_lines)
    raw_err = ""
    try:
        if consumer.stderr is not None:
            raw_err = consumer.stderr.read()
    except Exception:
        raw_err = ""

    # Index by built messageId (the service emits this in messageIds[0])
    by_msgid: Dict[str, Dict[str, Any]] = {}
    for evt in consumed:
        mids = evt.get("messageIds")
        if isinstance(mids, list) and mids:
            mid0 = str(mids[0])
            by_msgid[mid0] = evt

    case_results: List[Dict[str, Any]] = []
    passed = 0
    failed = 0
    for c in cases:
        exp_mid = c.expected_message_id()
        evt = by_msgid.get(exp_mid) if exp_mid else None
        got = evt is not None
        ok = (got == c.expect_event) if c.expect_event is False else got
        if ok:
            passed += 1
        else:
            failed += 1
        case_results.append(
            {
                "name": c.name,
                "notes": c.notes,
                "expectEvent": c.expect_event,
                "expectedMessageId": exp_mid,
                "gotEvent": got,
                "result": summarize_topic(evt) if evt else None,
            }
        )

    report = {
        "runId": run_id,
        "startedAt": utc_now_iso(),
        "kafka": {"container": KAFKA_CONTAINER, "bootstrap": BOOTSTRAP, "inputTopic": TOPIC_IN, "outputTopic": TOPIC_OUT},
        "consumerGroup": group_id,
        "cases": case_results,
        "observedStep6Events": len(consumed),
        "passCount": passed,
        "failCount": failed,
        "notes": [
            "Step 6 events are collected from ai-topic-metadata where metadata.step == '6'.",
            "Events are matched back to cases via TopicMetadataEvent.messageIds[0] (deterministic messageId).",
            "If Gemini embedding/model is unavailable, the service falls back to deterministic embeddings and deterministic topic metadata (still emits events).",
        ],
        "debug": {
            "producerStderr": prod_err.strip()[:4000],
            "consumerStderr": raw_err.strip()[:4000],
        },
    }

    logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    logs_dir = os.path.abspath(logs_dir)
    os.makedirs(logs_dir, exist_ok=True)
    out_path = os.path.join(logs_dir, f"{run_id}_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Wrote report: {out_path}")
    print(f"Observed step6 events: {len(consumed)} | Passed: {passed} | Failed: {failed}")
    for r in case_results:
        res = r["result"]
        if res:
            print(f"- {r['name']}: {res.get('action')} topicId={res.get('topicId')} participants={res.get('participantsCount')} actionItems={res.get('actionItemsCount')}")
        else:
            print(f"- {r['name']}: (no step6 event)")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
