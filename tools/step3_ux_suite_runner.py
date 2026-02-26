#!/usr/bin/env python3
"""
Step 3 UX metadata suite runner.

Creates a single TopicClusterRefinedEvent with N clusters derived from the
Synthetic_Slack_Messages.csv dataset, produces it to Kafka, then consumes Step 3
TopicMetadataEvent outputs and writes:
- JSONL dump of the raw output events
- A human-friendly JSON summary with validation results

This is meant for local verification of Step 3 fields required by the UI.
Primary focus: Situation / Impact / ProposedSolution / DecisionNeeded sections.
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

TOPIC_IN = os.environ.get("STEP3_INPUT_TOPIC", "ai-topic-refined")
TOPIC_OUT = os.environ.get("STEP3_OUTPUT_TOPIC", "ai-topic-metadata")

CSV_PATH = os.environ.get(
    "STEP3_CSV_PATH",
    "/home/maksym/sgh_works/lucid/phase_evaluation_engine/data/Synthetic_Slack_Messages.csv",
)

WORKSPACE_ID = os.environ.get("STEP3_WORKSPACE_ID", f"ws-step3-ux-{int(time.time())}")
BATCH_ID = os.environ.get("STEP3_BATCH_ID", f"{WORKSPACE_ID}:batch_{int(time.time())}")
TENANT_ID = os.environ.get("STEP3_TENANT_ID", "")

CLUSTER_TARGET = int(os.environ.get("STEP3_CLUSTER_TARGET", "20"))
CHUNK_SIZE = int(os.environ.get("STEP3_THREAD_CHUNK_SIZE", "12"))

MAX_WAIT_S = float(os.environ.get("STEP3_MAX_WAIT_S", "240"))

OUT_DIR = Path(os.environ.get("STEP3_OUT_DIR", "/home/maksym/sgh_works/lucid/logs"))


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


@dataclass
class CsvRow:
    row_id: int
    channel: str
    user_name: str
    user_id: str
    timestamp: str
    thread_id: Optional[str]
    text: str


def load_rows(path: str) -> List[CsvRow]:
    rows: List[CsvRow] = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for idx, row in enumerate(r, start=1):
            thread_id = row.get("thread_id") or None
            if thread_id == "None":
                thread_id = None
            rows.append(
                CsvRow(
                    row_id=idx,
                    channel=(row.get("channel") or "").strip(),
                    user_name=(row.get("user_name") or "").strip(),
                    user_id=(row.get("user_id") or "").strip(),
                    timestamp=(row.get("timestamp") or "").strip(),
                    thread_id=(thread_id.strip() if thread_id else None),
                    text=(row.get("text") or "").strip(),
                )
            )
    if not rows:
        raise RuntimeError(f"No rows loaded from CSV: {path}")
    return rows


def build_refined_clusters(rows: List[CsvRow], cluster_target: int, chunk_size: int) -> List[Dict[str, Any]]:
    by_thread: Dict[str, List[CsvRow]] = {}
    for r in rows:
        thread = r.thread_id or f"no_thread:{r.channel}"
        by_thread.setdefault(thread, []).append(r)

    # stable order: threads by first occurrence row_id
    thread_keys = sorted(by_thread.keys(), key=lambda k: by_thread[k][0].row_id)

    clusters: List[Dict[str, Any]] = []
    for thread_key in thread_keys:
        thread_rows = by_thread[thread_key]
        for part_idx in range(0, len(thread_rows), chunk_size):
            chunk = thread_rows[part_idx : part_idx + chunk_size]
            if not chunk:
                continue
            channel = chunk[0].channel or "unknown"
            thread_id = chunk[0].thread_id or None
            base = thread_key.replace("no_thread:", "").replace("#", "").replace(" ", "_")
            cluster_id = f"{base}_p{(part_idx // chunk_size) + 1}_{uuid.uuid4().hex[:8]}"
            draft_title = f"{channel} conversation ({base})"
            message_ids = [str(c.row_id) for c in chunk]

            clusters.append(
                {
                    "clusterId": cluster_id,
                    "draftTitle": draft_title,
                    "channel": channel,
                    "threadId": thread_id,
                    "participants": [],
                    "messageIds": message_ids,
                    "similarityScore": 1.0,
                }
            )
            if len(clusters) >= cluster_target:
                return clusters
    return clusters


def ensure_topics_exist(topics: List[str]) -> None:
    for t in topics:
        docker_exec(
            ["kafka-topics", "--bootstrap-server", BOOTSTRAP, "--create", "--if-not-exists", "--topic", t, "--partitions", "1", "--replication-factor", "1"],
            timeout_s=20,
        )


def produce_refined_event(event: Dict[str, Any]) -> Tuple[int, str]:
    payload = json.dumps(event, ensure_ascii=False) + "\n"
    proc = docker_exec(
        ["kafka-console-producer", "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC_IN],
        input_text=payload,
        timeout_s=30,
    )
    return proc.returncode, proc.stderr.decode("utf-8", errors="replace")


def consume_metadata_events(group: str, max_messages: int, timeout_s: float) -> List[Dict[str, Any]]:
    """
    Uses kafka-console-consumer with a timeout and extracts JSON objects.

    Some producers emit pretty-printed JSON spanning multiple lines; we therefore
    parse by brace counting instead of line-by-line json.loads().
    """
    # We intentionally read "whatever is available" within a short timeout and filter by workspace/batch.
    # This is robust for small, dedicated test topics and avoids consumer-group offset edge cases.
    cmd = [
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC_OUT,
        "--from-beginning",
        "--property",
        "print.value=true",
        "--property",
        "print.key=false",
        "--property",
        "print.timestamp=false",
        "--timeout-ms",
        str(int(timeout_s * 1000)),
    ]
    try:
        proc = docker_exec(cmd, timeout_s=int(timeout_s) + 30)
    except subprocess.TimeoutExpired:
        return []
    text = (proc.stdout or b"").decode("utf-8", errors="replace")

    events: List[Dict[str, Any]] = []
    buf: List[str] = []
    depth = 0
    in_obj = False
    for ch in text:
        if ch == "{" and not in_obj:
            in_obj = True
            depth = 1
            buf = ["{"]
            continue
        if not in_obj:
            continue
        buf.append(ch)
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                raw = "".join(buf)
                in_obj = False
                try:
                    events.append(json.loads(raw))
                except Exception:
                    pass
    return events


def get_topic_obj(evt: Dict[str, Any]) -> Dict[str, Any]:
    return evt.get("topic") if isinstance(evt.get("topic"), dict) else {}


def non_empty_str(v: Any) -> bool:
    return isinstance(v, str) and v.strip() != ""


def validate_event(evt: Dict[str, Any]) -> List[str]:
    topic = get_topic_obj(evt)
    missing: List[str] = []

    if not non_empty_str(topic.get("title")):
        missing.append("topic.title")

    # External party is allowed to be null, but should exist as a key for UX.
    if "externalParty" not in topic and "external_party" not in topic:
        missing.append("topic.externalParty")

    if not non_empty_str(topic.get("priority")):
        missing.append("topic.priority")
    if not non_empty_str(topic.get("suggestedAction")):
        missing.append("topic.suggestedAction")
    if not non_empty_str(topic.get("situation")):
        missing.append("topic.situation")
    if not non_empty_str(topic.get("impact")):
        missing.append("topic.impact")
    if not non_empty_str(topic.get("proposedSolution")):
        missing.append("topic.proposedSolution")
    if not non_empty_str(topic.get("decisionNeeded")):
        missing.append("topic.decisionNeeded")

    participants = topic.get("participants")
    if not isinstance(participants, list) or not participants:
        missing.append("topic.participants")

    action_items = topic.get("actionItems") or topic.get("action_items")
    if not isinstance(action_items, list) or not action_items:
        missing.append("topic.actionItems")

    tags = topic.get("tags")
    if not isinstance(tags, list) or not tags:
        missing.append("topic.tags")

    return missing


def summarize_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    topic = get_topic_obj(evt)
    return {
        "topicId": evt.get("topicId"),
        "clusterId": evt.get("clusterId"),
        "title": topic.get("title"),
        "externalParty": topic.get("externalParty") or topic.get("external_party"),
        "priority": topic.get("priority"),
        "deadline": topic.get("deadline"),
        "reason": topic.get("reason"),
        "suggestedAction": topic.get("suggestedAction"),
        "situation": topic.get("situation"),
        "impact": topic.get("impact"),
        "proposedSolution": topic.get("proposedSolution"),
        "decisionNeeded": topic.get("decisionNeeded"),
        "participantsCount": len(topic.get("participants") or []) if isinstance(topic.get("participants"), list) else 0,
        "actionItemsCount": len(topic.get("actionItems") or []) if isinstance(topic.get("actionItems"), list) else 0,
        "tagsCount": len(topic.get("tags") or []) if isinstance(topic.get("tags"), list) else 0,
    }


def main() -> int:
    if not TENANT_ID.strip():
        raise SystemExit("STEP3_TENANT_ID is required (Step3 rejects events without tenantId).")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = f"step3_ux_{int(time.time())}"
    group = f"ai-service-group-step3-ux-{uuid.uuid4().hex[:8]}"

    rows = load_rows(CSV_PATH)
    clusters = build_refined_clusters(rows, CLUSTER_TARGET, CHUNK_SIZE)
    if len(clusters) < CLUSTER_TARGET:
        raise SystemExit(f"Only built {len(clusters)} clusters; increase CSV size or reduce STEP3_CLUSTER_TARGET.")

    ensure_topics_exist([TOPIC_IN, TOPIC_OUT, f"{TOPIC_OUT}-dlq"])

    refined_event: Dict[str, Any] = {
        "eventId": str(uuid.uuid4()),
        "workspaceId": WORKSPACE_ID,
        "batchId": BATCH_ID,
        "providerId": "step2-suite",
        "promptVersion": "v1",
        "clusterCount": len(clusters),
        "clusters": clusters,
        "metadata": {
            "tenantId": TENANT_ID,
            "csvPath": CSV_PATH,
            "suite": "step3_ux",
            "generatedAt": utc_now_iso(),
        },
    }

    rc, err = produce_refined_event(refined_event)
    if rc != 0:
        raise SystemExit(f"Failed producing refined event to {TOPIC_IN}: {err}")

    # Consume outputs (wait in a couple of rounds to allow the service to process).
    all_events: List[Dict[str, Any]] = []
    seen_event_ids: set[str] = set()
    start = time.time()
    while time.time() - start < MAX_WAIT_S and len(all_events) < CLUSTER_TARGET:
        remaining = CLUSTER_TARGET - len(all_events)
        chunk = consume_metadata_events(group, remaining, timeout_s=10)
        if chunk:
            for evt in chunk:
                if evt.get("workspaceId") != WORKSPACE_ID or evt.get("batchId") != BATCH_ID:
                    continue
                event_id = evt.get("eventId")
                if isinstance(event_id, str) and event_id in seen_event_ids:
                    continue
                if isinstance(event_id, str):
                    seen_event_ids.add(event_id)
                all_events.append(evt)
        if len(all_events) >= CLUSTER_TARGET:
            break
        time.sleep(1.0)

    # Write raw JSONL
    raw_path = OUT_DIR / f"{run_id}.events.jsonl"
    with raw_path.open("w", encoding="utf-8") as f:
        for e in all_events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    # Validate and summarize
    summaries: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for e in all_events:
        missing = validate_event(e)
        summaries.append({**summarize_event(e), "missing": missing})
        if missing:
            failures.append(
                {
                    "topicId": e.get("topicId"),
                    "clusterId": e.get("clusterId"),
                    "missing": missing,
                }
            )

    report = {
        "runId": run_id,
        "startedAt": utc_now_iso(),
        "kafka": {
            "container": KAFKA_CONTAINER,
            "bootstrap": BOOTSTRAP,
            "inputTopic": TOPIC_IN,
            "outputTopic": TOPIC_OUT,
            "consumerGroup": group,
        },
        "inputs": {
            "csvPath": CSV_PATH,
            "workspaceId": WORKSPACE_ID,
            "batchId": BATCH_ID,
            "clusterTarget": CLUSTER_TARGET,
            "threadChunkSize": CHUNK_SIZE,
        },
        "outputs": {
            "eventsObserved": len(all_events),
            "eventsExpected": CLUSTER_TARGET,
            "failures": len(failures),
        },
        "files": {
            "rawEventsJsonl": str(raw_path),
        },
        "failuresDetail": failures[:50],
        "samples": summaries[: min(20, len(summaries))],
    }

    report_path = OUT_DIR / f"{run_id}.report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
