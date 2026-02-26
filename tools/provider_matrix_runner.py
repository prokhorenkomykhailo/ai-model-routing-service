#!/usr/bin/env python3
"""
Provider matrix runner for Steps 1/3/6.

Runs the ai-model-routing-service with different provider-hints and verifies:
  - Step 1: CSV runner emits a TopicClusterDraftEvent to a dedicated Kafka topic
  - Step 3: UX metadata runner produces N TopicMetadataEvents
  - Step 6: realistic suite runner produces create/update events (incl. edge cases)

This script is meant for local verification. It uses the mock server for OpenAI/HF
unless real keys/endpoints are configured via env vars.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


BASE = Path(os.environ.get("LUCID_HOME", "/home/maksym/sgh_works/lucid"))
SERVICE_DIR = BASE / "ai-model-routing-service"
SERVICE_JAR = SERVICE_DIR / "target" / "lucid-ai-routing-service-1.3.17.jar"

KAFKA_CONTAINER = os.environ.get("LUCID_KAFKA_CONTAINER", "lucid-kafka-1")
BOOTSTRAP = os.environ.get("LUCID_KAFKA_BOOTSTRAP", "localhost:9092")

CSV_PATH = Path(
    os.environ.get(
        "MATRIX_CSV_PATH",
        str(BASE / "phase_evaluation_engine/data/Synthetic_Slack_Messages.csv"),
    )
)

OUTDIR = Path(os.environ.get("MATRIX_OUT_DIR", str(BASE / "logs" / f"provider_matrix3_{int(time.time())}")))
OUTDIR.mkdir(parents=True, exist_ok=True)


def run(cmd: List[str], *, cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None, timeout_s: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def docker_exec(args: List[str], *, input_text: Optional[str] = None, timeout_s: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", "-i", KAFKA_CONTAINER, *args],
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def is_listening(host: str, port: int, *, timeout_s: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except Exception:
        return False


def ensure_mock_server(log_path: Path) -> Optional[subprocess.Popen]:
    """
    Ensure the local mock AI server is running on :9001 (OpenAI-compatible + HF-like).
    Returns the started process, or None if already running.
    """
    if is_listening("127.0.0.1", 9001):
        return None
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["python3", str(SERVICE_DIR / "tools" / "mock_ai_server.py"), "--port", "9001"],
        cwd=str(SERVICE_DIR),
        stdout=out,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Give it a moment to bind.
    deadline = time.time() + 5
    while time.time() < deadline:
        if is_listening("127.0.0.1", 9001):
            return proc
        time.sleep(0.1)
    raise RuntimeError("mock_ai_server failed to start on :9001")


def ensure_topics(topics: List[str]) -> None:
    for t in topics:
        docker_exec(
            [
                "kafka-topics",
                "--bootstrap-server",
                BOOTSTRAP,
                "--create",
                "--if-not-exists",
                "--topic",
                t,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ],
            timeout_s=20,
        )


def wait_health(port: int, timeout_s: int = 180) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = run(["curl", "-fsS", f"http://localhost:{port}/actuator/health"], timeout_s=10)
        if proc.returncode == 0:
            return True
        time.sleep(1.0)
    return False


def consume_first_json_by_workspace(topic: str, workspace_id: str, *, timeout_s: int = 20) -> Optional[Dict[str, Any]]:
    proc = docker_exec(
        [
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            "200",
            "--timeout-ms",
            str(int(timeout_s * 1000)),
        ],
        timeout_s=max(10, int(timeout_s) + 20),
    )

    text = proc.stdout or ""

    # Some producers may emit pretty-printed JSON with newlines; extract JSON objects by brace counting.
    objs: List[Dict[str, Any]] = []
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
                    objs.append(json.loads(raw))
                except Exception:
                    pass

    for obj in objs:
        if obj.get("workspaceId") == workspace_id:
            return obj
    return None


@dataclass(frozen=True)
class ProviderRun:
    provider_id: str
    label: str


def base_env() -> Dict[str, str]:
    env = dict(os.environ)
    # Best-effort load local dev secrets if present (do not print).
    env_file = BASE / "lucid-slack-ingestion-service/.env.local"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("\"").strip("'")
            if k and k not in env and v:
                env[k] = v

    env.setdefault("SPRING_KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP)
    env.setdefault("SPRING_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    env.setdefault("REDIS_HOST", "localhost")
    env.setdefault("REDIS_PORT", "6379")
    env.setdefault("ANONYMIZATION_SERVICE_ENABLED", "false")
    # Keep local runs quiet (no OTEL collector assumed).
    env.setdefault("OTEL_SDK_DISABLED", "true")

    # Minimal DB config so the service can boot (uses local dev Postgres container mapping 5434).
    env.setdefault("SPRING_DATASOURCE_URL", "jdbc:postgresql://localhost:5434/soundclassifiers")
    env.setdefault("SPRING_DATASOURCE_USERNAME", "postgres")
    env.setdefault("SPRING_DATASOURCE_PASSWORD", "localdev123")

    env.setdefault("TOPIC_UPDATE_ENABLED", "true")
    env.setdefault("TOPIC_METADATA_CSV_FALLBACK_ENABLED", "true")
    env.setdefault("TOPIC_METADATA_CSV_PATH", str(CSV_PATH))
    return env


def provider_env(provider_id: str) -> Dict[str, str]:
    env = base_env()

    env["TOPIC_CLUSTERING_PROVIDER_HINT"] = provider_id
    env["TOPIC_METADATA_PROVIDER_HINT"] = provider_id
    env["TOPIC_UPDATE_PROVIDER_HINT"] = provider_id

    # Mock server defaults (user can override by setting real endpoints/keys).
    if provider_id == "openaiProvider":
        env.setdefault("OPENAI_API_KEY", "test")
        env.setdefault("OPENAI_BASE_URL", "http://localhost:9001/v1")
    if provider_id == "huggingfaceProvider":
        env.setdefault("HUGGINGFACE_ENABLED", "true")
        env.setdefault("HUGGINGFACE_API_KEY", "test")
        env.setdefault("HUGGINGFACE_ENDPOINT", "http://localhost:9001/models")
        env.setdefault("HUGGINGFACE_MODEL", "mock")
    else:
        env.setdefault("HUGGINGFACE_ENABLED", "false")

    return env


def start_service(env: Dict[str, str], log_path: Path) -> subprocess.Popen:
    if not SERVICE_JAR.exists():
        raise RuntimeError(f"Service jar not found: {SERVICE_JAR}. Run ./mvnw -DskipTests package first.")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["java", "-jar", str(SERVICE_JAR), "--spring.profiles.active=local"],
        cwd=str(SERVICE_DIR),
        env=env,
        stdout=out,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def stop_process(proc: subprocess.Popen, *, timeout_s: int = 20) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.5)
    proc.kill()


def copy_latest(glob_pat: str, dst: Path) -> bool:
    matches = sorted(Path().glob(glob_pat), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        return False
    dst.write_text(matches[0].read_text(encoding="utf-8"), encoding="utf-8")
    return True


def main() -> int:
    mock_proc: Optional[subprocess.Popen] = None
    try:
        mock_proc = ensure_mock_server(OUTDIR / "mock_ai_server_9001.log")
    except Exception as e:
        # Only required for OpenAI/HF runs; allow Gemini-only users to proceed.
        print(f"WARNING: could not start mock AI server on :9001 ({e}). OpenAI/HF runs may fail.")

    providers = [
        ProviderRun("geminiProvider", "gemini"),
        ProviderRun("openaiProvider", "openai"),
        ProviderRun("huggingfaceProvider", "huggingface"),
    ]

    summary: List[Dict[str, Any]] = []

    try:
        for p in providers:
            tenant_id = str(uuid.uuid4())
            server_port = 18083 + (0 if p.provider_id == "geminiProvider" else 1 if p.provider_id == "openaiProvider" else 2)
            draft_topic = f"ai-topic-drafts-mx3-{p.label}-{int(time.time())}"
            # Keep Step 1 isolated from Step 2/3 noise during matrix runs.
            merge_split_draft_topic = f"ai-topic-drafts-mx3-merge-split-{p.label}-{int(time.time())}"
            merge_split_refined_topic = f"ai-topic-refined-mx3-merge-split-{p.label}-{int(time.time())}"
            refined_topic = f"ai-topic-refined-mx3-{p.label}-{int(time.time())}"
            metadata_topic = f"ai-topic-metadata-mx3-{p.label}-{int(time.time())}"
            ingest_topic = f"lucid-ingestion-messages-mx3-{p.label}-{int(time.time())}"

            ws_step1 = f"ws-step1-mx3-{p.label}-{int(time.time())}"
            ws_step3 = f"ws-step3-mx3-{p.label}-{int(time.time())}"

            ensure_topics(
                [
                    draft_topic,
                    merge_split_draft_topic,
                    merge_split_refined_topic,
                    refined_topic,
                    metadata_topic,
                    f"{metadata_topic}-dlq",
                    ingest_topic,
                ]
            )

            env = provider_env(p.provider_id)
            env["SERVER_PORT"] = str(server_port)
            env["KAFKA_TOPIC_INGESTION_MESSAGES"] = ingest_topic
            env["TOPIC_CLUSTERING_DRAFT_TOPIC"] = draft_topic
            env["TOPIC_MERGE_SPLIT_DRAFT_TOPIC"] = merge_split_draft_topic
            env["TOPIC_MERGE_SPLIT_REFINED_TOPIC"] = merge_split_refined_topic
            env["TOPIC_METADATA_REFINED_TOPIC"] = refined_topic
            env["TOPIC_METADATA_METADATA_TOPIC"] = metadata_topic
            env["TOPIC_EMBEDDING_METADATA_TOPIC"] = metadata_topic
            env["TOPIC_VISIBILITY_METADATA_TOPIC"] = metadata_topic

            # Step 1 runner (startup)
            env["TOPIC_CLUSTERING_RUNNER_ENABLED"] = "true"
            env["TOPIC_CLUSTERING_RUNNER_CSV_PATH"] = str(CSV_PATH)
            env["TOPIC_CLUSTERING_RUNNER_TENANT_ID"] = tenant_id
            env["TOPIC_CLUSTERING_RUNNER_WORKSPACE_ID"] = ws_step1
            env["TOPIC_CLUSTERING_RUNNER_WORKSPACE_NAME"] = f"Matrix3 {p.label}"
            env["TOPIC_CLUSTERING_RUNNER_BATCH_NUMBER"] = "1"

            run_out = OUTDIR / p.provider_id
            run_out.mkdir(parents=True, exist_ok=True)
            service_log = run_out / "service.log"

            proc = start_service(env, service_log)
            ok = wait_health(server_port)
            step1_path = run_out / "step1.event.json"
            step3_path = run_out / "step3.report.json"
            step6_path = run_out / "step6.report.json"

            step1_evt = None
            step3_ok = False
            step6_ok = False
            errors: List[str] = []

            try:
                if not ok:
                    errors.append("service_not_healthy")
                    continue

                # Step 1 runner is async-ish (it runs on startup but can still be processing when health flips to UP).
                # Retry consumption briefly to avoid flakiness.
                step1_evt = None
                deadline = time.time() + 75
                while time.time() < deadline and step1_evt is None:
                    step1_evt = consume_first_json_by_workspace(draft_topic, ws_step1, timeout_s=10)
                    if step1_evt is None:
                        time.sleep(1.0)
                if step1_evt:
                    step1_path.write_text(json.dumps(step1_evt, ensure_ascii=False, indent=2), encoding="utf-8")
                else:
                    errors.append("step1_no_event")

                # Step 3 suite (reduced target for matrix)
                step3_env = dict(env)
                step3_env.update(
                    {
                        "STEP3_TENANT_ID": tenant_id,
                        "STEP3_CSV_PATH": str(CSV_PATH),
                        "STEP3_CLUSTER_TARGET": "6",
                        "STEP3_MAX_WAIT_S": "120",
                        "STEP3_INPUT_TOPIC": refined_topic,
                        "STEP3_OUTPUT_TOPIC": metadata_topic,
                        "STEP3_WORKSPACE_ID": ws_step3,
                        "STEP3_BATCH_ID": f"{ws_step3}:batch_{int(time.time())}",
                        "STEP3_OUT_DIR": str(run_out),
                    }
                )
                r3 = run(["python3", "tools/step3_ux_suite_runner.py"], cwd=SERVICE_DIR, env=step3_env, timeout_s=180)
                (run_out / "step3.stdout.log").write_text(r3.stdout, encoding="utf-8")
                (run_out / "step3.stderr.log").write_text(r3.stderr, encoding="utf-8")
                # Copy latest report created in run_out
                reports = sorted(run_out.glob("step3_ux_*.report.json"), key=lambda x: x.stat().st_mtime, reverse=True)
                if reports:
                    step3_path.write_text(reports[0].read_text(encoding="utf-8"), encoding="utf-8")
                    step3_ok = True
                else:
                    errors.append("step3_no_report")

                # Step 6 realistic suite
                step6_env = dict(env)
                step6_env.update(
                    {
                        "STEP6_CSV_PATH": str(CSV_PATH),
                        "STEP6_INPUT_TOPIC": ingest_topic,
                        "STEP6_OUTPUT_TOPIC": metadata_topic,
                        "STEP6_WAIT_AFTER_PRODUCE_S": "2",
                        "STEP6_WAIT_BETWEEN_CREATE_UPDATE_S": "4",
                    }
                )
                r6 = run(["python3", "tools/step6_realistic_suite_runner.py"], cwd=SERVICE_DIR, env=step6_env, timeout_s=240)
                (run_out / "step6.stdout.log").write_text(r6.stdout, encoding="utf-8")
                (run_out / "step6.stderr.log").write_text(r6.stderr, encoding="utf-8")

                # Copy latest step6 report produced in deliverables
                deliver_dir = BASE / "deliverables/step6_delivery/logs"
                deliver_reports = sorted(deliver_dir.glob("step6_suite_*_report.json"), key=lambda x: x.stat().st_mtime, reverse=True)
                if deliver_reports:
                    step6_path.write_text(deliver_reports[0].read_text(encoding="utf-8"), encoding="utf-8")
                    step6_ok = True
                else:
                    errors.append("step6_no_report")

            finally:
                stop_process(proc)

            summary.append(
                {
                    "provider": p.provider_id,
                    "tenantId": tenant_id,
                    "topics": {
                        "draft": draft_topic,
                        "mergeSplitDraft": merge_split_draft_topic,
                        "mergeSplitRefined": merge_split_refined_topic,
                        "refined": refined_topic,
                        "metadata": metadata_topic,
                        "ingestion": ingest_topic,
                    },
                    "outputs": {
                        "step1_event": str(step1_path),
                        "step3_report": str(step3_path),
                        "step6_report": str(step6_path),
                    },
                    "ok": {"step1": step1_evt is not None, "step3": step3_ok, "step6": step6_ok},
                    "errors": errors,
                }
            )
    finally:
        if mock_proc is not None:
            try:
                stop_process(mock_proc)
            except Exception:
                pass

    (OUTDIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote matrix summary: {OUTDIR / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
