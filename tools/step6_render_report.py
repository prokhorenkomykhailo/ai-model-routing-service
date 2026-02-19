#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _get(d: Dict[str, Any], *path: str, default: Any = None) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def sentence(case: Dict[str, Any]) -> str:
    source = case.get("sourceType", "UNKNOWN")
    from_to = case.get("fromTo") if isinstance(case.get("fromTo"), dict) else {}
    message = case.get("message") if isinstance(case.get("message"), dict) else {}
    user = case.get("user") if isinstance(case.get("user"), dict) else {}
    step6 = case.get("step6") if isinstance(case.get("step6"), dict) else {}

    from_who = from_to.get("from") or user.get("displayName") or user.get("uniqueUserId") or user.get("slackUserId") or "unknown"
    to_where = from_to.get("to") or message.get("channelName") or message.get("channelId") or "unknown"
    text = str(message.get("text") or "").replace("\n", " ").strip()
    if len(text) > 140:
        text = text[:140] + "…"

    action = step6.get("action") or "none"
    title = step6.get("title") or ""
    reason = step6.get("reason") or ""
    sim = step6.get("similarityScore")
    if isinstance(sim, (int, float)):
        sim_txt = f"{sim:.2f}"
    else:
        sim_txt = "n/a"

    if source.upper() == "GMAIL":
        subject = from_to.get("subject") or ""
        subject_txt = f' (subject: "{subject}")' if subject else ""
        return (
            f'- [Gmail] From {from_who} to {to_where}{subject_txt}: "{text}" '
            f'→ Step 6: {action} (reason={reason}, similarity={sim_txt}) '
            f'→ topic "{title}"'
        ).strip()

    return (
        f'- [Slack] In {to_where}, {from_who} said: "{text}" '
        f'→ Step 6: {action} (reason={reason}, similarity={sim_txt}) '
        f'→ topic "{title}"'
    ).strip()


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: step6_render_report.py <report.json> [max_cases]", file=sys.stderr)
        return 2

    report_path = Path(sys.argv[1]).expanduser().resolve()
    max_cases: Optional[int] = None
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        try:
            max_cases = int(sys.argv[2])
        except Exception:
            max_cases = None

    report = json.loads(report_path.read_text(encoding="utf-8"))
    cases: List[Dict[str, Any]] = report.get("cases") or []
    if max_cases is not None:
        cases = cases[:max_cases]

    header = [
        f"# Step 6 Test Report ({report.get('runId')})",
        "",
        f"- Started: {_get(report, 'startedAt', default='')}",
        f"- Completed: {_get(report, 'completedAt', default='')}",
        f"- Inputs: {_get(report, 'inputs', 'total', default='?')} (edge={_get(report, 'inputs', 'edgeCount', default='?')}, csv={_get(report, 'inputs', 'csvCount', default='?')})",
        f"- Outputs: observed={_get(report, 'outputs', 'observedStep6Events', default='?')}, create={_get(report, 'outputs', 'creates', default='?')}, update={_get(report, 'outputs', 'updates', default='?')}",
        "",
        "## Cases",
        "",
    ]

    lines = header + [sentence(c) for c in cases]
    out_path = report_path.with_suffix(".summary.md")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

