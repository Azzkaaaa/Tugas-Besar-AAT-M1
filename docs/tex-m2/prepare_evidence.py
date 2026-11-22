"""Snapshot report provenance from the existing checkouts; never run a live test."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BASE = "infrastructure/docs/evidence/m2-required-scenarios/"

# These are pointers to retained original artifacts, not copies of their raw logs.
EVIDENCE = [
    ("E01", "Deklarasi host fisik dan node", ["topology.json"]),
    ("E02", "Kondisi akhir dan verifikasi snapshot 18", ["deployment/final-state-20261122.json", "deployment/snapshot-18.json"]),
    ("E03", "Platform empat kernel dan volume lokal", ["../m2-k3s-bootstrap/platform-contract.json"]),
    ("E04", "Penempatan 20.514 hazard live", ["problem1/live-canonical-placement.json"]),
    ("E05", "Ring dan estimasi storage sebelum fault", ["problem1/placement-before-loss-20261122.json"]),
    ("E06", "Counter normal dan burst 300 gempa", ["problem1/burst-load-practice.json"]),
    ("E07", "Scoped disk loss dan survival identitas", ["problem1/storage-loss-20261122/summary.json", "problem1/storage-loss-20261122/audit-after.json"]),
    ("E08", "Transkripsi browser analytics lengkap", ["problem1/browser-analytics-20261122.json"]),
    ("E09", "Pagination global dan FULL/summary", ["list/bounded-ordering-0b58a62-v2.json"]),
    ("E10", "Analytics versus Audit dan degradasi browser", ["problem1/analytics-after-7ccc9d0/analytics-audit-20261122.json", "problem1/browser-post-repair-20261122.json", "problem1/audit-d773970/baseline.json"]),
    ("E11", "Failover BMKG dan log polling", ["p2/leader-failover-bmkg.json"]),
    ("E12", "Clock skew node c", ["p2/clock-skew-bnpb-c.json"]),
    ("E13", "Freeze/resume node c", ["p2/freeze-resume-bnpb-c.json"]),
    ("E14", "Scaling, discovery dan throughput 1/3 replika", ["p2/scaling-throughput-20261122.json"]),
    ("E15", "Pergantian versi saat load", ["p2/version-change-20261122.json"]),
    ("E16", "k6 ramp dan batas generator", ["p2/quick-load-20261122/report.json", "p2/quick-load-20261122/README.md"]),
    ("E17", "k6 400 VU/120 s dengan pembaruan token", ["p2/quick-load-20261122/corrected-report.json"]),
    ("E18", "Arsip raw schema dan pemeriksaan hash", ["problem3/recovery-20261122/archival-and-replay.json"]),
    ("E19", "Replay 20 pesan dan pemeriksaan ulang", ["problem3/recovery-20261122/replay-enabled.json"]),
    ("E20", "Durability 12 field reports", ["problem3/field-reports.json"]),
    ("E21", "Otorisasi metadata dan raw Ops", ["problem3/operator-authorization.json", "problem3/raw-authorization.json"]),
    ("E22", "Diagnosis load lintas host dan trace", ["p2/load-bottleneck-20261122/README.md", "p2/load-bottleneck-20261122/tempo-breakdown.json"]),
    ("E23", "Test engine satelit dan reconnect", ["problem4/focused-tests-20261122.txt"]),
    ("E24", "Observasi live satelit speed 60", ["problem4/live-observation-20261122.json"]),
    ("E25", "Cassandra crash sebelum checkpoint", ["problem4/cassandra-recovery-20261122.txt"]),
    ("E26", "Kemajuan checkpoint satelit live", ["problem4/live-checkpoints-20261122.json"]),
    ("E27", "Karantina coastal, priority load, percobaan awal", ["problem5/practice-recovery-20261122.json"]),
    ("E28", "Sensor network outage dan recovery otomatis", ["problem5/practice-network-outage-20261122.json"]),
    ("E29", "Journal lengkap dan signature tiga critical", ["problem5/critical-reconciliation-20261122/report.json"]),
    ("E30", "Critical journal setelah recovery storage", ["problem5/critical-reconciliation-20261122/post-storage-comparison.json"]),
    ("E31", "Trace 45 span lintas enam layanan", ["telemetry/critical-trace-dc95832b.json"]),
]

SOURCES = [
    "IF4031 - Tugas Besar - M2.pdf",
    "Tugas-Besar-AAT-M1/docs/tex/main.tex",
    "Tugas-Besar-AAT-M1/docs/tex/IF4031_M1_nasigoreng.pdf",
    "Tugas-Besar-AAT-M1/docs/tex/contents/01-sistem.tex",
    "services/README.md",
    "infrastructure/README.md",
    "infrastructure/docs/m2-report-evidence-2026-11-22.md",
    "infrastructure/m2/sources.yaml",
    "infrastructure/m2/scaling.md",
    "infrastructure/m2/k3s/nodes/README.md",
    "services/docs/m2-write-authority.md",
    "services/docs/m2-audit.md",
    "services/docs/m2-analytics.md",
    "services/docs/m2-partial-reads.md",
    "services/docs/m2-problem-3.md",
    "services/docs/m2-problems-4-5.md",
    "services/docs/m2-satellite-state.md",
    "services/docs/m2-disk-state-plan.md",
    "services/docs/m2-streaming-snapshots.md",
    "services/docs/m2-critical-delivery.md",
    "services/docs/m2-read-admission.md",
    "services/docs/m2-stream-admission.md",
    "services/services/aggregator/internal/config/config.go",
    "services/services/aggregator/internal/storage/cassandra/canonical.go",
    "services/services/aggregator/internal/storage/cassandra/cassandra.go",
    "services/services/aggregator/internal/storage/cassandra/authority.go",
    "services/services/aggregator/internal/storage/cassandra/audit.go",
    "services/services/aggregator/internal/storage/cassandra/analytics.go",
    "services/services/aggregator/internal/m2/capture.go",
    "services/services/aggregator/internal/ingest/pipeline_test.go",
    "services/services/api-gateway/internal/config/config.go",
    "services/tests/load/aggregator.js",
]


def artifact(relative: str) -> dict:
    path = (ROOT / relative).resolve()
    path.relative_to(ROOT)  # Every input belongs to the intended workspace.
    data = path.read_bytes()
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def read_json(relative: str):
    return json.loads((ROOT / BASE / relative).read_text(encoding="utf-8-sig"))


def git_head(repo: str) -> str:
    directory = (ROOT / repo).as_posix()
    return subprocess.check_output(
        ["git", "-c", f"safe.directory={directory}", "-C", directory, "rev-parse", "HEAD"],
        text=True,
    ).strip()


def main() -> None:
    groups = [
        {"id": key, "title": title, "artifacts": [artifact(BASE + p) for p in paths]}
        for key, title, paths in EVIDENCE
    ]
    manifest = {
        "report_date": "2026-11-22",
        "scope": "Existing file inspection only; no live cluster or load test run.",
        "path_root": "Parent directory containing services and infrastructure checkouts",
        "repository_heads": {r: git_head(r) for r in ("services", "infrastructure", "Tugas-Besar-AAT-M1")},
        "m1_baseline": {"implementation": "122ca7b", "evidence": "4a1973b"},
        "deployed_aggregator": "d7739703983c5bb539988d159ed13644eb546ba9",
        "evidence": groups,
        "sources": [artifact(p) for p in SOURCES],
    }

    placement = read_json("problem1/live-canonical-placement.json")
    browser = read_json("problem1/browser-analytics-20261122.json")
    burst = read_json("problem1/burst-load-practice.json")
    loss = read_json("problem1/storage-loss-20261122/summary.json")
    critical = read_json("problem5/critical-reconciliation-20261122/report.json")
    ring = read_json("problem1/placement-before-loss-20261122.json")
    trace = read_json("telemetry/critical-trace-dc95832b.json")
    assert sum(placement["replica_record_counts"].values()) == 3 * placement["canonical_records_observed"]
    assert sum(browser["severity"].values()) == browser["total"]
    assert sum(v for k, v in browser["categories"].items() if k != "TSUNAMI") == browser["total"]
    assert sum(burst["burst_placement"]["replica_counts_per_node"].values()) == 3 * burst["N"]
    assert sum(loss["read_status_counts"].values()) == loss["read_sample_count"]
    assert len(critical["events"]) == critical["manifest"]["count"]
    metrics = {
        "scope": "Selected fields extracted from retained evidence; timestamps remain in the source artifacts.",
        "placement": {k: placement[k] for k in ("observed_at", "canonical_records_observed", "replica_record_counts", "limits")},
        "ring": [{k: n[k] for k in ("kubernetes_node", "pod", "state", "owns_effective", "load", "tokens")} for n in ring["nodes"]],
        "write_windows": {k: burst[k] for k in ("natural", "burst", "burst_placement")},
        "analytics": browser,
        "storage_loss": loss,
        "critical_receipts": critical,
        "trace": {k: trace[k] for k in ("trace_id", "services", "span_count", "orphan_spans", "critical_path_ms")},
    }
    out = HERE / "evidence"
    out.mkdir(exist_ok=True)
    for name, value in (("manifest.json", manifest), ("measurements.json", metrics)):
        (out / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    lines = [
        r"\begingroup\footnotesize",
        r"\begin{longtable}{@{}P{1cm}P{3.8cm}P{10.6cm}@{}}",
        r"\toprule Kode & Cakupan & Artefak relatif terhadap root workspace\\\midrule\endhead",
    ]
    for group in groups:
        paths = r"\par ".join(r"\code{" + a["path"] + "}" for a in group["artifacts"])
        lines.append(f"{group['id']} & {group['title']} & {paths}" + r"\\")
    lines += [r"\bottomrule\end{longtable}", r"\endgroup", ""]
    (out / "index.tex").write_text("\n".join(lines), encoding="utf-8")
    print(f"Prepared {len(groups)} evidence groups and {len(SOURCES)} supporting sources; numerical cross-checks passed.")


if __name__ == "__main__":
    main()
