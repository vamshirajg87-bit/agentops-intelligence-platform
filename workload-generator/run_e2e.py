"""
workload-generator/run_e2e.py

Small end-to-end test of the workload generator on the same separate,
disposable stack as run_smoke.py, carried through to the anomaly detector:

    REAL driver -> OTLP -> collector -> Kafka -> stream processor
        -> canonical Kafka topic -> storage consumer -> PostgreSQL
        -> analytics refresh (dbt) -> anomaly detector -> anomaly_events

Three workloads are sent, one after the other, with one request in flight
at a time so that spans are stored in the order they were planned:

    f-warm-002   normal             30 requests   history for the detectors
    f-slow-002   slow-tool          40 requests   12 slow tool calls
    f-fail-002   repeated-failure   40 requests   12 failed tool calls

Then the analytics models are rebuilt with the sanctioned refresh, the
existing anomaly detector runs twice with detector_config.json, and its
rows are reconciled with the plans:

    exact     the failure path: which requests are reported, by which
              detector, with which severity; and that a second detector run
              inserts nothing
    bounded   the latency path: real durations carry overhead, so the test
              asks for at least 10 of the 12 slow requests per signal and at
              most 3 healthy requests per signal

It only ORCHESTRATES.  The stack, the readiness checks, the execution and
reconciliation of a stage and the cleanup are run_smoke.py's, imported and
unchanged.  The detector's algorithms and thresholds are the existing ones;
detector_config.json only selects which of its groups run.

The first failure stops the run; nothing is retried or adjusted.  Cleanup
always runs.

    python workload-generator/run_e2e.py [--expect-head <commit>]

Exit codes:
    0  everything reconciled and cleanup was verified
    1  anything else

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_smoke as smoke  # noqa: E402
from run_smoke import Checks, SmokeFailure, Stage, say, section  # noqa: E402

DETECTOR_CONFIG: str = "workload-generator/detector_config.json"
REFRESH_SERVICE: str = "analytics-refresh"
REFRESH_PROFILE: str = "analytics-refresh"

#: What may differ from the commit while these files are under review.
ALLOWED_CHANGES: frozenset[str] = frozenset({
    "?? workload-generator/detector_config.json",
    "?? workload-generator/run_e2e.py",
    " M workload-generator/tests/unit/test_cli.py",
})

#: One request in flight at a time: spans are stored in planned order, which
#: the exact failure expectations below depend on.
CONCURRENCY: int = 1

# Seconds.
T_DRAIN: float = 120.0
T_REFRESH: float = 300.0
T_DETECTOR: float = 120.0

WARM = Stage(
    run_id="f-warm-002", scenario="normal", traces=30, max_duration=60,
    digest_prefix="7a58209c592ddb3e",
    successes=30, failures=0, failing_indexes=(),
    spans=210, trace_sizes={"7": 30}, status={"UNSET": 210},
    error_spans={}, error_types={}, scenario_roots={"normal": 30},
)
SLOW = Stage(
    run_id="f-slow-002", scenario="slow-tool", traces=40, max_duration=90,
    digest_prefix="82cb25d0b6292938",
    successes=40, failures=0, failing_indexes=(),
    spans=280, trace_sizes={"7": 40}, status={"UNSET": 280},
    error_spans={}, error_types={}, scenario_roots={"normal": 28, "slow-tool": 12},
)
FAIL = Stage(
    run_id="f-fail-002", scenario="repeated-failure", traces=40, max_duration=90,
    digest_prefix="db61f197d0da884a",
    successes=28, failures=12, failing_indexes=tuple(range(20, 32)),
    spans=256, trace_sizes={"7": 28, "5": 12}, status={"UNSET": 232, "ERROR": 24},
    error_spans={smoke.ROOT_SPAN: 12, smoke.TOOL_SPAN: 12},
    error_types={"ToolExecutionError": 12},
    scenario_roots={"normal": 28, "repeated-failure": 12},
)
STAGES: tuple[Stage, ...] = (WARM, SLOW, FAIL)

TOTAL_REQUESTS: int = sum(stage.traces for stage in STAGES)
TOTAL_SPANS: int = sum(stage.spans for stage in STAGES)
FAILED_TRACES: int = FAIL.failures
#: Indexes of the slow requests of the slow-tool run.
SLOW_INDEXES: tuple[int, ...] = tuple(range(20, 32))

# The detector's own rules, restated only as what this workload must produce.
#: An error is reported as INFO when it is the first of its group, and as
#: CRITICAL when it completes this many consecutive errors.
PERSISTENCE: int = 5
FAILURE_ERROR_TYPE: str = "ToolExecutionError"
FAILURE_EVENTS: int = 1 + (FAIL.failures - (PERSISTENCE - 1))

MIN_SLOW_FLAGGED: int = 10
MAX_HEALTHY_FLAGGED: int = 3

GROUPS: tuple[tuple[str, str], ...] = (
    ("trace_latency", smoke.ROOT_SPAN),
    ("tool_latency", ""),
    ("error_rate", smoke.ROOT_SPAN),
    ("tool_failure", ""),
)


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

def planned_requests(stage: Stage) -> list[dict[str, Any]]:
    """
    The requests of a stage as the generator's own planner orders them:
    index, request id, planned scenario, and whether the tool call is
    planned to fail.  Computed by the generator's code; nothing is planned
    here.
    """
    assert smoke.RUN.python is not None
    code = (
        "import json\n"
        "from workload_generator.plan import PlanConfig, build_plan\n"
        "from workload_generator.scenarios import Scenario\n"
        f"plan = build_plan(PlanConfig(seed={smoke.SEED}, run_id={stage.run_id!r},"
        f" scenario=Scenario({stage.scenario!r}), traces={stage.traces},"
        f" rate={float(smoke.RATE)!r}, persona=None, mode='real'))\n"
        "print(json.dumps([{'index': r.request_index, 'request_id': r.request_id,"
        " 'scenario': r.scenario.value, 'failing': r.fault is not None,"
        " 'retries': len(r.retries)} for r in plan.requests]))\n"
    )
    return json.loads(smoke.checked(
        [str(smoke.RUN.python), "-c", code], cwd=smoke.REPO / "workload-generator",
        timeout=smoke.T_COMMAND, what="planned requests", env=smoke.host_environment(),
    ))


def check_plans(plans: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    """The plans are the approved ones before anything is sent."""
    checks = Checks()
    checks.equal("requests in all", sum(len(plan) for plan in plans.values()), TOTAL_REQUESTS)
    checks.equal("planned retries in all",
                 sum(r["retries"] for plan in plans.values() for r in plan), 0)
    checks.equal("slow-tool requests",
                 tuple(r["index"] for r in plans[SLOW.run_id] if r["scenario"] == "slow-tool"),
                 SLOW_INDEXES)
    checks.equal("repeated-failure requests",
                 tuple(r["index"] for r in plans[FAIL.run_id] if r["failing"]),
                 FAIL.failing_indexes)
    checks.equal("failing requests outside the failure run",
                 sum(r["failing"] for run_id, plan in plans.items()
                     if run_id != FAIL.run_id for r in plan), 0)
    checks.equal("spans expected in all", TOTAL_SPANS, 746)
    checks.finish("plans")


def expected_failure_events(plan: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """
    request id -> severity the detectors' rules give the failing requests of
    one run, taken in planned order with no error before them: the first is
    INFO, the next ones are not reported, and from the one that completes
    PERSISTENCE consecutive errors on, each is CRITICAL.
    """
    failing = [r for r in sorted(plan, key=lambda r: r["index"]) if r["failing"]]
    expected = {}
    for position, request in enumerate(failing, start=1):
        if position == 1:
            expected[request["request_id"]] = "INFO"
        elif position >= PERSISTENCE:
            expected[request["request_id"]] = "CRITICAL"
    return expected


# ---------------------------------------------------------------------------
# Analytics refresh
# ---------------------------------------------------------------------------

def refresh_command(*args: str) -> list[str]:
    """A Compose command for the refresh service, which belongs to a profile."""
    return smoke.compose_command("--profile", REFRESH_PROFILE, *args)


def build_refresh_image() -> None:
    """Built before any traffic, so that the detector runs soon after it."""
    section("analytics refresh image")
    smoke.RUN.compose_used = True
    build = smoke.run(refresh_command("build", REFRESH_SERVICE), timeout=smoke.T_BUILD,
                      what="compose build analytics-refresh")
    smoke.write_log("build-refresh.log", build.stdout + build.stderr)
    if build.returncode != 0:
        raise SmokeFailure("building the refresh image failed:\n"
                           + "\n".join((build.stdout + build.stderr).splitlines()[-10:]))
    say("built")


def ledger_fingerprint() -> str:
    return smoke.psql(
        "SELECT count(*) || ' steps, ' || md5(string_agg(position || ':' || step || ':' "
        "|| checksum, ',' ORDER BY position)) FROM agentops_bootstrap.ledger"
    )


def refresh_analytics() -> None:
    """The sanctioned refresh: it refuses any database the bootstrap has not
    completed, rebuilds the models, re-applies and verifies the grants."""
    section("analytics refresh")
    smoke.check_host_processes()
    before = ledger_fingerprint()
    result = smoke.run(refresh_command("run", "--rm", "-T", REFRESH_SERVICE),
                       timeout=T_REFRESH, what="analytics refresh")
    output = result.stdout + result.stderr
    smoke.write_log("analytics-refresh.log", output)
    say(f"analytics-refresh exit code {result.returncode}")
    for line in output.strip().splitlines()[-4:]:
        say("  " + line[:200])
    if result.returncode != 0:
        raise SmokeFailure("the analytics refresh failed")
    checks = Checks()
    checks.equal("ledger unchanged by the refresh", ledger_fingerprint(), before)
    checks.equal("ledger steps", before.split(" ")[0], str(smoke.LEDGER_STEPS))
    found = json.loads(smoke.psql(
        "SELECT json_build_object("
        "'stg_spans', (SELECT count(*) FROM analytics.stg_telemetry_spans),"
        "'traces', (SELECT count(*) FROM analytics.mart_trace_metrics),"
        "'request_traces', (SELECT count(*) FROM analytics.mart_trace_metrics"
        f" WHERE root_service_name = '{smoke.SERVICE_NAME}' AND root_span_name = '{smoke.ROOT_SPAN}'),"
        "'error_traces', (SELECT count(*) FROM analytics.mart_trace_metrics WHERE has_error),"
        "'trace_spans', (SELECT coalesce(sum(span_count), 0) FROM analytics.mart_trace_metrics),"
        "'tool_spans', (SELECT count(*) FROM analytics.stg_telemetry_spans"
        f" WHERE span_name = '{smoke.TOOL_SPAN}'),"
        "'anomaly_rows', (SELECT count(*) FROM public.anomaly_events),"
        "'checkpoints', (SELECT count(*) FROM public.anomaly_detector_runs))"
    ))
    checks.equal("analytics.stg_telemetry_spans rows", found["stg_spans"], TOTAL_SPANS)
    checks.equal("analytics.mart_trace_metrics traces", found["traces"], TOTAL_REQUESTS)
    checks.equal("traces rooted at the request span", found["request_traces"], TOTAL_REQUESTS)
    checks.equal("traces with has_error", found["error_traces"], FAILED_TRACES)
    checks.equal("spans counted by the trace mart", found["trace_spans"], TOTAL_SPANS)
    checks.equal("tool.execute spans", found["tool_spans"], TOTAL_REQUESTS)
    checks.equal("anomaly_events rows before the detector", found["anomaly_rows"], 0)
    checks.equal("detector checkpoints before the detector", found["checkpoints"], 0)
    checks.finish("analytics refresh")


# ---------------------------------------------------------------------------
# Anomaly detector
# ---------------------------------------------------------------------------

_GROUP_LINE = re.compile(r"\[(\w+) / ([^/\]]+?) / ([^\]]*)\]\s*$")
_FIELD_LINE = re.compile(r"\s(\w+)\s*: (.*)$")


def parse_detector_log(text: str) -> tuple[list[dict[str, str]], dict[str, str]]:
    """
    (groups, summary) from the detector's log: one dict of its printed fields
    per group, in order, each with signal_path / service_name /
    operation_name; and the fields printed after "run complete".
    """
    groups: list[dict[str, str]] = []
    summary: dict[str, str] = {}
    target: Optional[dict[str, str]] = None
    for line in text.splitlines():
        header = _GROUP_LINE.search(line)
        if header:
            target = {"signal_path": header.group(1), "service_name": header.group(2),
                      "operation_name": header.group(3)}
            groups.append(target)
            continue
        if line.rstrip().endswith("run complete"):
            target = summary
            continue
        field = _FIELD_LINE.search(line)
        if field and target is not None:
            target[field.group(1)] = field.group(2).strip()
    return groups, summary


def run_detector(label: str) -> tuple[list[dict[str, str]], dict[str, str]]:
    """One pass of the existing detector, as its own role, on the isolated
    database."""
    assert smoke.RUN.python is not None
    result = smoke.run(
        [str(smoke.RUN.python), "main.py", "--config", str(smoke.REPO / DETECTOR_CONFIG)],
        cwd=smoke.REPO / "anomaly-detector", timeout=T_DETECTOR, what=f"detector {label}",
        env=smoke.host_environment(
            ANOMALY_DETECTOR_DB_HOST="127.0.0.1",
            ANOMALY_DETECTOR_DB_PORT=str(smoke.POSTGRES_PORT),
            ANOMALY_DETECTOR_DB_NAME="agentops",
            ANOMALY_DETECTOR_DB_USER="anomaly_detector",
            ANOMALY_DETECTOR_DB_PASSWORD=smoke.RUN.generated["ANOMALY_DETECTOR_DB_PASSWORD"],
        ),
    )
    output = result.stdout + result.stderr
    smoke.write_log(f"detector-{label}.log", output)
    wrong = smoke._WRONG_STACK.search(output)
    if wrong:
        raise SmokeFailure(f"detector {label}: mentions the normal stack's port {wrong.group(1)}")
    say(f"detector exit code {result.returncode}")
    if result.returncode != 0:
        raise SmokeFailure(f"detector {label} failed:\n" + "\n".join(output.splitlines()[-12:]))
    groups, summary = parse_detector_log(output)
    for group in groups:
        say("  %-13s %-17s status %s, candidates %s, emitted %s, inserted %s, duplicate %s" % (
            group["signal_path"], group["operation_name"] or "-", group.get("status"),
            group.get("candidates"), group.get("emitted"), group.get("inserted"),
            group.get("duplicate")))
    return groups, summary


def check_detector_run(
    groups: Sequence[Mapping[str, str]], summary: Mapping[str, str], checks: Checks,
) -> None:
    """What holds for every detector pass."""
    checks.equal("groups, in configured order",
                 [(g["signal_path"], g["operation_name"]) for g in groups], list(GROUPS))
    checks.equal("service of every group", sorted({g["service_name"] for g in groups}),
                 [smoke.SERVICE_NAME])
    checks.equal("group statuses", sorted({g.get("status") for g in groups}), ["ok"])
    checks.equal("checkpoints advanced",
                 sorted({g.get("checkpoint_advanced") for g in groups}), ["True"])
    checks.equal("groups attempted / ok / uncertain / error",
                 [summary.get("groups_attempted"), summary.get("groups_ok"),
                  summary.get("groups_uncertain"), summary.get("groups_error")],
                 [str(len(GROUPS)), str(len(GROUPS)), "0", "0"])
    checks.equal("completed all groups", summary.get("completed_all_groups"), "True")
    checks.equal("totals equal the sum of the groups",
                 [int(summary.get(name, -1)) for name in
                  ("total_candidates", "total_emitted", "total_inserted", "total_duplicate")],
                 [sum(int(g.get(name, 0)) for g in groups) for name in
                  ("candidates", "emitted", "inserted", "duplicate")])


_EVENTS_SQL = """
WITH roots AS (
    SELECT trace_id::text AS trace_id,
           coalesce(request_id, attributes->>'agentops.request_id') AS request_id,
           attributes->>'agentops.workload.run_id' AS run_id,
           attributes->>'agentops.workload.scenario' AS scenario,
           status_code = 'ERROR' AS root_error
    FROM telemetry_spans
    WHERE parent_span_id IS NULL AND attributes ? 'agentops.workload.run_id'
)
SELECT json_build_object(
  'rows', (SELECT count(*) FROM public.anomaly_events),
  'checkpoints', (SELECT count(*) FROM public.anomaly_detector_runs),
  'events', (SELECT coalesce(json_agg(json_build_object(
      'anomaly_type', a.anomaly_type, 'signal_name', a.signal_name,
      'detector_name', a.detector_name, 'severity', a.severity,
      'service_name', a.service_name, 'operation_name', a.operation_name,
      'span', a.span_id IS NOT NULL, 'run_id', r.run_id, 'scenario', r.scenario,
      'request_id', r.request_id, 'root_error', r.root_error)), '[]')
    FROM public.anomaly_events a LEFT JOIN roots r ON r.trace_id = a.trace_id)
)
"""


def read_anomalies() -> dict[str, Any]:
    """Every anomaly row with the workload request its trace belongs to."""
    return json.loads(smoke.psql(_EVENTS_SQL))


def reconcile_anomalies(
    events: Sequence[Mapping[str, Any]],
    plans: Mapping[str, Sequence[Mapping[str, Any]]],
    checks: Checks,
) -> None:
    """
    Compare the stored anomaly rows with the plans.  Pure: it reads nothing.

    The failure path is exact.  The latency path is bounded, because the
    durations the detector scores are measured, not planned.
    """
    run_ids = sorted(plans)
    checks.equal("events outside the workload's traces",
                 sum(1 for e in events if e["run_id"] not in run_ids), 0)
    checks.equal("service of every event", sorted({e["service_name"] for e in events}, key=str),
                 [smoke.SERVICE_NAME])
    checks.equal("anomaly types", sorted({e["anomaly_type"] for e in events}),
                 ["error_rate", "latency", "tool_failure"])

    # -- failures: exact ---------------------------------------------------
    expected = expected_failure_events(plans[FAIL.run_id])
    failing_ids = {r["request_id"] for r in plans[FAIL.run_id] if r["failing"]}
    checks.equal("failure events expected per detector", len(expected), FAILURE_EVENTS)
    for anomaly_type, detector, operation in (
        ("tool_failure", "ToolFailureDetector", f"{smoke.TOOL_SPAN}/{FAILURE_ERROR_TYPE}"),
        ("error_rate", "ErrorRateDetector", smoke.ROOT_SPAN),
    ):
        found = [e for e in events if e["anomaly_type"] == anomaly_type]
        checks.equal(f"{anomaly_type} events", len(found), FAILURE_EVENTS)
        checks.equal(f"{anomaly_type} run ids", sorted({e["run_id"] for e in found}, key=str),
                     [FAIL.run_id])
        checks.equal(f"{anomaly_type} detector / operation",
                     sorted({(e["detector_name"], e["operation_name"]) for e in found}, key=str),
                     [(detector, operation)])
        checks.equal(f"{anomaly_type} request -> severity equals the expected mapping",
                     {e["request_id"]: e["severity"] for e in found} == expected, True)
        checks.equal(f"{anomaly_type} severities",
                     smoke._tally(e["severity"] for e in found),
                     smoke._tally(expected.values()))
        checks.equal(f"{anomaly_type} events on requests not planned to fail",
                     sum(1 for e in found if e["request_id"] not in failing_ids), 0)
        checks.equal(f"{anomaly_type} events on traces that did not fail",
                     sum(1 for e in found if not e["root_error"]), 0)
    reported = {
        anomaly_type: sorted(
            (e["request_id"] for e in events if e["anomaly_type"] == anomaly_type), key=str,
        )
        for anomaly_type in ("tool_failure", "error_rate")
    }
    checks.equal("tool_failure and error_rate report the same requests",
                 reported["tool_failure"], reported["error_rate"])

    # -- latency: bounded --------------------------------------------------
    slow_ids = {r["request_id"] for r in plans[SLOW.run_id] if r["scenario"] == "slow-tool"}
    latency = [e for e in events if e["anomaly_type"] == "latency"]
    checks.equal("latency detector / signal",
                 sorted({(e["detector_name"], e["signal_name"]) for e in latency}),
                 [("LatencyDetector", "duration_ms")])
    checks.equal("latency events on failed traces",
                 sum(1 for e in latency if e["root_error"]), 0)
    checks.equal("latency events on requests planned to fail",
                 sum(1 for e in latency if e["request_id"] in failing_ids), 0)
    for name, operation, has_span in (
        ("trace_latency", smoke.ROOT_SPAN, False),
        ("tool_latency", smoke.TOOL_SPAN, True),
    ):
        found = [e for e in latency if e["operation_name"] == operation]
        checks.equal(f"{name} events are {'span' if has_span else 'trace'} level",
                     sorted({e["span"] for e in found}), [has_span])
        on_slow = {e["request_id"] for e in found if e["request_id"] in slow_ids}
        on_healthy = [e for e in found if e["request_id"] not in slow_ids]
        checks.true(f"{name}: slow requests flagged, at least {MIN_SLOW_FLAGGED} of "
                    f"{len(slow_ids)}", len(on_slow) >= MIN_SLOW_FLAGGED, len(on_slow))
        checks.true(f"{name}: healthy requests flagged, at most {MAX_HEALTHY_FLAGGED}",
                    len(on_healthy) <= MAX_HEALTHY_FLAGGED, len(on_healthy))
        checks.equal(f"{name}: at most one event per request",
                     len(found), len({e["request_id"] for e in found}))
        say(f"           {name} severities on slow requests: "
            f"{smoke._tally(e['severity'] for e in found if e['request_id'] in slow_ids)}; "
            f"healthy requests flagged by run: "
            f"{smoke._tally(e['run_id'] for e in on_healthy)}")
    checks.equal("latency events of other operations",
                 sum(1 for e in latency
                     if e["operation_name"] not in (smoke.ROOT_SPAN, smoke.TOOL_SPAN)), 0)
    above_info = sum(1 for e in latency
                     if e["request_id"] in slow_ids and e["severity"] != "INFO")
    checks.true("latency events above INFO on slow requests, at least 1",
                above_info >= 1, above_info)


def detect_and_reconcile(plans: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    section("anomaly detector: first run")
    groups, summary = run_detector("run-1")
    stored = read_anomalies()
    checks = Checks()
    check_detector_run(groups, summary, checks)
    checks.equal("candidates of every group", [g.get("candidates") for g in groups],
                 [str(TOTAL_REQUESTS)] * len(GROUPS))
    checks.equal("prior checkpoints", sorted({g.get("prior_checkpoint") for g in groups}),
                 ["None"])
    checks.equal("inserted equals emitted", summary.get("total_inserted"),
                 summary.get("total_emitted"))
    checks.equal("duplicates", summary.get("total_duplicate"), "0")
    checks.equal("anomaly_events rows equal the inserted total", stored["rows"],
                 int(summary.get("total_inserted", -1)))
    checks.equal("detector checkpoints", stored["checkpoints"], len(GROUPS))
    emitted = {g["signal_path"]: int(g.get("emitted", -1)) for g in groups}
    by_path = {
        "trace_latency": sum(1 for e in stored["events"] if e["anomaly_type"] == "latency"
                             and e["operation_name"] == smoke.ROOT_SPAN),
        "tool_latency": sum(1 for e in stored["events"] if e["anomaly_type"] == "latency"
                            and e["operation_name"] == smoke.TOOL_SPAN),
        "error_rate": sum(1 for e in stored["events"] if e["anomaly_type"] == "error_rate"),
        "tool_failure": sum(1 for e in stored["events"] if e["anomaly_type"] == "tool_failure"),
    }
    checks.equal("rows per signal path equal what each group emitted", by_path, emitted)
    checks.finish("detector run 1")

    section("anomaly reconciliation")
    checks = Checks()
    reconcile_anomalies(stored["events"], plans, checks)
    checks.finish("anomaly reconciliation")

    section("anomaly detector: second run")
    groups, summary = run_detector("run-2")
    again = read_anomalies()
    checks = Checks()
    check_detector_run(groups, summary, checks)
    checks.equal("inserted", summary.get("total_inserted"), "0")
    checks.equal("every emitted event is a duplicate", summary.get("total_duplicate"),
                 summary.get("total_emitted"))
    checks.equal("anomaly_events rows unchanged", again["rows"], stored["rows"])
    checks.equal("anomaly rows identical to the first run",
                 sorted(json.dumps(e, sort_keys=True) for e in again["events"])
                 == sorted(json.dumps(e, sort_keys=True) for e in stored["events"]), True)
    checks.equal("detector checkpoints", again["checkpoints"], len(GROUPS))
    say(f"  emitted again {summary.get('total_emitted')} of "
        f"{stored['rows']} (only observations inside the overlap are evaluated again)")
    checks.finish("detector run 2")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def configure_harness() -> None:
    """The D3 harness, unchanged on disk, with this test's values in place of
    its own: its stages, one request in flight, a longer drain allowance for
    the larger runs, and the files of this test as the allowed changes."""
    smoke.STAGES = STAGES
    smoke.CONCURRENCY = CONCURRENCY
    smoke.T_DRAIN = T_DRAIN
    smoke.ALLOWED_UNTRACKED = ALLOWED_CHANGES


def e2e(expect_head: Optional[str]) -> None:
    smoke.check_repository(expect_head)
    if not (smoke.REPO / DETECTOR_CONFIG).is_file():
        raise SmokeFailure(f"missing {DETECTOR_CONFIG}")
    smoke.check_ports()
    smoke.record_docker_state()
    smoke.create_workspace()
    smoke.create_virtual_environment()
    smoke.check_render()

    section("plans")
    plans = {stage.run_id: planned_requests(stage) for stage in STAGES}
    check_plans(plans)

    smoke.start_kafka()
    smoke.create_topics()
    smoke.start_postgres()
    smoke.run_bootstrap()
    build_refresh_image()
    smoke.start_collector()
    smoke.start_host_services()
    offsets = smoke.check_baseline()
    for stage in STAGES:
        # A stage that does not reconcile raises; nothing after it runs.
        offsets = smoke.run_stage(stage, offsets)
        smoke.check_normal_stack(f"after {stage.run_id}")
    smoke.check_totals(offsets)

    refresh_analytics()
    detect_and_reconcile(plans)
    smoke.check_host_processes()
    smoke.check_normal_stack("after the detector")


def remove_profile_resources() -> None:
    """The refresh service belongs to a profile, which a plain `down` leaves
    out; this one includes it, so that its image is removed with the rest."""
    if not (smoke.RUN.compose_used and smoke.RUN.env_file is not None
            and smoke.RUN.env_file.is_file()):
        return
    down = smoke.run(
        refresh_command("down", "--volumes", "--rmi", "local", "--remove-orphans"),
        timeout=smoke.T_DOWN, what="compose down",
    )
    say(f"compose --profile {REFRESH_PROFILE} down --volumes --rmi local --remove-orphans: "
        f"exit {down.returncode}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_e2e",
        description="Small end-to-end test of the workload generator, through to the "
                    "anomaly detector, on a disposable stack.",
    )
    parser.add_argument("--expect-head", default=None, metavar="COMMIT",
                        help="refuse to run unless HEAD is exactly this commit")
    args = parser.parse_args(argv)
    if sys.version_info[:2] != (3, 13):
        parser.error("run_e2e needs Python 3.13")
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(errors="replace")
    configure_harness()

    passed = False
    try:
        e2e(args.expect_head)
        passed = True
    except SmokeFailure as failure:
        section("FAILURE")
        say(str(failure))
    except KeyboardInterrupt:
        section("FAILURE")
        say("interrupted")
    except Exception as exc:  # anything unforeseen is a failure, and is cleaned up too
        section("FAILURE")
        say(f"{type(exc).__name__}: {exc}")
    try:
        section("cleanup of the refresh service")
        smoke.stop_host_processes()
        remove_profile_resources()
        clean = smoke.cleanup()
    except Exception as exc:
        clean = False
        say(f"CLEANUP PROBLEM: {type(exc).__name__}: {exc}")

    section("result")
    say(f"small end-to-end test: {'PASSED' if passed else 'FAILED'}")
    say(f"cleanup verified: {'yes' if clean else 'NO'}")
    return 0 if passed and clean else 1


if __name__ == "__main__":
    sys.exit(main())
