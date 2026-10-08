"""
benchmarks/calibrate_generator.py

Generator-only calibration, condition A: how much do the workload
generator's own request starts lag behind their planned times, with no
AgentOps platform running at all?

The real scheduling path is reused unchanged: real_driver.execute() with its
dispatcher, semaphore, ThreadPoolExecutor, fault injector, BatchSpanProcessor
and start-lag diagnostic, driven by the real clock (time.monotonic) and the
real pause (time.sleep).  Only the span exporter differs: spans go to an
in-memory exporter, so nothing is exported anywhere.  run_real, its
reachability check and the OTLP exporter are never called.

Traffic is the SMOKE cell's: seed 42, warm-up 25 and measured 100 requests,
rate 5 per second, concurrency 32 (arrivals and planned latencies depend on
the seed only).  Run ids are cal-a-wNN / cal-a-mNN; calibration ids are
cal-a-NN, never benchmark (bm-) ids.

Two modes in this one file:
    orchestrator  plans the series; with --execute runs it, one child
                  process at a time, under an exclusive ACTIVE_CALIBRATION
                  ownership record, with one exclusive result file per run
    child         (--child, started only by the orchestrator) installs the
                  Python network guard, then imports the driver and the demo
                  agent, runs warm-up + measured, writes its result, and
                  exits with its own status

Network safety, stated precisely:
    - The child installs a Python-level guard before importing the driver or
      the demo agent: socket.socket.connect / connect_ex / sendto,
      socket.create_connection and socket.getaddrinfo raise and are counted.
      Any attempt fails the run.
    - Native code (gRPC core, librdkafka) does not go through Python's socket
      module, so the guard cannot see it.  The orchestrator therefore also
      observes the child's and its descendants' TCP/UDP endpoints at the OS
      level, periodically.  Periodic observation does NOT prove that no
      short-lived native connection happened.
    - Real execution is therefore DISABLED here (EXECUTION_ENABLED = False):
      an independently enforced network-denial mechanism (for example an
      outbound firewall rule for the child's interpreter, or a network-less
      container) must be in place and approved before it may be enabled.
      Python guards alone are not claimed to isolate the run.

Outputs go only to <temp>/agentops-calibration (never inside the repository,
never inside the benchmark directory).  Nothing in the benchmark directory -
ACTIVE_RUN, ledgers, results - is read for writing.

Analysis is descriptive: delayed-request groups, delay propagation, burst
association, late-start distribution.  These are OBSERVATIONS; no cause of
a delay is inferred from them.

Standard library only at import time.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

HERE: Path = Path(__file__).resolve().parent
REPO: Path = HERE.parent
GENERATOR_DIR: Path = REPO / "workload-generator"
DEMO_DIR: Path = REPO / "demo-app"
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import metrics as M  # noqa: E402
import run_benchmark as RB  # noqa: E402

#: Real calibration runs are not reachable until an independently enforced
#: network-denial mechanism has been put in place and approved.
EXECUTION_ENABLED: bool = False

CONDITION: str = "a"
SEED: int = 42
RATE: float = 5.0
CONCURRENCY: int = 32
WARMUP_REQUESTS: int = 25
MEASURED_REQUESTS: int = 100
MAX_RUNS: int = 20
#: The driver's late-start threshold and the benchmark's 5% rule, unchanged.
LATE_START_SECONDS: float = RB.DRIVER_LATE_START_SECONDS
LATE_FRACTION: float = M.LATE_START_FRACTION

STAGE_MAX_SECONDS: float = 120.0
CHILD_TIMEOUT_SECONDS: float = 180.0
REST_SECONDS: float = 15.0
OBSERVE_INTERVAL_SECONDS: float = 2.0
SERIES_BUDGET_SECONDS: float = 30 * 60

DEFAULT_OUTPUT: Path = Path(tempfile.gettempdir()) / "agentops-calibration"
BENCHMARK_OUTPUT: Path = RB.DEFAULT_OUTPUT
OWNER_FILE: str = "ACTIVE_CALIBRATION"

#: Exit statuses of the child.
EXIT_OK, EXIT_NETWORK, EXIT_FAILED = 0, 3, 4

#: A delayed request: start lag above this belongs to a stall group.
GROUP_MIN_LAG_SECONDS: float = 0.02
#: Requests whose implied dispatch times are this close form one group.
GROUP_TOLERANCE_SECONDS: float = 0.005
#: Arrivals closer than this to a neighbour are inside a burst.
BURST_GAP_SECONDS: float = 0.01


class CalibrationError(Exception):
    """The calibration cannot go on.  The message is the evidence."""


class ExecutionDisabled(CalibrationError):
    """Real calibration was requested where it is not enabled."""


class NetworkBlocked(ConnectionRefusedError):
    """A network operation was attempted inside a calibration child."""


# ---------------------------------------------------------------------------
# Identifiers and plans
# ---------------------------------------------------------------------------

def calibration_id(number: int) -> str:
    if isinstance(number, bool) or not isinstance(number, int) or not 1 <= number <= MAX_RUNS:
        raise CalibrationError(f"run number must be 1-{MAX_RUNS}")
    return f"cal-{CONDITION}-{number:02d}"


def run_ids(number: int) -> tuple[str, str]:
    calibration_id(number)
    return f"cal-{CONDITION}-w{number:02d}", f"cal-{CONDITION}-m{number:02d}"


def plan_config(run_id: str, traces: int) -> dict[str, Any]:
    """The arguments of the SMOKE cell's plans, for any run id."""
    return {"seed": SEED, "run_id": run_id, "scenario": "normal", "traces": traces,
            "rate": RATE, "persona": None, "mode": "real"}


def build_plans(number: int) -> tuple[Any, Any]:
    """(warm-up plan, measured plan) built by the generator's own planner."""
    _ensure_generator_path()
    from workload_generator.plan import PlanConfig, build_plan
    from workload_generator.scenarios import Scenario

    plans = []
    for run_id, traces in zip(run_ids(number), (WARMUP_REQUESTS, MEASURED_REQUESTS)):
        config = plan_config(run_id, traces)
        config["scenario"] = Scenario(config["scenario"])
        plans.append(build_plan(PlanConfig(**config)))
    return plans[0], plans[1]


def _ensure_generator_path() -> None:
    for path in (str(GENERATOR_DIR), str(DEMO_DIR)):
        if path not in sys.path:
            sys.path.insert(0, path)


# ---------------------------------------------------------------------------
# Output location, ownership, exclusive results
# ---------------------------------------------------------------------------

def _norm(path: Path) -> str:
    return os.path.normcase(os.path.realpath(str(path)))


def check_output_dir(output_dir: Path) -> Path:
    """The calibration directory, or CalibrationError when it is inside the
    repository or inside the benchmark output directory."""
    target = _norm(output_dir)
    for forbidden, why in ((REPO, "the repository"), (BENCHMARK_OUTPUT, "the benchmark "
                                                                         "output directory")):
        root = _norm(forbidden)
        if target == root or target.startswith(root + os.sep):
            raise CalibrationError(f"calibration output is never written inside {why}")
    if "agentops-benchmarks" in {part.lower() for part in Path(target).parts}:
        raise CalibrationError("calibration output is never written inside the benchmark "
                               "output directory")
    return Path(output_dir).resolve()


def _exclusive_json(path: Path, payload: Mapping[str, Any]) -> None:
    RB._exclusive_json(path, payload)


def claim_series(output_dir: Path, *, runs: int, now: Callable[[], float] = time.time,
                 pid: Callable[[], int] = os.getpid) -> Path:
    """Take the one ACTIVE_CALIBRATION ownership; refuse if it exists."""
    output_dir = check_output_dir(output_dir)
    owner = output_dir / OWNER_FILE
    try:
        _exclusive_json(owner, {"condition": CONDITION, "runs": runs, "pid": pid(),
                                "started_utc": _iso(now())})
    except FileExistsError:
        raise CalibrationError(f"{owner} exists: another calibration owns this directory, or "
                               "an earlier one did not finish; review it") from None
    except OSError as exc:
        raise CalibrationError(f"cannot create {owner}: {type(exc).__name__}") from None
    return owner


def result_path(output_dir: Path, number: int) -> Path:
    return Path(output_dir) / f"{calibration_id(number)}.json"


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Python-level network guard (child only)
# ---------------------------------------------------------------------------

class NetworkGuard:
    """
    Makes Python-level network operations raise NetworkBlocked and records
    each attempt.  It cannot see sockets opened by native code.
    """

    TARGETS = (("socket", "connect"), ("socket", "connect_ex"), ("socket", "sendto"),
               (None, "create_connection"), (None, "getaddrinfo"))

    def __init__(self) -> None:
        self.violations: list[dict[str, str]] = []
        self._saved: list[tuple[Any, str, Any]] = []

    def _blocker(self, name: str) -> Callable[..., Any]:
        def blocked(*args: Any, **kwargs: Any) -> Any:
            self.violations.append({"operation": name,
                                    "arguments": repr(args[1:3] if name in (
                                        "connect", "connect_ex", "sendto") else args[:2])[:200]})
            raise NetworkBlocked(f"network operation {name} is blocked during calibration")
        return blocked

    def install(self) -> "NetworkGuard":
        import socket
        if self._saved:
            raise CalibrationError("the network guard is already installed")
        for owner_name, name in self.TARGETS:
            owner = socket.socket if owner_name else socket
            self._saved.append((owner, name, getattr(owner, name)))
            setattr(owner, name, self._blocker(name))
        return self

    def uninstall(self) -> None:
        for owner, name, original in reversed(self._saved):
            setattr(owner, name, original)
        self._saved.clear()

    @property
    def installed(self) -> bool:
        return bool(self._saved)


# ---------------------------------------------------------------------------
# Analysis (pure)
# ---------------------------------------------------------------------------

def diagnostic_problems(start_lags: Any) -> list[str]:
    """Why a start-lag diagnostic cannot be analysed; empty when it can.
    The same checks the benchmark applies."""
    return RB._diagnostic_problems(start_lags)


def stall_groups(entries: Sequence[Sequence[float]], *,
                 min_lag: float = GROUP_MIN_LAG_SECONDS,
                 tolerance: float = GROUP_TOLERANCE_SECONDS) -> list[dict[str, Any]]:
    """
    Requests delayed by more than min_lag, grouped by their implied dispatch
    time (planned offset + lag): consecutive requests whose dispatch times
    lie within `tolerance` were released together after one interruption.
    A group of size > 1 shows delay propagation; size 1 an isolated delay.
    Observation only: nothing here says what caused the interruption.
    """
    delayed = [(int(i), float(o), float(lag), float(o) + float(lag))
               for i, o, lag in entries if lag > min_lag]
    groups: list[list[tuple[int, float, float, float]]] = []
    for item in delayed:
        if groups and abs(item[3] - groups[-1][-1][3]) <= tolerance \
                and item[0] == groups[-1][-1][0] + 1:
            groups[-1].append(item)
        else:
            groups.append([item])
    return [{
        "request_indices": [g[0] for g in group],
        "dispatch_offset_s": round(group[0][3], 6),
        "max_lag_s": round(max(g[2] for g in group), 6),
        "propagated": len(group) > 1,
    } for group in groups]


def burst_association(entries: Sequence[Sequence[float]], *,
                      late_threshold: float = LATE_START_SECONDS,
                      burst_gap: float = BURST_GAP_SECONDS) -> dict[str, Any]:
    """How many late requests lie inside planned bursts, against the share of
    all requests that do.  An association, not a cause."""
    offsets = [float(e[1]) for e in entries]
    in_burst = set()
    for i in range(1, len(offsets)):
        if offsets[i] - offsets[i - 1] < burst_gap:
            in_burst.update((i - 1, i))
    late = [i for i, e in enumerate(entries) if e[2] > late_threshold]
    return {
        "requests": len(entries), "requests_in_bursts": len(in_burst),
        "burst_share": round(len(in_burst) / len(entries), 6) if entries else None,
        "late": len(late), "late_in_bursts": sum(1 for i in late if i in in_burst),
    }


def summarize_stage(report: Mapping[str, Any]) -> dict[str, Any]:
    """The calibration record of one driver report (measured or warm-up)."""
    started = report.get("started_requests")
    late = report.get("late_starts")
    lags = report.get("start_lags")
    problems = ["no start-lag diagnostic"] if lags is None else diagnostic_problems(lags)
    usable = not problems
    entries = [list(e) for e in lags["entries"]] if usable else []
    record: dict[str, Any] = {
        "started_requests": started, "not_started_requests": report.get("not_started_requests"),
        "late_starts": late,
        "late_start_percent": (round(100.0 * late / started, 6)
                               if isinstance(late, int) and isinstance(started, int) and started
                               else None),
        "max_start_lag_seconds": report.get("max_start_lag_seconds"),
        "mean_start_lag_seconds": report.get("mean_start_lag_seconds"),
        "run_status": report.get("run_status"),
        "diagnostic_usable": usable, "diagnostic_problems": problems,
        "start_lags": lags,
        "start_lag_percentiles": None, "late_request_indices": None,
        "stall_groups": None, "burst_association": None,
    }
    if usable and entries:
        record["start_lag_percentiles"] = {k: round(v, 6) for k, v in
                                           M.percentiles([e[2] for e in entries]).items()}
        record["late_request_indices"] = [e[0] for e in entries if e[2] > LATE_START_SECONDS]
        record["stall_groups"] = stall_groups(entries)
        record["burst_association"] = burst_association(entries)
    return record


def distribution(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Late-start distribution across completed runs (measured stages)."""
    percents = [r["measured"]["late_start_percent"] for r in records
                if r.get("status") == "completed"
                and r["measured"].get("late_start_percent") is not None]
    if not percents:
        return {"runs": 0, "note": "no completed run"}
    ordered = sorted(percents)
    groups = [g for r in records if r.get("status") == "completed"
              for g in (r["measured"].get("stall_groups") or [])]
    return {
        "runs": len(percents), "median_late_percent": M.percentile(ordered, 50),
        "min_late_percent": ordered[0], "max_late_percent": ordered[-1],
        "runs_over_rule": sum(1 for p in percents if p > 100 * LATE_FRACTION),
        "stall_groups": len(groups),
        "propagated_groups": sum(1 for g in groups if g["propagated"]),
        "kind": "OBSERVATION",
    }


# ---------------------------------------------------------------------------
# Child mode
# ---------------------------------------------------------------------------

def _import_driver() -> tuple[Any, Any]:
    """(execute, load_demo) of the real driver.  Called after the guard."""
    _ensure_generator_path()
    from workload_generator import real_driver
    return real_driver.execute, real_driver.load_demo


def _memory_exporter() -> Any:
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    return InMemorySpanExporter()


@dataclass
class ChildDeps:
    """What a child needs; tests replace any of it with fakes."""
    import_driver: Callable[[], tuple[Any, Any]] = _import_driver
    exporter: Callable[[], Any] = _memory_exporter
    plans: Callable[[int], tuple[Any, Any]] = build_plans
    clock: Callable[[], float] = time.monotonic
    pause: Callable[[float], None] = time.sleep
    wall: Callable[[], float] = time.time
    guard_factory: Callable[[], NetworkGuard] = NetworkGuard


def _os_description() -> str:
    """Platform text read in-process; platform.platform() may start a subprocess."""
    winver = getattr(sys, "getwindowsversion", None)
    if winver:
        v = winver()
        return f"{sys.platform} {v.major}.{v.minor}.{v.build}"
    return sys.platform


def child_main(number: int, output_dir: Path, deps: Optional[ChildDeps] = None) -> int:
    """
    One calibration run: guard -> import driver and demo -> warm-up ->
    measured -> exclusive result.  Returns the exit status.  Every outcome,
    failures included, is written to the run's result file.
    """
    deps = deps or ChildDeps()
    output_dir = check_output_dir(output_dir)
    path = result_path(output_dir, number)
    if path.exists():
        raise CalibrationError(f"{path} exists; a calibration run is never repeated")
    guard = deps.guard_factory().install()           # before any driver or demo import
    record: dict[str, Any] = {
        "calibration_id": calibration_id(number), "condition": CONDITION,
        "kind": "generator-only calibration; not a platform benchmark",
        "plan": {"warmup": plan_config(run_ids(number)[0], WARMUP_REQUESTS),
                 "measured": plan_config(run_ids(number)[1], MEASURED_REQUESTS),
                 "concurrency": CONCURRENCY, "exporter": "in-memory",
                 "late_start_seconds": LATE_START_SECONDS, "late_fraction": LATE_FRACTION},
        "started_utc": _iso(deps.wall()), "python": platform.python_version(),
        "os": _os_description(), "status": "failed", "warmup": None, "measured": None,
        "measured_duration_seconds": None, "network_guard_violations": [],
        "observations_note": "groups, propagation and burst association are observations; "
                             "they do not identify why a start was delayed",
    }
    status = EXIT_FAILED
    try:
        execute, load_demo = deps.import_driver()
        demo = load_demo()
        warm_plan, measured_plan = deps.plans(number)
        stages = {}
        for name, plan in (("warmup", warm_plan), ("measured", measured_plan)):
            began = deps.clock()
            report = execute(plan, demo=demo, span_exporter=deps.exporter(),
                             endpoint="in-memory", concurrency=CONCURRENCY,
                             max_duration_seconds=STAGE_MAX_SECONDS, pause=deps.pause,
                             clock=deps.clock)
            stages[name] = (report.to_dict(), deps.clock() - began)
            if guard.violations:
                raise NetworkBlocked("a network operation was attempted")
        record["warmup"] = summarize_stage(stages["warmup"][0])
        record["measured"] = summarize_stage(stages["measured"][0])
        record["measured_duration_seconds"] = round(stages["measured"][1], 6)
        record["status"] = "completed"
        status = EXIT_OK
    except NetworkBlocked:
        record["status"] = "network_violation"
        status = EXIT_NETWORK
    except Exception as exc:  # recorded as evidence; never retried
        record["status"] = "failed"
        record["failure"] = f"{type(exc).__name__}: {exc}"[:500]
    finally:
        record["network_guard_violations"] = list(guard.violations)
        if guard.violations and status == EXIT_OK:
            record["status"], status = "network_violation", EXIT_NETWORK
        record["finished_utc"] = _iso(deps.wall())
        record["child_exit_status"] = status
        guard.uninstall()
        _exclusive_json(path, record)
    return status


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def child_environment() -> dict[str, str]:
    env = {name: os.environ[name] for name in ("SYSTEMROOT", "PATH", "TEMP", "TMP")
           if name in os.environ}
    env.update(PYTHONPATH=os.pathsep.join((str(GENERATOR_DIR), str(DEMO_DIR))),
               PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    return env


def observe_connections(pid: int) -> list[str]:
    """
    TCP connections and UDP endpoints owned by `pid` and its descendants, at
    the OS level (Windows).  A periodic look: a connection opened and closed
    between two looks is not seen.
    """
    script = (
        f"$ids = @({int(pid)}); $all = Get-CimInstance Win32_Process; "
        "do { $n = $ids.Count; $ids = @($ids + ($all | Where-Object { $ids -contains "
        "$_.ParentProcessId } | ForEach-Object ProcessId)) | Sort-Object -Unique } "
        "while ($ids.Count -ne $n); "
        "Get-NetTCPConnection -ErrorAction SilentlyContinue | Where-Object { $ids -contains "
        "$_.OwningProcess } | ForEach-Object { 'tcp ' + $_.LocalAddress + ':' + $_.LocalPort "
        "+ ' ' + $_.RemoteAddress + ':' + $_.RemotePort + ' ' + $_.State }; "
        "Get-NetUDPEndpoint -ErrorAction SilentlyContinue | Where-Object { $ids -contains "
        "$_.OwningProcess } | ForEach-Object { 'udp ' + $_.LocalAddress + ':' + $_.LocalPort }"
    )
    done = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
                          capture_output=True, text=True, timeout=30)
    return [line.strip() for line in done.stdout.splitlines() if line.strip()]


@dataclass
class OrchestratorDeps:
    """What the orchestrator needs; tests replace any of it with fakes."""
    launch: Callable[[int, Path], Any] = None            # -> process with poll()/pid/returncode
    observe: Callable[[int], list[str]] = observe_connections
    clock: Callable[[], float] = time.monotonic
    pause: Callable[[float], None] = time.sleep
    now: Callable[[], float] = time.time


def _launch_child(number: int, output_dir: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-B", str(Path(__file__).resolve()), "--child", str(number),
         "--output-dir", str(output_dir)],
        cwd=str(REPO), env=child_environment(), stdin=subprocess.DEVNULL,
    )


@dataclass
class SeriesOutcome:
    completed: int = 0
    stopped_reason: Optional[str] = None
    ownership_released: bool = False
    runs: list[dict[str, Any]] = field(default_factory=list)


def run_series(runs: int, output_dir: Path, deps: Optional[OrchestratorDeps] = None
               ) -> SeriesOutcome:
    """
    The series, one child at a time, under ACTIVE_CALIBRATION.  Stops at
    the first run that is not clean (non-zero exit, a network observation,
    a timeout, a missing result, the series budget); never retries.
    Ownership is released only when every run completed cleanly.
    """
    deps = deps or OrchestratorDeps()
    launch = deps.launch or _launch_child
    if not 1 <= runs <= MAX_RUNS:
        raise CalibrationError(f"runs must be 1-{MAX_RUNS}")
    output_dir = check_output_dir(output_dir)
    for number in range(1, runs + 1):
        if result_path(output_dir, number).exists():
            raise CalibrationError(f"{result_path(output_dir, number).name} exists; "
                                   "calibration runs are never repeated")
    owner = claim_series(output_dir, runs=runs, now=deps.now)
    outcome = SeriesOutcome()
    series_start = deps.clock()
    for number in range(1, runs + 1):
        if deps.clock() - series_start > SERIES_BUDGET_SECONDS:
            outcome.stopped_reason = "series budget reached"
            break
        child = launch(number, output_dir)
        seen: list[str] = []
        began = deps.clock()
        timed_out = False
        while child.poll() is None:
            if deps.clock() - began > CHILD_TIMEOUT_SECONDS:
                timed_out = True          # never killed; the evidence stays
                break
            try:
                seen.extend(deps.observe(child.pid))
            except Exception as exc:  # an observation failure is evidence too
                seen.append(f"observation failed: {type(exc).__name__}")
            deps.pause(OBSERVE_INTERVAL_SECONDS)
        entry = {"calibration_id": calibration_id(number), "pid": child.pid,
                 "exit_status": None if timed_out else child.returncode,
                 "os_observations": sorted(set(seen)),
                 "result_file": result_path(output_dir, number).name}
        outcome.runs.append(entry)
        if timed_out:
            outcome.stopped_reason = f"{entry['calibration_id']}: no exit within " \
                                     f"{CHILD_TIMEOUT_SECONDS:.0f} s; not killed"
            break
        if child.returncode != EXIT_OK:
            outcome.stopped_reason = (f"{entry['calibration_id']}: child exit status "
                                      f"{child.returncode}")
            break
        if any(not line.startswith("observation failed") for line in seen):
            outcome.stopped_reason = f"{entry['calibration_id']}: OS-level network endpoints"
            break
        if not result_path(output_dir, number).exists():
            outcome.stopped_reason = f"{entry['calibration_id']}: no result file"
            break
        outcome.completed += 1
        if number < runs:
            deps.pause(REST_SECONDS)
    _exclusive_json(output_dir / f"series-{CONDITION}-{int(deps.now())}.json", {
        "condition": CONDITION, "runs_requested": runs, "completed": outcome.completed,
        "stopped_reason": outcome.stopped_reason, "runs": outcome.runs,
        "note": "OS observation is periodic and cannot prove that no short-lived native "
                "connection happened",
    })
    if outcome.stopped_reason is None and outcome.completed == runs:
        owner.unlink()
        outcome.ownership_released = True
    return outcome


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _require_execution() -> None:
    if not EXECUTION_ENABLED:
        raise ExecutionDisabled(
            "real calibration is not enabled: an independently enforced network-denial "
            "mechanism must be in place and approved first; nothing was started")


def main(argv: Optional[Sequence[str]] = None, *, stdout: Any = None) -> int:
    out = sys.stdout if stdout is None else stdout
    parser = argparse.ArgumentParser(prog="calibrate_generator",
                                     description="Generator-only start-lag calibration "
                                                 "(condition A). Without --execute it plans.")
    parser.add_argument("--runs", type=int, default=MAX_RUNS)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--child", type=int, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        output_dir = check_output_dir(Path(args.output_dir))
        if not 1 <= args.runs <= MAX_RUNS:
            raise CalibrationError(f"--runs must be 1-{MAX_RUNS}")
    except CalibrationError as exc:
        parser.error(str(exc))
    if args.child is not None:
        try:
            _require_execution()
        except ExecutionDisabled as exc:
            sys.stderr.write(f"calibrate_generator: error: {exc}\n")
            return 2
        return child_main(args.child, output_dir)  # pragma: no cover - execution disabled
    if not args.execute:
        out.write("PLAN ONLY - nothing was started.\n")
        out.write(f"output: {output_dir}\n")
        for number in range(1, args.runs + 1):
            warm, measured = run_ids(number)
            out.write(f"{calibration_id(number)}: warm-up {warm} ({WARMUP_REQUESTS}), measured "
                      f"{measured} ({MEASURED_REQUESTS}) at {RATE:g}/s, concurrency "
                      f"{CONCURRENCY}, seed {SEED}, in-memory exporter\n")
        return 0
    try:
        _require_execution()
    except ExecutionDisabled as exc:
        sys.stderr.write(f"calibrate_generator: error: {exc}\n")
        return 2
    outcome = run_series(args.runs, output_dir)  # pragma: no cover - execution disabled
    out.write(f"completed {outcome.completed}/{args.runs}; stopped: {outcome.stopped_reason}\n")
    return 0 if outcome.stopped_reason is None else 1


if __name__ == "__main__":
    sys.exit(main())
