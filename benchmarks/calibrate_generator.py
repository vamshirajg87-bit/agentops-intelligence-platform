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
                  ownership record, with exclusive journal, output and result
                  files per run
    child         (--child, started only by the orchestrator) checks the
                  series token, installs the Python network guard, then
                  imports the driver and the demo agent, runs warm-up +
                  measured, writes its result, and exits with its own status

Execution gates (all must hold; none is enough alone):
    - EXECUTION_ENABLED is True (it is False here);
    - every calibration id is in APPROVED_CALIBRATIONS (empty here);
    - --expect-head is the full 40-character Git HEAD and equals it;
    - a child runs only with the series token of the ACTIVE_CALIBRATION
      record in its output directory.
  authorize() is the only way to obtain an Authorization.  Every function
  that could start a process, import the driver or the demo, or observe the
  host requires one, so changing EXECUTION_ENABLED alone starts nothing.
  child_main() and run_series() take their dependencies explicitly; the
  production dependencies come only from the gated factories.

Network safety, stated precisely:
    - The intended denial mechanism is a Windows AppContainer with NO
      capabilities (no internetClient, internetClientServer or
      privateNetworkClientServer) and no loopback exemption: Windows then
      refuses the child's outbound and loopback connections in the kernel,
      for native code too.  The launcher here is an abstraction; its Win32
      backend is deliberately not implemented and refuses every call.
      AppContainer compatibility of CPython, the venv and the demo is NOT
      assumed: it must be shown by an isolation probe first.
    - Inside the child, as defence in depth only: a Python-level guard
      blocks socket creation, connect/connect_ex/sendto/sendmsg, and name
      resolution (getaddrinfo, getnameinfo, gethostbyname[_ex],
      gethostbyaddr, create_connection, create_server, socketpair, fromfd),
      and the native network libraries (grpc, the OTLP exporters,
      confluent_kafka, psycopg, kafka) are replaced by stubs that refuse
      to be used.  Any attempt fails the run and stops the series.
    - The orchestrator observes the child and its descendants at the OS
      level (TCP connections, UDP endpoints, loaded network DLLs), failing
      closed on any observation error.  The observation is periodic and
      cannot see name resolution done by the Windows DNS Client service.
    - Neither the Python guard nor the periodic observation guarantees
      network isolation.  They are evidence, not denial.

Proposed operational workflow (each step needs its OWN authorization; none
is performed by this file):
    1. Create the AppContainer profile APPCONTAINER_PROFILE with no
       capabilities (CreateAppContainerProfile); record its SID; confirm no
       loopback exemption (CheckNetIsolation LoopbackExempt -s).
    2. Snapshot the ACLs (icacls <path> /save) of the Python installation,
       the venv, the repository and a dedicated output directory; grant the
       profile SID read+execute on the first three and modify on the
       output directory only.
    3. Run an isolation probe in the AppContainer (no driver): the
       orchestrator listens on an ephemeral 127.0.0.1 port that the probe
       must fail to reach; getaddrinfo must fail; CPython and a demo import
       must work.  No external address is contacted.
    4. Restore the ACLs from the snapshot (icacls /restore) and verify them;
       delete the profile (DeleteAppContainerProfile) unless step 5 is
       authorized in the same window.
    5. Enabling calibration (EXECUTION_ENABLED, APPROVED_CALIBRATIONS) is a
       reviewed code change; running a calibration run or series is a
       separate authorization, followed by step 4.

Outputs go only to <temp>/agentops-calibration (never inside the repository,
never inside the benchmark directory).  Nothing in the benchmark directory
is written by this file.

Analysis is descriptive: delayed-request groups, delay propagation, burst
association, late-start distribution.  These are OBSERVATIONS; no cause of
a delay is inferred from them.

Standard library only at import time.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.abc
import importlib.machinery
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
import types
import uuid
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
#: Calibration ids that may run.  Empty: none may.
APPROVED_CALIBRATIONS: frozenset[str] = frozenset()

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
#: One OS observation may take this long before it counts as failed.
OBSERVE_TIMEOUT_SECONDS: float = 5.0
#: Longer than this between two observations is insufficient sampling.
MAX_SAMPLE_GAP_SECONDS: float = 6.0
MIN_SAMPLES: int = 1
#: Launcher and OS creation times of one process may differ by this much.
CREATION_TOLERANCE_SECONDS: float = 1.0
SERIES_BUDGET_SECONDS: float = 30 * 60

DEFAULT_OUTPUT: Path = Path(tempfile.gettempdir()) / "agentops-calibration"
BENCHMARK_OUTPUT: Path = RB.DEFAULT_OUTPUT
OWNER_FILE: str = "ACTIVE_CALIBRATION"
JOURNAL_DIR: str = "journal"

#: Exit statuses of the child.
EXIT_OK, EXIT_NETWORK, EXIT_FAILED = 0, 3, 4

#: A delayed request: start lag above this belongs to a stall group.
GROUP_MIN_LAG_SECONDS: float = 0.02
#: Requests whose implied dispatch times are this close form one group.
GROUP_TOLERANCE_SECONDS: float = 0.005
#: Arrivals closer than this to a neighbour are inside a burst.
BURST_GAP_SECONDS: float = 0.01

APPCONTAINER_PROFILE: str = "agentops.calibration.condition-a"
#: The calibration AppContainer's capabilities: none.
APPCONTAINER_CAPABILITIES: tuple[str, ...] = ()
#: Capability SIDs that would give an AppContainer network access.
NETWORK_CAPABILITY_SIDS: Mapping[str, str] = {
    "S-1-15-3-1": "internetClient",
    "S-1-15-3-2": "internetClientServer",
    "S-1-15-3-3": "privateNetworkClientServer",
}

_HEAD_RE = re.compile(r"[0-9a-f]{40}")
_TOKEN_RE = re.compile(r"[0-9a-f]{32}")


class CalibrationError(Exception):
    """The calibration cannot go on.  The message is the evidence."""


class ExecutionDisabled(CalibrationError):
    """Real calibration was requested where it is not enabled or authorized."""


class NetworkBlocked(ConnectionRefusedError):
    """A network operation was attempted inside a calibration child."""


# ---------------------------------------------------------------------------
# Execution gates
# ---------------------------------------------------------------------------

_SEAL = object()


@dataclass(frozen=True)
class Authorization:
    """Proof that authorize() passed.  Only authorize() can make a valid one."""
    calibration_ids: tuple[str, ...]
    expected_head: str
    seal: object = field(default=None, repr=False, compare=False)


def _require_enabled() -> None:
    if not EXECUTION_ENABLED:
        raise ExecutionDisabled(
            "real calibration is not enabled: an independently enforced network-denial "
            "mechanism must be in place and approved first; nothing was started")
    if not APPROVED_CALIBRATIONS:
        raise ExecutionDisabled("no calibration is approved (APPROVED_CALIBRATIONS is empty); "
                                "nothing was started")


def _require_authorization(auth: Any, calibration: Optional[str] = None) -> Authorization:
    """The gate of every production-capable function."""
    _require_enabled()
    if not isinstance(auth, Authorization) or auth.seal is not _SEAL:
        raise ExecutionDisabled("no valid calibration authorization; nothing was started")
    unapproved = sorted(set(auth.calibration_ids) - APPROVED_CALIBRATIONS)
    if unapproved:
        raise ExecutionDisabled(f"not approved: {', '.join(unapproved)}; nothing was started")
    if calibration is not None and calibration not in auth.calibration_ids:
        raise ExecutionDisabled(f"{calibration} is not covered by this authorization")
    return auth


def _git_head() -> str:
    _require_enabled()
    done = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(REPO), capture_output=True,
                          text=True, timeout=30)
    if done.returncode != 0:
        raise ExecutionDisabled("the Git HEAD cannot be read; nothing was started")
    return done.stdout.strip()


def authorize(calibration_ids: Sequence[str], expected_head: Optional[str]) -> Authorization:
    """An Authorization for exactly these approved ids at exactly this HEAD."""
    _require_enabled()
    ids = tuple(calibration_ids)
    if not ids or len(set(ids)) != len(ids):
        raise ExecutionDisabled("authorization needs distinct calibration ids")
    for cid in ids:
        if cid not in APPROVED_CALIBRATIONS:
            raise ExecutionDisabled(f"{cid} is not an approved calibration; nothing was started")
    if not isinstance(expected_head, str) or not _HEAD_RE.fullmatch(expected_head):
        raise ExecutionDisabled("--expect-head must be the full 40-character Git HEAD")
    actual = _git_head()
    if actual != expected_head:
        raise ExecutionDisabled(f"Git HEAD is {actual[:12]}, not the expected "
                                f"{expected_head[:12]}; nothing was started")
    return Authorization(ids, expected_head, _SEAL)


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


def plan_record(number: int) -> dict[str, Any]:
    """The run parameters a result must carry, exactly."""
    warm, measured = run_ids(number)
    return {"warmup": plan_config(warm, WARMUP_REQUESTS),
            "measured": plan_config(measured, MEASURED_REQUESTS),
            "concurrency": CONCURRENCY, "exporter": "in-memory",
            "late_start_seconds": LATE_START_SECONDS, "late_fraction": LATE_FRACTION}


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
# Output location, ownership, exclusive files
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


def claim_series(output_dir: Path, *, calibration_ids: Sequence[str], expected_head: str,
                 now: Callable[[], float], pid: Callable[[], int] = os.getpid,
                 new_token: Callable[[], str] = lambda: uuid.uuid4().hex) -> tuple[Path, str]:
    """Take the one ACTIVE_CALIBRATION ownership; refuse if it exists.
    Returns (record path, series token)."""
    output_dir = check_output_dir(output_dir)
    token = new_token()
    if not isinstance(token, str) or not _TOKEN_RE.fullmatch(token):
        raise CalibrationError("a series token is 32 lowercase hex characters")
    owner = output_dir / OWNER_FILE
    try:
        _exclusive_json(owner, {"condition": CONDITION, "calibration_ids": list(calibration_ids),
                                "series_token": token, "expected_head": expected_head,
                                "pid": pid(), "started_utc": _iso(now())})
    except FileExistsError:
        raise CalibrationError(f"{owner} exists: another calibration owns this directory, or "
                               "an earlier one did not finish; review it") from None
    except OSError as exc:
        raise CalibrationError(f"cannot create {owner}: {type(exc).__name__}") from None
    return owner, token


def check_ownership(output_dir: Path, number: int, series_token: Any) -> None:
    """A child runs only inside the series that owns its directory."""
    owner = Path(output_dir) / OWNER_FILE
    try:
        record = json.loads(owner.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise CalibrationError("no ACTIVE_CALIBRATION: a child runs only inside an owned "
                               "series") from None
    except (OSError, ValueError):
        raise CalibrationError("ACTIVE_CALIBRATION is unreadable or malformed; review it") \
            from None
    if not isinstance(record, dict) or not isinstance(record.get("series_token"), str) \
            or not isinstance(record.get("calibration_ids"), list):
        raise CalibrationError("ACTIVE_CALIBRATION is malformed; review it")
    if not isinstance(series_token, str) or record["series_token"] != series_token:
        raise CalibrationError("the series token does not match ACTIVE_CALIBRATION")
    if calibration_id(number) not in record["calibration_ids"]:
        raise CalibrationError(f"{calibration_id(number)} is not part of the owning series")


def result_path(output_dir: Path, number: int) -> Path:
    return Path(output_dir) / f"{calibration_id(number)}.json"


def _journal_path(output_dir: Path, stage: str, number: int) -> Path:
    return Path(output_dir) / JOURNAL_DIR / f"{stage}-{number:02d}.json"


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Python-level network guard (child only; defence in depth, not denial)
# ---------------------------------------------------------------------------

#: Native network libraries replaced by refusing stubs inside the child.
NATIVE_NETWORK_MODULES: tuple[str, ...] = (
    "grpc", "opentelemetry.exporter.otlp", "confluent_kafka", "kafka", "psycopg",
    "psycopg2", "psycopg_binary",
)


class _Refusing:
    """Stands for any name of a stubbed native network library."""

    def __init__(self, name: str, guard: "NetworkGuard") -> None:
        self._name, self._guard = name, guard

    def __getattr__(self, attr: str) -> "_Refusing":
        if attr.startswith("__"):
            raise AttributeError(attr)
        return _Refusing(f"{self._name}.{attr}", self._guard)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._guard.violations.append({"operation": "native library", "arguments": self._name})
        raise NetworkBlocked(f"{self._name} is a network library and is blocked during "
                             "calibration")


class _NativeStub(types.ModuleType):
    def __init__(self, name: str, guard: "NetworkGuard") -> None:
        super().__init__(name, "network library replaced during calibration")
        self._guard = guard

    def __getattr__(self, attr: str) -> Any:
        if attr.startswith("__"):
            raise AttributeError(attr)
        return _Refusing(f"{self.__name__}.{attr}", self._guard)


class _NativeStubFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, guard: "NetworkGuard") -> None:
        self.guard = guard

    @staticmethod
    def blocked(name: str) -> bool:
        return any(name == m or name.startswith(m + ".") for m in NATIVE_NETWORK_MODULES)

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> Any:
        if self.blocked(name):
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec: Any) -> types.ModuleType:
        return _NativeStub(spec.name, self.guard)

    def exec_module(self, module: types.ModuleType) -> None:
        self.guard.stubbed_imports.append(module.__name__)


class NetworkGuard:
    """
    Makes Python-level socket creation, connection, sending and name
    resolution raise NetworkBlocked, replaces the native network libraries
    by refusing stubs, and records each attempt.  Defence in depth: code
    that bypasses Python's socket module is not seen, so this guard does not
    guarantee isolation.
    """

    SOCKET_METHODS = ("__init__", "connect", "connect_ex", "sendto", "sendmsg")
    MODULE_FUNCTIONS = ("create_connection", "create_server", "getaddrinfo", "getnameinfo",
                        "gethostbyname", "gethostbyname_ex", "gethostbyaddr", "socketpair",
                        "fromfd")

    def __init__(self, *, block_native: bool = True) -> None:
        self.violations: list[dict[str, str]] = []
        self.stubbed_imports: list[str] = []
        self.block_native = block_native
        self._saved: list[tuple[Any, str, Any]] = []
        self._finder: Optional[_NativeStubFinder] = None

    def _blocker(self, name: str) -> Callable[..., Any]:
        def blocked(*args: Any, **kwargs: Any) -> Any:
            shown = args[1:3] if name in self.SOCKET_METHODS else args[:2]
            self.violations.append({"operation": name, "arguments": repr(shown)[:200]})
            raise NetworkBlocked(f"network operation {name} is blocked during calibration")
        return blocked

    def install(self) -> "NetworkGuard":
        import socket
        if self._saved or self._finder:
            raise CalibrationError("the network guard is already installed")
        if self.block_native:
            loaded = sorted(n for n in sys.modules if _NativeStubFinder.blocked(n))
            if loaded:
                raise CalibrationError("native network libraries are already loaded: "
                                       + ", ".join(loaded[:5]))
        targets = [(socket.socket, n) for n in self.SOCKET_METHODS] + \
                  [(socket, n) for n in self.MODULE_FUNCTIONS]
        for owner, name in targets:
            if hasattr(owner, name):                 # sendmsg / fromfd vary by platform
                self._saved.append((owner, name, getattr(owner, name)))
                setattr(owner, name, self._blocker(name))
        if self.block_native:
            self._finder = _NativeStubFinder(self)
            sys.meta_path.insert(0, self._finder)
        return self

    def uninstall(self) -> None:
        for owner, name, original in reversed(self._saved):
            setattr(owner, name, original)
        self._saved.clear()
        if self._finder is not None:
            if self._finder in sys.meta_path:
                sys.meta_path.remove(self._finder)
            for name in [n for n, m in sys.modules.items() if isinstance(m, _NativeStub)]:
                stub = sys.modules.pop(name)
                parent, _, child = name.rpartition(".")
                if parent in sys.modules and getattr(sys.modules[parent], child, None) is stub:
                    delattr(sys.modules[parent], child)
            self._finder = None

    @property
    def installed(self) -> bool:
        return bool(self._saved) or self._finder is not None


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


def result_problems(record: Any, number: int, exit_status: Optional[int]) -> list[str]:
    """Why a child's result is not a valid completed run; empty when it is."""
    if not isinstance(record, Mapping):
        return ["the result is not a JSON object"]
    problems: list[str] = []
    expected = {"calibration_id": calibration_id(number), "condition": CONDITION,
                "status": "completed", "child_exit_status": EXIT_OK}
    for key, value in expected.items():
        if record.get(key) != value:
            problems.append(f"{key} is {record.get(key)!r}, expected {value!r}")
    if exit_status != EXIT_OK:
        problems.append(f"actual child exit status {exit_status!r}")
    if record.get("child_exit_status") != exit_status:
        problems.append("the recorded child exit status differs from the actual one")
    if record.get("plan") != plan_record(number):
        problems.append("run parameters differ from the calibration plan")
    if record.get("network_guard_violations") != []:
        problems.append("network guard violations are recorded or missing")
    for stage, traces in (("warmup", WARMUP_REQUESTS), ("measured", MEASURED_REQUESTS)):
        summary = record.get(stage)
        if not isinstance(summary, Mapping):
            problems.append(f"the {stage} stage is missing")
            continue
        if summary.get("started_requests") != traces or summary.get("not_started_requests") != 0:
            problems.append(f"the {stage} stage did not start exactly {traces} requests")
        if summary.get("run_status") != "completed":
            problems.append(f"the {stage} stage status is {summary.get('run_status')!r}")
        if summary.get("diagnostic_usable") is not True:
            problems.append(f"the {stage} start-lag diagnostic is incomplete or unusable")
    return problems


# ---------------------------------------------------------------------------
# Child mode
# ---------------------------------------------------------------------------

def _import_driver(auth: Any) -> tuple[Any, Any]:
    """(execute, load_demo) of the real driver.  Called after the guard."""
    _require_authorization(auth)
    _ensure_generator_path()
    from workload_generator import real_driver
    return real_driver.execute, real_driver.load_demo


def _memory_exporter(auth: Any) -> Any:
    _require_authorization(auth)
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    return InMemorySpanExporter()


@dataclass
class ChildDeps:
    """What a child needs, always given explicitly; tests pass fakes."""
    import_driver: Callable[[], tuple[Any, Any]]
    exporter: Callable[[], Any]
    plans: Callable[[int], tuple[Any, Any]]
    clock: Callable[[], float]
    pause: Callable[[float], None]
    wall: Callable[[], float]
    guard_factory: Callable[[], NetworkGuard] = NetworkGuard


def production_child_deps(auth: Any, number: int) -> ChildDeps:
    """The real dependencies of one authorized child run."""
    _require_authorization(auth, calibration_id(number))
    return ChildDeps(import_driver=lambda: _import_driver(auth),
                     exporter=lambda: _memory_exporter(auth), plans=build_plans,
                     clock=time.monotonic, pause=time.sleep, wall=time.time)


def _os_description() -> str:
    """Platform text read in-process; platform.platform() may start a subprocess."""
    winver = getattr(sys, "getwindowsversion", None)
    if winver:
        v = winver()
        return f"{sys.platform} {v.major}.{v.minor}.{v.build}"
    return sys.platform


def child_main(number: int, output_dir: Path, deps: ChildDeps, *, series_token: str) -> int:
    """
    One calibration run: ownership -> guard -> import driver and demo ->
    warm-up -> measured -> exclusive result.  Returns the exit status.
    Every outcome after the guard, failures included, is written to the
    run's result file.
    """
    if not isinstance(deps, ChildDeps):
        raise TypeError("child_main needs explicit ChildDeps")
    output_dir = check_output_dir(output_dir)
    path = result_path(output_dir, number)
    if path.exists():
        raise CalibrationError(f"{path} exists; a calibration run is never repeated")
    check_ownership(output_dir, number, series_token)
    guard = deps.guard_factory().install()           # before any driver or demo import
    record: dict[str, Any] = {
        "calibration_id": calibration_id(number), "condition": CONDITION,
        "kind": "generator-only calibration; not a platform benchmark",
        "plan": plan_record(number),
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
        record["stubbed_native_imports"] = sorted(set(guard.stubbed_imports))
        if guard.violations and status == EXIT_OK:
            record["status"], status = "network_violation", EXIT_NETWORK
        record["finished_utc"] = _iso(deps.wall())
        record["child_exit_status"] = status
        guard.uninstall()
        _exclusive_json(path, record)
    return status


# ---------------------------------------------------------------------------
# Child command and launchers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChildCommand:
    """Everything a launcher needs to start one child, built explicitly."""
    calibration_id: str
    argv: tuple[str, ...]
    cwd: str
    env: Mapping[str, str]
    temp_dir: Path
    stdout_path: Path
    stderr_path: Path

    @property
    def command_line(self) -> str:
        """The Windows command line (CreateProcess quoting)."""
        return subprocess.list2cmdline(self.argv)

    @property
    def argv_fingerprint(self) -> str:
        return hashlib.sha256(self.command_line.encode("utf-8")).hexdigest()

    @property
    def environment_names(self) -> list[str]:
        return sorted(self.env)


def child_environment(temp_dir: Path) -> dict[str, str]:
    """The child's whole environment: an explicit allowlist, nothing more."""
    env = {name: os.environ[name] for name in ("SYSTEMROOT", "SYSTEMDRIVE", "PATH")
           if name in os.environ}
    env.update(PYTHONPATH=os.pathsep.join((str(GENERATOR_DIR), str(DEMO_DIR))),
               PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1",
               TEMP=str(temp_dir), TMP=str(temp_dir))
    return env


def _check_argument(part: str) -> str:
    if not part or any(c in part for c in '"\'!%\r\n\x00'):
        raise CalibrationError(f"unsafe command argument: {part[:60]!r}")
    return part


def build_child_command(number: int, output_dir: Path, *, series_token: str,
                        expected_head: str, interpreter: str) -> ChildCommand:
    """The exact child command.  Paths may contain spaces; quoting is done
    by list2cmdline, never by hand."""
    cid = calibration_id(number)
    if not isinstance(series_token, str) or not _TOKEN_RE.fullmatch(series_token):
        raise CalibrationError("a series token is 32 lowercase hex characters")
    if not isinstance(expected_head, str) or not _HEAD_RE.fullmatch(expected_head):
        raise CalibrationError("the expected head is the full 40-character Git HEAD")
    output_dir = Path(output_dir)
    argv = tuple(_check_argument(str(part)) for part in (
        interpreter, "-B", "-s", str(Path(__file__).resolve()), "--child", str(number),
        "--output-dir", str(output_dir), "--token", series_token, "--authorize", cid,
        "--expect-head", expected_head))
    temp_dir = output_dir / "temp" / cid
    return ChildCommand(calibration_id=cid, argv=argv, cwd=str(REPO),
                        env=child_environment(temp_dir), temp_dir=temp_dir,
                        stdout_path=output_dir / f"{cid}.stdout",
                        stderr_path=output_dir / f"{cid}.stderr")


def detached_orchestrator_command(*, interpreter: str, runs: int, output_dir: Path,
                                  expected_head: str, log_dir: Path) -> str:
    """
    The verified detached form for the future orchestrator, as text only:
        Start-Process cmd.exe -ArgumentList '/d /s /v:on /c "<cmd>
        1>LOG\\x.out 2>LOG\\x.err & >LOG\\x.exit echo exit_code=!errorlevel!"'
    The log directory must be space-free (an 8.3 path).  Never executed here.
    """
    log = str(log_dir)
    if not log or " " in log or any(c in log for c in '"\'!%&<>|^'):
        raise CalibrationError("the detached wrapper needs a space-free (8.3) log directory")
    ids: list[str] = []
    for number in range(1, runs + 1):
        ids += ["--authorize", calibration_id(number)]
    inner = subprocess.list2cmdline([_check_argument(str(p)) for p in (
        interpreter, "-B", str(Path(__file__).resolve()), "--execute", "--runs", str(runs),
        "--output-dir", str(output_dir), "--expect-head", expected_head, *ids)])
    return (f"Start-Process cmd.exe -ArgumentList '/d /s /v:on /c \"{inner} "
            f"1>{log}\\orchestrator.out 2>{log}\\orchestrator.err & "
            f">{log}\\orchestrator.exit echo exit_code=!errorlevel!\"'")


def validate_app_container_token(info: Any, expected_sid: str) -> list[str]:
    """Why a launched child's token is not the capability-free calibration
    AppContainer; empty when it is."""
    if not isinstance(info, Mapping):
        return ["no token information"]
    problems = []
    if info.get("is_app_container") is not True:
        problems.append("the process is not in an AppContainer")
    if info.get("app_container_sid") != expected_sid:
        problems.append("the AppContainer SID is not the calibration profile's")
    capabilities = info.get("capabilities")
    if not isinstance(capabilities, (list, tuple)):
        problems.append("the token capabilities are unknown")
    elif capabilities:
        named = [NETWORK_CAPABILITY_SIDS.get(c, c) for c in capabilities]
        problems.append("the token has capabilities: " + ", ".join(map(str, named)))
    if info.get("integrity_level") != "low":
        problems.append("the token integrity level is not low")
    if info.get("loopback_exempt") is not False:
        problems.append("absence of a loopback exemption is not shown")
    return problems


class AppContainerLauncher:
    """
    Starts a child suspended inside the existing calibration AppContainer
    profile, with no capabilities, checks the token, and only then resumes
    it.  It never creates the profile and never changes ACLs.  A child whose
    token is rejected never ran any code; it is discarded while suspended.
    """

    def __init__(self, auth: Any, api: Any) -> None:
        self.auth = _require_authorization(auth)
        self.api = api

    def __call__(self, command: ChildCommand, stdout: Any, stderr: Any) -> Any:
        _require_authorization(self.auth, command.calibration_id)
        if APPCONTAINER_CAPABILITIES:
            raise CalibrationError("the calibration AppContainer must have no capabilities")
        sid = self.api.profile_sid(APPCONTAINER_PROFILE)
        if not sid:
            raise CalibrationError(f"AppContainer profile {APPCONTAINER_PROFILE} does not exist; "
                                   "creating it needs its own authorization")
        process = self.api.create_suspended(command, sid, APPCONTAINER_CAPABILITIES,
                                            stdout, stderr)
        problems = validate_app_container_token(self.api.token_info(process), sid)
        if problems:
            self.api.discard_suspended(process)
            raise CalibrationError("AppContainer token rejected: " + "; ".join(problems))
        self.api.resume(process)
        return process


class Win32AppContainerApi:
    """
    The Win32 backend of AppContainerLauncher.  Deliberately not
    implemented: it is written and reviewed together with the isolation
    probe.  Every method checks the authorization and then refuses.
    """

    def __init__(self, auth: Any) -> None:
        self.auth = _require_authorization(auth)

    def _refuse(self, *args: Any, **kwargs: Any) -> Any:
        _require_authorization(self.auth)
        raise CalibrationError("the Win32 AppContainer backend is not implemented; it is "
                               "written with the isolation probe")

    profile_sid = create_suspended = token_info = resume = discard_suspended = _refuse


# ---------------------------------------------------------------------------
# OS-level observation (incomplete network evidence; fails closed)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    creation_time: str          # ISO-8601 UTC, as reported by the launcher


@dataclass
class Observation:
    problems: list[str]
    endpoints: list[str]
    processes: list[str]
    root_present: bool


def observation_script(pid: int) -> str:
    """PowerShell that lists the root process and its descendants (with
    creation times, so a reused PID is not mistaken for a descendant), their
    TCP connections, UDP endpoints and loaded network DLLs.  Any error is
    written to stderr with exit 1."""
    pid = int(pid)
    return (
        "$ErrorActionPreference = 'Stop'; try { "
        "$all = @(Get-CimInstance Win32_Process); "
        f"$root = $all | Where-Object {{ $_.ProcessId -eq {pid} }}; "
        "if (-not $root) { 'root-missing'; exit 0 }; "
        f"$ids = @({pid}); "
        "do { $n = $ids.Count; $ids = @(@($ids + ($all | Where-Object { "
        "($ids -contains $_.ParentProcessId) -and ($_.CreationDate -ge $root.CreationDate) } | "
        "ForEach-Object ProcessId)) | Sort-Object -Unique) } while ($ids.Count -ne $n); "
        "$all | Where-Object { $ids -contains $_.ProcessId } | ForEach-Object { 'proc ' + "
        "$_.ProcessId + ' ' + $_.ParentProcessId + ' ' + "
        "$_.CreationDate.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ') }; "
        "Get-NetTCPConnection | Where-Object { $ids -contains $_.OwningProcess } | "
        "ForEach-Object { 'tcp ' + $_.LocalAddress + ':' + $_.LocalPort + ' ' + "
        "$_.RemoteAddress + ':' + $_.RemotePort + ' ' + $_.State }; "
        "Get-NetUDPEndpoint | Where-Object { $ids -contains $_.OwningProcess } | "
        "ForEach-Object { 'udp ' + $_.LocalAddress + ':' + $_.LocalPort }; "
        "foreach ($id in $ids) { (Get-Process -Id $id).Modules | Where-Object { "
        "$_.ModuleName -match 'rdkafka|grpc|libpq' } | ForEach-Object { 'dll ' + $id + ' ' + "
        "$_.ModuleName } } "
        "} catch { [Console]::Error.WriteLine('observation failed: ' + "
        "$_.Exception.GetType().FullName); exit 1 }"
    )


def _parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def parse_observation(returncode: Any, stdout: str, stderr: str,
                      identity: ProcessIdentity) -> Observation:
    """An Observation; anything unexpected is a problem (fail closed)."""
    problems: list[str] = []
    if returncode != 0:
        problems.append(f"observation exited with {returncode!r}")
    if stderr.strip():
        problems.append("observation stderr: " + stderr.strip()[:300])
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    processes = [line for line in lines if line.startswith("proc ")]
    endpoints = [line for line in lines if line.split(" ", 1)[0] in ("tcp", "udp", "dll")]
    for line in lines:
        if line != "root-missing" and line not in processes and line not in endpoints:
            problems.append(f"unexpected observation output: {line[:100]}")
    root_present = False
    if "root-missing" not in lines:
        for line in processes:
            parts = line.split()
            try:
                pid, created = int(parts[1]), _parse_utc(parts[3])
                expected = _parse_utc(identity.creation_time)
            except (IndexError, ValueError):
                problems.append(f"malformed process line: {line[:100]}")
                continue
            if pid == identity.pid:
                if abs((created - expected).total_seconds()) > CREATION_TOLERANCE_SECONDS:
                    problems.append(f"PID {pid} now belongs to another process (creation time "
                                    "differs)")
                else:
                    root_present = True
        if not root_present and not any("PID" in p for p in problems):
            problems.append("the child's process identity is missing from the observation")
    if endpoints:
        problems.append("network endpoints or native network libraries observed")
    return Observation(problems, endpoints, processes, root_present)


def _run_powershell(auth: Any, script: str, timeout: float) -> tuple[int, str, str]:
    _require_authorization(auth)
    done = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
                          capture_output=True, text=True, timeout=timeout)
    return done.returncode, done.stdout, done.stderr


def observe_process(auth: Any, identity: ProcessIdentity, *,
                    runner: Optional[Callable[[str, float], tuple[int, str, str]]] = None
                    ) -> Observation:
    """One OS-level look at the child.  Incomplete network evidence: a
    connection opened and closed between looks, or name resolution done by
    the DNS Client service, is not seen."""
    _require_authorization(auth)
    run = runner or (lambda script, timeout: _run_powershell(auth, script, timeout))
    try:
        code, out, err = run(observation_script(identity.pid), OBSERVE_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        return Observation(["observation timed out"], [], [], False)
    except OSError as exc:
        return Observation([f"observation could not run: {type(exc).__name__}"], [], [], False)
    return parse_observation(code, out, err, identity)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@dataclass
class OrchestratorDeps:
    """What the orchestrator needs, always given explicitly; tests pass fakes.
    launch(command, stdout, stderr) returns a process with pid,
    creation_time, poll() and returncode."""
    launch: Callable[[ChildCommand, Any, Any], Any]
    observe: Callable[[ProcessIdentity], Observation]
    clock: Callable[[], float]
    pause: Callable[[float], None]
    now: Callable[[], float]
    interpreter: str
    expected_head: str


def production_orchestrator_deps(auth: Any) -> OrchestratorDeps:
    """The real dependencies of an authorized series."""
    auth = _require_authorization(auth)
    return OrchestratorDeps(
        launch=AppContainerLauncher(auth, Win32AppContainerApi(auth)),
        observe=lambda identity: observe_process(auth, identity),
        clock=time.monotonic, pause=time.sleep, now=time.time,
        interpreter=sys.executable, expected_head=auth.expected_head)


@dataclass
class SeriesOutcome:
    completed: int = 0
    stopped_reason: Optional[str] = None
    ownership_released: bool = False
    runs: list[dict[str, Any]] = field(default_factory=list)


def _wait_for_child(handle: Any, identity: ProcessIdentity, deps: OrchestratorDeps,
                    entry: dict[str, Any]) -> Optional[str]:
    """Poll and observe until the child exits.  A stop reason, or None."""
    cid = entry["calibration_id"]
    began = last_sample = deps.clock()
    while handle.poll() is None:
        if deps.clock() - began > CHILD_TIMEOUT_SECONDS:
            entry["child_left_running"] = True
            return (f"{cid}: no exit within {CHILD_TIMEOUT_SECONDS:.0f} s; PID {identity.pid} "
                    f"created {identity.creation_time} was not terminated")
        try:
            seen = deps.observe(identity)
        except Exception as exc:  # an observation that cannot run is a failure
            seen = Observation([f"observation raised {type(exc).__name__}"], [], [], False)
        sampled = deps.clock()
        entry["samples"] += 1
        entry["observations"].extend(seen.endpoints + seen.problems)
        if seen.endpoints:
            entry["child_left_running"] = handle.poll() is None
            return f"{cid}: OS-level observation saw network endpoints or libraries"
        if seen.problems or not seen.root_present:
            if handle.poll() is not None:
                entry["observations"].append("the last observation overlapped the child's exit")
                break
            entry["child_left_running"] = True
            reason = (seen.problems or ["the child's process identity is missing"])[0]
            return f"{cid}: observation failed closed: {reason}"
        if sampled - last_sample > MAX_SAMPLE_GAP_SECONDS:
            entry["child_left_running"] = True
            return (f"{cid}: insufficient sampling ({sampled - last_sample:.1f} s between "
                    "observations)")
        last_sample = sampled
        deps.pause(OBSERVE_INTERVAL_SECONDS)
    if entry["samples"] < MIN_SAMPLES:
        return f"{cid}: insufficient sampling (no observation while the child ran)"
    return None


def _read_result_problems(output_dir: Path, number: int, exit_status: Optional[int]
                          ) -> list[str]:
    path = result_path(output_dir, number)
    if not path.exists():
        return ["no result file"]
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [f"the result is unreadable: {type(exc).__name__}"]
    return result_problems(record, number, exit_status)


def _run_one(number: int, output_dir: Path, token: str, deps: OrchestratorDeps,
             outcome: SeriesOutcome) -> Optional[str]:
    """Launch, watch and validate one child.  A stop reason, or None."""
    command = build_child_command(number, output_dir, series_token=token,
                                  expected_head=deps.expected_head,
                                  interpreter=deps.interpreter)
    cid = command.calibration_id
    entry: dict[str, Any] = {
        "calibration_id": cid, "argv_fingerprint": command.argv_fingerprint,
        "environment_names": command.environment_names, "pid": None, "creation_time": None,
        "exit_status": None, "samples": 0, "observations": [], "result_problems": None,
        "child_left_running": False,
    }
    outcome.runs.append(entry)
    _exclusive_json(_journal_path(output_dir, "launch", number), {
        "calibration_id": cid, "argv_fingerprint": command.argv_fingerprint,
        "environment_names": command.environment_names, "utc": _iso(deps.now())})
    command.temp_dir.mkdir(parents=True, exist_ok=False)
    reason: Optional[str] = None
    handle = None
    with open(command.stdout_path, "xb") as stdout, open(command.stderr_path, "xb") as stderr:
        try:
            handle = deps.launch(command, stdout, stderr)
        except Exception as exc:
            reason = f"{cid}: launch failed: {type(exc).__name__}: {exc}"[:500]
        if handle is not None:
            identity = ProcessIdentity(int(handle.pid), str(handle.creation_time))
            entry["pid"], entry["creation_time"] = identity.pid, identity.creation_time
            _exclusive_json(_journal_path(output_dir, "process", number), {
                "calibration_id": cid, "pid": identity.pid,
                "creation_time": identity.creation_time,
                "argv_fingerprint": command.argv_fingerprint,
                "environment_names": command.environment_names, "utc": _iso(deps.now())})
            reason = _wait_for_child(handle, identity, deps, entry)
    if handle is not None and reason is None:
        entry["exit_status"] = handle.returncode
        entry["result_problems"] = _read_result_problems(output_dir, number, handle.returncode)
        if entry["result_problems"]:
            reason = f"{cid}: result rejected: {entry['result_problems'][0]}"
    _exclusive_json(_journal_path(output_dir, "complete", number),
                    {**entry, "stop_reason": reason, "utc": _iso(deps.now())})
    return reason


def run_series(runs: int, output_dir: Path, deps: OrchestratorDeps) -> SeriesOutcome:
    """
    The series, one child at a time, under ACTIVE_CALIBRATION.  Stops at
    the first run that is not clean (launch failure, timeout, observation
    failure, network evidence, non-zero exit, invalid result, the series
    budget, an interruption); never retries and never launches after a
    stop.  A series record is written in every case.  Ownership is released
    only when every run completed and validated.
    """
    if not isinstance(deps, OrchestratorDeps):
        raise TypeError("run_series needs explicit OrchestratorDeps")
    if isinstance(runs, bool) or not isinstance(runs, int) or not 1 <= runs <= MAX_RUNS:
        raise CalibrationError(f"runs must be 1-{MAX_RUNS}")
    output_dir = check_output_dir(output_dir)
    ids = [calibration_id(n) for n in range(1, runs + 1)]
    for number in range(1, runs + 1):
        for existing in (result_path(output_dir, number),
                         _journal_path(output_dir, "launch", number)):
            if existing.exists():
                raise CalibrationError(f"{existing.name} exists; calibration runs are never "
                                       "repeated")
    owner, token = claim_series(output_dir, calibration_ids=ids,
                                expected_head=deps.expected_head, now=deps.now)
    outcome = SeriesOutcome()
    series_start = deps.clock()
    try:
        for number in range(1, runs + 1):
            if deps.clock() - series_start > SERIES_BUDGET_SECONDS:
                outcome.stopped_reason = "series budget reached"
                break
            reason = _run_one(number, output_dir, token, deps, outcome)
            if reason is not None:
                outcome.stopped_reason = reason
                break
            outcome.completed += 1
            if number < runs:
                deps.pause(REST_SECONDS)
    except Exception as exc:
        outcome.stopped_reason = f"series failed: {type(exc).__name__}: {exc}"[:500]
    except BaseException as exc:
        outcome.stopped_reason = f"series interrupted: {type(exc).__name__}"
        raise
    finally:
        clean = outcome.stopped_reason is None and outcome.completed == runs
        _exclusive_json(output_dir / f"series-{CONDITION}-{int(deps.now())}.json", {
            "condition": CONDITION, "runs_requested": runs, "completed": outcome.completed,
            "stopped_reason": outcome.stopped_reason, "runs": outcome.runs,
            "ownership": "released" if clean else "retained for review",
            "note": "OS observation is periodic and cannot prove that no short-lived native "
                    "connection or name resolution happened",
        })
        if clean:
            owner.unlink()
            outcome.ownership_released = True
    return outcome


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None, *, stdout: Any = None) -> int:
    out = sys.stdout if stdout is None else stdout
    parser = argparse.ArgumentParser(prog="calibrate_generator",
                                     description="Generator-only start-lag calibration "
                                                 "(condition A). Without --execute it plans.")
    parser.add_argument("--runs", type=int, default=MAX_RUNS)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--authorize", action="append", default=[], metavar="CAL_ID")
    parser.add_argument("--expect-head", default=None)
    parser.add_argument("--child", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--token", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        output_dir = check_output_dir(Path(args.output_dir))
        if not 1 <= args.runs <= MAX_RUNS:
            raise CalibrationError(f"--runs must be 1-{MAX_RUNS}")
    except CalibrationError as exc:
        parser.error(str(exc))
    if args.child is not None:
        try:
            auth = authorize([calibration_id(args.child)], args.expect_head)
            deps = production_child_deps(auth, args.child)
            return child_main(args.child, output_dir, deps, series_token=args.token)
        except CalibrationError as exc:
            sys.stderr.write(f"calibrate_generator: error: {exc}\n")
            return 2
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
        auth = authorize(args.authorize, args.expect_head)
        if list(auth.calibration_ids) != [calibration_id(n) for n in range(1, args.runs + 1)]:
            raise ExecutionDisabled("--authorize must name exactly the calibration ids of "
                                    "--runs, in order")
        outcome = run_series(args.runs, output_dir, production_orchestrator_deps(auth))
    except CalibrationError as exc:
        sys.stderr.write(f"calibrate_generator: error: {exc}\n")
        return 2
    out.write(f"completed {outcome.completed}/{args.runs}; stopped: {outcome.stopped_reason}\n")
    return 0 if outcome.stopped_reason is None else 1


if __name__ == "__main__":
    sys.exit(main())
