"""
benchmarks/tests/test_calibration.py

Offline tests of the generator-only calibration harness (condition A).
No calibration runs, no demo agent, no Docker, no network: children, the
clock and the driver are fakes, and any process or socket attempt that is
not an explicit fake fails the test.
"""

from __future__ import annotations

import importlib.abc
import io
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_BENCH = Path(__file__).resolve().parents[1]
_REPO = _BENCH.parent
if str(_BENCH) not in sys.path:
    sys.path.insert(0, str(_BENCH))

import calibrate_generator as C  # noqa: E402
import metrics as M  # noqa: E402
import run_benchmark as RB  # noqa: E402


class LiveAccessAttempt(AssertionError):
    """A test reached something live."""


class _BlockHarness(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name in ("run_smoke", "run_e2e"):
            raise ImportError(f"{name} is blocked in the calibration tests")
        return None


@pytest.fixture(autouse=True)
def fail_closed(monkeypatch):
    def refuse(what):
        def call(*args, **kwargs):
            raise LiveAccessAttempt(f"{what} attempted")
        return call

    monkeypatch.setattr(sys, "meta_path", [_BlockHarness(), *sys.meta_path])
    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, refuse(f"subprocess.{name}"))
    monkeypatch.setattr(os, "system", refuse("os.system"))
    monkeypatch.setattr(socket, "create_connection", refuse("a network connection"))
    monkeypatch.setattr(socket.socket, "connect", refuse("a socket connection"))
    monkeypatch.setattr(C, "observe_connections", refuse("OS-level observation"))


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.pauses = []

    def clock(self):
        return self.now

    def pause(self, seconds):
        self.pauses.append(seconds)
        self.now += seconds


def _report(traces, late=0, lags=None, **overrides):
    lags = [0.001] * traces if lags is None else lags
    entries = [[i, round(0.2 * i, 6), lag] for i, lag in enumerate(lags)]
    report = {"started_requests": traces, "not_started_requests": 0, "late_starts": late,
              "max_start_lag_seconds": max(lags), "mean_start_lag_seconds": sum(lags) / traces,
              "run_status": "completed",
              "start_lags": {"limit": 10000, "expected": traces, "recorded": traces,
                             "complete": True, "truncated_reason": None, "entries": entries}}
    report.update(overrides)
    return SimpleNamespace(to_dict=lambda: report)


def _fake_deps(clock, *, execute=None, calls=None, import_check=None):
    calls = [] if calls is None else calls

    def default_execute(plan, **kwargs):
        calls.append((plan.traces, kwargs))
        clock.now += 20.0
        return _report(plan.traces)

    def import_driver():
        if import_check:
            import_check()
        return (execute or default_execute), (lambda: "demo")

    return C.ChildDeps(
        import_driver=import_driver, exporter=lambda: "memory-exporter",
        plans=lambda n: (SimpleNamespace(traces=C.WARMUP_REQUESTS),
                         SimpleNamespace(traces=C.MEASURED_REQUESTS)),
        clock=clock.clock, pause=clock.pause, wall=lambda: 1_800_000_000.0)


class FakeChild:
    def __init__(self, returncode=0, polls_before_exit=1, write=None):
        self.pid = 4242
        self.returncode = None
        self._final = returncode
        self._polls = polls_before_exit
        self._write = write
        self.killed = False

    def poll(self):
        if self._polls is None:
            return None
        if self._polls <= 0:
            if self._write:
                self._write()
                self._write = None
            self.returncode = self._final
            return self.returncode
        self._polls -= 1
        return None

    def kill(self):
        self.killed = True

    terminate = kill


# ---------------------------------------------------------------------------
# Plans and identifiers
# ---------------------------------------------------------------------------

class TestPlans:
    def test_traffic_identical_to_the_smoke_cell(self):
        sys.path.insert(0, str(_REPO / "workload-generator"))
        from workload_generator.plan import PlanConfig, build_plan
        from workload_generator.scenarios import Scenario

        warm, measured = C.build_plans(1)
        smoke_m = build_plan(PlanConfig(seed=42, run_id="b-smoke-r0005-c32-m1",
                                        scenario=Scenario("normal"), traces=100, rate=5.0))
        smoke_w = build_plan(PlanConfig(seed=42, run_id="b-smoke-r0005-c32-w1",
                                        scenario=Scenario("normal"), traces=25, rate=5.0))
        for ours, theirs in ((measured, smoke_m), (warm, smoke_w)):
            assert [r.arrival_offset_seconds for r in ours.requests] == \
                [r.arrival_offset_seconds for r in theirs.requests]
            assert [r.latency for r in ours.requests] == [r.latency for r in theirs.requests]
        assert len(measured.requests) == 100 and len(warm.requests) == 25
        assert C.build_plans(7)[1].requests[5].arrival_offset_seconds == \
            measured.requests[5].arrival_offset_seconds

    def test_parameters(self):
        assert (C.SEED, C.RATE, C.CONCURRENCY, C.WARMUP_REQUESTS, C.MEASURED_REQUESTS) == (
            42, 5.0, 32, 25, 100)

    def test_identifiers_are_calibration_ids(self):
        assert C.calibration_id(1) == "cal-a-01" and C.run_ids(20) == ("cal-a-w20", "cal-a-m20")
        for n in (1, 9, 20):
            assert not C.calibration_id(n).startswith("bm-")
            assert all(RB.RUN_ID_RE.fullmatch(r) for r in C.run_ids(n))
        for bad in (0, 21, True):
            with pytest.raises(C.CalibrationError):
                C.calibration_id(bad)

    def test_thresholds_are_the_existing_ones(self):
        sys.path.insert(0, str(_REPO / "workload-generator"))
        from workload_generator import real_driver
        assert C.LATE_START_SECONDS == real_driver.LATE_START_SECONDS == 0.1
        assert C.LATE_FRACTION == M.LATE_START_FRACTION == 0.05


class TestRealDriverReuse:
    def test_import_driver_returns_the_real_functions(self):
        sys.path.insert(0, str(_REPO / "workload-generator"))
        from workload_generator import real_driver
        execute, load_demo = C._import_driver()
        assert execute is real_driver.execute and load_demo is real_driver.load_demo

    def test_no_production_export_path_or_harness_in_the_source(self):
        text = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8")
        code = text.split('"""', 2)[2]
        for word in ("run_real(", "check_reachable", "build_otlp_exporter", "import run_smoke",
                     "import run_e2e", "docker", "compose", "OTLPSpanExporter"):
            assert word not in code, word

    def test_child_uses_real_time_by_default(self):
        import time
        deps = C.ChildDeps()
        assert deps.clock is time.monotonic and deps.pause is time.sleep
        assert deps.import_driver is C._import_driver and deps.exporter is C._memory_exporter


# ---------------------------------------------------------------------------
# Network guard
# ---------------------------------------------------------------------------

class TestNetworkGuard:
    def test_every_target_is_intercepted_and_recorded(self):
        guard = C.NetworkGuard().install()
        try:
            sock = socket.socket.__new__(socket.socket)
            for call in (lambda: socket.create_connection(("127.0.0.1", 1)),
                         lambda: socket.getaddrinfo("example.invalid", 80),
                         lambda: socket.socket.connect(sock, ("127.0.0.1", 1)),
                         lambda: socket.socket.connect_ex(sock, ("127.0.0.1", 1)),
                         lambda: socket.socket.sendto(sock, b"x", ("127.0.0.1", 1))):
                with pytest.raises(C.NetworkBlocked):
                    call()
            assert [v["operation"] for v in guard.violations] == [
                "create_connection", "getaddrinfo", "connect", "connect_ex", "sendto"]
        finally:
            guard.uninstall()

    def test_uninstall_restores_and_double_install_refused(self):
        before = (socket.getaddrinfo, socket.socket.sendto)
        guard = C.NetworkGuard().install()
        with pytest.raises(C.CalibrationError):
            guard.install()
        guard.uninstall()
        assert (socket.getaddrinfo, socket.socket.sendto) == before and not guard.installed

    def test_guard_is_installed_before_the_driver_import(self, tmp_path):
        clock, seen = FakeClock(), {}
        deps = _fake_deps(clock, import_check=lambda: seen.setdefault(
            "guarded", socket.getaddrinfo.__name__ == "blocked"))
        assert C.child_main(1, tmp_path, deps) == C.EXIT_OK
        assert seen["guarded"] is True
        assert socket.getaddrinfo.__name__ != "blocked"          # removed afterwards


# ---------------------------------------------------------------------------
# Child runs
# ---------------------------------------------------------------------------

class TestChild:
    def test_completed_run_record(self, tmp_path):
        clock, calls = FakeClock(), []
        assert C.child_main(3, tmp_path, _fake_deps(clock, calls=calls)) == C.EXIT_OK
        record = json.loads((tmp_path / "cal-a-03.json").read_text(encoding="utf-8"))
        assert record["status"] == "completed" and record["child_exit_status"] == 0
        assert record["calibration_id"] == "cal-a-03" and record["condition"] == "a"
        assert record["plan"]["measured"]["run_id"] == "cal-a-m03"
        assert record["measured"]["started_requests"] == 100
        assert record["measured"]["late_start_percent"] == 0.0
        assert record["measured_duration_seconds"] == 20.0
        assert record["network_guard_violations"] == []
        assert [c[0] for c in calls] == [25, 100]
        for _, kwargs in calls:
            assert kwargs["endpoint"] == "in-memory" and kwargs["concurrency"] == 32
            assert kwargs["span_exporter"] == "memory-exporter"
            assert kwargs["pause"] == clock.pause and kwargs["clock"] == clock.clock

    @pytest.mark.parametrize("swallowed", [False, True])
    def test_network_attempt_fails_the_run(self, tmp_path, swallowed):
        clock, calls = FakeClock(), []

        def leaking_execute(plan, **kwargs):
            calls.append(plan.traces)
            try:
                socket.getaddrinfo("collector.example", 4317)
            except OSError:
                if not swallowed:
                    raise
            return _report(plan.traces)

        status = C.child_main(1, tmp_path, _fake_deps(clock, execute=leaking_execute))
        record = json.loads((tmp_path / "cal-a-01.json").read_text(encoding="utf-8"))
        assert status == C.EXIT_NETWORK and record["status"] == "network_violation"
        assert record["network_guard_violations"][0]["operation"] == "getaddrinfo"
        assert calls == [25] and record["measured"] is None      # stopped after warm-up

    def test_failure_is_recorded_not_retried(self, tmp_path):
        clock, calls = FakeClock(), []

        def broken(plan, **kwargs):
            calls.append(plan.traces)
            raise RuntimeError("driver failed")

        assert C.child_main(2, tmp_path, _fake_deps(clock, execute=broken)) == C.EXIT_FAILED
        record = json.loads((tmp_path / "cal-a-02.json").read_text(encoding="utf-8"))
        assert record["status"] == "failed" and "driver failed" in record["failure"]
        assert calls == [25]

    def test_interruption_still_writes_evidence(self, tmp_path):
        clock = FakeClock()

        def interrupted(plan, **kwargs):
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            C.child_main(4, tmp_path, _fake_deps(clock, execute=interrupted))
        record = json.loads((tmp_path / "cal-a-04.json").read_text(encoding="utf-8"))
        assert record["status"] == "failed" and record["child_exit_status"] == C.EXIT_FAILED

    def test_a_run_is_never_repeated(self, tmp_path):
        clock = FakeClock()
        C.child_main(1, tmp_path, _fake_deps(clock))
        with pytest.raises(C.CalibrationError, match="never repeated"):
            C.child_main(1, tmp_path, _fake_deps(clock))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _orchestrator(clock, children, observe=lambda pid: [], launched=None):
    launched = [] if launched is None else launched

    def launch(number, output_dir):
        child = children[len(launched)](number, output_dir)
        launched.append(number)
        return child

    return C.OrchestratorDeps(launch=launch, observe=observe, clock=clock.clock,
                              pause=clock.pause, now=lambda: 1_800_000_000.0), launched


def _writing_child(returncode=0):
    def make(number, output_dir):
        return FakeChild(returncode, write=lambda: (output_dir / f"cal-a-{number:02d}.json")
                         .write_text("{}", encoding="utf-8"))
    return make


class TestOrchestrator:
    def test_clean_series_releases_ownership(self, tmp_path):
        clock = FakeClock()
        deps, launched = _orchestrator(clock, [_writing_child()] * 3)
        outcome = C.run_series(3, tmp_path, deps)
        assert outcome.completed == 3 and outcome.stopped_reason is None
        assert outcome.ownership_released and not (tmp_path / C.OWNER_FILE).exists()
        assert launched == [1, 2, 3]
        assert clock.pauses.count(C.REST_SECONDS) == 2
        summary = json.loads(next(tmp_path.glob("series-a-*.json")).read_text())
        assert "cannot prove" in summary["note"]

    def test_existing_ownership_refused(self, tmp_path):
        (tmp_path / C.OWNER_FILE).write_text("{}")
        deps, launched = _orchestrator(FakeClock(), [_writing_child()])
        with pytest.raises(C.CalibrationError, match="owns this directory"):
            C.run_series(1, tmp_path, deps)
        assert launched == []

    def test_existing_result_refused_before_ownership(self, tmp_path):
        (tmp_path / "cal-a-02.json").write_text("{}")
        deps, launched = _orchestrator(FakeClock(), [_writing_child()] * 3)
        with pytest.raises(C.CalibrationError, match="never repeated"):
            C.run_series(3, tmp_path, deps)
        assert launched == [] and not (tmp_path / C.OWNER_FILE).exists()

    def test_nonzero_child_stops_without_retry(self, tmp_path):
        deps, launched = _orchestrator(FakeClock(), [_writing_child(3), _writing_child()])
        outcome = C.run_series(2, tmp_path, deps)
        assert launched == [1] and "exit status 3" in outcome.stopped_reason
        assert (tmp_path / C.OWNER_FILE).exists() and not outcome.ownership_released

    def test_timeout_never_kills_and_keeps_ownership(self, tmp_path):
        clock = FakeClock()
        hung = FakeChild(polls_before_exit=None)
        deps, launched = _orchestrator(clock, [lambda n, d: hung])
        outcome = C.run_series(1, tmp_path, deps)
        assert "not killed" in outcome.stopped_reason and not hung.killed
        assert outcome.runs[0]["exit_status"] is None
        assert (tmp_path / C.OWNER_FILE).exists()

    def test_os_level_connection_stops_the_series(self, tmp_path):
        deps, launched = _orchestrator(
            FakeClock(), [lambda n, d: FakeChild(polls_before_exit=2, write=lambda: (
                d / "cal-a-01.json").write_text("{}"))] * 2,
            observe=lambda pid: ["tcp 127.0.0.1:50000 10.0.0.5:4317 Established"])
        outcome = C.run_series(2, tmp_path, deps)
        assert launched == [1] and "OS-level" in outcome.stopped_reason
        assert outcome.runs[0]["os_observations"]

    def test_missing_result_stops(self, tmp_path):
        deps, _ = _orchestrator(FakeClock(), [lambda n, d: FakeChild()])
        outcome = C.run_series(1, tmp_path, deps)
        assert "no result file" in outcome.stopped_reason

    def test_series_bounds(self, tmp_path):
        for runs in (0, 21):
            with pytest.raises(C.CalibrationError):
                C.run_series(runs, tmp_path, _orchestrator(FakeClock(), [])[0])

    def test_child_environment_is_explicit(self, monkeypatch):
        for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "PG_PASSWORD", "HTTPS_PROXY",
                     "AGENTOPS_DIRECT_EXPORT"):
            monkeypatch.setenv(name, "x")
        env = C.child_environment()
        assert set(env) <= {"SYSTEMROOT", "PATH", "TEMP", "TMP", "PYTHONPATH",
                            "PYTHONDONTWRITEBYTECODE", "PYTHONUNBUFFERED"}


# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------

class TestOutputPaths:
    @pytest.mark.parametrize("path", [
        _REPO, _REPO / "benchmarks", _REPO / "x" / "y",
        RB.DEFAULT_OUTPUT, RB.DEFAULT_OUTPUT / "ledger",
        Path(os.environ.get("TEMP", "C:/tmp")) / "agentops-benchmarks" / "cal",
    ])
    def test_forbidden_locations(self, path):
        with pytest.raises(C.CalibrationError, match="never written inside"):
            C.check_output_dir(path)

    def test_default_is_separate(self):
        C.check_output_dir(C.DEFAULT_OUTPUT)
        assert C.DEFAULT_OUTPUT.name == "agentops-calibration"


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _rep3_like_entries():
    _, measured = C.build_plans(1)
    offsets = [r.arrival_offset_seconds for r in measured.requests]
    lags = [0.0005] * 100
    for index, dispatch in ((19, offsets[19] + 0.0611), (53, offsets[53] + 0.0915),
                            (98, offsets[98] + 0.1402)):
        lags[index] = dispatch - offsets[index]
    group_end = offsets[47] + 0.1398
    for index in range(47, 52):
        lags[index] = group_end - offsets[index]
    return [[i, round(offsets[i], 6), round(lags[i], 6)] for i in range(100)]


class TestAnalysis:
    def test_groups_propagation_and_isolated(self):
        groups = C.stall_groups(_rep3_like_entries())
        assert [g["request_indices"] for g in groups] == [[19], [47, 48, 49, 50, 51], [53], [98]]
        assert [g["propagated"] for g in groups] == [False, True, False, False]

    def test_burst_association(self):
        result = C.burst_association(_rep3_like_entries())
        assert result["requests"] == 100 and result["late"] == 2
        assert result["requests_in_bursts"] == 29 and result["late_in_bursts"] == 0

    def test_summary_percentiles_and_late_indices(self):
        entries = _rep3_like_entries()
        report = _report(100, late=2, lags=[e[2] for e in entries]).to_dict()
        report["start_lags"]["entries"] = entries
        record = C.summarize_stage(report)
        lags = [e[2] for e in entries]
        assert record["late_request_indices"] == [47, 98]
        assert record["start_lag_percentiles"]["p95"] == round(M.percentile(lags, 95), 6)
        assert record["late_start_percent"] == 2.0 and record["diagnostic_usable"]

    @pytest.mark.parametrize("lags, reason", [
        (None, "no start-lag diagnostic"),
        ({"limit": 10000, "expected": 2, "recorded": 1, "complete": False,
          "truncated_reason": "limit", "entries": [[0, 0.0, 0.1]]}, "incomplete"),
        ({"entries": []}, "malformed"),
    ])
    def test_unusable_diagnostic_gives_no_numbers(self, lags, reason):
        record = C.summarize_stage({"started_requests": 2, "late_starts": 0,
                                    "start_lags": lags})
        assert not record["diagnostic_usable"] and reason in " ".join(
            record["diagnostic_problems"])
        for key in ("start_lag_percentiles", "late_request_indices", "stall_groups",
                    "burst_association"):
            assert record[key] is None

    def test_distribution_is_an_observation(self):
        runs = [{"status": "completed", "measured": {"late_start_percent": p,
                                                     "stall_groups": []}}
                for p in (0.0, 2.0, 10.0)] + [{"status": "failed", "measured": None}]
        dist = C.distribution(runs)
        assert dist == {"runs": 3, "median_late_percent": 2.0, "min_late_percent": 0.0,
                        "max_late_percent": 10.0, "runs_over_rule": 1, "stall_groups": 0,
                        "propagated_groups": 0, "kind": "OBSERVATION"}


# ---------------------------------------------------------------------------
# Gates and unchanged benchmark code
# ---------------------------------------------------------------------------

class TestGatesAndScope:
    def test_execution_is_disabled(self, tmp_path, capsys):
        assert C.EXECUTION_ENABLED is False
        assert C.main(["--execute", "--output-dir", str(tmp_path)]) == 2
        assert "not enabled" in capsys.readouterr().err
        assert C.main(["--child", "1", "--output-dir", str(tmp_path)]) == 2
        assert list(tmp_path.iterdir()) == []

    def test_plan_mode_starts_nothing(self, tmp_path):
        out = io.StringIO()
        assert C.main(["--runs", "3", "--output-dir", str(tmp_path / "c")], stdout=out) == 0
        lines = out.getvalue().splitlines()
        assert lines[0].startswith("PLAN ONLY")
        assert [line.split(":")[0] for line in lines if line.startswith("cal-a-")] == [
            "cal-a-01", "cal-a-02", "cal-a-03"]
        assert not (tmp_path / "c").exists()

    def test_benchmark_authorization_and_rules_unchanged(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == ["bm-smoke-r0005-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep3"]
        assert RB.DRIVER_LATE_START_SECONDS == 0.1 and M.LATE_START_FRACTION == 0.05
        assert RB.OWNER_FILE == "ACTIVE_RUN" and C.OWNER_FILE == "ACTIVE_CALIBRATION"

    def test_benchmark_directory_untouched_by_the_harness(self):
        text = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8")
        code = text.split('"""', 2)[2]
        assert "ACTIVE_RUN" not in code and "ledger" not in code.lower()
