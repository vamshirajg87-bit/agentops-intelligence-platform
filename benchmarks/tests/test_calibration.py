"""
benchmarks/tests/test_calibration.py

Offline tests of the generator-only calibration harness (condition A).
No calibration runs, no demo agent, no Docker, no network, no process
launches: children, launchers, observations, clocks and the driver are
fakes, and any process or socket attempt that is not an explicit fake fails
the test.  Where a test opens the execution gate, it does so in memory only.
"""

from __future__ import annotations

import hashlib
import importlib.abc
import io
import json
import os
import socket
import subprocess
import sys
import time
import types
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

HEAD = "a" * 40
TOKEN = "0123456789abcdef0123456789abcdef"
CREATED = "2026-10-09T10:00:00.000Z"
SPACED_PYTHON = r"C:\Program Files\Python 3\python.exe"


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
    # The child's guard refuses to start over loaded native network
    # libraries; hide any another test module loaded (restored afterwards).
    for name in [n for n in sys.modules if C._NativeStubFinder.blocked(n)]:
        monkeypatch.delitem(sys.modules, name)


def open_gate(monkeypatch, ids=("cal-a-01",), head=HEAD):
    """In memory only: enable, approve `ids`, and fake the Git HEAD."""
    monkeypatch.setattr(C, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(C, "APPROVED_CALIBRATIONS", frozenset(ids))
    monkeypatch.setattr(C, "_git_head", lambda: head)


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


def _child_deps(clock, *, execute=None, calls=None, import_check=None):
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


def _own(directory, runs=1, token=TOKEN):
    ids = [C.calibration_id(n) for n in range(1, runs + 1)]
    return C.claim_series(directory, calibration_ids=ids, expected_head=HEAD,
                          now=lambda: 1_800_000_000.0, new_token=lambda: token)


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


class FakeProcess:
    """A launched child: exits after `polls` polls (never when None)."""

    def __init__(self, *, polls=1, returncode=None, on_exit=None, pid=4242, created=CREATED):
        self.pid, self.creation_time = pid, created
        self.returncode = None
        self._polls, self._override, self._on_exit = polls, returncode, on_exit
        self.killed = False

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        if self._polls is None:
            return None
        if self._polls > 0:
            self._polls -= 1
            return None
        status = self._on_exit() if self._on_exit else 0
        self.returncode = status if self._override is None else self._override
        return self.returncode

    def kill(self):
        self.killed = True

    terminate = kill


def _arg(command, flag):
    argv = list(command.argv)
    return argv[argv.index(flag) + 1]


def real_child(*, execute=None, returncode=None, polls=1, write=None):
    """A fake process whose exit runs the real child_main logic with fakes
    (or `write` instead)."""
    def make(command):
        number, out = int(_arg(command, "--child")), Path(_arg(command, "--output-dir"))
        token = _arg(command, "--token")

        def finish():
            if write is not None:
                write(out / f"{command.calibration_id}.json")
                return 0
            return C.child_main(number, out, _child_deps(FakeClock(), execute=execute),
                                series_token=token)
        return FakeProcess(polls=polls, returncode=returncode, on_exit=finish)
    return make


def good_observe(identity):
    return C.Observation([], [], [f"proc {identity.pid} 1 {identity.creation_time}"], True)


def _orchestrator(clock, makers, *, observe=good_observe, launched=None):
    launched = [] if launched is None else launched

    def launch(command, stdout, stderr):
        assert stdout.writable() and stderr.writable()
        launched.append(command)
        return makers[len(launched) - 1](command)

    deps = C.OrchestratorDeps(launch=launch, observe=observe, clock=clock.clock,
                              pause=clock.pause, now=lambda: 1_800_000_000.0,
                              interpreter=SPACED_PYTHON, expected_head=HEAD)
    return deps, launched


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
    def test_the_real_driver_functions_are_targeted(self):
        code = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8").split('"""', 2)[2]
        assert "from workload_generator import real_driver" in code
        assert "return real_driver.execute, real_driver.load_demo" in code
        assert "InMemorySpanExporter()" in code

    def test_no_production_export_path_or_harness_in_the_source(self):
        code = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8").split('"""', 2)[2]
        for word in ("run_real(", "check_reachable", "build_otlp_exporter", "import run_smoke",
                     "import run_e2e", "docker", "compose", "OTLPSpanExporter("):
            assert word not in code, word

    def test_production_child_uses_real_time(self, monkeypatch):
        open_gate(monkeypatch)
        deps = C.production_child_deps(C.authorize(["cal-a-01"], HEAD), 1)
        assert deps.clock is time.monotonic and deps.pause is time.sleep
        assert deps.plans is C.build_plans and deps.guard_factory is C.NetworkGuard


# ---------------------------------------------------------------------------
# Execution gates
# ---------------------------------------------------------------------------

IDENTITY = C.ProcessIdentity(4242, CREATED)

LEAVES = {
    "_import_driver": lambda auth: C._import_driver(auth),
    "_memory_exporter": lambda auth: C._memory_exporter(auth),
    "_run_powershell": lambda auth: C._run_powershell(auth, "x", 1.0),
    "observe_process": lambda auth: C.observe_process(auth, IDENTITY),
    "production_child_deps": lambda auth: C.production_child_deps(auth, 1),
    "production_orchestrator_deps": lambda auth: C.production_orchestrator_deps(auth),
    "AppContainerLauncher": lambda auth: C.AppContainerLauncher(auth, object()),
    "Win32AppContainerApi": lambda auth: C.Win32AppContainerApi(auth),
}


class TestExecutionGates:
    def test_defaults_are_gone(self, tmp_path):
        with pytest.raises(TypeError):
            C.child_main(1, tmp_path)
        with pytest.raises(TypeError):
            C.run_series(1, tmp_path)
        with pytest.raises(TypeError):
            C.ChildDeps()
        with pytest.raises(TypeError):
            C.OrchestratorDeps()
        with pytest.raises(TypeError):
            C.child_main(1, tmp_path, object(), series_token=TOKEN)
        with pytest.raises(TypeError):
            C.run_series(1, tmp_path, object())
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize("leaf", sorted(LEAVES))
    def test_every_leaf_refuses_while_disabled(self, leaf):
        assert C.EXECUTION_ENABLED is False and C.APPROVED_CALIBRATIONS == frozenset()
        with pytest.raises(C.ExecutionDisabled, match="not enabled"):
            LEAVES[leaf](None)

    def test_head_and_authorize_refuse_while_disabled(self):
        with pytest.raises(C.ExecutionDisabled, match="not enabled"):
            C._git_head()
        with pytest.raises(C.ExecutionDisabled, match="not enabled"):
            C.authorize(["cal-a-01"], HEAD)

    @pytest.mark.parametrize("leaf", sorted(LEAVES))
    def test_the_flag_alone_is_not_enough(self, monkeypatch, leaf):
        monkeypatch.setattr(C, "EXECUTION_ENABLED", True)
        with pytest.raises(C.ExecutionDisabled, match="APPROVED_CALIBRATIONS is empty"):
            LEAVES[leaf](None)
        with pytest.raises(C.ExecutionDisabled, match="APPROVED_CALIBRATIONS is empty"):
            C.authorize(["cal-a-01"], HEAD)

    @pytest.mark.parametrize("leaf", sorted(LEAVES))
    def test_forged_authorizations_are_refused(self, monkeypatch, leaf):
        open_gate(monkeypatch)
        for forged in (None, C.Authorization(("cal-a-01",), HEAD),
                       C.Authorization(("cal-a-01",), HEAD, object()),
                       SimpleNamespace(calibration_ids=("cal-a-01",), expected_head=HEAD)):
            with pytest.raises(C.ExecutionDisabled, match="no valid calibration authorization"):
                LEAVES[leaf](forged)

    def test_authorization_checks(self, monkeypatch):
        open_gate(monkeypatch, ids=("cal-a-01",))
        cases = [(["cal-a-02"], HEAD, "not an approved"),
                 (["cal-a-01", "cal-a-01"], HEAD, "distinct"),
                 ([], HEAD, "distinct"),
                 (["cal-a-01"], "abc123", "full 40"),
                 (["cal-a-01"], None, "full 40"),
                 (["cal-a-01"], "b" * 40, "not the expected")]
        for ids, head, message in cases:
            with pytest.raises(C.ExecutionDisabled, match=message):
                C.authorize(ids, head)
        auth = C.authorize(["cal-a-01"], HEAD)
        assert auth.calibration_ids == ("cal-a-01",) and auth.expected_head == HEAD
        with pytest.raises(C.ExecutionDisabled, match="not covered"):
            C.production_child_deps(auth, 2)

    def test_authorization_dies_with_the_gate(self, monkeypatch):
        open_gate(monkeypatch)
        auth = C.authorize(["cal-a-01"], HEAD)
        monkeypatch.setattr(C, "APPROVED_CALIBRATIONS", frozenset({"cal-a-02"}))
        with pytest.raises(C.ExecutionDisabled, match="not approved: cal-a-01"):
            C._import_driver(auth)
        monkeypatch.setattr(C, "EXECUTION_ENABLED", False)
        with pytest.raises(C.ExecutionDisabled, match="not enabled"):
            C._import_driver(auth)

    def test_production_orchestrator_uses_the_app_container(self, monkeypatch):
        open_gate(monkeypatch)
        deps = C.production_orchestrator_deps(C.authorize(["cal-a-01"], HEAD))
        assert isinstance(deps.launch, C.AppContainerLauncher)
        assert isinstance(deps.launch.api, C.Win32AppContainerApi)
        assert deps.interpreter == sys.executable and deps.expected_head == HEAD
        with pytest.raises(C.CalibrationError, match="not implemented"):
            deps.launch.api.profile_sid(C.APPCONTAINER_PROFILE)

    def test_cli_refuses_while_disabled(self, tmp_path, capsys):
        assert C.main(["--execute", "--output-dir", str(tmp_path)]) == 2
        assert "not enabled" in capsys.readouterr().err
        assert C.main(["--child", "1", "--output-dir", str(tmp_path), "--token", TOKEN]) == 2
        assert "not enabled" in capsys.readouterr().err
        assert list(tmp_path.iterdir()) == []

    def test_cli_flag_alone_refused(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(C, "EXECUTION_ENABLED", True)
        assert C.main(["--execute", "--runs", "1", "--authorize", "cal-a-01",
                       "--expect-head", HEAD, "--output-dir", str(tmp_path)]) == 2
        assert "APPROVED_CALIBRATIONS is empty" in capsys.readouterr().err
        assert list(tmp_path.iterdir()) == []

    def test_cli_execute_needs_exact_ids(self, tmp_path, monkeypatch, capsys):
        open_gate(monkeypatch, ids=("cal-a-01", "cal-a-02"))
        assert C.main(["--execute", "--runs", "2", "--authorize", "cal-a-01",
                       "--expect-head", HEAD, "--output-dir", str(tmp_path)]) == 2
        assert "exactly the calibration ids" in capsys.readouterr().err
        assert C.main(["--execute", "--runs", "1", "--authorize", "cal-a-01",
                       "--expect-head", "c" * 40, "--output-dir", str(tmp_path)]) == 2
        assert "not the expected" in capsys.readouterr().err
        assert list(tmp_path.iterdir()) == []

    def test_cli_child_needs_the_series_token(self, tmp_path, monkeypatch, capsys):
        open_gate(monkeypatch)
        _own(tmp_path)
        child = ["--child", "1", "--output-dir", str(tmp_path), "--authorize", "cal-a-01",
                 "--expect-head", HEAD]
        assert C.main(child + ["--token", "f" * 32]) == 2
        assert "token does not match" in capsys.readouterr().err
        assert C.main(child) == 2
        assert not (tmp_path / "cal-a-01.json").exists()

    def test_plan_mode_starts_nothing(self, tmp_path):
        out = io.StringIO()
        assert C.main(["--runs", "3", "--output-dir", str(tmp_path / "c")], stdout=out) == 0
        lines = out.getvalue().splitlines()
        assert lines[0].startswith("PLAN ONLY")
        assert [line.split(":")[0] for line in lines if line.startswith("cal-a-")] == [
            "cal-a-01", "cal-a-02", "cal-a-03"]
        assert not (tmp_path / "c").exists()


# ---------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------

class TestOwnership:
    def test_claim_is_exclusive_and_records_the_token(self, tmp_path):
        owner, token = _own(tmp_path, runs=2)
        record = _read(owner)
        assert token == TOKEN and record["series_token"] == TOKEN
        assert record["calibration_ids"] == ["cal-a-01", "cal-a-02"]
        with pytest.raises(C.CalibrationError, match="owns this directory"):
            _own(tmp_path)
        with pytest.raises(C.CalibrationError, match="32 lowercase hex"):
            C.claim_series(tmp_path / "x", calibration_ids=["cal-a-01"], expected_head=HEAD,
                           now=time.time, new_token=lambda: "short")

    def test_child_without_ownership_never_starts(self, tmp_path):
        imported = []
        deps = _child_deps(FakeClock(), import_check=lambda: imported.append(1))
        with pytest.raises(C.CalibrationError, match="no ACTIVE_CALIBRATION"):
            C.child_main(1, tmp_path, deps, series_token=TOKEN)
        (tmp_path / C.OWNER_FILE).write_text("{not json", encoding="utf-8")
        with pytest.raises(C.CalibrationError, match="malformed"):
            C.child_main(1, tmp_path, deps, series_token=TOKEN)
        (tmp_path / C.OWNER_FILE).write_text('{"series_token": 1}', encoding="utf-8")
        with pytest.raises(C.CalibrationError, match="malformed"):
            C.child_main(1, tmp_path, deps, series_token=TOKEN)
        assert imported == [] and not (tmp_path / "cal-a-01.json").exists()

    def test_token_mismatch_and_foreign_run(self, tmp_path):
        _own(tmp_path, runs=1)
        deps = _child_deps(FakeClock())
        with pytest.raises(C.CalibrationError, match="token does not match"):
            C.child_main(1, tmp_path, deps, series_token="f" * 32)
        with pytest.raises(C.CalibrationError, match="not part of the owning series"):
            C.child_main(2, tmp_path, deps, series_token=TOKEN)
        assert sorted(p.name for p in tmp_path.iterdir()) == [C.OWNER_FILE]


# ---------------------------------------------------------------------------
# Network guard
# ---------------------------------------------------------------------------

class TestNetworkGuard:
    def test_every_python_target_is_intercepted(self):
        guard = C.NetworkGuard().install()
        try:
            sock = socket.socket.__new__(socket.socket)
            calls = [
                ("__init__", lambda: socket.socket()),
                ("create_connection", lambda: socket.create_connection(("127.0.0.1", 1))),
                ("create_server", lambda: socket.create_server(("127.0.0.1", 0))),
                ("getaddrinfo", lambda: socket.getaddrinfo("example.invalid", 80)),
                ("getnameinfo", lambda: socket.getnameinfo(("127.0.0.1", 80), 0)),
                ("gethostbyname", lambda: socket.gethostbyname("example.invalid")),
                ("gethostbyname_ex", lambda: socket.gethostbyname_ex("example.invalid")),
                ("gethostbyaddr", lambda: socket.gethostbyaddr("127.0.0.1")),
                ("socketpair", lambda: socket.socketpair()),
                ("connect", lambda: socket.socket.connect(sock, ("127.0.0.1", 1))),
                ("connect_ex", lambda: socket.socket.connect_ex(sock, ("127.0.0.1", 1))),
                ("sendto", lambda: socket.socket.sendto(sock, b"x", ("127.0.0.1", 1))),
            ]
            for _, call in calls:
                with pytest.raises(C.NetworkBlocked):
                    call()
            assert [v["operation"] for v in guard.violations] == [n for n, _ in calls]
        finally:
            guard.uninstall()

    def test_uninstall_restores_and_double_install_refused(self):
        before = (socket.getaddrinfo, socket.gethostbyname, socket.socket.__init__,
                  socket.socket.sendto, list(sys.meta_path))
        guard = C.NetworkGuard().install()
        with pytest.raises(C.CalibrationError, match="already installed"):
            guard.install()
        guard.uninstall()
        assert (socket.getaddrinfo, socket.gethostbyname, socket.socket.__init__,
                socket.socket.sendto, list(sys.meta_path)) == before
        assert not guard.installed

    def test_native_network_libraries_are_refusing_stubs(self):
        guard = C.NetworkGuard().install()
        try:
            import grpc
            from confluent_kafka import Producer
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            assert isinstance(grpc, C._NativeStub)
            for call in (lambda: grpc.insecure_channel("localhost:4317"),
                         lambda: Producer({"bootstrap.servers": "localhost:9092"}),
                         lambda: OTLPSpanExporter(endpoint="http://localhost:4317")):
                with pytest.raises(C.NetworkBlocked):
                    call()
            assert [v["arguments"] for v in guard.violations] == [
                "grpc.insecure_channel", "confluent_kafka.Producer",
                "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter"]
            assert "confluent_kafka" in guard.stubbed_imports
        finally:
            guard.uninstall()
        assert not any(isinstance(m, C._NativeStub) for m in sys.modules.values())
        assert not any(C._NativeStubFinder.blocked(n) for n in sys.modules)

    def test_refuses_over_already_loaded_native_libraries(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "grpc", types.ModuleType("grpc"))
        before = socket.getaddrinfo
        with pytest.raises(C.CalibrationError, match="already loaded: grpc"):
            C.NetworkGuard().install()
        assert socket.getaddrinfo is before

    def test_guard_is_installed_before_the_driver_import(self, tmp_path):
        _own(tmp_path)
        seen = {}

        def check():
            seen["socket"] = socket.getaddrinfo.__name__ == "blocked"
            seen["native"] = any(isinstance(f, C._NativeStubFinder) for f in sys.meta_path)

        deps = _child_deps(FakeClock(), import_check=check)
        assert C.child_main(1, tmp_path, deps, series_token=TOKEN) == C.EXIT_OK
        assert seen == {"socket": True, "native": True}
        assert socket.getaddrinfo.__name__ != "blocked"


# ---------------------------------------------------------------------------
# Child runs
# ---------------------------------------------------------------------------

class TestChild:
    def test_completed_run_record(self, tmp_path):
        _own(tmp_path, runs=3)
        clock, calls = FakeClock(), []
        assert C.child_main(3, tmp_path, _child_deps(clock, calls=calls),
                            series_token=TOKEN) == C.EXIT_OK
        record = _read(tmp_path / "cal-a-03.json")
        assert C.result_problems(record, 3, 0) == []
        assert record["plan"] == C.plan_record(3) and record["measured_duration_seconds"] == 20.0
        assert [c[0] for c in calls] == [25, 100]
        for _, kwargs in calls:
            assert kwargs["endpoint"] == "in-memory" and kwargs["concurrency"] == 32
            assert kwargs["span_exporter"] == "memory-exporter"

    @pytest.mark.parametrize("kind", ["socket", "socket-swallowed", "native-swallowed"])
    def test_network_attempt_fails_the_run(self, tmp_path, kind):
        _own(tmp_path)
        calls = []

        def leaking_execute(plan, **kwargs):
            calls.append(plan.traces)
            try:
                if kind.startswith("native"):
                    import grpc
                    grpc.insecure_channel("localhost:4317")
                else:
                    socket.getaddrinfo("collector.example", 4317)
            except OSError:
                if kind == "socket":
                    raise
            return _report(plan.traces)

        status = C.child_main(1, tmp_path, _child_deps(FakeClock(), execute=leaking_execute),
                              series_token=TOKEN)
        record = _read(tmp_path / "cal-a-01.json")
        assert status == C.EXIT_NETWORK and record["status"] == "network_violation"
        assert record["network_guard_violations"]
        assert calls == [25] and record["measured"] is None
        assert C.result_problems(record, 1, status)

    def test_failure_is_recorded_not_retried(self, tmp_path):
        _own(tmp_path)
        calls = []

        def broken(plan, **kwargs):
            calls.append(plan.traces)
            raise RuntimeError("driver failed")

        assert C.child_main(1, tmp_path, _child_deps(FakeClock(), execute=broken),
                            series_token=TOKEN) == C.EXIT_FAILED
        record = _read(tmp_path / "cal-a-01.json")
        assert record["status"] == "failed" and "driver failed" in record["failure"]
        assert calls == [25]

    def test_interruption_still_writes_evidence(self, tmp_path):
        _own(tmp_path)

        def interrupted(plan, **kwargs):
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            C.child_main(1, tmp_path, _child_deps(FakeClock(), execute=interrupted),
                         series_token=TOKEN)
        record = _read(tmp_path / "cal-a-01.json")
        assert record["status"] == "failed" and record["child_exit_status"] == C.EXIT_FAILED

    def test_a_run_is_never_repeated(self, tmp_path):
        _own(tmp_path)
        C.child_main(1, tmp_path, _child_deps(FakeClock()), series_token=TOKEN)
        with pytest.raises(C.CalibrationError, match="never repeated"):
            C.child_main(1, tmp_path, _child_deps(FakeClock()), series_token=TOKEN)


# ---------------------------------------------------------------------------
# Result validation
# ---------------------------------------------------------------------------

def _valid_record(tmp_path):
    _own(tmp_path)
    C.child_main(1, tmp_path, _child_deps(FakeClock()), series_token=TOKEN)
    return _read(tmp_path / "cal-a-01.json")


MUTATIONS = {
    "wrong id": lambda r: r.update(calibration_id="cal-a-02"),
    "failed status": lambda r: r.update(status="failed"),
    "recorded exit differs": lambda r: r.update(child_exit_status=4),
    "concurrency changed": lambda r: r["plan"].update(concurrency=16),
    "seed changed": lambda r: r["plan"]["measured"].update(seed=7),
    "violations": lambda r: r.update(network_guard_violations=[{"operation": "connect"}]),
    "violations missing": lambda r: r.pop("network_guard_violations"),
    "diagnostic incomplete": lambda r: r["measured"].update(diagnostic_usable=False),
    "short measured": lambda r: r["measured"].update(started_requests=99),
    "not started": lambda r: r["warmup"].update(not_started_requests=1),
    "deadline": lambda r: r["measured"].update(run_status="deadline_reached"),
    "no warm-up": lambda r: r.update(warmup=None),
}


class TestResultValidation:
    def test_valid_record(self, tmp_path):
        assert C.result_problems(_valid_record(tmp_path), 1, 0) == []

    @pytest.mark.parametrize("mutation", sorted(MUTATIONS))
    def test_malformed_records_are_rejected(self, tmp_path, mutation):
        record = _valid_record(tmp_path)
        MUTATIONS[mutation](record)
        assert C.result_problems(record, 1, 0)

    def test_exit_code_mismatch_and_non_objects(self, tmp_path):
        record = _valid_record(tmp_path)
        assert any("differs" in p for p in C.result_problems(record, 1, 4))
        assert C.result_problems([], 1, 0) == ["the result is not a JSON object"]


# ---------------------------------------------------------------------------
# Child command and launchers
# ---------------------------------------------------------------------------

def _windows_argv(command_line):
    import ctypes
    from ctypes import wintypes
    parse = ctypes.windll.shell32.CommandLineToArgvW
    parse.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int)]
    parse.restype = ctypes.POINTER(wintypes.LPWSTR)
    count = ctypes.c_int()
    argv = parse(command_line, ctypes.byref(count))
    try:
        return [argv[i] for i in range(count.value)]
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.cast(argv, ctypes.c_void_p))


class TestChildCommand:
    def _command(self, tmp_path, **kwargs):
        out = tmp_path / "with space" / "cal out"
        args = dict(series_token=TOKEN, expected_head=HEAD, interpreter=SPACED_PYTHON)
        args.update(kwargs)
        return C.build_child_command(2, out, **args), out

    def test_explicit_argv(self, tmp_path):
        command, out = self._command(tmp_path)
        assert command.argv == (
            SPACED_PYTHON, "-B", "-s", str((_BENCH / "calibrate_generator.py").resolve()),
            "--child", "2", "--output-dir", str(out), "--token", TOKEN,
            "--authorize", "cal-a-02", "--expect-head", HEAD)
        assert command.cwd == str(_REPO)
        assert command.stdout_path == out / "cal-a-02.stdout"
        assert command.stderr_path == out / "cal-a-02.stderr"
        assert command.argv_fingerprint == hashlib.sha256(
            command.command_line.encode("utf-8")).hexdigest()

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows command-line parsing")
    def test_spaces_survive_windows_quoting(self, tmp_path):
        command, _ = self._command(tmp_path)
        assert " " in command.argv[0] and " " in command.argv[7]
        assert _windows_argv(command.command_line) == list(command.argv)

    def test_environment_is_an_explicit_allowlist(self, tmp_path, monkeypatch):
        for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "PG_PASSWORD", "HTTPS_PROXY",
                     "AGENTOPS_DIRECT_EXPORT", "USERPROFILE"):
            monkeypatch.setenv(name, "x")
        command, out = self._command(tmp_path)
        assert set(command.environment_names) <= {
            "SYSTEMROOT", "SYSTEMDRIVE", "PATH", "PYTHONPATH", "PYTHONDONTWRITEBYTECODE",
            "PYTHONUNBUFFERED", "TEMP", "TMP"}
        assert command.env["TEMP"] == command.env["TMP"] == str(out / "temp" / "cal-a-02")

    @pytest.mark.parametrize("kwargs, message", [
        ({"interpreter": 'C:\\py"thon.exe'}, "unsafe"),
        ({"interpreter": "C:\\100%\\python.exe"}, "unsafe"),
        ({"interpreter": "C:\\a!b\\python.exe"}, "unsafe"),
        ({"series_token": "XYZ"}, "series token"),
        ({"expected_head": "abc"}, "40-character"),
    ])
    def test_unsafe_or_malformed_arguments(self, tmp_path, kwargs, message):
        with pytest.raises(C.CalibrationError, match=message):
            self._command(tmp_path, **kwargs)

    def test_detached_wrapper_keeps_the_verified_form(self, tmp_path):
        text = C.detached_orchestrator_command(
            interpreter=SPACED_PYTHON, runs=2, output_dir=tmp_path / "cal out",
            expected_head=HEAD, log_dir=Path(r"C:\Users\VAMSHI~1\AppData\Local\Temp\S"))
        assert text.startswith("Start-Process cmd.exe -ArgumentList '/d /s /v:on /c \"")
        assert text.endswith(r'1>C:\Users\VAMSHI~1\AppData\Local\Temp\S\orchestrator.out '
                             r'2>C:\Users\VAMSHI~1\AppData\Local\Temp\S\orchestrator.err & '
                             r'>C:\Users\VAMSHI~1\AppData\Local\Temp\S\orchestrator.exit '
                             "echo exit_code=!errorlevel!\"'")
        assert f'"{SPACED_PYTHON}"' in text and text.count("--authorize") == 2
        with pytest.raises(C.CalibrationError, match="space-free"):
            C.detached_orchestrator_command(interpreter=SPACED_PYTHON, runs=1,
                                            output_dir=tmp_path, expected_head=HEAD,
                                            log_dir=Path("C:/with space"))


GOOD_TOKEN = {"is_app_container": True, "app_container_sid": "S-1-15-2-99",
              "capabilities": [], "integrity_level": "low", "loopback_exempt": False}


class FakeAppContainerApi:
    def __init__(self, sid="S-1-15-2-99", token=None):
        self.sid, self.token, self.calls = sid, token or dict(GOOD_TOKEN), []

    def profile_sid(self, name):
        self.calls.append(("profile_sid", name))
        return self.sid

    def create_suspended(self, command, sid, capabilities, stdout, stderr):
        self.calls.append(("create_suspended", sid, capabilities))
        return FakeProcess()

    def token_info(self, process):
        self.calls.append(("token_info",))
        return self.token

    def resume(self, process):
        self.calls.append(("resume",))

    def discard_suspended(self, process):
        self.calls.append(("discard_suspended",))


class TestAppContainer:
    def _launch(self, monkeypatch, tmp_path, api, number=1):
        open_gate(monkeypatch)
        launcher = C.AppContainerLauncher(C.authorize(["cal-a-01"], HEAD), api)
        command = C.build_child_command(number, tmp_path, series_token=TOKEN,
                                        expected_head=HEAD, interpreter=SPACED_PYTHON)
        return launcher(command, io.BytesIO(), io.BytesIO())

    def test_capability_free_launch(self, monkeypatch, tmp_path):
        api = FakeAppContainerApi()
        assert isinstance(self._launch(monkeypatch, tmp_path, api), FakeProcess)
        assert [c[0] for c in api.calls] == ["profile_sid", "create_suspended", "token_info",
                                             "resume"]
        assert api.calls[0][1] == C.APPCONTAINER_PROFILE and api.calls[1][2] == ()
        assert C.APPCONTAINER_CAPABILITIES == ()

    @pytest.mark.parametrize("change, message", [
        ({"is_app_container": False}, "not in an AppContainer"),
        ({"app_container_sid": "S-1-15-2-1"}, "SID"),
        ({"capabilities": ["S-1-15-3-1"]}, "internetClient"),
        ({"capabilities": None}, "unknown"),
        ({"integrity_level": "medium"}, "integrity"),
        ({"loopback_exempt": True}, "loopback"),
        ({"loopback_exempt": None}, "loopback"),
    ])
    def test_rejected_token_is_never_resumed(self, monkeypatch, tmp_path, change, message):
        api = FakeAppContainerApi(token={**GOOD_TOKEN, **change})
        with pytest.raises(C.CalibrationError, match=message):
            self._launch(monkeypatch, tmp_path, api)
        assert [c[0] for c in api.calls][-1] == "discard_suspended"
        assert ("resume",) not in api.calls

    def test_missing_profile_is_never_created(self, monkeypatch, tmp_path):
        api = FakeAppContainerApi(sid=None)
        with pytest.raises(C.CalibrationError, match="its own authorization"):
            self._launch(monkeypatch, tmp_path, api)
        assert [c[0] for c in api.calls] == ["profile_sid"]

    def test_capabilities_constant_is_enforced(self, monkeypatch, tmp_path):
        monkeypatch.setattr(C, "APPCONTAINER_CAPABILITIES", ("S-1-15-3-1",))
        api = FakeAppContainerApi()
        with pytest.raises(C.CalibrationError, match="no capabilities"):
            self._launch(monkeypatch, tmp_path, api)
        assert api.calls == []

    def test_unauthorized_run_is_refused(self, monkeypatch, tmp_path):
        api = FakeAppContainerApi()
        with pytest.raises(C.ExecutionDisabled, match="not covered"):
            self._launch(monkeypatch, tmp_path, api, number=2)
        assert api.calls == []

    def test_token_validation_is_pure(self):
        assert C.validate_app_container_token(GOOD_TOKEN, "S-1-15-2-99") == []
        assert C.validate_app_container_token(None, "S-1-15-2-99") == ["no token information"]


# ---------------------------------------------------------------------------
# OS-level observation
# ---------------------------------------------------------------------------

PROC = f"proc 4242 1 {CREATED}"


class TestObservation:
    def test_script_fails_closed(self):
        script = C.observation_script(4242)
        assert "$ErrorActionPreference = 'Stop'" in script and "SilentlyContinue" not in script
        assert "exit 1" in script and "CreationDate -ge $root.CreationDate" in script
        assert "Get-NetTCPConnection" in script and "Get-NetUDPEndpoint" in script

    def test_clean_observation(self):
        seen = C.parse_observation(0, PROC + "\nproc 4300 4242 " + CREATED + "\n", "", IDENTITY)
        assert seen.problems == [] and seen.root_present and len(seen.processes) == 2

    @pytest.mark.parametrize("code, out, err, message", [
        (1, PROC, "", "exited with 1"),
        (0, PROC, "Access is denied", "stderr"),
        (0, "", "", "identity is missing"),
        (0, "proc 4242 1 2026-10-09T09:00:00.000Z", "", "another process"),
        (0, "proc 4242", "", "malformed"),
        (0, PROC + "\nsomething else", "", "unexpected"),
        (0, PROC + "\ntcp 127.0.0.1:50000 127.0.0.1:4317 Established", "", "endpoints"),
        (0, PROC + "\nudp 0.0.0.0:5353", "", "endpoints"),
        (0, PROC + "\ndll 4242 librdkafka.dll", "", "native network libraries"),
    ])
    def test_problems(self, code, out, err, message):
        seen = C.parse_observation(code, out, err, IDENTITY)
        assert any(message in p for p in seen.problems), seen.problems

    def test_root_missing_is_left_to_the_orchestrator(self):
        seen = C.parse_observation(0, "root-missing", "", IDENTITY)
        assert seen.problems == [] and seen.root_present is False

    def test_observe_process_failures(self, monkeypatch):
        open_gate(monkeypatch)
        auth = C.authorize(["cal-a-01"], HEAD)

        def timeout(script, limit):
            raise subprocess.TimeoutExpired("powershell", limit)

        def denied(script, limit):
            raise PermissionError("denied")

        assert C.observe_process(auth, IDENTITY, runner=timeout).problems == [
            "observation timed out"]
        assert "PermissionError" in C.observe_process(auth, IDENTITY, runner=denied).problems[0]
        ok = C.observe_process(auth, IDENTITY, runner=lambda s, t: (0, PROC, ""))
        assert ok.problems == [] and ok.root_present


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _series_record(directory):
    records = list(Path(directory).glob("series-a-*.json"))
    assert len(records) == 1
    return _read(records[0])


def _journal(directory, stage, number):
    return Path(directory) / "journal" / f"{stage}-{number:02d}.json"


class TestOrchestrator:
    def test_clean_series(self, tmp_path):
        clock = FakeClock()
        deps, launched = _orchestrator(clock, [real_child()] * 3)
        outcome = C.run_series(3, tmp_path, deps)
        assert outcome.completed == 3 and outcome.stopped_reason is None
        assert outcome.ownership_released and not (tmp_path / C.OWNER_FILE).exists()
        assert [c.calibration_id for c in launched] == ["cal-a-01", "cal-a-02", "cal-a-03"]
        assert clock.pauses.count(C.REST_SECONDS) == 2
        for number in (1, 2, 3):
            for stage in ("launch", "process", "complete"):
                assert _journal(tmp_path, stage, number).exists()
            complete = _read(_journal(tmp_path, "complete", number))
            assert complete["exit_status"] == 0 and complete["pid"] == 4242
            assert complete["creation_time"] == CREATED and complete["samples"] >= 1
            assert complete["argv_fingerprint"] == launched[number - 1].argv_fingerprint
            assert "PATH" in complete["environment_names"] and complete["stop_reason"] is None
            assert (tmp_path / f"cal-a-0{number}.stdout").exists()
        record = _series_record(tmp_path)
        assert record["ownership"] == "released" and "cannot prove" in record["note"]

    def test_existing_ownership_refused(self, tmp_path):
        (tmp_path / C.OWNER_FILE).write_text("{}")
        deps, launched = _orchestrator(FakeClock(), [real_child()])
        with pytest.raises(C.CalibrationError, match="owns this directory"):
            C.run_series(1, tmp_path, deps)
        assert launched == []

    @pytest.mark.parametrize("existing", ["cal-a-02.json", "journal/launch-02.json"])
    def test_existing_evidence_refused_before_ownership(self, tmp_path, existing):
        (tmp_path / existing).parent.mkdir(exist_ok=True)
        (tmp_path / existing).write_text("{}")
        deps, launched = _orchestrator(FakeClock(), [real_child()] * 3)
        with pytest.raises(C.CalibrationError, match="never repeated"):
            C.run_series(3, tmp_path, deps)
        assert launched == [] and not (tmp_path / C.OWNER_FILE).exists()

    def _stopped(self, tmp_path, makers, runs=3, **kwargs):
        clock = FakeClock()
        deps, launched = _orchestrator(clock, makers, **kwargs)
        outcome = C.run_series(runs, tmp_path, deps)
        assert outcome.stopped_reason and not outcome.ownership_released
        assert (tmp_path / C.OWNER_FILE).exists()
        assert _series_record(tmp_path)["ownership"] == "retained for review"
        assert len(launched) == 1                      # never retried, nothing after
        return outcome

    def test_nonzero_child_stops(self, tmp_path):
        def broken(plan, **kwargs):
            raise RuntimeError("boom")

        outcome = self._stopped(tmp_path, [real_child(execute=broken), real_child()])
        assert "result rejected" in outcome.stopped_reason
        assert _read(_journal(tmp_path, "complete", 1))["exit_status"] == C.EXIT_FAILED

    def test_exit_code_mismatch_stops(self, tmp_path):
        def broken(plan, **kwargs):
            raise RuntimeError("boom")

        outcome = self._stopped(tmp_path, [real_child(execute=broken, returncode=0)] * 2)
        assert "result rejected" in outcome.stopped_reason

    @pytest.mark.parametrize("content", ["{}", "not json", "[]"])
    def test_malformed_result_stops(self, tmp_path, content):
        outcome = self._stopped(tmp_path, [real_child(
            write=lambda p: p.write_text(content, encoding="utf-8"))] * 2)
        assert "result rejected" in outcome.stopped_reason

    def test_missing_result_stops(self, tmp_path):
        outcome = self._stopped(tmp_path, [real_child(write=lambda p: None)] * 2)
        assert "no result file" in outcome.stopped_reason

    def test_timeout_preserves_the_child(self, tmp_path):
        hung = FakeProcess(polls=None, pid=5150)
        outcome = self._stopped(tmp_path, [lambda c: hung, real_child()])
        assert "PID 5150" in outcome.stopped_reason and CREATED in outcome.stopped_reason
        assert "not terminated" in outcome.stopped_reason and not hung.killed
        complete = _read(_journal(tmp_path, "complete", 1))
        assert complete["child_left_running"] and complete["exit_status"] is None
        assert _read(_journal(tmp_path, "process", 1))["pid"] == 5150

    @pytest.mark.parametrize("observe, message", [
        (lambda i: C.Observation(["observation stderr: Access is denied"], [], [], False),
         "failed closed"),
        (lambda i: C.Observation(["observation timed out"], [], [], False), "failed closed"),
        (lambda i: C.Observation([], [], [], False), "identity is missing"),
        (lambda i: C.Observation(["network endpoints or native network libraries observed"],
                                 ["tcp 127.0.0.1:50000 127.0.0.1:9092 Established"], [PROC],
                                 True), "network endpoints"),
    ])
    def test_observation_failures_stop(self, tmp_path, observe, message):
        alive = FakeProcess(polls=None)
        outcome = self._stopped(tmp_path, [lambda c: alive, real_child()], observe=observe)
        assert message in outcome.stopped_reason and not alive.killed
        assert _read(_journal(tmp_path, "complete", 1))["child_left_running"]

    def test_observation_exception_stops(self, tmp_path):
        def raising(identity):
            raise RuntimeError("cim failure")

        outcome = self._stopped(tmp_path, [lambda c: FakeProcess(polls=None), real_child()],
                                observe=raising)
        assert "observation raised RuntimeError" in outcome.stopped_reason

    def test_missing_identity_at_exit_is_not_a_failure(self, tmp_path):
        state = {"n": 0}

        def observe(identity):
            state["n"] += 1
            return good_observe(identity) if state["n"] == 1 else C.Observation(
                [], [], [], False)

        deps, _ = _orchestrator(FakeClock(), [real_child(polls=2)], observe=observe)
        outcome = C.run_series(1, tmp_path, deps)
        assert outcome.stopped_reason is None and outcome.ownership_released

    def test_insufficient_sampling_stops(self, tmp_path):
        clock = FakeClock()

        def slow(identity):
            clock.now += C.MAX_SAMPLE_GAP_SECONDS + 1
            return good_observe(identity)

        deps, launched = _orchestrator(clock, [real_child(polls=3)] * 2, observe=slow)
        outcome = C.run_series(2, tmp_path, deps)
        assert "insufficient sampling" in outcome.stopped_reason and len(launched) == 1

    def test_no_observation_at_all_stops(self, tmp_path):
        outcome = self._stopped(tmp_path, [real_child(polls=0)] * 2)
        assert "no observation" in outcome.stopped_reason

    def test_launch_failure_is_recorded(self, tmp_path):
        def failing(command):
            raise OSError("CreateProcess failed")

        outcome = self._stopped(tmp_path, [failing, real_child()])
        assert "launch failed" in outcome.stopped_reason
        assert _journal(tmp_path, "launch", 1).exists()
        assert not _journal(tmp_path, "process", 1).exists()
        assert _read(_journal(tmp_path, "complete", 1))["pid"] is None

    def test_interruption_keeps_ownership_and_writes_the_record(self, tmp_path):
        def interrupted(command):
            raise KeyboardInterrupt

        deps, launched = _orchestrator(FakeClock(), [interrupted, real_child()])
        with pytest.raises(KeyboardInterrupt):
            C.run_series(2, tmp_path, deps)
        record = _series_record(tmp_path)
        assert record["stopped_reason"] == "series interrupted: KeyboardInterrupt"
        assert record["ownership"] == "retained for review"
        assert (tmp_path / C.OWNER_FILE).exists() and len(launched) == 1

    def test_crash_before_the_first_child(self, tmp_path):
        (tmp_path / "cal-a-01.stdout").write_text("")       # exclusive output refused
        deps, launched = _orchestrator(FakeClock(), [real_child()])
        outcome = C.run_series(1, tmp_path, deps)
        assert "series failed: FileExistsError" in outcome.stopped_reason
        assert launched == [] and (tmp_path / C.OWNER_FILE).exists()
        assert _series_record(tmp_path)["ownership"] == "retained for review"

    def test_series_bounds(self, tmp_path):
        for runs in (0, 21, True):
            with pytest.raises(C.CalibrationError):
                C.run_series(runs, tmp_path, _orchestrator(FakeClock(), [])[0])


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
# Unchanged benchmark code
# ---------------------------------------------------------------------------

class TestScope:
    def test_benchmark_authorization_and_rules_unchanged(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == ["bm-smoke-r0005-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep3"]
        assert RB.DRIVER_LATE_START_SECONDS == 0.1 and M.LATE_START_FRACTION == 0.05
        assert RB.OWNER_FILE == "ACTIVE_RUN" and C.OWNER_FILE == "ACTIVE_CALIBRATION"

    def test_benchmark_directory_untouched_by_the_harness(self):
        code = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8").split('"""', 2)[2]
        assert "ACTIVE_RUN" not in code and "ledger" not in code.lower()

    def test_gates_closed_in_source(self):
        code = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8")
        assert "\nEXECUTION_ENABLED: bool = False\n" in code
        assert "\nAPPROVED_CALIBRATIONS: frozenset[str] = frozenset()\n" in code
