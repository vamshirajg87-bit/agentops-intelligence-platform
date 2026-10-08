"""
benchmarks/tests/test_run_cell.py

Offline tests of one benchmark cell, driven end to end with a fake D3
harness and fake E2E helpers.  No process, socket, container, database or
telemetry is used: the `no_live_access` fixture makes any attempt fail.
"""

from __future__ import annotations

import dataclasses
import importlib.abc
import os
import json
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

_BENCH = Path(__file__).resolve().parents[1]
_REPO = _BENCH.parent
if str(_BENCH) not in sys.path:
    sys.path.insert(0, str(_BENCH))

import metrics as M  # noqa: E402
import run_benchmark as RB  # noqa: E402

LADDER = json.loads((_BENCH / "ladder.json").read_text(encoding="utf-8"))
SHA = "b" * 40
GB = 1024 ** 3


# ---------------------------------------------------------------------------
# Fail-closed isolation: whatever LIVE_ENABLED says, no test can reach the
# real harness, a process, a socket or Docker.
# ---------------------------------------------------------------------------

_HARNESS_MODULES = ("run_smoke", "run_e2e")
#: The real cell body, for tests that drive it with an injected fake harness.
REAL_EXECUTE_CELL = RB.execute_cell


class _BlockHarness(importlib.abc.MetaPathFinder):
    """Refuses to import the D3/E2E harness modules during the tests."""

    def find_spec(self, name, path=None, target=None):
        if name in _HARNESS_MODULES:
            raise ImportError(f"{name} is blocked in the benchmark tests")
        return None


class LiveAccessAttempt(AssertionError):
    """A test reached something live."""


@pytest.fixture(autouse=True)
def fail_closed(monkeypatch):
    def refuse(what):
        def call(*args, **kwargs):
            raise LiveAccessAttempt(f"{what} attempted: {args[:1]}")
        return call

    for name in _HARNESS_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    monkeypatch.setattr(sys, "meta_path", [_BlockHarness(), *sys.meta_path])
    monkeypatch.setattr(RB, "execute_cell", refuse("the real execute_cell"))
    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, refuse(f"subprocess.{name}"))
    monkeypatch.setattr(os, "system", refuse("os.system"))
    monkeypatch.setattr(socket, "create_connection", refuse("a network connection"))
    monkeypatch.setattr(socket.socket, "connect", refuse("a socket connection"))


@pytest.fixture(autouse=True)
def no_live_access(monkeypatch):
    def refuse(*args, **kwargs):
        raise LiveAccessAttempt(f"live access attempted: {args[:1]}")

    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(RB, "local_platform_info", lambda: {"os": "test"})


class Boom(RuntimeError):
    pass


class FakeSmoke:
    """Records every call; `fail` names the step that raises."""

    GROUP_STREAM, GROUP_STORAGE = "d3-stream-processor", "d3-storage-consumer"
    TOPIC_RAW, TOPIC_SPANS, TOPIC_DLQ = "raw", "spans", "dlq"
    POSTGRES_PORT, T_COMMAND = 15432, 60.0

    def __init__(self, tmp: Path, *, fail: str = "", cleanup_ok: bool = True,
                 stats: str = "proj-kafka-1|5.0%|100MiB / 1GiB\nproj-pg-1|1.0%|50MiB / 1GiB",
                 lag=0, clock=(1000.0, 1000.001, 1000.002), late_starts=0, dlq=0):
        self.calls: list[str] = []
        self.fail, self.cleanup_ok, self.stats, self.lag = fail, cleanup_ok, stats, lag
        self.clock_sample, self.late_starts, self.dlq = clock, late_starts, dlq
        self.RATE = self.SEED = self.CONCURRENCY = None
        self.T_DRAIN = self.SETTLE_SECONDS = None
        self.RUN = SimpleNamespace(temp=tmp, python=Path("python"),
                                   generated={"POSTGRES_PASSWORD": "never-printed"})
        self.stage_ids: list[str] = []
        self.psql_sql: list[str] = []
        self.commands: list[list[str]] = []
        self.offsets = {"raw": 0, "spans": 0, "dlq": 0}

    def _step(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise Boom(f"{name} failed")

    # -- stack ----------------------------------------------------------------
    def git(self, *args):
        return SHA + "\n"

    def repository_state(self):
        return []

    def __getattr__(self, name):
        if name in ("check_repository", "check_ports", "record_docker_state", "create_workspace",
                    "create_virtual_environment", "check_render", "start_kafka", "create_topics",
                    "start_postgres", "run_bootstrap", "start_collector", "start_host_services",
                    "check_host_processes", "stop_host_processes"):
            return lambda *args, **kwargs: self._step(name)
        raise AttributeError(name)

    def host_environment(self, **extra):
        return {"PATH": "x", **extra}

    def check_normal_stack(self, when):
        self._step("check_normal_stack")

    def check_baseline(self):
        self._step("check_baseline")
        return dict(self.offsets)

    def Stage(self, **kwargs):
        return SimpleNamespace(**kwargs)

    def planned_facts(self, stage):
        return {"digest": "d" * 64}

    def run_stage(self, stage, offsets):
        warm = stage.run_id.split("-")[-1].startswith("w")
        self._step("run_stage_warmup" if warm else "run_stage_measured")
        self.stage_ids.append(stage.run_id)
        self.offsets = {"raw": offsets["raw"] + stage.traces,
                        "spans": offsets["spans"] + stage.spans,
                        "dlq": offsets["dlq"] + (0 if warm else self.dlq)}
        finished = datetime.fromtimestamp(5000.0, tz=timezone.utc).isoformat()
        report = {"execution": {
            "started": datetime.fromtimestamp(4980.0, tz=timezone.utc).isoformat(),
            "finished": finished, "elapsed_seconds": 20.0,
            "executed_attempts": stage.traces, "started_requests": stage.traces,
            "not_started_requests": 0, "spans_ended": stage.spans,
            "spans_exported_successfully": stage.spans, "export_failures": 0,
            "flush_ok": True, "run_status": "completed", "problems": [],
            "achieved_submission_rate": stage.traces / 20.0,
            "late_starts": self.late_starts}}
        (self.RUN.temp / f"{stage.run_id}.json").write_text(json.dumps(report), encoding="utf-8")
        return dict(self.offsets)

    def compose(self, *args, **kwargs):
        self._step("compose_" + args[0])
        return "id-kafka\nid-pg\n"

    def group_lag(self, group, topic):
        if isinstance(self.lag, Exception):
            raise self.lag
        return self.lag

    def checked(self, command, *, timeout, what, env=None, **kwargs):
        self.commands.append(list(command))
        if command[:2] == ["docker", "stats"]:
            self._step("docker_stats")
            return self.stats
        if len(command) > 1 and command[1] == "-c":
            self._step("clock_probe")
            assert env and "PGPASSWORD" in env
            return json.dumps([list(self.clock_sample)])
        if command[:2] == ["docker", "info"]:
            return json.dumps({"ServerVersion": "29", "NCPU": 8, "MemTotal": 8 * GB})
        return "ok"

    def psql(self, sql):
        self._step("psql")
        self.psql_sql.append(sql)
        rows = 700
        return json.dumps({
            "rows": rows, "distinct_spans": rows, "roots": 100,
            "first_ingested": 5000.0, "last_ingested": 5020.0,
            "request_ms": [250.0] * 100, "persistence_ms": [40.0] * rows,
            "per_second": [35] * 20})

    def run(self, command, *, timeout, what, **kwargs):
        self._step("refresh_run")
        self.commands.append(list(command))
        code = 3 if self.fail == "refresh_exit" else 0
        return subprocess.CompletedProcess(
            command, code, "Finished running 9 view models in 0 hours 0 minutes and 2.5 "
            "seconds", "")

    def cleanup(self):
        self.calls.append("cleanup")
        if self.fail == "cleanup_raises":
            raise Boom("cleanup")
        return self.cleanup_ok


class FakeE2E:
    REFRESH_SERVICE = "analytics-refresh"

    def __init__(self, smoke: FakeSmoke, *, ledger_changes: bool = False):
        self.smoke, self.ledger_changes, self.ledger_reads = smoke, ledger_changes, 0

    def build_refresh_image(self):
        self.smoke._step("build_refresh_image")

    def refresh_command(self, *args):
        return ["docker", "compose", "--profile", "analytics-refresh", *args]

    def ledger_fingerprint(self):
        self.ledger_reads += 1
        return "17 steps, x" if not (self.ledger_changes and self.ledger_reads > 1) else "18"

    def run_detector(self, label):
        self.smoke._step("run_detector")
        return ([{"signal_path": "trace_latency", "operation_name": "agentops.request",
                  "duration_ms": "12.5"},
                 {"signal_path": "tool_failure", "operation_name": "", "duration_ms": "3"}], {})

    def remove_profile_resources(self):
        self.smoke.calls.append("remove_profile_resources")


class FakeSampler:
    """Samples once at start and once at stop, synchronously, like the real
    one: a sample that raises is kept as None."""

    instances: list["FakeSampler"] = []

    def __init__(self, sample, interval):
        self.sample, self.samples, self.errors, self.stops = sample, [], [], 0
        FakeSampler.instances.append(self)

    def _take(self, at):
        try:
            self.samples.append((at, self.sample()))
        except Exception as exc:
            self.samples.append((at, None))
            self.errors.append((at, str(exc)))

    def start(self):
        self._take(0.0)

    def stop(self):
        self.stops += 1
        if self.stops == 1:
            self._take(10.0)


def _cell(level="SMOKE", **changes):
    cell = RB.plan_cells(LADDER, level, rates=[5 if level == "SMOKE" else
                                             LADDER["levels"][[n for n, _ in M.LEVELS]
                                                              .index(level)]["rates"][0]],
                         repetitions=1)[0]
    return dataclasses.replace(cell, **changes) if changes else cell


def _run(tmp_path, *, smoke=None, e2e=None, cell=None, disk=500 * GB, clock=None, ladder=None,
         out=None):
    smoke = smoke or FakeSmoke(tmp_path)
    e2e = e2e or FakeE2E(smoke)
    ticks = iter(range(10 ** 6))
    result, path, clean = REAL_EXECUTE_CELL(
        cell or _cell(), ladder or LADDER, smoke=smoke, e2e=e2e, expect_head=None,
        output_dir=out or (tmp_path / "results"), disk_free=lambda: disk,
        clock=clock or (lambda: float(next(ticks))),
        wall=lambda: 5003.0, sampler_factory=FakeSampler)
    return result, path, clean, smoke


ORDER = ["check_repository", "check_ports", "record_docker_state", "create_workspace",
         "create_virtual_environment", "check_render", "start_kafka", "create_topics",
         "start_postgres", "run_bootstrap", "build_refresh_image", "start_collector",
         "start_host_services", "check_baseline", "run_stage_warmup", "clock_probe",
         "compose_ps", "docker_stats", "run_stage_measured", "docker_stats", "psql",
         "clock_probe", "check_host_processes", "refresh_run", "run_detector",
         "check_normal_stack", "stop_host_processes", "remove_profile_resources", "cleanup"]


class TestSuccess:
    def test_full_cell_is_valid(self, tmp_path):
        result, path, clean, smoke = _run(tmp_path)
        assert clean and result["verdict"] == "VALID"
        assert smoke.calls == ORDER
        assert path.parent == (tmp_path / "results").resolve() and path.is_file()
        M.validate_result(json.loads(path.read_text(encoding="utf-8")))
        assert (smoke.RATE, smoke.SEED, smoke.CONCURRENCY) == (5, 42, 32)
        assert smoke.T_DRAIN == 120.0 and smoke.SETTLE_SECONDS == 6.0

    def test_warmup_is_excluded_from_measured_counts(self, tmp_path):
        result, _, _, smoke = _run(tmp_path)
        cell = _cell()
        assert smoke.stage_ids == [cell.warmup_run_id, cell.measured_run_id]
        assert result["counts"]["canonical_records"] == 700        # not 700 + warm-up
        assert result["counts"]["raw_records"] == 100
        assert len(smoke.psql_sql) == 1
        assert cell.measured_run_id in smoke.psql_sql[0]
        assert cell.warmup_run_id not in smoke.psql_sql[0]

    def test_timing_kinds(self, tmp_path):
        analytics = _run(tmp_path)[0]["metrics"]["analytics"]
        assert analytics["refresh_seconds"]["kind"] == "OBSERVATION"
        assert analytics["refresh_dbt_seconds"] == {**analytics["refresh_dbt_seconds"],
                                                   "kind": "MEASURED", "value": 2.5}
        detector = analytics["detector_ms"]
        assert detector["trace_latency/agentops.request"]["kind"] == "MEASURED"
        assert detector["trace_latency/agentops.request"]["value"] == 12.5
        (tmp_path / "b").mkdir()
        lag = _run(tmp_path / "b")[0]["metrics"]["lag"]
        assert lag["drain_seconds"]["kind"] == "OBSERVATION" and lag["drain_seconds"]["value"] == 3

    def test_resource_sampling_is_limited_to_project_containers(self, tmp_path):
        _, _, _, smoke = _run(tmp_path)
        stats = [c for c in smoke.commands if c[:2] == ["docker", "stats"]]
        assert stats and all(c[-2:] == ["id-kafka", "id-pg"] for c in stats)
        assert all("--no-stream" in c for c in stats)

    def test_dbt_duration_missing_is_not_measured(self, tmp_path):
        smoke = FakeSmoke(tmp_path)
        smoke.run = lambda command, **kw: (smoke.calls.append("refresh_run") or
                                           subprocess.CompletedProcess(command, 0, "done", ""))
        analytics = _run(tmp_path, smoke=smoke)[0]["metrics"]["analytics"]
        assert analytics["refresh_dbt_seconds"]["kind"] == "NOT_MEASURED"
        assert analytics["refresh_seconds"]["kind"] == "OBSERVATION"

    def test_level_without_refresh_or_detector(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path, cell=_cell(refresh_and_detector=False))
        assert clean and result["verdict"] == "VALID"
        for step in ("build_refresh_image", "refresh_run", "run_detector",
                     "remove_profile_resources"):
            assert step not in smoke.calls
        analytics = result["metrics"]["analytics"]
        assert analytics["refresh_seconds"]["kind"] == "NOT_MEASURED"
        assert analytics["detector_ms"]["kind"] == "NOT_MEASURED"


STEPS = [s for s in ORDER if s not in ("stop_host_processes", "remove_profile_resources",
                                       "cleanup", "docker_stats")]


class TestFailures:
    @pytest.mark.parametrize("step", STEPS)
    def test_failure_at_each_step_is_invalid_and_cleaned_up(self, tmp_path, step):
        smoke = FakeSmoke(tmp_path, fail=step)
        result, path, clean, smoke = _run(tmp_path, smoke=smoke)
        assert result["verdict"] == "INVALID" and path.is_file()
        assert "Boom" in " ".join(g["evidence"] for g in result["integrity"]["gates"])
        tail = smoke.calls[smoke.calls.index(step) + 1:]
        assert tail[-1] == "cleanup" and "stop_host_processes" in tail
        built = "build_refresh_image" in smoke.calls
        assert ("remove_profile_resources" in tail) == built
        assert not any(c in tail for c in ("run_stage_measured", "run_detector")
                       if c != step and ORDER.index(c) < ORDER.index(step))

    def test_failed_refresh_exit_is_invalid(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, fail="refresh_exit"))[0]
        assert result["verdict"] == "INVALID"

    def test_refresh_that_changes_the_ledger_is_invalid(self, tmp_path):
        smoke = FakeSmoke(tmp_path)
        result = _run(tmp_path, smoke=smoke, e2e=FakeE2E(smoke, ledger_changes=True))[0]
        assert result["verdict"] == "INVALID"
        assert "ledger" in " ".join(g["evidence"] for g in result["integrity"]["gates"])

    def test_integrity_failure_is_invalid(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, dlq=1))[0]
        assert result["verdict"] == "INVALID" and result["integrity"]["ok"] is False

    def test_lag_sampling_failure_stays_unknown(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, lag=Boom("jvm")))[0]
        lag = result["metrics"]["lag"]
        assert lag["raw_max"]["kind"] == "NOT_MEASURED" and lag["raw_max"]["value"] is None
        assert result["verdict"] == "VALID"

    def test_unknown_lag_value_is_not_zero(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, lag=None))[0]
        assert result["metrics"]["lag"]["canonical_max"]["kind"] == "NOT_MEASURED"

    def test_generator_bound_from_scheduler(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, late_starts=6))[0]
        assert result["verdict"] == "GENERATOR_BOUND"


class TestStops:
    def test_disk_preflight_stops_before_the_stack(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path, disk=5 * GB)
        assert result["verdict"] == "STOPPED" and clean
        assert "preflight" in result["stop"]["reasons"][0]
        assert "start_kafka" not in smoke.calls and smoke.calls[-1] == "cleanup"

    def test_disk_projection_scales_with_level(self):
        assert RB.projected_need_bytes(_cell("SMALL")) > RB.projected_need_bytes(_cell())

    def test_memory_stop(self, tmp_path):
        smoke = FakeSmoke(tmp_path, stats="proj-pg-1|1.0%|950MiB / 1000MiB")
        result = _run(tmp_path, smoke=smoke)[0]
        assert result["verdict"] == "STOPPED"
        assert any("memory" in reason for reason in result["stop"]["reasons"])

    def test_cell_budget_stop(self, tmp_path):
        ladder = json.loads(json.dumps(LADDER))
        ladder["defaults"]["cell_budget_seconds"] = 2  # the fake clock ticks once per read
        result, _, clean, smoke = _run(tmp_path, ladder=ladder)
        assert result["verdict"] == "STOPPED" and clean
        assert "budget" in result["stop"]["reasons"][0]
        assert "run_stage_measured" not in smoke.calls and smoke.calls[-1] == "cleanup"

    def test_invalid_clock_blocks_persistence_latency_only(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, clock=(1000.0, 1000.0, 1000.05)))[0]
        assert result["verdict"] == "VALID" and result["clock"]["valid"] is False
        assert result["metrics"]["latency"]["persistence_ms"]["p95"]["kind"] == "NOT_MEASURED"
        assert not result["claims"]["persistence_latency"]

    def test_valid_clock_allows_persistence_latency(self, tmp_path):
        result = _run(tmp_path)[0]
        assert result["clock"]["valid"] is True
        assert result["metrics"]["latency"]["persistence_ms"]["p95"]["value"] == 40.0


class TestCleanup:
    def test_unverified_cleanup_stops_and_blocks(self, tmp_path):
        result, path, clean, smoke = _run(tmp_path, smoke=FakeSmoke(tmp_path, cleanup_ok=False))
        assert not clean and result["verdict"] == "STOPPED"
        assert "cleanup not verified" in result["stop"]["reasons"]
        assert (path.parent / RB.CLEANUP_MARKER).is_file()

    def test_cleanup_that_raises_is_unverified(self, tmp_path):
        result, _, clean, _ = _run(tmp_path, smoke=FakeSmoke(tmp_path, fail="cleanup_raises"))
        assert not clean and result["verdict"] == "STOPPED"

    def test_failing_cleanup_step_still_runs_the_rest(self, tmp_path):
        smoke = FakeSmoke(tmp_path, fail="stop_host_processes")
        result, _, clean, smoke = _run(tmp_path, smoke=smoke)
        assert not clean and smoke.calls[-2:] == ["remove_profile_resources", "cleanup"]
        assert result["verdict"] == "STOPPED"

    def test_samplers_are_stopped_even_on_failure(self, tmp_path):
        FakeSampler.instances.clear()
        _run(tmp_path, smoke=FakeSmoke(tmp_path, fail="run_stage_measured"))
        assert FakeSampler.instances and all(s.stops >= 1 for s in FakeSampler.instances)

    def test_result_never_written_inside_repository(self, tmp_path):
        smoke = FakeSmoke(tmp_path)
        with pytest.raises(RB.BenchmarkError):
            _run(tmp_path, smoke=smoke, out=_REPO / "benchmarks" / "results")
        assert smoke.calls[-1] == "cleanup"
        assert not (_REPO / "benchmarks" / "results").exists()

    def test_marker_never_inside_repository(self):
        with pytest.raises(RB.BenchmarkError):
            RB._mark_cleanup_unverified(_REPO / "benchmarks", _cell())


class TestLiveStaysDisabled:
    def test_gate_is_a_boolean(self):
        assert isinstance(RB.LIVE_ENABLED, bool)

    def test_run_cell_refuses_before_importing_the_harness(self, tmp_path, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        monkeypatch.setattr(RB, "execute_cell", lambda *a, **k: pytest.fail("reached"))
        for name in ("run_smoke", "run_e2e"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        with pytest.raises(RB.LiveDisabled):
            RB.run_cell(_cell(), LADDER, expect_head=None, output_dir=tmp_path)
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules

    def test_cli_live_refused(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        assert RB.main(["--level", "SMOKE", "--live", "--output-dir", str(tmp_path)]) == 2
        assert "not enabled" in capsys.readouterr().err

    def test_plan_mode_warns_about_cleanup_marker(self, tmp_path):
        import io
        (tmp_path / RB.CLEANUP_MARKER).write_text("x", encoding="utf-8")
        out = io.StringIO()
        assert RB.main(["--level", "SMOKE", "--output-dir", str(tmp_path)], stdout=out) == 0
        assert "live runs are blocked" in out.getvalue()

    def test_plan_mode_has_no_side_effects(self, tmp_path):
        import io
        out = io.StringIO()
        assert RB.main(["--level", "SMALL", "--output-dir", str(tmp_path / "o")], stdout=out) == 0
        assert out.getvalue().startswith("PLAN ONLY") and not (tmp_path / "o").exists()


# ---------------------------------------------------------------------------
# Exact-cell live authorization (Phase 14.4B3)
# ---------------------------------------------------------------------------

HEAD = "8" * 40
APPROVED = ["--level", "SMOKE", "--rate", "5", "--repetitions", "1", "--concurrency", "32",
            "--warmup-requests", "25"]


def _approved_cells(level="SMOKE", **overrides):
    kwargs = dict(rates=[5], repetitions=1, concurrency=32, warmup_requests=25)
    kwargs.update(overrides)
    return RB.plan_cells(LADDER, level, **kwargs)


class TestLiveAuthorization:
    def test_exact_approved_cell(self):
        cell = RB.check_live_authorization(
            _approved_cells(), authorize="bm-smoke-r0005-c32-rep1", expect_head=HEAD,
            warmup_requests=25)
        assert (cell.benchmark_id, cell.requests, cell.rate, cell.concurrency,
                cell.repetition, cell.warmup_requests) == (
            "bm-smoke-r0005-c32-rep1", 100, 5, 32, 1, 25)

    @pytest.mark.parametrize("authorize", [None, "", "bm-smoke-r0010-c32-rep1",
                                           "bm-small-r0005-c32-rep1"])
    def test_missing_or_wrong_authorization(self, authorize):
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            RB.check_live_authorization(_approved_cells(), authorize=authorize,
                                        expect_head=HEAD, warmup_requests=25)

    @pytest.mark.parametrize("head", [None, "", "812ff80", "Z" * 40, HEAD + "0"])
    def test_missing_or_malformed_head(self, head):
        with pytest.raises(RB.BenchmarkError, match="expect-head"):
            RB.check_live_authorization(_approved_cells(), authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=head, warmup_requests=25)

    @pytest.mark.parametrize("warmup", [None, 24, 26, 150])
    def test_wrong_warmup_argument(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 25"):
            RB.check_live_authorization(_approved_cells(), authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=HEAD, warmup_requests=warmup)

    def test_planned_warmup_must_match_too(self):
        with pytest.raises(RB.BenchmarkError, match="warmup_requests"):
            RB.check_live_authorization(_approved_cells(warmup_requests=30),
                                        authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=HEAD, warmup_requests=25)

    @pytest.mark.parametrize("overrides, field", [
        (dict(rates=[10]), "benchmark_id"),
        (dict(concurrency=16), "concurrency"),
        (dict(rates=[10], level="SMALL"), "level"),
    ])
    def test_wrong_cell_fields(self, overrides, field):
        with pytest.raises(RB.BenchmarkError, match=field):
            RB.check_live_authorization(_approved_cells(**overrides),
                                        authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=HEAD, warmup_requests=25)

    def test_wrong_repetition(self):
        second = dataclasses.replace(_approved_cells()[0], repetition=2)
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            RB.check_live_authorization([second], authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=HEAD, warmup_requests=25)

    @pytest.mark.parametrize("overrides", [dict(repetitions=2), dict(rates=[5, 10])])
    def test_multiple_planned_cells(self, overrides):
        with pytest.raises(RB.BenchmarkError, match="exactly one cell"):
            RB.check_live_authorization(_approved_cells(**overrides),
                                        authorize="bm-smoke-r0005-c32-rep1",
                                        expect_head=HEAD, warmup_requests=25)

    def test_warmup_override_validation_and_default(self):
        assert RB.plan_cells(LADDER, "SMOKE", rates=[5], repetitions=1)[0].warmup_requests == 150
        assert _approved_cells()[0].warmup_requests == 25
        for bad in (0, -1, RB.MAX_WARMUP_REQUESTS + 1, True, 2.5):
            with pytest.raises(RB.BenchmarkError, match="warm-up"):
                RB.plan_cells(LADDER, "SMOKE", warmup_requests=bad)


class TestLiveCli:
    """The CLI with the gate opened in memory only, and run_cell replaced:
    nothing is imported or started."""

    @pytest.fixture
    def gate_open(self, monkeypatch, tmp_path):
        calls = []
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", lambda cell, ladder, **kw: calls.append(
            (cell.benchmark_id, kw["expect_head"])) or ({"verdict": "VALID"}, tmp_path / "r", True))
        for name in ("run_smoke", "run_e2e"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        return calls

    def _main(self, tmp_path, *extra):
        return RB.main([*APPROVED, "--live", "--output-dir", str(tmp_path / "out"), *extra],
                       stdout=__import__("io").StringIO())

    def test_exact_cell_reaches_run_cell_once(self, gate_open, tmp_path):
        code = self._main(tmp_path, "--authorize", "bm-smoke-r0005-c32-rep1",
                          "--expect-head", HEAD)
        assert code == 0 and gate_open == [("bm-smoke-r0005-c32-rep1", HEAD)]

    @pytest.mark.parametrize("extra", [
        [],
        ["--authorize", "bm-smoke-r0005-c32-rep1"],
        ["--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0010-c32-rep1", "--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD, "--rate", "10"],
        ["--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD,
         "--repetitions", "2"],
        ["--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD,
         "--concurrency", "16"],
        ["--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD,
         "--warmup-requests", "26"],
    ])
    def test_everything_else_refused_before_anything_starts(self, gate_open, tmp_path, extra,
                                                           capsys):
        assert self._main(tmp_path, *extra) == 2
        assert gate_open == [] and "nothing was started" in capsys.readouterr().err
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules

    def test_wrong_level_refused(self, gate_open, tmp_path, capsys):
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetitions", "1",
                        "--concurrency", "32", "--warmup-requests", "25", "--live",
                        "--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD,
                        "--output-dir", str(tmp_path)])
        assert code == 2 and gate_open == []

    def test_cleanup_marker_still_blocks(self, gate_open, tmp_path, capsys):
        (tmp_path / "out").mkdir()
        (tmp_path / "out" / RB.CLEANUP_MARKER).write_text("x", encoding="utf-8")
        code = self._main(tmp_path, "--authorize", "bm-smoke-r0005-c32-rep1",
                          "--expect-head", HEAD)
        assert code == 2 and gate_open == [] and "cleanup" in capsys.readouterr().err

    def test_run_cell_refuses_other_cells_before_imports(self, monkeypatch, tmp_path):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "execute_cell", lambda *a, **k: pytest.fail("reached"))
        for name in ("run_smoke", "run_e2e"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        for cell in (_approved_cells(warmup_requests=150)[0], _approved_cells(rates=[10])[0]):
            with pytest.raises(RB.BenchmarkError, match="not the approved live cell"):
                RB.run_cell(cell, LADDER, expect_head=HEAD, output_dir=tmp_path)
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules


class TestGateClosed:
    """The gate's refusal, tested with the gate forced closed in memory."""

    @pytest.fixture(autouse=True)
    def closed(self, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)

    def test_exact_cell_is_still_refused_by_the_gate(self, tmp_path, capsys):
        code = RB.main([*APPROVED, "--live", "--authorize", "bm-smoke-r0005-c32-rep1",
                        "--expect-head", HEAD, "--output-dir", str(tmp_path)])
        assert code == 2 and "not enabled" in capsys.readouterr().err
        assert "run_smoke" not in sys.modules

    def test_plan_mode_with_override_has_no_side_effects(self, tmp_path):
        import io
        out = io.StringIO()
        assert RB.main([*APPROVED, "--output-dir", str(tmp_path / "o")], stdout=out) == 0
        text = out.getvalue()
        assert "warm-up 25 requests" in text and text.count("bm-") == 1
        assert not (tmp_path / "o").exists()

    def test_authorize_without_live_is_rejected(self):
        with pytest.raises(SystemExit):
            RB.main([*APPROVED, "--authorize", "bm-smoke-r0005-c32-rep1"])

# ---------------------------------------------------------------------------
# Both gate states, in memory only: nothing reaches the real harness
# ---------------------------------------------------------------------------

EXACT = [*APPROVED, "--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD]


@pytest.fixture(params=[False, True], ids=["gate-closed", "gate-open"])
def gate(request, monkeypatch):
    monkeypatch.setattr(RB, "LIVE_ENABLED", request.param)
    return request.param


class TestIsolationGuards:
    def test_guards_are_active(self, tmp_path):
        with pytest.raises(LiveAccessAttempt):
            subprocess.run(["docker", "ps"])
        with pytest.raises(LiveAccessAttempt):
            subprocess.Popen(["git", "status"])
        with pytest.raises(LiveAccessAttempt):
            subprocess.check_output(["docker", "info"])
        with pytest.raises(LiveAccessAttempt):
            os.system("docker ps")
        with pytest.raises(LiveAccessAttempt):
            socket.create_connection(("127.0.0.1", 15432))
        with pytest.raises(LiveAccessAttempt):
            RB.execute_cell(_cell(), LADDER, smoke=None, e2e=None, expect_head=HEAD,
                            output_dir=tmp_path, disk_free=lambda: 0)
        for name in ("run_smoke", "run_e2e"):
            with pytest.raises(ImportError, match="blocked"):
                __import__(name)

    def test_fake_harness_path_is_explicit(self):
        assert REAL_EXECUTE_CELL is not RB.execute_cell


class TestBothGateStates:
    def test_real_run_cell_never_reaches_the_harness(self, gate, tmp_path):
        cell = _approved_cells()[0]
        expected = ImportError if gate else RB.LiveDisabled
        with pytest.raises(expected):
            RB.run_cell(cell, LADDER, expect_head=HEAD, output_dir=tmp_path)
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules

    def test_exact_command_never_reaches_infrastructure(self, gate, tmp_path, capsys):
        args = [*EXACT, "--live", "--output-dir", str(tmp_path / "out")]
        if gate:
            # Authorized and open: the real run_cell is entered and stops at
            # the blocked harness import, before any process or container.
            with pytest.raises(ImportError, match="blocked"):
                RB.main(args)
        else:
            assert RB.main(args) == 2 and "not enabled" in capsys.readouterr().err
        assert not (tmp_path / "out").exists()

    @pytest.mark.parametrize("extra", [
        [],
        ["--authorize", "bm-smoke-r0005-c32-rep1"],
        ["--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0010-c32-rep1", "--expect-head", HEAD],
    ])
    def test_authorization_is_still_required(self, gate, tmp_path, capsys, extra):
        code = RB.main([*APPROVED, "--live", "--output-dir", str(tmp_path), *extra])
        err = capsys.readouterr().err
        assert code == 2 and "nothing was started" in err
        assert ("not enabled" in err) is (not gate)

    def test_other_cells_refused_by_run_cell_when_open(self, gate, tmp_path):
        cell = _approved_cells(rates=[10])[0]
        expected = RB.BenchmarkError if gate else RB.LiveDisabled
        with pytest.raises(expected):
            RB.run_cell(cell, LADDER, expect_head=HEAD, output_dir=tmp_path)

    def test_plan_mode_is_identical(self, gate, tmp_path):
        import io
        out = io.StringIO()
        assert RB.main([*APPROVED, "--output-dir", str(tmp_path / "o")], stdout=out) == 0
        assert out.getvalue().startswith("PLAN ONLY") and not (tmp_path / "o").exists()

    def test_fake_harness_cell_still_runs_offline(self, gate, tmp_path):
        result, path, clean, smoke = _run(tmp_path)
        assert clean and result["verdict"] == "VALID" and path.is_file()
        assert smoke.calls[-1] == "cleanup"