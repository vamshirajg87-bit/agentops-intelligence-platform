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
                  "duration_ms": "12.5", "candidates": "1300", "emitted": "22",
                  "inserted": "22", "duplicate": "0"},
                 {"signal_path": "tool_failure", "operation_name": "", "duration_ms": "3",
                  "candidates": "1300", "emitted": "0", "inserted": "0", "duplicate": "0"}], {})

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
        assert lag["stage_tail_seconds"]["kind"] == "OBSERVATION"
        assert lag["stage_tail_seconds"]["value"] == 3
        assert lag["drain_seconds"]["kind"] == "NOT_MEASURED"

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
APPROVED = ["--level", "SMOKE", "--rate", "5", "--repetition", "2", "--concurrency", "32",
            "--warmup-requests", "25"]


def _approved_cells(level="SMOKE", **overrides):
    kwargs = dict(rates=[5], repetition=2, concurrency=32, warmup_requests=25)
    if "repetitions" in overrides:
        kwargs.pop("repetition")
    kwargs.update(overrides)
    return RB.plan_cells(LADDER, level, **kwargs)


def _authorize(cells, authorize="bm-smoke-r0005-c32-rep2", expect_head=HEAD,
               warmup_requests=25, repetition=2):
    return RB.check_live_authorization(cells, authorize=authorize, expect_head=expect_head,
                                       warmup_requests=warmup_requests, repetition=repetition)


class TestLiveAuthorization:
    def test_exact_approved_cell(self):
        cell = _authorize(_approved_cells())
        assert (cell.benchmark_id, cell.requests, cell.rate, cell.concurrency,
                cell.repetition, cell.warmup_requests) == (
            "bm-smoke-r0005-c32-rep2", 100, 5, 32, 2, 25)
        cell = _authorize(_approved_cells(repetition=3), authorize="bm-smoke-r0005-c32-rep3",
                          repetition=3)
        assert (cell.benchmark_id, cell.repetition) == ("bm-smoke-r0005-c32-rep3", 3)

    @pytest.mark.parametrize("authorize", [None, "", "bm-smoke-r0010-c32-rep2",
                                           "bm-small-r0005-c32-rep2",
                                           "bm-smoke-r0005-c32-rep1",
                                           "bm-smoke-r0005-c32-rep6"])
    def test_missing_or_wrong_authorization(self, authorize):
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize(_approved_cells(), authorize=authorize)

    @pytest.mark.parametrize("head", [None, "", "812ff80", "Z" * 40, HEAD + "0"])
    def test_missing_or_malformed_head(self, head):
        with pytest.raises(RB.BenchmarkError, match="expect-head"):
            _authorize(_approved_cells(), expect_head=head)

    @pytest.mark.parametrize("warmup", [None, 24, 26, 150])
    def test_wrong_warmup_argument(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 25"):
            _authorize(_approved_cells(), warmup_requests=warmup)

    def test_planned_warmup_must_match_too(self):
        with pytest.raises(RB.BenchmarkError, match="warmup_requests"):
            _authorize(_approved_cells(warmup_requests=30))

    @pytest.mark.parametrize("overrides", [
        dict(rates=[10]), dict(concurrency=16), dict(rates=[10], level="SMALL"),
        dict(repetition=3),
    ])
    def test_wrong_cell_is_not_the_authorized_one(self, overrides):
        # rate, concurrency, level and repetition are all part of the id.
        with pytest.raises(RB.BenchmarkError, match="is not bm-smoke-r0005-c32-rep2"):
            _authorize(_approved_cells(**overrides))

    @pytest.mark.parametrize("repetition", [None, 1, 3])
    def test_repetition_must_be_selected_and_match(self, repetition):
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize(_approved_cells(), repetition=repetition)

    def test_rep1_is_refused_with_otherwise_correct_parameters(self):
        rep1 = _approved_cells(repetition=1)
        with pytest.raises(RB.BenchmarkError, match="not an approved live cell"):
            _authorize(rep1, authorize="bm-smoke-r0005-c32-rep1", repetition=1)
        assert not RB.is_approved_live_cell(rep1[0])

    @pytest.mark.parametrize("overrides", [dict(repetitions=2), dict(rates=[5, 10])])
    def test_multiple_planned_cells(self, overrides):
        with pytest.raises(RB.BenchmarkError, match="exactly one cell"):
            _authorize(_approved_cells(**overrides))

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

        def fake_run_cell(cell, ladder, **kw):
            calls.append((cell.benchmark_id, kw["expect_head"]))
            path = kw["output_dir"] / f"{cell.benchmark_id}-fake.json"
            path.write_text("{}", encoding="utf-8")
            return {"verdict": "VALID"}, path, True

        monkeypatch.setattr(RB, "run_cell", fake_run_cell)
        for name in ("run_smoke", "run_e2e"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        return calls

    def _main(self, tmp_path, *extra):
        return RB.main([*APPROVED, "--live", "--output-dir", str(tmp_path / "out"), *extra],
                       stdout=__import__("io").StringIO())

    def test_exact_cell_reaches_run_cell_once(self, gate_open, tmp_path):
        code = self._main(tmp_path, "--authorize", "bm-smoke-r0005-c32-rep2",
                          "--expect-head", HEAD)
        assert code == 0 and gate_open == [("bm-smoke-r0005-c32-rep2", HEAD)]

    @pytest.mark.parametrize("extra", [
        [],
        ["--authorize", "bm-smoke-r0005-c32-rep2"],
        ["--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0010-c32-rep1", "--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0005-c32-rep2", "--expect-head", HEAD, "--rate", "10"],
        ["--authorize", "bm-smoke-r0005-c32-rep3", "--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0005-c32-rep1", "--expect-head", HEAD],
        ["--authorize", "bm-smoke-r0005-c32-rep2", "--expect-head", HEAD,
         "--concurrency", "16"],
        ["--authorize", "bm-smoke-r0005-c32-rep2", "--expect-head", HEAD,
         "--warmup-requests", "26"],
    ])
    def test_everything_else_refused_before_anything_starts(self, gate_open, tmp_path, extra,
                                                           capsys):
        assert self._main(tmp_path, *extra) == 2
        assert gate_open == [] and "nothing was started" in capsys.readouterr().err
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules

    def test_wrong_level_refused(self, gate_open, tmp_path, capsys):
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "2",
                        "--concurrency", "32", "--warmup-requests", "25", "--live",
                        "--authorize", "bm-smoke-r0005-c32-rep2", "--expect-head", HEAD,
                        "--output-dir", str(tmp_path)])
        assert code == 2 and gate_open == []

    def test_cleanup_marker_still_blocks(self, gate_open, tmp_path, capsys):
        (tmp_path / "out").mkdir()
        (tmp_path / "out" / RB.CLEANUP_MARKER).write_text("x", encoding="utf-8")
        code = self._main(tmp_path, "--authorize", "bm-smoke-r0005-c32-rep2",
                          "--expect-head", HEAD)
        assert code == 2 and gate_open == [] and "cleanup" in capsys.readouterr().err

    def test_run_cell_refuses_other_cells_before_imports(self, monkeypatch, tmp_path):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "execute_cell", lambda *a, **k: pytest.fail("reached"))
        for name in ("run_smoke", "run_e2e"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        for cell in (_approved_cells(warmup_requests=150)[0], _approved_cells(rates=[10])[0],
                     _approved_cells(repetition=1)[0], _approved_cells(repetition=6)[0]):
            with pytest.raises(RB.BenchmarkError, match="not an approved live cell"):
                RB.run_cell(cell, LADDER, expect_head=HEAD, output_dir=tmp_path)
        assert "run_smoke" not in sys.modules and "run_e2e" not in sys.modules


class TestGateClosed:
    """The gate's refusal, tested with the gate forced closed in memory."""

    @pytest.fixture(autouse=True)
    def closed(self, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)

    def test_exact_cell_is_still_refused_by_the_gate(self, tmp_path, capsys):
        code = RB.main([*APPROVED, "--live", "--authorize", "bm-smoke-r0005-c32-rep2",
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
            RB.main([*APPROVED, "--authorize", "bm-smoke-r0005-c32-rep2"])

# ---------------------------------------------------------------------------
# Both gate states, in memory only: nothing reaches the real harness
# ---------------------------------------------------------------------------

EXACT = [*APPROVED, "--authorize", "bm-smoke-r0005-c32-rep2", "--expect-head", HEAD]


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
        out = tmp_path / "out"
        if gate:
            # Authorized and open: ownership and the ledger entry are taken,
            # then the real run_cell stops at the blocked harness import,
            # before any process or container.  The records stay.
            with pytest.raises(ImportError, match="blocked"):
                RB.main(args)
            assert (out / RB.OWNER_FILE).is_file()
            assert (out / "ledger" / "bm-smoke-r0005-c32-rep2.started").is_file()
            assert not (out / "ledger" / "bm-smoke-r0005-c32-rep2.done").exists()
            assert not list(out.glob("bm-*.json"))
        else:
            assert RB.main(args) == 2 and "not enabled" in capsys.readouterr().err
            assert not out.exists()

    @pytest.mark.parametrize("extra", [
        [],
        ["--authorize", "bm-smoke-r0005-c32-rep2"],
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

# ---------------------------------------------------------------------------
# Phase 14.4B3F measurement corrections
# ---------------------------------------------------------------------------

def _assembled(driver_overrides=None, db_overrides=None, drop=(), **evidence):
    smoke_driver = {"started": "2026-01-01T00:00:00+00:00",
                    "finished": "2026-01-01T00:00:20+00:00", "elapsed_seconds": 20.0,
                    "executed_attempts": 100, "started_requests": 100,
                    "not_started_requests": 0, "spans_ended": 700,
                    "spans_exported_successfully": 700, "export_failures": 0,
                    "flush_ok": True, "run_status": "completed", "problems": [],
                    "achieved_submission_rate": 5.0, "late_starts": 0,
                    "max_start_lag_seconds": 0.04, "mean_start_lag_seconds": 0.01}
    smoke_driver.update(driver_overrides or {})
    for name in drop:
        smoke_driver.pop(name)
    database = {"rows": 700, "distinct_spans": 700, "roots": 100, "first_ingested": 5000.0,
                "last_ingested": 5020.0, "request_ms": [250.0] * 100,
                "persistence_ms": [40.0] * 700, "per_second": [35] * 20}
    database.update(db_overrides or {})
    values = dict(cell=_cell(), git_sha=SHA, git_dirty=False, started=1000.0, finished=1100.0,
                  driver=smoke_driver, canonical_records=700, raw_records=140, dlq_records=0,
                  database=database, clock_before=[M.ClockSample(1000.0, 1000.001, 1000.002)],
                  clock_after=[M.ClockSample(1001.0, 1001.001, 1001.002)],
                  stage_tail_seconds=37.9)
    values.update(evidence)
    result = RB.assemble_result(RB.CellEvidence(**values), LADDER["stop_limits"])
    M.validate_result(result)
    return result


class TestStageTail:
    def test_stage_tail_is_an_observation_with_the_exact_method(self):
        tail = _assembled()["metrics"]["lag"]["stage_tail_seconds"]
        assert tail["kind"] == "OBSERVATION" and tail["value"] == 37.9
        assert tail["method"] == ("Driver finished -> run_stage returned; includes consumer "
                                  "drain, settle period and reconciliation.")

    def test_pure_drain_is_not_measured_and_never_estimated(self):
        drain = _assembled()["metrics"]["lag"]["drain_seconds"]
        assert drain["kind"] == "NOT_MEASURED" and drain["value"] is None
        assert "does not expose an isolated drain duration" in drain["reason"]

    def test_older_field_name_is_read_as_the_stage_tail(self):
        result = _assembled(stage_tail_seconds=None, drain_seconds=12.0)
        assert result["metrics"]["lag"]["stage_tail_seconds"]["value"] == 12.0
        assert result["metrics"]["lag"]["drain_seconds"]["kind"] == "NOT_MEASURED"

    def test_missing_tail_is_not_measured(self):
        tail = _assembled(stage_tail_seconds=None)["metrics"]["lag"]["stage_tail_seconds"]
        assert tail["kind"] == "NOT_MEASURED" and tail["value"] is None

    def test_live_cell_records_the_tail(self, tmp_path):
        result = _run(tmp_path)[0]
        assert result["metrics"]["lag"]["stage_tail_seconds"]["value"] == 3


class TestPersistedRate:
    def test_formula_is_n_minus_one_over_span(self):
        rate = _assembled()["metrics"]["throughput"]["spans_persisted_per_second"]
        assert rate["kind"] == "DERIVED" and rate["value"] == pytest.approx(699 / 20.0)
        assert rate["n"] == 700 and "(rows - 1)" in rate["method"]
        assert rate["window"]["clock"] == "database"

    def test_two_rows(self):
        result = _assembled(db_overrides={"rows": 2, "distinct_spans": 2,
                                          "last_ingested": 5000.5})
        value = result["metrics"]["throughput"]["spans_persisted_per_second"]
        assert value["value"] == pytest.approx(2.0)

    @pytest.mark.parametrize("overrides, reason", [
        ({"rows": 1, "distinct_spans": 1}, "at least 2"),
        ({"rows": 0, "distinct_spans": 0}, "at least 2"),
        ({"last_ingested": 5000.0}, "one instant"),
        ({"last_ingested": 4999.0}, "one instant"),
        ({"first_ingested": None}, "not read"),
    ])
    def test_edge_cases_are_not_measured(self, overrides, reason):
        rate = _assembled(db_overrides=overrides)["metrics"]["throughput"][
            "spans_persisted_per_second"]
        assert rate["kind"] == "NOT_MEASURED" and rate["value"] is None
        assert reason in rate["reason"]

    def test_generated_and_request_rates_unchanged(self):
        tp = _assembled()["metrics"]["throughput"]
        assert tp["spans_generated_per_second"]["kind"] == "DERIVED"
        assert tp["spans_generated_per_second"]["value"] == 35.0
        assert tp["requests_per_second"]["value"] == 5.0


class TestScheduler:
    def test_values_copied_from_the_driver(self):
        sched = _assembled(driver_overrides={"late_starts": 3, "started_requests": 98,
                                             "not_started_requests": 2,
                                             "max_start_lag_seconds": 0.4,
                                             "mean_start_lag_seconds": 0.05})["metrics"][
            "scheduler"]
        fields = ("late_starts", "started_requests", "not_started_requests",
                  "max_start_lag_seconds", "mean_start_lag_seconds")
        assert {k: sched[k]["value"] for k in fields} == {
            "late_starts": 3, "started_requests": 98, "not_started_requests": 2,
            "max_start_lag_seconds": 0.4, "mean_start_lag_seconds": 0.05}
        assert {sched[k]["kind"] for k in fields} == {"MEASURED"}

    def test_genuine_zero_stays_zero(self):
        late = _assembled()["metrics"]["scheduler"]["late_starts"]
        assert late["kind"] == "MEASURED" and late["value"] == 0

    @pytest.mark.parametrize("field", ["late_starts", "not_started_requests",
                                       "max_start_lag_seconds", "mean_start_lag_seconds"])
    def test_missing_field_is_not_measured_not_zero(self, field):
        result = _assembled(drop=(field,))
        metric = result["metrics"]["scheduler"][field]
        assert metric["kind"] == "NOT_MEASURED" and metric["value"] is None
        evidence = result["stop"]["generator_bound_evidence"]
        assert evidence[field] is None and field in evidence["missing"]

    def test_missing_late_starts_means_not_evaluated(self):
        result = _assembled(drop=("late_starts",))
        evidence = result["stop"]["generator_bound_evidence"]
        assert evidence["evaluated"] is False and result["stop"]["generator_bound"] is False
        assert any("not evaluated" in item for item in result["limitations"])

    def test_no_driver_report(self):
        result = _assembled(driver=None, canonical_records=None)
        assert all(m["kind"] == "NOT_MEASURED" for m in result["metrics"]["scheduler"].values())
        assert result["stop"]["generator_bound_evidence"]["evaluated"] is False

    def test_run_status_and_no_series_recorded(self):
        evidence = _assembled(driver_overrides={"run_status": "deadline_reached"})[
            "stop"]["generator_bound_evidence"]
        assert evidence["run_status"] == "deadline_reached"
        assert evidence["start_lag_series"] is None

    @pytest.mark.parametrize("overrides, bound, verdict", [
        ({}, False, "VALID"),
        ({"late_starts": 5}, False, "VALID"),
        ({"late_starts": 6}, True, "GENERATOR_BOUND"),
        ({"not_started_requests": 1}, True, "GENERATOR_BOUND"),
        # Also fails the driver-status integrity gate: INVALID takes precedence.
        ({"run_status": "deadline_reached"}, True, "INVALID"),
    ])
    def test_decision_reproducible_from_the_json(self, overrides, bound, verdict):
        result = json.loads(json.dumps(_assembled(driver_overrides=overrides)))
        evidence = result["stop"]["generator_bound_evidence"]
        again, reasons = RB.generator_bound_from_evidence(
            evidence, late_fraction=evidence["late_fraction"],
            lag_growth_samples=evidence["lag_growth_samples"])
        assert again == result["stop"]["generator_bound"] == evidence["generator_bound"] == bound
        assert list(reasons) == [r for r in result["verdict_reasons"]
                                 if r not in result["stop"]["reasons"]]
        assert result["verdict"] == verdict == M.verdict(
            integrity_ok=result["integrity"]["ok"], stop_reasons=result["stop"]["reasons"],
            generator_bound_run=again)


class TestHistoricalCompatibility:
    def _historical(self):
        """A result shaped as written before 14.4B3F: drain_seconds as an
        OBSERVATION, persisted rate MEASURED, no scheduler section and no
        generator-bound evidence."""
        result = json.loads(json.dumps(_assembled()))
        lag = result["metrics"]["lag"]
        lag["drain_seconds"] = {**lag.pop("stage_tail_seconds"),
                                "method": "end of driver run -> lag 0 and counts stable"}
        rate = result["metrics"]["throughput"]["spans_persisted_per_second"]
        rate.update(kind="MEASURED", value=700 / 20.0,
                    method="rows / (last - first ingested_at)")
        del result["metrics"]["scheduler"]
        del result["stop"]["generator_bound_evidence"]
        return result

    def test_older_results_still_validate(self):
        M.validate_result(self._historical())

    def test_validation_does_not_rewrite_older_results(self):
        older = self._historical()
        snapshot = json.dumps(older, sort_keys=True)
        M.validate_result(older)
        assert json.dumps(older, sort_keys=True) == snapshot


class TestCorrectionsKeepSafety:
    def test_authorization_and_isolation_unchanged(self, tmp_path, monkeypatch, capsys):
        assert sorted(RB.APPROVED_LIVE_CELLS) == ["bm-small-r0010-c32-rep1",
                                                 "bm-small-r0010-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep3",
                                                 "bm-smoke-r0005-c32-rep4",
                                                 "bm-smoke-r0005-c32-rep5"]
        assert [c["repetition"] for c in RB.APPROVED_LIVE_CELLS.values()] == [2, 3, 4, 5, 1, 2]
        with pytest.raises(LiveAccessAttempt):
            subprocess.run(["docker", "ps"])
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        assert RB.main([*EXACT, "--live", "--output-dir", str(tmp_path)]) == 2
        assert "not enabled" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Phase 14.4B3H2 repetition selection, run ledger and ownership
# ---------------------------------------------------------------------------

HISTORICAL_RESULT = Path(r"C:\Users\Vamshi Raj Goud\AppData\Local\Temp\agentops-benchmarks"
                         r"\bm-smoke-r0005-c32-rep1-20261008T174815.json")
HISTORICAL_SHA256_PREFIX = "e2f98781f555d31c"


def _rep_args(n, *extra):
    return ["--level", "SMOKE", "--rate", "5", "--repetition", str(n), "--concurrency", "32",
            "--warmup-requests", "25", "--live", "--authorize", f"bm-smoke-r0005-c32-rep{n}",
            "--expect-head", HEAD, *extra]


class TestRepetitionSelector:
    @pytest.mark.parametrize("n", [2, 3])
    def test_one_repetition_planned_alone(self, n):
        cells = RB.plan_cells(LADDER, "SMOKE", rates=[5], repetition=n, concurrency=32,
                              warmup_requests=25)
        assert [(c.benchmark_id, c.warmup_run_id, c.measured_run_id) for c in cells] == [
            (f"bm-smoke-r0005-c32-rep{n}", f"b-smoke-r0005-c32-w{n}",
             f"b-smoke-r0005-c32-m{n}")]

    @pytest.mark.parametrize("bad", [0, 11, -1, True, 2.0])
    def test_bounds(self, bad):
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            RB.plan_cells(LADDER, "SMOKE", repetition=bad)

    def test_conflicts_with_repetitions(self):
        with pytest.raises(RB.BenchmarkError, match="exclude each other"):
            RB.plan_cells(LADDER, "SMOKE", repetition=2, repetitions=3)
        with pytest.raises(SystemExit):
            RB.main(["--level", "SMOKE", "--repetition", "2", "--repetitions", "3"])

    def test_plan_mode_without_selector_unchanged(self):
        import io
        out = io.StringIO()
        assert RB.main(["--level", "SMOKE", "--rate", "5", "--output-dir",
                        str(Path(__import__("tempfile").gettempdir()) / "unused-bm")],
                       stdout=out) == 0
        assert out.getvalue().count("bm-smoke-r0005-c32-rep") == 3

    def test_plan_mode_with_selector(self, tmp_path):
        import io
        out = io.StringIO()
        assert RB.main(["--level", "SMOKE", "--rate", "5", "--repetition", "3",
                        "--output-dir", str(tmp_path / "o")], stdout=out) == 0
        assert out.getvalue().count("bm-") == 1 and "rep3" in out.getvalue()
        assert not (tmp_path / "o").exists()


def _claim(tmp_path, n=2, **kw):
    cell = _approved_cells(repetition=n)[0]
    return cell, RB.claim_run(tmp_path, cell, head=HEAD, arguments={"repetition": n},
                              now=lambda: 1_800_000_000.0, pid=lambda: 4242, **kw)


class TestLedger:
    def test_claim_records_owner_and_started(self, tmp_path):
        cell, started = _claim(tmp_path)
        owner = json.loads((tmp_path / RB.OWNER_FILE).read_text(encoding="utf-8"))
        record = json.loads(started.read_text(encoding="utf-8"))
        assert started == tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.started"
        assert owner == record
        assert record["benchmark_id"] == cell.benchmark_id and record["git_head"] == HEAD
        assert record["pid"] == 4242 and record["started_utc"].startswith("2027-01-15T")
        assert record["arguments"] == {"repetition": 2}

    def test_duplicate_started_refused(self, tmp_path):
        cell, started = _claim(tmp_path)
        (tmp_path / RB.OWNER_FILE).unlink()      # as after a released run
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            _claim(tmp_path)
        assert started.is_file()                 # never deleted
        assert not (tmp_path / RB.OWNER_FILE).exists()  # the refused claim released its own

    def test_existing_result_refused(self, tmp_path):
        (tmp_path / "bm-smoke-r0005-c32-rep2-20261009T000000.json").write_text("{}")
        with pytest.raises(RB.BenchmarkError, match="already exists"):
            _claim(tmp_path)
        assert not (tmp_path / "ledger").exists() and not (tmp_path / RB.OWNER_FILE).exists()

    def test_creation_failure_blocks(self, tmp_path, monkeypatch):
        real = RB._exclusive_json

        def failing(path, payload):
            if path.name.endswith(".started"):
                raise PermissionError("denied")
            return real(path, payload)

        monkeypatch.setattr(RB, "_exclusive_json", failing)
        with pytest.raises(RB.BenchmarkError, match="cannot create"):
            _claim(tmp_path)
        assert not (tmp_path / RB.OWNER_FILE).exists()

    def test_owner_creation_failure_blocks(self, tmp_path, monkeypatch):
        monkeypatch.setattr(RB, "_exclusive_json",
                            lambda path, payload: (_ for _ in ()).throw(OSError("disk")))
        with pytest.raises(RB.BenchmarkError, match="cannot create"):
            _claim(tmp_path)

    def test_simultaneous_ownership_conflict(self, tmp_path):
        _claim(tmp_path, n=2)
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            _claim(tmp_path, n=3)
        assert not (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep3.started").exists()

    def test_ledger_never_inside_repository(self):
        with pytest.raises(RB.BenchmarkError, match="never written inside the repository"):
            _claim(_REPO / "benchmarks")
        assert not (_REPO / "benchmarks" / RB.OWNER_FILE).exists()


class TestFinish:
    def _result_file(self, tmp_path, verdict="VALID"):
        path = tmp_path / "bm-smoke-r0005-c32-rep2-x.json"
        path.write_text(json.dumps({"verdict": verdict}), encoding="utf-8")
        return path

    def test_done_records_hash_and_intended_status(self, tmp_path):
        cell, _ = _claim(tmp_path)
        path = self._result_file(tmp_path)
        released = RB.finish_run(tmp_path, cell, result={"verdict": "VALID"}, result_path=path,
                                 cleanup_verified=True, intended_status=0)
        done = json.loads((tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.done").read_text())
        import hashlib
        assert done["result_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert done["verdict"] == "VALID" and done["cleanup_verified"] is True
        assert done["intended_return_status"] == 0
        assert "not here" in done["intended_return_status_note"]
        assert not any("exit_code" in key for key in done)   # no OS exit code invented
        assert released and not (tmp_path / RB.OWNER_FILE).exists()
        assert (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.started").is_file()

    def test_cleanup_uncertainty_keeps_ownership(self, tmp_path):
        cell, _ = _claim(tmp_path)
        path = self._result_file(tmp_path, "STOPPED")
        released = RB.finish_run(tmp_path, cell, result={"verdict": "STOPPED"},
                                 result_path=path, cleanup_verified=False, intended_status=1)
        assert not released and (tmp_path / RB.OWNER_FILE).is_file()
        done = json.loads((tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.done").read_text())
        assert done["cleanup_verified"] is False and done["intended_return_status"] == 1

    def test_recording_failure_keeps_ownership(self, tmp_path):
        cell, _ = _claim(tmp_path)
        with pytest.raises(OSError):
            RB.finish_run(tmp_path, cell, result={"verdict": "VALID"},
                          result_path=tmp_path / "missing.json", cleanup_verified=True,
                          intended_status=0)
        assert (tmp_path / RB.OWNER_FILE).is_file()
        assert not (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.done").exists()

    def test_done_cannot_be_written_twice(self, tmp_path):
        cell, _ = _claim(tmp_path)
        path = self._result_file(tmp_path)
        RB.finish_run(tmp_path, cell, result={"verdict": "VALID"}, result_path=path,
                      cleanup_verified=False, intended_status=1)
        with pytest.raises(FileExistsError):
            RB.finish_run(tmp_path, cell, result={"verdict": "VALID"}, result_path=path,
                          cleanup_verified=True, intended_status=0)


class TestLiveFlowWithLedger:
    """main() with the gate open in memory and run_cell replaced."""

    @pytest.fixture
    def open_gate(self, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        state = {"calls": [], "clean": True, "verdict": "VALID", "raise": None}

        def fake_run_cell(cell, ladder, **kw):
            state["calls"].append(cell.benchmark_id)
            if state["raise"]:
                raise state["raise"]
            path = kw["output_dir"] / f"{cell.benchmark_id}-fake.json"
            path.write_text(json.dumps({"verdict": state["verdict"]}), encoding="utf-8")
            return {"verdict": state["verdict"]}, path, state["clean"]

        monkeypatch.setattr(RB, "run_cell", fake_run_cell)
        return state

    def _main(self, tmp_path, n):
        import io
        out = io.StringIO()
        return RB.main(_rep_args(n, "--output-dir", str(tmp_path)), stdout=out), out.getvalue()

    @pytest.mark.parametrize("n", [2, 3])
    def test_each_approved_repetition_runs_once(self, open_gate, tmp_path, n):
        code, text = self._main(tmp_path, n)
        assert code == 0 and open_gate["calls"] == [f"bm-smoke-r0005-c32-rep{n}"]
        assert (tmp_path / "ledger" / f"bm-smoke-r0005-c32-rep{n}.done").is_file()
        assert not (tmp_path / RB.OWNER_FILE).exists()
        code, _ = self._main(tmp_path, n)        # the same repetition again
        assert code == 2 and open_gate["calls"] == [f"bm-smoke-r0005-c32-rep{n}"]

    def test_rep2_then_rep3_sequentially(self, open_gate, tmp_path):
        assert self._main(tmp_path, 2)[0] == 0 and self._main(tmp_path, 3)[0] == 0
        assert open_gate["calls"] == ["bm-smoke-r0005-c32-rep2", "bm-smoke-r0005-c32-rep3"]

    @pytest.mark.parametrize("n", [1, 6])
    def test_rep1_and_rep6_refused(self, open_gate, tmp_path, n, capsys):
        import io
        assert RB.main(_rep_args(n, "--output-dir", str(tmp_path)), stdout=io.StringIO()) == 2
        assert open_gate["calls"] == [] and "nothing was started" in capsys.readouterr().err
        assert not (tmp_path / RB.OWNER_FILE).exists()

    def test_crash_leaves_blocking_evidence(self, open_gate, tmp_path):
        open_gate["raise"] = RuntimeError("process died")
        with pytest.raises(RuntimeError):
            self._main(tmp_path, 2)
        assert (tmp_path / RB.OWNER_FILE).is_file()
        assert (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.started").is_file()
        assert not (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.done").exists()
        open_gate["raise"] = None
        assert self._main(tmp_path, 3)[0] == 2           # ownership still held
        assert open_gate["calls"] == ["bm-smoke-r0005-c32-rep2"]

    def test_unverified_cleanup_keeps_ownership_and_blocks(self, open_gate, tmp_path):
        open_gate.update(clean=False, verdict="STOPPED")
        code, text = self._main(tmp_path, 2)
        assert code == 1 and "ownership is kept" in text
        assert (tmp_path / RB.OWNER_FILE).is_file()
        open_gate.update(clean=True, verdict="VALID")
        assert self._main(tmp_path, 3)[0] == 2

    def test_not_valid_but_clean_releases(self, open_gate, tmp_path):
        open_gate.update(verdict="INVALID")
        code, _ = self._main(tmp_path, 2)
        done = json.loads((tmp_path / "ledger" / "bm-smoke-r0005-c32-rep2.done").read_text())
        assert code == 1 and done["intended_return_status"] == 1
        assert not (tmp_path / RB.OWNER_FILE).exists()

    def test_existing_marker_blocks_before_claim(self, open_gate, tmp_path):
        (tmp_path / RB.CLEANUP_MARKER).write_text("x", encoding="utf-8")
        assert self._main(tmp_path, 2)[0] == 2
        assert not (tmp_path / RB.OWNER_FILE).exists() and open_gate["calls"] == []

    def test_claim_happens_before_harness_import(self, monkeypatch, tmp_path):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        with pytest.raises(ImportError, match="blocked"):
            RB.main(_rep_args(2, "--output-dir", str(tmp_path)))
        assert (tmp_path / RB.OWNER_FILE).is_file()
        assert "run_smoke" not in sys.modules


class TestHistoricalRep1:
    def test_historical_result_unchanged_if_present(self):
        if not HISTORICAL_RESULT.exists():
            pytest.skip("historical result not on this machine")
        import hashlib
        digest = hashlib.sha256(HISTORICAL_RESULT.read_bytes()).hexdigest()
        assert digest.startswith(HISTORICAL_SHA256_PREFIX)


# ---------------------------------------------------------------------------
# Phase 14.4B3I-5 start-lag diagnostic (observational only)
# ---------------------------------------------------------------------------

def _diag(entries=None, **overrides):
    entries = [[i, round(0.2 * i, 6), lag] for i, lag in enumerate(
        [0.001, 0.12, 0.0, 0.25, 0.05, 0.101])] if entries is None else entries
    value = {"limit": 10000, "expected": len(entries), "recorded": len(entries),
             "complete": True, "truncated_reason": None, "entries": entries}
    value.update(overrides)
    return value


def _with_diag(diag, **driver_overrides):
    return _assembled(driver_overrides={"start_lags": diag, **driver_overrides})


class TestStartLagDiagnosticInResults:
    def test_complete_diagnostic_gives_measured_percentiles(self):
        result = _with_diag(_diag())
        sched = result["metrics"]["scheduler"]
        lags = [0.001, 0.12, 0.0, 0.25, 0.05, 0.101]
        for q in (50, 95, 99):
            assert sched[f"start_lag_p{q}"]["kind"] == "MEASURED"
            assert sched[f"start_lag_p{q}"]["value"] == pytest.approx(M.percentile(lags, q))
            assert sched[f"start_lag_p{q}"]["n"] == 6

    def test_late_indices_use_the_driver_threshold_strictly(self):
        evidence = _with_diag(_diag())["stop"]["generator_bound_evidence"]
        diag = evidence["start_lag_diagnostic"]
        assert diag["late_request_indices"] == [1, 3, 5]      # 0.12, 0.25, 0.101 > 0.1
        assert diag["late_threshold_seconds"] == 0.1 and diag["usable"] is True

    def test_threshold_matches_the_driver(self):
        sys.path.insert(0, str(_REPO / "workload-generator"))
        try:
            from workload_generator import real_driver
        finally:
            sys.path.remove(str(_REPO / "workload-generator"))
        assert RB.DRIVER_LATE_START_SECONDS == real_driver.LATE_START_SECONDS

    @pytest.mark.parametrize("diag, reason", [
        (_diag(complete=False, truncated_reason="limit"), "limit"),
        (_diag(complete=False, truncated_reason="missing or invalid start lag"),
         "missing or invalid start lag"),
        (_diag(recorded=5), "recorded does not equal the number of entries"),
        (_diag(expected=7), "recorded does not equal expected"),
        (_diag(entries=[[0, 0.0, 0.1], [0, 0.2, 0.1]]), "not strictly increasing"),
        (_diag(entries=[[0, 0.0, -0.1]]), "non-finite or negative"),
        (_diag(entries=[[0, 0.0, float("nan")]]), "non-finite or negative"),
        (_diag(entries=[[0, 0.0]]), "malformed entry"),
        ({"entries": []}, "malformed diagnostic object"),
    ])
    def test_unusable_diagnostic_suppresses_percentiles(self, diag, reason):
        result = _with_diag(diag)
        sched = result["metrics"]["scheduler"]
        for q in (50, 95, 99):
            assert sched[f"start_lag_p{q}"]["kind"] == "NOT_MEASURED"
            assert sched[f"start_lag_p{q}"]["value"] is None
            assert reason in sched[f"start_lag_p{q}"]["reason"]
        stored = result["stop"]["generator_bound_evidence"]["start_lag_diagnostic"]
        assert stored["usable"] is False and stored["late_request_indices"] is None

    def test_absent_diagnostic_is_not_measured(self):
        result = _assembled()                                  # no start_lags key
        sched = result["metrics"]["scheduler"]
        assert sched["start_lag_p95"]["kind"] == "NOT_MEASURED"
        assert "no start-lag diagnostic" in sched["start_lag_p95"]["reason"]
        assert result["stop"]["generator_bound_evidence"]["start_lag_diagnostic"] is None

    def test_series_key_stays_null(self):
        for diag in (_diag(), _diag(complete=False, truncated_reason="limit"), None):
            evidence = _with_diag(diag)["stop"]["generator_bound_evidence"]
            assert evidence["start_lag_series"] is None

    def test_diagnostic_is_never_passed_to_a_decision(self, monkeypatch):
        bound_calls, growing_calls = [], []
        real_bound, real_growing = M.generator_bound, M.lag_growing

        def spy_bound(**kwargs):
            bound_calls.append(kwargs)
            return real_bound(**kwargs)

        def spy_growing(series, **kwargs):
            growing_calls.append(list(series))
            return real_growing(series, **kwargs)

        monkeypatch.setattr(M, "generator_bound", spy_bound)
        monkeypatch.setattr(M, "lag_growing", spy_growing)
        _with_diag(_diag())
        assert len(bound_calls) == 1
        assert "start_lag_series" not in bound_calls[0]       # the default () is used
        # The only lag_growing inputs are the Kafka lag samples (none here).
        assert growing_calls == [[], []]

    @pytest.mark.parametrize("late", [0, 5, 6, 10])
    def test_verdict_invariant_across_diagnostic_states(self, late):
        verdicts = set()
        for diag in (_diag(), _diag(complete=False, truncated_reason="limit"),
                     _diag(entries=[[0, 0.0, 0.5]] * 2), None):
            result = _with_diag(diag, late_starts=late)
            verdicts.add((result["verdict"], result["stop"]["generator_bound"],
                          tuple(result["verdict_reasons"])))
        assert len(verdicts) == 1
        expected = "GENERATOR_BOUND" if late > 5 else "VALID"
        assert next(iter(verdicts))[0] == expected

    def test_reproduction_from_json_ignores_the_diagnostic(self):
        result = json.loads(json.dumps(_with_diag(_diag(), late_starts=6)))
        evidence = result["stop"]["generator_bound_evidence"]
        bound, _ = RB.generator_bound_from_evidence(
            evidence, late_fraction=evidence["late_fraction"],
            lag_growth_samples=evidence["lag_growth_samples"])
        assert bound is True and result["verdict"] == "GENERATOR_BOUND"

    def test_old_result_shape_without_diagnostic_validates(self):
        older = json.loads(json.dumps(_assembled()))
        evidence = older["stop"]["generator_bound_evidence"]
        del evidence["start_lag_diagnostic"]
        for q in (50, 95, 99):
            del older["metrics"]["scheduler"][f"start_lag_p{q}"]
        M.validate_result(older)

    def test_historical_results_unchanged_if_present(self):
        import hashlib
        expected = {"bm-smoke-r0005-c32-rep1-20261008T174815.json": "e2f98781f555d31c",
                    "bm-smoke-r0005-c32-rep2-20261008T190552.json": "088c501ca0db9acb"}
        base = HISTORICAL_RESULT.parent
        for name, prefix in expected.items():
            path = base / name
            if path.exists():
                assert hashlib.sha256(path.read_bytes()).hexdigest().startswith(prefix)


# ---------------------------------------------------------------------------
# B3J-48d: SMOKE repetitions 4 and 5 are approved cells (nothing is run)
# ---------------------------------------------------------------------------

SMOKE_REPEAT_FIELDS = {"level": "SMOKE", "requests": 100, "rate": 5, "concurrency": 32,
                       "warmup_requests": 25}


class TestSmokeRepetitionsFourAndFive:
    @pytest.mark.parametrize("n", [4, 5])
    def test_recognised_as_approved_with_the_smoke_configuration(self, n):
        cell = _approved_cells(repetition=n)[0]
        bid = f"bm-smoke-r0005-c32-rep{n}"
        assert cell.benchmark_id == bid and RB.is_approved_live_cell(cell)
        assert dict(RB.APPROVED_LIVE_CELLS[bid]) == {**SMOKE_REPEAT_FIELDS, "repetition": n}
        assert (cell.level, cell.requests, cell.rate, cell.concurrency, cell.repetition,
                cell.warmup_requests) == ("SMOKE", 100, 5, 32, n, 25)
        checked = _authorize(_approved_cells(repetition=n), authorize=bid, repetition=n)
        assert checked == cell

    def test_exactly_the_four_smoke_cells_are_approved(self):
        smoke = {k: v for k, v in RB.APPROVED_LIVE_CELLS.items() if k.startswith("bm-smoke-")}
        assert sorted(smoke) == [f"bm-smoke-r0005-c32-rep{n}" for n in (2, 3, 4, 5)]
        assert [c["repetition"] for _, c in sorted(smoke.items())] == [2, 3, 4, 5]
        for approved in smoke.values():
            assert {k: v for k, v in approved.items() if k != "repetition"} == SMOKE_REPEAT_FIELDS
        assert len(RB.APPROVED_LIVE_CELLS) == 6      # the four SMOKE cells plus SMALL-10 rep1, rep2

    def test_planned_ids_and_run_ids_are_distinct_across_all_repetitions(self):
        cells = [_approved_cells(repetition=n)[0] for n in (2, 3, 4, 5)]
        for attribute in ("benchmark_id", "warmup_run_id", "measured_run_id"):
            values = [getattr(c, attribute) for c in cells]
            assert len(set(values)) == 4, attribute
        assert [(c.benchmark_id, c.warmup_run_id, c.measured_run_id) for c in cells[2:]] == [
            ("bm-smoke-r0005-c32-rep4", "b-smoke-r0005-c32-w4", "b-smoke-r0005-c32-m4"),
            ("bm-smoke-r0005-c32-rep5", "b-smoke-r0005-c32-w5", "b-smoke-r0005-c32-m5")]
        for cell in cells:
            assert RB.RUN_ID_RE.fullmatch(cell.warmup_run_id)
            assert RB.RUN_ID_RE.fullmatch(cell.measured_run_id)

    @pytest.mark.parametrize("n", [4, 5])
    @pytest.mark.parametrize("overrides", [dict(rates=[10]), dict(concurrency=16),
                                           dict(warmup_requests=30)])
    def test_incorrect_parameters_are_refused(self, n, overrides):
        with pytest.raises(RB.BenchmarkError):
            _authorize(_approved_cells(repetition=n, **overrides),
                       authorize=f"bm-smoke-r0005-c32-rep{n}", repetition=n)

    @pytest.mark.parametrize("n", [4, 5])
    def test_wrong_selection_or_warmup_is_refused(self, n):
        bid = f"bm-smoke-r0005-c32-rep{n}"
        for repetition in (None, 2, 3, 6, n + 1 if n == 4 else 4):
            with pytest.raises(RB.BenchmarkError, match="repetition"):
                _authorize(_approved_cells(repetition=n), authorize=bid, repetition=repetition)
        for warmup in (None, 24, 26):
            with pytest.raises(RB.BenchmarkError, match="warmup-requests 25"):
                _authorize(_approved_cells(repetition=n), authorize=bid, repetition=n,
                           warmup_requests=warmup)
        with pytest.raises(RB.BenchmarkError, match="is not bm-smoke-r0005-c32-rep%d" % n):
            _authorize(_approved_cells(repetition=9 - n), authorize=bid, repetition=n)

    @pytest.mark.parametrize("authorize", ["bm-smoke-r0005-c32-rep6", "bm-smoke-r0005-c32-rep10",
                                           "bm-smoke-r0005-c32-rep1",
                                           "bm-small-r0010-c32-rep3", "bm-small-r0005-c32-rep4",
                                           "bm-smoke-r0010-c32-rep4"])
    def test_rep6_rep1_and_small_cells_stay_refused(self, authorize):
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize(_approved_cells(repetition=4), authorize=authorize, repetition=4)
        for cell in (_approved_cells(repetition=6)[0], _approved_cells(repetition=1)[0],
                     RB.plan_cells(LADDER, "SMALL", rates=[10], repetition=1,
                                   concurrency=32, warmup_requests=25)[0]):
            assert not RB.is_approved_live_cell(cell), cell.benchmark_id

    def test_cell_fields_must_match_the_approved_ones_exactly(self):
        base = _approved_cells(repetition=4)[0]
        for change in (dict(requests=101), dict(rate=10), dict(concurrency=31),
                       dict(warmup_requests=26), dict(repetition=6), dict(level="SMALL")):
            assert not RB.is_approved_live_cell(dataclasses.replace(base, **change)), change

    @pytest.mark.parametrize("n", [4, 5])
    def test_main_with_the_gate_closed_refuses_before_anything(self, n, monkeypatch, tmp_path,
                                                               capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMOKE", "--rate", "5", "--repetition", str(n),
                        "--concurrency", "32", "--warmup-requests", "25", "--live",
                        "--authorize", f"bm-smoke-r0005-c32-rep{n}", "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "not enabled" in capsys.readouterr().err
        assert not out.exists()

    @pytest.mark.parametrize("n", [4, 5])
    def test_main_in_plan_mode_touches_nothing(self, n, tmp_path):
        import io
        out = io.StringIO()
        target = tmp_path / "plan-only"
        assert RB.main(["--level", "SMOKE", "--rate", "5", "--repetition", str(n),
                        "--concurrency", "32", "--warmup-requests", "25",
                        "--output-dir", str(target)], stdout=out) == 0
        assert f"bm-smoke-r0005-c32-rep{n}" in out.getvalue() and "PLAN ONLY" in out.getvalue()
        assert not target.exists()

    @pytest.mark.parametrize("n", [4, 5])
    def test_a_new_claim_leaves_existing_rep2_and_rep3_records_unchanged(self, n, tmp_path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        existing = {}
        for old in (2, 3):
            for suffix, body in ((".started", "started-%d\n"), (".done", "done-%d\n")):
                path = ledger / f"bm-smoke-r0005-c32-rep{old}{suffix}"
                path.write_text(body % old, encoding="utf-8", newline="")
                existing[path] = path.read_bytes()
        result = tmp_path / "bm-smoke-r0005-c32-rep2-20261008T190552.json"
        result.write_text("{}", encoding="utf-8")
        existing[result] = result.read_bytes()
        cell, started = _claim(tmp_path, n=n)
        assert started == ledger / f"bm-smoke-r0005-c32-rep{n}.started" and started.is_file()
        for path, content in existing.items():
            assert path.read_bytes() == content, path.name
        assert sorted(p.name for p in ledger.iterdir()) == sorted(
            [f"bm-smoke-r0005-c32-rep{k}{s}" for k in (2, 3) for s in (".started", ".done")]
            + [f"bm-smoke-r0005-c32-rep{n}.started"])

    @pytest.mark.parametrize("n", [2, 3])
    def test_consumed_cells_stay_blocked_by_their_ledger_records(self, n, tmp_path):
        cell, started = _claim(tmp_path, n=n)
        (tmp_path / RB.OWNER_FILE).unlink()          # as after a released run
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            _claim(tmp_path, n=n)
        assert started.is_file()

    def test_rep4_and_rep5_cannot_be_claimed_together(self, tmp_path):
        _claim(tmp_path, n=4)
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            _claim(tmp_path, n=5)
        assert not (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep5.started").exists()

    def test_the_gates_and_measurement_inputs_are_untouched(self):
        assert RB.LIVE_ENABLED is True            # unchanged by this step
        assert RB.MAX_WARMUP_REQUESTS == 10000
        assert dict(RB._SMOKE_REPEAT) == {**SMOKE_REPEAT_FIELDS}


# ---------------------------------------------------------------------------
# B3J-54: the first exploratory SMALL cell, SMALL-10 (nothing is run)
# ---------------------------------------------------------------------------

SMALL10_ID = "bm-small-r0010-c32-rep1"
SMALL10_FIELDS = {"level": "SMALL", "requests": 1000, "rate": 10, "concurrency": 32,
                  "warmup_requests": 300, "repetition": 1}


def _small(level="SMALL", rate=10, repetition=1, concurrency=32, warmup=300):
    return RB.plan_cells(LADDER, level, rates=[rate], repetition=repetition,
                         concurrency=concurrency, warmup_requests=warmup)[0]


def _authorize_small(cell, *, authorize=SMALL10_ID, expect_head=HEAD, warmup=300, repetition=1):
    return RB.check_live_authorization([cell], authorize=authorize, expect_head=expect_head,
                                       warmup_requests=warmup, repetition=repetition)


class TestSmallTenFirstCell:
    def test_exact_fields_ids_and_authorization(self):
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_ID]) == SMALL10_FIELDS
        cell = _small()
        assert cell.benchmark_id == SMALL10_ID
        assert (cell.warmup_run_id, cell.measured_run_id) == ("b-small-r0010-c32-w1",
                                                              "b-small-r0010-c32-m1")
        assert (cell.level, cell.requests, cell.rate, cell.concurrency, cell.repetition,
                cell.warmup_requests) == ("SMALL", 1000, 10, 32, 1, 300)
        assert cell.planned_spans == 7000 and cell.measured_seconds == 100.0
        assert cell.live_allowed is True and cell.refresh_and_detector is True
        assert RB.RUN_ID_RE.fullmatch(cell.warmup_run_id)
        assert RB.RUN_ID_RE.fullmatch(cell.measured_run_id)
        assert RB.is_approved_live_cell(cell)
        assert _authorize_small(cell) == cell

    def test_the_default_warmup_of_the_ladder_is_the_approved_one(self):
        default = RB.plan_cells(LADDER, "SMALL", rates=[10], repetition=1, concurrency=32)[0]
        assert default.warmup_requests == 300 and RB.is_approved_live_cell(default)

    def test_exactly_six_cells_and_the_smoke_cells_unchanged(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == [
            SMALL10_ID, "bm-small-r0010-c32-rep2", "bm-smoke-r0005-c32-rep2",
            "bm-smoke-r0005-c32-rep3", "bm-smoke-r0005-c32-rep4", "bm-smoke-r0005-c32-rep5"]
        assert len(RB.APPROVED_LIVE_CELLS) == 6
        assert [k for k in RB.APPROVED_LIVE_CELLS if "small" in k] == [
            SMALL10_ID, "bm-small-r0010-c32-rep2"]
        for n in (2, 3, 4, 5):
            assert dict(RB.APPROVED_LIVE_CELLS[f"bm-smoke-r0005-c32-rep{n}"]) == {
                **SMOKE_REPEAT_FIELDS, "repetition": n}

    @pytest.mark.parametrize("warmup", [25, 150, 299, 301, 1000])
    def test_wrong_warmup_is_refused(self, warmup):
        cell = _small(warmup=warmup)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 300"):
            _authorize_small(cell, warmup=warmup)

    @pytest.mark.parametrize("warmup", [None, 0, 299, 25])
    def test_wrong_warmup_argument_is_refused(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 300"):
            _authorize_small(_small(), warmup=warmup)

    @pytest.mark.parametrize("change", [dict(requests=999), dict(requests=1001),
                                        dict(requests=100), dict(rate=5), dict(rate=25),
                                        dict(concurrency=16), dict(concurrency=31),
                                        dict(concurrency=33), dict(repetition=3),
                                        dict(repetition=4), dict(level="SMOKE"),
                                        dict(level="MEDIUM")])
    def test_any_changed_field_is_refused(self, change):
        assert not RB.is_approved_live_cell(dataclasses.replace(_small(), **change)), change

    @pytest.mark.parametrize("rate", [25, 50, 100])
    def test_other_small_rates_are_refused(self, rate):
        cell = _small(rate=rate)
        assert not RB.is_approved_live_cell(cell)
        bid = f"bm-small-r{rate:04d}-c32-rep1"
        assert cell.benchmark_id == bid
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=bid)
        with pytest.raises(RB.BenchmarkError, match="is not bm-small-r0010-c32-rep1"):
            _authorize_small(cell)

    @pytest.mark.parametrize("repetition", [3, 4, 5, 6])
    def test_other_small_repetitions_are_refused(self, repetition):
        cell = _small(repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r0010-c32-rep{repetition}",
                             repetition=repetition)
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(cell, repetition=repetition)

    @pytest.mark.parametrize("level", ["MEDIUM", "LARGE", "STRESS"])
    def test_every_other_level_is_refused(self, level):
        spec = [lv for lv in LADDER["levels"] if lv["name"] == level][0]
        cell = _small(level=level, rate=spec["rates"][0])
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=cell.benchmark_id)

    @pytest.mark.parametrize("authorize", [None, "", " ", SMALL10_ID + " ", SMALL10_ID.upper(),
                                           "bm-small-r0010-c32-rep3", "bm-small-r0010-c32-rep0",
                                           "bm-small-r0025-c32-rep1", "bm-small-r0010-c16-rep1",
                                           "bm-small-r0010-c32", "bm-smoke-r0010-c32-rep1",
                                           "bm-medium-r0050-c32-rep1"])
    def test_wrong_authorization_selectors_are_refused(self, authorize):
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(_small(), authorize=authorize)

    @pytest.mark.parametrize("repetition", [None, 0, 2, 5])
    def test_the_repetition_selector_must_be_one(self, repetition):
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(_small(), repetition=repetition)

    @pytest.mark.parametrize("head", [None, "", "812ff80", "Z" * 40, HEAD + "0", "B" * 40])
    def test_the_expected_head_must_be_a_full_lowercase_commit(self, head):
        with pytest.raises(RB.BenchmarkError, match="expect-head"):
            _authorize_small(_small(), expect_head=head)

    def test_smoke_authorizations_cannot_run_the_small_cell(self):
        for n in (2, 3, 4, 5):
            with pytest.raises(RB.BenchmarkError):
                _authorize_small(_small(), authorize=f"bm-smoke-r0005-c32-rep{n}",
                                 repetition=n)
            with pytest.raises(RB.BenchmarkError, match="is not bm-smoke"):
                RB.check_live_authorization(
                    [_small()], authorize=f"bm-smoke-r0005-c32-rep{n}", expect_head=HEAD,
                    warmup_requests=25, repetition=n)

    def test_more_than_one_planned_cell_is_refused(self):
        cells = RB.plan_cells(LADDER, "SMALL", rates=[10, 25], repetition=1, concurrency=32,
                              warmup_requests=300)
        with pytest.raises(RB.BenchmarkError, match="exactly one cell"):
            RB.check_live_authorization(cells, authorize=SMALL10_ID, expect_head=HEAD,
                                        warmup_requests=300, repetition=1)

    def test_plan_only_mode_writes_nothing(self, tmp_path):
        import io
        out, target = io.StringIO(), tmp_path / "plan-only"
        assert RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "1",
                        "--concurrency", "32", "--warmup-requests", "300",
                        "--output-dir", str(target)], stdout=out) == 0
        text = out.getvalue()
        assert "PLAN ONLY" in text and SMALL10_ID in text
        assert "b-small-r0010-c32-w1" in text and "b-small-r0010-c32-m1" in text
        assert not target.exists() and list(tmp_path.iterdir()) == []

    def test_gate_closed_cli_refuses_before_anything(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "1",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "not enabled" in capsys.readouterr().err
        assert not out.exists()

    @pytest.mark.parametrize("extra", [
        ["--rate", "25"], ["--rate", "50"], ["--rate", "100"], ["--concurrency", "16"],
        ["--warmup-requests", "25"], ["--warmup-requests", "301"], ["--repetition", "2"],
    ])
    def test_cli_refuses_wrong_arguments_before_anything_starts(self, extra, monkeypatch,
                                                                tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", lambda *a, **k: pytest.fail("reached run_cell"))
        args = {"--level": "SMALL", "--rate": "10", "--repetition": "1", "--concurrency": "32",
                "--warmup-requests": "300"}
        for key, value in zip(extra[::2], extra[1::2]):
            args[key] = value
        argv = [item for pair in args.items() for item in pair]
        out = tmp_path / "out"
        code = RB.main([*argv, "--live", "--authorize", SMALL10_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "nothing was started" in capsys.readouterr().err
        assert not out.exists()

    def test_the_cli_reaches_run_cell_once_for_the_exact_cell(self, monkeypatch, tmp_path):
        import io
        calls = []

        def fake_run_cell(cell, ladder, **kw):
            calls.append((cell.benchmark_id, kw["expect_head"]))
            path = kw["output_dir"] / f"{cell.benchmark_id}-fake.json"
            path.write_text("{}", encoding="utf-8")
            return {"verdict": "VALID"}, path, True

        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", fake_run_cell)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "1",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)], stdout=io.StringIO())
        assert code == 0 and calls == [(SMALL10_ID, HEAD)]
        assert (out / "ledger" / f"{SMALL10_ID}.started").is_file()
        assert (out / "ledger" / f"{SMALL10_ID}.done").is_file()
        assert not (out / RB.OWNER_FILE).exists()                 # released after a clean run

    def test_a_small_claim_leaves_the_smoke_records_untouched(self, tmp_path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        before = {}
        for n in (2, 3, 4, 5):
            for suffix in (".started", ".done"):
                path = ledger / f"bm-smoke-r0005-c32-rep{n}{suffix}"
                path.write_text(f"smoke-{n}{suffix}\n", encoding="utf-8", newline="")
                before[path] = path.read_bytes()
        result = tmp_path / "bm-smoke-r0005-c32-rep5-20261011T143111.json"
        result.write_text("{}", encoding="utf-8")
        before[result] = result.read_bytes()
        started = RB.claim_run(tmp_path, _small(), head=HEAD, arguments={"repetition": 1},
                               now=lambda: 1_800_000_000.0, pid=lambda: 4242)
        assert started == ledger / f"{SMALL10_ID}.started" and started.is_file()
        for path, content in before.items():
            assert path.read_bytes() == content, path.name
        assert sorted(p.name for p in ledger.iterdir()) == sorted(
            [f"bm-smoke-r0005-c32-rep{n}{s}" for n in (2, 3, 4, 5) for s in (".started", ".done")]
            + [f"{SMALL10_ID}.started"])

    def test_the_small_cell_cannot_run_twice(self, tmp_path):
        started = RB.claim_run(tmp_path, _small(), head=HEAD, arguments={},
                               now=lambda: 1_800_000_000.0, pid=lambda: 4242)
        (tmp_path / RB.OWNER_FILE).unlink()          # as after a released run
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            RB.claim_run(tmp_path, _small(), head=HEAD, arguments={})
        assert started.is_file()
        (tmp_path / "bm-small-r0010-c32-rep1-20261011T150000.json").write_text("{}")
        (tmp_path / "ledger" / f"{SMALL10_ID}.started").unlink()
        with pytest.raises(RB.BenchmarkError, match="already exists"):
            RB.claim_run(tmp_path, _small(), head=HEAD, arguments={})

    def test_the_small_and_smoke_cells_cannot_be_claimed_together(self, tmp_path):
        RB.claim_run(tmp_path, _small(), head=HEAD, arguments={})
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            _claim(tmp_path, n=4)
        assert not (tmp_path / "ledger" / "bm-smoke-r0005-c32-rep4.started").exists()

    def test_the_gates_ladder_and_thresholds_are_untouched(self):
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        spec = [lv for lv in LADDER["levels"] if lv["name"] == "SMALL"][0]
        assert (spec["target_requests"], spec["target_spans"], spec["rates"],
                spec["live_allowed"], spec["refresh_and_detector"]) == (
            1000, 7000, [10, 25, 50, 100], True, True)
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600
        assert LADDER["defaults"]["drain_limit_seconds"] == 120
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
        assert dict(RB._SMALL10_FIRST) == {k: v for k, v in SMALL10_FIELDS.items()
                                           if k != "repetition"}
        assert RB.projected_need_bytes(_small()) > RB.projected_need_bytes(_cell())


# ---------------------------------------------------------------------------
# B3J-59: optional diagnostics preserved in the result (nothing is run)
# ---------------------------------------------------------------------------

NEW_LEAVES = (("lag", "raw_series_records"), ("lag", "raw_series_times_s"),
              ("lag", "canonical_series_records"), ("lag", "canonical_series_times_s"),
              ("lag", "samples_taken"), ("lag", "sampler_started_epoch_s"),
              ("throughput", "persisted_per_second_series"),
              ("latency", "persistence_ms_max"), ("analytics", "detector_counts"))
RISING_LAG = [(float(10 * i), {"raw": i, "canonical": 10 * i + 1}) for i in range(8)]


def _without_new_leaves(result):
    stripped = json.loads(json.dumps(result))
    for section, name in NEW_LEAVES:
        stripped["metrics"][section].pop(name, None)
    return stripped


def _baseline(monkeypatch, **evidence):
    """The same evidence assembled with the diagnostics switched off."""
    monkeypatch.setattr(RB, "_add_diagnostics", lambda *args, **kwargs: None)
    return _assembled(**evidence)


class TestDiagnosticCapture:
    def test_lag_series_and_times_are_preserved(self):
        samples = [(0.0, {"raw": 0, "canonical": 0}), (10.02, {"raw": 3, "canonical": 120}),
                   (20.0, {"raw": 2, "canonical": 277}), (30.0449, {"raw": 0, "canonical": 10})]
        lag = _assembled(lag_samples=samples)["metrics"]["lag"]
        assert lag["raw_series_records"]["value"] == [0, 3, 2, 0]
        assert lag["canonical_series_records"]["value"] == [0, 120, 277, 10]
        assert lag["raw_series_times_s"]["value"] == [0.0, 10.02, 20.0, 30.045]
        assert lag["canonical_series_times_s"]["value"] == [0.0, 10.02, 20.0, 30.045]
        for name in ("raw_series_records", "canonical_series_records", "raw_series_times_s",
                     "canonical_series_times_s"):
            assert lag[name]["kind"] == "OBSERVATION" and lag[name]["n"] == 4
        assert lag["samples_taken"]["value"] == 4 and lag["samples_taken"]["kind"] == "OBSERVATION"
        assert max(lag["canonical_series_records"]["value"]) == lag["canonical_max"]["value"] == 277
        assert max(lag["raw_series_records"]["value"]) == lag["raw_max"]["value"] == 3

    def test_failed_samples_are_omitted_and_never_zero(self):
        samples = [(0.0, {"raw": 1, "canonical": 5}), (10.0, None),
                   (20.0, {"raw": None, "canonical": 9}), (30.0, {"raw": 4, "canonical": None})]
        lag = _assembled(lag_samples=samples)["metrics"]["lag"]
        assert lag["raw_series_records"]["value"] == [1, 4]
        assert lag["raw_series_times_s"]["value"] == [0.0, 30.0]
        assert lag["canonical_series_records"]["value"] == [5, 9]
        assert lag["canonical_series_times_s"]["value"] == [0.0, 20.0]
        assert lag["samples_taken"]["value"] == 4                 # attempted, failed ones included
        assert lag["raw_series_records"]["n"] == lag["raw_series_times_s"]["n"] == 2
        assert 0 not in lag["canonical_series_records"]["value"]

    def test_no_samples_or_only_failed_samples_are_not_measured(self):
        lag = _assembled()["metrics"]["lag"]                      # no samples at all
        for name in ("raw_series_records", "raw_series_times_s", "canonical_series_records",
                     "canonical_series_times_s", "samples_taken"):
            assert lag[name]["kind"] == "NOT_MEASURED" and lag[name]["value"] is None, name
        assert "no lag samples" in lag["raw_series_records"]["reason"]
        failed = _assembled(lag_samples=[(0.0, None), (10.0, None)])["metrics"]["lag"]
        assert failed["raw_series_records"]["kind"] == "NOT_MEASURED"
        assert failed["raw_series_records"]["reason"] == "every sample failed"
        assert failed["samples_taken"]["value"] == 2

    def test_sampler_start_is_optional(self):
        assert "sampler_started_epoch_s" not in _assembled()["metrics"]["lag"]
        leaf = _assembled(sampler_started=5000.5)["metrics"]["lag"]["sampler_started_epoch_s"]
        assert leaf["value"] == 5000.5 and leaf["kind"] == "OBSERVATION" and leaf["n"] == 1
        assert leaf["window"] == {"start": RB._iso(5000.5), "end": RB._iso(5000.5),
                                  "clock": "host"}
        assert "after this instant" in leaf["method"]

    def test_ingestion_histogram_is_preserved_with_its_limits_stated(self):
        counts = [41, 69, 71, 70, 68]
        result = _assembled(db_overrides={"per_second": counts})
        leaf = result["metrics"]["throughput"]["persisted_per_second_series"]
        assert leaf["value"] == counts and leaf["n"] == 5 and leaf["kind"] == "OBSERVATION"
        assert leaf["window"] == {"start": RB._iso(5000.0), "end": RB._iso(5020.0),
                                  "clock": "database"}
        for phrase in ("counts only", "seconds with no rows are omitted",
                       "no bucket carries a timestamp", "gaps are not represented"):
            assert phrase in leaf["method"], phrase
        assert len(leaf["value"]) == len(counts)          # nothing was padded for missing seconds

    @pytest.mark.parametrize("db_overrides", [{"per_second": []}, {"per_second": None}])
    def test_missing_or_empty_histogram_is_not_measured(self, db_overrides):
        leaf = _assembled(db_overrides=db_overrides)["metrics"]["throughput"][
            "persisted_per_second_series"]
        assert leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None
        assert "no per-second counts" in leaf["reason"]

    def test_no_database_result_is_handled(self):
        result = _assembled(database=None)
        leaf = result["metrics"]["throughput"]["persisted_per_second_series"]
        assert leaf["kind"] == "NOT_MEASURED" and leaf["reason"] == "no database result"
        assert result["metrics"]["latency"]["persistence_ms_max"]["kind"] == "NOT_MEASURED"

    def test_persistence_maximum_with_a_valid_clock(self):
        values = [10.0, 20.0, 4120.7, 30.0]
        leaf = _assembled(db_overrides={"persistence_ms": values})["metrics"]["latency"][
            "persistence_ms_max"]
        assert leaf["value"] == 4120.7 and leaf["kind"] == "DERIVED" and leaf["n"] == 4
        assert leaf["window"]["clock"] == "database"

    def test_persistence_maximum_is_not_reported_with_an_invalid_clock(self, tmp_path):
        result = _run(tmp_path, smoke=FakeSmoke(tmp_path, clock=(1000.0, 1000.0, 1000.05)))[0]
        assert result["clock"]["valid"] is False
        leaf = result["metrics"]["latency"]["persistence_ms_max"]
        assert leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None
        assert "clock skew outside its limits" in leaf["reason"]

    def test_persistence_maximum_without_clock_samples_or_spans(self):
        no_clock = _assembled(clock_before=[], clock_after=[])["metrics"]["latency"]
        assert no_clock["persistence_ms_max"]["reason"] == "clock skew not measured"
        empty = _assembled(db_overrides={"persistence_ms": []})["metrics"]["latency"]
        assert empty["persistence_ms_max"]["kind"] == "NOT_MEASURED"
        assert empty["persistence_ms_max"]["reason"] == "no spans read"

    def test_detector_counts_by_signal_through_the_cell(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path)
        counts = result["metrics"]["analytics"]["detector_counts"]
        assert clean and sorted(counts) == sorted(result["metrics"]["analytics"]["detector_ms"])
        trace = counts["trace_latency/agentops.request"]
        assert {k: v["value"] for k, v in trace.items()} == {
            "candidates": 1300, "emitted": 22, "inserted": 22, "duplicate": 0}
        assert all(v["kind"] == "OBSERVATION" and v["n"] == 1 for v in trace.values())
        assert counts["tool_failure/"]["emitted"]["value"] == 0

    def test_only_the_four_counts_are_read_and_there_is_no_severity(self, tmp_path):
        groups = [{"signal_path": "trace_latency", "operation_name": "x", "candidates": "10",
                   "emitted": "2", "inserted": "2", "duplicate": "0", "duration_ms": "5",
                   "severity": "WARNING", "severity_counts": "INFO=1"}]
        assert RB.detector_counts_from_groups(groups) == {
            "trace_latency/x": {"candidates": 10, "emitted": 2, "inserted": 2, "duplicate": 0}}
        result = _run(tmp_path)[0]
        assert "severity" not in json.dumps(result).lower()
        import inspect
        for function in (RB.detector_counts_from_groups, RB._diag_detector_counts,
                         RB._add_diagnostics):
            assert "severity" not in inspect.getsource(function).replace("No severity", "")

    @pytest.mark.parametrize("groups", [None, 5, [], [{}], [{"signal_path": "a"}],
                                        [{"signal_path": "a", "operation_name": "b",
                                          "emitted": "x"}], ["not a mapping"]])
    def test_detector_count_parsing_never_raises(self, groups):
        assert RB.detector_counts_from_groups(groups) is None

    def test_detector_counts_without_the_detector(self):
        cell = dataclasses.replace(_cell(), refresh_and_detector=False)
        leaf = _assembled(cell=cell)["metrics"]["analytics"]["detector_counts"]
        assert leaf["kind"] == "NOT_MEASURED" and "not run at level" in leaf["reason"]
        unread = _assembled()["metrics"]["analytics"]["detector_counts"]
        assert unread["kind"] == "NOT_MEASURED" and unread["reason"] == "detector counts not read"

    def test_diagnostic_failure_keeps_the_required_result(self, monkeypatch):
        samples = [(float("nan"), {"raw": 1, "canonical": 2}), (10.0, {"raw": 1, "canonical": 3})]
        broken = _assembled(lag_samples=samples, sampler_started=5000.5)
        baseline = _baseline(monkeypatch, lag_samples=samples)
        assert any(text.startswith("diagnostic lag series not recorded")
                   for text in broken["limitations"])
        assert "raw_series_records" not in broken["metrics"]["lag"]
        assert "samples_taken" not in broken["metrics"]["lag"]          # atomic: all or none
        assert "sampler_started_epoch_s" in broken["metrics"]["lag"]    # the others survive
        stripped = _without_new_leaves(broken)
        stripped["limitations"] = [t for t in stripped["limitations"]
                                   if not t.startswith("diagnostic ")]
        assert stripped == baseline
        M.validate_result(broken)

    @pytest.mark.parametrize("builder", ["_diag_lag_series", "_diag_sampler_start",
                                         "_diag_ingest_series", "_diag_persistence_max",
                                         "_diag_detector_counts"])
    def test_any_failing_builder_is_isolated(self, monkeypatch, builder):
        def boom(*args, **kwargs):
            raise RuntimeError("builder failed")

        monkeypatch.setattr(RB, builder, boom)
        result = _assembled(lag_samples=RISING_LAG[:3], sampler_started=1.0)
        assert sum(t.startswith("diagnostic ") and t.endswith("RuntimeError")
                   for t in result["limitations"]) == 1
        assert result["verdict"] == "VALID" and result["integrity"]["ok"] is True
        assert result["metrics"]["latency"]["persistence_ms"]["p95"]["value"] == 40.0
        M.validate_result(result)

    def test_an_existing_metric_is_never_replaced(self):
        metrics = {"lag": {"samples_taken": {"kept": True}}}
        limitations = []
        RB._add_diagnostics(metrics, limitations, (
            ("lag series", lambda: {("lag", "samples_taken"): M.Metric(
                3, "samples", "OBSERVATION", "harness", "x", None, 3).to_dict()}),))
        assert metrics["lag"]["samples_taken"] == {"kept": True}
        assert limitations == ["diagnostic lag series not recorded: BenchmarkError"]

    def test_existing_results_are_unchanged_apart_from_the_new_leaves(self, monkeypatch):
        evidence = dict(lag_samples=RISING_LAG, sampler_started=5000.5,
                        db_overrides={"per_second": [30, 40, 50]})
        with_diagnostics = _assembled(**evidence)
        baseline = _baseline(monkeypatch, **evidence)
        assert _without_new_leaves(with_diagnostics) == baseline
        assert with_diagnostics["verdict"] == baseline["verdict"] == "STOPPED"   # lag grew
        assert with_diagnostics["stop"] == baseline["stop"]
        assert with_diagnostics["integrity"] == baseline["integrity"]
        assert with_diagnostics["claims"] == baseline["claims"]
        assert with_diagnostics["limitations"] == baseline["limitations"]

    def test_the_cell_records_the_sampler_start_and_keeps_its_call_order(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path)
        assert clean and smoke.calls == ORDER                  # no new harness call or query
        leaf = result["metrics"]["lag"]["sampler_started_epoch_s"]
        assert leaf["value"] == 5003.0                         # the injected wall clock
        assert result["metrics"]["lag"]["samples_taken"]["value"] >= 1
        assert smoke.calls.count("psql") == 1

    def test_older_results_without_the_new_leaves_still_validate(self):
        M.validate_result(_without_new_leaves(_assembled(lag_samples=RISING_LAG)))
        older = _without_new_leaves(TestHistoricalCompatibility()._historical())
        assert not any(name in older["metrics"][section] for section, name in NEW_LEAVES)
        M.validate_result(older)                          # the pre-B3J-59 shape stays valid

    def test_serialization_validation_and_size(self, tmp_path, monkeypatch):
        evidence = dict(lag_samples=[(10.0 * i, {"raw": i, "canonical": 30 * i}) for i in range(12)],
                        sampler_started=5000.5, db_overrides={"per_second": list(range(100, 200))})
        result = _assembled(**evidence)
        path = RB.write_result(result, tmp_path / "out")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded == json.loads(json.dumps(result))
        M.validate_result(loaded)
        plain = json.dumps(_baseline(monkeypatch, **evidence), sort_keys=True)
        assert len(json.dumps(result, sort_keys=True)) - len(plain) < 6000

    def test_no_new_leaf_is_inside_the_percentile_mappings(self):
        latency = _assembled()["metrics"]["latency"]
        assert sorted(latency["persistence_ms"]) == ["p50", "p95", "p99"]
        assert sorted(latency["request_ms"]) == ["p50", "p95", "p99"]


# ---------------------------------------------------------------------------
# B3J-62: the SMALL-10 repeat, rep2 (nothing is run)
# ---------------------------------------------------------------------------

SMALL10_REP2_ID = "bm-small-r0010-c32-rep2"
SMALL10_REP2_FIELDS = {**SMALL10_FIELDS, "repetition": 2}


class TestSmallTenSecondRepetition:
    def test_rep2_is_approved_with_the_exact_specification(self):
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_REP2_ID]) == SMALL10_REP2_FIELDS
        cell = _small(repetition=2)
        assert cell.benchmark_id == SMALL10_REP2_ID and RB.is_approved_live_cell(cell)
        assert (cell.level, cell.rate, cell.requests, cell.warmup_requests, cell.concurrency,
                cell.seed, cell.repetition) == ("SMALL", 10, 1000, 300, 32, 42, 2)
        assert cell.target()["scenario"] == "normal"
        assert cell.planned_spans == 7000 and cell.measured_seconds == 100.0
        assert cell.live_allowed is True and cell.refresh_and_detector is True
        assert (cell.warmup_run_id, cell.measured_run_id) == ("b-small-r0010-c32-w2",
                                                              "b-small-r0010-c32-m2")
        assert RB.RUN_ID_RE.fullmatch(cell.warmup_run_id)
        assert RB.RUN_ID_RE.fullmatch(cell.measured_run_id)
        assert _authorize_small(cell, authorize=SMALL10_REP2_ID, repetition=2) == cell

    def test_rep1_and_rep2_differ_only_in_repetition_and_run_ids(self):
        first, second = _small(repetition=1).target(), _small(repetition=2).target()
        assert {k for k in first if first[k] != second[k]} == {
            "repetition", "warmup_run_id", "measured_run_id"}
        assert (first["repetition"], second["repetition"]) == (1, 2)
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_ID]) | {"repetition": 2} == dict(
            RB.APPROVED_LIVE_CELLS[SMALL10_REP2_ID])
        assert RB.REFRESH_TIMEOUT_SECONDS == 600.0 and LADDER["defaults"]["seed"] == 42

    def test_exactly_two_small_cells_and_four_smoke_cells(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == [
            SMALL10_ID, SMALL10_REP2_ID, "bm-smoke-r0005-c32-rep2", "bm-smoke-r0005-c32-rep3",
            "bm-smoke-r0005-c32-rep4", "bm-smoke-r0005-c32-rep5"]
        assert len(RB.APPROVED_LIVE_CELLS) == 6
        assert [k for k in RB.APPROVED_LIVE_CELLS if "small" in k] == [SMALL10_ID,
                                                                       SMALL10_REP2_ID]
        for n in (2, 3, 4, 5):
            assert dict(RB.APPROVED_LIVE_CELLS[f"bm-smoke-r0005-c32-rep{n}"]) == {
                **SMOKE_REPEAT_FIELDS, "repetition": n}

    @pytest.mark.parametrize("repetition", [3, 4, 5, 6, 10])
    def test_rep3_and_higher_stay_refused(self, repetition):
        cell = _small(repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r0010-c32-rep{repetition}",
                             repetition=repetition)
        for authorize, selector in ((SMALL10_ID, 1), (SMALL10_REP2_ID, 2)):
            with pytest.raises(RB.BenchmarkError):
                _authorize_small(cell, authorize=authorize, repetition=selector)

    def test_rep1_and_rep2_authorizations_cannot_be_interchanged(self):
        rep1, rep2 = _small(repetition=1), _small(repetition=2)
        assert _authorize_small(rep1) == rep1
        assert _authorize_small(rep2, authorize=SMALL10_REP2_ID, repetition=2) == rep2
        with pytest.raises(RB.BenchmarkError, match="is not bm-small-r0010-c32-rep1"):
            _authorize_small(rep2, authorize=SMALL10_ID, repetition=1)        # token 1, cell 2
        with pytest.raises(RB.BenchmarkError, match="is not bm-small-r0010-c32-rep2"):
            _authorize_small(rep1, authorize=SMALL10_REP2_ID, repetition=2)   # token 2, cell 1
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(rep2, authorize=SMALL10_ID, repetition=2)        # wrong selector
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(rep1, authorize=SMALL10_REP2_ID, repetition=1)

    @pytest.mark.parametrize("warmup", [None, 0, 25, 299, 301])
    def test_rep2_needs_the_exact_warmup_argument(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 300"):
            _authorize_small(_small(repetition=2), authorize=SMALL10_REP2_ID, repetition=2,
                             warmup=warmup)

    @pytest.mark.parametrize("change", [dict(requests=999), dict(rate=5), dict(rate=25),
                                        dict(concurrency=16), dict(concurrency=33),
                                        dict(warmup_requests=299), dict(level="SMOKE"),
                                        dict(repetition=1), dict(repetition=3)])
    def test_a_changed_rep2_field_is_not_the_approved_cell(self, change):
        cell = dataclasses.replace(_small(repetition=2), **change)
        approved = RB.APPROVED_LIVE_CELLS.get(cell.benchmark_id)
        assert approved is None or RB._cell_fields(cell) != dict(approved) or \
            cell.benchmark_id in (SMALL10_ID,) and change == dict(repetition=1)
        if change not in (dict(repetition=1),):
            assert not RB.is_approved_live_cell(cell), change

    @pytest.mark.parametrize("rate", [25, 50, 100])
    def test_other_small_rates_stay_refused_at_rep2(self, rate):
        cell = _small(rate=rate, repetition=2)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r{rate:04d}-c32-rep2", repetition=2)

    def test_a_rep2_claim_is_isolated_from_rep1_and_smoke(self, tmp_path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        before = {}
        names = [f"{cid}{suffix}" for cid in (SMALL10_ID, "bm-smoke-r0005-c32-rep2",
                                              "bm-smoke-r0005-c32-rep5")
                 for suffix in (".started", ".done")]
        for name in names:
            path = ledger / name
            path.write_text(f"record {name}\n", encoding="utf-8", newline="")
            before[path] = path.read_bytes()
        for result in ("bm-small-r0010-c32-rep1-20261011T155919.json",
                       "bm-smoke-r0005-c32-rep5-20261011T143111.json"):
            path = tmp_path / result
            path.write_text("{}", encoding="utf-8")
            before[path] = path.read_bytes()
        started = RB.claim_run(tmp_path, _small(repetition=2), head=HEAD, arguments={},
                               now=lambda: 1_800_000_000.0, pid=lambda: 4242)
        assert started == ledger / f"{SMALL10_REP2_ID}.started" and started.is_file()
        for path, content in before.items():
            assert path.read_bytes() == content, path.name
        assert sorted(p.name for p in ledger.iterdir()) == sorted(names + [started.name])
        assert not list(tmp_path.glob(f"{SMALL10_REP2_ID}-*.json"))

    def test_rep2_cannot_run_twice_nor_alongside_rep1(self, tmp_path):
        started = RB.claim_run(tmp_path, _small(repetition=2), head=HEAD, arguments={})
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            RB.claim_run(tmp_path, _small(repetition=1), head=HEAD, arguments={})
        assert not (tmp_path / "ledger" / f"{SMALL10_ID}.started").exists()
        (tmp_path / RB.OWNER_FILE).unlink()          # as after a released run
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            RB.claim_run(tmp_path, _small(repetition=2), head=HEAD, arguments={})
        assert started.is_file()

    def test_the_cli_reaches_run_cell_once_for_an_authorized_rep2(self, monkeypatch, tmp_path):
        import io
        calls = []

        def fake_run_cell(cell, ladder, **kw):
            calls.append((cell.benchmark_id, kw["expect_head"]))
            path = kw["output_dir"] / f"{cell.benchmark_id}-fake.json"
            path.write_text("{}", encoding="utf-8")
            return {"verdict": "VALID"}, path, True

        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", fake_run_cell)
        out = tmp_path / "out"
        (out / "ledger").mkdir(parents=True)
        rep1_record = out / "ledger" / f"{SMALL10_ID}.done"
        rep1_record.write_text("rep1", encoding="utf-8")
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "2",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP2_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)], stdout=io.StringIO())
        assert code == 0 and calls == [(SMALL10_REP2_ID, HEAD)]
        assert (out / "ledger" / f"{SMALL10_REP2_ID}.started").is_file()
        assert (out / "ledger" / f"{SMALL10_REP2_ID}.done").is_file()
        assert rep1_record.read_text(encoding="utf-8") == "rep1"
        assert not (out / RB.OWNER_FILE).exists()

    def test_gate_closed_cli_refuses_rep2_before_anything(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "2",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP2_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "not enabled" in capsys.readouterr().err
        assert not out.exists()

    @pytest.mark.parametrize("extra", [
        ["--rate", "25"], ["--rate", "50"], ["--rate", "100"], ["--concurrency", "16"],
        ["--warmup-requests", "25"], ["--warmup-requests", "301"], ["--repetition", "1"],
        ["--repetition", "3"],
    ])
    def test_cli_refuses_wrong_rep2_arguments_before_anything_starts(
            self, extra, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", lambda *a, **k: pytest.fail("reached run_cell"))
        args = {"--level": "SMALL", "--rate": "10", "--repetition": "2", "--concurrency": "32",
                "--warmup-requests": "300"}
        for key, value in zip(extra[::2], extra[1::2]):
            args[key] = value
        argv = [item for pair in args.items() for item in pair]
        out = tmp_path / "out"
        code = RB.main([*argv, "--live", "--authorize", SMALL10_REP2_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "nothing was started" in capsys.readouterr().err
        assert not out.exists()

    def test_plan_only_mode_for_rep2_writes_nothing(self, tmp_path):
        import io
        out, target = io.StringIO(), tmp_path / "plan-only"
        assert RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "2",
                        "--concurrency", "32", "--warmup-requests", "300",
                        "--output-dir", str(target)], stdout=out) == 0
        assert "PLAN ONLY" in out.getvalue() and SMALL10_REP2_ID in out.getvalue()
        assert "b-small-r0010-c32-w2" in out.getvalue() and "b-small-r0010-c32-m2" in out.getvalue()
        assert not target.exists() and list(tmp_path.iterdir()) == []

    def test_a_rep2_result_still_carries_the_diagnostics_and_validates(self):
        result = _assembled(cell=_small(repetition=2), lag_samples=RISING_LAG[:4],
                            sampler_started=5000.5)
        assert result["benchmark_id"] == SMALL10_REP2_ID
        for section, name in NEW_LEAVES:
            assert name in result["metrics"][section], name
        M.validate_result(result)     # the SMOKE-sized fixture is not a SMALL run: no verdict claim

    def test_the_gates_and_thresholds_are_untouched_by_rep2(self):
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        assert dict(RB._SMALL10_FIRST) == {k: v for k, v in SMALL10_FIELDS.items()
                                           if k != "repetition"}
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600
        assert LADDER["defaults"]["drain_limit_seconds"] == 120
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
