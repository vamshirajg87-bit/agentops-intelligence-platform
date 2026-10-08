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
                                           "bm-smoke-r0005-c32-rep4"])
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
                     _approved_cells(repetition=1)[0], _approved_cells(repetition=4)[0]):
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
        assert {k: v["value"] for k, v in sched.items()} == {
            "late_starts": 3, "started_requests": 98, "not_started_requests": 2,
            "max_start_lag_seconds": 0.4, "mean_start_lag_seconds": 0.05}
        assert {v["kind"] for v in sched.values()} == {"MEASURED"}

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
        assert sorted(RB.APPROVED_LIVE_CELLS) == ["bm-smoke-r0005-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep3"]
        assert [c["repetition"] for c in RB.APPROVED_LIVE_CELLS.values()] == [2, 3]
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

    @pytest.mark.parametrize("n", [1, 4])
    def test_rep1_and_rep4_refused(self, open_gate, tmp_path, n, capsys):
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
