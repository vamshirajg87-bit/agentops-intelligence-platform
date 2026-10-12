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


#: The real pre-claim checks (B3J-103), for tests that exercise them; every other test
#: runs with them replaced, as it runs with a fake harness: they would otherwise look at
#: the real repository and the real canonical output directory.
REAL_CHECK_LIVE_OUTPUT_DIR = RB.check_live_output_dir
REAL_VERIFY_REPOSITORY = RB.verify_repository_readonly


@pytest.fixture(autouse=True)
def pre_claim_checks_replaced(monkeypatch):
    monkeypatch.setattr(RB, "check_live_output_dir", lambda output_dir: Path(output_dir).resolve())
    monkeypatch.setattr(RB, "verify_repository_readonly",
                        lambda expect_head, **kw: {"head": expect_head, "branch": "test-branch"})


class Boom(RuntimeError):
    pass


def timed_series_payload(seconds=22, rows=700, start=4999, **changes):
    """What run_series_sql returns for a run of `rows` spans: 35 a second ended in
    seconds 0-19 and ingested one second later (offsets 1-20), 40 ms each."""
    ended = [35 if 0 <= i < 20 else 0 for i in range(seconds)]
    ingested = [35 if 1 <= i <= 20 else 0 for i in range(seconds)]
    offsets = [i for i, c in enumerate(ingested) if c]
    payload = {"start_epoch_s": start, "end_epoch_s": start + seconds - 1, "rows": rows,
               "cap_seconds": RB.MAX_TIMED_SERIES_SECONDS, "ended": ended, "ingested": ingested,
               "latency_offsets": offsets, "latency_mean_ms": [40.0] * len(offsets),
               "latency_max_ms": [45.5] * len(offsets)}
    payload.update(changes)
    return payload


def gap_series_payload(seconds=22, rows=700, start=4999, **changes):
    """What run_gap_sql returns for the same run as timed_series_payload: 35 rows a
    second ingested in seconds 1-20, three partitions (12 + 12 + 11 a second), each gap
    between 5 and 30 ms.  The first row (in second 1) has no gap."""
    ingested = [35 if 1 <= i <= 20 else 0 for i in range(seconds)]
    offsets = [i for i, c in enumerate(ingested) if c]
    payload = {
        "start_epoch_s": start, "end_epoch_s": start + seconds - 1, "rows": rows,
        "cap_seconds": RB.MAX_TIMED_SERIES_SECONDS, "stall_ms": RB.STALL_GAP_MS,
        "partition_limit": RB.MAX_GAP_PARTITIONS, "first_row_offset": 1,
        "gap_count": rows - 1, "gap_p50_ms": 14.0, "gap_p90_ms": 20.0, "gap_p99_ms": 28.0,
        "gap_max_ms": 30.0, "stall_gaps": 0, "ingested": ingested,
        "gap_offsets": offsets, "gap_counts": [34] + [35] * (len(offsets) - 1),
        "gap_median_ms": [14.0] * len(offsets), "gap_max_by_second_ms": [30.0] * len(offsets),
        "gap_stall_counts": [0] * len(offsets),
        "partitions": [
            {"partition": 0, "rows": 240, "min_offset": 0, "max_offset": 239,
             "distinct_offsets": 240, "per_second": [12 if 1 <= i <= 20 else 0 for i in range(seconds)]},
            {"partition": 1, "rows": 240, "min_offset": 0, "max_offset": 239,
             "distinct_offsets": 240, "per_second": [12 if 1 <= i <= 20 else 0 for i in range(seconds)]},
            {"partition": 2, "rows": 220, "min_offset": 0, "max_offset": 219,
             "distinct_offsets": 220, "per_second": [11 if 1 <= i <= 20 else 0 for i in range(seconds)]}]}
    payload.update(changes)
    return payload


class FakeSmoke:
    """Records every call; `fail` names the step that raises."""

    GROUP_STREAM, GROUP_STORAGE = "d3-stream-processor", "d3-storage-consumer"
    TOPIC_RAW, TOPIC_SPANS, TOPIC_DLQ = "raw", "spans", "dlq"
    POSTGRES_PORT, T_COMMAND = 15432, 60.0

    def __init__(self, tmp: Path, *, fail: str = "", cleanup_ok: bool = True,
                 stats: str = "proj-kafka-1|5.0%|100MiB / 1GiB\nproj-pg-1|1.0%|50MiB / 1GiB",
                 lag=0, clock=(1000.0, 1000.001, 1000.002), late_starts=0, dlq=0,
                 series=None, gap=None):
        self.calls: list[str] = []
        self.fail, self.cleanup_ok, self.stats, self.lag = fail, cleanup_ok, stats, lag
        self.clock_sample, self.late_starts, self.dlq = clock, late_starts, dlq
        self.series = series if series is not None else timed_series_payload()
        self.gap = gap if gap is not None else gap_series_payload()
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
        if "gap_ms" in sql:                                # the optional ingest-gap query
            return json.dumps(self.gap)
        if "generate_series" in sql:                       # the optional per-second query
            return json.dumps(self.series)
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
         "clock_probe", "psql", "psql", "check_host_processes", "refresh_run", "run_detector",
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
        assert len(smoke.psql_sql) == 3            # the required query and two optional ones
        for sql in smoke.psql_sql:
            assert cell.measured_run_id in sql
            assert cell.warmup_run_id not in sql

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
                                                 "bm-small-r0010-c32-rep3",
                                                 "bm-small-r0010-c32-rep4",
                                                 "bm-smoke-r0005-c32-rep2",
                                                 "bm-smoke-r0005-c32-rep3",
                                                 "bm-smoke-r0005-c32-rep4",
                                                 "bm-smoke-r0005-c32-rep5"]
        assert [c["repetition"] for c in RB.APPROVED_LIVE_CELLS.values()] == [2, 3, 4, 5, 1, 2, 3, 4]
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
        assert len(RB.APPROVED_LIVE_CELLS) == 8      # four SMOKE cells plus SMALL-10 rep1-rep4

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
                                           "bm-small-r0010-c32-rep5", "bm-small-r0005-c32-rep4",
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

    def test_exactly_eight_cells_and_the_smoke_cells_unchanged(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == [
            SMALL10_ID, "bm-small-r0010-c32-rep2", "bm-small-r0010-c32-rep3",
            "bm-small-r0010-c32-rep4", "bm-smoke-r0005-c32-rep2", "bm-smoke-r0005-c32-rep3",
            "bm-smoke-r0005-c32-rep4", "bm-smoke-r0005-c32-rep5"]
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        assert [k for k in RB.APPROVED_LIVE_CELLS if "small" in k] == [
            SMALL10_ID, "bm-small-r0010-c32-rep2", "bm-small-r0010-c32-rep3",
            "bm-small-r0010-c32-rep4"]
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
                                        dict(concurrency=33), dict(repetition=5),
                                        dict(repetition=6), dict(level="SMOKE"),
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

    @pytest.mark.parametrize("repetition", [5, 6, 7, 8])
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
                                           "bm-small-r0010-c32-rep5", "bm-small-r0010-c32-rep0",
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
              ("latency", "persistence_ms_max"), ("analytics", "detector_counts"),
              # B3J-68
              ("throughput", "timed_series_start_epoch_s"),
              ("throughput", "timed_ended_per_second_series"),
              ("throughput", "timed_ingested_per_second_series"),
              ("latency", "timed_persistence_ms_mean_series"),
              ("latency", "timed_persistence_ms_max_series"),
              ("latency", "timed_persistence_ms_series_offsets_s"),
              ("resources", "cpu_percent_max"), ("resources", "cpu_percent_series"),
              ("resources", "cpu_percent_series_times_s"),
              # B3J-84
              ("latency", "ingest_gap_ms_p50"), ("latency", "ingest_gap_ms_p90"),
              ("latency", "ingest_gap_ms_p99"), ("latency", "ingest_gap_ms_max"),
              ("latency", "ingest_gap_stalls"), ("latency", "ingest_gap_series_start_epoch_s"),
              ("latency", "ingest_gap_ms_median_by_second"),
              ("latency", "ingest_gap_ms_max_by_second"),
              ("latency", "ingest_gap_stalls_by_second"),
              ("latency", "ingest_gap_series_offsets_s"),
              ("throughput", "partition_rows"), ("throughput", "partition_offset_min"),
              ("throughput", "partition_offset_max"),
              ("throughput", "partition_distinct_offsets"),
              ("throughput", "partition_ingested_per_second_series"))
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
        assert clean and smoke.calls == ORDER                  # the one post-run query of B3J-68
        leaf = result["metrics"]["lag"]["sampler_started_epoch_s"]
        assert leaf["value"] == 5003.0                         # the injected wall clock
        assert result["metrics"]["lag"]["samples_taken"]["value"] >= 1
        assert smoke.calls.count("psql") == 3

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
        # B3J-59 allowed 6,000 bytes; B3J-68 and B3J-84 add 9 and 15 not-measured leaves
        # (about 250 bytes each) when no evidence exists.  A full-length result is bounded
        # separately in TestIngestGapLeaves.
        assert len(json.dumps(result, sort_keys=True)) - len(plain) < 14000

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

    def test_exactly_four_small_cells_and_four_smoke_cells(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == [
            SMALL10_ID, SMALL10_REP2_ID, "bm-small-r0010-c32-rep3", "bm-small-r0010-c32-rep4",
            "bm-smoke-r0005-c32-rep2", "bm-smoke-r0005-c32-rep3", "bm-smoke-r0005-c32-rep4",
            "bm-smoke-r0005-c32-rep5"]
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        assert [k for k in RB.APPROVED_LIVE_CELLS if "small" in k] == [
            SMALL10_ID, SMALL10_REP2_ID, "bm-small-r0010-c32-rep3", "bm-small-r0010-c32-rep4"]
        for n in (2, 3, 4, 5):
            assert dict(RB.APPROVED_LIVE_CELLS[f"bm-smoke-r0005-c32-rep{n}"]) == {
                **SMOKE_REPEAT_FIELDS, "repetition": n}

    @pytest.mark.parametrize("repetition", [5, 6, 7, 10])
    def test_rep5_and_higher_stay_refused(self, repetition):
        cell = _small(repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r0010-c32-rep{repetition}",
                             repetition=repetition)
        for authorize, selector in ((SMALL10_ID, 1), (SMALL10_REP2_ID, 2),
                                    ("bm-small-r0010-c32-rep3", 3),
                                    ("bm-small-r0010-c32-rep4", 4)):
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
                                        dict(repetition=1), dict(repetition=5)])
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


# ---------------------------------------------------------------------------
# B3J-68: time-aligned series and retained CPU samples (nothing is run)
# ---------------------------------------------------------------------------

TIMED_LEAVES = (("throughput", "timed_series_start_epoch_s"),
                ("throughput", "timed_ended_per_second_series"),
                ("throughput", "timed_ingested_per_second_series"),
                ("latency", "timed_persistence_ms_mean_series"),
                ("latency", "timed_persistence_ms_max_series"),
                ("latency", "timed_persistence_ms_series_offsets_s"))
CPU_LEAVES = (("resources", "cpu_percent_max"), ("resources", "cpu_percent_series"),
              ("resources", "cpu_percent_series_times_s"))
STATS = [(0.0, [{"name": "kafka", "cpu_percent": 5.0, "memory_used_bytes": 100,
                 "memory_limit_bytes": 1000},
                {"name": "pg", "cpu_percent": 0.0, "memory_used_bytes": 50,
                 "memory_limit_bytes": 1000}]),
         (14.2, [{"name": "kafka", "cpu_percent": 180.5, "memory_used_bytes": 110,
                  "memory_limit_bytes": 1000},
                 {"name": "pg", "cpu_percent": 12.25, "memory_used_bytes": 55,
                  "memory_limit_bytes": 1000}])]
INVALID_CLOCK = dict(clock_before=[M.ClockSample(1000.0, 1000.0, 1000.05)],
                     clock_after=[M.ClockSample(1001.0, 1001.0, 1001.05)])


def _timed(**changes):
    return _assembled(timed_series=timed_series_payload(**changes))["metrics"]


class TestTimedSeries:
    def test_the_sql_is_read_only_bounded_and_scoped_to_the_run(self):
        import re
        sql = RB.run_series_sql("b-small-r0010-c32-m2")
        words = set(re.findall(r"[a-z_]+", sql.lower()))
        assert sql.lstrip().upper().startswith("WITH") and ";" not in sql
        assert not words & {"insert", "update", "delete", "drop", "alter", "create", "truncate",
                            "grant", "revoke", "copy", "vacuum", "set"}
        assert sql.count("'b-small-r0010-c32-m2'") == 1
        assert "agentops.workload.run_id' = 'b-small-r0010-c32-m2'" in sql
        assert "parent_span_id IS NULL" in sql and "trace_id IN (SELECT trace_id FROM roots)" in sql
        assert str(RB.MAX_TIMED_SERIES_SECONDS) in sql and RB.MAX_TIMED_SERIES_SECONDS == 1800
        assert "CASE WHEN t1 >= t0 AND t1 - t0 <" in sql        # bounded in SQL, never truncated
        assert sql == RB.run_series_sql("b-small-r0010-c32-m2")      # deterministic
        assert "w2" not in sql

    @pytest.mark.parametrize("run_id", ["", "x'; DROP TABLE telemetry_spans; --", "B-SMALL",
                                        "b-small' OR '1'='1", "b-small\n-m2", None])
    def test_a_bad_run_id_is_refused(self, run_id):
        with pytest.raises((RB.BenchmarkError, TypeError)):
            RB.run_series_sql(run_id)

    def test_persistence_uses_the_same_expression_and_clocks_as_the_required_query(self):
        expression = "extract(epoch from (ingested_at - end_time)) * 1000.0"
        assert expression in RB.run_metrics_sql("b-small-r0010-c32-m2")
        assert expression in RB.run_series_sql("b-small-r0010-c32-m2")
        leaf = _timed()["latency"]["timed_persistence_ms_max_series"]
        assert leaf["source"] == "ingested_at (database clock) - end_time (host clock)"
        assert leaf["kind"] == "DERIVED" and leaf["unit"] == "ms"

    def test_the_required_query_is_untouched(self):
        import hashlib
        text = RB.run_metrics_sql("b-small-r0010-c32-m2")
        assert hashlib.sha256(text.encode()).hexdigest() == (
            "4d4154095e38f1f58e7ca4018e92379d8b5082ff0d027b37d42c064b4af3a936")
        assert "generate_series" not in text and "per_second" in text

    def test_the_axis_is_explicit_deterministic_and_aligned(self):
        first, second = _timed(), _timed()
        assert first["throughput"] == second["throughput"] and first["latency"] == second["latency"]
        start = first["throughput"]["timed_series_start_epoch_s"]
        assert start["value"] == 4999 and start["unit"] == "s" and start["kind"] == "OBSERVATION"
        assert start["window"] == {"start": RB._iso(4999), "end": RB._iso(5021), "clock": "host"}
        ended = first["throughput"]["timed_ended_per_second_series"]
        ingested = first["throughput"]["timed_ingested_per_second_series"]
        assert len(ended["value"]) == len(ingested["value"]) == ended["n"] == 22
        assert sum(ended["value"]) == sum(ingested["value"]) == 700
        assert "epoch" in start["method"] and "index i" in start["method"]
        offsets = first["latency"]["timed_persistence_ms_series_offsets_s"]["value"]
        assert offsets == list(range(1, 21))
        assert all(ingested["value"][i] > 0 for i in offsets)
        for name in ("timed_persistence_ms_mean_series", "timed_persistence_ms_max_series"):
            assert first["latency"][name]["n"] == len(offsets) == len(first["latency"][name]["value"])
        assert first["latency"]["timed_persistence_ms_mean_series"]["value"][0] == 40.0
        assert first["latency"]["timed_persistence_ms_max_series"]["value"][0] == 45.5
        assert ingested["source"].endswith("(database clock)")
        assert ended["source"].endswith("(host clock)")

    def test_zero_seconds_are_observed_zeros_and_latency_has_no_value_for_them(self):
        metrics = _timed()
        ended = metrics["throughput"]["timed_ended_per_second_series"]["value"]
        ingested = metrics["throughput"]["timed_ingested_per_second_series"]["value"]
        assert ended[:2] == [35, 35] and ended[20:] == [0, 0] and ingested[0] == 0
        assert ingested[21] == 0 and ingested.count(0) == 2
        latency = metrics["latency"]["timed_persistence_ms_mean_series"]["value"]
        assert len(latency) == 20 and 0 not in latency            # 2 empty seconds, no value

    def test_a_series_that_is_all_zeros_but_one_second_validates(self):
        payload = timed_series_payload(
            seconds=3, rows=7, start=100, ended=[7, 0, 0], ingested=[0, 0, 7],
            latency_offsets=[2], latency_mean_ms=[2000.0], latency_max_ms=[2000.0])
        metrics = _assembled(timed_series=payload, db_overrides={"rows": 7, "distinct_spans": 7,
                                                                  "roots": 100})["metrics"]
        assert metrics["throughput"]["timed_ingested_per_second_series"]["value"] == [0, 0, 7]

    def test_missing_measurements_are_not_measured_never_zero(self):
        for result in (_assembled(), _assembled(timed_series=None)):
            for section, name in TIMED_LEAVES:
                leaf = result["metrics"][section][name]
                assert leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None, name
                assert leaf["reason"] == "the optional per-second query did not run or failed"
        no_clock = _assembled(timed_series=timed_series_payload(), clock_before=[],
                              clock_after=[])["metrics"]
        invalid = _assembled(timed_series=timed_series_payload(), **INVALID_CLOCK)["metrics"]
        for metrics, reason in ((no_clock, "clock skew not measured"),
                                (invalid, "clock skew outside its limits")):
            for section, name in TIMED_LEAVES:
                leaf = metrics[section][name]
                assert leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None
                assert leaf["reason"].startswith(reason), (name, leaf["reason"])

    def test_a_run_with_no_spans_is_not_measured(self):
        empty = {"start_epoch_s": None, "end_epoch_s": None, "rows": 0, "cap_seconds": 1800,
                 "ended": None, "ingested": None, "latency_offsets": None,
                 "latency_mean_ms": None, "latency_max_ms": None}
        leaf = _assembled(timed_series=empty)["metrics"]["throughput"][
            "timed_ended_per_second_series"]
        assert leaf["kind"] == "NOT_MEASURED" and "no spans" in leaf["reason"]

    def test_the_series_length_bound_is_enforced(self):
        cap = RB.MAX_TIMED_SERIES_SECONDS
        at_cap = timed_series_payload(
            seconds=cap, ended=[0] * (cap - 1) + [700],
            ingested=[700] + [0] * (cap - 1), latency_offsets=[0],
            latency_mean_ms=[1.0], latency_max_ms=[1.0])
        metrics = _assembled(timed_series=at_cap)["metrics"]
        assert len(metrics["throughput"]["timed_ended_per_second_series"]["value"]) == cap
        over = timed_series_payload(
            seconds=cap + 1, ended=[0] * cap + [700], ingested=[700] + [0] * cap,
            latency_offsets=[0], latency_mean_ms=[1.0], latency_max_ms=[1.0])
        leaf = _assembled(timed_series=over)["metrics"]["throughput"][
            "timed_ended_per_second_series"]
        assert leaf["kind"] == "NOT_MEASURED" and "exceeds the bound of 1800 s" in leaf["reason"]

    @pytest.mark.parametrize("changes, why", [
        (dict(ended=[35] * 21), "one count per second"),
        (dict(ingested=[1.5] + [0] * 21), "not a count"),
        (dict(ingested=[True] + [0] * 21), "not a count"),
        (dict(ended=[-35] + [35] * 19 + [0, 0]), "not a count"),
        (dict(ended=[36] + [35] * 19 + [0, 0]), "counts 701 spans, not 700"),
        (dict(rows=699), "699 rows, the required query 700"),
        (dict(latency_offsets=[2, 1] + list(range(3, 21)) ), "latency offsets are not increasing"),
        (dict(latency_offsets=list(range(1, 20)) + [22]), "latency offsets are not increasing"),
        (dict(latency_mean_ms=[40.0] * 19), "latency arrays do not match"),
        (dict(latency_mean_ms=[float("nan")] + [40.0] * 19), "not a finite number"),
        (dict(latency_mean_ms=[50.0] * 20), "mean latency exceeds its maximum"),
        (dict(latency_offsets=list(range(0, 20))), "no ingested span"),
        (dict(latency_offsets=list(range(1, 20)), latency_mean_ms=[40.0] * 19,
              latency_max_ms=[45.0] * 19), "missing for seconds"),
        (dict(end_epoch_s=4990), "before the first span end"),
        (dict(start_epoch_s="4999"), "not a whole number"),
    ])
    def test_an_inconsistent_series_is_not_measured_and_changes_nothing(self, changes, why):
        baseline = _assembled()
        result = _assembled(timed_series=timed_series_payload(**changes))
        leaf = result["metrics"]["throughput"]["timed_ended_per_second_series"]
        assert leaf["kind"] == "NOT_MEASURED" and why in leaf["reason"], leaf["reason"]
        assert result["verdict"] == baseline["verdict"] == "VALID"
        assert result["integrity"] == baseline["integrity"]
        assert result["stop"] == baseline["stop"] and result["claims"] == baseline["claims"]

    def test_the_required_metrics_are_identical_with_and_without_the_series(self, monkeypatch):
        evidence = dict(timed_series=timed_series_payload(), resource_samples=STATS)
        with_series = _assembled(**evidence)
        baseline = _baseline(monkeypatch, **evidence)
        stripped = json.loads(json.dumps(with_series))
        for section, name in NEW_LEAVES + TIMED_LEAVES + CPU_LEAVES:
            stripped["metrics"][section].pop(name, None)
        assert stripped == baseline
        assert with_series["verdict"] == baseline["verdict"] == "VALID"

    def test_the_builder_failing_is_isolated(self, monkeypatch):
        monkeypatch.setattr(RB, "_diag_timed_series",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        result = _assembled(timed_series=timed_series_payload())
        assert "diagnostic timed series not recorded: RuntimeError" in result["limitations"]
        assert "timed_ended_per_second_series" not in result["metrics"]["throughput"]
        assert result["verdict"] == "VALID" and result["integrity"]["ok"] is True
        M.validate_result(result)

    def test_older_results_without_any_new_leaf_still_validate(self):
        result = _assembled(timed_series=timed_series_payload(), resource_samples=STATS)
        older = json.loads(json.dumps(result))
        for section, name in NEW_LEAVES + TIMED_LEAVES + CPU_LEAVES:
            older["metrics"][section].pop(name, None)
        M.validate_result(older)
        assert older["metrics"]["resources"] == {
            "memory_peak_bytes": result["metrics"]["resources"]["memory_peak_bytes"]}


class TestTimedSeriesInTheCell:
    def test_one_post_run_query_after_the_measured_stage_and_clock_check(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path)
        assert clean and result["verdict"] == "VALID"
        assert smoke.calls == ORDER and smoke.calls.count("psql") == 3
        measured = smoke.calls.index("run_stage_measured")
        assert smoke.calls.index("psql") > measured
        assert smoke.calls[smoke.calls.index("psql") + 1] == "clock_probe"
        assert smoke.calls[smoke.calls.index("clock_probe", measured) + 1] == "psql"
        first, second, _gap = smoke.psql_sql
        assert "distinct_spans" in first and "generate_series" not in first
        assert "generate_series" in second and "distinct_spans" not in second
        cell = _cell()
        assert cell.measured_run_id in second and cell.warmup_run_id not in second
        assert first == RB.run_metrics_sql(cell.measured_run_id)
        assert second == RB.run_series_sql(cell.measured_run_id)

    def test_the_cell_records_the_series_next_to_the_required_metrics(self, tmp_path):
        result = _run(tmp_path)[0]
        series = result["metrics"]["throughput"]["timed_ended_per_second_series"]
        assert series["kind"] == "OBSERVATION" and sum(series["value"]) == 700
        assert result["metrics"]["throughput"]["persisted_per_second_series"]["n"] == 20
        for section, name in TIMED_LEAVES + CPU_LEAVES:
            assert name in result["metrics"][section]

    @pytest.mark.parametrize("how", ["raises", "not_json", "wrong_shape"])
    def test_an_optional_query_failure_never_changes_the_verdict(self, tmp_path, monkeypatch, how):
        (tmp_path / "good").mkdir()
        (tmp_path / "bad").mkdir()
        good = _run(tmp_path / "good")
        smoke = FakeSmoke(tmp_path / "bad")
        original = smoke.psql

        def psql(sql):
            if "generate_series" not in sql:
                return original(sql)
            smoke._step("psql")
            smoke.psql_sql.append(sql)
            if how == "raises":
                raise Boom("series query failed")
            return "not json {" if how == "not_json" else "[1, 2, 3]"

        smoke.psql = psql
        result, path, clean, _ = _run(tmp_path / "bad", smoke=smoke)
        assert clean and result["verdict"] == good[0]["verdict"] == "VALID"
        for key in ("integrity", "stop", "claims", "counts", "clock"):
            assert result[key] == good[0][key], key
        if how == "wrong_shape":
            leaf = result["metrics"]["throughput"]["timed_ended_per_second_series"]
            assert leaf["kind"] == "NOT_MEASURED"
        else:
            assert any(t.startswith("diagnostic timed series query not recorded")
                       for t in result["limitations"])
            assert result["metrics"]["throughput"]["timed_ended_per_second_series"][
                "kind"] == "NOT_MEASURED"
        assert result["metrics"]["latency"]["persistence_ms"] == good[0]["metrics"]["latency"][
            "persistence_ms"]
        M.validate_result(json.loads(path.read_text(encoding="utf-8")))

    def test_a_failure_of_the_required_query_still_fails_the_cell_as_before(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path, smoke=FakeSmoke(tmp_path, fail="psql"))
        assert result["verdict"] != "VALID"
        assert smoke.calls.count("psql") == 1          # the optional query never ran


class TestRetainedCpu:
    def test_cpu_is_kept_per_container_from_the_existing_samples(self):
        resources = _assembled(resource_samples=STATS)["metrics"]["resources"]
        assert resources["cpu_percent_max"]["kafka"]["value"] == 180.5
        assert resources["cpu_percent_max"]["pg"]["value"] == 12.25
        assert resources["cpu_percent_series"]["kafka"]["value"] == [5.0, 180.5]
        assert resources["cpu_percent_series"]["pg"]["value"] == [0.0, 12.25]   # a real 0.0 stays
        assert resources["cpu_percent_series_times_s"]["pg"]["value"] == [0.0, 14.2]
        for name in ("cpu_percent_max", "cpu_percent_series"):
            assert resources[name]["kafka"]["unit"] == "%" and resources[name]["kafka"]["n"] == 2
            assert "host processes" in resources[name]["kafka"]["method"]
        assert resources["memory_peak_bytes"]["kafka"]["value"] == 110      # unchanged

    def test_no_samples_is_not_measured(self):
        resources = _assembled()["metrics"]["resources"]
        for name in ("cpu_percent_max", "cpu_percent_series", "cpu_percent_series_times_s"):
            leaf = resources[name]["all"]
            assert leaf["kind"] == "NOT_MEASURED" and leaf["reason"] == "no resource samples"

    @pytest.mark.parametrize("bad", [None, "5.0%", "", True, False, float("nan"),
                                     float("inf"), -1.0, [5.0], {"x": 1}])
    def test_a_malformed_value_is_skipped_never_zero(self, bad):
        samples = [(0.0, [{"name": "kafka", "cpu_percent": 1.5, "memory_used_bytes": 1,
                           "memory_limit_bytes": 10}]),
                   (10.0, [{"name": "kafka", "cpu_percent": bad, "memory_used_bytes": 1,
                            "memory_limit_bytes": 10}]),
                   (20.0, [{"name": "kafka", "memory_used_bytes": 1,
                            "memory_limit_bytes": 10}])]
        resources = _assembled(resource_samples=samples)["metrics"]["resources"]
        assert resources["cpu_percent_series"]["kafka"]["value"] == [1.5]
        assert resources["cpu_percent_series_times_s"]["kafka"]["value"] == [0.0]
        assert resources["cpu_percent_max"]["kafka"]["n"] == 1

    def test_every_malformed_shape_is_skipped_by_the_builder(self):
        evidence = SimpleNamespace(resource_samples=[
            None, 5, (1.0,), ("late", []), (float("nan"), [{"name": "a", "cpu_percent": 1.0}]),
            (1.0, None), (1.0, "rows"), (2.0, [None, 3, "row", {"cpu_percent": 4.0},
                                                {"name": 7, "cpu_percent": 4.0},
                                                {"name": "ok", "cpu_percent": 7.5}]),
            (3.0, [{"name": "ok", "cpu_percent": 0}])])
        added = RB._diag_cpu(evidence)
        assert added[("resources", "cpu_percent_series")]["ok"]["value"] == [7.5, 0.0]
        assert added[("resources", "cpu_percent_series_times_s")]["ok"]["value"] == [2.0, 3.0]
        assert set(added[("resources", "cpu_percent_max")]) == {"ok"}
        none = RB._diag_cpu(SimpleNamespace(resource_samples=[(1.0, [{"name": "a"}])]))
        leaf = none[("resources", "cpu_percent_max")]["all"]
        assert leaf["kind"] == "NOT_MEASURED" and "no sample held a usable cpu_percent" in leaf[
            "reason"]

    def test_no_new_sampling_and_the_same_sampler_interval(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path)
        assert clean and smoke.calls.count("docker_stats") == 2     # as in ORDER, before B3J-68
        assert smoke.calls == ORDER
        assert LADDER["defaults"]["sample_interval_seconds"] == 10
        resources = result["metrics"]["resources"]
        assert resources["cpu_percent_max"]["proj-kafka-1"]["value"] == 5.0

    def test_cpu_values_never_reach_the_stop_rules_or_the_verdict(self, monkeypatch):
        seen = []
        real = M.resource_stops

        def spy(**kwargs):
            seen.append(sorted(kwargs))
            return real(**kwargs)

        monkeypatch.setattr(M, "resource_stops", spy)
        hot = [(at, [dict(row, cpu_percent=100000.0) for row in rows]) for at, rows in STATS]
        quiet = [(at, [dict(row, cpu_percent=0.0) for row in rows]) for at, rows in STATS]
        loud, calm = _assembled(resource_samples=hot), _assembled(resource_samples=quiet)
        assert loud["verdict"] == calm["verdict"] == "VALID"
        assert loud["stop"] == calm["stop"] and loud["integrity"] == calm["integrity"]
        assert seen and all("cpu_percent" not in names for names in seen)
        assert loud["metrics"]["resources"]["memory_peak_bytes"] == calm["metrics"]["resources"][
            "memory_peak_bytes"]


class TestB3J68DidNotChangeSafetyOrTheAllowlist:
    def test_gates_thresholds_and_the_allowlist_after_the_rep4_patch(self):
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        assert sorted(RB.APPROVED_LIVE_CELLS) == [
            "bm-small-r0010-c32-rep1", "bm-small-r0010-c32-rep2", "bm-small-r0010-c32-rep3",
            "bm-small-r0010-c32-rep4", "bm-smoke-r0005-c32-rep2", "bm-smoke-r0005-c32-rep3",
            "bm-smoke-r0005-c32-rep4", "bm-smoke-r0005-c32-rep5"]
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        assert "bm-small-r0010-c32-rep5" not in RB.APPROVED_LIVE_CELLS
        assert LADDER["stop_limits"]["lag_growth_samples"] == 6
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
        assert LADDER["stop_limits"]["memory_fraction"] == 0.90
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600


# ---------------------------------------------------------------------------
# B3J-71: the SMALL-10 rep3 (nothing is run)
# ---------------------------------------------------------------------------

SMALL10_REP3_ID = "bm-small-r0010-c32-rep3"
SMALL10_REP3_FIELDS = {**SMALL10_FIELDS, "repetition": 3}
SMALL10_TOKENS = {1: SMALL10_ID, 2: SMALL10_REP2_ID, 3: SMALL10_REP3_ID}
ALLOWLISTED_CELLS = [SMALL10_ID, SMALL10_REP2_ID, SMALL10_REP3_ID,
                     "bm-small-r0010-c32-rep4", "bm-smoke-r0005-c32-rep2",
                     "bm-smoke-r0005-c32-rep3", "bm-smoke-r0005-c32-rep4",
                     "bm-smoke-r0005-c32-rep5"]


class TestSmallTenThirdRepetition:
    def test_rep3_is_approved_with_the_exact_specification(self):
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_REP3_ID]) == SMALL10_REP3_FIELDS
        cell = _small(repetition=3)
        assert cell.benchmark_id == SMALL10_REP3_ID and RB.is_approved_live_cell(cell)
        assert (cell.level, cell.rate, cell.requests, cell.warmup_requests, cell.concurrency,
                cell.seed, cell.repetition) == ("SMALL", 10, 1000, 300, 32, 42, 3)
        assert cell.target()["scenario"] == "normal"
        assert cell.planned_spans == 7000 and cell.measured_seconds == 100.0
        assert cell.live_allowed is True and cell.refresh_and_detector is True
        assert (cell.warmup_run_id, cell.measured_run_id) == ("b-small-r0010-c32-w3",
                                                              "b-small-r0010-c32-m3")
        assert RB.RUN_ID_RE.fullmatch(cell.warmup_run_id)
        assert RB.RUN_ID_RE.fullmatch(cell.measured_run_id)
        assert _authorize_small(cell, authorize=SMALL10_REP3_ID, repetition=3) == cell

    def test_the_three_small_cells_differ_only_in_repetition_and_run_ids(self):
        targets = {n: _small(repetition=n).target() for n in (1, 2, 3)}
        for a, b in ((1, 2), (1, 3), (2, 3)):
            changed = {k for k in targets[a] if targets[a][k] != targets[b][k]}
            assert changed == {"repetition", "warmup_run_id", "measured_run_id"}, (a, b)
        ids = [f"{p}{n}" for n in (1, 2, 3) for p in ("w", "m")]
        found = [targets[n][key] for n in (1, 2, 3)
                 for key in ("warmup_run_id", "measured_run_id")]
        assert [i[-2:] for i in found] == ids and len(set(found)) == 6
        for n in (2, 3):
            assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_TOKENS[n]]) == {
                **dict(RB.APPROVED_LIVE_CELLS[SMALL10_ID]), "repetition": n}

    def test_exactly_eight_cells_and_the_earlier_cells_unchanged(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == ALLOWLISTED_CELLS
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_ID]) == SMALL10_FIELDS
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_REP2_ID]) == SMALL10_REP2_FIELDS
        for n in (2, 3, 4, 5):
            assert dict(RB.APPROVED_LIVE_CELLS[f"bm-smoke-r0005-c32-rep{n}"]) == {
                **SMOKE_REPEAT_FIELDS, "repetition": n}
        assert [k for k in RB.APPROVED_LIVE_CELLS if "small" in k] == [
            SMALL10_ID, SMALL10_REP2_ID, SMALL10_REP3_ID, "bm-small-r0010-c32-rep4"]

    @pytest.mark.parametrize("repetition", [5, 6, 7, 10])
    def test_rep5_and_later_stay_refused(self, repetition):
        cell = _small(repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r0010-c32-rep{repetition}",
                             repetition=repetition)
        for selector, token in SMALL10_TOKENS.items():
            with pytest.raises(RB.BenchmarkError):
                _authorize_small(cell, authorize=token, repetition=selector)

    @pytest.mark.parametrize("rate", [25, 50, 100])
    def test_other_small_rates_stay_refused_at_rep3(self, rate):
        cell = _small(rate=rate, repetition=3)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r{rate:04d}-c32-rep3", repetition=3)

    @pytest.mark.parametrize("claimed, actual", [(1, 2), (1, 3), (2, 1), (2, 3), (3, 1), (3, 2)])
    def test_the_three_authorization_tokens_cannot_be_interchanged(self, claimed, actual):
        cell = _small(repetition=actual)
        assert _authorize_small(cell, authorize=SMALL10_TOKENS[actual], repetition=actual) == cell
        with pytest.raises(RB.BenchmarkError, match="is not " + SMALL10_TOKENS[claimed]):
            _authorize_small(cell, authorize=SMALL10_TOKENS[claimed], repetition=claimed)
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(cell, authorize=SMALL10_TOKENS[actual], repetition=claimed)

    @pytest.mark.parametrize("warmup", [None, 0, 25, 299, 301])
    def test_rep3_needs_the_exact_warmup_argument(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 300"):
            _authorize_small(_small(repetition=3), authorize=SMALL10_REP3_ID, repetition=3,
                             warmup=warmup)

    @pytest.mark.parametrize("change", [dict(requests=999), dict(requests=1001), dict(rate=5),
                                        dict(rate=25), dict(concurrency=16),
                                        dict(concurrency=33), dict(warmup_requests=299),
                                        dict(level="SMOKE"), dict(level="MEDIUM"),
                                        dict(repetition=5), dict(repetition=6)])
    def test_a_changed_rep3_field_is_not_an_approved_cell(self, change):
        cell = dataclasses.replace(_small(repetition=3), **change)
        assert not RB.is_approved_live_cell(cell), change

    def test_the_run_ids_come_from_the_repetition_and_cannot_be_chosen(self, capsys):
        cell = _small(repetition=3)
        assert dataclasses.replace(cell, repetition=2).measured_run_id == "b-small-r0010-c32-m2"
        assert "w3" not in _small(repetition=2).warmup_run_id
        assert "m3" not in _small(repetition=1).measured_run_id
        with pytest.raises(SystemExit):
            RB.main(["--help"])
        flags = {word for word in capsys.readouterr().out.split() if word.startswith("--")}
        assert not flags & {"--seed", "--scenario", "--run-id", "--warmup-run-id",
                            "--measured-run-id"}

    def test_a_rep3_claim_is_isolated_and_single_use(self, tmp_path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        existing = {}
        for name in (f"{SMALL10_ID}.started", f"{SMALL10_ID}.done", f"{SMALL10_REP2_ID}.started",
                     f"{SMALL10_REP2_ID}.done"):
            (ledger / name).write_text(f"record {name}\n", encoding="utf-8", newline="")
            existing[name] = (ledger / name).read_bytes()
        started = RB.claim_run(tmp_path, _small(repetition=3), head=HEAD, arguments={})
        assert started == ledger / f"{SMALL10_REP3_ID}.started" and started.is_file()
        for name, content in existing.items():
            assert (ledger / name).read_bytes() == content, name
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            RB.claim_run(tmp_path, _small(repetition=2), head=HEAD, arguments={})
        (tmp_path / RB.OWNER_FILE).unlink()
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            RB.claim_run(tmp_path, _small(repetition=3), head=HEAD, arguments={})

    def test_the_cli_reaches_run_cell_once_for_an_authorized_rep3(self, monkeypatch, tmp_path):
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
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "3",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP3_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)], stdout=io.StringIO())
        assert code == 0 and calls == [(SMALL10_REP3_ID, HEAD)]
        assert (out / "ledger" / f"{SMALL10_REP3_ID}.started").is_file()
        assert (out / "ledger" / f"{SMALL10_REP3_ID}.done").is_file()
        assert not (out / RB.OWNER_FILE).exists()

    @pytest.mark.parametrize("argv_changes", [
        {"--rate": "25"}, {"--concurrency": "16"}, {"--warmup-requests": "299"},
        {"--repetition": "4"}, {"--repetition": "2"}])
    def test_wrong_arguments_are_refused_before_anything_starts(
            self, argv_changes, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", lambda *a, **k: pytest.fail("reached run_cell"))
        args = {"--level": "SMALL", "--rate": "10", "--repetition": "3", "--concurrency": "32",
                "--warmup-requests": "300", **argv_changes}
        out = tmp_path / "out"
        code = RB.main([*[x for pair in args.items() for x in pair], "--live", "--authorize",
                        SMALL10_REP3_ID, "--expect-head", HEAD, "--output-dir", str(out)])
        assert code == 2 and "nothing was started" in capsys.readouterr().err
        assert not out.exists()

    def test_the_gate_closed_refuses_rep3(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "3",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP3_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "not enabled" in capsys.readouterr().err and not out.exists()

    def test_the_b3j68_diagnostics_stay_valid_for_a_rep3_result(self):
        result = _assembled(cell=_small(repetition=3), lag_samples=RISING_LAG[:4],
                            sampler_started=5000.5, timed_series=timed_series_payload(),
                            resource_samples=STATS)
        assert result["benchmark_id"] == SMALL10_REP3_ID
        for section, name in NEW_LEAVES:
            assert name in result["metrics"][section], name
        assert result["metrics"]["throughput"]["timed_ended_per_second_series"][
            "kind"] == "OBSERVATION"
        M.validate_result(result)

    def test_the_cell_run_for_rep3_uses_its_own_run_ids_in_both_queries(self, tmp_path):
        cell = _small(repetition=3)
        smoke = FakeSmoke(tmp_path)
        _run(tmp_path, smoke=smoke, cell=cell)
        assert smoke.stage_ids == ["b-small-r0010-c32-w3", "b-small-r0010-c32-m3"]
        for sql in smoke.psql_sql:
            assert "b-small-r0010-c32-m3" in sql
            assert "-m2" not in sql and "-m1" not in sql and "-w3" not in sql

    def test_verdicts_gates_and_thresholds_are_unchanged(self):
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        assert LADDER["stop_limits"]["lag_growth_samples"] == 6
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
        assert LADDER["stop_limits"]["memory_fraction"] == 0.90
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600
        assert LADDER["defaults"]["drain_limit_seconds"] == 120
        assert dict(RB._SMALL10_FIRST) == {k: v for k, v in SMALL10_FIELDS.items()
                                           if k != "repetition"}


# ---------------------------------------------------------------------------
# B3J-84: gaps between consecutive ingests and per-partition progress (nothing is run)
# ---------------------------------------------------------------------------

GAP_LEAVES = (("latency", "ingest_gap_ms_p50"), ("latency", "ingest_gap_ms_p90"),
              ("latency", "ingest_gap_ms_p99"), ("latency", "ingest_gap_ms_max"),
              ("latency", "ingest_gap_stalls"),
              ("latency", "ingest_gap_series_start_epoch_s"),
              ("latency", "ingest_gap_ms_median_by_second"),
              ("latency", "ingest_gap_ms_max_by_second"),
              ("latency", "ingest_gap_stalls_by_second"),
              ("latency", "ingest_gap_series_offsets_s"),
              ("throughput", "partition_rows"), ("throughput", "partition_offset_min"),
              ("throughput", "partition_offset_max"), ("throughput", "partition_distinct_offsets"),
              ("throughput", "partition_ingested_per_second_series"))
GAP_OVERALL = GAP_LEAVES[:5]
GAP_SECONDS = GAP_LEAVES[5:10] + GAP_LEAVES[14:]
GAP_PARTITIONS = GAP_LEAVES[10:14]


def _gaps(**changes):
    return _assembled(timed_series=timed_series_payload(),
                      gap_series=gap_series_payload(**changes))["metrics"]


def _is_not_measured(metrics, spec):
    out = {}
    for section, name in spec:
        leaf = metrics[section][name]
        if "kind" not in leaf:                      # a tree: one leaf per partition, or "all"
            leaf = leaf["all"] if "all" in leaf else next(iter(leaf.values()))
        out[name] = (leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None, leaf["reason"])
    return out


class TestIngestGapQuery:
    def test_the_sql_is_read_only_single_and_scoped_to_the_run(self):
        import re
        sql = RB.run_gap_sql("b-small-r0010-c32-m3")
        words = set(re.findall(r"[a-z_]+", sql.lower()))
        assert sql.lstrip().upper().startswith("WITH") and ";" not in sql and "--" not in sql
        assert not words & {"insert", "update", "delete", "drop", "alter", "create", "truncate",
                            "grant", "revoke", "copy", "vacuum", "set"}
        assert sql.count("'b-small-r0010-c32-m3'") == 1
        assert "agentops.workload.run_id' = 'b-small-r0010-c32-m3'" in sql
        assert "parent_span_id IS NULL" in sql and "trace_id IN (SELECT trace_id FROM roots)" in sql
        assert "w3" not in sql and sql == RB.run_gap_sql("b-small-r0010-c32-m3")

    @pytest.mark.parametrize("run_id", ["", "x'; DROP TABLE telemetry_spans; --", "B-SMALL",
                                        "b-small' OR '1'='1", "b-small\n-m2", None])
    def test_a_bad_run_id_is_refused(self, run_id):
        with pytest.raises((RB.BenchmarkError, TypeError)):
            RB.run_gap_sql(run_id)

    def test_the_order_is_deterministic_and_the_first_row_has_no_gap(self):
        sql = RB.run_gap_sql("b-small-r0010-c32-m3")
        assert "lag(s.ingested_at) OVER (ORDER BY s.ingested_at, s.id)" in sql
        assert "ORDER BY ingested_at, id LIMIT 1" in sql        # which row is first
        assert "coalesce(gap" not in sql.lower() and "gap_ms IS NOT NULL" in sql
        assert "* 1000.0" in sql and "ingested_at - end_time" not in sql   # a gap, not persistence

    def test_the_window_partitions_and_stall_are_bounded_in_sql(self):
        sql = RB.run_gap_sql("b-small-r0010-c32-m3")
        assert str(RB.MAX_TIMED_SERIES_SECONDS) in sql and RB.MAX_TIMED_SERIES_SECONDS == 1800
        assert "CASE WHEN t1 >= t0 AND t1 - t0 <" in sql
        assert f"LIMIT {RB.MAX_GAP_PARTITIONS}" in sql and RB.MAX_GAP_PARTITIONS == 16
        assert f"gap_ms > {RB.STALL_GAP_MS}" in sql and RB.STALL_GAP_MS == 100.0

    def test_the_existing_queries_are_untouched(self):
        import hashlib
        assert hashlib.sha256(RB.run_metrics_sql("b-small-r0010-c32-m3").encode()).hexdigest() == (
            "900ec6a02e15d892a75c62f4f853173f7d72cc484ec5f8de16f727b3fec065ec")
        assert hashlib.sha256(RB.run_series_sql("b-small-r0010-c32-m3").encode()).hexdigest() == (
            "98d52bd80b409e2551804633d3571ea228a5380f015dfe3a4a767bbc1308885a")
        assert "lag(" not in RB.run_series_sql("b-small-r0010-c32-m3")


class TestIngestGapLeaves:
    def test_overall_gap_statistics_are_recorded_with_their_counts(self):
        latency = _gaps()["latency"]
        for name, value in (("p50", 14.0), ("p90", 20.0), ("p99", 28.0), ("max", 30.0)):
            leaf = latency[f"ingest_gap_ms_{name}"]
            assert leaf["value"] == value and leaf["unit"] == "ms" and leaf["n"] == 699
            assert leaf["kind"] == "DERIVED" and leaf["source"].endswith("(database clock)")
            assert "first span has no gap" in leaf["method"]
        stalls = latency["ingest_gap_stalls"]
        assert stalls["value"] == 0 and stalls["kind"] == "OBSERVATION" and stalls["n"] == 699
        assert "100.0 ms" in stalls["method"]

    def test_per_second_values_are_sparse_aligned_and_never_zero_filled(self):
        latency = _gaps()["latency"]
        start = latency["ingest_gap_series_start_epoch_s"]
        assert start["value"] == 4999 == timed_series_payload()["start_epoch_s"]
        offsets = latency["ingest_gap_series_offsets_s"]["value"]
        assert offsets == list(range(1, 21))
        for name in ("ingest_gap_ms_median_by_second", "ingest_gap_ms_max_by_second",
                     "ingest_gap_stalls_by_second"):
            assert latency[name]["n"] == len(offsets) == len(latency[name]["value"])
            assert latency[name]["window"]["clock"] == "host"
        assert latency["ingest_gap_ms_median_by_second"]["value"][0] == 14.0
        assert latency["ingest_gap_ms_max_by_second"]["value"] == [30.0] * 20
        assert "never 0" in latency["ingest_gap_ms_median_by_second"]["method"]

    def test_partitions_are_reported_by_counts_and_bounds(self):
        throughput = _gaps()["throughput"]
        assert sorted(throughput["partition_rows"]) == ["p0", "p1", "p2"]
        assert [throughput["partition_rows"][p]["value"] for p in ("p0", "p1", "p2")] == [
            240, 240, 220]
        assert throughput["partition_offset_min"]["p2"]["value"] == 0
        assert throughput["partition_offset_max"]["p2"]["value"] == 219
        assert throughput["partition_distinct_offsets"]["p1"]["value"] == 240
        per_second = throughput["partition_ingested_per_second_series"]
        assert all(len(per_second[p]["value"]) == 22 for p in per_second)
        assert [sum(per_second[p]["value"]) for p in ("p0", "p1", "p2")] == [240, 240, 220]
        assert throughput["partition_rows"]["p0"]["n"] == 240

    def test_offsets_are_never_assumed_contiguous(self):
        parts = gap_series_payload()["partitions"]
        parts[0] = {**parts[0], "distinct_offsets": 200, "max_offset": 299}   # holes are real
        throughput = _gaps(partitions=parts)["throughput"]
        assert throughput["partition_distinct_offsets"]["p0"]["value"] == 200
        assert throughput["partition_offset_max"]["p0"]["value"] == 299
        assert throughput["partition_rows"]["p0"]["value"] == 240
        too_many = [dict(parts[0], distinct_offsets=241, max_offset=239)] + parts[1:]
        broken = _gaps(partitions=too_many)["throughput"]["partition_rows"]
        assert broken["all"]["kind"] == "NOT_MEASURED"        # more distinct offsets than fit

    def test_stalls_are_counted_per_second_and_overall(self):
        payload = gap_series_payload(
            gap_p99_ms=250.0, gap_max_ms=900.0, stall_gaps=3,
            gap_max_by_second_ms=[900.0, 150.0] + [30.0] * 18,
            gap_stall_counts=[2, 1] + [0] * 18)
        latency = _assembled(timed_series=timed_series_payload(),
                             gap_series=payload)["metrics"]["latency"]
        assert latency["ingest_gap_stalls"]["value"] == 3
        assert latency["ingest_gap_stalls_by_second"]["value"][:2] == [2, 1]
        assert latency["ingest_gap_ms_max"]["value"] == 900.0

    def test_unavailable_diagnostics_are_not_measured_never_zero(self):
        for result in (_assembled(), _assembled(timed_series=timed_series_payload())):
            for section, name in GAP_LEAVES:
                leaf = result["metrics"][section][name]
                leaf = leaf["all"] if name.startswith("partition_") else leaf
                assert leaf["kind"] == "NOT_MEASURED" and leaf["value"] is None, name
                assert leaf["reason"] == "the optional ingest-gap query did not run or failed"

    def test_an_invalid_or_missing_clock_keeps_only_the_database_clock_values(self):
        for evidence, reason in (
                (dict(clock_before=[], clock_after=[]), "clock skew not measured"),
                (INVALID_CLOCK, "clock skew outside its limits")):
            metrics = _assembled(timed_series=timed_series_payload(),
                                 gap_series=gap_series_payload(), **evidence)["metrics"]
            for name, (missing, why) in _is_not_measured(metrics, GAP_SECONDS).items():
                assert missing and why.startswith(reason), (name, why)
            for name, (missing, why) in _is_not_measured(metrics, GAP_OVERALL).items():
                assert not missing, name                 # database timestamps only
            for name, (missing, why) in _is_not_measured(metrics, GAP_PARTITIONS).items():
                assert not missing, name
            assert metrics["latency"]["ingest_gap_ms_p99"]["value"] == 28.0

    def test_the_window_bound_loses_only_the_per_second_values(self):
        cap = RB.MAX_TIMED_SERIES_SECONDS
        payload = gap_series_payload(end_epoch_s=4999 + cap)            # cap + 1 seconds
        metrics = _assembled(timed_series=timed_series_payload(),
                             gap_series=payload)["metrics"]
        for name, (missing, why) in _is_not_measured(metrics, GAP_SECONDS).items():
            assert missing and "exceeds the bound of 1800 s" in why, (name, why)
        assert not any(m for m, _ in _is_not_measured(metrics, GAP_OVERALL).values())
        assert not any(m for m, _ in _is_not_measured(metrics, GAP_PARTITIONS).values())

    def test_a_series_exactly_at_the_bound_is_accepted(self):
        cap = RB.MAX_TIMED_SERIES_SECONDS
        ingested = [700] + [0] * (cap - 1)
        parts = [{"partition": 0, "rows": 700, "min_offset": 0, "max_offset": 699,
                  "distinct_offsets": 700, "per_second": [700] + [0] * (cap - 1)}]
        payload = gap_series_payload(
            seconds=cap, ingested=ingested, first_row_offset=0, gap_offsets=[0],
            gap_counts=[699], gap_median_ms=[1.0], gap_max_by_second_ms=[30.0],
            gap_stall_counts=[0], partitions=parts)
        metrics = _assembled(timed_series=timed_series_payload(),
                             gap_series=payload)["metrics"]
        assert len(metrics["throughput"]["partition_ingested_per_second_series"]["p0"]["value"]) == cap
        assert not any(m for m, _ in _is_not_measured(metrics, GAP_SECONDS).values())

    @pytest.mark.parametrize("changes, where, why", [
        (dict(gap_count=700), "overall", "700 gaps for 700 rows"),                  # first row as 0
        (dict(gap_count=698), "overall", "698 gaps for 700 rows"),
        (dict(rows=699), "overall", "699 rows, the required query 700"),
        (dict(rows=0), "overall", "no spans"),
        (dict(stall_ms=50.0), "overall", "not the documented one"),
        (dict(gap_p50_ms=float("nan")), "overall", "finite non-negative"),
        (dict(gap_p50_ms=-1.0), "overall", "finite non-negative"),
        (dict(gap_p90_ms=True), "overall", "finite non-negative"),
        (dict(gap_p50_ms=25.0), "overall", "not ordered"),
        (dict(stall_gaps=700), "overall", "outside 0..gap count"),
        (dict(gap_max_ms=None), "overall", "finite non-negative"),
        (dict(partitions=[]), "partitions", "no partition"),
        (dict(partitions=None), "partitions", "no partition"),
        (dict(partitions="x"), "partitions", "no partition"),
        (dict(first_row_offset=21), "seconds", "first ingested row is outside"),
        (dict(first_row_offset=5), "seconds", "does not match its ingested rows"),
        (dict(ingested=[35] * 22), "seconds", "counts 770 spans"),
        (dict(ingested=[35] * 21), "seconds", "one count per second"),
        (dict(gap_offsets=[1, 1] + list(range(3, 21))), "seconds", "not increasing"),
        (dict(gap_counts=[35] * 20), "seconds", "does not match its ingested rows"),
        (dict(gap_median_ms=[40.0] * 20), "seconds", "gap statistics are inconsistent"),
        (dict(gap_max_by_second_ms=[31.0] * 20), "seconds", "inconsistent"),
        (dict(stall_gaps=2, gap_max_ms=900.0, gap_max_by_second_ms=[900.0] + [30.0] * 19,
              gap_stall_counts=[1] + [0] * 19), "seconds", "do not add up to the totals"),
        (dict(gap_max_by_second_ms=[29.0] * 20), "seconds", "overall maximum"),
        (dict(gap_offsets=list(range(1, 20)), gap_counts=[34] + [35] * 18,
              gap_median_ms=[14.0] * 19, gap_max_by_second_ms=[30.0] * 19,
              gap_stall_counts=[0] * 19), "seconds", "do not add up"),
        (dict(gap_median_ms=[14.0] * 19), "seconds", "arrays do not match"),
    ])
    def test_inconsistent_results_are_not_measured_and_change_nothing(self, changes, where, why):
        baseline = _assembled()
        result = _assembled(timed_series=timed_series_payload(),
                            gap_series=gap_series_payload(**changes))
        groups = {"overall": GAP_OVERALL, "partitions": GAP_PARTITIONS, "seconds": GAP_SECONDS}
        reasons = _is_not_measured(result["metrics"], groups[where])
        assert all(missing for missing, _ in reasons.values()), reasons
        assert any(why in reason for _, reason in reasons.values()), (why, reasons)
        assert result["verdict"] == baseline["verdict"] == "VALID"
        assert result["integrity"] == baseline["integrity"]
        assert result["stop"] == baseline["stop"] and result["claims"] == baseline["claims"]

    def test_a_partition_that_does_not_add_up_is_caught_in_both_forms(self):
        parts = gap_series_payload()["partitions"]
        short = [dict(parts[0], rows=239, distinct_offsets=239, max_offset=238)] + parts[1:]
        metrics = _gaps(partitions=short)
        assert "do not add up to the rows" in _is_not_measured(metrics, GAP_PARTITIONS)[
            "partition_rows"][1]
        uneven = [dict(parts[0], per_second=[12] * 22)] + parts[1:]
        reasons = _is_not_measured(_gaps(partitions=uneven), GAP_SECONDS)
        assert all(missing for missing, _ in reasons.values())
        assert any("add up" in why for _, why in reasons.values())
        assert not _is_not_measured(_gaps(partitions=uneven), GAP_OVERALL)[
            "ingest_gap_ms_p50"][0]

    def test_a_single_row_has_no_gap(self):
        payload = gap_series_payload(
            rows=1, gap_count=0, ingested=[0, 1] + [0] * 20, first_row_offset=1, gap_offsets=[],
            gap_counts=[], gap_median_ms=[], gap_max_by_second_ms=[], gap_stall_counts=[],
            gap_p50_ms=None, gap_p90_ms=None, gap_p99_ms=None, gap_max_ms=None, stall_gaps=0,
            partitions=[dict(gap_series_payload()["partitions"][0], rows=1, min_offset=5,
                             max_offset=5, distinct_offsets=1, per_second=[0, 1] + [0] * 20)])
        result = _assembled(timed_series=None, gap_series=payload,
                            db_overrides={"rows": 1, "distinct_spans": 1, "roots": 100})
        leaf = result["metrics"]["latency"]["ingest_gap_ms_p50"]
        assert leaf["kind"] == "NOT_MEASURED" and "fewer than two rows" in leaf["reason"]

    def test_nothing_required_changes_with_or_without_the_gap_data(self, monkeypatch):
        evidence = dict(timed_series=timed_series_payload(), gap_series=gap_series_payload(),
                        resource_samples=STATS)
        with_gaps = _assembled(**evidence)
        baseline = _baseline(monkeypatch, **evidence)
        stripped = json.loads(json.dumps(with_gaps))
        for section, name in NEW_LEAVES:
            stripped["metrics"][section].pop(name, None)
        assert stripped == baseline
        assert with_gaps["verdict"] == baseline["verdict"] == "VALID"

    def test_a_failing_builder_is_isolated(self, monkeypatch):
        monkeypatch.setattr(RB, "_diag_ingest_gaps",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        result = _assembled(gap_series=gap_series_payload())
        assert "diagnostic ingest gaps not recorded: RuntimeError" in result["limitations"]
        assert "ingest_gap_ms_p50" not in result["metrics"]["latency"]
        assert result["verdict"] == "VALID" and result["integrity"]["ok"] is True
        M.validate_result(result)

    def test_older_results_without_the_gap_leaves_still_validate(self):
        result = _assembled(timed_series=timed_series_payload(), gap_series=gap_series_payload())
        older = json.loads(json.dumps(result))
        for section, name in GAP_LEAVES:
            older["metrics"][section].pop(name, None)
        M.validate_result(older)
        assert "timed_ended_per_second_series" in older["metrics"]["throughput"]
        M.validate_result(json.loads(json.dumps(result)))

    def test_a_full_length_result_stays_small_enough(self, tmp_path):
        """The densest result the diagnostics allow: every supported partition, the whole
        supported window, every second populated in the gap series and the timed series.
        Measured when it was written: 239,338 bytes compact (763,322 as the pretty-printed
        file).  The limit is that plus about 15 percent; the floor shows the result is dense."""
        cap, n = RB.MAX_TIMED_SERIES_SECONDS, RB.MAX_GAP_PARTITIONS
        rows = cap * n
        base = [round(10.0 + (i % 97) * 0.731, 3) for i in range(cap)]       # 3-decimal values
        highs = [round(v + 10.0, 3) for v in base]
        highs[0] = round(max(highs) + 1.0, 3)                                   # below 100 ms
        parts = [{"partition": p, "rows": cap, "min_offset": 1000 * p,
                  "max_offset": 1000 * p + cap - 1, "distinct_offsets": cap,
                  "per_second": [1] * cap} for p in range(n)]
        gap = gap_series_payload(
            seconds=cap, rows=rows, ingested=[n] * cap, first_row_offset=0, gap_count=rows - 1,
            gap_p50_ms=max(base), gap_p90_ms=max(base) + 1, gap_p99_ms=max(base) + 2,
            gap_max_ms=max(highs), gap_offsets=list(range(cap)),
            gap_counts=[n - 1] + [n] * (cap - 1), gap_median_ms=base, gap_max_by_second_ms=highs,
            gap_stall_counts=[0] * cap, partitions=parts)
        latency = [round(1000.0 + (i % 997) * 0.731, 3) for i in range(cap)]
        series = timed_series_payload(
            seconds=cap, rows=rows, ended=[n] * cap, ingested=[n] * cap,
            latency_offsets=list(range(cap)), latency_mean_ms=latency,
            latency_max_ms=[round(v + 100.123, 3) for v in latency])
        result = _assembled(timed_series=series, gap_series=gap, resource_samples=STATS,
                            lag_samples=RISING_LAG, db_overrides={
                                "rows": rows, "distinct_spans": rows, "roots": 100})
        for section, name in TIMED_LEAVES + GAP_LEAVES:       # every diagnostic is really there
            leaf = result["metrics"][section][name]
            leaf = next(iter(leaf.values())) if "kind" not in leaf else leaf
            assert leaf["kind"] != "NOT_MEASURED", (section, name, leaf.get("reason"))
        assert len(result["metrics"]["throughput"]["partition_ingested_per_second_series"]) == n
        size = len(json.dumps(result, sort_keys=True))
        assert 200_000 < size < 275_000, size
        path = RB.write_result(result, tmp_path / "out")
        assert path.stat().st_size < 880_000                  # the pretty-printed file
        M.validate_result(json.loads(path.read_text(encoding="utf-8")))


class TestStallAndEchoConsistency:
    @staticmethod
    def _consistent(maximum, stalls):
        """An overall maximum and stall count that agree, with one second holding the maximum."""
        return dict(gap_max_ms=maximum, gap_p99_ms=min(28.0, maximum), stall_gaps=stalls,
                    gap_max_by_second_ms=[maximum] + [min(30.0, maximum)] * 19,
                    gap_stall_counts=[stalls] + [0] * 19)

    @pytest.mark.parametrize("maximum, stalls", [(30.0, 0), (99.999, 0), (100.0, 0),
                                                 (100.001, 1), (900.0, 5)])
    def test_the_strict_threshold_accepts_matching_maximum_and_stalls(self, maximum, stalls):
        metrics = _gaps(**self._consistent(maximum, stalls))
        for name, (missing, why) in _is_not_measured(metrics, GAP_OVERALL).items():
            assert not missing, (name, why)
        for name, (missing, why) in _is_not_measured(metrics, GAP_SECONDS).items():
            assert not missing, (name, why)
        assert metrics["latency"]["ingest_gap_stalls"]["value"] == stalls

    @pytest.mark.parametrize("maximum, stalls", [(100.001, 0), (900.0, 0), (30.0, 1),
                                                 (99.999, 1), (100.0, 1)])
    def test_a_mismatch_between_the_maximum_and_the_stall_count_is_not_measured(
            self, maximum, stalls):
        baseline = _assembled()
        result = _assembled(timed_series=timed_series_payload(),
                            gap_series=gap_series_payload(**self._consistent(maximum, stalls)))
        reasons = _is_not_measured(result["metrics"], GAP_OVERALL)
        assert all(missing for missing, _ in reasons.values())
        assert all("stall count does not match the maximum gap against 100.0 ms" in why
                   for _, why in reasons.values()), reasons
        assert result["verdict"] == baseline["verdict"] == "VALID"
        assert result["integrity"] == baseline["integrity"]
        assert result["stop"] == baseline["stop"] and result["claims"] == baseline["claims"]

    @pytest.mark.parametrize("seconds_max, seconds_stalls", [
        ([900.0, 150.0] + [30.0] * 18, [1, 0] + [0] * 18),      # 150 ms second without a stall
        ([900.0, 30.0] + [30.0] * 18, [1, 1] + [0] * 18),       # a stall in a 30 ms second
        ([900.0, 100.0] + [30.0] * 18, [1, 1] + [0] * 18),      # exactly 100 ms is no stall
        ([900.0, 100.001] + [30.0] * 18, [1, 0] + [0] * 18),    # just over the threshold
        ([900.0, 99.999] + [30.0] * 18, [1, 1] + [0] * 18)])    # just under the threshold
    def test_a_second_whose_stalls_do_not_match_its_maximum_is_not_measured(
            self, seconds_max, seconds_stalls):
        overall_stalls = sum(seconds_stalls)
        result = _assembled(timed_series=timed_series_payload(), gap_series=gap_series_payload(
            gap_max_ms=900.0, gap_p99_ms=28.0, stall_gaps=overall_stalls,
            gap_max_by_second_ms=seconds_max, gap_stall_counts=seconds_stalls))
        reasons = _is_not_measured(result["metrics"], GAP_SECONDS)
        assert all(missing for missing, _ in reasons.values()), reasons
        assert all("a second's stall count does not match its maximum gap" in why
                   for _, why in reasons.values()), reasons
        assert not any(m for m, _ in _is_not_measured(result["metrics"], GAP_OVERALL).values())
        assert result["verdict"] == "VALID"

    @pytest.mark.parametrize("changes, why", [
        (dict(cap_seconds=5), "window bound is not this version's"),
        (dict(cap_seconds=RB.MAX_TIMED_SERIES_SECONDS + 1), "window bound is not this version's"),
        (dict(cap_seconds="1800"), "cap_seconds is not a whole number"),
        (dict(cap_seconds=None), "cap_seconds is not a whole number"),
        (dict(cap_seconds=True), "cap_seconds is not a whole number"),
        (dict(partition_limit=99), "partition limit is not this version's"),
        (dict(partition_limit=RB.MAX_GAP_PARTITIONS - 1), "partition limit is not this version's"),
        (dict(partition_limit=None), "partition_limit is not a whole number"),
        (dict(partition_limit=16.0), "partition_limit is not a whole number")])
    def test_a_mismatched_configuration_echo_makes_the_diagnostic_not_measured(self, changes, why):
        baseline = _assembled()
        result = _assembled(timed_series=timed_series_payload(),
                            gap_series=gap_series_payload(**changes))
        for group in (GAP_OVERALL, GAP_PARTITIONS, GAP_SECONDS):
            assert all(missing for missing, _ in _is_not_measured(result["metrics"], group).values())
        assert any(why in reason for _, reason in
                   _is_not_measured(result["metrics"], GAP_OVERALL).values())
        assert result["verdict"] == baseline["verdict"] == "VALID"
        assert result["integrity"] == baseline["integrity"]

    def test_a_payload_that_echoes_the_current_constants_is_accepted(self):
        payload = gap_series_payload()
        assert (payload["cap_seconds"], payload["partition_limit"]) == (
            RB.MAX_TIMED_SERIES_SECONDS, RB.MAX_GAP_PARTITIONS) == (1800, 16)
        assert not any(m for m, _ in _is_not_measured(_gaps(), GAP_OVERALL).values())


class TestIngestGapInTheCell:
    def test_one_more_post_run_query_after_the_series_and_clock_check(self, tmp_path):
        result, _, clean, smoke = _run(tmp_path)
        assert clean and result["verdict"] == "VALID"
        assert smoke.calls == ORDER and smoke.calls.count("psql") == 3
        measured = smoke.calls.index("run_stage_measured")
        probe = smoke.calls.index("clock_probe", measured)
        assert smoke.calls[probe + 1:probe + 3] == ["psql", "psql"]
        assert smoke.calls.index("psql") > measured and smoke.calls.index("psql") < probe
        required, series, gaps = smoke.psql_sql
        cell = _cell()
        assert required == RB.run_metrics_sql(cell.measured_run_id)
        assert series == RB.run_series_sql(cell.measured_run_id)
        assert gaps == RB.run_gap_sql(cell.measured_run_id)
        assert cell.warmup_run_id not in gaps and cell.measured_run_id in gaps

    def test_the_cell_records_the_gaps_next_to_the_required_metrics(self, tmp_path):
        result = _run(tmp_path)[0]
        latency = result["metrics"]["latency"]
        assert latency["ingest_gap_ms_p99"]["value"] == 28.0
        assert result["metrics"]["throughput"]["partition_rows"]["p0"]["value"] == 240
        assert latency["persistence_ms"]["p95"]["kind"] == "DERIVED"          # untouched
        assert result["metrics"]["throughput"]["persisted_per_second_series"]["n"] == 20

    @pytest.mark.parametrize("how", ["raises", "not_json", "wrong_shape"])
    def test_a_failure_of_only_the_gap_query_never_changes_the_verdict(
            self, tmp_path, how):
        (tmp_path / "good").mkdir()
        (tmp_path / "bad").mkdir()
        good = _run(tmp_path / "good")
        smoke = FakeSmoke(tmp_path / "bad")
        original = smoke.psql

        def psql(sql):
            if "gap_ms" not in sql:
                return original(sql)
            smoke._step("psql")
            smoke.psql_sql.append(sql)
            if how == "raises":
                raise Boom("gap query failed")
            return "not json {" if how == "not_json" else "[1, 2, 3]"

        smoke.psql = psql
        result, path, clean, _ = _run(tmp_path / "bad", smoke=smoke)
        assert clean and result["verdict"] == good[0]["verdict"] == "VALID"
        for key in ("integrity", "stop", "claims", "counts", "clock"):
            assert result[key] == good[0][key], key
        assert good[0]["metrics"]["throughput"]["timed_ended_per_second_series"][
            "kind"] == "OBSERVATION"
        assert result["metrics"]["throughput"]["timed_ended_per_second_series"] == good[0][
            "metrics"]["throughput"]["timed_ended_per_second_series"]       # the series survives
        leaf = result["metrics"]["latency"]["ingest_gap_ms_p50"]
        assert leaf["kind"] == "NOT_MEASURED"
        if how != "wrong_shape":
            assert any(t.startswith("diagnostic ingest gap query not recorded")
                       for t in result["limitations"])
        assert result["metrics"]["latency"]["persistence_ms"] == good[0]["metrics"]["latency"][
            "persistence_ms"]
        M.validate_result(json.loads(path.read_text(encoding="utf-8")))

    def test_a_failing_series_query_leaves_the_gap_query_alone(self, tmp_path):
        smoke = FakeSmoke(tmp_path)
        original = smoke.psql

        def psql(sql):
            if "gap_ms" not in sql and "generate_series" in sql:
                smoke._step("psql")
                smoke.psql_sql.append(sql)
                raise Boom("series query failed")
            return original(sql)

        smoke.psql = psql
        result, _, clean, _ = _run(tmp_path, smoke=smoke)
        assert clean and result["verdict"] == "VALID"
        assert result["metrics"]["latency"]["ingest_gap_ms_p99"]["value"] == 28.0
        assert result["metrics"]["throughput"]["timed_ended_per_second_series"][
            "kind"] == "NOT_MEASURED"

    def test_the_required_query_failing_still_fails_the_cell_before_any_optional_query(
            self, tmp_path):
        result, _, clean, smoke = _run(tmp_path, smoke=FakeSmoke(tmp_path, fail="psql"))
        assert result["verdict"] != "VALID" and smoke.calls.count("psql") == 1


class TestB3J84DidNotChangeSafetyOrTheAllowlist:
    def test_gates_thresholds_ladder_and_the_allowlist_are_unchanged(self):
        import hashlib
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        assert sorted(RB.APPROVED_LIVE_CELLS) == ALLOWLISTED_CELLS
        assert "bm-small-r0010-c32-rep5" not in RB.APPROVED_LIVE_CELLS
        assert hashlib.sha256((Path(_BENCH) / "ladder.json").read_bytes()).hexdigest() == (
            "571138bb0c10e15547345aca5f15422742ffb03d998e88127c2d7aadc8b2c71f")
        assert LADDER["stop_limits"]["lag_growth_samples"] == 6
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
        assert LADDER["defaults"]["sample_interval_seconds"] == 10
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600


# ---------------------------------------------------------------------------
# B3J-89: the SMALL-10 rep4 (nothing is run)
# ---------------------------------------------------------------------------

SMALL10_REP4_ID = "bm-small-r0010-c32-rep4"
SMALL10_REP4_FIELDS = {**SMALL10_FIELDS, "repetition": 4}
ALL_TOKENS = {**SMALL10_TOKENS, 4: SMALL10_REP4_ID}


class TestSmallTenFourthRepetition:
    def test_rep4_is_approved_with_the_exact_specification(self):
        assert dict(RB.APPROVED_LIVE_CELLS[SMALL10_REP4_ID]) == SMALL10_REP4_FIELDS
        cell = _small(repetition=4)
        assert cell.benchmark_id == SMALL10_REP4_ID and RB.is_approved_live_cell(cell)
        assert (cell.level, cell.rate, cell.requests, cell.warmup_requests, cell.concurrency,
                cell.seed, cell.repetition) == ("SMALL", 10, 1000, 300, 32, 42, 4)
        assert cell.target()["scenario"] == "normal"
        assert cell.planned_spans == 7000 and cell.measured_seconds == 100.0
        assert cell.live_allowed is True and cell.refresh_and_detector is True
        assert (cell.warmup_run_id, cell.measured_run_id) == ("b-small-r0010-c32-w4",
                                                              "b-small-r0010-c32-m4")
        assert RB.RUN_ID_RE.fullmatch(cell.warmup_run_id)
        assert RB.RUN_ID_RE.fullmatch(cell.measured_run_id)
        assert _authorize_small(cell, authorize=SMALL10_REP4_ID, repetition=4) == cell

    def test_exactly_eight_cells_and_the_existing_seven_unchanged_and_accepted(self):
        assert sorted(RB.APPROVED_LIVE_CELLS) == ALLOWLISTED_CELLS
        assert len(RB.APPROVED_LIVE_CELLS) == 8
        for n in (1, 2, 3, 4):
            assert dict(RB.APPROVED_LIVE_CELLS[ALL_TOKENS[n]]) == {
                **SMALL10_FIELDS, "repetition": n}
            cell = _small(repetition=n)
            assert RB.is_approved_live_cell(cell) and cell.benchmark_id == ALL_TOKENS[n]
        for n in (2, 3, 4, 5):
            assert dict(RB.APPROVED_LIVE_CELLS[f"bm-smoke-r0005-c32-rep{n}"]) == {
                **SMOKE_REPEAT_FIELDS, "repetition": n}
            assert RB.is_approved_live_cell(_approved_cells(repetition=n)[0])

    def test_the_four_small_cells_differ_only_in_repetition_and_run_ids(self):
        targets = {n: _small(repetition=n).target() for n in (1, 2, 3, 4)}
        for a in (1, 2, 3):
            for b in range(a + 1, 5):
                changed = {k for k in targets[a] if targets[a][k] != targets[b][k]}
                assert changed == {"repetition", "warmup_run_id", "measured_run_id"}, (a, b)
        ids = [targets[n][key] for n in (1, 2, 3, 4) for key in ("warmup_run_id", "measured_run_id")]
        assert len(set(ids)) == 8

    @pytest.mark.parametrize("change", [dict(rate=5), dict(rate=25), dict(rate=50), dict(rate=100),
                                        dict(repetition=1), dict(repetition=2), dict(repetition=3),
                                        dict(repetition=5), dict(concurrency=16),
                                        dict(concurrency=31), dict(concurrency=33),
                                        dict(concurrency=64)])
    def test_a_wrong_rate_repetition_or_concurrency_is_not_the_approved_cell(self, change):
        cell = dataclasses.replace(_small(repetition=4), **change)
        if change == dict(repetition=1) or change in (dict(repetition=2), dict(repetition=3)):
            assert cell.benchmark_id == ALL_TOKENS[change["repetition"]]   # another approved cell
            assert RB.is_approved_live_cell(cell)
            with pytest.raises(RB.BenchmarkError, match="is not " + SMALL10_REP4_ID):
                _authorize_small(cell, authorize=SMALL10_REP4_ID, repetition=4)
        else:
            assert not RB.is_approved_live_cell(cell), change

    @pytest.mark.parametrize("warmup", [None, 0, 25, 150, 299, 301, 1000])
    def test_a_wrong_warmup_count_is_refused(self, warmup):
        with pytest.raises(RB.BenchmarkError, match="warmup-requests 300"):
            _authorize_small(_small(repetition=4), authorize=SMALL10_REP4_ID, repetition=4,
                             warmup=warmup)
        if warmup:
            cell = dataclasses.replace(_small(repetition=4), warmup_requests=warmup)
            assert not RB.is_approved_live_cell(cell)

    @pytest.mark.parametrize("requests", [1, 100, 999, 1001, 5000])
    def test_a_wrong_measured_request_count_is_refused(self, requests):
        cell = dataclasses.replace(_small(repetition=4), requests=requests)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="differs from the approved one in requests"):
            _authorize_small(cell, authorize=SMALL10_REP4_ID, repetition=4)

    def test_the_scenario_is_fixed_and_the_seed_comes_from_the_pinned_ladder(self):
        import hashlib
        ladder_path = RB.DEFAULT_CONFIG
        assert hashlib.sha256(Path(ladder_path).read_bytes()).hexdigest() == (
            "571138bb0c10e15547345aca5f15422742ffb03d998e88127c2d7aadc8b2c71f")
        loaded = RB.load_ladder(ladder_path)
        assert loaded["defaults"]["seed"] == 42 and loaded["defaults"]["scenario"] == "normal"
        cell = RB.plan_cells(loaded, "SMALL", rates=[10], repetition=4, concurrency=32,
                             warmup_requests=300)[0]
        assert cell.seed == 42 and cell.target()["scenario"] == "normal"
        bad = json.loads(json.dumps(loaded))
        bad["defaults"]["scenario"] = "slow"
        with pytest.raises(M.MetricError, match="normal scenario only"):
            M.check_ladder(bad)                      # another scenario cannot even be planned
        # What this does not do: the allowlist compares level, requests, rate, concurrency,
        # repetition and warm-up.  The seed is held by the ladder file, whose hash is checked
        # before a live run (see the B3J-72 review).

    @pytest.mark.parametrize("repetition", [5, 6, 7, 10])
    def test_rep5_and_later_stay_refused(self, repetition):
        cell = _small(repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r0010-c32-rep{repetition}",
                             repetition=repetition)
        for selector, token in ALL_TOKENS.items():
            with pytest.raises(RB.BenchmarkError):
                _authorize_small(cell, authorize=token, repetition=selector)

    @pytest.mark.parametrize("rate", [25, 50, 100])
    @pytest.mark.parametrize("repetition", [1, 4, 5])
    def test_other_small_rates_stay_refused(self, rate, repetition):
        cell = _small(rate=rate, repetition=repetition)
        assert not RB.is_approved_live_cell(cell)
        with pytest.raises(RB.BenchmarkError, match="authorize"):
            _authorize_small(cell, authorize=f"bm-small-r{rate:04d}-c32-rep{repetition}",
                             repetition=repetition)

    @pytest.mark.parametrize("claimed", [1, 2, 3])
    def test_rep4_and_the_earlier_authorization_tokens_cannot_be_interchanged(self, claimed):
        rep4, other = _small(repetition=4), _small(repetition=claimed)
        for cell, own in ((rep4, 4), (other, claimed)):
            assert _authorize_small(cell, authorize=ALL_TOKENS[own], repetition=own) == cell
        with pytest.raises(RB.BenchmarkError, match="is not " + ALL_TOKENS[claimed]):
            _authorize_small(rep4, authorize=ALL_TOKENS[claimed], repetition=claimed)
        with pytest.raises(RB.BenchmarkError, match="is not " + SMALL10_REP4_ID):
            _authorize_small(other, authorize=SMALL10_REP4_ID, repetition=4)
        with pytest.raises(RB.BenchmarkError, match="repetition"):
            _authorize_small(rep4, authorize=SMALL10_REP4_ID, repetition=claimed)

    def test_a_rep4_claim_is_isolated_and_single_use(self, tmp_path):
        ledger = tmp_path / "ledger"
        ledger.mkdir()
        existing = {}
        for n in (1, 2, 3):
            for suffix in (".started", ".done"):
                path = ledger / f"{ALL_TOKENS[n]}{suffix}"
                path.write_text(f"record {path.name}\n", encoding="utf-8", newline="")
                existing[path] = path.read_bytes()
        started = RB.claim_run(tmp_path, _small(repetition=4), head=HEAD, arguments={})
        assert started == ledger / f"{SMALL10_REP4_ID}.started" and started.is_file()
        for path, content in existing.items():
            assert path.read_bytes() == content, path.name
        with pytest.raises(RB.BenchmarkError, match="owns this output directory"):
            RB.claim_run(tmp_path, _small(repetition=3), head=HEAD, arguments={})
        (tmp_path / RB.OWNER_FILE).unlink()
        with pytest.raises(RB.BenchmarkError, match="was started before"):
            RB.claim_run(tmp_path, _small(repetition=4), head=HEAD, arguments={})

    def test_the_cli_reaches_run_cell_once_for_an_authorized_rep4(self, monkeypatch, tmp_path):
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
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "4",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP4_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)], stdout=io.StringIO())
        assert code == 0 and calls == [(SMALL10_REP4_ID, HEAD)]
        assert (out / "ledger" / f"{SMALL10_REP4_ID}.started").is_file()
        assert (out / "ledger" / f"{SMALL10_REP4_ID}.done").is_file()
        assert not (out / RB.OWNER_FILE).exists()

    @pytest.mark.parametrize("argv_changes", [
        {"--rate": "25"}, {"--concurrency": "16"}, {"--warmup-requests": "299"},
        {"--repetition": "5"}, {"--repetition": "3"}])
    def test_wrong_arguments_are_refused_before_anything_starts(
            self, argv_changes, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "run_cell", lambda *a, **k: pytest.fail("reached run_cell"))
        args = {"--level": "SMALL", "--rate": "10", "--repetition": "4", "--concurrency": "32",
                "--warmup-requests": "300", **argv_changes}
        out = tmp_path / "out"
        code = RB.main([*[x for pair in args.items() for x in pair], "--live", "--authorize",
                        SMALL10_REP4_ID, "--expect-head", HEAD, "--output-dir", str(out)])
        assert code == 2 and "nothing was started" in capsys.readouterr().err
        assert not out.exists()

    def test_the_gate_closed_refuses_rep4(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        out = tmp_path / "out"
        code = RB.main(["--level", "SMALL", "--rate", "10", "--repetition", "4",
                        "--concurrency", "32", "--warmup-requests", "300", "--live",
                        "--authorize", SMALL10_REP4_ID, "--expect-head", HEAD,
                        "--output-dir", str(out)])
        assert code == 2 and "not enabled" in capsys.readouterr().err and not out.exists()

    def test_the_cell_run_for_rep4_uses_its_own_run_ids_in_all_three_queries(self, tmp_path):
        smoke = FakeSmoke(tmp_path)
        _run(tmp_path, smoke=smoke, cell=_small(repetition=4))
        assert smoke.stage_ids == ["b-small-r0010-c32-w4", "b-small-r0010-c32-m4"]
        assert len(smoke.psql_sql) == 3
        for sql in smoke.psql_sql:
            assert "b-small-r0010-c32-m4" in sql
            assert not any(other in sql for other in ("-m1", "-m2", "-m3", "-w4"))

    def test_the_diagnostic_sql_gates_and_thresholds_are_unchanged(self):
        import hashlib
        digest = lambda text: hashlib.sha256(text.encode()).hexdigest()
        run_id = "b-small-r0010-c32-m4"
        assert digest(RB.run_metrics_sql(run_id)) == (
            "c883fac812a62628e6411d88dbd0de2062e4171d744512ea18384d75fdcbe736")
        assert digest(RB.run_series_sql(run_id)) == (
            "79251ad7f0c2aa7d74e50653797334efef83a3461d5699dabd1f5e9aa6d2bad2")
        assert digest(RB.run_gap_sql(run_id)) == (
            "b5af5c8dc43b1dfed6c2c7c1b20ccb4b8b44ff50d136a3851fea5bc0999de3c1")
        assert RB.LIVE_ENABLED is True and RB.MAX_WARMUP_REQUESTS == 10000
        assert RB.MAX_TIMED_SERIES_SECONDS == 1800 and RB.STALL_GAP_MS == 100.0
        assert LADDER["stop_limits"]["lag_growth_samples"] == 6
        assert LADDER["stop_limits"]["late_start_fraction"] == 0.05
        assert LADDER["stop_limits"]["memory_fraction"] == 0.90
        assert LADDER["defaults"]["sample_interval_seconds"] == 10
        assert LADDER["defaults"]["cell_budget_seconds"] == 3600
        assert LADDER["defaults"]["drain_limit_seconds"] == 120

    def test_a_rep4_result_still_validates_with_all_diagnostics(self):
        result = _assembled(cell=_small(repetition=4), lag_samples=RISING_LAG[:4],
                            sampler_started=5000.5, timed_series=timed_series_payload(),
                            gap_series=gap_series_payload(), resource_samples=STATS)
        assert result["benchmark_id"] == SMALL10_REP4_ID
        for section, name in NEW_LEAVES:
            assert name in result["metrics"][section], name
        M.validate_result(result)


# ---------------------------------------------------------------------------
# B3J-103: configuration integrity, marker binding, single use, validation order
# ---------------------------------------------------------------------------

import copy  # noqa: E402
import hashlib  # noqa: E402
import io  # noqa: E402
import threading  # noqa: E402

LADDER_BYTES = (_BENCH / "ladder.json").read_bytes()
APPROVED_ARGS = [*APPROVED, "--live", "--authorize", "bm-smoke-r0005-c32-rep2",
                 "--expect-head", HEAD]
SMOKE2 = "bm-smoke-r0005-c32-rep2"
OTHER_OWNER = "another run owns this"
MUTATIONS = {
    "seed": lambda d: d["defaults"].__setitem__("seed", 7),
    "detector off": lambda d: d["levels"][0].__setitem__("refresh_and_detector", False),
    "drain": lambda d: d["defaults"].__setitem__("drain_limit_seconds", 5),
    "settle": lambda d: d["defaults"].__setitem__("settle_seconds", 1),
    "budget": lambda d: d["defaults"].__setitem__("cell_budget_seconds", 99999),
    "sample interval": lambda d: d["defaults"].__setitem__("sample_interval_seconds", 1),
    "memory stop": lambda d: d["stop_limits"].__setitem__("memory_fraction", 1.0),
    "disk stop": lambda d: d["stop_limits"].__setitem__("disk_free_min_gb", 0.001),
    "lag stop": lambda d: d["stop_limits"].__setitem__("lag_growth_samples", 1000),
}


def _altered(name):
    data = copy.deepcopy(LADDER)
    MUTATIONS[name](data)
    M.check_ladder(data)                      # structurally valid: only the pin can refuse it
    return data


@pytest.fixture
def live(monkeypatch, tmp_path):
    """The CLI with the gate open in memory, the real pre-claim checks and the real claim,
    run_cell replaced, a canonical output directory inside tmp_path, a fake read-only git."""
    canonical = tmp_path / "canonical"
    git = {"head": HEAD, "branch": "work", "status": [], "staged": []}
    seen = SimpleNamespace(runs=[], canonical=canonical, git=git, started_during_run=None)
    monkeypatch.setattr(RB, "LIVE_ENABLED", True)
    monkeypatch.setattr(RB, "DEFAULT_OUTPUT", canonical)
    monkeypatch.setattr(RB, "check_live_output_dir", REAL_CHECK_LIVE_OUTPUT_DIR)
    monkeypatch.setattr(RB, "verify_repository_readonly",
                        lambda expect_head, **kw: REAL_VERIFY_REPOSITORY(
                            expect_head, read=lambda: dict(git)))

    def fake_run_cell(cell, ladder, **kw):
        seen.runs.append(cell.benchmark_id)
        started = kw["output_dir"] / "ledger" / f"{cell.benchmark_id}.started"
        seen.started_during_run = json.loads(started.read_text(encoding="utf-8"))
        path = kw["output_dir"] / f"{cell.benchmark_id}-fake.json"
        path.write_text("{}", encoding="utf-8")
        return {"verdict": "VALID"}, path, True

    monkeypatch.setattr(RB, "run_cell", fake_run_cell)
    return seen


def _live(args=None, *extra):
    return RB.main([*(APPROVED_ARGS if args is None else args), *extra], stdout=io.StringIO())


def _nothing_created(seen):
    c = seen.canonical
    return seen.runs == [] and (not c.exists() or not any(c.rglob("*")))


class TestB1ConfigurationIntegrity:
    def test_the_pin_is_the_real_ladders_hash(self):
        assert RB.APPROVED_LADDER_SHA256 == hashlib.sha256(LADDER_BYTES).hexdigest() == (
            "571138bb0c10e15547345aca5f15422742ffb03d998e88127c2d7aadc8b2c71f")
        data, digest = RB.read_ladder(RB.DEFAULT_CONFIG)
        assert digest == RB.APPROVED_LADDER_SHA256 and data == LADDER

    def test_the_hash_and_the_parse_come_from_the_same_bytes(self, tmp_path, monkeypatch):
        altered = json.dumps(_altered("seed")).encode("utf-8")
        path = tmp_path / "x.json"
        path.write_bytes(LADDER_BYTES)
        reads = []

        def once(self):
            reads.append(self)
            return altered

        monkeypatch.setattr(Path, "read_bytes", once)
        data, digest = RB.read_ladder(path)
        assert reads == [path]                                   # read once
        assert digest == hashlib.sha256(altered).hexdigest() and data["defaults"]["seed"] == 7

    @pytest.mark.parametrize("name", sorted(MUTATIONS))
    def test_every_altered_value_is_refused_by_the_effective_check(self, name):
        altered = _altered(name)
        cells = RB.plan_cells(altered, "SMOKE", rates=[5], repetition=2, concurrency=32,
                              warmup_requests=25)
        with pytest.raises(RB.BenchmarkError, match="effective configuration differs"):
            RB.check_effective_config(cells[0], altered)
        assert RB.check_effective_config(_approved_cells()[0], LADDER)["seed"] == 42

    @pytest.mark.parametrize("name", sorted(MUTATIONS))
    def test_an_altered_configuration_is_refused_before_anything_is_created(
            self, live, tmp_path, name, capsys):
        config = tmp_path / "alt.json"
        config.write_text(json.dumps(_altered(name), indent=2), encoding="utf-8")
        assert _live(None, "--config", str(config)) == 2
        assert "nothing was started" in capsys.readouterr().err and _nothing_created(live)

    def test_an_identical_content_configuration_at_another_path_is_accepted(self, live, tmp_path):
        config = tmp_path / "elsewhere" / "copy-of-the-ladder.json"
        config.parent.mkdir()
        config.write_bytes(LADDER_BYTES)
        assert _live(None, "--config", str(config)) == 0 and live.runs == [SMOKE2]

    def test_a_different_content_but_valid_configuration_is_refused_live_only(
            self, live, tmp_path, capsys):
        config = tmp_path / "reformatted.json"
        config.write_text(json.dumps(LADDER, indent=4), encoding="utf-8")   # same values
        assert _live(None, "--config", str(config)) == 2
        assert "SHA-256" in capsys.readouterr().err and _nothing_created(live)
        out = io.StringIO()                       # planning still takes any valid configuration
        assert RB.main(["--level", "SMOKE", "--config", str(config), "--output-dir",
                        str(tmp_path / "plan")], stdout=out) == 0
        assert out.getvalue().startswith("PLAN ONLY")
        assert not (tmp_path / "plan").exists()

    def test_run_cell_checks_the_effective_values_too(self, monkeypatch, tmp_path):
        monkeypatch.setattr(RB, "LIVE_ENABLED", True)
        monkeypatch.setattr(RB, "execute_cell", lambda *a, **k: pytest.fail("reached"))
        altered = _altered("seed")
        cell = RB.plan_cells(altered, "SMOKE", rates=[5], repetition=2, concurrency=32,
                             warmup_requests=25)[0]
        assert RB.is_approved_live_cell(cell)                    # the old check alone passed it
        with pytest.raises(RB.BenchmarkError, match="effective configuration differs"):
            RB.run_cell(cell, altered, expect_head=HEAD, output_dir=tmp_path)


class TestB3CanonicalOutputDirectory:
    def test_another_directory_is_refused_for_live(self, live, tmp_path, capsys):
        other = tmp_path / "somewhere-else"
        assert _live(None, "--output-dir", str(other)) == 2
        assert "canonical output directory" in capsys.readouterr().err
        assert not other.exists() and _nothing_created(live)

    def test_the_canonical_directory_is_accepted(self, live):
        assert _live(None, "--output-dir", str(live.canonical)) == 0
        assert live.runs == [SMOKE2]

    def test_a_used_cell_cannot_be_rerun_by_naming_another_directory(self, live, tmp_path):
        assert _live(None, "--output-dir", str(live.canonical)) == 0
        assert _live(None, "--output-dir", str(tmp_path / "fresh")) == 2
        assert _live(None, "--output-dir", str(live.canonical)) == 2
        assert live.runs == [SMOKE2]

    @pytest.mark.parametrize("make", ["started", "done", "result", "owner"])
    def test_existing_evidence_blocks_reuse_and_is_left_alone(self, live, make, capsys):
        c = live.canonical
        (c / "ledger").mkdir(parents=True)
        paths = {"started": c / "ledger" / f"{SMOKE2}.started",
                 "done": c / "ledger" / f"{SMOKE2}.done",
                 "result": c / f"{SMOKE2}-20261011T000000.json", "owner": c / RB.OWNER_FILE}
        paths[make].write_text("historical", encoding="utf-8")
        before = sorted(str(p.relative_to(c)) for p in c.rglob("*"))
        assert _live() == 2 and live.runs == []
        assert sorted(str(p.relative_to(c)) for p in c.rglob("*")) == before
        assert paths[make].read_text(encoding="utf-8") == "historical"
        assert "existing records block" in capsys.readouterr().err


class TestB4ValidationBeforeClaim:
    @pytest.mark.parametrize("change", [
        dict(head="9" * 40), dict(status=[" M benchmarks/run_benchmark.py"]),
        dict(status=["?? stray.txt"]), dict(staged=["a.py"]), dict(branch="HEAD")])
    def test_wrong_head_dirty_tree_or_staged_change_creates_nothing(
            self, live, change, capsys):
        live.git.update(change)
        assert _live() == 2 and "nothing was started" in capsys.readouterr().err
        assert _nothing_created(live)

    def test_the_order_of_the_checks_and_the_claim(self, live, monkeypatch):
        order = []
        for name in ("check_live_authorization", "check_effective_config",
                     "check_live_output_dir", "check_no_prior_evidence",
                     "verify_repository_readonly", "claim_run", "run_cell"):
            real = getattr(RB, name)

            def wrapped(*a, _n=name, _r=real, **k):
                order.append(_n)
                return _r(*a, **k)

            monkeypatch.setattr(RB, name, wrapped)
        assert _live() == 0
        assert order == ["check_live_authorization", "check_effective_config",
                         "check_live_output_dir", "check_no_prior_evidence",
                         "verify_repository_readonly", "claim_run", "run_cell"]

    def test_a_failure_before_the_claim_leaves_no_state_whatever_it_is(self, live, tmp_path):
        for argv in (["--authorize", "bm-smoke-r0005-c32-rep3"],
                     ["--output-dir", str(tmp_path / "x")]):
            assert _live(None, *argv) == 2
        live.git["head"] = "1" * 40
        assert _live() == 2 and _nothing_created(live)


def _good_binding():
    cell = _approved_cells()[0]
    return cell, {"branch": "work", "ladder_sha256": RB.APPROVED_LADDER_SHA256,
                  "config_path": "c.json", "effective_config": RB.effective_config(cell, LADDER)}


class TestB2MarkerBinding:
    def test_started_records_the_verified_configuration_and_run_identity(self, live):
        assert _live() == 0
        record = live.started_during_run
        cell = _approved_cells()[0]
        assert record["schema"] == 2 and record["benchmark_id"] == SMOKE2
        assert record["verified_head"] == record["git_head"] == HEAD
        assert record["branch"] == "work" and record["ladder_sha256"] == RB.APPROVED_LADDER_SHA256
        assert record["output_dir"] == str(live.canonical.resolve())
        assert record["config_path"] == str(RB.DEFAULT_CONFIG.resolve())
        effective = record["effective_config"]
        assert effective["seed"] == 42 and effective["scenario"] == "normal"
        assert effective["refresh_and_detector"] is True and effective["live_allowed"] is True
        assert (effective["drain_limit_seconds"], effective["settle_seconds"],
                effective["cell_budget_seconds"]) == (120, 6, 3600)
        assert effective["stop_limits"] == LADDER["stop_limits"]
        assert effective["target"] == cell.target()
        assert effective["target"]["warmup_run_id"] == "b-smoke-r0005-c32-w2"
        assert effective["target"]["measured_run_id"] == "b-smoke-r0005-c32-m2"
        assert RB.validate_marker(record, cell, live.canonical) == []
        assert record["arguments"]["authorize"] == SMOKE2 and record["arguments"]["live"] is True

    @pytest.mark.parametrize("field,value", [
        ("benchmark_id", "bm-smoke-r0005-c32-rep3"), ("verified_head", "7" * 40),
        ("git_head", "7" * 40), ("branch", "HEAD"), ("ladder_sha256", "0" * 64),
        ("output_dir", "/elsewhere"), ("nonce", ""), ("schema", 1),
        ("effective_config", {}), ("unexpected", 1)])
    def test_inconsistent_fields_are_reported(self, tmp_path, field, value):
        cell, binding = _good_binding()
        RB.claim_run(tmp_path, cell, head=HEAD, arguments={}, binding=binding)
        record = json.loads((tmp_path / "ledger" / f"{SMOKE2}.started").read_text())
        assert RB.validate_marker(record, cell, tmp_path) == []
        record[field] = value
        assert RB.validate_marker(record, cell, tmp_path)

    def test_a_changed_effective_value_or_run_id_is_reported(self, tmp_path):
        cell, binding = _good_binding()
        RB.claim_run(tmp_path, cell, head=HEAD, arguments={}, binding=binding)
        record = json.loads((tmp_path / "ledger" / f"{SMOKE2}.started").read_text())
        for edit in (lambda r: r["effective_config"].__setitem__("seed", 7),
                     lambda r: r["effective_config"]["stop_limits"].__setitem__(
                         "memory_fraction", 1.0),
                     lambda r: r["effective_config"].__setitem__("refresh_and_detector", False),
                     lambda r: r["effective_config"]["target"].__setitem__(
                         "measured_run_id", "b-other-m2")):
            broken = copy.deepcopy(record)
            edit(broken)
            assert RB.validate_marker(broken, cell, tmp_path)

    @pytest.mark.parametrize("bad", ["ladder", "seed", "head"])
    def test_claim_run_refuses_an_inconsistent_binding_and_creates_nothing(self, tmp_path, bad):
        cell, binding = _good_binding()
        head = HEAD
        if bad == "ladder":
            binding["ladder_sha256"] = "0" * 64
        elif bad == "seed":
            binding["effective_config"]["seed"] = 7
        else:
            head = "short"
        with pytest.raises(RB.BenchmarkError, match="inconsistent"):
            RB.claim_run(tmp_path, cell, head=head, arguments={}, binding=binding)
        assert not any(tmp_path.rglob("*"))

    def test_a_stored_record_that_differs_from_the_written_one_is_kept_for_review(
            self, tmp_path, monkeypatch):
        cell, binding = _good_binding()
        real = RB._exclusive_json

        def corrupting(path, payload):
            if path.name.endswith(".started"):
                payload = {**payload, "arguments": {"tampered": True}}
            return real(path, payload)

        monkeypatch.setattr(RB, "_exclusive_json", corrupting)
        # the in-memory record and the stored one differ: refused, nothing deleted
        with pytest.raises(RB.BenchmarkError, match="kept for review"):
            RB.claim_run(tmp_path, cell, head=HEAD, arguments={}, binding=binding)
        assert (tmp_path / "ledger" / f"{SMOKE2}.started").is_file()
        assert (tmp_path / RB.OWNER_FILE).is_file()

    def test_the_historical_format_is_unchanged(self, tmp_path):
        cell = _approved_cells()[0]
        started = RB.claim_run(tmp_path, cell, head=HEAD, arguments={"repetition": 2})
        record = json.loads(started.read_text(encoding="utf-8"))
        assert set(record) == {"benchmark_id", "git_head", "pid", "started_utc", "arguments"}
        assert RB.validate_marker(record, cell, tmp_path)        # not a schema-2 record


class TestAtomicClaim:
    def test_concurrent_claims_for_the_same_cell_cannot_both_succeed(self, tmp_path):
        cell, binding = _good_binding()
        gate, results = threading.Barrier(8), []

        def attempt():
            gate.wait()
            try:
                results.append(("ok", RB.claim_run(tmp_path, cell, head=HEAD, arguments={},
                                                    binding=binding)))
            except RB.BenchmarkError as exc:
                results.append(("refused", str(exc)))

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert [r[0] for r in results].count("ok") == 1 and len(results) == 8
        assert sorted(p.name for p in (tmp_path / "ledger").iterdir()) == [f"{SMOKE2}.started"]
        assert (tmp_path / RB.OWNER_FILE).is_file()

    def test_concurrent_claims_for_different_cells_in_one_directory_serialize(self, tmp_path):
        gate, results = threading.Barrier(2), []
        cells = [_approved_cells(repetition=n)[0] for n in (2, 3)]

        def attempt(cell):
            gate.wait()
            try:
                RB.claim_run(tmp_path, cell, head=HEAD, arguments={})
                results.append("ok")
            except RB.BenchmarkError:
                results.append("refused")

        threads = [threading.Thread(target=attempt, args=(c,)) for c in cells]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(results) == ["ok", "refused"]
        assert len(list((tmp_path / "ledger").iterdir())) == 1

    def test_a_failure_while_writing_the_record_leaves_no_partial_started(
            self, tmp_path, monkeypatch):
        cell, binding = _good_binding()
        real_fsync, calls = os.fsync, []

        def failing_fsync(fd):
            calls.append(fd)
            if len(calls) == 2:                     # the owner file was fine; the record fails
                raise OSError("disk")
            return real_fsync(fd)

        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(RB.BenchmarkError, match="cannot create"):
            RB.claim_run(tmp_path, cell, head=HEAD, arguments={}, binding=binding)
        assert not (tmp_path / "ledger" / f"{SMOKE2}.started").exists()
        assert not (tmp_path / RB.OWNER_FILE).exists()           # its own ownership released
        assert [p.name for p in (tmp_path / "ledger").iterdir()] == []   # no temporary left

    def test_another_owners_claim_is_never_removed(self, tmp_path, monkeypatch):
        cell, binding = _good_binding()
        real = RB._exclusive_json

        def raced(path, payload):
            if path.name.endswith(".started"):
                (tmp_path / RB.OWNER_FILE).write_text(OTHER_OWNER, encoding="utf-8")
                raise OSError("disk")
            return real(path, payload)

        monkeypatch.setattr(RB, "_exclusive_json", raced)
        with pytest.raises(RB.BenchmarkError, match="cannot create"):
            RB.claim_run(tmp_path, cell, head=HEAD, arguments={}, binding=binding)
        assert (tmp_path / RB.OWNER_FILE).read_text(encoding="utf-8") == OTHER_OWNER
        assert not (tmp_path / "ledger" / f"{SMOKE2}.started").exists()

    def test_an_existing_record_is_never_replaced(self, tmp_path):
        target = tmp_path / "ledger" / "x.started"
        RB._exclusive_json(target, {"a": 1})
        with pytest.raises(FileExistsError):
            RB._exclusive_json(target, {"a": 2})
        assert json.loads(target.read_text(encoding="utf-8")) == {"a": 1}
        assert [p.name for p in target.parent.iterdir()] == ["x.started"]


# ---------------------------------------------------------------------------
# B3J-106: the real Git-state reader and the canonical output directory
# ---------------------------------------------------------------------------
#
# The autouse fail_closed fixture makes subprocess.run refuse everything.  The Git tests
# below put the REAL subprocess.run back, for read-only git commands against a temporary
# repository only (RB.REPO is pointed at it); nothing here touches the AgentOps repository.

import shutil  # noqa: E402

# subprocess.run is replaced by autouse fixtures only inside tests; at import time it is real.
REAL_SUBPROCESS_RUN = subprocess.run
REAL_POPEN = subprocess.Popen          # subprocess.run calls Popen

needs_git = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")


def _git_in(repo, *args):
    result = REAL_SUBPROCESS_RUN(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t",
         "-c", "core.autocrlf=false", "-c", "commit.gpgsign=false", *args],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)
    assert result.returncode == 0, (args, result.stderr)
    return result.stdout.strip()


@pytest.fixture
def temp_repo(tmp_path, monkeypatch):
    """A temporary repository with one commit on branch `work`; the real subprocess.run is
    restored and RB.REPO points at it.  Returns (repo, head)."""
    monkeypatch.setattr(subprocess, "Popen", REAL_POPEN)
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_in(repo, "init", "-q", "-b", "work")
    (repo / "tracked.txt").write_text("one\n", encoding="utf-8", newline="\n")
    _git_in(repo, "add", "-A")
    _git_in(repo, "commit", "-q", "-m", "base")
    monkeypatch.setattr(subprocess, "run", REAL_SUBPROCESS_RUN)
    monkeypatch.setattr(RB, "REPO", repo)
    return repo, _git_in(repo, "rev-parse", "HEAD")


def _damage(repo, kind):
    if kind == "dirty tracked file":
        (repo / "tracked.txt").write_text("changed\n", encoding="utf-8", newline="\n")
    elif kind == "staged change":
        (repo / "staged.txt").write_text("x\n", encoding="utf-8", newline="\n")
        _git_in(repo, "add", "staged.txt")
    elif kind == "untracked file":
        (repo / "stray.txt").write_text("x\n", encoding="utf-8", newline="\n")
    elif kind == "detached head":
        _git_in(repo, "checkout", "-q", "--detach")
    elif kind == "staged deletion":
        _git_in(repo, "rm", "-q", "tracked.txt")
    else:
        raise AssertionError(kind)


DAMAGE = ["dirty tracked file", "staged change", "untracked file", "detached head",
          "staged deletion"]


@needs_git
class TestRealGitStateReader:
    def test_a_clean_repository_is_read_correctly(self, temp_repo):
        repo, head = temp_repo
        state = RB.read_git_state()
        assert state == {"head": head, "branch": "work", "status": [], "staged": []}
        assert RB.read_git_state(repo) == state
        assert REAL_VERIFY_REPOSITORY(head) == {"head": head, "branch": "work"}

    def test_a_dirty_tracked_file_is_reported(self, temp_repo):
        _damage(temp_repo[0], "dirty tracked file")
        state = RB.read_git_state()
        assert state["status"] == [" M tracked.txt"] and state["staged"] == []

    def test_a_staged_change_is_reported_in_both_lists(self, temp_repo):
        _damage(temp_repo[0], "staged change")
        state = RB.read_git_state()
        assert state["staged"] == ["staged.txt"] and state["status"] == ["A  staged.txt"]

    def test_an_untracked_file_is_reported(self, temp_repo):
        _damage(temp_repo[0], "untracked file")
        state = RB.read_git_state()
        assert state["status"] == ["?? stray.txt"] and state["staged"] == []

    def test_a_detached_head_reads_as_head_not_a_branch(self, temp_repo):
        repo, head = temp_repo
        _damage(repo, "detached head")
        state = RB.read_git_state()
        assert state["branch"] == "HEAD" and state["head"] == head

    def test_a_nested_untracked_file_is_listed_individually(self, temp_repo):
        repo, _ = temp_repo
        (repo / "dir").mkdir()
        (repo / "dir" / "a.txt").write_text("x\n", encoding="utf-8")
        assert RB.read_git_state()["status"] == ["?? dir/a.txt"]    # --untracked-files=all

    @pytest.mark.parametrize("kind", DAMAGE)
    def test_verify_rejects_every_invalid_state(self, temp_repo, kind):
        repo, head = temp_repo
        _damage(repo, kind)
        with pytest.raises(RB.BenchmarkError):
            REAL_VERIFY_REPOSITORY(head)

    def test_verify_rejects_a_wrong_expected_head(self, temp_repo):
        with pytest.raises(RB.BenchmarkError, match="not the expected commit"):
            REAL_VERIFY_REPOSITORY("1" * 40)

    def test_reading_never_changes_the_repository(self, temp_repo):
        repo, _ = temp_repo
        _damage(repo, "untracked file")
        _damage(repo, "dirty tracked file")
        before = (sorted(p.relative_to(repo).as_posix() for p in repo.rglob("*")
                         if ".git" not in p.parts), _git_in(repo, "status", "--porcelain"),
                  (repo / ".git" / "index").read_bytes())
        for _ in range(3):
            RB.read_git_state()
        after = (sorted(p.relative_to(repo).as_posix() for p in repo.rglob("*")
                        if ".git" not in p.parts), _git_in(repo, "status", "--porcelain"),
                 (repo / ".git" / "index").read_bytes())
        assert before == after
        assert not (repo / ".git" / "index.lock").exists()

    def test_a_repository_without_commits_fails_closed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(subprocess, "Popen", REAL_POPEN)
        empty = tmp_path / "empty"
        empty.mkdir()
        _git_in(empty, "init", "-q", "-b", "work")
        monkeypatch.setattr(subprocess, "run", REAL_SUBPROCESS_RUN)
        with pytest.raises(RB.BenchmarkError, match=r"git rev-parse: exit \d+"):
            RB.read_git_state(empty)

    def test_a_directory_that_is_not_a_repository_fails_closed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(subprocess, "run", REAL_SUBPROCESS_RUN)
        monkeypatch.setattr(subprocess, "Popen", REAL_POPEN)
        plain = tmp_path / "plain"
        plain.mkdir()
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
        with pytest.raises(RB.BenchmarkError, match=r"git rev-parse: exit \d+"):
            RB.read_git_state(plain)


class TestGitCommandFailures:
    """The reader's handling of a failing or odd git, with controlled subprocess results."""

    @staticmethod
    def _script(monkeypatch, results):
        issued = []

        def fake(command, **kwargs):
            issued.append(list(command))
            step = results[len(issued) - 1]
            if isinstance(step, BaseException):
                raise step
            code, out = step
            return subprocess.CompletedProcess(command, code, out, "")

        monkeypatch.setattr(subprocess, "run", fake)
        return issued

    GOOD = [(0, "a" * 40 + "\n"), (0, "work\n"), (0, ""), (0, "")]

    def test_the_four_commands_are_the_read_only_ones(self, monkeypatch):
        issued = self._script(monkeypatch, self.GOOD)
        assert RB.read_git_state(Path("somewhere")) == {
            "head": "a" * 40, "branch": "work", "status": [], "staged": []}
        assert [c[c.index("-C") + 2:] for c in issued] == [
            ["rev-parse", "HEAD"], ["rev-parse", "--abbrev-ref", "HEAD"],
            ["status", "--porcelain", "--untracked-files=all"],
            ["diff", "--cached", "--name-only"]]
        assert all(c[:3] == ["git", "--no-optional-locks", "-C"] for c in issued)

    @pytest.mark.parametrize("failing", [0, 1, 2, 3])
    def test_a_nonzero_exit_of_any_command_fails_closed(self, monkeypatch, failing):
        results = list(self.GOOD)
        results[failing] = (128, "")
        self._script(monkeypatch, results)
        with pytest.raises(RB.BenchmarkError, match="exit 128"):
            RB.read_git_state(Path("somewhere"))

    @pytest.mark.parametrize("error", [FileNotFoundError("git"), PermissionError("x"),
                                       subprocess.TimeoutExpired("git", 60)])
    def test_a_missing_git_or_a_timeout_fails_closed(self, monkeypatch, error):
        self._script(monkeypatch, [error])
        with pytest.raises(RB.BenchmarkError, match="git rev-parse"):
            RB.read_git_state(Path("somewhere"))

    @pytest.mark.parametrize("results", [
        [(0, ""), (0, "work\n"), (0, ""), (0, "")],                    # empty HEAD
        [(0, "garbage\n"), (0, "work\n"), (0, ""), (0, "")],           # not a commit id
        [(0, "a" * 40 + "\n"), (0, ""), (0, ""), (0, "")],             # no branch name
        [(0, "a" * 40 + "\n"), (0, "work\n"), (0, "\n\n  \n"), (0, "\n")],  # blank lines only
    ])
    def test_malformed_output_never_verifies_a_state(self, monkeypatch, results):
        self._script(monkeypatch, results)
        state = RB.read_git_state(Path("somewhere"))
        if state["head"] == "a" * 40 and state["branch"] == "work":
            assert state["status"] == [] and state["staged"] == []     # blank lines are ignored
            return
        with pytest.raises(RB.BenchmarkError):
            REAL_VERIFY_REPOSITORY("a" * 40, read=lambda: state)

    def test_output_without_a_proper_head_is_refused_even_when_expected(self, monkeypatch):
        self._script(monkeypatch, [(0, "garbage\n"), (0, "work\n"), (0, ""), (0, "")])
        state = RB.read_git_state(Path("somewhere"))
        assert state["head"] == "garbage"
        with pytest.raises(RB.BenchmarkError):                         # expect-head is validated
            RB.check_live_authorization(_approved_cells(), authorize=SMOKE2,
                                        expect_head=state["head"], warmup_requests=25,
                                        repetition=2)


@needs_git
class TestInvalidGitStateCreatesNoClaim:
    """main with the REAL verify_repository_readonly and the REAL read_git_state on a
    temporary repository: an invalid state is refused before any claim exists."""

    @pytest.fixture
    def real_git(self, live, temp_repo, monkeypatch):
        repo, head = temp_repo
        monkeypatch.setattr(RB, "verify_repository_readonly", REAL_VERIFY_REPOSITORY)
        args = [*APPROVED, "--live", "--authorize", SMOKE2, "--expect-head", head]
        live.repo, live.head, live.args = repo, head, args
        return live

    def test_the_stubs_are_not_in_force(self, real_git):
        assert RB.verify_repository_readonly is REAL_VERIFY_REPOSITORY
        assert RB.check_live_output_dir is REAL_CHECK_LIVE_OUTPUT_DIR
        assert RB.read_git_state.__module__ == "run_benchmark"

    def test_a_valid_state_is_claimed_with_the_real_head_and_branch(self, real_git):
        assert _live(real_git.args) == 0 and real_git.runs == [SMOKE2]
        record = real_git.started_during_run
        assert record["verified_head"] == real_git.head and record["branch"] == "work"

    @pytest.mark.parametrize("kind", DAMAGE)
    def test_every_invalid_state_is_refused_with_nothing_created(self, real_git, kind, capsys):
        _damage(real_git.repo, kind)
        assert _live(real_git.args) == 2
        assert "nothing was started" in capsys.readouterr().err
        assert _nothing_created(real_git)

    def test_a_wrong_expected_head_is_refused_with_nothing_created(self, real_git):
        assert _live(real_git.args[:-1] + ["2" * 40]) == 2 and _nothing_created(real_git)


@pytest.fixture
def canonical_dir(tmp_path, monkeypatch):
    canonical = tmp_path / "canonical"
    monkeypatch.setattr(RB, "DEFAULT_OUTPUT", canonical)
    monkeypatch.setattr(RB, "check_live_output_dir", REAL_CHECK_LIVE_OUTPUT_DIR)
    return canonical


class TestCanonicalOutputDirectory:
    def test_the_exact_path_is_accepted_whether_or_not_it_exists(self, canonical_dir):
        assert RB.check_live_output_dir(canonical_dir) == canonical_dir.resolve()
        assert not canonical_dir.exists()                      # checking creates nothing
        canonical_dir.mkdir()
        assert RB.check_live_output_dir(canonical_dir) == canonical_dir.resolve()

    @pytest.mark.parametrize("spelling", [
        lambda c: c / "sub" / "..",
        lambda c: c.parent / "elsewhere" / ".." / c.name,
        lambda c: Path(str(c) + os.sep),
        lambda c: Path(str(c) + os.sep + "." + os.sep),
        lambda c: c.parent / "." / c.name,
    ], ids=["dotdot-below", "dotdot-sibling", "trailing-sep", "trailing-dot", "dot-segment"])
    def test_equivalent_spellings_resolve_to_the_canonical_location(self, canonical_dir, spelling):
        canonical_dir.mkdir()
        assert RB.check_live_output_dir(spelling(canonical_dir)) == canonical_dir.resolve()

    @pytest.mark.skipif(os.name != "nt", reason="case-insensitive paths are a Windows rule")
    def test_case_differences_are_the_same_directory_on_windows(self, canonical_dir):
        canonical_dir.mkdir()
        shouted = canonical_dir.parent / canonical_dir.name.upper()
        assert RB.check_live_output_dir(shouted) == shouted.resolve()
        assert os.path.normcase(str(shouted.resolve())) == os.path.normcase(
            str(canonical_dir.resolve()))

    @pytest.mark.skipif(os.name == "nt", reason="case-sensitive paths are a POSIX rule")
    def test_case_differences_are_another_directory_on_posix(self, canonical_dir):
        with pytest.raises(RB.BenchmarkError, match="canonical output directory"):
            RB.check_live_output_dir(canonical_dir.parent / canonical_dir.name.upper())

    @pytest.mark.parametrize("other", [
        lambda c: c.parent / "another",
        lambda c: c.parent / (c.name + "2"),                   # shares the prefix
        lambda c: c / "child",                                 # inside the canonical one
        lambda c: c.parent,                                    # its parent
        lambda c: c.parent / "x" / c.name,                     # same name, another place
    ], ids=["sibling", "prefix", "child", "parent", "same-name-elsewhere"])
    def test_a_different_directory_is_rejected(self, canonical_dir, other):
        canonical_dir.mkdir()
        with pytest.raises(RB.BenchmarkError, match="canonical output directory"):
            RB.check_live_output_dir(other(canonical_dir))

    def test_a_symlink_alias_is_the_canonical_directory(self, canonical_dir, tmp_path):
        canonical_dir.mkdir()
        alias = tmp_path / "alias"
        try:
            os.symlink(canonical_dir, alias, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("symbolic links are not available to this user")
        assert RB.check_live_output_dir(alias) == canonical_dir.resolve()
        with pytest.raises(RB.BenchmarkError):                 # ".." leaves the link's parent
            RB.check_live_output_dir(alias / "..")

    @pytest.mark.skipif(os.name != "nt", reason="junctions are a Windows feature")
    def test_a_junction_alias_is_the_canonical_directory(self, canonical_dir, tmp_path,
                                                         monkeypatch):
        monkeypatch.setattr(subprocess, "Popen", REAL_POPEN)
        canonical_dir.mkdir()
        junction = tmp_path / "junction"
        made = REAL_SUBPROCESS_RUN(["cmd", "/c", "mklink", "/J", str(junction), str(canonical_dir)],
                                   capture_output=True, text=True, stdin=subprocess.DEVNULL,
                                   timeout=30)
        if made.returncode != 0 or not junction.exists():
            pytest.skip("a junction could not be created here")
        assert RB.check_live_output_dir(junction) == canonical_dir.resolve()

    def test_an_alias_to_a_different_directory_is_rejected(self, canonical_dir, tmp_path):
        other = tmp_path / "other"
        other.mkdir()
        alias = tmp_path / "alias-to-other"
        try:
            os.symlink(other, alias, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("symbolic links are not available to this user")
        with pytest.raises(RB.BenchmarkError, match="canonical output directory"):
            RB.check_live_output_dir(alias)

    def test_the_default_is_a_directory_outside_the_repository(self):
        default = RB.DEFAULT_OUTPUT.resolve()
        assert default.name == "agentops-benchmarks" and _REPO not in default.parents

    def test_the_real_check_is_in_force_in_the_live_fixture(self, live):
        assert RB.check_live_output_dir is REAL_CHECK_LIVE_OUTPUT_DIR
        assert RB.verify_repository_readonly is not REAL_VERIFY_REPOSITORY   # injected reader

    def test_an_equivalent_spelling_shares_one_ledger_through_main(self, live, tmp_path):
        spelled = live.canonical / "sub" / ".."
        assert _live(None, "--output-dir", str(spelled)) == 0 and live.runs == [SMOKE2]
        assert _live(None, "--output-dir", str(live.canonical)) == 2          # same ledger
        assert _live(None, "--output-dir", str(Path(str(live.canonical) + os.sep))) == 2
        assert live.runs == [SMOKE2]
        assert sorted(p.name for p in (live.canonical / "ledger").iterdir()) == [
            f"{SMOKE2}.done", f"{SMOKE2}.started"]
        assert not (tmp_path / "sub").exists()

    def test_every_other_test_still_runs_with_the_stubs(self, monkeypatch):
        # outside the live fixture, the autouse stubs are in force (documented, not hidden)
        assert RB.check_live_output_dir is not REAL_CHECK_LIVE_OUTPUT_DIR
        assert RB.verify_repository_readonly is not REAL_VERIFY_REPOSITORY
