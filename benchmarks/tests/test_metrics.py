"""
benchmarks/tests/test_metrics.py

Offline tests of the benchmark arithmetic, rules, result schema and the
orchestrator's offline paths.  Nothing here starts a process, opens a
socket, touches Docker or a database, or sends telemetry; several tests
prove that.
"""

from __future__ import annotations

import copy
import importlib.abc
import importlib.util
import io
import json
import math
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

_BENCH = Path(__file__).resolve().parents[1]
_REPO = _BENCH.parent
if str(_BENCH) not in sys.path:
    sys.path.insert(0, str(_BENCH))

import metrics as M  # noqa: E402
import run_benchmark as RB  # noqa: E402

LADDER = json.loads((_BENCH / "ladder.json").read_text(encoding="utf-8"))
STOPS = LADDER["stop_limits"]


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


def _metric(**overrides):
    base = {"value": 1.0, "unit": "ms", "kind": "MEASURED", "source": "s", "method": "m",
            "window": None, "n": 1, "reason": None}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Percentiles
# ---------------------------------------------------------------------------

class TestPercentile:
    def test_empty_sample_has_no_percentile(self):
        with pytest.raises(M.MetricError, match="empty"):
            M.percentile([], 50)
        with pytest.raises(M.MetricError):
            M.percentiles([])

    @pytest.mark.parametrize("q", [0, 50, 95, 99, 100])
    def test_single_value_is_every_percentile(self, q):
        assert M.percentile([7.5], q) == 7.5

    def test_linear_interpolation(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert M.percentile(values, 50) == pytest.approx(5.5)
        assert M.percentile(values, 95) == pytest.approx(9.55)
        assert M.percentile(values, 99) == pytest.approx(9.91)
        assert M.percentile(values, 0) == 1 and M.percentile(values, 100) == 10

    def test_order_does_not_matter(self):
        assert M.percentiles([5, 1, 4, 2, 3]) == M.percentiles([1, 2, 3, 4, 5])

    def test_two_values(self):
        assert M.percentile([10, 20], 50) == 15

    def test_matches_known_nearest_rank_boundaries(self):
        values = list(range(101))
        assert M.percentiles(values) == {"p50": 50.0, "p95": 95.0, "p99": 99.0}

    @pytest.mark.parametrize("q", [-0.1, 100.1, float("nan"), float("inf")])
    def test_q_out_of_range(self, q):
        with pytest.raises(M.MetricError):
            M.percentile([1, 2], q)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), "3", None, True])
    def test_non_finite_or_non_numeric_values_rejected(self, bad):
        with pytest.raises(M.MetricError):
            M.percentile([1.0, bad], 50)


# ---------------------------------------------------------------------------
# Rates
# ---------------------------------------------------------------------------

class TestRate:
    def test_basic(self):
        assert M.rate(700, 10.0) == 70.0
        assert M.rate(0, 5) == 0.0

    @pytest.mark.parametrize("seconds", [0, 0.0, -1, float("nan"), float("inf")])
    def test_zero_or_bad_duration_rejected(self, seconds):
        with pytest.raises(M.MetricError):
            M.rate(10, seconds)

    @pytest.mark.parametrize("count", [-1, 1.5, True, None])
    def test_bad_count_rejected(self, count):
        with pytest.raises(M.MetricError):
            M.rate(count, 1.0)


# ---------------------------------------------------------------------------
# Metrics and NOT_MEASURED
# ---------------------------------------------------------------------------

class TestMetric:
    def test_kinds_are_exactly_four(self):
        assert M.KINDS == ("MEASURED", "DERIVED", "OBSERVATION", "NOT_MEASURED")

    def test_not_measured_has_reason_and_no_value(self):
        metric = M.not_measured("ms", "src", "because").to_dict()
        assert metric["value"] is None and metric["n"] is None and metric["reason"] == "because"
        M.check_metric(metric, "x")

    def test_not_measured_never_zero(self):
        with pytest.raises(M.MetricError, match="no value"):
            M.check_metric(_metric(kind="NOT_MEASURED", value=0, n=None, reason="r"), "x")

    def test_not_measured_needs_reason(self):
        with pytest.raises(M.MetricError, match="reason"):
            M.check_metric(_metric(kind="NOT_MEASURED", value=None, n=None, reason=" "), "x")

    def test_measured_needs_value(self):
        with pytest.raises(M.MetricError, match="NOT_MEASURED instead"):
            M.check_metric(_metric(value=None), "x")

    def test_measured_value_zero_is_allowed(self):
        M.check_metric(_metric(value=0, n=5), "x")

    def test_only_not_measured_has_reason(self):
        with pytest.raises(M.MetricError, match="reason"):
            M.check_metric(_metric(reason="why"), "x")

    @pytest.mark.parametrize("kind", ["measured", "ESTIMATED", "", None])
    def test_unsupported_kind_rejected(self, kind):
        with pytest.raises(M.MetricError, match="kind"):
            M.check_metric(_metric(kind=kind), "x")

    def test_missing_or_extra_keys_rejected(self):
        bad = _metric()
        del bad["n"]
        with pytest.raises(M.MetricError, match="keys"):
            M.check_metric(bad, "x")
        with pytest.raises(M.MetricError, match="keys"):
            M.check_metric({**_metric(), "extra": 1}, "x")

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), "1", [], [1, "x"]])
    def test_bad_values_rejected(self, value):
        with pytest.raises(M.MetricError):
            M.check_metric(_metric(value=value), "x")

    def test_bad_window_rejected(self):
        with pytest.raises(M.MetricError, match="window"):
            M.check_metric(_metric(window={"start": "a"}), "x")
        M.check_metric(_metric(window={"start": "a", "end": "b", "clock": "database"}), "x")

    def test_negative_n_rejected(self):
        with pytest.raises(M.MetricError):
            M.check_metric(_metric(n=-1), "x")

    def test_latency_metrics_empty_is_not_measured(self):
        found = M.latency_metrics([], unit="ms", kind="MEASURED", source="s", method="m",
                                  window=None, reason_if_empty="nothing")
        assert sorted(found) == ["p50", "p95", "p99"]
        assert all(m.kind == "NOT_MEASURED" and m.value is None for m in found.values())

    def test_latency_metrics_values(self):
        found = M.latency_metrics([1, 2, 3], unit="ms", kind="MEASURED", source="s",
                                  method="m", window=None, reason_if_empty="x")
        assert found["p50"].value == 2 and found["p50"].n == 3


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def _rec(**overrides):
    values = dict(planned_spans=700, spans_ended=700, spans_exported=700,
                  canonical_records=700, rows_stored=700, distinct_span_keys=700, dlq_records=0)
    values.update(overrides)
    return M.reconcile(**values)


class TestReconcile:
    def test_success(self):
        result = _rec()
        assert result.ok and result.missing == 0 and result.duplicates == 0
        assert all(check[3] for check in result.checks)

    def test_missing_spans(self):
        result = _rec(canonical_records=690, rows_stored=690, distinct_span_keys=690)
        assert not result.ok and result.missing == 10 and result.duplicates == 0

    def test_duplicate_canonical_records(self):
        result = _rec(canonical_records=705)
        assert not result.ok and result.duplicates == 5

    def test_duplicate_rows(self):
        result = _rec(canonical_records=702, rows_stored=702, distinct_span_keys=700)
        assert not result.ok and result.duplicates == 4

    def test_export_shortfall(self):
        assert not _rec(spans_exported=699).ok

    def test_dlq_fails(self):
        result = _rec(dlq_records=1)
        assert not result.ok
        assert ("DLQ records", 1, 0, False) in result.checks

    def test_fewer_ended_than_planned(self):
        assert not _rec(spans_ended=693).ok

    @pytest.mark.parametrize("name", ["rows_stored", "dlq_records"])
    def test_bad_inputs(self, name):
        with pytest.raises(M.MetricError):
            _rec(**{name: -1})

    def test_serializable(self):
        json.dumps(_rec(dlq_records=2).to_dict())


# ---------------------------------------------------------------------------
# Clock skew
# ---------------------------------------------------------------------------

def _sample(skew_ms, half_width_ms, at=1000.0):
    half = half_width_ms / 1000.0
    return M.ClockSample(at - half, at + skew_ms / 1000.0, at + half)


class TestClock:
    def test_skew_and_uncertainty(self):
        sample = _sample(3.0, 2.0)
        assert sample.skew_ms == pytest.approx(3.0)
        assert sample.uncertainty_ms == pytest.approx(2.0)

    def test_valid(self):
        check = M.clock_check([_sample(3, 1)], [_sample(5, 2)])
        assert check.valid and check.drift_ms == pytest.approx(2.0)
        assert check.skew_ms == pytest.approx(4.0)

    def test_uncertainty_at_limit_is_valid(self):
        assert M.clock_check([_sample(0, 10)], [_sample(0, 10)]).valid

    def test_uncertainty_over_limit_is_invalid(self):
        check = M.clock_check([_sample(0, 10.5)], [_sample(0, 1)])
        assert not check.valid and "uncertainty" in check.reasons[0]

    def test_drift_at_limit_is_valid(self):
        assert M.clock_check([_sample(0, 1)], [_sample(5, 1)]).valid

    def test_drift_over_limit_is_invalid(self):
        check = M.clock_check([_sample(0, 1)], [_sample(5.5, 1)])
        assert not check.valid and "drift" in check.reasons[0]

    def test_best_sample_is_narrowest(self):
        best = M.best_sample([_sample(9, 8), _sample(1, 0.5), _sample(4, 3)])
        assert best.skew_ms == pytest.approx(1.0)

    def test_no_samples(self):
        with pytest.raises(M.MetricError):
            M.clock_check([], [_sample(0, 1)])

    def test_reversed_bracket_rejected(self):
        with pytest.raises(M.MetricError):
            M.ClockSample(10.0, 10.0, 9.0)


# ---------------------------------------------------------------------------
# Generator-bound
# ---------------------------------------------------------------------------

class TestGeneratorBound:
    """O1: only the scheduler's own evidence makes a run GENERATOR_BOUND."""

    def test_on_time_scheduler_is_not_bound(self):
        assert M.generator_bound(late_starts=5, started_requests=100) == (False, ())

    def test_late_starts_exactly_5_percent_is_not_bound(self):
        assert not M.generator_bound(late_starts=5, started_requests=100)[0]

    def test_late_starts_over_5_percent_is_bound(self):
        bound, reasons = M.generator_bound(late_starts=6, started_requests=100)
        assert bound and "late" in reasons[0]

    def test_growing_start_lag_is_bound(self):
        bound, reasons = M.generator_bound(
            late_starts=0, started_requests=100,
            start_lag_series=[0.01, 0.02, 0.05, 0.09, 0.2, 0.4])
        assert bound and "start lag grew" in reasons[0]

    def test_start_lag_that_does_not_keep_growing_is_not_bound(self):
        assert not M.generator_bound(
            late_starts=0, started_requests=100,
            start_lag_series=[0.01, 0.02, 0.05, 0.09, 0.2, 0.1, 0.3])[0]
        assert not M.generator_bound(late_starts=0, started_requests=100,
                                     start_lag_series=[0.01, 0.02, 0.05, 0.09, 0.2])[0]

    def test_requests_never_started_or_deadline_is_bound(self):
        assert M.generator_bound(late_starts=0, started_requests=90,
                                 not_started_requests=10)[0]
        assert M.generator_bound(late_starts=0, started_requests=100,
                                 deadline_reached=True)[0]

    def test_throughput_is_not_an_input(self):
        """Low achieved throughput from run, export or flush overhead alone
        cannot make a run GENERATOR_BOUND: the rule has no throughput input."""
        with pytest.raises(TypeError):
            M.generator_bound(late_starts=0, started_requests=100, requested_rate=5,
                              achieved_rate=1)

    def test_several_reasons(self):
        assert len(M.generator_bound(late_starts=50, started_requests=90,
                                     not_started_requests=10, deadline_reached=True)[1]) == 3

    @pytest.mark.parametrize("kwargs", [
        dict(late_starts=0, started_requests=0),
        dict(late_starts=3, started_requests=2),
        dict(late_starts=-1, started_requests=2),
        dict(late_starts=0, started_requests=2, deadline_reached="yes"),
        dict(late_starts=0, started_requests=2, start_lag_series=[0.1, float("nan")]),
        dict(late_starts=0, started_requests=2, lag_growth_samples=1),
    ])
    def test_unjudgeable_inputs(self, kwargs):
        with pytest.raises(M.MetricError):
            M.generator_bound(**kwargs)


# ---------------------------------------------------------------------------
# Lag growth
# ---------------------------------------------------------------------------

class TestLagGrowth:
    def test_six_increasing_samples_stop(self):
        assert M.lag_growing([1, 2, 3, 4, 5, 6])

    def test_five_increasing_samples_do_not(self):
        assert not M.lag_growing([1, 2, 3, 4, 5])

    def test_plateau_breaks_the_run(self):
        assert not M.lag_growing([1, 2, 3, 3, 4, 5, 6, 7])
        assert M.lag_growing([1, 2, 3, 3, 4, 5, 6, 7, 8])

    def test_unknown_sample_breaks_the_run(self):
        assert not M.lag_growing([1, 2, 3, None, 4, 5, 6, 7])

    def test_drop_breaks_the_run(self):
        assert not M.lag_growing([5, 6, 7, 1, 2, 3, 4])

    def test_zero_lag_never_stops(self):
        assert not M.lag_growing([0] * 20)

    def test_custom_run(self):
        assert M.lag_growing([1, 2, 3], run=3)
        with pytest.raises(M.MetricError):
            M.lag_growing([1, 2], run=1)

    def test_negative_sample_rejected(self):
        with pytest.raises(M.MetricError):
            M.lag_growing([1, -2])


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

GB = 1024 ** 3


class TestResourceStops:
    def test_nothing_wrong(self):
        assert M.resource_stops(memory=[M.MemorySample("kafka", 50, 100)],
                                disk_free_bytes=100 * GB, projected_need_bytes=GB,
                                cpu_percent=[(0, 50), (60, 60)]) == ()

    def test_memory_at_90_percent_is_fine_above_stops(self):
        assert M.resource_stops(memory=[M.MemorySample("kafka", 90, 100)]) == ()
        assert "kafka memory" in M.resource_stops(memory=[M.MemorySample("kafka", 91, 100)])[0]

    def test_zero_memory_limit_rejected(self):
        with pytest.raises(M.MetricError):
            M.resource_stops(memory=[M.MemorySample("x", 1, 0)])

    def test_disk_floor(self):
        assert M.resource_stops(disk_free_bytes=10 * GB) == ()
        assert M.resource_stops(disk_free_bytes=10 * GB - 1)

    def test_disk_projection(self):
        assert M.resource_stops(disk_free_bytes=30 * GB, projected_need_bytes=15 * GB) == ()
        assert M.resource_stops(disk_free_bytes=30 * GB, projected_need_bytes=16 * GB)

    def test_cpu_saturation_needs_sixty_seconds(self):
        saturated = [(t, 100.0) for t in range(0, 61, 10)]
        assert M.resource_stops(cpu_percent=saturated[:-1]) == ()
        assert "CPU" in M.resource_stops(cpu_percent=saturated)[0]

    def test_cpu_dip_resets(self):
        samples = [(0, 100), (30, 100), (40, 50), (50, 100), (100, 100)]
        assert M.resource_stops(cpu_percent=samples) == ()

    def test_cpu_samples_must_be_ordered(self):
        with pytest.raises(M.MetricError):
            M.resource_stops(cpu_percent=[(10, 1), (5, 1)])

    def test_thresholds_from_ladder(self):
        reasons = M.resource_stops(memory=[M.MemorySample("pg", 81, 100)],
                                   memory_fraction=0.8)
        assert reasons


# ---------------------------------------------------------------------------
# Verdicts and claims
# ---------------------------------------------------------------------------

class TestVerdict:
    @pytest.mark.parametrize("integrity, stops, bound, expected", [
        (True, [], False, "VALID"),
        (True, [], True, "GENERATOR_BOUND"),
        (True, ["lag"], True, "STOPPED"),
        (False, ["lag"], True, "INVALID"),
        (False, [], False, "INVALID"),
        (None, ["timeout"], False, "STOPPED"),
        (None, [], False, "INVALID"),
        (None, [], True, "INVALID"),
    ])
    def test_precedence(self, integrity, stops, bound, expected):
        assert M.verdict(integrity_ok=integrity, stop_reasons=stops,
                         generator_bound_run=bound) == expected

    def test_claims_follow_verdict(self):
        valid = M.claim_eligibility("VALID", True)
        assert all(valid[k] for k in ("platform_throughput", "span_volume_handled",
                                      "persistence_latency", "no_loss_for_this_run"))
        assert not valid["load_source_limit_only"]
        assert not M.claim_eligibility("VALID", False)["persistence_latency"]
        assert not M.claim_eligibility("VALID", None)["persistence_latency"]
        bound = M.claim_eligibility("GENERATOR_BOUND", True)
        assert bound["load_source_limit_only"] and not bound["platform_throughput"]
        for value in ("INVALID", "STOPPED"):
            assert not any(M.claim_eligibility(value, True).values())

    def test_unknown_verdict(self):
        with pytest.raises(M.MetricError):
            M.claim_eligibility("GREAT", True)


# ---------------------------------------------------------------------------
# Ladder: targets only
# ---------------------------------------------------------------------------

class TestLadder:
    def test_shipped_ladder_is_valid(self):
        M.check_ladder(LADDER)

    def test_levels_and_targets(self):
        assert [(lv["name"], lv["target_requests"], lv["target_spans"])
                for lv in LADDER["levels"]] == [
            ("SMOKE", 100, 700), ("SMALL", 1000, 7000), ("MEDIUM", 10000, 70000),
            ("LARGE", 100000, 700000), ("STRESS", 500000, 3500000)]

    def test_ladder_is_targets_not_results(self):
        assert LADDER["kind"] == "TARGETS"

        def keys(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    yield key
                    yield from keys(value)
            elif isinstance(node, list):
                for value in node:
                    yield from keys(value)

        for key in keys(LADDER):
            assert not any(word in key.lower() for word in (
                "achieved", "result", "measured_rate", "p50", "p95", "p99", "throughput",
                "latency"))

    def test_only_smoke_and_small_live(self):
        assert [lv["name"] for lv in LADDER["levels"] if lv["live_allowed"]] == ["SMOKE", "SMALL"]

    def test_results_keys_rejected(self):
        bad = copy.deepcopy(LADDER)
        bad["achieved"] = {}
        with pytest.raises(M.MetricError):
            M.check_ladder(bad)
        bad = copy.deepcopy(LADDER)
        bad["levels"][0]["achieved_rate"] = 3
        with pytest.raises(M.MetricError):
            M.check_ladder(bad)

    @pytest.mark.parametrize("mutate", [
        lambda d: d.__setitem__("kind", "RESULTS"),
        lambda d: d["levels"][3].__setitem__("live_allowed", True),
        lambda d: d["levels"][4].__setitem__("refresh_and_detector", True),
        lambda d: d["levels"][0].__setitem__("target_spans", 701),
        lambda d: d["levels"][1].__setitem__("rates", [50, 10]),
        lambda d: d["levels"][0].__setitem__("rates", [100]),
        lambda d: d["defaults"].__setitem__("concurrency", 65),
        lambda d: d["defaults"].__setitem__("scenario", "api-500"),
        lambda d: d["stop_limits"].__setitem__("memory_fraction", 1.5),
        lambda d: d["levels"].reverse(),
    ])
    def test_inconsistent_ladders_rejected(self, mutate):
        bad = copy.deepcopy(LADDER)
        mutate(bad)
        with pytest.raises(M.MetricError):
            M.check_ladder(bad)


# ---------------------------------------------------------------------------
# Cells and ids
# ---------------------------------------------------------------------------

class TestCells:
    def test_smoke_plan(self):
        cells = RB.plan_cells(LADDER, "SMOKE")
        assert [(c.rate, c.repetition) for c in cells] == [(5, 1), (5, 2), (5, 3),
                                                           (10, 1), (10, 2), (10, 3)]
        first = cells[0]
        assert first.benchmark_id == "bm-smoke-r0005-c32-rep1"
        assert first.warmup_run_id == "b-smoke-r0005-c32-w1"
        assert first.measured_run_id == "b-smoke-r0005-c32-m1"
        assert first.measured_seconds == 20 and first.planned_spans == 700
        assert first.warmup_requests == 150

    def test_ids_are_unique_and_valid(self):
        cells = [c for name, _ in M.LEVELS for c in RB.plan_cells(LADDER, name)]
        ids = [c.benchmark_id for c in cells]
        runs = [r for c in cells for r in (c.warmup_run_id, c.measured_run_id)]
        assert len(set(ids)) == len(ids) and len(set(runs)) == len(runs)
        assert all(RB.RUN_ID_RE.fullmatch(r) for r in runs)

    def test_target_holds_no_result(self):
        target = RB.plan_cells(LADDER, "SMALL")[0].target()
        assert not any(k.startswith(("achieved", "p9", "result")) for k in target)

    @pytest.mark.parametrize("kwargs", [dict(rates=[7]), dict(repetitions=0),
                                        dict(concurrency=0), dict(concurrency=65)])
    def test_bad_selection(self, kwargs):
        with pytest.raises(RB.BenchmarkError):
            RB.plan_cells(LADDER, "SMOKE", **kwargs)

    def test_unknown_level(self):
        with pytest.raises(RB.BenchmarkError):
            RB.plan_cells(LADDER, "HUGE")


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

class TestParsers:
    def test_sizes(self):
        assert RB.parse_size("512MiB") == 512 * 1024 ** 2
        assert RB.parse_size("1.5GiB") == int(1.5 * 1024 ** 3)
        assert RB.parse_size("12kB") == 12000
        with pytest.raises(RB.BenchmarkError):
            RB.parse_size("lots")

    def test_docker_stats(self):
        rows = RB.parse_docker_stats("pg|12.50%|100MiB / 7.6GiB\nkafka|3.0%|1GiB / 8GiB\n\n")
        assert rows[0] == {"name": "pg", "cpu_percent": 12.5,
                           "memory_used_bytes": 100 * 1024 ** 2,
                           "memory_limit_bytes": int(round(7.6 * 1024 ** 3))}
        with pytest.raises(RB.BenchmarkError):
            RB.parse_docker_stats("bad line")

    @pytest.mark.parametrize("text, seconds", [
        ("Finished running 9 view models in 0 hours 0 minutes and 2.31 seconds (2.31s).", 2.31),
        ("Finished running 5 table models in 1 minutes and 3.5 seconds", 63.5),
        ("Finished running 3 models in 12.0 seconds", 12.0),
        ("nothing here", None),
    ])
    def test_dbt_seconds(self, text, seconds):
        assert RB.parse_dbt_seconds(text) == seconds

    def test_detector_durations(self):
        log = ("x INFO    [tool_latency / agentops-demo-app / ]\n"
               "x INFO      duration_ms         : 12.5\n"
               "x INFO    [trace_latency / agentops-demo-app / agentops.request]\n"
               "x INFO      duration_ms         : 7.0\n")
        assert RB.parse_detector_durations(log) == {"tool_latency/": 12.5,
                                                    "trace_latency/agentops.request": 7.0}

    def test_clock_samples(self):
        samples = RB.parse_clock_samples("[[1.0, 1.0005, 1.001]]")
        assert samples[0].uncertainty_ms == pytest.approx(0.5)
        for bad in ("", "[]", "[[1, 2]]", "[[2, 1, 1]]"):
            with pytest.raises(RB.BenchmarkError):
                RB.parse_clock_samples(bad)

    def test_clock_probe_code_has_no_credentials(self):
        code = RB.clock_probe_code(5)
        compile(code, "probe", "exec")
        assert "os.environ['PGPASSWORD']" in code and "range(5)" in code
        with pytest.raises(RB.BenchmarkError):
            RB.clock_probe_code(0)

    def test_run_metrics_sql_refuses_bad_run_id(self):
        assert "b-smoke-r0005-c32-m1" in RB.run_metrics_sql("b-smoke-r0005-c32-m1")
        with pytest.raises(RB.BenchmarkError):
            RB.run_metrics_sql("x'; DROP TABLE telemetry_spans; --")

    def test_machine_baseline_records_failures_as_not_measured(self):
        def run(command):
            if command[:2] == ["docker", "info"]:
                return json.dumps({"ServerVersion": "29", "NCPU": 8, "MemTotal": 1, "Secret": "x"})
            raise OSError("no such tool")

        machine = RB.collect_machine_baseline(run, platform_info={"os": "test"})
        assert machine["os"] == "test" and machine["docker"]["NCPU"] == 8
        assert "Secret" not in machine["docker"]
        assert machine["images"]["kind"] == "NOT_MEASURED" and "OSError" in machine["images"]["reason"]


class TestSampler:
    def test_samples_and_errors_kept_never_zero(self):
        calls = iter([1, ValueError("boom"), 3])

        def sample():
            value = next(calls, 9)
            if isinstance(value, Exception):
                raise value
            return value

        sampler = RB.PeriodicSampler(sample, 0.001)
        sampler.start()
        import time
        deadline = time.monotonic() + 5
        while len(sampler.samples) < 3 and time.monotonic() < deadline:
            time.sleep(0.005)
        sampler.stop()
        values = [v for _, v in sampler.samples[:3]]
        assert values == [1, None, 3] and sampler.errors[0][1].startswith("ValueError")

    def test_bad_interval(self):
        with pytest.raises(RB.BenchmarkError):
            RB.PeriodicSampler(lambda: 1, 0)


# ---------------------------------------------------------------------------
# Result assembly, schema, serialization
# ---------------------------------------------------------------------------

SHA = "a" * 40


def _driver(**overrides):
    base = {"started": "2026-01-01T00:00:00+00:00", "finished": "2026-01-01T00:00:20+00:00",
            "elapsed_seconds": 20.0, "executed_attempts": 100, "started_requests": 100,
            "spans_ended": 700, "spans_exported_successfully": 700, "export_failures": 0,
            "flush_ok": True, "run_status": "completed", "problems": [],
            "achieved_submission_rate": 5.0, "late_starts": 0}
    base.update(overrides)
    return base


def _db(rows=700, **overrides):
    base = {"rows": rows, "distinct_spans": rows, "roots": 100, "first_ingested": 1000.0,
            "last_ingested": 1020.0, "request_ms": [200.0 + i for i in range(100)],
            "persistence_ms": [30.0 + i % 10 for i in range(rows)], "per_second": [35] * 20}
    base.update(overrides)
    return base


def _evidence(**overrides):
    cell = RB.plan_cells(LADDER, "SMOKE", rates=[5], repetitions=1)[0]
    values = dict(cell=cell, git_sha=SHA, git_dirty=False, started=1000.0, finished=1100.0,
                  machine={"os": "test"}, driver=_driver(), canonical_records=700,
                  raw_records=140, dlq_records=0, database=_db(),
                  clock_before=[_sample(2, 0.5)], clock_after=[_sample(3, 0.5)],
                  lag_samples=[(0, {"raw": 0, "canonical": 1}), (10, {"raw": 0, "canonical": 0})],
                  resource_samples=[(0, [{"name": "pg", "cpu_percent": 1.0,
                                          "memory_used_bytes": 10, "memory_limit_bytes": 100}])],
                  drain_seconds=4.0, refresh_seconds=2.3,
                  detector_ms={"trace_latency/agentops.request": 5.0})
    values.update(overrides)
    return RB.CellEvidence(**values)


class TestAssembleResult:
    def test_valid_run(self):
        result = RB.assemble_result(_evidence(), STOPS)
        assert result["verdict"] == "VALID" and result["integrity"]["ok"] is True
        assert result["claims"]["platform_throughput"] and result["claims"]["persistence_latency"]
        tp = result["metrics"]["throughput"]
        assert tp["spans_persisted_per_second"]["value"] == pytest.approx(35.0)
        assert tp["spans_persisted_per_second"]["kind"] == "MEASURED"
        assert tp["spans_generated_per_second"]["kind"] == "DERIVED"
        assert result["metrics"]["latency"]["request_ms"]["p50"]["value"] == pytest.approx(249.5)
        assert result["metrics"]["latency"]["persistence_ms"]["p95"]["kind"] == "DERIVED"
        assert result["metrics"]["lag"]["canonical_max"]["kind"] == "OBSERVATION"
        assert result["metrics"]["analytics"]["detection_latency"]["kind"] == "NOT_MEASURED"
        assert set(result) == M.RESULT_KEYS | M.RESULT_OPTIONAL_KEYS
        M.validate_result(result)

    def test_target_and_achieved_are_separate(self):
        result = RB.assemble_result(_evidence(), STOPS)
        assert set(result["workload"]) == {"target"}
        assert result["workload"]["target"]["rate_per_second"] == 5
        assert "spans_ended" in result["counts"]

    def test_missing_evidence_is_not_measured_and_invalid(self):
        result = RB.assemble_result(_evidence(driver=None, database=None, canonical_records=None,
                                              clock_before=[], lag_samples=[],
                                              resource_samples=[], drain_seconds=None,
                                              failure="RuntimeError: x"), STOPS)
        assert result["verdict"] == "INVALID" and result["integrity"]["ok"] is None
        assert result["counts"]["spans_ended"]["kind"] == "NOT_MEASURED"
        assert result["counts"]["rows_stored"]["kind"] == "NOT_MEASURED"
        tp = result["metrics"]["throughput"]
        assert tp["spans_persisted_per_second"]["value"] is None
        assert result["clock"]["kind"] == "NOT_MEASURED"
        assert not any(result["claims"].values())

    def test_empty_database_mapping_is_not_counted_as_zero(self):
        result = RB.assemble_result(_evidence(database={}), STOPS)
        assert result["counts"]["rows_stored"]["kind"] == "NOT_MEASURED"
        assert result["verdict"] == "INVALID"

    def test_integrity_failure(self):
        result = RB.assemble_result(_evidence(dlq_records=1), STOPS)
        assert result["verdict"] == "INVALID" and result["integrity"]["ok"] is False
        assert any(g["name"] == "DLQ records" and not g["ok"] for g in result["integrity"]["gates"])

    def test_duplicates_fail(self):
        result = RB.assemble_result(_evidence(canonical_records=705), STOPS)
        assert result["verdict"] == "INVALID"

    def test_generator_bound_from_late_starts(self):
        result = RB.assemble_result(_evidence(driver=_driver(late_starts=6)), STOPS)
        assert result["verdict"] == "GENERATOR_BOUND"
        assert result["claims"]["load_source_limit_only"]
        assert not result["claims"]["platform_throughput"]
        assert any("load source" in item for item in result["limitations"])

    def test_low_achieved_throughput_alone_is_not_generator_bound(self):
        """Overhead (last request, export, flush) lowers executed / elapsed;
        with the scheduler on time the run stays VALID, and the low
        throughput is still reported as it was."""
        result = RB.assemble_result(_evidence(driver=_driver(
            achieved_submission_rate=1.0, elapsed_seconds=100.0, late_starts=0)), STOPS)
        assert result["verdict"] == "VALID" and not result["stop"]["generator_bound"]
        assert result["metrics"]["throughput"]["requests_per_second"]["value"] == pytest.approx(1.0)

    def test_lag_growth_stops(self):
        lags = [(i * 10, {"raw": i, "canonical": 0}) for i in range(1, 8)]
        result = RB.assemble_result(_evidence(lag_samples=lags), STOPS)
        assert result["verdict"] == "STOPPED" and "raw lag" in result["stop"]["reasons"][0]

    def test_stop_reason_from_harness(self):
        result = RB.assemble_result(_evidence(stop_reasons=["cleanup not verified"]), STOPS)
        assert result["verdict"] == "STOPPED"

    def test_invalid_clock_blocks_persistence_latency(self):
        result = RB.assemble_result(_evidence(clock_after=[_sample(30, 0.5)]), STOPS)
        assert result["verdict"] == "VALID" and result["clock"]["valid"] is False
        assert result["metrics"]["latency"]["persistence_ms"]["p50"]["kind"] == "NOT_MEASURED"
        assert "drift" in result["metrics"]["latency"]["persistence_ms"]["p50"]["reason"]
        assert not result["claims"]["persistence_latency"]

    def test_level_without_detector(self):
        evidence = _evidence()
        evidence.cell = RB.plan_cells(LADDER, "LARGE", rates=[100], repetitions=1)[0]
        result = RB.assemble_result(evidence, STOPS)
        assert result["metrics"]["analytics"]["detector_ms"]["kind"] == "NOT_MEASURED"

    def test_serialization_round_trip(self, tmp_path):
        result = RB.assemble_result(_evidence(), STOPS)
        path = RB.write_result(result, tmp_path)
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded == json.loads(json.dumps(result))
        M.validate_result(loaded)
        with pytest.raises(FileExistsError):
            RB.write_result(result, tmp_path)

    def test_results_never_written_inside_repository(self):
        result = RB.assemble_result(_evidence(), STOPS)
        with pytest.raises(RB.BenchmarkError):
            RB.write_result(result, _REPO / "benchmarks" / "results")
        assert not (_REPO / "benchmarks" / "results").exists()


class TestValidateResult:
    def _valid(self):
        return json.loads(json.dumps(RB.assemble_result(_evidence(), STOPS)))

    @pytest.mark.parametrize("mutate, message", [
        (lambda r: r.pop("limitations"), "missing keys"),
        (lambda r: r.__setitem__("bonus", 1), "unexpected keys"),
        (lambda r: r.__setitem__("benchmark_id", "x"), "benchmark_id"),
        (lambda r: r.__setitem__("git_sha", "abc"), "git_sha"),
        (lambda r: r.__setitem__("git_dirty", "no"), "git_dirty"),
        (lambda r: r["timestamps"].__setitem__("started", "yesterday"), "timestamps"),
        (lambda r: r["workload"].__setitem__("achieved", {}), "targets only"),
        (lambda r: r["counts"].__setitem__("rows_stored", -1), "counts.rows_stored"),
        (lambda r: r["counts"].__setitem__("rows_stored", 1.5), "counts.rows_stored"),
        (lambda r: r["metrics"]["throughput"]["spans_persisted_per_second"].__setitem__(
            "kind", "GUESSED"), "kind"),
        (lambda r: r["metrics"]["throughput"]["spans_persisted_per_second"].__setitem__(
            "value", None), "NOT_MEASURED instead"),
        (lambda r: r["metrics"].__setitem__("empty", {}), "nested mappings"),
        (lambda r: r.__setitem__("verdict", "EXCELLENT"), "verdict"),
        (lambda r: r.__setitem__("verdict", "INVALID"), "contradicts"),
        (lambda r: r["claims"].__setitem__("platform_throughput", False), "claims"),
        (lambda r: r["integrity"]["gates"][0].__setitem__("ok", False), "gate did not pass"),
        (lambda r: r.__setitem__("clock", {"skew_ms": 1}), "clock"),
        (lambda r: r.__setitem__("limitations", [""]), "limitations"),
    ])
    def test_malformed_results_rejected(self, mutate, message):
        result = self._valid()
        mutate(result)
        with pytest.raises(M.MetricError, match=message):
            M.validate_result(result)

    def test_generator_bound_cannot_claim_capacity(self):
        result = json.loads(json.dumps(RB.assemble_result(
            _evidence(driver=_driver(late_starts=50)), STOPS)))
        result["verdict"] = "VALID"
        with pytest.raises(M.MetricError, match="contradicts"):
            M.validate_result(result)

    def test_not_a_mapping(self):
        with pytest.raises(M.MetricError):
            M.validate_result([])


# ---------------------------------------------------------------------------
# Command line and safety boundaries
# ---------------------------------------------------------------------------

@pytest.fixture
def no_live_access(monkeypatch):
    """Any attempt to open a connection or start a process fails the test."""
    def refuse(*args, **kwargs):
        raise AssertionError(f"live access attempted: {args[:1]}")

    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(os, "system", refuse)


def _fresh_import(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look their module up while it loads
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[name]
    return module


class TestSafety:
    def test_import_has_no_live_side_effects(self, no_live_access):
        before = set(sys.modules)
        _fresh_import("metrics_probe", _BENCH / "metrics.py")
        module = _fresh_import("run_benchmark_probe", _BENCH / "run_benchmark.py")
        loaded = set(sys.modules) - before
        for forbidden in ("run_smoke", "run_e2e", "psycopg", "confluent_kafka", "opentelemetry",
                          "grpc", "docker", "workload_generator"):
            assert not any(m == forbidden or m.startswith(forbidden + ".") for m in loaded)
        # Importing has no live side effect whatever the gate's value.
        assert isinstance(module.LIVE_ENABLED, bool)

    def test_metrics_module_has_no_io_imports(self):
        text = (_BENCH / "metrics.py").read_text(encoding="utf-8")
        for word in ("import socket", "import subprocess", "import os", "open(", "psycopg",
                     "docker", "urllib", "import json"):
            assert word not in text.split('"""', 2)[2]

    def test_plan_mode_touches_nothing(self, no_live_access, tmp_path):
        out = io.StringIO()
        code = RB.main(["--level", "SMOKE", "--output-dir", str(tmp_path / "out")], stdout=out)
        text = out.getvalue()
        assert code == 0 and text.startswith("PLAN ONLY")
        assert "bm-smoke-r0005-c32-rep1" in text and "b-smoke-r0010-c32-m3" in text
        assert not (tmp_path / "out").exists()

    def test_live_is_refused_before_anything_starts(self, no_live_access, tmp_path, capsys,
                                                    monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        code = RB.main(["--level", "SMOKE", "--live", "--output-dir", str(tmp_path)])
        assert code == 2 and "not enabled" in capsys.readouterr().err
        assert "run_smoke" not in sys.modules

    def test_run_cell_is_unreachable(self, no_live_access, tmp_path, monkeypatch):
        monkeypatch.setattr(RB, "LIVE_ENABLED", False)
        cell = RB.plan_cells(LADDER, "SMOKE", rates=[5], repetitions=1)[0]
        with pytest.raises(RB.LiveDisabled):
            RB.run_cell(cell, LADDER, expect_head=None, output_dir=tmp_path)
        assert "run_smoke" not in sys.modules

    def test_output_inside_repository_refused(self, no_live_access):
        with pytest.raises(SystemExit):
            RB.main(["--level", "SMOKE", "--output-dir", str(_REPO / "benchmarks")])

    def test_default_output_is_outside_repository(self):
        assert _REPO not in RB.DEFAULT_OUTPUT.resolve().parents

    def test_offline_validation_does_no_live_access(self, no_live_access, tmp_path):
        result = RB.assemble_result(_evidence(), STOPS)
        M.validate_result(result)
        RB.write_result(result, tmp_path)
        M.check_ladder(RB.load_ladder(_BENCH / "ladder.json"))

    def test_unknown_level_and_rate_rejected(self, no_live_access):
        with pytest.raises(SystemExit):
            RB.main(["--level", "HUGE"])
        with pytest.raises(SystemExit):
            RB.main(["--level", "SMOKE", "--rate", "7"])

    def test_no_synthetic_mode(self):
        text = (_BENCH / "run_benchmark.py").read_text(encoding="utf-8").lower()
        assert "--mode synthetic" not in text and "synthetic_driver" not in text


def test_math_is_finite_everywhere():
    result = RB.assemble_result(_evidence(), STOPS)

    def walk(node):
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, float):
            assert math.isfinite(node)

    walk(result)
