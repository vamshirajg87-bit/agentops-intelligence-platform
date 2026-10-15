"""
benchmarks/metrics.py

The arithmetic and the rules of the pipeline benchmark: percentiles, rates,
count reconciliation, clock skew, stop rules, verdicts, and the shape of a
result.

Every function here is pure and deterministic.  Nothing here opens a socket,
a file, a process or a database connection; the orchestrator
(run_benchmark.py) gathers the numbers and hands them in.

Kinds of value
--------------
    MEASURED      read directly from a counter or timestamp of the system
    DERIVED       computed from measured values (a rate, a difference)
    OBSERVATION   sampled while the run was going on; coarse, and affected
                  by the sampling itself
    NOT_MEASURED  not available; carries the reason, and never a value.
                  An unavailable number is never reported as 0.

Verdicts
--------
    VALID            every integrity gate passed and no stop rule fired
    INVALID          an integrity gate failed, or was never evaluated
    STOPPED          a stop rule ended the run before it could complete
    GENERATOR_BOUND  complete and correct, but the load source could not
                     keep the requested rate; describes the load source,
                     never the platform's capacity

Precedence: INVALID > STOPPED > GENERATOR_BOUND > VALID.

Standard library only.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence

KINDS: tuple[str, ...] = ("MEASURED", "DERIVED", "OBSERVATION", "NOT_MEASURED")
VERDICTS: tuple[str, ...] = ("VALID", "INVALID", "GENERATOR_BOUND", "STOPPED")
PERCENTILES: tuple[int, ...] = (50, 95, 99)

#: Defaults of the stop and validity limits; the ladder file can set its own.
SKEW_UNCERTAINTY_MAX_MS: float = 10.0
SKEW_DRIFT_MAX_MS: float = 5.0
GENERATOR_RATE_FRACTION: float = 0.95
LATE_START_FRACTION: float = 0.05
LAG_GROWTH_SAMPLES: int = 6
MEMORY_FRACTION_MAX: float = 0.90
DISK_FREE_MIN_BYTES: int = 10 * 1024 ** 3
DISK_PROJECTION_FACTOR: float = 2.0
CPU_SATURATION_PERCENT: float = 99.5
CPU_SATURATION_SECONDS: float = 60.0

RESULT_KEYS: frozenset[str] = frozenset({
    "benchmark_id", "git_sha", "git_dirty", "timestamps", "machine", "workload",
    "counts", "metrics", "clock", "integrity", "verdict", "limitations",
})
#: Keys a result may carry besides the required ones.
RESULT_OPTIONAL_KEYS: frozenset[str] = frozenset({"verdict_reasons", "stop", "claims"})
METRIC_KEYS: frozenset[str] = frozenset({
    "value", "unit", "kind", "source", "method", "window", "n", "reason",
})
BENCHMARK_ID_RE = re.compile(r"bm-[a-z]+-r[0-9]{4}-c[0-9]{2}-rep[0-9]+")
GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


class MetricError(ValueError):
    """Input that cannot be turned into a trustworthy number."""


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------

def _finite(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetricError(f"{name} must be a number, got {type(value).__name__}")
    if not math.isfinite(value):
        raise MetricError(f"{name} must be finite, got {value!r}")
    return float(value)


def _count(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetricError(f"{name} must be an integer count, got {type(value).__name__}")
    if value < 0:
        raise MetricError(f"{name} must not be negative, got {value}")
    return value


def percentile(values: Sequence[float], q: float) -> float:
    """
    The q-th percentile, 0 <= q <= 100, by linear interpolation between the
    two nearest ranks (the "linear" method; numpy's default).  One value is
    its own percentile at every q.

    Raises:
        MetricError  no values, a value that is not a finite number, or q out
                     of range.  An empty sample has no percentile; the caller
                     reports it as NOT_MEASURED.
    """
    q = _finite(q, "q")
    if not 0.0 <= q <= 100.0:
        raise MetricError(f"q must be between 0 and 100, got {q}")
    if len(values) == 0:
        raise MetricError("no values: a percentile of an empty sample does not exist")
    ordered = sorted(_finite(v, "value") for v in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q / 100.0
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def percentiles(values: Sequence[float]) -> dict[str, float]:
    """{"p50": ..., "p95": ..., "p99": ...}; raises MetricError on an empty sample."""
    return {f"p{q}": percentile(values, q) for q in PERCENTILES}


def rate(count: int, seconds: float) -> float:
    """
    Events per second over a window.

    Raises:
        MetricError  a negative or non-integer count, or a window that is not
                     a positive finite number of seconds (a zero-length window
                     has no rate).
    """
    count = _count(count, "count")
    seconds = _finite(seconds, "seconds")
    if seconds <= 0.0:
        raise MetricError(f"the window must be longer than 0 seconds, got {seconds}")
    return count / seconds


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Metric:
    """
    One reported number and how it was obtained.

    window   {"start": ISO-8601, "end": ISO-8601, "clock": "host" | "database" |
             "driver"} or None when the value is not tied to a time window
    n        how many observations the value rests on; None only for
             NOT_MEASURED
    reason   why a NOT_MEASURED value is missing; None otherwise
    """

    value: Any
    unit: str
    kind: str
    source: str
    method: str
    window: Optional[Mapping[str, str]] = None
    n: Optional[int] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        check_metric(self.to_dict(), "metric")

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value, "unit": self.unit, "kind": self.kind,
            "source": self.source, "method": self.method,
            "window": None if self.window is None else dict(self.window),
            "n": self.n, "reason": self.reason,
        }


def not_measured(unit: str, source: str, reason: str) -> Metric:
    """A value that is not available, with the reason.  Never 0."""
    return Metric(value=None, unit=unit, kind="NOT_MEASURED", source=source,
                  method="not measured", window=None, n=None, reason=reason)


def check_metric(data: Any, where: str) -> None:
    """
    Raises MetricError unless data is one well-formed metric.

    A NOT_MEASURED metric has value None, n None and a reason.  Every other
    kind has a finite numeric value (or a list of them), a non-negative
    integer n, and no reason.
    """
    if not isinstance(data, Mapping):
        raise MetricError(f"{where}: a metric must be a mapping")
    keys = set(data)
    if keys != METRIC_KEYS:
        missing, extra = sorted(METRIC_KEYS - keys), sorted(keys - METRIC_KEYS)
        raise MetricError(f"{where}: keys differ (missing {missing}, unexpected {extra})")
    kind = data["kind"]
    if kind not in KINDS:
        raise MetricError(f"{where}: kind {kind!r} is not one of {list(KINDS)}")
    for name in ("unit", "source", "method"):
        if not isinstance(data[name], str) or not data[name].strip():
            raise MetricError(f"{where}: {name} must be a non-empty string")
    window = data["window"]
    if window is not None:
        if not isinstance(window, Mapping) or set(window) != {"start", "end", "clock"}:
            raise MetricError(f"{where}: window must hold exactly start, end and clock")
        if not all(isinstance(window[k], str) and window[k] for k in ("start", "end", "clock")):
            raise MetricError(f"{where}: window values must be non-empty strings")
    if kind == "NOT_MEASURED":
        if data["value"] is not None:
            raise MetricError(f"{where}: a NOT_MEASURED metric has no value")
        if data["n"] is not None:
            raise MetricError(f"{where}: a NOT_MEASURED metric has no n")
        if not isinstance(data["reason"], str) or not data["reason"].strip():
            raise MetricError(f"{where}: a NOT_MEASURED metric needs a reason")
        return
    if data["reason"] is not None:
        raise MetricError(f"{where}: only a NOT_MEASURED metric carries a reason")
    value = data["value"]
    if value is None:
        raise MetricError(f"{where}: a {kind} metric needs a value; use NOT_MEASURED instead")
    if isinstance(value, list):
        if not value:
            raise MetricError(f"{where}: an empty series is NOT_MEASURED, not a value")
        for item in value:
            _finite(item, f"{where} value item")
    else:
        _finite(value, f"{where} value")
    _count(data["n"], f"{where} n")


def latency_metrics(
    values: Sequence[float], *, unit: str, kind: str, source: str, method: str,
    window: Optional[Mapping[str, str]], reason_if_empty: str,
) -> dict[str, Metric]:
    """p50 / p95 / p99 as metrics, or three NOT_MEASURED metrics for an
    empty sample."""
    if not values:
        return {f"p{q}": not_measured(unit, source, reason_if_empty) for q in PERCENTILES}
    found = percentiles(values)
    return {
        name: Metric(value=value, unit=unit, kind=kind, source=source, method=method,
                     window=window, n=len(values))
        for name, value in found.items()
    }


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Reconciliation:
    ok: bool
    missing: int
    duplicates: int
    checks: tuple[tuple[str, Any, Any, bool], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok, "missing": self.missing, "duplicates": self.duplicates,
            "checks": [
                {"name": n, "found": f, "expected": e, "ok": o} for n, f, e, o in self.checks
            ],
        }


def reconcile(
    *, planned_spans: int, spans_ended: int, spans_exported: int, canonical_records: int,
    rows_stored: int, distinct_span_keys: int, dlq_records: int,
) -> Reconciliation:
    """
    Every span planned is ended, exported, published once to the canonical
    topic and stored once; nothing goes to the DLQ.

    missing     planned spans with no stored row
    duplicates  canonical records or rows beyond one per distinct span
    """
    values = {
        "planned_spans": planned_spans, "spans_ended": spans_ended,
        "spans_exported": spans_exported, "canonical_records": canonical_records,
        "rows_stored": rows_stored, "distinct_span_keys": distinct_span_keys,
        "dlq_records": dlq_records,
    }
    for name, value in values.items():
        _count(value, name)
    checks = (
        ("spans ended = planned", spans_ended, planned_spans),
        ("spans exported = ended", spans_exported, spans_ended),
        ("canonical records = ended", canonical_records, spans_ended),
        ("rows stored = canonical records", rows_stored, canonical_records),
        ("distinct spans stored = rows", distinct_span_keys, rows_stored),
        ("DLQ records", dlq_records, 0),
    )
    rows = tuple((name, found, expected, found == expected) for name, found, expected in checks)
    missing = max(0, planned_spans - distinct_span_keys)
    duplicates = max(0, canonical_records - distinct_span_keys) + max(
        0, rows_stored - distinct_span_keys)
    return Reconciliation(
        ok=all(row[3] for row in rows) and missing == 0 and duplicates == 0,
        missing=missing, duplicates=duplicates, checks=rows,
    )


# ---------------------------------------------------------------------------
# Clock skew between the host and the database
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClockSample:
    """A database clock reading bracketed by two host clock readings, all
    in seconds since the epoch."""

    host_before: float
    database: float
    host_after: float

    def __post_init__(self) -> None:
        for name in ("host_before", "database", "host_after"):
            _finite(getattr(self, name), name)
        if self.host_after < self.host_before:
            raise MetricError("host_after is earlier than host_before")

    @property
    def skew_ms(self) -> float:
        """database - host, taking the host time as the middle of the bracket."""
        return (self.database - (self.host_before + self.host_after) / 2.0) * 1000.0

    @property
    def uncertainty_ms(self) -> float:
        return (self.host_after - self.host_before) / 2.0 * 1000.0


def best_sample(samples: Sequence[ClockSample]) -> ClockSample:
    """The sample with the narrowest bracket; MetricError when there is none."""
    if not samples:
        raise MetricError("no clock samples")
    return min(samples, key=lambda s: (s.uncertainty_ms, s.host_before))


@dataclass(frozen=True)
class ClockCheck:
    skew_ms: float
    uncertainty_ms: float
    drift_ms: float
    valid: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"skew_ms": self.skew_ms, "uncertainty_ms": self.uncertainty_ms,
                "drift_ms": self.drift_ms, "valid": self.valid, "reasons": list(self.reasons)}


def clock_check(
    before: Sequence[ClockSample], after: Sequence[ClockSample], *,
    max_uncertainty_ms: float = SKEW_UNCERTAINTY_MAX_MS,
    max_drift_ms: float = SKEW_DRIFT_MAX_MS,
) -> ClockCheck:
    """
    The host-to-database skew, measured before and after a run.  Latencies
    that cross the two clocks are valid only when both brackets are at most
    max_uncertainty_ms wide (half-width) and the skew moved by at most
    max_drift_ms during the run.
    """
    first, last = best_sample(before), best_sample(after)
    uncertainty = max(first.uncertainty_ms, last.uncertainty_ms)
    drift = abs(last.skew_ms - first.skew_ms)
    reasons = []
    if uncertainty > max_uncertainty_ms:
        reasons.append(f"skew uncertainty {uncertainty:.3f} ms > {max_uncertainty_ms} ms")
    if drift > max_drift_ms:
        reasons.append(f"skew drift {drift:.3f} ms > {max_drift_ms} ms")
    return ClockCheck(skew_ms=(first.skew_ms + last.skew_ms) / 2.0, uncertainty_ms=uncertainty,
                      drift_ms=drift, valid=not reasons, reasons=tuple(reasons))


# ---------------------------------------------------------------------------
# Stop and classification rules
# ---------------------------------------------------------------------------

def generator_bound(
    *, late_starts: int, started_requests: int, not_started_requests: int = 0,
    deadline_reached: bool = False, start_lag_series: Sequence[float] = (),
    late_fraction: float = LATE_START_FRACTION,
    lag_growth_samples: int = LAG_GROWTH_SAMPLES,
) -> tuple[bool, tuple[str, ...]]:
    """
    True when the load source's SCHEDULER could not keep the planned start
    times, so the run describes the load source rather than the platform:

        more than late_fraction of the started requests started late
        (the driver's own definition of late), or
        planned requests never started, or the deadline was reached, or
        the start lag grew across lag_growth_samples consecutive samples
        (when such a series is available).

    The achieved throughput (requests / wall time) is deliberately NOT used:
    it includes the last request's run time and the final flush, so a short
    run on time would look slow.  It is reported separately.
    """
    late_starts = _count(late_starts, "late_starts")
    started_requests = _count(started_requests, "started_requests")
    not_started_requests = _count(not_started_requests, "not_started_requests")
    if not isinstance(deadline_reached, bool):
        raise MetricError("deadline_reached must be true or false")
    if started_requests == 0 and not_started_requests == 0:
        raise MetricError("no request was planned to start: the load source cannot be judged")
    if late_starts > started_requests:
        raise MetricError("more late starts than started requests")
    reasons = []
    if started_requests:
        share = late_starts / started_requests
        if share > late_fraction:
            reasons.append(f"{share:.1%} of requests started late > {late_fraction:.0%}")
    if not_started_requests:
        reasons.append(f"{not_started_requests} planned requests never started")
    if deadline_reached:
        reasons.append("the deadline was reached before every request started")
    lags = [_finite(value, "start lag") for value in start_lag_series]
    if lag_growth_samples < 2:
        raise MetricError("lag_growth_samples must be at least 2")
    streak = 1
    for previous, current in zip(lags, lags[1:]):
        streak = streak + 1 if current > previous else 1
        if streak >= lag_growth_samples:
            reasons.append(f"start lag grew across {lag_growth_samples} consecutive samples")
            break
    return bool(reasons), tuple(reasons)


def lag_growing(samples: Sequence[Optional[int]], *, run: int = LAG_GROWTH_SAMPLES) -> bool:
    """
    True when `run` consecutive lag samples each exceed the one before
    (run - 1 increases in a row).  An unknown sample (None) breaks a run;
    it neither proves nor disproves growth.
    """
    if run < 2:
        raise MetricError("run must be at least 2")
    streak = 1
    previous: Optional[int] = None
    for sample in samples:
        if sample is not None:
            _count(sample, "lag sample")
        if sample is not None and previous is not None and sample > previous:
            streak += 1
            if streak >= run:
                return True
        else:
            streak = 1
        previous = sample
    return False


@dataclass(frozen=True)
class MemorySample:
    container: str
    used_bytes: int
    limit_bytes: int


def resource_stops(
    *,
    memory: Sequence[MemorySample] = (),
    disk_free_bytes: Optional[int] = None,
    projected_need_bytes: Optional[int] = None,
    cpu_percent: Sequence[tuple[float, float]] = (),
    memory_fraction: float = MEMORY_FRACTION_MAX,
    disk_free_min_bytes: int = DISK_FREE_MIN_BYTES,
    disk_projection_factor: float = DISK_PROJECTION_FACTOR,
    cpu_saturation_percent: float = CPU_SATURATION_PERCENT,
    cpu_saturation_seconds: float = CPU_SATURATION_SECONDS,
) -> tuple[str, ...]:
    """
    Reasons to stop for resources; empty when none applies.

    memory          a container above memory_fraction of its limit
    disk            free space below the larger of disk_free_min_bytes and
                    disk_projection_factor x the projected need
    cpu_percent     (seconds since start, percent) samples in time order;
                    stop when every sample over cpu_saturation_seconds or
                    more is at or above cpu_saturation_percent
    """
    reasons = []
    for sample in memory:
        used = _count(sample.used_bytes, "used_bytes")
        limit = _count(sample.limit_bytes, "limit_bytes")
        if limit == 0:
            raise MetricError(f"{sample.container}: a memory limit of 0 cannot be judged")
        if used / limit > memory_fraction:
            reasons.append(f"{sample.container} memory {used / limit:.1%} of its limit "
                           f"> {memory_fraction:.0%}")
    if disk_free_bytes is not None:
        free = _count(disk_free_bytes, "disk_free_bytes")
        need = 0 if projected_need_bytes is None else _count(projected_need_bytes,
                                                              "projected_need_bytes")
        floor = max(disk_free_min_bytes, int(disk_projection_factor * need))
        if free < floor:
            reasons.append(f"disk free {free} bytes < required {floor} bytes")
    saturated_since: Optional[float] = None
    last_time: Optional[float] = None
    for at, percent in cpu_percent:
        at = _finite(at, "cpu sample time")
        percent = _finite(percent, "cpu percent")
        if last_time is not None and at < last_time:
            raise MetricError("cpu samples must be in time order")
        last_time = at
        if percent >= cpu_saturation_percent:
            saturated_since = at if saturated_since is None else saturated_since
            if at - saturated_since >= cpu_saturation_seconds:
                reasons.append(f"CPU at or above {cpu_saturation_percent}% for "
                               f"{at - saturated_since:.0f} s")
                break
        else:
            saturated_since = None
    return tuple(reasons)


def verdict(
    *, integrity_ok: Optional[bool], stop_reasons: Sequence[str], generator_bound_run: bool,
) -> str:
    """
    INVALID > STOPPED > GENERATOR_BOUND > VALID.

    integrity_ok None means the gates were never evaluated: STOPPED when a
    stop rule explains why, INVALID otherwise.  A failed gate is INVALID
    even when the run was also stopped.
    """
    if integrity_ok is False:
        return "INVALID"
    if stop_reasons:
        return "STOPPED"
    if integrity_ok is None:
        return "INVALID"
    if generator_bound_run:
        return "GENERATOR_BOUND"
    return "VALID"


def claim_eligibility(verdict_value: str, clock_valid: Optional[bool]) -> dict[str, bool]:
    """
    Which kinds of statement a later reviewer MAY consider for this result.
    It produces no claim text; it only records what the evidence allows.
    """
    if verdict_value not in VERDICTS:
        raise MetricError(f"unknown verdict {verdict_value!r}")
    valid = verdict_value == "VALID"
    return {
        "platform_throughput": valid,
        "span_volume_handled": valid,
        "persistence_latency": valid and clock_valid is True,
        "no_loss_for_this_run": valid,
        "load_source_limit_only": verdict_value == "GENERATOR_BOUND",
    }


# ---------------------------------------------------------------------------
# Ladder configuration
# ---------------------------------------------------------------------------

LEVELS: tuple[tuple[str, int], ...] = (
    ("SMOKE", 100), ("SMALL", 1000), ("MEDIUM", 10000), ("LARGE", 100000), ("STRESS", 500000),
)
_LADDER_KEYS = frozenset({"kind", "note", "schema_version", "defaults", "stop_limits", "levels"})
_DEFAULT_KEYS = frozenset({
    "scenario", "seed", "concurrency", "warmup_seconds", "min_measured_seconds",
    "repetitions", "drain_limit_seconds", "settle_seconds", "sample_interval_seconds",
    "cell_budget_seconds",
})
_STOP_KEYS = frozenset({
    "memory_fraction", "disk_free_min_gb", "disk_projection_factor",
    "cpu_saturation_percent", "cpu_saturation_seconds", "lag_growth_samples",
    "generator_rate_fraction", "late_start_fraction", "skew_uncertainty_ms", "skew_drift_ms",
})
_LEVEL_KEYS = frozenset({
    "name", "target_requests", "target_spans", "rates", "live_allowed",
    "refresh_and_detector", "note",
})
SPANS_PER_HEALTHY_REQUEST: int = 7


def check_ladder(data: Any) -> None:
    """Raises MetricError unless data is a complete, consistent ladder of
    TARGETS (it must hold no result)."""
    if not isinstance(data, Mapping) or set(data) != _LADDER_KEYS:
        raise MetricError(f"the ladder must hold exactly {sorted(_LADDER_KEYS)}")
    if data["kind"] != "TARGETS":
        raise MetricError("the ladder's kind must be TARGETS: it describes targets, not results")
    if data["schema_version"] != 1:
        raise MetricError("unsupported ladder schema_version")
    defaults, stops = data["defaults"], data["stop_limits"]
    if not isinstance(defaults, Mapping) or set(defaults) != _DEFAULT_KEYS:
        raise MetricError(f"defaults must hold exactly {sorted(_DEFAULT_KEYS)}")
    if defaults["scenario"] != "normal":
        raise MetricError("throughput benchmarks use the normal scenario only")
    for name in _DEFAULT_KEYS - {"scenario"}:
        _finite(defaults[name], f"defaults.{name}")
        if defaults[name] < (0 if name in ("seed", "warmup_seconds") else 1):
            raise MetricError(f"defaults.{name} is out of range")
    if not 1 <= defaults["concurrency"] <= 64:
        raise MetricError("defaults.concurrency must be 1-64 (the generator's limit)")
    if not isinstance(stops, Mapping) or set(stops) != _STOP_KEYS:
        raise MetricError(f"stop_limits must hold exactly {sorted(_STOP_KEYS)}")
    for name in _STOP_KEYS:
        if _finite(stops[name], f"stop_limits.{name}") <= 0:
            raise MetricError(f"stop_limits.{name} must be positive")
    for name in ("memory_fraction", "generator_rate_fraction", "late_start_fraction"):
        if stops[name] > 1:
            raise MetricError(f"stop_limits.{name} is a fraction")
    levels = data["levels"]
    if not isinstance(levels, list) or [lv.get("name") for lv in levels] != [n for n, _ in LEVELS]:
        raise MetricError(f"levels must be, in order, {[n for n, _ in LEVELS]}")
    for level, (name, requests) in zip(levels, LEVELS):
        if set(level) != _LEVEL_KEYS:
            raise MetricError(f"level {name} must hold exactly {sorted(_LEVEL_KEYS)}")
        if level["target_requests"] != requests:
            raise MetricError(f"level {name} targets {requests} requests")
        if level["target_spans"] != requests * SPANS_PER_HEALTHY_REQUEST:
            raise MetricError(f"level {name}: target_spans must be "
                              f"{SPANS_PER_HEALTHY_REQUEST} x target_requests")
        rates = level["rates"]
        if not rates or rates != sorted(set(rates)) or not all(
                isinstance(r, int) and not isinstance(r, bool) and 1 <= r <= 10000 for r in rates):
            raise MetricError(f"level {name}: rates must be ascending distinct integers 1-10000")
        if requests / rates[-1] < defaults["min_measured_seconds"]:
            raise MetricError(f"level {name}: {requests} requests at {rates[-1]}/s are shorter "
                              f"than the minimum measured period")
        for flag in ("live_allowed", "refresh_and_detector"):
            if not isinstance(level[flag], bool):
                raise MetricError(f"level {name}: {flag} must be true or false")
    if any(level["live_allowed"] for level in levels if level["name"] in ("LARGE", "STRESS")):
        raise MetricError("LARGE and STRESS need their own authorization; live_allowed must be false")
    if any(level["refresh_and_detector"] for level in levels
           if level["name"] in ("LARGE", "STRESS")):
        raise MetricError("the detector is quadratic in history; not above MEDIUM")


# ---------------------------------------------------------------------------
# Result schema
# ---------------------------------------------------------------------------

_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


def _walk_metrics(node: Any, path: str) -> Iterable[tuple[str, Any]]:
    if isinstance(node, Mapping) and "kind" in node:
        yield path, node
        return
    if not isinstance(node, Mapping) or not node:
        raise MetricError(f"{path}: metrics must be nested mappings ending in metrics")
    for key, child in node.items():
        yield from _walk_metrics(child, f"{path}.{key}")


def validate_result(result: Any) -> None:
    """
    Raises MetricError listing every problem of a benchmark result; returns
    None for a well-formed one.  A result is checked for its shape and for
    the consistency of its verdict, never for whether its numbers are good.
    """
    problems: list[str] = []

    def problem(text: str) -> None:
        problems.append(text)

    if not isinstance(result, Mapping):
        raise MetricError("a result must be a mapping")
    keys = set(result)
    if not RESULT_KEYS <= keys:
        problem(f"missing keys {sorted(RESULT_KEYS - keys)}")
    if keys - RESULT_KEYS - RESULT_OPTIONAL_KEYS:
        problem(f"unexpected keys {sorted(keys - RESULT_KEYS - RESULT_OPTIONAL_KEYS)}")
    if problems:
        raise MetricError("; ".join(problems))

    if not isinstance(result["benchmark_id"], str) or not BENCHMARK_ID_RE.fullmatch(
            result["benchmark_id"]):
        problem("benchmark_id does not match bm-<level>-r<rate>-c<concurrency>-rep<n>")
    if not isinstance(result["git_sha"], str) or not GIT_SHA_RE.fullmatch(result["git_sha"]):
        problem("git_sha must be 40 lowercase hex characters")
    if not isinstance(result["git_dirty"], bool):
        problem("git_dirty must be true or false")
    stamps = result["timestamps"]
    if not isinstance(stamps, Mapping) or not {"started", "finished"} <= set(stamps):
        problem("timestamps must hold started and finished")
    else:
        for name, value in stamps.items():
            if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
                problem(f"timestamps.{name} is not an ISO-8601 time with a zone")
    if not isinstance(result["machine"], Mapping):
        problem("machine must be a mapping")

    workload = result["workload"]
    if not isinstance(workload, Mapping) or "target" not in workload:
        problem("workload must hold its target")
    elif any(key in workload for key in ("achieved", "results", "metrics")):
        problem("workload holds targets only; what was achieved belongs in counts and metrics")

    counts = result["counts"]
    if not isinstance(counts, Mapping):
        problem("counts must be a mapping")
    else:
        for name, value in counts.items():
            if isinstance(value, Mapping):
                try:
                    check_metric(value, f"counts.{name}")
                except MetricError as exc:
                    problem(str(exc))
                else:
                    if value["kind"] != "NOT_MEASURED":
                        problem(f"counts.{name}: a count is an integer or NOT_MEASURED")
            elif isinstance(value, bool) or not isinstance(value, int) or value < 0:
                problem(f"counts.{name} must be a non-negative integer or NOT_MEASURED")

    try:
        for path, metric in _walk_metrics(result["metrics"], "metrics"):
            try:
                check_metric(metric, path)
            except MetricError as exc:
                problem(str(exc))
    except MetricError as exc:
        problem(str(exc))

    clock = result["clock"]
    if isinstance(clock, Mapping) and clock.get("kind") == "NOT_MEASURED":
        try:
            check_metric(clock, "clock")
        except MetricError as exc:
            problem(str(exc))
        clock_valid: Optional[bool] = None
    elif isinstance(clock, Mapping) and {"skew_ms", "uncertainty_ms", "drift_ms", "valid",
                                         "reasons"} == set(clock):
        clock_valid = clock["valid"] if isinstance(clock["valid"], bool) else None
        if clock_valid is None:
            problem("clock.valid must be true or false")
    else:
        problem("clock must be a clock check or NOT_MEASURED")
        clock_valid = None

    integrity = result["integrity"]
    if not isinstance(integrity, Mapping) or "ok" not in integrity or "gates" not in integrity:
        problem("integrity must hold ok and gates")
        integrity_ok: Optional[bool] = None
    else:
        integrity_ok = integrity["ok"]
        if integrity_ok not in (True, False, None):
            problem("integrity.ok must be true, false or null")
        gates = integrity["gates"]
        if not isinstance(gates, list) or any(
                not isinstance(g, Mapping) or set(g) != {"name", "ok", "evidence"} for g in gates):
            problem("integrity.gates must be a list of {name, ok, evidence}")
        elif integrity_ok is True and (not gates or not all(g["ok"] is True for g in gates)):
            problem("integrity.ok is true but a gate did not pass")

    value = result["verdict"]
    if value not in VERDICTS:
        problem(f"verdict {value!r} is not one of {list(VERDICTS)}")
    else:
        stop = result.get("stop", {"reasons": [], "generator_bound": False})
        reasons = stop.get("reasons", []) if isinstance(stop, Mapping) else None
        bound = stop.get("generator_bound", False) if isinstance(stop, Mapping) else None
        if not isinstance(reasons, list) or not isinstance(bound, bool):
            problem("stop must hold a reasons list and generator_bound")
        else:
            expected = verdict(integrity_ok=integrity_ok if integrity_ok in (True, False) else None,
                               stop_reasons=reasons, generator_bound_run=bound)
            if expected != value:
                problem(f"verdict {value} contradicts the evidence, which gives {expected}")
        claims = result.get("claims")
        if claims is not None and claims != claim_eligibility(value, clock_valid):
            problem("claims do not follow from the verdict and the clock check")

    limitations = result["limitations"]
    if not isinstance(limitations, list) or not all(
            isinstance(item, str) and item.strip() for item in limitations):
        problem("limitations must be a list of non-empty strings")

    if problems:
        raise MetricError("; ".join(problems))
