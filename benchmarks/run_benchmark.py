"""
benchmarks/run_benchmark.py

Pipeline benchmark of the AgentOps observability stack on the separate,
disposable stack of workload-generator/run_smoke.py:

    REAL workload -> OTLP -> collector -> Kafka raw -> stream processor
        -> Kafka canonical -> storage consumer -> PostgreSQL
        (-> analytics refresh -> anomaly detector, small levels only)

One CELL is one (level, rate, concurrency, repetition) on a fresh stack:

    warm-up run (own run id, excluded)  ->  drain
    measured run (own run id)           ->  drain, settle, reconcile
    clock-skew check before and after the measured run
    lag and container-resource samples while it runs
    -> one JSON result, validated by metrics.validate_result

Every number is labelled MEASURED, DERIVED, OBSERVATION or NOT_MEASURED (see
metrics.py); a missing number is NOT_MEASURED with its reason, never 0.  A
result's verdict decides what it may later be used for; this program writes
no claim.

Modes
-----
Without --live it only PLANS: it validates the ladder, prints the cells, run
ids, durations and where results would go, and touches nothing.

    python benchmarks/run_benchmark.py --level SMOKE

With --live it runs cells for real.  Live execution is DISABLED in this
version (LIVE_ENABLED is False): --live is refused before anything starts.
A reviewed step turns it on.

Results go outside the repository (default: <temp>/agentops-benchmarks).

Importing this module starts nothing: no process, socket, file write or
container.  The D3 harness (run_smoke.py) is imported only inside the live
functions.

Standard library only (plus metrics.py beside it).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

HERE: Path = Path(__file__).resolve().parent
REPO: Path = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import metrics as M  # noqa: E402

#: Live execution is not reachable in this version.  Changing it is a
#: reviewed step of its own.
LIVE_ENABLED: bool = False

DEFAULT_CONFIG: Path = HERE / "ladder.json"
DEFAULT_OUTPUT: Path = Path(tempfile.gettempdir()) / "agentops-benchmarks"
RUN_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{2,38}[a-z0-9]")
SERVICE_NAME: str = "agentops-demo-app"
ROOT_SPAN: str = "agentops.request"


class BenchmarkError(Exception):
    """The benchmark cannot go on.  The message is the evidence."""


class LiveDisabled(BenchmarkError):
    """Live execution was requested where it is not enabled."""


# ---------------------------------------------------------------------------
# Configuration and cells
# ---------------------------------------------------------------------------

def load_ladder(path: Path) -> dict[str, Any]:
    """Read and validate the ladder (targets only)."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"cannot read the ladder {path}: {exc}") from None
    try:
        M.check_ladder(data)
    except M.MetricError as exc:
        raise BenchmarkError(f"the ladder is not valid: {exc}") from None
    return data


@dataclass(frozen=True)
class Cell:
    level: str
    rate: int
    concurrency: int
    repetition: int
    requests: int
    seed: int
    warmup_requests: int
    live_allowed: bool
    refresh_and_detector: bool

    @property
    def measured_seconds(self) -> float:
        return self.requests / self.rate

    @property
    def planned_spans(self) -> int:
        return self.requests * M.SPANS_PER_HEALTHY_REQUEST

    @property
    def stem(self) -> str:
        return f"{self.level.lower()}-r{self.rate:04d}-c{self.concurrency:02d}"

    @property
    def benchmark_id(self) -> str:
        return f"bm-{self.stem}-rep{self.repetition}"

    @property
    def warmup_run_id(self) -> str:
        return f"b-{self.stem}-w{self.repetition}"

    @property
    def measured_run_id(self) -> str:
        return f"b-{self.stem}-m{self.repetition}"

    def target(self) -> dict[str, Any]:
        """What the cell is meant to do.  Never what it achieved."""
        return {
            "level": self.level, "scenario": "normal", "seed": self.seed,
            "requests": self.requests, "planned_spans": self.planned_spans,
            "rate_per_second": self.rate, "concurrency": self.concurrency,
            "measured_seconds": self.measured_seconds,
            "warmup_requests": self.warmup_requests, "repetition": self.repetition,
            "warmup_run_id": self.warmup_run_id, "measured_run_id": self.measured_run_id,
        }


def plan_cells(
    ladder: Mapping[str, Any], level: str, *, rates: Optional[Sequence[int]] = None,
    repetitions: Optional[int] = None, concurrency: Optional[int] = None,
) -> list[Cell]:
    """The cells of one level, in the order they would run: each rate in
    ascending order, its repetitions one after another."""
    found = [lv for lv in ladder["levels"] if lv["name"] == level]
    if not found:
        raise BenchmarkError(f"unknown level {level!r}")
    spec, defaults = found[0], ladder["defaults"]
    chosen = list(spec["rates"] if rates is None else rates)
    unknown = [r for r in chosen if r not in spec["rates"]]
    if unknown:
        raise BenchmarkError(f"rates {unknown} are not in the ladder for {level}")
    reps = defaults["repetitions"] if repetitions is None else repetitions
    if not 1 <= reps <= 10:
        raise BenchmarkError("repetitions must be 1-10")
    conc = defaults["concurrency"] if concurrency is None else concurrency
    if not 1 <= conc <= 64:
        raise BenchmarkError("concurrency must be 1-64")
    cells = []
    for rate in sorted(chosen):
        for repetition in range(1, reps + 1):
            cell = Cell(
                level=level, rate=rate, concurrency=conc, repetition=repetition,
                requests=spec["target_requests"], seed=defaults["seed"],
                warmup_requests=max(1, int(rate * defaults["warmup_seconds"])),
                live_allowed=spec["live_allowed"],
                refresh_and_detector=spec["refresh_and_detector"],
            )
            for run_id in (cell.warmup_run_id, cell.measured_run_id):
                if not RUN_ID_RE.fullmatch(run_id):
                    raise BenchmarkError(f"run id {run_id!r} is not valid for the generator")
            if not M.BENCHMARK_ID_RE.fullmatch(cell.benchmark_id):
                raise BenchmarkError(f"benchmark id {cell.benchmark_id!r} is not valid")
            cells.append(cell)
    return cells


# ---------------------------------------------------------------------------
# Parsers of tool output (pure)
# ---------------------------------------------------------------------------

_UNITS = {"B": 1, "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3, "TB": 1000 ** 4,
          "KIB": 1024, "MIB": 1024 ** 2, "GIB": 1024 ** 3, "TIB": 1024 ** 4}
_SIZE_RE = re.compile(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)\s*", re.IGNORECASE)


def parse_size(text: str) -> int:
    """'512MiB' / '1.5GiB' / '12kB' -> bytes."""
    match = _SIZE_RE.fullmatch(text)
    if match is None:
        raise BenchmarkError(f"not a size: {text!r}")
    return int(round(float(match.group(1)) * _UNITS[match.group(2).upper()]))


def parse_docker_stats(text: str) -> list[dict[str, Any]]:
    """
    Lines of `docker stats --no-stream --format
    "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"` -> one dict per container:
    name, cpu_percent, memory_used_bytes, memory_limit_bytes.
    """
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != 3:
            raise BenchmarkError(f"unexpected docker stats line: {line!r}")
        name, cpu, memory = (p.strip() for p in parts)
        used, _, limit = memory.partition("/")
        rows.append({
            "name": name,
            "cpu_percent": float(cpu.rstrip("%")),
            "memory_used_bytes": parse_size(used),
            "memory_limit_bytes": parse_size(limit),
        })
    return rows


_DBT_FINISHED_RE = re.compile(
    r"Finished running .*? in (?:(\d+) hours?,?\s*)?(?:(\d+) minutes?\s*(?:and\s*)?)?"
    r"(\d+(?:\.\d+)?) seconds")


def parse_dbt_seconds(text: str) -> Optional[float]:
    """dbt's own 'Finished running ... in X seconds' as seconds, or None."""
    match = _DBT_FINISHED_RE.search(text)
    if match is None:
        return None
    hours, minutes, seconds = match.groups()
    return int(hours or 0) * 3600 + int(minutes or 0) * 60 + float(seconds)


_GROUP_RE = re.compile(r"\[(\w+) / ([^/\]]+?) / ([^\]]*)\]\s*$")
_DURATION_RE = re.compile(r"\sduration_ms\s*: ([0-9]+(?:\.[0-9]+)?)\s*$")


def parse_detector_durations(text: str) -> dict[str, float]:
    """'signal_path/operation' -> duration_ms, from the detector's log."""
    found: dict[str, float] = {}
    current: Optional[str] = None
    for line in text.splitlines():
        header = _GROUP_RE.search(line)
        if header:
            current = f"{header.group(1)}/{header.group(3)}"
            continue
        duration = _DURATION_RE.search(line)
        if duration and current is not None:
            found[current] = float(duration.group(1))
            current = None
    return found


def clock_probe_code(samples: int) -> str:
    """
    Python run in the temporary environment, next to the database: each
    sample brackets one `SELECT clock_timestamp()` between two host clock
    readings, so a bracket is one local round trip wide.  Connection
    settings come from its environment (PG*), never from this program's
    arguments.
    """
    if not 1 <= samples <= 50:
        raise BenchmarkError("samples must be 1-50")
    return (
        "import json, os, time\n"
        "import psycopg\n"
        "out = []\n"
        "with psycopg.connect(host=os.environ['PGHOST'], port=os.environ['PGPORT'],"
        " dbname=os.environ['PGDATABASE'], user=os.environ['PGUSER'],"
        " password=os.environ['PGPASSWORD'], autocommit=True) as conn:\n"
        "    cur = conn.cursor()\n"
        "    cur.execute('SELECT 1')\n"
        f"    for _ in range({samples}):\n"
        "        before = time.time()\n"
        "        cur.execute('SELECT extract(epoch from clock_timestamp())::float8')\n"
        "        database = cur.fetchone()[0]\n"
        "        after = time.time()\n"
        "        out.append([before, database, after])\n"
        "print(json.dumps(out))\n"
    )


def parse_clock_samples(text: str) -> list[M.ClockSample]:
    try:
        rows = json.loads(text)
    except json.JSONDecodeError as exc:
        raise BenchmarkError(f"clock probe output is not JSON: {exc}") from None
    if not isinstance(rows, list) or not rows:
        raise BenchmarkError("clock probe returned no samples")
    try:
        return [M.ClockSample(*row) for row in rows]
    except (TypeError, M.MetricError) as exc:
        raise BenchmarkError(f"bad clock sample: {exc}") from None


# ---------------------------------------------------------------------------
# Machine baseline
# ---------------------------------------------------------------------------

def collect_machine_baseline(
    run: Callable[[Sequence[str]], str], *, platform_info: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Environment facts, no secrets.  run(command) returns a command's output;
    platform_info comes from the caller (OS, CPU, RAM, disk, Python).  A
    fact that cannot be read is recorded as NOT_MEASURED with the reason.
    """
    machine: dict[str, Any] = dict(platform_info)

    def fact(name: str, command: Sequence[str], parse: Callable[[str], Any]) -> None:
        try:
            machine[name] = parse(run(command))
        except Exception as exc:  # recorded, never invented
            machine[name] = {"kind": "NOT_MEASURED", "reason": f"{type(exc).__name__}: {exc}"}

    def docker_info(text: str) -> dict[str, Any]:
        info = json.loads(text)
        return {key: info.get(key) for key in (
            "ServerVersion", "OperatingSystem", "KernelVersion", "NCPU", "MemTotal",
            "Driver", "CgroupVersion")}

    fact("docker", ["docker", "info", "--format", "{{json .}}"], docker_info)
    fact("docker_compose", ["docker", "compose", "version", "--short"], str.strip)
    fact("images", ["docker", "images", "--no-trunc", "--format",
                    "{{.Repository}}:{{.Tag}} {{.ID}}"],
         lambda text: sorted(line for line in text.splitlines() if line.strip()))
    return machine


def local_platform_info() -> dict[str, Any]:
    """OS, CPU, memory, disk and Python facts of this machine (live only)."""
    import os
    import platform
    import shutil

    info: dict[str, Any] = {
        "os": platform.platform(), "machine": platform.machine(),
        "processor": platform.processor() or None, "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
    }
    usage = shutil.disk_usage(REPO.anchor or "/")
    info["disk_free_bytes"], info["disk_total_bytes"] = usage.free, usage.total
    if sys.platform == "win32":
        import ctypes

        class _Status(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
        status = _Status()
        status.dwLength = ctypes.sizeof(_Status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            info["memory_total_bytes"] = status.ullTotalPhys
    return info


# ---------------------------------------------------------------------------
# Sampling while a run goes on
# ---------------------------------------------------------------------------

class PeriodicSampler:
    """
    Calls sample() every interval seconds on a background thread until
    stop(); keeps (seconds since start, value) pairs.  A sample that raises
    is kept as (time, None) with the error, never as 0.
    """

    def __init__(self, sample: Callable[[], Any], interval: float,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if interval <= 0:
            raise BenchmarkError("interval must be positive")
        self._sample, self._interval, self._clock = sample, interval, clock
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.samples: list[tuple[float, Any]] = []
        self.errors: list[tuple[float, str]] = []

    def start(self) -> None:
        start = self._clock()

        def loop() -> None:
            while not self._stop.is_set():
                at = self._clock() - start
                try:
                    self.samples.append((at, self._sample()))
                except Exception as exc:
                    self.samples.append((at, None))
                    self.errors.append((at, f"{type(exc).__name__}: {exc}"))
                self._stop.wait(self._interval)

        self._thread = threading.Thread(target=loop, name="benchmark-sampler", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 30.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)


# ---------------------------------------------------------------------------
# Database queries (read-only; run by the live code through the harness)
# ---------------------------------------------------------------------------

def run_metrics_sql(run_id: str) -> str:
    """One JSON object describing what the database holds for a run."""
    if not RUN_ID_RE.fullmatch(run_id):
        raise BenchmarkError(f"bad run id {run_id!r}")
    return f"""
WITH roots AS (
    SELECT trace_id FROM telemetry_spans
    WHERE parent_span_id IS NULL AND attributes->>'agentops.workload.run_id' = '{run_id}'
), spans AS (
    SELECT * FROM telemetry_spans WHERE trace_id IN (SELECT trace_id FROM roots)
)
SELECT json_build_object(
  'rows', (SELECT count(*) FROM spans),
  'distinct_spans', (SELECT count(DISTINCT (trace_id, span_id)) FROM spans),
  'roots', (SELECT count(*) FROM roots),
  'first_ingested', (SELECT extract(epoch from min(ingested_at))::float8 FROM spans),
  'last_ingested', (SELECT extract(epoch from max(ingested_at))::float8 FROM spans),
  'request_ms', (SELECT coalesce(json_agg(duration_ms ORDER BY duration_ms), '[]')
                 FROM spans WHERE parent_span_id IS NULL),
  'persistence_ms', (SELECT coalesce(json_agg(
        extract(epoch from (ingested_at - end_time)) * 1000.0 ORDER BY 1), '[]') FROM spans),
  'per_second', (SELECT coalesce(json_agg(c ORDER BY s), '[]') FROM (
        SELECT date_trunc('second', ingested_at) AS s, count(*) AS c FROM spans GROUP BY 1) t)
)
"""


# ---------------------------------------------------------------------------
# Result assembly
# ---------------------------------------------------------------------------

def _get(source: Optional[Mapping[str, Any]], key: str) -> Any:
    """source[key], or None when there is no source or no such key."""
    return None if source is None else source.get(key)


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


@dataclass
class CellEvidence:
    """Everything one cell produced; any field may be missing (None)."""

    cell: Cell
    git_sha: str
    git_dirty: bool
    started: float
    finished: float
    machine: dict[str, Any] = field(default_factory=dict)
    driver: Optional[Mapping[str, Any]] = None        # the generator's execution report
    canonical_records: Optional[int] = None
    raw_records: Optional[int] = None
    dlq_records: Optional[int] = None
    database: Optional[Mapping[str, Any]] = None      # run_metrics_sql output
    clock_before: Sequence[M.ClockSample] = ()
    clock_after: Sequence[M.ClockSample] = ()
    lag_samples: Sequence[tuple[float, Any]] = ()
    resource_samples: Sequence[tuple[float, Any]] = ()
    drain_seconds: Optional[float] = None
    refresh_seconds: Optional[float] = None
    detector_ms: Optional[Mapping[str, float]] = None
    process_exits: Sequence[str] = ()
    stop_reasons: Sequence[str] = ()
    failure: Optional[str] = None
    limitations: list[str] = field(default_factory=list)


def assemble_result(evidence: CellEvidence, stops: Mapping[str, Any]) -> dict[str, Any]:
    """
    The result of one cell from its evidence.  Pure: what is missing is
    NOT_MEASURED with a reason; the verdict follows from the integrity gates,
    the stop rules and the load-source check, in that order.
    """
    cell, driver, db = evidence.cell, evidence.driver, evidence.database
    limitations = list(evidence.limitations)
    window_driver = window_db = None
    if driver is not None:
        window_driver = {"start": driver["started"], "end": driver["finished"], "clock": "driver"}
    if db is not None and db.get("first_ingested") is not None:
        window_db = {"start": _iso(db["first_ingested"]), "end": _iso(db["last_ingested"]),
                     "clock": "database"}

    # -- counts ---------------------------------------------------------------
    def count_or_missing(value: Optional[int], source: str, reason: str) -> Any:
        return value if value is not None else M.not_measured("spans", source, reason).to_dict()

    counts: dict[str, Any] = {
        "planned_spans": cell.planned_spans,
        "spans_ended": count_or_missing(_get(driver, "spans_ended"), "driver report",
                                        "the measured run did not produce a driver report"),
        "spans_exported": count_or_missing(_get(driver, "spans_exported_successfully"),
                                           "driver report", "no driver report"),
        "raw_records": count_or_missing(evidence.raw_records, "raw topic offsets",
                                        "raw topic offsets were not read"),
        "canonical_records": count_or_missing(evidence.canonical_records,
                                              "canonical topic offsets",
                                              "canonical topic offsets were not read"),
        "dlq_records": count_or_missing(evidence.dlq_records, "DLQ offsets",
                                        "DLQ offsets were not read"),
        "rows_stored": count_or_missing(_get(db, "rows"), "telemetry_spans",
                                        "the database was not read"),
        "distinct_spans": count_or_missing(_get(db, "distinct_spans"), "telemetry_spans",
                                           "the database was not read"),
    }

    # -- integrity ------------------------------------------------------------
    gates: list[dict[str, Any]] = []
    integrity_ok: Optional[bool]
    db_complete = db is not None and all(
        db.get(key) is not None for key in ("rows", "distinct_spans", "roots"))
    driver_complete = driver is not None and all(
        driver.get(key) is not None for key in ("spans_ended", "spans_exported_successfully"))
    needed = (evidence.canonical_records, evidence.dlq_records)
    if not (db_complete and driver_complete) or any(item is None for item in needed):
        integrity_ok = None
        gates.append({"name": "evidence complete", "ok": False,
                      "evidence": "driver report, offsets or database counts missing"})
    else:
        rec = M.reconcile(
            planned_spans=cell.planned_spans, spans_ended=driver["spans_ended"],
            spans_exported=driver["spans_exported_successfully"],
            canonical_records=evidence.canonical_records, rows_stored=db["rows"],
            distinct_span_keys=db["distinct_spans"], dlq_records=evidence.dlq_records,
        )
        for name, found, expected, ok in rec.checks:
            gates.append({"name": name, "ok": ok, "evidence": f"{found} (expected {expected})"})
        gates.append({"name": "no missing spans", "ok": rec.missing == 0,
                      "evidence": f"missing {rec.missing}"})
        gates.append({"name": "no duplicate spans", "ok": rec.duplicates == 0,
                      "evidence": f"duplicates {rec.duplicates}"})
        gates.append({"name": "driver exit and status",
                      "ok": driver.get("run_status") == "completed"
                      and driver.get("export_failures") == 0 and driver.get("flush_ok") is True
                      and not driver.get("problems"),
                      "evidence": f"status {driver.get('run_status')}, export failures "
                                  f"{driver.get('export_failures')}, flush "
                                  f"{driver.get('flush_ok')}, problems "
                                  f"{len(driver.get('problems') or [])}"})
        gates.append({"name": "roots = requests", "ok": db.get("roots") == cell.requests,
                      "evidence": f"{db.get('roots')} (expected {cell.requests})"})
        gates.append({"name": "host processes alive", "ok": not evidence.process_exits,
                      "evidence": "; ".join(evidence.process_exits) or "none exited"})
        gates.append({"name": "run completed", "ok": evidence.failure is None,
                      "evidence": evidence.failure or "no failure"})
        integrity_ok = all(g["ok"] for g in gates)

    # -- clock ----------------------------------------------------------------
    clock: dict[str, Any]
    clock_valid: Optional[bool] = None
    if evidence.clock_before and evidence.clock_after:
        check = M.clock_check(evidence.clock_before, evidence.clock_after,
                              max_uncertainty_ms=stops["skew_uncertainty_ms"],
                              max_drift_ms=stops["skew_drift_ms"])
        clock, clock_valid = check.to_dict(), check.valid
    else:
        clock = M.not_measured("ms", "clock probe",
                               "no clock samples before and after the run").to_dict()

    # -- metrics --------------------------------------------------------------
    metrics: dict[str, Any] = {"throughput": {}, "latency": {}, "lag": {}, "resources": {},
                               "analytics": {}}
    if driver is not None and driver.get("elapsed_seconds", 0) > 0:
        elapsed = driver["elapsed_seconds"]
        metrics["throughput"]["requests_per_second"] = M.Metric(
            M.rate(driver["executed_attempts"], elapsed), "requests/s", "DERIVED",
            "driver report", "executed attempts / driver elapsed", window_driver,
            driver["executed_attempts"]).to_dict()
        metrics["throughput"]["spans_generated_per_second"] = M.Metric(
            M.rate(driver["spans_ended"], elapsed), "spans/s", "DERIVED", "driver report",
            "spans ended / driver elapsed", window_driver, driver["spans_ended"]).to_dict()
    else:
        for name, unit in (("requests_per_second", "requests/s"),
                           ("spans_generated_per_second", "spans/s")):
            metrics["throughput"][name] = M.not_measured(
                unit, "driver report", "no driver report with a positive elapsed time").to_dict()
    if db is not None and db.get("first_ingested") is not None and \
            db["last_ingested"] > db["first_ingested"]:
        span = db["last_ingested"] - db["first_ingested"]
        metrics["throughput"]["spans_persisted_per_second"] = M.Metric(
            M.rate(db["rows"], span), "spans/s", "MEASURED", "telemetry_spans.ingested_at",
            "rows / (last - first ingested_at)", window_db, db["rows"]).to_dict()
    else:
        metrics["throughput"]["spans_persisted_per_second"] = M.not_measured(
            "spans/s", "telemetry_spans.ingested_at",
            "no rows, or all rows stored within one instant").to_dict()

    request_ms = list(db.get("request_ms") or []) if db is not None else []
    metrics["latency"]["request_ms"] = {
        k: v.to_dict() for k, v in M.latency_metrics(
            request_ms, unit="ms", kind="MEASURED", source="root span duration_ms",
            method="root span end - start (host clock); includes the planned simulated waits",
            window=window_db, reason_if_empty="no root spans read").items()}
    persistence_ms = list(db.get("persistence_ms") or []) if db is not None else []
    if clock_valid is True:
        metrics["latency"]["persistence_ms"] = {
            k: v.to_dict() for k, v in M.latency_metrics(
                persistence_ms, unit="ms", kind="DERIVED",
                source="ingested_at (database clock) - end_time (host clock)",
                method="per span; valid only within the recorded clock skew",
                window=window_db, reason_if_empty="no spans read").items()}
    else:
        reason = ("clock skew not measured" if clock_valid is None
                  else "clock skew outside its limits: " + "; ".join(clock["reasons"]))
        metrics["latency"]["persistence_ms"] = {
            f"p{q}": M.not_measured("ms", "ingested_at - end_time", reason).to_dict()
            for q in M.PERCENTILES}

    lag_values = [v for _, v in evidence.lag_samples]
    if lag_values and any(v is not None for v in lag_values):
        for group in ("raw", "canonical"):
            series = [v.get(group) if isinstance(v, Mapping) else None for v in lag_values]
            known = [s for s in series if s is not None]
            metrics["lag"][f"{group}_max"] = (M.Metric(
                max(known), "records", "OBSERVATION", "kafka-consumer-groups",
                "maximum of periodic samples", None, len(known)).to_dict() if known else
                M.not_measured("records", "kafka-consumer-groups",
                               "every sample failed").to_dict())
    else:
        for group in ("raw", "canonical"):
            metrics["lag"][f"{group}_max"] = M.not_measured(
                "records", "kafka-consumer-groups", "no lag samples taken").to_dict()
    metrics["lag"]["drain_seconds"] = (M.Metric(
        evidence.drain_seconds, "s", "OBSERVATION", "harness wall clock",
        "end of driver run -> lag 0 and counts stable", None, 1).to_dict()
        if evidence.drain_seconds is not None else
        M.not_measured("s", "harness wall clock", "drain not reached").to_dict())

    memory_peaks: dict[str, int] = {}
    for _, rows in evidence.resource_samples:
        for row in rows or []:
            memory_peaks[row["name"]] = max(memory_peaks.get(row["name"], 0),
                                            row["memory_used_bytes"])
    metrics["resources"]["memory_peak_bytes"] = (
        {name: M.Metric(value, "bytes", "OBSERVATION", "docker stats", "maximum of samples",
                        None, len(evidence.resource_samples)).to_dict()
         for name, value in sorted(memory_peaks.items())}
        or {"all": M.not_measured("bytes", "docker stats", "no resource samples").to_dict()})

    if not cell.refresh_and_detector:
        reason = f"not run at level {cell.level}"
        metrics["analytics"]["refresh_seconds"] = M.not_measured("s", "dbt", reason).to_dict()
        metrics["analytics"]["detector_ms"] = M.not_measured("ms", "detector", reason).to_dict()
    else:
        metrics["analytics"]["refresh_seconds"] = (M.Metric(
            evidence.refresh_seconds, "s", "MEASURED", "dbt log",
            "dbt 'Finished running ... in X seconds'", None, 1).to_dict()
            if evidence.refresh_seconds is not None else
            M.not_measured("s", "dbt log", "refresh duration not read").to_dict())
        metrics["analytics"]["detector_ms"] = (
            {group: M.Metric(value, "ms", "MEASURED", "detector log", "duration_ms per group",
                             None, 1).to_dict()
             for group, value in sorted(evidence.detector_ms.items())}
            if evidence.detector_ms else
            M.not_measured("ms", "detector log", "detector durations not read").to_dict())
    metrics["analytics"]["detection_latency"] = M.not_measured(
        "s", "anomaly_events", "batch detector: detected_at - event_time reflects when the "
        "detector was run, not detector speed").to_dict()
    metrics["throughput"]["otlp_export_latency"] = M.not_measured(
        "ms", "exporter", "export calls are not timed in this version").to_dict()

    # -- stop rules and verdict -------------------------------------------------
    stop_reasons = list(evidence.stop_reasons)
    canonical_lag = [v.get("canonical") if isinstance(v, Mapping) else None
                     for v in lag_values]
    raw_lag = [v.get("raw") if isinstance(v, Mapping) else None for v in lag_values]
    for name, series in (("raw", raw_lag), ("canonical", canonical_lag)):
        if M.lag_growing(series, run=int(stops["lag_growth_samples"])):
            stop_reasons.append(f"{name} lag grew across {stops['lag_growth_samples']} samples")
    bound, bound_reasons = False, ()
    if driver is not None and driver.get("started_requests"):
        bound, bound_reasons = M.generator_bound(
            requested_rate=cell.rate,
            achieved_rate=driver.get("achieved_submission_rate", 0.0),
            late_starts=driver.get("late_starts", 0),
            started_requests=driver["started_requests"],
            rate_fraction=stops["generator_rate_fraction"],
            late_fraction=stops["late_start_fraction"])
    value = M.verdict(integrity_ok=integrity_ok, stop_reasons=stop_reasons,
                      generator_bound_run=bound)
    if value == "GENERATOR_BOUND":
        limitations.append("GENERATOR_BOUND: these numbers describe the load source, not "
                           "the platform's capacity")
    limitations.append("one machine, one Docker Desktop VM; results describe this setup only")
    result = {
        "benchmark_id": cell.benchmark_id,
        "git_sha": evidence.git_sha,
        "git_dirty": evidence.git_dirty,
        "timestamps": {"started": _iso(evidence.started), "finished": _iso(evidence.finished)},
        "machine": evidence.machine,
        "workload": {"target": cell.target()},
        "counts": counts,
        "metrics": metrics,
        "clock": clock,
        "integrity": {"ok": integrity_ok, "gates": gates},
        "verdict": value,
        "verdict_reasons": list(bound_reasons) + stop_reasons,
        "stop": {"reasons": stop_reasons, "generator_bound": bound},
        "claims": M.claim_eligibility(value, clock_valid),
        "limitations": limitations,
    }
    M.validate_result(result)
    return result


def write_result(result: Mapping[str, Any], output_dir: Path) -> Path:
    """Write one result as JSON to a NEW file outside the repository."""
    output_dir = Path(output_dir).resolve()
    if output_dir == REPO or REPO in output_dir.parents:
        raise BenchmarkError("results are never written inside the repository")
    M.validate_result(result)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = result["timestamps"]["finished"].replace(":", "").replace("-", "")[:15]
    path = output_dir / f"{result['benchmark_id']}-{stamp}.json"
    with open(path, "x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return path


# ---------------------------------------------------------------------------
# Live execution (disabled in this version)
# ---------------------------------------------------------------------------

def _require_live() -> None:
    if not LIVE_ENABLED:
        raise LiveDisabled(
            "live benchmark execution is not enabled in this version; nothing was started")


def run_cell(cell: Cell, ladder: Mapping[str, Any], *, expect_head: Optional[str],
             output_dir: Path) -> dict[str, Any]:
    """
    One cell on a fresh isolated stack.  Structure only until a reviewed step
    enables it: the D3 harness provides the stack, readiness, the exact
    stage reconciliation and the cleanup; this adds timing, sampling and the
    result.
    """
    _require_live()
    if not cell.live_allowed:
        raise BenchmarkError(f"level {cell.level} is not allowed live")
    sys.path.insert(0, str(REPO / "workload-generator"))
    import run_smoke as smoke  # noqa: E402  imported only here, never at module load

    defaults, stops = ladder["defaults"], ladder["stop_limits"]
    smoke.RATE, smoke.SEED, smoke.CONCURRENCY = cell.rate, cell.seed, cell.concurrency
    smoke.T_DRAIN = float(defaults["drain_limit_seconds"])
    smoke.SETTLE_SECONDS = float(defaults["settle_seconds"])
    evidence = CellEvidence(cell=cell, git_sha=smoke.git("rev-parse", "HEAD").strip(),
                            git_dirty=bool(smoke.repository_state()), started=time.time(),
                            finished=time.time())
    lag_sampler = resource_sampler = None
    try:
        smoke.check_repository(expect_head)
        smoke.check_ports()
        smoke.record_docker_state()
        evidence.machine = collect_machine_baseline(
            lambda command: smoke.checked(list(command), timeout=smoke.T_COMMAND,
                                          what=command[0]),
            platform_info=local_platform_info())
        smoke.create_workspace()
        smoke.create_virtual_environment()
        smoke.check_render()
        smoke.start_kafka()
        smoke.create_topics()
        smoke.start_postgres()
        smoke.run_bootstrap()
        smoke.start_collector()
        smoke.start_host_services()
        offsets = smoke.check_baseline()
        offsets = smoke.run_stage(_stage(smoke, cell, warmup=True), offsets)
        evidence.clock_before = _clock(smoke)
        lag_sampler = PeriodicSampler(lambda: {
            "raw": smoke.group_lag(smoke.GROUP_STREAM, smoke.TOPIC_RAW),
            "canonical": smoke.group_lag(smoke.GROUP_STORAGE, smoke.TOPIC_SPANS)},
            defaults["sample_interval_seconds"])
        resource_sampler = PeriodicSampler(lambda: parse_docker_stats(smoke.checked(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"], timeout=smoke.T_COMMAND,
            what="docker stats")), defaults["sample_interval_seconds"])
        lag_sampler.start()
        resource_sampler.start()
        before = dict(offsets)
        measured = _stage(smoke, cell, warmup=False)
        after = smoke.run_stage(measured, offsets)
        evidence.canonical_records = after[smoke.TOPIC_SPANS] - before[smoke.TOPIC_SPANS]
        evidence.raw_records = after[smoke.TOPIC_RAW] - before[smoke.TOPIC_RAW]
        evidence.dlq_records = after[smoke.TOPIC_DLQ] - before[smoke.TOPIC_DLQ]
        report = json.loads((smoke.RUN.temp / f"{cell.measured_run_id}.json").read_text(
            encoding="utf-8"))
        evidence.driver = report["execution"]
        evidence.database = json.loads(smoke.psql(run_metrics_sql(cell.measured_run_id)))
        evidence.clock_after = _clock(smoke)
    except LiveDisabled:
        raise
    except Exception as exc:
        evidence.failure = f"{type(exc).__name__}: {exc}"
    finally:
        for sampler in (lag_sampler, resource_sampler):
            if sampler is not None:
                sampler.stop()
        if lag_sampler is not None:
            evidence.lag_samples = list(lag_sampler.samples)
        if resource_sampler is not None:
            evidence.resource_samples = list(resource_sampler.samples)
        evidence.finished = time.time()
        if not smoke.cleanup():
            evidence.stop_reasons = list(evidence.stop_reasons) + ["cleanup not verified"]
    evidence.limitations.append(
        "refresh and detector timing are not part of this version's live flow")
    result = assemble_result(evidence, stops)
    write_result(result, output_dir)
    return result


def _stage(smoke: Any, cell: Cell, *, warmup: bool) -> Any:
    requests = cell.warmup_requests if warmup else cell.requests
    run_id = cell.warmup_run_id if warmup else cell.measured_run_id
    seconds = max(60, int(math.ceil(requests / cell.rate)) * 2)
    planned = smoke.planned_facts(smoke.Stage(
        run_id=run_id, scenario="normal", traces=requests, max_duration=seconds,
        digest_prefix="", successes=requests, failures=0, failing_indexes=(),
        spans=requests * 7, trace_sizes={"7": requests}, status={"UNSET": requests * 7},
        error_spans={}, error_types={}, scenario_roots={"normal": requests}))
    return smoke.Stage(
        run_id=run_id, scenario="normal", traces=requests, max_duration=seconds,
        digest_prefix=planned["digest"], successes=requests, failures=0, failing_indexes=(),
        spans=requests * 7, trace_sizes={"7": requests}, status={"UNSET": requests * 7},
        error_spans={}, error_types={}, scenario_roots={"normal": requests})


def _clock(smoke: Any) -> list[M.ClockSample]:
    output = smoke.checked(
        [str(smoke.RUN.python), "-c", clock_probe_code(10)], timeout=smoke.T_COMMAND,
        what="clock probe",
        env=smoke.host_environment(PGHOST="127.0.0.1", PGPORT=str(smoke.POSTGRES_PORT),
                                   PGDATABASE="agentops", PGUSER="agentops",
                                   PGPASSWORD=smoke.RUN.generated["POSTGRES_PASSWORD"]))
    return parse_clock_samples(output)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _plan_text(cells: Sequence[Cell], output_dir: Path) -> str:
    lines = ["PLAN ONLY - nothing was started, nothing was sent.",
             f"results would be written to: {output_dir}"]
    for cell in cells:
        lines.append(
            f"{cell.benchmark_id}: {cell.requests} requests ({cell.planned_spans} spans) at "
            f"{cell.rate}/s, concurrency {cell.concurrency}, measured "
            f"{cell.measured_seconds:.0f} s, warm-up {cell.warmup_requests} requests; run ids "
            f"{cell.warmup_run_id} / {cell.measured_run_id}; live allowed "
            f"{'yes' if cell.live_allowed else 'no'}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None, *, stdout: Any = None) -> int:
    out = sys.stdout if stdout is None else stdout
    parser = argparse.ArgumentParser(
        prog="run_benchmark",
        description="Pipeline benchmark on a disposable stack. Without --live it only plans.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), metavar="PATH")
    parser.add_argument("--level", required=True, choices=[name for name, _ in M.LEVELS])
    parser.add_argument("--rate", type=int, action="append", default=None,
                        help="one of the level's rates; repeat for several (default: all)")
    parser.add_argument("--repetitions", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT), metavar="DIR")
    parser.add_argument("--expect-head", default=None, metavar="COMMIT")
    parser.add_argument("--live", action="store_true",
                        help="run the cells for real (disabled in this version)")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
    if output_dir == REPO or REPO in output_dir.parents:
        parser.error("--output-dir must be outside the repository")
    try:
        ladder = load_ladder(Path(args.config))
        cells = plan_cells(ladder, args.level, rates=args.rate, repetitions=args.repetitions,
                           concurrency=args.concurrency)
    except BenchmarkError as exc:
        parser.error(str(exc))

    if not args.live:
        out.write(_plan_text(cells, output_dir))
        return 0
    try:
        _require_live()
    except LiveDisabled as exc:
        sys.stderr.write(f"run_benchmark: error: {exc}\n")
        return 2
    for cell in cells:  # pragma: no cover - unreachable while LIVE_ENABLED is False
        result = run_cell(cell, ladder, expect_head=args.expect_head, output_dir=output_dir)
        out.write(f"{cell.benchmark_id}: {result['verdict']}\n")
        if result["verdict"] != "VALID":
            out.write("stopping the ladder: a cell was not VALID\n")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
