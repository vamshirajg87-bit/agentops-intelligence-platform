"""
trace-viewer/queries.py

PostgreSQL data-access layer for the trace viewer.

All functions accept a psycopg.Connection and use %(name)s parameter binding.
No filter values are interpolated into SQL strings.

Public API:
    TraceListResult            — structured result of list_traces()
    list_traces()              → TraceListResult   paginated traces + total count
    fetch_trace_spans()        → list[SpanRecord]  all spans for one trace
    get_trace_reconstruction() → TraceReconstruction  full tree for one trace
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows

from reconstruction import SpanRecord, TraceReconstruction, build_trace_tree

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class TraceListResult:
    """Structured result of list_traces().

    total  — number of traces matching the has_error filter before pagination.
    traces — rows for the current page, ordered trace_start_time DESC, trace_id ASC.
    """
    total: int
    traces: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# SQL fragments
# ---------------------------------------------------------------------------

# All 16 columns from analytics.int_trace_spans.
_LIST_TRACES_SELECT = """\
SELECT
    trace_id,
    trace_start_time,
    trace_end_time,
    trace_duration_ms,
    root_span_id,
    root_span_name,
    root_service_name,
    request_id,
    session_id,
    span_count,
    service_count,
    error_span_count,
    agent_span_count,
    tool_span_count,
    retrieval_span_count,
    has_error
FROM analytics.int_trace_spans
"""

# Fixed literal WHERE clause when has_error filter is active.
# The value is always bound as a psycopg parameter — never interpolated.
_LIST_TRACES_WHERE_HAS_ERROR = "WHERE has_error = %(has_error)s\n"

# Deterministic ordering: primary trace_start_time DESC, trace_id ASC as
# tie-breaker so pages are stable when multiple traces share a start time.
_LIST_TRACES_ORDER = "ORDER BY trace_start_time DESC, trace_id ASC\n"

_FETCH_SPANS_SQL = """\
SELECT
    trace_id,
    span_id,
    parent_span_id,
    span_name,
    span_kind,
    service_name,
    start_time,
    end_time,
    duration_ms,
    status_code,
    status_message,
    request_id,
    session_id,
    gen_ai_operation,
    agent_name,
    agent_operation,
    tool_name,
    tool_status,
    retrieval_result_count,
    retrieval_top_relevance_score,
    error_type,
    error_message
FROM analytics.stg_telemetry_spans
WHERE trace_id = %(trace_id)s
"""

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _validate_trace_id(trace_id: str) -> None:
    """Raise ValueError if trace_id is not a 32-character lowercase hex string."""
    if not isinstance(trace_id, str) or not _TRACE_ID_RE.match(trace_id):
        raise ValueError(
            f"trace_id must be a 32-character lowercase hex string, got {trace_id!r}"
        )


# ---------------------------------------------------------------------------
# Row mapping
# ---------------------------------------------------------------------------

def _row_to_span_record(row: dict[str, Any]) -> SpanRecord:
    """Map a psycopg dict_row to a SpanRecord."""
    return SpanRecord(
        span_id=row["span_id"],
        parent_span_id=row["parent_span_id"],
        span_name=row["span_name"],
        start_time=row["start_time"],
        end_time=row["end_time"],
        duration_ms=row["duration_ms"],
        status_code=row["status_code"],
        trace_id=row.get("trace_id"),
        service_name=row.get("service_name"),
        span_kind=row.get("span_kind"),
        status_message=row.get("status_message"),
        request_id=row.get("request_id"),
        session_id=row.get("session_id"),
        gen_ai_operation=row.get("gen_ai_operation"),
        agent_name=row.get("agent_name"),
        agent_operation=row.get("agent_operation"),
        tool_name=row.get("tool_name"),
        tool_status=row.get("tool_status"),
        retrieval_result_count=row.get("retrieval_result_count"),
        retrieval_top_relevance_score=row.get("retrieval_top_relevance_score"),
        error_type=row.get("error_type"),
        error_message=row.get("error_message"),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_traces(
    conn: psycopg.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    has_error: bool | None = None,
) -> TraceListResult:
    """
    Return a paginated page of traces from analytics.int_trace_spans.

    Both the count (for pagination) and the page rows respect the has_error
    filter. Two SQL statements are executed on a single cursor within the
    same function call: a COUNT for the total and a paginated SELECT for
    the rows.

    Args:
        conn:       Open psycopg connection with SELECT on
                    analytics.int_trace_spans.
        limit:      Maximum number of rows to return. Must be >= 1.
        offset:     Number of rows to skip. Must be >= 0.
        has_error:  None  → all traces (no WHERE clause).
                    True  → only traces where has_error = true.
                    False → only traces where has_error = false.
                    The value is always passed as a bound parameter;
                    it is never interpolated into the SQL string.

    Returns:
        TraceListResult with:
            total  — total traces matching the filter (before pagination).
            traces — rows for this page, each a dict of all 16 int_trace_spans
                     columns with native Python types.
        Ordering: trace_start_time DESC, trace_id ASC.

    Raises:
        ValueError     if limit < 1 or offset < 0.
        psycopg.Error  on any database error.
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    if offset < 0:
        raise ValueError(f"offset must be >= 0, got {offset}")

    params: dict[str, Any] = {"limit": limit, "offset": offset}

    # Choose fixed WHERE fragment; bind value as parameter — never interpolated.
    if has_error is not None:
        where = _LIST_TRACES_WHERE_HAS_ERROR
        params["has_error"] = has_error
    else:
        where = ""

    count_sql = f"SELECT COUNT(*) FROM analytics.int_trace_spans\n{where}"
    page_sql = (
        f"{_LIST_TRACES_SELECT}"
        f"{where}"
        f"{_LIST_TRACES_ORDER}"
        "LIMIT %(limit)s OFFSET %(offset)s"
    )

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()["count"]

        cur.execute(page_sql, params)
        traces = cur.fetchall()

    return TraceListResult(total=total, traces=traces)


def fetch_trace_spans(
    conn: psycopg.Connection,
    trace_id: str,
) -> list[SpanRecord]:
    """
    Return all spans for trace_id from analytics.stg_telemetry_spans.

    Args:
        conn:      Open psycopg connection with SELECT on
                   analytics.stg_telemetry_spans.
        trace_id:  32-character lowercase hex string.

    Returns:
        List of SpanRecord objects. Empty list if trace_id is not found.

    Raises:
        ValueError     if trace_id fails validation.
        psycopg.Error  on any database error.
    """
    _validate_trace_id(trace_id)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_FETCH_SPANS_SQL, {"trace_id": trace_id})
        rows = cur.fetchall()

    return [_row_to_span_record(row) for row in rows]


def get_trace_reconstruction(
    conn: psycopg.Connection,
    trace_id: str,
) -> TraceReconstruction:
    """
    Fetch all spans for trace_id and reconstruct the full trace tree.

    Args:
        conn:      Open psycopg connection with SELECT on
                   analytics.stg_telemetry_spans.
        trace_id:  32-character lowercase hex string.

    Returns:
        TraceReconstruction. span_count == 0 if trace_id is not found.

    Raises:
        ValueError     if trace_id fails validation.
        psycopg.Error  on any database error.
    """
    spans = fetch_trace_spans(conn, trace_id)
    return build_trace_tree(spans)
