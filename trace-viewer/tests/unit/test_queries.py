"""
trace-viewer/tests/unit/test_queries.py

Unit tests for queries.py.

All tests are offline — no live database connection required. The psycopg
Connection is replaced by a MagicMock whose cursor context manager returns
controlled row data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from queries import (
    TraceListResult,
    _LIST_TRACES_ORDER,
    _LIST_TRACES_WHERE_HAS_ERROR,
    _row_to_span_record,
    _validate_trace_id,
    fetch_trace_spans,
    get_trace_reconstruction,
    list_traces,
)
from reconstruction import TraceReconstruction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_TRACE_ID = "a" * 32  # 32 lowercase hex chars


def _dt() -> datetime:
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _minimal_span_row(
    span_id: str = "s1",
    parent_span_id: str | None = None,
    **extra,
) -> dict:
    """Minimal row dict with all required SpanRecord fields."""
    return {
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "span_name": "test.span",
        "start_time": _dt(),
        "end_time": _dt(),
        "duration_ms": 10.0,
        "status_code": "OK",
        **extra,
    }


class _MockCursor:
    """
    Simulates a psycopg cursor for testing list_traces(), which calls
    execute() twice on the same cursor:
        1. count_sql  → fetchone() returns {"count": count_value}
        2. page_sql   → fetchall() returns page_rows

    Also captures every (sql, params) pair passed to execute().
    """

    def __init__(self, count_value: int, page_rows: list[dict]) -> None:
        self._count_value = count_value
        self._page_rows = page_rows
        self.execute_calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self) -> dict:
        return {"count": self._count_value}

    def fetchall(self) -> list[dict]:
        return self._page_rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _make_conn_for_list(
    count_value: int = 0,
    page_rows: list[dict] | None = None,
) -> tuple[MagicMock, _MockCursor]:
    """Return (conn, cursor) wired for list_traces()."""
    cur = _MockCursor(count_value, page_rows or [])
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _make_conn_for_fetch(rows: list[dict]) -> tuple[MagicMock, MagicMock]:
    """Return (conn, cursor) wired for fetch_trace_spans() / get_trace_reconstruction()."""
    cur = MagicMock()
    cur.fetchall.return_value = rows

    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = ctx

    return conn, cur


# ---------------------------------------------------------------------------
# _validate_trace_id
# ---------------------------------------------------------------------------

class TestValidateTraceId:
    def test_valid_32_hex_lower(self):
        _validate_trace_id("0" * 32)
        _validate_trace_id("a" * 32)
        _validate_trace_id("f" * 32)
        _validate_trace_id("0123456789abcdef" * 2)

    def test_rejects_uppercase(self):
        with pytest.raises(ValueError):
            _validate_trace_id("A" * 32)

    def test_rejects_31_chars(self):
        with pytest.raises(ValueError):
            _validate_trace_id("a" * 31)

    def test_rejects_33_chars(self):
        with pytest.raises(ValueError):
            _validate_trace_id("a" * 33)

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError):
            _validate_trace_id("")

    def test_rejects_non_hex(self):
        with pytest.raises(ValueError):
            _validate_trace_id("g" * 32)

    def test_rejects_spaces(self):
        with pytest.raises(ValueError):
            _validate_trace_id(" " * 32)

    def test_rejects_non_string(self):
        with pytest.raises(ValueError):
            _validate_trace_id(None)  # type: ignore[arg-type]

    def test_rejects_int(self):
        with pytest.raises(ValueError):
            _validate_trace_id(12345678901234567890123456789012)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _row_to_span_record
# ---------------------------------------------------------------------------

class TestRowToSpanRecord:
    def test_required_fields_mapped(self):
        row = _minimal_span_row("sp1")
        span = _row_to_span_record(row)
        assert span.span_id == "sp1"
        assert span.parent_span_id is None
        assert span.span_name == "test.span"
        assert span.duration_ms == 10.0
        assert span.status_code == "OK"

    def test_optional_fields_none_when_absent(self):
        row = _minimal_span_row()
        span = _row_to_span_record(row)
        assert span.trace_id is None
        assert span.service_name is None
        assert span.span_kind is None
        assert span.agent_name is None
        assert span.tool_name is None
        assert span.retrieval_result_count is None
        assert span.retrieval_top_relevance_score is None
        assert span.error_type is None
        assert span.error_message is None

    def test_optional_fields_populated(self):
        row = _minimal_span_row(
            trace_id=_VALID_TRACE_ID,
            service_name="my-service",
            span_kind="INTERNAL",
            agent_name="agent1",
            tool_name="search",
            tool_status="success",
            retrieval_result_count=5,
            retrieval_top_relevance_score=0.95,
            error_type="TimeoutError",
            error_message="timed out",
        )
        span = _row_to_span_record(row)
        assert span.trace_id == _VALID_TRACE_ID
        assert span.service_name == "my-service"
        assert span.span_kind == "INTERNAL"
        assert span.agent_name == "agent1"
        assert span.tool_name == "search"
        assert span.tool_status == "success"
        assert span.retrieval_result_count == 5
        assert span.retrieval_top_relevance_score == 0.95
        assert span.error_type == "TimeoutError"
        assert span.error_message == "timed out"

    def test_parent_span_id_preserved(self):
        row = _minimal_span_row("child", parent_span_id="parent1")
        span = _row_to_span_record(row)
        assert span.parent_span_id == "parent1"

    def test_start_end_time_preserved(self):
        t = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        row = _minimal_span_row()
        row["start_time"] = t
        row["end_time"] = t
        span = _row_to_span_record(row)
        assert span.start_time == t
        assert span.end_time == t


# ---------------------------------------------------------------------------
# list_traces — return type and basic structure
# ---------------------------------------------------------------------------

class TestListTracesReturnType:
    def test_returns_trace_list_result(self):
        conn, _ = _make_conn_for_list(count_value=0, page_rows=[])
        result = list_traces(conn)
        assert isinstance(result, TraceListResult)

    def test_result_has_total_and_traces(self):
        page = [{"trace_id": _VALID_TRACE_ID}]
        conn, _ = _make_conn_for_list(count_value=1, page_rows=page)
        result = list_traces(conn)
        assert result.total == 1
        assert result.traces == page

    def test_empty_result(self):
        conn, _ = _make_conn_for_list(count_value=0, page_rows=[])
        result = list_traces(conn)
        assert result.total == 0
        assert result.traces == []


# ---------------------------------------------------------------------------
# list_traces — total reflects filter, not page size
# ---------------------------------------------------------------------------

class TestListTracesTotal:
    def test_total_mapped_from_count_query(self):
        conn, _ = _make_conn_for_list(count_value=42, page_rows=[])
        result = list_traces(conn)
        assert result.total == 42

    def test_total_can_exceed_page_size(self):
        # 100 total, but only 1 row on this page (limit=1, offset=50)
        conn, _ = _make_conn_for_list(count_value=100, page_rows=[{"x": 1}])
        result = list_traces(conn, limit=1, offset=50)
        assert result.total == 100
        assert len(result.traces) == 1


# ---------------------------------------------------------------------------
# list_traces — has_error filter
# ---------------------------------------------------------------------------

class TestListTracesHasErrorFilter:
    def test_no_filter_sends_no_where_clause(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        count_sql, _ = cur.execute_calls[0]
        page_sql, _ = cur.execute_calls[1]
        assert "WHERE" not in count_sql
        assert "WHERE" not in page_sql

    def test_has_error_true_sends_where_clause(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn, has_error=True)
        count_sql, count_params = cur.execute_calls[0]
        page_sql, page_params = cur.execute_calls[1]
        assert "WHERE" in count_sql
        assert "WHERE" in page_sql
        assert count_params["has_error"] is True
        assert page_params["has_error"] is True

    def test_has_error_false_sends_where_clause(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn, has_error=False)
        count_sql, count_params = cur.execute_calls[0]
        page_sql, page_params = cur.execute_calls[1]
        assert "WHERE" in count_sql
        assert "WHERE" in page_sql
        assert count_params["has_error"] is False
        assert page_params["has_error"] is False

    def test_has_error_value_is_bound_not_interpolated(self):
        # The WHERE fragment must use %(has_error)s, not a bare True/False
        assert "%(has_error)s" in _LIST_TRACES_WHERE_HAS_ERROR
        # No literal Python True/False in the SQL fragment
        assert "True" not in _LIST_TRACES_WHERE_HAS_ERROR
        assert "False" not in _LIST_TRACES_WHERE_HAS_ERROR

    def test_count_and_page_both_receive_has_error_param(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn, has_error=True)
        assert len(cur.execute_calls) == 2
        _, count_params = cur.execute_calls[0]
        _, page_params = cur.execute_calls[1]
        assert "has_error" in count_params
        assert "has_error" in page_params

    def test_no_filter_has_error_not_in_params(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        _, count_params = cur.execute_calls[0]
        _, page_params = cur.execute_calls[1]
        assert "has_error" not in count_params
        assert "has_error" not in page_params


# ---------------------------------------------------------------------------
# list_traces — pagination parameter binding
# ---------------------------------------------------------------------------

class TestListTracesPagination:
    def test_default_limit_and_offset_bound(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        _, page_params = cur.execute_calls[1]
        assert page_params["limit"] == 50
        assert page_params["offset"] == 0

    def test_custom_limit_and_offset_bound(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn, limit=10, offset=20)
        _, page_params = cur.execute_calls[1]
        assert page_params["limit"] == 10
        assert page_params["offset"] == 20

    def test_count_query_does_not_use_limit_offset(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn, limit=5, offset=10)
        count_sql, _ = cur.execute_calls[0]
        assert "LIMIT" not in count_sql
        assert "OFFSET" not in count_sql

    def test_limit_zero_raises(self):
        conn, _ = _make_conn_for_list()
        with pytest.raises(ValueError, match="limit"):
            list_traces(conn, limit=0)

    def test_limit_negative_raises(self):
        conn, _ = _make_conn_for_list()
        with pytest.raises(ValueError, match="limit"):
            list_traces(conn, limit=-1)

    def test_offset_negative_raises(self):
        conn, _ = _make_conn_for_list()
        with pytest.raises(ValueError, match="offset"):
            list_traces(conn, offset=-1)


# ---------------------------------------------------------------------------
# list_traces — SQL contract assertions
# ---------------------------------------------------------------------------

class TestListTracesSqlContract:
    def test_count_query_targets_int_trace_spans(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        count_sql, _ = cur.execute_calls[0]
        assert "analytics.int_trace_spans" in count_sql

    def test_page_query_targets_int_trace_spans(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        _, page_sql = (cur.execute_calls[1][0], cur.execute_calls[1][0])
        page_sql = cur.execute_calls[1][0]
        assert "analytics.int_trace_spans" in page_sql

    def test_deterministic_ordering_in_page_sql(self):
        assert "trace_start_time DESC" in _LIST_TRACES_ORDER
        assert "trace_id ASC" in _LIST_TRACES_ORDER

    def test_two_execute_calls_per_list_traces(self):
        conn, cur = _make_conn_for_list()
        list_traces(conn)
        assert len(cur.execute_calls) == 2


# ---------------------------------------------------------------------------
# fetch_trace_spans
# ---------------------------------------------------------------------------

class TestFetchTraceSpans:
    def test_returns_span_records(self):
        rows = [_minimal_span_row("s1"), _minimal_span_row("s2")]
        conn, _ = _make_conn_for_fetch(rows)
        result = fetch_trace_spans(conn, _VALID_TRACE_ID)
        assert len(result) == 2
        assert result[0].span_id == "s1"
        assert result[1].span_id == "s2"

    def test_trace_id_passed_as_parameter(self):
        conn, cur = _make_conn_for_fetch([])
        fetch_trace_spans(conn, _VALID_TRACE_ID)
        args, _ = cur.execute.call_args
        params = args[1]
        assert params["trace_id"] == _VALID_TRACE_ID

    def test_empty_result_for_unknown_trace(self):
        conn, _ = _make_conn_for_fetch([])
        result = fetch_trace_spans(conn, _VALID_TRACE_ID)
        assert result == []

    def test_invalid_trace_id_raises_before_query(self):
        conn, cur = _make_conn_for_fetch([])
        with pytest.raises(ValueError):
            fetch_trace_spans(conn, "not-valid")
        cur.execute.assert_not_called()

    def test_uppercase_trace_id_raises(self):
        conn, cur = _make_conn_for_fetch([])
        with pytest.raises(ValueError):
            fetch_trace_spans(conn, "A" * 32)
        cur.execute.assert_not_called()

    def test_sql_uses_stg_telemetry_spans(self):
        from queries import _FETCH_SPANS_SQL
        assert "analytics.stg_telemetry_spans" in _FETCH_SPANS_SQL

    def test_sql_filters_by_trace_id_parameter(self):
        from queries import _FETCH_SPANS_SQL
        assert "%(trace_id)s" in _FETCH_SPANS_SQL
        assert "WHERE trace_id = %(trace_id)s" in _FETCH_SPANS_SQL

    def test_required_columns_in_sql(self):
        from queries import _FETCH_SPANS_SQL
        for col in ("span_id", "parent_span_id", "span_name", "start_time",
                    "end_time", "duration_ms", "status_code"):
            assert col in _FETCH_SPANS_SQL, f"Missing column: {col}"

    def test_optional_columns_in_sql(self):
        from queries import _FETCH_SPANS_SQL
        for col in ("trace_id", "service_name", "span_kind", "agent_name",
                    "tool_name", "error_type"):
            assert col in _FETCH_SPANS_SQL, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# get_trace_reconstruction
# ---------------------------------------------------------------------------

class TestGetTraceReconstruction:
    def test_returns_trace_reconstruction(self):
        rows = [_minimal_span_row("root")]
        conn, _ = _make_conn_for_fetch(rows)
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert isinstance(result, TraceReconstruction)

    def test_span_count_matches_row_count(self):
        rows = [_minimal_span_row("s1"), _minimal_span_row("s2", parent_span_id="s1")]
        conn, _ = _make_conn_for_fetch(rows)
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert result.span_count == 2

    def test_empty_trace_returns_empty_reconstruction(self):
        conn, _ = _make_conn_for_fetch([])
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert result.span_count == 0
        assert result.roots == []
        assert result.orphans == []
        assert result.cycle_members == []
        assert result.has_cycle is False

    def test_root_span_placed_in_roots(self):
        rows = [_minimal_span_row("root", parent_span_id=None)]
        conn, _ = _make_conn_for_fetch(rows)
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert len(result.roots) == 1
        assert result.roots[0].span.span_id == "root"

    def test_parent_child_relationship_reconstructed(self):
        rows = [
            _minimal_span_row("parent", parent_span_id=None),
            _minimal_span_row("child", parent_span_id="parent"),
        ]
        conn, _ = _make_conn_for_fetch(rows)
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert len(result.roots) == 1
        assert len(result.roots[0].children) == 1
        assert result.roots[0].children[0].span.span_id == "child"

    def test_invalid_trace_id_raises_before_query(self):
        conn, cur = _make_conn_for_fetch([])
        with pytest.raises(ValueError):
            get_trace_reconstruction(conn, "bad")
        cur.execute.assert_not_called()

    def test_no_cycle_flag_for_clean_tree(self):
        rows = [
            _minimal_span_row("r", parent_span_id=None),
            _minimal_span_row("c", parent_span_id="r"),
        ]
        conn, _ = _make_conn_for_fetch(rows)
        result = get_trace_reconstruction(conn, _VALID_TRACE_ID)
        assert result.has_cycle is False
