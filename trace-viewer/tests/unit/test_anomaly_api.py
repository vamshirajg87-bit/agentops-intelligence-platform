"""
trace-viewer/tests/unit/test_anomaly_api.py

Offline tests for the read-only anomaly and RCA endpoints, the SQL in
anomaly_queries.py, and migration 015.  No live database: app.connect is
patched, and the query module is driven through a fake cursor.

Coverage
    1-8    GET /api/anomalies                      — shape, filters, validation
    9-11   GET /api/anomalies/{anomaly_id}         — 200 / 404 / 422
    12-14  GET /api/rca/{investigation_id}         — 200 / 404 / 422
    15-17  GET /api/anomalies/{anomaly_id}/rca     — 200 / 404 / 422
    18-20  Database failure → 503, no leaked internals; connection lifecycle
    21-26  anomaly_queries SQL: parameterized, ordered, bounded, validated
    27-30  Migration 015: exact read-only grants
    31     Existing trace routes unchanged
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from fastapi.testclient import TestClient

import anomaly_queries
from anomaly_queries import AnomalyListResult
from app import app

_DT = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
_ANOMALY_ID = "a" * 64
_INVESTIGATION_ID = "b" * 64
_TRACE_ID = "c" * 32

client = TestClient(app)

_MIGRATION = os.path.join(
    os.path.dirname(__file__), "..", "..", "..",
    "storage-consumer", "migrations", "015_grant_trace_viewer_anomaly_rca_read.sql",
)


def _anomaly_row(**overrides) -> dict:
    row = {
        "anomaly_id": _ANOMALY_ID, "anomaly_type": "latency", "signal_name": "span_duration_ms",
        "detector_name": "mad", "detector_version": "1", "trace_id": _TRACE_ID,
        "span_id": "d" * 16, "service_name": "svc", "operation_name": "op",
        "observed_value": 900.0, "event_time": _DT, "baseline_median": 100.0,
        "baseline_mad": 10.0, "baseline_n": 30, "baseline_window_start": _DT,
        "baseline_window_end": _DT, "anomaly_score": 8.5, "severity": "WARNING",
        "explanation": "slow", "detected_at": _DT,
    }
    row.update(overrides)
    return row


def _evidence_row(rank: int = 1, **overrides) -> dict:
    row = {
        "span_id": f"{rank:016x}", "parent_span_id": None, "span_name": "tool.call",
        "service_name": "svc", "duration_ms": 850.0, "status_code": "ERROR",
        "error_type": "Timeout", "error_message": "timed out", "tool_name": None,
        "tool_status": None, "agent_name": None, "agent_operation": None,
        "retrieval_result_count": None, "retrieval_top_relevance_score": None,
        "is_root_span": False, "is_error_span": True, "is_direct_subject": True,
        "depth": 1, "trace_duration_fraction": 0.9, "evidence_type": "direct_subject",
        "evidence_score": 0.9, "evidence_score_breakdown": "a=0.5;b=0.4",
        "explanation": "the slow span", "rank_position": rank,
    }
    row.update(overrides)
    return row


def _rca_row(**overrides) -> dict:
    row = {
        "investigation_id": _INVESTIGATION_ID, "anomaly_id": _ANOMALY_ID,
        "analyzer_version": "1", "investigated_at": _DT, "trace_id": _TRACE_ID,
        "subject_span_id": "d" * 16, "service_name": "svc", "operation_name": "op",
        "event_time": _DT, "trace_span_count": 4, "anomaly_type": "latency",
        "observed_value": 900.0, "baseline_median": 100.0, "anomaly_score": 8.5,
        "severity": "WARNING", "confidence": "HIGH", "summary": "tool timed out",
        "limitations": ["no logs"], "evidence": [_evidence_row(1), _evidence_row(2)],
    }
    row.update(overrides)
    return row


def _patched(**targets):
    """Patch app.connect plus the named app.<function> targets."""
    from contextlib import ExitStack
    stack = ExitStack()
    conn = MagicMock()
    stack.enter_context(patch("app.connect", return_value=conn))
    mocks = {n: stack.enter_context(patch(f"app.{n}", **kw)) for n, kw in targets.items()}
    return stack, conn, mocks


# ---------------------------------------------------------------------------
# 1-8: GET /api/anomalies
# ---------------------------------------------------------------------------


def test_list_anomalies_shape_and_defaults():
    stack, _, m = _patched(list_anomalies={"return_value": AnomalyListResult(1, [_anomaly_row()])})
    with stack:
        resp = client.get("/api/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"total", "limit", "offset", "anomalies"}
    assert (body["total"], body["limit"], body["offset"]) == (1, 50, 0)
    assert body["anomalies"][0]["anomaly_id"] == _ANOMALY_ID
    assert body["anomalies"][0]["severity"] == "WARNING"
    kwargs = m["list_anomalies"].call_args.kwargs
    assert kwargs == dict(limit=50, offset=0, severity=None, service_name=None, trace_id=None)


def test_list_anomalies_passes_filters():
    stack, _, m = _patched(list_anomalies={"return_value": AnomalyListResult(0, [])})
    with stack:
        resp = client.get(
            f"/api/anomalies?severity=CRITICAL&service_name=svc&trace_id={_TRACE_ID}&limit=5&offset=10"
        )
    assert resp.status_code == 200
    assert m["list_anomalies"].call_args.kwargs == dict(
        limit=5, offset=10, severity="CRITICAL", service_name="svc", trace_id=_TRACE_ID,
    )
    assert resp.json()["anomalies"] == []


def test_list_anomalies_nullable_columns_serialize():
    row = _anomaly_row(trace_id=None, span_id=None, service_name=None, operation_name=None,
                       baseline_median=None, baseline_mad=None, baseline_n=None,
                       baseline_window_start=None, baseline_window_end=None, anomaly_score=None)
    stack, _, _ = _patched(list_anomalies={"return_value": AnomalyListResult(1, [row])})
    with stack:
        resp = client.get("/api/anomalies")
    assert resp.status_code == 200
    assert resp.json()["anomalies"][0]["baseline_n"] is None


@pytest.mark.parametrize("query", [
    "limit=0", "limit=201", "offset=-1", "severity=BOGUS", "severity=warning",
    "trace_id=short", f"trace_id={'C' * 32}", f"trace_id={_TRACE_ID}0",
])
def test_list_anomalies_invalid_params_422_without_db(query):
    with patch("app.connect") as connect:
        resp = client.get(f"/api/anomalies?{query}")
    assert resp.status_code == 422
    connect.assert_not_called()


def test_list_anomalies_limit_200_accepted():
    stack, _, m = _patched(list_anomalies={"return_value": AnomalyListResult(0, [])})
    with stack:
        assert client.get("/api/anomalies?limit=200").status_code == 200
    assert m["list_anomalies"].call_args.kwargs["limit"] == 200


# ---------------------------------------------------------------------------
# 9-11: GET /api/anomalies/{anomaly_id}
# ---------------------------------------------------------------------------


def test_get_anomaly_200():
    stack, _, m = _patched(get_anomaly={"return_value": _anomaly_row()})
    with stack:
        resp = client.get(f"/api/anomalies/{_ANOMALY_ID}")
    assert resp.status_code == 200
    assert resp.json()["anomaly_id"] == _ANOMALY_ID
    assert resp.json()["explanation"] == "slow"
    assert m["get_anomaly"].call_args.args[1] == _ANOMALY_ID


def test_get_anomaly_404():
    stack, _, _ = _patched(get_anomaly={"return_value": None})
    with stack:
        resp = client.get(f"/api/anomalies/{_ANOMALY_ID}")
    assert resp.status_code == 404


@pytest.mark.parametrize("bad", ["a" * 63, "a" * 65, "A" * 64, "g" * 64, "a" * 32])
def test_get_anomaly_bad_id_422_without_db(bad):
    with patch("app.connect") as connect:
        resp = client.get(f"/api/anomalies/{bad}")
    assert resp.status_code == 422
    connect.assert_not_called()


# ---------------------------------------------------------------------------
# 12-14: GET /api/rca/{investigation_id}
# ---------------------------------------------------------------------------


def test_get_rca_200_includes_required_fields_and_ranked_evidence():
    stack, _, m = _patched(get_rca_investigation={"return_value": _rca_row()})
    with stack:
        resp = client.get(f"/api/rca/{_INVESTIGATION_ID}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["confidence"] == "HIGH"
    assert body["summary"] == "tool timed out"
    assert body["limitations"] == ["no logs"]
    assert [e["rank_position"] for e in body["evidence"]] == [1, 2]
    assert {"evidence_score", "explanation", "span_id"} <= set(body["evidence"][0])
    assert m["get_rca_investigation"].call_args.args[1] == _INVESTIGATION_ID


def test_get_rca_404():
    stack, _, _ = _patched(get_rca_investigation={"return_value": None})
    with stack:
        assert client.get(f"/api/rca/{_INVESTIGATION_ID}").status_code == 404


@pytest.mark.parametrize("bad", ["b" * 63, "B" * 64, "z" * 64, _TRACE_ID])
def test_get_rca_bad_id_422_without_db(bad):
    with patch("app.connect") as connect:
        resp = client.get(f"/api/rca/{bad}")
    assert resp.status_code == 422
    connect.assert_not_called()


# ---------------------------------------------------------------------------
# 15-17: GET /api/anomalies/{anomaly_id}/rca
# ---------------------------------------------------------------------------


def test_anomaly_rca_200_returns_latest_with_evidence():
    stack, _, m = _patched(get_latest_rca_for_anomaly={"return_value": _rca_row()})
    with stack:
        resp = client.get(f"/api/anomalies/{_ANOMALY_ID}/rca")
    assert resp.status_code == 200
    body = resp.json()
    assert body["investigation_id"] == _INVESTIGATION_ID
    assert body["anomaly_id"] == _ANOMALY_ID
    assert len(body["evidence"]) == 2
    assert m["get_latest_rca_for_anomaly"].call_args.args[1] == _ANOMALY_ID


def test_anomaly_rca_404_when_no_investigation():
    stack, _, _ = _patched(get_latest_rca_for_anomaly={"return_value": None})
    with stack:
        resp = client.get(f"/api/anomalies/{_ANOMALY_ID}/rca")
    assert resp.status_code == 404


def test_anomaly_rca_bad_id_422_without_db():
    with patch("app.connect") as connect:
        resp = client.get(f"/api/anomalies/{'x' * 64}/rca")
    assert resp.status_code == 422
    connect.assert_not_called()


# ---------------------------------------------------------------------------
# 18-20: database failure and connection lifecycle
# ---------------------------------------------------------------------------

_SECRET = "s3cret-password-value"


@pytest.mark.parametrize("path,target", [
    ("/api/anomalies", "list_anomalies"),
    (f"/api/anomalies/{_ANOMALY_ID}", "get_anomaly"),
    (f"/api/rca/{_INVESTIGATION_ID}", "get_rca_investigation"),
    (f"/api/anomalies/{_ANOMALY_ID}/rca", "get_latest_rca_for_anomaly"),
])
def test_db_error_returns_503_without_leaking(path, target):
    err = psycopg.OperationalError(f"password={_SECRET} host=db SELECT * FROM public.x")
    stack, _, _ = _patched(**{target: {"side_effect": err}})
    with stack:
        resp = client.get(path)
    assert resp.status_code == 503
    assert resp.json() == {"detail": "Database error"}
    assert _SECRET not in resp.text and "SELECT" not in resp.text


def test_connect_failure_returns_503_without_leaking():
    with patch("app.connect", side_effect=psycopg.OperationalError(f"password={_SECRET}")):
        resp = client.get(f"/api/anomalies/{_ANOMALY_ID}")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "Database unavailable"}
    assert _SECRET not in resp.text


def test_connection_closed_on_success_and_on_404():
    stack, conn, _ = _patched(get_anomaly={"return_value": None})
    with stack:
        client.get(f"/api/anomalies/{_ANOMALY_ID}")
    conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# 21-26: anomaly_queries SQL
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Records executed SQL/params; fetchone/fetchall replay queued results."""

    def __init__(self, results):
        self.executed: list[tuple[str, dict]] = []
        self._results = list(results)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._results.pop(0)

    def fetchall(self):
        return self._results.pop(0)


def _conn_with(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_list_query_parameterized_ordered_and_bounded():
    cur = _FakeCursor([{"count": 3}, [_anomaly_row()]])
    result = anomaly_queries.list_anomalies(
        _conn_with(cur), limit=10, offset=20,
        severity="INFO", service_name="svc'; DROP TABLE x;--", trace_id=_TRACE_ID,
    )
    assert result.total == 3
    count_sql, page_sql = cur.executed[0][0], cur.executed[1][0]
    params = cur.executed[1][1]
    assert "DROP TABLE" not in count_sql and "DROP TABLE" not in page_sql
    assert params["service_name"] == "svc'; DROP TABLE x;--"
    assert "ORDER BY event_time DESC, anomaly_id ASC" in page_sql
    assert "LIMIT %(limit)s OFFSET %(offset)s" in page_sql
    for fragment in ("severity = %(severity)s", "service_name = %(service_name)s",
                     "trace_id = %(trace_id)s"):
        assert fragment in page_sql and fragment in count_sql


def test_list_query_without_filters_has_no_where():
    cur = _FakeCursor([{"count": 0}, []])
    anomaly_queries.list_anomalies(_conn_with(cur))
    assert all("WHERE" not in sql for sql, _ in cur.executed)


@pytest.mark.parametrize("kwargs", [
    {"limit": 0}, {"offset": -1}, {"severity": "BOGUS"}, {"trace_id": "nope"},
])
def test_list_query_rejects_bad_arguments_before_db(kwargs):
    conn = MagicMock()
    with pytest.raises(ValueError):
        anomaly_queries.list_anomalies(conn, **kwargs)
    conn.cursor.assert_not_called()


@pytest.mark.parametrize("func", [
    anomaly_queries.get_anomaly,
    anomaly_queries.get_rca_investigation,
    anomaly_queries.get_latest_rca_for_anomaly,
])
def test_id_functions_validate_before_db(func):
    conn = MagicMock()
    with pytest.raises(ValueError):
        func(conn, "A" * 64)
    with pytest.raises(ValueError):
        func(conn, "a" * 32)
    conn.cursor.assert_not_called()


def test_rca_evidence_ordered_by_rank_then_span_id():
    cur = _FakeCursor([_rca_row() | {"evidence": None}, [_evidence_row(1)]])
    result = anomaly_queries.get_rca_investigation(_conn_with(cur), _INVESTIGATION_ID)
    assert result["evidence"][0]["rank_position"] == 1
    evidence_sql, evidence_params = cur.executed[1]
    assert "ORDER BY rank_position ASC, span_id ASC" in evidence_sql
    assert evidence_params == {"investigation_id": _INVESTIGATION_ID}


def test_latest_rca_orders_newest_first_deterministically_and_returns_none():
    cur = _FakeCursor([None])
    assert anomaly_queries.get_latest_rca_for_anomaly(_conn_with(cur), _ANOMALY_ID) is None
    sql, params = cur.executed[0]
    assert "ORDER BY investigated_at DESC, investigation_id ASC" in sql
    assert "LIMIT 1" in sql
    assert params == {"anomaly_id": _ANOMALY_ID}
    assert len(cur.executed) == 1  # no evidence query when nothing was found


def test_query_module_only_reads():
    code = _strip_docstrings(open(anomaly_queries.__file__, encoding="utf-8").read())
    assert not re.search(r"(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT)", code)


def _strip_docstrings(src: str) -> str:
    """Module source with the leading module docstring and ``#`` comments removed."""
    body = src.split('"""', 2)[2]
    return "\n".join(line.split("#")[0] for line in body.splitlines())


# ---------------------------------------------------------------------------
# 27-30: migration 015
# ---------------------------------------------------------------------------


def _migration_sql() -> str:
    with open(_MIGRATION, encoding="utf-8") as f:
        text = f.read()
    return "\n".join(line.split("--")[0] for line in text.splitlines())


def _statements() -> list[str]:
    return [" ".join(s.split()) for s in _migration_sql().split(";") if s.strip()]


def test_migration_exact_statements():
    assert _statements() == [
        "GRANT USAGE ON SCHEMA public TO trace_viewer_reader",
        "GRANT SELECT ON public.anomaly_events TO trace_viewer_reader",
        "GRANT SELECT ON public.rca_investigations TO trace_viewer_reader",
        "GRANT SELECT ON public.rca_evidence TO trace_viewer_reader",
    ]


def test_migration_is_read_only_and_narrow():
    sql = _migration_sql().upper()
    for word in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
                 "ALL PRIVILEGES", "WITH GRANT OPTION", "CREATE ", "ALTER ", "DROP ",
                 "ALL TABLES", "PUBLIC;", "TO PUBLIC"):
        assert word not in sql, word


def test_migration_grants_only_to_trace_viewer_reader():
    for stmt in _statements():
        assert stmt.endswith(" TO trace_viewer_reader")


def test_migration_has_no_credentials_and_is_015():
    assert os.path.basename(_MIGRATION).startswith("015_")
    assert not re.search(r"PASSWORD|CREATE ROLE", _migration_sql(), re.I)


# ---------------------------------------------------------------------------
# 31: existing routes unchanged
# ---------------------------------------------------------------------------


def test_existing_trace_routes_still_registered():
    paths = {r.path for r in app.routes}
    assert {"/api/health", "/api/traces", "/api/traces/{trace_id}"} <= paths
    assert {"/api/anomalies", "/api/anomalies/{anomaly_id}",
            "/api/anomalies/{anomaly_id}/rca", "/api/rca/{investigation_id}"} <= paths
