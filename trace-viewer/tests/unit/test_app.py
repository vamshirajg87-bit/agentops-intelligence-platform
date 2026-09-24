"""
trace-viewer/tests/unit/test_app.py

TestClient unit tests for trace-viewer/app.py.

No live database.  All psycopg.Connection objects are MagicMock instances
obtained by patching app.connect.  No dependency_overrides are used — the
DB connection lives inside route bodies, so patching app.connect is both
necessary and sufficient.

The key architectural invariant under test:
    FastAPI validates query/path parameters BEFORE the route body runs.
    Because get_db() is a contextmanager inside the route body (not a
    Depends), malformed requests return 422 without ever calling connect().

Coverage targets
    1-2    GET /api/health
    3-9    GET /api/traces  — success
    10-14  GET /api/traces  — validation + DB error
    15-19  GET /api/traces/{trace_id}  — success + 404
    20-23  GET /api/traces/{trace_id}  — path validation
    24-25  GET /api/traces/{trace_id}  — DB error
    26-31  Connection lifecycle
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from fastapi.testclient import TestClient

from app import app
from queries import TraceListResult
from reconstruction import SpanRecord, TraceNode, TraceReconstruction

# ---------------------------------------------------------------------------
# Constants and factories
# ---------------------------------------------------------------------------

_DT = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
_VALID_TRACE_ID = "a" * 32

client = TestClient(app)


def _make_trace_row(**overrides) -> dict:
    row = {
        "trace_id": _VALID_TRACE_ID,
        "trace_start_time": _DT,
        "trace_end_time": _DT,
        "trace_duration_ms": 100.0,
        "root_span_id": "b" * 32,
        "root_span_name": "root-span",
        "root_service_name": "my-service",
        "request_id": None,
        "session_id": None,
        "span_count": 1,
        "service_count": 1,
        "error_span_count": 0,
        "agent_span_count": 0,
        "tool_span_count": 0,
        "retrieval_span_count": 0,
        "has_error": False,
    }
    row.update(overrides)
    return row


def _make_span_record(**overrides) -> SpanRecord:
    kwargs: dict = dict(
        span_id="c" * 32,
        parent_span_id=None,
        span_name="test-span",
        start_time=_DT,
        end_time=_DT,
        duration_ms=50.0,
        status_code="STATUS_CODE_OK",
    )
    kwargs.update(overrides)
    return SpanRecord(**kwargs)


def _make_reconstruction(span_count: int = 1, **overrides) -> TraceReconstruction:
    node = TraceNode(span=_make_span_record())
    kwargs: dict = dict(
        roots=[node],
        orphans=[],
        cycle_members=[],
        has_cycle=False,
        span_count=span_count,
    )
    kwargs.update(overrides)
    return TraceReconstruction(**kwargs)


# ---------------------------------------------------------------------------
# 1-2: GET /api/health  — no DB dependency
# ---------------------------------------------------------------------------


def test_health_returns_200():
    resp = client.get("/api/health")
    assert resp.status_code == 200


def test_health_response_shape():
    resp = client.get("/api/health")
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# 3-9: GET /api/traces  — success
# ---------------------------------------------------------------------------


def test_list_traces_default_params():
    """Default limit=50, offset=0, has_error=None are forwarded to list_traces."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=0, traces=[])
        client.get("/api/traces")
    mock_lt.assert_called_once_with(mock_conn, limit=50, offset=0, has_error=None)


def test_list_traces_echoes_limit_and_offset():
    """limit and offset from the query string appear in the response body."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=5, traces=[_make_trace_row()])
        resp = client.get("/api/traces?limit=10&offset=20")
    assert resp.status_code == 200
    assert resp.json()["limit"] == 10
    assert resp.json()["offset"] == 20


def test_list_traces_total_in_response():
    """total from TraceListResult is passed through to the response."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=42, traces=[])
        resp = client.get("/api/traces")
    assert resp.status_code == 200
    assert resp.json()["total"] == 42


def test_list_traces_has_error_true():
    """has_error=true is parsed as bool True and forwarded."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=1, traces=[_make_trace_row(has_error=True)])
        client.get("/api/traces?has_error=true")
    mock_lt.assert_called_once_with(mock_conn, limit=50, offset=0, has_error=True)


def test_list_traces_has_error_false():
    """has_error=false is parsed as bool False and forwarded."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=0, traces=[])
        client.get("/api/traces?has_error=false")
    mock_lt.assert_called_once_with(mock_conn, limit=50, offset=0, has_error=False)


def test_list_traces_empty_result():
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=0, traces=[])
        resp = client.get("/api/traces")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    assert resp.json()["traces"] == []


def test_list_traces_response_shape():
    """A single trace row is serialized with all expected fields."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces") as mock_lt:
        mock_lt.return_value = TraceListResult(total=1, traces=[_make_trace_row()])
        resp = client.get("/api/traces")
    assert resp.status_code == 200
    t = resp.json()["traces"][0]
    assert t["trace_id"] == _VALID_TRACE_ID
    assert t["span_count"] == 1
    assert t["has_error"] is False
    assert t["root_span_name"] == "root-span"
    assert t["service_count"] == 1


# ---------------------------------------------------------------------------
# 10-14: GET /api/traces  — validation + DB error
#
# Validation tests (10-13) require NO mock for connect().
# FastAPI validates query params before the route body runs — connect()
# is never called for malformed requests.
# ---------------------------------------------------------------------------


def test_list_traces_limit_zero_returns_422():
    resp = client.get("/api/traces?limit=0")
    assert resp.status_code == 422


def test_list_traces_limit_exceeds_max_returns_422():
    """limit > _MAX_LIMIT (200) returns 422."""
    resp = client.get("/api/traces?limit=201")
    assert resp.status_code == 422


def test_list_traces_negative_offset_returns_422():
    resp = client.get("/api/traces?offset=-1")
    assert resp.status_code == 422


def test_list_traces_invalid_has_error_returns_422():
    """A non-boolean has_error value returns 422."""
    resp = client.get("/api/traces?has_error=notabool")
    assert resp.status_code == 422


def test_list_traces_db_error_returns_503():
    """psycopg.Error raised by list_traces is caught by the global handler → 503."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.list_traces", side_effect=psycopg.OperationalError("db down")):
        resp = client.get("/api/traces")
    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database error"


# ---------------------------------------------------------------------------
# 15-19: GET /api/traces/{trace_id}  — success + 404
# ---------------------------------------------------------------------------


def test_get_trace_success():
    """A found trace returns 200 with trace_id, span_count, and has_cycle."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=_make_reconstruction(span_count=3)):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == _VALID_TRACE_ID
    assert body["span_count"] == 3
    assert body["has_cycle"] is False


def test_get_trace_response_shape():
    """The response includes all required top-level keys."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=_make_reconstruction()):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 200
    for key in ("trace_id", "span_count", "has_cycle", "roots", "orphans", "cycle_members"):
        assert key in resp.json(), f"missing key: {key}"


def test_get_trace_span_fields():
    """SpanResponse inside a root node exposes all SpanRecord required fields."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=_make_reconstruction()):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    root = resp.json()["roots"][0]
    span = root["span"]
    assert span["span_id"] == "c" * 32
    assert span["span_name"] == "test-span"
    assert span["status_code"] == "STATUS_CODE_OK"
    assert span["duration_ms"] == 50.0
    assert root["is_cycle_member"] is False


def test_get_trace_children_recursive():
    """Nested children in the tree are serialized recursively."""
    mock_conn = MagicMock()
    child = TraceNode(span=_make_span_record(span_id="d" * 32, parent_span_id="c" * 32))
    parent = TraceNode(span=_make_span_record(), children=[child])
    recon = TraceReconstruction(
        roots=[parent], orphans=[], cycle_members=[], has_cycle=False, span_count=2,
    )
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=recon):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    root = resp.json()["roots"][0]
    assert len(root["children"]) == 1
    assert root["children"][0]["span"]["span_id"] == "d" * 32


def test_get_trace_not_found_returns_404():
    """span_count == 0 in the reconstruction → 404 with trace_id in detail."""
    mock_conn = MagicMock()
    empty_recon = TraceReconstruction(
        roots=[], orphans=[], cycle_members=[], has_cycle=False, span_count=0,
    )
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=empty_recon):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 404
    assert _VALID_TRACE_ID in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 20-23: GET /api/traces/{trace_id}  — path parameter validation
#
# Validation tests require NO mock for connect().
# FastAPI validates path params before the route body runs.
# ---------------------------------------------------------------------------


def test_get_trace_invalid_id_uppercase_returns_422():
    """Uppercase hex characters don't match the lowercase-only pattern → 422."""
    resp = client.get(f"/api/traces/{'A' * 32}")
    assert resp.status_code == 422


def test_get_trace_invalid_id_too_short_returns_422():
    resp = client.get(f"/api/traces/{'a' * 31}")
    assert resp.status_code == 422


def test_get_trace_invalid_id_too_long_returns_422():
    resp = client.get(f"/api/traces/{'a' * 33}")
    assert resp.status_code == 422


def test_get_trace_invalid_id_non_hex_returns_422():
    """31 valid hex chars + 'g' (invalid hex digit) → 422."""
    resp = client.get(f"/api/traces/{'a' * 31}g")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 24-25: GET /api/traces/{trace_id}  — DB error
# ---------------------------------------------------------------------------


def test_get_trace_db_error_returns_503():
    """psycopg.DatabaseError raised by get_trace_reconstruction → 503."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", side_effect=psycopg.DatabaseError("query failed")):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 503
    assert resp.json()["detail"] == "Database error"


def test_get_trace_connection_error_returns_503():
    """psycopg.OperationalError from a dropped connection → 503."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", side_effect=psycopg.OperationalError("dropped")):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# 26-31: Connection lifecycle
# ---------------------------------------------------------------------------


def test_connect_not_called_for_invalid_limit():
    """
    Malformed request (limit=0): FastAPI returns 422 before the route body
    runs — connect() is never called.

    Proves: DB unavailable + malformed request = 422, not 503.
    """
    with patch("app.connect") as mock_connect:
        resp = client.get("/api/traces?limit=0")
    assert resp.status_code == 422
    mock_connect.assert_not_called()


def test_connect_not_called_for_invalid_trace_id():
    """
    Malformed trace_id: FastAPI returns 422 before the route body runs —
    connect() is never called.
    """
    with patch("app.connect") as mock_connect:
        resp = client.get(f"/api/traces/{'A' * 32}")
    assert resp.status_code == 422
    mock_connect.assert_not_called()


def test_connect_raises_psycopg_error_returns_503():
    """psycopg.Error from connect() itself → 503 with a safe, generic message."""
    with patch("app.connect", side_effect=psycopg.OperationalError("connection refused")):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 503
    body = resp.json()
    assert body["detail"] == "Database unavailable"
    # No credentials or connection strings in response
    assert "password" not in str(body).lower()
    assert "refused" not in str(body).lower()


def test_connect_fails_no_close_attempt():
    """If connect() fails, get_db never reaches the yield — close() is never called."""
    mock_connect = MagicMock(side_effect=RuntimeError("no password"))
    with patch("app.connect", mock_connect):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 503
    # The connection object that WOULD have been returned was never used
    mock_connect.return_value.close.assert_not_called()


def test_connection_closes_on_success():
    """get_db's finally block calls conn.close() after a successful request."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", return_value=_make_reconstruction()):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 200
    mock_conn.close.assert_called_once()


def test_connection_closes_on_query_exception():
    """get_db's finally block calls conn.close() even when a query raises psycopg.Error."""
    mock_conn = MagicMock()
    with patch("app.connect", return_value=mock_conn), \
         patch("app.get_trace_reconstruction", side_effect=psycopg.DatabaseError("boom")):
        resp = client.get(f"/api/traces/{_VALID_TRACE_ID}")
    assert resp.status_code == 503
    mock_conn.close.assert_called_once()
