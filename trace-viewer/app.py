"""
trace-viewer/app.py

FastAPI application for the AgentOps Trace Viewer API.

Routing layer only — no SQL, no trace-tree reconstruction.
All data access is delegated to Phase 9.3 (queries.py).
All tree building is delegated to Phase 9.2 (reconstruction.py).

HTTP request
    ↓
FastAPI validates query/path parameters  ← 422 returned here if malformed
    ↓
Route body: get_db() opens connection    ← 503 returned here if DB unavailable
    ↓
Phase 9.3 query layer  (queries.py)
    ↓
Phase 9.2 reconstruction  (reconstruction.py)
    ↓
JSON response

Dependency-ordering note:
    FastAPI resolves Depends() before validating route-function parameters
    (query/path).  Placing the DB connection in a contextmanager inside the
    route body — rather than as Depends(get_db) — guarantees that a malformed
    request returns 422 before a DB connection is ever attempted.
"""

from __future__ import annotations

import contextlib
import logging
import pathlib
from contextlib import contextmanager
from datetime import datetime
from typing import Annotated, Generator

import psycopg
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from db import connect
from queries import get_trace_reconstruction, list_traces
from reconstruction import SpanRecord, TraceNode, TraceReconstruction

logger = logging.getLogger(__name__)

# Maximum allowed value for the `limit` query parameter.
_MAX_LIMIT: int = 200

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AgentOps Trace Viewer API",
    version="0.1.0",
)

_STATIC_DIR = pathlib.Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str


class TraceSummary(BaseModel):
    """One trace row from analytics.int_trace_spans."""

    trace_id: str
    trace_start_time: datetime
    trace_end_time: datetime
    trace_duration_ms: float
    root_span_id: str | None
    root_span_name: str | None
    root_service_name: str | None
    request_id: str | None
    session_id: str | None
    span_count: int
    service_count: int
    error_span_count: int
    agent_span_count: int
    tool_span_count: int
    retrieval_span_count: int
    has_error: bool


class TraceListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    traces: list[TraceSummary]


class SpanResponse(BaseModel):
    """Serialized SpanRecord — mirrors all 22 Phase 9.2 SpanRecord fields."""

    # Required fields (7)
    span_id: str
    parent_span_id: str | None
    span_name: str
    start_time: datetime
    end_time: datetime
    duration_ms: float
    status_code: str
    # Optional fields (15)
    trace_id: str | None = None
    service_name: str | None = None
    span_kind: str | None = None
    status_message: str | None = None
    request_id: str | None = None
    session_id: str | None = None
    gen_ai_operation: str | None = None
    agent_name: str | None = None
    agent_operation: str | None = None
    tool_name: str | None = None
    tool_status: str | None = None
    retrieval_result_count: int | None = None
    retrieval_top_relevance_score: float | None = None
    error_type: str | None = None
    error_message: str | None = None

    @classmethod
    def from_span_record(cls, span: SpanRecord) -> SpanResponse:
        return cls(
            span_id=span.span_id,
            parent_span_id=span.parent_span_id,
            span_name=span.span_name,
            start_time=span.start_time,
            end_time=span.end_time,
            duration_ms=span.duration_ms,
            status_code=span.status_code,
            trace_id=span.trace_id,
            service_name=span.service_name,
            span_kind=span.span_kind,
            status_message=span.status_message,
            request_id=span.request_id,
            session_id=span.session_id,
            gen_ai_operation=span.gen_ai_operation,
            agent_name=span.agent_name,
            agent_operation=span.agent_operation,
            tool_name=span.tool_name,
            tool_status=span.tool_status,
            retrieval_result_count=span.retrieval_result_count,
            retrieval_top_relevance_score=span.retrieval_top_relevance_score,
            error_type=span.error_type,
            error_message=span.error_message,
        )


class TraceNodeResponse(BaseModel):
    """One node in the reconstructed trace tree (recursive)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    span: SpanResponse
    children: list[TraceNodeResponse]
    is_cycle_member: bool

    @classmethod
    def from_trace_node(cls, node: TraceNode) -> TraceNodeResponse:
        return cls(
            span=SpanResponse.from_span_record(node.span),
            children=[cls.from_trace_node(c) for c in node.children],
            is_cycle_member=node.is_cycle_member,
        )


# Required for Pydantic v2 to resolve the self-referential `children` field.
TraceNodeResponse.model_rebuild()


class TraceDetailResponse(BaseModel):
    """Full reconstructed trace returned by GET /api/traces/{trace_id}."""

    trace_id: str
    span_count: int
    has_cycle: bool
    roots: list[TraceNodeResponse]
    orphans: list[TraceNodeResponse]
    cycle_members: list[TraceNodeResponse]

    @classmethod
    def from_reconstruction(
        cls,
        trace_id: str,
        result: TraceReconstruction,
    ) -> TraceDetailResponse:
        return cls(
            trace_id=trace_id,
            span_count=result.span_count,
            has_cycle=result.has_cycle,
            roots=[TraceNodeResponse.from_trace_node(n) for n in result.roots],
            orphans=[TraceNodeResponse.from_trace_node(n) for n in result.orphans],
            cycle_members=[TraceNodeResponse.from_trace_node(n) for n in result.cycle_members],
        )


# ---------------------------------------------------------------------------
# DB context manager
# ---------------------------------------------------------------------------


@contextmanager
def get_db() -> Generator[psycopg.Connection, None, None]:
    """
    Context manager that opens a psycopg connection for one request.

    Used inside route bodies (not as Depends) so that FastAPI validates
    query/path parameters before any DB connection is attempted.

    Connection parameters are read from TRACE_VIEWER_DB_* environment
    variables by Phase 9.3 db.connect().

    The connection is always closed in the finally block, whether the
    request succeeds or fails.

    If connect() itself fails (missing password → RuntimeError, or
    PostgreSQL unreachable → psycopg.Error), raises HTTPException(503)
    before yielding — no close() is attempted on a non-existent connection.

    Raises:
        HTTPException(503)  if the connection cannot be established.
    """
    try:
        conn = connect()
    except (RuntimeError, psycopg.Error) as exc:
        logger.error("DB connection failed: %s", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Database unavailable") from None
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(psycopg.Error)
async def database_error_handler(request: Request, exc: psycopg.Error) -> JSONResponse:
    """
    Catch any psycopg.Error that escapes a route body.

    Returns a generic 503 without leaking the password, connection string,
    SQL text, or internal stack trace.
    """
    logger.error("Database error during request: %s", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "Database error"})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Liveness check — does not depend on PostgreSQL."""
    return HealthResponse(status="ok")


# Path type that FastAPI validates before calling the route body.
# A trace_id that does not match returns 422 automatically.
_TraceIdPath = Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")]


@app.get("/api/traces", response_model=TraceListResponse)
def list_traces_endpoint(
    limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    has_error: Annotated[bool | None, Query()] = None,
) -> TraceListResponse:
    """
    Return a paginated list of traces from analytics.int_trace_spans.

    Query parameters:
        limit      1–200, default 50
        offset     >= 0, default 0
        has_error  optional boolean filter; omit to return all traces

    Response:
        total   — total matching traces before pagination
        limit   — echoed from request
        offset  — echoed from request
        traces  — rows for this page
    """
    with get_db() as conn:
        result = list_traces(conn, limit=limit, offset=offset, has_error=has_error)
    return TraceListResponse(
        total=result.total,
        limit=limit,
        offset=offset,
        traces=[TraceSummary.model_validate(row) for row in result.traces],
    )


@app.get("/api/traces/{trace_id}", response_model=TraceDetailResponse)
def get_trace_endpoint(
    trace_id: _TraceIdPath,
) -> TraceDetailResponse:
    """
    Return the full reconstructed trace tree for trace_id.

    Delegates to Phase 9.3 fetch + Phase 9.2 reconstruction.
    Returns 404 if the trace_id is syntactically valid but not found.
    """
    with get_db() as conn:
        try:
            result = get_trace_reconstruction(conn, trace_id)
        except ValueError:
            # Phase 9.3 validation is equivalent to the HTTP path pattern,
            # so this should never be reached via normal API usage.
            # Caught here as a defense-in-depth measure to prevent 500 leaks.
            raise HTTPException(
                status_code=422,
                detail=f"Invalid trace_id: {trace_id}",
            )
    if result.span_count == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Trace not found: {trace_id}",
        )
    return TraceDetailResponse.from_reconstruction(trace_id, result)
