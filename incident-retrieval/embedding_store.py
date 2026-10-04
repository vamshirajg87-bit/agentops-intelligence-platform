"""
incident-retrieval/embedding_store.py

Phase 12.5: Database access for embedding persistence.

This module holds every SQL statement of the phase.  It reads the
investigation and evidence fields that the incident document needs and writes
one row to public.rca_investigation_embeddings.

Role
----
All statements run as the incident_retrieval role (migration 011) and use
only what that role is granted:

    public.rca_investigations            SELECT on selected columns
    public.rca_evidence                  SELECT on selected columns
    public.rca_investigation_embeddings  INSERT, SELECT

Idempotency
-----------
embedding rows:  ON CONFLICT (investigation_id, doc_version, model_name,
                 model_revision) DO NOTHING

First-write-wins: an existing row is never overwritten.  No UPDATE, no DELETE.

Transactions
------------
Functions here never commit or roll back.  The caller (embedding_pipeline)
owns the transaction and supplies an open connection, which it also closes.

SQL safety
----------
All values are %(name)s bound parameters.  No string interpolation.  Explicit
column lists.  No SELECT *.

The embedding is bound as text in pgvector's input format and cast with
::vector.  embedded_at is not supplied; the column has DEFAULT NOW().

Public API:
    EMBEDDING_COLUMN_DIMENSION    — dimension of the embedding column (384)
    fetch_investigation()         — one investigation, or None
    fetch_evidence()              — its evidence rows, rank_position ASC
    fetch_stored_document_text()  — document_text of an embedding, or None
    insert_embedding()            — insert one row; True if inserted
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import psycopg
import psycopg.rows

from embedding_record import format_vector_literal
from incident_document import EvidenceRow, InvestigationRow


#: public.rca_investigation_embeddings.embedding is vector(384).
EMBEDDING_COLUMN_DIMENSION: int = 384


# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

_SQL_FETCH_INVESTIGATION = """\
SELECT
    investigation_id,
    anomaly_type,
    service_name,
    operation_name,
    confidence
FROM public.rca_investigations
WHERE investigation_id = %(investigation_id)s
"""

_SQL_FETCH_EVIDENCE = """\
SELECT
    investigation_id,
    rank_position,
    evidence_type,
    evidence_score,
    span_name,
    service_name,
    status_code,
    error_type,
    tool_name,
    tool_status,
    agent_name,
    agent_operation,
    retrieval_result_count,
    retrieval_top_relevance_score,
    is_direct_subject,
    is_root_span,
    trace_duration_fraction
FROM public.rca_evidence
WHERE investigation_id = %(investigation_id)s
ORDER BY rank_position ASC
"""

_SQL_FETCH_STORED_DOCUMENT_TEXT = """\
SELECT
    document_text
FROM public.rca_investigation_embeddings
WHERE embedding_id = %(embedding_id)s
"""

# embedded_at is intentionally absent: the column has DEFAULT NOW() and is
# set by the database server.
_SQL_INSERT_EMBEDDING = """\
INSERT INTO public.rca_investigation_embeddings (
    embedding_id,
    investigation_id,
    doc_version,
    model_name,
    model_revision,
    document_text,
    embedding
) VALUES (
    %(embedding_id)s,
    %(investigation_id)s,
    %(doc_version)s,
    %(model_name)s,
    %(model_revision)s,
    %(document_text)s,
    %(embedding)s::vector
) ON CONFLICT (investigation_id, doc_version, model_name, model_revision) DO NOTHING"""


# ---------------------------------------------------------------------------
# Row mappers
# ---------------------------------------------------------------------------

def _row_to_investigation(row: dict[str, Any]) -> InvestigationRow:
    """Map a psycopg dict_row from public.rca_investigations."""
    return InvestigationRow(
        investigation_id=row["investigation_id"],
        anomaly_type=row["anomaly_type"],
        service_name=row["service_name"],
        operation_name=row["operation_name"],
        confidence=row["confidence"],
    )


def _row_to_evidence(row: dict[str, Any]) -> EvidenceRow:
    """Map a psycopg dict_row from public.rca_evidence."""
    return EvidenceRow(
        investigation_id=row["investigation_id"],
        rank_position=row["rank_position"],
        evidence_type=row["evidence_type"],
        evidence_score=row["evidence_score"],
        span_name=row["span_name"],
        service_name=row["service_name"],
        status_code=row["status_code"],
        error_type=row["error_type"],
        tool_name=row["tool_name"],
        tool_status=row["tool_status"],
        agent_name=row["agent_name"],
        agent_operation=row["agent_operation"],
        retrieval_result_count=row["retrieval_result_count"],
        retrieval_top_relevance_score=row["retrieval_top_relevance_score"],
        is_direct_subject=row["is_direct_subject"],
        is_root_span=row["is_root_span"],
        trace_duration_fraction=row["trace_duration_fraction"],
    )


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

def fetch_investigation(
    conn: psycopg.Connection,
    investigation_id: str,
) -> Optional[InvestigationRow]:
    """
    Fetch one investigation by investigation_id.

    Returns None if investigation_id does not exist.

    Raises:
        psycopg.Error  on any database error.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_SQL_FETCH_INVESTIGATION, {"investigation_id": investigation_id})
        row = cur.fetchone()

    if row is None:
        return None
    return _row_to_investigation(row)


def fetch_evidence(
    conn: psycopg.Connection,
    investigation_id: str,
) -> list[EvidenceRow]:
    """
    Fetch all evidence rows of an investigation, ordered rank_position ASC.

    Returns an empty list if the investigation has no evidence.

    Raises:
        psycopg.Error  on any database error.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_SQL_FETCH_EVIDENCE, {"investigation_id": investigation_id})
        rows = cur.fetchall()

    return [_row_to_evidence(row) for row in rows]


def fetch_stored_document_text(
    conn: psycopg.Connection,
    embedding_id: str,
) -> Optional[str]:
    """
    Return the document_text stored for embedding_id.

    Returns None if no embedding row with that embedding_id exists.

    Raises:
        psycopg.Error  on any database error.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_SQL_FETCH_STORED_DOCUMENT_TEXT, {"embedding_id": embedding_id})
        row = cur.fetchone()

    if row is None:
        return None
    return row["document_text"]


def insert_embedding(
    conn: psycopg.Connection,
    *,
    embedding_id: str,
    investigation_id: str,
    doc_version: str,
    model_name: str,
    model_revision: str,
    document_text: str,
    vector: Sequence[float],
) -> bool:
    """
    Insert one embedding row; first-write-wins.

    Returns True if the row was inserted, False if a row with the same
    (investigation_id, doc_version, model_name, model_revision) already
    existed, in which case nothing is changed.

    Does not commit.

    Raises:
        ValueError     if vector does not have EMBEDDING_COLUMN_DIMENSION
                       elements or holds a non-numeric or non-finite value.
                       Raised before any SQL is executed.
        psycopg.Error  on any database error.
    """
    if len(vector) != EMBEDDING_COLUMN_DIMENSION:
        raise ValueError(
            f"vector must have {EMBEDDING_COLUMN_DIMENSION} elements, "
            f"got {len(vector)}"
        )
    params = {
        "embedding_id": embedding_id,
        "investigation_id": investigation_id,
        "doc_version": doc_version,
        "model_name": model_name,
        "model_revision": model_revision,
        "document_text": document_text,
        "embedding": format_vector_literal(vector),
    }
    with conn.cursor() as cur:
        cur.execute(_SQL_INSERT_EMBEDDING, params)
        return cur.rowcount == 1
