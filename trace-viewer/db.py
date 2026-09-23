"""
trace-viewer/db.py

PostgreSQL connection factory for the trace viewer.

Reads connection parameters from TRACE_VIEWER_DB_* environment variables.
TRACE_VIEWER_DB_PASSWORD has no default and must be set explicitly before
calling connect().

The returned connection is synchronous (psycopg 3). The caller is responsible
for closing it.
"""

from __future__ import annotations

import os

import psycopg


def connect() -> psycopg.Connection:
    """
    Return a new psycopg 3 connection for trace viewer queries.

    Environment variables:
        TRACE_VIEWER_DB_HOST      default: localhost
        TRACE_VIEWER_DB_PORT      default: 5432
        TRACE_VIEWER_DB_NAME      default: agentops
        TRACE_VIEWER_DB_USER      default: trace_viewer_reader
        TRACE_VIEWER_DB_PASSWORD  required — no default

    Raises:
        RuntimeError            if TRACE_VIEWER_DB_PASSWORD is not set.
        psycopg.OperationalError  if the connection attempt fails.
    """
    password = os.environ.get("TRACE_VIEWER_DB_PASSWORD")
    if not password:
        raise RuntimeError(
            "TRACE_VIEWER_DB_PASSWORD environment variable is not set"
        )

    host = os.environ.get("TRACE_VIEWER_DB_HOST", "localhost")
    port = int(os.environ.get("TRACE_VIEWER_DB_PORT", "5432"))
    dbname = os.environ.get("TRACE_VIEWER_DB_NAME", "agentops")
    user = os.environ.get("TRACE_VIEWER_DB_USER", "trace_viewer_reader")

    return psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
