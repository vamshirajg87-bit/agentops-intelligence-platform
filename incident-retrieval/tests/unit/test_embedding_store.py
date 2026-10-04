"""
incident-retrieval/tests/unit/test_embedding_store.py

Unit tests for embedding_store.py.

No live PostgreSQL required.  DB access is mocked with MagicMock.  The SQL
constants are read from the module and inspected as text, and the granted
columns are read from migration 011 on disk.

Test inventory:
    ST01  SQL shape: explicit columns, granted columns only, bound parameters
    ST02  SQL safety: no UPDATE / DELETE / TRUNCATE / SELECT * / DDL
    ST03  fetch_investigation
    ST04  fetch_evidence
    ST05  fetch_stored_document_text
    ST06  insert_embedding
    ST07  Transactions: the store never commits or rolls back
    ST08  Import boundary
"""

from __future__ import annotations

import ast
import os
import re
from unittest.mock import MagicMock

import psycopg.rows
import pytest

import embedding_store
from embedding_store import (
    EMBEDDING_COLUMN_DIMENSION,
    fetch_evidence,
    fetch_investigation,
    fetch_stored_document_text,
    insert_embedding,
)
from incident_document import EvidenceRow, InvestigationRow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMPONENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_MODULE_PATH = os.path.join(_COMPONENT_DIR, "embedding_store.py")
_MIGRATION_011 = os.path.join(
    _COMPONENT_DIR, "..", "storage-consumer", "migrations",
    "011_create_incident_retrieval_role.sql",
)
_MIGRATION_010 = os.path.join(
    _COMPONENT_DIR, "..", "storage-consumer", "migrations",
    "010_create_rca_investigation_embeddings.sql",
)

_INV = "a" * 64
_EMB = "e" * 64

_ALL_SQL = {
    "fetch_investigation": embedding_store._SQL_FETCH_INVESTIGATION,
    "fetch_evidence": embedding_store._SQL_FETCH_EVIDENCE,
    "fetch_stored_document_text": embedding_store._SQL_FETCH_STORED_DOCUMENT_TEXT,
    "insert_embedding": embedding_store._SQL_INSERT_EMBEDDING,
}

_INVESTIGATION_COLUMNS = [
    "investigation_id", "anomaly_type", "service_name", "operation_name", "confidence",
]

_EVIDENCE_COLUMNS = [
    "investigation_id", "rank_position", "evidence_type", "evidence_score",
    "span_name", "service_name", "status_code", "error_type", "tool_name",
    "tool_status", "agent_name", "agent_operation", "retrieval_result_count",
    "retrieval_top_relevance_score", "is_direct_subject", "is_root_span",
    "trace_duration_fraction",
]

_INSERT_COLUMNS = [
    "embedding_id", "investigation_id", "doc_version", "model_name",
    "model_revision", "document_text", "embedding",
]


def _normalise(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _select_columns(sql: str) -> list[str]:
    match = re.search(r"SELECT\s+(.*?)\s+FROM\s", sql, re.DOTALL | re.IGNORECASE)
    assert match, f"SELECT ... FROM not found in: {sql!r}"
    return [c.strip() for c in match.group(1).split(",")]


def _granted_columns(table: str) -> set[str]:
    """Columns granted to incident_retrieval on a table, read from migration 011."""
    with open(_MIGRATION_011, encoding="utf-8") as fh:
        sql = "\n".join(line.split("--")[0] for line in fh.read().splitlines())
    match = re.search(
        r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+" + re.escape(table) + r"\s+TO\s+incident_retrieval",
        sql, re.IGNORECASE,
    )
    assert match, f"column grant on {table} not found in migration 011"
    columns = [c.strip() for c in match.group(1).split(",")]
    assert len(columns) == len(set(columns))
    return set(columns)


def _make_conn(fetchone=None, fetchall=None, rowcount: int = 1):
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall if fetchall is not None else []
    cur.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _investigation_row(**overrides) -> dict:
    row = {
        "investigation_id": _INV,
        "anomaly_type": "tool_failure",
        "service_name": "agentops-demo-app",
        "operation_name": "tool.execute/ToolExecutionError",
        "confidence": "HIGH",
    }
    row.update(overrides)
    return row


def _evidence_row(rank: int = 1, **overrides) -> dict:
    row = {
        "investigation_id": _INV,
        "rank_position": rank,
        "evidence_type": "mixed",
        "evidence_score": 0.9,
        "span_name": "tool.execute",
        "service_name": "agentops-demo-app",
        "status_code": "ERROR",
        "error_type": "ToolExecutionError",
        "tool_name": None,
        "tool_status": None,
        "agent_name": "tool_agent",
        "agent_operation": "execute",
        "retrieval_result_count": None,
        "retrieval_top_relevance_score": None,
        "is_direct_subject": True,
        "is_root_span": False,
        "trace_duration_fraction": 0.0103,
    }
    row.update(overrides)
    return row


def _vector(dimension: int = 384) -> tuple[float, ...]:
    return tuple([0.05] * dimension)


def _insert(conn, **overrides) -> bool:
    kwargs = dict(
        embedding_id=_EMB,
        investigation_id=_INV,
        doc_version="1.0.0",
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        model_revision="1110a243fdf4706b3f48f1d95db1a4f5529b4d41",
        document_text="anomaly: tool_failure\nevidence: none",
        vector=_vector(),
    )
    kwargs.update(overrides)
    return insert_embedding(conn, **kwargs)


# ---------------------------------------------------------------------------
# ST01  SQL shape
# ---------------------------------------------------------------------------

class TestSqlShape:

    def test_investigation_columns_exact(self):
        assert _select_columns(_ALL_SQL["fetch_investigation"]) == _INVESTIGATION_COLUMNS

    def test_evidence_columns_exact(self):
        assert _select_columns(_ALL_SQL["fetch_evidence"]) == _EVIDENCE_COLUMNS

    def test_stored_text_columns_exact(self):
        assert _select_columns(_ALL_SQL["fetch_stored_document_text"]) == ["document_text"]

    def test_granted_column_counts_read_from_migration_011(self):
        """Guards the grant parser itself: 10 investigation and 17 evidence columns."""
        assert len(_granted_columns("public.rca_investigations")) == 10
        assert len(_granted_columns("public.rca_evidence")) == 17

    def test_investigation_columns_are_all_granted(self):
        assert set(_INVESTIGATION_COLUMNS) <= _granted_columns("public.rca_investigations")

    def test_evidence_columns_are_all_granted(self):
        assert set(_EVIDENCE_COLUMNS) <= _granted_columns("public.rca_evidence")

    def test_evidence_columns_are_exactly_the_granted_seventeen(self):
        assert set(_EVIDENCE_COLUMNS) == _granted_columns("public.rca_evidence")
        assert len(_EVIDENCE_COLUMNS) == 17

    def test_investigation_columns_match_investigation_row_fields(self):
        import dataclasses
        assert _INVESTIGATION_COLUMNS == [f.name for f in dataclasses.fields(InvestigationRow)]

    def test_evidence_columns_match_evidence_row_fields(self):
        import dataclasses
        assert _EVIDENCE_COLUMNS == [f.name for f in dataclasses.fields(EvidenceRow)]

    @pytest.mark.parametrize(
        "excluded",
        ["error_message", "explanation", "span_id", "parent_span_id", "is_error_span",
         "duration_ms", "depth", "evidence_score_breakdown", "anomaly_id",
         "analyzer_version", "investigated_at", "trace_id", "subject_span_id",
         "observed_value", "baseline_median", "anomaly_score"],
    )
    def test_ungranted_column_never_referenced(self, excluded):
        for name, sql in _ALL_SQL.items():
            assert not re.search(rf"\b{excluded}\b", sql), name

    def test_tables_are_schema_qualified(self):
        assert "FROM public.rca_investigations" in _ALL_SQL["fetch_investigation"]
        assert "FROM public.rca_evidence" in _ALL_SQL["fetch_evidence"]
        assert "FROM public.rca_investigation_embeddings" in _ALL_SQL["fetch_stored_document_text"]
        assert "INSERT INTO public.rca_investigation_embeddings" in _ALL_SQL["insert_embedding"]

    def test_only_the_three_expected_tables(self):
        tables = set()
        for sql in _ALL_SQL.values():
            tables.update(re.findall(r"\b(?:FROM|INTO|JOIN)\s+([\w.]+)", sql, re.IGNORECASE))
        assert tables == {
            "public.rca_investigations",
            "public.rca_evidence",
            "public.rca_investigation_embeddings",
        }

    def test_evidence_ordered_by_rank_position(self):
        assert _normalise(_ALL_SQL["fetch_evidence"]).endswith("ORDER BY rank_position ASC")

    def test_lookups_filter_by_bound_parameter(self):
        assert "WHERE investigation_id = %(investigation_id)s" in _ALL_SQL["fetch_investigation"]
        assert "WHERE investigation_id = %(investigation_id)s" in _ALL_SQL["fetch_evidence"]
        assert "WHERE embedding_id = %(embedding_id)s" in _ALL_SQL["fetch_stored_document_text"]

    def test_insert_columns_exact(self):
        match = re.search(r"INSERT INTO [\w.]+\s*\((.*?)\)\s*VALUES", _ALL_SQL["insert_embedding"], re.DOTALL)
        assert [c.strip() for c in match.group(1).split(",")] == _INSERT_COLUMNS

    def test_insert_values_are_bound_parameters_in_column_order(self):
        match = re.search(r"VALUES\s*\((.*?)\)\s*ON CONFLICT", _ALL_SQL["insert_embedding"], re.DOTALL)
        values = [v.strip() for v in match.group(1).split(",")]
        assert values == [
            "%(embedding_id)s", "%(investigation_id)s", "%(doc_version)s",
            "%(model_name)s", "%(model_revision)s", "%(document_text)s",
            "%(embedding)s::vector",
        ]

    def test_insert_casts_embedding_to_vector(self):
        assert "%(embedding)s::vector" in _ALL_SQL["insert_embedding"]

    def test_insert_does_not_supply_embedded_at(self):
        assert "embedded_at" not in _ALL_SQL["insert_embedding"]

    def test_insert_conflict_clause_is_the_four_column_contract(self):
        assert _normalise(_ALL_SQL["insert_embedding"]).endswith(
            "ON CONFLICT (investigation_id, doc_version, model_name, model_revision) DO NOTHING"
        )

    def test_conflict_columns_match_migration_010_unique_constraint(self):
        with open(_MIGRATION_010, encoding="utf-8") as fh:
            migration = _normalise(fh.read())
        assert "UNIQUE (investigation_id, doc_version, model_name, model_revision)" in migration

    def test_insert_columns_exist_in_migration_010(self):
        with open(_MIGRATION_010, encoding="utf-8") as fh:
            migration = fh.read()
        for column in _INSERT_COLUMNS:
            assert re.search(rf"^\s*{column}\s+\S+", migration, re.MULTILINE), column

    def test_column_dimension_matches_migration_010(self):
        with open(_MIGRATION_010, encoding="utf-8") as fh:
            assert f"vector({EMBEDDING_COLUMN_DIMENSION})" in fh.read()
        assert EMBEDDING_COLUMN_DIMENSION == 384

    @pytest.mark.parametrize("name", sorted(_ALL_SQL))
    def test_every_placeholder_is_a_named_parameter(self, name):
        sql = _ALL_SQL[name]
        assert "%s" not in re.sub(r"%\(\w+\)s", "", sql)
        assert "{" not in sql and "}" not in sql


# ---------------------------------------------------------------------------
# ST02  SQL safety
# ---------------------------------------------------------------------------

class TestSqlSafety:

    @pytest.fixture(scope="class")
    def module_source(self) -> str:
        with open(_MODULE_PATH, encoding="utf-8") as fh:
            return fh.read()

    @pytest.mark.parametrize("name", sorted(_ALL_SQL))
    @pytest.mark.parametrize(
        "keyword",
        ["UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "CREATE", "GRANT", "COPY"],
    )
    def test_forbidden_keyword_absent(self, name, keyword):
        assert not re.search(rf"\b{keyword}\b", _ALL_SQL[name], re.IGNORECASE)

    @pytest.mark.parametrize("name", sorted(_ALL_SQL))
    def test_no_select_star(self, name):
        assert not re.search(r"SELECT\s+\*", _ALL_SQL[name], re.IGNORECASE)
        assert "*" not in _ALL_SQL[name]

    def test_no_do_update(self):
        assert "DO UPDATE" not in _normalise(_ALL_SQL["insert_embedding"]).upper()

    def test_exactly_four_sql_statements_in_module(self):
        tree = ast.parse(open(_MODULE_PATH, encoding="utf-8").read())
        constants = [
            node.targets[0].id for node in tree.body
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id.startswith("_SQL_")
        ]
        assert sorted(constants) == [
            "_SQL_FETCH_EVIDENCE", "_SQL_FETCH_INVESTIGATION",
            "_SQL_FETCH_STORED_DOCUMENT_TEXT", "_SQL_INSERT_EMBEDDING",
        ]

    def test_exactly_one_write_statement(self):
        writes = [n for n, sql in _ALL_SQL.items() if re.search(r"\bINSERT\b", sql, re.IGNORECASE)]
        assert writes == ["insert_embedding"]

    def test_no_f_string_or_format_sql(self, module_source):
        tree = ast.parse(module_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "execute":
                    assert isinstance(node.args[0], ast.Name), "execute() must take a SQL constant"
                    assert node.args[0].id.startswith("_SQL_")

    def test_no_out_of_scope_relations(self, module_source):
        executable = "\n".join(_ALL_SQL.values()).lower()
        for name in ("anomaly_events", "telemetry_spans", "analytics.", "anomaly_detector_runs", "_seq"):
            assert name not in executable


# ---------------------------------------------------------------------------
# ST03  fetch_investigation
# ---------------------------------------------------------------------------

class TestFetchInvestigation:

    def test_returns_investigation_row(self):
        conn, _ = _make_conn(fetchone=_investigation_row())
        assert fetch_investigation(conn, _INV) == InvestigationRow(
            investigation_id=_INV,
            anomaly_type="tool_failure",
            service_name="agentops-demo-app",
            operation_name="tool.execute/ToolExecutionError",
            confidence="HIGH",
        )

    def test_returns_none_when_absent(self):
        conn, _ = _make_conn(fetchone=None)
        assert fetch_investigation(conn, _INV) is None

    def test_null_service_and_operation_preserved(self):
        conn, _ = _make_conn(fetchone=_investigation_row(service_name=None, operation_name=None))
        row = fetch_investigation(conn, _INV)
        assert row.service_name is None and row.operation_name is None

    def test_executes_the_sql_constant_with_bound_id(self):
        conn, cur = _make_conn(fetchone=_investigation_row())
        fetch_investigation(conn, _INV)
        cur.execute.assert_called_once_with(
            embedding_store._SQL_FETCH_INVESTIGATION, {"investigation_id": _INV},
        )

    def test_uses_dict_row_cursor(self):
        conn, _ = _make_conn(fetchone=_investigation_row())
        fetch_investigation(conn, _INV)
        conn.cursor.assert_called_once_with(row_factory=psycopg.rows.dict_row)

    def test_values_are_not_modified(self):
        conn, _ = _make_conn(fetchone=_investigation_row(service_name="  Padded  ", confidence="LOW"))
        row = fetch_investigation(conn, _INV)
        assert row.service_name == "  Padded  " and row.confidence == "LOW"

    def test_database_error_propagates(self):
        conn, cur = _make_conn()
        cur.execute.side_effect = psycopg.OperationalError("boom")
        with pytest.raises(psycopg.OperationalError):
            fetch_investigation(conn, _INV)


# ---------------------------------------------------------------------------
# ST04  fetch_evidence
# ---------------------------------------------------------------------------

class TestFetchEvidence:

    def test_returns_evidence_rows_in_cursor_order(self):
        conn, _ = _make_conn(fetchall=[_evidence_row(1), _evidence_row(2, span_name="b"), _evidence_row(3)])
        rows = fetch_evidence(conn, _INV)
        assert [type(r) for r in rows] == [EvidenceRow] * 3
        assert [r.rank_position for r in rows] == [1, 2, 3]
        assert rows[1].span_name == "b"

    def test_empty_list_when_no_evidence(self):
        conn, _ = _make_conn(fetchall=[])
        assert fetch_evidence(conn, _INV) == []

    def test_full_row_mapping(self):
        conn, _ = _make_conn(fetchall=[_evidence_row(
            7, evidence_type="duration", evidence_score=0.25, span_name="s", service_name="svc",
            status_code="UNSET", error_type=None, tool_name="t", tool_status="ok",
            agent_name="a", agent_operation="op", retrieval_result_count=3,
            retrieval_top_relevance_score=0.4, is_direct_subject=False, is_root_span=True,
            trace_duration_fraction=1.2,
        )])
        assert fetch_evidence(conn, _INV) == [EvidenceRow(
            investigation_id=_INV, rank_position=7, evidence_type="duration",
            evidence_score=0.25, span_name="s", service_name="svc", status_code="UNSET",
            error_type=None, tool_name="t", tool_status="ok", agent_name="a",
            agent_operation="op", retrieval_result_count=3,
            retrieval_top_relevance_score=0.4, is_direct_subject=False, is_root_span=True,
            trace_duration_fraction=1.2,
        )]

    def test_nulls_preserved(self):
        conn, _ = _make_conn(fetchall=[_evidence_row()])
        row = fetch_evidence(conn, _INV)[0]
        assert row.tool_name is None and row.retrieval_result_count is None

    def test_executes_the_sql_constant_with_bound_id(self):
        conn, cur = _make_conn(fetchall=[])
        fetch_evidence(conn, _INV)
        cur.execute.assert_called_once_with(
            embedding_store._SQL_FETCH_EVIDENCE, {"investigation_id": _INV},
        )

    def test_uses_dict_row_cursor(self):
        conn, _ = _make_conn(fetchall=[])
        fetch_evidence(conn, _INV)
        conn.cursor.assert_called_once_with(row_factory=psycopg.rows.dict_row)

    def test_returns_a_list(self):
        conn, _ = _make_conn(fetchall=[_evidence_row()])
        assert type(fetch_evidence(conn, _INV)) is list


# ---------------------------------------------------------------------------
# ST05  fetch_stored_document_text
# ---------------------------------------------------------------------------

class TestFetchStoredDocumentText:

    def test_returns_text(self):
        conn, _ = _make_conn(fetchone={"document_text": "anomaly: latency\nevidence: none"})
        assert fetch_stored_document_text(conn, _EMB) == "anomaly: latency\nevidence: none"

    def test_returns_none_when_absent(self):
        conn, _ = _make_conn(fetchone=None)
        assert fetch_stored_document_text(conn, _EMB) is None

    def test_text_returned_unmodified(self):
        text = "  café \U0001F680\nline two  "
        conn, _ = _make_conn(fetchone={"document_text": text})
        assert fetch_stored_document_text(conn, _EMB) == text

    def test_executes_the_sql_constant_with_bound_id(self):
        conn, cur = _make_conn(fetchone=None)
        fetch_stored_document_text(conn, _EMB)
        cur.execute.assert_called_once_with(
            embedding_store._SQL_FETCH_STORED_DOCUMENT_TEXT, {"embedding_id": _EMB},
        )


# ---------------------------------------------------------------------------
# ST06  insert_embedding
# ---------------------------------------------------------------------------

class TestInsertEmbedding:

    def test_returns_true_when_row_inserted(self):
        conn, _ = _make_conn(rowcount=1)
        assert _insert(conn) is True

    def test_returns_false_on_conflict(self):
        conn, _ = _make_conn(rowcount=0)
        assert _insert(conn) is False

    def test_executes_the_sql_constant(self):
        conn, cur = _make_conn()
        _insert(conn)
        assert cur.execute.call_count == 1
        assert cur.execute.call_args.args[0] is embedding_store._SQL_INSERT_EMBEDDING

    def test_parameter_keys_exact(self):
        conn, cur = _make_conn()
        _insert(conn)
        assert set(cur.execute.call_args.args[1]) == set(_INSERT_COLUMNS)

    def test_parameters_passed_through_unchanged(self):
        conn, cur = _make_conn()
        _insert(conn, document_text="  text \U0001F680\nline  ")
        params = cur.execute.call_args.args[1]
        assert params["embedding_id"] == _EMB
        assert params["investigation_id"] == _INV
        assert params["doc_version"] == "1.0.0"
        assert params["model_name"] == "sentence-transformers/all-MiniLM-L6-v2"
        assert params["model_revision"] == "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
        assert params["document_text"] == "  text \U0001F680\nline  "

    def test_embedding_bound_as_vector_literal(self):
        vector = tuple((i - 192) / 1000.0 for i in range(384))
        conn, cur = _make_conn()
        _insert(conn, vector=vector)
        literal = cur.execute.call_args.args[1]["embedding"]
        assert type(literal) is str
        assert literal.startswith("[") and literal.endswith("]")
        assert tuple(float(x) for x in literal[1:-1].split(",")) == vector

    def test_no_embedded_at_parameter(self):
        conn, cur = _make_conn()
        _insert(conn)
        assert "embedded_at" not in cur.execute.call_args.args[1]

    @pytest.mark.parametrize("dimension", [0, 1, 383, 385, 768])
    def test_wrong_dimension_raises_before_any_sql(self, dimension):
        conn, cur = _make_conn()
        with pytest.raises(ValueError, match="384 elements"):
            _insert(conn, vector=_vector(dimension))
        conn.cursor.assert_not_called()
        cur.execute.assert_not_called()

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), None, True, "0.1"],
                             ids=["nan", "inf", "none", "bool", "str"])
    def test_bad_vector_value_raises_before_any_sql(self, bad):
        vector = list(_vector())
        vector[5] = bad
        conn, cur = _make_conn()
        with pytest.raises(ValueError):
            _insert(conn, vector=vector)
        cur.execute.assert_not_called()

    def test_arguments_are_keyword_only(self):
        conn, _ = _make_conn()
        with pytest.raises(TypeError):
            insert_embedding(conn, _EMB, _INV, "1.0.0", "m", "r", "text", _vector())  # type: ignore[misc]

    def test_vector_is_not_mutated(self):
        vector = list(_vector())
        snapshot = list(vector)
        conn, _ = _make_conn()
        _insert(conn, vector=vector)
        assert vector == snapshot

    def test_database_error_propagates(self):
        conn, cur = _make_conn()
        cur.execute.side_effect = psycopg.errors.ForeignKeyViolation("fk")
        with pytest.raises(psycopg.Error):
            _insert(conn)


# ---------------------------------------------------------------------------
# ST07  Transactions
# ---------------------------------------------------------------------------

class TestNoTransactionControl:

    def test_fetches_never_commit_or_rollback(self):
        conn, _ = _make_conn(fetchone=_investigation_row(), fetchall=[_evidence_row()])
        fetch_investigation(conn, _INV)
        fetch_evidence(conn, _INV)
        text_conn, _ = _make_conn(fetchone={"document_text": "text"})
        fetch_stored_document_text(text_conn, _EMB)
        for used in (conn, text_conn):
            used.commit.assert_not_called()
            used.rollback.assert_not_called()
            used.close.assert_not_called()

    def test_insert_never_commits_or_rolls_back(self):
        conn, _ = _make_conn()
        _insert(conn)
        conn.commit.assert_not_called()
        conn.rollback.assert_not_called()
        conn.close.assert_not_called()

    def test_insert_error_does_not_roll_back_here(self):
        conn, cur = _make_conn()
        cur.execute.side_effect = psycopg.OperationalError("boom")
        with pytest.raises(psycopg.OperationalError):
            _insert(conn)
        conn.rollback.assert_not_called()

    def test_module_never_calls_commit_rollback_connect_or_close(self):
        tree = ast.parse(open(_MODULE_PATH, encoding="utf-8").read())
        attributes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert attributes.isdisjoint({"commit", "rollback", "connect", "close"})


# ---------------------------------------------------------------------------
# ST08  Import boundary
# ---------------------------------------------------------------------------

def _imported_modules(path: str) -> set[str]:
    tree = ast.parse(open(path, encoding="utf-8").read())
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "relative imports are not allowed"
            modules.add((node.module or "").split(".")[0])
    return modules


class TestImportBoundary:

    def test_tested_module_is_the_incident_retrieval_one(self):
        assert os.path.abspath(embedding_store.__file__) == _MODULE_PATH

    def test_imports_exact(self):
        assert _imported_modules(_MODULE_PATH) == {
            "__future__", "typing", "psycopg", "embedding_record", "incident_document",
        }

    @pytest.mark.parametrize(
        "forbidden",
        ["rca_persist", "rca_source", "rca_models", "rca_confidence", "rca_ranker",
         "pgvector", "numpy", "torch", "sentence_transformers",
         "sentence_transformer_backend", "embedding_provider", "embedding_pipeline",
         "retrieval_db", "os"],
    )
    def test_forbidden_module_not_imported(self, forbidden):
        assert forbidden not in _imported_modules(_MODULE_PATH)

    @pytest.mark.parametrize(
        "module", ["incident_signal.py", "embedding_record.py", "embedding_pipeline.py",
                   "embedding_provider.py", "incident_document.py",
                   "sentence_transformer_backend.py"],
    )
    def test_other_modules_do_not_import_psycopg(self, module):
        assert "psycopg" not in _imported_modules(os.path.join(_COMPONENT_DIR, module))

    def test_only_store_and_db_modules_import_psycopg(self):
        importers = sorted(
            name for name in os.listdir(_COMPONENT_DIR)
            if name.endswith(".py")
            and "psycopg" in _imported_modules(os.path.join(_COMPONENT_DIR, name))
        )
        assert importers == ["embedding_store.py", "retrieval_db.py"]

    def test_only_the_store_contains_sql(self):
        for name in os.listdir(_COMPONENT_DIR):
            if not name.endswith(".py") or name == "embedding_store.py":
                continue
            tree = ast.parse(open(os.path.join(_COMPONENT_DIR, name), encoding="utf-8").read())
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    assert node.func.attr not in {"execute", "executemany"}, name
