"""
benchmarks/evidence_store.py

Offline evidence store (slice E1) for the future isolated AppContainer
runs: the immutable, hash-chained evidence record schema, chain
verification, recovery classification, and a writer that works ONLY
through an injected file-system interface (FsOps).

Nothing here touches a real file system, process or network: there is no
real FsOps adapter (EVIDENCE_WRITES_ENABLED is False and
require_real_writes() always refuses), the harness does not import this
module, and the writer only ever calls the object it is given.

Integrity model, stated precisely: a hash chain detects a deleted,
reordered, truncated or altered record.  It is NOT authentication - anyone
able to rewrite every record can recompute every hash.  The verified chain
tip digest is exposed so that it can later be anchored in separately
trusted evidence.  "Verified written" means written, flushed through FsOps,
closed, re-read and matched; it is not a claim of crash durability.

Standard library only.
"""

from __future__ import annotations

import hashlib
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import appcontainer_preflight as P  # noqa: E402

#: No real file-system adapter exists; this flag does not enable anything.
EVIDENCE_WRITES_ENABLED: bool = False
EVIDENCE_SCHEMA: str = "agentops-evidence/1"
WRITER_VERSION: str = "evidence-store/e1"
MAX_SEQUENCE: int = 999

RECORD_KINDS: tuple[str, ...] = (
    "prelaunch", "job_configured", "process_created", "job_verified_and_validated",
    "resume_intent", "resume_result", "execution_observation", "termination_intent",
    "termination_result", "final_inspection", "complete", "evidence_failure",
    "recovery_inspection",
)
#: The lifecycle stage each kind makes durable (None: no stage).
KIND_STAGES: Mapping[str, Optional[str]] = {
    "prelaunch": "prelaunch", "job_configured": "job_configured",
    "process_created": "process_created",
    "job_verified_and_validated": "job_verified_and_validated",
    "resume_intent": "resume_intent", "resume_result": "resume_result",
    "execution_observation": None, "termination_intent": "timeout_or_termination",
    "termination_result": "timeout_or_termination", "final_inspection": "final_inspection",
    "complete": "complete", "evidence_failure": None, "recovery_inspection": None,
}
#: Which kind may follow which.  The first record is always "prelaunch".
_AFTER_ANY_RUN = {"evidence_failure", "recovery_inspection"}
ALLOWED_NEXT: Mapping[str, frozenset[str]] = {
    "prelaunch": frozenset({"job_configured", "final_inspection"} | _AFTER_ANY_RUN),
    "job_configured": frozenset({"process_created", "final_inspection"} | _AFTER_ANY_RUN),
    "process_created": frozenset({"job_verified_and_validated", "termination_intent",
                                  "final_inspection"} | _AFTER_ANY_RUN),
    "job_verified_and_validated": frozenset({"resume_intent", "termination_intent",
                                             "final_inspection"} | _AFTER_ANY_RUN),
    "resume_intent": frozenset({"resume_result"} | _AFTER_ANY_RUN),
    "resume_result": frozenset({"execution_observation", "termination_intent",
                                "final_inspection"} | _AFTER_ANY_RUN),
    "execution_observation": frozenset({"execution_observation", "termination_intent",
                                        "final_inspection"} | _AFTER_ANY_RUN),
    "termination_intent": frozenset({"termination_result"} | _AFTER_ANY_RUN),
    "termination_result": frozenset({"final_inspection"} | _AFTER_ANY_RUN),
    "final_inspection": frozenset({"complete"} | _AFTER_ANY_RUN),
    "complete": frozenset(),
    "evidence_failure": frozenset({"recovery_inspection"}),
    "recovery_inspection": frozenset({"recovery_inspection"}),
}

#: Envelope fields that never change within one chain.
CONSTANT_FIELDS: tuple[str, ...] = (
    "run_id", "series_token_sha256", "expected_head", "object_format",
    "repository_manifest_sha256", "runtime_manifest_sha256", "parent",
)
ENVELOPE_FIELDS: tuple[str, ...] = CONSTANT_FIELDS + (
    "utc", "monotonic_ms", "context_sha256", "job", "process", "lifecycle", "integrity",
)
RECORD_FIELDS: frozenset[str] = frozenset(
    ("schema", "kind", "sequence", "previous_record_sha256", "body") + ENVELOPE_FIELDS)
LIFECYCLE_KEYS: frozenset[str] = frozenset(P.LifecycleState().as_dict())

_HEX64 = re.compile(r"[0-9a-f]{64}")
_HEX32 = re.compile(r"[0-9a-f]{32}")
_NAME_RE = re.compile(r"(\d{3})-([a-z_]+)\.json")


class EvidenceError(P.PreflightError):
    """An evidence record or chain is invalid (nothing was written)."""


class EvidenceDisabled(EvidenceError):
    """Real evidence writes are not available."""


class EvidenceFailed(EvidenceError):
    """A durable write failed; the writer is in the evidence-failure state."""


def require_real_writes() -> None:
    """Always refuses: slice E1 has no real file-system adapter."""
    if not EVIDENCE_WRITES_ENABLED:
        raise EvidenceDisabled("real evidence writes are not enabled "
                               "(EVIDENCE_WRITES_ENABLED is False)")
    raise EvidenceDisabled("no real file-system adapter exists in this slice")


def record_name(sequence: Any, kind: Any) -> str:
    """The immutable file name of one record, e.g. '004-resume_intent.json'."""
    if isinstance(sequence, bool) or not isinstance(sequence, int) \
            or not 0 <= sequence <= MAX_SEQUENCE:
        raise EvidenceError(f"invalid sequence {sequence!r}")
    if kind not in RECORD_KINDS:
        raise EvidenceError(f"unknown record kind {kind!r}")
    return f"{sequence:03d}-{kind}.json"


def record_digest(record: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical record bytes (identifies, never authenticates)."""
    return hashlib.sha256(P.canonical_json(record)).hexdigest()


def _identity(value: Any) -> Optional[tuple[int, str]]:
    if not isinstance(value, Mapping) or set(value) != {"pid", "creation_time"}:
        return None
    pid, creation = value["pid"], value["creation_time"]
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 \
            or P._parse_utc(creation) is None:
        return None
    return pid, creation


def record_problems(record: Any) -> list[str]:
    """Why one record is not well-formed (its own content only)."""
    if not isinstance(record, Mapping):
        return ["the record is not an object"]
    problems = []
    missing = sorted(RECORD_FIELDS - set(record))
    extra = sorted(set(record) - RECORD_FIELDS)
    if missing:
        problems.append("missing fields: " + ", ".join(missing))
    if extra:
        problems.append("unexpected fields: " + ", ".join(extra))
    if missing:
        return problems
    try:
        P.canonical_json(record)
    except P.PreflightError as exc:
        problems.append(f"not canonical: {exc}")
    if record["schema"] != EVIDENCE_SCHEMA:
        problems.append("wrong schema")
    kind = record["kind"]
    if kind not in RECORD_KINDS:
        problems.append(f"unknown kind {kind!r}")
    sequence = record["sequence"]
    if isinstance(sequence, bool) or not isinstance(sequence, int) \
            or not 0 <= sequence <= MAX_SEQUENCE:
        problems.append("invalid sequence")
    previous = record["previous_record_sha256"]
    if sequence == 0 and previous is not None:
        problems.append("the first record has a previous digest")
    if sequence != 0 and (not isinstance(previous, str) or not _HEX64.fullmatch(previous)):
        problems.append("previous_record_sha256 is malformed")
    if not isinstance(record["run_id"], str) or not P._RUN_ID_RE.fullmatch(record["run_id"]):
        problems.append("run_id is not an approved form")
    if P._parse_utc(record["utc"]) is None:
        problems.append("utc is malformed")
    monotonic = record["monotonic_ms"]
    if isinstance(monotonic, bool) or not isinstance(monotonic, int) or monotonic < 0:
        problems.append("monotonic_ms is malformed")
    for name in ("series_token_sha256", "repository_manifest_sha256",
                 "runtime_manifest_sha256"):
        if not isinstance(record[name], str) or not _HEX64.fullmatch(record[name]):
            problems.append(f"{name} is not a SHA-256")
    if record["context_sha256"] is not None and (
            not isinstance(record["context_sha256"], str)
            or not _HEX64.fullmatch(record["context_sha256"])):
        problems.append("context_sha256 is malformed")
    object_format = record["object_format"]
    length = {"sha1": 40, "sha256": 64}.get(object_format)
    if length is None:
        problems.append("unknown object format")
    elif not isinstance(record["expected_head"], str) or not re.fullmatch(
            rf"[0-9a-f]{{{length}}}", record["expected_head"]):
        problems.append(f"expected_head is not a {object_format} commit id")
    if _identity(record["parent"]) is None:
        problems.append("parent identity is malformed")
    if record["process"] is not None and _identity(record["process"]) is None:
        problems.append("process identity is malformed")
    job = record["job"]
    if not isinstance(job, Mapping) or set(job) != {"job_id", "limits_verified", "assignment"} \
            or (job["job_id"] is not None and (not isinstance(job["job_id"], str)
                                               or not _HEX32.fullmatch(job["job_id"]))) \
            or not isinstance(job["limits_verified"], bool) \
            or job["assignment"] not in P.JOB_ASSIGNMENTS:
        problems.append("job identity is malformed")
    lifecycle = record["lifecycle"]
    if not isinstance(lifecycle, Mapping) or set(lifecycle) != LIFECYCLE_KEYS \
            or lifecycle.get("execution_history") not in (None,) + P.EXECUTION_HISTORIES \
            or lifecycle.get("liveness") not in (None,) + P.LIVENESS_STATES:
        problems.append("lifecycle is malformed")
    integrity = record["integrity"]
    if not isinstance(integrity, Mapping) or integrity.get("writer_version") != WRITER_VERSION:
        problems.append("integrity block is malformed")
    body = record["body"]
    if not isinstance(body, Mapping):
        problems.append("body is not an object")
        return problems
    problems.extend(_body_problems(kind, record, body))
    return problems


def _body_problems(kind: str, record: Mapping[str, Any], body: Mapping[str, Any]) -> list[str]:
    problems = []
    if kind == "process_created":
        identity = _identity({k: body.get(k) for k in ("pid", "creation_time")})
        if identity is None or record["process"] is None \
                or _identity(record["process"]) != identity:
            problems.append("process_created must name the process in body and envelope")
    elif kind == "resume_result":
        if body.get("execution_history") not in P.EXECUTION_HISTORIES:
            problems.append("resume_result needs an execution history")
    elif kind == "termination_result":
        if not all(isinstance(body.get(k), bool) for k in ("verified", "api_ok", "signaled")) \
                or body.get("cause") not in P.TERMINATION_EXIT_CODES:
            problems.append("termination_result body is malformed")
        elif body["verified"] and not (body["api_ok"] and body["signaled"]
                                       and body.get("exit_code")
                                       == P.TERMINATION_EXIT_CODES[body["cause"]]):
            problems.append("termination_result claims verification without exit proof")
    elif kind == "final_inspection":
        if body.get("liveness") not in P.LIVENESS_STATES:
            problems.append("final_inspection needs a liveness")
    elif kind == "evidence_failure":
        if not isinstance(body.get("operation"), str) or body.get("failed_kind") \
                not in RECORD_KINDS:
            problems.append("evidence_failure body is malformed")
    elif kind == "complete":
        if body.get("ownership") not in ("released", "retained") \
                or not isinstance(body.get("chain"), list):
            problems.append("complete body is malformed")
        elif body["ownership"] == "released" and record["process"] is not None \
                and record["lifecycle"].get("liveness") != P.CONFIRMED_EXITED:
            problems.append("ownership released without a confirmed exit")
    return problems


def build_record(kind: str, envelope: Mapping[str, Any], body: Mapping[str, Any], *,
                 sequence: int, previous_record_sha256: Optional[str]) -> dict[str, Any]:
    """One validated record (pure)."""
    if not isinstance(envelope, Mapping) or set(envelope) != set(ENVELOPE_FIELDS):
        missing = sorted(set(ENVELOPE_FIELDS) - set(envelope or {}))
        extra = sorted(set(envelope or {}) - set(ENVELOPE_FIELDS))
        raise EvidenceError(f"malformed envelope (missing {missing}, unexpected {extra})")
    record = {"schema": EVIDENCE_SCHEMA, "kind": kind, "sequence": sequence,
              "previous_record_sha256": previous_record_sha256, "body": dict(body),
              **{k: envelope[k] for k in ENVELOPE_FIELDS}}
    problems = record_problems(record)
    if problems:
        raise EvidenceError("invalid record: " + "; ".join(problems))
    return record


@dataclass
class ChainState:
    """The verified prefix of a chain."""
    ok: bool
    problems: list[str]
    count: int
    last_verified_sequence: Optional[int]
    tip_digest: Optional[str]
    kinds: list[str]
    durable_stages: list[str]
    process: Optional[tuple[int, str]]
    complete: bool
    evidence_failure: bool
    resume_result_history: Optional[str]
    termination_verified: bool


def verify_chain(records: Sequence[Any], *, expected_run_id: Optional[str] = None,
                 expected_head: Optional[str] = None,
                 expected_token_sha256: Optional[str] = None) -> ChainState:
    """
    Verify an ordered list of parsed records.  Verification stops at the
    first problem; the state describes the verified prefix only.
    """
    problems: list[str] = []
    verified: list[Mapping[str, Any]] = []
    digests: list[str] = []
    first: Optional[Mapping[str, Any]] = None
    process: Optional[tuple[int, str]] = None
    context = job_id = None
    for index, record in enumerate(records):
        own = record_problems(record)
        if own:
            problems.append(f"record {index}: " + "; ".join(own))
            break
        if record["sequence"] != index:
            problems.append(f"record {index}: sequence {record['sequence']} (missing, "
                            "duplicate or reordered records)")
            break
        if index == 0:
            if record["kind"] != "prelaunch":
                problems.append("the chain does not start with prelaunch")
                break
            first = record
            for label, wanted, actual in (
                    ("run_id", expected_run_id, record["run_id"]),
                    ("HEAD", expected_head, record["expected_head"]),
                    ("owner digest", expected_token_sha256, record["series_token_sha256"])):
                if wanted is not None and actual != wanted:
                    problems.append(f"unexpected {label}")
            if problems:
                break
        else:
            if record["previous_record_sha256"] != digests[-1]:
                problems.append(f"record {index}: broken previous digest")
                break
            if record["kind"] not in ALLOWED_NEXT[verified[-1]["kind"]]:
                problems.append(f"record {index}: {record['kind']} may not follow "
                                f"{verified[-1]['kind']}")
                break
            changed = [f for f in CONSTANT_FIELDS if record[f] != first[f]]
            if changed:
                label = ("owner digest" if "series_token_sha256" in changed
                         else ", ".join(changed))
                problems.append(f"record {index}: inconsistent {label}")
                break
        identity = _identity(record["process"]) if record["process"] is not None else None
        if process is None and identity is not None and record["kind"] != "process_created":
            problems.append(f"record {index}: process identity before process_created")
            break
        if process is not None and identity != process:
            problems.append(f"record {index}: inconsistent process identity")
            break
        if record["kind"] == "process_created":
            if process is not None:
                problems.append(f"record {index}: a second process_created")
                break
            process = identity
        if record["context_sha256"] is not None:
            if context is not None and record["context_sha256"] != context:
                problems.append(f"record {index}: inconsistent context digest")
                break
            context = record["context_sha256"]
        elif context is not None:
            problems.append(f"record {index}: context digest disappeared")
            break
        if record["job"]["job_id"] is not None:
            if job_id is not None and record["job"]["job_id"] != job_id:
                problems.append(f"record {index}: inconsistent job identity")
                break
            job_id = record["job"]["job_id"]
        elif job_id is not None:
            problems.append(f"record {index}: job identity disappeared")
            break
        if record["kind"] == "complete":
            expected_chain = [{"sequence": r["sequence"], "kind": r["kind"], "sha256": d}
                              for r, d in zip(verified, digests)]
            if record["body"]["chain"] != expected_chain:
                problems.append(f"record {index}: malformed final chain")
                break
        verified.append(record)
        digests.append(record_digest(record))
    stages: list[str] = []
    for record in verified:
        stage = KIND_STAGES[record["kind"]]
        if stage is not None and stage not in stages:
            stages.append(stage)
    results = [r for r in verified if r["kind"] == "resume_result"]
    terminations = [r for r in verified if r["kind"] == "termination_result"]
    return ChainState(
        ok=not problems, problems=problems, count=len(verified),
        last_verified_sequence=len(verified) - 1 if verified else None,
        tip_digest=digests[-1] if digests else None,
        kinds=[r["kind"] for r in verified], durable_stages=stages, process=process,
        complete=any(r["kind"] == "complete" for r in verified),
        evidence_failure=any(r["kind"] == "evidence_failure" for r in verified),
        resume_result_history=results[-1]["body"]["execution_history"] if results else None,
        termination_verified=bool(terminations and terminations[-1]["body"]["verified"]))


# ---------------------------------------------------------------------------
# Writer over an injected FsOps
# ---------------------------------------------------------------------------

class FsOps:
    """
    The interface a file-system adapter must provide.  Slice E1 has no real
    implementation; tests inject an in-memory fake.
        dir_identity(path) -> {"exists", "reparse", "final_path", "volume_serial"}
        open_exclusive(path) -> handle   (FileExistsError when it exists)
        write(handle, data) -> int       (bytes written)
        flush(handle) -> None            (FlushFileBuffers)
        close(handle) -> None
        read_all(path) -> bytes
        file_identity(path) -> {"links", "reparse", "volume_serial", "file_index", "size"}
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise EvidenceDisabled("FsOps is an interface; no real adapter exists")


@dataclass
class EvidenceState:
    evidence_failed: bool = False
    last_verified_sequence: Optional[int] = None
    last_verified_digest: Optional[str] = None
    failed_operation: Optional[str] = None
    failed_name: Optional[str] = None
    partial_record_possible: bool = False
    ownership_retained: bool = True
    containment_required: str = "none"
    failure_record_attempted: bool = False
    errors: list[str] = field(default_factory=list)


class EvidenceWriter:
    """
    Writes immutable, chained records into one run directory through the
    injected FsOps only.  Each write: validate, check the directory
    identity, exclusive create, complete write, flush, checked close,
    re-read, byte count and SHA-256, file identity - and only then advance
    the chain.  A failed name stays consumed; nothing is overwritten,
    renamed or retried.
    """

    def __init__(self, run_dir: str, fs: Any, *, expected_dir: Mapping[str, Any],
                 run_id: str, expected_head: str, token_sha256: str) -> None:
        if fs is None:
            require_real_writes()
        if not isinstance(run_dir, str) or not run_dir or not isinstance(expected_dir, Mapping):
            raise EvidenceError("a run directory and its expected identity are required")
        self.run_dir, self.fs, self.expected_dir = run_dir.rstrip("\\"), fs, dict(expected_dir)
        self.expected = {"run_id": run_id, "head": expected_head, "token": token_sha256}
        self.records: list[dict[str, Any]] = []
        self.next_sequence = 0
        self.state = EvidenceState()

    def _durable_stages(self) -> list[str]:
        return verify_chain(self.records).durable_stages

    def stage_permissions(self) -> dict[str, Any]:
        """P.next_stage_allowed over the verified records and failure state."""
        return P.next_stage_allowed(self._durable_stages(), self.state.evidence_failed)

    @property
    def may_resume(self) -> bool:
        return self.stage_permissions()["may_resume"]

    @property
    def may_launch(self) -> bool:
        return self.stage_permissions()["may_launch"]

    def _check_directory(self) -> None:
        identity = self.fs.dir_identity(self.run_dir)
        if not isinstance(identity, Mapping) or identity.get("exists") is not True:
            raise _WriteFailure("directory_missing", False)
        if identity.get("reparse") is not False:
            raise _WriteFailure("directory_reparse_point", False)
        final = identity.get("final_path")
        if not isinstance(final, str) or final.casefold() != str(
                self.expected_dir.get("final_path", "")).casefold():
            raise _WriteFailure("directory_path_mismatch", False)
        if identity.get("volume_serial") != self.expected_dir.get("volume_serial"):
            raise _WriteFailure("directory_volume_mismatch", False)

    def write(self, kind: str, envelope: Mapping[str, Any], body: Mapping[str, Any]) -> str:
        """Write the next record; returns its digest.  Raises EvidenceError
        (nothing written, nothing consumed) for an invalid record, and
        EvidenceFailed (the name consumed) for a failed durable write."""
        if self.state.evidence_failed:
            raise EvidenceFailed("the writer is in the evidence-failure state; no further "
                                 "records except one failure record")
        return self._write(kind, envelope, body)

    def write_failure_record(self, envelope: Mapping[str, Any]) -> Optional[str]:
        """One best-effort evidence_failure record after a failure."""
        if not self.state.evidence_failed or self.state.failure_record_attempted:
            raise EvidenceError("a failure record is written once, after a failure")
        self.state.failure_record_attempted = True
        failed_kind = (self.state.failed_name or "000-prelaunch.json").split("-", 1)[1][:-5]
        body = {"operation": self.state.failed_operation, "failed_kind": failed_kind,
                "failed_name": self.state.failed_name,
                "containment_required": self.state.containment_required}
        try:
            return self._write("evidence_failure", envelope, body, after_failure=True)
        except EvidenceFailed:
            return None

    def _write(self, kind: str, envelope: Mapping[str, Any], body: Mapping[str, Any], *,
               after_failure: bool = False) -> str:
        previous = self.records[-1] if self.records else None
        record = build_record(kind, envelope, body, sequence=self.next_sequence,
                              previous_record_sha256=record_digest(previous) if previous else None)
        if after_failure:
            chain = verify_chain(self.records, expected_run_id=self.expected["run_id"],
                                 expected_head=self.expected["head"],
                                 expected_token_sha256=self.expected["token"])
        else:
            chain = verify_chain(self.records + [record], expected_run_id=self.expected["run_id"],
                                 expected_head=self.expected["head"],
                                 expected_token_sha256=self.expected["token"])
        if not chain.ok:
            raise EvidenceError("the record would break the chain: " + "; ".join(chain.problems))
        name = record_name(self.next_sequence, kind)
        path = f"{self.run_dir}\\{name}"
        data = P.canonical_json(record)
        digest = hashlib.sha256(data).hexdigest()
        handle = None
        try:
            self._check_directory()
            try:
                handle = self.fs.open_exclusive(path)
            except FileExistsError:
                raise _WriteFailure("create_duplicate", False) from None
            except OSError as exc:
                raise _WriteFailure(f"create:{type(exc).__name__}", False) from None
            self.next_sequence += 1                       # the name is consumed from here
            written = 0
            while written < len(data):
                try:
                    count = self.fs.write(handle, data[written:])
                except OSError as exc:
                    raise _WriteFailure(f"write:{type(exc).__name__}", True) from None
                if isinstance(count, bool) or not isinstance(count, int) or count <= 0 \
                        or count > len(data) - written:
                    raise _WriteFailure("write_short", True)
                written += count
            try:
                self.fs.flush(handle)
            except OSError as exc:
                raise _WriteFailure(f"flush:{type(exc).__name__}", True) from None
            try:
                closing, handle = handle, None
                self.fs.close(closing)
            except OSError as exc:
                raise _WriteFailure(f"close:{type(exc).__name__}", True) from None
            try:
                back = self.fs.read_all(path)
            except OSError as exc:
                raise _WriteFailure(f"readback:{type(exc).__name__}", True) from None
            if not isinstance(back, (bytes, bytearray)) or len(back) != len(data) \
                    or hashlib.sha256(bytes(back)).hexdigest() != digest:
                raise _WriteFailure("readback_mismatch", True)
            identity = self.fs.file_identity(path)
            if not isinstance(identity, Mapping) or identity.get("links") != 1 \
                    or identity.get("reparse") is not False \
                    or identity.get("volume_serial") != self.expected_dir.get("volume_serial") \
                    or identity.get("size") != len(data):
                raise _WriteFailure("file_identity", True)
        except _WriteFailure as failure:
            self._fail(failure.operation, name, failure.partial, handle)
            raise EvidenceFailed(f"{name}: {failure.operation}") from None
        except BaseException:
            self._fail("interrupted", name, True, handle)
            raise
        self.records.append(record)
        self.state.last_verified_sequence = record["sequence"]
        self.state.last_verified_digest = digest
        if kind == "complete" and body.get("ownership") == "released":
            self.state.ownership_retained = False
        return digest

    def _fail(self, operation: str, name: str, partial: bool, handle: Any) -> None:
        if handle is not None:
            try:
                self.fs.close(handle)
            except Exception as exc:  # recorded; the failure stands
                self.state.errors.append(f"close after failure: {type(exc).__name__}")
        if self.state.evidence_failed:                    # the failure record itself failed
            self.state.errors.append(f"failure record {name}: {operation}")
            return
        self.state.evidence_failed = True
        self.state.failed_operation, self.state.failed_name = operation, name
        self.state.partial_record_possible = partial
        self.state.ownership_retained = True
        self.state.containment_required = P.next_stage_allowed(
            self._durable_stages(), True)["containment"]
        self.state.errors.append(f"{name}: {operation}")

    def complete_body(self, ownership: str) -> dict[str, Any]:
        """The body of the complete record: the full chain listing."""
        return {"ownership": ownership,
                "chain": [{"sequence": r["sequence"], "kind": r["kind"],
                           "sha256": record_digest(r)} for r in self.records]}


class _WriteFailure(Exception):
    def __init__(self, operation: str, partial: bool) -> None:
        super().__init__(operation)
        self.operation, self.partial = operation, partial


# ---------------------------------------------------------------------------
# Recovery classification (pure; no file system, no process lookup)
# ---------------------------------------------------------------------------

INSPECTION_RESULTS: tuple[str, ...] = ("ok", "not_found", "access_denied", "error")


def classify_recovery(chain: ChainState, inspection: Optional[Mapping[str, Any]]
                      ) -> dict[str, Any]:
    """
    Execution history and liveness of a crashed run from its verified chain
    and an OS inspection by the RECORDED identity.  Exit is classified only
    with PID and creation-time evidence; a missing record never establishes
    termination; recovery never releases ownership.
    """
    if not isinstance(chain, ChainState):
        raise EvidenceError("classify_recovery needs a ChainState")
    stages = set(chain.durable_stages)
    result: dict[str, Any] = {"execution_history": None, "liveness": None,
                              "exit_evidence": None, "exit_code": None,
                              "termination_established": False, "ownership": "retained",
                              "reason": ""}
    if "job_configured" not in stages:
        result.update(liveness="not_applicable", reason="no job was configured, so no "
                                                        "process could have been created")
        return result
    if chain.process is None:
        result.update(execution_history=P.STATE_UNKNOWN, liveness=P.STATE_UNKNOWN,
                      reason="a process may exist but its identity was never durably "
                             "recorded")
        return result
    if "resume_intent" not in stages:
        result["execution_history"] = P.NEVER_EXECUTED
    elif chain.resume_result_history is not None:
        result["execution_history"] = chain.resume_result_history
    else:
        result["execution_history"] = P.MAY_HAVE_EXECUTED
    result["termination_established"] = chain.termination_verified
    if not isinstance(inspection, Mapping):
        result.update(liveness=P.STATE_UNKNOWN, reason="no inspection")
        return result
    pid, creation = chain.process
    if inspection.get("pid") != pid:
        result.update(liveness=P.STATE_UNKNOWN, reason="the inspection is about another PID")
        return result
    status = inspection.get("result")
    if status == "not_found":
        result.update(liveness=P.CONFIRMED_EXITED, exit_evidence="absent_from_process_table",
                      reason="no process has the recorded PID")
    elif status == "ok" and inspection.get("creation_time") not in (None, creation):
        result.update(liveness=P.CONFIRMED_EXITED, exit_evidence="absent_from_process_table",
                      reason="the PID now belongs to another process (creation time differs)")
    elif status == "ok" and inspection.get("creation_time") == creation:
        signaled, code = inspection.get("signaled"), inspection.get("exit_code")
        if signaled is True and isinstance(code, int) and not isinstance(code, bool):
            result.update(liveness=P.CONFIRMED_EXITED, exit_evidence="wait+exit_code",
                          exit_code=code, reason="signalled with an exit code")
        elif signaled is False and code is None:
            result.update(liveness=P.ALIVE_AT_LAST_CHECK, reason="the child is still alive")
        else:
            result.update(liveness=P.STATE_UNKNOWN, reason="inconclusive wait evidence")
    else:
        result.update(liveness=P.STATE_UNKNOWN,
                      reason=f"inspection {status!r}: identity not established")
    return result
