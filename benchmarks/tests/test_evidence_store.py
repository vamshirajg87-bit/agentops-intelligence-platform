"""
benchmarks/tests/test_evidence_store.py

Offline tests of the evidence store (slice E1).  An in-memory fake FsOps
with fault injection is the only "file system"; any real file-system
write, process, socket or subprocess fails the test.
"""

from __future__ import annotations

import copy
import errno
import hashlib
import os
import pathlib
import socket
import subprocess
import sys
from pathlib import Path

import pytest

_BENCH = Path(__file__).resolve().parents[1]
if str(_BENCH) not in sys.path:
    sys.path.insert(0, str(_BENCH))

import appcontainer_preflight as P  # noqa: E402
import calibrate_generator as C  # noqa: E402
import evidence_store as E  # noqa: E402

RUN, HEAD, TOKEN_SHA = "probe-01", "a" * 40, "7" * 64
RUN_DIR = r"C:\Users\u\AppData\Local\agentops-probe\evidence\probe-01"
CREATED = "2026-10-09T12:00:00.000Z"
PROCESS = {"pid": 4242, "creation_time": CREATED}
JOB_ID = "b" * 32
SOURCE = (_BENCH / "evidence_store.py").read_text(encoding="utf-8")


class LiveAccessAttempt(AssertionError):
    """A test reached something real."""


@pytest.fixture(autouse=True)
def fail_closed(monkeypatch):
    def refuse(what):
        def call(*args, **kwargs):
            raise LiveAccessAttempt(f"{what} attempted")
        return call

    for name in ("open", "write", "fsync", "mkdir", "makedirs", "replace", "rename",
                 "remove", "unlink", "rmdir", "system"):
        monkeypatch.setattr(os, name, refuse(f"os.{name}"))
    for name in ("write_text", "write_bytes", "mkdir", "touch", "unlink", "rename",
                 "replace"):
        monkeypatch.setattr(pathlib.Path, name, refuse(f"Path.{name}"))
    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, refuse(f"subprocess.{name}"))
    monkeypatch.setattr(socket, "create_connection", refuse("a network connection"))


class FakeFs:
    """In-memory FsOps with fault injection."""

    def __init__(self, **overrides):
        self.files, self.calls, self.fail = {}, [], {}
        self.dir = {"exists": True, "reparse": False, "final_path": RUN_DIR,
                    "volume_serial": 77}
        self.write_mode, self.corrupt, self.identity_override = None, False, {}
        self.handles, self.open_handles, self.next_handle = {}, set(), 1
        self.__dict__.update(overrides)

    def _step(self, op):
        self.calls.append(op)
        failure = self.fail.get(op)
        if failure is not None:
            raise failure

    def dir_identity(self, path):
        self._step("dir_identity")
        return dict(self.dir)

    def open_exclusive(self, path):
        self._step("open")
        if path in self.files:
            raise FileExistsError(path)
        handle, self.next_handle = self.next_handle, self.next_handle + 1
        self.handles[handle] = path
        self.files[path] = b""
        self.open_handles.add(handle)
        return handle

    def write(self, handle, data):
        self._step("write")
        if self.write_mode == "zero":
            return 0
        if self.write_mode == "overclaim":
            self.files[self.handles[handle]] += data
            return len(data) + 1
        if self.write_mode == "lie":
            self.files[self.handles[handle]] += data[:-1]
            return len(data)
        count = len(data) if self.write_mode != "chunks" else min(16, len(data))
        self.files[self.handles[handle]] += data[:count]
        return count

    def flush(self, handle):
        self._step("flush")

    def close(self, handle):
        self.open_handles.discard(handle)          # a failed close still ends the handle
        self._step("close")

    def read_all(self, path):
        self._step("read")
        data = self.files[path]
        if self.corrupt:
            data = bytes([data[0] ^ 1]) + data[1:]
        return data

    def file_identity(self, path):
        self._step("identity")
        return {"links": 1, "reparse": False, "volume_serial": 77, "file_index": 9,
                "size": len(self.files[path]), **self.identity_override}


def envelope(**changes):
    base = {
        "run_id": RUN, "series_token_sha256": TOKEN_SHA, "expected_head": HEAD,
        "object_format": "sha1", "repository_manifest_sha256": "1" * 64,
        "runtime_manifest_sha256": "2" * 64,
        "parent": {"pid": 100, "creation_time": "2026-10-09T11:00:00Z"},
        "utc": "2026-10-09T12:00:00Z", "monotonic_ms": 0, "context_sha256": None,
        "job": {"job_id": None, "limits_verified": False, "assignment": "not_created"},
        "process": None, "lifecycle": P.LifecycleState().as_dict(),
        "integrity": {"writer_version": E.WRITER_VERSION},
    }
    base.update(changes)
    return base


def _lifecycle(**changes):
    return {**P.LifecycleState().as_dict(), **changes}


JOB = {"job_id": JOB_ID, "limits_verified": True, "assignment": "assigned_at_creation"}
RUNNING = dict(job=JOB, context_sha256="3" * 64, process=PROCESS)
EXITED = _lifecycle(phase="exited", process_created=True, pid=4242, creation_time=CREATED,
                    execution_history=P.MAY_HAVE_EXECUTED, liveness=P.CONFIRMED_EXITED)

#: A complete, verified run: (kind, envelope changes, body).
STEPS = [
    ("prelaunch", {}, {"result": "passed"}),
    ("job_configured", dict(job=JOB, context_sha256="3" * 64), {"limits": "verified"}),
    ("process_created", RUNNING, dict(PROCESS)),
    ("job_verified_and_validated", RUNNING, {"members": [4242]}),
    ("resume_intent", RUNNING, {}),
    ("resume_result", RUNNING, {"execution_history": P.MAY_HAVE_EXECUTED}),
    ("termination_intent", RUNNING, {"cause": "job_terminated_timeout"}),
    ("termination_result", RUNNING, {"cause": "job_terminated_timeout", "verified": True,
                                     "api_ok": True, "signaled": True,
                                     "exit_code": P.TIMEOUT_EXIT_CODE}),
    ("final_inspection", {**RUNNING, "lifecycle": EXITED}, {"liveness": P.CONFIRMED_EXITED}),
]


def writer(fs=None):
    return E.EvidenceWriter(RUN_DIR, fs or FakeFs(), expected_dir={
        "final_path": RUN_DIR, "volume_serial": 77}, run_id=RUN, expected_head=HEAD,
        token_sha256=TOKEN_SHA)


def write_steps(w, count=None):
    for kind, changes, body in STEPS[:count]:
        w.write(kind, envelope(**changes), body)
    return w


def complete(w, ownership="released"):
    return w.write("complete", envelope(**RUNNING, lifecycle=EXITED), w.complete_body(ownership))


def full_chain():
    w = write_steps(writer())
    complete(w)
    return w


# ---------------------------------------------------------------------------
# Gates and scope
# ---------------------------------------------------------------------------

class TestGatesAndScope:
    def test_no_real_writes(self):
        assert E.EVIDENCE_WRITES_ENABLED is False
        with pytest.raises(E.EvidenceDisabled, match="not enabled"):
            E.require_real_writes()
        with pytest.raises(E.EvidenceDisabled):
            E.FsOps()
        with pytest.raises(E.EvidenceDisabled):
            E.EvidenceWriter(RUN_DIR, None, expected_dir={}, run_id=RUN, expected_head=HEAD,
                             token_sha256=TOKEN_SHA)

    def test_source_has_no_real_io(self):
        code = SOURCE.split('"""', 2)[2]
        for word in ("open(", "os.", "import os", "mkdir", "write_text", "write_bytes",
                     "shutil", "subprocess", "socket", "ctypes", "WinDLL", "fsync(",
                     "unlink", "rename(", "replace(path", "calibrate_generator"):
            assert word not in code, word

    def test_nothing_imports_the_store(self):
        for name in ("calibrate_generator.py", "appcontainer_preflight.py", "win32_job.py"):
            assert "evidence_store" not in (_BENCH / name).read_text(encoding="utf-8")

    def test_existing_gates_unchanged(self):
        assert (C.EXECUTION_ENABLED, C.APPROVED_CALIBRATIONS, C.PROBE_ENABLED,
                C.APPROVED_PROBES, C.AUTHORIZED_APPCONTAINER_SID,
                C.AUTHORIZED_PROFILE_PREFLIGHT_SHA256) == (False, frozenset(), False,
                                                          frozenset(), None, None)
        assert P.HOST_INVENTORY_ENABLED is False
        assert P.TERMINATION_POLICY == "conditionally_accepted_design_only"
        assert P.build_plan({}, expected_head=None)["recommendation"] == "NO-GO"


# ---------------------------------------------------------------------------
# Pure record helpers
# ---------------------------------------------------------------------------

class TestRecords:
    def test_record_name(self):
        assert E.record_name(4, "resume_intent") == "004-resume_intent.json"
        assert E.record_name(999, "complete") == "999-complete.json"
        for sequence, kind in ((-1, "complete"), (1000, "complete"), (True, "complete"),
                               (1.0, "complete"), (1, "bogus"), (1, None)):
            with pytest.raises(E.EvidenceError):
                E.record_name(sequence, kind)

    def test_canonical_serialization_and_digest(self):
        record = E.build_record("prelaunch", envelope(), {"b": 1, "a": 2}, sequence=0,
                                previous_record_sha256=None)
        data = P.canonical_json(record)
        assert data.startswith(b'{"body":{"a":2,"b":1}') and b" " not in data.split(b'"utc"')[0]
        assert E.record_digest(record) == hashlib.sha256(data).hexdigest()

    @pytest.mark.parametrize("changes, message", [
        ({"run_id": "bm-x"}, "run_id"), ({"utc": "noon"}, "utc"),
        ({"monotonic_ms": 1.5}, "monotonic_ms"), ({"monotonic_ms": -1}, "monotonic_ms"),
        ({"series_token_sha256": "x"}, "series_token_sha256"),
        ({"expected_head": "a" * 64}, "sha1 commit id"),
        ({"object_format": "md5"}, "unknown object format"),
        ({"parent": {"pid": 0, "creation_time": CREATED}}, "parent identity"),
        ({"process": {"pid": 1}}, "process identity"),
        ({"job": {"job_id": "x", "limits_verified": True, "assignment": "unknown"}}, "job"),
        ({"lifecycle": {"liveness": "dead"}}, "lifecycle"),
        ({"integrity": {}}, "integrity"),
        ({"context_sha256": "short"}, "context_sha256"),
    ])
    def test_malformed_envelopes(self, changes, message):
        with pytest.raises(E.EvidenceError, match=message):
            E.build_record("prelaunch", envelope(**changes), {}, sequence=0,
                           previous_record_sha256=None)

    def test_envelope_shape(self):
        bad = envelope()
        del bad["utc"]
        with pytest.raises(E.EvidenceError, match="malformed envelope"):
            E.build_record("prelaunch", bad, {}, sequence=0, previous_record_sha256=None)
        with pytest.raises(E.EvidenceError, match="malformed envelope"):
            E.build_record("prelaunch", {**envelope(), "extra": 1}, {}, sequence=0,
                           previous_record_sha256=None)
        with pytest.raises(E.EvidenceError, match="first record has a previous"):
            E.build_record("prelaunch", envelope(), {}, sequence=0,
                           previous_record_sha256="0" * 64)
        with pytest.raises(E.EvidenceError, match="previous_record_sha256"):
            E.build_record("job_configured", envelope(), {}, sequence=1,
                           previous_record_sha256=None)

    @pytest.mark.parametrize("kind, changes, body, message", [
        ("process_created", {}, dict(PROCESS), "name the process"),
        ("process_created", {"process": PROCESS}, {"pid": 1, "creation_time": CREATED},
         "name the process"),
        ("resume_result", {}, {"execution_history": "ran"}, "execution history"),
        ("termination_result", {}, {"cause": "job_terminated_timeout", "verified": True,
                                    "api_ok": True, "signaled": False, "exit_code": None},
         "without exit proof"),
        ("termination_result", {}, {"cause": "job_terminated_timeout", "verified": True,
                                    "api_ok": True, "signaled": True, "exit_code": 0},
         "without exit proof"),
        ("termination_result", {}, {"cause": "reboot", "verified": False, "api_ok": True,
                                    "signaled": False}, "malformed"),
        ("final_inspection", {}, {"liveness": "gone"}, "needs a liveness"),
        ("evidence_failure", {}, {"operation": "write", "failed_kind": "x"}, "malformed"),
        ("complete", {}, {"ownership": "kept", "chain": []}, "malformed"),
        ("complete", {"process": PROCESS}, {"ownership": "released", "chain": []},
         "without a confirmed exit"),
    ])
    def test_body_rules(self, kind, changes, body, message):
        with pytest.raises(E.EvidenceError, match=message):
            E.build_record(kind, envelope(**changes), body, sequence=1,
                           previous_record_sha256="0" * 64)


# ---------------------------------------------------------------------------
# Chain verification
# ---------------------------------------------------------------------------

class TestChain:
    def test_complete_chain(self):
        w = full_chain()
        chain = E.verify_chain(w.records, expected_run_id=RUN, expected_head=HEAD,
                               expected_token_sha256=TOKEN_SHA)
        assert chain.ok and chain.count == 10 and chain.complete
        assert chain.tip_digest == w.state.last_verified_digest == E.record_digest(w.records[-1])
        assert chain.durable_stages == list(P.LIFECYCLE_STAGES)
        assert chain.process == (4242, CREATED) and chain.termination_verified
        assert not w.state.ownership_retained

    def _problem(self, records, **expected):
        chain = E.verify_chain(records, **expected)
        assert not chain.ok
        return chain, " ".join(chain.problems)

    def test_missing_duplicate_and_reordered(self):
        records = full_chain().records
        _, text = self._problem(records[:3] + records[4:])
        assert "sequence" in text
        _, text = self._problem(records[:4] + [records[3]] + records[4:])
        assert "sequence" in text
        swapped = records[:3] + [records[4], records[3]] + records[5:]
        chain, text = self._problem(swapped)
        assert "sequence" in text and chain.count == 3

    def test_broken_previous_digest(self):
        records = copy.deepcopy(full_chain().records)
        records[2]["body"]["note"] = "altered"
        chain, text = self._problem(records)
        assert "record 3: broken previous digest" in text and chain.count == 3

    def test_malformed_final_chain(self):
        records = copy.deepcopy(full_chain().records)
        records[-1]["body"]["chain"].pop()
        chain, text = self._problem(records)
        assert "malformed final chain" in text and chain.count == 9 and not chain.complete

    def test_inconsistent_run_and_owner(self):
        records = copy.deepcopy(full_chain().records)
        records[3]["run_id"] = "probe-02"
        _, text = self._problem(records)
        assert "inconsistent run_id" in text or "broken previous" in text
        records = copy.deepcopy(full_chain().records)
        records[1]["series_token_sha256"] = "8" * 64
        _, text = self._problem(records)
        assert "inconsistent owner digest" in text or "broken previous" in text

    def test_expected_identity(self):
        records = full_chain().records
        assert "unexpected HEAD" in self._problem(records, expected_head="c" * 40)[1]
        assert "unexpected run_id" in self._problem(records, expected_run_id="probe-02")[1]
        assert "unexpected owner digest" in self._problem(
            records, expected_token_sha256="9" * 64)[1]

    def test_process_identity_rules(self):
        w = write_steps(writer(), 2)
        with pytest.raises(E.EvidenceError, match="process identity before process_created"):
            w.write("final_inspection", envelope(**RUNNING), {"liveness": P.STATE_UNKNOWN})
        w = write_steps(writer(), 4)
        moved = {**RUNNING, "process": {"pid": 4243, "creation_time": CREATED}}
        with pytest.raises(E.EvidenceError, match="inconsistent process identity"):
            w.write("resume_intent", envelope(**moved), {})
        with pytest.raises(E.EvidenceError, match="inconsistent process identity"):
            w.write("resume_intent", envelope(job=JOB, context_sha256="3" * 64), {})

    def test_stage_order_is_enforced_by_the_writer(self):
        w = write_steps(writer(), 1)
        before = (w.next_sequence, len(w.fs.files))
        for kind in ("resume_intent", "resume_result", "complete", "termination_result"):
            with pytest.raises(E.EvidenceError, match="may not follow"):
                w.write(kind, envelope(), {"execution_history": P.NEVER_EXECUTED,
                                           "ownership": "retained", "chain": [],
                                           "cause": "job_terminated_timeout",
                                           "verified": False, "api_ok": False,
                                           "signaled": False})
        assert (w.next_sequence, len(w.fs.files)) == before         # nothing consumed
        w2 = writer()
        with pytest.raises(E.EvidenceError, match="does not start with prelaunch"):
            w2.write("job_configured", envelope(), {})

    def test_hash_chain_is_not_authentication(self):
        rewritten = writer()
        for kind, changes, body in STEPS:
            rewritten.write(kind, envelope(**changes), {**body, "forged": True})
        complete(rewritten)
        assert E.verify_chain(rewritten.records).ok                  # recomputed hashes pass
        assert rewritten.state.last_verified_digest != full_chain().state.last_verified_digest

    def test_no_resume_without_durable_intent(self):
        w = write_steps(writer(), 4)
        assert not w.may_resume
        w.write("resume_intent", envelope(**RUNNING), {})
        assert w.may_resume
        w2 = write_steps(writer(), 2)
        assert w2.may_launch and not write_steps(writer(), 1).may_launch


# ---------------------------------------------------------------------------
# Writer failures (fake file system)
# ---------------------------------------------------------------------------

def _fail_at(index, **fs_changes):
    """Write STEPS up to index, then fail writing STEPS[index]."""
    w = write_steps(writer(), index)
    w.fs.__dict__.update(fs_changes)
    kind, changes, body = STEPS[index]
    with pytest.raises(E.EvidenceFailed) as raised:
        w.write(kind, envelope(**changes), body)
    return w, raised.value


class TestWriterFailures:
    def test_protocol_order(self):
        fs = FakeFs()
        writer(fs).write("prelaunch", envelope(), {})
        assert fs.calls == ["dir_identity", "open", "write", "flush", "close", "read",
                            "identity"]
        assert fs.files == {RUN_DIR + r"\000-prelaunch.json": P.canonical_json(
            E.build_record("prelaunch", envelope(), {}, sequence=0,
                           previous_record_sha256=None))}

    def test_chunked_writes_complete(self):
        w = writer(FakeFs(write_mode="chunks"))
        w.write("prelaunch", envelope(), {"x": "y" * 100})
        assert w.state.last_verified_sequence == 0 and not w.state.evidence_failed

    @pytest.mark.parametrize("fs_changes, operation, partial", [
        ({"write_mode": "zero"}, "write_short", True),
        ({"write_mode": "overclaim"}, "write_short", True),
        ({"write_mode": "lie"}, "readback_mismatch", True),
        ({"fail": {"write": OSError(errno.ENOSPC, "disk full")}}, "write:OSError", True),
        ({"fail": {"flush": OSError(errno.EIO, "flush failed")}}, "flush:OSError", True),
        ({"fail": {"close": OSError(errno.EIO, "close failed")}}, "close:OSError", True),
        ({"corrupt": True}, "readback_mismatch", True),
        ({"fail": {"read": OSError(errno.EIO, "read failed")}}, "readback:OSError", True),
        ({"identity_override": {"links": 2}}, "file_identity", True),
        ({"identity_override": {"reparse": True}}, "file_identity", True),
        ({"identity_override": {"volume_serial": 78}}, "file_identity", True),
        ({"identity_override": {"size": 1}}, "file_identity", True),
        ({"fail": {"open": OSError(errno.ENOSPC, "disk full")}}, "create:OSError", False),
        ({"fail": {"open": PermissionError(errno.EACCES, "denied")}},
         "create:PermissionError", False),
        ({"dir": {"exists": False}}, "directory_missing", False),
        ({"dir": {"exists": True, "reparse": True, "final_path": RUN_DIR,
                  "volume_serial": 77}}, "directory_reparse_point", False),
        ({"dir": {"exists": True, "reparse": False, "final_path": r"C:\elsewhere",
                  "volume_serial": 77}}, "directory_path_mismatch", False),
        ({"dir": {"exists": True, "reparse": False, "final_path": RUN_DIR,
                  "volume_serial": 1}}, "directory_volume_mismatch", False),
    ])
    def test_each_failure(self, fs_changes, operation, partial):
        w, error = _fail_at(4, **fs_changes)
        state = w.state
        assert state.evidence_failed and state.failed_operation == operation
        assert state.partial_record_possible is partial
        assert state.failed_name == "004-resume_intent.json"
        assert state.last_verified_sequence == 3 and state.ownership_retained
        assert state.containment_required == "discard_if_created"   # before resume
        assert not w.may_resume and not w.may_launch
        assert w.fs.open_handles == set()                             # no handle leak
        assert w.next_sequence == (5 if partial else 4)               # name consumed
        with pytest.raises(E.EvidenceFailed, match="evidence-failure state"):
            w.write("resume_intent", envelope(**RUNNING), {})          # no retry

    def test_duplicate_creation_leaves_the_existing_file(self):
        fs = FakeFs()
        path = RUN_DIR + r"\000-prelaunch.json"
        fs.files[path] = b"existing"
        w = writer(fs)
        with pytest.raises(E.EvidenceFailed, match="create_duplicate"):
            w.write("prelaunch", envelope(), {})
        assert fs.files[path] == b"existing" and not w.state.partial_record_possible

    def test_failure_after_resume_requires_containment(self):
        w, _ = _fail_at(5, write_mode="zero")
        assert w.state.containment_required == "bounded_termination"
        assert w.state.ownership_retained and not w.may_resume

    def test_failure_before_any_process(self):
        w, _ = _fail_at(1, write_mode="zero")
        assert w.state.containment_required == "none"
        w, _ = _fail_at(2, write_mode="zero")                      # process may exist
        assert w.state.containment_required == "discard_if_created"

    def test_interrupted_write(self):
        w = write_steps(writer(), 2)
        w.fs.fail = {"flush": KeyboardInterrupt()}
        with pytest.raises(KeyboardInterrupt):
            w.write(*STEPS[2][:1], envelope(**STEPS[2][1]), STEPS[2][2])
        assert w.state.evidence_failed and w.state.failed_operation == "interrupted"
        assert w.state.partial_record_possible and w.fs.open_handles == set()

    def test_failure_record_once(self):
        w, _ = _fail_at(4, write_mode="zero")
        w.fs.write_mode = None
        digest = w.write_failure_record(envelope(**RUNNING))
        record = w.records[-1]
        assert digest and record["kind"] == "evidence_failure" and record["sequence"] == 5
        assert record["body"]["operation"] == "write_short"
        assert record["body"]["failed_kind"] == "resume_intent"
        with pytest.raises(E.EvidenceError, match="written once"):
            w.write_failure_record(envelope(**RUNNING))

    def test_failure_record_can_fail_too(self):
        w, _ = _fail_at(4, write_mode="zero")
        assert w.write_failure_record(envelope(**RUNNING)) is None
        assert w.state.failed_operation == "write_short"              # first failure kept
        assert any("failure record" in e for e in w.state.errors)

    def test_failure_record_needs_a_failure(self):
        w = write_steps(writer(), 2)
        with pytest.raises(E.EvidenceError, match="after a failure"):
            w.write_failure_record(envelope())

    def test_no_implicit_termination_or_release(self):
        w = write_steps(writer(), 7)
        with pytest.raises(E.EvidenceError, match="without exit proof"):
            w.write("termination_result", envelope(**RUNNING),
                    {"cause": "job_terminated_timeout", "verified": True, "api_ok": True,
                     "signaled": False, "exit_code": None})
        w.write("termination_result", envelope(**RUNNING),
                {"cause": "job_terminated_timeout", "verified": False, "api_ok": True,
                 "signaled": False, "exit_code": None})
        assert not E.verify_chain(w.records).termination_verified
        w.write("final_inspection", envelope(**RUNNING), {"liveness": P.STATE_UNKNOWN})
        with pytest.raises(E.EvidenceError, match="without a confirmed exit"):
            w.write("complete", envelope(**RUNNING), w.complete_body("released"))
        w.write("complete", envelope(**RUNNING), w.complete_body("retained"))
        assert w.state.ownership_retained


# ---------------------------------------------------------------------------
# Recovery classification
# ---------------------------------------------------------------------------

def chain_after(count):
    return E.verify_chain(write_steps(writer(), count).records)


NOT_FOUND = {"pid": 4242, "result": "not_found"}


class TestRecovery:
    def test_crash_at_every_stage(self):
        expectations = {
            1: (None, "not_applicable"),
            2: (P.STATE_UNKNOWN, P.STATE_UNKNOWN),
            3: (P.NEVER_EXECUTED, P.CONFIRMED_EXITED),
            4: (P.NEVER_EXECUTED, P.CONFIRMED_EXITED),
            5: (P.MAY_HAVE_EXECUTED, P.CONFIRMED_EXITED),
            6: (P.MAY_HAVE_EXECUTED, P.CONFIRMED_EXITED),
            7: (P.MAY_HAVE_EXECUTED, P.CONFIRMED_EXITED),
            8: (P.MAY_HAVE_EXECUTED, P.CONFIRMED_EXITED),
            9: (P.MAY_HAVE_EXECUTED, P.CONFIRMED_EXITED),
        }
        for count, (history, liveness) in expectations.items():
            result = E.classify_recovery(chain_after(count), NOT_FOUND)
            assert (result["execution_history"], result["liveness"]) == (history, liveness), count
            assert result["ownership"] == "retained"

    def test_identity_never_recorded(self):
        result = E.classify_recovery(chain_after(2), {"pid": 4242, "result": "ok",
                                                      "creation_time": CREATED,
                                                      "signaled": True, "exit_code": 0})
        assert result["liveness"] == P.STATE_UNKNOWN and "never durably recorded" in \
            result["reason"]

    def test_recorded_history_after_resume(self):
        w = write_steps(writer(), 5)
        w.write("resume_result", envelope(**RUNNING), {"execution_history": P.NEVER_EXECUTED})
        result = E.classify_recovery(E.verify_chain(w.records), NOT_FOUND)
        assert result["execution_history"] == P.NEVER_EXECUTED

    @pytest.mark.parametrize("inspection, liveness, evidence", [
        ({"pid": 4242, "result": "ok", "creation_time": "2026-10-09T13:00:00.000Z"},
         P.CONFIRMED_EXITED, "absent_from_process_table"),
        ({"pid": 4242, "result": "ok", "creation_time": CREATED, "signaled": True,
          "exit_code": 3}, P.CONFIRMED_EXITED, "wait+exit_code"),
        ({"pid": 4242, "result": "ok", "creation_time": CREATED, "signaled": False,
          "exit_code": None}, P.ALIVE_AT_LAST_CHECK, None),
        ({"pid": 4242, "result": "ok", "creation_time": CREATED, "signaled": None},
         P.STATE_UNKNOWN, None),
        ({"pid": 4242, "result": "access_denied"}, P.STATE_UNKNOWN, None),
        ({"pid": 4242, "result": "error"}, P.STATE_UNKNOWN, None),
        ({"pid": 999, "result": "not_found"}, P.STATE_UNKNOWN, None),
        (None, P.STATE_UNKNOWN, None),
    ])
    def test_inspection_outcomes(self, inspection, liveness, evidence):
        result = E.classify_recovery(chain_after(6), inspection)
        assert (result["liveness"], result["exit_evidence"]) == (liveness, evidence)

    def test_missing_termination_record_never_establishes_termination(self):
        chain = chain_after(7)                                    # termination_intent only
        assert "termination_intent" in chain.kinds and not chain.termination_verified
        result = E.classify_recovery(chain, None)
        assert result["liveness"] == P.STATE_UNKNOWN
        assert result["termination_established"] is False
        result = E.classify_recovery(chain, NOT_FOUND)
        assert result["liveness"] == P.CONFIRMED_EXITED
        assert result["termination_established"] is False           # exited, not "terminated"

    def test_verified_termination_is_reported(self):
        result = E.classify_recovery(chain_after(8), NOT_FOUND)
        assert result["termination_established"] is True

    def test_requires_a_chain_state(self):
        with pytest.raises(E.EvidenceError):
            E.classify_recovery({"durable_stages": []}, NOT_FOUND)
