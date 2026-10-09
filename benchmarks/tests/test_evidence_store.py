"""
benchmarks/tests/test_evidence_store.py

Offline tests of the evidence store (slice E1).  An in-memory fake FsOps
with fault injection is the only "file system"; any real file-system
write, process, socket or subprocess fails the test.
"""

from __future__ import annotations

import copy
import ctypes
import errno
import hashlib
import os
import pathlib
import re
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
    monkeypatch.setattr(ctypes, "WinDLL", refuse("a real DLL load"))
    monkeypatch.setattr(E, "_kernel32_loader", refuse("the real kernel32 loader"))


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
        assert not re.search(r"(?<![\w.])open\(", code)            # only self._open(...)
        for word in ("os.", "import os", "mkdir", "write_text", "write_bytes", "shutil",
                     "subprocess", "socket", "fsync(", "unlink", "rename(", "replace(path",
                     "CreateDirectory", "DeleteFile", "MoveFile", "RemoveDirectory"):
            assert word not in code, word
        start = code.index("# WindowsFsOps (slice E2)")
        end = code.index("# Recovery classification")
        outside = code[:start] + code[end:]
        for word in ("ctypes", "WinDLL", "calibrate_generator", "kernel32"):
            assert word not in outside, word                       # native code only in E2

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


# ---------------------------------------------------------------------------
# WindowsFsOps (E2) over a fake kernel32
# ---------------------------------------------------------------------------

class FakeFunction:
    def __init__(self, name, implementation, log):
        self.name, self.implementation, self.log = name, implementation, log

    def __call__(self, *args):
        self.log.append(self.name)
        return self.implementation(*args)


class FakeDll:
    def __init__(self, kernel32):
        self._k = kernel32

    def __getattr__(self, name):
        if name.startswith("_") or name not in FakeKernel32Fs.APIS:
            raise LiveAccessAttempt(f"unexpected Windows API {name}")
        return FakeFunction(name, getattr(self._k, name), self._k.log)


class FakeKernel32Fs:
    """A tiny NTFS-like file system behind the eight kernel32 functions."""

    APIS = ("CreateFileW", "WriteFile", "ReadFile", "FlushFileBuffers", "CloseHandle",
            "GetFileInformationByHandle", "GetFinalPathNameByHandleW", "GetHandleInformation")

    def __init__(self, **overrides):
        self.dirs = {RUN_DIR: {"attrs": E.FILE_ATTRIBUTE_DIRECTORY, "volume": 77}}
        self.files, self.handles, self.closed, self.log, self.loads = {}, {}, [], [], []
        self.fail, self.last_error, self.next_handle = {}, 0, 0x100
        self.inherit_flags, self.write_mode, self.read_overclaim = 0, None, False
        self.final_override = self.final_length = None
        self.file_info, self.dir_info, self.calls_args = {}, {}, []
        self.__dict__.update(overrides)

    def loader(self, name):
        self.loads.append(name)
        return FakeDll(self)

    def _failing(self, api):
        code = self.fail.get(api)
        if code is not None:
            self.last_error = code
            return True
        return False

    def CreateFileW(self, path, access, share, attributes, disposition, flags, template):
        self.calls_args.append((path, access, share, attributes, disposition, flags, template))
        assert attributes is None and template is None          # non-inheritable, no template
        if self._failing("CreateFileW"):
            return -1
        if flags & E.FILE_FLAG_BACKUP_SEMANTICS:
            assert flags & E.FILE_FLAG_OPEN_REPARSE_POINT
            if path not in self.dirs:
                self.last_error = 3
                return -1
            kind = "dir"
        elif disposition == E.CREATE_NEW:
            assert (access, share) == (E.GENERIC_WRITE, 0)
            assert flags & E.FILE_FLAG_WRITE_THROUGH
            if path in self.files:
                self.last_error = 80
                return -1
            if path.rsplit("\\", 1)[0] not in self.dirs:
                self.last_error = 3
                return -1
            self.files[path] = {"data": bytearray(), "attrs": E.FILE_ATTRIBUTE_NORMAL,
                                "links": 1, "volume": 77, "index": len(self.files) + 1}
            kind = "write"
        else:
            assert disposition == E.OPEN_EXISTING and flags & E.FILE_FLAG_OPEN_REPARSE_POINT
            if path not in self.files:
                self.last_error = 2
                return -1
            kind = "read" if access == E.GENERIC_READ else "identity"
        handle, self.next_handle = self.next_handle, self.next_handle + 4
        self.handles[handle] = {"path": path, "kind": kind, "pos": 0}
        return handle

    def GetHandleInformation(self, handle, flags):
        if self._failing("GetHandleInformation"):
            return 0
        flags._obj.value = self.inherit_flags
        return 1

    def WriteFile(self, handle, address, length, written, overlapped):
        if self._failing("WriteFile"):
            return 0
        data = ctypes.string_at(address, length)
        claimed = {"zero": 0, "half": max(1, length // 2), "over": length + 1,
                   "lie": length}.get(self.write_mode, length)
        stored = data[:min(claimed, length)] if self.write_mode != "lie" else data[:-1]
        self.files[self.handles[handle]["path"]]["data"] += stored
        written._obj.value = claimed
        return 1

    def FlushFileBuffers(self, handle):
        return 0 if self._failing("FlushFileBuffers") else 1

    def CloseHandle(self, handle):
        self.closed.append(handle)
        self.handles.pop(handle, None)
        return 0 if self._failing("CloseHandle") else 1

    def ReadFile(self, handle, address, length, read, overlapped):
        if self._failing("ReadFile"):
            return 0
        entry = self.handles[handle]
        data = bytes(self.files[entry["path"]]["data"])
        chunk = data[entry["pos"]:entry["pos"] + length]
        if self.read_overclaim and chunk:
            read._obj.value = length + 5
            return 1
        ctypes.memmove(address, chunk, len(chunk))
        entry["pos"] += len(chunk)
        read._obj.value = len(chunk)
        return 1

    def GetFileInformationByHandle(self, handle, info_ref):
        if self._failing("GetFileInformationByHandle"):
            return 0
        entry = self.handles[handle]
        if entry["kind"] == "dir":
            source = self.dirs[entry["path"]]
            values = {"attrs": source["attrs"], "volume": source["volume"], "size": 0,
                      "links": 1, "index": 1, **self.dir_info}
        else:
            source = self.files[entry["path"]]
            values = {"attrs": source["attrs"], "volume": source["volume"],
                      "size": len(source["data"]), "links": source["links"],
                      "index": source["index"], **self.file_info}
        info = info_ref._obj
        info.dwFileAttributes, info.dwVolumeSerialNumber = values["attrs"], values["volume"]
        info.nFileSizeHigh, info.nFileSizeLow = values["size"] >> 32, values["size"] & 0xFFFFFFFF
        info.nNumberOfLinks = values["links"]
        info.nFileIndexHigh, info.nFileIndexLow = 0, values["index"]
        return 1

    def GetFinalPathNameByHandleW(self, handle, address, capacity, flags):
        if self._failing("GetFinalPathNameByHandleW"):
            return 0
        assert flags == E.FINAL_PATH_FLAGS and capacity == E.FINAL_PATH_CAPACITY
        text = self.final_override if self.final_override is not None \
            else "\\\\?\\" + self.handles[handle]["path"]
        encoded = (text + "\0").encode("utf-16-le")
        ctypes.memmove(address, encoded, len(encoded))
        return len(text) if self.final_length is None else self.final_length


def open_gates(monkeypatch):
    """In memory only: the evidence-write flag and a probe authorization."""
    monkeypatch.setattr(E, "EVIDENCE_WRITES_ENABLED", True)
    monkeypatch.setattr(C, "PROBE_ENABLED", True)
    monkeypatch.setattr(C, "APPROVED_PROBES", frozenset({"probe-01"}))
    monkeypatch.setattr(C, "_git_head", lambda *require: HEAD)
    return C.authorize_probe("probe-01", HEAD)


def win_fs(monkeypatch, **overrides):
    kernel32 = FakeKernel32Fs(**overrides)
    fs = E.WindowsFsOps(open_gates(monkeypatch), loader=kernel32.loader,
                        last_error=lambda: kernel32.last_error)
    return fs, kernel32


class TestWindowsFsOpsGates:
    def test_disabled_flag_refuses_before_loading(self):
        kernel32 = FakeKernel32Fs()
        with pytest.raises(E.EvidenceDisabled, match="not enabled"):
            E.WindowsFsOps(object(), loader=kernel32.loader)
        assert kernel32.loads == [] and kernel32.log == []

    def test_flag_alone_is_not_enough(self, monkeypatch):
        monkeypatch.setattr(E, "EVIDENCE_WRITES_ENABLED", True)
        for forged in (None, object(), C.ProbeAuthorization("probe-01", HEAD)):
            with pytest.raises(E.EvidenceDisabled, match="no valid authorization"):
                E.WindowsFsOps(forged, loader=FakeKernel32Fs().loader)

    def test_gates_rechecked_on_every_call(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        monkeypatch.setattr(E, "EVIDENCE_WRITES_ENABLED", False)
        with pytest.raises(E.EvidenceDisabled):
            fs.dir_identity(RUN_DIR)
        monkeypatch.setattr(E, "EVIDENCE_WRITES_ENABLED", True)
        monkeypatch.setattr(C, "PROBE_ENABLED", False)
        with pytest.raises(E.EvidenceDisabled):
            fs.dir_identity(RUN_DIR)
        assert kernel32.loads == [] and kernel32.log == []

    def test_real_loader_is_never_used(self, monkeypatch):
        fs = E.WindowsFsOps(open_gates(monkeypatch))
        assert not fs.loaded
        with pytest.raises(LiveAccessAttempt, match="real kernel32 loader"):
            fs.dir_identity(RUN_DIR)


@pytest.fixture
def no_harness_import(monkeypatch):
    """Any attempt to import the harness (or run_benchmark) or to probe the
    temporary directory fails the test."""
    import builtins
    import importlib
    import tempfile
    real_import = builtins.__import__
    attempts = []

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in ("calibrate_generator", "run_benchmark"):
            attempts.append(name)
            raise LiveAccessAttempt(f"import of {name} attempted")
        return real_import(name, *args, **kwargs)

    def refuse(what):
        def call(*args, **kwargs):
            attempts.append(what)
            raise LiveAccessAttempt(f"{what} attempted")
        return call

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(importlib, "import_module", refuse("importlib.import_module"))
    monkeypatch.setattr(tempfile, "gettempdir", refuse("tempfile.gettempdir"))
    monkeypatch.setattr(tempfile, "_get_default_tempdir", refuse("the tempdir probe"))
    return attempts


def fake_harness(**attributes):
    """A module object posing as the harness (it can never be the real one)."""
    import importlib.machinery
    import types as pytypes
    module = pytypes.ModuleType(E.HARNESS_MODULE)
    module.__spec__ = importlib.machinery.ModuleSpec(E.HARNESS_MODULE, None)
    module.__spec__._initializing = False
    module.__file__ = C.__file__
    module._require_any_authorization = C._require_any_authorization
    module.CalibrationError = C.CalibrationError
    for name, value in attributes.items():
        setattr(module, name, value)
    return module


class TestHarnessLookup:
    def test_lookup_only_in_source(self):
        code = SOURCE.split('"""', 2)[2]
        section = code[code.index("# WindowsFsOps (slice E2)"):code.index(
            "# Recovery classification")]
        for word in ("import calibrate_generator", "import run_benchmark", "importlib",
                     "__import__", "tempfile", "exec(", "eval("):
            assert word not in section, word
        assert "sys.modules.get(HARNESS_MODULE)" in section

    def test_missing_module_refuses_without_import(self, monkeypatch, no_harness_import):
        auth = open_gates(monkeypatch)
        monkeypatch.delitem(sys.modules, E.HARNESS_MODULE)
        kernel32 = FakeKernel32Fs()
        with pytest.raises(E.EvidenceDisabled, match="not loaded; the evidence adapter never "
                                                     "imports it"):
            E.WindowsFsOps(auth, loader=kernel32.loader)
        assert no_harness_import == [] and kernel32.loads == []
        assert E.HARNESS_MODULE not in sys.modules                 # not preloaded or repaired

    def test_removal_after_construction_blocks_every_call(self, monkeypatch, no_harness_import):
        fs, kernel32 = win_fs(monkeypatch)
        monkeypatch.delitem(sys.modules, E.HARNESS_MODULE)
        for call in (lambda: fs.dir_identity(RUN_DIR), lambda: fs.open_exclusive("x"),
                     lambda: fs.write(1, b"x"), lambda: fs.flush(1), lambda: fs.close(1),
                     lambda: fs.read_all("x"), lambda: fs.file_identity("x")):
            with pytest.raises(E.EvidenceDisabled, match="not loaded"):
                call()
        assert kernel32.log == [] and kernel32.loads == [] and no_harness_import == []

    def test_partially_initialized_module_refuses(self, monkeypatch, no_harness_import):
        auth = open_gates(monkeypatch)
        monkeypatch.setattr(C.__spec__, "_initializing", True, raising=False)
        with pytest.raises(E.EvidenceDisabled, match="partially initialized"):
            E.WindowsFsOps(auth, loader=FakeKernel32Fs().loader)
        monkeypatch.setattr(C.__spec__, "_initializing", False, raising=False)
        monkeypatch.setitem(sys.modules, E.HARNESS_MODULE, fake_harness(__spec__=None))
        with pytest.raises(E.EvidenceDisabled, match="partially initialized"):
            E.WindowsFsOps(auth, loader=FakeKernel32Fs().loader)
        assert no_harness_import == []

    @pytest.mark.parametrize("attributes, message", [
        ({"__name__": "other"}, "unexpected name"),
        ({"__file__": r"C:\elsewhere\calibrate_generator.py"}, "replaced"),
        ({"__file__": None}, "replaced"),
        ({}, "not the module's own"),                         # a perfect-looking impostor
        ({"_require_any_authorization": None}, "missing or not the module's own"),
    ])
    def test_replaced_or_malformed_module_refuses(self, monkeypatch, no_harness_import,
                                                  attributes, message):
        auth = open_gates(monkeypatch)
        monkeypatch.setitem(sys.modules, E.HARNESS_MODULE, fake_harness(**attributes))
        with pytest.raises(E.EvidenceDisabled, match=message):
            E.WindowsFsOps(auth, loader=FakeKernel32Fs().loader)
        assert no_harness_import == []

    def test_tampered_real_module_refuses(self, monkeypatch, no_harness_import):
        auth = open_gates(monkeypatch)
        monkeypatch.setattr(C, "_require_any_authorization", lambda authorization: authorization)
        with pytest.raises(E.EvidenceDisabled, match="not the module's own"):
            E.WindowsFsOps(auth, loader=FakeKernel32Fs().loader)
        monkeypatch.undo()
        auth = open_gates(monkeypatch)
        monkeypatch.setattr(C, "CalibrationError", "not a type")
        with pytest.raises(E.EvidenceDisabled, match="error type"):
            E.WindowsFsOps(auth, loader=FakeKernel32Fs().loader)

    def test_presence_is_not_authorization(self, monkeypatch, no_harness_import):
        monkeypatch.setattr(E, "EVIDENCE_WRITES_ENABLED", True)
        assert sys.modules[E.HARNESS_MODULE] is C
        for forged in (None, object(), C.ProbeAuthorization("probe-01", HEAD)):
            with pytest.raises(E.EvidenceDisabled, match="no valid authorization"):
                E.WindowsFsOps(forged, loader=FakeKernel32Fs().loader)
        assert no_harness_import == []

    def test_valid_authorization_with_the_loaded_harness(self, monkeypatch, no_harness_import):
        fs, kernel32 = win_fs(monkeypatch)
        w = write_steps(win_writer(fs))
        complete(w)
        assert E.verify_chain(w.records).ok and no_harness_import == []
        assert kernel32.loads == ["kernel32"]

    def test_revoked_authorization_refuses_every_operation(self, monkeypatch,
                                                           no_harness_import):
        fs, kernel32 = win_fs(monkeypatch)
        monkeypatch.setattr(C, "APPROVED_PROBES", frozenset())         # revoke
        for call in (lambda: fs.dir_identity(RUN_DIR), lambda: fs.open_exclusive("x"),
                     lambda: fs.write(1, b"x"), lambda: fs.flush(1), lambda: fs.close(1),
                     lambda: fs.read_all("x"), lambda: fs.file_identity("x")):
            with pytest.raises(E.EvidenceDisabled, match="no valid authorization"):
                call()
        assert kernel32.log == [] and kernel32.loads == [] and no_harness_import == []

    def test_disabled_flag_never_looks_up_or_imports(self, monkeypatch, no_harness_import):
        monkeypatch.delitem(sys.modules, E.HARNESS_MODULE)
        with pytest.raises(E.EvidenceDisabled, match="not enabled"):
            E.WindowsFsOps(object(), loader=FakeKernel32Fs().loader)
        assert no_harness_import == []


class TestWindowsFsOpsContracts:
    @pytest.mark.skipif(ctypes.sizeof(ctypes.c_void_p) != 8, reason="x64 layout")
    def test_structure_layout(self):
        t = E._fs_types()
        assert ctypes.sizeof(t.INFO) == 52 and ctypes.sizeof(t.FILETIME) == 8
        assert t.INFO.dwVolumeSerialNumber.offset == 28 and t.INFO.nNumberOfLinks.offset == 40
        assert t.INFO.nFileIndexLow.offset == 48

    def test_signatures(self):
        t = E._fs_types()
        signatures = E._fs_signatures(t)
        assert set(signatures) == set(FakeKernel32Fs.APIS)
        assert signatures["CreateFileW"][0] is t.w.HANDLE
        assert len(signatures["CreateFileW"][1]) == 7
        assert signatures["GetFinalPathNameByHandleW"][0] is t.w.DWORD

    def test_flags(self):
        assert (E.CREATE_NEW, E.OPEN_EXISTING, E.GENERIC_WRITE, E.GENERIC_READ) == (
            1, 3, 0x40000000, 0x80000000)
        assert (E.FILE_FLAG_WRITE_THROUGH, E.FILE_FLAG_BACKUP_SEMANTICS,
                E.FILE_FLAG_OPEN_REPARSE_POINT, E.FILE_ATTRIBUTE_REPARSE_POINT) == (
            0x80000000, 0x02000000, 0x00200000, 0x400)

    @pytest.mark.parametrize("text, expected", [
        ("\\\\?\\C:\\Users\\u\\x", "C:\\Users\\u\\x"),
        ("\\\\?\\UNC\\server\\share\\x", "\\\\server\\share\\x"),
    ])
    def test_final_path_normalization(self, text, expected):
        assert E.normalize_final_path(text) == expected

    @pytest.mark.parametrize("text", ["C:\\x", "\\\\?\\Volume{1}\\x", "", None, "\\\\?\\C:\\a\x00"])
    def test_malformed_final_paths(self, text):
        with pytest.raises(OSError):
            E.normalize_final_path(text)

    def test_windows_error_mapping(self):
        assert isinstance(E.windows_error("x", 80), FileExistsError)
        assert isinstance(E.windows_error("x", 5), PermissionError)
        assert isinstance(E.windows_error("x", 3), FileNotFoundError)
        full = E.windows_error("x", 112)
        assert full.errno == errno.ENOSPC and full.windows_code == 112
        assert type(E.windows_error("x", 1234)) is OSError


def win_writer(fs):
    return E.EvidenceWriter(RUN_DIR, fs, expected_dir={"final_path": RUN_DIR,
                                                       "volume_serial": 77},
                            run_id=RUN, expected_head=HEAD, token_sha256=TOKEN_SHA)


class TestWindowsFsOpsWrites:
    def test_full_chain_matches_the_fake_path(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        w = write_steps(win_writer(fs))
        complete(w)
        chain = E.verify_chain(w.records)
        assert chain.ok and chain.complete
        assert chain.tip_digest == full_chain().state.last_verified_digest
        name = RUN_DIR + "\\000-prelaunch.json"
        assert bytes(kernel32.files[name]["data"]) == P.canonical_json(w.records[0])
        assert kernel32.handles == {} and kernel32.loads == ["kernel32"]
        assert kernel32.dirs == {RUN_DIR: {"attrs": E.FILE_ATTRIBUTE_DIRECTORY, "volume": 77}}

    def test_partial_writes_progress(self, monkeypatch):
        fs, _ = win_fs(monkeypatch, write_mode="half")
        w = win_writer(fs)
        w.write("prelaunch", envelope(), {"x": "y" * 200})
        assert w.state.last_verified_sequence == 0

    def _fails(self, monkeypatch, operation, partial=None, **overrides):
        fs, kernel32 = win_fs(monkeypatch, **overrides)
        w = win_writer(fs)
        with pytest.raises(E.EvidenceFailed):
            w.write("prelaunch", envelope(), {})
        assert w.state.failed_operation == operation, w.state.failed_operation
        if partial is not None:
            assert w.state.partial_record_possible is partial
        assert kernel32.handles == {}                              # every handle closed
        return w, kernel32

    @pytest.mark.parametrize("overrides, operation, partial", [
        ({"write_mode": "zero"}, "write_short", True),
        ({"write_mode": "over"}, "write_short", True),
        ({"write_mode": "lie"}, "readback_mismatch", True),
        ({"fail": {"WriteFile": 112}}, "write:OSError", True),
        ({"fail": {"FlushFileBuffers": 1117}}, "flush:OSError", True),
        ({"fail": {"ReadFile": 23}}, "readback:OSError", True),
        ({"read_overclaim": True}, "readback:OSError", True),
        ({"file_info": {"links": 2}}, "file_identity", True),
        ({"file_info": {"attrs": E.FILE_ATTRIBUTE_NORMAL | E.FILE_ATTRIBUTE_REPARSE_POINT}},
         "file_identity", True),
        ({"file_info": {"volume": 78}}, "file_identity", True),
        ({"file_info": {"attrs": E.FILE_ATTRIBUTE_DIRECTORY}}, "file_identity:OSError", True),
        ({"inherit_flags": 1}, "create_unusable:OSError", True),
        ({"fail": {"GetHandleInformation": 6}}, "create_unusable:OSError", True),
        ({"fail": {"CreateFileW": 112}}, "directory_identity:OSError", False),
        ({"dirs": {}}, "directory_missing", False),
        ({"dir_info": {"attrs": E.FILE_ATTRIBUTE_DIRECTORY | E.FILE_ATTRIBUTE_REPARSE_POINT}},
         "directory_reparse_point", False),
        ({"dir_info": {"attrs": E.FILE_ATTRIBUTE_NORMAL}}, "directory_identity:OSError", False),
        ({"dir_info": {"volume": 78}}, "directory_volume_mismatch", False),
        ({"final_override": "\\\\?\\C:\\elsewhere"}, "directory_path_mismatch", False),
        ({"final_override": "C:\\no-prefix"}, "directory_identity:OSError", False),
        ({"final_length": 0}, "directory_identity:OSError", False),
        ({"final_length": 3}, "directory_identity:OSError", False),
        ({"fail": {"GetFileInformationByHandle": 6}}, "directory_identity:OSError", False),
    ])
    def test_failures(self, monkeypatch, overrides, operation, partial):
        self._fails(monkeypatch, operation, partial, **overrides)

    def test_duplicate_create(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        name = RUN_DIR + "\\000-prelaunch.json"
        kernel32.files[name] = {"data": bytearray(b"existing"), "attrs": 0x80, "links": 1,
                                "volume": 77, "index": 1}
        w = win_writer(fs)
        with pytest.raises(E.EvidenceFailed, match="create_duplicate"):
            w.write("prelaunch", envelope(), {})
        assert bytes(kernel32.files[name]["data"]) == b"existing"
        assert not w.state.partial_record_possible

    @pytest.mark.parametrize("code, operation", [(112, "create:OSError"),
                                                 (5, "create:PermissionError"),
                                                 (3, "create:FileNotFoundError")])
    def test_create_errors(self, monkeypatch, code, operation):
        fs, kernel32 = win_fs(monkeypatch)
        real_create = kernel32.CreateFileW

        def create(path, access, share, attributes, disposition, flags, template):
            if disposition == E.CREATE_NEW:
                kernel32.last_error = code
                return -1
            return real_create(path, access, share, attributes, disposition, flags, template)

        kernel32.CreateFileW = create
        w = win_writer(fs)
        with pytest.raises(E.EvidenceFailed, match=operation):
            w.write("prelaunch", envelope(), {})
        assert kernel32.files == {} and not w.state.partial_record_possible

    def test_close_failure(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        real_close = kernel32.CloseHandle

        def close(handle):
            kind = kernel32.handles.get(handle, {}).get("kind")
            real_close(handle)
            return 0 if kind == "write" else 1

        kernel32.CloseHandle = close
        w = win_writer(fs)
        with pytest.raises(E.EvidenceFailed, match="close:OSError"):
            w.write("prelaunch", envelope(), {})
        assert w.state.partial_record_possible

    def test_short_read_and_growth(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        name = RUN_DIR + "\\x.json"
        kernel32.files[name] = {"data": bytearray(b"abc"), "attrs": 0x80, "links": 1,
                                "volume": 77, "index": 5}
        assert fs.read_all(name) == b"abc"
        kernel32.file_info = {"size": 5}
        with pytest.raises(OSError, match="short read"):
            fs.read_all(name)
        kernel32.file_info = {"size": 2}
        with pytest.raises(OSError, match="grew"):
            fs.read_all(name)
        kernel32.file_info = {"size": E.MAX_RECORD_BYTES + 1}
        with pytest.raises(OSError, match="too large"):
            fs.read_all(name)
        assert kernel32.handles == {}

    def test_file_identity_values(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        name = RUN_DIR + "\\x.json"
        kernel32.files[name] = {"data": bytearray(b"abcd"), "attrs": 0x80, "links": 1,
                                "volume": 77, "index": 9}
        assert fs.file_identity(name) == {"links": 1, "reparse": False, "volume_serial": 77,
                                          "file_index": 9, "size": 4}
        assert fs.dir_identity(RUN_DIR) == {"exists": True, "reparse": False,
                                            "final_path": RUN_DIR, "volume_serial": 77}
        assert kernel32.handles == {}

    def test_open_flags(self, monkeypatch):
        fs, kernel32 = win_fs(monkeypatch)
        win_writer(fs).write("prelaunch", envelope(), {})
        creates = [args for args in kernel32.calls_args if args[4] == E.CREATE_NEW]
        assert len(creates) == 1
        _, access, share, attributes, _, flags, template = creates[0]
        assert (access, share, attributes, template) == (E.GENERIC_WRITE, 0, None, None)
        assert flags == E.FILE_ATTRIBUTE_NORMAL | E.FILE_FLAG_WRITE_THROUGH
        for path, access, share, attributes, disposition, flags, template in kernel32.calls_args:
            if disposition == E.OPEN_EXISTING:
                assert flags & E.FILE_FLAG_OPEN_REPARSE_POINT        # never follow a reparse
        assert "GetHandleInformation" in kernel32.log
