"""
benchmarks/tests/test_win32_job.py

Mocked tests of the isolated Job Object backend.  A fake kernel32 and fake
handles are injected; any real DLL load, process, socket or subprocess
fails the test.  Authorization is opened in memory only.
"""

from __future__ import annotations

import ctypes
import os
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
import win32_job as W  # noqa: E402

HEAD = "a" * 40
PID, CREATED = 4242, "2026-10-09T12:00:00.000Z"
PROCESS, JOB = 0x101, 0x500
SOURCE = (_BENCH / "win32_job.py").read_text(encoding="utf-8")


class LiveAccessAttempt(AssertionError):
    """A test reached something real."""


@pytest.fixture(autouse=True)
def fail_closed(monkeypatch):
    def refuse(what):
        def call(*args, **kwargs):
            raise LiveAccessAttempt(f"{what} attempted")
        return call

    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, refuse(f"subprocess.{name}"))
    monkeypatch.setattr(os, "system", refuse("os.system"))
    monkeypatch.setattr(socket, "create_connection", refuse("a network connection"))
    monkeypatch.setattr(socket.socket, "connect", refuse("a socket connection"))
    monkeypatch.setattr(ctypes, "WinDLL", refuse("a real DLL load"))
    monkeypatch.setattr(W, "_kernel32_loader", refuse("the real kernel32 loader"))


def authorize(monkeypatch):
    """A probe authorization, opened in memory only."""
    monkeypatch.setattr(C, "PROBE_ENABLED", True)
    monkeypatch.setattr(C, "APPROVED_PROBES", frozenset({"probe-01"}))
    monkeypatch.setattr(C, "_git_head", lambda *require: HEAD)
    return C.authorize_probe("probe-01", HEAD)


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
        if name.startswith("_") or not hasattr(type(self._k), name):
            raise LiveAccessAttempt(f"unexpected Windows API {name}")
        return FakeFunction(name, getattr(self._k, name), self._k.log)


class FakeKernel32:
    """Fake job functions with scripted state; returns 0 for APIs in fail."""

    def __init__(self, **overrides):
        self.log, self.loads, self.fail = [], [], set()
        self.job, self.inherit_flags, self.in_job = JOB, 0, 1
        self.stored, self.readback, self.limit_length = None, None, None
        self.accounting = {"active": 1, "total": 1, "terminated": 0}
        self.pids, self.assigned, self.pid_length = [PID], None, None
        self.terminated, self.closed, self.created_with = [], [], None
        self.empty_on_terminate, self.last_error = True, 5
        self.__dict__.update(overrides)

    def loader(self, name):
        self.loads.append(name)
        return FakeDll(self)

    def CreateJobObjectW(self, attributes, name):
        self.created_with = (attributes, name)
        return 0 if "CreateJobObjectW" in self.fail else self.job

    def GetHandleInformation(self, handle, out):
        if "GetHandleInformation" in self.fail:
            return 0
        out._obj.value = self.inherit_flags
        return 1

    def SetInformationJobObject(self, job, information_class, address, size):
        if "SetInformationJobObject" in self.fail:
            return 0
        t = W._job_types()
        assert information_class == W.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION
        assert size == ctypes.sizeof(t.EXTENDED)
        info = t.EXTENDED.from_address(address)
        self.stored = (info.BasicLimitInformation.LimitFlags,
                       info.BasicLimitInformation.ActiveProcessLimit)
        return 1

    def QueryInformationJobObject(self, job, information_class, address, length, returned):
        if "QueryInformationJobObject" in self.fail:
            return 0
        t = W._job_types()
        if information_class == W.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION:
            info = t.EXTENDED()
            flags, limit = self.readback or self.stored
            info.BasicLimitInformation.LimitFlags = flags
            info.BasicLimitInformation.ActiveProcessLimit = limit
            data, size = bytes(info), self.limit_length
        elif information_class == W.JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION:
            info = t.ACCOUNTING(ActiveProcesses=self.accounting["active"],
                                TotalProcesses=self.accounting["total"],
                                TotalTerminatedProcesses=self.accounting["terminated"])
            data, size = bytes(info), None
        elif information_class == W.JOB_OBJECT_BASIC_PROCESS_ID_LIST:
            offset = t.PID_LIST.ProcessIdList.offset
            assigned = len(self.pids) if self.assigned is None else self.assigned
            header = (ctypes.c_uint32 * 2)(assigned, len(self.pids))
            pad = b"\0" * (offset - ctypes.sizeof(header))
            data = bytes(header) + pad + bytes((ctypes.c_size_t * len(self.pids))(*self.pids))
            size = self.pid_length
        else:
            raise AssertionError(f"unexpected class {information_class}")
        if len(data) > length:
            return 0
        ctypes.memmove(address, data, len(data))
        returned._obj.value = len(data) if size is None else size
        return 1

    def IsProcessInJob(self, process, job, out):
        if "IsProcessInJob" in self.fail:
            return 0
        out._obj.value = self.in_job
        return 1

    def AssignProcessToJobObject(self, job, process):
        return 0 if "AssignProcessToJobObject" in self.fail else 1

    def TerminateJobObject(self, job, code):
        self.terminated.append((job, code))
        if "TerminateJobObject" in self.fail:
            return 0
        if self.empty_on_terminate:
            self.accounting = {**self.accounting, "active": 0, "terminated": 1}
        return 1

    def CloseHandle(self, handle):
        self.closed.append(handle)
        return 0 if "CloseHandle" in self.fail else 1


def make_api(monkeypatch, **overrides):
    kernel32 = FakeKernel32(**overrides)
    api = W.CtypesJobApi(authorize(monkeypatch), loader=kernel32.loader,
                         last_error=lambda: kernel32.last_error)
    return api, kernel32


def make_controller(monkeypatch, **overrides):
    api, kernel32 = make_api(monkeypatch, **overrides)
    return W.JobController(api.auth, api), kernel32


def verified(monkeypatch, **overrides):
    controller, kernel32 = make_controller(monkeypatch, **overrides)
    controller.configure()
    membership = controller.verify_membership(PROCESS, PID, CREATED, CREATED)
    return controller, kernel32, membership


def accept_policy(monkeypatch):
    """In memory only: the policy a future reviewed change could set."""
    monkeypatch.setattr(P, "TERMINATION_POLICY", W.TERMINATION_ACCEPTED)


# ---------------------------------------------------------------------------
# Layouts, signatures, isolation
# ---------------------------------------------------------------------------

class TestLayoutAndSignatures:
    @pytest.mark.skipif(ctypes.sizeof(ctypes.c_void_p) != 8, reason="x64 layouts")
    def test_x64_layouts(self):
        t = W._job_types()
        assert ctypes.sizeof(t.BASIC) == 64 and ctypes.sizeof(t.IO) == 48
        assert ctypes.sizeof(t.EXTENDED) == 144 and ctypes.sizeof(t.ACCOUNTING) == 48
        assert t.PID_LIST.ProcessIdList.offset == 8
        assert t.BASIC.LimitFlags.offset == 16 and t.BASIC.ActiveProcessLimit.offset == 40
        assert t.ACCOUNTING.ActiveProcesses.offset == 40

    def test_signatures(self):
        t = W._job_types()
        signatures = W._job_signatures(t)
        assert set(signatures) == {
            "CreateJobObjectW", "SetInformationJobObject", "QueryInformationJobObject",
            "AssignProcessToJobObject", "IsProcessInJob", "TerminateJobObject",
            "GetHandleInformation", "CloseHandle"}
        assert signatures["CreateJobObjectW"][0] is t.w.HANDLE
        assert len(signatures["QueryInformationJobObject"][1]) == 5
        assert signatures["TerminateJobObject"][1] == [t.w.HANDLE, t.w.UINT]
        assert (W.JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION, W.JOB_OBJECT_BASIC_PROCESS_ID_LIST,
                W.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, W.PROC_THREAD_ATTRIBUTE_JOB_LIST,
                W.HANDLE_FLAG_INHERIT) == (1, 3, 9, 0x0002000D, 1)

    def test_harness_never_imports_the_backend(self):
        harness = (_BENCH / "calibrate_generator.py").read_text(encoding="utf-8")
        planner = (_BENCH / "appcontainer_preflight.py").read_text(encoding="utf-8")
        assert "win32_job" not in harness and "win32_job" not in planner

    def test_source_scope(self):
        code = SOURCE.split('"""', 2)[2]
        for word in ("subprocess", "socket", "TerminateProcess", "OpenProcess",
                     "__del__", "atexit", "weakref", "finalize", "CreateProcess(",
                     "os.kill", "taskkill"):
            assert word not in code, word


class TestGates:
    def test_refuses_while_disabled(self):
        with pytest.raises(C.ExecutionDisabled):
            W.CtypesJobApi(None, loader=FakeKernel32().loader)
        with pytest.raises(C.ExecutionDisabled):
            W.JobController(None, object())

    def test_gate_rechecked_before_loading(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch)
        assert not api.loaded and kernel32.loads == []
        monkeypatch.setattr(C, "APPROVED_PROBES", frozenset())
        with pytest.raises(C.ExecutionDisabled):
            api.create_job()
        assert kernel32.loads == [] and kernel32.log == []

    def test_controller_gate_rechecked(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch)
        monkeypatch.setattr(C, "PROBE_ENABLED", False)
        for call in (controller.configure, controller.close):
            with pytest.raises(C.ExecutionDisabled):
                call()
        assert kernel32.log == []

    def test_real_loader_is_never_used(self, monkeypatch):
        api = W.CtypesJobApi(authorize(monkeypatch))
        with pytest.raises(LiveAccessAttempt, match="real kernel32 loader"):
            api.create_job()


# ---------------------------------------------------------------------------
# Native wrappers
# ---------------------------------------------------------------------------

class TestCtypesJobApi:
    def test_create_job_is_unnamed_and_default_secured(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch)
        assert api.create_job() == JOB and kernel32.created_with == (None, None)
        assert kernel32.loads == ["kernel32"]
        api.handle_inheritable(JOB)
        assert kernel32.loads == ["kernel32"]                     # loaded once

    @pytest.mark.parametrize("name, call", [
        ("CreateJobObjectW", lambda api: api.create_job()),
        ("GetHandleInformation", lambda api: api.handle_inheritable(JOB)),
        ("SetInformationJobObject", lambda api: api.set_limits(JOB, 0x2408, 1)),
        ("QueryInformationJobObject", lambda api: api.query_accounting(JOB)),
        ("IsProcessInJob", lambda api: api.is_process_in_job(PROCESS, JOB)),
        ("AssignProcessToJobObject", lambda api: api.assign(JOB, PROCESS)),
        ("TerminateJobObject", lambda api: api.terminate_job(JOB, 0x0BAE)),
        ("CloseHandle", lambda api: api.close(JOB)),
    ])
    def test_native_failures_carry_get_last_error(self, monkeypatch, name, call):
        api, _ = make_api(monkeypatch, fail={name}, last_error=1450)
        with pytest.raises(C.Win32Error, match=f"{name} failed \\(Windows error 1450\\)"):
            call(api)

    def test_limits_round_trip(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch)
        api.set_limits(JOB, 0x2408, 1)
        assert kernel32.stored == (0x2408, 1)
        assert api.query_limits(JOB) == {"limit_flags": 0x2408, "active_process_limit": 1}

    def test_malformed_lengths(self, monkeypatch):
        api, _ = make_api(monkeypatch, stored=(0x2408, 1), limit_length=100)
        with pytest.raises(C.Win32Error, match="limit length 100"):
            api.query_limits(JOB)
        api, _ = make_api(monkeypatch, stored=(0x2408, 1), limit_length=10_000)
        with pytest.raises(C.Win32Error, match="exceeds the buffer"):
            api.query_limits(JOB)

    def test_accounting_and_inheritance(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch, inherit_flags=0x3)
        assert api.query_accounting(JOB) == {"active": 1, "total": 1, "terminated": 0}
        assert api.handle_inheritable(JOB) is True
        kernel32.inherit_flags = 0x2
        assert api.handle_inheritable(JOB) is False

    @pytest.mark.parametrize("overrides, message", [
        ({"pids": [PID, 7], "assigned": 3}, "inconsistent"),
        ({"pids": list(range(1, 10))}, None),
        ({"pids": [PID], "pid_length": 4}, "PID list length 4"),
        ({"pids": [PID, 7], "pid_length": 12}, "inconsistent"),
    ])
    def test_pid_list_malformed(self, monkeypatch, overrides, message):
        api, _ = make_api(monkeypatch, **overrides)
        with pytest.raises(C.Win32Error, match=message or ""):
            api.query_pid_list(JOB)

    def test_pid_list(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch, pids=[PID, 7])
        assert api.query_pid_list(JOB) == [PID, 7]
        kernel32.pids = []
        assert api.query_pid_list(JOB) == []

    def test_is_process_in_job(self, monkeypatch):
        api, kernel32 = make_api(monkeypatch)
        assert api.is_process_in_job(PROCESS, JOB) is True
        kernel32.in_job = 0
        assert api.is_process_in_job(PROCESS, JOB) is False
        kernel32.in_job = 2
        with pytest.raises(C.Win32Error, match="malformed result 2"):
            api.is_process_in_job(PROCESS, JOB)


# ---------------------------------------------------------------------------
# Controller: configuration
# ---------------------------------------------------------------------------

class TestConfigure:
    def test_configured_exactly(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch)
        assert controller.configure() == JOB and controller.ownership == "owned_empty"
        assert kernel32.stored == (P.JOB_LIMIT_POLICY["required_flags"], 1)
        assert kernel32.log == ["CreateJobObjectW", "GetHandleInformation",
                                "SetInformationJobObject", "QueryInformationJobObject"]
        with pytest.raises(W.JobFailure, match="configure in state owned_empty"):
            controller.configure()

    def test_creation_failure(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch, fail={"CreateJobObjectW"})
        with pytest.raises(W.JobFailure) as raised:
            controller.configure()
        assert raised.value.key == "create_job_failed" and controller.ownership == "none"
        assert kernel32.closed == []

    @pytest.mark.parametrize("overrides, key", [
        ({"inherit_flags": 1}, "inheritable_job_handle"),
        ({"fail": {"SetInformationJobObject"}}, "set_job_information_failed"),
        ({"readback": (0x0408, 1)}, "job_readback_mismatch"),
        ({"readback": (0x2408 | 0x800, 1)}, "job_readback_mismatch"),
        ({"readback": (0x2408 | 0x2, 1)}, "job_readback_mismatch"),
        ({"readback": (0x2408, 2)}, "job_readback_mismatch"),
        ({"limit_length": 100}, "job_readback_mismatch"),
        ({"fail": {"GetHandleInformation"}}, "job_readback_mismatch"),
    ])
    def test_refusals_close_the_empty_job_once(self, monkeypatch, overrides, key):
        controller, kernel32 = make_controller(monkeypatch, **overrides)
        with pytest.raises(W.JobFailure) as raised:
            controller.configure()
        assert raised.value.key == key
        assert kernel32.closed == [JOB]                           # no handle leak
        assert controller.ownership == "closed" and raised.value.ownership == "closed"
        assert "TerminateJobObject" not in kernel32.log

    def test_close_failure_during_refusal_retains(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch, inherit_flags=1,
                                               fail={"CloseHandle"})
        with pytest.raises(W.JobFailure) as raised:
            controller.configure()
        assert controller.ownership == "retained_close_failed"
        assert raised.value.ownership == "retained_close_failed"
        assert any("close after" in e for e in controller.errors)


# ---------------------------------------------------------------------------
# Controller: membership
# ---------------------------------------------------------------------------

class TestMembership:
    def test_exactly_one_verified_child(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        assert membership == W.Membership(PID, CREATED, 1)
        assert controller.ownership == "owned_with_child"

    @pytest.mark.parametrize("overrides, args, message", [
        ({"in_job": 0}, None, "outside the job"),
        ({"pids": [PID, 999]}, None, "not \\[4242\\]"),
        ({"pids": []}, None, "not \\[4242\\]"),
        ({"pids": [999]}, None, "not \\[4242\\]"),
        ({"accounting": {"active": 2, "total": 2, "terminated": 0}}, None, "2 active"),
        ({}, (PROCESS, PID, CREATED, "2026-10-09T12:00:01.000Z"), "creation time differs"),
        ({}, (0, PID, CREATED, CREATED), "no valid process handle"),
        ({}, (PROCESS, 0, CREATED, CREATED), "PID is invalid"),
        ({}, (PROCESS, True, CREATED, CREATED), "PID is invalid"),
    ])
    def test_contradictory_membership(self, monkeypatch, overrides, args, message):
        controller, kernel32 = make_controller(monkeypatch, **overrides)
        controller.configure()
        with pytest.raises(W.JobFailure, match=message) as raised:
            controller.verify_membership(*(args or (PROCESS, PID, CREATED, CREATED)))
        assert raised.value.key == "not_in_job"
        assert controller.ownership == "retained_unverified" and controller.membership is None
        assert kernel32.closed == []                              # never silently closed

    def test_native_error_during_membership(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch)
        controller.configure()
        kernel32.fail = {"QueryInformationJobObject"}
        with pytest.raises(W.JobFailure, match="Win32Error"):
            controller.verify_membership(PROCESS, PID, CREATED, CREATED)
        assert controller.ownership == "retained_unverified" and kernel32.closed == []

    def test_membership_requires_a_configured_job(self, monkeypatch):
        controller, _ = make_controller(monkeypatch)
        with pytest.raises(W.JobFailure, match="membership in state none"):
            controller.verify_membership(PROCESS, PID, CREATED, CREATED)


# ---------------------------------------------------------------------------
# Controller: termination (gated)
# ---------------------------------------------------------------------------

def _wait(signaled=True, log=None):
    def wait(process, milliseconds):
        if log is not None:
            log.append(("wait", process, milliseconds))
        return signaled
    return wait


def _code(value):
    return lambda process: value


class TestTerminationGate:
    def test_design_only_policy_blocks_before_any_native_call(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        assert P.TERMINATION_POLICY == "conditionally_accepted_design_only"
        calls_before = list(kernel32.log)
        with pytest.raises(W.TerminationRefused, match="not accepted"):
            controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == [] and kernel32.log == calls_before
        assert controller.ownership == "owned_with_child"

    @pytest.mark.parametrize("policy", ["conditionally_accepted_design_only", "never",
                                        "Accepted", ""])
    def test_every_other_policy_value_refuses(self, monkeypatch, policy):
        controller, kernel32, membership = verified(monkeypatch)
        monkeypatch.setattr(P, "TERMINATION_POLICY", policy)
        with pytest.raises(W.TerminationRefused):
            controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == []

    def test_refused_without_authorization_even_with_policy(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        monkeypatch.setattr(C, "PROBE_ENABLED", False)
        with pytest.raises(C.ExecutionDisabled):
            controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == []


class TestTerminationInterface:
    """The future interface, reachable only with the policy accepted in
    memory and a fake kernel32."""

    def test_verified_termination(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        waits = []
        outcome = controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                       wait=_wait(True, waits),
                                       exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert outcome.verified and outcome.active_after == 0
        assert kernel32.terminated == [(JOB, P.TIMEOUT_EXIT_CODE)]
        assert waits == [("wait", PROCESS, 5000)]
        assert controller.ownership == "child_exited"
        state = P.LifecycleState(phase="resumed", process_created=True, pid=PID,
                                 creation_time=CREATED, job_assignment="assigned_at_creation",
                                 job_limits_verified=True,
                                 execution_history=P.MAY_HAVE_EXECUTED,
                                 liveness=P.ALIVE_AT_LAST_CHECK, handles_retained=True)
        state = P.lifecycle_transition(state, "terminate_result",
                                       outcome.lifecycle_evidence(PID, CREATED))
        assert (state.liveness, state.termination_cause) == (P.CONFIRMED_EXITED,
                                                             "job_terminated_timeout")
        controller.close()
        assert controller.ownership == "closed" and kernel32.closed == [JOB]

    def test_stale_membership_never_terminates(self, monkeypatch):
        controller, kernel32, old = verified(monkeypatch)
        controller.verify_membership(PROCESS, PID, CREATED, CREATED)       # newer snapshot
        accept_policy(monkeypatch)
        with pytest.raises(W.TerminationRefused, match="stale or foreign"):
            controller.terminate("job_terminated_timeout", PROCESS, old, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        foreign = W.Membership(999, CREATED, controller.membership.epoch)
        with pytest.raises(W.TerminationRefused, match="stale or foreign"):
            controller.terminate("job_terminated_timeout", PROCESS, foreign, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == []

    @pytest.mark.parametrize("change", [
        {"pids": [PID, 999]}, {"pids": []}, {"in_job": 0},
        {"accounting": {"active": 2, "total": 2, "terminated": 0}},
    ])
    def test_membership_changed_since_the_snapshot(self, monkeypatch, change):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        kernel32.__dict__.update(change)
        with pytest.raises(W.TerminationRefused, match="no longer verified"):
            controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                 wait=_wait(), exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == [] and controller.ownership == "retained_unverified"

    def test_wrong_handle_identity_never_terminates(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        with pytest.raises(W.TerminationRefused):
            controller.terminate("job_terminated_timeout", PROCESS, membership,
                                 "2026-10-09T13:00:00.000Z", wait=_wait(),
                                 exit_code=_code(P.TIMEOUT_EXIT_CODE))
        assert kernel32.terminated == []

    def test_unknown_cause(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        with pytest.raises(W.TerminationRefused, match="unknown cause"):
            controller.terminate("reboot", PROCESS, membership, CREATED, wait=_wait(),
                                 exit_code=_code(0))
        assert kernel32.terminated == []

    @pytest.mark.parametrize("overrides, wait, code, active_reason", [
        ({"fail": {"TerminateJobObject"}}, _wait(True), P.TIMEOUT_EXIT_CODE, "api failure"),
        ({}, _wait(False), P.TIMEOUT_EXIT_CODE, "no signal"),
        ({}, _wait(True), 0, "wrong exit code"),
        ({"empty_on_terminate": False}, _wait(True), P.TIMEOUT_EXIT_CODE, "job not empty"),
    ])
    def test_termination_without_exit_proof(self, monkeypatch, overrides, wait, code,
                                            active_reason):
        controller, kernel32, membership = verified(monkeypatch, **overrides)
        accept_policy(monkeypatch)
        outcome = controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                       wait=wait, exit_code=_code(code))
        assert not outcome.verified, active_reason
        assert controller.ownership == "retained_unverified"
        assert any("exit not verified" in e for e in controller.errors)
        with pytest.raises(W.TerminationRefused, match="no verified child"):
            controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                 wait=wait, exit_code=_code(code))     # never retried
        with pytest.raises(W.JobFailure, match="unsafe_close_refused"):
            controller.close()
        assert kernel32.closed == [] and len(kernel32.terminated) == 1

    def test_lifecycle_evidence_is_truthful(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)
        outcome = controller.terminate("job_terminated_timeout", PROCESS, membership, CREATED,
                                       wait=_wait(True), exit_code=_code(7))
        assert outcome.lifecycle_evidence(PID, CREATED) == {
            "pid": PID, "creation_time": CREATED, "cause": "job_terminated_timeout",
            "api_ok": True, "signaled": True, "exit_code": 7}

    def test_interrupted_termination_retains_ownership(self, monkeypatch):
        controller, kernel32, membership = verified(monkeypatch)
        accept_policy(monkeypatch)

        def interrupted(process, milliseconds):
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            controller.terminate("job_terminated_operator", PROCESS, membership, CREATED,
                                 wait=interrupted, exit_code=_code(P.OPERATOR_EXIT_CODE))
        assert controller.ownership == "retained_unverified"
        assert any("interrupted" in e for e in controller.errors)
        with pytest.raises(W.JobFailure):
            controller.close()
        assert kernel32.closed == []


# ---------------------------------------------------------------------------
# Controller: natural exit and close
# ---------------------------------------------------------------------------

class TestNaturalExitAndClose:
    def test_natural_exit(self, monkeypatch):
        controller, kernel32, _ = verified(monkeypatch)
        assert controller.record_natural_exit(PROCESS, wait=_wait(False),
                                              exit_code=_code(0)) is None
        assert controller.ownership == "owned_with_child"
        kernel32.accounting = {"active": 0, "total": 1, "terminated": 0}
        assert controller.record_natural_exit(PROCESS, wait=_wait(True),
                                              exit_code=_code(0)) == 0
        assert controller.ownership == "child_exited"
        controller.close()
        assert kernel32.closed == [JOB] and "TerminateJobObject" not in kernel32.log

    def test_exit_with_a_non_empty_job(self, monkeypatch):
        controller, kernel32, _ = verified(monkeypatch)
        with pytest.raises(W.JobFailure, match="not empty"):
            controller.record_natural_exit(PROCESS, wait=_wait(True), exit_code=_code(0))
        assert controller.ownership == "retained_unverified"

    def test_close_an_empty_job(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch,
                                               accounting={"active": 0, "total": 0,
                                                           "terminated": 0})
        controller.configure()
        controller.close()
        assert controller.ownership == "closed" and kernel32.closed == [JOB]
        with pytest.raises(W.JobFailure, match="state closed"):
            controller.close()

    def test_live_child_job_is_never_closed(self, monkeypatch):
        controller, kernel32, _ = verified(monkeypatch)
        with pytest.raises(W.JobFailure, match="unsafe_close_refused"):
            controller.close()
        assert kernel32.closed == [] and controller.ownership == "owned_with_child"

    def test_active_process_found_at_close(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch)
        controller.configure()
        with pytest.raises(W.JobFailure, match="1 active processes"):
            controller.close()
        assert controller.ownership == "retained_unverified" and kernel32.closed == []

    def test_close_failure_is_never_retried(self, monkeypatch):
        controller, kernel32 = make_controller(monkeypatch, fail={"CloseHandle"},
                                               accounting={"active": 0, "total": 0,
                                                           "terminated": 0})
        controller.configure()
        with pytest.raises(W.JobFailure, match="close_failed"):
            controller.close()
        assert controller.ownership == "retained_close_failed"
        with pytest.raises(W.JobFailure, match="state retained_close_failed"):
            controller.close()
        assert kernel32.closed == [JOB]

    def test_ownership_states(self):
        assert W.OWNERSHIP_STATES == (
            "none", "owned_empty", "owned_with_child", "child_exited",
            "retained_unverified", "retained_close_failed", "closed")
