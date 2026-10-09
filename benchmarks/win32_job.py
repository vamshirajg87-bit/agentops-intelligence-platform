"""
benchmarks/win32_job.py

An isolated, fail-closed Windows Job Object backend for the future staged
AppContainer child.  Nothing calls it: the calibration harness does not
import this module, and no existing process creation, resume, timeout or
termination behaviour changes.

    CtypesJobApi   documented kernel32 job functions through ctypes, every
                   call gated by the harness authorization (closed), the
                   DLL loaded only on first use through an injectable
                   loader; malformed or short native output is an error,
                   never a default.
    JobController  explicit ownership of exactly one unnamed,
                   non-inheritable job: configuration with exact read-back
                   (KILL_ON_JOB_CLOSE, ACTIVE_PROCESS limit 1,
                   DIE_ON_UNHANDLED_EXCEPTION, no breakaway), membership
                   verification of exactly one child (a snapshot, not a
                   guarantee), a termination interface that refuses while
                   the lifecycle TERMINATION_POLICY is design-only, and a
                   close that never closes a job that may hold a live child
                   (closing it would terminate the child).

There is no automatic retry and no cleanup through object destruction.

Standard library only.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

HERE: Path = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import appcontainer_preflight as P  # noqa: E402
import calibrate_generator as C  # noqa: E402

#: JOBOBJECTINFOCLASS values (documented).
JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
JOB_OBJECT_BASIC_PROCESS_ID_LIST = 3
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
#: For the future creation path (UpdateProcThreadAttribute); unused here.
PROC_THREAD_ATTRIBUTE_JOB_LIST = 0x0002000D
HANDLE_FLAG_INHERIT = 0x00000001
#: How many member PIDs a membership query can return; more is an error.
PID_LIST_CAPACITY = 8
#: The only lifecycle policy under which termination may be requested.
TERMINATION_ACCEPTED = "accepted"

OWNERSHIP_STATES: tuple[str, ...] = (
    "none",                 # no job created
    "owned_empty",          # job configured, no child verified in it
    "owned_with_child",     # exactly one verified child (a snapshot)
    "child_exited",         # the child's exit was verified; the job is empty
    "retained_unverified",  # a child may live: the handle is never closed here
    "retained_close_failed",  # CloseHandle failed; nothing is retried
    "closed",
)


class JobFailure(C.CalibrationError):
    """A refused or failed job operation, with its FAILURE_ACTIONS key."""

    def __init__(self, key: str, message: str, *, ownership: str) -> None:
        super().__init__(f"{key}: {message}")
        self.key, self.ownership = key, ownership


class TerminationRefused(JobFailure):
    """Termination was requested where it is not permitted."""


_TYPES: Any = None


def _job_types() -> Any:
    """ctypes structures of the documented job types (built on first use)."""
    global _TYPES
    if _TYPES is None:
        import ctypes
        import types
        from ctypes import wintypes as w

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong),
                        ("PerJobUserTimeLimit", ctypes.c_longlong),
                        ("LimitFlags", w.DWORD),
                        ("MinimumWorkingSetSize", ctypes.c_size_t),
                        ("MaximumWorkingSetSize", ctypes.c_size_t),
                        ("ActiveProcessLimit", w.DWORD),
                        ("Affinity", ctypes.c_size_t),
                        ("PriorityClass", w.DWORD),
                        ("SchedulingClass", w.DWORD)]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                        ("IoInfo", IO_COUNTERS),
                        ("ProcessMemoryLimit", ctypes.c_size_t),
                        ("JobMemoryLimit", ctypes.c_size_t),
                        ("PeakProcessMemoryUsed", ctypes.c_size_t),
                        ("PeakJobMemoryUsed", ctypes.c_size_t)]

        class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
            _fields_ = [("TotalUserTime", ctypes.c_longlong),
                        ("TotalKernelTime", ctypes.c_longlong),
                        ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                        ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                        ("TotalPageFaultCount", w.DWORD),
                        ("TotalProcesses", w.DWORD),
                        ("ActiveProcesses", w.DWORD),
                        ("TotalTerminatedProcesses", w.DWORD)]

        class JOBOBJECT_BASIC_PROCESS_ID_LIST(ctypes.Structure):
            _fields_ = [("NumberOfAssignedProcesses", w.DWORD),
                        ("NumberOfProcessIdsInList", w.DWORD),
                        ("ProcessIdList", ctypes.c_size_t * 1)]

        _TYPES = types.SimpleNamespace(
            ctypes=ctypes, w=w, BASIC=JOBOBJECT_BASIC_LIMIT_INFORMATION, IO=IO_COUNTERS,
            EXTENDED=JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            ACCOUNTING=JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
            PID_LIST=JOBOBJECT_BASIC_PROCESS_ID_LIST)
    return _TYPES


def _job_signatures(t: Any) -> dict[str, tuple[Any, list[Any]]]:
    """name -> (restype, argtypes); all in kernel32."""
    c, w = t.ctypes, t.w
    return {
        "CreateJobObjectW": (w.HANDLE, [c.c_void_p, w.LPCWSTR]),
        "SetInformationJobObject": (w.BOOL, [w.HANDLE, c.c_int, c.c_void_p, w.DWORD]),
        "QueryInformationJobObject": (w.BOOL, [w.HANDLE, c.c_int, c.c_void_p, w.DWORD,
                                               c.POINTER(w.DWORD)]),
        "AssignProcessToJobObject": (w.BOOL, [w.HANDLE, w.HANDLE]),
        "IsProcessInJob": (w.BOOL, [w.HANDLE, w.HANDLE, c.POINTER(w.BOOL)]),
        "TerminateJobObject": (w.BOOL, [w.HANDLE, w.UINT]),
        "GetHandleInformation": (w.BOOL, [w.HANDLE, c.POINTER(w.DWORD)]),
        "CloseHandle": (w.BOOL, [w.HANDLE]),
    }


def _kernel32_loader(name: str) -> Any:
    import ctypes
    return ctypes.WinDLL(name, use_last_error=True)


class CtypesJobApi:
    """The documented job functions; gated per call; kernel32 loaded lazily."""

    def __init__(self, auth: Any, *, loader: Optional[Callable[[str], Any]] = None,
                 last_error: Optional[Callable[[], int]] = None) -> None:
        self.auth = C._require_any_authorization(auth)
        self._loader = loader or _kernel32_loader
        self._kernel32: Any = None
        self._last_error = last_error

    @property
    def loaded(self) -> bool:
        return self._kernel32 is not None

    def _api(self, name: str) -> Any:
        C._require_any_authorization(self.auth)
        restype, argtypes = _job_signatures(_job_types())[name]
        if self._kernel32 is None:
            self._kernel32 = self._loader("kernel32")
        function = getattr(self._kernel32, name)
        function.restype, function.argtypes = restype, argtypes
        return function

    def _error(self, api: str) -> C.Win32Error:
        code = (self._last_error or _job_types().ctypes.get_last_error)()
        return C.Win32Error(api, code)

    def create_job(self) -> int:
        """An unnamed job with default (non-inheritable) security."""
        handle = self._api("CreateJobObjectW")(None, None)
        if not C._valid_handle(handle):
            raise self._error("CreateJobObjectW")
        return int(handle)

    def handle_inheritable(self, handle: int) -> bool:
        t = _job_types()
        flags = t.w.DWORD(0)
        if not self._api("GetHandleInformation")(handle, t.ctypes.byref(flags)):
            raise self._error("GetHandleInformation")
        return bool(flags.value & HANDLE_FLAG_INHERIT)

    def set_limits(self, job: int, limit_flags: int, active_process_limit: int) -> None:
        t = _job_types()
        info = t.EXTENDED()
        info.BasicLimitInformation.LimitFlags = limit_flags
        info.BasicLimitInformation.ActiveProcessLimit = active_process_limit
        if not self._api("SetInformationJobObject")(
                job, JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, t.ctypes.addressof(info),
                t.ctypes.sizeof(info)):
            raise self._error("SetInformationJobObject")

    def _query(self, job: int, information_class: int, buffer: Any) -> int:
        t = _job_types()
        returned = t.w.DWORD(0)
        if not self._api("QueryInformationJobObject")(
                job, information_class, t.ctypes.addressof(buffer), t.ctypes.sizeof(buffer),
                t.ctypes.byref(returned)):
            raise self._error("QueryInformationJobObject")
        if returned.value > t.ctypes.sizeof(buffer):
            raise C.Win32Error("QueryInformationJobObject",
                               f"returned length {returned.value} exceeds the buffer")
        return int(returned.value)

    def query_limits(self, job: int) -> dict[str, int]:
        t = _job_types()
        info = t.EXTENDED()
        length = self._query(job, JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, info)
        if length != t.ctypes.sizeof(info):
            raise C.Win32Error("QueryInformationJobObject", f"limit length {length}")
        return {"limit_flags": int(info.BasicLimitInformation.LimitFlags),
                "active_process_limit": int(info.BasicLimitInformation.ActiveProcessLimit)}

    def query_accounting(self, job: int) -> dict[str, int]:
        t = _job_types()
        info = t.ACCOUNTING()
        length = self._query(job, JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION, info)
        if length != t.ctypes.sizeof(info):
            raise C.Win32Error("QueryInformationJobObject", f"accounting length {length}")
        return {"active": int(info.ActiveProcesses), "total": int(info.TotalProcesses),
                "terminated": int(info.TotalTerminatedProcesses)}

    def query_pid_list(self, job: int) -> list[int]:
        """The member PIDs; a truncated, inconsistent or short answer is an
        error."""
        t = _job_types()
        c = t.ctypes
        offset = t.PID_LIST.ProcessIdList.offset
        size = offset + PID_LIST_CAPACITY * c.sizeof(c.c_size_t)
        buffer = (c.c_size_t * (-(-size // c.sizeof(c.c_size_t))))()
        length = self._query(job, JOB_OBJECT_BASIC_PROCESS_ID_LIST, buffer)
        if length < offset:
            raise C.Win32Error("QueryInformationJobObject", f"PID list length {length}")
        header = t.PID_LIST.from_buffer(buffer)
        assigned, listed = int(header.NumberOfAssignedProcesses), int(
            header.NumberOfProcessIdsInList)
        if listed > PID_LIST_CAPACITY or listed != assigned \
                or length < offset + listed * c.sizeof(c.c_size_t):
            raise C.Win32Error("QueryInformationJobObject",
                               f"PID list inconsistent ({assigned} assigned, {listed} listed, "
                               f"length {length})")
        ids = (c.c_size_t * listed).from_buffer(buffer, offset) if listed else []
        return [int(pid) for pid in ids]

    def assign(self, job: int, process: int) -> None:
        if not self._api("AssignProcessToJobObject")(job, process):
            raise self._error("AssignProcessToJobObject")

    def is_process_in_job(self, process: int, job: int) -> bool:
        t = _job_types()
        result = t.w.BOOL(0)
        if not self._api("IsProcessInJob")(process, job, t.ctypes.byref(result)):
            raise self._error("IsProcessInJob")
        if result.value not in (0, 1):
            raise C.Win32Error("IsProcessInJob", f"malformed result {result.value}")
        return result.value == 1

    def terminate_job(self, job: int, exit_code: int) -> None:
        if not self._api("TerminateJobObject")(job, exit_code):
            raise self._error("TerminateJobObject")

    def close(self, handle: int) -> None:
        if not self._api("CloseHandle")(handle):
            raise self._error("CloseHandle")


@dataclass(frozen=True)
class Membership:
    """One membership snapshot: exactly this child, at this moment."""
    pid: int
    creation_time: str
    epoch: int


@dataclass
class TerminationOutcome:
    cause: str
    api_ok: bool = False
    signaled: bool = False
    exit_code: Optional[int] = None
    active_after: Optional[int] = None
    verified: bool = False
    errors: list[str] = field(default_factory=list)

    def lifecycle_evidence(self, pid: int, creation_time: str) -> dict[str, Any]:
        """Truthful evidence for appcontainer_preflight.lifecycle_transition
        (the process-level wait and exit code as observed).  Job emptiness is
        tracked by the controller's ownership state."""
        return {"pid": pid, "creation_time": creation_time, "cause": self.cause,
                "api_ok": self.api_ok, "signaled": self.signaled,
                "exit_code": self.exit_code if self.signaled else None}


class JobController:
    """
    Explicit ownership of one job.  Every method refuses outside its
    ownership state; nothing is retried; nothing is cleaned up implicitly.
    """

    def __init__(self, auth: Any, api: Any) -> None:
        self.auth = C._require_any_authorization(auth)
        self.api = api
        self.ownership = "none"
        self.job: Optional[int] = None
        self.membership: Optional[Membership] = None
        self._epoch = 0
        self.errors: list[str] = []

    def _gate(self) -> None:
        C._require_any_authorization(self.auth)

    def _fail(self, key: str, message: str, cls: type = JobFailure) -> JobFailure:
        self.errors.append(f"{key}: {message}")
        return cls(key, message, ownership=self.ownership)

    def _close_empty(self, why: str) -> None:
        """Close a job that cannot hold a process yet (configuration
        failure).  A close failure retains the handle."""
        try:
            self.api.close(self.job)
        except Exception as exc:
            self.ownership = "retained_close_failed"
            self.errors.append(f"close after {why} failed: {exc}")
            return
        self.ownership, self.job = "closed", None

    def configure(self) -> int:
        """Create and configure the job; exact read-back required."""
        self._gate()
        if self.ownership != "none":
            raise self._fail("job_readback_mismatch", f"configure in state {self.ownership}")
        try:
            self.job = self.api.create_job()
        except Exception as exc:
            raise self._fail("create_job_failed", str(exc)) from None
        self.ownership = "owned_empty"
        try:
            if self.api.handle_inheritable(self.job):
                raise self._fail("inheritable_job_handle", "the job handle is inheritable")
            policy = P.JOB_LIMIT_POLICY
            try:
                self.api.set_limits(self.job, policy["required_flags"],
                                    policy["active_process_limit"])
            except Exception as exc:
                raise self._fail("set_job_information_failed", str(exc)) from None
            readback = self.api.query_limits(self.job)
            problems = [p for p in P.job_limits_problems(
                {**readback, "job_handle_inheritable": False, "in_job_before_resume": True})]
            if problems:
                raise self._fail("job_readback_mismatch", "; ".join(problems))
        except JobFailure as failure:
            self._close_empty(failure.key)
            failure.ownership = self.ownership
            raise
        except Exception as exc:
            failure = self._fail("job_readback_mismatch", f"{type(exc).__name__}: {exc}")
            self._close_empty(failure.key)
            failure.ownership = self.ownership
            raise failure from None
        return self.job

    def _snapshot(self, process: int, pid: int, creation_time: str,
                  handle_creation_time: str) -> Membership:
        """Exactly one verified child, now."""
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise self._fail("not_in_job", "the expected PID is invalid")
        if not isinstance(creation_time, str) or creation_time != handle_creation_time:
            raise self._fail("not_in_job", "the process handle's creation time differs from "
                                           "the created child's")
        if not C._valid_handle(process):
            raise self._fail("not_in_job", "no valid process handle")
        if not self.api.is_process_in_job(process, self.job):
            raise self._fail("not_in_job", "IsProcessInJob reports the child outside the job")
        pids = self.api.query_pid_list(self.job)
        if pids != [pid]:
            raise self._fail("not_in_job", f"the job members are {pids}, not [{pid}]")
        accounting = self.api.query_accounting(self.job)
        if accounting["active"] != 1:
            raise self._fail("not_in_job", f"{accounting['active']} active processes, not 1")
        self._epoch += 1
        return Membership(pid, creation_time, self._epoch)

    def verify_membership(self, process: int, pid: int, creation_time: str,
                          handle_creation_time: str) -> Membership:
        """A membership snapshot of exactly one child.  Not a permanent
        guarantee: termination re-verifies."""
        self._gate()
        if self.ownership not in ("owned_empty", "owned_with_child"):
            raise self._fail("not_in_job", f"membership in state {self.ownership}")
        try:
            self.membership = self._snapshot(process, pid, creation_time, handle_creation_time)
        except JobFailure:
            # A child may be in the job: never close it here (that would
            # terminate it); the caller discards through its own handle.
            self.ownership, self.membership = "retained_unverified", None
            raise
        except Exception as exc:
            self.ownership, self.membership = "retained_unverified", None
            raise self._fail("not_in_job", f"{type(exc).__name__}: {exc}") from None
        self.ownership = "owned_with_child"
        return self.membership

    def terminate(self, cause: str, process: int, membership: Membership,
                  handle_creation_time: str, *, wait: Callable[[int, int], bool],
                  exit_code: Callable[[int], int],
                  wait_milliseconds: int = 5000) -> TerminationOutcome:
        """
        The future termination interface.  Refused - before any native call
        - unless the lifecycle TERMINATION_POLICY is "accepted" (it is
        design-only), the job owns exactly the verified child, and the
        membership evidence is the latest snapshot; membership is then
        re-verified immediately before TerminateJobObject.  Exit proof:
        signalled wait, the cause's exit code, and an empty job.
        """
        self._gate()
        if P.TERMINATION_POLICY != TERMINATION_ACCEPTED:
            raise self._fail("termination_refused", "the termination policy is "
                             f"{P.TERMINATION_POLICY!r}, not accepted", TerminationRefused)
        if cause not in P.TERMINATION_EXIT_CODES:
            raise self._fail("termination_refused", f"unknown cause {cause!r}",
                             TerminationRefused)
        if self.ownership != "owned_with_child" or self.membership is None:
            raise self._fail("termination_refused", f"no verified child (state "
                             f"{self.ownership})", TerminationRefused)
        if membership != self.membership:
            raise self._fail("termination_refused", "stale or foreign membership evidence",
                             TerminationRefused)
        try:
            fresh = self._snapshot(process, membership.pid, membership.creation_time,
                                   handle_creation_time)
        except Exception as exc:
            self.ownership, self.membership = "retained_unverified", None
            raise self._fail("termination_refused", f"membership no longer verified: {exc}",
                             TerminationRefused) from None
        self.membership = fresh
        outcome = TerminationOutcome(cause)
        code = P.TERMINATION_EXIT_CODES[cause]
        try:
            try:
                self.api.terminate_job(self.job, code)
                outcome.api_ok = True
            except Exception as exc:
                outcome.errors.append(f"TerminateJobObject: {exc}")
            if outcome.api_ok:
                outcome.signaled = bool(wait(process, wait_milliseconds))
                if outcome.signaled:
                    outcome.exit_code = int(exit_code(process))
                    outcome.active_after = self.api.query_accounting(self.job)["active"]
        except BaseException as exc:          # interrupted: state first, then re-raise
            self.ownership = "retained_unverified"
            self.errors.append(f"termination interrupted: {type(exc).__name__}")
            raise
        outcome.verified = (outcome.api_ok and outcome.signaled
                            and outcome.exit_code == code and outcome.active_after == 0)
        if outcome.verified:
            self.ownership, self.membership = "child_exited", None
        else:
            self.ownership = "retained_unverified"
            outcome.errors.append("exit not verified (signalled wait, exit code "
                                  f"{code:#x} and an empty job are all required)")
            self.errors.extend(outcome.errors)
        return outcome

    def record_natural_exit(self, process: int, *, wait: Callable[[int, int], bool],
                            exit_code: Callable[[int], int]) -> Optional[int]:
        """The child exited by itself: only a signalled wait plus an empty
        job moves ownership to child_exited."""
        self._gate()
        if self.ownership != "owned_with_child":
            raise self._fail("not_in_job", f"natural exit in state {self.ownership}")
        if not wait(process, 0):
            return None
        code = int(exit_code(process))
        if self.api.query_accounting(self.job)["active"] != 0:
            self.ownership = "retained_unverified"
            raise self._fail("not_in_job", "the child exited but the job is not empty")
        self.ownership, self.membership = "child_exited", None
        return code

    def close(self) -> None:
        """Close the job only when it provably holds no live child; never
        retried after a failure."""
        self._gate()
        if self.ownership not in ("owned_empty", "child_exited"):
            raise self._fail("unsafe_close_refused", f"the job may hold a live child or "
                             f"cannot be closed (state {self.ownership})")
        active = self.api.query_accounting(self.job)["active"]
        if active != 0:
            self.ownership = "retained_unverified"
            raise self._fail("unsafe_close_refused", f"{active} active processes in the job")
        try:
            self.api.close(self.job)
        except Exception as exc:
            self.ownership = "retained_close_failed"
            raise self._fail("close_failed", str(exc)) from None
        self.ownership, self.job = "closed", None
