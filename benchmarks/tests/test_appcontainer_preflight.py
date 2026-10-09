"""
benchmarks/tests/test_appcontainer_preflight.py

Offline tests of the read-only AppContainer staging planner.  Only fake
host data is used: the real collectors are disabled, and any process,
socket or registry access fails the test.
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
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

HEAD = "a" * 40
SOURCE = (_BENCH / "appcontainer_preflight.py").read_text(encoding="utf-8")


class LiveAccessAttempt(AssertionError):
    """A test reached the real host."""


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
    monkeypatch.setattr(P.RealHostSource, "_registry_value",
                        staticmethod(refuse("a registry read")))


REPO_FILES = [
    "benchmarks/calibrate_generator.py", "benchmarks/metrics.py",
    "benchmarks/run_benchmark.py", "benchmarks/ladder.json", "benchmarks/report.py",
    "benchmarks/tests/test_metrics.py",
    "workload-generator/workload_generator/__init__.py",
    "workload-generator/workload_generator/plan.py",
    "workload-generator/workload_generator/scenarios.py",
    "workload-generator/workload_generator/real_driver.py",
    "workload-generator/tests/test_plan.py",
    "demo-app/graph.py", "demo-app/state.py", "demo-app/main.py",
    "demo-app/agents/retrieval.py", "demo-app/observability/tracing.py",
    "demo-app/observability/__pycache__/tracing.cpython-313.py",
]
REQUIRED = [
    "benchmarks/calibrate_generator.py", "benchmarks/ladder.json", "benchmarks/metrics.py",
    "benchmarks/run_benchmark.py", "demo-app/agents/retrieval.py", "demo-app/graph.py",
    "demo-app/observability/tracing.py", "demo-app/state.py",
    "workload-generator/workload_generator/__init__.py",
    "workload-generator/workload_generator/plan.py",
    "workload-generator/workload_generator/real_driver.py",
    "workload-generator/workload_generator/scenarios.py",
]


def good_inventory():
    return {
        "windows": {"collected": True, "edition": "Core", "build": "26200.1",
                    "architecture": "AMD64", "pointer_bits": 64},
        "python": {"collected": True, "executable": r"C:\Py\python.exe", "home": r"C:\Py",
                   "version": [3, 13, 7], "files": P._python_runtime_files((3, 13)),
                   "sha256": {"python.exe": "1" * 64}},
        "venv": {"collected": True, "site_packages": r"C:\v\Lib\site-packages",
                 "top_level": ["pydantic", "pydantic_core", "langgraph", "opentelemetry",
                               "grpc"],
                 "native": ["pydantic_core/_pydantic_core.cp313-win_amd64.pyd",
                            "grpc/_cython/cygrpc.cp313-win_amd64.pyd"],
                 "pth": [{"name": "distutils-precedence.pth",
                          "lines": ["import os; var = 'SETUPTOOLS_USE_DISTUTILS'"]}]},
        "onedrive": {"collected": True, "roots": [r"C:\Users\u\OneDrive"], "placeholders": [],
                     "repo_under_onedrive": True},
        "repository": {"collected": True, "root": r"C:\Users\u\OneDrive\repo", "head": HEAD,
                       "git_kind": "directory", "files": list(REPO_FILES)},
        "staging_parent": {"collected": True, "path": r"C:\Users\u\AppData\Local",
                           "exists": True, "writable": True, "root_exists": False},
        "services": {"collected": True, "MpsSvc": {"status": "Running", "start": "Automatic"},
                     "BFE": {"status": "Running", "start": "Automatic"}},
        "firewall_profiles": {"collected": True,
                              "enabled": {"domain": True, "private": True, "public": True}},
        "appcontainer_profiles": {"collected": True,
                                  "profiles": [{"name": "other.app", "sid": "S-1-15-2-5"}]},
        "loopback_exemptions": {"collected": True, "sids": [], "names": []},
        "privileges": {"collected": True, "is_admin": False,
                       "privileges": ["SeChangeNotifyPrivilege"]},
        "agentops_baseline": {"collected": True, "containers": ["agentops-postgres"]},
    }


def findings(inventory, expected_head=HEAD):
    return P.inventory_findings(inventory, expected_head=expected_head)


def blockers(inventory, expected_head=HEAD):
    return [f"{f.area}: {f.message}" for f in findings(inventory, expected_head)
            if f.severity == "blocker"]


def has(messages, text):
    return any(text in m for m in messages)


class FakeSource:
    def __init__(self, data):
        self.data, self.calls = data, []

    def collect(self, name):
        self.calls.append(name)
        value = self.data.get(name)
        if isinstance(value, Exception):
            raise value
        return value


# ---------------------------------------------------------------------------
# Gates and scope
# ---------------------------------------------------------------------------

class TestGates:
    def test_disabled_by_default(self):
        assert P.HOST_INVENTORY_ENABLED is False
        assert "\nHOST_INVENTORY_ENABLED: bool = False\n" in SOURCE

    @pytest.mark.parametrize("name", P.COLLECTORS)
    def test_every_real_collector_refuses(self, name):
        with pytest.raises(P.InventoryDisabled, match="not enabled"):
            P.RealHostSource().collect(name)

    def test_collect_inventory_refuses_as_a_whole(self):
        with pytest.raises(P.InventoryDisabled):
            P.collect_inventory(P.RealHostSource())
        with pytest.raises(P.InventoryDisabled):
            P._sha256_file(Path(__file__))

    def test_unknown_collector(self, monkeypatch):
        monkeypatch.setattr(P, "HOST_INVENTORY_ENABLED", True)        # in memory only
        with pytest.raises(P.PreflightError, match="unknown collector"):
            P.RealHostSource().collect("bogus")

    def test_cli_collect_refused(self, tmp_path, capsys):
        out = io.StringIO()
        assert P.main(["--collect"], stdout=out) == 2
        assert "not enabled" in capsys.readouterr().err and out.getvalue() == ""

    def test_cli_plans_from_supplied_data_only(self, tmp_path):
        source = tmp_path / "inventory.json"
        source.write_text(json.dumps(good_inventory()), encoding="utf-8")
        before = sorted(tmp_path.iterdir())
        out = io.StringIO()
        assert P.main(["--inventory-json", str(source), "--expected-head", HEAD],
                      stdout=out) == 0
        plan = json.loads(out.getvalue())
        assert plan["recommendation"] == "NO-GO" and plan["host_inventory_enabled"] is False
        assert sorted(tmp_path.iterdir()) == before                 # nothing written

    def test_cli_unreadable_input(self, tmp_path, capsys):
        assert P.main(["--inventory-json", str(tmp_path / "missing.json")]) == 2
        assert "cannot read" in capsys.readouterr().err

    def test_no_host_changing_operation_exists(self):
        code = SOURCE.split('"""', 2)[2]
        for word in ("CreateAppContainerProfile(", "DeleteAppContainerProfile(", "icacls ",
                     "subprocess", "Popen", "import socket", "mkdir", "makedirs", "shutil",
                     "copyfile", "SetValue", "CreateKey", "DeleteKey", "WinDLL", "windll",
                     "unlink", "rmdir", "write_text", "write_bytes", "NetworkIsolationSet",
                     "netsh", "docker", "wsl"):
            assert word not in code, word
        assert re.findall(r"\bopen\([^)]*\)", code) == ['open(path, "rb")']
        assert "KEY_READ" in code

    def test_profile_name_matches_the_harness(self):
        assert P.PROFILE_NAME == C.APPCONTAINER_PROFILE


class TestCollectInventory:
    def test_fake_source(self):
        source = FakeSource(good_inventory())
        inventory = P.collect_inventory(source)
        assert source.calls == list(P.COLLECTORS) and inventory == good_inventory()

    def test_failing_collector_is_recorded_not_raised(self):
        data = good_inventory()
        data["services"] = RuntimeError("OpenSCManager failed")
        data["venv"] = None
        inventory = P.collect_inventory(FakeSource(data))
        assert inventory["services"] == {"collected": False,
                                         "error": "RuntimeError: OpenSCManager failed"}
        assert inventory["venv"]["collected"] is False
        assert has(blockers(inventory), "services: not collected (RuntimeError")


# ---------------------------------------------------------------------------
# Inventory findings
# ---------------------------------------------------------------------------

class TestInventoryFindings:
    def test_clean_inventory_has_no_blocker(self):
        result = findings(good_inventory())
        assert [f for f in result if f.severity == "blocker"] == []
        assert any(f.area == "venv" and "executes code" in f.message for f in result)
        assert any(f.severity == "risk" and f.area == "onedrive" for f in result)

    def test_uncollected_evidence_blocks(self):
        result = blockers({})
        for name in P.COLLECTORS:
            assert has(result, f"{name}: not collected")
        inventory = good_inventory()
        inventory["services"] = dict(P.NOT_IMPLEMENTED)
        assert has(blockers(inventory), "separately reviewed read-only implementation")

    @pytest.mark.parametrize("missing", ["python313.dll", "DLLs/_socket.pyd",
                                         "vcruntime140.dll", "Lib/os.py"])
    def test_missing_python_dll(self, missing):
        inventory = good_inventory()
        inventory["python"]["files"].remove(missing)
        assert has(blockers(inventory), f"missing Python runtime file {missing}")

    def test_missing_native_extension(self):
        inventory = good_inventory()
        inventory["venv"]["native"] = ["grpc/_cython/cygrpc.cp313-win_amd64.pyd"]
        assert has(blockers(inventory), "native extension pydantic_core/_pydantic_core")

    def test_missing_package_and_incomplete_dependency_inventory(self):
        inventory = good_inventory()
        inventory["venv"]["top_level"].remove("langgraph")
        assert has(blockers(inventory), "required package langgraph missing")
        inventory["venv"]["top_level"] = []
        assert has(blockers(inventory), "incomplete dependency inventory")

    def test_onedrive_placeholder(self):
        inventory = good_inventory()
        inventory["onedrive"]["placeholders"] = ["demo-app/graph.py"]
        assert has(blockers(inventory), "OneDrive placeholder (not local): demo-app/graph.py")

    @pytest.mark.parametrize("architecture, bits", [("ARM64", 64), ("x86", 32), ("AMD64", 32),
                                                    (None, 64)])
    def test_unsupported_architecture(self, architecture, bits):
        inventory = good_inventory()
        inventory["windows"].update(architecture=architecture, pointer_bits=bits)
        assert has(blockers(inventory), "unsupported architecture")

    @pytest.mark.parametrize("service, state", [
        ("MpsSvc", {"status": "Stopped", "start": "Automatic"}),
        ("BFE", {"status": "Running", "start": "Manual"}),
        ("BFE", None),
    ])
    def test_stopped_service(self, service, state):
        inventory = good_inventory()
        inventory["services"][service] = state
        assert has(blockers(inventory), f"{service} is not Running/Automatic")

    def test_existing_foreign_profile(self):
        inventory = good_inventory()
        inventory["appcontainer_profiles"]["profiles"].append(
            {"name": P.PROFILE_NAME, "sid": "S-1-15-2-77"})
        assert has(blockers(inventory), "already exists (SID S-1-15-2-77)")

    def test_loopback_exemption(self):
        inventory = good_inventory()
        inventory["loopback_exemptions"] = {"collected": True, "sids": ["S-1-15-2-77"],
                                            "names": [P.PROFILE_NAME]}
        assert has(blockers(inventory), f"{P.PROFILE_NAME} has a loopback exemption")
        inventory["loopback_exemptions"]["names"] = ["other.app"]
        assert not has(blockers(inventory), "loopback exemption")
        assert any(f.area == "loopback_exemptions" and f.severity == "info"
                   for f in findings(inventory))

    @pytest.mark.parametrize("change, message", [
        ({"exists": False}, "missing or inaccessible"),
        ({"path": None}, "missing or inaccessible"),
        ({"writable": False}, "not writable"),
        ({"root_exists": True}, "never reused"),
        ({"path": r"C:\Users\u\OneDrive\Local"}, "under OneDrive"),
    ])
    def test_inaccessible_staging_path(self, change, message):
        inventory = good_inventory()
        inventory["staging_parent"].update(change)
        assert has(blockers(inventory), message)

    def test_incorrect_git_head(self):
        assert has(blockers(good_inventory(), expected_head="b" * 40), "incorrect Git HEAD")
        inventory = good_inventory()
        inventory["repository"]["head"] = "not-a-sha"
        assert has(blockers(inventory), "HEAD is unknown")

    def test_missing_source_file(self):
        inventory = good_inventory()
        inventory["repository"]["files"].remove("demo-app/graph.py")
        assert has(blockers(inventory), "required source file missing: demo-app/graph.py")

    def test_risks(self):
        inventory = good_inventory()
        inventory["privileges"] = {"collected": True, "is_admin": True, "privileges": []}
        inventory["firewall_profiles"]["enabled"]["public"] = False
        risks = [f.message for f in findings(inventory) if f.severity == "risk"]
        assert has(risks, "running elevated") and has(risks, "SeChangeNotifyPrivilege")
        assert has(risks, "firewall profiles disabled: public")


# ---------------------------------------------------------------------------
# Staging plan, manifest and recommendation
# ---------------------------------------------------------------------------

class TestStaging:
    def test_layout_is_pure(self, tmp_path):
        layout = P.propose_layout(str(tmp_path / "parent"))
        assert layout.root.endswith(r"parent\agentops-probe")
        assert layout.python.endswith(r"agentops-probe\python")
        assert not (tmp_path / "parent").exists()
        assert P.propose_layout(None).root == r"%LOCALAPPDATA%\agentops-probe"

    def test_required_repository_files(self):
        required, missing = P.required_repository_files(REPO_FILES)
        assert required == REQUIRED and missing == []
        _, missing = P.required_repository_files(["benchmarks/metrics.py"])
        assert "benchmarks/calibrate_generator.py" in missing

    def test_acl_intentions_are_minimal(self):
        grants = P.acl_intentions(P.propose_layout(r"C:\L"))
        assert {g["principal"] for g in grants} == {f"<SID of {P.PROFILE_NAME}>"}
        assert [g["rights"] for g in grants] == ["RX", "RX", "RX", "M"]
        assert grants[0]["inheritance"].startswith("none")
        assert grants[3]["path"] == r"C:\L\agentops-probe\out"
        text = json.dumps(grants)
        for broad in ("ALL APPLICATION PACKAGES", "S-1-15-2-1\"", "Everyone", "Users"):
            assert broad not in text


def _manifest(files=None, head=HEAD):
    files = files if files is not None else [(r, f"# {r}\n".encode()) for r in REQUIRED]
    return P.build_manifest(files, source_head=head, staged_repo=r"C:\L\agentops-probe\repo")


def _ls_tree(manifest):
    return {e["source_relative"]: e["git_blob_sha1"] for e in manifest["entries"]}


class TestManifest:
    def test_entries(self):
        manifest = P.build_manifest([("benchmarks\\metrics.py", b"hello\n")], source_head=HEAD,
                                    staged_repo=r"C:\S\repo")
        entry = manifest["entries"][0]
        assert entry == {"source_relative": "benchmarks/metrics.py",
                         "staged_path": r"C:\S\repo\benchmarks\metrics.py", "size": 6,
                         "sha256": hashlib.sha256(b"hello\n").hexdigest(),
                         "git_blob_sha1": "ce013625030ba8dba906f756967f9e9ca394464a"}
        assert manifest["source_head"] == HEAD and manifest["schema"] == P.SCHEMA_MANIFEST

    def test_complete_and_verified(self):
        manifest = _manifest()
        assert P.manifest_findings(manifest, required=REQUIRED, expected_head=HEAD,
                                   ls_tree=_ls_tree(manifest)) == []

    def test_incomplete_manifest(self):
        manifest = _manifest([(r, b"x") for r in REQUIRED[1:]])
        result = P.manifest_findings(manifest, required=REQUIRED, expected_head=HEAD,
                                     ls_tree=_ls_tree(manifest))
        assert has([f.message for f in result], f"incomplete manifest: {REQUIRED[0]} missing")

    def test_incorrect_head(self):
        manifest = _manifest(head="c" * 40)
        result = P.manifest_findings(manifest, required=REQUIRED, expected_head=HEAD,
                                     ls_tree=_ls_tree(manifest))
        assert has([f.message for f in result], "incorrect Git HEAD")

    def test_committed_content_checks(self):
        manifest = _manifest()
        tree = _ls_tree(manifest)
        tree[REQUIRED[0]] = "0" * 40
        del tree[REQUIRED[1]]
        messages = [f.message for f in P.manifest_findings(
            manifest, required=REQUIRED, expected_head=HEAD, ls_tree=tree)]
        assert has(messages, f"{REQUIRED[0]} differs from the committed file")
        assert has(messages, f"{REQUIRED[1]} is not tracked at HEAD")
        messages = [f.message for f in P.manifest_findings(
            manifest, required=REQUIRED, expected_head=HEAD, ls_tree=None)]
        assert has(messages, "committed content not verified")

    def test_malformed_entries(self):
        manifest = _manifest()
        broken = copy.deepcopy(manifest)
        broken["entries"][0]["sha256"] = "short"
        broken["entries"][1]["staged_path"] = r"C:\elsewhere\x.py"
        broken["entries"].append(dict(broken["entries"][2]))
        messages = [f.message for f in P.manifest_findings(
            broken, required=REQUIRED, expected_head=HEAD, ls_tree=_ls_tree(manifest))]
        assert has(messages, "incomplete entry") and has(messages, "mirrored path")
        assert has(messages, "duplicate entry")
        assert [f.message for f in P.manifest_findings(
            {"schema": "x"}, required=REQUIRED, expected_head=HEAD, ls_tree=None)] == [
            "no manifest in the expected format"]


class TestPlan:
    def test_default_plan_is_no_go(self):
        plan = P.build_plan({}, expected_head=None)
        assert plan["recommendation"] == "NO-GO" and plan["schema"] == P.SCHEMA_PLAN
        assert has(plan["reasons"], "windows: not collected")
        assert has(plan["reasons"], "no manifest in the expected format")

    def test_even_a_perfect_plan_is_no_go_until_the_harness_is_resolved(self):
        manifest = _manifest()
        plan = P.build_plan(good_inventory(), expected_head=HEAD, manifest=manifest,
                            ls_tree=_ls_tree(manifest),
                            verified={p: True for p in P.PREREQUISITES})
        assert plan["recommendation"] == "NO-GO"
        assert [r for r in plan["reasons"]] == [
            "harness: " + P.GIT_HEAD_STRATEGY["harness_incompatibility"]]

    def test_plan_contents(self):
        plan = P.build_plan(good_inventory(), expected_head=HEAD)
        staging = plan["staging"]
        assert staging["layout"]["root"] == r"C:\Users\u\AppData\Local\agentops-probe"
        assert staging["repository_files"] == REQUIRED
        assert "python313.dll" in staging["python_runtime_files"]
        assert staging["site_packages"]["destination"].endswith(
            r"agentops-probe\python\Lib\site-packages")
        assert "pydantic_core/_pydantic_core.cp313-win_amd64.pyd" in staging["native_extensions"]
        assert plan["manifest_format"]["fields"] == ["source_relative", "staged_path", "size",
                                                     "sha256", "git_blob_sha1"]
        assert "no_fabrication" in plan["git_head_strategy"]
        for key in ("permissions", "verification_checks", "host_side_effects",
                    "rollback_prerequisites", "unresolved_assumptions"):
            assert plan[key]

    def test_recommend(self):
        verified = {p: True for p in P.PREREQUISITES}
        assert P.recommend([], verified) == ("GO", [])
        assert P.recommend([P._risk("x", "y")], verified) == ("GO", [])
        decision, reasons = P.recommend([], {**verified, "host_prep_tool_reviewed": False})
        assert decision == "NO-GO" and has(reasons, "host_prep_tool_reviewed")
        assert P.recommend([P._blocker("a", "b")], verified) == ("NO-GO", ["a: b"])
