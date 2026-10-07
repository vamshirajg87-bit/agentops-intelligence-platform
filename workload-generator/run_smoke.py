"""
workload-generator/run_smoke.py

Live smoke test of the workload generator on a separate, disposable stack.

It proves one path, for two small workloads (NORMAL and API_500):

    REAL driver -> OTLP -> collector -> Kafka -> stream processor
        -> canonical Kafka topic -> storage consumer -> PostgreSQL

It only ORCHESTRATES.  Planning, fault injection, stream processing, storage
and the database bootstrap are the existing components, run unchanged:

    Compose project   agentops-workload-smoke (docker-compose.yml plus
                      compose.smoke.override.yml), services kafka,
                      kafka-init, postgres, db-bootstrap, otel-collector
    host processes    stream-processor/main.py, storage-consumer/main.py and
                      python -m workload_generator, from a fresh virtual
                      environment built from the pinned requirements

Everything temporary (generated passwords, the virtual environment, logs,
reports, snapshots) lives in one directory outside the repository and is
deleted at the end.  The normal stack is never addressed: every Compose
command names the project, both files and the temporary environment file,
and the Docker state outside the project is compared before and after.

The first failure stops the run; nothing is retried or adjusted.  Cleanup
always runs.

    python workload-generator/run_smoke.py [--expect-head <commit>]

Exit codes:
    0  both stages reconciled exactly and cleanup was verified
    1  anything else

Standard library only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

REPO: Path = Path(__file__).resolve().parent.parent

PROJECT: str = "agentops-workload-smoke"
COMPOSE_FILE: str = "docker-compose.yml"
OVERRIDE_FILE: str = "workload-generator/compose.smoke.override.yml"
ALLOWED_UNTRACKED: frozenset[str] = frozenset({
    "?? workload-generator/compose.smoke.override.yml",
    "?? workload-generator/run_smoke.py",
})

OTLP_PORT: int = 14317
KAFKA_PORT: int = 19092
POSTGRES_PORT: int = 15432
#: Ports of the normal stack.  Seeing one of them in a log of this run means
#: something was pointed at the wrong stack.
NORMAL_STACK_PORTS: tuple[int, ...] = (9092, 5432, 4317)
NORMAL_POSTGRES_CONTAINER: str = "agentops-postgres"

BROKER_INTERNAL: str = "agentops-kafka:29092"
KAFKA_BIN: str = "/opt/kafka/bin"
TOPIC_RAW: str = "agentops.telemetry.otlp.traces"
TOPIC_SPANS: str = "agentops.telemetry.spans"
TOPIC_DLQ: str = "agentops.telemetry.dlq"
TOPICS: tuple[str, ...] = (TOPIC_RAW, TOPIC_SPANS, TOPIC_DLQ)
PARTITIONS: int = 3
GROUP_STREAM: str = "d3-stream-processor"
GROUP_STORAGE: str = "d3-storage-consumer"

SERVICES: tuple[str, ...] = ("kafka", "kafka-init", "postgres", "db-bootstrap", "otel-collector")
LEDGER_STEPS: int = 17
SERVICE_NAME: str = "agentops-demo-app"
ROOT_SPAN: str = "agentops.request"
TOOL_SPAN: str = "tool.execute"

PASSWORD_VARIABLES: tuple[str, ...] = (
    "POSTGRES_PASSWORD",
    "GF_DATASOURCE_AGENTOPS_PASSWORD",
    "TRACE_VIEWER_DB_PASSWORD",
    "ANOMALY_DETECTOR_DB_PASSWORD",
    "RCA_ANALYZER_DB_PASSWORD",
    "INCIDENT_RETRIEVAL_DB_PASSWORD",
    "ALERT_EVALUATOR_DB_PASSWORD",
    "ALERT_NOTIFIER_DB_PASSWORD",
)

# Seconds.
T_BUILD: float = 600.0
T_VENV: float = 900.0
T_BROKER: float = 90.0
T_KAFKA_INIT: float = 60.0
T_POSTGRES: float = 90.0
T_BOOTSTRAP: float = 300.0
T_COLLECTOR: float = 30.0
T_HOST_SERVICE: float = 45.0
T_DRAIN: float = 60.0
T_DOWN: float = 120.0
T_COMMAND: float = 60.0
SETTLE_SECONDS: float = 6.0

_SENSITIVE_NAME = re.compile(
    r"(?i)^(OTEL_|PG|POSTGRES_|KAFKA_|CONSUMER_|TOPIC_|GF_|COMPOSE_|AGENTOPS_DIRECT_EXPORT$)"
    r"|_PASSWORD$"
)
_WRONG_STACK = re.compile(
    r"[A-Za-z0-9.\-\]]:(%s)(?![0-9])" % "|".join(str(port) for port in NORMAL_STACK_PORTS)
)


class SmokeFailure(Exception):
    """The run cannot continue.  Its message is the evidence."""


@dataclass(frozen=True)
class Stage:
    run_id: str
    scenario: str
    traces: int
    max_duration: int
    digest_prefix: str
    successes: int
    failures: int
    failing_indexes: tuple[int, ...]
    spans: int
    trace_sizes: Mapping[str, int]
    status: Mapping[str, int]
    error_spans: Mapping[str, int]
    error_types: Mapping[str, int]
    #: Roots per scenario.  Each root carries the scenario planned for ITS
    #: request: outside an episode that is "normal", whatever the run's is.
    scenario_roots: Mapping[str, int]


STAGES: tuple[Stage, ...] = (
    Stage(
        run_id="d3-normal-002", scenario="normal", traces=10, max_duration=60,
        digest_prefix="822cf8a0c3b1833a",
        successes=10, failures=0, failing_indexes=(),
        spans=70, trace_sizes={"7": 10}, status={"UNSET": 70},
        error_spans={}, error_types={}, scenario_roots={"normal": 10},
    ),
    Stage(
        run_id="d3-api500-002", scenario="api-500", traces=20, max_duration=90,
        digest_prefix="ae77fbb8ebca1e03",
        successes=17, failures=3, failing_indexes=(10, 13, 14),
        spans=134, trace_sizes={"7": 17, "5": 3}, status={"UNSET": 128, "ERROR": 6},
        error_spans={ROOT_SPAN: 3, TOOL_SPAN: 3},
        error_types={"UpstreamServerError": 3},
        scenario_roots={"normal": 17, "api-500": 3},
    ),
)
RATE: int = 2
SEED: int = 42
CONCURRENCY: int = 2


@dataclass
class Run:
    """What the run owns and has to give back."""
    temp: Optional[Path] = None
    env_file: Optional[Path] = None
    python: Optional[Path] = None
    generated: dict[str, str] = field(default_factory=dict)
    processes: dict[str, subprocess.Popen] = field(default_factory=dict)
    logs: dict[str, Path] = field(default_factory=dict)
    before: Optional[dict[str, list[str]]] = None
    normal_postgres: Optional[str] = None
    compose_used: bool = False
    caches_before: frozenset[str] = frozenset()
    #: Generated passwords that had to be masked before a log was written.
    masked_in_logs: int = 0
    started: float = field(default_factory=time.monotonic)


RUN = Run()


# ---------------------------------------------------------------------------
# Output and subprocesses
# ---------------------------------------------------------------------------

def redact(text: str) -> str:
    for value in RUN.generated.values():
        text = text.replace(value, "<redacted>")
    return text


def say(text: str = "") -> None:
    print(redact(text), flush=True)


def section(title: str) -> None:
    say(f"\n=== {title}  [+{time.monotonic() - RUN.started:.0f}s]")


def write_log(name: str, text: str) -> None:
    """Keep the output of a tool in the temporary directory, masked."""
    assert RUN.temp is not None
    RUN.masked_in_logs += sum(text.count(value) for value in RUN.generated.values())
    (RUN.temp / name).write_text(redact(text), encoding="utf-8")


def tool_environment() -> dict[str, str]:
    """
    The environment for docker, git and pip: this process's own, without any
    variable that could reconfigure the stack.  Compose prefers the shell
    environment over --env-file, so nothing of that kind may reach it.
    """
    return {
        name: value for name, value in os.environ.items()
        if _SENSITIVE_NAME.search(name) is None
    }


def host_environment(**extra: str) -> dict[str, str]:
    """The explicit environment of a host process: a few system variables
    plus what that process is deliberately given."""
    env = {
        name: os.environ[name]
        for name in ("SYSTEMROOT", "PATH", "TEMP", "TMP") if name in os.environ
    }
    env["PYTHONUNBUFFERED"] = "1"
    # The components are imported from the repository; nothing is written there.
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = str(REPO / "demo-app")
    env.update(extra)
    return env


def run(
    command: Sequence[str], *, timeout: float, cwd: Path = REPO,
    env: Optional[Mapping[str, str]] = None, what: str = "",
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            list(command), cwd=cwd, env=dict(tool_environment() if env is None else env),
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=timeout, stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        raise SmokeFailure(f"{what or command[0]}: no result within {timeout:.0f} seconds")
    except FileNotFoundError:
        raise SmokeFailure(f"{what or command[0]}: {command[0]} is not installed")


def checked(command: Sequence[str], *, timeout: float, what: str, **kwargs: Any) -> str:
    result = run(command, timeout=timeout, what=what, **kwargs)
    if result.returncode != 0:
        tail = "\n".join((result.stdout + result.stderr).strip().splitlines()[-12:])
        raise SmokeFailure(f"{what}: exit {result.returncode}\n{tail}")
    return result.stdout


def compose_command(*args: str) -> list[str]:
    """Every Compose command names the project, both files and the temporary
    environment file."""
    if RUN.env_file is None:
        raise SmokeFailure("no Compose command before the environment file exists")
    return [
        "docker", "compose", "-p", PROJECT, "-f", COMPOSE_FILE, "-f", OVERRIDE_FILE,
        "--env-file", str(RUN.env_file), *args,
    ]


def compose(*args: str, timeout: float = T_COMMAND, what: str = "") -> str:
    RUN.compose_used = True
    return checked(compose_command(*args), timeout=timeout, what=what or "compose " + args[0])


def wait_for(what: str, timeout: float, probe: Callable[[], Any], interval: float = 2.0) -> Any:
    """probe() returns a value when ready, or None.  Its last detail, when it
    sets one on itself, is part of the failure."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value is not None:
            return value
        if time.monotonic() >= deadline:
            detail = getattr(probe, "detail", "")
            raise SmokeFailure(f"{what}: not reached within {timeout:.0f} seconds\n{detail}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------

def git(*args: str) -> str:
    return checked(["git", *args], timeout=T_COMMAND, what="git " + args[0])


def repository_state() -> list[str]:
    return [line for line in git("status", "--porcelain", "--untracked-files=all").splitlines()
            if line.strip()]


def bytecode_caches() -> frozenset[str]:
    """__pycache__ directories of the repository, outside local environments."""
    found = set()
    for directory, names, _ in os.walk(REPO):
        names[:] = [name for name in names if name not in (".git", "venv", ".venv", "node_modules")]
        if "__pycache__" in names:
            found.add(str(Path(directory, "__pycache__").relative_to(REPO)))
            names.remove("__pycache__")
    return frozenset(found)


def check_repository(expect_head: Optional[str]) -> None:
    section("repository")
    head = git("rev-parse", "HEAD").strip()
    say(f"branch {git('rev-parse', '--abbrev-ref', 'HEAD').strip()}")
    say(f"HEAD   {head}")
    if expect_head is not None and head != expect_head:
        raise SmokeFailure(f"HEAD is not the expected commit {expect_head}")
    unexpected = [line for line in repository_state() if line not in ALLOWED_UNTRACKED]
    if unexpected:
        raise SmokeFailure("the working tree holds other changes:\n" + "\n".join(unexpected))
    if git("diff", "--cached", "--name-only").strip():
        raise SmokeFailure("the index is not empty")
    for path in (COMPOSE_FILE, OVERRIDE_FILE):
        if not (REPO / path).is_file():
            raise SmokeFailure(f"missing {path}")
    RUN.caches_before = bytecode_caches()
    say("working tree: committed code plus the two smoke files; index empty")


def check_ports() -> None:
    section("ports")
    for port in (OTLP_PORT, KAFKA_PORT, POSTGRES_PORT):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(1.0)
            in_use = probe.connect_ex(("127.0.0.1", port)) == 0
        if not in_use:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as holder:
                    holder.bind(("127.0.0.1", port))
            except OSError:
                in_use = True
        say(f"127.0.0.1:{port}  {'IN USE' if in_use else 'free'}")
        if in_use:
            raise SmokeFailure(f"port {port} is in use")


def docker_lines(*args: str) -> list[str]:
    return sorted(
        line for line in checked(["docker", *args], timeout=T_COMMAND,
                                 what="docker " + args[0]).splitlines() if line.strip()
    )


def docker_fingerprint() -> dict[str, list[str]]:
    return {
        "containers": docker_lines("ps", "-a", "--no-trunc", "--format",
                                   "{{.ID}} {{.Names}} {{.State}}"),
        "volumes": docker_lines("volume", "ls", "--format", "{{.Name}}"),
        "networks": docker_lines("network", "ls", "--no-trunc", "--format", "{{.ID}} {{.Name}}"),
        "images": docker_lines("images", "--no-trunc", "--format",
                               "{{.Repository}}:{{.Tag}} {{.ID}}"),
    }


def short_hash(lines: Sequence[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


def normal_postgres() -> Optional[str]:
    """'<container id> <state> <health>' of the normal database container, or
    None when this machine has none."""
    result = run(
        ["docker", "inspect", "-f",
         "{{.Id}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}",
         NORMAL_POSTGRES_CONTAINER],
        timeout=T_COMMAND, what="docker inspect",
    )
    return result.stdout.strip() if result.returncode == 0 else None


def check_normal_stack(when: str) -> None:
    now = normal_postgres()
    if now != RUN.normal_postgres:
        raise SmokeFailure(
            f"{NORMAL_POSTGRES_CONTAINER} changed ({when}): was {RUN.normal_postgres}, is {now}"
        )
    say(f"{NORMAL_POSTGRES_CONTAINER} ({when}): unchanged"
        + (f", {now.split(' ', 1)[1]}" if now else ", not present on this machine"))


def record_docker_state() -> None:
    section("normal Docker state before")
    RUN.before = docker_fingerprint()
    for kind, lines in RUN.before.items():
        say(f"{kind}: {len(lines)} (fingerprint {short_hash(lines)})")
    for line in RUN.before["containers"]:
        say(f"  container {line[:12]} {line.split(' ', 1)[1]}")
    taken = [line for kind in RUN.before.values() for line in kind if PROJECT in line]
    if taken:
        raise SmokeFailure("objects of the smoke project already exist:\n" + "\n".join(taken))
    RUN.normal_postgres = normal_postgres()
    say(f"{NORMAL_POSTGRES_CONTAINER}: {RUN.normal_postgres or 'not present on this machine'}")
    if RUN.normal_postgres and not RUN.normal_postgres.endswith("running healthy"):
        raise SmokeFailure(f"{NORMAL_POSTGRES_CONTAINER} is not running and healthy")


# ---------------------------------------------------------------------------
# Temporary directory, credentials, virtual environment
# ---------------------------------------------------------------------------

def create_workspace() -> None:
    section("temporary workspace")
    temp = Path(tempfile.mkdtemp(prefix="agentops-d3-smoke-")).resolve()
    if temp == REPO or REPO in temp.parents:
        shutil.rmtree(temp, ignore_errors=True)
        raise SmokeFailure("the temporary directory would be inside the repository")
    RUN.temp = temp
    say("created outside the repository")

    RUN.generated = {name: secrets.token_urlsafe(24) for name in PASSWORD_VARIABLES}
    env_file = temp / "smoke.env"
    with open(env_file, "x", encoding="utf-8", newline="\n") as handle:
        handle.writelines(f"{name}={value}\n" for name, value in RUN.generated.items())
    RUN.env_file = env_file
    say(f"generated {len(RUN.generated)} test-only passwords "
        f"({len(set(RUN.generated.values()))} distinct); they are never printed")
    (temp / "docker-before.json").write_text(json.dumps(RUN.before, indent=1), encoding="utf-8")


def create_virtual_environment() -> None:
    section("fresh virtual environment")
    assert RUN.temp is not None
    venv = RUN.temp / "venv"
    checked([sys.executable, "-m", "venv", str(venv)], timeout=T_VENV, what="python -m venv")
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not python.is_file():
        raise SmokeFailure("the virtual environment has no interpreter")
    RUN.python = python
    install = run(
        [str(python), "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
         "-r", "workload-generator/requirements.txt",
         "-r", "stream-processor/requirements.txt",
         "-r", "storage-consumer/requirements.txt"],
        timeout=T_VENV, what="pip install",
    )
    write_log("pip-install.log", install.stdout + install.stderr)
    if install.returncode != 0:
        tail = "\n".join((install.stdout + install.stderr).strip().splitlines()[-8:])
        raise SmokeFailure(f"pip install: exit {install.returncode}\n{tail}")
    versions = json.loads(checked(
        [str(python), "-c",
         "import json, sys\n"
         "from importlib.metadata import version\n"
         "names = ('protobuf', 'opentelemetry-sdk', 'opentelemetry-exporter-otlp-proto-grpc',"
         " 'opentelemetry-proto', 'langgraph', 'pydantic', 'confluent-kafka', 'psycopg',"
         " 'grpcio')\n"
         "found = {name: version(name) for name in names}\n"
         "found['python'] = '.'.join(map(str, sys.version_info[:3]))\n"
         "print(json.dumps(found))"],
        timeout=T_COMMAND, what="version check", env=host_environment(),
    ))
    for name, value in sorted(versions.items()):
        say(f"{name:40} {value}")
    listed = checked([str(python), "-m", "pip", "list", "--format=freeze",
                      "--disable-pip-version-check"], timeout=T_COMMAND, what="pip list")
    say(f"packages installed: {len(listed.splitlines())}; "
        f"dbt packages: {sum(line.lower().startswith('dbt') for line in listed.splitlines())}")
    pip_check = run([str(python), "-m", "pip", "check", "--disable-pip-version-check"],
                    timeout=T_COMMAND, what="pip check")
    say(f"pip check: exit {pip_check.returncode} ({pip_check.stdout.strip() or 'no output'})")
    if not versions["python"].startswith("3.13."):
        raise SmokeFailure("the virtual environment is not Python 3.13")
    if int(versions["protobuf"].split(".")[0]) >= 6:
        raise SmokeFailure("protobuf 6 or later was installed")
    if pip_check.returncode != 0:
        raise SmokeFailure("pip check reported broken requirements")


# ---------------------------------------------------------------------------
# Isolated infrastructure
# ---------------------------------------------------------------------------

def check_render() -> None:
    section("Compose render")
    compose("config", "--quiet", what="compose config")
    # Parsed, never printed: the rendered model holds the generated passwords.
    model = json.loads(compose("config", "--format", "json", what="compose config"))
    expected_ports = {
        "kafka": [("127.0.0.1", str(KAFKA_PORT), 9092)],
        "otel-collector": [("127.0.0.1", str(OTLP_PORT), 4317)],
        "postgres": [("127.0.0.1", str(POSTGRES_PORT), 5432)],
        "kafka-init": [],
        "db-bootstrap": [],
    }
    if model.get("name") != PROJECT:
        raise SmokeFailure(f"rendered project name is {model.get('name')!r}")
    for service, ports in expected_ports.items():
        rendered = model["services"][service]
        if rendered.get("container_name"):
            raise SmokeFailure(f"{service} still has a fixed container name")
        found = [(p.get("host_ip"), str(p.get("published")), p.get("target"))
                 for p in rendered.get("ports", [])]
        if found != ports:
            raise SmokeFailure(f"{service} publishes {found}, expected {ports}")
        say(f"{service:15} no fixed name; ports "
            + (", ".join(f"{h}:{p}->{t}" for h, p, t in found) or "none"))
    kafka = model["services"]["kafka"]
    say(f"kafka hostname {kafka.get('hostname')}; aliases "
        f"{kafka['networks']['agentops-net'].get('aliases')}")
    say(f"kafka advertised {kafka['environment']['KAFKA_ADVERTISED_LISTENERS']}")
    say("networks " + ", ".join(sorted(n["name"] for n in model["networks"].values())))
    say("volumes  " + ", ".join(sorted(v["name"] for v in model["volumes"].values())))
    say("render: exit 0")


def container_id(service: str) -> str:
    found = compose("ps", "-a", "-q", service).split()
    if len(found) != 1:
        raise SmokeFailure(f"{service}: expected one container, found {len(found)}")
    return found[0]


def container_state(service: str) -> tuple[str, str, int]:
    """(status, health, exit code)"""
    text = checked(
        ["docker", "inspect", "-f",
         "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}"
         "|{{.State.ExitCode}}", container_id(service)],
        timeout=T_COMMAND, what="docker inspect",
    ).strip()
    status, health, code = text.split("|")
    return status, health, int(code)


def kafka_tool(tool: str, *args: str) -> subprocess.CompletedProcess:
    RUN.compose_used = True
    return run(
        compose_command("exec", "-T", "kafka", f"{KAFKA_BIN}/{tool}",
                        "--bootstrap-server", BROKER_INTERNAL, *args),
        timeout=T_COMMAND, what=tool,
    )


def wait_exited(service: str, timeout: float) -> int:
    def probe() -> Optional[int]:
        status, _, code = container_state(service)
        probe.detail = f"{service} is {status}"  # type: ignore[attr-defined]
        return code if status == "exited" else None
    return wait_for(f"{service} to finish", timeout, probe)


def start_kafka() -> None:
    section("Kafka")
    compose("up", "-d", "kafka", timeout=T_BROKER, what="compose up kafka")

    def probe() -> Optional[bool]:
        status, _, _ = container_state("kafka")
        if status != "running":
            raise SmokeFailure(f"the broker container is {status}")
        result = kafka_tool("kafka-topics.sh", "--list")
        probe.detail = (result.stdout + result.stderr).strip()[-600:]  # type: ignore[attr-defined]
        return True if result.returncode == 0 else None

    started = time.monotonic()
    wait_for("the broker to answer its own topic tool", T_BROKER, probe, interval=3.0)
    say(f"broker running; topic tool connected through {BROKER_INTERNAL} "
        f"after {time.monotonic() - started:.0f}s")


def topic_partitions() -> dict[str, int]:
    result = kafka_tool("kafka-topics.sh", "--describe")
    if result.returncode != 0:
        raise SmokeFailure("kafka-topics --describe failed:\n" + result.stderr[-600:])
    found = {}
    for line in result.stdout.splitlines():
        match = re.match(r"Topic:\s*(\S+)\s.*PartitionCount:\s*(\d+)", line)
        if match and not match.group(1).startswith("__"):
            found[match.group(1)] = int(match.group(2))
    return found


def create_topics() -> None:
    section("topics")
    compose("up", "-d", "kafka-init", timeout=T_KAFKA_INIT, what="compose up kafka-init")
    code = wait_exited("kafka-init", T_KAFKA_INIT)
    say(f"kafka-init exit code {code}")
    if code != 0:
        raise SmokeFailure("kafka-init failed:\n" + compose("logs", "--no-color", "kafka-init")[-800:])
    found = topic_partitions()
    for name, count in sorted(found.items()):
        say(f"{name}: {count} partitions")
    if found != {name: PARTITIONS for name in TOPICS}:
        raise SmokeFailure("the topics are not exactly the three expected ones")


def start_postgres() -> None:
    section("PostgreSQL")
    assert RUN.temp is not None
    build = run(compose_command("build", "postgres"), timeout=T_BUILD, what="compose build postgres")
    RUN.compose_used = True
    write_log("build-postgres.log", build.stdout + build.stderr)
    if build.returncode != 0:
        raise SmokeFailure("building the postgres image failed:\n"
                           + "\n".join((build.stdout + build.stderr).splitlines()[-10:]))
    compose("up", "-d", "--no-build", "postgres", timeout=T_POSTGRES, what="compose up postgres")

    def probe() -> Optional[bool]:
        status, health, _ = container_state("postgres")
        probe.detail = f"postgres is {status} / {health}"  # type: ignore[attr-defined]
        if status != "running":
            raise SmokeFailure(f"the postgres container is {status}")
        return True if health == "healthy" else None

    started = time.monotonic()
    wait_for("postgres to be healthy", T_POSTGRES, probe)
    say(f"image built; container running and healthy after {time.monotonic() - started:.0f}s")


def psql(sql: str) -> str:
    """One read-only statement in the ISOLATED database, through the project's
    own postgres container."""
    return compose(
        "exec", "-T", "postgres", "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1",
        "-U", "agentops", "-d", "agentops", "-c", " ".join(sql.split()), what="psql",
    ).strip()


def run_bootstrap() -> None:
    section("database bootstrap")
    assert RUN.temp is not None
    build = run(compose_command("build", "db-bootstrap"), timeout=T_BUILD,
                what="compose build db-bootstrap")
    write_log("build-bootstrap.log", build.stdout + build.stderr)
    if build.returncode != 0:
        raise SmokeFailure("building the bootstrap image failed:\n"
                           + "\n".join((build.stdout + build.stderr).splitlines()[-10:]))
    compose("up", "-d", "--no-build", "db-bootstrap", timeout=T_BOOTSTRAP,
            what="compose up db-bootstrap")
    code = wait_exited("db-bootstrap", T_BOOTSTRAP)
    output = compose("logs", "--no-color", "db-bootstrap")
    write_log("bootstrap.log", output)
    say(f"db-bootstrap exit code {code}")
    for line in output.strip().splitlines()[-4:]:
        say("  " + line[:200])
    if code != 0:
        raise SmokeFailure("the bootstrap failed")
    steps, last = psql(
        "SELECT count(*) || '|' || (SELECT step FROM agentops_bootstrap.ledger "
        "ORDER BY position DESC LIMIT 1) FROM agentops_bootstrap.ledger"
    ).split("|")
    say(f"ledger: {steps} steps recorded, last step '{last}'")
    if int(steps) != LEDGER_STEPS or last != "complete":
        raise SmokeFailure("the ledger is not COMPLETE with 17 steps")
    say("ledger state COMPLETE")


def start_collector() -> None:
    section("collector")
    compose("up", "-d", "otel-collector", timeout=T_COLLECTOR, what="compose up otel-collector")

    def probe() -> Optional[str]:
        status, _, _ = container_state("otel-collector")
        if status != "running":
            raise SmokeFailure(f"the collector container is {status}:\n"
                               + compose("logs", "--no-color", "otel-collector")[-800:])
        logs = compose("logs", "--no-color", "otel-collector")
        probe.detail = logs[-600:]  # type: ignore[attr-defined]
        return logs if "Everything is ready" in logs else None

    logs = wait_for("the collector to report it is ready", T_COLLECTOR, probe)
    time.sleep(3.0)
    logs = compose("logs", "--no-color", "otel-collector")
    bad = [
        line for line in logs.splitlines()
        if re.search(r"\t(error|fatal)\t|\"level\":\"(error|fatal)\"", line)
        or (re.search(r"(?i)kafka", line)
            and re.search(r"(?i)connection refused|no such host|unreachable|failed to", line))
    ]
    if bad:
        raise SmokeFailure("the collector logged errors at start:\n" + "\n".join(bad[:6]))
    with socket.create_connection(("127.0.0.1", OTLP_PORT), timeout=5.0):
        pass
    say(f"running; reported ready; no error line in {len(logs.splitlines())} startup log lines")
    say(f"127.0.0.1:{OTLP_PORT} accepts TCP connections")


# ---------------------------------------------------------------------------
# Host processes
# ---------------------------------------------------------------------------

def start_host_process(name: str, directory: str, env: Mapping[str, str]) -> None:
    assert RUN.temp is not None and RUN.python is not None
    log = RUN.temp / f"{name}.log"
    handle = open(log, "w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            [str(RUN.python), "main.py"], cwd=REPO / directory, env=dict(env),
            stdout=handle, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
    finally:
        handle.close()
    RUN.processes[name] = process
    RUN.logs[name] = log


def read_log(name: str) -> str:
    return RUN.logs[name].read_text(encoding="utf-8", errors="replace")


def check_host_processes() -> None:
    for name, process in RUN.processes.items():
        if process.poll() is not None:
            raise SmokeFailure(
                f"{name} exited with code {process.returncode}:\n" + read_log(name)[-1200:]
            )
        text = read_log(name)
        wrong = _WRONG_STACK.search(text)
        if wrong:
            raise SmokeFailure(f"{name} mentions the normal stack's port {wrong.group(1)}")
        if "Traceback" in text or "halted" in text:
            raise SmokeFailure(f"{name} reported a failure:\n" + text[-1200:])


def wait_started(name: str, expected: Sequence[str]) -> None:
    def probe() -> Optional[bool]:
        check_host_processes()
        text = read_log(name)
        probe.detail = text[-600:]  # type: ignore[attr-defined]
        return True if all(line in text for line in expected) and "Subscribed to" in text else None

    wait_for(f"{name} to print its configuration", T_HOST_SERVICE, probe, interval=1.0)
    for line in read_log(name).splitlines():
        if line.startswith("  "):
            say("  " + line.strip())


def group_rows(group: str, topic: str) -> Optional[list[dict[str, str]]]:
    """The rows of one topic in the consumer group description, or None when
    the group has no stable description yet."""
    result = kafka_tool("kafka-consumer-groups.sh", "--describe", "--group", group)
    group_rows.detail = (result.stdout + result.stderr).strip()[-800:]  # type: ignore[attr-defined]
    header: Optional[list[str]] = None
    rows = []
    for line in result.stdout.splitlines():
        cells = line.split()
        if cells[:1] == ["GROUP"]:
            header = cells
        elif header and len(cells) == len(header) and cells[0] == group:
            row = dict(zip(header, cells))
            if row.get("TOPIC") == topic:
                rows.append(row)
    return rows if len(rows) == PARTITIONS else None


def group_assigned(group: str, topic: str) -> Optional[bool]:
    rows = group_rows(group, topic)
    group_assigned.detail = getattr(group_rows, "detail", "")  # type: ignore[attr-defined]
    if rows is None:
        return None
    return True if all(row.get("CONSUMER-ID", "-") != "-" for row in rows) else None


def group_lag(group: str, topic: str) -> Optional[int]:
    """Messages the group has not committed, or None when unknown.  A
    partition that never held a message has no committed offset and no lag."""
    rows = group_rows(group, topic)
    if rows is None:
        return None
    total = 0
    for row in rows:
        if row["LAG"] == "-":
            total += int(row["LOG-END-OFFSET"]) if row["LOG-END-OFFSET"] != "-" else 0
        else:
            total += int(row["LAG"])
    return total


def start_host_services() -> None:
    section("stream processor")
    start_host_process("stream-processor", "stream-processor", host_environment(
        KAFKA_BOOTSTRAP_SERVERS=f"localhost:{KAFKA_PORT}",
        CONSUMER_GROUP_ID=GROUP_STREAM,
        CONSUMER_AUTO_OFFSET_RESET="earliest",
        TOPIC_OTLP_TRACES=TOPIC_RAW,
        TOPIC_SPANS=TOPIC_SPANS,
        TOPIC_DLQ=TOPIC_DLQ,
    ))
    wait_started("stream-processor", (
        f"bootstrap : localhost:{KAFKA_PORT}", f"group     : {GROUP_STREAM}",
        f"input     : {TOPIC_RAW}", f"trusted   : {TOPIC_SPANS}", f"dlq       : {TOPIC_DLQ}",
    ))
    say("alive; configuration points to the isolated broker")

    section("storage consumer")
    start_host_process("storage-consumer", "storage-consumer", host_environment(
        KAFKA_BOOTSTRAP_SERVERS=f"localhost:{KAFKA_PORT}",
        CONSUMER_GROUP_ID=GROUP_STORAGE,
        CONSUMER_AUTO_OFFSET_RESET="earliest",
        TOPIC_SPANS=TOPIC_SPANS,
        PG_HOST="127.0.0.1",
        PG_PORT=str(POSTGRES_PORT),
        PG_DATABASE="agentops",
        PG_USER="agentops",
        PG_PASSWORD=RUN.generated["POSTGRES_PASSWORD"],
    ))
    wait_started("storage-consumer", (
        f"bootstrap  : localhost:{KAFKA_PORT}", f"group      : {GROUP_STORAGE}",
        f"topic      : {TOPIC_SPANS}", f"pg host    : 127.0.0.1:{POSTGRES_PORT}",
        "pg database: agentops",
    ))
    say("alive; configuration points to the isolated broker and database")

    section("consumer groups")
    for group, topic in ((GROUP_STREAM, TOPIC_RAW), (GROUP_STORAGE, TOPIC_SPANS)):
        wait_for(f"group {group} to be assigned all partitions", T_HOST_SERVICE,
                 lambda: group_assigned(group, topic), interval=3.0)
        say(f"{group}: assigned {PARTITIONS} partitions of {topic}")
    check_host_processes()


def stop_host_processes() -> list[str]:
    """Stop only the processes this run started.  Returns what went wrong."""
    problems = []
    for name, process in RUN.processes.items():
        if process.poll() is None:
            if os.name == "nt":
                # The interpreter of a virtual environment is a child of its
                # launcher; both belong to this run.
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               capture_output=True, timeout=30)
            else:
                process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    problems.append(f"{name} (pid {process.pid}) did not stop")
                    continue
        say(f"{name}: stopped (pid {process.pid})")
    return problems


# ---------------------------------------------------------------------------
# Offsets, database, reconciliation
# ---------------------------------------------------------------------------

def end_offsets() -> dict[str, int]:
    """Sum of the end offsets of every partition, per topic."""
    totals = {}
    for topic in TOPICS:
        result = kafka_tool("kafka-get-offsets.sh", "--topic", topic)
        if result.returncode != 0:
            raise SmokeFailure(f"kafka-get-offsets {topic} failed:\n" + result.stderr[-600:])
        parts = [line.rsplit(":", 2) for line in result.stdout.split() if line.count(":") >= 2]
        if len(parts) != PARTITIONS or any(name != topic for name, _, _ in parts):
            raise SmokeFailure(f"unexpected offsets for {topic}: {result.stdout.strip()!r}")
        totals[topic] = sum(int(offset) for _, _, offset in parts)
    return totals


def stored_rows() -> int:
    return int(psql("SELECT count(*) FROM telemetry_spans"))


_RUN_SQL = """
WITH roots AS (
    SELECT * FROM telemetry_spans
    WHERE parent_span_id IS NULL
      AND attributes->>'agentops.workload.run_id' = '{run_id}'
), spans AS (
    SELECT * FROM telemetry_spans WHERE trace_id IN (SELECT trace_id FROM roots)
), sizes AS (
    SELECT trace_id, count(*) AS n FROM spans GROUP BY trace_id
)
SELECT json_build_object(
  'roots', (SELECT count(*) FROM roots),
  'root_traces', (SELECT count(DISTINCT trace_id) FROM roots),
  'request_ids', (SELECT coalesce(json_agg(rid ORDER BY rid), '[]') FROM (
      SELECT coalesce(request_id, attributes->>'agentops.request_id') AS rid FROM roots) t),
  'spans', (SELECT count(*) FROM spans),
  'trace_sizes', (SELECT coalesce(json_object_agg(n::text, c), '{{}}') FROM (
      SELECT n, count(*) AS c FROM sizes GROUP BY n) t),
  'status', (SELECT coalesce(json_object_agg(status_code, c), '{{}}') FROM (
      SELECT status_code, count(*) AS c FROM spans GROUP BY status_code) t),
  'error_spans', (SELECT coalesce(json_object_agg(span_name, c), '{{}}') FROM (
      SELECT span_name, count(*) AS c FROM spans WHERE status_code = 'ERROR'
      GROUP BY span_name) t),
  'error_roots', (SELECT count(*) FROM roots WHERE status_code = 'ERROR'),
  'error_types', (SELECT coalesce(json_object_agg(error_type, c), '{{}}') FROM (
      SELECT error_type, count(*) AS c FROM spans WHERE error_type IS NOT NULL
      GROUP BY error_type) t),
  'error_type_spans', (SELECT coalesce(json_object_agg(span_name, c), '{{}}') FROM (
      SELECT span_name, count(*) AS c FROM spans WHERE error_type IS NOT NULL
      GROUP BY span_name) t),
  'root_names', (SELECT coalesce(json_agg(DISTINCT span_name), '[]') FROM roots),
  'modes', (SELECT coalesce(json_agg(DISTINCT attributes->>'agentops.workload.mode'), '[]')
            FROM roots),
  'root_scenarios', (SELECT coalesce(json_object_agg(
      coalesce(request_id, attributes->>'agentops.request_id'),
      attributes->>'agentops.workload.scenario'), '{{}}') FROM roots),
  'error_root_request_ids', (SELECT coalesce(json_agg(
      coalesce(request_id, attributes->>'agentops.request_id')), '[]')
      FROM roots WHERE status_code = 'ERROR'),
  'roots_without_persona', (SELECT count(*) FROM roots
      WHERE coalesce(attributes->>'agentops.workload.persona', '') = ''),
  'services', (SELECT coalesce(json_agg(DISTINCT service_name), '[]') FROM spans),
  'kafka_topics', (SELECT coalesce(json_agg(DISTINCT kafka_topic), '[]') FROM spans)
)
"""

_GLOBAL_SQL = """
SELECT json_build_object(
  'rows', (SELECT count(*) FROM telemetry_spans),
  'run_ids', (SELECT coalesce(json_agg(DISTINCT attributes->>'agentops.workload.run_id'), '[]')
              FROM telemetry_spans WHERE attributes ? 'agentops.workload.run_id'),
  'rows_outside_workload_traces', (SELECT count(*) FROM telemetry_spans WHERE trace_id NOT IN (
      SELECT trace_id FROM telemetry_spans
      WHERE parent_span_id IS NULL AND attributes ? 'agentops.workload.run_id'))
)
"""


class Checks:
    """Exact comparisons.  Every one is printed; one mismatch fails the stage."""

    def __init__(self) -> None:
        self.failed: list[str] = []

    def equal(self, name: str, found: Any, expected: Any) -> None:
        ok = found == expected
        say(f"  {'ok      ' if ok else 'MISMATCH'} {name}: {found}"
            + ("" if ok else f"   (expected {expected})"))
        if not ok:
            self.failed.append(name)

    def true(self, name: str, condition: bool, found: Any) -> None:
        say(f"  {'ok      ' if condition else 'MISMATCH'} {name}: {found}")
        if not condition:
            self.failed.append(name)

    def finish(self, what: str) -> None:
        if self.failed:
            raise SmokeFailure(f"{what}: mismatch in " + ", ".join(self.failed))


def _tally(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def planned_facts(stage: Stage) -> dict[str, Any]:
    """What the generator's own planner says about the stage.  Computed by the
    generator's code in the fresh environment; nothing is planned here."""
    assert RUN.python is not None
    code = (
        "import json\n"
        "from workload_generator.faults import attempts\n"
        "from workload_generator.plan import PlanConfig, build_plan\n"
        "from workload_generator.report import plan_digest\n"
        "from workload_generator.scenarios import Scenario\n"
        f"plan = build_plan(PlanConfig(seed={SEED}, run_id={stage.run_id!r},"
        f" scenario=Scenario({stage.scenario!r}), traces={stage.traces},"
        f" rate={float(RATE)!r}, persona=None, mode='real'))\n"
        "tries = [a for r in plan.requests for a in attempts(r)]\n"
        "print(json.dumps({'digest': plan_digest(plan),"
        " 'request_ids': sorted(a.request_id for a in tries),"
        " 'scenario_by_request': {a.request_id: r.scenario.value"
        " for r in plan.requests for a in attempts(r)},"
        " 'failing_request_ids': sorted(a.request_id for a in tries"
        " if a.effects.fault is not None),"
        " 'failing_indexes': [r.request_index for r in plan.requests if r.fault is not None],"
        " 'failing_attempts': sum(a.effects.fault is not None for a in tries)}))\n"
    )
    return json.loads(checked(
        [str(RUN.python), "-c", code], cwd=REPO / "workload-generator",
        timeout=T_COMMAND, what="planned facts", env=host_environment(),
    ))


def run_stage(stage: Stage, baseline: dict[str, int]) -> dict[str, int]:
    """Send one workload and reconcile it exactly.  Returns the end offsets
    afterwards, the baseline of the next stage."""
    assert RUN.temp is not None and RUN.python is not None
    label = f"stage {stage.run_id}"
    spans_before = stored_rows()

    section(f"{label}: plan")
    if int(psql(
        "SELECT count(*) FROM telemetry_spans WHERE attributes->>'agentops.workload.run_id' "
        f"= '{stage.run_id}'"
    )) != 0:
        raise SmokeFailure(f"{label}: the run id is already in the database")
    planned = planned_facts(stage)
    checks = Checks()
    checks.equal("planned submissions", len(planned["request_ids"]), stage.traces)
    checks.equal("distinct request ids", len(set(planned["request_ids"])), stage.traces)
    checks.equal("failing request indexes", tuple(planned["failing_indexes"]),
                 stage.failing_indexes)
    checks.equal("failing attempts", planned["failing_attempts"], stage.failures)
    checks.equal("planned scenario per request -> requests",
                 _tally(planned["scenario_by_request"].values()), dict(stage.scenario_roots))
    checks.true("plan digest", planned["digest"].startswith(stage.digest_prefix),
                planned["digest"][:16] + "...")
    checks.finish(f"{label} plan")

    section(f"{label}: driver")
    check_host_processes()
    report_path = RUN.temp / f"{stage.run_id}.json"
    command = [
        str(RUN.python), "-m", "workload_generator",
        "--mode", "real", "--scenario", stage.scenario, "--traces", str(stage.traces),
        "--rate", str(RATE), "--seed", str(SEED), "--concurrency", str(CONCURRENCY),
        "--run-id", stage.run_id, "--max-duration", str(stage.max_duration),
        "--send", "--endpoint", f"http://localhost:{OTLP_PORT}",
        "--report", str(report_path),
    ]
    say("python -m workload_generator " + " ".join(command[3:-1]) + " <temp report>")
    result = run(command, cwd=REPO / "workload-generator", env=host_environment(),
                 timeout=stage.max_duration + 120, what=label)
    output = result.stdout + result.stderr
    write_log(f"{stage.run_id}.log", output)
    wrong = _WRONG_STACK.search(output)
    if wrong:
        raise SmokeFailure(f"{label}: the generator mentions port {wrong.group(1)}")
    if result.returncode != 0 or not report_path.is_file():
        raise SmokeFailure(f"{label}: generator exit {result.returncode}\n{output[-1500:]}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    execution = report["execution"]
    attempts = {name: count for name, count in execution["attempts"].items() if count}
    expected_attempts = {"expected_success": stage.successes}
    if stage.failures:
        expected_attempts["expected_failure"] = stage.failures
    checks = Checks()
    checks.equal("exit code", result.returncode, 0)
    checks.equal("run id", report["run_id"], stage.run_id)
    checks.equal("plan digest equals the planned one", report["plan_digest"], planned["digest"])
    checks.equal("planned requests", report["planned_requests"], stage.traces)
    checks.equal("planned retries", report["planned_retries"], 0)
    checks.equal("planned submissions", report["planned_submissions"], stage.traces)
    checks.equal("endpoint", execution["endpoint"], f"http://localhost:{OTLP_PORT}")
    checks.equal("started requests", execution["started_requests"], stage.traces)
    checks.equal("not started requests", execution["not_started_requests"], 0)
    checks.equal("executed attempts", execution["executed_attempts"], stage.traces)
    checks.equal("executed retries", execution["executed_retries"], 0)
    checks.equal("attempt outcomes (non-zero)", attempts, expected_attempts)
    checks.equal("integrity problems", execution["problems"], [])
    checks.equal("spans ended", execution["spans_ended"], stage.spans)
    checks.equal("spans exported successfully", execution["spans_exported_successfully"],
                 stage.spans)
    checks.equal("export failures", execution["export_failures"], 0)
    checks.equal("flush ok", execution["flush_ok"], True)
    checks.equal("run status", execution["run_status"], "completed")
    say(f"  elapsed {execution['elapsed_seconds']:.1f}s; late starts {execution['late_starts']}")
    checks.finish(f"{label} driver")

    section(f"{label}: drain")

    def drained() -> Optional[tuple[dict[str, int], int, int]]:
        check_host_processes()
        offsets = end_offsets()
        stream, storage = group_lag(GROUP_STREAM, TOPIC_RAW), group_lag(GROUP_STORAGE, TOPIC_SPANS)
        rows = stored_rows() - spans_before
        drained.detail = (  # type: ignore[attr-defined]
            f"canonical delta {offsets[TOPIC_SPANS] - baseline[TOPIC_SPANS]}, rows {rows}, "
            f"stream lag {stream}, storage lag {storage}"
        )
        done = (offsets[TOPIC_SPANS] - baseline[TOPIC_SPANS] >= stage.spans
                and rows >= stage.spans and stream == 0 and storage == 0)
        return (offsets, stream, storage) if done else None

    started = time.monotonic()
    try:
        wait_for(f"{label} to drain", T_DRAIN, drained)
        say(f"drained after {time.monotonic() - started:.0f}s; "
            f"settling {SETTLE_SECONDS:.0f}s to catch late or duplicate records")
    except SmokeFailure as failure:
        # The exact counts below are the evidence of what did not arrive.
        say(str(failure))
    time.sleep(SETTLE_SECONDS)
    check_host_processes()
    offsets = end_offsets()
    stream_lag, storage_lag = group_lag(GROUP_STREAM, TOPIC_RAW), group_lag(GROUP_STORAGE, TOPIC_SPANS)
    (RUN.temp / f"offsets-after-{stage.run_id}.json").write_text(json.dumps(offsets), encoding="utf-8")

    section(f"{label}: Kafka reconciliation")
    checks = Checks()
    checks.equal(f"{GROUP_STREAM} lag", stream_lag, 0)
    checks.equal(f"{GROUP_STORAGE} lag", storage_lag, 0)
    checks.equal("canonical topic delta", offsets[TOPIC_SPANS] - baseline[TOPIC_SPANS], stage.spans)
    checks.equal("DLQ delta", offsets[TOPIC_DLQ] - baseline[TOPIC_DLQ], 0)
    checks.equal("DLQ end offset", offsets[TOPIC_DLQ], 0)
    raw_delta = offsets[TOPIC_RAW] - baseline[TOPIC_RAW]
    checks.true("raw OTLP topic delta > 0 (not compared with spans)", raw_delta > 0, raw_delta)

    section(f"{label}: database reconciliation")
    found = json.loads(psql(_RUN_SQL.format(run_id=stage.run_id)))
    # Ordered here: the database sorts text by its own collation.
    found["request_ids"] = sorted(rid or "" for rid in found["request_ids"])
    checks.equal("root spans", found["roots"], stage.traces)
    checks.equal("distinct root trace ids", found["root_traces"], stage.traces)
    checks.equal("request-id set equals the planned set", found["request_ids"],
                 planned["request_ids"])
    say(f"           request ids sha256 {short_hash(found['request_ids'])} "
        f"(planned {short_hash(planned['request_ids'])})")
    checks.equal("total spans", found["spans"], stage.spans)
    checks.equal("spans per trace -> traces", found["trace_sizes"], dict(stage.trace_sizes))
    checks.equal("status codes", found["status"], dict(stage.status))
    checks.equal("ERROR spans by name", found["error_spans"], dict(stage.error_spans))
    checks.equal("ERROR root spans", found["error_roots"], stage.failures)
    checks.equal("error_type values", found["error_types"], dict(stage.error_types))
    checks.equal("spans carrying error_type", found["error_type_spans"],
                 {TOOL_SPAN: stage.failures} if stage.failures else {})
    checks.equal("root span name", found["root_names"], [ROOT_SPAN])
    checks.equal("agentops.workload.mode", found["modes"], ["real"])
    checks.equal("agentops.workload.scenario per root -> roots",
                 _tally(found["root_scenarios"].values()), dict(stage.scenario_roots))
    checks.equal("every root carries the scenario planned for its request",
                 found["root_scenarios"] == planned["scenario_by_request"], True)
    checks.equal("ERROR roots are exactly the planned failing requests",
                 sorted(found["error_root_request_ids"]), planned["failing_request_ids"])
    checks.equal("roots of the run's scenario are exactly the planned ones",
                 sorted(r for r, s in found["root_scenarios"].items() if s != "normal"),
                 sorted(r for r, s in planned["scenario_by_request"].items() if s != "normal"))
    checks.equal("roots without agentops.workload.persona", found["roots_without_persona"], 0)
    checks.equal("service_name", found["services"], [SERVICE_NAME])
    checks.equal("kafka_topic", found["kafka_topics"], [TOPIC_SPANS])
    checks.equal("rows added to telemetry_spans", stored_rows() - spans_before, stage.spans)

    section(f"{label}: exporter to storage")
    say(f"  driver spans ended {execution['spans_ended']} = exporter reported success for "
        f"{execution['spans_exported_successfully']} = canonical topic records "
        f"{offsets[TOPIC_SPANS] - baseline[TOPIC_SPANS]} = rows stored {found['spans']}")
    check_host_processes()
    checks.finish(label)
    say(f"{label}: PASS")
    return offsets


def check_baseline() -> dict[str, int]:
    section("baseline")
    check_host_processes()
    offsets = end_offsets()
    rows = stored_rows()
    checks = Checks()
    checks.equal("telemetry_spans rows", rows, 0)
    checks.equal("canonical topic end offsets", offsets[TOPIC_SPANS], 0)
    checks.equal("DLQ end offsets", offsets[TOPIC_DLQ], 0)
    checks.equal("raw OTLP topic end offsets", offsets[TOPIC_RAW], 0)
    checks.equal("topics", topic_partitions(), {name: PARTITIONS for name in TOPICS})
    status, health, _ = container_state("postgres")
    checks.equal("isolated postgres", f"{status} {health}", "running healthy")
    checks.equal("isolated collector", container_state("otel-collector")[0], "running")
    checks.equal("isolated broker", container_state("kafka")[0], "running")
    checks.finish("baseline")
    check_normal_stack("before any telemetry")
    return offsets


def check_totals(offsets: dict[str, int]) -> None:
    section("isolated database total")
    found = json.loads(psql(_GLOBAL_SQL))
    checks = Checks()
    checks.equal("rows in telemetry_spans", found["rows"], sum(stage.spans for stage in STAGES))
    checks.equal("workload run ids", sorted(found["run_ids"]),
                 sorted(stage.run_id for stage in STAGES))
    checks.equal("rows outside the workload's traces", found["rows_outside_workload_traces"], 0)
    checks.equal("canonical topic records in all", offsets[TOPIC_SPANS],
                 sum(stage.spans for stage in STAGES))
    checks.equal("DLQ records in all", offsets[TOPIC_DLQ], 0)
    say(f"  raw OTLP topic records in all: {offsets[TOPIC_RAW]}")
    checks.finish("totals")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _remove_tree(path: Path) -> bool:
    def retry(function: Callable[..., Any], target: str, _exc: Any) -> None:
        os.chmod(target, stat.S_IWRITE)
        function(target)

    for _ in range(5):
        try:
            if sys.version_info >= (3, 12):
                shutil.rmtree(path, onexc=retry)
            else:
                shutil.rmtree(path, onerror=retry)
        except OSError:
            time.sleep(2.0)
        if not path.exists():
            return True
    return False


def cleanup() -> bool:
    """Give everything back.  Returns True when all of it was verified."""
    section("cleanup")
    problems = stop_host_processes()

    leaked = 0
    if RUN.temp is not None and RUN.temp.exists():
        for path in RUN.temp.glob("*"):
            if path.is_file() and path.name != "smoke.env":
                text = path.read_text(encoding="utf-8", errors="replace")
                leaked += sum(text.count(value) for value in RUN.generated.values())
        say(f"generated passwords found in logs, reports and snapshots: {leaked} "
            f"(masked before writing: {RUN.masked_in_logs})")
        if leaked or RUN.masked_in_logs:
            problems.append("a generated password reached a temporary log")

    if RUN.compose_used and RUN.env_file is not None and RUN.env_file.is_file():
        down = run(
            compose_command("down", "--volumes", "--rmi", "local", "--remove-orphans"),
            timeout=T_DOWN, what="compose down",
        )
        say(f"compose down --volumes --rmi local --remove-orphans: exit {down.returncode}")
        if down.returncode != 0:
            problems.append("compose down failed: " + redact(down.stderr.strip()[-400:]))
    else:
        say("compose down: not needed, no Compose command had run")

    label = f"label=com.docker.compose.project={PROJECT}"
    for kind, command in (
        ("containers", ["ps", "-a", "-q", "--filter", label]),
        ("networks", ["network", "ls", "-q", "--filter", label]),
        ("volumes", ["volume", "ls", "-q", "--filter", label]),
        ("images", ["images", "-q", "--filter", label]),
    ):
        left = docker_lines(*command)
        say(f"smoke {kind} left: {len(left)}")
        if left:
            problems.append(f"{len(left)} smoke {kind} left")

    after = docker_fingerprint()
    if RUN.before is not None:
        known = {line.rsplit(" ", 1)[1] for line in RUN.before["images"]}
        for line in after["images"]:
            name, image_id = line.rsplit(" ", 1)
            if image_id not in known:
                # Absent before this run, so this run brought it; removed by
                # its exact id, never forced.
                removed = run(["docker", "image", "rm", image_id], timeout=T_COMMAND,
                              what="docker image rm")
                say(f"image new in this run: {name} {image_id[:19]} -> "
                    f"{'removed' if removed.returncode == 0 else 'NOT removed'}")
        for name in after["volumes"]:
            if name not in RUN.before["volumes"]:
                # Absent before this run and left behind by its containers.
                removed = run(["docker", "volume", "rm", name], timeout=T_COMMAND,
                              what="docker volume rm")
                say(f"volume new in this run: {name[:19]} -> "
                    f"{'removed' if removed.returncode == 0 else 'NOT removed'}")
        after = docker_fingerprint()

    if RUN.temp is not None:
        gone = _remove_tree(RUN.temp)
        say(f"temporary directory deleted: {'yes' if gone else 'NO'}")
        if not gone:
            problems.append("the temporary directory could not be deleted")

    section("normal Docker state after")
    if RUN.before is None:
        say("no state was recorded before; nothing to compare")
    else:
        for kind, lines in after.items():
            same = lines == RUN.before[kind]
            say(f"{kind}: {len(lines)} (fingerprint {short_hash(lines)}) "
                f"{'identical' if same else 'DIFFERENT'}")
            if not same:
                problems.append(f"Docker {kind} differ from before")
                for line in sorted(set(lines) ^ set(RUN.before[kind])):
                    say(f"  {'+' if line in lines else '-'} {line}")
        now = normal_postgres()
        say(f"{NORMAL_POSTGRES_CONTAINER}: {now or 'not present on this machine'}")
        if now != RUN.normal_postgres:
            problems.append(f"{NORMAL_POSTGRES_CONTAINER} changed")

    section("repository after")
    try:
        state = repository_state()
        for line in state:
            say(line)
        if any(line not in ALLOWED_UNTRACKED for line in state):
            problems.append("the repository holds unexpected changes")
        caches = sorted(bytecode_caches() - RUN.caches_before)
        say(f"__pycache__ directories written during the run: {len(caches)}")
        if caches:
            problems.append("bytecode caches were written to the repository")
    except SmokeFailure as failure:
        problems.append(str(failure))

    for problem in problems:
        say(f"CLEANUP PROBLEM: {problem}")
    return not problems


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def smoke(expect_head: Optional[str]) -> None:
    check_repository(expect_head)
    check_ports()
    record_docker_state()
    create_workspace()
    create_virtual_environment()
    check_render()
    start_kafka()
    create_topics()
    start_postgres()
    run_bootstrap()
    start_collector()
    start_host_services()
    offsets = check_baseline()
    for stage in STAGES:
        # A stage that does not reconcile raises; the next one never runs.
        offsets = run_stage(stage, offsets)
        check_normal_stack(f"after {stage.run_id}")
    check_totals(offsets)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_smoke",
        description="Live smoke test of the workload generator on a disposable stack.",
    )
    parser.add_argument("--expect-head", default=None, metavar="COMMIT",
                        help="refuse to run unless HEAD is exactly this commit")
    args = parser.parse_args(argv)
    if sys.version_info[:2] != (3, 13):
        parser.error("run_smoke needs Python 3.13")

    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(errors="replace")

    passed = False
    try:
        smoke(args.expect_head)
        passed = True
    except SmokeFailure as failure:
        section("FAILURE")
        say(str(failure))
    except KeyboardInterrupt:
        section("FAILURE")
        say("interrupted")
    except Exception as exc:  # anything unforeseen is a failure, and is cleaned up too
        section("FAILURE")
        say(f"{type(exc).__name__}: {exc}")
    try:
        clean = cleanup()
    except Exception as exc:
        clean = False
        say(f"CLEANUP PROBLEM: {type(exc).__name__}: {exc}")

    section("result")
    say(f"live smoke: {'PASSED' if passed else 'FAILED'}")
    say(f"cleanup verified: {'yes' if clean else 'NO'}")
    return 0 if passed and clean else 1


if __name__ == "__main__":
    sys.exit(main())
