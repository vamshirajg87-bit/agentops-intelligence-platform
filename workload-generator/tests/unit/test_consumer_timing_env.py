"""
workload-generator/tests/unit/test_consumer_timing_env.py

The storage consumer's timing switch in the D3 smoke harness (run_smoke.py).
No process, Docker or network: start_host_process and the waits are replaced.

Proves:
  - timing is off by default and nothing is inherited from the ambient environment
  - it is passed to the storage consumer only when the harness setting is above 0
  - no other host process, and no other setting of the consumer, changes
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_WG = Path(__file__).resolve().parents[2]
if str(_WG) not in sys.path:
    sys.path.insert(0, str(_WG))

import run_smoke as smoke  # noqa: E402

VARIABLE = "CONSUMER_TIMING_INTERVAL_SECONDS"


@pytest.fixture
def started(monkeypatch):
    """Runs start_host_services with the process start and the waits replaced; returns
    {process name: environment it would have been given}."""
    environments = {}
    monkeypatch.setattr(smoke, "start_host_process",
                        lambda name, directory, env: environments.__setitem__(name, dict(env)))
    monkeypatch.setattr(smoke, "wait_started", lambda *a, **k: None)
    monkeypatch.setattr(smoke, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr(smoke, "check_host_processes", lambda: None)
    monkeypatch.setattr(smoke.RUN, "generated", {"POSTGRES_PASSWORD": "not-a-real-password"})
    monkeypatch.delenv(VARIABLE, raising=False)
    return environments


def test_the_harness_setting_is_off_by_default():
    assert smoke.CONSUMER_TIMING_INTERVAL == 0.0


def test_the_ambient_environment_is_never_inherited(monkeypatch, started):
    monkeypatch.setenv(VARIABLE, "5")
    assert VARIABLE not in smoke.host_environment()
    smoke.start_host_services()
    assert set(started) == {"stream-processor", "storage-consumer"}
    assert all(VARIABLE not in env for env in started.values())


def test_disabled_leaves_the_consumer_environment_exactly_as_before(monkeypatch, started):
    monkeypatch.setattr(smoke, "CONSUMER_TIMING_INTERVAL", 0.0)
    smoke.start_host_services()
    consumer = started["storage-consumer"]
    assert VARIABLE not in consumer
    assert consumer["PG_PASSWORD"] == "not-a-real-password"
    assert consumer["CONSUMER_GROUP_ID"] == smoke.GROUP_STORAGE
    assert consumer["KAFKA_BOOTSTRAP_SERVERS"] == f"127.0.0.1:{smoke.KAFKA_PORT}"


@pytest.mark.parametrize("interval,text", [(5.0, "5"), (2.5, "2.5"), (10, "10")])
def test_enabled_passes_the_interval_to_the_storage_consumer_only(monkeypatch, started,
                                                                  interval, text):
    monkeypatch.setattr(smoke, "CONSUMER_TIMING_INTERVAL", interval)
    smoke.start_host_services()
    assert started["storage-consumer"][VARIABLE] == text
    assert VARIABLE not in started["stream-processor"]


def test_enabling_changes_exactly_one_variable(monkeypatch, started):
    smoke.start_host_services()
    plain = {name: dict(env) for name, env in started.items()}
    started.clear()
    monkeypatch.setattr(smoke, "CONSUMER_TIMING_INTERVAL", 5.0)
    smoke.start_host_services()
    assert started["stream-processor"] == plain["stream-processor"]
    consumer = dict(started["storage-consumer"])
    assert consumer.pop(VARIABLE) == "5"
    assert consumer == plain["storage-consumer"]


def test_a_non_positive_setting_is_off(monkeypatch, started):
    for value in (0, 0.0, -1.0):
        started.clear()
        monkeypatch.setattr(smoke, "CONSUMER_TIMING_INTERVAL", value)
        smoke.start_host_services()
        assert VARIABLE not in started["storage-consumer"]
