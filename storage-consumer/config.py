"""
storage-consumer/config.py

Configuration constants for the Phase 6 storage consumer.

All values can be overridden via environment variables.
Defaults target the local Docker Compose development stack accessed from
the Windows host (Kafka and PostgreSQL exposed on localhost).

PYTHONPATH note: The storage consumer deserializes CanonicalSpanEvent from
demo-app/schemas/telemetry.py. Set PYTHONPATH to include demo-app/ before
running the storage consumer:

    $env:PYTHONPATH = "$PWD\\demo-app"          # PowerShell
    export PYTHONPATH="$PWD/demo-app"            # bash/zsh

Consumer group isolation
------------------------
CONSUMER_GROUP_ID defaults to "agentops-storage-consumer", which is
intentionally distinct from the Phase 5 stream processor group
("agentops-stream-processor"). The two services consume agentops.telemetry.spans
independently, maintaining separate committed offsets on the broker.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Kafka connectivity
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------

# The trusted canonical spans topic produced by the Phase 5 stream processor.
TOPIC_SPANS: str = os.environ.get(
    "TOPIC_SPANS", "agentops.telemetry.spans"
)

# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

# Must differ from the Phase 5 stream processor group ID so the storage
# consumer maintains its own independent offset position on TOPIC_SPANS.
CONSUMER_GROUP_ID: str = os.environ.get(
    "CONSUMER_GROUP_ID", "agentops-storage-consumer"
)

# "earliest" ensures no spans are missed after a fresh deployment or restart.
CONSUMER_AUTO_OFFSET_RESET: str = os.environ.get(
    "CONSUMER_AUTO_OFFSET_RESET", "earliest"
)

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

PG_HOST: str = os.environ.get("PG_HOST", "localhost")
PG_PORT: int = int(os.environ.get("PG_PORT", "5432"))
PG_DATABASE: str = os.environ.get("PG_DATABASE", "agentops")
PG_USER: str = os.environ.get("PG_USER", "agentops")
# Local-development credential only — not a production secret.
PG_PASSWORD: str = os.environ.get("PG_PASSWORD", "agentops_dev")
