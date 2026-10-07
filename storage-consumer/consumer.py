"""
storage-consumer/consumer.py

Orchestration layer for the agentops storage consumer.

Connects the Kafka message stream to the PostgreSQL span repository with
correct at-least-once offset semantics: the Kafka offset is committed only
after the database transaction commits.

PROCESS_MESSAGE CONTRACT
------------------------
process_message() returns normally only when the span record is
commit-eligible: it has been persisted (or already existed) in the database
and the transaction has committed.

Any propagated exception means the caller must not commit the source offset.
The consumer loop halts.

RUN_CONSUMER_LOOP CONTRACT
--------------------------
Source offsets are committed only via:
    consumer.commit(message=msg, asynchronous=False)
called after process_message() returns normally.

consumer.close() is guaranteed to execute via try/finally cleanup.

FAILURE POLICY
--------------
This is a trusted-topic consumer. Tombstone payloads, malformed JSON, or
CanonicalSpanEvent validation failures all halt the consumer rather than
routing to a DLQ. The operator must investigate and resolve the root cause.
"""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

import confluent_kafka

from schemas.telemetry import CanonicalSpanEvent

if TYPE_CHECKING:
    from repository import SpanRepository


def process_message(
    msg: confluent_kafka.Message,
    repository: SpanRepository,
) -> None:
    """
    Process a single Kafka message from the trusted spans topic.

    Precondition: msg.error() has already been checked by the caller and is None.

    Steps:
        1. Reject tombstone (None value) — raises RuntimeError
        2. Decode UTF-8 bytes — UnicodeDecodeError propagates
        3. Parse JSON — JSONDecodeError propagates
        4. Validate as CanonicalSpanEvent — ValidationError propagates
        5. Persist via repository.insert_span() — any psycopg exception propagates

    Both True (new row) and False (ON CONFLICT DO NOTHING) from insert_span
    are treated as success: the message is commit-eligible.

    Never commits a Kafka offset.
    """
    raw_value = msg.value()

    if raw_value is None:
        raise RuntimeError(
            f"Tombstone message received on trusted topic "
            f"{msg.topic()!r} partition={msg.partition()} offset={msg.offset()}"
        )

    text = raw_value.decode("utf-8")
    event_dict = json.loads(text)
    event = CanonicalSpanEvent.model_validate(event_dict)

    repository.insert_span(
        event,
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
    )


def run_consumer_loop(
    consumer: confluent_kafka.Consumer,
    repository: SpanRepository,
    shutdown_event: threading.Event,
    poll_timeout_seconds: float = 1.0,
) -> None:
    """
    Drive the consumer loop until shutdown_event is set or an error halts.

    consumer.close() is guaranteed to execute via finally block.
    Source offsets are committed only by consumer.commit() after successful
    process_message() completion.
    """
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=poll_timeout_seconds)

            if msg is None:
                continue

            if msg.error() is not None:
                raise RuntimeError(f"consumer error: {msg.error()}")

            process_message(msg, repository)

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
