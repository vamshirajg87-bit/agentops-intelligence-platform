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
    from timing import TimingRecorder


def process_message(
    msg: confluent_kafka.Message,
    repository: SpanRepository,
    timing: TimingRecorder | None = None,
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

    timing (optional, default None = no timing work): a TimingRecorder that is
    given the duration of steps 2-4 ("decode_validate") and of step 5
    ("insert_txn"), and whether the insert was new or a conflict.  It only
    observes; it changes no step, no result and no exception.
    """
    started = timing.clock() if timing is not None else 0.0
    raw_value = msg.value()

    if raw_value is None:
        raise RuntimeError(
            f"Tombstone message received on trusted topic "
            f"{msg.topic()!r} partition={msg.partition()} offset={msg.offset()}"
        )

    text = raw_value.decode("utf-8")
    event_dict = json.loads(text)
    event = CanonicalSpanEvent.model_validate(event_dict)

    if timing is None:
        repository.insert_span(
            event,
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )
        return

    validated = timing.clock()
    timing.record("decode_validate", validated - started)
    inserted = repository.insert_span(
        event,
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
    )
    timing.record("insert_txn", timing.clock() - validated)
    timing.count("conflicts" if inserted is False else "inserted")


def run_consumer_loop(
    consumer: confluent_kafka.Consumer,
    repository: SpanRepository,
    shutdown_event: threading.Event,
    poll_timeout_seconds: float = 1.0,
    timing: TimingRecorder | None = None,
) -> None:
    """
    Drive the consumer loop until shutdown_event is set or an error halts.

    consumer.close() is guaranteed to execute via finally block.
    Source offsets are committed only by consumer.commit() after successful
    process_message() completion.

    timing (optional, default None = no timing work) only observes: it adds no
    step, thread or retry, and a stage that raises is not recorded and its
    exception propagates exactly as without it.  The final summary is written
    in the finally block before the consumer is closed.
    """
    try:
        while not shutdown_event.is_set():
            cycle_started = timing.clock() if timing is not None else 0.0
            msg = consumer.poll(timeout=poll_timeout_seconds)

            if msg is None:
                if timing is not None:
                    timing.count("idle_polls")
                    timing.maybe_emit()
                continue

            if msg.error() is not None:
                raise RuntimeError(f"consumer error: {msg.error()}")

            if timing is None:
                process_message(msg, repository)

                consumer.commit(message=msg, asynchronous=False)
                continue

            polled = timing.clock()
            timing.record("poll", polled - cycle_started)
            process_message(msg, repository, timing)

            commit_started = timing.clock()
            consumer.commit(message=msg, asynchronous=False)
            finished = timing.clock()
            timing.record("kafka_commit", finished - commit_started)
            timing.record("cycle", finished - cycle_started)
            timing.count("messages")
            timing.maybe_emit()
    finally:
        if timing is not None:
            timing.final()
        consumer.close()
