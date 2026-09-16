"""
stream-processor/processor.py

Orchestration layer for the agentops stream processor.

Connects the OTLP decoder, canonicalizer, span deduplicator, and Kafka
publisher into a source-offset-correct consumer loop.

PROCESS_MESSAGE CONTRACT
------------------------
process_message() returns normally only when the complete source record is
commit-eligible: every span has been published to the trusted topic with
positive broker ack and marked in the deduplicator, sent to the DLQ with
positive broker ack, or skipped as a confirmed in-process duplicate.

Any propagated exception means the source record is NOT commit-eligible.
The caller must not commit the source offset. The consumer loop halts.

RUN_CONSUMER_LOOP CONTRACT
--------------------------
Source offsets are committed only via:
    consumer.commit(message=msg, asynchronous=False)
called after process_message() returns normally.

consumer.close() is guaranteed to execute even if publisher.close() raises,
via nested try/finally cleanup.

PHASE BOUNDARY
--------------
Phase 5.5 ships orchestration and unit tests only.
The existing CanonicalSpanExporter direct Kafka write path is not modified.
Phase 5.6 proves end-to-end correctness and disables that path.
"""

from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone

import confluent_kafka

from canonicalizer import CanonicalizationError, canonicalize
from deduplicator import SpanDeduplicator
from dlq import DLQEvent, DLQReason, build_dlq_event
from otlp_decoder import OTLPDecodeError, decode
from publisher import KafkaPublisher


def process_message(
    msg: confluent_kafka.Message,
    publisher: KafkaPublisher,
    deduplicator: SpanDeduplicator,
    source_topic: str,
    source_partition: int,
    source_offset: int,
) -> None:
    """
    Process a single Kafka message.

    Precondition: msg.error() has already been checked by the caller and is None.

    Returns normally when the complete source record is commit-eligible.
    Any propagated exception means the caller must not commit the source offset.

    Does not depend on confluent_kafka.Consumer.
    """
    raw_value = msg.value()

    if raw_value is None:
        publisher.publish_dlq(
            DLQEvent(
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                failed_at=datetime.now(tz=timezone.utc),
                reason=DLQReason.OTLP_DECODE_FAILURE,
                error_message="Kafka message value is None",
                source_payload_sha256=None,
            )
        )
        return

    source_payload_sha256: str = hashlib.sha256(raw_value).hexdigest()

    try:
        spans = decode(raw_value)
    except OTLPDecodeError as exc:
        publisher.publish_dlq(
            build_dlq_event(
                exc=exc,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                source_payload_sha256=source_payload_sha256,
            )
        )
        return

    for span_dict in spans:
        try:
            event = canonicalize(span_dict)
        except CanonicalizationError as exc:
            publisher.publish_dlq(
                build_dlq_event(
                    exc=exc,
                    source_topic=source_topic,
                    source_partition=source_partition,
                    source_offset=source_offset,
                    trace_id=span_dict.get("trace_id"),
                    span_id=span_dict.get("span_id"),
                    service_name=span_dict.get("service_name"),
                    source_payload_sha256=source_payload_sha256,
                )
            )
            continue

        if deduplicator.contains(event.trace_id, event.span_id):
            continue

        publisher.publish_span(event)
        deduplicator.mark(event.trace_id, event.span_id)


def run_consumer_loop(
    consumer: confluent_kafka.Consumer,
    publisher: KafkaPublisher,
    deduplicator: SpanDeduplicator,
    shutdown_event: threading.Event,
    poll_timeout_seconds: float = 1.0,
) -> None:
    """
    Drive the consumer loop until shutdown_event is set or an error halts.

    consumer.close() is guaranteed to execute even if publisher.close() raises.
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

            source_topic = msg.topic()
            source_partition = msg.partition()
            source_offset = msg.offset()

            process_message(
                msg,
                publisher,
                deduplicator,
                source_topic,
                source_partition,
                source_offset,
            )

            consumer.commit(message=msg, asynchronous=False)
    finally:
        try:
            publisher.close()
        finally:
            consumer.close()
