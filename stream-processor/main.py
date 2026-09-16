"""
stream-processor/main.py

Runtime entry point for the agentops stream processor.

Constructs Consumer, Producer, KafkaPublisher, SpanDeduplicator, and
threading.Event, then delegates entirely to run_consumer_loop().

Contains no processing logic — all business logic lives in processor.py.

Usage:
    PYTHONPATH=/path/to/repo/demo-app python stream-processor/main.py

    # Windows PowerShell:
    $env:PYTHONPATH = "$PWD\\demo-app"
    python stream-processor/main.py
"""

from __future__ import annotations

import sys
import threading

import confluent_kafka

import config
from deduplicator import SpanDeduplicator
from processor import run_consumer_loop
from publisher import KafkaPublisher


def main() -> None:
    print("agentops stream processor starting")
    print(f"  bootstrap : {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  group     : {config.CONSUMER_GROUP_ID}")
    print(f"  input     : {config.TOPIC_OTLP_TRACES}")
    print(f"  trusted   : {config.TOPIC_SPANS}")
    print(f"  dlq       : {config.TOPIC_DLQ}")
    print(f"  dedup size: {config.DEDUP_MAX_SIZE}")

    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP_ID,
        "auto.offset.reset": config.CONSUMER_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC_OTLP_TRACES])
    print(f"Subscribed to {config.TOPIC_OTLP_TRACES!r}. Press Ctrl+C to stop.")

    producer = confluent_kafka.Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    publisher = KafkaPublisher(
        producer=producer,
        retries=config.PUBLISH_RETRIES,
        timeout_seconds=config.PUBLISH_TIMEOUT_SECONDS,
    )

    deduplicator = SpanDeduplicator(max_size=config.DEDUP_MAX_SIZE)
    shutdown_event = threading.Event()

    try:
        run_consumer_loop(
            consumer=consumer,
            publisher=publisher,
            deduplicator=deduplicator,
            shutdown_event=shutdown_event,
        )
    except KeyboardInterrupt:
        # run_consumer_loop's finally block already closed publisher and consumer.
        print("\nagentops stream processor stopped.")
        sys.exit(0)
    except Exception as exc:
        print(f"\nagentops stream processor halted: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
