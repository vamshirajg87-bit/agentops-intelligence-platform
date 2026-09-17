"""
storage-consumer/main.py

Runtime entry point for the agentops storage consumer.

Constructs the Kafka Consumer, psycopg Connection, SpanRepository, and
threading.Event, then delegates entirely to run_consumer_loop().

Contains no processing logic — all business logic lives in consumer.py
and repository.py.

Usage:
    PYTHONPATH=/path/to/repo/demo-app python storage-consumer/main.py

    # Windows PowerShell:
    $env:PYTHONPATH = "$PWD\\demo-app"
    python storage-consumer/main.py
"""

from __future__ import annotations

import sys
import threading

import confluent_kafka
import psycopg

import config
from consumer import run_consumer_loop
from repository import SpanRepository


def main() -> None:
    print("agentops storage consumer starting")
    print(f"  bootstrap  : {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  group      : {config.CONSUMER_GROUP_ID}")
    print(f"  topic      : {config.TOPIC_SPANS}")
    print(f"  pg host    : {config.PG_HOST}:{config.PG_PORT}")
    print(f"  pg database: {config.PG_DATABASE}")

    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP_ID,
        "auto.offset.reset": config.CONSUMER_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC_SPANS])
    print(f"Subscribed to {config.TOPIC_SPANS!r}. Press Ctrl+C to stop.")

    connection = psycopg.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DATABASE,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        autocommit=True,
    )
    repository = SpanRepository(connection)
    shutdown_event = threading.Event()

    try:
        run_consumer_loop(
            consumer=consumer,
            repository=repository,
            shutdown_event=shutdown_event,
        )
    except KeyboardInterrupt:
        print("\nagentops storage consumer stopped.")
        sys.exit(0)
    except Exception as exc:
        print(f"\nagentops storage consumer halted: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
