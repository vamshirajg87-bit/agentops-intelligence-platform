"""
observability/tracing.py

OpenTelemetry tracing foundation for the AgentOps demo-app.
"""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

from observability.canonical_exporter import CanonicalSpanExporter


SERVICE_NAME = "agentops-demo-app"

_tracing_configured = False


def configure_tracing() -> None:
    global _tracing_configured

    if _tracing_configured:
        return

    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
        }
    )

    provider = TracerProvider(resource=resource)

    # Development console output.
    provider.add_span_processor(
        SimpleSpanProcessor(ConsoleSpanExporter())
    )

    # Local canonical Kafka development path.
    provider.add_span_processor(
        SimpleSpanProcessor(CanonicalSpanExporter())
    )

    # Production-style OTLP path to OpenTelemetry Collector.
    provider.add_span_processor(
        SimpleSpanProcessor(
            OTLPSpanExporter(
                endpoint="http://localhost:4317",
                insecure=True,
            )
        )
    )

    trace.set_tracer_provider(provider)

    _tracing_configured = True


def get_tracer(name: str):
    return trace.get_tracer(name)