"""
OpenTelemetry distributed tracing and structured logging setup for Discovery Service.
Connects spans to Jaeger (via OTel Collector :4317) and logs to Loki (:3100).
"""
import logging
from src.infrastructure.config import settings

logger = logging.getLogger("DiscoveryObservability")


def setup_discovery_observability(service_name: str = settings.SERVICE_NAME) -> None:
    """Initializes OpenTelemetry distributed tracing (for Jaeger), structured logging (for Loki), and HTTPX instrumentation."""
    try:
        from shared.common.observability import setup_logging
        setup_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO)

    # Silence noisy exporter connection retry warnings when collector is offline during local unit tests
    for noisy in [
        "opentelemetry.exporter.otlp.proto.grpc.exporter",
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
        "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
        "opentelemetry.sdk.trace.export",
        "opentelemetry.sdk._logs.export"
    ]:
        logging.getLogger(noisy).setLevel(logging.CRITICAL)

    endpoint = settings.OTEL_EXPORTER_OTLP_ENDPOINT

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        resource = Resource.create(attributes={
            "service.name": service_name,
            "environment": settings.ENVIRONMENT
        })

        tracer_provider = TracerProvider(resource=resource)
        otlp_span_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        span_processor = BatchSpanProcessor(
            otlp_span_exporter,
            schedule_delay_millis=500,
            max_export_batch_size=64
        )
        tracer_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(tracer_provider)
        logger.info("OpenTelemetry TracerProvider registered successfully for Jaeger.")
    except Exception as e:
        logger.warning(f"Could not initialize OTLP Span Exporter for Jaeger: {e}")

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        logger.info("HTTPX auto-instrumentation enabled for distributed tracing.")
    except Exception as e:
        logger.debug(f"HTTPX auto-instrumentation skipped: {e}")

    try:
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

        logger_provider = LoggerProvider(resource=resource)
        set_logger_provider(logger_provider)
        log_exporter = OTLPLogExporter(endpoint=endpoint, insecure=True)
        log_processor = BatchLogRecordProcessor(log_exporter)
        logger_provider.add_log_record_processor(log_processor)

        otel_handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
        logging.getLogger().addHandler(otel_handler)
        logger.info("OpenTelemetry LoggerProvider registered successfully for Loki.")
    except Exception as e:
        logger.debug(f"OTLP Log Exporter skipped: {e}")
