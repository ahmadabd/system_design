import os
import logging
from fastapi import FastAPI
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from prometheus_fastapi_instrumentator import Instrumentator

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            log_record['trace_id'] = format(current_span.get_span_context().trace_id, '032x')
            log_record['span_id'] = format(current_span.get_span_context().span_id, '016x')
        log_record['service_name'] = os.getenv("SERVICE_NAME", "merchant-copilot-service")
        log_record['severity'] = record.levelname
        log_record['logger'] = record.name

def setup_logging():
    logHandler = logging.StreamHandler()
    formatter = CustomJsonFormatter('%(timestamp)s %(severity)s %(logger)s %(message)s')
    logHandler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.addHandler(logHandler)
    root_logger.setLevel(logging.INFO)

def setup_copilot_observability(service_name: str = "merchant-copilot-service") -> None:
    """Sets up OpenTelemetry Tracer, HTTPX instrumentation, and JSON Logging for Merchant Copilot"""
    setup_logging()
    logger = logging.getLogger("Observability")
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    
    resource = Resource.create(attributes={
        "service.name": service_name,
        "environment": os.getenv("ENVIRONMENT", "production")
    })
    
    provider = TracerProvider(resource=resource)
    try:
        otlp_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        span_processor = BatchSpanProcessor(otlp_exporter, schedule_delay_millis=1000)
        provider.add_span_processor(span_processor)
        trace.set_tracer_provider(provider)
        HTTPXClientInstrumentor().instrument()
        logger.info("OpenTelemetry TracerProvider and HTTPX instrumentation registered successfully for Merchant Copilot.")
    except Exception as e:
        logger.warning(f"Could not connect to OTel exporter ({e}). Continuing in local trace mode.")

def instrument_app(app: FastAPI):
    """Instruments FastAPI and Prometheus endpoints"""
    try:
        FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")
        Instrumentator().instrument(app).expose(app, endpoint="/metrics")
    except Exception as e:
        logging.getLogger("Observability").warning(f"Instrumentation notice: {e}")
