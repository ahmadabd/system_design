import os
import logging
import signal
import asyncio
from fastapi import FastAPI
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        # Inject trace context into JSON log logs for Loki matching
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            log_record['trace_id'] = format(current_span.get_span_context().trace_id, '032x')
            log_record['span_id'] = format(current_span.get_span_context().span_id, '016x')
        log_record['service_name'] = os.getenv("SERVICE_NAME", "unknown-service")
        log_record['severity'] = record.levelname
        log_record['logger'] = record.name

def setup_logging():
    """Converts default python logging into structured JSON logging for Loki ingestion"""
    logHandler = logging.StreamHandler()
    formatter = CustomJsonFormatter('%(timestamp)s %(severity)s %(logger)s %(message)s')
    logHandler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.addHandler(logHandler)
    root_logger.setLevel(logging.INFO)
    
    # Ensure uvicorn logs also use this formatter
    for uvicorn_logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        ul = logging.getLogger(uvicorn_logger_name)
        ul.handlers = []
        ul.addHandler(logHandler)
        ul.propagate = False

def setup_observability(app: FastAPI, service_name: str) -> None:
    """Initializes standard OpenTelemetry, Prometheus metrics, and JSON logging"""
    # 1. Initialize structured logging
    setup_logging()
    
    logger = logging.getLogger("Observability")
    
    # 2. Setup OpenTelemetry Tracer and Logger
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    logger.info(f"Connecting OTel Tracer & Logger to exporter endpoint: {endpoint}")
    
    resource = Resource.create(attributes={
        "service.name": service_name,
        "environment": os.getenv("ENVIRONMENT", "production")
    })
    
    # Tracer initialization
    provider = TracerProvider(resource=resource)
    try:
        otlp_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        span_processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(span_processor)
        trace.set_tracer_provider(provider)
        
        # Instrument FastAPI app
        FastAPIInstrumentor.instrument_app(app)
        logger.info("OpenTelemetry FastAPI auto-instrumentation successful.")
    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry Tracer: {e}", exc_info=True)

    # Logger initialization
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)
    try:
        log_exporter = OTLPLogExporter(endpoint=endpoint, insecure=True)
        log_processor = BatchLogRecordProcessor(log_exporter)
        logger_provider.add_log_record_processor(log_processor)
        
        # Integrate with standard Python logging (capture all INFO and above logs)
        otel_handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
        logging.getLogger().addHandler(otel_handler)
        logger.info("OpenTelemetry OTLP Logging initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry Logging: {e}", exc_info=True)
        
    # 3. Setup Prometheus FastAPI Instrumentator
    try:
        Instrumentator().instrument(app).expose(app, endpoint="/metrics")
        logger.info("Prometheus metrics instrumented and exposed at /metrics")
    except Exception as e:
        logger.error(f"Failed to initialize Prometheus metrics: {e}", exc_info=True)

def register_graceful_shutdown(app: FastAPI, cleanup_callbacks: list):
    """Registers signal handlers to cooperatively drain traffic and clean up resources"""
    logger = logging.getLogger("ShutdownHandler")
    
    async def shutdown_handler(sig_num):
        logger.warning(f"Received shutdown signal {signal.Signals(sig_num).name} (SIGTERM/SIGINT). Draining traffic...")
        # 1. Wait a brief grace period to allow in-flight connections to complete
        logger.info("Traffic draining in progress: sleeping for 3 seconds...")
        await asyncio.sleep(3.0)
        
        # 2. Call all cleanup callbacks
        logger.info("Executing microservice resource cleanups...")
        for callback in cleanup_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                logger.error(f"Error during cleanup callback: {e}", exc_info=True)
                
        logger.warning("Resource cleanup and traffic draining completed. Terminating process.")
    
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGTERM, signal.SIGINT]:
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
        except ValueError:
            # Under some environments/windows, add_signal_handler is not fully supported
            pass
