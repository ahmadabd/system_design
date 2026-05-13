from fastapi import FastAPI
import os
import sqlite3
import httpx
import logging
from contextlib import asynccontextmanager
from pythonjsonlogger import jsonlogger
import random

from opentelemetry import trace, _logs
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
from opentelemetry.sdk.resources import Resource

from prometheus_fastapi_instrumentator import Instrumentator

# --- OpenTelemetry & Logging Setup ---
OTEL_COLLECTOR_ENDPOINT = "otel-collector:4317"

resource = Resource.create({
    "service.name": "fastapi-loadbalancer-demo",
    "instance.id": os.getenv("PORT", "unknown")
})

# Tracing
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Logging (OTel)
logger_provider = LoggerProvider(resource=resource)
_logs.set_logger_provider(logger_provider)
log_exporter = OTLPLogExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

# Structured Logging (Local JSON for Loki/Promtail)
class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            log_record['trace_id'] = format(span_context.trace_id, '032x')
            log_record['span_id'] = format(span_context.span_id, '016x')
        log_record['instance_port'] = os.getenv('PORT', 'unknown')

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 1. OTel Log Handler
root_logger.addHandler(LoggingHandler(logger_provider=logger_provider))

# 2. JSON Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(CustomJsonFormatter('%(timestamp)s %(levelname)s %(name)s %(message)s'))
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# Instrument libraries
HTTPXClientInstrumentor().instrument()
SQLite3Instrumentor().instrument()

# --- Database Setup ---
DB_PATH = "test.db"
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, url TEXT, status_code INTEGER)")
    conn.commit()
    conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("Application started")
    yield
    logger.info("Application shutting down")
    processor.shutdown()
    logger_provider.shutdown()

app = FastAPI(lifespan=lifespan)

# --- Prometheus Metrics ---
Instrumentator().instrument(app).expose(app)

@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"message": "Hello", "instance_port": os.getenv("PORT", "unknown")}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/trace-demo")
async def trace_demo():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("Process Trace Demo") as span:
        logger.info("Starting trace-demo request")
        span.set_attribute("api.endpoint", "/trace-demo")
        
        logger.info("trace-demo request completed successfully")
        return {
            "message": "Request completed and logged",
            "code": random.randint(1, 1000)
        }

FastAPIInstrumentor.instrument_app(app)