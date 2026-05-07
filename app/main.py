from fastapi import FastAPI
import os
import sqlite3
import httpx
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
from opentelemetry.sdk.resources import Resource

# Setup OpenTelemetry
resource = Resource.create({
    "service.name": "fastapi-loadbalancer-demo",
    "instance.id": os.getenv("PORT", "unknown")
})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Instrument libraries
HTTPXClientInstrumentor().instrument()
SQLite3Instrumentor().instrument()

# Database Setup
DB_PATH = "test.db"
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, url TEXT, status_code INTEGER)")
    conn.commit()
    conn.close()

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    init_db()
    yield
    # Shutdown logic
    print("Graceful shutdown: Flushing OpenTelemetry spans...")
    processor.shutdown()
    print("Shutdown complete.")

app = FastAPI(lifespan=lifespan)

@app.get("/")
def root():
    return {"message": "Hello", "instance_port": os.getenv("PORT", "unknown")}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/trace-demo")
async def trace_demo():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("Process Trace Demo") as span:
        span.set_attribute("api.endpoint", "/trace-demo")
        span.set_attribute("instance.port", os.getenv("PORT", "unknown"))
        
        url = "https://www.google.com"
        span.set_attribute("target.url", url)
        
        # External Request
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            span.set_attribute("http.status_code", response.status_code)
        
        # SQLite Write
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO requests (url, status_code) VALUES (?, ?)", (url, response.status_code))
        conn.commit()
        conn.close()
        
        return {
            "message": "Request completed and logged",
            "url": url,
            "status_code": response.status_code,
            "instance": os.getenv("PORT", "unknown")
        }

FastAPIInstrumentor.instrument_app(app)