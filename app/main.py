from fastapi import FastAPI
import os
import httpx
import logging
from contextlib import asynccontextmanager
from pythonjsonlogger import jsonlogger
import random
import time
import asyncio
import psycopg2
from psycopg2 import pool
import redis

from opentelemetry import trace, _logs
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import Resource

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from circuitbreaker import CircuitBreaker, CircuitBreakerError as CircuitBreakerOpenException

# --- OpenTelemetry & Logging Setup ---
# Using the service.name and instance.id to distinguish instances in Grafana/Jaeger
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
Psycopg2Instrumentor().instrument()
RedisInstrumentor().instrument()

# --- Database Setup ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/system_design")
db_pool = None

def init_db():
    global db_pool
    logger.info("Initializing database connection pool...")
    retries = 10
    while retries > 0:
        try:
            # Connect to PostgreSQL to verify connection and create table
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_requests (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    endpoint VARCHAR(255),
                    instance_port VARCHAR(50),
                    status_code INTEGER
                )
            """)
            conn.commit()
            cursor.close()
            conn.close()
            
            # Create ThreadedConnectionPool for FastAPI endpoints
            db_pool = pool.ThreadedConnectionPool(1, 20, DATABASE_URL)
            logger.info("Database connection pool initialized successfully!")
            break
        except Exception as e:
            logger.warning(f"Database connection failed: {e}. Retrying in 2 seconds...")
            retries -= 1
            time.sleep(2)
    if not db_pool:
        logger.error("Failed to initialize database connection pool after retries.")

# --- Redis Setup ---
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = None

def init_redis():
    global redis_client
    logger.info("Initializing Redis client...")
    retries = 10
    while retries > 0:
        try:
            redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            redis_client.ping()
            logger.info("Redis initialized successfully!")
            break
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Retrying in 2 seconds...")
            retries -= 1
            time.sleep(2)
    if not redis_client:
        logger.error("Failed to initialize Redis client after retries.")

# --- Circuit Breaker Setup ---
# Instantiate a circuit breaker to protect simulated unreliable external calls
# Failure threshold is 3 consecutive failures, recovery timeout is 10.0 seconds.
cb = CircuitBreaker(name="FlakyServiceBreaker", failure_threshold=3, recovery_timeout=10.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_redis()
    logger.info("Application started")
    yield
    logger.info("Application shutting down")
    if db_pool:
        db_pool.closeall()
    processor.shutdown()
    logger_provider.shutdown()

app = FastAPI(lifespan=lifespan)

class CircuitBreakerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/circuit-breaker-demo":
            try:
                # If the circuit breaker is OPEN, short-circuit and raise the open exception
                if cb.opened:
                    raise CircuitBreakerOpenException(cb)

                # Use the context manager of the standard circuitbreaker library.
                # Awaiting inside the 'with cb' block ensures async exceptions are caught.
                with cb:
                    response = await call_next(request)
                    if response.status_code >= 500:
                        raise Exception("Service returned unhealthy status code")
                return response
            except CircuitBreakerOpenException as cbe:
                logger.warning("Circuit breaker is OPEN via Middleware! Returning cached fallback value.")
                fallback_val = "FALLBACK_VALUE_LOCAL (Service Unhealthy via Middleware)"
                if redis_client:
                    try:
                        cached = redis_client.get("last_flaky_success")
                        if cached:
                            fallback_val = f"FALLBACK_VALUE_FROM_REDIS ({cached}) (via Middleware)"
                    except Exception as re:
                        logger.error(f"Redis fallback read failed in Middleware: {re}")
                        
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "circuit_breaker_open_fallback",
                        "circuit_breaker_state": cb.state.upper(),
                        "message": str(cbe),
                        "fallback_data": fallback_val
                    }
                )
            except Exception as e:
                logger.error(f"Request failed in Middleware: {str(e)}. Failure count: {cb._failure_count}")
                return JSONResponse(
                    status_code=502,
                    content={
                        "detail": f"External call failed (Breaker State: {cb.state.upper()}). Error: {str(e)}"
                    }
                )
        else:
            return await call_next(request)

app.add_middleware(CircuitBreakerMiddleware)

# --- Prometheus Metrics ---
Instrumentator().instrument(app).expose(app)

@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"message": "Hello", "instance_port": os.getenv("PORT", "unknown")}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/slow")
async def slow():
    await asyncio.sleep(3)
    return {"message": "Finally finished!"}

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

# --- New PostgreSQL Demo Endpoint ---
@app.get("/db-demo")
def db_demo():
    logger.info("DB demo endpoint called")
    if not db_pool:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Database connection pool not available")
    
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Insert a request record
        cursor.execute(
            "INSERT INTO system_requests (endpoint, instance_port, status_code) VALUES (%s, %s, %s)",
            ("/db-demo", os.getenv("PORT", "unknown"), 200)
        )
        conn.commit()
        
        # Retrieve the count of records
        cursor.execute("SELECT COUNT(*) FROM system_requests")
        count = cursor.fetchone()[0]
        cursor.close()
        
        return {
            "message": "Successfully recorded request in PostgreSQL!",
            "total_requests_recorded": count,
            "instance_port": os.getenv("PORT", "unknown")
        }
    except Exception as e:
        logger.error(f"DB operation failed: {e}")
        if conn:
            conn.rollback()
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)

# --- New Redis Demo Endpoint ---
@app.get("/cache-demo")
def cache_demo(key: str = "demo_key"):
    logger.info(f"Cache demo endpoint called for key: {key}")
    if not redis_client:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Redis client not available")
        
    try:
        # Check cache
        cached_val = redis_client.get(key)
        if cached_val:
            logger.info(f"Cache HIT for key '{key}'")
            return {
                "source": "cache",
                "key": key,
                "value": cached_val,
                "instance_port": os.getenv("PORT", "unknown")
            }
            
        logger.info(f"Cache MISS for key '{key}'. Performing simulated expensive computation...")
        # Simulate an expensive operation (e.g. database read or third-party call)
        time.sleep(1.0)
        computed_val = f"computed_at_{time.time()}_on_port_{os.getenv('PORT', 'unknown')}"
        
        # Cache the result with a 10 seconds TTL
        redis_client.setex(key, 10, computed_val)
        
        return {
            "source": "simulated_expensive_computation",
            "key": key,
            "value": computed_val,
            "instance_port": os.getenv("PORT", "unknown")
        }
    except Exception as e:
        logger.error(f"Redis operation failed: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Redis operation failed: {str(e)}")

# --- Flaky Service (Simulating external microservice) ---
@app.get("/flaky-service")
def flaky_service(fail: bool = False):
    logger.info("Flaky service endpoint called")
    if fail or random.random() < 0.7:
        logger.warning("Flaky service is failing! Returning HTTP 500.")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail="Flaky service failure!")
        
    # On success, update a Redis key to act as a fallback value for circuit breaker
    if redis_client:
        try:
            success_time = time.strftime("%Y-%m-%d %H:%M:%S")
            redis_client.set("last_flaky_success", f"Success at {success_time} on port {os.getenv('PORT')}")
        except Exception as e:
            logger.warning(f"Failed to cache success timestamp in Redis: {e}")
            
    return {"status": "success", "message": "Flaky service call succeeded!"}

# --- Circuit Breaker Demo Endpoint ---
@app.get("/circuit-breaker-demo")
async def circuit_breaker_demo(fail: bool = False):
    logger.info("Circuit breaker demo called")
    async with httpx.AsyncClient() as client:
        url = f"http://127.0.0.1:8000/flaky-service?fail={str(fail).lower()}"
        response = await client.get(url, timeout=2.0)
        if response.status_code >= 500:
            raise Exception(f"Service returned unhealthy status code: {response.status_code}")
        return {
            "status": "success",
            "circuit_breaker_state": cb.state.upper(),
            "data": response.json()
        }

# --- Circuit Breaker Status Endpoint ---
@app.get("/circuit-breaker-status")
def get_cb_status():
    seconds_since_open = 0.0
    if cb.opened:
        seconds_since_open = time.monotonic() - cb._opened
        
    return {
        "name": cb.name,
        "state": cb.state.upper(),
        "failure_count": cb._failure_count,
        "failure_threshold": cb._failure_threshold,
        "recovery_timeout": cb._recovery_timeout,
        "seconds_since_state_change": seconds_since_open,
        "instance_port": os.getenv("PORT", "unknown")
    }

FastAPIInstrumentor.instrument_app(app)