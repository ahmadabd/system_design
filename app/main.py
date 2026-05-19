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
import signal
import sys

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
from prometheus_client import Gauge, Counter
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

# --- Prometheus Metrics for Circuit Breakers ---
PROM_CB_STATE = Gauge(
    "circuit_breaker_state",
    "Current state of the circuit breaker (0=CLOSED, 1=HALF-OPEN, 2=OPEN)",
    ["name"]
)
PROM_CB_FAILURES = Counter(
    "circuit_breaker_failures_total",
    "Total failures registered by the circuit breaker",
    ["name"]
)

# --- Observable Circuit Breaker Subclass ---
class ObservableCircuitBreaker(CircuitBreaker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_seen_state = "CLOSED"
        # Set initial Prometheus state
        PROM_CB_STATE.labels(name=self.name).set(0) # 0 = CLOSED
        
    def reset(self):
        super().reset()
        self._check_state_change_to("CLOSED")
        
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type and self.is_failure(exc_type, exc_value):
            PROM_CB_FAILURES.labels(name=self.name).inc()
        res = super().__exit__(exc_type, exc_value, traceback)
        self._check_state_change_to(self.state.upper())
        return res
        
    @property
    def state(self):
        current = super().state
        self._check_state_change_to(current.upper())
        return current
        
    def _check_state_change_to(self, current: str):
        if hasattr(self, 'last_seen_state') and current != self.last_seen_state:
            logger.warning(
                f"[CircuitBreaker-{self.name}] State transition detected: {self.last_seen_state} -> {current}"
            )
            
            # OTel Span Event
            span = trace.get_current_span()
            if span.is_recording():
                span.add_event(
                    "circuit_breaker_state_change",
                    {
                        "cb.name": self.name,
                        "cb.old_state": self.last_seen_state,
                        "cb.new_state": current
                    }
                )
                
            self.last_seen_state = current
            
            # Update Prometheus Gauge
            state_val = 0 if current == "CLOSED" else (1 if current == "HALF_OPEN" else 2)
            PROM_CB_STATE.labels(name=self.name).set(state_val)

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
cb = ObservableCircuitBreaker(name="FlakyServiceBreaker", failure_threshold=3, recovery_timeout=10.0)
db_breaker = ObservableCircuitBreaker(name="PostgresBreaker", failure_threshold=3, recovery_timeout=10.0)
redis_breaker = ObservableCircuitBreaker(name="RedisBreaker", failure_threshold=3, recovery_timeout=10.0)

# --- Graceful Shutdown Setup ---
is_shutting_down = False
original_sigterm_handler = None
original_sigint_handler = None
SHUTDOWN_COOLDOWN = int(os.getenv("SHUTDOWN_COOLDOWN", "10"))

def custom_sigterm_handler(signum, frame):
    global is_shutting_down
    if not is_shutting_down:
        is_shutting_down = True
        logger.warning("Received SIGTERM signal. Starting graceful shutdown sequence...")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(run_cooldown_and_exit(signum))
        except RuntimeError:
            logger.error("No running event loop found during SIGTERM.")
            sys.exit(0)

def custom_sigint_handler(signum, frame):
    global is_shutting_down
    if not is_shutting_down:
        is_shutting_down = True
        logger.warning("Received SIGINT signal. Starting graceful shutdown sequence...")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(run_cooldown_and_exit(signum))
        except RuntimeError:
            logger.error("No running event loop found during SIGINT.")
            sys.exit(0)

async def run_cooldown_and_exit(signum):
    logger.warning(f"Graceful Shutdown: Starting {SHUTDOWN_COOLDOWN}s cooldown phase. Health checks will now fail.")
    
    # Wait for the cooldown to allow the load balancer (Traefik) to detect failure and update routing
    await asyncio.sleep(SHUTDOWN_COOLDOWN)
    
    logger.warning("Graceful Shutdown: Cooldown finished. Restoring original handlers and propagating signal to Uvicorn...")
    
    # Restore original handlers so standard Uvicorn shutdown runs
    if signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        if original_sigterm_handler and callable(original_sigterm_handler):
            original_sigterm_handler(signum, None)
        else:
            sys.exit(0)
    elif signum == signal.SIGINT:
        signal.signal(signal.SIGINT, original_sigint_handler)
        if original_sigint_handler and callable(original_sigint_handler):
            original_sigint_handler(signum, None)
        else:
            raise KeyboardInterrupt()

def setup_graceful_shutdown():
    global original_sigterm_handler, original_sigint_handler
    original_sigterm_handler = signal.signal(signal.SIGTERM, custom_sigterm_handler)
    original_sigint_handler = signal.signal(signal.SIGINT, custom_sigint_handler)
    logger.info("Graceful shutdown signal handlers registered successfully.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_redis()
    
    # Register graceful shutdown handlers to intercept signals after Uvicorn starts
    setup_graceful_shutdown()
    
    logger.info("Application started")
    yield
    logger.info("Application shutting down")
    if db_pool:
        db_pool.closeall()
    processor.shutdown()
    logger_provider.shutdown()

app = FastAPI(lifespan=lifespan)

# (Middleware removed - transitioned to clean function decorators)

# --- Prometheus Metrics ---
Instrumentator().instrument(app).expose(app)

@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"message": "Hello", "instance_port": os.getenv("PORT", "unknown")}

@app.get("/health")
def health():
    if is_shutting_down:
        return JSONResponse(
            status_code=503,
            content={"status": "shutting_down", "message": "Service is shutting down, draining active traffic"}
        )
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
@db_breaker
def execute_db_query():
    conn = db_pool.getconn()
    try:
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
        return count
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise e
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)

@app.get("/db-demo")
def db_demo():
    logger.info("DB demo endpoint called")
    
    if not db_pool:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Database connection pool not available")
    
    try:
        count = execute_db_query()
        return {
            "message": "Successfully recorded request in PostgreSQL!",
            "total_requests_recorded": count,
            "instance_port": os.getenv("PORT", "unknown")
        }
    except CircuitBreakerOpenException as cbe:
        logger.warning(f"PostgresBreaker is OPEN! Returning local fallback response. Error: {cbe}")
        return {
            "status": "db_breaker_open_fallback",
            "message": "Database is currently down. Returning locally simulated fallback stats.",
            "total_requests_recorded": 9999,
            "instance_port": f"{os.getenv('PORT', 'unknown')} (FALLBACK)"
        }
    except Exception as e:
        logger.error(f"DB operation failed: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


# --- New Redis Demo Endpoint ---
@redis_breaker
def get_or_set_cache(key: str):
    # Check cache
    cached_val = redis_client.get(key)
    if cached_val:
        logger.info(f"Cache HIT for key '{key}'")
        return cached_val, "cache"
        
    logger.info(f"Cache MISS for key '{key}'. Performing simulated expensive computation...")
    # Simulate an expensive operation
    time.sleep(1.0)
    computed_val = f"computed_at_{time.time()}_on_port_{os.getenv('PORT', 'unknown')}"
    
    # Cache the result with a 10 seconds TTL
    redis_client.setex(key, 10, computed_val)
    return computed_val, "simulated_expensive_computation"

@app.get("/cache-demo")
def cache_demo(key: str = "demo_key"):
    logger.info(f"Cache demo endpoint called for key: {key}")
    
    if not redis_client:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Redis client not available")
        
    try:
        val, source = get_or_set_cache(key)
        return {
            "source": source,
            "key": key,
            "value": val.decode("utf-8") if isinstance(val, bytes) else val,
            "instance_port": os.getenv("PORT", "unknown")
        }
    except CircuitBreakerOpenException as cbe:
        logger.warning(f"RedisBreaker is OPEN! Bypassing cache directly to simulated fallback calculation. Error: {cbe}")
        computed_val = f"computed_at_{time.time()}_on_port_{os.getenv('PORT', 'unknown')} (BYPASS)"
        return {
            "status": "redis_breaker_open_fallback",
            "message": "Redis cache is currently down. Bypassing cache to perform local calculation directly.",
            "source": "simulated_expensive_computation_bypass",
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
@cb
async def call_flaky_service(fail: bool):
    async with httpx.AsyncClient() as client:
        # Note: calling our simulated internal flaky service
        url = f"http://127.0.0.1:8000/flaky-service?fail={str(fail).lower()}"
        response = await client.get(url, timeout=2.0)
        if response.status_code >= 500:
            raise Exception(f"Service returned unhealthy status code: {response.status_code}")
        return response.json()

@app.get("/circuit-breaker-demo")
async def circuit_breaker_demo(fail: bool = False):
    logger.info("Circuit breaker demo called")
    try:
        data = await call_flaky_service(fail)
        return {
            "status": "success",
            "circuit_breaker_state": cb.state.upper(),
            "data": data
        }
    except CircuitBreakerOpenException as cbe:
        logger.warning(f"Circuit breaker is OPEN! Returning cached fallback value. Error: {cbe}")
        fallback_val = "FALLBACK_VALUE_LOCAL (Service Unhealthy)"
        if redis_client:
            try:
                cached = redis_client.get("last_flaky_success")
                if cached:
                    fallback_val = f"FALLBACK_VALUE_FROM_REDIS ({cached})"
            except Exception as re:
                logger.error(f"Redis fallback read failed: {re}")
                
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
        logger.error(f"Request failed: {str(e)}. Failure count: {cb._failure_count}")
        return JSONResponse(
            status_code=502,
            content={
                "detail": f"External call failed (Breaker State: {cb.state.upper()}). Error: {str(e)}"
            }
        )

# --- Circuit Breaker Status Endpoint ---
@app.get("/circuit-breaker-status")
def get_cb_status():
    breakers = [cb, db_breaker, redis_breaker]
    status_list = []
    
    for b in breakers:
        seconds_since_open = 0.0
        if b.opened:
            seconds_since_open = time.monotonic() - b._opened
            
        status_list.append({
            "name": b.name,
            "state": b.state.upper(),
            "failure_count": b._failure_count,
            "failure_threshold": b._failure_threshold,
            "recovery_timeout": b._recovery_timeout,
            "seconds_since_state_change": seconds_since_open
        })
        
    return {
        "instance_port": os.getenv("PORT", "unknown"),
        "breakers": status_list
    }

FastAPIInstrumentor.instrument_app(app)