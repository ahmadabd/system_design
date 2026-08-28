import logging
import asyncio
import signal
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.infrastructure.config import settings
from src.infrastructure.observability import setup_graphrag_observability, instrument_app
from src.infrastructure.graph_store import graph_store
from src.infrastructure.default_knowledge import DEFAULT_GRAPH_NODES, DEFAULT_GRAPH_EDGES
from src.adapter.qdrant_entity_adapter import qdrant_entity_adapter
from src.adapter.community_detector import HierarchicalCommunityDetector
from src.adapter.messaging_sub import GraphRAGEventConsumer
from src.presentation.api import router as graphrag_router

logger = logging.getLogger("GraphRAGMain")
community_detector = HierarchicalCommunityDetector(graph_store)
event_consumer = GraphRAGEventConsumer(graph_store, qdrant_entity_adapter)


def _seed_initial_knowledge_graph():
    """Populates graph_store with baseline supplier/component/defect nodes and edges"""
    graph_store.clear()
    for node in DEFAULT_GRAPH_NODES:
        graph_store.add_node(node)
    for edge in DEFAULT_GRAPH_EDGES:
        graph_store.add_edge(edge)
    logger.info(f"Knowledge Graph populated with {len(DEFAULT_GRAPH_NODES)} nodes and {len(DEFAULT_GRAPH_EDGES)} edges.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Setup Observability
    setup_graphrag_observability(settings.SERVICE_NAME)
    logger.info(f"Starting {settings.SERVICE_NAME} on port {settings.PORT} (Environment: {settings.ENVIRONMENT})...")

    # 2. Seed Baseline Knowledge Graph
    _seed_initial_knowledge_graph()

    # 3. Initialize Qdrant Collection & Index Entity Vectors
    qdrant_entity_adapter.init_collection()
    qdrant_entity_adapter.index_entities(DEFAULT_GRAPH_NODES)

    # 4. Precompute Hierarchical Community Clusters
    communities = community_detector.detect_communities()
    logger.info(f"Precomputed {len(communities)} hierarchical community clusters on startup.")

    # 5. Start Kafka Integration Event Ingestion
    await event_consumer.start(settings.KAFKA_BOOTSTRAP_SERVERS)

    yield

    # Graceful Teardown
    logger.info(f"Shutting down {settings.SERVICE_NAME} gracefully...")
    await event_consumer.stop()


app = FastAPI(
    title="Knowledge Graph RAG Service",
    description="Microsoft GraphRAG paradigm: Multi-Hop Relational Traversal, Louvain Community Detection, and Global Map-Reduce",
    version="1.0.0",
    lifespan=lifespan
)


def register_graceful_shutdown(app: FastAPI, cleanup_callbacks: list, drain_seconds: float = 3.0):
    """Registers signal handlers to cooperatively drain traffic and clean up resources"""
    shut_logger = logging.getLogger("ShutdownHandler")

    async def shutdown_handler(sig_num):
        shut_logger.warning(f"Received shutdown signal {signal.Signals(sig_num).name} (SIGTERM/SIGINT). Draining in-flight traffic...")
        shut_logger.info(f"Traffic draining in progress: sleeping for {drain_seconds} seconds...")
        await asyncio.sleep(drain_seconds)

        shut_logger.info("Executing GraphRAG resource cleanups...")
        for callback in cleanup_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                shut_logger.error(f"Error during cleanup callback: {e}", exc_info=True)

        shut_logger.warning("Resource cleanup and traffic draining completed. Terminating process.")

    try:
        loop = asyncio.get_event_loop()
        for sig in [signal.SIGTERM, signal.SIGINT]:
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
            except (ValueError, NotImplementedError):
                pass
    except Exception:
        pass


instrument_app(app)
app.include_router(graphrag_router)

# Register cooperative graceful SIGTERM/SIGINT shutdown with traffic draining
register_graceful_shutdown(
    app,
    [event_consumer.stop]
)
