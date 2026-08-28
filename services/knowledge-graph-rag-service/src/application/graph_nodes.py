import time
import logging
from opentelemetry import trace

from src.domain.graph_state import GraphRAGState
from src.infrastructure.graph_store import graph_store
from src.adapter.qdrant_entity_adapter import qdrant_entity_adapter
from src.adapter.community_detector import HierarchicalCommunityDetector
from src.adapter.entity_extractor import EntityExtractor
from src.adapter.llm_adapter import graphrag_llm_adapter
from src.infrastructure.metrics import (
    graphrag_node_duration_seconds,
    graph_traversal_duration_seconds
)

logger = logging.getLogger("GraphRAGNodes")
tracer = trace.get_tracer("knowledge-graph-rag-service")

community_detector = HierarchicalCommunityDetector(graph_store)
entity_extractor = EntityExtractor(graph_store)


def graph_query_classifier_node(state: GraphRAGState) -> dict:
    """Classifies search mode: 'local_multihop' or 'global_community'"""
    with tracer.start_as_current_span("LangGraph node: graph_query_classifier") as span:
        start = time.perf_counter()
        query = state["query"]
        requested_mode = state.get("search_mode", "auto")

        if requested_mode and requested_mode in ["local_multihop", "global_community"]:
            mode = requested_mode
        else:
            mode = entity_extractor.classify_search_mode(query)

        span.set_attribute("graphrag.classified_mode", mode)
        span.set_attribute("graphrag.query", query)

        duration = time.perf_counter() - start
        graphrag_node_duration_seconds.labels(node_name="graph_query_classifier").observe(duration)
        logger.info(f"[Node: graph_query_classifier] Query: '{query}' -> Mode: '{mode}' ({duration:.3f}s)")
        return {"search_mode": mode}


def entity_extractor_node(state: GraphRAGState) -> dict:
    """Extracts seed entity IDs using keyword heuristics and Qdrant semantic similarity"""
    with tracer.start_as_current_span("LangGraph node: entity_extractor") as span:
        start = time.perf_counter()
        query = state["query"]
        tenant_id = state.get("tenant_id", "store_tech")

        # 1. Exact / keyword heuristic matching
        heuristic_seeds = entity_extractor.extract_seed_entities(query)

        # 2. Semantic vector search fallback if heuristics did not yield seeds
        vector_entities = []
        if not heuristic_seeds:
            vector_entities = qdrant_entity_adapter.search_entities(query, limit=2, tenant_id=tenant_id)
            vector_seeds = [e.get("node_id") for e in vector_entities if e.get("node_id")]
            combined_seeds = vector_seeds
        else:
            combined_seeds = heuristic_seeds

        # Fallback to default product if nothing matched
        if not combined_seeds:
            for nid in graph_store.graph.nodes():
                combined_seeds.append(nid)
                if len(combined_seeds) >= 1:
                    break

        span.set_attribute("graphrag.extracted_seed_count", len(combined_seeds))
        span.set_attribute("graphrag.seed_entities", ",".join(combined_seeds))

        duration = time.perf_counter() - start
        graphrag_node_duration_seconds.labels(node_name="entity_extractor").observe(duration)
        logger.info(f"[Node: entity_extractor] Discovered seed entities: {combined_seeds} ({duration:.3f}s)")
        return {
            "extracted_seed_entities": combined_seeds,
            "matched_graph_nodes": vector_entities
        }


def local_subgraph_traverser_node(state: GraphRAGState) -> dict:
    """Traverses multi-hop entity neighborhoods and constructs causal relational chains"""
    with tracer.start_as_current_span("LangGraph node: local_subgraph_traverser") as span:
        start = time.perf_counter()
        seeds = state.get("extracted_seed_entities", [])
        
        # 1. Extract 2-hop neighborhood subgraph
        sub_nodes, sub_edges = graph_store.extract_subgraph(seed_node_ids=seeds, max_hops=2)

        # 2. Build multi-hop reasoning hops without duplicates
        seen_hops = set()
        reasoning_hops = []
        for e in sub_edges:
            src_name = graph_store.get_node(e["source"]).get("name", e["source"]) if graph_store.get_node(e["source"]) else e["source"]
            tgt_name = graph_store.get_node(e["target"]).get("name", e["target"]) if graph_store.get_node(e["target"]) else e["target"]
            rel = e.get("relation", "RELATED_TO")
            desc = e.get("description", "")
            desc_text = f" ({desc})" if desc else ""
            hop_str = f"**[{src_name}]** ──`{rel}`──► **[{tgt_name}]**{desc_text}"
            if hop_str not in seen_hops:
                seen_hops.add(hop_str)
                reasoning_hops.append(hop_str)

        # Render Mermaid diagram
        mermaid_code = graph_store.to_mermaid(sub_nodes, sub_edges)

        span.set_attribute("graphrag.subgraph_nodes_count", len(sub_nodes))
        span.set_attribute("graphrag.subgraph_edges_count", len(sub_edges))

        duration = time.perf_counter() - start
        graphrag_node_duration_seconds.labels(node_name="local_subgraph_traverser").observe(duration)
        graph_traversal_duration_seconds.labels(hops="2").observe(duration)
        logger.info(f"[Node: local_subgraph_traverser] Extracted {len(sub_nodes)} nodes, {len(sub_edges)} edges ({duration:.3f}s)")

        return {
            "subgraph_nodes": sub_nodes,
            "subgraph_edges": sub_edges,
            "reasoning_hops": reasoning_hops,
            "mermaid_subgraph": mermaid_code
        }


def global_community_reducer_node(state: GraphRAGState) -> dict:
    """Dispatches Map-Reduce summarization over all detected Leiden/Louvain community clusters"""
    with tracer.start_as_current_span("LangGraph node: global_community_reducer") as span:
        start = time.perf_counter()
        query = state["query"]

        # 1. Retrieve or detect community clusters
        clusters = community_detector.get_cached_communities()
        span.set_attribute("graphrag.communities_count", len(clusters))

        # 2. Map Phase: Extract relevance insight per cluster
        map_insights = []
        for c in clusters:
            map_insights.append({
                "community_id": c.id,
                "title": c.title,
                "severity": c.severity_rating,
                "summary": c.summary,
                "key_findings": c.key_findings
            })

        duration = time.perf_counter() - start
        graphrag_node_duration_seconds.labels(node_name="global_community_reducer").observe(duration)
        logger.info(f"[Node: global_community_reducer] Processed Map-Reduce across {len(clusters)} communities ({duration:.3f}s)")

        return {
            "community_clusters": [c.model_dump() for c in clusters],
            "intermediate_map_insights": map_insights
        }


def graph_reasoning_synthesizer_node(state: GraphRAGState) -> dict:
    """Synthesizes the final executive investigation report with structured Markdown and visuals"""
    with tracer.start_as_current_span("LangGraph node: graph_reasoning_synthesizer") as span:
        start = time.perf_counter()
        query = state["query"]
        mode = state.get("search_mode", "local_multihop")

        if mode == "global_community":
            clusters = [c for c in community_detector.get_cached_communities()]
            insights = state.get("intermediate_map_insights", [])
            report = graphrag_llm_adapter.synthesize_global_community_report(query, clusters, insights)
        else:
            sub_nodes = state.get("subgraph_nodes", [])
            sub_edges = state.get("subgraph_edges", [])
            hops = state.get("reasoning_hops", [])
            seed_nodes = [graph_store.get_node(sid) for sid in state.get("extracted_seed_entities", []) if graph_store.get_node(sid)]
            report = graphrag_llm_adapter.synthesize_local_multihop_report(
                query=query,
                seed_nodes=seed_nodes,
                subgraph_nodes=sub_nodes,
                subgraph_edges=sub_edges,
                reasoning_hops=hops
            )

        duration = time.perf_counter() - start
        graphrag_node_duration_seconds.labels(node_name="graph_reasoning_synthesizer").observe(duration)
        logger.info(f"[Node: graph_reasoning_synthesizer] Synthesized report ({len(report)} chars) in {duration:.3f}s")

        return {"final_markdown_report": report}
