from prometheus_client import Counter, Histogram, Gauge

graphrag_requests_total = Counter(
    "graphrag_requests_total",
    "Total number of GraphRAG queries processed",
    ["search_mode", "status", "tenant_id"]
)

graphrag_node_duration_seconds = Histogram(
    "graphrag_node_duration_seconds",
    "Time spent executing individual LangGraph GraphRAG nodes",
    ["node_name"]
)

graph_traversal_duration_seconds = Histogram(
    "graph_traversal_duration_seconds",
    "Time spent traversing multi-hop subgraphs in NetworkX",
    ["hops"]
)

graph_entities_total_gauge = Gauge(
    "graph_entities_total",
    "Total number of entities stored in the knowledge graph",
    ["entity_type", "tenant_id"]
)

graph_relations_total_gauge = Gauge(
    "graph_relations_total",
    "Total number of relationships stored in the knowledge graph",
    ["relation_type"]
)

graph_communities_total_gauge = Gauge(
    "graph_communities_total",
    "Total number of detected hierarchical Leiden/Louvain community clusters"
)
