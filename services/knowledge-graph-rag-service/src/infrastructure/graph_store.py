import json
import logging
import os
from typing import List, Dict, Any, Optional, Set, Tuple
import networkx as nx
from opentelemetry import trace

from src.domain.graph_entities import GraphNode, GraphEdge, EntityType, RelationType
from src.infrastructure.metrics import (
    graph_entities_total_gauge,
    graph_relations_total_gauge,
    graph_traversal_duration_seconds
)

logger = logging.getLogger("GraphStore")
tracer = trace.get_tracer("knowledge-graph-rag-service")


class KnowledgeGraphStore:
    """
    In-memory NetworkX MultiDiGraph manager supporting entity resolution,
    k-hop neighborhood extractions, multi-hop pathfinding, and JSON persistence.
    """
    def __init__(self, persistence_path: Optional[str] = None):
        self.graph = nx.MultiDiGraph()
        self.persistence_path = persistence_path
        self._node_types: Dict[str, EntityType] = {}

    def clear(self) -> None:
        """Clears all nodes and edges from the in-memory graph"""
        self.graph.clear()
        self._node_types.clear()
        self._update_metrics()

    def add_node(self, node: GraphNode) -> None:
        """Adds or updates an entity node in the graph"""
        self.graph.add_node(
            node.id,
            name=node.name,
            type=node.type.value if isinstance(node.type, EntityType) else str(node.type),
            description=node.description,
            properties=node.properties,
            tenant_id=node.tenant_id
        )
        self._node_types[node.id] = node.type
        self._update_metrics()

    def add_edge(self, edge: GraphEdge) -> None:
        """Adds a directed typed relationship between two entities without duplicates"""
        # Ensure source and target nodes exist
        if not self.graph.has_node(edge.source):
            self.add_node(GraphNode(id=edge.source, name=edge.source, type=EntityType.PRODUCT))
        if not self.graph.has_node(edge.target):
            self.add_node(GraphNode(id=edge.target, name=edge.target, type=EntityType.COMPONENT))

        rel_val = edge.relation.value if isinstance(edge.relation, RelationType) else str(edge.relation)

        # Prevent duplicate parallel edges with the same relation
        if self.graph.has_edge(edge.source, edge.target):
            existing_edges = self.graph.get_edge_data(edge.source, edge.target)
            if any(e.get("relation") == rel_val for e in existing_edges.values()):
                return

        self.graph.add_edge(
            edge.source,
            edge.target,
            relation=rel_val,
            weight=edge.weight,
            description=edge.description,
            properties=edge.properties
        )
        self._update_metrics()

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves raw node attributes"""
        if self.graph.has_node(node_id):
            attrs = dict(self.graph.nodes[node_id])
            attrs["id"] = node_id
            return attrs
        return None

    def get_all_nodes(self, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns all nodes, optionally filtered by tenant"""
        nodes = []
        for n, data in self.graph.nodes(data=True):
            if tenant_id and data.get("tenant_id") and data.get("tenant_id") != tenant_id:
                continue
            item = dict(data)
            item["id"] = n
            nodes.append(item)
        return nodes

    def get_all_edges(self) -> List[Dict[str, Any]]:
        """Returns all edges with metadata"""
        edges = []
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            edges.append({
                "source": u,
                "target": v,
                "relation": data.get("relation", "RELATED_TO"),
                "weight": data.get("weight", 1.0),
                "description": data.get("description", "")
            })
        return edges

    def extract_subgraph(self, seed_node_ids: List[str], max_hops: int = 2) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extracts a k-hop bidirectional neighborhood subgraph around seed entity nodes.
        Returns: (subgraph_nodes, subgraph_edges)
        """
        with tracer.start_as_current_span("GraphStore: extract_subgraph") as span:
            span.set_attribute("graph.seed_nodes", ",".join(seed_node_ids))
            span.set_attribute("graph.max_hops", max_hops)

            visited_nodes: Set[str] = set()
            frontier: Set[str] = set(seed_node_ids).intersection(set(self.graph.nodes()))

            for hop in range(max_hops + 1):
                visited_nodes.update(frontier)
                if hop == max_hops:
                    break
                next_frontier: Set[str] = set()
                for current in frontier:
                    # Forward successors
                    next_frontier.update(self.graph.successors(current))
                    # Only follow predecessors if the current node is not a global hub (Store/Warehouse)
                    curr_type = self._node_types.get(current)
                    if curr_type not in [EntityType.STORE, EntityType.WAREHOUSE]:
                        next_frontier.update(self.graph.predecessors(current))
                frontier = next_frontier - visited_nodes

            # Build sub-nodes and sub-edges
            subgraph_nodes: List[Dict[str, Any]] = []
            for n in visited_nodes:
                data = dict(self.graph.nodes[n])
                data["id"] = n
                subgraph_nodes.append(data)

            subgraph_edges: List[Dict[str, Any]] = []
            seen_edges = set()
            for u in visited_nodes:
                for v in visited_nodes:
                    if self.graph.has_edge(u, v):
                        for k, edge_data in self.graph.get_edge_data(u, v).items():
                            rel = edge_data.get("relation", "RELATED_TO")
                            edge_key = (u, v, rel)
                            if edge_key not in seen_edges:
                                seen_edges.add(edge_key)
                                subgraph_edges.append({
                                    "source": u,
                                    "target": v,
                                    "relation": rel,
                                    "weight": edge_data.get("weight", 1.0),
                                    "description": edge_data.get("description", "")
                                })

            span.set_attribute("graph.subgraph_nodes_count", len(subgraph_nodes))
            span.set_attribute("graph.subgraph_edges_count", len(subgraph_edges))
            logger.info(f"Extracted {max_hops}-hop subgraph with {len(subgraph_nodes)} nodes and {len(subgraph_edges)} edges.")
            return subgraph_nodes, subgraph_edges

    def find_relational_paths(self, source_id: str, target_id: str, max_length: int = 3) -> List[List[Dict[str, Any]]]:
        """Finds all directed relational paths between source and target entities up to max_length"""
        if not self.graph.has_node(source_id) or not self.graph.has_node(target_id):
            return []

        paths = []
        try:
            simple_paths = list(nx.all_simple_paths(self.graph, source_id, target_id, cutoff=max_length))
            for path in simple_paths:
                path_details = []
                for i in range(len(path) - 1):
                    u, v = path[i], path[i+1]
                    edge_data = self.graph.get_edge_data(u, v, 0) or {}
                    path_details.append({
                        "from": u,
                        "from_name": self.graph.nodes[u].get("name", u),
                        "to": v,
                        "to_name": self.graph.nodes[v].get("name", v),
                        "relation": edge_data.get("relation", "RELATED_TO"),
                        "description": edge_data.get("description", "")
                    })
                paths.append(path_details)
        except Exception as e:
            logger.warning(f"Error finding paths between '{source_id}' and '{target_id}': {e}")
        return paths

    def to_mermaid(self, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
        """Renders a Mermaid flowchart diagram representing the subgraph"""
        lines = ["graph TD"]
        # Node styling per EntityType
        for n in nodes:
            nid = str(n.get("id", "")).replace("-", "_").replace(" ", "_").replace(".", "_")
            name = n.get("name", nid)
            ntype = n.get("type", "Entity")
            lines.append(f'    {nid}["{name} ({ntype})"]')

        for e in edges:
            src = str(e.get("source", "")).replace("-", "_").replace(" ", "_").replace(".", "_")
            tgt = str(e.get("target", "")).replace("-", "_").replace(" ", "_").replace(".", "_")
            rel = e.get("relation", "RELATED_TO")
            lines.append(f'    {src} -->|"{rel}"| {tgt}')

        return "\n".join(lines)

    def to_undirected(self) -> nx.Graph:
        """Converts MultiDiGraph to simple undirected Graph for community detection algorithms"""
        undirected = nx.Graph()
        for n, data in self.graph.nodes(data=True):
            undirected.add_node(n, **data)
        for u, v, data in self.graph.edges(data=True):
            if undirected.has_edge(u, v):
                undirected[u][v]["weight"] = undirected[u][v].get("weight", 1.0) + data.get("weight", 1.0)
            else:
                undirected.add_edge(u, v, weight=data.get("weight", 1.0))
        return undirected

    def export_json(self) -> str:
        """Exports graph structure as a serialized JSON string"""
        data = {
            "nodes": [dict(data, id=n) for n, data in self.graph.nodes(data=True)],
            "edges": [
                {
                    "source": u,
                    "target": v,
                    "relation": data.get("relation"),
                    "weight": data.get("weight"),
                    "description": data.get("description"),
                    "properties": data.get("properties", {})
                }
                for u, v, data in self.graph.edges(data=True)
            ]
        }
        return json.dumps(data, indent=2)

    def load_from_json(self, json_data: str) -> int:
        """Loads nodes and edges from serialized JSON"""
        data = json.loads(json_data)
        count = 0
        for n in data.get("nodes", []):
            node_type = EntityType(n.get("type", "Product")) if n.get("type") in EntityType._value2member_map_ else EntityType.PRODUCT
            self.add_node(GraphNode(
                id=n.get("id"),
                name=n.get("name", n.get("id")),
                type=node_type,
                description=n.get("description", ""),
                properties=n.get("properties", {}),
                tenant_id=n.get("tenant_id", "store_tech")
            ))
            count += 1

        for e in data.get("edges", []):
            rel_type = RelationType(e.get("relation", "SUPPLIED_BY")) if e.get("relation") in RelationType._value2member_map_ else RelationType.SUPPLIED_BY
            self.add_edge(GraphEdge(
                source=e.get("source"),
                target=e.get("target"),
                relation=rel_type,
                weight=float(e.get("weight", 1.0)),
                description=e.get("description", ""),
                properties=e.get("properties", {})
            ))
        logger.info(f"Loaded {count} nodes and {len(data.get('edges', []))} edges into KnowledgeGraphStore.")
        return count

    def _update_metrics(self):
        """Updates Prometheus entity and relation counters"""
        try:
            for n, data in self.graph.nodes(data=True):
                etype = data.get("type", "Unknown")
                tenant = data.get("tenant_id", "store_tech")
                graph_entities_total_gauge.labels(entity_type=etype, tenant_id=tenant).set(1)
            for u, v, data in self.graph.edges(data=True):
                rel = data.get("relation", "Unknown")
                graph_relations_total_gauge.labels(relation_type=rel).set(1)
        except Exception:
            pass


graph_store = KnowledgeGraphStore()

# Auto-seed baseline knowledge graph
try:
    from src.infrastructure.default_knowledge import DEFAULT_GRAPH_NODES, DEFAULT_GRAPH_EDGES
    for _n in DEFAULT_GRAPH_NODES:
        graph_store.add_node(_n)
    for _e in DEFAULT_GRAPH_EDGES:
        graph_store.add_edge(_e)
except Exception as _seed_err:
    pass
