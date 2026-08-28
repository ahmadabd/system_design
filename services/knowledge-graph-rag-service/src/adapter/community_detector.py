import logging
from typing import List, Dict, Any, Optional
import networkx as nx
from opentelemetry import trace

from src.domain.graph_entities import CommunityCluster
from src.infrastructure.graph_store import KnowledgeGraphStore
from src.infrastructure.metrics import graph_communities_total_gauge

logger = logging.getLogger("CommunityDetector")
tracer = trace.get_tracer("knowledge-graph-rag-service")


class HierarchicalCommunityDetector:
    """
    Implements Louvain/Leiden modularity clustering over the knowledge graph.
    Discovers topologically dense communities representing incident clusters,
    supplier ecosystems, and component failure groups.
    """
    def __init__(self, graph_store: KnowledgeGraphStore):
        self.graph_store = graph_store
        self._cached_communities: List[CommunityCluster] = []

    def detect_communities(self) -> List[CommunityCluster]:
        """
        Runs Louvain modularity community detection on the underlying undirected graph.
        Returns a list of structured CommunityCluster domain objects with precomputed summaries.
        """
        with tracer.start_as_current_span("CommunityDetector: detect_communities") as span:
            undirected = self.graph_store.to_undirected()
            if len(undirected.nodes) == 0:
                return []

            # 1. Attempt Louvain community detection
            partition: Dict[str, int] = {}
            try:
                import community as community_louvain
                partition = community_louvain.best_partition(undirected)
            except Exception as e:
                logger.debug(f"python-louvain not available or failed ({e}), using NetworkX connected components / greedy modularity.")
                try:
                    communities_generator = nx.community.greedy_modularity_communities(undirected)
                    for comm_idx, comm_nodes in enumerate(communities_generator):
                        for node_id in comm_nodes:
                            partition[node_id] = comm_idx
                except Exception as nx_err:
                    logger.warning(f"Greedy modularity failed ({nx_err}), fallback to connected components.")
                    for comm_idx, comm_nodes in enumerate(nx.connected_components(undirected)):
                        for node_id in comm_nodes:
                            partition[node_id] = comm_idx

            # 2. Group nodes by community ID
            communities_map: Dict[int, List[str]] = {}
            for node_id, comm_id in partition.items():
                if comm_id not in communities_map:
                    communities_map[comm_id] = []
                communities_map[comm_id].append(node_id)

            # 3. Generate structured summaries for each community
            clusters: List[CommunityCluster] = []
            for comm_id, node_ids in communities_map.items():
                cluster = self._build_cluster_summary(comm_id, node_ids)
                clusters.append(cluster)

            self._cached_communities = clusters
            graph_communities_total_gauge.set(len(clusters))
            span.set_attribute("graph.detected_communities_count", len(clusters))
            logger.info(f"Successfully detected {len(clusters)} hierarchical community clusters across {len(undirected.nodes)} nodes.")
            return clusters

    def _build_cluster_summary(self, comm_id: int, node_ids: List[str]) -> CommunityCluster:
        """Synthesizes an executive summary and title for a detected community cluster"""
        node_details = []
        types_present = set()
        has_defect = False
        defect_details = []

        for nid in node_ids:
            data = self.graph_store.get_node(nid)
            if data:
                ntype = data.get("type", "")
                types_present.add(ntype)
                name = data.get("name", nid)
                desc = data.get("description", "")
                node_details.append(f"{name} ({ntype}): {desc}")
                if ntype == "Defect":
                    has_defect = True
                    defect_details.append(name)

        # Generate representative title
        sample_names = [self.graph_store.get_node(nid).get("name", nid) for nid in node_ids[:3] if self.graph_store.get_node(nid)]
        title = f"Community #{comm_id + 1}: {' & '.join(sample_names)}"

        # Compute severity
        severity = "CRITICAL" if has_defect else "LOW" if len(node_ids) < 3 else "MEDIUM"

        summary_text = (
            f"This cluster contains {len(node_ids)} interrelated entities spanning {', '.join(types_present)}. "
            f"Key entities include: {'; '.join(node_details[:4])}. "
        )
        if has_defect:
            summary_text += f"Active defects tracked: {', '.join(defect_details)}."

        return CommunityCluster(
            id=comm_id,
            level=0,
            title=title,
            summary=summary_text,
            member_node_ids=node_ids,
            key_findings=[d for d in node_details[:5]],
            severity_rating=severity
        )

    def get_cached_communities(self) -> List[CommunityCluster]:
        """Returns cached community clusters or triggers fresh detection"""
        if not self._cached_communities:
            return self.detect_communities()
        return self._cached_communities
