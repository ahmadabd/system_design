import pytest
from httpx import AsyncClient, ASGITransport

from src.domain.graph_entities import GraphNode, GraphEdge, EntityType, RelationType
from src.infrastructure.graph_store import KnowledgeGraphStore
from src.infrastructure.default_knowledge import DEFAULT_GRAPH_NODES, DEFAULT_GRAPH_EDGES
from src.adapter.community_detector import HierarchicalCommunityDetector
from src.adapter.entity_extractor import EntityExtractor
from src.adapter.qdrant_entity_adapter import QdrantEntityAdapter
from src.adapter.llm_adapter import GraphRAGLLMAdapter
from src.application.workflow import graphrag_app
from src.main import app


@pytest.fixture
def populated_graph_store():
    store = KnowledgeGraphStore()
    for n in DEFAULT_GRAPH_NODES:
        store.add_node(n)
    for e in DEFAULT_GRAPH_EDGES:
        store.add_edge(e)
    return store


def test_entity_and_relation_graph_store(populated_graph_store):
    store = populated_graph_store
    assert len(store.graph.nodes) >= 15
    assert len(store.graph.edges) >= 15

    # Test 2-hop subgraph extraction around laptop
    sub_nodes, sub_edges = store.extract_subgraph(["prod_gaming_laptop_pro"], max_hops=2)
    node_ids = [n["id"] for n in sub_nodes]
    assert "prod_gaming_laptop_pro" in node_ids
    assert "comp_vapor_chamber_cooler" in node_ids
    assert len(sub_edges) > 0

    # Test pathfinding from product to defect
    paths = store.find_relational_paths("prod_gaming_laptop_pro", "defect_thermal_throttling", max_length=4)
    assert len(paths) >= 1
    # Check that CoolMaster or Vapor Chamber is along the path
    first_path = paths[0]
    path_nodes = [step["from"] for step in first_path] + [first_path[-1]["to"]]
    assert "comp_vapor_chamber_cooler" in path_nodes

    # Test mermaid rendering
    mermaid = store.to_mermaid(sub_nodes, sub_edges)
    assert "graph TD" in mermaid
    assert "Gaming_Laptop_Pro" in mermaid or "prod_gaming_laptop_pro" in mermaid


def test_community_detection_and_summarization(populated_graph_store):
    detector = HierarchicalCommunityDetector(populated_graph_store)
    communities = detector.detect_communities()
    assert len(communities) >= 2

    # Check that communities contain member nodes and structured summaries
    for c in communities:
        assert c.id >= 0
        assert len(c.member_node_ids) > 0
        assert len(c.summary) > 10
        assert c.severity_rating in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


def test_entity_extractor_and_mode_classification(populated_graph_store):
    extractor = EntityExtractor(populated_graph_store)

    # Local mode detection
    local_mode = extractor.classify_search_mode("Why is the gaming laptop overheating and throttling?")
    assert local_mode == "local_multihop"

    # Global mode detection
    global_mode = extractor.classify_search_mode("Give me a high-level summary of all supplier defects across all products")
    assert global_mode == "global_community"

    # Seed entity extraction
    seeds = extractor.extract_seed_entities("The Shure SM7B microphone has a terrible ground loop hum buzzing noise")
    assert "prod_shure_sm7b" in seeds or "defect_ground_loop_hum" in seeds


def test_qdrant_entity_adapter_fallback():
    adapter = QdrantEntityAdapter()
    vec = adapter.embed_text("High performance laptop cooling defect")
    assert len(vec) == 384
    assert isinstance(vec[0], float)


def test_llm_adapter_heuristics():
    llm = GraphRAGLLMAdapter()
    local_report = llm._generate_heuristic_local_report(
        query="Why is the laptop overheating?",
        seed_nodes=[{"name": "Gaming Laptop Pro", "type": "Product"}],
        subgraph_nodes=[
            {"name": "Gaming Laptop Pro", "type": "Product"},
            {"name": "CoolMaster Thermal", "type": "Supplier", "properties": {"country": "China"}},
            {"name": "Thermal Throttling", "type": "Defect"}
        ],
        subgraph_edges=[{"source": "Gaming Laptop Pro", "target": "CoolMaster Thermal", "relation": "SUPPLIED_BY"}],
        reasoning_hops=["[Laptop] -> [Cooler] -> [Defect]"]
    )
    assert "GraphRAG Multi-Hop Root-Cause Investigation" in local_report
    assert "CoolMaster" in local_report


@pytest.mark.asyncio
async def test_compiled_graphrag_workflow_end_to_end():
    # 1. Test Local Multi-Hop Query
    local_input = {
        "query": "Why is the Gaming Laptop Pro overheating and who is the supplier?",
        "tenant_id": "store_tech",
        "session_id": "test_session_local",
        "search_mode": "auto",
        "extracted_seed_entities": [],
        "matched_graph_nodes": [],
        "subgraph_nodes": [],
        "subgraph_edges": [],
        "community_clusters": [],
        "intermediate_map_insights": [],
        "final_markdown_report": "",
        "mermaid_subgraph": "",
        "reasoning_hops": [],
        "confidence_score": 1.0
    }
    local_result = await graphrag_app.ainvoke(local_input)
    assert local_result["search_mode"] == "local_multihop"
    assert len(local_result["subgraph_nodes"]) > 0
    assert len(local_result["reasoning_hops"]) > 0
    assert "GraphRAG Multi-Hop Root-Cause Investigation" in local_result["final_markdown_report"]
    assert "graph TD" in local_result["mermaid_subgraph"]

    # 2. Test Global Community Map-Reduce Query
    global_input = {
        "query": "What are our overall product risks and supplier defects across all catalog items?",
        "tenant_id": "store_tech",
        "session_id": "test_session_global",
        "search_mode": "global_community",
        "extracted_seed_entities": [],
        "matched_graph_nodes": [],
        "subgraph_nodes": [],
        "subgraph_edges": [],
        "community_clusters": [],
        "intermediate_map_insights": [],
        "final_markdown_report": "",
        "mermaid_subgraph": "",
        "reasoning_hops": [],
        "confidence_score": 1.0
    }
    global_result = await graphrag_app.ainvoke(global_input)
    assert global_result["search_mode"] == "global_community"
    assert len(global_result["community_clusters"]) > 0
    assert "GraphRAG Global Platform Intelligence Briefing" in global_result["final_markdown_report"]


@pytest.mark.asyncio
async def test_fastapi_endpoints():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Health check
        h_resp = await client.get("/health")
        assert h_resp.status_code == 200
        assert h_resp.json()["status"] == "healthy"

        # 2. Stats endpoint
        stats_resp = await client.get("/stats")
        assert stats_resp.status_code == 200
        stats = stats_resp.json()
        assert stats["total_nodes"] >= 15
        assert stats["total_edges"] >= 15

        # 3. Subgraph visualization
        sub_resp = await client.get("/subgraph?seeds=prod_gaming_laptop_pro&hops=2")
        assert sub_resp.status_code == 200
        sub_data = sub_resp.json()
        assert "graph TD" in sub_data["mermaid_code"]

        # 4. Communities endpoint
        comm_resp = await client.get("/communities")
        assert comm_resp.status_code == 200
        assert comm_resp.json()["total_communities"] >= 2

        # 5. Query endpoint
        q_resp = await client.post(
            "/query",
            json={
                "query": "What caused the defect in Herman Miller Aeron Chair lumbar bracket?",
                "tenant_id": "store_tech"
            }
        )
        assert q_resp.status_code == 200
        data = q_resp.json()
        assert "GraphRAG" in data["final_markdown_report"]
        assert len(data["reasoning_hops"]) >= 1
