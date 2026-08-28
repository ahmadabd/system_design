import re
import logging
from typing import List, Tuple
from src.infrastructure.graph_store import KnowledgeGraphStore

logger = logging.getLogger("EntityExtractor")


class EntityExtractor:
    """Extracts seed entity references and classifies GraphRAG search mode (Local vs Global)"""
    def __init__(self, graph_store: KnowledgeGraphStore):
        self.graph_store = graph_store

    def classify_search_mode(self, query: str) -> str:
        """
        Determines whether the query requires:
        - 'global_community': Broad, holistic questions across the entire platform
        - 'local_multihop': Focused questions targeting specific products, components, or suppliers
        """
        q_lower = query.lower()
        global_triggers = [
            "across all", "all products", "all suppliers", "summary of all", "high level",
            "overall risks", "root causes across", "holistic", "entire catalog", "what are the main issues",
            "all defects", "common themes", "platform overview", "system-wide"
        ]
        if any(trigger in q_lower for trigger in global_triggers):
            return "global_community"
        return "local_multihop"

    def extract_seed_entities(self, query: str) -> List[str]:
        """
        Identifies seed node IDs present in the graph by checking exact/fuzzy keyword matches.
        """
        q_lower = query.lower()
        matched_node_ids = []

        # Generic words that should not trigger loose keyword matches
        stopwords = {
            "with", "from", "unit", "inch", "flagship", "defect", "defects",
            "supplier", "suppliers", "component", "components", "batch", "batches",
            "review", "reviews", "customer", "store", "tenant", "product", "products",
            "responsible", "causing", "caused", "issue", "problem", "overheating",
            "laptop", "audio", "office", "chair", "parts"
        }

        # 1. First priority: Alias Map (exact domain concepts)
        alias_map = {
            "gaming laptop": "prod_gaming_laptop_pro",
            "laptop pro": "prod_gaming_laptop_pro",
            "rtx 4080": "comp_rtx4080_mobile_gpu",
            "vapor chamber": "comp_vapor_chamber_cooler",
            "coolmaster": "supp_coolmaster_thermal",
            "overheat": "defect_thermal_throttling",
            "overheating": "defect_thermal_throttling",
            "thermal throttling": "defect_thermal_throttling",
            "shure": "prod_shure_sm7b",
            "sm7b": "prod_shure_sm7b",
            "neutrik": "comp_neutrik_xlr_cable",
            "xlr": "comp_neutrik_xlr_cable",
            "ground loop": "defect_ground_loop_hum",
            "buzz": "defect_ground_loop_hum",
            "hum": "defect_ground_loop_hum",
            "aeron": "prod_aeron_chair",
            "herman miller": "prod_aeron_chair",
            "posturefit": "comp_posturefit_bracket",
            "lumbar": "comp_posturefit_bracket",
            "bracket fracture": "defect_bracket_fracture",
            "polymer hydrolysis": "defect_bracket_fracture"
        }
        for kw, target_id in alias_map.items():
            if kw in q_lower and target_id in self.graph_store.graph.nodes() and target_id not in matched_node_ids:
                matched_node_ids.append(target_id)

        # 2. Check full node names / IDs present in query text
        for node_id, data in self.graph_store.graph.nodes(data=True):
            name = str(data.get("name", "")).lower()
            nid = str(node_id).lower()
            if (name and len(name) > 6 and name in q_lower) or (nid and len(nid) > 6 and nid in q_lower):
                if node_id not in matched_node_ids:
                    matched_node_ids.append(node_id)
                continue

            # Check specific high-signal keywords (excluding stopwords)
            keywords = [w for w in re.findall(r"\w+", name) if len(w) > 4 and w not in stopwords]
            if any(k in q_lower for k in keywords):
                if node_id not in matched_node_ids:
                    matched_node_ids.append(node_id)

        # 3. Fallback generic word aliases if nothing matched yet
        if not matched_node_ids:
            fallback_map = {
                "overheat": "defect_thermal_throttling",
                "overheating": "defect_thermal_throttling",
                "thermal": "defect_thermal_throttling",
                "throttling": "defect_thermal_throttling",
                "laptop": "prod_gaming_laptop_pro",
                "gpu": "comp_rtx4080_mobile_gpu",
                "mic": "prod_shure_sm7b",
                "microphone": "prod_shure_sm7b",
                "buzz": "defect_ground_loop_hum",
                "hum": "defect_ground_loop_hum",
                "chair": "prod_aeron_chair",
                "snap": "defect_bracket_fracture",
                "broken": "defect_bracket_fracture"
            }
            for kw, target_id in fallback_map.items():
                if kw in q_lower and target_id in self.graph_store.graph.nodes() and target_id not in matched_node_ids:
                    matched_node_ids.append(target_id)

        return matched_node_ids
