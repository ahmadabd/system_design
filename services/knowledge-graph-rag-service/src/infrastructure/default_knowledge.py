from typing import List
from src.domain.graph_entities import GraphNode, GraphEdge, EntityType, RelationType


DEFAULT_GRAPH_NODES: List[GraphNode] = [
    # ── Scenario 1: GPU Thermal & Overheating Cluster ──────────────────────────
    GraphNode(
        id="prod_gaming_laptop_pro",
        name="Gaming Laptop Pro (32GB RAM, RTX 4080)",
        type=EntityType.PRODUCT,
        description="Flagship 16-inch OLED gaming laptop with RTX 4080 and i9-14900HX processor.",
        properties={"price": 1899.99, "stock": 5, "store_id": 1, "sku": "LAP-RTX4080-01"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="comp_rtx4080_mobile_gpu",
        name="NVIDIA GeForce RTX 4080 Mobile Die",
        type=EntityType.COMPONENT,
        description="12GB GDDR6X Ada Lovelace high-performance mobile silicon processor.",
        properties={"tdp_watts": 175, "architecture": "Ada Lovelace"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="supp_tsmc_taiwan",
        name="TSMC Fab 18 (Tainan, Taiwan)",
        type=EntityType.SUPPLIER,
        description="Tier-1 semiconductor foundry fabricating 4N custom silicon wafers.",
        properties={"country": "Taiwan", "sla_rating": "99.8%", "lead_time_days": 45},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="comp_vapor_chamber_cooler",
        name="Ultra-Thin Dual Vapor Chamber Heatsink",
        type=EntityType.COMPONENT,
        description="Copper liquid-wicking vapor chamber thermal module with twin radial fans.",
        properties={"cooling_capacity_watts": 220, "material": "Electrolytic Copper"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="supp_coolmaster_thermal",
        name="CoolMaster Thermal Solutions Ltd (Shenzhen)",
        type=EntityType.SUPPLIER,
        description="Component vendor manufacturing custom vapor chambers and liquid-metal thermal interfaces.",
        properties={"country": "China", "iso_certified": True, "lead_time_days": 18},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="batch_thermal_2026_b",
        name="Production Batch #2026-B Vapor Chambers",
        type=EntityType.BATCH,
        description="Q1 2026 manufacturing run of 2,500 copper vapor chamber cooling assemblies.",
        properties={"produced_qty": 2500, "production_date": "2026-01-15", "qc_pass_rate": "91.2%"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="defect_thermal_throttling",
        name="Defect #DEF-8802: Micro-Cavity Seal Leakage & Thermal Throttling",
        type=EntityType.DEFECT,
        description="Improper solder seal in Batch #2026-B causing de-pressurization of vapor liquid and catastrophic throttling above 85C.",
        properties={"severity": "CRITICAL", "failure_rate_pct": 14.8, "incident_count": 37},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="review_laptop_overheat_01",
        name="Customer Review #REV-901: Instant thermal shutdown in Premiere Pro",
        type=EntityType.REVIEW,
        description="Laptop hits 98C within 3 minutes of 4K video rendering and triggers emergency hardware shutdown.",
        properties={"rating": 1, "sentiment": "negative", "order_id": 101, "customer_id": 14},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="warehouse_west_hub",
        name="Ontario West Logistics Depot (California)",
        type=EntityType.WAREHOUSE,
        description="Primary fulfillment warehouse servicing western US orders.",
        properties={"capacity_units": 50000, "zone": "US-WEST"},
        tenant_id="store_tech"
    ),

    # ── Scenario 2: Microphone XLR Cable Ground Loop Buzz Cluster ─────────────
    GraphNode(
        id="prod_shure_sm7b",
        name="Shure SM7B Dynamic Cardioid Vocal Microphone",
        type=EntityType.PRODUCT,
        description="Industry standard dynamic broadcast and studio vocal microphone.",
        properties={"price": 399.00, "stock": 15, "store_id": 1, "sku": "MIC-SM7B-01"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="comp_neutrik_xlr_cable",
        name="Braided Shielded 3-Pin Balanced XLR Connector",
        type=EntityType.COMPONENT,
        description="Heavy-duty zinc diecast shell audio cable with gold-plated contacts.",
        properties={"pin_plating": "Gold", "impedance_ohms": 150},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="supp_neutrik_audio",
        name="Neutrik AG (Schaan, Liechtenstein)",
        type=EntityType.SUPPLIER,
        description="Precision European audio interconnect and pro-audio hardware manufacturer.",
        properties={"country": "Liechtenstein", "sla_rating": "99.9%", "lead_time_days": 14},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="batch_xlr_501",
        name="Batch #501 Broadcast Audio Cables",
        type=EntityType.BATCH,
        description="Batch of 1,000 studio XLR interconnects distributed in bundles.",
        properties={"produced_qty": 1000, "qc_pass_rate": "98.5%"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="defect_ground_loop_hum",
        name="Defect #DEF-3011: Unsoldered Pin 1 Ground Shell Shielding",
        type=EntityType.DEFECT,
        description="Intermittent missing ground bridge on Pin 1 causing 60Hz EMI hum when connected to ungrounded audio interfaces.",
        properties={"severity": "MEDIUM", "failure_rate_pct": 4.2, "incident_count": 12},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="review_mic_hum_02",
        name="Customer Review #REV-412: Loud electrical hum in OBS stream",
        type=EntityType.REVIEW,
        description="Constant 60Hz background buzzing noise whenever phantom power is turned on with the bundled cable.",
        properties={"rating": 2, "sentiment": "negative", "order_id": 105},
        tenant_id="store_tech"
    ),

    # ── Scenario 3: Ergonomic Chair PostureFit Bracket Snap Cluster ───────────
    GraphNode(
        id="prod_aeron_chair",
        name="Herman Miller Aeron Ergonomic Office Chair",
        type=EntityType.PRODUCT,
        description="Ergonomic mesh office task chair with PostureFit SL adjustable lumbar spine support.",
        properties={"price": 695.00, "stock": 12, "store_id": 1, "sku": "CHR-AERON-01"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="comp_posturefit_bracket",
        name="PostureFit SL Polymer Lumbar Pivot Bracket",
        type=EntityType.COMPONENT,
        description="Glass-filled nylon injection-molded dual spine stabilization bracket.",
        properties={"tensile_strength_mpa": 180, "material": "PA66-GF30"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="supp_michigan_polymers",
        name="Great Lakes Advanced Polymer Molding LLC (Grand Rapids, MI)",
        type=EntityType.SUPPLIER,
        description="North American high-durability polymer injection molding specialist.",
        properties={"country": "USA", "lead_time_days": 10},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="batch_lumbar_889",
        name="Batch #889 PostureFit Nylon Brackets",
        type=EntityType.BATCH,
        description="Sub-supplier resin pellet mix with suboptimal drying moisture content.",
        properties={"produced_qty": 5000, "qc_pass_rate": "89.0%"},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="defect_bracket_fracture",
        name="Defect #DEF-5109: Polymer Hydrolysis Stress Fracture",
        type=EntityType.DEFECT,
        description="Excess resin moisture during molding caused brittleness, resulting in bracket snaps under >90kg occupant weight.",
        properties={"severity": "HIGH", "failure_rate_pct": 8.7, "incident_count": 29},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="review_chair_snap_03",
        name="Customer Review #REV-719: Back support snapped on day 12",
        type=EntityType.REVIEW,
        description="Leaned back during Zoom meeting and heard a loud plastic snap. Lumbar pad completely dislodged.",
        properties={"rating": 1, "sentiment": "negative", "order_id": 112},
        tenant_id="store_tech"
    ),
    GraphNode(
        id="store_tech",
        name="TechHub Flagship Store (Tenant)",
        type=EntityType.STORE,
        description="Official high-end electronics and workstation retail store.",
        properties={"store_id": 1, "active_merchants": 4},
        tenant_id="store_tech"
    )
]


DEFAULT_GRAPH_EDGES: List[GraphEdge] = [
    # ── Laptop Relational Edges ───────────────────────────────────────────────
    GraphEdge(
        source="prod_gaming_laptop_pro",
        target="comp_rtx4080_mobile_gpu",
        relation=RelationType.CONTAINS_COMPONENT,
        description="Laptop motherboard integrates the NVIDIA RTX 4080 GPU die."
    ),
    GraphEdge(
        source="comp_rtx4080_mobile_gpu",
        target="supp_tsmc_taiwan",
        relation=RelationType.SUPPLIED_BY,
        description="NVIDIA 4N custom wafer fabricated by TSMC Taiwan."
    ),
    GraphEdge(
        source="prod_gaming_laptop_pro",
        target="comp_vapor_chamber_cooler",
        relation=RelationType.CONTAINS_COMPONENT,
        description="Laptop thermal dissipation relies on the CoolMaster dual vapor chamber."
    ),
    GraphEdge(
        source="comp_vapor_chamber_cooler",
        target="supp_coolmaster_thermal",
        relation=RelationType.SUPPLIED_BY,
        description="Vapor chamber heatsink manufactured and assembled by CoolMaster Shenzhen."
    ),
    GraphEdge(
        source="comp_vapor_chamber_cooler",
        target="batch_thermal_2026_b",
        relation=RelationType.PRODUCED_IN_BATCH,
        description="Cooler unit belongs to the Q1 2026 Batch #2026-B production run."
    ),
    GraphEdge(
        source="batch_thermal_2026_b",
        target="defect_thermal_throttling",
        relation=RelationType.REPORTED_DEFECT,
        description="Batch #2026-B exhibits 14.8% micro-cavity solder seal failure."
    ),
    GraphEdge(
        source="defect_thermal_throttling",
        target="review_laptop_overheat_01",
        relation=RelationType.CAUSED_RETURN_IN,
        description="Thermal throttling directly prompted customer return Review #REV-901."
    ),
    GraphEdge(
        source="prod_gaming_laptop_pro",
        target="warehouse_west_hub",
        relation=RelationType.SHIPPED_FROM,
        description="Units dispatched from Ontario West Logistics Depot."
    ),
    GraphEdge(
        source="prod_gaming_laptop_pro",
        target="store_tech",
        relation=RelationType.SOLD_BY,
        description="Retails exclusively on TechHub Flagship Store."
    ),

    # ── Microphone Relational Edges ───────────────────────────────────────────
    GraphEdge(
        source="prod_shure_sm7b",
        target="comp_neutrik_xlr_cable",
        relation=RelationType.CONTAINS_COMPONENT,
        description="Microphone bundled with Neutrik 3-Pin Balanced studio interconnect."
    ),
    GraphEdge(
        source="comp_neutrik_xlr_cable",
        target="supp_neutrik_audio",
        relation=RelationType.SUPPLIED_BY,
        description="Precision XLR interconnect manufactured by Neutrik Liechtenstein."
    ),
    GraphEdge(
        source="comp_neutrik_xlr_cable",
        target="batch_xlr_501",
        relation=RelationType.PRODUCED_IN_BATCH,
        description="Cable produced under Batch #501."
    ),
    GraphEdge(
        source="batch_xlr_501",
        target="defect_ground_loop_hum",
        relation=RelationType.REPORTED_DEFECT,
        description="Batch #501 experienced missing solder bridge on Pin 1 ground shell."
    ),
    GraphEdge(
        source="defect_ground_loop_hum",
        target="review_mic_hum_02",
        relation=RelationType.CAUSED_RETURN_IN,
        description="Ground loop EMI hum caused negative customer Review #REV-412."
    ),
    GraphEdge(
        source="prod_shure_sm7b",
        target="store_tech",
        relation=RelationType.SOLD_BY,
        description="Retails on TechHub Flagship Store."
    ),

    # ── Ergonomic Chair Relational Edges ──────────────────────────────────────
    GraphEdge(
        source="prod_aeron_chair",
        target="comp_posturefit_bracket",
        relation=RelationType.CONTAINS_COMPONENT,
        description="Chair features PostureFit SL adjustable spinal stabilization bracket."
    ),
    GraphEdge(
        source="comp_posturefit_bracket",
        target="supp_michigan_polymers",
        relation=RelationType.SUPPLIED_BY,
        description="Injection-molded by Great Lakes Advanced Polymer Molding LLC."
    ),
    GraphEdge(
        source="comp_posturefit_bracket",
        target="batch_lumbar_889",
        relation=RelationType.PRODUCED_IN_BATCH,
        description="Molded from Batch #889 polymer resin pellets."
    ),
    GraphEdge(
        source="batch_lumbar_889",
        target="defect_bracket_fracture",
        relation=RelationType.REPORTED_DEFECT,
        description="High resin moisture during molding caused structural embrittlement."
    ),
    GraphEdge(
        source="defect_bracket_fracture",
        target="review_chair_snap_03",
        relation=RelationType.CAUSED_RETURN_IN,
        description="Bracket fracture directly caused customer return Review #REV-719."
    ),
    GraphEdge(
        source="prod_aeron_chair",
        target="store_tech",
        relation=RelationType.SOLD_BY,
        description="Retails on TechHub Flagship Store."
    )
]
