import re
import json
import logging
from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from src.application.graph_state import SupportAgentState
from src.application.tools import get_order_status, get_product_info, get_user_profile
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.adapter.llm_adapter import OpenRouterLLMAdapter
from src.infrastructure.qdrant_setup import qdrant_manager
from src.infrastructure.llm_setup import llm_manager

logger = logging.getLogger("GraphNodes")

vector_adapter = QdrantVectorAdapter(qdrant_manager)
llm_adapter = OpenRouterLLMAdapter(llm_manager)

# ---------------------------------------------------------------------------
# Node 1: Intent Router & Entity Extractor
# ---------------------------------------------------------------------------
async def router_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Classifies the incoming user message intent and extracts key entities like order_id or product_id.
    """
    messages = state["messages"]
    last_user_message = ""
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage) or (hasattr(msg, "type") and msg.type == "human"):
            last_user_message = msg.content
            break

    logger.info(f"[router_node] Classifying intent for user message: '{last_user_message}'")

    entities: Dict[str, Any] = {}
    
    # 1. Regex Entity Extraction (Order ID, Product ID)
    order_match = re.search(r'(?:order|tracking|package)\s*#?\s*(\d+)', last_user_message, re.IGNORECASE)
    if order_match:
        entities["order_id"] = int(order_match.group(1))
    elif "order_id" in state.get("extracted_entities", {}):
        # Retain order_id from conversation memory if user is in a continuous thread
        entities["order_id"] = state["extracted_entities"]["order_id"]

    product_match = re.search(r'(?:product|item|sku)\s*#?\s*(\d+)', last_user_message, re.IGNORECASE)
    if product_match:
        entities["product_id"] = int(product_match.group(1))

    # 2. Attach User ID from Authentication State / Context
    if state.get("user_id"):
        try:
            entities["user_id"] = int(state["user_id"])
        except (ValueError, TypeError):
            pass

    # 3. Intent Classification Heuristics & Keywords
    msg_lower = last_user_message.lower()
    
    has_policy_keywords = any(kw in msg_lower for kw in [
        "return", "refund", "exchange", "policy", "ship", "deliver", "sla",
        "damaged", "broken", "cancel", "pay", "cash", "cod", "charge", "invoice",
        "warranty", "cost", "fee", "how long", "how much", "courier", "door", "transit", "days", "hours"
    ])
    has_order_keywords = "order_id" in entities or any(kw in msg_lower for kw in ["where is my", "track", "status of order", "my package", "my order", "my purchase", "recent orders"])
    has_product_keywords = "product_id" in entities or any(kw in msg_lower for kw in ["in stock", "price of", "product details"])

    is_chitchat = any(msg_lower.strip().startswith(greeting) for greeting in ["hi", "hello", "hey", "thanks", "thank you", "good morning", "good evening", "who are you"])

    if has_policy_keywords and (has_order_keywords or has_product_keywords):
        intent = "hybrid"
    elif has_order_keywords:
        intent = "order_inquiry"
    elif has_product_keywords:
        intent = "product_inquiry"
    elif has_policy_keywords or not is_chitchat:
        # Default to policy FAQ if not explicit chitchat so knowledge base is always queried
        intent = "policy_faq"
    else:
        intent = "general"

    logger.info(f"[router_node] Classified intent='{intent}', entities={entities}")
    return {
        "intent": intent,
        "extracted_entities": entities
    }

# ---------------------------------------------------------------------------
# Node 2: Two-Stage Hybrid Policy Retrieval & Re-ranking Node
# ---------------------------------------------------------------------------
async def retrieve_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Executes Two-Stage Hybrid Retrieval:
    Stage 1: Dense Vector (Qdrant) + Sparse Keyword (BM25) fused via Reciprocal Rank Fusion (RRF).
    Stage 2: FlashRank Cross-Encoder Re-Ranking selecting Top-3 precision chunks.
    """
    from src.adapter.hybrid_retriever import hybrid_retriever

    messages = state["messages"]
    last_user_message = ""
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage) or (hasattr(msg, "type") and msg.type == "human"):
            last_user_message = msg.content
            break

    logger.info(f"[retrieve_node] Running Two-Stage Hybrid Search & Reranking for: '{last_user_message}'")
    chunks = await hybrid_retriever.retrieve_and_rerank(last_user_message, top_k=3)

    retrieved_docs = []
    sources = []
    for c in chunks:
        doc_dict = {
            "source": c.metadata.get("source", "policy.md"),
            "title": c.metadata.get("title", "Policy"),
            "content": c.content,
            "score": c.score
        }
        retrieved_docs.append(doc_dict)
        sources.append({
            "source": doc_dict["source"],
            "title": doc_dict["title"],
            "score": doc_dict["score"]
        })

    logger.info(f"[retrieve_node] Selected {len(retrieved_docs)} reranked policy chunks.")
    return {
        "retrieved_docs": retrieved_docs,
        "sources": sources
    }

# ---------------------------------------------------------------------------
# Node 3: Document Relevance Grader (Self-Reflection)
# ---------------------------------------------------------------------------
async def grade_documents_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Evaluates whether the retrieved and reranked policy documents are relevant to avoid hallucination.
    """
    docs = state.get("retrieved_docs", [])
    if not docs:
        logger.info("[grade_documents_node] No documents to grade.")
        return {"is_docs_relevant": False}

    top_score = docs[0].get("score", 0.0) if docs else 0.0
    # Any positive match score from RRF or FlashRank indicates relevant context
    is_relevant = top_score > 0.0001
    logger.info(f"[grade_documents_node] Top reranked chunk score={top_score:.6f}, is_docs_relevant={is_relevant}")
    return {"is_docs_relevant": is_relevant}



# ---------------------------------------------------------------------------
# Node 4: Downstream Microservices Tool Execution Node
# ---------------------------------------------------------------------------
async def tools_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Executes live microservice client tools (Order, Product, User).
    """
    from src.application.tools import get_user_orders
    entities = state.get("extracted_entities", {})
    tool_results: List[Dict[str, Any]] = []

    # 1. If explicit order_id is given, query specific order status
    if "order_id" in entities:
        order_id = entities["order_id"]
        logger.info(f"[tools_node] Executing get_order_status for order_id={order_id}")
        res = await get_order_status.ainvoke({"order_id": order_id})
        if res.get("found") and res.get("product_id"):
            prod_info = await get_product_info.ainvoke({"product_id": res["product_id"]})
            if prod_info.get("found"):
                res["product_name"] = prod_info.get("name")
        tool_results.append({"tool": "get_order_status", "output": res})
    # 2. If user_id is given without a specific order_id, query user's order history
    elif "user_id" in entities:
        user_id = entities["user_id"]
        logger.info(f"[tools_node] Executing get_user_orders for user_id={user_id}")
        res = await get_user_orders.ainvoke({"user_id": user_id})
        if res.get("found") and res.get("orders"):
            for ord_item in res["orders"]:
                if ord_item.get("product_id"):
                    prod_info = await get_product_info.ainvoke({"product_id": ord_item["product_id"]})
                    if prod_info.get("found"):
                        ord_item["product_name"] = prod_info.get("name")
        tool_results.append({"tool": "get_user_orders", "output": res})

    # 3. Direct Product catalog inquiry
    if "product_id" in entities and "order_id" not in entities:
        product_id = entities["product_id"]
        logger.info(f"[tools_node] Executing get_product_info for product_id={product_id}")
        res = await get_product_info.ainvoke({"product_id": product_id})
        tool_results.append({"tool": "get_product_info", "output": res})

    logger.info(f"[tools_node] Completed tool execution with {len(tool_results)} results.")
    return {"tool_results": tool_results}



# ---------------------------------------------------------------------------
# Node 5: Final Response Generator Node
# ---------------------------------------------------------------------------
SYSTEM_AGENT_PROMPT = """You are an expert, friendly, and proactive AI Customer Support Specialist for our modern e-commerce platform.

### YOUR CONTEXT:
1. **Live Service Tool Data**:
{tool_context}

2. **Official Store Policy Documents**:
{policy_context}

### INSTRUCTIONS:
- If live order data is available, address the customer's specific order status, item, total, and tracking directly.
- If policy documents are provided, accurately explain the rules (e.g. 30-day return window, 60-min cancellation limit, 48-hr damage reporting).
- If the customer asks a hybrid question (e.g., "My order #12 is damaged, can I get a refund?"), synthesize the live order details with the exact policy steps (e.g. photos required within 48h for replacement/refund).
- If an order was not found, politely advise the customer to verify their order number.
- Maintain a warm, clear, and reassuring tone with bullet points and bold key terms.
"""

async def generate_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Synthesizes conversational history, tool execution results, and retrieved policy docs into a final answer.
    """
    # 1. Format Live Tool Context
    tool_results = state.get("tool_results", [])
    if tool_results:
        tool_lines = []
        for tr in tool_results:
            tool_lines.append(f"Tool: {tr['tool']}\nData: {json.dumps(tr['output'], indent=2)}")
        tool_context = "\n\n".join(tool_lines)
    else:
        tool_context = "No specific order or product tool lookup was triggered."

    # 2. Format Policy Context
    docs = state.get("retrieved_docs", [])
    is_relevant = state.get("is_docs_relevant", True)
    if docs and is_relevant:
        policy_lines = [f"--- {d['title']} ---\n{d['content']}" for d in docs]
        policy_context = "\n\n".join(policy_lines)
    else:
        policy_context = "No specific policy document applied to this query."

    system_prompt = SYSTEM_AGENT_PROMPT.format(
        tool_context=tool_context,
        policy_context=policy_context
    )

    # 3. Assemble Full Conversation Messages
    messages_to_send = [SystemMessage(content=system_prompt)]
    # Include all conversation history from state
    for msg in state["messages"]:
        messages_to_send.append(msg)

    logger.info(f"[generate_node] Generating response with {len(messages_to_send)} total prompt messages.")
    answer_text = await llm_adapter.invoke(messages_to_send)

    ai_message = AIMessage(content=str(answer_text))
    return {
        "messages": [ai_message],
        "final_answer": str(answer_text)
    }
