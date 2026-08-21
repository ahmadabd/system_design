import re
import json
import logging
from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from opentelemetry import trace
from src.application.graph_state import SupportAgentState
from src.application.tools import get_order_status, get_product_info, get_user_profile
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.adapter.llm_adapter import OpenRouterLLMAdapter
from src.infrastructure.qdrant_setup import qdrant_manager
from src.infrastructure.llm_setup import llm_manager

logger = logging.getLogger("GraphNodes")
tracer = trace.get_tracer("support-service.langgraph")

vector_adapter = QdrantVectorAdapter(qdrant_manager)
llm_adapter = OpenRouterLLMAdapter(llm_manager)

# ---------------------------------------------------------------------------
# Node 1: Intent Router & Entity Extractor
# ---------------------------------------------------------------------------
async def router_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Classifies the incoming user message intent and extracts key entities like order_id or product_id.
    """
    with tracer.start_as_current_span("langgraph.router") as span:
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
        
        # Check for direct state-changing mutation requests (Cancel order, Refund order)
        has_mutation_keywords = any(kw in msg_lower for kw in [
            "cancel order", "cancel my order", "cancel this order", "please cancel", "i want to cancel", "cancel it", "cancel #", "cancel order #"
        ]) and ("order_id" in entities or "user_id" in entities)

        is_chitchat = any(msg_lower.strip().startswith(greeting) for greeting in ["hi", "hello", "hey", "thanks", "thank you", "good morning", "good evening", "who are you"])

        if has_mutation_keywords:
            intent = "order_action"
        elif has_policy_keywords and (has_order_keywords or has_product_keywords):
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

        span.set_attribute("langgraph.intent", intent)
        span.set_attribute("langgraph.extracted_entities", json.dumps(entities))

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
    with tracer.start_as_current_span("langgraph.retrieve") as span:
        from src.adapter.hybrid_retriever import hybrid_retriever

        messages = state["messages"]
        last_user_message = ""
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage) or (hasattr(msg, "type") and msg.type == "human"):
                last_user_message = msg.content
                break

        span.set_attribute("rag.query", last_user_message)
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

        span.set_attribute("rag.retrieved_chunks_count", len(retrieved_docs))
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
    with tracer.start_as_current_span("langgraph.grade_documents") as span:
        docs = state.get("retrieved_docs", [])
        if not docs:
            logger.info("[grade_documents_node] No documents to grade.")
            return {"is_docs_relevant": False}

        top_score = docs[0].get("score", 0.0) if docs else 0.0
        # Any positive match score from RRF or FlashRank indicates relevant context
        is_relevant = top_score > 0.0001
        span.set_attribute("rag.is_relevant", is_relevant)
        span.set_attribute("rag.top_score", top_score)
        logger.info(f"[grade_documents_node] Top reranked chunk score={top_score:.6f}, is_docs_relevant={is_relevant}")
        return {"is_docs_relevant": is_relevant}



# ---------------------------------------------------------------------------
# Node 4: Downstream Microservices Tool Execution Node
# ---------------------------------------------------------------------------
async def tools_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Executes live microservice client tools (Order, Product, User).
    """
    with tracer.start_as_current_span("langgraph.tools") as span:
        from src.application.tools import get_user_orders
        entities = state.get("extracted_entities", {})
        tool_results: List[Dict[str, Any]] = []

        # 1. If explicit order_id is given, query specific order status
        if "order_id" in entities:
            order_id = entities["order_id"]
            span.set_attribute("tools.order_id", order_id)
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
            span.set_attribute("tools.user_id", user_id)
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
            span.set_attribute("tools.product_id", product_id)
            logger.info(f"[tools_node] Executing get_product_info for product_id={product_id}")
            res = await get_product_info.ainvoke({"product_id": product_id})
            tool_results.append({"tool": "get_product_info", "output": res})

        span.set_attribute("tools.executed_count", len(tool_results))
        logger.info(f"[tools_node] Completed tool execution with {len(tool_results)} results.")
        return {"tool_results": tool_results}



# ---------------------------------------------------------------------------
# Node 5: Response Generator Node (Self-Correction Aware)
# ---------------------------------------------------------------------------
SYSTEM_AGENT_PROMPT = """You are an expert, friendly, and proactive AI Customer Support Specialist for our modern e-commerce platform.

### YOUR CONTEXT:
1. **Live Service Tool Data**:
{tool_context}

2. **Official Store Policy Documents**:
{policy_context}
{correction_directive}

### INSTRUCTIONS:
- If live order data is available, address the customer's specific order status, item, total, and tracking directly.
- If policy documents are provided, accurately explain the rules (e.g. 30-day return window, 60-min cancellation limit, 48-hr damage reporting).
- If the customer asks a hybrid question (e.g., "My order #12 is damaged, can I get a refund?"), synthesize the live order details with the exact policy steps (e.g. photos required within 48h for replacement/refund).
- If an order was not found, politely advise the customer to verify their order number.
- Maintain a warm, clear, and reassuring tone with bullet points and bold key terms.
- Stick strictly to the facts provided. Never invent dates, policies, or financial promises.
- CRITICAL: Output ONLY the direct final customer support answer. Do NOT output internal thinking processes, chain-of-thought scratchpads, or "Here's a thinking process".
"""


async def generate_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Synthesizes conversational history, tool execution results, and retrieved policy docs into a response.
    Supports Self-RAG corrective feedback loops.
    """
    with tracer.start_as_current_span("langgraph.generate") as span:
        retry_count = state.get("retry_count", 0)
        correction_feedback = state.get("correction_feedback")
        span.set_attribute("llm.retry_count", retry_count)
        
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

        # 3. Format Correction Directive if this is a retry turn
        if correction_feedback:
            correction_directive = f"\n\n### ⚠️ CRITICAL CORRECTION DIRECTIVE:\n{correction_feedback}"
            span.set_attribute("self_rag.correction_directive", correction_feedback)
            logger.info(f"[generate_node] Retrying generation (Attempt {retry_count + 1}) with feedback: {correction_feedback}")
        else:
            correction_directive = ""

        system_prompt = SYSTEM_AGENT_PROMPT.format(
            tool_context=tool_context,
            policy_context=policy_context,
            correction_directive=correction_directive
        )

        # 4. Assemble Full Conversation Messages
        messages_to_send = [SystemMessage(content=system_prompt)]
        for msg in state["messages"]:
            messages_to_send.append(msg)

        logger.info(f"[generate_node] Generating response (retry={retry_count}) with {len(messages_to_send)} prompt messages.")
        answer_text = await llm_adapter.invoke(messages_to_send)

        ai_message = AIMessage(content=str(answer_text))
        return {
            "messages": [ai_message],
            "final_answer": str(answer_text),
            "retry_count": retry_count + 1
        }


# ---------------------------------------------------------------------------
# Node 6: Self-RAG Hallucination Grader Node (LLM-as-a-Judge)
# ---------------------------------------------------------------------------
HALLUCINATION_GRADER_PROMPT = """You are a strict, objective AI Fact-Checking Grader.
Your job is to assess whether the assistant's response is grounded in and supported by the provided facts.

### PROVIDED FACTS (GROUND TRUTH):
Tool Data: {tool_context}
Policy Documents: {policy_context}

### ASSISTANT RESPONSE:
{answer}

### CRITERIA:
1. "grounded": Every single factual claim, SLA timeframe, return window, or order detail is directly supported by the provided facts, OR the response is polite conversational chitchat.
2. "not_grounded": The response contains fabricated numbers, made-up policies, or claims that contradict the facts.

Output ONLY a single word: either "grounded" or "not_grounded".
"""

async def check_hallucination_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Reflective node evaluating factual consistency of the generated response against context.
    """
    with tracer.start_as_current_span("langgraph.check_hallucination") as span:
        answer = state.get("final_answer", "")
        docs = state.get("retrieved_docs", [])
        tool_results = state.get("tool_results", [])
        intent = state.get("intent", "general")

        # If general greeting / chitchat, automatically considered grounded
        if intent == "general" or (not docs and not tool_results):
            span.set_attribute("self_rag.verdict", "grounded_skip")
            logger.info("[check_hallucination_node] General query, marking grounded.")
            return {"hallucination_status": "grounded", "correction_feedback": None}

        tool_context = json.dumps(tool_results) if tool_results else "None"
        policy_context = "\n".join([d.get("content", "") for d in docs]) if docs else "None"

        prompt = HALLUCINATION_GRADER_PROMPT.format(
            tool_context=tool_context,
            policy_context=policy_context,
            answer=answer
        )

        try:
            verdict = await llm_adapter.invoke([SystemMessage(content=prompt)])
            verdict_str = str(verdict).strip().lower()
            
            # Check verdict robustly
            if "not_grounded" in verdict_str or "ungrounded" in verdict_str:
                status_val = "not_grounded"
                feedback = "Your previous answer contained ungrounded claims. Stick STRICTLY to the facts provided in the policy context and live tool data."
            else:
                status_val = "grounded"
                feedback = None

            span.set_attribute("self_rag.verdict", status_val)
            logger.info(f"[check_hallucination_node] Hallucination grading verdict: '{status_val}' (raw: {verdict_str[:40]})")
            return {
                "hallucination_status": status_val,
                "correction_feedback": feedback
            }
        except Exception as e:
            span.set_attribute("self_rag.error", str(e))
            logger.warning(f"[check_hallucination_node] Grader evaluation failed: {e}. Defaulting to grounded.")
            return {
                "hallucination_status": "grounded",
                "correction_feedback": None
            }


# ---------------------------------------------------------------------------
# Node 7: Self-RAG Answer Quality & Usefulness Grader Node
# ---------------------------------------------------------------------------
ANSWER_GRADER_PROMPT = """You are a quality assurance evaluator for customer support.
Assess whether the assistant's response directly and helpfully answers the user's question.

### USER QUESTION:
{question}

### ASSISTANT RESPONSE:
{answer}

### CRITERIA:
1. "useful": The response answers, clarifies, or resolves the user's question in a clear, friendly, and helpful way.
2. "not_useful": The response is completely evasive, empty, or fails to address the question.

Output ONLY a single word: either "useful" or "not_useful".
"""

async def grade_answer_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Reflective node evaluating whether the response adequately addresses the user's request.
    """
    answer = state.get("final_answer", "")
    if not answer or len(answer.strip()) < 10:
        logger.warning("[grade_answer_node] Empty or too short answer, marking not_useful.")
        return {"answer_quality": "not_useful"}

    messages = state.get("messages", [])
    last_user_msg = ""
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage) or (hasattr(msg, "type") and msg.type == "human"):
            last_user_msg = msg.content
            break

    prompt = ANSWER_GRADER_PROMPT.format(
        question=last_user_msg or "General query",
        answer=answer
    )

    try:
        verdict = await llm_adapter.invoke([SystemMessage(content=prompt)])
        verdict_str = str(verdict).strip().lower()
        
        # Robust parsing: If model says not_useful explicitly at start
        if verdict_str.startswith("not_useful") or "is not useful" in verdict_str:
            status_val = "not_useful"
        else:
            status_val = "useful"
        
        logger.info(f"[grade_answer_node] Answer usefulness verdict: '{status_val}' (raw: {verdict_str[:40]})")
        return {"answer_quality": status_val}
    except Exception as e:
        logger.warning(f"[grade_answer_node] Grader failed: {e}. Defaulting to useful.")
        return {"answer_quality": "useful"}


# ---------------------------------------------------------------------------
# Node 8: Clarification Fallback Node
# ---------------------------------------------------------------------------
async def clarification_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Fallback node providing a polite clarification request if the answer was empty or completely off-topic.
    """
    existing_answer = state.get("final_answer", "")
    # If there is already a substantive answer, preserve it
    if existing_answer and len(existing_answer.strip()) > 30:
        logger.info("[clarification_node] Preserving substantive generated answer.")
        return {"final_answer": existing_answer}

    logger.info("[clarification_node] Providing clarification fallback response.")
    clarification_text = (
        "I want to make sure I get you the exact information you need! "
        "Could you please share a few more details, your order number, or rephrase your question?"
    )
    return {
        "final_answer": clarification_text,
        "messages": [AIMessage(content=clarification_text)]
    }


# ---------------------------------------------------------------------------
# Node 9: HITL Prepare Action Node (Pauses before mutation via Breakpoint)
# ---------------------------------------------------------------------------
async def prepare_action_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Validates mutation eligibility (e.g. order cancellation) and sets up the pending approval payload.
    Execution pauses right after this node at the 'execute_action' breakpoint.
    """
    with tracer.start_as_current_span("langgraph.prepare_action") as span:
        from src.application.tools import order_client, product_client

        entities = state.get("extracted_entities", {})
        order_id = entities.get("order_id")

        if not order_id and state.get("user_id"):
            # Look up most recent user order
            user_orders = await order_client.list_user_orders(int(state["user_id"]))
            if user_orders:
                order_id = user_orders[0].get("id")

        if not order_id:
            msg = "I'd be happy to help cancel your order, but I need an order number. Could you please provide your order ID (e.g. #1)?"
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None
            }

        span.set_attribute("hitl.order_id", order_id)
        logger.info(f"[prepare_action_node] Checking eligibility to cancel order #{order_id}")
        order_data = await order_client.get_order(order_id)
        
        if not order_data:
            msg = f"I couldn't locate order #{order_id} in our system. Please double-check your order confirmation email."
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None
            }

        order_status = str(order_data.get("status", "UNKNOWN")).upper()
        total_price = order_data.get("total_price", 0.0)
        product_id = order_data.get("product_id")
        span.set_attribute("hitl.order_status", order_status)
        
        product_name = "Item"
        if product_id:
            prod_data = await product_client.get_product(product_id)
            if prod_data:
                product_name = prod_data.get("name", "Item")

        # In our e-commerce policy: only PENDING / CREATED orders can be cancelled directly
        if order_status in ["PENDING", "CREATED", "PAID"]:
            pending_payload = {
                "action_type": "cancel_order",
                "order_id": order_id,
                "details": f"Order #{order_id} ({product_name}) - Total: ${total_price}",
                "confirmation_prompt": f"Please confirm: Do you want to cancel Order #{order_id} ({product_name}) for ${total_price}? This action will permanently cancel the order and initiate an immediate refund."
            }
            msg = (
                f"I have verified your order #{order_id} (**{product_name}** - **${total_price}**). "
                f"It is currently **{order_status}** and eligible for cancellation.\n\n"
                f"⚠️ **Confirmation Required**: Because order cancellation is permanent, please confirm below to proceed with the cancellation and refund."
            )
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": pending_payload
            }
        elif order_status in ["CANCELLED", "CANCELED"]:
            msg = f"Order #{order_id} ({product_name}) has already been **CANCELLED**. Your payment has been reversed."
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None
            }
        else:
            msg = (
                f"Order #{order_id} ({product_name}) is currently **{order_status}**. "
                f"Per our policy, orders that have already reached fulfillment or shipping cannot be cancelled directly in flight. "
                f"You may refuse delivery upon arrival or return it within 30 days for a full refund."
            )
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None
            }


# ---------------------------------------------------------------------------
# Node 10: HITL Execute Action Node (Executes Mutation after Approval)
# ---------------------------------------------------------------------------
async def execute_action_node(state: SupportAgentState) -> Dict[str, Any]:
    """
    Executes the approved mutation against downstream microservices.
    Only triggered when resumed with human approval.
    """
    with tracer.start_as_current_span("langgraph.execute_action") as span:
        from src.application.tools import order_client

        pending_action = state.get("pending_action", {})
        action_approved = state.get("action_approved")
        order_id = pending_action.get("order_id")
        span.set_attribute("hitl.order_id", order_id or -1)
        span.set_attribute("hitl.action_approved", bool(action_approved))

        logger.info(f"[execute_action_node] Executing action with approval status: {action_approved} for order #{order_id}")

        if not action_approved:
            msg = f"Order cancellation request for Order #{order_id} has been cancelled. Your order remains active and unchanged."
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None,
                "action_result": {"status": "aborted", "order_id": order_id}
            }

        # Execute cancellation in order-service
        cancel_res = await order_client.cancel_order(order_id)
        
        if cancel_res.get("success") is not False:
            msg = (
                f"✅ **Order #{order_id} has been successfully CANCELLED.**\n\n"
                f"Per our store policy, an automatic payment reversal has been initiated. "
                f"Funds will unlock on your original payment method in **1 to 3 business days**."
            )
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None,
                "action_result": {"status": "success", "order_id": order_id, "data": cancel_res}
            }
        else:
            msg = f"⚠️ We encountered an issue while cancelling Order #{order_id}: {cancel_res.get('message', 'Downstream error')}. Please contact human support."
            return {
                "final_answer": msg,
                "messages": [AIMessage(content=msg)],
                "pending_action": None,
                "action_result": {"status": "error", "message": cancel_res.get("message")}
            }




