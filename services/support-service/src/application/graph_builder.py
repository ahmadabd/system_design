import logging
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from src.application.graph_state import SupportAgentState
from src.application.graph_nodes import (
    router_node,
    retrieve_node,
    grade_documents_node,
    tools_node,
    generate_node,
    check_hallucination_node,
    grade_answer_node,
    clarification_node
)

logger = logging.getLogger("GraphBuilder")

def route_decision(state: SupportAgentState) -> str:
    """Evaluates router node output and directs graph execution along the appropriate edge"""
    intent = state.get("intent", "general")
    logger.info(f"[route_decision] Routing based on intent: {intent}")
    
    if intent == "policy_faq":
        return "retrieve"
    elif intent in ["order_inquiry", "product_inquiry"]:
        return "tools"
    elif intent == "hybrid":
        return "retrieve"
    else:
        return "generate"

def post_retrieve_decision(state: SupportAgentState) -> str:
    """If the query is hybrid, route to tools next; otherwise proceed to document grading"""
    intent = state.get("intent", "policy_faq")
    if intent == "hybrid":
        return "tools"
    return "grade_docs"

def post_tools_decision(state: SupportAgentState) -> str:
    """If the query is hybrid and docs haven't been graded, grade them, otherwise generate"""
    intent = state.get("intent", "")
    if intent == "hybrid":
        return "grade_docs"
    return "generate"

def after_hallucination_decision(state: SupportAgentState) -> str:
    """Self-RAG loop: If hallucination detected and retry_count <= 2, regenerate with correction; else finish"""
    status_val = state.get("hallucination_status", "grounded")
    retry_count = state.get("retry_count", 0)
    
    if status_val == "not_grounded" and retry_count <= 2:
        logger.warning(f"[after_hallucination_decision] Hallucination flagged. Triggering self-correction loop #{retry_count}...")
        return "generate"
    return "end"

class SupportGraphWorkflow:
    """Compiles and coordinates the LangGraph state machine with memory checkpointing and Self-RAG reflection"""
    def __init__(self):
        self.checkpointer = MemorySaver()
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(SupportAgentState)

        # 1. Register Core & Self-RAG Reflection Nodes
        workflow.add_node("router", router_node)
        workflow.add_node("retrieve", retrieve_node)
        workflow.add_node("grade_docs", grade_documents_node)
        workflow.add_node("tools", tools_node)
        workflow.add_node("generate", generate_node)
        workflow.add_node("check_hallucination", check_hallucination_node)

        # 2. Define Entry Point
        workflow.set_entry_point("router")

        # 3. Add Conditional Routing Edges
        workflow.add_conditional_edges(
            "router",
            route_decision,
            {
                "retrieve": "retrieve",
                "tools": "tools",
                "generate": "generate"
            }
        )

        workflow.add_conditional_edges(
            "retrieve",
            post_retrieve_decision,
            {
                "tools": "tools",
                "grade_docs": "grade_docs"
            }
        )

        workflow.add_conditional_edges(
            "tools",
            post_tools_decision,
            {
                "grade_docs": "grade_docs",
                "generate": "generate"
            }
        )

        workflow.add_edge("grade_docs", "generate")
        
        # 4. Self-RAG Reflection Cycle (Generate -> Check Hallucination -> Loop or END)
        workflow.add_edge("generate", "check_hallucination")

        workflow.add_conditional_edges(
            "check_hallucination",
            after_hallucination_decision,
            {
                "generate": "generate",
                "end": END
            }
        )

        logger.info("Compiling Self-RAG LangGraph workflow with MemorySaver checkpointer...")
        return workflow.compile(checkpointer=self.checkpointer)


    async def invoke(self, message: str, session_id: str = "default-session", user_id: str | None = None) -> dict:
        """Executes full multi-turn conversation step against the compiled graph"""
        from langchain_core.messages import HumanMessage
        
        initial_state = {
            "messages": [HumanMessage(content=message)],
            "session_id": session_id,
            "user_id": user_id,
            "intent": None,
            "extracted_entities": {},
            "retrieved_docs": [],
            "tool_results": [],
            "is_docs_relevant": None,
            "final_answer": None,
            "sources": [],
            "retry_count": 0,
            "hallucination_status": None,
            "answer_quality": None,
            "correction_feedback": None
        }

        config = {"configurable": {"thread_id": session_id}}
        return await self.graph.ainvoke(initial_state, config=config)

    async def get_history(self, session_id: str):
        """Retrieves raw message history for a given session thread from checkpointer"""
        config = {"configurable": {"thread_id": session_id}}
        state_snapshot = await self.graph.aget_state(config)
        if state_snapshot and "messages" in state_snapshot.values:
            return state_snapshot.values["messages"]
        return []

# Singleton workflow instance
support_workflow = SupportGraphWorkflow()
