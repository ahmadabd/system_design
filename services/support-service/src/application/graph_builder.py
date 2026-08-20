import logging
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from src.application.graph_state import SupportAgentState
from src.application.graph_nodes import (
    router_node,
    retrieve_node,
    grade_documents_node,
    tools_node,
    generate_node
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

class SupportGraphWorkflow:
    """Compiles and coordinates the LangGraph state machine with memory checkpointing"""
    def __init__(self):
        self.checkpointer = MemorySaver()
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(SupportAgentState)

        # 1. Register Nodes
        workflow.add_node("router", router_node)
        workflow.add_node("retrieve", retrieve_node)
        workflow.add_node("grade_docs", grade_documents_node)
        workflow.add_node("tools", tools_node)
        workflow.add_node("generate", generate_node)

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
        workflow.add_edge("generate", END)

        logger.info("Compiling LangGraph workflow with MemorySaver checkpointer...")
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
            "sources": []
        }

        config = {"configurable": {"thread_id": session_id}}
        result = await self.graph.ainvoke(initial_state, config=config)
        return result

    async def get_history(self, session_id: str):
        """Retrieves checkpointed message history for a given session"""
        config = {"configurable": {"thread_id": session_id}}
        state = await self.graph.aget_state(config)
        if state and state.values and "messages" in state.values:
            return state.values["messages"]
        return []

# Singleton workflow coordinator
support_workflow = SupportGraphWorkflow()
