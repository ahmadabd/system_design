import json
import logging
import re
from typing import Dict, Any, List, Optional
try:
    from langchain_core.messages import SystemMessage
except ImportError:
    class SystemMessage:
        def __init__(self, content: str):
            self.content = content


logger = logging.getLogger("RAGEvaluator")

# ---------------------------------------------------------------------------
# Evaluator Prompts (LLM-as-a-Judge)
# ---------------------------------------------------------------------------
CONTEXT_RELEVANCE_PROMPT = """You are an expert AI Benchmark Evaluator for Information Retrieval.
Assess whether the retrieved document chunks are relevant and necessary for answering the user's query.

### USER QUESTION:
{question}

### RETRIEVED CHUNKS:
{contexts}

### TASK:
Count the total number of chunks. For each chunk, determine if it contains information relevant to answering the question.
Calculate context_relevance as (number_of_relevant_chunks / total_chunks).
If total_chunks is 0:
- If the question does not require policy retrieval (e.g. greeting or pure order ID lookup), score is 1.0.
- Otherwise score is 0.0.

Respond STRICTLY in JSON format:
{{
  "total_chunks": <int>,
  "relevant_chunks": <int>,
  "score": <float between 0.0 and 1.0>,
  "reasoning": "<short explanation>"
}}
"""

FAITHFULNESS_PROMPT = """You are a strict, forensic Fact-Checking Evaluator for AI systems.
Evaluate whether EVERY claim and statement in the AI response is completely grounded in and supported by the provided facts.

### PROVIDED FACTS (GROUND TRUTH):
Retrieved Policy Documents:
{policy_contexts}

Live Microservice / Tool Data:
{tool_contexts}

### ASSISTANT RESPONSE:
{answer}

### TASK:
1. Break down the response into individual factual claims (statements about return days, dollar fees, order status, item names, shipping SLAs).
2. Count total claims and how many are directly substantiated by the provided facts.
3. Calculate faithfulness as (substantiated_claims / total_claims). If response is conversational chitchat or pure clarification, score is 1.0.

Respond STRICTLY in JSON format:
{{
  "total_claims": <int>,
  "substantiated_claims": <int>,
  "score": <float between 0.0 and 1.0>,
  "reasoning": "<short explanation>"
}}
"""

ANSWER_RELEVANCE_PROMPT = """You are an expert Evaluator scoring User Intent Fulfillment.
Assess whether the assistant's response directly, completely, and accurately answers the user's question without evading or drifting off-topic.

### USER QUESTION:
{question}

### ASSISTANT RESPONSE:
{answer}

### SCORING RUBRIC (0.0 to 1.0):
- 1.0: Directly and completely answers all parts of the user's question clearly.
- 0.8: Answers the core question well, with minor omission or slight wordiness.
- 0.5: Partially answers the question or gives generic information.
- 0.0: Irrelevant, off-topic, evasive, or fails to address the question.

Respond STRICTLY in JSON format:
{{
  "score": <float between 0.0 and 1.0>,
  "reasoning": "<short explanation>"
}}
"""


def _extract_json_block(text: str) -> Dict[str, Any]:
    """Robustly extracts JSON payload from model response containing reasoning or markdown blocks"""
    text_clean = text.strip()
    match = re.search(r"```json\s*(\{.*?\})\s*```", text_clean, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    
    match_raw = re.search(r"(\{.*\})", text_clean, re.DOTALL)
    if match_raw:
        return json.loads(match_raw.group(1))
    
    return json.loads(text_clean)


class RAGTriadEvaluator:
    """
    Automated RAG Triad Evaluator measuring:
    1. Context Relevance (Retriever precision)
    2. Faithfulness (Generator hallucination check)
    3. Answer Relevance (Customer intent satisfaction)
    """
    def __init__(self, llm_adapter):
        self.llm_adapter = llm_adapter

    async def evaluate_context_relevance(self, question: str, retrieved_docs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Evaluates whether retrieved documents contain relevant context for the question"""
        if not retrieved_docs:
            return {
                "score": 1.0,
                "reasoning": "No retrieval needed for this inquiry category."
            }

        formatted_chunks = "\n---\n".join([
            f"Chunk {i+1} [Source: {d.get('source', 'unknown')} | Score: {d.get('score', 0)}]:\n{d.get('content', '')}"
            for i, d in enumerate(retrieved_docs)
        ])

        prompt = CONTEXT_RELEVANCE_PROMPT.format(
            question=question,
            contexts=formatted_chunks
        )

        try:
            res = await self.llm_adapter.invoke([SystemMessage(content=prompt)])
            data = _extract_json_block(str(res))
            return {
                "score": max(0.0, min(1.0, float(data.get("score", 1.0)))),
                "reasoning": data.get("reasoning", "")
            }
        except Exception as e:
            logger.warning(f"Context relevance evaluation error: {e}. Defaulting to 1.0")
            return {"score": 1.0, "reasoning": str(e)}

    async def evaluate_faithfulness(
        self, 
        answer: str, 
        retrieved_docs: List[Dict[str, Any]], 
        tool_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Evaluates whether generated answer contains hallucinations or ungrounded statements"""
        policy_text = "\n---\n".join([d.get("content", "") for d in retrieved_docs]) or "None"
        tool_text = json.dumps(tool_results, indent=2) if tool_results else "None"

        prompt = FAITHFULNESS_PROMPT.format(
            policy_contexts=policy_text,
            tool_contexts=tool_text,
            answer=answer
        )

        try:
            res = await self.llm_adapter.invoke([SystemMessage(content=prompt)])
            data = _extract_json_block(str(res))
            return {
                "score": max(0.0, min(1.0, float(data.get("score", 1.0)))),
                "reasoning": data.get("reasoning", "")
            }
        except Exception as e:
            logger.warning(f"Faithfulness evaluation error: {e}. Defaulting to 1.0")
            return {"score": 1.0, "reasoning": str(e)}

    async def evaluate_answer_relevance(self, question: str, answer: str) -> Dict[str, Any]:
        """Evaluates whether the response directly addresses the user question"""
        prompt = ANSWER_RELEVANCE_PROMPT.format(
            question=question,
            answer=answer
        )

        try:
            res = await self.llm_adapter.invoke([SystemMessage(content=prompt)])
            data = _extract_json_block(str(res))
            return {
                "score": max(0.0, min(1.0, float(data.get("score", 1.0)))),
                "reasoning": data.get("reasoning", "")
            }
        except Exception as e:
            logger.warning(f"Answer relevance evaluation error: {e}. Defaulting to 1.0")
            return {"score": 1.0, "reasoning": str(e)}

    async def evaluate_single_sample(
        self,
        test_case: Dict[str, Any],
        actual_output: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Evaluates all 3 RAG Triad dimensions for a single conversation sample"""
        question = test_case["question"]
        answer = actual_output.get("final_answer") or actual_output.get("answer", "")
        retrieved_docs = actual_output.get("retrieved_docs") or actual_output.get("sources", [])
        tool_results = actual_output.get("tool_results", [])

        # Run 3 evaluators
        ctx_rel = await self.evaluate_context_relevance(question, retrieved_docs)
        faith = await self.evaluate_faithfulness(answer, retrieved_docs, tool_results)
        ans_rel = await self.evaluate_answer_relevance(question, answer)

        triad_avg = round((ctx_rel["score"] + faith["score"] + ans_rel["score"]) / 3.0, 3)

        return {
            "id": test_case["id"],
            "category": test_case.get("category", "general"),
            "question": question,
            "answer_preview": answer[:150] + "..." if len(answer) > 150 else answer,
            "metrics": {
                "context_relevance": ctx_rel["score"],
                "faithfulness": faith["score"],
                "answer_relevance": ans_rel["score"],
                "rag_triad_score": triad_avg
            },
            "reasoning": {
                "context_relevance": ctx_rel["reasoning"],
                "faithfulness": faith["reasoning"],
                "answer_relevance": ans_rel["reasoning"]
            }
        }
