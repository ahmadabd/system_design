import os
import sys
import unittest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

# Insert support-service into python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
support_service_dir = os.path.join(project_root, "services", "support-service")
if support_service_dir not in sys.path:
    sys.path.insert(0, support_service_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from eval.evaluator import RAGTriadEvaluator, _extract_json_block

class TestRAGEvaluation(unittest.TestCase):

    def test_extract_json_block_markdown_wrapped(self):
        """Verify robust extraction of markdown-wrapped JSON"""
        raw_output = """Here is my evaluation:
```json
{
  "total_chunks": 3,
  "relevant_chunks": 3,
  "score": 1.0,
  "reasoning": "All chunks relevant"
}
```
"""
        parsed = _extract_json_block(raw_output)
        self.assertEqual(parsed["score"], 1.0)
        self.assertEqual(parsed["relevant_chunks"], 3)

    def test_extract_json_block_raw_braces(self):
        """Verify extraction when model outputs JSON without backticks"""
        raw_output = 'Analysis complete. {"score": 0.85, "reasoning": "Direct answer with minor verbosity"}'
        parsed = _extract_json_block(raw_output)
        self.assertEqual(parsed["score"], 0.85)
        self.assertIn("Direct", parsed["reasoning"])

    def test_evaluator_context_relevance(self):
        """Verify context relevance scoring with mocked LLM evaluator"""
        async def _run():
            mock_llm = MagicMock()
            mock_llm.invoke = AsyncMock(return_value='{"score": 0.95, "reasoning": "High precision"}')
            
            evaluator = RAGTriadEvaluator(mock_llm)
            docs = [{"source": "test.md", "content": "Return window is 30 days."}]
            
            result = await evaluator.evaluate_context_relevance("What is the return window?", docs)
            self.assertEqual(result["score"], 0.95)
            self.assertEqual(result["reasoning"], "High precision")
        
        asyncio.run(_run())

    def test_evaluator_faithfulness_and_relevance(self):
        """Verify faithfulness and answer relevance evaluations"""
        async def _run():
            mock_llm = MagicMock()
            mock_llm.invoke = AsyncMock(side_effect=[
                '{"score": 1.0, "reasoning": "100% grounded"}',  # Faithfulness
                '{"score": 0.90, "reasoning": "Direct answer"}'   # Answer Relevance
            ])
            
            evaluator = RAGTriadEvaluator(mock_llm)
            
            sample = {
                "id": "tc-test",
                "question": "What is shipping time?",
                "category": "policy_faq"
            }
            actual_output = {
                "final_answer": "Standard shipping takes 5-7 business days.",
                "retrieved_docs": [{"source": "shipping.md", "content": "Standard: 5-7 days"}],
                "tool_results": []
            }
            
            # Mock context relevance directly on evaluator
            evaluator.evaluate_context_relevance = AsyncMock(return_value={"score": 1.0, "reasoning": "Perfect context"})
            
            eval_res = await evaluator.evaluate_single_sample(sample, actual_output)
            self.assertEqual(eval_res["metrics"]["rag_triad_score"], 0.967)
            self.assertEqual(eval_res["metrics"]["faithfulness"], 1.0)
            self.assertEqual(eval_res["metrics"]["answer_relevance"], 0.90)

        asyncio.run(_run())

if __name__ == "__main__":
    unittest.main()
