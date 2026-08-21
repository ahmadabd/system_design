import os
import sys
import json
import time
import asyncio
import logging

# Ensure python can import support service modules
current_dir = os.path.dirname(os.path.abspath(__file__))
service_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(service_dir)
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("RAGBenchmarkRunner")

from src.adapter.llm_adapter import OpenRouterLLMAdapter
from src.infrastructure.llm_setup import llm_manager
from src.application.graph_builder import support_workflow
from eval.evaluator import RAGTriadEvaluator
from shared.common.tenant import set_tenant, TenantContext

async def run_benchmark(dataset_path: str = None, output_report_md: str = None, output_report_json: str = None):
    """Executes full automated evaluation against the golden benchmark dataset"""
    if dataset_path is None:
        dataset_path = os.path.join(current_dir, "benchmark_dataset.json")
    if output_report_md is None:
        output_report_md = os.path.join(current_dir, "eval_report.md")
    if output_report_json is None:
        output_report_json = os.path.join(current_dir, "eval_report.json")

    with open(dataset_path, "r") as f:
        test_cases = json.load(f)

    logger.info(f"Loaded {len(test_cases)} benchmark test cases from {dataset_path}")

    # Set tenant context
    set_tenant(TenantContext(slug="store_tech"))

    evaluator_llm = OpenRouterLLMAdapter(llm_manager)
    evaluator = RAGTriadEvaluator(evaluator_llm)

    results = []
    total_start = time.perf_counter()

    print("\n" + "="*80)
    print("🚀 STARTING AUTOMATED RAG TRIAD BENCHMARK SUITE")
    print("="*80 + "\n")

    for idx, tc in enumerate(test_cases, 1):
        tc_id = tc["id"]
        question = tc["question"]
        user_id = tc.get("user_id")
        category = tc.get("category", "general")

        print(f"[{idx}/{len(test_cases)}] Evaluating {tc_id} ({category}): '{question}'...")
        
        t0 = time.perf_counter()
        session_id = f"eval-{tc_id}-{int(time.time())}"
        
        try:
            # 1. Execute live state machine workflow
            output = await support_workflow.invoke(
                message=question,
                session_id=session_id,
                user_id=user_id
            )
            elapsed_sec = time.perf_counter() - t0

            # 2. Evaluate with LLM-as-a-judge
            eval_result = await evaluator.evaluate_single_sample(tc, output)
            eval_result["latency_seconds"] = round(elapsed_sec, 2)
            results.append(eval_result)

            metrics = eval_result["metrics"]
            print(f"    -> Context Rel: {metrics['context_relevance']:.2f} | Faithfulness: {metrics['faithfulness']:.2f} | Ans Rel: {metrics['answer_relevance']:.2f} | Overall: {metrics['rag_triad_score']:.2f} ({elapsed_sec:.1f}s)\n")
        except Exception as sample_err:
            elapsed_sec = time.perf_counter() - t0
            logger.error(f"Sample {tc_id} failed during evaluation: {sample_err}")
            results.append({
                "id": tc_id,
                "category": category,
                "question": question,
                "answer_preview": f"ERROR: {str(sample_err)}",
                "latency_seconds": round(elapsed_sec, 2),
                "metrics": {
                    "context_relevance": 0.0,
                    "faithfulness": 0.0,
                    "answer_relevance": 0.0,
                    "rag_triad_score": 0.0
                },
                "reasoning": {"error": str(sample_err)}
            })
            print(f"    -> ⚠️ Sample failed with error: {sample_err} ({elapsed_sec:.1f}s)\n")


    total_duration = round(time.perf_counter() - total_start, 2)

    # 3. Calculate Aggregate Metrics
    n = len(results)
    avg_ctx = round(sum(r["metrics"]["context_relevance"] for r in results) / n, 3)
    avg_faith = round(sum(r["metrics"]["faithfulness"] for r in results) / n, 3)
    avg_ans = round(sum(r["metrics"]["answer_relevance"] for r in results) / n, 3)
    avg_triad = round(sum(r["metrics"]["rag_triad_score"] for r in results) / n, 3)
    avg_latency = round(sum(r["latency_seconds"] for r in results) / n, 2)

    # Quality Gate Check (Thresholds: Triad >= 0.85, Faithfulness >= 0.90)
    passed_gate = (avg_triad >= 0.85) and (avg_faith >= 0.90)
    gate_status = "PASSED ✅" if passed_gate else "FAILED ❌"

    summary = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_test_cases": n,
        "total_duration_seconds": total_duration,
        "average_latency_seconds": avg_latency,
        "quality_gate": gate_status,
        "aggregate_metrics": {
            "mean_context_relevance": avg_ctx,
            "mean_faithfulness": avg_faith,
            "mean_answer_relevance": avg_ans,
            "mean_rag_triad_score": avg_triad
        },
        "detailed_results": results
    }

    # 4. Save JSON Report
    with open(output_report_json, "w") as f:
        json.dump(summary, f, indent=2)

    # 5. Render Markdown Report
    md_content = rf"""# 📊 Automated RAG Triad Evaluation Report

- **Date**: {summary['timestamp']}
- **Total Test Cases**: {n}
- **Quality Gate**: **{gate_status}**
- **Average Latency**: {avg_latency}s per turn

---

## 🎯 Executive Summary Metrics

| Metric Dimension | Target Threshold | Actual Score | Status |
| :--- | :---: | :---: | :---: |
| **Context Relevance** (Retriever Quality) | $\ge 0.85$ | **{avg_ctx:.3f}** | {'✅ PASS' if avg_ctx >= 0.85 else '⚠️ WARN'} |
| **Faithfulness** (Hallucination Defense) | $\ge 0.90$ | **{avg_faith:.3f}** | {'✅ PASS' if avg_faith >= 0.90 else '❌ FAIL'} |
| **Answer Relevance** (Intent Fulfillment) | $\ge 0.85$ | **{avg_ans:.3f}** | {'✅ PASS' if avg_ans >= 0.85 else '⚠️ WARN'} |
| **Overall RAG Triad Score** | $\ge 0.85$ | **{avg_triad:.3f}** | **{gate_status}** |


---

## 📋 Per-Query Benchmark Breakdown

| ID | Category | Question | Context Rel | Faithfulness | Ans Rel | Overall | Latency |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
"""

    for r in results:
        m = r["metrics"]
        q_short = r["question"][:40] + "..." if len(r["question"]) > 40 else r["question"]
        md_content += f"| `{r['id']}` | `{r['category']}` | {q_short} | {m['context_relevance']:.2f} | {m['faithfulness']:.2f} | {m['answer_relevance']:.2f} | **{m['rag_triad_score']:.2f}** | {r['latency_seconds']}s |\n"

    md_content += "\n---\n\n*Generated autonomously by Support Service RAG Evaluation Engine.*\n"

    with open(output_report_md, "w") as f:
        f.write(md_content)

    print("\n" + "="*80)
    print(f"📊 BENCHMARK COMPLETE: Quality Gate {gate_status}")
    print(f"   Context Relevance:  {avg_ctx:.3f}")
    print(f"   Faithfulness:       {avg_faith:.3f}")
    print(f"   Answer Relevance:   {avg_ans:.3f}")
    print(f"   Overall RAG Triad:  {avg_triad:.3f}")
    print(f"   Reports written to: {output_report_md} and {output_report_json}")
    print("="*80 + "\n")

    return summary

if __name__ == "__main__":
    asyncio.run(run_benchmark())
