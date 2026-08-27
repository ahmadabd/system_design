import logging
import re
import json
from typing import Dict, Any, List, Optional
from src.infrastructure.config import settings

logger = logging.getLogger("CopilotLLMAdapter")

class CopilotLLMAdapter:
    """Manages LLM completions for Intent Classification, Text-to-SQL, Self-Correction, and Synthesis"""
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.model = settings.OPENAI_MODEL
        self._llm = None

    def _get_llm(self):
        if self._llm is None and self.api_key:
            try:
                from langchain_openai import ChatOpenAI
                self._llm = ChatOpenAI(
                    model=self.model,
                    api_key=self.api_key,
                    temperature=0.1
                )
                logger.info(f"Initialized ChatOpenAI with model '{self.model}'.")
            except Exception as e:
                logger.warning(f"Could not initialize ChatOpenAI ({e}). Using offline heuristics.")
                self._llm = None
        return self._llm

    def classify_intent(self, query: str) -> str:
        """Classifies user intent as 'structured_analytics', 'policy_guidelines', or 'hybrid'"""
        llm = self._get_llm()
        if llm:
            try:
                prompt = (
                    "Classify the following merchant inquiry into exactly ONE of these three intents:\n"
                    "- 'structured_analytics': quantitative queries regarding revenue, orders, stock counts, pricing, sales numbers.\n"
                    "- 'policy_guidelines': questions regarding store policies, return deadlines, warranties, shipping SLAs, fees.\n"
                    "- 'hybrid': queries asking both quantitative analytics AND policy/guideline questions.\n\n"
                    f"Query: \"{query}\"\n"
                    "Respond with ONLY the intent name in lowercase."
                )
                resp = llm.invoke(prompt)
                content = resp.content.strip().lower()
                if "hybrid" in content:
                    return "hybrid"
                elif "policy" in content:
                    return "policy_guidelines"
                elif "analytics" in content or "structured" in content or "sql" in content:
                    return "structured_analytics"
            except Exception as e:
                logger.warning(f"LLM intent classification error: {e}. Using rule fallback.")

        # Rule-based fallback
        q_lower = query.lower()
        has_policy = any(k in q_lower for k in ["policy", "return", "warranty", "sla", "deadline", "damaged", "shipping tier", "fee", "commission", "guideline"])
        has_analytics = any(k in q_lower for k in ["revenue", "sales", "price", "stock", "orders", "top", "best", "total", "average", "count", "cost", "sum", "how many", "which product"])

        if has_policy and has_analytics:
            return "hybrid"
        elif has_policy:
            return "policy_guidelines"
        return "structured_analytics"

    def generate_sql(self, query: str, tenant_id: str, linked_schemas: str) -> str:
        """Generates a valid, read-only ClickHouse SQL query"""
        llm = self._get_llm()
        if llm:
            try:
                prompt = (
                    f"You are a ClickHouse SQL expert. Generate a single, read-only ClickHouse SQL SELECT query to answer the user request.\n"
                    f"Target Store Tenant: '{tenant_id}' (You MUST include `WHERE tenant_id = '{tenant_id}'` in all queries!)\n\n"
                    f"Available Table Schemas:\n{linked_schemas}\n\n"
                    f"User Query: \"{query}\"\n\n"
                    f"Rules:\n"
                    f"1. Generate ONLY the raw SQL query with no markdown backticks, explanations, or quotes.\n"
                    f"2. Query MUST be a SELECT statement against database `copilot_analytics`.\n"
                    f"3. MUST include `WHERE tenant_id = '{tenant_id}'`.\n"
                    f"4. Use ClickHouse aggregate functions: count(), sum(), avg(), min(), max().\n"
                    f"5. Add LIMIT 20 if unbounded."
                )
                resp = llm.invoke(prompt)
                raw_sql = resp.content.strip()
                clean = re.sub(r"^```(sql)?", "", raw_sql, flags=re.IGNORECASE).rstrip("`").strip()
                return clean
            except Exception as e:
                logger.warning(f"LLM SQL generation error: {e}. Using deterministic fallback SQL.")

        # Deterministic offline SQL generator
        q_lower = query.lower()
        if "revenue" in q_lower or "sales" in q_lower or "total amount" in q_lower:
            return f"SELECT sum(total_amount) AS total_revenue, count() AS total_orders FROM copilot_analytics.orders_analytics WHERE tenant_id = '{tenant_id}' AND status = 'CONFIRMED'"
        elif "order" in q_lower or "status" in q_lower:
            return f"SELECT status, count() AS order_count, sum(total_amount) AS total_amount FROM copilot_analytics.orders_analytics WHERE tenant_id = '{tenant_id}' GROUP BY status"
        elif "payment" in q_lower:
            return f"SELECT payment_method, status, count() AS count, sum(amount) AS volume FROM copilot_analytics.payments_analytics WHERE tenant_id = '{tenant_id}' GROUP BY payment_method, status"
        elif "top" in q_lower or "best" in q_lower:
            return f"SELECT id, name, category, price, stock FROM copilot_analytics.products_analytics WHERE tenant_id = '{tenant_id}' ORDER BY price DESC LIMIT 5"
        else:
            return f"SELECT id, name, category, price, stock FROM copilot_analytics.products_analytics WHERE tenant_id = '{tenant_id}' ORDER BY stock DESC LIMIT 10"

    def fix_sql_error(self, query: str, tenant_id: str, failed_sql: str, error_message: str, linked_schemas: str) -> str:
        """Heals a failed SQL query given the database or AST error message"""
        llm = self._get_llm()
        if llm:
            try:
                prompt = (
                    f"You are a ClickHouse SQL self-correction agent. The previous SQL query produced an error.\n\n"
                    f"Target Tenant: '{tenant_id}' (Enforce `tenant_id = '{tenant_id}'`)\n"
                    f"User Query: \"{query}\"\n"
                    f"Failed SQL: {failed_sql}\n"
                    f"Error Message: {error_message}\n"
                    f"Available Table Schemas:\n{linked_schemas}\n\n"
                    f"Fix the query. Return ONLY the raw corrected ClickHouse SQL SELECT statement without markdown or explanations."
                )
                resp = llm.invoke(prompt)
                clean = re.sub(r"^```(sql)?", "", resp.content.strip(), flags=re.IGNORECASE).rstrip("`").strip()
                return clean
            except Exception as e:
                logger.warning(f"LLM SQL self-correction error: {e}. Using healed fallback.")

        # Fallback self-correction: enforce WHERE tenant_id predicate and valid database prefix
        corrected = failed_sql
        if "tenant_id" not in corrected.lower():
            if "WHERE" in corrected.upper():
                corrected = corrected.replace("WHERE", f"WHERE tenant_id = '{tenant_id}' AND ", 1)
            else:
                corrected += f" WHERE tenant_id = '{tenant_id}'"
        return corrected

    def synthesize_report(
        self,
        query: str,
        tenant_id: str,
        sql_rows: Optional[List[Dict[str, Any]]] = None,
        generated_sql: Optional[str] = None,
        policies: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """Synthesizes structured SQL data and unstructured policy documents into an executive markdown report"""
        llm = self._get_llm()
        if llm:
            try:
                prompt = (
                    f"You are the Merchant Copilot Executive Assistant for store tenant '{tenant_id}'.\n"
                    f"User Query: \"{query}\"\n\n"
                    f"Structured SQL Result (from ClickHouse OLAP):\n{json.dumps(sql_rows or [], indent=2)}\n\n"
                    f"Unstructured Policy Context (from Qdrant Vector DB):\n{json.dumps(policies or [], indent=2)}\n\n"
                    f"Task: Write a concise, professional executive response in GitHub Markdown format.\n"
                    f"- Include data tables for quantitative figures.\n"
                    f"- Cite specific policies or SLA deadlines if applicable.\n"
                    f"- Provide actionable business insights."
                )
                resp = llm.invoke(prompt)
                return resp.content.strip()
            except Exception as e:
                logger.warning(f"LLM report synthesis error: {e}. Using structured template fallback.")

        # Heuristic markdown generator
        sections = [f"### 📊 Merchant Copilot Executive Report (`{tenant_id}`)\n"]
        sections.append(f"**Query**: *\"{query}\"*\n")

        # 1. SQL Data Table
        if sql_rows:
            sections.append("#### 📈 Quantitative Data (ClickHouse OLAP)")
            headers = list(sql_rows[0].keys())
            header_row = "| " + " | ".join(headers) + " |"
            divider_row = "| " + " | ".join(["---"] * len(headers)) + " |"
            data_rows = []
            for r in sql_rows:
                row_str = "| " + " | ".join(str(r.get(h, "")) for h in headers) + " |"
                data_rows.append(row_str)
            sections.append("\n".join([header_row, divider_row] + data_rows) + "\n")
            if generated_sql:
                sections.append(f"> **Executed SQL**: `{generated_sql}`\n")
        elif generated_sql:
            sections.append("#### 📈 Quantitative Data (ClickHouse OLAP)\n*No matching records found for the given criteria.*\n")

        # 2. Policy References
        if policies:
            sections.append("#### 📜 Store Policies & SLA Guidelines (Qdrant Vector Store)")
            for p in policies:
                sections.append(f"- **{p.get('title', 'Policy')}** (*Category: {p.get('category', 'General')}*): {p.get('content', '')}")
            sections.append("")

        sections.append("💡 *Report automatically generated with AST validation and multi-tenant isolation.*")
        return "\n".join(sections)


llm_adapter = CopilotLLMAdapter()
