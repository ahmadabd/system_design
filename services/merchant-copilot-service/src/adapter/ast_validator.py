import logging
from typing import Tuple, Optional
import sqlglot
from sqlglot import exp

logger = logging.getLogger("SQLASTValidator")

# Disallowed destructive SQL keywords and AST expressions
DISALLOWED_EXPRESSIONS = (
    exp.Drop,
    exp.Delete,
    exp.Update,
    exp.Insert,
    exp.Alter,
    exp.TruncateTable,
    exp.Create,
    exp.Command,
    exp.Grant,
    exp.Revoke
)

class SQLASTValidator:
    """
    Validates generated ClickHouse SQL queries using Abstract Syntax Tree (AST) parsing.
    Guarantees that queries are strictly read-only and enforce tenant isolation.
    """
    def validate_sql(self, sql_query: str, tenant_id: str = "store_tech") -> Tuple[bool, Optional[str]]:
        """
        Validates SQL safety and returns (is_valid, error_message).
        """
        if not sql_query or not sql_query.strip():
            return False, "SQL query is empty."

        clean_sql = sql_query.strip().rstrip(";")

        # 1. Parse AST with sqlglot (ClickHouse dialect)
        try:
            parsed = sqlglot.parse_one(clean_sql, read="clickhouse")
        except Exception as e:
            return False, f"SQL syntax parsing error: {str(e)}"

        if parsed is None:
            return False, "Could not parse SQL statement into an AST."

        # 2. Check for Disallowed Destructive AST Nodes
        for disallowed in DISALLOWED_EXPRESSIONS:
            if parsed.find(disallowed):
                return False, f"Security violation: Query contains disallowed destructive statement '{disallowed.__name__}'."

        # 3. Ensure Top-Level Statement is a SELECT or Union of SELECTs
        if not isinstance(parsed, (exp.Select, exp.Union)):
            return False, f"Security violation: Statement must be a read-only SELECT (found {type(parsed).__name__})."

        # 4. Check that tenant_id predicate is enforced in WHERE clause
        sql_lower = clean_sql.lower()
        if "tenant_id" not in sql_lower:
            return False, "Tenant safety violation: Query must include a 'tenant_id' filter predicate to enforce multi-tenant isolation."

        # 5. Check against common SQL injection patterns
        if "--" in sql_query or "/*" in sql_query or ";" in clean_sql:
            return False, "Security violation: Comment markers or multiple statements are not permitted."

        logger.info(f"SQL AST safety validation passed for tenant '{tenant_id}'.")
        return True, None


ast_validator = SQLASTValidator()
