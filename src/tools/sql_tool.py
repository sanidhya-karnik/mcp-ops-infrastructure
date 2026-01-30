"""
SQL Query Tool
Provides safe, validated SQL query execution against the operations database.
"""

import hashlib
import time
from typing import Any

import structlog

from ..database.connection import get_operations_db
from ..schemas.models import SQLQueryInput, SQLQueryResult, ToolError

logger = structlog.get_logger()


async def execute_sql_query(
    query: str,
    parameters: dict[str, Any] | None = None,
    limit: int = 100
) -> dict[str, Any]:
    """
    Execute a SQL query with full validation.
    
    This tool provides:
    - Schema validation via Pydantic
    - SQL injection prevention
    - Query limiting
    - Performance tracking
    
    Args:
        query: SQL SELECT query to execute
        parameters: Optional query parameters
        limit: Maximum rows to return (1-1000)
        
    Returns:
        SQLQueryResult or ToolError as dict
    """
    start_time = time.perf_counter()
    
    # Validate input using Pydantic schema
    try:
        validated_input = SQLQueryInput(
            query=query,
            parameters=parameters,
            limit=limit
        )
    except Exception as e:
        logger.warning("SQL query validation failed", error=str(e))
        return ToolError(
            code="VALIDATION_ERROR",
            message=str(e),
            details={"query": query[:100]}
        ).model_dump()
    
    # Execute query
    try:
        db = get_operations_db()
        columns, rows = await db.execute_query(
            validated_input.query,
            validated_input.parameters,
            validated_input.limit
        )
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        # Generate query hash for audit trail
        query_hash = hashlib.sha256(
            validated_input.query.encode()
        ).hexdigest()[:16]
        
        result = SQLQueryResult(
            success=True,
            row_count=len(rows),
            columns=columns,
            rows=rows,
            execution_time_ms=round(execution_time, 2),
            query_hash=query_hash
        )
        
        logger.info(
            "SQL query executed",
            rows=len(rows),
            time_ms=execution_time,
            query_hash=query_hash
        )
        
        return result.model_dump()
        
    except Exception as e:
        logger.error("SQL query execution failed", error=str(e))
        return ToolError(
            code="EXECUTION_ERROR",
            message=f"Query failed: {str(e)}",
            details={"query_preview": query[:100]}
        ).model_dump()


async def get_database_schema() -> dict[str, Any]:
    """
    Get the schema of available tables.
    Useful for LLMs to understand what data is available.
    
    Returns:
        Dict with table names and their columns
    """
    try:
        db = get_operations_db()
        schema = await db.get_table_schema()
        
        return {
            "success": True,
            "tables": schema,
            "description": {
                "customers": "Customer information including company and lifetime value",
                "products": "Available products with pricing and inventory",
                "orders": "Customer orders with status and shipping info",
                "metrics": "Daily business metrics and KPIs"
            }
        }
        
    except Exception as e:
        return ToolError(
            code="SCHEMA_ERROR",
            message=f"Failed to get schema: {str(e)}"
        ).model_dump()
