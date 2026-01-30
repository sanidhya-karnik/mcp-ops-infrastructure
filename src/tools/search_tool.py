"""
Search Tool
Provides web search capabilities using Tavily API.
"""

import time
from typing import Any

import httpx
import structlog

from ..config import get_settings
from ..schemas.models import (
    SearchInput, SearchResponse, SearchResult, ToolError
)

logger = structlog.get_logger()


async def execute_search(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic"
) -> dict[str, Any]:
    """
    Execute a web search using Tavily API.
    
    This tool provides:
    - Schema-validated input
    - Rate limiting awareness
    - Structured response format
    
    Args:
        query: Search query string
        max_results: Maximum results to return (1-10)
        search_depth: 'basic' or 'advanced' search
        
    Returns:
        SearchResponse or ToolError as dict
    """
    settings = get_settings()
    start_time = time.perf_counter()
    
    # Validate input
    try:
        validated_input = SearchInput(
            query=query,
            max_results=max_results,
            search_depth=search_depth
        )
    except Exception as e:
        logger.warning("Search validation failed", error=str(e))
        return ToolError(
            code="VALIDATION_ERROR",
            message=str(e)
        ).model_dump()
    
    # Check API key
    if not settings.tavily_api_key:
        logger.warning("Tavily API key not configured")
        return ToolError(
            code="CONFIG_ERROR",
            message="Search API not configured. Set TAVILY_API_KEY environment variable."
        ).model_dump()
    
    # Execute search
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": settings.tavily_api_key,
                    "query": validated_input.query,
                    "max_results": validated_input.max_results,
                    "search_depth": validated_input.search_depth,
                    "include_answer": False,
                    "include_raw_content": False
                }
            )
            
            response.raise_for_status()
            data = response.json()
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        # Parse results
        results = [
            SearchResult(
                title=r.get("title", ""),
                url=r.get("url", ""),
                content=r.get("content", ""),
                score=r.get("score")
            )
            for r in data.get("results", [])
        ]
        
        result = SearchResponse(
            success=True,
            query=validated_input.query,
            results=results,
            total_results=len(results)
        )
        
        logger.info(
            "Search executed",
            query=query[:50],
            results=len(results),
            time_ms=execution_time
        )
        
        return result.model_dump()
        
    except httpx.HTTPStatusError as e:
        logger.error("Tavily API error", status=e.response.status_code)
        return ToolError(
            code="API_ERROR",
            message=f"Search API error: {e.response.status_code}",
            details={"response": e.response.text[:200]}
        ).model_dump()
        
    except httpx.TimeoutException:
        logger.error("Search timeout")
        return ToolError(
            code="TIMEOUT_ERROR",
            message="Search request timed out"
        ).model_dump()
        
    except Exception as e:
        logger.error("Search failed", error=str(e))
        return ToolError(
            code="SEARCH_ERROR",
            message=f"Search failed: {str(e)}"
        ).model_dump()
