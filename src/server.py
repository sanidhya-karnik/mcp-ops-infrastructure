"""
MCP Operations Server
Main entry point for the Model Context Protocol server.

This server provides:
- Schema-driven LLM interactions via Pydantic models
- Authenticated, auditable workflows
- Constrained tool usage with role-based permissions
- Integration with external APIs and databases
"""

import asyncio
import time
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    CallToolResult,
    ListToolsResult,
    GetPromptResult,
    Prompt,
    PromptMessage,
    PromptArgument
)
import structlog

from .config import get_settings
from .auth.authenticator import get_authenticator, AuthenticationError, AuthorizationError
from .audit.logger import get_audit_logger
from .database.connection import get_operations_db
from .schemas.models import UserRole
from .tools.sql_tool import execute_sql_query, get_database_schema
from .tools.search_tool import execute_search
from .tools.weather_tool import get_weather, geocode_location

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger()

# Initialize MCP server
server = Server("mcp-ops-server")


# ============== Tool Definitions ==============

TOOLS = [
    Tool(
        name="sql_query",
        description="""Execute a SQL SELECT query against the operations database.
        
Available tables:
- customers: id, name, email, company, industry, created_at, lifetime_value, is_active
- products: id, name, category, price, stock_quantity, is_available  
- orders: id, customer_id, order_date, total_amount, status, shipping_city, shipping_country
- metrics: id, date, metric_name, value, dimension

Only SELECT queries are allowed. Dangerous operations are blocked.
Returns structured results with row count and execution time.""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL SELECT query to execute"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (default: 100, max: 1000)",
                    "default": 100
                }
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="database_schema",
        description="Get the schema of available database tables. Use this first to understand what data is available.",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": []
        }
    ),
    Tool(
        name="web_search",
        description="""Search the web using Tavily API.
        
Returns structured search results with titles, URLs, and content snippets.
Requires TAVILY_API_KEY to be configured.""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum results (default: 5, max: 10)",
                    "default": 5
                },
                "search_depth": {
                    "type": "string",
                    "enum": ["basic", "advanced"],
                    "description": "Search depth",
                    "default": "basic"
                }
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="weather",
        description="""Get weather forecast for a location.
        
Provide latitude and longitude coordinates. Use geocode_location first if you have a city name.
Returns current conditions and daily forecast.""",
        inputSchema={
            "type": "object",
            "properties": {
                "latitude": {
                    "type": "number",
                    "description": "Latitude (-90 to 90)"
                },
                "longitude": {
                    "type": "number",
                    "description": "Longitude (-180 to 180)"
                },
                "days": {
                    "type": "integer",
                    "description": "Forecast days (default: 1, max: 7)",
                    "default": 1
                }
            },
            "required": ["latitude", "longitude"]
        }
    ),
    Tool(
        name="geocode_location",
        description="Look up coordinates (latitude/longitude) for a city or location name. Use this before calling the weather tool.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "City name or address"
                }
            },
            "required": ["location"]
        }
    ),
    Tool(
        name="view_audit_log",
        description="""View recent audit log entries.
        
Shows tool execution history including user, tool, success status, and timing.
Requires 'analyst' or 'admin' role.""",
        inputSchema={
            "type": "object",
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "Filter by tool name (optional)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum entries to return (default: 20)",
                    "default": 20
                }
            },
            "required": []
        }
    )
]


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools."""
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """
    Execute a tool with authentication, authorization, and audit logging.
    """
    settings = get_settings()
    authenticator = get_authenticator()
    audit_logger = get_audit_logger()
    
    start_time = time.perf_counter()
    
    # Extract API key from arguments (passed by client)
    api_key = arguments.pop("_api_key", None)
    
    # Authenticate
    try:
        if settings.auth_enabled:
            auth_context = authenticator.authenticate(api_key)
        else:
            auth_context = type('AuthContext', (), {
                'user_id': 'anonymous',
                'role': UserRole.ADMIN
            })()
    except AuthenticationError as e:
        return [TextContent(
            type="text",
            text=f'{{"error": true, "code": "AUTH_ERROR", "message": "{str(e)}"}}'
        )]
    
    # Map tool names to required permissions
    tool_permission_map = {
        "sql_query": "sql_query",
        "database_schema": "sql_query",
        "web_search": "search",
        "weather": "weather",
        "geocode_location": "geocoding",
        "view_audit_log": "view_audit"
    }
    
    # Authorize
    required_permission = tool_permission_map.get(name)
    if required_permission:
        try:
            authenticator.authorize(auth_context, required_permission)
        except AuthorizationError as e:
            return [TextContent(
                type="text",
                text=f'{{"error": true, "code": "AUTH_ERROR", "message": "{str(e)}"}}'
            )]
    
    # Execute tool
    result = None
    error_message = None
    success = True
    
    try:
        if name == "sql_query":
            result = await execute_sql_query(
                query=arguments.get("query", ""),
                limit=arguments.get("limit", 100)
            )
        elif name == "database_schema":
            result = await get_database_schema()
        elif name == "web_search":
            result = await execute_search(
                query=arguments.get("query", ""),
                max_results=arguments.get("max_results", 5),
                search_depth=arguments.get("search_depth", "basic")
            )
        elif name == "weather":
            result = await get_weather(
                latitude=arguments.get("latitude", 0),
                longitude=arguments.get("longitude", 0),
                days=arguments.get("days", 1)
            )
        elif name == "geocode_location":
            result = await geocode_location(
                location=arguments.get("location", "")
            )
        elif name == "view_audit_log":
            entries = await audit_logger.query(
                tool_name=arguments.get("tool_name"),
                limit=arguments.get("limit", 20)
            )
            result = {
                "success": True,
                "entries": [e.model_dump() for e in entries],
                "count": len(entries)
            }
        else:
            result = {"error": True, "code": "UNKNOWN_TOOL", "message": f"Unknown tool: {name}"}
            success = False
            
    except Exception as e:
        error_message = str(e)
        result = {"error": True, "code": "EXECUTION_ERROR", "message": error_message}
        success = False
        logger.error("Tool execution failed", tool=name, error=error_message)
    
    execution_time = (time.perf_counter() - start_time) * 1000
    
    # Check if result indicates an error
    if isinstance(result, dict) and result.get("error"):
        success = False
        error_message = result.get("message", "Unknown error")
    
    # Log to audit trail
    if settings.audit_enabled:
        await audit_logger.log(
            user_id=auth_context.user_id,
            user_role=auth_context.role,
            tool_name=name,
            input_data=arguments,
            output_summary=str(result)[:500] if result else "No result",
            success=success,
            execution_time_ms=execution_time,
            error_message=error_message
        )
    
    # Return result
    import json
    return [TextContent(
        type="text",
        text=json.dumps(result, indent=2, default=str)
    )]


# ============== Prompts ==============

@server.list_prompts()
async def list_prompts() -> list[Prompt]:
    """List available prompts."""
    return [
        Prompt(
            name="data-analysis",
            description="Analyze data from the operations database",
            arguments=[
                PromptArgument(
                    name="question",
                    description="The analytical question to answer",
                    required=True
                )
            ]
        ),
        Prompt(
            name="search-and-summarize",
            description="Search the web and summarize findings",
            arguments=[
                PromptArgument(
                    name="topic",
                    description="Topic to search for",
                    required=True
                )
            ]
        )
    ]


@server.get_prompt()
async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
    """Get a prompt by name."""
    if name == "data-analysis":
        question = arguments.get("question", "") if arguments else ""
        return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=f"""You have access to an operations database. First use database_schema to understand the available tables, then write and execute SQL queries to answer this question:

{question}

Provide a clear analysis with the data you find."""
                    )
                )
            ]
        )
    elif name == "search-and-summarize":
        topic = arguments.get("topic", "") if arguments else ""
        return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user", 
                    content=TextContent(
                        type="text",
                        text=f"""Search the web for information about: {topic}

Summarize your findings, citing sources where appropriate."""
                    )
                )
            ]
        )
    
    raise ValueError(f"Unknown prompt: {name}")


# ============== Server Lifecycle ==============

async def initialize():
    """Initialize all server components."""
    logger.info("Initializing MCP Operations Server...")
    
    # Initialize database
    db = get_operations_db()
    await db.initialize()
    
    # Initialize audit logger
    audit_logger = get_audit_logger()
    await audit_logger.initialize()
    
    logger.info("Server initialization complete")


async def shutdown():
    """Clean shutdown of server components."""
    logger.info("Shutting down...")
    
    db = get_operations_db()
    await db.close()
    
    audit_logger = get_audit_logger()
    await audit_logger.close()
    
    logger.info("Shutdown complete")


async def main():
    """Main entry point."""
    await initialize()
    
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
