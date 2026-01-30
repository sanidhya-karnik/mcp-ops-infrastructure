"""
Pydantic schemas for schema-driven LLM interactions.
All tool inputs/outputs are strictly validated.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, field_serializer, ConfigDict


# ============== Auth Schemas ==============

class UserRole(str, Enum):
    """Role-based access control levels."""
    ADMIN = "admin"          # Full access
    ANALYST = "analyst"      # Read + search, no writes
    READONLY = "readonly"    # Read-only database access


class AuthContext(BaseModel):
    """Authentication context for each request."""
    user_id: str = Field(..., min_length=1, max_length=100)
    role: UserRole
    api_key_hash: str = Field(..., min_length=8)
    authenticated_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = ConfigDict(frozen=True)


# ============== Tool Input Schemas ==============

class SQLQueryInput(BaseModel):
    """Schema for SQL query tool - prevents injection via validation."""
    query: str = Field(
        ..., 
        min_length=1, 
        max_length=2000,
        description="SQL SELECT query to execute"
    )
    parameters: Optional[dict[str, Any]] = Field(
        default=None,
        description="Query parameters for parameterized queries"
    )
    limit: int = Field(
        default=100, 
        ge=1, 
        le=1000,
        description="Maximum rows to return"
    )
    
    @field_validator('query')
    @classmethod
    def validate_safe_query(cls, v: str) -> str:
        """Ensure only SELECT queries are allowed."""
        normalized = v.strip().upper()
        
        # Block dangerous operations
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 
            'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE', '--', ';'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in normalized:
                raise ValueError(f"Query contains forbidden keyword: {keyword}")
        
        if not normalized.startswith('SELECT'):
            raise ValueError("Only SELECT queries are permitted")
            
        return v


class SearchInput(BaseModel):
    """Schema for web search tool."""
    query: str = Field(
        ..., 
        min_length=1, 
        max_length=500,
        description="Search query"
    )
    max_results: int = Field(
        default=5, 
        ge=1, 
        le=10,
        description="Maximum number of results"
    )
    search_depth: str = Field(
        default="basic",
        pattern="^(basic|advanced)$",
        description="Search depth: 'basic' or 'advanced'"
    )


class WeatherInput(BaseModel):
    """Schema for weather API tool."""
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    days: int = Field(default=1, ge=1, le=7, description="Forecast days")
    
    @field_validator('latitude', 'longitude')
    @classmethod
    def round_coordinates(cls, v: float) -> float:
        """Round to reasonable precision."""
        return round(v, 4)


class GeocodingInput(BaseModel):
    """Schema for location lookup."""
    location: str = Field(
        ..., 
        min_length=1, 
        max_length=200,
        description="City name or address"
    )


# ============== Tool Output Schemas ==============

class SQLQueryResult(BaseModel):
    """Structured output from SQL queries."""
    success: bool
    row_count: int
    columns: list[str]
    rows: list[dict[str, Any]]
    execution_time_ms: float
    query_hash: str  # For audit trail
    
    
class SearchResult(BaseModel):
    """Single search result."""
    title: str
    url: str
    content: str
    score: Optional[float] = None


class SearchResponse(BaseModel):
    """Structured output from search."""
    success: bool
    query: str
    results: list[SearchResult]
    total_results: int
    

class WeatherData(BaseModel):
    """Weather data point."""
    date: str
    temperature_max: Optional[float] = None
    temperature_min: Optional[float] = None
    temperature_current: Optional[float] = None
    precipitation_probability: Optional[int] = None
    weather_code: Optional[int] = None
    weather_description: Optional[str] = None


class WeatherResponse(BaseModel):
    """Structured output from weather API."""
    success: bool
    location: dict[str, float]  # lat/lon
    timezone: str
    forecast: list[WeatherData]
    

class GeocodingResult(BaseModel):
    """Location lookup result."""
    name: str
    latitude: float
    longitude: float
    country: str
    admin1: Optional[str] = None  # State/province


class GeocodingResponse(BaseModel):
    """Structured output from geocoding."""
    success: bool
    query: str
    results: list[GeocodingResult]


# ============== Audit Schemas ==============

class AuditEntry(BaseModel):
    """Schema for audit log entries."""
    id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_id: str
    user_role: UserRole
    tool_name: str
    input_data: dict[str, Any]
    output_summary: str
    success: bool
    error_message: Optional[str] = None
    execution_time_ms: float
    ip_address: Optional[str] = None
    
    @field_serializer('timestamp')
    def serialize_datetime(self, v: datetime) -> str:
        return v.isoformat()


# ============== Error Schemas ==============

class ToolError(BaseModel):
    """Standardized error response."""
    error: bool = True
    code: str
    message: str
    details: Optional[dict[str, Any]] = None
