"""
Tests for MCP Operations Infrastructure tools.
"""

import pytest
from unittest.mock import AsyncMock, patch

from src.schemas.models import (
    SQLQueryInput, SearchInput, WeatherInput, GeocodingInput,
    UserRole, AuthContext
)
from src.auth.authenticator import Authenticator


class TestSchemaValidation:
    """Test Pydantic schema validation."""
    
    def test_sql_query_valid(self):
        """Valid SELECT query should pass."""
        query = SQLQueryInput(query="SELECT * FROM customers")
        assert query.query == "SELECT * FROM customers"
        assert query.limit == 100
    
    def test_sql_query_blocks_delete(self):
        """DELETE should be blocked."""
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="DELETE FROM customers")
    
    def test_sql_query_blocks_drop(self):
        """DROP should be blocked."""
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="DROP TABLE customers")
    
    def test_sql_query_blocks_update(self):
        """UPDATE should be blocked."""
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="UPDATE customers SET name='test'")
    
    def test_sql_query_blocks_insert(self):
        """INSERT should be blocked."""
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="INSERT INTO customers VALUES (1, 'test')")
    
    def test_sql_query_requires_select(self):
        """Non-SELECT queries should be blocked."""
        with pytest.raises(ValueError, match="Only SELECT"):
            SQLQueryInput(query="SHOW TABLES")
    
    def test_sql_query_blocks_sql_injection(self):
        """SQL injection attempts should be blocked."""
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="SELECT * FROM customers; DROP TABLE customers;")
        
        with pytest.raises(ValueError, match="forbidden keyword"):
            SQLQueryInput(query="SELECT * FROM customers -- comment")
    
    def test_search_input_valid(self):
        """Valid search input should pass."""
        search = SearchInput(query="test query", max_results=5)
        assert search.query == "test query"
        assert search.max_results == 5
        assert search.search_depth == "basic"
    
    def test_search_input_limits(self):
        """Search limits should be enforced."""
        with pytest.raises(ValueError):
            SearchInput(query="test", max_results=100)  # Max is 10
    
    def test_weather_input_valid(self):
        """Valid weather input should pass."""
        weather = WeatherInput(latitude=42.3601, longitude=-71.0589, days=3)
        assert weather.latitude == 42.3601
        assert weather.longitude == -71.0589
    
    def test_weather_input_bounds(self):
        """Coordinate bounds should be enforced."""
        with pytest.raises(ValueError):
            WeatherInput(latitude=100, longitude=0)  # Lat max is 90
        
        with pytest.raises(ValueError):
            WeatherInput(latitude=0, longitude=200)  # Lon max is 180
    
    def test_geocoding_input_valid(self):
        """Valid geocoding input should pass."""
        geo = GeocodingInput(location="Boston")
        assert geo.location == "Boston"


class TestAuthentication:
    """Test authentication and authorization."""
    
    def test_role_permissions_admin(self):
        """Admin should have all permissions."""
        auth = Authenticator()
        allowed = auth.get_allowed_tools(UserRole.ADMIN)
        
        assert "sql_query" in allowed
        assert "search" in allowed
        assert "weather" in allowed
        assert "view_audit" in allowed
        assert "manage_users" in allowed
    
    def test_role_permissions_analyst(self):
        """Analyst should have limited permissions."""
        auth = Authenticator()
        allowed = auth.get_allowed_tools(UserRole.ANALYST)
        
        assert "sql_query" in allowed
        assert "search" in allowed
        assert "view_audit" in allowed
        assert "manage_users" not in allowed
    
    def test_role_permissions_readonly(self):
        """Readonly should have minimal permissions."""
        auth = Authenticator()
        allowed = auth.get_allowed_tools(UserRole.READONLY)
        
        assert "sql_query" in allowed
        assert "weather" in allowed
        assert "search" not in allowed
        assert "view_audit" not in allowed


class TestDatabaseSchema:
    """Test database operations."""
    
    def test_limit_enforcement(self):
        """Query limit should be enforced."""
        query = SQLQueryInput(query="SELECT * FROM customers", limit=5)
        assert query.limit == 5
        
        query = SQLQueryInput(query="SELECT * FROM customers", limit=1000)
        assert query.limit == 1000
        
        with pytest.raises(ValueError):
            SQLQueryInput(query="SELECT * FROM customers", limit=2000)


# Integration tests would go here (marked to skip if no DB available)
@pytest.mark.asyncio
class TestToolsIntegration:
    """Integration tests for tools (require running services)."""
    
    @pytest.mark.skip(reason="Requires database")
    async def test_sql_query_execution(self):
        """Test actual SQL query execution."""
        from src.tools.sql_tool import execute_sql_query
        
        result = await execute_sql_query("SELECT COUNT(*) as count FROM customers")
        
        assert result["success"] is True
        assert "rows" in result
        assert len(result["rows"]) > 0
    
    @pytest.mark.skip(reason="Requires API key")
    async def test_web_search(self):
        """Test web search functionality."""
        from src.tools.search_tool import execute_search
        
        result = await execute_search("Python programming")
        
        assert result["success"] is True
        assert len(result["results"]) > 0
    
    @pytest.mark.skip(reason="Requires network")
    async def test_weather_fetch(self):
        """Test weather API."""
        from src.tools.weather_tool import get_weather
        
        # Boston coordinates
        result = await get_weather(latitude=42.3601, longitude=-71.0589)
        
        assert result["success"] is True
        assert len(result["forecast"]) > 0
    
    @pytest.mark.skip(reason="Requires network")
    async def test_geocoding(self):
        """Test geocoding functionality."""
        from src.tools.weather_tool import geocode_location
        
        result = await geocode_location("Boston")
        
        assert result["success"] is True
        assert len(result["results"]) > 0
        assert result["results"][0]["country"] == "United States"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
