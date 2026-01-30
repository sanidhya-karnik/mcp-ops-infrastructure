"""
Configuration management using Pydantic Settings.
Loads from environment variables with validation.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Server Configuration
    server_name: str = Field(default="mcp-ops-server", description="MCP server name")
    server_version: str = Field(default="1.0.0")
    debug: bool = Field(default=False)
    
    # API Keys
    tavily_api_key: Optional[str] = Field(default=None, description="Tavily search API key")
    
    # Auth Configuration
    auth_enabled: bool = Field(default=True, description="Enable authentication")
    api_keys: str = Field(
        default="",
        description="Comma-separated list of valid API keys with roles (key:role,key:role)"
    )
    
    # Database Configuration
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_user: str = Field(default="mcp_user")
    postgres_password: str = Field(default="mcp_password")
    postgres_db: str = Field(default="mcp_audit")
    
    sqlite_path: str = Field(default="data/operations.db", description="SQLite DB for operations data")
    
    # Rate Limiting
    rate_limit_requests: int = Field(default=100, description="Requests per window")
    rate_limit_window_seconds: int = Field(default=60)
    
    # Audit Configuration
    audit_enabled: bool = Field(default=True)
    audit_retention_days: int = Field(default=90)
    
    @property
    def postgres_url(self) -> str:
        """Construct PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def postgres_url_sync(self) -> str:
        """Construct synchronous PostgreSQL URL (for migrations)."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    def get_api_key_roles(self) -> dict[str, str]:
        """Parse API keys and their roles."""
        if not self.api_keys:
            return {}
        
        result = {}
        for pair in self.api_keys.split(","):
            pair = pair.strip()
            if ":" in pair:
                key, role = pair.split(":", 1)
                result[key.strip()] = role.strip()
        return result


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
