"""
Authentication and authorization module.
Implements token-based auth with role-based permissions.
"""

import hashlib
import secrets
from datetime import datetime
from functools import wraps
from typing import Callable, Optional

from ..config import get_settings
from ..schemas.models import AuthContext, UserRole, ToolError


class AuthenticationError(Exception):
    """Raised when authentication fails."""
    pass


class AuthorizationError(Exception):
    """Raised when user lacks permission for an action."""
    pass


class Authenticator:
    """Handles API key authentication and role-based authorization."""
    
    # Define permissions per role
    ROLE_PERMISSIONS: dict[UserRole, set[str]] = {
        UserRole.ADMIN: {
            "sql_query", "search", "weather", "geocoding",
            "view_audit", "manage_users"
        },
        UserRole.ANALYST: {
            "sql_query", "search", "weather", "geocoding",
            "view_audit"
        },
        UserRole.READONLY: {
            "sql_query", "weather", "geocoding"
        },
    }
    
    def __init__(self):
        self.settings = get_settings()
        self._api_key_cache: dict[str, tuple[str, UserRole]] = {}
        self._load_api_keys()
    
    def _load_api_keys(self) -> None:
        """Load and hash API keys from configuration."""
        key_roles = self.settings.get_api_key_roles()
        
        for key, role_str in key_roles.items():
            try:
                role = UserRole(role_str.lower())
                key_hash = self._hash_key(key)
                self._api_key_cache[key_hash] = (key[:8] + "...", role)
            except ValueError:
                # Skip invalid roles
                pass
    
    @staticmethod
    def _hash_key(api_key: str) -> str:
        """Hash API key for secure storage/comparison."""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    def authenticate(self, api_key: Optional[str]) -> AuthContext:
        """
        Authenticate request using API key.
        
        Args:
            api_key: The API key from request header
            
        Returns:
            AuthContext with user info and permissions
            
        Raises:
            AuthenticationError: If authentication fails
        """
        if not self.settings.auth_enabled:
            # Auth disabled - return default admin context
            return AuthContext(
                user_id="anonymous",
                role=UserRole.ADMIN,
                api_key_hash="disabled",
                authenticated_at=datetime.utcnow()
            )
        
        if not api_key:
            raise AuthenticationError("API key required")
        
        key_hash = self._hash_key(api_key)
        
        if key_hash not in self._api_key_cache:
            raise AuthenticationError("Invalid API key")
        
        masked_key, role = self._api_key_cache[key_hash]
        
        return AuthContext(
            user_id=masked_key,  # Use masked key as user ID
            role=role,
            api_key_hash=key_hash[:16],  # Truncated for audit
            authenticated_at=datetime.utcnow()
        )
    
    def authorize(self, auth_context: AuthContext, tool_name: str) -> bool:
        """
        Check if user is authorized for a specific tool.
        
        Args:
            auth_context: Authenticated user context
            tool_name: Name of the tool being accessed
            
        Returns:
            True if authorized
            
        Raises:
            AuthorizationError: If user lacks permission
        """
        allowed_tools = self.ROLE_PERMISSIONS.get(auth_context.role, set())
        
        if tool_name not in allowed_tools:
            raise AuthorizationError(
                f"Role '{auth_context.role.value}' cannot access tool '{tool_name}'"
            )
        
        return True
    
    def get_allowed_tools(self, role: UserRole) -> set[str]:
        """Get set of tools allowed for a role."""
        return self.ROLE_PERMISSIONS.get(role, set())
    
    @staticmethod
    def generate_api_key() -> str:
        """Generate a secure random API key."""
        return secrets.token_urlsafe(32)


# Global authenticator instance
_authenticator: Optional[Authenticator] = None


def get_authenticator() -> Authenticator:
    """Get or create authenticator instance."""
    global _authenticator
    if _authenticator is None:
        _authenticator = Authenticator()
    return _authenticator


def require_auth(tool_name: str):
    """
    Decorator to enforce authentication and authorization on tools.
    
    Usage:
        @require_auth("sql_query")
        async def sql_query_tool(auth: AuthContext, input: SQLQueryInput):
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, api_key: Optional[str] = None, **kwargs):
            authenticator = get_authenticator()
            
            try:
                auth_context = authenticator.authenticate(api_key)
                authenticator.authorize(auth_context, tool_name)
            except (AuthenticationError, AuthorizationError) as e:
                return ToolError(
                    code="AUTH_ERROR",
                    message=str(e)
                ).model_dump()
            
            # Inject auth context into function
            return await func(*args, auth=auth_context, **kwargs)
        
        return wrapper
    return decorator
