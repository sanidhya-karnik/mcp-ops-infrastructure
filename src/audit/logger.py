"""
Audit logging module.
Provides persistent, queryable audit trail for all tool executions.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Float, String, Boolean, Text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.future import select
import structlog

from ..config import get_settings
from ..schemas.models import AuditEntry, UserRole

Base = declarative_base()
logger = structlog.get_logger()


class AuditLog(Base):
    """SQLAlchemy model for audit log entries."""
    
    __tablename__ = "audit_logs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    user_id = Column(String(100), index=True)
    user_role = Column(String(20))
    tool_name = Column(String(50), index=True)
    input_data = Column(Text)  # JSON serialized
    output_summary = Column(Text)
    success = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)
    execution_time_ms = Column(Float)
    ip_address = Column(String(45), nullable=True)


class AuditLogger:
    """
    Handles audit logging to PostgreSQL.
    Provides both async logging and query capabilities.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._engine = None
        self._async_session = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize database connection and create tables."""
        if self._initialized:
            return
            
        if not self.settings.audit_enabled:
            logger.info("Audit logging disabled")
            return
        
        try:
            # Create async engine
            self._engine = create_async_engine(
                self.settings.postgres_url,
                echo=self.settings.debug
            )
            
            # Create tables
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            # Create session factory
            self._async_session = sessionmaker(
                self._engine, 
                class_=AsyncSession, 
                expire_on_commit=False
            )
            
            self._initialized = True
            logger.info("Audit logger initialized", database=self.settings.postgres_db)
            
        except Exception as e:
            logger.error("Failed to initialize audit logger", error=str(e))
            # Don't raise - audit failures shouldn't break the application
    
    async def log(
        self,
        user_id: str,
        user_role: UserRole,
        tool_name: str,
        input_data: dict[str, Any],
        output_summary: str,
        success: bool,
        execution_time_ms: float,
        error_message: Optional[str] = None,
        ip_address: Optional[str] = None
    ) -> Optional[str]:
        """
        Log a tool execution to the audit trail.
        
        Returns:
            Audit entry ID if successful, None otherwise
        """
        if not self.settings.audit_enabled or not self._initialized:
            return None
        
        entry_id = str(uuid.uuid4())
        
        try:
            # Sanitize input data - remove sensitive fields
            sanitized_input = self._sanitize_input(input_data)
            
            audit_entry = AuditLog(
                id=entry_id,
                timestamp=datetime.utcnow(),
                user_id=user_id,
                user_role=user_role.value,
                tool_name=tool_name,
                input_data=json.dumps(sanitized_input),
                output_summary=output_summary[:1000],  # Truncate
                success=success,
                error_message=error_message,
                execution_time_ms=execution_time_ms,
                ip_address=ip_address
            )
            
            async with self._async_session() as session:
                session.add(audit_entry)
                await session.commit()
            
            logger.info(
                "Audit logged",
                audit_id=entry_id,
                tool=tool_name,
                user=user_id,
                success=success
            )
            
            return entry_id
            
        except Exception as e:
            logger.error("Audit logging failed", error=str(e))
            return None
    
    def _sanitize_input(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """Remove sensitive data from input before logging."""
        sensitive_keys = {'password', 'api_key', 'token', 'secret', 'credential'}
        
        sanitized = {}
        for key, value in input_data.items():
            if any(s in key.lower() for s in sensitive_keys):
                sanitized[key] = "[REDACTED]"
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_input(value)
            else:
                sanitized[key] = value
        
        return sanitized
    
    async def query(
        self,
        user_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        success_only: bool = False,
        limit: int = 100
    ) -> list[AuditEntry]:
        """Query audit logs with filters."""
        if not self._initialized:
            return []
        
        try:
            async with self._async_session() as session:
                query = select(AuditLog).order_by(AuditLog.timestamp.desc())
                
                if user_id:
                    query = query.where(AuditLog.user_id == user_id)
                if tool_name:
                    query = query.where(AuditLog.tool_name == tool_name)
                if start_time:
                    query = query.where(AuditLog.timestamp >= start_time)
                if end_time:
                    query = query.where(AuditLog.timestamp <= end_time)
                if success_only:
                    query = query.where(AuditLog.success == True)
                
                query = query.limit(limit)
                
                result = await session.execute(query)
                logs = result.scalars().all()
                
                return [
                    AuditEntry(
                        id=log.id,
                        timestamp=log.timestamp,
                        user_id=log.user_id,
                        user_role=UserRole(log.user_role),
                        tool_name=log.tool_name,
                        input_data=json.loads(log.input_data),
                        output_summary=log.output_summary,
                        success=log.success,
                        error_message=log.error_message,
                        execution_time_ms=log.execution_time_ms,
                        ip_address=log.ip_address
                    )
                    for log in logs
                ]
                
        except Exception as e:
            logger.error("Audit query failed", error=str(e))
            return []
    
    async def close(self) -> None:
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
            self._initialized = False


# Global audit logger instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """Get or create audit logger instance."""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger
