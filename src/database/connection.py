"""
Database module for operational data.
Uses SQLite for the demo/operations database.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Optional
import random

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, 
    ForeignKey, create_engine, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import structlog

from ..config import get_settings

Base = declarative_base()
logger = structlog.get_logger()


# ============== Sample Data Models ==============

class Customer(Base):
    """Sample customer table."""
    __tablename__ = "customers"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True)
    company = Column(String(100))
    industry = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    lifetime_value = Column(Float, default=0.0)
    is_active = Column(Boolean, default=True)
    
    orders = relationship("Order", back_populates="customer")


class Product(Base):
    """Sample product table."""
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category = Column(String(50))
    price = Column(Float, nullable=False)
    stock_quantity = Column(Integer, default=0)
    is_available = Column(Boolean, default=True)


class Order(Base):
    """Sample order table."""
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    order_date = Column(DateTime, default=datetime.utcnow)
    total_amount = Column(Float, nullable=False)
    status = Column(String(20), default="pending")
    shipping_city = Column(String(50))
    shipping_country = Column(String(50))
    
    customer = relationship("Customer", back_populates="orders")


class Metric(Base):
    """Sample metrics/KPI table."""
    __tablename__ = "metrics"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    metric_name = Column(String(50), nullable=False)
    value = Column(Float, nullable=False)
    dimension = Column(String(50))


# ============== Database Manager ==============

class OperationsDatabase:
    """
    Manages the SQLite operations database.
    Provides async query execution with safety checks.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._engine = None
        self._async_session = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize database and create sample data."""
        if self._initialized:
            return
        
        # Ensure data directory exists
        db_path = self.settings.sqlite_path
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        
        # Create async engine for SQLite
        self._engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
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
        
        # Populate sample data if empty
        await self._populate_sample_data()
        
        self._initialized = True
        logger.info("Operations database initialized", path=db_path)
    
    async def _populate_sample_data(self) -> None:
        """Populate database with sample data for demos."""
        async with self._async_session() as session:
            # Check if data already exists
            result = await session.execute(text("SELECT COUNT(*) FROM customers"))
            count = result.scalar()
            
            if count > 0:
                return
            
            logger.info("Populating sample data...")
            
            # Sample data
            companies = [
                ("Acme Corp", "Technology"), ("Global Industries", "Manufacturing"),
                ("DataFlow Inc", "Technology"), ("Green Energy Co", "Energy"),
                ("HealthFirst", "Healthcare"), ("FinanceHub", "Finance"),
                ("RetailMax", "Retail"), ("LogiTech Solutions", "Technology"),
                ("BioMed Research", "Healthcare"), ("Urban Development", "Real Estate")
            ]
            
            products = [
                ("Enterprise License", "Software", 999.99),
                ("API Access - Basic", "Software", 49.99),
                ("API Access - Pro", "Software", 199.99),
                ("Consulting Hour", "Services", 250.00),
                ("Data Analysis Package", "Services", 1500.00),
                ("Training Workshop", "Services", 500.00),
                ("Support Plan - Basic", "Support", 99.99),
                ("Support Plan - Premium", "Support", 299.99),
            ]
            
            cities = [
                ("New York", "USA"), ("San Francisco", "USA"), ("London", "UK"),
                ("Berlin", "Germany"), ("Tokyo", "Japan"), ("Sydney", "Australia"),
                ("Toronto", "Canada"), ("Singapore", "Singapore")
            ]
            
            # Create customers
            customers = []
            for i, (company, industry) in enumerate(companies):
                customer = Customer(
                    name=f"Contact {i+1}",
                    email=f"contact{i+1}@{company.lower().replace(' ', '')}.com",
                    company=company,
                    industry=industry,
                    created_at=datetime.utcnow() - timedelta(days=random.randint(30, 365)),
                    lifetime_value=round(random.uniform(1000, 50000), 2),
                    is_active=random.random() > 0.1
                )
                customers.append(customer)
                session.add(customer)
            
            # Create products
            for name, category, price in products:
                product = Product(
                    name=name,
                    category=category,
                    price=price,
                    stock_quantity=random.randint(10, 1000),
                    is_available=True
                )
                session.add(product)
            
            await session.flush()  # Get customer IDs
            
            # Create orders
            statuses = ["completed", "pending", "shipped", "cancelled"]
            for _ in range(50):
                customer = random.choice(customers)
                city, country = random.choice(cities)
                order = Order(
                    customer_id=customer.id,
                    order_date=datetime.utcnow() - timedelta(days=random.randint(1, 90)),
                    total_amount=round(random.uniform(100, 5000), 2),
                    status=random.choice(statuses),
                    shipping_city=city,
                    shipping_country=country
                )
                session.add(order)
            
            # Create metrics
            metric_names = ["daily_revenue", "active_users", "conversion_rate", "churn_rate"]
            for i in range(30):
                date = datetime.utcnow() - timedelta(days=i)
                for metric in metric_names:
                    entry = Metric(
                        date=date,
                        metric_name=metric,
                        value=round(random.uniform(100, 10000) if "revenue" in metric 
                                   else random.uniform(0, 100), 2),
                        dimension="overall"
                    )
                    session.add(entry)
            
            await session.commit()
            logger.info("Sample data populated successfully")
    
    async def execute_query(
        self, 
        query: str, 
        parameters: Optional[dict[str, Any]] = None,
        limit: int = 100
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """
        Execute a SELECT query safely.
        
        Returns:
            Tuple of (column_names, rows)
        """
        if not self._initialized:
            await self.initialize()
        
        async with self._async_session() as session:
            # Add LIMIT if not present
            if "LIMIT" not in query.upper():
                query = f"{query} LIMIT {limit}"
            
            result = await session.execute(text(query), parameters or {})
            
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            
            return columns, rows
    
    async def get_table_schema(self) -> dict[str, list[str]]:
        """Get schema information for all tables."""
        if not self._initialized:
            await self.initialize()
        
        tables = {
            "customers": ["id", "name", "email", "company", "industry", 
                         "created_at", "lifetime_value", "is_active"],
            "products": ["id", "name", "category", "price", 
                        "stock_quantity", "is_available"],
            "orders": ["id", "customer_id", "order_date", "total_amount",
                      "status", "shipping_city", "shipping_country"],
            "metrics": ["id", "date", "metric_name", "value", "dimension"]
        }
        return tables
    
    async def close(self) -> None:
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
            self._initialized = False


# Global instance
_operations_db: Optional[OperationsDatabase] = None


def get_operations_db() -> OperationsDatabase:
    """Get or create operations database instance."""
    global _operations_db
    if _operations_db is None:
        _operations_db = OperationsDatabase()
    return _operations_db
