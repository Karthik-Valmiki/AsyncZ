"""
Driver : asyncpg  (postgresql+asyncpg://)
Session: AsyncSession  (used with async with / async for)
"""

import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

load_dotenv()

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:first123@localhost:5432/asyncT",
)

engine = create_async_engine(
    DATABASE_URL,
    pool_size=20,  # base connections always open
    max_overflow=40,  # extra burst connections allowed
    pool_timeout=30,  # seconds to wait for a connection before raising
    pool_recycle=1800,  # recycle stale connections every 30 min
    echo=False,  # set True for SQL debug logs
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # keeps attributes accessible after commit
)


# Declarative base — shared by all ORM models
class Base(DeclarativeBase):
    pass


# FastAPI dependency — yields an async DB session per request
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
