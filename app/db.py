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

# Pure SQLAlchemy defaults for connection pools
pool_size = int(os.getenv("DB_POOL_SIZE", "20"))
max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))

engine = create_async_engine(
    DATABASE_URL,
    pool_size=pool_size,
    max_overflow=max_overflow,
    pool_timeout=30,
    pool_recycle=1800,
    echo=False,
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


# FastAPI dependency — yields an async DB session per request.
# Commit is NOT called here — the endpoint owns the commit so it can
# control exactly when the DB write is finalised before the Redis push.
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
