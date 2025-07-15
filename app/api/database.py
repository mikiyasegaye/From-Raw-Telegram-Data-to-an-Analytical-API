import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Generator

from app.core.config import settings

logger = logging.getLogger(__name__)

# Create database engine
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False  # Set to True for SQL query logging
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db() -> Generator:
    """
    Database session context manager.
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()

async def test_database_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        with get_db() as db:
            result = db.execute(text("SELECT 1"))
            result.fetchone()
            return True
    except SQLAlchemyError as e:
        logger.error(f"Database connection test failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database test: {e}")
        return False

async def get_database_stats() -> dict:
    """
    Get basic database statistics.
    
    Returns:
        dict: Database statistics
    """
    try:
        with get_db() as db:
            # Get table row counts
            stats = {}
            
            # Check if dbt models exist
            tables_to_check = [
                'fct_messages',
                'dim_channels', 
                'dim_dates',
                'stg_telegram_messages'
            ]
            
            for table in tables_to_check:
                try:
                    result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    stats[f"{table}_count"] = count
                except SQLAlchemyError:
                    stats[f"{table}_count"] = 0
                    logger.warning(f"Table {table} not found or not accessible")
            
            return stats
            
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return {"error": str(e)} 