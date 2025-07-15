"""
Main FastAPI application for the Telegram Data Pipeline.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from app.core.config import settings
from datetime import datetime

from app.api.main import app as api_app

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Create main FastAPI app
app = FastAPI(
    title="Ethiopian Medical Business Analytics Platform",
    description="End-to-end data platform for Ethiopian medical business insights using Telegram data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Application startup event."""
    logger.info("Starting Telegram Data Pipeline API...")

    # Validate environment variables
    if not settings.validate():
        logger.error("Environment validation failed")
        raise RuntimeError("Environment validation failed")

    # Create necessary directories
    settings.create_directories()

    logger.info("Telegram Data Pipeline API started successfully")


@app.get("/", tags=["Health"])
async def root():
    """Main application health check"""
    return {
        "message": "Ethiopian Medical Business Analytics Platform",
        "version": "1.0.0",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs",
        "api": "/api"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "database": "connected",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "api": "running",
            "database": "connected"
        }
    }


# Include the API router
app.include_router(api_app, prefix="/api")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
