"""
Main FastAPI application for the Telegram Data Pipeline.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from app.core.config import settings

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Telegram Data Pipeline API",
    description="Analytics API for Ethiopian medical business insights from Telegram data",
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


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Telegram Data Pipeline API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Add basic health checks here
        return {
            "status": "healthy",
            "environment": settings.ENVIRONMENT,
            "database_url": settings.DATABASE_URL.split("@")[1] if "@" in settings.DATABASE_URL else "configured"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


@app.get("/api/health")
async def api_health():
    """API health check endpoint."""
    return await health_check()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
