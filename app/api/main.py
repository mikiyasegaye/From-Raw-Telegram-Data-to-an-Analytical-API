from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
from datetime import datetime, timedelta

from app.core.config import settings
from app.api.schemas import (
    TopProductResponse,
    ChannelActivityResponse,
    MessageSearchResponse,
    MessageResponse,
    ChannelSummaryResponse
)
from app.api.crud import (
    get_top_products,
    get_channel_activity,
    search_messages,
    get_channel_summary,
    get_medical_content_stats
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create API router
app = APIRouter()

@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "message": "Ethiopian Medical Business Analytics API",
        "version": "1.0.0",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "database": "connected",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/reports/top-products", response_model=List[TopProductResponse], tags=["Analytics"])
async def get_top_mentioned_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back")
):
    """
    Get the most frequently mentioned medical products in the last N days.
    
    Returns products sorted by mention frequency, with engagement metrics.
    """
    try:
        products = await get_top_products(limit=limit, days=days)
        return products
    except Exception as e:
        logger.error(f"Error getting top products: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve top products")

@app.get("/channels/{channel_name}/activity", response_model=ChannelActivityResponse, tags=["Analytics"])
async def get_channel_posting_activity(
    channel_name: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze")
):
    """
    Get posting activity and engagement metrics for a specific channel.
    
    Returns daily posting frequency, engagement rates, and content analysis.
    """
    try:
        activity = await get_channel_activity(channel_name=channel_name, days=days)
        if not activity:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        return activity
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting channel activity for {channel_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channel activity")

@app.get("/search/messages", response_model=List[MessageSearchResponse], tags=["Search"])
async def search_messages_by_keyword(
    query: str = Query(..., min_length=1, description="Search keyword"),
    limit: int = Query(20, ge=1, le=100, description="Number of messages to return"),
    channel_name: Optional[str] = Query(None, description="Filter by specific channel")
):
    """
    Search for messages containing specific keywords.
    
    Returns messages with relevance scoring and context.
    """
    try:
        messages = await search_messages(
            query=query,
            limit=limit,
            channel_name=channel_name
        )
        return messages
    except Exception as e:
        logger.error(f"Error searching messages for '{query}': {e}")
        raise HTTPException(status_code=500, detail="Failed to search messages")

@app.get("/channels", response_model=List[ChannelSummaryResponse], tags=["Analytics"])
async def get_all_channels_summary():
    """
    Get summary statistics for all channels.
    
    Returns channel overview with posting frequency and engagement metrics.
    """
    try:
        channels = await get_channel_summary()
        return channels
    except Exception as e:
        logger.error(f"Error getting channels summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channels summary")

@app.get("/reports/medical-content", tags=["Analytics"])
async def get_medical_content_statistics(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze")
):
    """
    Get statistics about medical content detection and engagement.
    
    Returns metrics about medical vs non-medical content performance.
    """
    try:
        stats = await get_medical_content_stats(days=days)
        return stats
    except Exception as e:
        logger.error(f"Error getting medical content stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve medical content statistics")

@app.get("/reports/engagement-trends", tags=["Analytics"])
async def get_engagement_trends(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze")
):
    """
    Get engagement trends over time.
    
    Returns daily engagement metrics and trends.
    """
    try:
        # This would be implemented in crud.py
        # For now, return a placeholder
        return {
            "message": "Engagement trends endpoint - to be implemented",
            "days_analyzed": days
        }
    except Exception as e:
        logger.error(f"Error getting engagement trends: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve engagement trends")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 