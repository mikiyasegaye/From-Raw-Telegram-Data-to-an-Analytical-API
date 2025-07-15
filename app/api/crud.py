import logging
from sqlalchemy import text, func
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import re

from app.api.database import get_db
from app.api.schemas import (
    TopProductResponse,
    ChannelActivityResponse,
    MessageSearchResponse,
    ChannelSummaryResponse
)

logger = logging.getLogger(__name__)

async def get_top_products(limit: int = 10, days: int = 30) -> List[TopProductResponse]:
    """
    Get the most frequently mentioned medical products.
    
    Args:
        limit: Number of products to return
        days: Number of days to look back
        
    Returns:
        List of top products with engagement metrics
    """
    try:
        with get_db() as db:
            # Query the fact table for product mentions
            query = text("""
                WITH product_mentions AS (
                    SELECT 
                        message_text,
                        channel_name,
                        posted_date,
                        engagement_score,
                        is_medical_content
                    FROM fct_messages 
                    WHERE posted_date >= CURRENT_DATE - INTERVAL ':days days'
                    AND is_medical_content = true
                ),
                extracted_products AS (
                    SELECT 
                        message_text,
                        channel_name,
                        posted_date,
                        engagement_score,
                        -- Extract potential product names (simplified)
                        CASE 
                            WHEN message_text ILIKE '%paracetamol%' THEN 'Paracetamol'
                            WHEN message_text ILIKE '%amoxicillin%' THEN 'Amoxicillin'
                            WHEN message_text ILIKE '%vitamin%' THEN 'Vitamin'
                            WHEN message_text ILIKE '%antibiotic%' THEN 'Antibiotic'
                            WHEN message_text ILIKE '%painkiller%' THEN 'Painkiller'
                            WHEN message_text ILIKE '%syrup%' THEN 'Syrup'
                            WHEN message_text ILIKE '%tablet%' THEN 'Tablet'
                            WHEN message_text ILIKE '%capsule%' THEN 'Capsule'
                            WHEN message_text ILIKE '%injection%' THEN 'Injection'
                            WHEN message_text ILIKE '%cream%' THEN 'Cream'
                            ELSE 'Other Medical Product'
                        END as product_name
                    FROM product_mentions
                    WHERE message_text ~* '(paracetamol|amoxicillin|vitamin|antibiotic|painkiller|syrup|tablet|capsule|injection|cream)'
                )
                SELECT 
                    product_name,
                    COUNT(*) as mention_count,
                    SUM(engagement_score) as total_engagement,
                    AVG(engagement_score) as avg_engagement,
                    ARRAY_AGG(DISTINCT channel_name) as channels,
                    MAX(posted_date) as last_mentioned
                FROM extracted_products
                WHERE product_name != 'Other Medical Product'
                GROUP BY product_name
                ORDER BY mention_count DESC, total_engagement DESC
                LIMIT :limit
            """)
            
            result = db.execute(query, {"limit": limit, "days": days})
            rows = result.fetchall()
            
            products = []
            for row in rows:
                products.append(TopProductResponse(
                    product_name=row.product_name,
                    mention_count=row.mention_count,
                    total_engagement=row.total_engagement,
                    avg_engagement=float(row.avg_engagement) if row.avg_engagement else 0.0,
                    channels=row.channels,
                    last_mentioned=row.last_mentioned
                ))
            
            return products
            
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_top_products: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in get_top_products: {e}")
        raise

async def get_channel_activity(channel_name: str, days: int = 30) -> Optional[ChannelActivityResponse]:
    """
    Get posting activity and engagement metrics for a specific channel.
    
    Args:
        channel_name: Name of the channel
        days: Number of days to analyze
        
    Returns:
        Channel activity data or None if channel not found
    """
    try:
        with get_db() as db:
            # Get channel activity data
            query = text("""
                WITH channel_data AS (
                    SELECT 
                        channel_name,
                        posted_date,
                        engagement_score,
                        is_medical_content,
                        message_text
                    FROM fct_messages 
                    WHERE channel_name = :channel_name
                    AND posted_date >= CURRENT_DATE - INTERVAL ':days days'
                ),
                daily_stats AS (
                    SELECT 
                        DATE(posted_date) as date,
                        COUNT(*) as posts,
                        SUM(engagement_score) as daily_engagement
                    FROM channel_data
                    GROUP BY DATE(posted_date)
                )
                SELECT 
                    :channel_name as channel_name,
                    COUNT(*) as total_messages,
                    COUNT(*)::float / :days as avg_daily_posts,
                    SUM(engagement_score) as total_engagement,
                    AVG(engagement_score) as avg_engagement_per_post,
                    (COUNT(*) FILTER (WHERE is_medical_content = true)::float / COUNT(*) * 100) as medical_content_percentage,
                    ARRAY_AGG(DISTINCT 
                        CASE 
                            WHEN message_text ILIKE '%paracetamol%' THEN 'Paracetamol'
                            WHEN message_text ILIKE '%amoxicillin%' THEN 'Amoxicillin'
                            WHEN message_text ILIKE '%vitamin%' THEN 'Vitamin'
                            ELSE 'Other'
                        END
                    ) FILTER (WHERE message_text ~* '(paracetamol|amoxicillin|vitamin)') as top_products
                FROM channel_data
            """)
            
            result = db.execute(query, {"channel_name": channel_name, "days": days})
            row = result.fetchone()
            
            if not row:
                return None
            
            # Get daily activity trend
            trend_query = text("""
                SELECT 
                    DATE(posted_date) as date,
                    COUNT(*) as posts,
                    SUM(engagement_score) as engagement
                FROM fct_messages 
                WHERE channel_name = :channel_name
                AND posted_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY DATE(posted_date)
                ORDER BY date
            """)
            
            trend_result = db.execute(trend_query, {"channel_name": channel_name, "days": days})
            trend_rows = trend_result.fetchall()
            
            activity_trend = [
                {
                    "date": row.date.isoformat(),
                    "posts": row.posts,
                    "engagement": row.engagement
                }
                for row in trend_rows
            ]
            
            return ChannelActivityResponse(
                channel_name=row.channel_name,
                total_messages=row.total_messages,
                avg_daily_posts=float(row.avg_daily_posts),
                total_engagement=row.total_engagement,
                avg_engagement_per_post=float(row.avg_engagement_per_post),
                medical_content_percentage=float(row.medical_content_percentage),
                top_products=[p for p in row.top_products if p != 'Other'],
                activity_trend=activity_trend,
                period_days=days
            )
            
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_channel_activity: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in get_channel_activity: {e}")
        raise

async def search_messages(
    query: str, 
    limit: int = 20, 
    channel_name: Optional[str] = None
) -> List[MessageSearchResponse]:
    """
    Search for messages containing specific keywords.
    
    Args:
        query: Search keyword
        limit: Maximum number of results
        channel_name: Optional channel filter
        
    Returns:
        List of matching messages
    """
    try:
        with get_db() as db:
            # Build the search query
            base_query = """
                SELECT 
                    message_id,
                    channel_name,
                    message_text,
                    posted_date,
                    engagement_score,
                    has_media,
                    is_medical_content,
                    -- Simple relevance scoring
                    CASE 
                        WHEN message_text ILIKE :exact_query THEN 1.0
                        WHEN message_text ILIKE :partial_query THEN 0.7
                        ELSE 0.3
                    END as relevance_score
                FROM fct_messages 
                WHERE message_text ILIKE :search_pattern
            """
            
            params = {
                "exact_query": query,
                "partial_query": f"%{query}%",
                "search_pattern": f"%{query}%",
                "limit": limit
            }
            
            if channel_name:
                base_query += " AND channel_name = :channel_name"
                params["channel_name"] = channel_name
            
            base_query += """
                ORDER BY relevance_score DESC, engagement_score DESC, posted_date DESC
                LIMIT :limit
            """
            
            result = db.execute(text(base_query), params)
            rows = result.fetchall()
            
            messages = []
            for row in rows:
                messages.append(MessageSearchResponse(
                    message_id=row.message_id,
                    channel_name=row.channel_name,
                    message_text=row.message_text,
                    posted_date=row.posted_date,
                    engagement_score=row.engagement_score,
                    relevance_score=float(row.relevance_score),
                    has_media=row.has_media,
                    is_medical_content=row.is_medical_content
                ))
            
            return messages
            
    except SQLAlchemyError as e:
        logger.error(f"Database error in search_messages: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in search_messages: {e}")
        raise

async def get_channel_summary() -> List[ChannelSummaryResponse]:
    """
    Get summary statistics for all channels.
    
    Returns:
        List of channel summaries
    """
    try:
        with get_db() as db:
            query = text("""
                SELECT 
                    channel_name,
                    COUNT(*) as total_messages,
                    SUM(engagement_score) as total_engagement,
                    AVG(engagement_score) as avg_engagement_per_post,
                    (COUNT(*) FILTER (WHERE is_medical_content = true)::float / COUNT(*) * 100) as medical_content_percentage,
                    MAX(posted_date) as last_activity,
                    ARRAY_AGG(DISTINCT 
                        CASE 
                            WHEN message_text ILIKE '%paracetamol%' THEN 'Paracetamol'
                            WHEN message_text ILIKE '%amoxicillin%' THEN 'Amoxicillin'
                            WHEN message_text ILIKE '%vitamin%' THEN 'Vitamin'
                            ELSE 'Other'
                        END
                    ) FILTER (WHERE message_text ~* '(paracetamol|amoxicillin|vitamin)') as top_products
                FROM fct_messages
                GROUP BY channel_name
                ORDER BY total_engagement DESC
            """)
            
            result = db.execute(query)
            rows = result.fetchall()
            
            channels = []
            for row in rows:
                channels.append(ChannelSummaryResponse(
                    channel_name=row.channel_name,
                    total_messages=row.total_messages,
                    total_engagement=row.total_engagement,
                    avg_engagement_per_post=float(row.avg_engagement_per_post),
                    medical_content_percentage=float(row.medical_content_percentage),
                    last_activity=row.last_activity,
                    top_products=[p for p in row.top_products if p != 'Other']
                ))
            
            return channels
            
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_channel_summary: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in get_channel_summary: {e}")
        raise

async def get_medical_content_stats(days: int = 30) -> Dict[str, Any]:
    """
    Get statistics about medical content detection and engagement.
    
    Args:
        days: Number of days to analyze
        
    Returns:
        Dictionary with medical content statistics
    """
    try:
        with get_db() as db:
            query = text("""
                SELECT 
                    COUNT(*) as total_messages,
                    COUNT(*) FILTER (WHERE is_medical_content = true) as medical_messages,
                    COUNT(*) FILTER (WHERE is_medical_content = false) as non_medical_messages,
                    AVG(engagement_score) FILTER (WHERE is_medical_content = true) as avg_engagement_medical,
                    AVG(engagement_score) FILTER (WHERE is_medical_content = false) as avg_engagement_non_medical
                FROM fct_messages 
                WHERE posted_date >= CURRENT_DATE - INTERVAL ':days days'
            """)
            
            result = db.execute(query, {"days": days})
            row = result.fetchone()
            
            if not row:
                return {"error": "No data found"}
            
            total_messages = row.total_messages
            medical_messages = row.medical_messages
            medical_percentage = (medical_messages / total_messages * 100) if total_messages > 0 else 0
            
            return {
                "total_messages": total_messages,
                "medical_messages": medical_messages,
                "non_medical_messages": row.non_medical_messages,
                "medical_percentage": round(medical_percentage, 2),
                "avg_engagement_medical": float(row.avg_engagement_medical) if row.avg_engagement_medical else 0.0,
                "avg_engagement_non_medical": float(row.avg_engagement_non_medical) if row.avg_engagement_non_medical else 0.0,
                "period_days": days
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error in get_medical_content_stats: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in get_medical_content_stats: {e}")
        raise 