from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class TopProductResponse(BaseModel):
    """Schema for top mentioned products"""
    product_name: str = Field(..., description="Name of the medical product")
    mention_count: int = Field(..., description="Number of times mentioned")
    total_engagement: int = Field(..., description="Total engagement (views + forwards + replies)")
    avg_engagement: float = Field(..., description="Average engagement per mention")
    channels: List[str] = Field(..., description="Channels where product was mentioned")
    last_mentioned: Optional[datetime] = Field(None, description="Last time product was mentioned")

class ChannelActivityResponse(BaseModel):
    """Schema for channel activity data"""
    channel_name: str = Field(..., description="Name of the channel")
    total_messages: int = Field(..., description="Total messages in the period")
    avg_daily_posts: float = Field(..., description="Average daily posts")
    total_engagement: int = Field(..., description="Total engagement")
    avg_engagement_per_post: float = Field(..., description="Average engagement per post")
    medical_content_percentage: float = Field(..., description="Percentage of medical content")
    top_products: List[str] = Field(..., description="Top mentioned products")
    activity_trend: List[Dict[str, Any]] = Field(..., description="Daily activity trend")
    period_days: int = Field(..., description="Number of days analyzed")

class MessageSearchResponse(BaseModel):
    """Schema for message search results"""
    message_id: int = Field(..., description="Unique message ID")
    channel_name: str = Field(..., description="Channel where message was posted")
    message_text: str = Field(..., description="Message content")
    posted_date: datetime = Field(..., description="When message was posted")
    engagement_score: int = Field(..., description="Total engagement (views + forwards + replies)")
    relevance_score: float = Field(..., description="Relevance score for search query")
    has_media: bool = Field(..., description="Whether message contains media")
    is_medical_content: bool = Field(..., description="Whether content is medical-related")

class MessageResponse(BaseModel):
    """Schema for individual message data"""
    message_id: int = Field(..., description="Unique message ID")
    channel_name: str = Field(..., description="Channel name")
    message_text: str = Field(..., description="Message content")
    posted_date: datetime = Field(..., description="Posting date")
    views: int = Field(..., description="Number of views")
    forwards: int = Field(..., description="Number of forwards")
    replies: int = Field(..., description="Number of replies")
    engagement_score: int = Field(..., description="Total engagement score")
    is_medical_content: bool = Field(..., description="Medical content flag")
    has_media: bool = Field(..., description="Media presence flag")

class ChannelSummaryResponse(BaseModel):
    """Schema for channel summary data"""
    channel_name: str = Field(..., description="Channel name")
    total_messages: int = Field(..., description="Total messages")
    total_engagement: int = Field(..., description="Total engagement")
    avg_engagement_per_post: float = Field(..., description="Average engagement per post")
    medical_content_percentage: float = Field(..., description="Percentage of medical content")
    last_activity: Optional[datetime] = Field(None, description="Last activity date")
    top_products: List[str] = Field(..., description="Top mentioned products")

class MedicalContentStatsResponse(BaseModel):
    """Schema for medical content statistics"""
    total_messages: int = Field(..., description="Total messages analyzed")
    medical_messages: int = Field(..., description="Messages with medical content")
    non_medical_messages: int = Field(..., description="Messages without medical content")
    medical_percentage: float = Field(..., description="Percentage of medical content")
    avg_engagement_medical: float = Field(..., description="Average engagement for medical content")
    avg_engagement_non_medical: float = Field(..., description="Average engagement for non-medical content")
    top_medical_products: List[str] = Field(..., description="Top mentioned medical products")
    period_days: int = Field(..., description="Analysis period in days")

class EngagementTrendResponse(BaseModel):
    """Schema for engagement trends"""
    daily_trends: List[Dict[str, Any]] = Field(..., description="Daily engagement trends")
    weekly_averages: List[Dict[str, Any]] = Field(..., description="Weekly average engagement")
    period_days: int = Field(..., description="Analysis period in days")

class ErrorResponse(BaseModel):
    """Schema for error responses"""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(default_factory=datetime.now, description="Error timestamp")

class SearchRequest(BaseModel):
    """Schema for search requests"""
    query: str = Field(..., min_length=1, description="Search query")
    limit: int = Field(20, ge=1, le=100, description="Maximum number of results")
    channel_name: Optional[str] = Field(None, description="Filter by channel")
    date_from: Optional[datetime] = Field(None, description="Start date for search")
    date_to: Optional[datetime] = Field(None, description="End date for search") 