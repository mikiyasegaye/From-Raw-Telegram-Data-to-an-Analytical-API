{{ config(materialized = 'table') }} with messages as (
    select *
    from {{ ref('stg_telegram_messages') }}
),
dim_channels as (
    select *
    from {{ ref('dim_channels') }}
),
dim_dates as (
    select *
    from {{ ref('dim_dates') }}
)
select -- Primary key
    m.message_id,
    -- Foreign keys
    m.channel as channel_id,
    m.message_date_day as date_id,
    -- Message content and metadata
    m.message,
    m.message_length,
    m.message_type,
    m.has_text,
    m.has_media,
    m.media_type,
    m.media_filename,
    m.media_path,
    m.is_medical_content,
    -- Sender information
    m.sender_id,
    m.sender_username,
    m.sender_first_name,
    m.sender_last_name,
    m.sender_full_name,
    -- Message relationships
    m.reply_to_msg_id,
    m.forward_from_id,
    m.forward_from_name,
    -- Engagement metrics
    m.views,
    m.forwards,
    m.replies,
    m.reaction_count,
    m.reactions,
    -- Timestamps
    m.message_date,
    m.extraction_date,
    m.created_at,
    -- Channel dimension attributes
    dc.channel_category,
    dc.channel_description,
    dc.business_sector,
    -- Date dimension attributes
    dd.year,
    dd.month,
    dd.day,
    dd.day_of_week,
    dd.day_name,
    dd.month_name,
    dd.year_month,
    dd.is_weekend,
    dd.season,
    dd.quarter,
    -- Derived metrics
    case
        when m.views > 0 then m.views
        else 0
    end as engagement_score,
    case
        when m.message_length > 0 then m.message_length
        else 0
    end as content_score,
    case
        when m.is_medical_content then 1
        else 0
    end as medical_content_flag,
    case
        when m.has_media then 1
        else 0
    end as media_flag,
    -- Business metrics
    case
        when m.views > 0
        and m.forwards > 0 then (
            m.views + m.forwards * 2 + m.replies * 3 + m.reaction_count * 2
        )
        when m.views > 0 then (m.views + m.replies * 3 + m.reaction_count * 2)
        else (m.replies * 3 + m.reaction_count * 2)
    end as viral_score,
    case
        when m.is_medical_content
        and m.has_media then 'Medical Media'
        when m.is_medical_content then 'Medical Text'
        when m.has_media then 'Non-Medical Media'
        else 'Non-Medical Text'
    end as content_category
from messages m
    left join dim_channels dc on m.channel = dc.channel_id
    left join dim_dates dd on m.message_date_day = dd.date_id