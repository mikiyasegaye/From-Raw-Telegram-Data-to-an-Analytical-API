{{ config(materialized = 'table') }} with channel_data as (
    select distinct channel,
        count(*) as total_messages,
        count(distinct sender_id) as unique_senders,
        min(message_date) as first_message_date,
        max(message_date) as last_message_date,
        avg(message_length) as avg_message_length,
        sum(
            case
                when has_media then 1
                else 0
            end
        ) as media_messages,
        sum(
            case
                when is_medical_content then 1
                else 0
            end
        ) as medical_content_messages,
        sum(views) as total_views,
        sum(forwards) as total_forwards,
        sum(replies) as total_replies,
        sum(reaction_count) as total_reactions
    from {{ ref('stg_telegram_messages') }}
    group by channel
),
channel_metadata as (
    select channel,
        case
            when channel = 'CheMed123' then 'Medical Supplies and Equipment'
            when channel = 'lobelia4cosmetics' then 'Cosmetics and Beauty Products'
            when channel = 'tikvahpharma' then 'Pharmaceutical Products'
            else 'Other'
        end as channel_category,
        case
            when channel = 'CheMed123' then 'Medical equipment, supplies, and healthcare products'
            when channel = 'lobelia4cosmetics' then 'Cosmetics, beauty products, and skincare items'
            when channel = 'tikvahpharma' then 'Pharmaceutical products and medications'
            else 'General business content'
        end as channel_description,
        case
            when channel in ('CheMed123', 'tikvahpharma') then 'Healthcare'
            when channel = 'lobelia4cosmetics' then 'Beauty'
            else 'Other'
        end as business_sector
    from channel_data
)
select channel as channel_id,
    channel_category,
    channel_description,
    business_sector,
    total_messages,
    unique_senders,
    first_message_date,
    last_message_date,
    round(avg_message_length, 2) as avg_message_length,
    media_messages,
    medical_content_messages,
    total_views,
    total_forwards,
    total_replies,
    total_reactions,
    case
        when total_messages > 0 then round(
            (
                (medical_content_messages::float / total_messages) * 100
            )::numeric,
            2
        )
        else 0
    end as medical_content_percentage,
    case
        when total_messages > 0 then round(
            ((media_messages::float / total_messages) * 100)::numeric,
            2
        )
        else 0
    end as media_percentage,
    current_timestamp as created_at
from channel_data
    join channel_metadata using (channel)