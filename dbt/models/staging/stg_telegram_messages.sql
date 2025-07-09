{{ config(materialized = 'view') }} with source as (
    select *
    from {{ source('raw', 'telegram_messages') }}
),
cleaned as (
    select -- Primary key
        id as message_id,
        -- Message content
        message,
        case
            when message is null
            or message = '' then 0
            else length(message)
        end as message_length,
        -- Timestamps
        date as message_date,
        date_trunc('day', date) as message_date_day,
        date_trunc('week', date) as message_date_week,
        date_trunc('month', date) as message_date_month,
        -- Sender information
        sender_id,
        sender_username,
        coalesce(sender_first_name, '') as sender_first_name,
        coalesce(sender_last_name, '') as sender_last_name,
        case
            when sender_first_name is not null
            and sender_last_name is not null then sender_first_name || ' ' || sender_last_name
            when sender_first_name is not null then sender_first_name
            when sender_last_name is not null then sender_last_name
            else 'Unknown'
        end as sender_full_name,
        -- Message relationships
        reply_to_msg_id,
        forward_from_id,
        forward_from_name,
        -- Media information
        has_media,
        media_type,
        media_filename,
        media_path,
        -- Engagement metrics
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards,
        coalesce(replies, 0) as replies,
        -- Reactions (parsed from JSON)
        reactions,
        case
            when reactions is not null then jsonb_array_length(reactions)
            else 0
        end as reaction_count,
        -- Channel and extraction info
        channel,
        extraction_date,
        created_at,
        -- Derived fields
        case
            when has_media = true then 'media'
            when message is not null
            and message != '' then 'text'
            else 'other'
        end as message_type,
        case
            when message is not null
            and message != '' then true
            else false
        end as has_text,
        -- Business logic for medical/pharmaceutical content
        case
            when lower(message) like '%medicine%'
            or lower(message) like '%drug%'
            or lower(message) like '%pharma%'
            or lower(message) like '%medication%'
            or lower(message) like '%prescription%'
            or lower(message) like '%treatment%'
            or lower(message) like '%symptom%'
            or lower(message) like '%disease%'
            or lower(message) like '%health%'
            or lower(message) like '%medical%'
            or lower(message) like '%doctor%'
            or lower(message) like '%hospital%'
            or lower(message) like '%clinic%'
            or lower(message) like '%pharmacy%'
            or lower(message) like '%cosmetic%'
            or lower(message) like '%beauty%'
            or lower(message) like '%skincare%'
            or lower(message) like '%supplement%'
            or lower(message) like '%vitamin%' then true
            else false
        end as is_medical_content
    from source
    where id is not null -- Ensure we have valid message IDs
)
select *
from cleaned