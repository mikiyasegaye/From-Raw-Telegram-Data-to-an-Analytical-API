{{config(materialized = 'table') }} with date_spine as (
    select date_series::date as date_id,
        date_series as full_date,
        extract(
            year
            from date_series
        ) as year,
        extract(
            month
            from date_series
        ) as month,
        extract(
            day
            from date_series
        ) as day,
        extract(
            dow
            from date_series
        ) as day_of_week,
        extract(
            doy
            from date_series
        ) as day_of_year,
        to_char(date_series, 'Day') as day_name,
        to_char(date_series, 'Month') as month_name,
        to_char(date_series, 'YYYY-MM') as year_month,
        to_char(date_series, 'YYYY') as year_str,
        to_char(date_series, 'MM') as month_str,
        to_char(date_series, 'DD') as day_str,
        case
            when extract(
                dow
                from date_series
            ) in (0, 6) then true
            else false
        end as is_weekend,
        case
            when extract(
                month
                from date_series
            ) in (12, 1, 2) then 'Winter'
            when extract(
                month
                from date_series
            ) in (3, 4, 5) then 'Spring'
            when extract(
                month
                from date_series
            ) in (6, 7, 8) then 'Summer'
            else 'Fall'
        end as season,
        case
            when extract(
                month
                from date_series
            ) in (1, 2, 3) then 'Q1'
            when extract(
                month
                from date_series
            ) in (4, 5, 6) then 'Q2'
            when extract(
                month
                from date_series
            ) in (7, 8, 9) then 'Q3'
            else 'Q4'
        end as quarter
    from generate_series(
            (
                select min(message_date_day)
                from {{ ref('stg_telegram_messages') }}
            ),
            (
                select max(message_date_day)
                from {{ ref('stg_telegram_messages') }}
            ),
            interval '1 day'
        ) as date_series
),
message_metrics as (
    select message_date_day as date_id,
        count(*) as total_messages,
        count(distinct sender_id) as unique_senders,
        count(distinct channel) as active_channels,
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
        sum(reaction_count) as total_reactions,
        avg(message_length) as avg_message_length,
        sum(message_length) as total_message_length
    from {{ ref('stg_telegram_messages') }}
    group by message_date_day
)
select d.date_id,
    d.full_date,
    d.year,
    d.month,
    d.day,
    d.day_of_week,
    d.day_of_year,
    d.day_name,
    d.month_name,
    d.year_month,
    d.year_str,
    d.month_str,
    d.day_str,
    d.is_weekend,
    d.season,
    d.quarter,
    coalesce(m.total_messages, 0) as total_messages,
    coalesce(m.unique_senders, 0) as unique_senders,
    coalesce(m.active_channels, 0) as active_channels,
    coalesce(m.media_messages, 0) as media_messages,
    coalesce(m.medical_content_messages, 0) as medical_content_messages,
    coalesce(m.total_views, 0) as total_views,
    coalesce(m.total_forwards, 0) as total_forwards,
    coalesce(m.total_replies, 0) as total_replies,
    coalesce(m.total_reactions, 0) as total_reactions,
    coalesce(m.avg_message_length, 0) as avg_message_length,
    coalesce(m.total_message_length, 0) as total_message_length,
    case
        when m.total_messages > 0 then round(
            (
                (
                    m.medical_content_messages::float / m.total_messages
                ) * 100
            )::numeric,
            2
        )
        else 0
    end as medical_content_percentage,
    case
        when m.total_messages > 0 then round(
            (
                (m.media_messages::float / m.total_messages) * 100
            )::numeric,
            2
        )
        else 0
    end as media_percentage,
    current_timestamp as created_at
from date_spine d
    left join message_metrics m on d.date_id = m.date_id