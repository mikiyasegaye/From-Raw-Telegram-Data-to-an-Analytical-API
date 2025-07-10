-- Custom test: Message engagement metrics should be consistent
-- This test ensures that engagement metrics are logically consistent
with message_engagement as (
    select message_id,
        views,
        forwards,
        replies,
        reaction_count,
        engagement_score,
        viral_score
    from {{ ref('fct_messages') }}
    where views > 0
        or forwards > 0
        or replies > 0
        or reaction_count > 0
),
validation as (
    select count(*) as total_engaged_messages,
        count(
            case
                when views < 0 then 1
            end
        ) as negative_views,
        count(
            case
                when forwards < 0 then 1
            end
        ) as negative_forwards,
        count(
            case
                when replies < 0 then 1
            end
        ) as negative_replies,
        count(
            case
                when reaction_count < 0 then 1
            end
        ) as negative_reactions,
        count(
            case
                when engagement_score < 0 then 1
            end
        ) as negative_engagement_score,
        count(
            case
                when viral_score < 0 then 1
            end
        ) as negative_viral_score
    from message_engagement
)
select *
from validation
where negative_views > 0 -- Should not have negative views
    or negative_forwards > 0 -- Should not have negative forwards
    or negative_replies > 0 -- Should not have negative replies
    or negative_reactions > 0 -- Should not have negative reactions
    or negative_engagement_score > 0 -- Should not have negative engagement score
    or negative_viral_score > 0 -- Should not have negative viral score