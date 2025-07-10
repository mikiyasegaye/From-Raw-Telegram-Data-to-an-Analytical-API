-- Custom test: Medical content should be consistent across healthcare channels
-- This test ensures that healthcare channels (CheMed123, tikvahpharma) have a reasonable percentage of medical content
with channel_medical_content as (
    select channel_id,
        medical_content_percentage
    from {{ ref('dim_channels') }}
    where business_sector = 'Healthcare'
),
validation as (
    select count(*) as healthcare_channels,
        avg(medical_content_percentage) as avg_medical_content_percentage
    from channel_medical_content
    where medical_content_percentage > 0 -- At least some medical content
)
select *
from validation
where healthcare_channels = 0 -- Should have at least one healthcare channel
    or avg_medical_content_percentage < 5 -- Healthcare channels should have at least 5% medical content on average