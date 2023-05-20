{{
    config(
        materialized='view'
    )
}}

WITH transformed_data AS (
    SELECT * FROM {{ ref('stg_ads_bing_all_data') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_ads_creative_facebook_all_data') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_ads_tiktok_ads_all_data') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_promoted_tweets_twitter_all_data') }}
)

SELECT
    SAFE_CAST(ad_id AS STRING) AS ad_id,
    SAFE_CAST(add_to_cart AS STRING) AS add_to_cart,
    SAFE_CAST(adset_id AS STRING) AS adset_id,
    SAFE_CAST(campaign_id AS STRING) AS campaign_id,
    SAFE_CAST(channel AS STRING) AS channel,
    SAFE_CAST(clicks AS INT64) AS clicks,
    SAFE_CAST(comments AS INT64) AS comments,
    SAFE_CAST(creative_id AS STRING) AS creative_id,
    SAFE_CAST(date AS DATE) AS date,
    SAFE_CAST(engagements AS INT64) AS engagements,
    SAFE_CAST(impressions AS INT64) AS impressions,
    SAFE_CAST(installs AS INT64) AS installs,
    SAFE_CAST(likes AS INT64) AS likes,
    SAFE_CAST(link_clicks AS INT64) AS link_clicks,
    SAFE_CAST(placement_id AS STRING) AS placement_id,
    SAFE_CAST(post_click_conversions AS INT64) AS post_click_conversions,
    SAFE_CAST(post_view_conversions AS INT64) AS post_view_conversions,
    SAFE_CAST(posts AS INT64) AS posts,
    SAFE_CAST(purchase AS INT64) AS purchase,
    SAFE_CAST(registrations AS INT64) AS registrations,
    SAFE_CAST(revenue AS INT64) AS revenue,
    SAFE_CAST(shares AS INT64) AS shares,
    SAFE_CAST(spend AS INT64) AS spend,
    SAFE_CAST(total_conversions AS INT64) AS total_conversions,
    SAFE_CAST(video_views AS INT64) AS video_views
FROM transformed_data