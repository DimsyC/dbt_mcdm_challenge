SELECT 
        ad_id,
        add_to_cart,
        adset_id,
        campaign_id,
        channel,
        clicks,
        comments,
        creative_id,
        date,
        CASE
            WHEN objective = 'PRODUCT_CATALOG_SALES' THEN purchase
            WHEN objective = 'CONVERSIONS' THEN (purchase+complete_registration+mobile_app_install)
            WHEN objective = 'OUTCOME_SALES' THEN purchase
            WHEN objective = 'LINK_CLICKS' THEN inline_link_clicks
            WHEN objective = 'VIDEO_VIEWS' THEN views
            WHEN objective = 'REACH' THEN impressions
            WHEN objective = 'OUTCOME_AWARENESS' THEN views
            WHEN objective = 'OUTCOME_LEADS' THEN complete_registration
            WHEN objective = 'OUTCOME_ENGAGEMENT' THEN (likes+shares+comments+clicks+views+inline_link_clicks)
            WHEN objective = 'LEAD_GENERATION' THEN complete_registration
            WHEN objective = 'POST_ENGAGEMENT' THEN (likes+shares+comments+clicks+views+inline_link_clicks)
            WHEN objective = 'BRAND_AWARENESS' THEN views
            WHEN objective = 'APP_INSTALLS' THEN mobile_app_install
            ELSE NUll
        END AS engagements,
        impressions,
        mobile_app_install AS installs,
        likes,
        inline_link_clicks AS link_clicks,
        NULL AS placement_id,
        NULL AS post_click_conversions,
        NULL AS post_view_conversions,
        NULL AS posts,
        purchase,
        complete_registration AS registrations,
        purchase_value AS revenue,
        shares,
        spend,
        (purchase+complete_registration+mobile_app_install) AS total_conversions,
        views AS video_views
    FROM {{ ref('src_ads_creative_facebook_all_data') }}
