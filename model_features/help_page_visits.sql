-- To do: update with LTV when access granted

----------- Help page visits & their value ------------
-- getting the average customer lifetime XCCY volume associated with customers who visit different help pages
--   e.g. "how to send large amounts" has higher value customers on average.

with help_article_value as (
    select 
        help.ARTICLE__ENGLISH_TITLE,
        median(lifetime.LIFETIME_XCCY_INVOICE_VALUE_GBP) as median_profile_xccy_volume,
        -- avg was skewed. could do a different percentile.
        count(*) as count_profiles
    FROM rpt_cs_data.analytical_help_centre__events help
        join reports.REPORT_USER_PROFILE_LIFETIME_ACTIONS lifetime on help.PROFILE__ID = lifetime.USER_PROFILE_ID
    WHERE 1 = 1
        AND user__id IS NOT NULL
        AND EVENT__NAME = 'help flows - page view - article'
    group by 1
    having count(*) > 3000 -- filter out small data
    order by 2 desc
),

user_help_events as (
    SELECT 
        user__id AS user_id,
        EVENT__ID,
        event__ts AS help_centre_event_ts,
        user.DATE_CREATED as registration_date,
        event__name AS help_centre_event_name,
        help.article__english_title AS help_centre_article,
        help_article_value.median_profile_xccy_volume as article_viewer_median_lifetime_xccy_volume,
        dateadd(HOUR, 12, event__ts) AS help_centre_event_twelve_hours_after_ts
    FROM rpt_cs_data.analytical_help_centre__events help
        left join fx.user user on help.USER__ID = user.id -- we only want help page visits before the user converted
        inner join SANDBOX_DB.SANDBOX_ALL.HOMEPAGE_EXPERIMENT_MODEL_BASE_TABLE base on help.USER__ID = base.USER_ID
        and help.EVENT__TS < base.FIRST_PROXY_EVENT_TIME -- TO DO: INNER JOIN WHERE HELP EVENTS ARE BEFORE CONVERSION EVENT
        join help_article_value on help.ARTICLE__ENGLISH_TITLE = help_article_value.ARTICLE__ENGLISH_TITLE
    WHERE 1 = 1
        AND user__id IS NOT NULL
        AND help_centre_event_name = 'help flows - page view - article'
),

help_page_visit_events__user_level as (
    select 
        user_id,
        count(distinct EVENT__ID) as count_distinct_help_page_visit_events_before_conversion,
        avg(article_viewer_median_lifetime_xccy_volume) as average_help_event_customer_lifetime_xccy_volume,
        -- average across all help pages visited before conversion for: the median customer lifetime XCCY volume for customers who visit specific help pages
        max(article_viewer_median_lifetime_xccy_volume) as max_help_event_customer_lifetime_xccy_volume -- max of all help pages visited before conversion for: the median customer lifetime XCCY volume for customers who visit specific help pages
    from user_help_events
    group by user_id
)

select *
from help_page_visit_events__user_level