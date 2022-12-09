



with visit_base_table as (
select
       fu.ID,
       a.REGISTRATION_TIMESTAMP_AT,
       sb.FIRST_PROXY_EVENT_TIME,
       a.VISIT_DATE_AT,
       a.VISIT_ID,
       a.LANGUAGE,
       a.DEVICE,
      a.COUNTRY,
       min(VISIT_DATE_AT) over (partition by fu.ID) first_visit_time,

       date_trunc(day,a.REGISTRATION_TIMESTAMP_AT) registration_date,
       date_trunc(day,sb.FIRST_PROXY_EVENT_TIME) first_proxy_event_date,
       date_trunc(day,a.VISIT_DATE_AT) visit_date,

       rank() over (partition by  fu.ID order by VISIT_DATE_AT asc) rank_of_visit,
       datediff(day,min(VISIT_DATE_AT) over (partition by fu.ID),REGISTRATION_TIMESTAMP_AT) days_from_first_visit_to_registration,
       datediff(day,REGISTRATION_TIMESTAMP_AT,sb.FIRST_PROXY_EVENT_TIME) days_from_registration_to_proxy_conversion,
       URL,
       case when VISIT_DATE_AT <= REGISTRATION_TIMESTAMP_AT then 1 else 0 end as Is_before_registration,
       case when (VISIT_DATE_AT > REGISTRATION_TIMESTAMP_AT AND VISIT_DATE_AT <= SB.FIRST_PROXY_EVENT_time ) then 1 else 0 end as Is_between_registration_and_conversion,
       case when URL like 'https://wise.com/' then 1 else 0 end as Is_URL_wise_com,
       case when URL like 'https://wise.com/us/' then 1 else 0 end as Is_URL_wise_com_US,
       case when URL like 'https://transferwise.com/' then 1 else 0 end as Is_URL_transferwise_com,
       case when URL like 'https://wise.com/recipients/' then 1 else 0 end as Is_URL_recipient,
       case when URL like 'https://wise.com/gb/business/' then 1 else 0 end as Is_URL_gb_business,
       case when URL like 'https://wise.com/login/' then 1 else 0 end as Is_URL_login,
       case when URL like 'https://wise.com/send/' then 1 else 0 end as Is_URL_send,
       case when URL like 'https://transferwise.com/send/' then 1 else 0 end as Is_URL_transferwise_com_send,
       case when URL like 'https://wise.com/home/' then 1 else 0 end as Is_URL_home,
       case when URL like 'https://wise.com/card-management' then 1 else 0 end as Is_URL_card_management,
       case when URL like 'https://transferwise.com/transferflow' then 1 else 0 end as Is_URL_transferflow,
       case when URL like 'https://wise.com/help/' then 1 else 0 end as Is_URL_help,
       case when URL like 'https://wise.com/account-selector/' then 1 else 0 end as Is_URL_account_selector,
       case when URL like 'https://wise.com/cards/' then 1 else 0 end as Is_URL_cards,
       case when URL like 'https://transferwise.com/recipients/' then 1 else 0 end as Is_URL_transferwise_recipients,
       case when URL like 'https://wise.com/get-started/' then 1 else 0 end as Is_URL_get_started,
       case when URL like 'https://wise.com/flows/open-balance' then 1 else 0 end as Is_URL_open_balance,
       case when URL like 'https://wise.com/gb/multi-currency-account/' then 1 else 0 end as Is_URL_multi_currency_account,
       case when URL like 'https://wise.com/invite' then 1 else 0 end as Is_URL_invite,
       case when URL like 'https://wise.com/profile/create/' then 1 else 0 end as Is_URL_profile_create


from rpt_marketing.dim_visits__touchpoints  a
left join fx.USER fu on fu.ID = a.USER_ID
left join SANDBOX_DB.SANDBOX_ALL.homepage_experiment_model_base_table sb on sb.USER_Id = fu.ID
where 1=1
and fu.ID is not null
-- and fu.ID in (26003721, 40871901,36999442,35400617)
-- and VISIT_DATE_AT <= REGISTRATION_TIMESTAMP_AT

)

, first_visit_table as (
select
*
from visit_base_table a
where 1=1
and rank_of_visit=1

)

,visit_summary_table as (
select
a.id,
a.first_visit_time,
a.REGISTRATION_TIMESTAMP_AT,
a.first_proxy_event_time,
f.URL as first_visit_URL,
a.LANGUAGE,
a.DEVICE,
a.country,
a.days_from_first_visit_to_registration,
a.days_from_registration_to_proxy_conversion,

--pre post visit
count( distinct case when a.is_before_registration =1 then a.visit_date else null end) as ttl_unique_days_visited_pre_registration,
count( distinct case when a.Is_between_registration_and_conversion =1 then a.visit_date else null end) as ttl_unique_days_visited_between_registration_conversion,
sum(a.Is_before_registration) as ttl_URL_visits_before_registration,
sum(a.Is_between_registration_and_conversion) as ttl_URL_visits_between_registration_and_conversion,

--pre post url  features
sum(iff(a.Is_before_registration=1,a.Is_URL_wise_com,0)) as ttl_Is_URL_wise_com_pre_registration,
sum(iff(a.Is_before_registration=1,a.IS_URL_WISE_COM_US,0)) as ttl_Is_URL_wise_com_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_transferwise_com,0)) as ttl_Is_URL_transferwise_com_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_recipient,0)) as ttl_Is_URL_recipient_pre_registration,

sum(iff(a.Is_before_registration=1,a.Is_URL_gb_business,0)) as ttl_Is_URL_gb_business_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_login,0)) as ttl_Is_URL_login_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_send,0)) as ttl_Is_URL_send_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_transferwise_com_send,0)) as ttl_Is_URL_transferwise_com_send_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_home,0)) as ttl_Is_URL_home_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_card_management,0)) as ttl_Is_URL_card_management_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_transferflow,0)) as ttl_Is_URL_transferflow_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_account_selector,0)) as ttl_Is_URL_account_selector_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_cards,0)) as ttl_Is_URL_cards_pre_registration,

sum(iff(a.Is_before_registration=1,a.Is_URL_transferwise_recipients,0)) as ttl_Is_URL_transferwise_recipients_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_get_started,0)) as ttl_Is_URL_get_started_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_open_balance,0)) as ttl_Is_URL_open_balance_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_multi_currency_account,0)) as ttl_Is_URL_multi_currency_account_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_invite,0)) as ttl_Is_URL_invite_pre_registration,
sum(iff(a.Is_before_registration=1,a.Is_URL_profile_create,0)) as ttl_Is_URL_profile_create_pre_registration,

--between registration and conversion
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_wise_com,0)) as ttl_Is_URL_wise_com_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.IS_URL_WISE_COM_US,0)) as ttl_Is_URL_wise_com_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_transferwise_com,0)) as ttl_Is_URL_transferwise_com_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_recipient,0)) as ttl_Is_URL_recipient_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_gb_business,0)) as ttl_Is_URL_gb_business_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_login,0)) as ttl_Is_URL_login_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_send,0)) as ttl_Is_URL_send_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_transferwise_com_send,0)) as ttl_Is_URL_transferwise_com_send_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_home,0)) as ttl_Is_URL_home_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_card_management,0)) as ttl_Is_URL_card_management_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_transferflow,0)) as ttl_Is_URL_transferflow_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_account_selector,0)) as ttl_Is_URL_account_selector_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_cards,0)) as ttl_Is_URL_cards_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_transferwise_recipients,0)) as ttl_Is_URL_transferwise_recipients_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_get_started,0)) as ttl_Is_URL_get_started_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_open_balance,0)) as ttl_Is_URL_open_balance_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_multi_currency_account,0)) as ttl_Is_URL_multi_currency_account_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_invite,0)) as ttl_Is_URL_invite_post_registration,
sum(iff(a.Is_between_registration_and_conversion=1,a.Is_URL_profile_create,0)) as ttl_Is_URL_profile_create_post_registration


from visit_base_table a
left join first_visit_table f on a.ID = f.ID
group by 1,2,3,4,5,6,7,8,9,10
)
select * from visit_summary_table


