create or replace  table SANDBOX_DB.SANDBOX_ALL.homepage_experiment_model_base_table as
with all_users_exp as (
select  distinct lcu.USER_ID
        ,lcu.BEST_GUESS_COUNTRY
        ,lcu.REGISTRATION_TIMESTAMP
        ,lcu.CONVERSION_TIMESTAMP
        ,lcu.CROSS_CURRENCY_CONVERSION_TIMESTAMP
        ,lcu.FIRST_PRODUCT_USED
        ,lcu.FIRST_PRODUCT_TRANSACTION_TIME
        ,lcu.SECOND_PRODUCT_USED
        ,lcu.SECOND_PRODUCT_TRANSACTION_TIME
        ,lcu.FIRST_PRODUCT_XCCY_USED
        ,lcu.FIRST_PRODUCT_XCCY_TRANSACTION_TIME
        ,lcu.LTV_12M

        ,iff(lcu.CONVERSION_TIMESTAMP is null,0,1) as Is_Converted_all_time
        ,iff(lcu.CROSS_CURRENCY_CONVERSION_TIMESTAMP is null, 0,1) as Is_XCCY_Converted_all_time
        ,sum(case
            when (ras.ACTION_COMPLETION_TIME::date between lcu.REGISTRATION_TIMESTAMP::date and dateadd(month,12,lcu.REGISTRATION_TIMESTAMP::date))
        --add flag for agg here!
            and FLAG_FOR_AGGREGATIONS=1
            then INVOICE_VALUE_GBP else 0 end ) as all_volume_since_exp
        ,sum(case
            when (ras.ACTION_COMPLETION_TIME::date between lcu.REGISTRATION_TIMESTAMP::date and dateadd(month,12,lcu.REGISTRATION_TIMESTAMP::date))
            and ras.SOURCE_CURRENCY!=ras.TARGET_CURRENCY
            and FLAG_FOR_AGGREGATIONS=1
            then INVOICE_VALUE_GBP else 0 end ) as xccy_volume_since_exp


from RPT_MARKETING.LOOKUP_CONVERTED_USERS_MERGED as lcu
left join reports.REPORT_ACTION_STEP as ras on ras.USER_ID = lcu.USER_ID
where 1 = 1
    and lcu.REGISTRATION_TIMESTAMP:: date  between '2021-01-01' and dateadd(day,-10,current_date)
    --and lcu.BEST_GUESS_COUNTRY in ('AUS','CAN','USA','GBR','NZL')
/*
 and lcu.USER_ID in (

                44720690
                ,44485214
                ,44695805
                ,44889104
                ,44301963
                ,44537066
                ,44499731
                ,44318171
                ,44991479 --send segment, received, but refuned
                ,45160801 -- balance conversion. within own account
                ,45079950 -- converted alltime =0 but MCA/SEND SEND =1; received time, but refund
                ,45303738 --converted=1, send = 1; send success
)

 */
 group by 1,2,3,4,5,6,7,8,9,10,11,12

)

--##  MCA SEGMENT 1  - bank detail activated
, bank_activated as (
select  a.USER_ID
        ,a.REGISTRATION_TIME
        ,min(a.FIRST_ACCOUNT_DETAILS_ISSUANCE_TIME) over (partition by a.USER_ID) as first_proxy_event_time
        ,datediff(day,a.REGISTRATION_TIME,first_proxy_event_time) Days_to_first_proxy_event
        ,max( case when  a.FIRST_ACCOUNT_DETAILS_ISSUANCE_TIME::date between a.REGISTRATION_TIME::date and dateadd(day,10,a.REGISTRATION_TIME::date)
         then 1 else 0 end) over (partition by a.user_id)  as Is_within_10day_proxy
        ,'Bank_Activated' as Conversion_Product
from reports.RECEIVE_ACCOUNT_DETAILS_ACTIVATION_FUNNEL a
where 1=1
and a.FIRST_ACCOUNT_DETAILS_ISSUANCE_TIME is not null
/*
    and USER_ID in (45011062
                ,45064413
                ,45160801  --exists, but null bank_detail issue time
)
 */

)
--select * from bank_activated ;

--### MCA Segment #2 : Currency Converted
, currency_converted as (
select
    distinct
    a.USER_ID
    ,b.DATE_CREATED as Registration_time
    ,min(a.ACTION_COMPLETION_TIME) over (partition by a.user_id ) first_proxy_event_time
    ,datediff(day,b.DATE_CREATED,first_proxy_event_time) Days_to_first_proxy_event
    ,max( case when  a.ACTION_COMPLETION_TIME::date between b.DATE_CREATED::date and dateadd(day,10,b.DATE_CREATED::date)
         then 1 else 0 end) over (partition by a.user_id)  as Is_within_10day_proxy
    ,'Currency_Converted' as Conversion_Product
from reports.REPORT_ACTION_STEP a
left join fx.USER b on a.USER_ID = b.ID
where 1=1
    and a.DESCRIPTION = 'Balance Conversion'
    and a.ACTION_COMPLETION_TIME is not null
  /*
  and a.USER_ID in (
        44899155 0
   */
)
--select * from currency_converted ;

--##  MCA SEGMENT  # 3 plastic card issued ##
, card_issued as (
SELECT
    distinct a.USER_ID
    ,b.DATE_CREATED as Registration_time
    ,min(a.CARD_CREATION_TIME) over (partition by a.USER_ID) as first_proxy_event_time
    ,datediff(day,b.DATE_CREATED,first_proxy_event_time) Days_to_first_proxy_event
    ,max( case when  a.CARD_CREATION_TIME::date between b.DATE_CREATED::date and dateadd(day,10,b.DATE_CREATED::date)
         then 1 else 0 end) over (partition by a.user_id)  as Is_within_10day_proxy
    ,'Card_Issued' as Conversion_Product

FROM reports.report_plastic_timestamps_by_order a
left join fx.USER b on a.USER_ID = b.ID
WHERE 1=1
  and a.TOKEN IS NOT NULL
    and a.CARD_CREATION_TIME is not null
 /* and a.USER_ID in (
      45104034
    ,45530439
    )
  */
)
--select * from card_issued


--## SEND SEGMENTS #1  -----
--check for 'receive_time' --time when TW receives deposit fund from sender

,send_money as (
select distinct ras.USER_ID
       ,fu.DATE_CREATED as registration_time
      ,min(lc.receive_time) over(partition by ras.USER_ID) as first_proxy_event_time
      ,datediff(day,fu.DATE_CREATED,first_proxy_event_time) Days_to_first_proxy_event
      ,max(case when  lc.RECEIVE_TIME::date between fu.DATE_CREATED::date and dateadd(day,10,fu.DATE_CREATED::date)
         then 1 else 0 end) over (partition by ras.user_id)  as Is_within_10day
        ,'Send_Money' as Conversion_Product
from ESTIMATOR.TRANSFER_LIFECYCLE lc
left join REPORTS.REPORT_ACTION_STEP ras on ras.REQUEST_ID = lc.transfer_id
left join fx.USER as fu on fu.ID = ras.USER_ID

where 1 = 1
--only these 2 types count as sending money. Exclude sending to yourself
and DESCRIPTION in ('Balance Withdrawal', 'Sendmoney Transfer')
and lc.RECEIVE_TIME is not null
/*
  and ras.USER_ID in (45168629
                ,44815710
                ,44301963
                )
*/

)
--select * from send_money;

--##  UNION ALL  ### ---
,all_product_conversion as (
select * from bank_activated
union  all
select * from currency_converted
union all
select * from card_issued
union all
select * from send_money
)



,all_product_conversion_ranked as (
select
        a.USER_ID
        ,a.REGISTRATION_TIME
        ,a.first_proxy_event_time
        ,a.Days_to_first_proxy_event
        ,a.Is_within_10day_proxy

        ,b.BEST_GUESS_COUNTRY
        ,b.Is_Converted_all_time
        ,b.Is_XCCY_Converted_all_time
        ,b.all_volume_since_exp
        ,b.xccy_volume_since_exp
        ,b.LTV_12M
        ,a.Conversion_Product
        ,iff(a.Conversion_Product= 'Send_Money','SEND','MCA') as Conversion_Product_Segment
        ,rank() over (partition by a.USER_ID order by first_proxy_event_time asc ) as prod_rnk
from all_product_conversion  a
left join all_users_exp b on a.USER_ID = b.USER_ID
where 1=1
and b.USER_ID is not null

)

,First_MCA_SEND_Product_conversion as (
select a.*
from all_product_conversion_ranked a
where prod_rnk = 1
)



, final_output_all_registered_user_level_ltv as (
select  distinct a.USER_ID
        ,a.REGISTRATION_TIMESTAMP

        ,a.BEST_GUESS_COUNTRY

        ,a.Is_Converted_all_time
        ,a.Is_XCCY_Converted_all_time
        ,b.first_proxy_event_time
        ,Days_to_first_proxy_event
        ,b.Is_within_10day_proxy
        ,b.Conversion_Product_Segment
        ,b.Conversion_Product
        ,case   when b.Is_within_10day_proxy=1 then b.Conversion_Product_Segment
                when b.Is_within_10day_proxy=0 and b.Conversion_Product_Segment is not null
                    then b.Conversion_Product_Segment || '_Beyond_10day'
                when b.Conversion_Product_Segment is null and a.Is_Converted_all_time=1 then 'Other'
            else 'No_Conversion' end as Proxy_Conversion_Main


       ,coalesce(a.LTV_12M , 0) as LTV_12M
       ,coalesce(a.all_volume_since_exp, 0) as all_volume_since_exp
       ,coalesce(b.xccy_volume_since_exp,0) as all_xccy_volume_since_exp

from all_users_exp a
left join First_MCA_SEND_Product_conversion b on a.USER_ID = b.USER_ID
)

--select * from final_output_all_registered_user_level_ltv;

,final_output_all_registered_user_level_product_segment_LTV as (
select a.*,
       avg(a.all_volume_since_exp) over (partition by Proxy_Conversion_Main) as avg_product_segment_volume_since_exp,  --f12m transaction
       avg(a.LTV_12M) over (partition by Proxy_Conversion_Main) as avg_product_segment_LTV_12M  --how much im worth to biz

from final_output_all_registered_user_level_ltv a

)


--select * from final_output_all_registered_user_level_product_segment_LTV

,feature_homepage_exp_all_custo_variant_control as (
select
    f.NAME as experiment_name,
    a.DATE_CREATED as feature_assigned_time,
    v.USER_ID,
    lcu.REGISTRATION_TIMESTAMP,
    a.VARIANT as Is_Variant

from  feature.FEATURES f
left join feature.assignments a on f.id = a.feature_id
left join feature.VISITORS v on  a.VISITOR_ID = v.ID

left join RPT_MARKETING.LOOKUP_CONVERTED_USERS_MERGED as lcu on v.USER_ID = lcu.USER_ID
where 1=1
  and f.NAME = 'show-homev1-experiment'
  and a.DATE_CREATED >= '2022-10-14' --feature assigned after oct 14
  and lcu.REGISTRATION_TIMESTAMP :: date >= '2022-10-14'
  and a.VARIANT is not null
  and v.ID is not null

)


-- ###  STEP  4 -------- JOIN Experiment Control/Variant  to ALL REGISTER USERS
, experiment_user_level_product_conversion_ltv as (
select   distinct b.USER_ID
                ,a.experiment_name
        ,a.feature_assigned_time
        ,a.Is_Variant
        ,b.REGISTRATION_TIMESTAMP
        ,b.BEST_GUESS_COUNTRY

        ,b.Is_Converted_all_time
        ,b.Is_XCCY_Converted_all_time
        ,b.first_proxy_event_time

        ,b.Proxy_Conversion_Main
        ,b.LTV_12M


        ,b.all_volume_since_exp
        ,b.all_xccy_volume_since_exp
        ,b.avg_product_segment_volume_since_exp
        ,b.avg_product_segment_LTV_12M
        ,b.days_to_first_proxy_event
        ,b.Is_within_10day_proxy
        ,b.Conversion_Product_Segment
        ,b.Conversion_Product


from   final_output_all_registered_user_level_product_segment_LTV b
left join    feature_homepage_exp_all_custo_variant_control a on b.USER_ID = a.USER_ID

--where b.USER_ID is not null

)

select * from experiment_user_level_product_conversion_ltv

--## this is the query that creates the Sandbox Table
--select * from SANDBOX_DB.SANDBOX_ALL.homepage_experiment_model_base_table