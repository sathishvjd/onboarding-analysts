-- intent picker & marketing channel
-- Note - `signup_platform` (e.g. iOS, web, android) would be useful but looks like data is dead since 2021H2

with onboarding_data__user_level as (
    select USER_ID,
           -- fields likely to be useful
           FIRST_INTENT,      -- about 60% complete
           SAW_INTENT_PICKER, -- might be useful to consider alongside first_intent - not sure what causes this though
           channel,           -- about 40% complete - could get marketing info from elsewhere
           CHANNEL_GROUPS,    -- could get marketing info from elsewhere
           SIGNUP_PLATFORM,   -- 4% complete (down from ~100% in the first half of 2021, falling since then) -- this would be useful 
    from reports.USER_ONBOARDING_CONVERSION
)

select * from onboarding_data__user_level