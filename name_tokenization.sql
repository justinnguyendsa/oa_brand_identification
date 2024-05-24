with 

data_token_non_stw as (
    select
        oaid_raw_lookup
        , oa_name
        , arrayJoin(arraySplit((x, y) -> y, groupArray(word), arrayMap(x -> x != 1, arrayDifference(groupArray(wordIndex))))) as w
    from {{#9838-name-preprocessing}}
    where is_stw = 0
    group by oaid_raw_lookup, oa_name
)

, re_tokenize as (
    select 
        oaid_raw_lookup
        , oa_name
        , num_w
        , arrayStringConcat(arrayJoin(arrayMap(x -> arrayMap(y -> arrayElement(w, y), x), arrayMap(x -> range(x, num_w + x), range(1, length(w) - num_w + 2)))), ' ') as token
        , substringIndexUTF8(token, ' ', 1) as main_token
        , case
            when num_w = 4 then substringIndexUTF8(token, ' ', 3) 
            when num_w = 3 then substringIndexUTF8(token, ' ', 2) 
            when num_w = 2 then substringIndexUTF8(token, ' ', 1)
        end as parent_token
        , sum(1) over (partition by token) as token_count
        , sum(1) over (partition by token) / sum(case when num_w = 1 then 1 else 0 end) over (partition by main_token) as token_pct
        
        , substringIndexUTF8(token, ' ', 1) as w1
        , case when num_w >= 2 then substringIndexUTF8(token, ' ', 2) end as w2
        , case when num_w >= 3 then substringIndexUTF8(token, ' ', 3) end as w3
        , case when num_w = 4 then token end as w4
        
        , 1 as f1
        , sum(case when num_w = 2 then 1 else 0 end) over (partition by w2) / sum(case when num_w = 1 then 1 else 0 end) over (partition by w1) as f2
        , sum(case when num_w = 3 then 1 else 0 end) over (partition by w3) / sum(case when num_w = 1 then 1 else 0 end) over (partition by w1) as f3
        , sum(case when num_w = 4 then 1 else 0 end) over (partition by w4) / sum(case when num_w = 1 then 1 else 0 end) over (partition by w1) as f4
        
    from data_token_non_stw
    cross join (select toInt8(arrayJoin(['1', '2', '3', '4'])) as num_w) as num_word
    where num_w <= length(w)
)

, distinct_token as (
    select
        num_w
        , token 
        , main_token
        , token_count
        , token_pct
        , w1, w2, w3, w4
        , f1, f2, f3, f4
        
        , groupArray(array(toString(oaid_raw_lookup), oa_name)) as oainfo_arr
    from re_tokenize
    group by num_w
        , token 
        , main_token
        , token_count
        , token_pct
        , w1, w2, w3, w4
        , f1, f2, f3, f4
)

, F3_F4_calculation as (
    select
        *
        , f4 as F4 
        , case when f3 - sum(f4) over (partition by w3) > 0.01 then f3 - sum(f4) over (partition by w3) else 0 end as F3
    from distinct_token
    where token_pct > 0.01
)

, F2_calculation as (
    select
        *
        , case when f2 - sum(F3 + F4) over (partition by w2) > 0.01 then f2 - sum(F3 + F4) over (partition by w2) else 0 end as F2
    from F3_F4_calculation
)

, F1_calculation as (
    select
        *
        , case when f1 - sum(F2 + F3 + F4) over (partition by w1) > 0.01 then f1 - sum(F2 + F3 + F4) over (partition by w1) else 0 end as F1
    from F2_calculation
)

, tokens as (
    select
        num_w
        , token 
        , token_count 
        , case
            when num_w = 4 then F4 
            when num_w = 3 then F3 
            when num_w = 2 then F2
            when num_w = 1 then F1
        end as token_pct
        , hasSubstr(brand_tokens, array(token)) as is_brand_token
        , hasSubstr(business_type_tokens, array(token)) as is_business_type_token
        , oainfo_arr
    from F1_calculation
    cross join {{#10073-data-oa-brand-tokens}} as oa_brand_tokens
    cross join {{#10075-data-oa-business-type-tokens}} as oa_business_type_tokens
)

select 
    arrayElement(arrayJoin(oainfo_arr), 1) as oaId_raw 
    , arrayElement(arrayJoin(oainfo_arr), 2) as oa_name
    , max(is_brand_token) over (partition by oaId_raw) as is_brand
    , * except oainfo_arr
from tokens 
where token_pct > 0 and token_count >= 5