with 

stw_finding as (
    select
        oaid_raw_lookup
        , oa_name 
        , nameFormatted
        , nameArray
        , arrayJoin(arrayDistinct(arrayConcat(stw_word, groupArray(loc_word)))) as stw
        , sum(notEmpty(stw)) over (partition by oaid_raw_lookup) as stw_count
    from (
        select
            oaid_raw_lookup
            
            /* Name preprocessing */
            , oa_name
            , lowerUTF8(replaceRegexpAll(oa_name, '!|\"|#|\$|%|\(|\)|\*|\+|,|-|–|\.|/|:|;|<|=|>|\?|@|\[|\\||]|\^|_|`|{|\||}|~', ' ')) as nameFormatted
            , splitByWhitespace(nameFormatted) as nameArray
            
            , arraySlice( /* Cắt arr theo số lượng stopwords */
                arrayReverseSort( /* Sort để stopwords đứng đầu arr */
                    arrayMap(
                        x -> arrayElement(stw_arr, x) /* Lấy stopwords theo vị trí đã đánh dấu */
                        , arrayMap( 
                            x, y -> if(x = true, y, x) /* Đánh dấu vị trí stopwords trong stw_arr */
                            , arrayMap(x -> hasSubstr(nameArray, x), stw_arr) /* Check stopwords có trong name không */
                            , range(1, length(stw_arr) + 1)
                        )
                    )
                )
                , 1
                , countEqual(arrayMap(x -> hasSubstr(nameArray, x) , stw_arr), true)
            ) as stw_word
            
            /* Location preprocessing */
            , provice_iso_code
            , name_VI as provinceName
            , splitByWhitespace(arrayJoin(splitByString(',', lowerUTF8(replaceRegexpAll(location_address, '!|\"|#|\$|%|\(|\)|\*|\+|-|–|\.|/|:|;|<|=|>|\?|@|\[|\\||]|\^|_|`|{|\||}|~', ' '))))) as locationArray
            
            , arrayElement(
                loc_stw_arr
                , indexOf(arrayMap(x -> hasSubstr(locationArray, x) , loc_stw_arr), true)
            ) as loc_stw /* Lấy location stopword */
            
            , if(
                empty(loc_stw) or length(locationArray) - length(loc_stw) = 1, locationArray
                , arraySlice(locationArray, indexOf(locationArray, arrayElement(loc_stw, 1)) + length(loc_stw))
            ) as loc_piece /* Lấy tên địa điểm */
            
            , if(notEmpty(loc_piece) = 1 and hasSubstr(nameArray, loc_piece) = 1, loc_piece, []) as loc_word /* Bug: Nếu tên OA nằm có trong địa chỉ thì có thể tên OA bị tính là location stopwords và bị loại khỏi data tokens */
            
            , sum(notEmpty(loc_piece) * hasSubstr(nameArray, loc_piece)) over (partition by oaid_raw_lookup) as loc_word_count
            
        from {{#6399-oainfo}} as oainfo 
        left join tb_dim_location as location on oainfo.provice_iso_code = location.ISO3166_2_CODE
        cross join (select stw_arr as loc_stw_arr from {{#9862-data-list-stop-words}} where stw_type = 'location') as loc_stopwords
        cross join (select stw_arr from {{#9862-data-list-stop-words}} where stw_type = 'oa name') as stopwords
        where 1 = 1 
            and certified = 1
            and main_cate_id = 10003
            and lowerUTF8(oa_name) not like '%test%'
    )
    group by oaid_raw_lookup
        , oa_name 
        , nameFormatted
        , nameArray
        , stw_word
)

, stw_mapping as (
    select distinct
        oaid_raw_lookup
        , oa_name 
        , nameFormatted
        , nameArray
        , stw
        , case 
            when notEmpty(stw) = 1 
            then 
                arrayResize(
                    flatten(
                        array(
                            arrayWithConstant(
                                indexOf(
                                    arrayMap(
                                        x -> arrayMap(y -> arrayElement(nameArray, y), x)
                                        , arrayMap(
                                            x -> range(x, length(stw) + x)
                                            , range(1, length(nameArray) - length(stw) + 2)
                                        )
                                    )
                                    , stw
                                )
                                , cast(1 as int)
                            )
                            , arrayWithConstant(length(stw) - 1, cast(0 as int))
                        )
                    )
                    , length(nameArray), 1
                )
            else arrayWithConstant(length(nameArray), cast(1 as int))
        end as stwPosition
    from stw_finding
    where notEmpty(stw) = 1 or stw_count = 0
)

select
    oaid_raw_lookup
    , oa_name 
    
    , arraySplit((x, y) -> y, nameArray, arrayMap(x -> x = toString(length(groupArray(stw))), splitByString('', toString(sum(toInt32(arrayStringConcat(stwPosition, ''))))))) as nameArraySplitted
    , arrayJoin(nameArraySplitted) as wordArray
    , indexOf(nameArraySplitted, wordArray) as wordIndex
    , has(groupArray(stw), wordArray) as is_stw
    , arrayStringConcat(wordArray, ' ') as word
from stw_mapping
where length(nameArray) <= 10
group by oaid_raw_lookup
    , oa_name 
    , nameArray