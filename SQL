SELECT
        mm_customer                    ,
        ad_id                          ,
        ad_type                        ,
        headline_p1                    ,
        SUM(conversions) AS conversions,
        MIN(DATE)        AS min_date   ,
        MAX(DATE)        AS max_date
    FROM
        test_ds_jsmith.adgroupad_by_day_parquet_v05
    WHERE
        mm_customer = 'System1'
        AND DATE BETWEEN from_iso8601_date('2022-02-01') AND from_iso8601_date('2022-02-28')
    GROUP BY
        mm_customer,
        ad_id      ,
        ad_type    ,
        headline_p1
    ORDER BY
        conversions DESC
_______

SELECT
        mm_customer                    ,
        SUM(conversions) AS conversions,
        SUM(cost)        AS cost       ,
        MIN(DATE)        AS min_date   ,
        MAX(DATE)        AS max_date
    FROM
        test_ds_jsmith.keyword_by_day_parquet_v0_5
    WHERE
        DATE BETWEEN from_iso8601_date('2022-02-01') AND from_iso8601_date('2022-02-28')
    GROUP BY
        mm_customer
_______

SELECT
        adgroup_name                   ,
        USER                           ,
        class2                         ,
        COUNT(DISTINCT ad_id2)         ,
        SUM(cost)              AS cost       ,
        SUM(impressions)       AS impressions,
        SUM(clicks)            AS clicks     ,
        SUM(conversions)       AS conversions,
        SUM(conversions_value) AS conversions_value
    FROM
        (   SELECT
                    COALESCE(class,'existing_pre_2021_ad') AS class2,
                    COALESCE(a.ad_id,b.ad_id)              AS ad_id2,
                    a.*                                             ,
                    b.*
                FROM
                    (   SELECT
                                mm_customer_id                 ,
                                campaign_id                    ,
                                campaign_name                  ,
                                adgroup_id                     ,
                                adgroup_name                   ,
                                ad_id                          ,
                                SUM(impressions)      AS impressions,
                                SUM(cost)             AS cost       ,
                                SUM(clicks)           AS clicks     ,
                                SUM(conversions)      AS conversions,
                                SUM(conversionsvalue) AS conversions_value
                            FROM
                                test_ds_jsmith.adgroupad_by_day_parquet_v05
                            WHERE
                                mm_customer_id = '5328144940'
                                AND DATE BETWEEN from_iso8601_date('2021-01-01') AND from_iso8601_date('2021-12-31')
                            GROUP BY
                                mm_customer_id,
                                campaign_id   ,
                                campaign_name ,
                                adgroup_id    ,
                                adgroup_name  ,
                                ad_id ) a
                    FULL JOIN
                        (   SELECT
                                    'mm_new_2021_ad'                  AS class,
                                    DATE(date_parse(DATE,'%m/%d/%Y')) AS date2,
                                    *
                                FROM
                                    mm_datalake_prod.internal_deployment_summary
                                WHERE
                                    task_type IN ('adPulse')
                                    AND custid = '5328144940'
                                    AND deploy_action IN ('adCreate')
                                    AND DATE(date_parse(DATE,'%m/%d/%Y')) BETWEEN from_iso8601_date('2021-01-01') AND from_iso8601_date('2021-12-31')
                                    AND success IN (TRUE)) b
                        ON
                            a.mm_customer_id=b.custid
                            AND a.adgroup_id=b.ad_group_id )
    GROUP BY
        adgroup_name,
        USER        ,
        class2
_______

/*CREATE TABLE
        test_ds_jsmith.deployments_2021 WITH
        (
            format='PARQUET'                                                                    ,
            parquet_compression='SNAPPY'                                                        ,
            external_location = 's3://motivemetrics-ds-jsmith/deployments_2021/20220209_152206/'
           
        )
        AS*/
_______


SELECT
        a.*,
        b.*
    FROM
        mm_datalake_prod.internal_templates a
        INNER JOIN
            mm_datalake_prod.accounts_prod b
            ON
                a.customer_id=b."mm customerid"
                AND b."mm customer"='Remitly' limit 10

_______

SELECT
        mm_customer                          ,
        ad_id                                ,
        ad_type                              ,
        headline_p1                          ,
        SUM(impressions) AS _2022_impressions,
        MIN(DATE)        AS min_date         ,
        MAX(DATE)        AS max_date         ,
        SUM(
            CASE
                WHEN     DATE >= CURRENT_DATE - INTERVAL '7' DAY
                    THEN impressions
                ELSE 0
            END) AS last_7_day_impressions,
        MIN(
            CASE
                WHEN     DATE >= CURRENT_DATE - INTERVAL '7' DAY
                    THEN DATE
                ELSE NULL
            END) AS last_7_day_min_day,
        MAX(
            CASE
                WHEN     DATE >= CURRENT_DATE - INTERVAL '7' DAY
                    THEN DATE
                ELSE NULL
            END) AS last_7_day_max_day
    FROM
        test_ds_jsmith.adgroupad_by_day_parquet_v05
    WHERE
        mm_customer = 'AutoWeb'
        AND DATE BETWEEN from_iso8601_date('2022-01-01') AND from_iso8601_date('2022-03-07')
    GROUP BY
        mm_customer,
        ad_id      ,
        ad_type    ,
        headline_p1
    ORDER BY
        _2022_impressions DESC
        limit 1000
________

SELECT
        keyword_text                                                           ,
        reduce (agg_clicks['BROAD'], 0.0 , (s,x) -> s+x,s->s)  AS BROAD_clicks ,
        reduce (agg_clicks['EXACT'], 0.0 , (s,x) -> s+x,s->s)  AS EXACT_clicks ,
        reduce (agg_clicks['PHRASE'], 0.0 , (s,x) -> s+x,s->s) AS PHRASE_clicks,
        reduce (agg_cost['BROAD'], 0.0 , (s,x) -> s+x,s->s)    AS BROAD_cost   ,
        reduce (agg_cost['EXACT'], 0.0 , (s,x) -> s+x,s->s)    AS EXACT_cost   ,
        reduce (agg_cost['PHRASE'], 0.0 , (s,x) -> s+x,s->s)   AS PHRASE_cost
    FROM
        (   SELECT
                    keyword_text                                         ,
                    multimap_agg(keyword_matchtype, clicks) AS agg_clicks,
                    multimap_agg(keyword_matchtype, cost)   AS agg_cost  
                FROM
                    (   SELECT
                                keyword_text         ,
                                keyword_matchtype    ,
                                SUM(clicks) AS clicks,
                                SUM(cost)   AS cost  
                            FROM
                                test_ds_jsmith.keyword_by_day_parquet_v0_5
                            WHERE
                                mm_customer='AutoWeb'
                                AND DATE BETWEEN from_iso8601_date('2022-01-01') AND from_iso8601_date('2022-03-07')
                                AND account_name IN ('Autoweb.com')
                                /*AND keyword_text IN ('honda auto sales')*/
                            GROUP BY
                                keyword_text,
                                keyword_matchtype limit 100)
                GROUP BY
                    keyword_text )
________

SELECT
        keyword_id  ,
        keyword_text,
        keyword_matchtype,
        mm_customer
    FROM
        test_ds_jsmith.keyword_by_day_parquet_v0_5
    GROUP BY
        keyword_id  ,
        keyword_text,
        keyword_matchtype,
        mm_customer
    ORDER BY
        cast(keyword_id as bigint) asc limit 100
_______

SELECT
        SUM(impressions)                               AS impressions             ,
        SUM(all_possible_impressions)                  AS all_possible_impressions,
        SUM(impressions)/SUM(all_possible_impressions) AS impression_share        , 
        mm_customer
    FROM
        test_ds_jsmith.keyword_by_day_parquet_v0_5
    GROUP BY
        mm_customer


