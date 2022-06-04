SELECT
        b.custid       ,
        b.account_name ,
        b.campaign       ,
        b.ad_group       ,
        b.ad_id        ,
        b.ad_group_id  ,
        b.labels       ,
        b.ad_type      ,
        CASE
            WHEN (
                    labels IN ('MM_031921_01',
                               'MM_031921_02',
                               'MM_031921_R1'))
                THEN 'SQ-Exp'
            ELSE 'SQ-Cntr'
        END         AS sq_class         ,
        MIN(a.DATE) AS min_date         ,
        MAX(a.DATE) AS max_date         ,
        a.date                          ,
        SUM(impressions)      AS impressions ,
        SUM(clicks)           AS clicks      ,
        SUM(cost)             AS cost        ,
        SUM(conversions)      AS conversions ,
        SUM(conversionsvalue) AS conversionsvalue
        /*b.*,a.* */
    FROM
        test_ds_jsmith.adgroupad_by_day_parquet_v05 a
        RIGHT JOIN
            (   SELECT
                        *
                    FROM
                        thisdbshouldwork.deployment_summary
                    WHERE
                        ad_group_id IN
                        (   SELECT
                                    DISTINCT ad_group_id
                                FROM
                                    thisdbshouldwork.deployment_summary
                                WHERE
                                    (
                                        labels IN ('MM_031921_01',
                                                   'MM_031921_02',
                                                   'MM_031921_R1')
                                        AND
                                        (
                                            account_name LIKE '%Tech%'
                                            OR account_name LIKE '%Fin%')
                                        AND success IN (TRUE)
                                        AND completed IN (TRUE)) )
                        AND success IN (TRUE)
                        AND completed IN (TRUE)
                        /*AND ad_id = '508741372070'*/
            ) b
            ON
                a.mm_customer_id = b.custid
                AND a.adgroup_id=b.ad_group_id
                AND a.ad_id=b.ad_id
                and a.mm_customer_id  IN ('2113857674',
                           '3788024678')
                and a.date >= from_iso8601_date('2020-04-01')
 group by
        a.date         ,
        b.custid       ,
        b.account_name ,
        campaign       ,
        ad_group       ,
        b.ad_id        ,
        b.ad_group_id  ,
        b.labels       ,
        b.ad_type