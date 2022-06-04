/*ad impact analysis, i only include Tech and spend metrics*/
SELECT
                    d.mm_customer                                              ,
                    d.account_name                                             ,
                    d.mm_customer_id                                           ,        
                   /* SUM(COALESCE(impressions, 0))      AS ad_group_ad_id_impressions,
                    SUM(COALESCE(clicks, 0))           AS ad_group_ad_id_clicks     ,*/
                    SUM(COALESCE(cost, 0))             AS ad_group_ad_id_cost       
                    /*SUM(COALESCE(conversions, 0))      AS ad_group_ad_id_conversions,
                    SUM(COALESCE(conversionsvalue, 0)) AS ad_group_ad_id_conversionsvalue*/
                FROM
                    test_ds_jsmith.adgroupad_by_day_parquet_v05 a
                    RIGHT JOIN
                        (   SELECT
                                    a2.ad_group_id              ,
                                    a2.date                     ,
                                    a2.task_name                ,
                                    a2.ad_id                    ,
                                    a2.labels                   ,
                                    a2.custid AS mm_customer_id ,
                                    b.account_name              ,
                                    b.mm_customer
                                FROM
                                    (select
                                      *
                                     from
                                    thisdbshouldwork.deployment_summary
                                     where account_name = 'Info-Technology - google' and custid = '3788024678') a2
                                    LEFT JOIN
                                        (   SELECT
                                                    "account name" AS account_name,
                                                    "mm customer"  AS mm_customer ,
                                                    customerid
                                                FROM
                                                    test_ds_jsmith.xlab_json_prod
                                                WHERE
                                                    partition_0 =
                                                    (   SELECT
                                                                MAX(partition_0)
                                                            FROM
                                                                test_ds_jsmith.xlab_json_prod )) b
                                        ON
                                            a2.custid = b.customerid                              
                                WHERE
                                    task_type IN ('adPulse')
                                    AND deploy_action IN ('adCreate')
                                    AND success = TRUE
                                    AND completed=TRUE
                                    AND mm_customer='System1'
                                    AND b.account_name = 'OO-Info-Technology'	) d
                        ON
                            a.mm_customer_id = d.mm_customer_id
                            AND a.adgroup_id=d.ad_group_id
                            AND a.ad_id=d.ad_id
                GROUP BY
                    d.mm_customer    ,
                    d.account_name   ,
                    d.mm_customer_id;
