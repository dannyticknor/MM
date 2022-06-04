
 with cte as(SELECT
        a_date,
        a_mm_customer,
        d_mm_customer,
        a_adgroupid,
        d_adgroupid,
        a_adid,
        d_adid,
        labels,
        impressions,
        sum(impressions) over(order by a_date)as cum_impressions,
        clicks,
        cost,
        conversions,
        conversionsvalue
        
FROM
(SELECT
        coalesce(to_char(a.date,'YYYY-MM-DD'),d.date) as a_date                                 ,
        a.mm_customer   as a_mm_customer                        ,
        d.mm_customer   as d_mm_customer                        ,
        a.account_name                          ,
        /*d.account_name                          ,*/
        a.adgroup_id      as a_adgroupid                      ,
        d.ad_group_id     as d_adgroupid                      ,
        a.ad_id           as a_adid                     ,
        d.ad_id           as d_adid                      ,
        COALESCE(labels , 'UNK') AS labels      ,
        coalesce(SUM(impressions),0)        AS impressions ,
        coalesce(SUM(clicks),0)             AS clicks      ,
        coalesce(SUM(cost),0)                AS cost        ,
        coalesce(SUM(conversions),0)         AS conversions ,
        coalesce(SUM(conversionsvalue),0)    AS conversionsvalue
    FROM
        test_ds_jsmith.adgroupad_by_day_parquet_v05 a
        FULL OUTER JOIN
            (   SELECT
                        a2.*           ,
                        b.account_name,
                        b.mm_customer
                    FROM
                        thisdbshouldwork.deployment_summary a2
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
                                a2.custid = b.customerid) d
            ON
                a.mm_customer_id = d.custid
                AND a.adgroup_id=d.ad_group_id
                AND a.ad_id=d.ad_id
                AND d.success = TRUE
                AND d.completed=TRUE
                AND task_type IN ('adPulse')
                AND deploy_action IN ('adCreate')
                
    GROUP BY
        coalesce(to_char(a.date,'YYYY-MM-DD'),d.date)        ,
        a.mm_customer  ,
        d.mm_customer  ,
        a.account_name ,
        /*d.account_name ,*/
        a.adgroup_id   ,
        d.ad_group_id  ,
        labels         ,
        a.ad_id        ,
        d.ad_id
    HAVING
        d.ad_group_id IS NOT NULL
        AND a.adgroup_id IS NULL AND COALESCE(a.mm_customer,d.mm_customer) IN ('System1')
 )sub)
 
 select 
      count(*),
      sum(case when labels = 'UNK' then 1 else 0 end) nunk
 from cte 

