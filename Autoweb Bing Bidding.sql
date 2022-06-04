#####Autoweb Bid Recommendation 
#######dashboard: https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/e9b5a5f8-87fc-490a-85e6-19c61f0f4034
SELECT
        *
    FROM
        test_ds_jsmith.autoweb_bing_bidding_scopes a
        LEFT JOIN
            (   SELECT
                        REPLACE(REPLACE(campaign_id,'[',''),']','') AS campaign_id               ,
                        REPLACE(REPLACE(adgroup_id,'[',''),']','')  AS adgroup_id                ,
                        REPLACE(REPLACE(keyword_id,'[',''),']','')  AS keyword_id                ,
                        SUM(spend)                                  AS keyword_id_spend          ,
                        SUM(clicks)                                 AS keyword_id_clicks         ,
                        SUM(all_conversions)                        AS keyword_id_all_conversions,
                        SUM(all_revenue)                            AS keyword_id_all_revenue
                    FROM
                        test_ds_jsmith.autoweb_bing_bidding_keyword_metrics
                    GROUP BY
                        campaign_id,
                        adgroup_id ,
                        keyword_id) b
            ON
                a.campaign_id=b.campaign_id
                AND a.adgroup_id=b.adgroup_id
                AND a.keyword_id = b.keyword_id
        LEFT JOIN
            (   SELECT
                        REPLACE(REPLACE(campaign_id,'[',''),']','') AS campaign_id               ,
                        REPLACE(REPLACE(adgroup_id,'[',''),']','')  AS adgroup_id                ,
                        SUM(spend)                                  AS adgroup_id_spend          ,
                        SUM(clicks)                                 AS adgroup_id_clicks         ,
                        SUM(all_conversions)                        AS adgroup_id_all_conversions,
                        SUM(all_revenue)                            AS adgroup_id_all_revenue
                    FROM
                        test_ds_jsmith.autoweb_bing_bidding_keyword_metrics
                    GROUP BY
                        campaign_id,
                        adgroup_id ) c
            ON
                a.campaign_id=c.campaign_id
                AND a.adgroup_id=c.adgroup_id
        LEFT JOIN
            (   SELECT
                        REPLACE(REPLACE(campaign_id,'[',''),']','') AS campaign_id                ,
                        SUM(spend)                                  AS campaign_id_spend          ,
                        SUM(clicks)                                 AS campaign_id_clicks         ,
                        SUM(all_conversions)                        AS campaign_id_all_conversions,
                        SUM(all_revenue)                            AS campaign_id_all_revenue
                    FROM
                        test_ds_jsmith.autoweb_bing_bidding_keyword_metrics
                    GROUP BY
                        campaign_id ) d
            ON
                a.campaign_id=d.campaign_id
