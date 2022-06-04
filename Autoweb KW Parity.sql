SELECT
        keyword_text                                               ,
        match_type                                                 ,
        MAX(autoweb_com_keyword)       AS autoweb_com_keyword      ,
        MAX(auto_price_finder_keyword) AS auto_price_finder_keyword,
        MAX(car_com_keyword)           AS car_com_keyword          ,
        MAX(autosite_keyword)          AS autosite_keyword
    FROM
        (   SELECT
                    IF(account='Autoweb.com',1,0)       AS autoweb_com_keyword      ,
                    IF(account='Auto Price Finder',1,0) AS auto_price_finder_keyword,
                    IF(account='Car.com',1,0)           AS car_com_keyword          ,
                    IF(account='Autosite',1,0)          AS autosite_keyword         ,
                    keyword_text                                                    ,
                    match_type                                                      ,
                    account
                FROM
                    test_ds_jsmith.autoweb_keyword_account_parity
                WHERE
                    keyword_text <> '')
    GROUP BY 
        keyword_text ,
        match_type