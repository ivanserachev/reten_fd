SELECT
    *
    EXCLUDE deal_id,
    count(DISTINCT deal_id) AS sales
FROM
    (
        WITH DEALS AS (
            SELECT
                USER_ID
              , DEAL_ID
              , CASE
                    WHEN EVENT_RANK = 20 THEN 'Выдача'
                    WHEN EVENT_RANK >= 10 THEN 'Одобрение'
                    ELSE 'Заявка'
                END AS DEAL_STATUS
            FROM PROD_DWH.KIMBALL.CALCULATION_MFO
            WHERE
                  DEAL_ID IS NOT NULL
              AND EVENT_NAME NOT IN ('mfoIssuedDealStatus', 'mfoPromotionStart', 'mfoPromotionFinish')
              AND (ERROR_MESSAGE NOT IN
                   ('Техническое закрытие зависших сделок', 'Техническое закрытие зависших сделок без оффера') OR
                   ERROR_MESSAGE IS NULL)
              AND EVENT_RANK NOT IN (22, 23, 24, 11)
            QUALIFY
                ROW_NUMBER() OVER (PARTITION BY DEAL_ID ORDER BY EVENT_RANK DESC) = 1
                      )
           , CLIENT_INFO AS (
            SELECT
                USER_ID
              , CLIENT_DATA['age']::NUMBER                                                                  AS AGE
              , CLIENT_DATA['monthlyIncome']::NUMBER                                                        AS INCOME
              , CASE
                    WHEN CLIENT_DATA['gender']::STRING IN ('female', 'FEMALE') THEN 'Женщина'
                    WHEN CLIENT_DATA['gender']::STRING IN ('male', 'MALE') THEN 'Мужчина'
                    ELSE CLIENT_DATA['gender']::STRING
                END                                                                                         AS GENDER
              , CLIENT_DATA['maritalStatus']::STRING                                                        AS MARITAL_STATUS
              , CLIENT_DATA['residentialAddress']['federal_district']::STRING                               AS FEDERAL_DISTRICT
              , CLIENT_DATA['residentialAddress']['region_with_type']::STRING                               AS REGION
              , IFF(CLIENT_DATA['okbGen7MfoIlV1']::NUMBER = 0, NULL,
                    CLIENT_DATA['okbGen7MfoIlV1']::NUMBER)                                                  AS OKB_RATING
              , CLIENT_DATA['employment']::STRING                                                           AS EMPLOYMENT_TYPE
              , CLIENT_DATA['dependents']::STRING                                                           AS DEPENDENTS
              , CLIENT_DATA['education']::STRING                                                            AS EDUCATION
              , CLIENT_DATA['lastPlaceworkExperience']::NUMBER                                              AS WORK_EXPIRIENCE
              , CLIENT_DATA['birthPlace']::VARCHAR                                                          AS BIRTHPLACE
              , CLIENT_DATA['email']::VARCHAR                                                               AS EMAIL
              , CONCAT(CLIENT_DATA['lastName']::STRING, ' ', CLIENT_DATA['firstName']::STRING, ' ',
                       CLIENT_DATA['middleName']::STRING)                                                   AS FIO
              , CLIENT_DATA['organizationAddress']['federal_district']::VARCHAR                             AS WORK_PLACE_FEDERAL_DISTRICT
              , CLIENT_DATA['organizationAddress']['region']::VARCHAR                                       AS WORK_PLACE_REGION
              , CLIENT_DATA['organizationAddress']['region_type_full']::VARCHAR                             AS WORK_PLACE_REGION_TYPE_FULL
              , CLIENT_DATA['organizationAddress']['city']::VARCHAR                                         AS WORK_PLACE_CITY
              , CLIENT_DATA['organizationAddress']['city_type_full']::VARCHAR                               AS WORK_PLACE_CITY_TYPE_FULL
              , CLIENT_DATA['organizationAddress']['city_district_with_type']::VARCHAR                      AS WORK_PLACE_CITY_DISTRICT_WITH_TYPE
              , CLIENT_DATA['organizationAddress']['street_with_type']::VARCHAR                             AS WORK_PLACE_STREET
              , CLIENT_DATA['organizationAddress']['city_fias_id']::VARCHAR                                 AS WORK_PLACE_CITY_FIAS_ID
              , CLIENT_DATA['organizationAddress']['city_kladr_id']::VARCHAR                                AS WORK_PLACE_CITY_KLADR_ID
              , CLIENT_DATA['organizationAddress']['fias_code']::VARCHAR                                    AS WORK_PLACE_FIAS_CODE
              , CLIENT_DATA['organizationAddress']['area_fias_id']::VARCHAR                                 AS WORK_PLACE_AREA_FIAS_ID
              , CLIENT_DATA['organizationAddress']['area_kladr_id']::VARCHAR                                AS WORK_PLACE_AREA_KLADR_ID
              , CLIENT_DATA['organizationAddress']['city_district_fias_id']::VARCHAR                        AS WORK_PLACE_CITY_DISTRICT_FIAS_ID
              , CLIENT_DATA['organizationAddress']['city_district_kladr_id']::VARCHAR                       AS WORK_PLACE_CITY_DISTRICT_KLADR_ID
              , CLIENT_DATA['organizationAddress']['house_fias_id']::VARCHAR                                AS WORK_PLACE_HOUSE_FIAS_ID
              , CLIENT_DATA['organizationAddress']['house_kladr_id']::VARCHAR                               AS WORK_PLACE_HOUSE_KLADR_ID
              , CLIENT_DATA['organizationAddress']['kladr_id']::VARCHAR                                     AS WORK_PLACE_KLADR_ID
              , CLIENT_DATA['organizationAddress']['region_fias_id']::VARCHAR                               AS WORK_PLACE_REGION_FIAS_ID
              , CLIENT_DATA['organizationAddress']['region_kladr_id']::VARCHAR                              AS WORK_PLACE_REGION_KLADR_ID
              , CLIENT_DATA['organizationAddress']['settlement_fias_id']::VARCHAR                           AS WORK_PLACE_SETTLEMENT_FIAS_ID
              , CLIENT_DATA['organizationAddress']['settlement_kladr_id']::VARCHAR                          AS WORK_PLACE_SETTLEMENT_KLADR_ID
              , CLIENT_DATA['organizationAddress']['stead_fias_id']::VARCHAR                                AS WORK_PLACE_STEAD_FIAS_ID
              , CLIENT_DATA['organizationAddress']['stead_kladr_id']::VARCHAR                               AS WORK_PLACE_STEAD_KLADR_ID
              , CLIENT_DATA['organizationAddress']['stead_type_full']::VARCHAR                              AS WORK_PLACE_STEAD_TYPE_FULL
              , CLIENT_DATA['organizationAddress']['street_fias_id']::VARCHAR                               AS WORK_PLACE_STREET_FIAS_ID
              , CLIENT_DATA['organizationAddress']['street_kladr_id']::VARCHAR                              AS WORK_PLACE_STREET_KLADR_ID
              , CLIENT_DATA['organizationAddress']['fias_id']::VARCHAR                                      AS WORK_PLACE_FIAS_ID
              , CLIENT_DATA['organizationAddress']['house_type_full']::VARCHAR                              AS WORK_PLACE_HOUSE_TYPE_FULL
              , CLIENT_DATA['organizationAddress']['result']::VARCHAR                                       AS WORK_PLACE_ADDRESS
              , CLIENT_DATA['organizationAddress']['capital_marker']::VARCHAR                               AS WORK_PLACE_CAPITAL_MARKER
              , CLIENT_DATA['organizationAddress']['fias_level']::VARCHAR                                   AS WORK_PLACE_FIAS_LEVEL
              , CLIENT_DATA['organizationAddress']['postal_code']::VARCHAR                                  AS WORK_PLACE_POSTAL_CODE
              , CLIENT_DATA['organizationAddress']['timezone']::VARCHAR                                     AS WORK_PLACE_TIMEZONE
              , CLIENT_DATA['organizationData']['data']['employee_count']::NUMBER                           AS WORK_PLACE_EMPLOYEE_COUNT
              , CLIENT_DATA['organizationData']['data']['inn']::VARCHAR                                     AS WORK_PLACE_INN
              , CLIENT_DATA['organizationData']['data']['okved']::VARCHAR                                   AS WORK_PLACE_OKVED
              , CLIENT_DATA['organizationData']['data']['okved_type']::VARCHAR                              AS WORK_PLACE_OKVED_TYPE
              , CLIENT_DATA['organizationData']['data']['okato']::VARCHAR                                   AS WORK_PLACE_OKATO
              , CLIENT_DATA['organizationData']['data']['okfs']::VARCHAR                                    AS WORK_PLACE_OKFS
              , CLIENT_DATA['organizationData']['data']['okogu']::VARCHAR                                   AS WORK_PLACE_OKOGU
              , CLIENT_DATA['organizationData']['data']['okpo']::VARCHAR                                    AS WORK_PLACE_OKPO
              , CLIENT_DATA['organizationData']['data']['oktmo']::VARCHAR                                   AS WORK_PLACE_OKTMO
              , CLIENT_DATA['organizationData']['data']['ogrn']::VARCHAR                                    AS WORK_PLACE_OGRN
              , TO_DATE(CLIENT_DATA['organizationData']['data']['ogrn_date']::VARCHAR)                      AS WORK_PLACE_OGRN_DATE
              , CLIENT_DATA['organizationData']['value']::VARCHAR                                           AS WORK_PLACE_NAME
              , CLIENT_DATA['organizationData']['data']['opf']['short']::VARCHAR                            AS WORK_PLACE_OPF
              , CLIENT_DATA['organizationData']['data']['opf']['full']::VARCHAR                             AS WORK_PLACE_OPF_FULL
              , CLIENT_DATA['organizationData']['data']['finance']['expense']::VARCHAR                      AS WORK_PLACE_FINANCE_EXPENSE
              , CLIENT_DATA['organizationData']['data']['finance']['income']::VARCHAR                       AS WORK_PLACE_FINANCE_INCOME
              , CLIENT_DATA['organizationData']['data']['finance']['penalty']::VARCHAR                      AS WORK_PLACE_FINANCE_PENALTY
              , CLIENT_DATA['organizationData']['data']['finance']['revenue']::VARCHAR                      AS WORK_PLACE_FINANCE_REVENUE
              , CLIENT_DATA['organizationData']['data']['finance']['tax_system']::VARCHAR                   AS WORK_PLACE_FINANCE_TAX_SYSTEM
              , CLIENT_DATA['organizationData']['data']['finance']['year']::VARCHAR                         AS WORK_PLACE_FINANCE_YEAR
              , CLIENT_DATA['passportIssueDate']::VARCHAR                                                   AS PASSPORTISSUEDATE
              , CLIENT_DATA['passportIssuedBy']::VARCHAR                                                    AS PASSPORTISSUEDBY
              , CLIENT_DATA['passportIssuerCode']::VARCHAR                                                  AS PASSPORTISSUERCODE
              , CLIENT_DATA['passportSeriesNumber']::VARCHAR                                                AS PASSPORTSERIESNUMBER
              , CLIENT_DATA['phone']::VARCHAR                                                               AS PHONE
              , CLIENT_DATA['registrationAddress']['federal_district']::VARCHAR                             AS REGISTRATIONADDRESS_FEDERAL_DISTRICT
              , CLIENT_DATA['registrationAddress']['region']::VARCHAR                                       AS REGISTRATIONADDRESS_REGION
              , CLIENT_DATA['registrationAddress']['region_type_full']::VARCHAR                             AS REGISTRATIONADDRESS_REGION_TYPE_FULL
              , CASE
                    WHEN CLIENT_DATA['registrationAddress']['city']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['city']::VARCHAR
                    WHEN CLIENT_DATA['registrationAddress']['settlement_with_type']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['settlement_with_type']::VARCHAR
                    WHEN CLIENT_DATA['registrationAddress']['area_with_type']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['area_with_type']::VARCHAR
                    ELSE CLIENT_DATA['registrationAddress']['region']::VARCHAR
                END                                                                                         AS REGISTRATIONADDRESS_CITY
              , CASE
                    WHEN CLIENT_DATA['registrationAddress']['city_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['city_type_full']::VARCHAR
                    WHEN CLIENT_DATA['registrationAddress']['settlement_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['settlement_type_full']::VARCHAR
                    WHEN CLIENT_DATA['registrationAddress']['area_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['registrationAddress']['area_type_full']::VARCHAR
                    ELSE CLIENT_DATA['registrationAddress']['region_type_full']::VARCHAR
                END                                                                                         AS REGISTRATIONADDRESS_CITY_TYPE_FULL
              , CLIENT_DATA['registrationAddress']['city_district_with_type']::VARCHAR                      AS REGISTRATIONADDRESS_CITY_DISTRICT_WITH_TYPE
              , CLIENT_DATA['registrationAddress']['street_with_type']::VARCHAR                             AS REGISTRATIONADDRESS_STREET
              , CLIENT_DATA['registrationAddress']['city_fias_id']::VARCHAR                                 AS REGISTRATIONADDRESS_CITY_FIAS_ID
              , CLIENT_DATA['registrationAddress']['city_kladr_id']::VARCHAR                                AS REGISTRATIONADDRESS_CITY_KLADR_ID
              , CLIENT_DATA['registrationAddress']['fias_code']::VARCHAR                                    AS REGISTRATIONADDRESS_FIAS_CODE
              , CLIENT_DATA['registrationAddress']['area_fias_id']::VARCHAR                                 AS REGISTRATIONADDRESS_AREA_FIAS_ID
              , CLIENT_DATA['registrationAddress']['area_kladr_id']::VARCHAR                                AS REGISTRATIONADDRESS_AREA_KLADR_ID
              , CLIENT_DATA['registrationAddress']['city_district_fias_id']::VARCHAR                        AS REGISTRATIONADDRESS_CITY_DISTRICT_FIAS_ID
              , CLIENT_DATA['registrationAddress']['city_district_kladr_id']::VARCHAR                       AS REGISTRATIONADDRESS_CITY_DISTRICT_KLADR_ID
              , CLIENT_DATA['registrationAddress']['house_fias_id']::VARCHAR                                AS REGISTRATIONADDRESS_HOUSE_FIAS_ID
              , CLIENT_DATA['registrationAddress']['house_kladr_id']::VARCHAR                               AS REGISTRATIONADDRESS_HOUSE_KLADR_ID
              , CLIENT_DATA['registrationAddress']['kladr_id']::VARCHAR                                     AS REGISTRATIONADDRESS_KLADR_ID
              , CLIENT_DATA['registrationAddress']['region_fias_id']::VARCHAR                               AS REGISTRATIONADDRESS_REGION_FIAS_ID
              , CLIENT_DATA['registrationAddress']['region_kladr_id']::VARCHAR                              AS REGISTRATIONADDRESS_REGION_KLADR_ID
              , CLIENT_DATA['registrationAddress']['settlement_fias_id']::VARCHAR                           AS REGISTRATIONADDRESS_SETTLEMENT_FIAS_ID
              , CLIENT_DATA['registrationAddress']['settlement_kladr_id']::VARCHAR                          AS REGISTRATIONADDRESS_SETTLEMENT_KLADR_ID
              , CLIENT_DATA['registrationAddress']['stead_fias_id']::VARCHAR                                AS REGISTRATIONADDRESS_STEAD_FIAS_ID
              , CLIENT_DATA['registrationAddress']['stead_kladr_id']::VARCHAR                               AS REGISTRATIONADDRESS_STEAD_KLADR_ID
              , CLIENT_DATA['registrationAddress']['stead_type_full']::VARCHAR                              AS REGISTRATIONADDRESS_STEAD_TYPE_FULL
              , CLIENT_DATA['registrationAddress']['street_fias_id']::VARCHAR                               AS REGISTRATIONADDRESS_STREET_FIAS_ID
              , CLIENT_DATA['registrationAddress']['street_kladr_id']::VARCHAR                              AS REGISTRATIONADDRESS_STREET_KLADR_ID
              , CLIENT_DATA['registrationAddress']['house_type_full']::VARCHAR                              AS REGISTRATIONADDRESS_HOUSE_TYPE_FULL
              , CLIENT_DATA['registrationAddress']['result']::VARCHAR                                       AS REGISTRATIONADDRESS_ADDRESS
              , CLIENT_DATA['registrationAddress']['capital_marker']::VARCHAR                               AS REGISTRATIONADDRESS_CAPITAL_MARKER
              , CLIENT_DATA['registrationAddress']['fias_level']::VARCHAR                                   AS REGISTRATIONADDRESS_FIAS_LEVEL
              , CLIENT_DATA['registrationAddress']['postal_code']::VARCHAR                                  AS REGISTRATIONADDRESS_POSTAL_CODE
              , CLIENT_DATA['registrationAddress']['timezone']::VARCHAR                                     AS REGISTRATIONADDRESS_TIMEZONE
              , CLIENT_DATA['registrationAddress']['tax_office']::VARCHAR                                   AS REGISTRATIONADDRESS_TAX_OFFICE
              , CLIENT_DATA['registrationAddress']['tax_office_legal']::VARCHAR                             AS REGISTRATIONADDRESS_TAX_OFFICE_LEGAL
              , CLIENT_DATA['residentialAddress']['federal_district']::VARCHAR                              AS RESIDENTIALADDRESS_FEDERAL_DISTRICT
              , CLIENT_DATA['residentialAddress']['region']::VARCHAR                                        AS RESIDENTIALADDRESS_REGION
              , CLIENT_DATA['residentialAddress']['region_type_full']::VARCHAR                              AS RESIDENTIALADDRESS_REGION_TYPE_FULL
              , CASE
                    WHEN CLIENT_DATA['residentialAddress']['city']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['city']::VARCHAR
                    WHEN CLIENT_DATA['residentialAddress']['settlement_with_type']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['settlement_with_type']::VARCHAR
                    WHEN CLIENT_DATA['residentialAddress']['area_with_type']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['area_with_type']::VARCHAR
                    ELSE CLIENT_DATA['residentialAddress']['region']::VARCHAR
                END                                                                                         AS RESIDENTIALADDRESS_CITY
              , CASE
                    WHEN CLIENT_DATA['residentialAddress']['city_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['city_type_full']::VARCHAR
                    WHEN CLIENT_DATA['residentialAddress']['settlement_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['settlement_type_full']::VARCHAR
                    WHEN CLIENT_DATA['residentialAddress']['area_type_full']::VARCHAR IS NOT NULL
                        THEN CLIENT_DATA['residentialAddress']['area_type_full']::VARCHAR
                    ELSE CLIENT_DATA['residentialAddress']['region_type_full']::VARCHAR
                END                                                                                         AS RESIDENTIALADDRESS_CITY_TYPE_FULL
              , CLIENT_DATA['residentialAddress']['city_district_with_type']::VARCHAR                       AS RESIDENTIALADDRESS_CITY_DISTRICT_WITH_TYPE
              , CLIENT_DATA['residentialAddress']['street_with_type']::VARCHAR                              AS RESIDENTIALADDRESS_STREET
              , CLIENT_DATA['residentialAddress']['city_fias_id']::VARCHAR                                  AS RESIDENTIALADDRESS_CITY_FIAS_ID
              , CLIENT_DATA['residentialAddress']['city_kladr_id']::VARCHAR                                 AS RESIDENTIALADDRESS_CITY_KLADR_ID
              , CLIENT_DATA['residentialAddress']['fias_code']::VARCHAR                                     AS RESIDENTIALADDRESS_FIAS_CODE
              , CLIENT_DATA['residentialAddress']['area_fias_id']::VARCHAR                                  AS RESIDENTIALADDRESS_AREA_FIAS_ID
              , CLIENT_DATA['residentialAddress']['area_kladr_id']::VARCHAR                                 AS RESIDENTIALADDRESS_AREA_KLADR_ID
              , CLIENT_DATA['residentialAddress']['city_district_fias_id']::VARCHAR                         AS RESIDENTIALADDRESS_CITY_DISTRICT_FIAS_ID
              , CLIENT_DATA['residentialAddress']['city_district_kladr_id']::VARCHAR                        AS RESIDENTIALADDRESS_CITY_DISTRICT_KLADR_ID
              , CLIENT_DATA['residentialAddress']['house_fias_id']::VARCHAR                                 AS RESIDENTIALADDRESS_HOUSE_FIAS_ID
              , CLIENT_DATA['residentialAddress']['house_kladr_id']::VARCHAR                                AS RESIDENTIALADDRESS_HOUSE_KLADR_ID
              , CLIENT_DATA['residentialAddress']['kladr_id']::VARCHAR                                      AS RESIDENTIALADDRESS_KLADR_ID
              , CLIENT_DATA['residentialAddress']['region_fias_id']::VARCHAR                                AS RESIDENTIALADDRESS_REGION_FIAS_ID
              , CLIENT_DATA['residentialAddress']['region_kladr_id']::VARCHAR                               AS RESIDENTIALADDRESS_REGION_KLADR_ID
              , CLIENT_DATA['residentialAddress']['settlement_fias_id']::VARCHAR                            AS RESIDENTIALADDRESS_SETTLEMENT_FIAS_ID
              , CLIENT_DATA['residentialAddress']['settlement_kladr_id']::VARCHAR                           AS RESIDENTIALADDRESS_SETTLEMENT_KLADR_ID
              , CLIENT_DATA['residentialAddress']['stead_fias_id']::VARCHAR                                 AS RESIDENTIALADDRESS_STEAD_FIAS_ID
              , CLIENT_DATA['residentialAddress']['stead_kladr_id']::VARCHAR                                AS RESIDENTIALADDRESS_STEAD_KLADR_ID
              , CLIENT_DATA['residentialAddress']['stead_type_full']::VARCHAR                               AS RESIDENTIALADDRESS_STEAD_TYPE_FULL
              , CLIENT_DATA['residentialAddress']['street_fias_id']::VARCHAR                                AS RESIDENTIALADDRESS_STREET_FIAS_ID
              , CLIENT_DATA['residentialAddress']['street_kladr_id']::VARCHAR                               AS RESIDENTIALADDRESS_STREET_KLADR_ID
              , CLIENT_DATA['residentialAddress']['house_type_full']::VARCHAR                               AS RESIDENTIALADDRESS_HOUSE_TYPE_FULL
              , CLIENT_DATA['residentialAddress']['result']::VARCHAR                                        AS RESIDENTIALADDRESS_ADDRESS
              , CLIENT_DATA['residentialAddress']['capital_marker']::VARCHAR                                AS RESIDENTIALADDRESS_CAPITAL_MARKER
              , CLIENT_DATA['residentialAddress']['fias_level']::VARCHAR                                    AS RESIDENTIALADDRESS_FIAS_LEVEL
              , CLIENT_DATA['residentialAddress']['postal_code']::VARCHAR                                   AS RESIDENTIALADDRESS_POSTAL_CODE
              , CLIENT_DATA['residentialAddress']['timezone']::VARCHAR                                      AS RESIDENTIALADDRESS_TIMEZONE
              , CLIENT_DATA['residentialAddress']['tax_office']::VARCHAR                                    AS RESIDENTIALADDRESS_TAX_OFFICE
              , CLIENT_DATA['residentialAddress']['tax_office_legal']::VARCHAR                              AS RESIDENTIALADDRESS_TAX_OFFICE_LEGAL
              , CLIENT_DATA['snils']::VARCHAR                                                               AS SNILS
            FROM PROD_DWH.KIMBALL.CALCULATION_MFO
            WHERE
                CLIENT_DATA IS NOT NULL
            QUALIFY
                ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY CALCULATION_DTM DESC) = 1
                            )
           , ASKED_PARAMS AS (
            SELECT
                DEAL_ID
              , LAST_VALUE(CREDIT_AMOUNT) OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM DESC) AS ASKED_AMOUNT
              , LAST_VALUE(CREDIT_TERM) OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM DESC)   AS ASKED_TERM
            FROM PROD_DWH.KIMBALL.CALCULATION_MFO
            WHERE
                  EVENT_NAME IN ('mfoCreateApplication', 'mfoFormLoad')
              AND CREDIT_AMOUNT IS NOT NULL
              AND DEAL_ID IS NOT NULL
            QUALIFY
                ROW_NUMBER() OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM) = 1
                             )
           , APPROVE_PARAMS AS (
            SELECT
                DEAL_ID
              , LAST_VALUE(CREDIT_AMOUNT) OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM DESC) AS APPROVE_AMOUNT
              , LAST_VALUE(CREDIT_TERM) OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM DESC)   AS APPROVE_TERM
            FROM PROD_DWH.KIMBALL.CALCULATION_MFO
            WHERE
                  EVENT_NAME NOT IN ('mfoCreateApplication', 'mfoFormLoad')
              AND CREDIT_AMOUNT IS NOT NULL
              AND DEAL_ID IS NOT NULL
            QUALIFY
                ROW_NUMBER() OVER (PARTITION BY DEAL_ID ORDER BY CALCULATION_DTM) = 1
                               )
        SELECT
            *
        FROM
            DEALS
            LEFT JOIN CLIENT_INFO
                USING (USER_ID)
            LEFT JOIN ASKED_PARAMS
                USING (DEAL_ID)
            LEFT JOIN APPROVE_PARAMS
                USING (DEAL_ID)
    )
WHERE
    FEDERAL_DISTRICT IS NOT NULL
    AND
    deal_status = 'Выдача'
    GROUP BY ALL