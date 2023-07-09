DEFINE PIF_DATE = "20230630";
SELECT policy_term.*
--    policy_term.*,
--    producer_profit_center_bu.*,
--    company.*,
--    excess.*,
--    risk.*,
--    location.*,
--    risk_property_structure.*,
--    covered_property_structure.*,
--    risk_peril_product.*,
--    product.*,
--    peril.*,
--    limit_property_structure.*,
--    deductible_property_structure.*,
--    facultative.*
FROM 
        xdm_mart.policy_term
    INNER JOIN
        xdm_mart.producer_profit_center_bu
      on
        policy_term.producer_profit_center_bu_id =  producer_profit_center_bu.producer_profit_center_bu_id
    INNER JOIN
        xdm_mart.company
      on
        policy_term.company_id =  company.company_id
    LEFT JOIN
        xdm_mart.excess
      on  
        policy_term.policy_term_id = excess.policy_term_id
    INNER JOIN 
            xdm_mart.risk 
        ON 
            policy_term.policy_term_id = risk.policy_term_id
    INNER JOIN 
            xdm_mart.location 
        ON 
            risk.location_id = location.location_id
    INNER JOIN 
            xdm_mart.risk_property_structure 
        ON 
            risk.risk_id = risk_property_structure.risk_id
    INNER JOIN 
            xdm_mart.covered_property_structure 
        ON 
            risk.risk_id = covered_property_structure.risk_id
    INNER JOIN 
            xdm_mart.risk_peril_product
        ON
            risk.risk_id = risk_peril_product.risk_id
    LEFT JOIN
            xdm_mart.product
        ON
            risk_peril_product.product_id = product.product_id
    LEFT JOIN
            xdm_mart.peril
        ON
            risk_peril_product.peril_id = peril.peril_id
    LEFT JOIN 
            xdm_mart.limit_property_structure 
        ON 
            risk_peril_product.risk_peril_product_id = limit_property_structure.risk_peril_product_id
    LEFT JOIN
            xdm_mart.deductible_property_structure
        ON
            risk_peril_product.risk_peril_product_id = deductible_property_structure.risk_peril_product_id
    LEFT JOIN
            xdm_mart.facultative
        ON
            risk.risk_id = facultative.risk_id
WHERE policy_term.BUSINESS_UNIT_CD like 'A%'
    and
      policy_term.POLICY_TERM_EFFECTIVE_DT <= to_date(&PIF_DATE, 'yyyymmdd')
    and
      policy_term.POLICY_TERM_EXPIRATION_DT > to_date(&PIF_DATE, 'yyyymmdd')
    and
      (policy_term.CANCELLATION_EFFECTIVE_DT > to_date(&PIF_DATE, 'yyyymmdd') or policy_term.CANCELLATION_EFFECTIVE_DT is null)
    and 
      (risk_peril_product.deleted_dt >= to_date(&PIF_DATE, 'yyyymmdd') or risk_peril_product.deleted_dt is null)
    and
      (risk_property_structure.deleted_dt >= to_date(&PIF_DATE, 'yyyymmdd') or risk_property_structure.deleted_dt is null)
    --and rownum <= 100

ORDER BY 
        policy_term.POLICY_NUMBER_CD 
    ,   policy_term.POLICY_MODULE_CD
    ,   risk_property_structure.location_cd 
    ,   risk_property_structure.building_cd 
 --   ,   peril.peril_cd;