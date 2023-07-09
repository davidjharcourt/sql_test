SELECT 
        policy_term.BUSINESS_UNIT_CD 
    ,   policy_term.BUSINESS_UNIT_DESC
    ,   producer_profit_center_bu.PROFIT_CENTER_CD as PROFIT_CENTER_CD
    ,   producer_profit_center_bu.PROFIT_CENTER_DESC 
 --   ,   peril.peril_cd
 --   ,   peril.peril_desc
    ,   product.product_cd
    ,   product.product_desc
    ,   policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO as ACCNTNUM
    ,   policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO||CASE WHEN excess.policy_term_id IS NOT NULL THEN '-'||lpad(excess.LAYER_NO,2,'0') ELSE NULL END as POLICYNUM
    ,   policy_term.PRIMARY_INSURED_NM as ACCNTNAME
    ,   policy_term.POLICY_TERM_EFFECTIVE_DT
    ,   policy_term.POLICY_TERM_EXPIRATION_DT
    ,   policy_term.CANCELLATION_EFFECTIVE_DT
    ,   policy_term.SIC_CD
    ,   policy_term.SIC_Desc
    ,   risk.RISK_CATG_CD 
    ,   risk.RISK_CATG_DESC 
    ,   location.STREET_ADDRESS_1_TXT 
    ,   location.CITY_NM 
    ,   location.STATE_PROVINCE_CD 
    ,   location.STATE_PROVINCE_NM 
    ,   location.POSTAL_CD 
    ,   location.COUNTRY_CD 
    ,   location.COUNTRY_NM 
    ,   risk_property_structure.risk_id
    ,   risk_property_structure.location_cd 
    ,   risk_property_structure.building_cd 
    ,   risk_property_structure.construction_cd 
    ,   risk_property_structure.CONSTRUCTION_DESC
    ,   risk_property_structure.occupancy_scheme_desc 
    ,   risk_property_structure.occupancy_cd 
    ,   risk_property_structure.occupancy_desc 
    ,   CASE WHEN risk_property_structure.stories_no = '' THEN 'Unknown' ELSE risk_property_structure.stories_no END as stories_no
    ,   CASE when risk_property_structure.built_yr = '1900' THEN '' else risk_property_structure.built_yr end as built_yr
    ,   risk_property_structure.square_footage_no 
    ,   covered_property_structure.building_replacement_amt 
    ,   covered_property_structure.contents_replacement_amt 
    ,   covered_property_structure.business_int_replacement_amt 
    ,   covered_property_structure.earthquake_ind 
    ,   covered_property_structure.hurricane_ind 
    ,   covered_property_structure.wind_hail_ind 
    ,   covered_property_structure.fire_ind 
    ,   covered_property_structure.terrorism_ind 
    ,   covered_property_structure.flood_ind 
    ,   covered_property_structure.nbcr_ind 
--    ,   limit_property_structure.blanket_agg_limit_amt     
--    ,   limit_property_structure.blanket_limit_amt 
    ,   limit_property_structure.blanket_building_limit_amt 
    ,   limit_property_structure.blanket_contents_limit_amt 
    ,   limit_property_structure.blanket_bi_limit_amt 
    ,   limit_property_structure.blanket_combined_limit_amt 
--    ,   limit_property_structure.site_limit_amt 
--    ,   limit_property_structure.combined_limit_amt     
    ,   limit_property_structure.building_limit_amt 
    ,   limit_property_structure.contents_limit_amt 
    ,   limit_property_structure.bi_limit_amt 
--    ,   deductible_property_structure.blanket_agg_deductible_amt   
    ,   deductible_property_structure.blanket_deductible_amt  
    ,   deductible_property_structure.blanket_bldg_deductible_amt 
    ,   deductible_property_structure.blanket_cnts_deductible_amt 
    ,   deductible_property_structure.blanket_bi_deductible_amt 
    ,   deductible_property_structure.blanket_bi_deductible_pcnt
    ,   deductible_property_structure.blanket_bi_deductible_dur_no
    ,   deductible_property_structure.blanket_bi_duration_unit_desc
    ,   deductible_property_structure.blanket_comb_deductible_amt 
    ,   deductible_property_structure.site_deductible_amt 
    ,   deductible_property_structure.combined_deductible_amt
--    ,   null as building_deductible_amt
--    ,   deductible_property_structure.building_deductible_pcnt
--    ,   null as contents_deductible_amt
    ,   deductible_property_structure.contents_deductible_pcnt
--    ,   null as bi_deductible_amt
--    ,   deductible_property_structure.bi_deductible_duration_no
--    ,   deductible_property_structure.bi_duration_unit_desc
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
WHERE 
      producer_profit_center_bu.PROFIT_CENTER_CD <> '2758'
    and
      policy_term.BUSINESS_UNIT_CD like 'A%'
    and
      policy_term.POLICY_TERM_EFFECTIVE_DT <= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
    and
      policy_term.POLICY_TERM_EXPIRATION_DT > to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
    and
      (policy_term.CANCELLATION_EFFECTIVE_DT > to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd') or policy_term.CANCELLATION_EFFECTIVE_DT is null)
    and 
      (risk_peril_product.deleted_dt >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd') or risk_peril_product.deleted_dt is null)
    and
      (risk_property_structure.deleted_dt >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd') or risk_property_structure.deleted_dt is null)
    and 
      peril.peril_cd in ('FIRE')
ORDER BY 
        policy_term.POLICY_NUMBER_CD 
    ,   policy_term.POLICY_MODULE_CD
    ,   risk_property_structure.location_cd 
    ,   risk_property_structure.building_cd 
 --   ,   peril.peril_cd