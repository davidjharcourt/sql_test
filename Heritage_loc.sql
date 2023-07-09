--DEFINE BU = "'GARS'";
--DEFINE LOBNAME = "'GARS_Condo'";
--DEFINE PIF_DATE_yyyymmdd = "'20201231'"; 

-- replace mdm construction_cd with the number used by many systems
-- long-term this will be replaced with a lookup to the mdm data loaded into xdm_stage
WITH CONSTRUCTION_DECODE as (
  select 'UNKNOWN' as CONSTRUCTION_DESC, 0 as CONSTRUCTION_CD     from dual union
  select 'FRAME' as CONSTRUCTION_DESC, 1 as CONSTRUCTION_CD     from dual union
  select 'JOISTED MASONRY' as CONSTRUCTION_DESC, 2 as CONSTRUCTION_CD     from dual union
  select 'NON-COMBUSTIBLE' as CONSTRUCTION_DESC, 3 as CONSTRUCTION_CD     from dual union
  select 'MASONRY NON-COMBUSTIBLE' as CONSTRUCTION_DESC, 4 as CONSTRUCTION_CD     from dual union
  select 'MODIFIED FIRE RESISTIVE' as CONSTRUCTION_DESC, 5 as CONSTRUCTION_CD     from dual union
  select 'FIRE RESISTIVE' as CONSTRUCTION_DESC, 6 as CONSTRUCTION_CD     from dual union
  select 'SUP MSNRY NON-COMBUSTIBLE' as CONSTRUCTION_DESC, 9 as CONSTRUCTION_CD     from dual
)

SELECT 
/* SECTION BEGIN */ --  Retain all source ID's in final export to track cardinality

        policy_term.policy_term_id 
    ,   risk.location_id 
    ,   risk.risk_id 
    ,   risk_property_structure.risk_property_structure_id 
    ,   covered_property_structure.covered_property_structure_id 

 /* SECTION END */ 

 /* SECTION BEGIN */ --  Policy Characteristics
 
    ,   policy_term.BUSINESS_UNIT_CD 
    ,   policy_term.BUSINESS_UNIT_DESC 
    ,   policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO as ACCNTNUM
    ,   policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO||CASE WHEN excess.policy_term_id IS NOT NULL THEN '-'||lpad(excess.LAYER_NO,2,'0') ELSE NULL END as POLICYNUM
    ,   policy_term.PRIMARY_INSURED_NM as ACCNTNAME
    ,   producer_profit_center_bu.PROFIT_CENTER_CD as BRANCHNAME
    ,   producer_profit_center_bu.PROFIT_CENTER_DESC
    ,   :LOBNAME as LOBNAME 
    ,   company.COMPANY_CD
    ,   company.COMPANY_NM
    ,   company.COMPANY_ABRV_CD
    ,   policy_term.POLICY_SYMBOL_CD 
    ,   policy_term.POLICY_SYMBOL_DESC 
    ,   policy_term.POLICY_NUMBER_CD 
    ,   policy_term.POLICY_MODULE_CD 
    ,   policy_term.CURRENT_VERSION_NO 

 /* SECTION END */ 

 /* SECTION BEGIN */ --  Location and risk dimensions
 
    ,   risk.RISK_CATG_CD 
    ,   risk.RISK_CATG_DESC 
    ,   location.PLACE_DESC 
    ,   location.STREET_ADDRESS_1_TXT 
    ,   location.STREET_ADDRESS_2_TXT 
    ,   location.STREET_ADDRESS_3_TXT 
    ,   location.STREET_ADDRESS_4_TXT 
    ,   location.CITY_NM 
    ,   location.STATE_PROVINCE_CD 
    ,   location.STATE_PROVINCE_NM 
    ,   location.POSTAL_CD 
    ,   location.COUNTY_CD 
    ,   location.COUNTY_NM 
    ,   location.COUNTRY_CD 
    ,   location.COUNTRY_NM 
    ,   location.COUNTRY_SCHEMA_CD 
    ,   location.SECTION_TXT 
    ,   location.RANGE_TXT 
    ,   location.TOWNSHIP_TXT 
    ,   location.LONGITUDE_CD 
    ,   location.LATITUDE_CD 
    ,   location.GEO_RESOLUTION_CD 
    ,   risk_property_structure.location_cd 
    ,   risk_property_structure.building_cd 
        -- !! number of buildings 
    ,   CONSTRUCTION_DECODE.construction_cd AS construction_cd_num 
    ,   risk_property_structure.construction_cd 
    ,   risk_property_structure.construction_desc 
    ,   risk_property_structure.construction_scheme_desc 
    ,   risk_property_structure.occupancy_cd 
    ,   risk_property_structure.occupancy_desc 
    ,   risk_property_structure.occupancy_scheme_desc 
    ,   risk_property_structure.stories_no 
    ,   covered_property_structure.building_percent_complete_no 
    ,   case when risk_property_structure.built_yr = '1900' then '9999' else risk_property_structure.built_yr end as built_yr
    ,   risk_property_structure.electrical_update_yr 
    ,   risk_property_structure.heating_update_yr 
    ,   risk_property_structure.other_update_yr 
    ,   risk_property_structure.plumbing_update_yr 
    ,   risk_property_structure.roof_update_yr 
    ,   covered_property_structure.contents_rate_grade_cd 
    ,   risk_property_structure.square_footage_no 

 /* SECTION END */ 
 
 /* SECTION BEGIN */ --  Location and risk facts
 
    ,   covered_property_structure.building_replacement_amt 
    ,   covered_property_structure.contents_replacement_amt 
    ,   covered_property_structure.business_int_replacement_amt 
    ,   covered_property_structure.building_cvrg_a_amt 
    ,   covered_property_structure.building_cvrg_b_amt 
    ,   covered_property_structure.contents_cvrg_c_amt 
    ,   covered_property_structure.business_int_cvrg_d_amt 
    ,   covered_property_structure.earthquake_ind 
    ,   covered_property_structure.hurricane_ind 
    ,   covered_property_structure.wind_hail_ind 
    ,   covered_property_structure.fire_ind 
    ,   covered_property_structure.terrorism_ind 
    ,   covered_property_structure.flood_ind 
    ,   covered_property_structure.nbcr_ind 

 /* SECTION END */ 
 
 /* SECTION BEGIN */ -- Premium Detail

    ,   covered_property_structure.written_premium_amt
    ,   covered_property_structure.annualized_premium_amt

/* SECTION END */

/* SECTION BEGIN */ --  Risk Peril Product

    ,   risk_peril_product.risk_peril_product_id
    ,   risk_peril_product.peril_id
    ,   peril.peril_cd
    ,   peril.peril_desc
    ,   risk_peril_product.source_peril_cd
    ,   risk_peril_product.product_id
    ,   product.product_cd
    ,   product.product_desc
    ,   risk_peril_product.source_product_cd
 
 /* SECTION END */ 

 /* SECTION BEGIN */ --  Limit detail

    ,   limit_property_structure.limit_property_structure_id
    ,   limit_property_structure.blanket_agg_limit_cd 
    ,   limit_property_structure.blanket_agg_limit_amt
     
    ,   limit_property_structure.blanket_limit_cd 
    ,   limit_property_structure.blanket_limit_amt 

    ,   limit_property_structure.blanket_building_limit_cd 
    ,   limit_property_structure.blanket_building_limit_amt 
    ,   limit_property_structure.blanket_contents_limit_cd 
    ,   limit_property_structure.blanket_contents_limit_amt 
    ,   limit_property_structure.blanket_bi_limit_cd 
    ,   limit_property_structure.blanket_bi_limit_amt 
    ,   limit_property_structure.blanket_combined_limit_amt 
    ,   limit_property_structure.blanket_combined_limit_cd 

    ,   limit_property_structure.site_limit_amt 
    ,   limit_property_structure.combined_limit_amt
     
    ,   limit_property_structure.building_limit_amt 
    ,   limit_property_structure.contents_limit_amt 
    ,   limit_property_structure.bi_limit_amt 

/* SECTION END */

 /* SECTION BEGIN */ --  Limit detail

    ,   deductible_property_structure.deduct_property_structure_id
    
    ,   deductible_property_structure.blanket_agg_deductible_cd 
    ,   deductible_property_structure.blanket_agg_deductible_amt
     
    ,   deductible_property_structure.blanket_deductible_cd 
    ,   deductible_property_structure.blanket_deductible_amt 

    ,   deductible_property_structure.blanket_bldg_deductible_cd 
    ,   deductible_property_structure.blanket_bldg_deductible_amt 
    ,   deductible_property_structure.blanket_cnts_deductible_cd 
    ,   deductible_property_structure.blanket_cnts_deductible_amt 
    ,   deductible_property_structure.blanket_bi_deductible_cd 
    ,   deductible_property_structure.blanket_bi_deductible_amt 
    ,   deductible_property_structure.blanket_bi_deductible_pcnt
    ,   deductible_property_structure.blanket_bi_deductible_dur_no
    ,   deductible_property_structure.blanket_bi_duration_unit_desc
    ,   deductible_property_structure.blanket_comb_deductible_amt 
    ,   deductible_property_structure.blanket_comb_deductible_cd 

    ,   deductible_property_structure.site_deductible_amt 
    ,   deductible_property_structure.combined_deductible_amt
     
    ,   null as building_deductible_amt
    ,   deductible_property_structure.building_deductible_pcnt
    ,   null as contents_deductible_amt
    ,   deductible_property_structure.contents_deductible_pcnt
    ,   null as bi_deductible_amt
    ,   deductible_property_structure.bi_deductible_duration_no
    ,   deductible_property_structure.bi_duration_unit_desc

/* SECTION END */

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
        AND
            risk_property_structure.deleted_dt >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
    LEFT JOIN
           CONSTRUCTION_DECODE 
        ON
            trim(risk_property_structure.construction_cd) = trim(CONSTRUCTION_DECODE.CONSTRUCTION_DESC) 
    INNER JOIN 
            xdm_mart.covered_property_structure 
        ON 
            risk.risk_id = covered_property_structure.risk_id
    INNER JOIN 
            xdm_mart.risk_peril_product
        ON
            risk.risk_id = risk_peril_product.risk_id
        AND
            risk_peril_product.deleted_dt >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
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
      policy_term.BUSINESS_UNIT_CD = :BU
    and
      policy_term.POLICY_TERM_EFFECTIVE_DT <= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
    and
      policy_term.POLICY_TERM_EXPIRATION_DT >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
    and
      (policy_term.CANCELLATION_EFFECTIVE_DT >= to_date(:PIF_DATE_yyyymmdd, 'yyyymmdd')
        or policy_term.CANCELLATION_EFFECTIVE_DT is NULL)
ORDER BY 
        policy_term.POLICY_NUMBER_CD 
    ,   policy_term.POLICY_MODULE_CD
    ,   risk_property_structure.location_cd 
    ,   risk_property_structure.building_cd 
    ,   peril.peril_cd
;