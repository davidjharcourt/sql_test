/*
	Query Name:
		xdm_mart_location.sql
    
	Key Background and Resources:
		1.	XDM_MART is loaded on a monthly basis. 
		2.	To report a bug in data seen in raw XDM_MART data, please submit a bug request here: https://gaig.sharepoint.com/sites/ExposureDataMart-XDM#xdm-bug-report-and-feature-requests.
        3.  To review the path that data takes from source system to XDM_MART, please review the following diagram: https://gaig.sharepoint.com/:u:/r/sites/ExposureDataMart-XDM/Shared%20Documents/Data%20Lineage-Overview.svg?csf=1&web=1&e=aAvvMv. 
        4.  For data quality review, check Tableau: 
    
	Purposes:	
		1.	Produce a table containing all business units in the shape of AIR_Contract_File that retains some of the structure and concepts of XDM_MART (e.g. policy_term_id, risk_id, policy peril indicators, etc.)
	
    Filters:
		1.	Mod period is set by replacement of the variable :PIF_DATE_yyyymmdd
	
    Outstanding Errors:
        1.  
*/

WITH policy_product as (
    /*
        Purposes:	
        1.	Determine policies in force as of PIF_DATE_yyyymmdd
        2.  Determine the product (via XDM_MART.PRODUCT) for each policy      
        Granularity of Ouptut:
        1.  Policy Term
        Outstanding Questions / Requests:
        1.  How will ISO Package policies with multiple products (e.g. DIC vs PROP) impact this? Do we want to model the lines within a policy separately or combined?
    */

	select
		distinct
            policy_term.policy_term_id
        ,   policy_term.business_unit_cd -- only needed for subsetting for now
        , 	product.product_id
        , 	product.product_cd
        , 	product.product_desc
        ,   mod_period.mod_period
	from
			xdm_mart.policy_term
		inner join 
				xdm_mart.risk  
			on 
				policy_term.policy_term_id = risk.policy_term_id
		inner join 
				xdm_mart.risk_peril_product
			on 
				risk.risk_id = risk_peril_product.risk_id
		inner join 
				xdm_mart.product
			on 
				risk_peril_product.product_id = product.product_id
        left join (
            select to_date(':pif_date', 'yyyy/mm/dd') as mod_period
            from dual
        ) mod_period
            on 1 = 1
	where 
            policy_term.POLICY_TERM_EFFECTIVE_DT <= mod_period.mod_period
		and
			policy_term.POLICY_TERM_EXPIRATION_DT > mod_period.mod_period
		--and
		--	(policy_term.CANCELLATION_EFFECTIVE_DT > mod_period.mod_period or policy_term.CANCELLATION_EFFECTIVE_DT is null)
)
-- !!debug
, policy_product_subset_pre as (
    select policy_product.*, row_number() over (partition by business_unit_cd, product_desc order by policy_term_id desc) as bu_product_row_number
    from policy_product
)
-- !!debug
, policy_product_subset as (
    select policy_product_subset_pre.*
    from policy_product_subset_pre
    where bu_product_row_number <= 5 
    -- heritage sublimit
    or policy_term_id in  (
            15787
        ,   15880
        ,   14305
        ,   13662
    )
    -- pimx multilayer
    or policy_term_id = 8341
)

, policy_product_listagg as (
    select 
            policy_product.policy_term_id
        ,   policy_product.business_unit_cd -- only needed for subsetting for now
        ,   policy_product.mod_period
        , 	listagg(policy_product.product_id, '___') within group (order by product_id) as product_id
        , 	listagg(policy_product.product_cd, '___') within group (order by product_id) as product_cd
        , 	listagg(policy_product.product_desc, '___') within group (order by product_id) as product_desc
    from
        policy_product
    group by      
            policy_product.policy_term_id
        ,   policy_product.business_unit_cd
        ,   policy_product.mod_period
)   

, location_selection as (
    SELECT 
        /* SECTION BEGIN */ --  Retain all source ID's in final export to track cardinality

            policy_term.policy_term_id 
        ,   risk.risk_id 
        ,   risk.location_id 
        ,   risk.RISK_PLACE_HASH_TXT
        ,   risk_property_structure.risk_property_structure_id 
        ,   covered_property_structure.covered_property_structure_id 

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Policy Characteristics

        ,   policy_term.BUSINESS_UNIT_CD 
        ,   policy_term.BUSINESS_UNIT_DESC 

        ,   producer_profit_center_bu.PROFIT_CENTER_CD
        ,   producer_profit_center_bu.PROFIT_CENTER_DESC

        , 	producer_profit_center_bu.PRODUCER_CD
        , 	producer_profit_center_bu.PRODUCER_DESC

        ,   customer.customer_cd

		, 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD as SPM
        , 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO as SPMV

        ,   policy_term.POLICY_SYMBOL_CD
        ,   policy_term.POLICY_SYMBOL_DESC 
        ,   policy_term.POLICY_NUMBER_CD
        ,   policy_term.POLICY_MODULE_CD
        ,   policy_term.CURRENT_VERSION_NO
        ,   policy_term.PRIMARY_INSURED_NM

        , 	policy_term.policy_term_effective_dt 
        , 	policy_term.policy_term_expiration_dt 
        , 	policy_term.CANCELLATION_EFFECTIVE_DT  

        ,   company.COMPANY_CD
        ,   company.COMPANY_NM
        ,   company.COMPANY_ABRV_CD
        
        ,   policy_product.product_cd
        ,   policy_product.product_desc
        ,   policy_product.mod_period
		,	policy_term.sic_cd
		,   policy_term.sic_desc

        /* SECTION END */ 

        /* SECTION BEGIN */ -- Policy Peril Indicators

        , 	policy_term.FIRE_IND AS POLICY_FIRE_IND
        , 	policy_term.HURRICANE_IND AS POLICY_HURRICANE_IND
        , 	policy_term.EARTHQUAKE_IND AS POLICY_EARTHQUAKE_IND
        , 	policy_term.FLOOD_IND AS POLICY_FLOOD_IND
        , 	policy_term.WIND_HAIL_IND AS POLICY_WIND_HAIL_IND
		, 	policy_term.TERRORISM_IND AS POLICY_TERRORISM_IND
		, 	policy_term.NBCR_IND as POLICY_NBCR_IND

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Risk dimensions

        ,   risk.RISK_CATG_CD 
        ,   risk.RISK_CATG_DESC 

        ,   risk_source_system.source_system_cd
        ,   risk_source_system.source_system_desc

        ,   risk_property_structure.location_cd 
        ,   risk_property_structure.building_cd 
        ,   risk_property_structure.deleted_dt as risk_deleted_dt

        /* SECTION END */


        /* SECTION BEGIN */ --  Structure Coverage Indicators

        ,   covered_property_structure.fire_ind         as structure_fire_ind
        ,   covered_property_structure.hurricane_ind    as structure_hurricane_ind
        ,   covered_property_structure.earthquake_ind   as structure_earthquake_ind
        ,   covered_property_structure.flood_ind        as structure_flood_ind
        ,   covered_property_structure.wind_hail_ind    as structure_wind_hail_ind
        ,   covered_property_structure.terrorism_ind    as structure_terrorism_ind
        ,   covered_property_structure.nbcr_ind         as structure_nbcr_ind

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Location dimensions

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

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Structure Dimensions

        ,   NVL(CONSTRUCTION_DECODE.construction_cd, CONSTRUCTION_DECODE2.construction_cd) AS construction_cd_num 
        ,   risk_property_structure.construction_cd 
        ,   risk_property_structure.construction_desc 
        ,   nvl(risk_property_structure.construction_scheme_desc, case when NVL(CONSTRUCTION_DECODE.construction_cd, CONSTRUCTION_DECODE2.construction_cd) is not null then 'ISO_FIRE' else null end) as construction_scheme_desc
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

        /* SECTION BEGIN */ --  Structure Facts

        ,   covered_property_structure.building_replacement_amt 
        ,   covered_property_structure.contents_replacement_amt 
        ,   covered_property_structure.business_int_replacement_amt 
        ,   covered_property_structure.building_cvrg_a_amt 
        ,   covered_property_structure.building_cvrg_b_amt 
        ,   covered_property_structure.contents_cvrg_c_amt 
        ,   covered_property_structure.business_int_cvrg_d_amt

        /* SECTION END */

        /* SECTION BEGIN */ -- Premium Facts

        ,   covered_property_structure.written_premium_amt
        ,   covered_property_structure.annualized_premium_amt

        /* SECTION END */

         ,   case policy_product.business_unit_cd
                when 'PIM' then
                    case
                        when policy_product.product_cd = 'PIMPROPEX' then 'PIMx'
                        else 'PIMt'
                    end
                when 'AED' then 'ALT'
                when 'GARS' then
                    case
                        when policy_product.product_cd = 'DPG' then 'GARS_Condo'
                        else 'GARS'
                    end
                else policy_product.business_unit_cd
            end as UDF5


    FROM
            policy_product_listagg policy_product
--            policy_product_subset policy_product
        INNER JOIN
                xdm_mart.policy_term
            ON  
                policy_product.policy_term_id = policy_term.policy_term_id
        LEFT JOIN
                xdm_mart.policy
            on  
                policy_term.policy_id = policy.policy_id
        LEFT JOIN
                xdm_mart.customer
            on  
                policy.customer_id = customer.customer_id
        INNER JOIN
            xdm_mart.producer_profit_center_bu
          on
            policy_term.producer_profit_center_bu_id =  producer_profit_center_bu.producer_profit_center_bu_id
        INNER JOIN
            xdm_mart.company
          on
            policy_term.company_id =  company.company_id
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
               (
                    risk_property_structure.deleted_dt >= policy_product.mod_period
                  or
                    risk_property_structure.deleted_dt is null
                )
        LEFT JOIN
          XDM_STAGE.LK_ISO_FIRE_CONSTRUC_DESC@FDWT_TEST.PROD.GAI.COM CONSTRUCTION_DECODE 
            ON
                trim(risk_property_structure.construction_cd) = trim(CONSTRUCTION_DECODE.CONSTRUCTION_DESC)
            AND
                risk_property_structure.source_system_id = construction_decode.source_system_id
        LEFT JOIN
          XDM_STAGE.LK_ISO_FIRE_CONSTRUC_DESC@FDWT_TEST.PROD.GAI.COM CONSTRUCTION_DECODE2 
            ON
                trim(risk_property_structure.construction_desc) = trim(CONSTRUCTION_DECODE2.CONSTRUCTION_DESC) 
            AND
                risk_property_structure.source_system_id = construction_decode2.source_system_id
        INNER JOIN 
                xdm_mart.covered_property_structure 
            ON 
                risk.risk_id = covered_property_structure.risk_id
        LEFT JOIN
                xdm_mart.source_system risk_source_system
            ON
                risk.source_system_id = risk_source_system.source_system_id
    ORDER BY 
            policy_term.POLICY_NUMBER_CD 
        ,   policy_term.POLICY_MODULE_CD
        ,   risk_property_structure.location_cd 
        ,   risk_property_structure.building_cd 
)
, location_selection_by_UDF5 as (

select

       /* SECTION BEGIN */ --  Retain all source ID's in final export to track cardinality

            policy_term_id
        ,   risk_id
        ,   location_id
        ,   risk_property_structure_id
        ,   covered_property_structure_id

        /* SECTION END */

        /* SECTION BEGIN */ --  Policy Characteristics

        ,   BUSINESS_UNIT_CD
        ,   BUSINESS_UNIT_DESC

        ,   PROFIT_CENTER_CD
        ,   PROFIT_CENTER_DESC

        , 	PRODUCER_CD
        , 	PRODUCER_DESC

        ,   customer_cd

		, 	SPM
        , 	SPMV

        ,   POLICY_SYMBOL_CD
        ,   POLICY_SYMBOL_DESC
        ,   POLICY_NUMBER_CD
        ,   POLICY_MODULE_CD
        ,   CURRENT_VERSION_NO
        ,   PRIMARY_INSURED_NM

        , 	policy_term_effective_dt
        , 	policy_term_expiration_dt
        , 	CANCELLATION_EFFECTIVE_DT

        ,   COMPANY_CD
        ,   COMPANY_NM
        ,   COMPANY_ABRV_CD

        ,   product_cd
        ,   product_desc
        ,   mod_period

		,	sic_cd
		,	sic_desc

        /* SECTION END */

        /* SECTION BEGIN */ -- Policy Peril Indicators

        , 	POLICY_FIRE_IND
        , 	POLICY_HURRICANE_IND
        , 	POLICY_EARTHQUAKE_IND
        , 	POLICY_FLOOD_IND
        , 	POLICY_WIND_HAIL_IND
		, 	POLICY_TERRORISM_IND
		, 	POLICY_NBCR_IND

        /* SECTION END */

        /* SECTION BEGIN */ --  Risk dimensions

        ,   RISK_CATG_CD
        ,   RISK_CATG_DESC

        ,   source_system_cd
        ,   source_system_desc

        ,   location_cd
        ,   building_cd
        ,   risk_deleted_dt

        /* SECTION END */


        /* SECTION BEGIN */ --  Structure Coverage Indicators

        ,   structure_fire_ind
        ,   structure_hurricane_ind
        ,   structure_earthquake_ind
        ,   structure_flood_ind
        ,   structure_wind_hail_ind
        ,   structure_terrorism_ind
        ,   structure_nbcr_ind

        /* SECTION END */

        /* SECTION BEGIN */ --  Location dimensions

        ,   PLACE_DESC
        ,   STREET_ADDRESS_1_TXT
        ,   STREET_ADDRESS_2_TXT
        ,   STREET_ADDRESS_3_TXT
        ,   STREET_ADDRESS_4_TXT
        ,   CITY_NM
        ,   STATE_PROVINCE_CD
        ,   STATE_PROVINCE_NM
        ,   POSTAL_CD
        ,   COUNTY_CD
        ,   COUNTY_NM
        ,   COUNTRY_CD
        ,   COUNTRY_NM
        ,   COUNTRY_SCHEMA_CD
        ,   SECTION_TXT
        ,   RANGE_TXT
        ,   TOWNSHIP_TXT
        ,   LONGITUDE_CD
        ,   LATITUDE_CD
        ,   GEO_RESOLUTION_CD

        /* SECTION END */

        /* SECTION BEGIN */ --  Structure Dimensions

        ,   construction_cd_num
        ,   construction_cd
        ,   construction_desc
        ,   construction_scheme_desc
        ,   occupancy_cd
        ,   occupancy_desc
        ,   occupancy_scheme_desc
        ,   stories_no
        ,   building_percent_complete_no
        ,   built_yr
        ,   electrical_update_yr
        ,   heating_update_yr
        ,   other_update_yr
        ,   plumbing_update_yr
        ,   roof_update_yr
        ,   contents_rate_grade_cd
        ,   square_footage_no

        /* SECTION END */

        /* SECTION BEGIN */ --  Structure Facts

        ,   building_replacement_amt
        ,   contents_replacement_amt
        ,   business_int_replacement_amt
        ,   building_cvrg_a_amt
        ,   building_cvrg_b_amt
        ,   contents_cvrg_c_amt
        ,   business_int_cvrg_d_amt

        /* SECTION END */

        /* SECTION BEGIN */ -- Premium Facts

        ,   written_premium_amt
        ,   annualized_premium_amt
        ,  RISK_PLACE_HASH_TXT


from location_selection
where :UDF5

)
, location_detail as (
    SELECT 
        /* SECTION BEGIN */ --  Retain all source ID's in final export to track cardinality

            policy_term.policy_term_id 
        ,   risk.risk_id 
        ,   risk.location_id 
        ,   risk_peril_product.risk_peril_product_id
        ,   risk_peril_product.peril_id
        ,   risk_peril_product.product_id
        ,   risk_property_structure.risk_property_structure_id 
        ,   covered_property_structure.covered_property_structure_id 
        ,   limit_property_structure.limit_property_structure_id
        ,   deductible_property_structure.deduct_property_structure_id

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Policy Characteristics

        ,   policy_term.BUSINESS_UNIT_CD 
        ,   policy_term.BUSINESS_UNIT_DESC 

        ,   producer_profit_center_bu.PROFIT_CENTER_CD
        ,   producer_profit_center_bu.PROFIT_CENTER_DESC

        , 	producer_profit_center_bu.PRODUCER_CD
        , 	producer_profit_center_bu.PRODUCER_DESC

        ,   customer.customer_cd

		, 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD as SPM
        , 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO as SPMV

        ,   policy_term.POLICY_SYMBOL_CD
        ,   policy_term.POLICY_SYMBOL_DESC 
        ,   policy_term.POLICY_NUMBER_CD
        ,   policy_term.POLICY_MODULE_CD
        ,   policy_term.CURRENT_VERSION_NO
        ,   policy_term.PRIMARY_INSURED_NM

        , 	policy_term.policy_term_effective_dt 
        , 	policy_term.policy_term_expiration_dt 
        , 	policy_term.CANCELLATION_EFFECTIVE_DT  

        ,   product.product_cd
        ,   product.product_desc
        ,   risk_peril_product.source_product_cd

        ,   company.COMPANY_CD
        ,   company.COMPANY_NM
        ,   company.COMPANY_ABRV_CD
		,   policy_term.sic_cd
		,   policy_term.sic_desc

        /* SECTION END */ 

        /* SECTION BEGIN */ -- Policy Peril Indicators

        , 	policy_term.FIRE_IND AS POLICY_FIRE_IND
        , 	policy_term.HURRICANE_IND AS POLICY_HURRICANE_IND
        , 	policy_term.EARTHQUAKE_IND AS POLICY_EARTHQUAKE_IND
        , 	policy_term.FLOOD_IND AS POLICY_FLOOD_IND
        , 	policy_term.WIND_HAIL_IND AS POLICY_WIND_HAIL_IND
		, 	policy_term.TERRORISM_IND AS POLICY_TERRORISM_IND
		, 	policy_term.NBCR_IND as POLICY_NBCR_IND

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Risk dimensions

        ,   risk.RISK_CATG_CD 
        ,   risk.RISK_CATG_DESC 

        ,   risk_source_system.source_system_cd
        ,   risk_source_system.source_system_desc

        ,   risk_property_structure.location_cd 
        ,   risk_property_structure.building_cd 
        ,   risk_property_structure.deleted_dt as risk_deleted_dt

        /* SECTION END */


        /* SECTION BEGIN */ --  Structure Coverage Indicators

        ,   covered_property_structure.fire_ind         as structure_fire_ind
        ,   covered_property_structure.hurricane_ind    as structure_hurricane_ind
        ,   covered_property_structure.earthquake_ind   as structure_earthquake_ind
        ,   covered_property_structure.flood_ind        as structure_flood_ind
        ,   covered_property_structure.wind_hail_ind    as structure_wind_hail_ind
        ,   covered_property_structure.terrorism_ind    as structure_terrorism_ind
        ,   covered_property_structure.nbcr_ind         as structure_nbcr_ind

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Location dimensions

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

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Structure Dimensions

        ,   NVL(CONSTRUCTION_DECODE.construction_cd, CONSTRUCTION_DECODE2.construction_cd) AS construction_cd_num 
        ,   risk_property_structure.construction_cd 
        ,   risk_property_structure.construction_desc 
        ,   nvl(risk_property_structure.construction_scheme_desc, case when NVL(CONSTRUCTION_DECODE.construction_cd, CONSTRUCTION_DECODE2.construction_cd) is not null then 'ISO_FIRE' else null end) as construction_scheme_desc
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

        /* SECTION BEGIN */ --  Structure Facts

        ,   covered_property_structure.building_replacement_amt 
        ,   covered_property_structure.contents_replacement_amt 
        ,   covered_property_structure.business_int_replacement_amt 
        ,   covered_property_structure.building_cvrg_a_amt 
        ,   covered_property_structure.building_cvrg_b_amt 
        ,   covered_property_structure.contents_cvrg_c_amt 
        ,   covered_property_structure.business_int_cvrg_d_amt

        /* SECTION END */

        /* SECTION BEGIN */ -- Premium Facts

        ,   covered_property_structure.written_premium_amt
        ,   covered_property_structure.annualized_premium_amt

        /* SECTION END */

        /* SECTION BEGIN */ --  Product Dimensions

        ,   peril.peril_cd
        ,   peril.peril_desc
        ,   risk_peril_product.source_peril_cd

        /* SECTION END */ 

        /* SECTION BEGIN */ --  Limit Dimensions and Facts

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

        /* SECTION BEGIN */ --  Deductible Dimensions and Facts

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

        ,   deductible_property_structure.building_deductible_amt
        ,   deductible_property_structure.building_deductible_pcnt
        ,   deductible_property_structure.contents_deductible_amt
        ,   deductible_property_structure.contents_deductible_pcnt
        ,   deductible_property_structure.bi_deductible_amt
        ,   deductible_property_structure.bi_deductible_duration_no
        ,   deductible_property_structure.bi_duration_unit_desc

    /* SECTION END */

    FROM
            policy_product_listagg policy_product
--            policy_product_subset policy_product
        INNER JOIN
                xdm_mart.policy_term
            ON  
                policy_product.policy_term_id = policy_term.policy_term_id
        LEFT JOIN
                xdm_mart.policy
            on  
                policy_term.policy_id = policy.policy_id
        LEFT JOIN
                xdm_mart.customer
            on  
                policy.customer_id = customer.customer_id
        INNER JOIN
            xdm_mart.producer_profit_center_bu
          on
            policy_term.producer_profit_center_bu_id =  producer_profit_center_bu.producer_profit_center_bu_id
        INNER JOIN
            xdm_mart.company
          on
            policy_term.company_id =  company.company_id
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
               (
                    risk_property_structure.deleted_dt >= policy_product.mod_period
                  or
                    risk_property_structure.deleted_dt is null
                )
        LEFT JOIN
          XDM_STAGE.LK_ISO_FIRE_CONSTRUC_DESC@FDWT_TEST.PROD.GAI.COM CONSTRUCTION_DECODE 
            ON
                trim(risk_property_structure.construction_cd) = trim(CONSTRUCTION_DECODE.CONSTRUCTION_DESC)
            AND
                risk_property_structure.source_system_id = construction_decode.source_system_id
        LEFT JOIN
          XDM_STAGE.LK_ISO_FIRE_CONSTRUC_DESC@FDWT_TEST.PROD.GAI.COM CONSTRUCTION_DECODE2 
            ON
                trim(risk_property_structure.construction_desc) = trim(CONSTRUCTION_DECODE2.CONSTRUCTION_DESC) 
            AND
                risk_property_structure.source_system_id = construction_decode2.source_system_id
        INNER JOIN 
                xdm_mart.covered_property_structure 
            ON 
                risk.risk_id = covered_property_structure.risk_id
        INNER JOIN 
                xdm_mart.risk_peril_product
            ON
                risk.risk_id = risk_peril_product.risk_id
           AND
          (
              risk_property_structure.deleted_dt >= policy_product.mod_period
            or
              risk_property_structure.deleted_dt is null
                )
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
                xdm_mart.source_system risk_source_system
            ON
                risk.source_system_id = risk_source_system.source_system_id
    ORDER BY 
            policy_term.POLICY_NUMBER_CD 
        ,   policy_term.POLICY_MODULE_CD
        ,   risk_property_structure.location_cd 
        ,   risk_property_structure.building_cd 
        ,   peril.peril_cd
)
, location_limit_deductible as (
  select
      policy_term_id
    , risk_id
    , peril_cd
    , building_limit_amt
    , contents_limit_amt
    , bi_limit_amt
    , building_deductible_amt	
    , building_deductible_pcnt	
    , contents_deductible_amt	
    , contents_deductible_pcnt	
    , bi_deductible_amt
  from
      location_detail
)
, location_perils as (
    select 
            policy_term_id
        ,   risk_id 
        ,   structure_fire_ind
        ,   structure_hurricane_ind
        ,   structure_earthquake_ind
        ,   structure_flood_ind
        ,   structure_wind_hail_ind
        ,   structure_terrorism_ind
        ,   structure_nbcr_ind
    from 
            location_selection_by_UDF5
)
, location_limit_deductible_pvt as (
  select *
  from location_limit_deductible
  pivot (
      max(building_limit_amt) bldg_limit
    , max(building_deductible_amt) as bldg_ded_amt
    , max(building_deductible_pcnt) as bldg_ded_pcnt
    , max(contents_limit_amt) cnts_limit
    , max(contents_deductible_amt) as cnts_ded_amt
    , max(contents_deductible_pcnt) as cnts_ded_pcnt
    , max(bi_limit_amt) bi_limit
    , max(bi_deductible_amt) as bi_ded_amt
    for peril_cd in (
          'EARTHQUAKE' EARTHQUAKE
        , 'FIRE'       FIRE
        , 'FLOOD'      FLOOD
        , 'HURRICANE'  HURRICANE
        , 'NBCR'       NBCR
        , 'TERRORISM'  TERRORISM
        , 'WIND_HAIL'  WIND_HAIL
    )
  )
)


, loc_perils1 as (
    SELECT 
            location_perils.policy_term_id 
        ,   location_perils.risk_id 
        ,   CASE 
                WHEN location_perils.structure_fire_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.FIRE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.FIRE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.FIRE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.FIRE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.FIRE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.FIRE_BI_LIMIT,0) > 0 THEN 'C' 
                ELSE '' 
            END as LocLimitType
		,   CASE 
                WHEN location_perils.structure_fire_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.FIRE_BLDG_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.FIRE_BLDG_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.FIRE_BLDG_LIMIT as varchar(100)) 
                ELSE '' 
            END as LimitBldg
		,   '' as LimitOther
		,   CASE 
                WHEN location_perils.structure_fire_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.FIRE_CNTS_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.FIRE_CNTS_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.FIRE_CNTS_LIMIT as varchar(100)) 
                ELSE '' 
            END as LimitContent
		,   CASE 
                WHEN location_perils.structure_fire_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.FIRE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.FIRE_BI_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.FIRE_BI_LIMIT as varchar(100)) 
                ELSE '' 
            END as LimitTime
-- Cast as Varchar returned exponential values, adding a cast as Int fixed the issue 
		,   '' as Participation1
		,   '' as Participation2
        ,   CASE 
                WHEN location_perils.structure_fire_ind = 0 THEN ''
                WHEN location_perils.structure_fire_ind = 1 THEN 'N'
                ELSE ''
            END as DeductType
        ,   '' as DeductBldg
		,   '' as DeductOther
		,   '' as DeductContent
        ,   '' as DeductTime
    from location_perils
        left join location_limit_deductible_pvt
            on location_perils.risk_id = location_limit_deductible_pvt.risk_id
)
, loc_perils2 as (
    SELECT 
            location_perils.policy_term_id 
        ,   location_perils.risk_id 
        ,   CASE 
                WHEN location_perils.structure_hurricane_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.HURRICANE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.HURRICANE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.HURRICANE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.HURRICANE_BI_LIMIT,0) > 0 THEN 'C' 
                ELSE '' 
            END as "LocLimitType 2"
		,   CASE 
                WHEN location_perils.structure_hurricane_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_BLDG_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitBldg 2"
		,   '' as "LimitOther 2"
		,   CASE 
                WHEN location_perils.structure_hurricane_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_CNTS_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_CNTS_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_CNTS_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitContent 2"
		,   CASE 
                WHEN location_perils.structure_hurricane_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.HURRICANE_BI_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_BI_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitTime 2"
		,   '' as "Participation1 2"
		,   '' as "Participation2 2"
        ,CASE 
			  WHEN location_perils.structure_hurricane_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_PCNT,0) > 0 THEN 'C'
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_AMT,0) > 0 THEN 'S' 
			  ELSE 'N' 
		 END as "DeductType 2"
        ,CASE 
			  WHEN location_perils.structure_hurricane_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_AMT,0), nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_PCNT,0)) = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_BLDG_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_BLDG_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.HURRICANE_BLDG_DED_PCNT / 100 as VARCHAR(100))
			  ELSE ''
		 END as "DeductBldg 2"
		,'' as "DeductOther 2"
		,CASE WHEN location_perils.structure_hurricane_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.HURRICANE_CNTS_DED_AMT,0), nvl(location_limit_deductible_pvt.HURRICANE_CNTS_DED_PCNT,0)) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.HURRICANE_CNTS_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_CNTS_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_CNTS_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.HURRICANE_CNTS_DED_PCNT / 100 as VARCHAR(100))
              ELSE '' 
		END as "DeductContent 2"
        ,CASE WHEN location_perils.structure_hurricane_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.HURRICANE_BI_DED_AMT,0) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.HURRICANE_BI_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.HURRICANE_BI_DED_AMT  as VARCHAR(100))
              ELSE '' 
		 END as "DeductTime 2"	
    from location_perils
        left join location_limit_deductible_pvt
            on location_perils.risk_id = location_limit_deductible_pvt.risk_id
)
, loc_perils3 as (
    SELECT 
            location_perils.policy_term_id 
        ,   location_perils.risk_id 
        ,   CASE 
                WHEN location_perils.structure_earthquake_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_LIMIT,0) > 0 THEN 'C' 
                ELSE '' 
            END as "LocLimitType 3"
		,   CASE 
                WHEN location_perils.structure_earthquake_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_BLDG_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitBldg 3"
		,   '' as "LimitOther 3"
		,   CASE 
                WHEN location_perils.structure_earthquake_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_CNTS_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitContent 3"
		,   CASE 
                WHEN location_perils.structure_earthquake_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_BI_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitTime 3"
		,   '' as "Participation1 3"
		,   '' as "Participation2 3"
        ,CASE 
			  WHEN location_perils.structure_earthquake_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_PCNT,0) > 0 THEN 'C'
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_AMT,0) > 0 THEN 'S' 
			  ELSE 'N' 
		 END as "DeductType 3"
        ,CASE 
			  WHEN location_perils.structure_earthquake_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_AMT,0), nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_PCNT,0)) = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_BLDG_DED_PCNT / 100 as VARCHAR(100))
			  ELSE ''
		 END as "DeductBldg 3"
		,'' as "DeductOther 3"
		,CASE WHEN location_perils.structure_earthquake_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_AMT,0), nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_PCNT,0)) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_CNTS_DED_PCNT / 100 as VARCHAR(100))
              ELSE '' 
		END as "DeductContent 3"
        ,CASE WHEN location_perils.structure_earthquake_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_DED_AMT,0) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.EARTHQUAKE_BI_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.EARTHQUAKE_BI_DED_AMT  as VARCHAR(100))
              ELSE '' 
		 END as "DeductTime 3"	
    from location_perils
        left join location_limit_deductible_pvt
            on location_perils.risk_id = location_limit_deductible_pvt.risk_id
)
, loc_perils4 as (
    SELECT 
            location_perils.policy_term_id 
        ,   location_perils.risk_id 
        ,   CASE 
                WHEN location_perils.structure_terrorism_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.terrorism_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.terrorism_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.terrorism_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.terrorism_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.terrorism_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.terrorism_BI_LIMIT,0) > 0 THEN 'C' 
                ELSE '' 
            END as "LocLimitType 4"
		,   CASE 
                WHEN location_perils.structure_terrorism_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.terrorism_BLDG_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.terrorism_BLDG_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.terrorism_BLDG_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitBldg 4"
		,   '' as "LimitOther 4"
		,   CASE 
                WHEN location_perils.structure_terrorism_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.terrorism_CNTS_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.terrorism_CNTS_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.terrorism_CNTS_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitContent 4"
		,   CASE 
                WHEN location_perils.structure_terrorism_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.terrorism_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.terrorism_BI_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.terrorism_BI_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitTime 4"
		,   '' as "Participation1 4"
		,   '' as "Participation2 4"
        ,   CASE 
                WHEN location_perils.structure_terrorism_ind = 0 THEN ''
                WHEN location_perils.structure_terrorism_ind = 1 THEN 'N'
                ELSE ''
            END as "DeductType 4"
        ,   '' as "DeductBldg 4"
		,   '' as "DeductOther 4"
		,   '' as "DeductContent 4"
        ,   '' as "DeductTime 4"
    from location_perils
        left join location_limit_deductible_pvt
            on location_perils.risk_id = location_limit_deductible_pvt.risk_id
)
, loc_perils5 as (
    SELECT 
            location_perils.policy_term_id 
        ,   location_perils.risk_id 
        ,   CASE 
                WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.wind_hail_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.wind_hail_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_LIMIT,0) + nvl(location_limit_deductible_pvt.wind_hail_CNTS_LIMIT,0) + nvl(location_limit_deductible_pvt.wind_hail_BI_LIMIT,0) > 0 THEN 'C' 
                ELSE '' 
            END as "LocLimitType 5"
		,   CASE 
                WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_BLDG_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitBldg 5"
		,   '' as "LimitOther 5"
		,   CASE 
                WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.wind_hail_CNTS_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.wind_hail_CNTS_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_CNTS_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitContent 5"
		,   CASE 
                WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BI_LIMIT,0) = 0 THEN '' 
                WHEN nvl(location_limit_deductible_pvt.wind_hail_BI_LIMIT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_BI_LIMIT as varchar(100)) 
                ELSE '' 
            END as "LimitTime 5"
		,   '' as "Participation1 5"
		,   '' as "Participation2 5"
        ,CASE 
			  WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_PCNT,0) > 0 THEN 'C'
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_AMT,0) > 0 THEN 'S' 
			  ELSE 'N' 
		 END as "DeductType 5"
        ,CASE 
			  WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_AMT,0), nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_PCNT,0)) = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_BLDG_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_BLDG_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.wind_hail_BLDG_DED_PCNT  / 100 as VARCHAR(100))
			  ELSE ''
		 END as "DeductBldg 5"
		,'' as "DeductOther 5"
		,CASE WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
			  WHEN greatest(nvl(location_limit_deductible_pvt.wind_hail_CNTS_DED_AMT,0), nvl(location_limit_deductible_pvt.wind_hail_CNTS_DED_PCNT,0)) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.wind_hail_CNTS_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_CNTS_DED_AMT  as VARCHAR(100))
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_CNTS_DED_PCNT,0)> 0 THEN cast(location_limit_deductible_pvt.wind_hail_CNTS_DED_PCNT / 100 as VARCHAR(100))
              ELSE '' 
		END as "DeductContent 5"
        ,CASE WHEN location_perils.structure_wind_hail_ind = 0 THEN ''
			  WHEN nvl(location_limit_deductible_pvt.wind_hail_BI_DED_AMT,0) = 0 THEN ''
              WHEN nvl(location_limit_deductible_pvt.wind_hail_BI_DED_AMT,0) > 0 THEN cast(location_limit_deductible_pvt.wind_hail_BI_DED_AMT  as VARCHAR(100))
              ELSE '' 
		 END as "DeductTime 5"	
    from location_perils
        left join location_limit_deductible_pvt
            on location_perils.risk_id = location_limit_deductible_pvt.risk_id
)

, xdm_mart_location as (
    select
        
        /* 
            Columns For Further Procesing Later 
                Status: Complete
  
        */
            location_selection.policy_term_id
        ,   location_selection.risk_id
        ,   location_selection.LOCATION_CD
        ,   location_selection.BUILDING_CD
        
        ,   location_selection.product_cd
        
        ,   case location_selection.business_unit_cd
                when 'PIM' then 
                    case 
                        when location_selection.product_cd = 'PIMPROPEX' then 'PIMx'
                        else 'PIMt'
                    end
                when 'AED' then 'ALT'
                when 'GARS' then 
                    case 
                        when location_selection.product_cd = 'DPG' then 'GARS_Condo'
                        else 'GARS'
                    end
                else location_selection.business_unit_cd
            end as "LOB"
            
        , 	location_selection.PROFIT_CENTER_CD
        ,   location_selection.PROFIT_CENTER_DESC
        , 	location_selection.PRODUCER_CD
        ,   location_selection.PRODUCER_DESC
        
        , 	location_selection.POLICY_FIRE_IND
        , 	location_selection.POLICY_HURRICANE_IND
        , 	location_selection.POLICY_EARTHQUAKE_IND
        , 	location_selection.POLICY_FLOOD_IND
        , 	location_selection.POLICY_WIND_HAIL_IND
		, 	location_selection.POLICY_TERRORISM_IND
		, 	location_selection.POLICY_NBCR_IND

        ,   location_selection.structure_fire_ind
        ,   location_selection.structure_hurricane_ind
        ,   location_selection.structure_earthquake_ind
        ,   location_selection.structure_flood_ind
        ,   location_selection.structure_wind_hail_ind
        ,   location_selection.structure_terrorism_ind
        ,   location_selection.structure_nbcr_ind
        
        ,   SYSDATE as AsOfDate
        ,   location_selection.mod_period
    
        /* 
            Required Identifiers 
        
                Status:     Ready for Review
        */
        
        ,   location_selection.SPM as "ContractID"
        ,   location_selection.SPM || '_' || location_selection.RISK_CATG_CD || '_' || location_selection.RISK_ID as "LocationID"
        
        ,   'Location ' || location_selection.LOCATION_CD || ', ' || 'Building ' || location_selection.BUILDING_CD as "LocationName"
        
        /* Basic Location Details 
        
                Status:     Ready for Review
        */
        
        ,	location_selection.STREET_ADDRESS_1_TXT as "Street"
        ,	location_selection.CITY_NM as "City"
        ,	location_selection.STATE_PROVINCE_CD as "Area"
        ,   location_selection.POSTAL_CD as "PostalCode"
        ,   case 
                when location_selection.COUNTRY_CD = 'US' then location_selection.COUNTY_CD 
                when location_selection.COUNTRY_CD = 'CA' then substr(location_selection.POSTAL_CD, 1, 3)
                else null
            end as "SubArea"
        ,   location_selection.COUNTRY_CD as "CountryISO"

        /* Source columns to be converted by LK_CONSTRUC_CODE_CONVERSION on PAIR  
        
                Status:     !! Downstream Development Needed
        */
        
        ,   location_selection.construction_scheme_desc as xdm_construction_scheme_desc
        ,   location_selection.construction_cd_num as xdm_construction_cd_num
        ,   location_selection.construction_cd  as xdm_construction_cd
        ,   location_selection.construction_desc as xdm_construction_desc
        ,   null as ConstructionOther

        /* Source columns to be converted by LK_OCC_CODE_CONVERSION on PAIR   
        
                Status:     !! Downstream Development Needed
        */    
        
        ,	case when location_selection.OCCUPANCY_CD is NULL then 'SIC' 
				else location_selection.OCCUPANCY_SCHEME_DESC  end                   as xdm_occupancy_scheme_desc
        ,	case when location_selection.OCCUPANCY_CD is NULL and location_selection.SIC_CD is not NULL then location_selection.SIC_CD
				when location_selection.OCCUPANCY_CD is NULL and location_selection.SIC_CD is NULL then '0'
				else location_selection.OCCUPANCY_CD end                              as xdm_occupancy_cd
        ,	case when location_selection.OCCUPANCY_CD is NULL then location_selection.SIC_DESC
				else location_selection.OCCUPANCY_DESC end                            as xdm_occupancy_desc
        
        /* Primary Structure Characteristics   
        
                Status:     Ready for Review
        */
        
        ,   location_selection.built_yr as "YearBuilt"
        ,   ceil(location_selection.stories_no) as "NumberOfStories"
        
        ,   1 as "RiskCount"
        ,   location_selection.square_footage_no as "GrossArea"
        
        /* Builder's Risk Fields   
        
                Status:     Ready for Review
        */
        
        ,   null as "InceptionDate"
        ,   null as "ExpirationDate"
        ,   null as "ProjectCompletion"
        ,   null as "ProjectPhaseCode"
        
        /* Campus Fields  
        
                Status:     Ready for Review
        */ 
        ,   null as "IsPrimary"
        ,   null as "LocationGroup"
        
        /* Location-Level Premium  
        
                Status:     Complete
        */
        ,   case location_selection.COUNTRY_CD
                when 'US' then 'USD'
                when 'CA' then 'CAD'
                else 'USD' 
            end as "Currency"
        ,   location_selection.written_premium_amt as "Premium"

        /* Structure Values   
        
                Status:     Ready for Review
        */ 
        
        ,	location_selection.BUILDING_REPLACEMENT_AMT as "BuildingValue"
        ,   null as "OtherValue"
        ,	location_selection.CONTENTS_REPLACEMENT_AMT as "ContentsValue"
        ,	location_selection.BUSINESS_INT_REPLACEMENT_AMT as "TimeElementValue"
        
        /* Wildfire as Loc Peril 1 (LocPeril1)   
        
                Status:     !! Development Needed
        */
        
        ,   '' as LocPerils    
        ,   loc_perils1.LocLimitType
        ,   loc_perils1.LimitBldg
        ,   loc_perils1.LimitOther
        ,   loc_perils1.LimitContent
        ,   loc_perils1.LimitTime
        ,   loc_perils1.Participation1
        ,   loc_perils1.Participation2
        ,   loc_perils1.DeductType
        ,   loc_perils1.DeductBldg
        ,   loc_perils1.DeductOther
        ,   loc_perils1.DeductContent
        ,   loc_perils1.DeductTime
        ,   '' as "SublimitArea"
        
        /* Wind (Convective, Storm Surge, and Hurricane) as Loc Peril 2 (LocPeril2)    
        
                Status:     !! Development Needed
        */
        
        ,   null as "LocPerils 2"
        ,   loc_perils2."LocLimitType 2"
        ,   loc_perils2."LimitBldg 2"
        ,   loc_perils2."LimitOther 2"
        ,   loc_perils2."LimitContent 2"
        ,   loc_perils2."LimitTime 2"
        ,   loc_perils2."Participation1 2"
        ,   loc_perils2."Participation2 2"
        ,   loc_perils2."DeductType 2"
        ,   loc_perils2."DeductBldg 2"
        ,   loc_perils2."DeductOther 2"
        ,   loc_perils2."DeductContent 2"
        ,   loc_perils2."DeductTime 2"
        ,   null as "SublimitArea 2"
        
        /*  Earthquake Shake (Earthquake Shake, Liquidifaction, and Landslide) as Loc Peril 3 (LocPeril3)    
        
                Status:     !! Development Needed
        */
        
        ,   null as "LocPerils 3"
        ,   loc_perils3."LocLimitType 3"
        ,   loc_perils3."LimitBldg 3"
        ,   loc_perils3."LimitOther 3"
        ,   loc_perils3."LimitContent 3"
        ,   loc_perils3."LimitTime 3"
        ,   loc_perils3."Participation1 3"
        ,   loc_perils3."Participation2 3"
        ,   loc_perils3."DeductType 3"
        ,   loc_perils3."DeductBldg 3"
        ,   loc_perils3."DeductOther 3"
        ,   loc_perils3."DeductContent 3"
        ,   loc_perils3."DeductTime 3"
        ,   null as "SublimitArea 3"
        
        /* Terrorism as Loc Peril 4 (LocPeril4)    
        
                Status:     !! Development Needed
        */
        
        ,   null as "LocPerils 4"
        ,   loc_perils4."LocLimitType 4"
        ,   loc_perils4."LimitBldg 4"
        ,   loc_perils4."LimitOther 4"
        ,   loc_perils4."LimitContent 4"
        ,   loc_perils4."LimitTime 4"
        ,   loc_perils4."Participation1 4"
        ,   loc_perils4."Participation2 4"
        ,   loc_perils4."DeductType 4"
        ,   loc_perils4."DeductBldg 4"
        ,   loc_perils4."DeductOther 4"
        ,   loc_perils4."DeductContent 4"
        ,   loc_perils4."DeductTime 4"
        ,   null as "SublimitArea 4"
        
        /* Not used at this time    
        
                Status:     Ready for Review
        */ 
        
        ,   null as "LocPerils 5"
        ,   loc_perils5."LocLimitType 5"
        ,   loc_perils5."LimitBldg 5"
        ,   loc_perils5."LimitOther 5"
        ,   loc_perils5."LimitContent 5"
        ,   loc_perils5."LimitTime 5"
        ,   loc_perils5."Participation1 5"
        ,   loc_perils5."Participation2 5"
        ,   loc_perils5."DeductType 5"
        ,   loc_perils5."DeductBldg 5"
        ,   loc_perils5."DeductOther 5"
        ,   loc_perils5."DeductContent 5"
        ,   loc_perils5."DeductTime 5"
        ,   null as "SublimitArea 5"
        
        /* Time Element Coverage Days Covered    
        
                Status:     Ready for Review
        */ 
        
        ,   case
                when location_selection.BUSINESS_INT_REPLACEMENT_AMT is not null then 365
                else null
            end as "DaysCovered"
            
        /* Secondary Modifiers    
        
                Status:     Ready for Review
        */
        
        ,   null as "CustomFloodSOP"
        ,   null as "CustomFloodZone"
        ,   null as "FloorOfInterest"
        ,   null as "BuildingCondition"
        ,   null as "BuildingShape"
        ,   null as "Torsion"
        ,   null as "SoftStory"
        ,   null as "ShapeIrregularity"
        ,   null as "SpecialConstruction"
        ,   null as "Retrofit"
        ,   null as "ShortColumn"
        ,   null as "Ornamentation"
        ,   null as "WaterHeater"
        ,   null as "Redundancy"
        ,   null as "TallOneStory"
        ,   null as "Equipment"
        ,   null as "SealofApproval"
        ,   null as "RoofGeometry"
        ,   null as "RoofPitch"
        ,   null as "RoofCover"
        ,   null as "RoofDeck"
        ,   null as "RoofCoverAttachment"
        ,   null as "RoofDeckAttachment"
        ,   null as "RoofAnchorage"
        ,   null as "RoofAttachedStructure"
        ,   null as "RoofYearBuilt"
        ,   null as "Tank"
        ,   null as "RoofHailImpactResistance"
        ,   null as "Chimney"
        ,   null as "WallType"
        ,   null as "WallSiding"
        ,   null as "GlassType"
        ,   null as "GlassPercent"
        ,   null as "WindowProtection"
        ,   null as "ExternalDoors"
        ,   null as "BuildingExteriorOpening"
        ,   null as "BrickVeneer"
        ,   null as "FoundationConnection"
        ,   null as "FoundationType"
        ,   null as "InternalPartition"
        ,   null as "TransitionInSRC"
        ,   null as "AttachedStructures"
        ,   null as "AppurtenantStructures"
        ,   null as "Pounding"
        ,   null as "TreeExposure"
        ,   null as "SmallDebris"
        ,   null as "LargeMissile"
        ,   null as "TerrainRoughness"
        ,   null as "AdjacentBuildingHeight"
        ,   null as "BasementLevelCount"
        ,   null as "BasementFinishType"
        ,   null as "CustomElevation"
        ,   null as "CustomElevationUnit"
        ,   null as "BaseFloodElevation"
        ,   null as "BaseFloodElevationUnit"
        ,   null as "FirstFloorHeight"
        ,   null as "FirstFloorHeightUnit"
        ,   null as "ServiceEquipmentProtection"
        ,   null as "FIRMCompliance"
        ,   null as "ContentVulnerability"
        ,   null as "Certified Structures (IBHS)"
        ,   null as "PPC Code"
        ,   null as "Fire Sprinklers"
        
        /* User Defined Fields    
        
                Status:     Complete
        */
        
        ,   'XDM-'||location_selection.source_system_desc as "UDF1" -- Source System
        ,   location_selection.building_replacement_amt as "UDF2" -- Builder's Risk Completed Building Value (doesn't include anything besides building i.e., soft costs, BI)
        ,   100 as "UDF3" -- SGV1_.05^SGV2_.10^SGV3_x
        ,   null as "UDF4" -- not used 
        ,   null as "UDF5" -- not used
        
        /* Geocoding Fields
        
            Status:         Complete
        */
        
        ,   null as "Latitude" 
        ,   null as "Longitude"
        ,   null as "UserGeocodeMatchLevel"        
        ,   location_selection.RISK_PLACE_HASH_TXT


    from location_selection_by_UDF5 location_selection
        
        left join loc_perils1
            on location_selection.risk_id = loc_perils1.risk_id
            
        left join loc_perils2
            on location_selection.risk_id = loc_perils2.risk_id
            
        left join loc_perils3
            on location_selection.risk_id = loc_perils3.risk_id
            
        left join loc_perils4
            on location_selection.risk_id = loc_perils4.risk_id
            
        left join loc_perils5
            on location_selection.risk_id = loc_perils5.risk_id
            
    order by "ContractID", "LocationName"
)
select *
---- !! debug
--, policy_count as (
--    select 'Policy' as granularity, 'Initial' as source_name, count(1) as record_count
--    from policy_product_subset 
----    from policy_product
--    union
--    select 'Policy' as granularity, 'Final' as source_name, count(distinct "ContractID") as record_count
--    from xdm_mart_location
--)
---- !! debug
--, location_count as (
--    select 'Location' as granularity, 'Initial' as source_name, count(distinct risk_id) as record_count
--    from location_detail 
----    from policy_product
--    union
--    select 'Location' as granularity, 'Final' as source_name, count(1) as record_count
--    from xdm_mart_location
--)
---- !! debug
--, location_count_detail as (
--    select 'Policy-Location' as granularity, 'Initial' as source_name, spm as ContractId, count(distinct risk_id) as record_count
--    from location_detail 
--    group by 'Policy-Location', 'Initial', spm
--    union
--    select 'Policy-Location' as granularity, 'Final' as source_name, "ContractID", count(1) as record_count
--    from xdm_mart_location
--    group by 'Policy-Location', 'Initial', "ContractID"
--)

---- !! debug
--, all_counts as (
--    select * from policy_Count
--    union 
--    select * from location_count
--)

from xdm_mart_location a
--inner join xdm_stage.S_EXCESS_RATML_RISK_PLACE b
--    on a.RISK_PLACE_HASH_TXT = b.RISK_PLACE_HASH_TXT
;