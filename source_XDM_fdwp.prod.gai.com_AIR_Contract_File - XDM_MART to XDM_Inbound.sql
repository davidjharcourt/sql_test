/*
	Query Name:
		xdm_mart_account.sql
    
	Key Background and Resources:
		1.	XDM_MART is loaded on a monthly basis. 
		2.	To report a bug in data seen in raw XDM_MART data, please submit a bug request here: https://gaig.sharepoint.com/sites/ExposureDataMart-XDM#xdm-bug-report-and-feature-requests.
        3.  To review the path that data takes from source system to XDM_MART, please review the following diagram: https://gaig.sharepoint.com/:u:/r/sites/ExposureDataMart-XDM/Shared%20Documents/Data%20Lineage-Overview.svg?csf=1&web=1&e=aAvvMv. 
        4.  For data quality review, check Tableau: 
    
	Purposes:	
		1.	Produce a table containing all business units in the shape of AIR_Contract_File that retains some of the structure and concepts of XDM_MART (e.g. policy_term_id, risk_id, policy peril indicators, etc.)
	
    Filters:
		1.	Mod period is set by looking up the current date in xdm_stage.outbound_date_to_mod_period@fdwt.dev.gai.com.

    Outstanding Errors:
        1.  policy_term_id, 11374. spm CPPE79745200. policy_terrorism_ind != excess_layer_terrorism_ind
        2.  risk peril product deductible count not matching policy peril indicator sum?
            -- excess
            ---- risk_count * excess_layer_peril_count != actual_count
            ------  8341
            ---- risk_count * excess_layer_peril_count = actual_count
            ------  11084
            ------  11096
            ------  11374
            ------  11463
            ------  11754
            -- traditional
            ---- 11740

*/


--CREATE VIEW XDM_STAGE.OUTBOUND_AIR_CONTRACT as 

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
--            select to_date('2022/06/30', 'yyyy/mm/dd') as mod_period
            select to_date(':pif_date', 'yyyy/mm/dd') as mod_period
            from dual
        ) mod_period
            on 1 = 1
	where 
            policy_term.POLICY_TERM_EFFECTIVE_DT <= mod_period.mod_period
		and
			policy_term.POLICY_TERM_EXPIRATION_DT > mod_period.mod_period
		and
			(policy_term.CANCELLATION_EFFECTIVE_DT > mod_period.mod_period or policy_term.CANCELLATION_EFFECTIVE_DT is null)
)
---- !!debug
--, policy_product_subset_pre as (
--    select policy_product.*, row_number() over (partition by business_unit_cd, product_desc order by policy_term_id desc) as bu_product_row_number
--    from policy_product
--)
---- !!debug
--, policy_product_subset as (
--    select policy_product_subset_pre.*
--    from policy_product_subset_pre
----    where bu_product_row_number <= 5 
----    -- heritage sublimit
----    or policy_term_id in  (
----            15787
----        ,   15880
----        ,   14305
----        ,   13662
----    )
----    -- pimx multilayer
----    or policy_term_id = 8341
--)
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
, policy_product_w_fdw_policyid as (
    /*
        Purposes:	
		
        1.	Pair an XDM_MART policy term with its corresponding FDW.D_POLICY policy id
        
        Granularity of Ouptut:
        
        1.  Policy Term
                
        Outstanding Questions / Requests:
        
        N/A
	
    */
    select 
            policy_product_subset.policy_term_id
        ,   d_policy.policy_id
    from policy_product_listagg policy_product_subset
        left join xdm_mart.policy_term 
            on policy_product_subset.policy_term_id = policy_Term.policy_Term_id
        left join fdw.d_policy
            on  policy_term.policy_symbol_cd    = d_policy.policy_symbol
            and policy_term.policy_number_cd    = d_policy.policy_number
            and policy_term.policy_module_cd    = d_policy.policy_module
            and policy_Term.current_version_no  = d_policy.policy_version_cd
)
, policy_premium_fdw as (
    /*
        Purposes:	
		
        1.	Determine the premium for a given policy term
        
        Granularity of Ouptut:
        
        1.  Policy Term
        
        Outstanding Questions / Requests:
        
        1.  Need to standardize filter down to property premium. Should we use asl or iasl? P&IM PIF query may be able to be inserted in.
	
    */
    select 
            policy_product_w_fdw_policyid.policy_term_id	
        ,   policy_product_w_fdw_policyid.policy_id		
        ,   sum(f_divisional_premiums.CHANGE_WRITTEN_PREMIUM) as total_policy_premium
    from policy_product_w_fdw_policyid
        left join fdw.f_divisional_premiums
            on policy_product_w_fdw_policyid.policy_id = f_divisional_premiums.policy_id
            and f_divisional_premiums.TRANSACTION_ACCOUNTING_YEAR >= EXTRACT(YEAR from SYSDATE) - 5
        left join FDW.D_TRANSACTION_ITEMS
            on f_divisional_premiums.WRITTEN_TRANSACTION_ID = D_TRANSACTION_ITEMS.TRANSACTION_ITEMS_ID
            and D_TRANSACTION_ITEMS.REIN_TRANSACTION_TYPE_DESC in ('Direct', 'Direct Funded')
    group by 
            policy_product_w_fdw_policyid.policy_term_id	
        ,   policy_product_w_fdw_policyid.policy_id		
)
, excess_layer_peril as (

    /*
        Purposes:	
		
        1.	Pivot the excess table from long to wide (i.e. one record per policy per layer per periil to one record per policy per layer)
        2.  Determine the perils covered by each excess layer
        
        Granularity of Ouptut:
        
        1.  Policy Term x Layer (i.e. one or more records per policy term)
        
        Outstanding Questions / Requests:
        
        N/A
        
    */
    
    select 
            excess.policy_term_id
        ,   excess.layer_no
        ,   excess.limit_amt
        ,   excess.attachment_point_amt
        ,   excess.part_of_amt
        ,   excess.part_of_pcnt
        ,   sum(case when peril_cd = 'FIRE' then 1 else 0 end)          as layer_fire_ind
        ,   sum(case when peril_cd = 'HURRICANE' then 1 else 0 end)     as layer_hurricane_ind
        ,   sum(case when peril_cd = 'EARTHQUAKE' then 1 else 0 end)    as layer_earthquake_ind
        ,   sum(case when peril_cd = 'FLOOD' then 1 else 0 end)         as layer_flood_ind
        ,   sum(case when peril_cd = 'WIND_HAIL' then 1 else 0 end)     as layer_wind_hail_ind
        ,   sum(case when peril_cd = 'TERRORISM' then 1 else 0 end)     as layer_terrorism_ind
        ,   sum(case when peril_cd = 'NBCR' then 1 else 0 end)          as layer_nbcr_ind
        ,   sum(case when peril_cd = '(none)' then 1 else 0 end)        as layer_none_ind
    from xdm_mart.excess
        left join xdm_mart.peril
            on excess.peril_id = peril.peril_id
    group by         
            excess.policy_term_id
        ,   excess.layer_no
        ,   excess.limit_amt
        ,   excess.attachment_point_amt
        ,   excess.part_of_amt
        ,   excess.part_of_pcnt
)
, policy_layer_enriched as (

    /*
        Purposes:	
		
        1.	Gather policy term, customer, producer, business unit, and product to one central view 
                
        Granularity of Ouptut:
        
        1.  Policy Term x Layer (i.e. one or more records per policy term)
        
        Outstanding Questions / Requests:
        
        1.  Is the producer country sufficient to determine the currency to use?
        
    */
	select
            policy_product.mod_period
        ,   policy_term.policy_term_id
		, 	policy_term.COMPANY_ID
		, 	policy_term.producer_profit_center_bu_id
		  
		, 	policy_term.BUSINESS_UNIT_CD
		, 	policy_term.BUSINESS_UNIT_DESC

		, 	producer_profit_center_bu.PROFIT_CENTER_CD
		, 	producer_profit_center_bu.PROFIT_CENTER_DESC

		, 	producer_profit_center_bu.PRODUCER_CD
		, 	producer_profit_center_bu.PRODUCER_DESC
        ,   d_geography.COUNTRY_CODE as PRODUCER_COUNTRY_CD
        
        ,   customer.customer_cd

		, 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD as SPM
        , 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO as SPMV
		, 	policy_term.POLICY_SYMBOL_CD||policy_term.POLICY_NUMBER_CD||policy_term.POLICY_MODULE_CD||policy_term.CURRENT_VERSION_NO||CASE WHEN excess_layer_peril.policy_term_id IS NOT NULL THEN '-'||lpad(excess_layer_peril.LAYER_NO,2,'0') ELSE NULL END as SPMVL

        ,   policy_product.product_cd
        ,   policy_product.product_desc

		, 	policy_term.POLICY_SYMBOL_CD
		, 	policy_term.POLICY_SYMBOL_DESC 
		, 	policy_term.POLICY_NUMBER_CD
		, 	policy_term.POLICY_MODULE_CD
		, 	policy_term.CURRENT_VERSION_NO
		, 	policy_term.PRIMARY_INSURED_NM

		, 	policy_term.POLICY_TERM_EFFECTIVE_DT
		, 	policy_term.POLICY_TERM_EXPIRATION_DT
		, 	policy_term.CANCELLATION_EFFECTIVE_DT

		, 	company.COMPANY_CD
		, 	company.COMPANY_NM
		, 	company.COMPANY_ABRV_CD

		, 	CASE WHEN excess_layer_peril.policy_term_id IS NULL THEN 0 ELSE 1 END as excess_policy_ind
        
		, 	policy_term.FIRE_IND AS POLICY_FIRE_IND
        , 	policy_term.HURRICANE_IND AS POLICY_HURRICANE_IND
        , 	policy_term.EARTHQUAKE_IND AS POLICY_EARTHQUAKE_IND
        , 	policy_term.FLOOD_IND AS POLICY_FLOOD_IND
        , 	policy_term.WIND_HAIL_IND AS POLICY_WIND_HAIL_IND
		, 	policy_term.TERRORISM_IND AS POLICY_TERRORISM_IND
		, 	policy_term.NBCR_IND as POLICY_NBCR_IND

		, 	policy_term.UNDERWRITER_NM

        ,   excess_layer_peril.layer_no                 as excess_layer_no
        
        ,   excess_layer_peril.limit_amt                as excess_limit_amt
        ,   excess_layer_peril.attachment_point_amt     as excess_attachment_point_amt
        ,   excess_layer_peril.part_of_amt              as excess_part_of_amt
        ,   excess_layer_peril.part_of_pcnt             as excess_part_of_pcnt
        
        ,   excess_layer_peril.layer_fire_ind           as excess_layer_fire_ind
        ,   excess_layer_peril.layer_hurricane_ind      as excess_layer_hurricane_ind
        ,   excess_layer_peril.layer_earthquake_ind     as excess_layer_earthquake_ind
        ,   excess_layer_peril.layer_flood_ind          as excess_layer_flood_ind
        ,   excess_layer_peril.layer_wind_hail_ind      as excess_layer_wind_hail_ind
        ,   excess_layer_peril.layer_terrorism_ind      as excess_layer_terrorism_ind
        ,   excess_layer_peril.layer_nbcr_ind           as excess_layer_nbcr_ind
        ,   excess_layer_peril.layer_none_ind           as excess_layer_none_ind
        
        ,   policy_term.POLICY_ENTRY_SYSTEM_DESC
        
	from 
			 policy_product_listagg policy_product
		left join 
				xdm_mart.policy_term
			on
				policy_product.policy_term_id = policy_term.policy_term_id
        left join 
                xdm_mart.policy
            on  
                policy_term.policy_id = policy.policy_id
        left join
                xdm_mart.customer
            on  
                policy.customer_id = customer.customer_id
		left join
				xdm_mart.producer_profit_center_bu
			on
				policy_term.producer_profit_center_bu_id =  producer_profit_center_bu.producer_profit_center_bu_id
    left join 
        fdw.d_producer
      on
        producer_profit_center_bu.producer_cd = d_producer.producer_code
    left join 
        fdw.d_geography
      on 
        d_producer.location_State = d_geography.state_province_code
      and 
        d_producer.state_number = d_geography.rein_state_province_code
		left join 
				xdm_mart.company
			on
				policy_term.company_id =  company.company_id
		left join
				excess_layer_peril
			on
				policy_term.policy_term_id = excess_layer_peril.policy_term_id
)
, policy_term_layer_count as (
    /*
        Purposes:	
		
        1.	Count number of layers per policy term
                
        Granularity of Ouptut:
        
        1.  Policy Term
        
        Outstanding Questions / Requests:
        
        N/A
        
    */
    select
            policy_layer_enriched.policy_term_id
        ,   sum(policy_layer_enriched.excess_policy_ind) as layer_count
    from
            policy_layer_enriched
    group by 
            policy_layer_enriched.policy_term_id
)
, policy_term_peril_count as (
    /*
        Purposes:	
		
        1.	Count number of perils per policy term and layer
                
        Granularity of Ouptut:
        
        1.  Policy Term
        
        Outstanding Questions / Requests:
        
        N/A
        
    */
    select distinct
            policy_layer_enriched.policy_term_id
        ,   policy_layer_enriched.POLICY_NBCR_IND + policy_layer_enriched.POLICY_TERRORISM_IND + policy_layer_enriched.POLICY_EARTHQUAKE_IND + policy_layer_enriched.POLICY_HURRICANE_IND + policy_layer_enriched.POLICY_WIND_HAIL_IND + policy_layer_enriched.POLICY_FIRE_IND + policy_layer_enriched.POLICY_FLOOD_IND as policy_peril_count
        ,   policy_layer_enriched.EXCESS_LAYER_NBCR_IND + policy_layer_enriched.EXCESS_LAYER_TERRORISM_IND + policy_layer_enriched.EXCESS_LAYER_EARTHQUAKE_IND + policy_layer_enriched.EXCESS_LAYER_HURRICANE_IND + policy_layer_enriched.EXCESS_LAYER_WIND_HAIL_IND + policy_layer_enriched.EXCESS_LAYER_FIRE_IND + policy_layer_enriched.EXCESS_LAYER_FLOOD_IND as excess_layer_peril_count
    from
            policy_layer_enriched
)
, policy_term_risk_count as (
    /*
        Purposes:	
		
        1.	Count number of risks (buildings) per policy term
                
        Granularity of Ouptut:
        
        1.  Policy Term
        
        Outstanding Questions / Requests:
        
        N/A
        
    */
    select 
            policy_layer_enriched.policy_term_id
        ,   count(1) as risk_count
    from 
            policy_layer_enriched
        left join 
                xdm_mart.risk
            on 
                policy_layer_enriched.policy_term_id = risk.policy_term_id
    group by policy_layer_enriched.policy_term_id
)
, location_deductible_detail as (
    /*
        Purposes:	
		
        1.	Determine the risks on policies in force, their locations, and the deductibles that apply
        2.  Remove risks that have been deleted from a policy
        3.  Remove perils that have been deleted from a risk
                
        Granularity of Ouptut:
        
        1.  Policy Term x Layer (i.e. one or more records per policy term) x Risks (i.e. one or more risks per policy term) x Peril (i.e. one or more perils per risk) x  Product (i.e. one or more products per risk per peril)
        
        Outstanding Questions / Requests:
        
        1. How does this work with multi-product ISO Package?
        
    */

  select 
            policy_layer_enriched.mod_period
      ,     policy_layer_enriched.policy_term_id
      ,     policy_layer_enriched.BUSINESS_UNIT_CD
      ,     policy_layer_enriched.BUSINESS_UNIT_DESC
      , 	policy_layer_enriched.PROFIT_CENTER_CD
      , 	policy_layer_enriched.PROFIT_CENTER_DESC
      , 	policy_layer_enriched.PRODUCER_CD
      , 	policy_layer_enriched.PRODUCER_DESC
      ,     policy_layer_enriched.spmv
      ,     risk.risk_id
      ,     risk_property_structure.LOCATION_CD
      ,     risk_property_structure.BUILDING_CD
      ,     product.product_cd
      ,     peril.peril_id
      ,     peril.peril_cd
      ,     peril.peril_desc
      ,     location.PLACE_DESC 
      ,     location.STREET_ADDRESS_1_TXT 
      ,     location.STREET_ADDRESS_2_TXT 
      ,     location.STREET_ADDRESS_3_TXT 
      ,     location.STREET_ADDRESS_4_TXT 
      ,     location.CITY_NM 
      ,     location.STATE_PROVINCE_CD 
      ,     location.STATE_PROVINCE_NM 
      ,     location.POSTAL_CD 
      ,     location.COUNTY_CD 
      ,     location.COUNTY_NM 
      ,     location.COUNTRY_CD 
      ,     location.COUNTRY_NM 
      ,     location.COUNTRY_SCHEMA_CD 
      ,     location.SECTION_TXT 
      ,     location.RANGE_TXT 
      ,     location.TOWNSHIP_TXT 
      ,     location.LONGITUDE_CD 
      ,     location.LATITUDE_CD 
      ,     location.GEO_RESOLUTION_CD 
      ,     risk_property_structure.RISK_PROPERTY_STRUCTURE_ID
      ,     risk_property_structure.STRUCTURE_DESC
      ,     risk_property_structure.STORIES_NO
      ,     risk_property_structure.SQUARE_FOOTAGE_NO
      ,     risk_property_structure.CONSTRUCTION_SCHEME_DESC
      ,     risk_property_structure.CONSTRUCTION_CD
      ,     risk_property_structure.CONSTRUCTION_DESC
      ,     risk_property_structure.OCCUPANCY_SCHEME_DESC
      ,     risk_property_structure.OCCUPANCY_CD
      ,     risk_property_structure.OCCUPANCY_DESC
      , 	deductible_property_structure.DEDUCT_PROPERTY_STRUCTURE_ID
      , 	deductible_property_structure.RISK_PERIL_PRODUCT_ID
      , 	deductible_property_structure.BLANKET_AGG_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_AGG_DEDUCTIBLE_CD
      , 	deductible_property_structure.BLANKET_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_DEDUCTIBLE_PCNT
      , 	deductible_property_structure.BLANKET_DEDUCTIBLE_CD
      , 	deductible_property_structure.BLANKET_BLDG_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_BLDG_DEDUCTIBLE_CD
      , 	deductible_property_structure.BLANKET_CNTS_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_CNTS_DEDUCTIBLE_CD
      , 	deductible_property_structure.BLANKET_BI_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_BI_DEDUCTIBLE_PCNT
      , 	deductible_property_structure.BLANKET_BI_DEDUCTIBLE_DUR_NO
      , 	deductible_property_structure.BLANKET_BI_DURATION_UNIT_DESC
      , 	deductible_property_structure.BLANKET_BI_DEDUCTIBLE_CD
      , 	deductible_property_structure.BLANKET_COMB_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BLANKET_COMB_DEDUCTIBLE_CD
      , 	deductible_property_structure.SITE_DEDUCTIBLE_AMT
      , 	deductible_property_structure.COMBINED_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BUILDING_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BUILDING_DEDUCTIBLE_PCNT
      , 	deductible_property_structure.CONTENTS_DEDUCTIBLE_AMT
      , 	deductible_property_structure.CONTENTS_DEDUCTIBLE_PCNT
      , 	deductible_property_structure.BI_DEDUCTIBLE_AMT
      , 	deductible_property_structure.BI_DEDUCTIBLE_PCNT
      , 	deductible_property_structure.BI_DEDUCTIBLE_DURATION_NO
      , 	deductible_property_structure.BI_DURATION_UNIT_DESC
      , 	deductible_property_structure.SOURCE_SYSTEM_ID
      , 	deductible_property_structure.CREATE_TS
      , 	deductible_property_structure.UPDATE_TS
      , 	deductible_property_structure.DEDUCTIBLE_HASH_TXT
    FROM policy_layer_enriched
        INNER JOIN
                xdm_mart.risk
            ON
                policy_layer_enriched.policy_term_id = risk.policy_term_id
        INNER JOIN 
                xdm_mart.location 
            ON 
                risk.location_id = location.location_id
        INNER JOIN 
                xdm_mart.risk_property_structure
            ON
                risk.risk_id = risk_property_structure.risk_id
            AND
                 (risk_property_structure.deleted_dt > policy_layer_enriched.mod_period or risk_property_structure.deleted_dt is null)
        INNER JOIN 
                  xdm_mart.risk_peril_product
              ON
                  risk.risk_id = risk_peril_product.risk_id
              AND
                  (risk_peril_product.deleted_dt > policy_layer_enriched.mod_period or risk_peril_product.deleted_dt is null)
        INNER JOIN
                  xdm_mart.product
              ON
                  risk_peril_product.product_id = product.product_id
        LEFT JOIN
                  xdm_mart.peril
              ON
                  risk_peril_product.peril_id = peril.peril_id
        LEFT JOIN
                  xdm_mart.deductible_property_structure
              ON
                  risk_peril_product.risk_peril_product_id = deductible_property_structure.risk_peril_product_id
)
---- !!debug
--, location_deductible_count as (
--    select 
--            location_deductible_detail.policy_Term_id
--        ,   count(1) as actual_count
--    from 
--            location_deductible_detail
--    group by 
--            location_deductible_detail.policy_Term_id
--)
---- !!debug
--, count_comparison as (
--    select 
--            location_deductible_count.policy_Term_id
--        ,   policy_term_layer_count.layer_count
--        ,   policy_term_peril_count.policy_peril_count
--        ,   policy_term_peril_count.excess_layer_peril_count
--        ,   policy_term_risk_count.risk_count
--        ,   policy_term_peril_count.policy_peril_count * policy_term_risk_count.risk_count as theoretical_count
--        ,   location_deductible_count.actual_count
--        ,   case when policy_term_peril_count.policy_peril_count * policy_term_risk_count.risk_count = location_deductible_count.actual_count then 1 else 0 end as match_indicator
--    from 
--            location_deductible_count
--        left join
--                policy_term_layer_count
--            on
--                location_deductible_count.policy_term_id = policy_term_layer_count.policy_term_id
--        left join
--                policy_term_peril_count
--            on
--                location_deductible_count.policy_term_id = policy_term_peril_count.policy_term_id
--        left join
--                policy_term_risk_count
--            on
--                location_deductible_count.policy_term_id = policy_term_risk_count.policy_term_id
--    order by 
--            location_deductible_count.policy_Term_id
--)
----!!debug
--, count_mismatches as (
--    -- excess
--    ---- risk_count * excess_layer_peril_count != actual_count
--    ------  8341
--    ---- risk_count * excess_layer_peril_count = actual_count
--    ------  11084
--    ------  11096
--    ------  11374
--    ------  11463
--    ------  11754
--    -- traditional
--    ---- 11740
--    select *
--    from count_comparison
--    where match_indicator = 0
--    order by policy_term_id
--)
, blanket_peril_deductible as (
    /*
        Purposes:	
		
        1.	Determine the count of risks per blanket deductible per peril
        2.  Create sublimit indicators
                
        Granularity of Ouptut:
        
        1.  Policy Term x Layer (i.e. one or more records per policy term) x Peril (i.e. one or more perils per risk) x Product
        
        Outstanding Questions / Requests:
        
        1.  Does this break with Excess multi-layer??
    */
    
  select 
          policy_term_id
      ,   peril_cd
      ,   case when peril_cd = 'FIRE' then 1 else 0 end as sublimit_fire_ind
      ,   case when peril_cd = 'HURRICANE' then 1 else 0 end as sublimit_hurricane_ind
      ,   case when peril_cd = 'EARTHQUAKE' then 1 else 0 end as sublimit_earthquake_ind
      ,   case when peril_cd = 'FLOOD' then 1 else 0 end as sublimit_flood_ind
      ,   case when peril_cd = 'WIND_HAIL' then 1 else 0 end as sublimit_wind_hail_ind
      ,   case when peril_cd = 'TERRORISM' then 1 else 0 end as sublimit_terrorism_ind
      ,   case when peril_cd = 'NBCR' then 1 else 0 end as sublimit_nbcr_ind
      ,   BLANKET_AGG_DEDUCTIBLE_AMT
      ,   BLANKET_AGG_DEDUCTIBLE_CD
      ,   BLANKET_DEDUCTIBLE_AMT
      ,   BLANKET_DEDUCTIBLE_PCNT
      ,   BLANKET_DEDUCTIBLE_CD
      ,   BLANKET_BLDG_DEDUCTIBLE_AMT
      ,   BLANKET_BLDG_DEDUCTIBLE_CD
      ,   BLANKET_CNTS_DEDUCTIBLE_AMT
      ,   BLANKET_CNTS_DEDUCTIBLE_CD
      ,   BLANKET_BI_DEDUCTIBLE_AMT
      ,   BLANKET_BI_DEDUCTIBLE_PCNT
      ,   BLANKET_BI_DEDUCTIBLE_DUR_NO
      ,   BLANKET_BI_DURATION_UNIT_DESC
      ,   BLANKET_BI_DEDUCTIBLE_CD
      ,   BLANKET_COMB_DEDUCTIBLE_AMT
      ,   BLANKET_COMB_DEDUCTIBLE_CD
      ,   count(1) as blanket_risk_count
  from
        location_deductible_detail
  group by 
          policy_term_id
      ,   peril_cd
      ,   case when peril_cd = 'FIRE' then 1 else 0 end 
      ,   case when peril_cd = 'HURRICANE' then 1 else 0 end
      ,   case when peril_cd = 'EARTHQUAKE' then 1 else 0 end
      ,   case when peril_cd = 'FLOOD' then 1 else 0 end
      ,   case when peril_cd = 'WIND_HAIL' then 1 else 0 end
      ,   case when peril_cd = 'TERRORISM' then 1 else 0 end
      ,   case when peril_cd = 'NBCR' then 1 else 0 end
      ,   BLANKET_AGG_DEDUCTIBLE_AMT
      ,   BLANKET_AGG_DEDUCTIBLE_CD
      ,   BLANKET_DEDUCTIBLE_AMT
      ,   BLANKET_DEDUCTIBLE_PCNT
      ,   BLANKET_DEDUCTIBLE_CD
      ,   BLANKET_BLDG_DEDUCTIBLE_AMT
      ,   BLANKET_BLDG_DEDUCTIBLE_CD
      ,   BLANKET_CNTS_DEDUCTIBLE_AMT
      ,   BLANKET_CNTS_DEDUCTIBLE_CD
      ,   BLANKET_BI_DEDUCTIBLE_AMT
      ,   BLANKET_BI_DEDUCTIBLE_PCNT
      ,   BLANKET_BI_DEDUCTIBLE_DUR_NO
      ,   BLANKET_BI_DURATION_UNIT_DESC
      ,   BLANKET_BI_DEDUCTIBLE_CD
      ,   BLANKET_COMB_DEDUCTIBLE_AMT
      ,   BLANKET_COMB_DEDUCTIBLE_CD
)
, policy_peril_deductible_pre as (
    /*
        Purposes:	
		
        1.	Determine the count of risks and perils per blanket deductible
        2.  Join in the total count of risks and perils per policy
                
        Granularity of Ouptut:
        
        1.  Policy Term x Layer (i.e. one or more records per policy term) x Unique Blanket_Deductible_Amt
        
        Outstanding Questions / Requests:
        
        1.  Does this break with Excess multi-layer??
        2.  How to handle this when multiple perils have different blankets but same blanket_deductible_amt? No way to distinguish a multi-peril blanket (E.g. flood and earthquake) from two single-peril blankets that happen to share a common blanket_deductible_amt but are really two separatate deductibles. Does it matter given the unique peril_Cd_list values?
    */

    select 
            blanket_peril_deductible.policy_term_id
        ,   policy_term_risk_count.risk_count as policy_risk_count
        ,   policy_term_peril_count.policy_peril_count
        ,   blanket_peril_deductible.blanket_deductible_amt
        ,   blanket_peril_deductible.blanket_deductible_pcnt
        ,   blanket_peril_deductible.blanket_risk_count
        ,   count(1) as blanket_peril_count
        ,   max(sublimit_fire_ind) as sublimit_fire_ind
        ,   max(sublimit_hurricane_ind) as sublimit_hurricane_ind
        ,   max(sublimit_earthquake_ind) as sublimit_earthquake_ind
        ,   max(sublimit_flood_ind) as sublimit_flood_ind
        ,   max(sublimit_wind_hail_ind) as sublimit_wind_hail_ind
        ,   max(sublimit_terrorism_ind) as sublimit_terrorism_ind
        ,   max(sublimit_nbcr_ind) as sublimit_nbcr_ind
        ,   listagg(blanket_peril_deductible.peril_cd, ', ') WITHIN GROUP (ORDER BY blanket_peril_deductible.peril_cd) as peril_cd_list
    from blanket_peril_deductible
        left join policy_term_risk_count
            on blanket_peril_deductible.policy_term_id = policy_term_risk_count.policy_term_id
        left join policy_term_peril_count
            on blanket_peril_deductible.policy_term_id = policy_term_peril_count.policy_term_id
    where 
            (
                    (blanket_peril_deductible.BLANKET_DEDUCTIBLE_AMT is not null and blanket_peril_deductible.BLANKET_DEDUCTIBLE_AMT != 0) 
                or 
                    (blanket_peril_deductible.BLANKET_DEDUCTIBLE_PCNT is not null and blanket_peril_deductible.BLANKET_DEDUCTIBLE_PCNT != 0)
            )
    group by 
            blanket_peril_deductible.policy_term_id
        ,   policy_term_risk_count.risk_count
        ,   policy_term_peril_count.policy_peril_count
        ,   blanket_peril_deductible.blanket_deductible_amt
        ,   blanket_peril_deductible.blanket_deductible_pcnt
        ,   blanket_peril_deductible.blanket_risk_count
    order by 
            blanket_peril_count
)
---- !!debug
--, policy_peril_ded_preDEBUG as (
--    select policy_layer_enriched.*
--    from policy_layer_enriched
--        inner join policy_peril_deductible_pre
--            on policy_layer_enriched.policy_term_id = policy_peril_deductible_pre.policy_term_id
--            and policy_peril_deductible_pre.blanket_peril_count <= 2
--)
, policy_peril_deductible_pre2 as (
    select 
            policy_peril_deductible_pre.*
        ,   case when blanket_risk_count = policy_risk_count then 1 else 0 end as policy_blanket_ind
    from policy_peril_deductible_pre
)
---- !!debug
--, three_and_two_blankets as (
--    select policy_layer_enriched.spm, policy_peril_deductible_pre2.*
--    from policy_peril_deductible_pre2
--        left join policy_layer_enriched
--            on policy_peril_deductible_pre2.policy_Term_id = policy_layer_enriched.policy_term_id
--    where policy_peril_deductible_pre2.policy_term_id in (
--    15787
--    ,15880
--    ,14305
--    ,13662
--    )
--    order by policy_peril_deductible_pre2.policy_term_id, policy_blanket_ind, SUBLIMIT_FIRE_IND
--)
, policy_peril_deductible_pre3 as (
    select 
            policy_peril_deductible_pre2.*
            
        ,   case 
                when policy_blanket_ind = 1 and SUBLIMIT_FIRE_IND = 1
                    then nvl(blanket_deductible_amt,blanket_deductible_pcnt) 
                else null 
            end as DedAmt1
        ,   case 
                when policy_blanket_ind = 1 and SUBLIMIT_FIRE_IND = 0
                    then case 
                        -- should this be a lookup table?
                        when (SUBLIMIT_HURRICANE_IND = 1 or SUBLIMIT_WIND_HAIL_IND = 1) then 'AllLoc-Wind' 
                        when (SUBLIMIT_EARTHQUAKE_IND = 1) then 'AllLoc-EQ'
                        when (SUBLIMIT_FLOOD_IND = 1) then 'AllLoc-Flood' 
                        else null 
                        end
                else null
            end as SublimitArea
        ,   case 
                when policy_blanket_ind = 1 and SUBLIMIT_FIRE_IND = 0
                    then 'B'
                else null 
            end as SublimitDedType
        ,   case 
                when policy_blanket_ind = 1 and SUBLIMIT_FIRE_IND = 0
                    then nvl(blanket_deductible_amt,blanket_deductible_pcnt) 
                else null 
            end as SublimitDedAmt1
        
    from policy_peril_deductible_pre2
    where not (SUBLIMIT_FIRE_IND = 0 and SUBLIMIT_FLOOD_IND = 1 and SUBLIMIT_EARTHQUAKE_IND = 0 and SUBLIMIT_HURRICANE_IND = 0 and SUBLIMIT_WIND_HAIL_IND = 0) -- !!filter
    order by policy_peril_deductible_pre2.policy_term_id, policy_blanket_ind, sublimit_fire_ind
)
, policy_peril_deductible_pre4 as (
    select 
            POLICY_TERM_ID
        ,   count(1) over (partition by policy_Term_id) as raw_layer_Count
        ,   last_value(DEDAMT1 ignore nulls) over (partition by policy_term_id order by policy_term_id, policy_blanket_ind, sublimit_fire_ind RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as dedamt1
        ,   sublimit_fire_ind
        ,   sublimit_hurricane_ind
        ,   sublimit_earthquake_ind
        ,   sublimit_flood_ind
        ,   sublimit_wind_hail_ind
        ,   sublimit_terrorism_ind
        ,   sublimit_nbcr_ind
        ,   SUBLIMITAREA	
        ,   SUBLIMITDEDTYPE	
        ,   SUBLIMITDEDAMT1
    from policy_peril_deductible_pre3
)
, policy_peril_deductible as (
    select
            POLICY_TERM_ID
        ,   dedamt1
        ,   sublimit_fire_ind
        ,   sublimit_hurricane_ind
        ,   sublimit_earthquake_ind
        ,   sublimit_flood_ind
        ,   sublimit_wind_hail_ind
        ,   sublimit_terrorism_ind
        ,   sublimit_nbcr_ind
        ,   SUBLIMITAREA	
        ,   SUBLIMITDEDTYPE	
        ,   SUBLIMITDEDAMT1
    from policy_peril_deductible_pre4
    where raw_layer_count = 1 or (raw_layer_count > 1 and sublimitarea is not null)
)

, policy_peril_deductible_scrub as (

    select
    
            
                policy_peril_deductible.*
            ,   case when (policy_peril_deductible.sublimit_fire_ind + policy_peril_deductible.sublimit_hurricane_ind +  policy_peril_deductible.sublimit_earthquake_ind + policy_peril_deductible.sublimit_flood_ind + policy_peril_deductible.sublimit_wind_hail_ind + policy_peril_deductible.sublimit_terrorism_ind + policy_peril_deductible.sublimit_nbcr_ind) = policy_term_peril_count.policy_peril_count then 1 else 0 end as isAllPerilDeductible
            
    from policy_peril_deductible
        left join policy_term_peril_count
            on policy_peril_deductible.policy_term_id = policy_term_peril_count.policy_term_id
            
)

, location_limit_detail as (
    select
          policy_layer_enriched.mod_period
      ,   policy_layer_enriched.policy_term_id
      ,   policy_layer_enriched.BUSINESS_UNIT_CD
      ,   policy_layer_enriched.BUSINESS_UNIT_DESC
      ,   policy_layer_enriched.PROFIT_CENTER_CD
      ,   policy_layer_enriched.PROFIT_CENTER_DESC
      ,   policy_layer_enriched.PRODUCER_CD
      ,   policy_layer_enriched.PRODUCER_DESC
      ,   policy_layer_enriched.spmv
      ,   risk.risk_id
      ,   risk_property_structure.LOCATION_CD
      ,   risk_property_structure.BUILDING_CD
      ,   product.product_cd
      ,   peril.peril_id
      ,   peril.peril_desc
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
      ,   risk_property_structure.RISK_PROPERTY_STRUCTURE_ID
      ,   risk_property_structure.STRUCTURE_DESC
      ,   risk_property_structure.STORIES_NO
      ,   risk_property_structure.SQUARE_FOOTAGE_NO
      ,   risk_property_structure.CONSTRUCTION_SCHEME_DESC
      ,   risk_property_structure.CONSTRUCTION_CD
      ,   risk_property_structure.CONSTRUCTION_DESC
      ,   risk_property_structure.OCCUPANCY_SCHEME_DESC
      ,   risk_property_structure.OCCUPANCY_CD
      ,   risk_property_structure.OCCUPANCY_DESC
      ,   limit_property_structure.risk_peril_product_id
      ,   limit_property_structure.limit_property_structure_id
      ,   limit_property_structure.blanket_agg_limit_amt
      ,   limit_property_structure.blanket_agg_limit_cd
      ,   limit_property_structure.blanket_limit_amt
      ,   limit_property_structure.blanket_limit_cd
      ,   limit_property_structure.blanket_building_limit_amt
      ,   limit_property_structure.blanket_building_limit_cd
      ,   limit_property_structure.blanket_contents_limit_amt
      ,   limit_property_structure.blanket_contents_limit_cd
      ,   limit_property_structure.blanket_bi_limit_amt
      ,   limit_property_structure.blanket_bi_limit_cd
      ,   limit_property_structure.blanket_combined_limit_amt
      ,   limit_property_structure.blanket_combined_limit_cd
      ,   limit_property_structure.site_limit_amt
      ,   limit_property_structure.combined_limit_amt
      ,   limit_property_structure.building_limit_amt
      ,   limit_property_structure.contents_limit_amt
      ,   limit_property_structure.bi_limit_amt
      ,   limit_property_structure.source_system_id
      ,   limit_property_structure.create_ts
      ,   limit_property_structure.update_ts
      ,   limit_property_structure.limit_hash_txt
    from policy_layer_enriched
    inner join
          xdm_mart.risk
        on
          policy_layer_enriched.policy_term_id = risk.policy_term_id
    INNER JOIN 
            xdm_mart.location 
        ON 
            risk.location_id = location.location_id
      INNER JOIN 
            xdm_mart.risk_property_structure
        ON
            risk.risk_id = risk_property_structure.risk_id
        AND
            (risk_property_structure.deleted_dt > policy_layer_enriched.mod_period or risk_property_structure.deleted_dt is null)
      INNER JOIN 
                  xdm_mart.risk_peril_product
              ON
                  risk.risk_id = risk_peril_product.risk_id
              AND
                  (risk_peril_product.deleted_dt > policy_layer_enriched.mod_period or risk_peril_product.deleted_dt is null)
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
)
, policy_term_risk_peril_count as (
    select policy_term_id, peril_id, count(distinct risk_id) as policy_peril_risk_count
    from location_limit_detail
    group by policy_term_id, peril_id
)
, limit_pre1 as (
  select 
          policy_term_id
      ,   spmv
      ,   peril_id
      ,   peril_desc
      
      ,   blanket_agg_limit_amt
      ,   blanket_agg_limit_cd
      ,   blanket_limit_amt
      ,   blanket_limit_cd
      ,   blanket_building_limit_amt
      ,   blanket_building_limit_cd
      ,   blanket_contents_limit_amt
      ,   blanket_contents_limit_cd
      ,   blanket_bi_limit_amt
      ,   blanket_bi_limit_cd
      ,   blanket_combined_limit_amt
      ,   blanket_combined_limit_cd
      
      ,   SITE_LIMIT_AMT	
      ,   COMBINED_LIMIT_AMT	
      ,   BUILDING_LIMIT_AMT	
      ,   CONTENTS_LIMIT_AMT	
      ,   BI_LIMIT_AMT
  
      ,   count(distinct risk_id) as policy_peril_location_count
  from
        location_limit_detail
  group by 
          policy_term_id
      ,   spmv
      ,   peril_id
      ,   peril_desc
      ,   blanket_agg_limit_amt
      ,   blanket_agg_limit_cd
      ,   blanket_limit_amt
      ,   blanket_limit_cd
      ,   blanket_building_limit_amt
      ,   blanket_building_limit_cd
      ,   blanket_contents_limit_amt
      ,   blanket_contents_limit_cd
      ,   blanket_bi_limit_amt
      ,   blanket_bi_limit_cd
      ,   blanket_combined_limit_amt
      ,   blanket_combined_limit_cd
      
      ,   SITE_LIMIT_AMT	
      ,   COMBINED_LIMIT_AMT	
      ,   BUILDING_LIMIT_AMT	
      ,   CONTENTS_LIMIT_AMT	
      ,   BI_LIMIT_AMT
)
, limit_pre2 as (
-- no examples currently
    select 
            limit_pre1.*
        ,   policy_term_risk_count.risk_count
        ,   case when limit_pre1.policy_peril_location_count = policy_term_risk_count.risk_count and policy_term_risk_count.risk_count <> 1 then 1 else 0 end as policy_blanket_ind
    from limit_pre1
        left join policy_term_risk_count
            on limit_pre1.policy_term_id = policy_term_risk_count.policy_term_id
        where 
            not    (
                        (limit_pre1.BLANKET_LIMIT_AMT is null or limit_pre1.BLANKET_LIMIT_AMT = 0) 
                    and 
                        (limit_pre1.BLANKET_BUILDING_LIMIT_AMT is null and limit_pre1.BLANKET_BUILDING_LIMIT_AMT = 0)
                    and
                        (limit_pre1.BLANKET_CONTENTS_LIMIT_AMT is null and limit_pre1.BLANKET_CONTENTS_LIMIT_AMT = 0)
                    and
                        (limit_pre1.BLANKET_BI_LIMIT_AMT is null and limit_pre1.BLANKET_BI_LIMIT_AMT = 0)
                    and 
                        (limit_pre1.BLANKET_COMBINED_LIMIT_AMT is null and limit_pre1.BLANKET_COMBINED_LIMIT_AMT = 0)
                    and        
                        (limit_pre1.site_limit_amt is null and limit_pre1.site_limit_amt = 0)
                    and    
                        (limit_pre1.combined_limit_amt is null and limit_pre1.combined_limit_amt = 0)
                    and 
                        (limit_pre1.building_limit_amt is null or limit_pre1.building_limit_amt = 0)
                    and 
                        (limit_pre1.contents_limit_amt is null or limit_pre1.contents_limit_amt = 0)
                    and 
                        (limit_pre1.bi_limit_amt is null or limit_pre1.bi_limit_amt = 0)
                )
)
-- xdm view (all BU) monthly 
-- save data to PAIR ALL_AIR_CONTRACT_FILE (all BU)
    -- view with join to Peril_Combinations_2 to create peril string for contract, layer, sublimit, and location perils
-- split out data to each database (SHS_202112.AIR_CONTRACT_FILE_XDM)
, final_table_schema as (
    select 
        /* Contract Detail */    
            policy_layer_enriched.SPM as "ContractID"
        , 	policy_layer_enriched.PRIMARY_INSURED_NM as "InsuredName"
        ,   case policy_layer_enriched.business_unit_cd
                when 'PIM' then 
                    case 
                        when policy_layer_enriched.product_cd = 'PIMPROPEX' then 'PIMx'
                        else 'PIMt'
                    end
                when 'AED' then 'ALT'
                when 'GARS' then 
                    case 
                        when policy_layer_enriched.product_cd = 'DPG' then 'GARS_Condo'
                        else 'GARS'
                    end
                else policy_layer_enriched.business_unit_cd
            end as "LOB"
        ,   case policy_layer_enriched.business_unit_cd 
                when 'AED' then 'ALT'
                else policy_layer_enriched.business_unit_cd
            end as "UDF1"
        , 	policy_layer_enriched.PROFIT_CENTER_CD||'-'||policy_layer_enriched.PROFIT_CENTER_DESC as "UDF2"
        , 	policy_layer_enriched.PRODUCER_CD||'-'||policy_layer_enriched.PRODUCER_DESC as "ProducerName"
        ,   policy_layer_enriched.UNDERWRITER_NM as Underwriter
        
        -- peril indicators translated to peril strings in PAIR
        , 	policy_layer_enriched.excess_policy_ind
        , 	policy_layer_enriched.POLICY_NBCR_IND
        , 	policy_layer_enriched.POLICY_TERRORISM_IND
        , 	policy_layer_enriched.POLICY_EARTHQUAKE_IND
        , 	policy_layer_enriched.POLICY_HURRICANE_IND
        , 	policy_layer_enriched.POLICY_WIND_HAIL_IND
        , 	policy_layer_enriched.POLICY_FIRE_IND
        , 	policy_layer_enriched.POLICY_FLOOD_IND   
        , 	policy_layer_enriched.POLICY_TERM_EFFECTIVE_DT as InceptionDate
        , 	policy_layer_enriched.POLICY_TERM_EXPIRATION_DT as ExpirationDate
        
        ,   case
              when policy_layer_enriched.PRODUCER_COUNTRY_CD = 'US' then 'USD'
              when policy_layer_enriched.PRODUCER_COUNTRY_CD = 'CAN' then 'CAD'  -- ask TL (Agri) and LH (RES)?
              else 'USD'
            end as Currency
    --    , 	policy_layer_enriched.PROFIT_CENTER_CD as Branch
        
        /* Layer Detail */
        ,   case 
              when policy_layer_enriched.excess_policy_ind = 0 then policy_layer_enriched.SPM||'-01' 
              when policy_layer_enriched.excess_policy_ind = 1 then policy_layer_enriched.SPM||'-'||lpad(policy_layer_enriched.EXCESS_LAYER_NO,2,'0')
              else null
            end as LayerID
        -- peril indicators translated to peril strings in PAIR    
        ,   policy_layer_enriched.excess_layer_fire_ind
        ,   policy_layer_enriched.excess_layer_hurricane_ind
        ,   policy_layer_enriched.excess_layer_earthquake_ind
        ,   policy_layer_enriched.excess_layer_flood_ind
        ,   policy_layer_enriched.excess_layer_wind_hail_ind
        ,   policy_layer_enriched.excess_layer_terrorism_ind
        ,   policy_layer_enriched.excess_layer_nbcr_ind
        ,   policy_layer_enriched.excess_layer_none_ind
        
        /* Layer - Limit Detail */
        ,   case 
              when policy_layer_enriched.excess_policy_ind = 0 then ''
              when policy_layer_enriched.excess_policy_ind = 1 then 'E'
              else null 
            end as LimitType
        ,   case 
              when policy_layer_enriched.excess_policy_ind = 0 then null -- what should this be??
              when policy_layer_enriched.excess_policy_ind = 1 then policy_layer_enriched.EXCESS_PART_OF_AMT
              else null 
            end as Limit1
        ,   '' as LimitA -- should be limit_property_structure.blanket_building_limit_amt if blanket_building_limit_cd = 'ALL'
        ,   '' as LimitB
        ,   '' as LimitC
        ,   '' as LimitD
        ,   case 
              when policy_layer_enriched.excess_policy_ind = 0 then null -- what should this be??
              when policy_layer_enriched.excess_policy_ind = 1 then policy_layer_enriched.EXCESS_ATTACHMENT_POINT_AMT
              else null 
            end as AttachmentAmt
        ,   case 
              when policy_layer_enriched.excess_policy_ind = 0 then null  -- what should this be??
              when policy_layer_enriched.excess_policy_ind = 1 then policy_layer_enriched.EXCESS_LIMIT_AMT
              else null 
            end as Limit2      
        
        /* Layer - Deductible Detail. */
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 1  then 'MI' else null end as DedType -- '' if no policy deductible
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 1  then policy_peril_deductible.dedamt1 else null end as DedAmt1
        ,   '' as DedAmt2
        
        /* Layer - Premium Detail */
        ,   policy_premium_fdw.total_policy_premium as Premium -- do we have premium at the layer level or just policy level premium??
        
        /* Sublimit Detail */ 
        -- case when not needed if left joining and null is acceptable. keeping for clarity and potential for need for empty string
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_fire_ind else null end as sublimit_fire_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_hurricane_ind else null end as sublimit_hurricane_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_earthquake_ind else null end as sublimit_earthquake_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_flood_ind else null end as sublimit_flood_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_wind_hail_ind else null end as sublimit_wind_hail_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_terrorism_ind else null end as sublimit_terrorism_ind
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimit_nbcr_ind else null end as sublimit_nbcr_ind
        
        ,   case when policy_peril_deductible.policy_term_id is not null and isAllPerilDeductible = 0 then policy_peril_deductible.sublimitarea else null end as SublimitArea
    
        /* Sublimit - Limit Detail */ -- check Heritage for EQ sublimit
        ,   '' as SublimitType
        ,   '' as SubLimitOcc
        ,   '' as SublimitLimitA
        ,   '' as SublimitLimitB
        ,   '' as SublimitLimitC
        ,   '' as SublimitLimitD
        ,   case when policy_peril_deductible.policy_term_id is not null then policy_peril_deductible.sublimitdedamt1 else null end as SublimitAttachAmt
        ,   '' as SublimitPart
    
        /* Sublimit - Deductible Detail */
        ,   '' as SublimitDedType
        ,   '' as SublimitDedAmt1
        ,   '' as SublimitDedAmt2
    
        /* Policy Detail */
        , 	policy_layer_enriched.POLICY_NBCR_IND as "UDF3"
        ,   'XDM-'||policy_layer_enriched.POLICY_ENTRY_SYSTEM_DESC as "UDF4"
        ,   case policy_layer_enriched.business_unit_cd
                when 'PIM' then 
                    case 
                        when policy_layer_enriched.product_cd = 'PIMPROPEX' then 'PIMx'
                        else 'PIMt'
                    end
                when 'AED' then 'ALT'
                when 'GARS' then 
                    case 
                        when policy_layer_enriched.product_cd = 'DPG' then 'GARS_Condo'
                        else 'GARS'
                    end
                else policy_layer_enriched.business_unit_cd
            end as "UDF5"
        
        ,   SYSDATE as AsOfDate
        ,   policy_layer_enriched.mod_period as ModPeriod
        
        /* XDM identifiers */
        ,   policy_layer_enriched.policy_Term_id
        ,   policy_layer_enriched.product_cd
        
        
    from policy_layer_enriched
      left join 
            policy_premium_fdw
        on 
            policy_layer_enriched.policy_term_id = policy_premium_fdw.policy_term_id
      left join
            policy_peril_deductible_scrub policy_peril_deductible
        on
            policy_layer_enriched.policy_term_id = policy_peril_deductible.policy_term_id
    --where policy_layer_enriched.policy_term_id = 15787
    order by 
          policy_layer_enriched.policy_Term_id
    --    , policy_limit.peril_desc
)

select *
from final_table_schema
where :UDF5
;

-- !! summary
--, business_unit_product_summary as (
--    select mod_period, businesS_unit_desc, product_desc, count(1) as pif_count
--    from policy_product
--    group by mod_period, businesS_unit_desc, product_desc
--    order by business_unit_desc, product_desc
--)