/*
Mid Continent control totals
compare current to prior db
*/
declare @Current_db varchar(50) = '[MC_202212_v2]'
	declare @Current_db_ContractFileName varchar(255) = '[dbo].[AIR_Contract_File]'
	declare @Current_db_LocationFileName varchar(255) = '[dbo].[AIR_Location_File]'

declare @Prior_db varchar(50) = '[MC_202206_v1]'
	declare @Prior_db_ContractFileName varchar(255) = '[dbo].[AIR_Contract_File]'
	declare @Prior_db_LocationFileName varchar(255) = '[dbo].[AIR_Location_File]'

declare @Prior2_db varchar(50) = '[MC_202112_v2]'
	declare @Prior2_db_ContractFileName varchar(255) = '[dbo].[AIR_Contract_File]'
	declare @Prior2_db_LocationFileName varchar(255) = '[dbo].[AIR_Location_File]'

declare @DoesYourBUHaveReinsurance varchar(1) = 'N'												--<<<< Make this 'N' when no reinsurance file is available
		declare @Current_db_ReinsuranceFileName varchar(255) = '[dbo].[AIR_Reinsurance_File]'

/*|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
*/
/*		Compare record counts from the Contract table current vs prior*/
select 'Record Counts for Contract Table'
exec('
	select ''' + @Current_db + ''' Contract_rec_Count, count(*) rec_Count from '+ @Current_db +'.'+ @Current_db_ContractFileName +'
	union all
	select ''' + @Prior_db + ''', count(*) rec_Count from '+ @Prior_db +'.'+ @Prior_db_ContractFileName + '
	union all
	select ''' + @Prior2_db + ''', count(*) rec_Count from '+ @Prior2_db +'.' +  @Prior2_db_ContractFileName +'
	')

/*		Compare record counts from the Location table current vs prior*/
select 'Record Counts for Location Table'
exec('
	select ''' + @Current_db + ''' Location_rec_Count, count(*) rec_Count from '+ @Current_db +'.'+ @Current_db_LocationFileName +'
	union all
	select ''' + @Prior_db + ''', count(*) rec_Count from '+ @Prior_db +'.'+ @Prior_db_LocationFileName +' 
	union all
	select ''' + @Prior2_db + ''', count(*) rec_Count from '+ @Prior2_db +'.'+ @Prior2_db_LocationFileName +' 
	')

/*		Compare sum(buildingvalue) from the Location table current vs prior*/
select 'Summed Building Value for Location Table'
exec('
	select ''' + @Current_db + ''' Location_sum_Building, sum(cast(BuildingValue as float)) SumBuildingValue from '+ @Current_db +'.'+ @Current_db_LocationFileName +'
	union all
	select ''' + @Prior_db + ''', sum(cast(BuildingValue as float)) SumBuildingValue from '+ @Prior_db +'.'+ @Prior_db_LocationFileName +' 
	union all
	select ''' + @Prior2_db + ''', sum(cast(BuildingValue as float)) SumBuildingValue from '+ @Prior2_db +'.'+ @Prior2_db_LocationFileName +' 
	')

/*		Compare sum(deductbldg) from the Location table current vs prior*/
select 'Summed deductbldg from Location Table'
exec('
	select ''' + @Current_db + ''' Location_sum_DeductBldg, sum(CAST(DeductBldg as FLOAT)) SumDeductBldg from '+ @Current_db +'.'+ @Current_db_LocationFileName +'
	union all
	select ''' + @Prior_db + ''', sum(CAST(DeductBldg as FLOAT)) SumDeductBldg from '+ @Prior_db +'.'+ @Prior_db_LocationFileName +' 
	union all
	select ''' + @Prior2_db + ''', sum(CAST(DeductBldg as FLOAT)) SumDeductBldg from '+ @Prior2_db +'.'+ @Prior2_db_LocationFileName +' 
	')

/*See if location counts are smaller in new dataset*/
select 'Location counts by policy for Location Table'
EXEC(';WITH CurrentLocCounts as (
	select 
		CASE WHEN LEN(contractid) = 12 THEN substring(contractid,4,7)
		WHEN LEN(contractid) <> 12 THEN left(contractid,7)
		END as POL
		,	contractID CurrentContractID
		,	count(distinct(locationID)) loccount   
	from ' + @Current_db + '.'+ @Current_db_LocationFileName +' 
	group by contractid 
	),
	PriorLocCounts as (
	select 
		CASE WHEN LEN(contractid) = 12 THEN substring(contractid,4,7)
			WHEN LEN(contractid) <> 12 THEN left(contractid,7)
		END as POL
		,	contractID PriorContractID
		,	count(distinct(locationID)) loccount 
	from ' + @Prior_db + '.'+ @Prior_db_LocationFileName +' 
	group by contractid 
	)
	select b.Priorcontractid,b.loccount,a.Currentcontractid,a.loccount,b.loccount - a.loccount Diff 
	from CurrentLocCounts a 
	join PriorLocCounts b 
		on a.POL = b.POL
	order by b.loccount - a.loccount desc')

/*Make sure that all contractid's in account have locations applied*/
select 'Verify all contractids from Contract table exist in Location table. Only looks for where count in location = 0. Research these'
EXEC('select  a.ContractID, count(b.LocationID)
	from ' + @Current_db + '.'+ @Current_db_ContractFileName +' a
	LEFT JOIN ' + @Current_db + '.'+ @Current_db_LocationFileName +' b
		on a.ContractID = b.contractid
	group by a.ContractID
	having count(b.LocationID) = 0')


/*Make sure that all contractid's in location have an account applied*/
select 'Verify all contractids from Location table exist in Contract table. Only looks for where count in Contract = 0. Research these'
EXEC('select  a.ContractID, count(b.ContractID)
	from ' + @Current_db + '.'+ @Current_db_LocationFileName +' a
	LEFT JOIN ' + @Current_db + '.'+ @Current_db_ContractFileName +' b
		on a.ContractID = b.contractid
	group by a.ContractID
	having count(b.ContractID) = 0')

/*Look at SublimitPeril Counts between current and prior*/
select 'Check the SublimitPeril Counts between Current | Prior | Prior2 databases'
EXEC(';
WITH currentSubPerilCount as (
select ''' + @Current_db + ''' db,sublimitPerils,Count(SublimitPerils) Counts
from ' + @Current_db + '.'+ @Current_db_ContractFileName +' 
where SublimitPerils is not null and SublimitPerils <> ''''
group by sublimitPerils
),
PriorSubPerilCount as (
select ''' + @Prior_db + ''' db,sublimitPerils,Count(SublimitPerils) Counts
from ' + @Prior_db + '.'+ @Prior_db_ContractFileName + '
where SublimitPerils is not null and SublimitPerils <> ''''
group by sublimitPerils
),
Prior2SubPerilCount as (
select ''' + @Prior2_db + ''' db,sublimitPerils,Count(SublimitPerils) Counts
from ' + @Prior2_db + '.'+ @Prior2_db_ContractFileName + ' 
where SublimitPerils is not null and SublimitPerils <> ''''
group by sublimitPerils
)
select * from currentSubPerilCount union all
select * from PriorSubPerilCount union all
select * from Prior2SubPerilCount
order by sublimitPerils,db
')

/*Check LayerID formatting*/
exec('
select 
	 CASE 
		WHEN CHARINDEX(''-'',Layerid,1) = 8 and Len(Layerid) = 10 THEN ''7Digit-mod''
		WHEN CHARINDEX(''-'',Layerid,1) = 13 and Len(Layerid) = 15 THEN ''SymPolMod-LayerNumber''
		WHEN len(Layerid) = 12 then ''Sym+Pol+Mod''
		ELSE ''Check these''
	end as Layerid_Format
	,	count(1)
	,	max(Layerid) as [Sample]
from ' + @Current_db + '.'+ @Current_db_ContractFileName +'
group by 
	CASE 
		WHEN CHARINDEX(''-'',Layerid,1) = 8 and Len(Layerid) = 10 THEN ''7Digit-mod''
		WHEN CHARINDEX(''-'',Layerid,1) = 13 and Len(Layerid) = 15 THEN ''SymPolMod-LayerNumber''
		WHEN len(Layerid) = 12 then ''Sym+Pol+Mod''
		ELSE ''Check these''
	end
')

/*Occupancy Checks*/
select 'Comparing Occupancy code and counts from Current to Prior'
EXEC('
;WITH 
ALL_OCCS_Current_And_Prior as (
	select distinct OccupancyCode 
	from ' + @Current_db + '.'+ @Current_db_LocationFileName +'
	Union all
	select distinct OccupancyCode 
	from ' + @Prior_db + '.'+ @Prior_db_LocationFileName +'
),
DistinctOccs as (
	Select Distinct OccupancyCode
	from ALL_OCCS_Current_And_Prior
),
Current_Occ_Counts as (
	select c.OccupancyCode, Count(c.OccupancyCode) CurrentCount
	FROM ' + @Current_db + '.'+ @Current_db_LocationFileName +' c 
	Group by c.OccupancyCode
),
Prior_Occ_Counts as (
select p.OccupancyCode, Count(p.OccupancyCode) PriorCount
FROM ' + @Prior_db + '.'+ @Prior_db_LocationFileName +' p 
Group by p.OccupancyCode
),
OccupancyTable as (
	Select 
			a.OccupancyCode
		,	o.AIROccupancy
		,	sum(c.CurrentCount) CurrentCount
		,	sum(p.PriorCount) PriorCount
		,	coalesce(sum(c.CurrentCount),0) - coalesce(sum(p.PriorCount),0) diff
	From DistinctOccs a
	LEFT join Current_Occ_Counts c
		on a.occupancyCode = c.OccupancyCode
	LEFT join Prior_Occ_Counts p
		on a.occupancyCode = p.OccupancyCode
	left join  [Reference].[dbo].[VALIDATE_OCC_CODE_REGION] o
		on a.OccupancyCode = o.AIROccupancyCode
		and o.AIRConstOccRegionCode = ''US''
	group by a.OccupancyCode
		,	o.AIROccupancy
),
all_ccupancy as (
select 
		OccupancyCode
	,	AIROccupancy
	,	CurrentCount
	,	PriorCount
	,	diff
from OccupancyTable
UNION
select 
		''TOTAL'' [OccupancyCode]
	,	'''' [AIROccupancy]
	,	sum(CurrentCount) [CurrentCount]
	,	sum(PriorCount) [PriorCount]
	,	sum(diff) [diff]
from OccupancyTable
)
select * from all_ccupancy order by diff desc
')

--/*Construction Checks*/
select 'Comparing Construction code and counts from Current to Prior'
EXEC(';WITH ALLConstructionCodes as (
	select distinct ConstructionCode,CountryISO from ' + @Current_db + '.'+ @Current_db_LocationFileName +'
	Union 
	select distinct ConstructionCode,CountryISO from ' + @Prior_db + '.'+ @Prior_db_LocationFileName +'
),
	DistinctConstructionCodes as (
	Select Distinct ConstructionCode,CountryISO from ALLConstructionCodes
),
	CurrentConstructionCounts as (
	Select ConstructionCode,count(1) CurrentCounts
	From ' + @Current_db + '.'+ @Current_db_LocationFileName +'
	group by ConstructionCode
),
	PriorConstructionCounts as (
	Select ConstructionCode,count(1) PriorCounts
	From ' + @Prior_db + '.'+ @Prior_db_LocationFileName +'
	group by ConstructionCode
),
ConstructionTable as (
Select
		a.ConstructionCode
	,	CON.AIRConstructionCategory
	,	CON.AIRConstruction
	,	SUM(c.CurrentCounts) [CurrentCounts]
	,	SUM(p.PriorCounts) [PriorCounts]
	,	COALESCE(SUM(c.CurrentCounts),0) - COALESCE(SUM(p.PriorCounts),0) [diff]
FROM
	DistinctConstructionCodes a
LEFT JOIN CurrentConstructionCounts c
	on a.ConstructionCode = c.ConstructionCode
LEFT JOIN PriorConstructionCounts p
	on a.ConstructionCode = p.ConstructionCode
LEFT JOIN  [Reference].[dbo].[VALIDATE_CONSTRUC_CODE_REGION] CON
	on a.ConstructionCode = CON.AIRConstructionCode
	and a.CountryISO = CON.AIRConstOccRegionCode
Group by 
	a.ConstructionCode
	,	CON.AIRConstructionCategory
	,	CON.AIRConstruction
)
Select
	ConstructionCode
	,	AIRConstructionCategory
	,	AIRConstruction
	,	CurrentCounts
	,	PriorCounts
	,	diff
FROM ConstructionTable
UNION
Select
		''TOTAL'' [ConstructionCode]
	,	'''' [AIRConstructionCategory]
	,	'''' [AIRConstruction]
	,	Sum(CurrentCounts) [CurrentCounts]
	,	Sum(PriorCounts) [PriorCounts]
	,	SUM(Diff) [diff]
FROM ConstructionTable
')

/*Control Totals by State comparing Current and Prior Location Table*/
select 'Control Totals by State comparing Current and Prior Location Table'
EXEC('
WITH Current_CTE AS
	(	Select cr.Area as [State],
		SUM(cast(cr.BuildingValue as float) + cast(cr.OtherValue as float) + cast(cr.ContentsValue as float) + cast(cr.TimeElementValue as float)) as [Current_TIV],
		COUNT(cr.LocationID) as [Current_Locs]
		From ' + @Current_db + '.'+ @Current_db_LocationFileName +' cr
		Group By cr.Area
	),

Prior_CTE AS
	(	Select pr.Area as [State],
		SUM(cast(pr.BuildingValue as float) + cast(pr.OtherValue as float) + cast(pr.ContentsValue as float) + cast(pr.TimeElementValue as float)) as [Prior_TIV],
		COUNT(pr.LocationID) as [Prior_Locs]
		From ' + @Prior_db + '.'+ @Prior_db_LocationFileName +' pr
		Group By pr.Area 
	)

Select pr.State,
	   FORMAT(cr.Current_TIV, ''C0'') as [Current_TIV],
	   FORMAT(pr.Prior_TIV, ''C0'') as [Prior_TIV],
	   FORMAT(cr.Current_TIV - pr.Prior_TIV, ''C0'') as [TIV_Change],
	   FORMAT((cr.Current_TIV - pr.Prior_TIV) / pr.Prior_TIV, ''P'') as [TIV % Change],
	   FORMAT(cr.Current_Locs, ''N0'') as [Current_Locs],
	   FORMAT(pr.Prior_Locs, ''N0'') as [Prior_Locs],
	   FORMAT(cr.Current_Locs - pr.Prior_Locs, ''N0'') as [Loc_Change],
	   FORMAT(CONVERT(float, cr.Current_Locs - pr.Prior_Locs) / (pr.Prior_Locs), ''P'') as [Loc % Change]
From Current_CTE cr
Left Join Prior_CTE pr
On cr.State = pr.State
')

IF @DoesYourBUHaveReinsurance = 'N' 
	 goto EOF
ELSE goto Reinsurance

Reinsurance:
/*Verify that all exposureid's in Reinsurance have a connection to a Layerid in Contract*/
select 'Verify that all exposureid''s in Reinsurance have a connection to a Layerid in Contract'
EXEC('
select r.ContractID,c.ContractID,r.Exposureid,c.LayerID
from ' + @Current_db + '.'+ @Current_db_ReinsuranceFileName +' r
left join ' + @Current_db + '.'+ @Current_db_ContractFileName +' c
	on r.Exposureid = c.layerid
where c.contractid is null
')

/*Check Exposureid formatting*/
select 'Verify the format of the ExposureID in the reinsurance file'
EXEC('
select 
	 CASE 
		WHEN CHARINDEX(''-'',Exposureid,1) = 8 and Len(Exposureid) = 10 THEN ''7Digit-mod''
		WHEN len(Exposureid) = 12 then ''Sym+Pol+Mod''
		ELSE ''Check these''
	end as Exposureid_Format
	,	count(1)
	,	max(ExposureID) as [Sample]
from ' + @Current_db + '.'+ @Current_db_ReinsuranceFileName +'
group by 
	CASE 
		WHEN CHARINDEX(''-'',Exposureid,1) = 8 and Len(Exposureid) = 10 THEN ''7Digit-mod''
		WHEN len(Exposureid) = 12 then ''Sym+Pol+Mod''
		ELSE ''Check these''
	end
')


EOF: