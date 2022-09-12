
create table pth_rmp.corp_sample as 
with 
_compy_main_info_ as
(
	select a.*
	from pth_rmp.rmp_company_info_main a 
	join (select max(etl_date) max_etl_date from pth_rmp.rmp_company_info_main) b 
		on a.etl_date=b.max_etl_date
		
),
compy_type_range as 
(
	select 
		a.*,
		ROW_NUMBER() over(partition by company_type
							--order by company_type desc ,is_list,is_bond,rating,regorg_prov,regorg_city,regorg_county,industryphy_name 
							order by company_type
						)
			as rm_company_type,
		-- ROW_NUMBER() over(partition by is_list,is_bond
		-- 					order by 1 )
		-- 	as rm_is_bond_list,
		ROW_NUMBER() over(partition by rating
							order by 1 )
			as rm_rating,
		ROW_NUMBER() over(partition by regorg_prov,regorg_city,regorg_county
							order by 1 )
			as rm_region,
		ROW_NUMBER() over(partition by industryphy_name
							order by 1 )
			as rm_gb
	from
	(
		select distinct
			corp_id,
			corp_name as corp_nm,
			credit_code as credit_cd,
			company_type,
			is_list,
			is_bond,
			rating,
			regorg_prov,
			regorg_city,
			regorg_county,
			industryphy_name
		from _compy_main_info_
		where industryphy_name in ('采矿业','交通运输、仓储和邮政业','住宿和餐饮业','国标组织') 
		  and regorg_prov in ('上海','湖北','广东')
	)A 
)
select 
	* 
from 
(
	select distinct
		*
		--ROW_NUMBER() over(order by 1) as id
	from 
	(
		select 
			*
		from compy_type_range
		where rm_company_type<=5
		order by company_type desc
		union all 
		select 
			*
		from compy_type_range
		where rm_rating<=5
		order by rating desc
		union all
		select 
			*
		from compy_type_range
		where rm_region<=5
		order by regorg_prov,regorg_city,regorg_county
		union all 
		select 
			*
		from compy_type_range
		where rm_gb<=5
		order by industryphy_name desc
	)B 
)C
;


