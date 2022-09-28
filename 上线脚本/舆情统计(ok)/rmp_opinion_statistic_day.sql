-- 舆情统计日表 RMP_OPINION_STATISTIC_DAY (同步方式：一天多批次覆盖)  --
--入参：${ETL_DATE}(20220818 int)，用于筛选score_dt
-- /*2022-9-19 不剔除快讯和政府预警，将 快讯和政府预警 纳入统计范围*/
with 
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  join (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main ) b2
		  	on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
rmp_company_info_main_ as 
(
	select 
		corp_id,credit_code as credit_cd,
		is_list,is_bond,
		list_board,bond_type,
		regorg_prov,   --省
		industryphy_name as gb_tag,
		zjh_industry_l1 as zjh_tag,
		sw_industry_l1 as sw_tag,
		wind_industry_l1 as wind_tag
	from pth_rmp.rmp_company_info_main a 
	join (select max(etl_date) as max_etl_date from pth_rmp.rmp_company_info_main where delete_flag=0) b
		on a.etl_date=b.max_etl_date 
	group by corp_id,credit_code,is_list,is_bond,list_board,bond_type,regorg_prov,industryphy_name,zjh_industry_l1,sw_industry_l1,wind_industry_l1
),
rmp_company_info_main_union_ as 
(
	select distinct
		corp_id,
		1 as statistic_dim,  
		case list_board
			when 1 then '主板'
			when 2 then '中小板'
			when 3 then '创业板'
			when 4 then  '新三板'
			when 5 then '科创板'
			else '未知上市板块'
		end as level_type_list, 
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from rmp_company_info_main_ 
	where is_list=1 and is_bond=0
	union all 
	select 	distinct
		corp_id,
		1 as statistic_dim, 
		case bond_type
			when 1 then '产业债'
			when 2 then '城投债'
			else '未知名债'
		end as level_type_list,		
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from rmp_company_info_main_ 
	where is_list=0 and is_bond=1
),
rmp_company_info_main_union_hy_ as 
(
	select distinct
		corp_id,
		statistic_dim,
		3 as industry_class,   
		level_type_list,
		gb_tag as level_type_ii
	from rmp_company_info_main_union_ 
	union all 
	select distinct
		corp_id,
		statistic_dim,
		4 as industry_class,   
		level_type_list,
		zjh_tag as level_type_ii
	from rmp_company_info_main_union_ 
	union all
	select distinct
		corp_id,
		statistic_dim,
		1 as industry_class,  
		level_type_list,
		sw_tag as level_type_ii
	from rmp_company_info_main_union_
	union all 
	select distinct
		corp_id,
		statistic_dim,
		2 as industry_class,  
		level_type_list,
		wind_tag as level_type_ii
	from rmp_company_info_main_union_  
),
main_news_without_kxun_region as 
(
	select distinct
		chg.corp_id,
		chg.corp_name as corp_nm,
		a.newsdate as news_dt,
		a.newscode as news_id,
		cast(a.CRNW0003_006 as int) as news_importance,
		cast(a.CRNW0003_006 as int) as importance
	from hds.tr_ods_rmp_fi_x_news_tcrnw0003_all_v2 a 
	join corp_chg chg 
		on a.itcode2=chg.source_id and chg.source_code='FI'
	where a.flag<>'1'
	  and cast(a.CRNW0003_006 as int)<0  
),
industry_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		main.importance,  
		b.statistic_dim,
		b.industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join rmp_company_info_main_union_hy_ b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.industry_class,b.level_type_list,b.level_type_ii
),
rmp_company_info_main_union_region_ as
(
	select distinct
		corp_id,
		2 as statistic_dim,  
		'' as level_type_list,
		regorg_prov as level_type_ii
	from rmp_company_info_main_
),
region_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		importance,  
		b.statistic_dim,
		-1 as industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join rmp_company_info_main_union_region_ b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.level_type_list,b.level_type_ii
)
------------------------------ temp_table above ---------------------------------------------------------
insert overwrite table pth_rmp.RMP_OPINION_STATISTIC_DAY partition(etl_date=${ETL_DATE})
select 
	concat(batch_dt,statistic_dim,cast(industry_class as string),level_type_list,level_type_ii,'0') as sid_kw,
	*
from 
(
	select distinct
		from_unixtime(unix_timestamp(cast(current_timestamp() as string),'yyyy-MM-dd HH:mm:ss')) as batch_dt,
		score_dt,   
		statistic_dim,  
		industry_class,  
		importance,
		level_type_list,
		level_type_ii,
		opinion_cnt,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
	from
	(
		select * from industry_class_yq
		union all 
		select * from region_class_yq
	)A 
)Fi
where score_dt= to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
;
