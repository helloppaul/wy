-- 舆情统计日表 RMP_OPINION_STATISTIC_DAY (同步方式：一天多批次覆盖)  --
--入参：${ETL_DATE}(20220818 int)，用于筛选score_dt
--PS:不依赖 舆情风险信息整合表，直接依赖上游hds表为主
-- /* 2022-9-19 不剔除快讯和政府预警，将 快讯和政府预警 纳入统计范围 */
-- /* 2022-11-15 舆情统计日表 效率优化 */
-- /* 2022-11-18 修复 sid_kw重复的问题，增加importance作为业务主键 */
-- /* 2022-12-15 新增 在地区维度分类下，在原有省的分类下再细分产业和板块，细分后再做舆情统计 */


set hive.exec.parallel=true;
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

--01 副本临时表创建 --
drop table if exists pth_rmp.company_info_main_03_yqtj;
create table pth_rmp.company_info_main_03_yqtj stored as parquet
as 
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
	where a.etl_date in  (select max(etl_date) as max_etl_date from pth_rmp.rmp_company_info_main)
	  and a.delete_flag=0 
		-- on a.etl_date=b.max_etl_date 
	group by corp_id,credit_code,is_list,is_bond,list_board,bond_type,regorg_prov,industryphy_name,zjh_industry_l1,sw_industry_l1,wind_industry_l1
;


--02 代码逻辑 --
with 
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  where cid1.etl_date in (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance)
			-- on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  where b1.etl_date in (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main )
		  	-- on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
rmp_company_info_main_ as 
(
	select a.*
	from pth_rmp.company_info_main_03_yqtj a
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
	from (select itcode2,newsdate,newscode,CRNW0003_006,flag from hds.tr_ods_rmp_fi_x_news_tcrnw0003_all_v2 where etl_date=${ETL_DATE}) a 
	join corp_chg chg 
		on a.itcode2=chg.source_id and chg.source_code='FI'
	where a.flag<>'1'
	  and cast(a.CRNW0003_006 as int)<0  
),
industry_class_yq as 
(
	select 
		main.corp_id,
		to_date(main.news_dt) as score_dt,
		main.importance,  
		b.statistic_dim,
		b.industry_class,
		b.level_type_list,
		b.level_type_ii,
		main.news_id
		-- count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join rmp_company_info_main_union_hy_ b 
		on main.corp_id=b.corp_id
	group by main.corp_id,to_date(main.news_dt),main.importance,b.statistic_dim,b.industry_class,b.level_type_list,b.level_type_ii,main.news_id
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
		main.corp_id,
		to_date(main.news_dt) as score_dt,
		main.importance,  
		b.statistic_dim,
		-1 as industry_class,
		c.level_type_list,
		b.level_type_ii,
		main.news_id
		-- count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join rmp_company_info_main_union_region_ b 
		on main.corp_id=b.corp_id  
	left join industry_class_yq c    --对于区域的统计维度，在已有对省统计的基础上，在对上市，发债做统计
		on main.corp_id=c.corp_id
	group by main.corp_id,to_date(main.news_dt),main.importance,b.statistic_dim,c.level_type_list,b.level_type_ii,main.news_id  --去重
),
industry_class_union_region_class_statistic as 
(
	select 
		score_dt,
		importance,
		statistic_dim,
		industry_class,
		level_type_list,
		level_type_ii,
		count(news_id) as opinion_cnt
	from industry_class_yq
	group by score_dt,importance,statistic_dim,industry_class,level_type_list,level_type_ii
	union all 
	select 
		score_dt,
		importance,
		statistic_dim,
		industry_class,
		level_type_list,
		level_type_ii,
		count(news_id) as opinion_cnt
	from region_class_yq
	group by score_dt,importance,statistic_dim,industry_class,level_type_list,level_type_ii
)
------------------------------ temp_table above ---------------------------------------------------------
insert overwrite table pth_rmp.rmp_opinion_statistic_day partition(etl_date=${ETL_DATE})
select 
	md5(concat(cast(score_dt as string),statistic_dim,cast(industry_class as string),nvl(level_type_list,'0'),level_type_ii,cast(importance as string),'0')) as sid_kw,
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
	from industry_class_union_region_class_statistic
)Fi
where score_dt= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
;

