-- 舆情统计日表 RMP_OPINION_STATISTIC_DAY (同步方式：一天多批次插入)  --
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
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
_rmp_company_info_main_ as 
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
_rmp_company_info_main_union as --合并单纯上市和单纯发债企业，并获取单纯上市企业对应的板块(新三板，创业板...)和单纯发债企业对应的板块(城投债，产业债)
(
	select distinct
		corp_id,
		--credit_cd,
		1 as statistic_dim,  --1:'行业' 2:'地区' 
		case list_board
			when 1 then '主板'
			when 2 then '中小板'
			when 3 then '创业板'
			when 4 then  '新三板'
			when 5 then '科创板'
			else '未知上市板块'
		end as level_type_list,  -- 企业属性/上市具体板块
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from _rmp_company_info_main_ 
	where is_list=1 and is_bond=0
	union all 
	select 	distinct
		corp_id,
		--credit_cd,
		1 as statistic_dim,  --1:'行业' 2:'地区'
		case bond_type
			when 1 then '产业债'
			when 2 then '城投债'
			else '未知名债'
		end as level_type_list,		 -- 企业属性/发债具体分类	
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from _rmp_company_info_main_ 
	where is_list=0 and is_bond=1
),
_rmp_company_info_main_union_hy as 
(
	select distinct
		corp_id,
		statistic_dim,
		3 as industry_class,    --国标行业
		level_type_list,
		gb_tag as level_type_ii
	from _rmp_company_info_main_union 
	union all 
	select distinct
		corp_id,
		statistic_dim,
		4 as industry_class,   --证监会行业
		level_type_list,
		zjh_tag as level_type_ii
	from _rmp_company_info_main_union 
	union all
	select distinct
		corp_id,
		statistic_dim,
		1 as industry_class,  --申万行业
		level_type_list,
		sw_tag as level_type_ii
	from _rmp_company_info_main_union
	union all 
	select distinct
		corp_id,
		statistic_dim,
		2 as industry_class,  --wind行业
		level_type_list,
		wind_tag as level_type_ii
	from _rmp_company_info_main_union  
),
main_news_without_kxun_region as  --主体舆情(区域舆情)
(
	select distinct
		chg.corp_id,
		chg.corp_name as corp_nm,
		a.newsdate as news_dt,
		a.newscode as news_id,
		--a.CRNW0003_001 as index_code,  --指标代码
		--a.CRNW0003_010 as data_type, --数据类别  '1':'精编' ,'2':'自编','3':'全量','4':'快讯'
		cast(a.CRNW0003_006 as int) as news_importance,  --预警分类重要性评分(用于判断是否是负面新闻);-3:'严重负面',-2:'重大负面',-3:'一般负面'
		cast(a.CRNW0003_006 as int) as importance
	from hds.tr_ods_rmp_fi_x_news_tcrnw0003_all_v2 a 
	join corp_chg chg 
		on a.itcode2=chg.source_id and chg.source_code='FI'
	where a.flag<>'1'
	  --and a.CRNW0003_001<>'6012000'  --政府预警'6012000'
	  --and a.CRNW0003_010<>'4'
	  and cast(a.CRNW0003_006 as int)<0   --负面舆情限制条件
),
industry_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		main.importance,  --严重程度  1:'一般负面',2:'一般负面',3:'严重负面'
		b.statistic_dim,
		b.industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join _rmp_company_info_main_union_hy b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.industry_class,b.level_type_list,b.level_type_ii
),
_rmp_company_info_main_union_region as
(
	select distinct
		corp_id,
		2 as statistic_dim,  --1:'行业' 2:'地区'
		'' as level_type_list,
		regorg_prov as level_type_ii
	from _rmp_company_info_main_
),
region_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		importance,  --严重程度  1:'一般负面',2:'一般负面',3:'严重负面'
		b.statistic_dim,
		-1 as industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join _rmp_company_info_main_union_region b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.level_type_list,b.level_type_ii
)
------------------------------ 以上部分为临时表 ---------------------------------------------------------
-- insert into pth_rmp.RMP_OPINION_STATISTIC_DAY
select distinct
	from_unixtime(unix_timestamp(cast(current_timestamp() as string),'yyyy-MM-dd HH:mm:ss')) as batch_dt,
	score_dt,   --！！！推送oracle时，不推送该字段
	statistic_dim,  --统计维度，1:'行业' 2:'地区'
	industry_class,  -- -1:地区 1:'申万行业' 2:'wind行业' 3:'国标行业' 4:'证监会行业'  99:未知行业
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
)A where score_dt= to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
;
 

