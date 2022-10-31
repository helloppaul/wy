-- RMP_INDUSTRY_NEWS_INFO (?同步方式：一天多批次插入) --
-- 入参：${ETL_DATE}(20220818 int)  -> to_date(notice_dt)
--/*2022-10-31 效率优化 （1）接口层增加时间限制 （2）增加调优参数 跑一天大约 6-10min */
--PS:不依赖 舆情风险信息整合表，直接依赖上游hds表为主

set hive.exec.parallel=true;
set hive.auto.convert.join=ture;

--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with gb AS  --国标分类数据
(
	select hindustryid,industryid,industryname,industrylevel
	from hds.tr_ods_rmp_fi_tq_oa_industryclass
	where isvalid='1' and indclasscode='2219'
	group by hindustryid,industryid,industryname,industrylevel  --去重
),
tr_ods_rmp_fi_x_news_tcrnw0001_ as 
(
	select 
		crnw0001_002,newscode,crnw0001_003,CRNW0001_007,CRNW0001_010,flag
	from 
	(
		select 
			crnw0001_002,newscode,crnw0001_003,CRNW0001_007,CRNW0001_010,flag,
			row_number() over(partition by newscode order by flag desc) as rm
		from hds.tr_ods_rmp_fi_x_news_tcrnw0001
		where flag<>'1'
		and etl_date = ${ETL_DATE}
	) A where rm=1
),
tr_ods_rmp_fi_x_news_tcrnw0002_ as 
(
	select 
		NEWSCODE,CRNW0002_001,flag
	from 
	(
		select 
			NEWSCODE,CRNW0002_001,flag,
			row_number() over(partition by newscode order by flag desc) as rm
		from hds.tr_ods_rmp_fi_x_news_tcrnw0002
		where flag<>'1'
		and etl_date = ${ETL_DATE} 
	)A where rm=1
),
tr_ods_rmp_fi_x_news_tcrnw0006_ as 
(
	select 
		NEWSCODE,CRNW0006_001,flag
	from 
	(
		select 
			NEWSCODE,CRNW0006_001,flag,
			row_number() over(partition by NEWSCODE,CRNW0006_001 order by flag desc) as rm
		from hds.tr_ods_rmp_fi_x_news_tcrnw0006 
		where flag<>'1'
		and etl_date = ${ETL_DATE}
	) A where rm=1
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
gb_drill_up_4 as --国标分类数据上钻
(
	select distinct
		gb4.industryid as gb4_ind,gb4.industryname as gb4_ind_nm,
		gb3.industryid as gb3_ind,gb3.industryname as gb3_ind_nm,
		gb2.industryid as gb2_ind,gb2.industryname as gb2_ind_nm,
		gb1.industryid as gb1_ind,gb1.industryname as gb1_ind_nm,
		gb4.industryid,gb4.industryname,4 AS lv
	from gb gb4
	join gb gb3 
		on gb4.hindustryid = gb3.industryid
	join gb gb2
		on gb3.hindustryid = gb2.industryid
	join gb gb1 
		on gb2.hindustryid = gb1.industryid
),
gb_drill_up_3 as --国标分类数据上钻
(
	select distinct
		gb3.industryid as gb3_ind,gb3.industryname as gb3_ind_nm,
		gb2.industryid as gb2_ind,gb2.industryname as gb2_ind_nm,
		gb1.industryid as gb1_ind,gb1.industryname as gb1_ind_nm,
		gb3.industryid,gb3.industryname,3 AS lv
	from (select * from gb where industrylevel='3') gb3 
	join gb gb2
		on gb3.hindustryid = gb2.industryid
	join gb gb1 
		on gb2.hindustryid = gb1.industryid
),
gb_drill_up_2 as --国标分类数据上钻
(
	select distinct
		gb2.industryid as gb2_ind,gb2.industryname as gb2_ind_nm,
		gb1.industryid as gb1_ind,gb1.industryname as gb1_ind_nm,
		gb2.industryid,gb2.industryname,2 AS lv
	from (select * from gb where industrylevel='2') gb2
	join gb gb1 
		on gb2.hindustryid = gb1.industryid
),
gb_summ as --国标分类数据汇总
(
	select distinct
		industryid as gb_industry_tag_cd,
		industryname as gb_industry_tag,
		'' as gb_industry_tag_ii_cd,
		'' as gb_industry_tag_ii,
		industryid,   --第1层的id
		industryname
	from gb where industrylevel='1' 
	UNION ALL
	select distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   --第2层的id
		industryname
	from gb_drill_up_2
	UNION ALL 
	select distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   ----第3层的id
		industryname
	from gb_drill_up_3
	UNION ALL 
	SELECT distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   ----第4层的id
		industryname
	FROM gb_drill_up_4
),
hy as 
(
	select 
		newscode,
		crnw0006_001        
	from tr_ods_rmp_fi_x_news_tcrnw0006_ a 
),
news_info as 
(
	select 
		max(crnw0001_002) as crnw0001_002,
		newscode,
		max(crnw0001_003) as crnw0001_003,
		max(CRNW0001_007) as CRNW0001_007,
		max(CRNW0001_010) as CRNW0001_010
	from tr_ods_rmp_fi_x_news_tcrnw0001_ a
	group by a.newscode
),
news_detail as 
(
	select 
		a.NEWSCODE,
		max(a.CRNW0002_001) as CRNW0002_001
	from tr_ods_rmp_fi_x_news_tcrnw0002_ a
	group by a.NEWSCODE
)
insert  overwrite table pth_rmp.RMP_INDUSTRY_NEWS_INFO partition(etl_date=${ETL_DATE})
------------------------------ 以上为临时表 ---------------------------------------------------------
select 
	concat(cast(news_id as string),gb_industry_tag_cd,gb_industry_tag_ii_cd) as sid_kw,
	A.*
from 
(
	select distinct 
		-- concat(cast(news.newscode as string),gb_summ.gb_industry_tag_ii_cd) as sid_kw,
		news_info.crnw0001_002 as notice_dt,
		news_info.newscode as news_id,   --新闻编码已经包含时间
		news_info.crnw0001_003 as news_title,
		gb_summ.gb_industry_tag_cd,
		gb_summ.gb_industry_tag,
		gb_summ.gb_industry_tag_ii_cd,
		gb_summ.gb_industry_tag_ii,
		news_info.CRNW0001_007 as news_from,
		news_info.CRNW0001_010 as news_url,
		news_detail.CRNW0002_001 as news,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
	from hy
	join news_info
		on hy.NEWSCODE = news_info.NEWSCODE 
	left join gb_summ
		on cast(hy.CRNW0006_001 as string) = gb_summ.industryid
	left join news_detail
		on news_info.NEWSCODE = news_detail.NEWSCODE
)A
where to_date(notice_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
;
