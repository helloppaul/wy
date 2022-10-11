-- RMP_INDUSTRY_NEWS_INFO (ͬ����ʽ��һ������θ���) --
-- ��Σ�${ETL_DATE}(20220818 int)  -> to_date(notice_dt)
with gb AS  --�����������
(
	select * 
	from hds.tr_ods_rmp_fi_tq_oa_industryclass
	where isvalid='1' and indclasscode='2219'
),
gb_drill_up_4 as --���������������
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
gb_drill_up_3 as --���������������
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
gb_drill_up_2 as --���������������
(
	select distinct
		gb2.industryid as gb2_ind,gb2.industryname as gb2_ind_nm,
		gb1.industryid as gb1_ind,gb1.industryname as gb1_ind_nm,
		gb2.industryid,gb2.industryname,2 AS lv
	from (select * from gb where industrylevel='2') gb2
	join gb gb1 
		on gb2.hindustryid = gb1.industryid
),
gb_summ as --����������ݻ���
(

	select distinct
		industryid as gb_industry_tag_cd,
		industryname as gb_industry_tag,
		'' as gb_industry_tag_ii_cd,
		'' as gb_industry_tag_ii,
		industryid,   --��1���id
		industryname
	from gb where industrylevel='1' 
	UNION ALL
	select distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   --��2���id
		industryname
	from gb_drill_up_2
	UNION ALL 
	select distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   ----��3���id
		industryname
	from gb_drill_up_3
	UNION ALL 
	SELECT distinct
		gb1_ind as gb_industry_tag_cd, 
		gb1_ind_nm as gb_industry_tag,
		gb2_ind as gb_industry_tag_ii_cd,
		gb2_ind_nm as gb_industry_tag_ii,
		industryid,   ----��4���id
		industryname
	FROM gb_drill_up_4
)
insert  overwrite table pth_rmp.RMP_INDUSTRY_NEWS_INFO partition(etl_date=${ETL_DATE})
------------------------------ ���ϲ���Ϊ��ʱ�� ---------------------------------------------------------
select distinct 
	md5(concat(cast(news.crnw0001_002 as string),news.newscode,gb_summ.gb_industry_tag_ii_cd,'0')) as sid_kw,
	news.crnw0001_002 as notice_dt,
	news.newscode as news_id,   --���ű����Ѿ�����ʱ��
	news.crnw0001_003 as news_title,
	gb_summ.gb_industry_tag_cd,
	gb_summ.gb_industry_tag,
	gb_summ.gb_industry_tag_ii_cd,
	gb_summ.gb_industry_tag_ii,
	news.CRNW0001_007 as news_from,
	news.CRNW0001_010 as news_url,
	-- news_detail.CRNW0002_001 as news
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from (select * from hds.tr_ods_rmp_fi_x_news_tcrnw0006 where flag<>'1') hy
join (select * from hds.tr_ods_rmp_fi_x_news_tcrnw0001 where flag<>'1')news
	on hy.NEWSCODE = news.NEWSCODE 
left join gb_summ
	on cast(hy.CRNW0006_001 as string) = gb_summ.industryid
-- left join (select * from hds.tr_ods_rmp_fi_x_news_tcrnw0002 where flag<>'1') news_detail
-- 	on news.NEWSCODE = news_detail.NEWSCODE
where to_date(news.crnw0001_002)=to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1)) 
;
