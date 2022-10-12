-- 归因报告 RMP_ATTRIBUTION_SUMM --
-- 依赖 first,main,rel,last 四部分
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
---------------------- 以上部分为临时表 --------------------------------------------------------------------------
insert into pth_rmp.RMP_ATTRIBUTION_SUMM 
select 
	md5(concat(batch_dt,corp_id,'0')) as sid_kw,
	batch_dt,
	corp_id,
	corp_nm,
	credit_code as credit_cd,
	score_dt,
	report_msg1,
	report_msg2,
	'' as report_msg5,
	0 as delete_flag,
	'' as create_by,
	CURRENT_TIMESTAMP() as create_time,
	'' as update_by,
	CURRENT_TIMESTAMP() as update_time,
	0 as version
from 
(
	select 
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		chg.credit_code,
		main.score_dt,
		one.First_sentence as report_msg1,
		case 
			when main.score_hit=1 and main.score>=main.rel_score_summ then 
				concat(main.Main_sentence,'\\r\\n',rel.rel_sentence,'\\r\\n',lst.last_sentence)
			when main.score_hit=1 and main.score<main.rel_score_summ then 
				concat(rel.rel_sentence,'\\r\\n',main.Main_sentence,'\\r\\n',lst.last_sentence)
			when main.score_hit=0  then 
				concat(main.Main_sentence,'\\r\\n',rel.rel_sentence,'\\r\\n',lst.last_sentence)
			-- when score_hit=0 and main.score<main.rel_score_summ then 
				-- concat(rel.rel_sentence,'\\r\\n',main.Main_sentence,'\\r\\n',lst.last_sentence)
		end as report_msg2
	from pth_rmp.RMP_ATTRIBUTION_SUMM_FIRST_TEMP one 
	left join pth_rmp.RMP_ATTRIBUTION_SUMM_MAIN_TEMP main
		on one.corp_id=main.corp_id and one.score_dt=main.score_dt
	left join pth_rmp.RMP_ATTRIBUTION_SUMM_REL_TEMP rel 
		on one.corp_id = rel.corp_id and one.score_dt = rel.score_dt
	left join pth_rmp.RMP_ATTRIBUTION_SUMM_LAST_TEMP lst 
		on one.corp_id = lst.corp_id and one.score_dt = lst.score_dt 
	left join (select * from corp_chg where source_code='FI') chg
		on one.corp_id=chg.corp_id
)A 
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
;
