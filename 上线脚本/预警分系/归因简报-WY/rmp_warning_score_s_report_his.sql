-- RMP_WARNING_SCORE_S_REPORT (同步方式：一天单批次插入) --
------------------------------------以上部分为临时表-------------------------------------------------------------------
insert into pth_rmp.RMP_WARNING_SCORE_S_REPORT_HIS partition(etl_date=${ETL_DATE})
select distinct
	a.sid_kw,
	a.corp_id,
	a.corp_nm,
	a.score_dt,
	a.report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
  -- cast(from_unixtime(unix_timestamp(to_date(a.score_dt),'yyyy-MM-dd') ,'yyyyMMdd') as int) as dt
from pth_rmp.RMP_WARNING_SCORE_S_REPORT a
join (select score_dt,max(batch_dt) as max_batch_dt from pth_rmp.RMP_WARNING_SCORE_S_REPORT where delete_flag=0 group by score_dt) b
	on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;

