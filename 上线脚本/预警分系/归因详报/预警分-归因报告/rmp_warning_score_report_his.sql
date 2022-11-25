-- RMP_WARNING_SCORE_REPORT_HIS (一天多次覆盖式插入) --
insert overwrite table  app_ehzh.rmp_warning_score_report_his partition(etl_date=${ETL_DATE})
select 
	a.sid_kw,  
	a.corp_id,
	a.corp_nm,
	a.credit_cd,
	a.score_dt,
	a.report_msg1,
	a.report_msg2,
	a.report_msg3,
	a.report_msg4,
	a.report_msg5,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.rmp_warning_score_report a   --@pth_rmp.rmp_warning_score_report
join (select score_dt,max(batch_dt) as max_batch_dt from pth_rmp.rmp_warning_score_report group by score_dt) b  --@pth_rmp.rmp_warning_score_report
	on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_date=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;