-- RMP_WARNING_SCORE_REPORT (一天多批次插入) --
insert into pth_rmp.rmp_warning_score_report partition(etl_date=${ETL_DATE})
select 
	-- '' as sid_kw,  --impala
	md5(concat(a.batch_dt,a.corp_id,cast(a.score_dt as string))) as sid_kw,  --hive
	a.batch_dt,
	a.corp_id,
	a.corp_nm,
	a.credit_cd,
	a.score_dt,
	a.msg1 as report_msg1,
	b.msg2 as report_msg2,
	c.msg3 as report_msg3,
	d.msg4 as report_msg4,
	b.msg5 as report_msg5,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.rmp_warning_score_report1 a
left join pth_rmp.rmp_warning_score_report2 b
	on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.score_dt=b.score_dt
left join pth_rmp.rmp_warning_score_report3 c
	on a.batch_dt=c.batch_dt and a.corp_id=c.corp_id and a.score_dt=c.score_dt
left join pth_rmp.rmp_warning_score_report4 d
	on a.batch_dt=d.batch_dt and a.corp_id=d.corp_id and a.score_dt=d.score_dt
where a.score_date=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;