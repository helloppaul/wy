-- RMP_WARNING_SCORE_MODEL_HIS (一天多次插入) --
insert into pth_rmp.RMP_WARNING_SCORE_MODEL_HIS partition(dt=${ETL_DATE})
select 
	a.sid_kw,  --@impala
	a.corp_id,
	a.corp_nm,
	a.credit_cd,
	a.score_date,
	a.synth_warnlevel, 
	a.synth_score,  
	a.model_version,
	a.adjust_warnlevel,   
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_WARNING_SCORE_MODEL a
join (select max(batch_dt) as max_batch_dt,score_date from pth_rmp.RMP_WARNING_SCORE_MODEL where delete_flag=0 group by score_date) b
	on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
where a.delete_flag=0
  and a.score_date=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  
