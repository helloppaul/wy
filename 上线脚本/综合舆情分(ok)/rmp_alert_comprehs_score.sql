-- 综合舆情分 RMP_ALERT_COMPREHS_SCORE (同步方式：一天多批次插入)--
-- with RMP_ALERT_COMPREHS_SCORE_TEMP_ as 
-- (
-- 	select * from 
-- )
insert into pth_rmp.RMP_ALERT_COMPREHS_SCORE
select distinct
	a.sid_kw,
	a.batch_dt,
	a.corp_id,
	a.corp_nm,
	a.credit_code,
	a.score_dt,
	a.comprehensive_score,
	a.score_hit,
	a.label_hit,
	a.alert,
	a.fluctuated,
	a.model_version,
	a.adjust_warnlevel,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a
join (select max(batch_dt) as max_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP group by score_dt) b
	on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
where a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;

