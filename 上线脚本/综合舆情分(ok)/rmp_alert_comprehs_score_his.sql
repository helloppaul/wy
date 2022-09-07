-- 综合舆情分历史 RMP_ALERT_COMPREHS_SCORE_HIS (同步方式：一天单批次插入)--
insert into pth_rmp.RMP_ALERT_COMPREHS_SCORE_HIS
select distinct
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
from pth_rmp.RMP_ALERT_COMPREHS_SCORE a
join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE) b
	on a.score_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(current_timestamp(),-1))   --当天一开始，将昨天的最新批次的数据同步到历史表
;