-- 单主体舆情分历史 RMP_ALERT_SCORE_SUMM_HIS (同步方式：一天单批次插入) --
insert into pth_rmp.RMP_ALERT_SCORE_SUMM_HIS partition(dt=${ETL_DATE})
select distinct
	a.corp_id,
	a.corp_nm,
	a.credit_code,
	a.score_dt,
	a.score,
	a.score_hit_ci,
	a.score_hit_yq,
	a.score_hit,
	a.label_hit ,
	a.alert ,
	a.fluctuated ,
	a.model_version,
	0 as delete_flag,
	'' as create_by,
	CURRENT_TIMESTAMP() as create_time,
	'' as update_by,
	CURRENT_TIMESTAMP() as update_time,
	0 as version
from pth_rmp.RMP_ALERT_SCORE_SUMM a 
join (select score_dt,max(batch_dt) as max_batch_dt from pth_rmp.RMP_ALERT_SCORE_SUMM group by score_dt) b
	on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --当天一开始，将昨天的最新批次的数据同步到历史表
--   and a.score_dt=to_date(date_add(current_timestamp(),-1))   --当天一开始，将昨天的最新批次的数据同步到历史表
  --and to_date(score_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
;