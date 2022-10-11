-- 综合舆情分历史 RMP_ALERT_COMPREHS_SCORE_HIS (同步方式：一天多批次覆盖)--
insert overwrite table pth_rmp.RMP_ALERT_COMPREHS_SCORE_HIS partition(etl_date=${ETL_DATE})
select distinct
	a.sid_kw,
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
join (select max(batch_dt) as max_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE group by score_dt) b
	on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1)) --注意ETL_DATE传进来的日期为执行日期前一天
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --当天一开始，将昨天的最新批次的数据同步到历史表
--   and a.score_dt=to_date(date_add(current_timestamp(),-1))   --当天一开始，将昨天的最新批次的数据同步到历史表
;
-- truncate table pth_rmp.RMP_ALERT_COMPREHS_SCORE; --历史表衍生完成，删除前一天的日表数据