-- 归因报告历史 RMP_ATTRIBUTION_SUMM_HIS (同步方式：一天多批次覆盖)--
-- 依赖 first,main,rel,last 四部分
insert overwrite table  pth_rmp.RMP_ATTRIBUTION_SUMM_HIS partition(etl_date=${ETL_DATE})
select distinct
	a.sid_kw,
	a.corp_id,
	a.corp_nm,
	a.credit_cd,
	a.score_dt,
	a.report_msg1,
	a.report_msg2,
	a.report_msg5,
	0 as delete_flag,
	'' as create_by,
	CURRENT_TIMESTAMP() as create_time,
	'' as update_by,
	CURRENT_TIMESTAMP() as update_time,
	0 as version
from pth_rmp.RMP_ATTRIBUTION_SUMM a 
join (select score_dt,max(batch_dt) as max_batch_dt from pth_rmp.RMP_ATTRIBUTION_SUMM group by score_dt) b
	on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --当天一开始，将昨天的最新批次的数据同步到历史表
;