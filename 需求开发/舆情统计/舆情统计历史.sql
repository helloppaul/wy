-- 舆情统计历史 RMP_OPINION_STATISTIC_HIS (同步方式：一天单批次) --
insert into pth_rmp.RMP_OPINION_STATISTIC_HIS partition(dt=${ETL_DATE})
select 
	select distinct
	current_timestamp() as batch_dt,
	score_dt,   --！！！推送oracle时，不推送该字段
	statistic_dim,  --统计维度，1:'行业' 2:'地区'
	industry_class,  -- -1:地区 1:'申万行业' 2:'wind行业' 3:'国标行业' 4:'证监会行业'  99:未知行业
	importance,
	level_type_list,
	level_type_ii,
	opinion_cnt,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_OPINION_STATISTIC_DAY a 
join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_OPINION_STATISTIC_DAY where delete_flag=0) b
	on a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --当天一开始，将昨天的最新批次的数据同步到历史表