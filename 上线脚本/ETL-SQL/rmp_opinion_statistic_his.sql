-- ÓßÇéÍ³¼ÆÈÕ±íÀúÊ·±í RMP_OPINION_STATISTIC_HIS (Í¬²½·½Ê½£ºÒ»Ìì¶àÅú´Î¸²¸Ç) --
insert into pth_rmp.RMP_OPINION_STATISTIC_HIS partition(etl_date=${ETL_DATE})
select 
	distinct
	a.sid_kw,
	a.score_dt,   
	a.statistic_dim,  
	a.industry_class,  
	a.importance,
	a.level_type_list,
	a.level_type_ii,
	a.opinion_cnt,
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
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --ï¿½ï¿½ï¿½ï¿½Ò»ï¿½ï¿½Ê¼ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Îµï¿½ï¿½ï¿½ï¿½ï¿½Í?ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ê·ï¿½ï¿½