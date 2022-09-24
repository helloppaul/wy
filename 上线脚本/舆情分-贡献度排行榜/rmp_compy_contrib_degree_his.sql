-- 贡献度排行榜历史 RMP_COMPY_CONTRIB_DEGREE_HIS (一天多批次覆盖) --
-- 入参：${ETL_DATE} -> to_date(score_dt) --
-- where corp_nm='海通创新证券投资有限公司'  and score_dt='2022-07-03'  and relation_nm in ('上海海通证券资产管理有限公司,海通证券股份有限公司')
insert into pth_rmp.RMP_COMPY_CONTRIB_DEGREE_HIS partition(etl_date=${ETL_DATE})
select 
	a.sid_kw,
	a.corp_id,
	a.corp_nm,
	a.score_dt,
	a.relation_id,
	a.relation_nm,
	a.relation_type_l2_line,
	a.contribution_degree,
	a.rank_num,
	a.abnormal_flag,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_COMPY_CONTRIB_DEGREE a 
join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.RMP_COMPY_CONTRIB_DEGREE group by score_dt) b 
	on a.batch_dt = b.new_batch_dt and a.score_dt=b.score_dt
where a.delete_flag=0
  and a.score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  
;
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --当天一开始，将昨天的最新批次的数据同步到历史表
--   and to_date(score_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
