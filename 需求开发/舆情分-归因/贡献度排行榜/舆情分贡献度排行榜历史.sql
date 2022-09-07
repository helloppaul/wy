-- 贡献度排行榜历史 RMP_COMPY_CONTRIB_DEGREE_HIS --
-- 入参：${ETL_DATE} -> to_date(score_dt) --
-- where corp_nm='海通创新证券投资有限公司'  and score_dt='2022-07-03'  and relation_nm in ('上海海通证券资产管理有限公司,海通证券股份有限公司')
select 
	corp_id,
	corp_nm,
	score_dt,
	relation_id,
	relation_nm,
	relation_type_l2_line,
	contribution_degree,
	rank_num,
	abnormal_flag,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from RMP_COMPY_CONTRIB_DEGREE a 
join (select max(batch_dt) as new_batch_dt from RMP_COMPY_CONTRIB_DEGREE) b 
	on a.batch_dt = b.new_batch_dt
where a.delete_flag=0
  and to_date(score_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
