-- 贡献度排行榜 RMP_COMPY_CONTRIB_DEGREE --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_nm='海通创新证券投资有限公司'  and score_dt='2022-07-03'  and relation_nm in ('上海海通证券资产管理有限公司,海通证券股份有限公司')
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select distinct * from RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt from RMP_ALERT_COMPREHS_SCORE_TEMP )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.r_score_cal is not null  --有关联方
),
company_core_rel_ as 
(
	select distinct a.* from pth_rmp.rmp_company_core_rel a 
	join (select max(relation_dt) as max_relation_dt from pth_rmp.rmp_company_core_rel) b
		on a.relation_dt=b.max_relation_dt
)
-- insert into RMP_COMPY_CONTRIB_DEGREE 
select 
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	relation_id,
	relation_nm,
	relation_type_l2_line,
	contribution_degree,
	rank() over(partition by corp_id,score_dt order by contribution_degree desc) as rank_num,
	abnormal_flag,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from
(
	select 
		com.batch_dt,
		com.corp_id ,
		com.corp_nm ,
		com.score_dt,
		com.relation_id,
		com.relation_nm,
		--concat_ws(',\\r\\n',collect_set(rel.relation_type_l2)) as relation_type_l2_line,
		group_concat(CFG.rel_type_desc,'，') as relation_type_l2_line,
		com.r_score_cal as contribution_degree,
		sc.alert as abnormal_flag
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com
	left join company_core_rel_ rel 
		on com.corp_id = rel.corp_id and com.relation_id = rel.relation_id
	join pth_rmp.rmp_compy_core_rel_degree_cfg CFG 
		on rel.relation_type_l2_code=CFG.rel_type_ii_cd
	left join rmp_alert_score_summ sc
		on com.relation_id = sc.corp_id and com.score_dt=sc.score_dt and nvl(com.batch_dt,'')=nvl(sc.batch_dt,'')
	group by com.batch_dt,com.corp_id,com.corp_nm,com.corp_nm,com.score_dt,com.relation_id,com.relation_nm,com.r_score_cal,sc.alert
)Final where to_date(score_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  ;