-- 贡献度排行榜 RMP_COMPY_CONTRIB_DEGREE --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_nm='海通创新证券投资有限公司'  and score_dt='2022-07-03'  and relation_nm in ('上海海通证券资产管理有限公司,海通证券股份有限公司')
-- /* 2022-9-20 rank_num 调整为row_number()排序 */
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select distinct a.* from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP group by score_dt)b  
		on a.batch_dt = b.new_batch_dt and a.score_dt=b.score_dt
	where a.r_score_cal is not null  --有关联方
),
rmp_alert_score_summ_ as 
(
	select 
		nvl(a.batch_dt,'') as batch_dt, 
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
		a.model_version
	from pth_rmp.rmp_alert_score_summ a --pth_rmp.rmp_alert_score_summ
	where delete_flag=0
),
rmp_alert_score_summ_batch_ as --最新批次的单舆情分数据
(
	select a.* from rmp_alert_score_summ_ a 
	join (select max(batch_dt) as new_batch_dt,score_dt from rmp_alert_score_summ_ group by score_dt)b  
		on a.batch_dt=b.new_batch_dt and a.score_dt=b.score_dt
),
company_core_rel_ as 
(
	select distinct a.* 
	from pth_rmp.RMP_COMPANY_CORE_REL a 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and a.relation_dt in (select max(relation_dt) max_relation_dt from pth_rmp.RMP_COMPANY_CORE_REL)

)
insert into pth_rmp.RMP_COMPY_CONTRIB_DEGREE 
select 
	md5(concat(batch_dt,corp_id,relation_id)) as sid_kw,
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	relation_id,
	relation_nm,
	relation_type_l2_line,
	contribution_degree,
	ROW_NUMBER() over(partition by corp_id,score_dt order by contribution_degree desc) as rank_num,
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
		concat_ws(',\\r\\n',collect_set(rel.relation_type_l2)) as relation_type_l2_line,
		-- group_concat(CFG.rel_type_desc,'，') as relation_type_l2_line,
		com.r_score_cal as contribution_degree,
		sc.alert as abnormal_flag
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com
	left join company_core_rel_ rel 
		on com.corp_id = rel.corp_id and com.relation_id = rel.relation_id
	join pth_rmp.rmp_compy_core_rel_degree_cfg CFG 
		on rel.relation_type_l2_code=CFG.rel_type_ii_cd
	left join rmp_alert_score_summ_batch_ sc
		on com.relation_id = sc.corp_id and to_date(com.score_dt)=to_date(sc.score_dt) --and nvl(com.batch_dt,'')=nvl(sc.batch_dt,'')
	group by com.batch_dt,com.corp_id,com.corp_nm,com.corp_nm,com.score_dt,com.relation_id,com.relation_nm,com.r_score_cal,sc.alert
)Final 
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1))
;