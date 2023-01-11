-- 贡献度排行榜 RMP_COMPY_CONTRIB_DEGREE --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_nm='海通创新证券投资有限公司'  and score_dt='2022-07-03'  and relation_nm in ('上海海通证券资产管理有限公司,海通证券股份有限公司')
-- /* 2022-9-20 rank_num 调整为row_number()排序 */
-- /* 2023-01-06 增加 参数优化，且对rmp_alert_score_summ_和RMP_ALERT_COMPREHS_SCORE_TEMP_Batch 取对应日期 */
-- /* 2023-01-06 代码效率优化，注意 where etl_date的使用方法需要和初始化代码区别，初始化代码不能直接用etl_date>= <= */


set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8; 
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select distinct a.* from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt,score_dt,max(update_time) as max_update_time from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP 
		  where etl_date=${ETL_DATE}
		  group by score_dt
		 )b  
		on a.batch_dt = b.new_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
	where a.r_score_cal is not null  --有关联方
	  and a.etl_date=${ETL_DATE}
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
	  and etl_date=${ETL_DATE}
	--   and to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
rmp_alert_score_summ_batch_ as --最新批次的单舆情分数据
(
	select a.* from rmp_alert_score_summ_ a 
	join (select max(batch_dt) as new_batch_dt,score_dt from rmp_alert_score_summ_ group by score_dt)b  
		on a.batch_dt=b.new_batch_dt and a.score_dt=b.score_dt
),
company_core_rel_ as 
(
	select distinct gd.* 
	from pth_rmp.RMP_COMPANY_CORE_REL gd
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and gd.etl_date in (select max(etl_date) max_etl_date from pth_rmp.RMP_COMPANY_CORE_REL where type_='gd')
	  and type_='gd'
		-- on a.relation_dt=b.max_relation_dt
   union all 
   select distinct dwtz.* 
	from pth_rmp.RMP_COMPANY_CORE_REL dwtz 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and dwtz.etl_date in (select max(etl_date) max_etl_date from pth_rmp.RMP_COMPANY_CORE_REL where type_='dwtz')
	  and type_='dwtz'
	union all 
   select distinct skr.* 
	from pth_rmp.RMP_COMPANY_CORE_REL skr 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and skr.etl_date in (select max(etl_date) max_etl_date from pth_rmp.RMP_COMPANY_CORE_REL where type_='skr')
	  and type_='skr'
		union all 
   select distinct ssfz.* 
	from pth_rmp.RMP_COMPANY_CORE_REL ssfz 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and ssfz.etl_date in (select max(etl_date) max_etl_date from pth_rmp.RMP_COMPANY_CORE_REL where type_='ssfz')
	  and type_='ssfz'
		union all
	select distinct xtskr.* 
	from pth_rmp.RMP_COMPANY_CORE_REL xtskr 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and xtskr.etl_date in (select max(etl_date) max_etl_date from pth_rmp.RMP_COMPANY_CORE_REL where type_='xtskr')
	  and type_='xtskr'

)
insert into pth_rmp.RMP_COMPY_CONTRIB_DEGREE partition(etl_date=${ETL_DATE})
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
		concat_ws('，',collect_set(CFG.rel_type_desc)) as relation_type_l2_line,
		-- group_concat(distinct CFG.rel_type_desc,'，') as relation_type_l2_line,
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
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;