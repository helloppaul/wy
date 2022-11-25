-- 综合舆情分 RMP_ALERT_COMPREHS_SCORE (同步方式：一天多批次插入)--
-- with RMP_ALERT_COMPREHS_SCORE_TEMP_ as 
-- (
-- 	select * from 
-- )

set hive.exec.parallel=true;
set hive.auto.convert.join=ture; 

 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with
RMP_ALERT_COMPREHS_SCORE_TEMP_BATCH as 
(
	select distinct
		a.batch_dt,
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
		a.adjust_warnlevel
	from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a
	 join (select max(batch_dt) as max_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP  group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt 
	where a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
RMP_ALERT_COMPREHS_SCORE_TEMP_RESULT as
(
	select 
		a.batch_dt,
		a.corp_id,
		max(a.corp_nm) as corp_nm,
		max(a.credit_code) as credit_code,
		a.score_dt,
		max(a.comprehensive_score) as comprehensive_score,
		max(a.score_hit) as score_hit,
		max(a.label_hit) as label_hit,
		max(a.alert) as alert,
		max(a.fluctuated) as fluctuated,
		max(a.model_version) as model_version,
		case
			when max(b.alert)>0 then 
				'-3' 	--高风险 (一个月有异动)
			else '-2'    --中风险 (一个月内无异动)
		end as adjust_warnlevel,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
	from RMP_ALERT_COMPREHS_SCORE_TEMP_BATCH a 
	join RMP_ALERT_COMPREHS_SCORE_TEMP_BATCH b 
		on a.corp_id=b.corp_id
	where b.score_dt <= a.score_dt
	  and b.score_dt > date_add(a.score_dt,-30)
	group by a.batch_dt,a.corp_id,a.score_dt
)
insert into pth_rmp.RMP_ALERT_COMPREHS_SCORE partition(etl_date=${ETL_DATE})
select 
	md5(concat(to_date(a.batch_dt),nvl(a.corp_id,''),'0')) as sid_kw,
	a.*
from RMP_ALERT_COMPREHS_SCORE_TEMP_RESULT a
-- where a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;
-- select distinct
-- 	--a.sid_kw,modify yangcan 20221113 sid_kw是由corp_id,relation_id拼接后转换而来,此处distinct去重没有效果
-- 	md5(concat(to_date(a.batch_dt),nvl(a.corp_id,''),'0')) as sid_kw,
-- 	a.batch_dt,
-- 	a.corp_id,
-- 	a.corp_nm,
-- 	a.credit_code,
-- 	a.score_dt,
-- 	a.comprehensive_score,
-- 	a.score_hit,
-- 	a.label_hit,
-- 	a.alert,
-- 	a.fluctuated,
-- 	a.model_version,
-- 	a.adjust_warnlevel,
-- 	0 as delete_flag,
-- 	'' as create_by,
-- 	current_timestamp() as create_time,
-- 	'' as update_by,
-- 	current_timestamp() update_time,
-- 	0 as version
-- from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a
-- join (select max(batch_dt) as max_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP group by score_dt) b
-- 	on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt


;

