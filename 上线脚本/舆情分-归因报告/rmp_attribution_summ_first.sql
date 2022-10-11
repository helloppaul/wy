-- 归因报告 RMP_ATTRIBUTION_SUMM_FIRST_TEMP --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and corp_nm='深圳比亚迪光电子有限公司'  
-- and score_dt='2022-08-02'  and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
-- /* 2022-9-3 首段新增特殊情形 */
--依赖 RMP_ALERT_COMPREHS_SCORE_TEMP
drop table if exists pth_rmp.RMP_ATTRIBUTION_SUMM_FIRST_TEMP;
create table if not exists pth_rmp.RMP_ATTRIBUTION_SUMM_FIRST_TEMP AS 
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select * from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.alert=1 
	  --and a.corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and to_date(a.score_dt)='2022-08-02'  --and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
),
First_ as   --主体名称
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		second_score+third_score as rel_score_summ,
		score_hit,
		label_hit,
		if(score<>0,'主体自身','') as ztzs,  
		if(second_score+third_score<>0,'关联方舆情风险','') as glf,
		-- case
			-- WHEN score_hit=1 and label_hit=0 then '相较过去14天平均水平表现异常。'
			-- when score_hit=1 and label_hit=1 then '相较过去14天平均水平表现异常，'
		-- end as score_hit_msg,
		-- CASE
			-- when score_hit=1 and label_hit=1 then '同时命中重要风险事件。'
			-- when score_hit=0 and label_hit=1 then '命中重要风险事件。'
			-- when label_hit=0 then '' 
		-- end as label_hit_msg,
		case 
			WHEN score_hit=1 and label_hit=0 then '相较过去14天平均水平表现异常。'
			WHEN score_hit=1 and label_hit=1 then '相较过去14天平均水平表现异常，同时命中重要风险事件。'
			WHEN score_hit=0 and label_hit=0 then ''
			WHEN score_hit=0 and label_hit=1 then '相较过去14天平均水平未表现异常，但命中重要风险事件。'
		end as hit_msg
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch
),
First_msg as   --主体名称
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		rel_score_summ,
		score_hit,
		label_hit,
		concat(
			'	',corp_nm,'综合舆情分触发异动预警,',ztzs,glf,
			hit_msg
		) as sentence_1_1
	from First_
)
---------------------- 以上部分为临时表 --------------------------------------------------------------------------
select 
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	score,
	rel_score_summ,
	score_hit,
	label_hit,
	sentence_1_1 as First_sentence
from First_msg
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1)) 
;
