-- 归因报告 ATTRIBUTION_SUMM --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and corp_nm='深圳比亚迪光电子有限公司'  
-- and score_dt='2022-08-02'  and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select * from RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt from RMP_ALERT_COMPREHS_SCORE_TEMP )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.alert=1 
	  and a.corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and to_date(a.score_dt)='2022-08-02'  --and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
),
First_ as   --主体名称
(
	select distinct
		corp_id,
		corp_nm,
		score_dt,
		if(score<>0,'主体自身','') as ztzs,  
		if(second_score+third_score<>0,'关联方舆情风险','') as glf,
		case
			WHEN score_hit=1 and label_hit=0 then '相较过去14天平均水平表现异常。'
			when score_hit=1 and label_hit=1 then '相较过去14天平均水平表现异常，'
		end as score_hit_msg,
		CASE
			when score_hit=1 and label_hit=1 then '同时命中重要风险事件。'
			when score_hit=0 and label_hit=1 then '命中重要风险事件。'
			when label_hit=0 then '' 
		end as label_hit_msg
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch
),
First_msg as   --主体名称
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			corp_nm,',','当前综合舆情分触发异动预警,',ztzs,glf,',',
			score_hit_msg,label_hit_msg
		) as sentence_1_1
	from First_
)
select 
	* 
from First_msg;
