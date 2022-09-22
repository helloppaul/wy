-- 归因报告 RMP_ATTRIBUTION_SUMM_LAST_TEMP --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and corp_nm='深圳比亚迪光电子有限公司'  
-- and score_dt='2022-08-02'  and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
--依赖 pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP，pth_rmp.rmp_opinion_risk_info
drop table if exists pth_rmp.RMP_ATTRIBUTION_SUMM_LAST_TEMP;
create table if not exists pth_rmp.RMP_ATTRIBUTION_SUMM_LAST_TEMP as 
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Main as  --最新批次的综合舆情分数据,仅主体信息
(
	select distinct 
		batch_dt,corp_id,corp_nm,score_dt,cast(score as float) as score,
		second_score,third_score,comprehensive_score 
	from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.alert=1 --and to_date(a.score_dt)='2022-08-03' 
	  --and a.corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and to_date(a.score_dt)='2022-08-02'  --and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
),
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Rel as  --最新批次的综合舆情分数据,且有关联方
(
	select 
		batch_dt,corp_id,corp_nm,score_dt,cast(score as float) as score,
		relation_id,relation_nm,
		second_score,third_score,comprehensive_score,
		score_hit,label_hit,alert
	from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.alert=1 
	  --and a.corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and to_date(a.score_dt)='2022-08-02'  --and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
),
com_score_with_risk_info_main AS   --主体段落的带风险事件的综合舆情分数据
(
	select 
		com.corp_id,
		com.corp_nm,
		com.score_dt, 
		rsk.signal_type,		
		rsk.msg_id,    --企业和新闻是一对多的关系
		rsk.case_type,
		rsk.case_type_ii,
		min(rsk.importance) as importance   --新闻原始脏数据清理
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Main com
	join pth_rmp.rmp_opinion_risk_info rsk
		on com.corp_id=rsk.corp_id and to_date(com.score_dt)=to_date(rsk.notice_dt)
	group by com.corp_id,com.corp_nm,com.score_dt,rsk.msg_id,rsk.case_type,rsk.case_type_ii,rsk.signal_type
),
com_score_with_risk_info_rel AS  --关联方段落的带风险事件的综合舆情分数据
(
	select 
		com.corp_id,
		com.corp_nm,
		com.score_dt,
		com.relation_id,
		com.relation_nm,  
		rsk.signal_type,		
		rsk.msg_id,    --企业和新闻是一对多的关系
		rsk.case_type,
		rsk.case_type_ii,
		min(rsk.importance) as importance   --新闻原始脏数据清理
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Rel com
	join pth_rmp.rmp_opinion_risk_info rsk
		on com.relation_id=rsk.corp_id and to_date(com.score_dt)=to_date(rsk.notice_dt)
	group by com.corp_id,com.corp_nm,com.score_dt,com.relation_id,com.relation_nm, rsk.msg_id,rsk.case_type,rsk.case_type_ii,rsk.signal_type
),
main_rsk_info as 
(
	select * from 
	(
		select *,row_number() over(partition by corp_id,score_dt order by 1) as rm
		from 
		(	
			select distinct
				com_rsk.corp_id,
				com_rsk.corp_nm,
				com_rsk.score_dt,
				com_rsk.signal_type,
				com_rsk.case_type,
				com_rsk.case_type_ii
			from com_score_with_risk_info_main com_rsk 
			where com_rsk.signal_type=0
			union all 
			select distinct
				com_rsk.corp_id,
				com_rsk.corp_nm,
				com_rsk.score_dt,
				com_rsk.signal_type,
				com_rsk.case_type,
				com_rsk.case_type_ii
			from com_score_with_risk_info_main com_rsk 
			join (select * from pth_rmp.rmp_opinion_risk_info_tag where importance=-3) tag
				on com_rsk.case_type_ii = tag.tag_ii
			where com_rsk.signal_type in (1,2)
		)A  
	)B where rm<=5
),
last_for_main_rsk_info_msg as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',collect_set(case_type)) as last_sentence_main_case_type
		-- group_concat(distinct case_type,'、') as last_sentence_main_case_type
	from com_score_with_risk_info_main
	group by corp_id,corp_nm,score_dt
),
rel as  --关联方列举
(
	select *
	from 
	(
		select *,row_number() over(partition by corp_id,score_dt order by 1) as rm
		FROM
		(
			select distinct
				corp_id,
				corp_nm,
				score_dt,
				relation_id,
				relation_nm
			from com_score_with_risk_info_rel
		)A 
	)B where rm<=3  --限制关联方数量最多显示3个
),
last_for_rel_msg as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		case 
			when rm>3 then concat(rel_msg,'等')
			else rel_msg
		end as last_sentence_rel
	from 
	(
		select 
			corp_id,
			corp_nm,
			score_dt,
			concat_ws('、',collect_set(relation_nm)) as rel_msg,
			-- group_concat(distinct relation_nm,'、') as rel_msg,
			rm
		from rel
		group by corp_id,corp_nm,score_dt,rm
	)A  
),
rel_rsk_info as   --关联方风险事件(新闻+司法诚信)
(
	select * from 
	(
		select *,row_number() over(partition by corp_id,score_dt,relation_id order by 1) as rm
		from 
		(	--新闻部分
			select distinct
				com_rsk.corp_id,
				com_rsk.corp_nm,
				com_rsk.score_dt, 
				com_rsk.relation_id,
				com_rsk.relation_nm,
				com_rsk.case_type,
				com_rsk.case_type_ii
			from com_score_with_risk_info_rel com_rsk 
			where com_rsk.signal_type=0
			union all 
			--司法诚信部分
			select distinct
				com_rsk.corp_id,
				com_rsk.corp_nm,
				com_rsk.score_dt, 
				com_rsk.relation_id,
				com_rsk.relation_nm,
				com_rsk.case_type,
				com_rsk.case_type_ii
			from com_score_with_risk_info_rel com_rsk 
			join (select * from pth_rmp.rmp_opinion_risk_info_tag where importance=-3) tag
				on com_rsk.case_type_ii = tag.tag_ii
			where com_rsk.signal_type in (1,2)
		)A
	)B where rm<=5  --仅展示5个风险事件标签
),
last_for_rel_rsk_info_msg as   --关联方风险事件(新闻+司法诚信)
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',collect_set(case_type)) as last_sentence_rel_case_type
		-- group_concat(distinct case_type,'、') as last_sentence_rel_case_type
	from rel_rsk_info
	group by corp_id,corp_nm,score_dt
),
last_msg as 
(
	select
		com.batch_dt,
		com.corp_id,
		com.corp_nm,
		com.score_dt,
		com.score,
		(com.second_score+com.third_score) as rel_score_summ,
		concat('综合考虑主体及其关联方风险情况，建议重点关注',
			if(com.score>0,concat('主体自身的',main_msg.last_sentence_main_case_type,'。'),''),
			case 
				when com.score>0 and (com.second_score+com.third_score)>0 then 
					concat('同时也需关注',rel_msg.last_sentence_rel,'的',rel_rsk_info_msg.last_sentence_rel_case_type,'所产生的传导影响。' )
				when com.score>0 and (com.second_score+com.third_score)=0 then 
					''
				when com.score=0 then 
					concat(rel_msg.last_sentence_rel,'的',rel_rsk_info_msg.last_sentence_rel_case_type,'所产生的传导影响。' )
			end
		) as last_sentence
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Main com
	left join last_for_main_rsk_info_msg main_msg
		on com.corp_id=main_msg.corp_id and com.score_dt=main_msg.score_dt
	left join last_for_rel_msg rel_msg
		on com.corp_id=rel_msg.corp_id and com.score_dt=rel_msg.score_dt
	left join last_for_rel_rsk_info_msg rel_rsk_info_msg
		on com.corp_id=rel_rsk_info_msg.corp_id and com.score_dt=rel_rsk_info_msg.score_dt
)
---------------------- 以上部分为临时表 --------------------------------------------------------------------------
select 
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	score,
	rel_score_summ,
	last_sentence
from last_msg
where to_date(score_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd'))) 
;

