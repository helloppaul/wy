-- 归因报告 RMP_ATTRIBUTION_SUMM_REL_TEMP --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and corp_nm='深圳比亚迪光电子有限公司'  
-- and score_dt='2022-08-02'  and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
-- /* 2022-9-3 命中重大风险事件调整为使用label_hit=2的标签进行判断 */
-- /* 2022-9-3 关联方段落新增特殊情形 */
-- /*2022-11-04 解决效率层面优化的带来的mapjoin过大导致的报错问题  */
--2.主体label_hit=1，则显示命中重大风险事件 ；关联方 label_hit=2，则显示命中重大风险事件 
--3.排序问题，hive可使用sort_array()进行升序排序，解决impala无法排序拼接的问题
--依赖 pth_rmp.RMP_COMPY_CONTRIB_DEGREE pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP  pth_rmp.rmp_opinion_risk_info

set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.RMP_ATTRIBUTION_SUMM_REL_TEMP;
create table if not exists pth_rmp.RMP_ATTRIBUTION_SUMM_REL_TEMP AS 
with 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select a.* from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP group by score_dt )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	  and a.score_dt= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
RMP_COMPY_CONTRIB_DEGREE_ as 
(
	select *
	from pth_rmp.RMP_COMPY_CONTRIB_DEGREE
	where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
rmp_opinion_risk_info_ as 
(
	select *
	from pth_rmp.rmp_opinion_risk_info 
	where notice_date = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
RMP_COMPY_CONTRIB_DEGREE_BATCH as 
(
	select a.*
	from RMP_COMPY_CONTRIB_DEGREE_ a
	join (select score_dt, max(batch_dt) as max_batch_dt from RMP_COMPY_CONTRIB_DEGREE_ group by score_dt) b 
		on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
),
com_score_with_contrib_degree as 
(
	select distinct
		com.batch_dt,
		com.corp_id,
		com.corp_nm,
		com.score_dt,
		com.score,
		com.relation_id,
		com.relation_nm,
		--contrb.relation_type_l1,  --标签
		--contrb.relation_type_l2,
		contrb.relation_type_l2_line as relation_tag,
		--concat(contrb.relation_type_l1,'_',contrb.relation_type_l2) as relation_tag,  --use
		com.r_score,  --每个关联方的舆情分
		com.r,
		com.r_score_cal, --计算后的每个关联方舆情分 
		com.second_score,
		com.third_score,
		com.origin_comprehensive_score,
		com.comprehensive_score	,
		com.score_hit,
		com.label_hit
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com 
	join RMP_COMPY_CONTRIB_DEGREE_BATCH contrb
		on com.corp_id=contrb.corp_id and to_date(com.score_dt)=to_date(contrb.score_dt) and com.relation_id=contrb.relation_id
),
Third_one as  --放关联方归因
(
	select 
		'关联方舆情风险' as fix1,
		batch_dt,
		second_score,
		third_score,
		score,
		(max(second_score)+max(third_score))/max(origin_comprehensive_score) as rel_contrib_degree,  --关联方贡献度
		corp_id,
		corp_nm,
		score_dt,
		score_hit,
		label_hit,
		if(third_score>second_score, max_rel_contrib_corp_nm,NULL) AS max_rel_contrib_corp_nm,
		rel_cnt,
		concat_ws('、',sort_array(collect_set(concat(relation_nm,'(',relation_tag,')')))) as rel_msg
		-- group_concat(concat(relation_nm,'(',relation_tag,')'),'、') as rel_msg
	from 
	(
		select *,row_number() over(partition by corp_id,score_dt order by 1) as rm
		from 
		(
			select 
				batch_dt,
				score,
				second_score,
				third_score,
				origin_comprehensive_score,
				comprehensive_score,
				corp_id,
				corp_nm,
				score_dt,
				score_hit,
				label_hit,
				count(relation_id) over(partition by corp_id,score_dt) as rel_cnt, 
				first_value(relation_nm) over(partition by corp_id,corp_nm order by r_score_cal desc rows between unbounded preceding and unbounded following) 
				 as max_rel_contrib_corp_nm,
				relation_id,
				relation_nm,
				--row_number() over(partition by corp_id,score_dt order by 1) as rm,
				relation_tag
			from com_score_with_contrib_degree
		) B 
	)A where rm <=5
	   group by batch_dt,corp_id,corp_nm,score_dt,score_hit,label_hit,rel_cnt,max_rel_contrib_corp_nm,score,second_score,third_score,comprehensive_score
),
Third_one_msg as 
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		second_score,
		third_score,
		score_hit,
		label_hit,
		if(score_hit=0,'',
			case 
				when round(rel_contrib_degree*100,0)=0 then ''
				else 
					concat(
						'关联方舆情风险','(','贡献度占比',cast(round(rel_contrib_degree*100,0) as string),'%',')。',
						corp_nm,'共',cast(rel_cnt as string),'个','关联方贡献风险，','分别为',rel_msg,'。',
						if(max_rel_contrib_corp_nm is null,'',concat('其中',max_rel_contrib_corp_nm,'为主要的舆情风险贡献来源。') )
					)					-- concat(
						-- '异常维度为关联方舆情风险','(','贡献度占比',cast(round(rel_contrib_degree*100,0) as string),'%',')。',
						-- corp_nm,'共',cast(rel_cnt as string),'个','关联方贡献风险，','分别为',rel_msg,'。',
						-- if(max_rel_contrib_corp_nm is null,'',concat('其中',max_rel_contrib_corp_nm,'为主要的舆情风险贡献来源。') )
					-- )
			end
		) as sentence_3_1
	from Third_one
),
com_score_with_risk_info AS
(
	select 
		com.corp_id,
		com.corp_nm,
		com.score_dt,
		com.label_hit,
		com.relation_id,
		com.relation_nm,  
		rsk.signal_type,		
		rsk.msg_id,    --企业和新闻是一对多的关系
		rsk.case_type,
		rsk.case_type_ii,
		min(rsk.importance) as importance   --新闻原始脏数据清理
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com
	join rmp_opinion_risk_info_ rsk
		on com.relation_id=rsk.corp_id and to_date(com.score_dt)=to_date(rsk.notice_dt)
	group by com.corp_id,com.corp_nm,com.score_dt,com.label_hit,com.relation_id,com.relation_nm, rsk.msg_id,rsk.case_type,rsk.case_type_ii,rsk.signal_type
),
Third_two as  --风险事件的描述(仅新闻)
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		risk_cnt,
		concat_ws('、',sort_array(collect_set(tmp_risk_imp_msg))) as risk_imp_msg
		-- group_concat(tmp_risk_imp_msg,'、')  as risk_imp_msg
	from 
	(
		select 
			corp_id,
			corp_nm,
			score_dt,
			risk_cnt,
			importance,
			concat(importance_map,cast(risk_imp_cnt as string),'条') as tmp_risk_imp_msg
			--废弃 concat_ws('、',sort_array(collect_set(concat(importance_map,cast(risk_imp_cnt as string),'条')))) as risk_imp_msg 废弃
			--废弃 group_concat(concat(importance_map,cast(risk_imp_cnt as string),'条'),'、')  as risk_imp_msg 废弃
		from 
		(	
			select distinct *
			from
			(
				select 
					corp_id,
					corp_nm,
					score_dt,
					importance,
					case importance
						when -3 then '严重负面'
						when -2 then '重要负面'
						when -1 then '一般负面'
					End as importance_map,
					count(msg_id)over(partition by corp_id,corp_nm,to_date(score_dt)) as risk_cnt,
					count(msg_id)over(partition by corp_id,corp_nm,to_date(score_dt),importance order by importance asc) as risk_imp_cnt
				FROM com_score_with_risk_info 
			)B
		)A-- group by corp_id,corp_nm,score_dt,risk_cnt 
	)C group by corp_id,corp_nm,score_dt,risk_cnt
),
Third_two_msg as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			'新增风险事件数量',cast(risk_cnt as string),'条，',
			'其中',risk_imp_msg,'。'
		) as sentence_3_2
	from Third_two
),
Third_three AS  
(	
	select 
	    corp_id,
		corp_nm,
		score_dt,
		max(rm) as rm,
		concat_ws('、',sort_array(collect_set(rel_rsk_msg))) as rsk_msg
		-- group_concat(distinct rel_rsk_msg,'、')  as rsk_msg

	from 
	(
		select 
			*,
			row_number() over(partition by corp_id,corp_nm,to_date(score_dt) order by importance asc) as rm,
			concat(case_type_ii,'(',importance_map,')') rel_rsk_msg
		from 
		(
			select
				corp_id,
				corp_nm,
				score_dt,
				relation_id,
				relation_nm,
				case_type_ii,
				importance,
				case importance
					when -3 then '严重负面'
					when -2 then '重要负面'
					when -1 then '一般负面'
				End as importance_map
			from com_score_with_risk_info where signal_type=0  --仅新闻
		) A order by importance asc
	)B where rm<=10 group by corp_id,corp_nm,score_dt
),
Third_three_msg as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			'具体风险事件包括：',rsk_msg,if(rm>10,'等',''),'。'
		) as sentence_3_3
	from Third_three
),
Third_four as --重要风险事件
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',sort_array(collect_set(relation_nm))) as imp_risk_rel,
		-- group_concat(relation_nm,'、') as imp_risk_rel,   --命中重大风险事件的关联方
		concat_ws('、',sort_array(collect_set(tag_ii))) as imp_tag
		-- group_concat(distinct tag_ii,'、') as imp_tag   --重大风险事件
	from 
	(	select *,row_number() over(partition by corp_id,score_dt order by 1) as rm
		FROM
		(
			select distinct 
				com_rsk.corp_id,
				com_rsk.corp_nm,
				com_rsk.score_dt,
				com_rsk.relation_id,
				com_rsk.relation_nm,
				com_rsk.case_type_ii as tag_ii
				-- tag.tag_ii
			from com_score_with_risk_info com_rsk
			where com_rsk.label_hit=2
			-- join (select * from pth_rmp.rmp_opinion_risk_info_tag where importance=-3) tag
				-- on com_rsk.case_type_ii = tag.tag_ii
		)B 
	)A where rm<=5
	   group by corp_id,corp_nm,score_dt
	
),
Third_four_msg as 
(
	select  
		corp_id,
		corp_nm,
		score_dt,
		if(imp_risk_rel<>'',concat('同时，',imp_risk_rel,'命中了重要风险事件，','主要为',imp_tag,'。'),'')
		  as sentence_3_4
	from Third_four
),
Third_msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,a.score_dt,
		a.score,
		a.second_score+a.third_score as rel_score_summ,
		a.score_hit,
		a.label_hit,
		concat(sentence_3_1,sentence_3_2,sentence_3_3,sentence_3_4) as sentence_3
	FROM
	(
		select 
			a.batch_dt,
			a.corp_id,
			a.corp_nm,a.score_dt,
			a.second_score,
			a.third_score,
			a.score,
			a.score_hit,
			a.label_hit,
			a.sentence_3_1,
			nvl(b.sentence_3_2,'') as sentence_3_2,
			nvl(c.sentence_3_3,'') as sentence_3_3,
			nvl(d.sentence_3_4,'') as sentence_3_4
		from Third_one_msg a 
		left join Third_two_msg b
			on a.corp_id=b.corp_id and a.score_dt = b.score_dt
		left join Third_three_msg c 
			on a.corp_id=c.corp_id and a.score_dt=c.score_dt 
		left join Third_four_msg d
			on a.corp_id=d.corp_id and a.score_dt=d.score_dt
	)A
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
	CASE
		when rel_score_summ=0 THEN   --关联方段落部分不显示
			''	
		when score_hit=1 and rel_score_summ>0  and rel_score_summ>score THEN
			concat('	其中，主要异常维度为',sentence_3)
		when score_hit=1 and rel_score_summ>0  and rel_score_summ<score THEN
			concat('	其次为',sentence_3)
		when score_hit=0 and rel_score_summ>0 THEN 
			concat('	关联方层面，',sentence_3)
		-- when score_hit=0 and rel_score_summ>0  and rel_score_summ>score THEN 
			-- concat('	其中关联方层面，',sentence_3)
		-- when score_hit=0 and rel_score_summ>0  and rel_score_summ<score THEN
			-- concat('	其次关联方层面，',sentence_3)
	end as rel_sentence
from Third_msg
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
;