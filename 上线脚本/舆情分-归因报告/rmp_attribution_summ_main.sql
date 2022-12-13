-- 归因报告 RMP_ATTRIBUTION_SUMM_MAIN_TEMP --
-- 入参：${ETL_DATE} -> to_date(score_dt)。入参给NULL，初始化全部数据 --
-- where corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and corp_nm='深圳比亚迪光电子有限公司'  
-- and score_dt='2022-08-02'  and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
-- /* 2022-9-3 命中重大风险事件调整为使用label_hit=1的标签进行判断 */
-- /* 2022-9-3 主体段落新增特殊情形 */  
-- /*2022-10-31 效率优化 （1）接口层做时间限制 （2）增加调优参数 */
-- /*2022-11-04 解决效率层面优化的带来的mapjoin过大导致的报错问题  */
--依赖 pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP,hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf
--/*2022-12-12 增加pth_rmp.rmp_opinion_risk_info的副本表pth_rmp.rmp_opinion_risk_info_04，供下游04组加工任务使用*/


set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


drop table if exists pth_rmp.RMP_ATTRIBUTION_SUMM_MAIN_TEMP;
create table if not exists pth_rmp.RMP_ATTRIBUTION_SUMM_MAIN_TEMP AS 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,仅主体信息
(
	select distinct 
		a.batch_dt,a.corp_id,a.corp_nm,a.score_dt,
		a.score,a.second_score,a.third_score,a.origin_comprehensive_score,a.comprehensive_score,a.score_hit,a.label_hit
	from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP group by score_dt)b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	  and a.score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	  --and a.corp_id='pz00e1c32133191ee1a9cc3556af92f8ea' and to_date(a.score_dt)='2022-08-02'  --and relation_nm in ('比亚迪股份有限公司','比亚迪汽车工业有限公司','上海比亚迪电动车有限公司')
),
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  where cid1.etl_date in (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance)
			-- on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  where b1.etl_date in (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main )
		  	-- on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
rmp_opinion_risk_info_ as 
(
	select *
	from pth_rmp.rmp_opinion_risk_info_04 --init   20221114000000
	where notice_date <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	--modify yangcan 20221115 取跑批日期当天及前一天数据
	and notice_date >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
),   
tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_ as --舆情分-贡献度占比
(
	select a.*,to_date(a.end_dt) as score_dt
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf a
	where to_date(a.end_dt) =  to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	  --modify yangcan 20221114 指标个数重复统计
	  and a.etl_date in(select max(b.etl_date)
	                    from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf b
	                   where to_date(b.end_dt) =  to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)))
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
rmp_opinion_featpct_desc_cfg_ as 
(
	select *
	from pth_rmp.rmp_opinion_featpct_desc_cfg
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_batch as 
(
	select a.*
	from tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_ a
	join (select score_dt, max(end_dt) as max_end_dt from tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_ group by score_dt) b 
		on a.score_dt=b.score_dt and a.end_dt=b.max_end_dt
),
Second_one as  --放主体归因
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		score_hit,
		label_hit,
		second_score,
		third_score,
		(score/origin_comprehensive_score) as main_contrib_degree
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch a 
),
Second_one_msg as  --放主体归因信息
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
		case
		    
			--when cast(main_contrib_degree*100 as decimal(10,2))=0 then '' modify yangcan 20221114 得分未异动，不展示贡献度
			when score_hit=0 then ''
			--else concat('异常维度为主体自身舆情风险','(','贡献度占比',cast(round(main_contrib_degree*100,0) as string),'%)','。' )	
			else concat('<span class="WEIGHT">主体自身舆情风险','(','贡献度占比',cast(round(main_contrib_degree*100,0) as string),'%)</span>','。' )	
		end as sentence_2_1
	from Second_one a
),
sentiself_feapct_intf_newest as --模型_单主体舆情分_特征贡献度(最新批次+corp_id转码后数据)
(
	select distinct *
	from 
	(
		select 
			cast(a.end_dt as string) as batch_dt,
			chg.corp_id,chg.corp_name as corp_nm,
			a.end_dt,   --原始endt_dt,带有时分秒，关联时需要去掉时分秒
			a.feature_name,  
			a.feature_importance,  --特征shap值
			c.feature_value,  --原始特征值
			a.feature_pct,  --特征贡献度
			a.feature_risk_interval,  --是否高特征贡献度（0/1,1代表高）
			count(a.feature_name) over(partition by a.corp_code,a.end_dt) as feat_cnt  --特征名称总数
		from tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_batch a
		-- tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_ a--app_ehzh.rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf a   --app_ehzh_train.featpct_senti_self
		-- join (select max(end_dt) as max_end_dt from tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_)b--app_ehzh.rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf) b
		-- 	on a.end_dt=b.max_end_dt
		join (select * from corp_chg where source_code='FI') chg
			on cast(a.corp_code as string) = chg.source_id
		join (select a.*
		        from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf a
               where to_date(a.end_dt) =  to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	             and a.etl_date in(select max(b.etl_date)
	                                 from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf b
	                                where to_date(b.end_dt) =  to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)))
			)c --app_ehzh.rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf c   --app_ehzh_train.featvalue_senti_self
		on a.corp_code=c.corp_code and a.feature_name=c.feature_name and a.end_dt=c.end_dt
	)A 
),
sentiself_feapct_intf_newest_with_desc_cfg as 
(
	select  
		A.*,
		B.index_min_val,
		B.index_max_val,
		B.index_desc,
		B.index_unit
	from sentiself_feapct_intf_newest A
	left join rmp_opinion_featpct_desc_cfg_ B
		on A.feature_name = B.feature_name 
	where B.index_min_val=-1
	  and A.feature_name<>'importance_avg_abs'
	union all 
	select  
		A.*,
		B.index_min_val,
		B.index_max_val,
		B.index_desc,
		B.index_unit
	from sentiself_feapct_intf_newest A
	left join rmp_opinion_featpct_desc_cfg_ B
		on A.feature_name = B.feature_name 
	where B.index_min_val<>-1 
	  and A.feature_name<>'importance_avg_abs' 
	  --and A.feature_pct=B.index_min_val
	  and A.feature_value=B.index_min_val
	union all 
	select  
		A.*,
		B.index_min_val,
		B.index_max_val,
		B.index_desc,
		B.index_unit
	from sentiself_feapct_intf_newest A
	left join rmp_opinion_featpct_desc_cfg_ B
		on A.feature_name = B.feature_name 
	where B.index_min_val<>-1
	  and A.feature_name='importance_avg_abs' 
	  --and A.feature_pct>=B.index_min_val and A.feature_pct<B.index_max_val
	  and A.feature_value>=B.index_min_val and A.feature_value<B.index_max_val
),
sentiself_feapct_intf_newest_with_accum AS  --处理后的特征贡献度(按照累计贡献度，大于90%累计贡献度的指标)
(
	-- select *,count(feature_name) over(partition by corp_id,end_dt) as feat_cnt_0p9 from
	-- (
		select 
			batch_dt,
			corp_id,
			corp_nm,
			end_dt,
			feature_name,
			feature_value,
			feature_importance,
			feature_pct,
			feat_cnt,
			sum(feature_pct) over(partition by corp_id,end_dt order by feature_pct desc rows between unbounded preceding and current row) 
			 as accum_feature_pct,  --累计贡献度占比
			index_desc,
			index_unit
		from sentiself_feapct_intf_newest_with_desc_cfg
	-- )A where accum_feature_pct>0.9
),
sentiself_feapct_intf_newest_with_accum_0p9 as 
(
	select a.*,count(feature_name) over(partition by a.corp_id,a.end_dt) as feat_cnt_0p9
	from sentiself_feapct_intf_newest_with_accum a
	join ( select corp_id,corp_nm,end_dt,min(accum_feature_pct) as min_0p9_accum_feature_pct 
		   from sentiself_feapct_intf_newest_with_accum  where accum_feature_pct>= 0.9
		   group by corp_id,corp_nm,end_dt
		 )b on a.corp_id=b.corp_id and a.end_dt=b.end_dt
	where a.accum_feature_pct <= b.min_0p9_accum_feature_pct and a.feature_importance>0
),
com_score_with_feapct as --带有特征贡献度的综合舆情分数据
(
	select distinct
		com.batch_dt,
		com.corp_id,
		com.corp_nm,
		com.score_dt,
		com.score_hit,
		com.label_hit,
		contrb_fea.feature_name,
		contrb_fea.feature_value,
		contrb_fea.feature_importance,
		contrb_fea.feature_pct,
		contrb_fea.feat_cnt,
		contrb_fea.accum_feature_pct,
		contrb_fea.index_desc,
		contrb_fea.index_unit,
		contrb_fea.feat_cnt_0p9
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com 
	join sentiself_feapct_intf_newest_with_accum_0p9 contrb_fea
		on com.corp_id=contrb_fea.corp_id and com.score_dt=to_date(contrb_fea.end_dt)
),
second_two as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score_hit,
		label_hit,
		feat_cnt,
		feat_cnt_0p9,
		max(accum_feature_pct) as accum_feature_pct,
		concat_ws('、',sort_array(collect_set(feat_desc))) as feat_desc_summ
		-- group_concat(distinct feat_desc,'、') as feat_desc_summ
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			score_hit,
			label_hit,
			feature_name,
			feature_importance,
			feature_value,
			--feature_pct,
			feat_cnt,
			feat_cnt_0p9,
			accum_feature_pct,
			index_desc,
			index_unit,
			concat(index_desc,
				case 
					when index_unit='%' then 
						concat(cast(cast(feature_value*100 as decimal(10,0)) as string),index_unit)
					when index_unit='条' then 
						concat(cast(feature_value as string),index_unit)
					when index_unit='p' then 
						concat(cast(feature_value as string))
					else 
						''
				end) as feat_desc  --特征信息描述		
		from com_score_with_feapct
	)A group by batch_dt,corp_id,corp_nm,score_dt,score_hit,label_hit,feat_cnt,feat_cnt_0p9
),
second_two_msg as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score_hit,
		label_hit,
		case 
			when score_hit=0 then 
				''
			else
				concat(
					'主体自身舆情分纳入的',cast(feat_cnt as string),'个指标中，',
					--cast(feat_cnt_0p9 as string),'个指标贡献舆情风险90%，','主要为一天内',  modify yangcan 20221114 贡献度取实际贡献度而不是固定90%
					cast(feat_cnt_0p9 as string),'个指标贡献舆情风险',cast(round(accum_feature_pct*100) as string),'%，主要为一天内',
					feat_desc_summ,'。'	
				) 
		end as sentence_2_2
	from second_two
),
com_score_with_risk_info AS
(
	select 
		com.corp_id,
		com.corp_nm,
		com.score_dt, 
		com.score_hit,
		com.label_hit,
		rsk.signal_type,		
		rsk.msg_id,    --企业和新闻是一对多的关系
		rsk.case_type,
		rsk.case_type_ii,
		min(rsk.importance) as importance   --新闻原始脏数据清理
	from RMP_ALERT_COMPREHS_SCORE_TEMP_Batch com
	join rmp_opinion_risk_info_ rsk
		--on com.corp_id=rsk.corp_id and to_date(com.score_dt)=to_date(rsk.notice_dt) modify yangcan 20221115
		 on com.corp_id=rsk.corp_id
		where from_unixtime(unix_timestamp(cast(rsk.notice_dt as string),'yyyy-MM-dd HH:mm:ss')) >= from_unixtime(unix_timestamp(cast(com.batch_dt as string),'yyyy-MM-dd HH:mm:ss')-24*3600)
		  and from_unixtime(unix_timestamp(cast(rsk.notice_dt as string),'yyyy-MM-dd HH:mm:ss')) <  from_unixtime(unix_timestamp(cast(com.batch_dt as string),'yyyy-MM-dd HH:mm:ss'))
	group by com.corp_id,com.corp_nm,com.score_dt,com.score_hit,com.label_hit,rsk.msg_id,rsk.case_type,rsk.case_type_ii,rsk.signal_type
),
second_three as 
(
	select 
	    corp_id,
		corp_nm,
		score_dt,
		score_hit,
		label_hit,
		max(rm) as rm,
		--concat_ws('、',sort_array(collect_set(corp_rsk_msg))) as rsk_msg
		regexp_replace(regexp_replace(regexp_replace(concat_ws('、',sort_array(collect_set(case when rm<=10 then corp_rsk_msg else null end))),'sort01',''),'sort02',''),'sort03','') as rsk_msg
		-- group_concat(distinct corp_rsk_msg,'、')  as rsk_msg
	from 
	(
		select 
			*,
			row_number() over(partition by corp_id,corp_nm,to_date(score_dt) order by importance asc) as rm,
			concat(case importance when -3 then 'sort01'
			                       when -2 then 'sort02'
								   when -1 then 'sort03'
				   end
				   ,case_type_ii,'(',importance_map,')') as corp_rsk_msg
		from 
		(
			select 
				corp_id,
				corp_nm,
				score_dt,
				score_hit,
				label_hit,
				--case_type,
				case_type_ii,
				--importance,
				--case minimportance
				--	when -3 then '严重负面'
				--	when -2 then '重要负面'
				--	when -1 then '一般负面'
				--End as importance_map
				min(importance) as importance,
				case min(importance)
					when -3 then '严重负面'
					when -2 then '重要负面'
					when -1 then '一般负面'
				End as importance_map
			from com_score_with_risk_info where signal_type=0  --仅新闻
		   group by corp_id,corp_nm,score_dt,score_hit,label_hit,case_type_ii
		) A 
	)B  group by corp_id,corp_nm,score_dt,score_hit,label_hit
),
second_three_msg as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		case 
			when score_hit=0 then 
				concat(
					'新增风险事件包括：',rsk_msg,if(rm>10,'等',''),'。'
				) 
			else
				concat(
					'具体风险事件包括：',rsk_msg,if(rm>10,'等',''),'。'	
				) 
		end as sentence_2_3
	from second_three
),
second_four as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',sort_array(collect_set(corp_nm))) as imp_risk_corp,
		-- group_concat(corp_nm,'、') as imp_risk_corp,   --命中重大风险事件的关联方
		concat_ws('、',sort_array(collect_set(tag_ii))) as imp_tag  --重大风险事件
		-- group_concat(distinct tag_ii,'、') as imp_tag   --重大风险事件
	from 
	(
		select distinct
			com_rsk.corp_id,
			com_rsk.corp_nm,
			com_rsk.score_dt,
			com_rsk.case_type_ii as tag_ii
			-- tag.tag_ii
		from com_score_with_risk_info com_rsk 
		join (select * from pth_rmp.rmp_opinion_risk_info_tag where importance=-3) tag
			 on com_rsk.case_type_ii = tag.tag_ii
		where com_rsk.label_hit=1
	)A group by corp_id,corp_nm,score_dt
),
second_four_msg as 
(
	select  
		corp_id,
		corp_nm,
		score_dt,
		if(imp_risk_corp<>'',concat('同时，',imp_risk_corp,'命中了重要风险事件，','主要为',imp_tag,'。'),'')
		  as sentence_2_4
	from second_four
),
second_msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,a.score_dt,
		a.score,
		a.second_score+a.third_score as rel_score_summ,
		a.score_hit,
		a.label_hit,
		concat(sentence_2_1,sentence_2_2,sentence_2_3,sentence_2_4) as sentence_2
	FROM
	(
		select 
			a.batch_dt,
			a.corp_id,
			a.corp_nm,a.score_dt,
			a.score,
			a.second_score,
			a.third_score,
			a.sentence_2_1,
			a.score_hit,
			a.label_hit,
			nvl(b.sentence_2_2,'') as sentence_2_2,
			nvl(c.sentence_2_3,'') as sentence_2_3,
			nvl(d.sentence_2_4,'') as sentence_2_4
		from Second_one_msg a 
		left join Second_two_msg b
			on a.corp_id=b.corp_id and a.score_dt = b.score_dt
		left join Second_three_msg c 
			on a.corp_id=c.corp_id and a.score_dt=c.score_dt 
		left join Second_four_msg d
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
	rel_score_summ,  --关联方 second_score+third_score
	score_hit,
	label_hit,
	CASE
		when score=0 THEN   --主体段落部分不显示
			''
		when score_hit=1 and score>0 and score>=rel_score_summ then 
			concat('	其中，主要异常维度为',sentence_2)
		when score_hit=1 and score>0 and score<rel_score_summ then 
			concat('	其次为',sentence_2)
		when score_hit=0 and score>0 then 
			concat('	主体层面，',sentence_2)
		-- when score_hit=0 and score>0 and score>rel_score_summ then 
			-- concat('	其中主体层面，',sentence_2)
		-- when score_hit=0 and score>0 and score<rel_score_summ then 
			-- concat('	其次主体层面，',sentence_2)
	end as Main_sentence
from second_msg
where to_date(score_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;
