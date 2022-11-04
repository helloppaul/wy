--（1）DDL rmp_attribution_summ_init hive执行 --
-- 归因报告历史 rmp_attribution_summ_init --
drop table if exists pth_rmp.rmp_attribution_summ_init;
create table pth_rmp.rmp_attribution_summ_init
(
	sid_kw string,
	corp_id string,
	corp_nm string,
	credit_cd string,
	score_dt timestamp,
	report_msg1 string,
	report_msg2 string,
	report_msg5 string,
	delete_flag TINYINT,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



--（2）sql初始化 详报第一段 impala执行--
drop table if exists pth_rmp.rmp_attribution_summ_first_temp_init_impala;
create table if not exists pth_rmp.rmp_attribution_summ_first_temp_init_impala AS 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select a.* from pth_rmp.rmp_alert_comprehs_score_temp_init a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.rmp_alert_comprehs_score_temp_init group by score_dt)b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	--   and a.score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
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
where to_date(score_dt)>= '2022-09-09'
  and to_date(score_dt)<= '2022-10-14'
;

--（3）sql初始化 详报第二段(主体) impala执行 --
drop table if exists pth_rmp.rmp_attribution_summ_main_temp_init_impala;
create table  pth_rmp.rmp_attribution_summ_main_temp_init_impala AS 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,仅主体信息
(
	select distinct 
		a.batch_dt,a.corp_id,a.corp_nm,a.score_dt,
		a.score,a.second_score,a.third_score,a.origin_comprehensive_score,a.comprehensive_score,a.score_hit,a.label_hit
	from pth_rmp.rmp_alert_comprehs_score_temp_init a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.rmp_alert_comprehs_score_temp_init group by score_dt)b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	--   and a.score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
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
	from pth_rmp.rmp_opinion_risk_info_init
	where notice_date >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf_ as --舆情分-贡献度占比
(
	select *,to_date(end_dt) as score_dt
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_intp_sentiself_feapct_intf
	where to_date(end_dt) >=  to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
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
			when cast(main_contrib_degree*100 as decimal(10,2))=0 then ''
			--else concat('异常维度为主体自身舆情风险','(','贡献度占比',cast(round(main_contrib_degree*100,0) as string),'%)','。' )	
			else concat('主体自身舆情风险','(','贡献度占比',cast(round(main_contrib_degree*100,0) as string),'%)','。' )	
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
		join hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf c --app_ehzh.rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf c   --app_ehzh_train.featvalue_senti_self
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
	  and A.feature_pct=B.index_min_val
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
	  and A.feature_pct>=B.index_min_val and A.feature_pct<B.index_max_val
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
		-- concat_ws('、',sort_array(collect_set(feat_desc))) as feat_desc_summ
		group_concat(distinct feat_desc,'、') as feat_desc_summ
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
					cast(feat_cnt_0p9 as string),'个指标贡献舆情风险90%，','主要为一天内',
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
		on com.corp_id=rsk.corp_id and to_date(com.score_dt)=to_date(rsk.notice_dt)
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
		-- concat_ws('、',sort_array(collect_set(corp_rsk_msg))) as rsk_msg
		group_concat(distinct corp_rsk_msg,'、')  as rsk_msg
	from 
	(
		select 
			*,
			row_number() over(partition by corp_id,corp_nm,to_date(score_dt) order by importance asc) as rm,
			concat(case_type_ii,'(',importance_map,')') as corp_rsk_msg
		from 
		(
			select distinct
				corp_id,
				corp_nm,
				score_dt,
				score_hit,
				label_hit,
				case_type,
				case_type_ii,
				importance,
				case importance
					when -3 then '严重负面'
					when -2 then '重要负面'
					when -1 then '一般负面'
				End as importance_map
			from com_score_with_risk_info where signal_type=0  --仅新闻
		) A 
	)B where rm<=10 group by corp_id,corp_nm,score_dt,score_hit,label_hit
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
		-- concat_ws('、',sort_array(collect_set(corp_nm))) as imp_risk_corp,
		group_concat(corp_nm,'、') as imp_risk_corp,   --命中重大风险事件的关联方
		-- concat_ws('、',sort_array(collect_set(tag_ii))) as imp_tag  --重大风险事件
		group_concat(distinct tag_ii,'、') as imp_tag   --重大风险事件
	from 
	(
		select distinct
			com_rsk.corp_id,
			com_rsk.corp_nm,
			com_rsk.score_dt,
			com_rsk.case_type_ii as tag_ii
			-- tag.tag_ii
		from com_score_with_risk_info com_rsk 
		where com_rsk.label_hit=1
		-- join (select * from pth_rmp.rmp_opinion_risk_info_tag where importance=-3) tag
			-- on com_rsk.case_type_ii = tag.tag_ii
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
		when score_hit=1 and score>0 and score>rel_score_summ then 
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
where to_date(score_dt)>= '2022-09-09'
  and to_date(score_dt)<= '2022-10-14'
;



--（4）sql初始化 详报第三段(关联方) impala执行 --
drop table if exists pth_rmp.rmp_attribution_summ_rel_temp_init_impala;
create table if not exists pth_rmp.rmp_attribution_summ_rel_temp_init_impala AS 
with 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch as  --最新批次的综合舆情分数据,且有关联方
(
	select a.* from pth_rmp.rmp_alert_comprehs_score_temp_init a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.rmp_alert_comprehs_score_temp_init group by score_dt )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	--   and a.score_dt= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
RMP_COMPY_CONTRIB_DEGREE_ as 
(
	select *,score_dt as batch_dt
	from pth_rmp.rmp_compy_contrib_degree_init
	-- where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
rmp_opinion_risk_info_ as 
(
	select *
	from pth_rmp.rmp_opinion_risk_info_init
	where notice_date >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
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
		-- concat_ws('、',sort_array(collect_set(concat(relation_nm,'(',relation_tag,')')))) as rel_msg
		group_concat(concat(relation_nm,'(',relation_tag,')'),'、') as rel_msg
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
		-- concat_ws('、',sort_array(collect_set(tmp_risk_imp_msg))) as risk_imp_msg
		group_concat(tmp_risk_imp_msg,'、')  as risk_imp_msg
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
		-- concat_ws('、',sort_array(collect_set(rel_rsk_msg))) as rsk_msg
		group_concat(distinct rel_rsk_msg,'、')  as rsk_msg

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
		-- concat_ws('、',sort_array(collect_set(relation_nm))) as imp_risk_rel,
		group_concat(relation_nm,'、') as imp_risk_rel,   --命中重大风险事件的关联方
		-- concat_ws('、',sort_array(collect_set(tag_ii))) as imp_tag
		group_concat(distinct tag_ii,'、') as imp_tag   --重大风险事件
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
where to_date(score_dt)>= '2022-09-09'
  and to_date(score_dt)<= '2022-10-14'
;



--（5）sql初始化 详报第四段(汇总) impala执行 --
drop table if exists pth_rmp.rmp_attribution_summ_last_temp_init_impala;
create table if not exists pth_rmp.rmp_attribution_summ_last_temp_init_impala as 
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
with 
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Main as  --最新批次的综合舆情分数据,仅主体信息
(
	select distinct 
		a.batch_dt,a.corp_id,a.corp_nm,a.score_dt,cast(a.score as float) as score,
		a.second_score,a.third_score,a.origin_comprehensive_score,a.comprehensive_score 
	from pth_rmp.rmp_alert_comprehs_score_temp_init a 
	join (select max(batch_dt) as new_batch_dt,score_dt from pth_rmp.rmp_alert_comprehs_score_temp_init group by score_dt)b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'') and a.score_dt=b.score_dt
	where a.alert=1 
	--   and a.score_dt= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
RMP_ALERT_COMPREHS_SCORE_TEMP_Batch_Rel as  --最新批次的综合舆情分数据,且有关联方
(
	select distinct
		a.batch_dt,a.corp_id,a.corp_nm,a.score_dt,cast(a.score as float) as score,
		a.relation_id,a.relation_nm,
		a.second_score,a.third_score,a.origin_comprehensive_score,a.comprehensive_score,
		a.score_hit,a.label_hit,a.alert
	from pth_rmp.rmp_alert_comprehs_score_temp_init a 
	join (select max(batch_dt) as new_batch_dt from pth_rmp.rmp_alert_comprehs_score_temp_init )b  
		on nvl(a.batch_dt,'') = nvl(b.new_batch_dt,'')
	where a.alert=1 
	--   and a.score_dt= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
),
rmp_opinion_risk_info_ as 
(
	select *
	from pth_rmp.rmp_opinion_risk_info_init
	where notice_date >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
rmp_opinion_risk_info_tag_ as 
(
	select *
	from pth_rmp.rmp_opinion_risk_info_tag
	where importance=-3
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
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
	join rmp_opinion_risk_info_ rsk
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
	join rmp_opinion_risk_info_ rsk
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
			join rmp_opinion_risk_info_tag_ tag
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
		-- concat_ws('、',collect_set(case_type)) as last_sentence_main_case_type
		group_concat(distinct case_type,'、') as last_sentence_main_case_type
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
			select 
				corp_id,
				corp_nm,
				score_dt,
				relation_id,
				max(relation_nm) as relation_nm
			from com_score_with_risk_info_rel
			group by corp_id,corp_nm,score_dt,relation_id  --去重重复的relation_nm
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
			when rel_cnt>3 then concat(rel_msg,'等')
			else rel_msg
		end as last_sentence_rel
	from 
	(
		select 
			corp_id,
			corp_nm,
			score_dt,
			-- concat_ws('、',collect_set(relation_nm)) as rel_msg,
			group_concat(distinct relation_nm,'、') as rel_msg,
			count(relation_nm) as rel_cnt
		from rel
		group by corp_id,corp_nm,score_dt
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
			join rmp_opinion_risk_info_tag_ tag
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
		-- concat_ws('、',collect_set(case_type)) as last_sentence_rel_case_type
		group_concat(distinct case_type,'、') as last_sentence_rel_case_type
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
select distinct
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	score,
	rel_score_summ,
	last_sentence
from last_msg
where to_date(score_dt)>= '2022-09-09'
  and to_date(score_dt)<= '2022-10-14'
;



--（4）sql初始化 合并4个段落 hive执行 --
with 
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
)
---------------------- 以上部分为临时表 --------------------------------------------------------------------------
insert into pth_rmp.rmp_attribution_summ_init partition(etl_date=19900101)
select 
	md5(concat(batch_dt,corp_id,'0')) as sid_kw,
	-- batch_dt,
	corp_id,
	corp_nm,
	credit_code as credit_cd,
	score_dt,
	nvl(report_msg1,'') as report_msg1,
	nvl(report_msg2,'') as report_msg2,
	'' as report_msg5,
	0 as delete_flag,
	'' as create_by,
	CURRENT_TIMESTAMP() as create_time,
	'' as update_by,
	CURRENT_TIMESTAMP() as update_time,
	0 as version
from 
(
	select 
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		chg.credit_code,
		main.score_dt,
		one.First_sentence as report_msg1,
		case 
			when main.score_hit=1 and main.score>=main.rel_score_summ then 
				concat(main.Main_sentence,'\\r\\n',rel.rel_sentence,'\\r\\n',lst.last_sentence)
			when main.score_hit=1 and main.score<main.rel_score_summ then 
				concat(rel.rel_sentence,'\\r\\n',main.Main_sentence,'\\r\\n',lst.last_sentence)
			when main.score_hit=0  then 
				concat(main.Main_sentence,'\\r\\n',rel.rel_sentence,'\\r\\n',lst.last_sentence)
			-- when score_hit=0 and main.score<main.rel_score_summ then 
				-- concat(rel.rel_sentence,'\\r\\n',main.Main_sentence,'\\r\\n',lst.last_sentence)
		end as report_msg2
	from pth_rmp.rmp_attribution_summ_first_temp_init_impala one 
	left join pth_rmp.rmp_attribution_summ_main_temp_init_impala main
		on one.corp_id=main.corp_id and one.score_dt=main.score_dt and one.batch_dt=main.batch_dt 
	left join pth_rmp.rmp_attribution_summ_rel_temp_init_impala rel 
		on one.corp_id = rel.corp_id and one.score_dt = rel.score_dt and one.batch_dt=rel.batch_dt
	left join pth_rmp.rmp_attribution_summ_last_temp_init_impala lst 
		on one.corp_id = lst.corp_id and one.score_dt = lst.score_dt and one.batch_dt=lst.batch_dt
	left join (select * from corp_chg where source_code='FI') chg
		on one.corp_id=chg.corp_id
)A 
;
