-- 综合舆情分中间表 rmp_alert_comprehs_score_temp (同步方式：一天多批次插入)--
--入参：${ETL_DATE}(20220818 int)
-- /* 2022-9-5 新闻重复数占比统计更新，增加近24小时新闻的统计 */
-- /* 2022-9-17 新增 综合舆情分分值归一到 0-100 的处理*/
with corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 --@rmp_company_id_relevance
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
rmp_calendar_ as 
(
	select * from pth_rmp.rmp_calendar --@rmp_calendar
),
RMP_ALERT_SCORE_SUMM_ as
(	select a.batch_dt,b.corp_id,b.corp_name as corp_nm,
	to_date(score_dt) as score_dt,  --已转换为日期，不带时分秒（原始值为带批次时间的日期 '2022-01-02 02:00:00'）
	score,
	score_hit_yq,score_hit_ci,score_hit,
	label_hit,alert,fluctuated,model_version
	from pth_rmp.RMP_ALERT_SCORE_SUMM a --app_ehzh.RMP_ALERT_SCORE_SUMM a   --@RMP_ALERT_SCORE_SUMM ->pth_rmp.RMP_ALERT_SCORE_SUMM
	join (select * from corp_chg where source_code='FI')b on a.corp_id=b.corp_id
	where a.delete_flag=0
),
RMP_COMPANY_CORE_REL_ as 
(
	select a.* 
	from pth_rmp.RMP_COMPANY_CORE_REL a 
	join (select max(relation_dt) max_relation_dt from pth_rmp.RMP_COMPANY_CORE_REL) b 
		on a.relation_dt=b.max_relation_dt
),
news as(   --！！！注意此处notice_dt 处理为日期型，当日数据需要实时处理
	select distinct corp_id,corp_nm,to_date(notice_dt) as notice_dt,msg_id,0 as today_flag
	from pth_rmp.rmp_opinion_risk_info  --@rmp_opinion_risk_info
	where signal_type=0 and delete_flag=0 
	  and notice_dt<to_date(current_timestamp())
	UNION ALL 
	select distinct corp_id,corp_nm,to_date(current_timestamp()) as notice_dt,msg_id,1 as today_flag
	from pth_rmp.rmp_opinion_risk_info  --@rmp_opinion_risk_info
	where signal_type=0 and delete_flag=0    
	  and notice_dt>= from_unixtime((unix_timestamp()-3600*24)) and  notice_dt<= current_timestamp()
),
Single_news as (
	select corp_id,corp_nm,notice_dt,count(*) as yq_num from news group by corp_id,corp_nm,notice_dt
),
Cross_news as (
	select a.corp_id,a.corp_nm,a.notice_dt,count(*) as cross_yq_num ,b.corp_id as rel_corp_id, b.corp_nm as rel_corp_nm
	from news a join news b
		on a.notice_dt = b.notice_dt and a.msg_id=b.msg_id 
	where a.corp_id<>b.corp_id
	group by a.corp_id ,a.corp_nm,b.corp_id ,b.corp_nm ,a.notice_dt
),
news_duplicates_static as
(
	select 
		distinct sn.corp_id, sn.notice_dt ,cn.rel_corp_id, cn.rel_corp_nm, sn.yq_num ,cn.cross_yq_num, 
		(cn.cross_yq_num/sn.yq_num) as news_duplicates_ratio
	from Cross_news cn
	join Single_news sn
		on cn.corp_id=sn.corp_id and cn.notice_dt=sn.notice_dt
),
deal_featvalue as(
	select 
		distinct batch_dt,corp_code,
		to_date(end_dt) as end_dt,
		nvl(feature_value,0) as yq_num,tmp_score_hit from
	(
		select 
			batch_dt,
			cast(corp_code as string) corp_code,
			end_dt,
			feature_name,
			feature_value,
			case 
				when (feature_name='total_num' and feature_value>=3)  
				  or (feature_name='importance_-3_num' and feature_value>=1) THEN 1
				ELSE
					0
			END as tmp_score_hit
		from (	select distinct 
					to_date(end_dt) as batch_dt,
					corp_code,end_dt,
					feature_name,feature_value
				from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf --app_ehzh.rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf  --@featvalue_senti_self -> hds.
				where feature_name in ('total_num','importance_-3_num')
			 )f0
	)f where feature_name='total_num' 
), --处理后特征值
relcompy_with_importance as 
(
	select distinct
		relation_dt,corp_id,relation_id,relation_nm,compy_type,relation_type_l2_code,IMPORTANCE,
		case
			when importance = 3 and	compy_type in ('上市','发债') then 
				1
			when importance = 3 and compy_type in ('新三板','金融机构') then
				0.9
			when importance = 3 and (compy_type is NULL or compy_type = '其他') THEN
				0.8
			when importance = 2 and compy_type in ('上市','发债') then 
				0.7
			when importance = 2 and compy_type in ('新三板','金融机构') then
				0.6
			when importance = 2 and (compy_type is NULL or compy_type = '其他') THEN
				0.5
			when importance = 1 and compy_type in ('上市','发债') then 
				0.4
			when importance = 1 and compy_type in ('新三板','金融机构') then
				0.3
			when importance = 1 and (compy_type is NULL or compy_type = '其他') THEN  --compy_type is NULL -> compy_type = '其他'
				0.2
			ELSE
				0
		end as r   -- 对主体影响力
	FROM
	(
		select 
			cl.dt as relation_dt,D.*
		from
		(
			select 
				a.corp_id,
				a.relation_id,
				a.relation_nm,
				a.compy_type,
				a.relation_type_l2_code,
				CFG.IMPORTANCE,
				row_number() over(partition by a.corp_id,a.relation_id order by CFG.IMPORTANCE desc) as RM
			from RMP_COMPANY_CORE_REL_ a 
			join pth_rmp.RMP_COMPY_CORE_REL_DEGREE_CFG CFG 
				on a.relation_type_l2_code = CFG.rel_type_ii_cd
		)D cross join rmp_calendar_ cl
	)B where RM =1   --取每家企业对应关联方的关联方密切程度最高的密切程度，作为该关联方对主体的密切程度
),
core_relcompy_score as    --（考虑存中间表，数据量大）
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		relation_id,
		relation_nm,
		compy_type,
		r_score,
		relation_type_l2_code,
		r_importance,
		r,
		news_duplicates_ratio,
		cast(r*r_score*(1-news_duplicates_ratio) as double) as r_score_cal,
		model_version
	from
	(
		select 
			b.batch_dt,
			a.corp_id,
			'' as corp_nm,
			b.score_dt,
			a.relation_id,
			a.relation_nm,
			a.compy_type,
			b.score as r_score, --关联方舆情分
			a.relation_type_l2_code,
			a.importance as r_importance,    --每家企业对应关联方的关联方密切程度最高的密切程度，作为该关联方对主体的密切程度
			a.r,    --关联方对主体影响力
			nvl(ns.news_duplicates_ratio,0) as news_duplicates_ratio,  --新闻重复数占比
			b.model_version
		from relcompy_with_importance a 
		join RMP_ALERT_SCORE_SUMM_ b 
			on a.relation_id=b.corp_id and a.relation_dt=b.score_dt
		left join news_duplicates_static ns 
			on a.corp_id=ns.corp_id and a.relation_id = ns.rel_corp_id and a.relation_dt=ns.notice_dt
	)C
),
core_relcompy_score_res as   --关联方的 综合舆情分结果 （考虑存中间表）
(
	select 	
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		relation_id,
		relation_nm,
		compy_type,
		--relation_type_l2_code,
		r_score,
		r_importance,  
		r,
		r_score_cal,
		news_duplicates_ratio,
		0.3*sum(r_score_cal) over(partition by corp_id,score_dt) as second_score,
		0.7*max(r_score_cal) over(partition by corp_id,score_dt) as third_score,
		model_version
	from core_relcompy_score
),
com_score_temp as  --计算得到综合舆情分
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		Main_score_hit_yq,
		main_score_hit,
		main_label_hit,
		main_score_hit_ci,
		relation_id,
		relation_nm,
		r_score,
		r_importance,
		r,
		r_score_cal,
		news_duplicates_ratio,
		second_score,
		third_score,
		model_version,
		case 
			when comprehensive_score > 200  then 
				200 / 2
			else  
				comprehensive_score / 2
		end as comprehensive_score  --！！！档位划分，未来可能还会调整
	from 
	(
		select 
			*,
			second_score+third_score+score as comprehensive_score  --计算得到综合舆情分
			-- batch_dt,corp_id,corp_nm,
			-- score_dt,score,
			-- Main_score_hit_yq,main_score_hit_ci,main_score_hit,main_label_hit,
			-- relation_id,relation_nm,r_score,r_importance,r,r_score_cal,news_duplicates_ratio,
			-- second_score,third_score,model_version,
			-- second_score+third_score+score as comprehensive_score  --计算得到综合舆情分
		from 
		(
			select distinct
				rc.batch_dt,
				nvl(rc.corp_id,sc.corp_id) as corp_id,   --合并左右连接的企业id
				rc.corp_nm,
				nvl(rc.score_dt,sc.score_dt) as score_dt,
				nvl(sc.score,0) as score,
				nvl(sc.score_hit_yq,0) as Main_score_hit_yq,
				nvl(sc.score_hit_ci,0) as main_score_hit_ci,
				nvl(sc.score_hit,0) as main_score_hit,
				nvl(sc.label_hit,0) as main_label_hit,
				rc.relation_id,
				rc.relation_nm,
				rc.r_score,
				rc.r_importance,
				rc.r,
				rc.r_score_cal,
				rc.news_duplicates_ratio,
				nvl(rc.second_score,0) as second_score,
				nvl(rc.third_score,0) as third_score,
				rc.model_version
			from core_relcompy_score_res rc
			full join RMP_ALERT_SCORE_SUMM_ sc
				on rc.corp_id = sc.corp_id and rc.score_dt=sc.score_dt
		)A
	)B
),
label_hit_tab AS  --风险预警
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		main_score_hit_ci,
		main_score_hit,
		main_label_hit,
		relation_id,
		relation_nm,
		r,
		r_score_cal,
		r_score,
		r_importance,
		news_duplicates_ratio,
		tag_importance,
		case 
			when main_label_hit=1 then 1
			when main_label_hit=0 and rel_label_hit_summ=1 then 2
			when main_label_hit=0 and rel_label_hit_summ=0 then 0
		end as label_hit
	from 
	(
		select 
			B.*,
			max(rel_label_hit) over(partition by B.corp_id,B.score_dt) as rel_label_hit_summ
		from 
		(
			select distinct
				batch_dt,
				--max(batch_dt) as batch_dt,
				corp_id, 
				--max(corp_nm) as corp_nm,
				corp_nm,
				score_dt,
				-- max(score) as score,
				score,
				-- max(main_score_hit_ci) as main_score_hit_ci,
				main_score_hit_ci,
				-- max(main_score_hit) as main_score_hit,
				main_score_hit,
				-- max(main_label_hit) as main_label_hit,
				main_label_hit,
				relation_id,
				-- max(relation_nm) as relation_nm,
				relation_nm,
				-- max(r_score) as r_score,
				r_score,
				r,
				-- max(r) as r,
				-- max(r_score_cal) as r_score_cal,
				r_score_cal,
				r_importance,
				-- max(news_duplicates_ratio) as news_duplicates_ratio,
				news_duplicates_ratio,
				-- min(tag_importance) as tag_importance,
				tag_importance,
				case 
					when r_importance in (2,3) and tag_importance=-3 then 1
					else 0
				End as rel_label_hit  --关联方的风险预警
			FROM
			(
				select 
					A.*,
					nvl(tag.importance,0) as tag_importance,
					row_number() over(partition by a.corp_id,a.score_dt,a.relation_id order by a.r_importance asc) as min_rm
				from com_score_temp A
				left join (  select distinct corp_id,corp_nm,notice_dt,case_type_ii_cd,case_type_ii 
							 from pth_rmp.rmp_opinion_risk_info where delete_flag=0  --@rmp_opinion_risk_info
						   ) rsk 
					on A.relation_id=rsk.corp_id and A.score_dt=to_date(rsk.notice_dt)
				left join (  select * 
							 from pth_rmp.rmp_opinion_risk_info_tag
							 where importance=-3
						  ) tag 
					on rsk.case_type_ii_cd=tag.tag_ii_cd
			)B_ where min_rm=1  --group by corp_id,score_dt,relation_id,r_importance
		)B
	)C
),
label_hit_tab_bac AS  --风险预警
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		main_score_hit_ci,
		main_score_hit,
		main_label_hit,
		relation_id,
		relation_nm,
		r,
		r_score_cal,
		r_score,
		r_importance,
		news_duplicates_ratio,
		tag_importance,
		case 
			when main_label_hit=1 then 1
			when main_label_hit=0 and rel_label_hit_summ=1 then 2
			when main_label_hit=0 and rel_label_hit_summ=0 then 0
		end as label_hit
	from 
	(
		select 
			B.*,
			max(rel_label_hit) over(partition by B.corp_id,B.score_dt) as rel_label_hit_summ
		from 
		(
			select 
				max(batch_dt) as batch_dt,
				corp_id, 
				max(corp_nm) as corp_nm,
				score_dt,
				max(score) as score,
				max(main_score_hit_ci) as main_score_hit_ci,
				max(main_score_hit) as main_score_hit,
				max(main_label_hit) as main_label_hit,
				relation_id,
				max(relation_nm) as relation_nm,
				max(r_score) as r_score,
				max(r) as r,
				max(r_score_cal) as r_score_cal,
				r_importance,
				max(news_duplicates_ratio) as news_duplicates_ratio,
				min(tag_importance) as tag_importance,
				case 
					when r_importance in (2,3) and min(tag_importance)=-3 then 1
					else 0
				End as rel_label_hit  --关联方的风险预警
			FROM
			(
				select 
					A.*,
					nvl(tag.importance,0) as tag_importance 
				from com_score_temp A
				left join (  select distinct corp_id,corp_nm,notice_dt,case_type_ii_cd,case_type_ii 
							 from pth_rmp.rmp_opinion_risk_info where delete_flag=0  --@rmp_opinion_risk_info
						   ) rsk 
					on A.relation_id=rsk.corp_id and A.score_dt=to_date(rsk.notice_dt)
				left join (  select * 
							 from pth_rmp.rmp_opinion_risk_info_tag
							 where importance=-3
						  ) tag 
					on rsk.case_type_ii_cd=tag.tag_ii_cd
			)B_ group by corp_id,score_dt,relation_id,r_importance
		)B
	)C
),
Main_com_score AS
(
	select 
		A.*,
		nvl(Df.yq_num,0) as yq_num
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			score,
			Main_score_hit_yq,
			main_score_hit_ci,
			main_score_hit,
			main_label_hit,
			second_score,
			third_score,
			model_version,
			comprehensive_score
		from com_score_temp
	)A left join deal_featvalue Df 
		on A.corp_id = Df.corp_code and A.score_dt=Df.end_dt
)
insert into pth_rmp.rmp_alert_comprehs_score_temp  --@pth_rmp.rmp_alert_comprehs_score_temp
select distinct
	md5(concat(to_date(G.batch_dt),G.corp_id,lb.relation_id,'0')) as sid_kw,
	cast(G.batch_dt as string) as batch_dt,
	G.corp_id,
	chg.corp_name as corp_nm,
	chg.credit_code as credit_code,
	G.score_dt,
	round(lb.score,4) as score,
	lb.relation_id,
	lb.relation_nm,
	lb.r_score,
	lb.r,
	lb.r_score_cal,
	lb.news_duplicates_ratio,
	round(G.second_score,4) as second_score,
	round(G.third_score) as third_score,
	round(G.comprehensive_score,4) as comprehensive_score,
	G.score_hit,
	lb.label_hit,
	if(G.score_hit=1 or lb.label_hit=1,1,0) as alert,   
	G.fluctuated,
	'' AS adjust_warnlevel,
	G.model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from 
(
	select 
		batch_dt,
		corp_id,
		score_dt,
		--Main_score_hit_yq,
		second_score,
		third_score,
		comprehensive_score,
		model_version,
		mu,
		sigma,
		if(comprehensive_score>ci,1,0) as score_hit , --！！！2022-8-25 上线临时调整，得分预警不考虑舆情数量的限制条件
		--if(comprehensive_score>ci and Main_score_hit_yq=1,1,0) as score_hit ,   --！！！备份
		fluctuated
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			score_dt,
			Main_score_hit_yq,
			second_score,
			third_score,
			comprehensive_score,
			model_version,
			mu,
			sqrt(sigma_tmp/12-1) as sigma,
			mu + sqrt(sigma_tmp/12-1) as ci,  --置信区间下限
			fluctuated
		from 
		(
			select 
				E.batch_dt,
				E.corp_id,
				E.score_dt,
				E.Main_score_hit_yq,
				E.main_score_hit_ci,
				E.main_score_hit,
				E.second_score,
				E.third_score,
				E.comprehensive_score,
				E.model_version,
				E.mu,
				E.cal_score_dt,
				sum(power(E.comprehensive_score-E.mu,2)) over(partition by E.corp_id,E.score_dt) as sigma_tmp,
				round((nvl(E.mu,-0.1)-E.comprehensive_score)/greatest(abs(nvl(E.mu,-0.1)),0.1),6) as fluctuated
			from 
			(
				select 
					batch_dt,
					corp_id,
					score_dt,
					Main_score_hit_yq,
					main_score_hit_ci,
					main_score_hit,
					second_score,
					third_score,
					comprehensive_score,
					model_version,
					yq_num,
					cal_score_dt,
					count(*) over(partition by corp_id,score_dt) as cal_score_dt_cnt,  --查看近12天统计日期实际数量
					avg(cal_comprehensive_score) over(partition by corp_id,score_dt order by yq_num desc) as mu
				from 
				( 	select a.batch_dt,a.corp_id,a.score_dt,
						   a.Main_score_hit_yq,a.main_score_hit_ci,a.main_score_hit,
						   a.second_score,a.third_score,
						   a.comprehensive_score,a.model_version,
						   b.score_dt as cal_score_dt,b.yq_num,b.comprehensive_score as cal_comprehensive_score,b.RM
					from Main_com_score a 
					join (select *,row_number() over(partition by corp_id order by yq_num desc) as RM from Main_com_score) b 
						on a.corp_id=b.corp_id 
					where b.score_dt<=a.score_dt and b.score_dt>=date_add(a.score_dt,-13)
				)D where rm<=12
			)E
		)F
	)F1
)G join label_hit_tab lb on G.corp_id=lb.corp_id and G.score_dt = lb.score_dt
   join corp_chg chg on g.corp_id=chg.corp_id
where G.score_dt = from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
;