-- 综合舆情分中间表 rmp_alert_comprehs_score_temp (同步方式：一天多批次插入)--
--入参：${ETL_DATE}(20220818 int)
-- /* 2022-9-5 新闻重复数占比统计更新，增加近24小时新闻的统计 */
-- /* 2022-9-17 新增 综合舆情分分值归一到 0-100 的处理*/
-- /* 2022-9-28  增加 origin_comprehensive_score 字段*/
-- /* 2022-10-26 效率优化，近14天数据，除最新一天需计算，其余直接取历史表  impala:35s*/
-- /* 2022-10-27 hive层效率优化，采用mapjoin方式提升关联效率，同时调整单主体舆情分取最大批次的方法 hive:1h27min */
-- /* 2022-11-05 修复model_version和adjust_warnlevel反了的问题 */
-- /* 2022-11-23 取单主体舆情分接口的时，增加去除上游因追批导致数据的重复问题 */
-- /* 2022-12-02 合并YC代码，r系数修复 */
-- /* 2022-12-02 alert逻辑调整 综合舆情分>=20且alert=1，最终才异动 */
-- /* 2022-12-12 由读取pth_rmp.rmp_opinion_risk_info,改为读取更高效的副本表pth_rmp.rmp_opinion_risk_info_04 */
-- /* 2022-12-15 综合舆情分代码逻辑调整升级，增加调整等级逻辑以及企业主体纳入上市发债企业 */
-- /* 2022-12-27 修复缺失库名pth_rmp前缀的问题 */
-- /* 2023-01-06 修改重要关联方表取数逻辑*/
-- /* 2023-01-10 SQL性能调优 */
-- /* 2023-01-10 update_time取对应最大批次，防止追批重复数据 */



-- PS: 可以将综合舆情分发任务 拆解为：com_score_temp,label_hit_tab(这两部分并行)； insert部分(依赖前两部分完成后执行)
--依赖 pth_rmp.rmp_calendar,pth_rmp.RMP_ALERT_SCORE_SUMM,pth_rmp.RMP_COMPANY_CORE_REL,pth_rmp.RMP_COMPY_CORE_REL_DEGREE_CFG
	-- pth_rmp.rmp_opinion_risk_info,hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf(特征原始值) 



-- 00 创建 临时表副本 --
set hive.exec.parallel=true;

insert overwrite table pth_rmp.corp_chg_04_zhyqf
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code--,a.etl_date as id_etl_date,b.etl_date as info_etl_date
		,b.is_list,b.is_bond
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
;


set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.auto.convert.join=ture;
set hive.mapjoin.smalltable.filesize=300000000;  --300MB 
set hive.ignore.mapjoin.hint = false;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

-- 01 无调整等级字段逻辑 部分 --
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with 
corp_chg as 
(
	select corp_id,corp_name,credit_code,source_id,source_code
	from pth_rmp.corp_chg_04_zhyqf
	group by corp_id,corp_name,credit_code,source_id,source_code
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
RMP_ALERT_SCORE_SUMM_ as --取距离当前ETL_date最近的14天单主体舆情分数据（单主体舆情分不一定每家企业每天都有数据）
(	
	-- select distinct
	-- 	0 as his_flag,
	-- 	batch_dt,   
	-- 	corp_id,corp_nm,credit_code,score_dt,score,yq_num,score_hit_ci,score_hit_yq,score_hit,label_hit,alert,fluctuated,model_version,delete_flag--,update_time
	-- from pth_rmp.RMP_ALERT_SCORE_SUMM a
	-- where a.delete_flag=0
	-- -- 取距离当前ETL_DATE最近一天的日期 --
	-- and a.score_dt in (select max(score_dt) from pth_rmp.RMP_ALERT_SCORE_SUMM where to_date(score_dt) = from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd') )	  
	-- UNION ALL 
	select distinct
		1 as his_flag,
		score_dt as batch_dt,
		corp_id,corp_nm,credit_code,score_dt,score,yq_num,score_hit_ci,score_hit_yq,score_hit,label_hit,alert,fluctuated,model_version,delete_flag,update_time
     from pth_rmp.RMP_ALERT_SCORE_SUMM
    where delete_flag=0
	  and etl_date >=cast(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')-13*3600*24,'yyyyMMdd') as int)
	  and etl_date <=cast(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')-0*3600*24,'yyyyMMdd') as int)
	  and score_dt<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	  and score_dt>= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-13))
),
rmp_opinion_risk_info_ as 
(
   --当日数据
	select * 
	from pth_rmp.rmp_opinion_risk_info_04   --@pth_rmp.rmp_opinion_risk_info_04
	where delete_flag=0
	  and notice_dt>= from_unixtime((unix_timestamp()-3600*24))
	  and notice_dt< current_timestamp()
	  and cast(${ETL_DATE} as string)=cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string)
	union all
	--历史数据
	select * 
	from pth_rmp.rmp_opinion_risk_info_04   --@pth_rmp.rmp_opinion_risk_info_04
	where delete_flag=0
	  and to_date(notice_dt) = from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
	  and cast(${ETL_DATE} as string)<cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string)
),
RMP_COMPANY_CORE_REL_ as 
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
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
CFG_rmp_opinion_risk_info_tag as 
(
	select * 
	from pth_rmp.rmp_opinion_risk_info_tag
),
-- CFG_rmp_calendar as 
-- (
-- 	select * from pth_rmp.rmp_calendar --@rmp_calendar
-- ),
CFG_RMP_COMPY_CORE_REL_DEGREE as   --重要关联方强度配置表
(
	select *
	from pth_rmp.RMP_COMPY_CORE_REL_DEGREE_CFG
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
MID_RMP_ALERT_SCORE_SUMM as  -- 取每天最新批次的 单主体舆情分数据
(
	--当日数据&历史
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		to_date(a.score_dt) as score_dt,  --已转换为日期，不带时分秒（原始值为带批次时间的日期 '2022-01-02 02:00:00'）
		--modify yangcan 20221110
		round(a.score,4) as score,
		a.yq_num,
		a.score_hit_yq,
		a.score_hit_ci,
		a.score_hit,
		a.label_hit,
		alert,
		a.fluctuated,
		a.model_version 
	from RMP_ALERT_SCORE_SUMM_ a
	join (select max(batch_dt) as max_batch_dt,score_dt as score_dt,max(update_time) as max_update_time from RMP_ALERT_SCORE_SUMM_ group by score_dt) b  
		on a.batch_dt=b.max_batch_dt and a.score_dt = b.score_dt and a.update_time=b.max_update_time
	where 1=1 
	--   and a.his_flag=0
	--   and a.batch_dt in (select max(batch_dt) as max_batch_dt from RMP_ALERT_SCORE_SUMM_)

	-- --历史数据
	-- UNION ALL 
	-- 	select 
	-- 	a.batch_dt,
	-- 	a.corp_id,
	-- 	a.corp_nm,
	-- 	to_date(a.score_dt) as score_dt,  --已转换为日期，不带时分秒（原始值为带批次时间的日期 '2022-01-02 02:00:00'）
	-- 	--modify yangcan 20221110
	-- 	round(a.score,4) as score,
	-- 	a.yq_num,
	-- 	a.score_hit_yq,
	-- 	a.score_hit_ci,
	-- 	a.score_hit,
	-- 	a.label_hit,
	-- 	a.alert,
	-- 	a.fluctuated,
	-- 	a.model_version 
	-- from RMP_ALERT_SCORE_SUMM_ a
	-- where 1=1 
	--   and a.delete_flag=0
	--   and a.his_flag=1
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
news as(   --！！！注意此处notice_dt 处理为日期型，当日数据需要实时处理  --
	select distinct corp_id,corp_nm,to_date(current_timestamp()) as notice_dt,msg_id,1 as today_flag
	from rmp_opinion_risk_info_  
	where signal_type=0  --2022-11-09 21：00：00                              --2022-11-10 21：00：00
	 and notice_dt>= from_unixtime((unix_timestamp()-3600*24)) and  notice_dt<= current_timestamp()
	-- select distinct corp_id,corp_nm,to_date(notice_dt) as notice_dt,msg_id
	--   from rmp_opinion_risk_info_	
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
relcompy_with_importance as 
(
	select distinct
		relation_dt,corp_id,relation_id,relation_nm,compy_type,relation_type_l2_code,IMPORTANCE,
		case
			--when importance = 3 and	compy_type in ('上市','发债') then 
			when importance = 3 and	(instr(compy_type,'上市')>0 or instr(compy_type,'发债')>0) then 
				1
			--when importance = 3 and compy_type in ('新三板','金融机构') then
			when importance = 3 and (instr(compy_type,'新三板')>0 or instr(compy_type,'金融机构')>0) then
				0.9
			--when importance = 3 and (compy_type is NULL or compy_type = '其他') THEN
			when importance = 3 and (compy_type is NULL or instr(compy_type,'其他')>0) THEN
				0.8
			--when importance = 2 and compy_type in ('上市','发债') then 
			when importance = 2 and (instr(compy_type,'上市')>0 or instr(compy_type,'发债')>0) then 
				0.7
			--when importance = 2 and compy_type in ('新三板','金融机构') then
			when importance = 2 and (instr(compy_type,'新三板')>0 or instr(compy_type,'金融机构')>0) then
				0.6
			--when importance = 2 and (compy_type is NULL or compy_type = '其他') THEN
			when importance = 2 and (compy_type is NULL or instr(compy_type,'其他')>0) THEN
				0.5
			--when importance = 1 and compy_type in ('上市','发债') then 
			when importance = 1 and (instr(compy_type,'上市')>0 or instr(compy_type,'发债')>0) then 
				0.4
			--when importance = 1 and compy_type in ('新三板','金融机构') then
			when importance = 1 and (instr(compy_type,'新三板')>0 or instr(compy_type,'金融机构')>0) then
				0.3
			--when importance = 1 and (compy_type is NULL or compy_type = '其他') THEN  --compy_type is NULL -> compy_type = '其他'
			when importance = 1 and (compy_type is NULL or instr(compy_type,'其他')>0) THEN  --compy_type is NULL -> compy_type = '其他'
				0.2
			ELSE
				0
		end as r   -- 对主体影响力
	FROM
	(
		select 
			-- cl.dt as relation_dt,
			D.*
		from
		(
			select 
				a.relation_dt,  --若只跑当天，则relation_dt取最新值没问题。若跑初始化，则取用最新一天的关联方作为历史初始化
				a.corp_id,
				a.relation_id,
				a.relation_nm,
				a.compy_type,
				a.relation_type_l2_code,
				CFG.IMPORTANCE,
				row_number() over(partition by a.corp_id,a.relation_id order by CFG.IMPORTANCE desc) as RM
			from CFG_RMP_COMPY_CORE_REL_DEGREE CFG   --效率优化：小表关联大表
			join RMP_COMPANY_CORE_REL_ a 
				on a.relation_type_l2_code = CFG.rel_type_ii_cd
		)D --cross join CFG_rmp_calendar cl
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
		model_version,
		rel_yq_num
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
			b.model_version,
			b.yq_num as rel_yq_num
		from MID_RMP_ALERT_SCORE_SUMM b   --效率优化：小表join大表
		join relcompy_with_importance a 
			on a.relation_id=b.corp_id   --已取最新一天的关联方数据  --and a.relation_dt=b.score_dt
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
		--yangcan modify 20221110
		round(0.3*sum(r_score_cal) over(partition by corp_id,score_dt),4) as second_score,
		round(0.7*max(r_score_cal) over(partition by corp_id,score_dt),4) as third_score,
		model_version,
		rel_yq_num
	from core_relcompy_score
),
-- 综合舆情分 --
com_score_temp as  --计算得到综合舆情分
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		score,
		yq_num, 
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
		origin_comprehensive_score,
		case 
			when comprehensive_score > 200  then 
				200 / 2
			else  
				comprehensive_score / 2
		end as comprehensive_score,  --！！！档位划分，未来可能还会调整
		rel_yq_num
	from 
	(
		select 
			*,
			second_score+third_score+score as origin_comprehensive_score,
			second_score+third_score+score as comprehensive_score  --计算得到综合舆情分
			-- batch_dt,corp_id,corp_nm,
			-- score_dt,score,
			-- Main_score_hit_yq,main_score_hit_ci,main_score_hit,main_label_hit,
			-- relation_id,relation_nm,r_score,r_importance,r,r_score_cal,news_duplicates_ratio,
			-- second_score,third_score,model_version,
			-- second_score+third_score+score as comprehensive_score  --计算得到综合舆情分
		from 
		(
			select     --效率优化
				distinct  
				nvl(rc.batch_dt,sc.batch_dt) as batch_dt,
				nvl(rc.corp_id,sc.corp_id) as corp_id,   --合并左右连接的企业id
				rc.corp_nm,
				nvl(rc.score_dt,sc.score_dt) as score_dt,
				nvl(sc.score,0) as score,
				nvl(sc.yq_num,0) as yq_num,  --主体层的舆情数量
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
				rc.model_version,
				rc.rel_yq_num
			from  MID_RMP_ALERT_SCORE_SUMM sc    --效率优化：小表join大表
			full join core_relcompy_score_res rc 
				on rc.corp_id = sc.corp_id and rc.score_dt=sc.score_dt
		)A
	)B
),
-- 风险事件标签 -- 
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
				select      --效率优化：小表读进内存并且直接在Map层完成关联
					A.*,
					nvl(tag.importance,0) as tag_importance,
					row_number() over(partition by a.corp_id,a.score_dt,a.relation_id order by a.r_importance asc) as min_rm
				from (  select *                          --效率优化：标签小表关联大表
							 from CFG_rmp_opinion_risk_info_tag
							 where importance=-3
					 ) tag  
					 right join 
					(  select distinct corp_id,corp_nm,notice_dt,case_type_ii_cd,case_type_ii 
							 from rmp_opinion_risk_info_  --@rmp_opinion_risk_info_04
					) rsk 
						on rsk.case_type_ii_cd=tag.tag_ii_cd
					right join com_score_temp A 
						on A.relation_id=rsk.corp_id and A.score_dt=to_date(rsk.notice_dt)
			)B_ where min_rm=1  
		)B
	)C
)
insert overwrite table pth_rmp.rmp_alert_comprehs_score_temp_01   --只保留每天最新批次数据
select distinct
	-- md5(concat(G.batch_dt,cast(g.score_dt as string),nvl(G.corp_id,''),nvl(lb.relation_id,''),'0')) as sid_kw,
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
	--round(G.second_score,4) as second_score,
	G.second_score  as second_score, 
	--round(G.third_score) as third_score,
	G.third_score as third_score,
	origin_comprehensive_score,
	--round(G.comprehensive_score,4) as comprehensive_score,
	G.comprehensive_score as comprehensive_score,
	G.score_hit,
	lb.label_hit,
	--if(G.score_hit=1 or lb.label_hit=1,1,0) as alert, modify yangcan 20221110
	if((((G.score_hit=1  or lb.label_hit=2) and G.comprehensive_score>=20 ) or lb.label_hit=1),1,0) as alert,  --20221202 增加在原有异动基础上，当综合舆情分>=20才异动
	G.fluctuated,
	G.model_version,
	'' AS adjust_warnlevel,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from 
(
	select distinct
		batch_dt,
		corp_id,
		score_dt,
		--Main_score_hit_yq,
		second_score,
		third_score,
		origin_comprehensive_score,
		comprehensive_score,
		model_version,
		mu,
		sigma,
		if(comprehensive_score>ci,1,0) as score_hit , --！！！2022-8-25 上线临时调整，得分预警不考虑舆情数量的限制条件
		--if(comprehensive_score>ci and Main_score_hit_yq=1,1,0) as score_hit ,   --！！！备份
		fluctuated
	from 
	(
		select 
			max(batch_dt) over(partition by corp_id,score_dt) as batch_dt,  --此处会有近14天所有企业batch_dt，取对应企业每天最大批次时间
			corp_id,
			score_dt,
			Main_score_hit_yq,
			second_score,
			third_score,
			origin_comprehensive_score,
			comprehensive_score,
			model_version,
			mu,
			sqrt(sigma_tmp/(14-1)) as sigma,
			mu + sqrt(sigma_tmp/(14-1)) as ci,  --置信区间下限
			fluctuated,
			row_number() over(partition by corp_id,score_dt order by fluctuated desc) as fluctuated_rm
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
				origin_comprehensive_score,
				comprehensive_score,
				model_version,
				mu,
				--E.cal_score_dt,
				case when cal_score_dt_cnt >=14 then sum(power(d2_comprehensive_score-mu,2)) over(partition by corp_id,score_dt)
				     else (sum(power(d2_comprehensive_score-mu,2)) over(partition by corp_id,score_dt))+(14-cal_score_dt_cnt)*power(0-mu,2) 
			    end as sigma_tmp,
				round((nvl(mu,-0.1)-comprehensive_score)/greatest(abs(nvl(mu,-0.1)),0.1),6) as fluctuated
			from 
			(
				select 
					d1.batch_dt,
					d1.corp_id,
					d1.score_dt,
					d1.Main_score_hit_yq, 
					d1.main_score_hit_ci,
					d1.main_score_hit,
					d1.second_score,
					d1.third_score,
					d1.comprehensive_score,
					d1.model_version,
					--yq_num,
					--cal_score_dt,
					d1.origin_comprehensive_score,
					count(d2.score_dt) over(partition by d1.corp_id,d1.score_dt ) as cal_score_dt_cnt,  --查看近14天统计日期实际数量
					(sum(d2.comprehensive_score) over(partition by d1.corp_id,d1.score_dt ))/14 as mu,
					d2.comprehensive_score as d2_comprehensive_score
				from com_score_temp d1    --新增逻辑(兼容跑一段时间综合舆情分 2022-11-24 hz)
				join com_score_temp d2 
					on d1.corp_id=d2.corp_id 
				where d2.score_dt <= d1.score_dt 
				  and d2.score_dt >= date_add(d1.score_dt,-13) 
			)E
		)F
	)F1 where fluctuated_rm=1
)G join label_hit_tab lb on G.corp_id=lb.corp_id and G.score_dt = lb.score_dt
   join corp_chg chg on g.corp_id=chg.corp_id and chg.source_code='FI'
where G.batch_dt is not null
  and G.comprehensive_score<>0
  and G.score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;




-- 03 调整等级字段逻辑 part1(数据准备) --
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint = false;
--—————————————————————————————————————————————————————— 临时落地表 ————————————————————————————————————————————————————————————————————————————————--
insert overwrite table pth_rmp.rmp_alert_comprehs_score_temp_batch_copy
	select b.*
	from 
	(
		select   --除去当日之外的，29天数据
			a.batch_dt,a.corp_id,a.corp_nm,a.credit_code,a.score_dt,a.score,a.relation_id,a.relation_nm,a.r_score,a.r,a.r_score_cal
			,a.news_duplicates_ratio,a.second_score,a.third_score,a.origin_comprehensive_score ,a.comprehensive_score,a.score_hit,a.label_hit,a.alert,a.fluctuated,a.model_version
			,row_number() over(partition by a.sid_kw order by 1) as rm
		from pth_rmp.rmp_alert_comprehs_score_temp a 	--除去当日最新批次数据
		join (select max(batch_dt) as max_batch_dt,score_dt from pth_rmp.rmp_alert_comprehs_score_temp group by score_dt) b 
			on a.batch_dt=b.max_batch_dt and  a.score_dt=b.score_dt 
		where a.score_dt < to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
		  and a.score_dt > to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
		union all 
		select    --当日数据
			a.batch_dt,a.corp_id,a.corp_nm,a.credit_code,a.score_dt,a.score,a.relation_id,a.relation_nm,a.r_score,a.r,a.r_score_cal
			,a.news_duplicates_ratio,a.second_score,a.third_score,a.origin_comprehensive_score ,a.comprehensive_score,a.score_hit,a.label_hit,a.alert,a.fluctuated,a.model_version
			,row_number() over(partition by a.corp_id,a.score_dt,a.batch_dt,a.relation_id order by 1) as rm
		from pth_rmp.rmp_alert_comprehs_score_temp_01 a    --当日最新批次数据
		where a.batch_dt in (select max(batch_dt) from pth_rmp.rmp_alert_comprehs_score_temp_01)  
	) b where b.rm=1
;
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
-- drop table if exists pth_rmp.tmp_cal_copy;
-- create table pth_rmp.tmp_cal_copy stored as parquet as 
with 
rmp_alert_comprehs_score_temp_batch_ as
(
	select *
	from pth_rmp.rmp_alert_comprehs_score_temp_batch_copy
),
compy_info as
(
	select 
		a.corp_id,
		max(a.corp_name) as corp_nm,
		b.score_dt,
		b.batch_dt
	from pth_rmp.corp_chg_04_zhyqf a
	cross join (select score_dt,batch_dt from rmp_alert_comprehs_score_temp_batch_ where score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) group by score_dt,batch_dt) b
	where (a.is_list=1 or a.is_bond=1) 
	group by a.corp_id,b.score_dt,b.batch_dt
	-- select 
	-- 	a.corp_id,
	-- 	max(a.corp_name) as corp_nm
	-- from pth_rmp.corp_chg_04_zhyqf a
	-- where (a.is_list=1 or a.is_bond=1) 
	-- group by a.corp_id
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
tmp_cal as --获取上市发债以及综合舆情分有的主体作为主表
(
	select 
		-- b.sid_kw,
		nvl(b.batch_dt,a.batch_dt) as batch_dt,
		-- nvl(b.batch_dt,to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd'))) ) as batch_dt,
		nvl(a.corp_id,b.corp_id) as corp_id,
		nvl(a.corp_nm,b.corp_nm) as corp_nm,
		b.credit_code,
		to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd'))) as score_dt,
		-- nvl(b.score_dt,to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))) as score_dt,
		b.score	,
		b.relation_id	,
		b.relation_nm	,
		b.r_score	,
		b.r	,
		b.r_score_cal	,
		b.news_duplicates_ratio	,
		b.second_score	,
		b.third_score	,
		b.origin_comprehensive_score ,
		b.comprehensive_score	,
		b.score_hit	,
		b.label_hit	,
		b.alert	,
		b.fluctuated	,
		b.model_version
	from compy_info a 
	full join (select * from rmp_alert_comprehs_score_temp_batch_ where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) ) b 
		on a.corp_id=b.corp_id
)
insert overwrite table pth_rmp.tmp_cal_copy
select distinct * from tmp_cal
;


-- 04 调整等级字段逻辑 part2(计算调整等级) --
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint = false;

with rmp_alert_comprehs_score_temp_batch_ as 
(
	select *
	from pth_rmp.rmp_alert_comprehs_score_temp_batch_copy
)
insert into pth_rmp.rmp_alert_comprehs_score_temp  partition(etl_date=${ETL_DATE})
select 
	md5(concat(batch_dt,cast(score_dt as string),nvl(corp_id,''),nvl(relation_id,''),'0')) as sid_kw,
	batch_dt,
	corp_id,
	corp_nm,
	credit_code,
	score_dt,
	score,
	relation_id,
	relation_nm,
	r_score,
	r,
	r_score_cal,
	news_duplicates_ratio,
	second_score,
	third_score,
	origin_comprehensive_score ,
	comprehensive_score,
	score_hit,
	label_hit,
	alert,
	fluctuated,
	model_version,
	adjust_warnlevel,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from 
(
	select
		max(a.batch_dt) over() as batch_dt ,
		a.corp_id	,
		a.corp_nm	,
		a.credit_code	,
		a.score_dt	,
		a.score	,
		a.relation_id	,
		a.relation_nm	,
		a.r_score	,
		a.r	,
		a.r_score_cal	,
		a.news_duplicates_ratio	,
		a.second_score	,
		a.third_score	,
		a.origin_comprehensive_score ,
		a.comprehensive_score	,
		a.score_hit	,
		a.label_hit	,
		a.alert	,
		a.fluctuated	,
		a.model_version,
		case 
			when max(b.alert) over(partition by a.batch_dt,a.corp_id,a.score_dt)>0 then 
				'-3'
			when max(b.alert) over(partition by a.batch_dt,a.corp_id,a.score_dt)=0 then 
				'-2'
			else 
				NULL 
		end as adjust_warnlevel
	from pth_rmp.tmp_cal_copy a      --仅包含当日综合舆情分数据 且 增加了上市发债企业数据
	left join rmp_alert_comprehs_score_temp_batch_ b  -- 包含了当日在内30天的综合舆情分数据(随着跑批时间的推移，历史数据将会包含上市发债企业)
		on a.corp_id = b.corp_id 
	where b.score_dt > date_add(a.score_dt,-30)
	  and b.score_dt <= a.score_dt 
	-- group by a.batch_dt,a.corp_id,a.score_dt
)B 
where adjust_warnlevel is not null  --30天内无任何异动的企业不在当日数据的考察范围内，给予剔除
  and score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
group by batch_dt,corp_id,corp_nm,credit_code,score_dt,score,relation_id,relation_nm,r_score,r,r_score_cal,news_duplicates_ratio,second_score,third_score,origin_comprehensive_score ,comprehensive_score,score_hit,label_hit,alert,fluctuated,model_version,adjust_warnlevel
;