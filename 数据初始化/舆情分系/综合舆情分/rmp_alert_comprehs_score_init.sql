
-- DDL 综合舆情分temp hive执行-- 
drop table if exists pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP_INIT;
create table pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP_INIT
(	
	sid_kw string,
	batch_dt string,
	corp_id	string,
	corp_nm	string,
	credit_code	string,
	score_dt timestamp,
	score	double,
	relation_id	string,
	relation_nm	string,
	r_score	double,
	r	double,
	r_score_cal	double,
	news_duplicates_ratio	double,
	second_score	double,
	third_score	double,
	origin_comprehensive_score double,
	comprehensive_score	double,
	score_hit	tinyint,
	label_hit	tinyint,
	alert	tinyint,
	fluctuated	double,
	model_version	string,
	adjust_warnlevel  string,
	delete_flag	tinyint,
	create_by	string,
	create_time	timestamp,
	update_by	string,
	update_time	timestamp,
	version	tinyint
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile
;


-- 初始化sql (pth_rmp.rmp_alert_comprehs_score_temp_init) hive执行 PS:约30min/天 -- 
-- PS:上游单主体舆情分日期最早在2022-09-09 --
set hive.exec.parallel=true;
set hive.auto.convert.join=ture;
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
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
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
RMP_ALERT_SCORE_SUMM_ as --取距离当前ETL_date最近的14天单主体舆情分数据（单主体舆情分不一定每家企业每天都有数据）
(	
	select 
		0 as his_flag,
		batch_dt,   
		corp_id,corp_nm,credit_code,score_dt,score,yq_num,score_hit_ci,score_hit_yq,score_hit,label_hit,alert,fluctuated,model_version,delete_flag,update_time
	from pth_rmp.RMP_ALERT_SCORE_SUMM a
	where a.delete_flag=0
	-- 取距离当前ETL_DATE最近一天的日期 --
	and a.score_dt in (select max(score_dt) from pth_rmp.RMP_ALERT_SCORE_SUMM where score_dt <= to_date('2022-10-30'))  
	UNION ALL 
	select 
		1 as his_flag,
		score_dt as batch_dt,
		corp_id,corp_nm,credit_code,score_dt,score,yq_num,score_hit_ci,score_hit_yq,score_hit,label_hit,alert,fluctuated,model_version,delete_flag,update_time
	from 
	(   --取除去距离当前ETL_DATE最近一天日期的近13的数据
		select *,row_number() over(partition by corp_id order by score_dt desc) as rm
		from pth_rmp.RMP_ALERT_SCORE_SUMM_INIT --@初始化表  
		where 1=1
		  and delete_flag=0 
		  and score_dt>=to_date('2022-09-09')
		  --and score_dt<=from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
	)A --where rm<=14
),
rmp_opinion_risk_info_ as 
(
	select * 
	from pth_rmp.rmp_opinion_risk_info_init     --@初始化表 --@pth_rmp.rmp_opinion_risk_info
	where delete_flag=0
	  -- 时间限制(近两天数据，保证新闻重复数占比统计)
	  and to_date(notice_dt) >= '2022-09-09'
	  --and to_date(notice_dt) >= from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' )-1,'yyyy-MM-dd')
	  --and to_date(notice_dt) <= from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
),
RMP_COMPANY_CORE_REL_ as 
(
	select distinct a.* 
	from pth_rmp.RMP_COMPANY_CORE_REL_INIT a 
	where 1 = 1
	  -- 时间限制(自动取最大日期)
	  and a.relation_dt in (select max(relation_dt) max_relation_dt from pth_rmp.RMP_COMPANY_CORE_REL_INIT)
		-- on a.relation_dt=b.max_relation_dt
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
CFG_rmp_opinion_risk_info_tag as 
(
	select * 
	from pth_rmp.rmp_opinion_risk_info_tag
),
CFG_RMP_COMPY_CORE_REL_DEGREE as   --重要关联方强度配置表
(
	select *
	from pth_rmp.RMP_COMPY_CORE_REL_DEGREE_CFG
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
MID_RMP_ALERT_SCORE_SUMM as  -- 取每天最新批次的 单主体舆情分数据
(
	--当日数据
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		to_date(a.score_dt) as score_dt,  --已转换为日期，不带时分秒（原始值为带批次时间的日期 '2022-01-02 02:00:00'）
		a.score,
		a.yq_num,
		a.score_hit_yq,
		a.score_hit_ci,
		a.score_hit,
		a.label_hit,alert,
		a.fluctuated,
		a.model_version 
	from RMP_ALERT_SCORE_SUMM_ a
	where 1=1 
	  and a.delete_flag=0
	  and a.his_flag=0
	  and a.batch_dt in (select max(batch_dt) as max_batch_dt from RMP_ALERT_SCORE_SUMM_)
	-- join (select max(batch_dt) as max_batch_dt,score_dt as score_dt from RMP_ALERT_SCORE_SUMM_ group by score_dt) b  
	-- 	on a.batch_dt=b.max_batch_dt and a.score_dt = b.score_dt
	--历史数据
	UNION ALL 
		select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		to_date(a.score_dt) as score_dt,  --已转换为日期，不带时分秒（原始值为带批次时间的日期 '2022-01-02 02:00:00'）
		a.score,
		a.yq_num,
		a.score_hit_yq,
		a.score_hit_ci,
		a.score_hit,
		a.label_hit,alert,
		a.fluctuated,
		a.model_version 
	from RMP_ALERT_SCORE_SUMM_ a
	where 1=1 
	  and a.delete_flag=0
	  and a.his_flag=1
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
news as(   --！！！注意此处notice_dt 处理为日期型，当日数据需要实时处理
	select distinct corp_id,corp_nm,to_date(notice_dt) as notice_dt,msg_id,0 as today_flag
	from rmp_opinion_risk_info_ 
	where signal_type=0
	  and notice_dt<to_date(current_timestamp())
	UNION ALL 
	select distinct corp_id,corp_nm,to_date(current_timestamp()) as notice_dt,msg_id,1 as today_flag
	from rmp_opinion_risk_info_  
	where signal_type=0
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
		)D 
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
		0.3*sum(r_score_cal) over(partition by corp_id,score_dt) as second_score,
		0.7*max(r_score_cal) over(partition by corp_id,score_dt) as third_score,
		model_version
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
		end as comprehensive_score  --！！！档位划分，未来可能还会调整
	from 
	(
		select 
			*,
			second_score+third_score+score as origin_comprehensive_score,
			second_score+third_score+score as comprehensive_score  --计算得到综合舆情分
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
				rc.model_version
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
				corp_id, 
				corp_nm,
				score_dt,
				score,
				main_score_hit_ci,
				main_score_hit,
				main_label_hit,
				relation_id,
				relation_nm,
				r_score,
				r,
				r_score_cal,
				r_importance,
				news_duplicates_ratio,
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
							 from rmp_opinion_risk_info_  --@rmp_opinion_risk_info
					) rsk 
						on rsk.case_type_ii_cd=tag.tag_ii_cd
					right join com_score_temp A 
						on A.relation_id=rsk.corp_id and A.score_dt=to_date(rsk.notice_dt)
				
				-- left join (  select distinct corp_id,corp_nm,notice_dt,case_type_ii_cd,case_type_ii 
				-- 			 from rmp_opinion_risk_info_  --@rmp_opinion_risk_info
				-- 		   ) rsk 
				-- 	on A.relation_id=rsk.corp_id and A.score_dt=to_date(rsk.notice_dt)
				-- left join (  select * 
				-- 			 from CFG_rmp_opinion_risk_info_tag
				-- 			 where importance=-3
				-- 		  ) tag 
				-- 	on rsk.case_type_ii_cd=tag.tag_ii_cd
			)B_ where min_rm=1  --group by corp_id,score_dt,relation_id,r_importance
		)B
	)C
)
insert into pth_rmp.rmp_alert_comprehs_score_temp_init  partition(etl_date=19900101)
select distinct
	md5(concat(to_date(G.batch_dt),nvl(G.corp_id,''),nvl(lb.relation_id,''),'0')) as sid_kw,
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
	origin_comprehensive_score,
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
		fluctuated
	from 
	(
		select 
			max(batch_dt) over(partition by corp_id,score_dt) as batch_dt,
			corp_id,
			score_dt,
			Main_score_hit_yq,
			second_score,
			third_score,
			origin_comprehensive_score,
			comprehensive_score,
			model_version,
			mu,
			sqrt(sigma_tmp/12-1) as sigma,
			mu + sqrt(sigma_tmp/12-1) as ci,  --置信区间下限
			fluctuated,
			row_number() over(partition by corp_id,score_dt order by fluctuated desc) as fluctuated_rm
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
				E.origin_comprehensive_score,
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
					origin_comprehensive_score,
					count(*) over(partition by corp_id,score_dt) as cal_score_dt_cnt,  --查看近12天统计日期实际数量
					avg(cal_comprehensive_score) over(partition by corp_id,score_dt order by yq_num desc) as mu
				from 
				( 	select a.batch_dt,a.corp_id,a.score_dt,
						   a.Main_score_hit_yq,a.main_score_hit_ci,a.main_score_hit,
						   a.second_score,a.third_score,
						   a.comprehensive_score,a.model_version,a.origin_comprehensive_score,
						   b.score_dt as cal_score_dt,b.yq_num,b.comprehensive_score as cal_comprehensive_score,b.RM
					from com_score_temp a 
					join (select *,row_number() over(partition by corp_id order by yq_num desc) as RM from com_score_temp) b 
						on a.corp_id=b.corp_id 
					where b.score_dt<=a.score_dt and b.score_dt>=date_add(a.score_dt,-13)
				)D where rm<=12
			)E
		)F
	)F1 where fluctuated_rm=1
)G join label_hit_tab lb on G.corp_id=lb.corp_id and G.score_dt = lb.score_dt
   join corp_chg chg on g.corp_id=chg.corp_id and chg.source_code='FI'
where G.batch_dt is not null
  and G.score_dt >= to_date('2022-09-09') 
  and G.score_dt <= to_date('2022-10-14') 
;