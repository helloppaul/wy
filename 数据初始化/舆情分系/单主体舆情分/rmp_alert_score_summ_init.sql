-- DDL hive执行-- 
drop table if exists pth_rmp.rmp_alert_score_summ_init;
create table pth_rmp.rmp_alert_score_summ_init
(	
	sid_kw string,
	batch_dt string,
	corp_id string,
	corp_nm  string,
	credit_code  string,
	score_dt  TIMESTAMP,
	score  double,
	yq_num int,
	score_hit_ci tinyint,
	score_hit_yq tinyint,
	score_hit tinyint,
	label_hit  tinyint,
	alert  tinyint,
	fluctuated  double,
	model_version string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



-- 初始化sql impala执行-- 
create table pth_rmp.rmp_alert_score_summ_init_impala as 
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
rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf_  as   -- 模型_舆情分  原始接口
(
	select * 
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf
),
rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf_ as  -- 模型_特征原始值  原始接口
(
	select *
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf
),
rmp_opinion_risk_info_ as   --modify yangcan 跑批日期为当天,取当前系统时间-24小时数据,跑批日期为历史日期,取跑批日期当天数据
(
	select * 
	from pth_rmp.rmp_opinion_risk_info_init   --@pth_rmp.rmp_opinion_risk_info
	where delete_flag=0
	  and etl_date=19900101
	--   and notice_dt>= from_unixtime((unix_timestamp()-3600*24))
	--   and notice_dt< current_timestamp()
	--   and cast(${ETL_DATE} as string)=cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string)
	-- union all
	-- --历史数据
	-- select * 
	-- from pth_rmp.rmp_opinion_risk_info   --@pth_rmp.rmp_opinion_risk_info
	-- where delete_flag=0
	--   and to_date(notice_dt) = from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
	--   and cast(${ETL_DATE} as string)<cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as string)
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
mid_opinion_alert_score as   --单主体舆情分  取每天最新批次数据 (如果只有一天的数据，相当于取当天最大批次数据)
(
	select a.*,chg.corp_id,chg.corp_name as corp_nm,chg.credit_code
	from rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt,max(etl_date) as max_etl_date  from rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf_ group by to_date(rating_dt)) b  
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt) = b.score_dt and a.etl_date=b.max_etl_date
	join corp_chg chg 
		on chg.source_id = cast(a.corp_code as string)
	where chg.source_code='FI'
	-- --只取最近14天单主体舆情分 yangcan modify 20221116
    --   and to_date(a.rating_dt)<=	to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	--   and to_date(a.rating_dt)>=	to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-13))
),
mid_opinion_feat as   --特征原始值  取每天最新批次数据 (如果只有一天的数据，相当于取当天最大批次数据)
(
	select a.*,chg.corp_id,chg.corp_name as corp_nm 
	from rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt,max(etl_date) as max_etl_date from rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf_ group by to_date(end_dt)) b  
		on a.end_dt=b.max_end_dt and to_date(a.end_dt) = b.score_dt and a.etl_date=b.max_etl_date
	join corp_chg chg 
		on chg.source_id = cast(a.corp_code as string)
	where chg.source_code='FI' 
	--   and to_date(a.end_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	--   and to_date(a.end_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-13))
),
-- modelres_adjusted_senti_self_ as 
-- (
-- 	select a.*,chg.corp_id,chg.corp_name as corp_nm,chg.credit_code
-- 	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf a --app_ehzh.rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj a --modelres_adjusted_senti_self a
-- 	join corp_chg chg on chg.source_id = cast(a.corp_code as string)
-- 	where chg.source_code='FI' 
-- ),
-- featvalue_senti_self_ as 
-- (
-- 	select a.*,chg.corp_id,chg.corp_name as corp_nm 
-- 	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf a --app_ehzh.rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf a --featvalue_senti_self a
-- 	join corp_chg chg on chg.source_id = cast(a.corp_code as string)
-- 	where chg.source_code='FI' 
-- ),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
label_hit_tab AS
(
	select distinct
		batch_dt,corp_id,corp_nm,credit_code,score_dt,score,yq_num,tmp_score_hit,model_version,
		if(tag_importance=-3,1,0) as label_hit  --风险预警
		-- row_number() over(partition by k.corp_id order by k.yq_num desc) as rn,  --add yangcan 20221116
		-- count(1) over(partition by k.corp_id) as cnt	
	from 
	(
		select 
			cast(o.rating_dt as string) as batch_dt,
			o.corp_id ,
			max(o.corp_nm) as corp_nm,
			max(o.credit_code) as credit_code,
			to_date(o.rating_dt) as score_dt,
			max(o.total_score_adjusted) as score,  --舆情分
			max(o1.yq_num) as yq_num,
			max(o1.tmp_score_hit) as tmp_score_hit,
			max(o1.model_version)as model_version,
			min(nvl(tag.importance,0)) as tag_importance
		from mid_opinion_alert_score o --单主体舆情分
		join
		(
			--select distinct batch_dt,corp_id,corp_code,end_dt,nvl(feature_value,0) as yq_num,tmp_score_hit,model_version from
			select batch_dt,
			       corp_id,
				   corp_code,
				   end_dt,
				   max(case feature_name when 'total_num' then feature_value else 0 end) as yq_num,
				   max(tmp_score_hit) as tmp_score_hit,
				   model_version
			from (
				select 
					nvl(batch_dt,'') batch_dt,
					corp_id,
					corp_code,
					end_dt,
					feature_name,
					feature_value,
					model_version,
					case 
						when (feature_name='total_num' and feature_value>=3)  
						  or (feature_name='importance_-3_num' and feature_value>=1) THEN 1
						ELSE
							0
					END as tmp_score_hit
				from (	select distinct 
							to_date(end_dt) as batch_dt,
							corp_id,corp_code,
							end_dt,
							feature_name,feature_value,
							model_version
						from mid_opinion_feat
						where feature_name in ('total_num','importance_-3_num')
					 )f0
			)f group by batch_dt,corp_id,corp_code,end_dt,model_version
		)o1 on o.corp_code=o1.corp_code and o.rating_dt=o1.end_dt
		left join rmp_opinion_risk_info_ o2 
			on o.corp_id=o2.corp_id and to_date(o.rating_dt) = to_date(o2.notice_dt)
		left join (select * from pth_rmp.RMP_OPINION_RISK_INFO_Tag where importance=-3) tag 
			on tag.tag_ii_cd=o2.case_type_ii_cd
		group by o.corp_id,o.rating_dt
	)K
)
select distinct
	-- MD5(concat(E.batch_dt,E.corp_id,'0')) as sid_kw,
	-- E.batch_dt,     --batch_dt来自于模型 单主体舆情分-调整后的模型结果
	E.corp_id,
	E.corp_nm,
	E.credit_code,
	E.score_dt,   --原始模型组提供的值
	E.score,
	E.yq_num,  --不需要刷到oracle
	E.score_hit_ci,   --不需要刷到oracle
	E.score_hit_yq,  --不需要刷到oracle
	E.score_hit,
	E.label_hit,
	--if(E.score_hit_ci=1 or E.label_hit=1,1,0) as alert,  --！！！2022-8-25 上线临时调整，得分预警不考虑舆情数量的限制条件
	--if(E.score_hit=1 or E.label_hit=1,1,0) as alert,  --！！！最终预警，分值预警和风险预警同时  ，备份
	if(E.score_hit=1 or E.label_hit=1,1,0) as alert,  --modify yangcan 20221113
	E.fluctuated,
	E.model_version,
	0 as delete_flag,
	'' as create_by,
	CURRENT_TIMESTAMP() as create_time,
	'' as update_by,
	CURRENT_TIMESTAMP() as update_time,
	0 as version
from 
(
	select 
		batch_dt,
		cast(corp_id as string) as corp_id,
		corp_nm,
		credit_code,
		to_date(score_dt) as score_dt,
		score,
		yq_num,
		if(score>ci,1,0) as score_hit_ci,   --分值预警中间值1
		tmp_score_hit as score_hit_yq,  --分值预警中间值2，用于辅助综合舆情分计算
		CASE 
			when score>ci and tmp_score_hit=1 THEN 1
			else 0 
		END AS score_hit, --分值预警
		label_hit,
		fluctuated,
		model_version
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			credit_code,
			score_dt,
			score,
			yq_num,
			mu + sqrt(sigma_tmp/(14-1)) as ci,  --置信区间下限
			--importance,
			tmp_score_hit,
			label_hit,  --风险预警
			fluctuated,
			model_version
		from 
		(
			select distinct * 
			from 
			(
				select
					batch_dt,
					corp_id,
					corp_nm,
					credit_code,
					score_dt,
					score,
					yq_num,
					tmp_score_hit,  
					label_hit,
					mu,
					model_version,
					--sum(power(b.score-b.mu,2)) over(partition by B.corp_id order by B.yq_num rows between 14 preceding and current row) as sigma_tmp,  --14天舆情分里面剔除舆情数量倒数少的两天，计算12天的舆情分标准差
					case when cnt>=14 then sum(power(b_score-mu,2)) over(partition by corp_id,score_dt )
                         else sum(power(b_score-mu,2)) over(partition by corp_id,score_dt )+(14-cnt)*power(0-mu,2)
						 end as sigma_tmp,  --modify yangcan 20221116
					round((nvl(mu,-0.1)-score)/greatest(abs(nvl(mu,-0.1)),0.1),6) as fluctuated
				from 
				(
					select 
						a.batch_dt,
						a.corp_id,
						a.corp_nm,
						a.credit_code,
						a.score_dt,
						--a.rating_dt,
						a.score, 
						a.yq_num,
						a.tmp_score_hit,
						a.label_hit,
						a.model_version,
						--avg(score) over(partition by corp_id order by yq_num rows between 14 preceding and current row) as mu   --14天舆情分里面剔除舆情数量倒数少的两天,计算12天的舆情分均值
						(sum(b.score) over(partition by a.corp_id,a.score_dt ))/14 as mu,
						-- a.cnt,
						count(b.score_dt) over(partition by a.corp_id,a.score_dt) as cnt,  --实际有数据天数 hz 2022-11-25
						b.score as b_score
					from  label_hit_tab A
					left join label_hit_tab B          --新增逻辑(兼容跑一段时间舆情分 2022-11-23 hz)
						on a.corp_id = b.corp_id 
					where b.score_dt <= a.score_dt    
					  and b.score_dt >= date_add(a.score_dt,-13)
					--   and b.rn<=12
					-- where rn<=12  
				) B 
			)C1
		)C
	)D	
)E 
-- where E.score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1))
where E.score_dt >= to_date(date_add(from_unixtime(unix_timestamp(cast(${beg_dt} as string),'yyyyMMdd')),0))
  and E.score_dt <= to_date(date_add(from_unixtime(unix_timestamp(cast(${end_dt} as string),'yyyyMMdd')),0))
; 


-- 初始化sql(pth_rmp.rmp_alert_score_summ_init) hive执行插入 -- 
insert into pth_rmp.rmp_alert_score_summ_init partition(etl_date=19900101)
select 
	MD5(concat(score_dt,corp_id,'0')) as sid_kw,
	cast(score_dt as string) as batch_dt,
	corp_id ,
	corp_nm  ,
	credit_code  ,
	score_dt  ,
	score  ,
	yq_num ,
	score_hit_ci ,
	score_hit_yq ,
	score_hit ,
	label_hit  ,
	alert  ,
	fluctuated  ,
	model_version ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version
from (select distinct * from pth_rmp.rmp_alert_score_summ_init_impala) a

;