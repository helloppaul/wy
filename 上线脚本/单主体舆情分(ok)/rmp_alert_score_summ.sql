-- 单主体舆情分 rmp_alert_score_summ (同步方式：一天多批次插入)--
--入参：${ETL_DATE}(20220818 int)
with 
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  join (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main ) b2
		  	on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
modelres_adjusted_senti_self_ as 
(
	select a.*,chg.corp_id,chg.corp_name as corp_nm,chg.credit_code
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj_intf a --app_ehzh.rsk_rmp_warncntr_opnwrn_rslt_sentiself_adj a --modelres_adjusted_senti_self a
	join corp_chg chg on chg.source_id = cast(a.corp_code as string)
	where chg.source_code='FI' 
),
featvalue_senti_self_ as 
(
	select a.*,chg.corp_id,chg.corp_name as corp_nm 
	from hds.tr_ods_ais_me_rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf a --app_ehzh.rsk_rmp_warncntr_opnwrn_feat_sentiself_val_intf a --featvalue_senti_self a
	join corp_chg chg on chg.source_id = cast(a.corp_code as string)
	where chg.source_code='FI' 
),
label_hit_tab AS
(
	select 
		batch_dt,corp_id,corp_nm,credit_code,score_dt,score,yq_num,tmp_score_hit,model_version,
		if(tag_importance=-3,1,0) as label_hit  --风险预警
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
		from modelres_adjusted_senti_self_ o --单主体舆情分
		join
		(
			select distinct batch_dt,corp_id,corp_code,end_dt,nvl(feature_value,0) as yq_num,tmp_score_hit,model_version from
			(
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
						from featvalue_senti_self_
						where feature_name in ('total_num','importance_-3_num')
					 )f0
			)f where feature_name='total_num' 
		)o1 on o.corp_code=o1.corp_code and o.rating_dt=o1.end_dt
		left join pth_rmp.RMP_OPINION_RISK_INFO o2 
			on o.corp_id=o2.corp_id and to_date(o.rating_dt) = to_date(o2.notice_dt)
		left join (select * from pth_rmp.RMP_OPINION_RISK_INFO_Tag where importance=-3) tag 
			on tag.tag_ii_cd=o2.case_type_ii_cd
		group by o.corp_id,o.rating_dt
	)K
)
insert into pth_rmp.rmp_alert_score_summ
select
	MD5(concat(E.batch_dt,E.corp_id,'0')) as sid_kw,
	E.batch_dt,     --batch_dt来自于模型 单主体舆情分-调整后的模型结果
	E.corp_id,
	E.corp_nm,
	E.credit_code,
	E.score_dt,   --原始模型组提供的值
	E.score,
	E.score_hit_ci,
	E.score_hit_yq,
	E.score_hit,
	E.label_hit,
	if(E.score_hit_ci=1 or E.label_hit=1,1,0) as alert,  --！！！2022-8-25 上线临时调整，得分预警不考虑舆情数量的限制条件
	--if(E.score_hit=1 or E.label_hit=1,1,0) as alert,  --！！！最终预警，分值预警和风险预警同时  ，备份
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
			mu + sqrt(sigma_tmp/12-1) as ci,  --置信区间下限
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
					B.batch_dt,
					B.corp_id,
					B.corp_nm,
					credit_code,
					B.score_dt,
					B.score,
					B.tmp_score_hit,  
					B.label_hit,
					B.mu,
					B.model_version,
					sum(power(b.score-b.mu,2)) over(partition by B.corp_id order by B.yq_num rows between 12 preceding and current row) as sigma_tmp,  --14天舆情分里面剔除舆情数量倒数少的两天，计算12天的舆情分标准差
					round((nvl(B.mu,-0.1)-score)/greatest(abs(nvl(B.mu,-0.1)),0.1),6) as fluctuated
				from 
				(
					select 
						batch_dt,
						corp_id,
						corp_nm,
						credit_code,
						score_dt,
						--rating_dt,
						score, 
						tmp_score_hit,
						label_hit,
						yq_num,
						model_version,
						avg(score) over(partition by corp_id order by yq_num rows between 12 preceding and current row) as mu   --14天舆情分里面剔除舆情数量倒数少的两天,计算12天的舆情分均值
					from  label_hit_tab A 
				) B 
			)C1
		)C
	)D	
)E where E.score_dt = from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
; 