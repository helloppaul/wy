-- RMP_WARNING_SCORE_S_REPORT_ZX 归因简报zx --
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业/证监会一级行业 的特殊corp_chg  (特殊2)
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,  --1 产业债 2 城投债
	b.industryphy_name,
	b.zjh_industry_l1 
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
-- 时间限制开关 --
timeLimit_switch as 
(
    select True as flag   --TRUE:时间约束，FLASE:时间不做约束，通常用于初始化
    -- select False as flag
),
-- 预警分 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --预警分_融合调整后综合  原始接口
(
    -- 时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
feat_CFG as  --特征手工配置表
(
    select distinct
        feature_cd,
        feature_name,
        substr(sub_model_type,1,6) as sub_model_type,  --取前两个中文字符
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type not in ('中频-产业','中频-城投','无监督')
    union all 
    select distinct
        feature_cd,
        feature_name,
        sub_model_type,
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type in ('中频-产业','中频-城投','无监督')
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
RMP_WARNING_SCORE_MODEL_ as  --预警分-模型结果表
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 预警分
		a.interval_text_adjusted,
		case a.interval_text_adjusted
			when '绿色预警' then '-1' 
			when '黄色预警' then '-2'
			when '橙色预警' then '-3'
			when '红色预警' then '-4'
			when '风险已暴露' then '-5'
		end as synth_warnlevel,  -- 综合预警等级,
		case
			when a.interval_text_adjusted in ('绿色预警','黄色预警') then 
				'-1'   --低风险
			when a.interval_text_adjusted  = '橙色预警' then 
				'-2'  --中风险
			when a.interval_text_adjusted  ='红色预警' then 
				'-3'  --高风险
			when a.interval_text_adjusted  ='风险已暴露' then 
				'-4'   --风险已暴露
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a   
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据（当天数据做范围限制）
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	-- where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- 模型外挂规则 --
warn_adj_rule_cfg as --预警分-模型外挂规则配置表   取最新etl_date的数据 (更新频率:日度更新)
(
	select distinct
		a.etl_date,
		b.corp_id, 
		b.corp_name as corp_nm,
		a.category,
		a.reason
	from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
	join corp_chg b 
		on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
	where a.operator = '自动-风险已暴露规则'
	  and a.ETL_DATE in (select max(etl_date) from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
s_report_Data_Prepare as 
(
	select distinct *
	from 
	(
		select 
			main.batch_dt,
			main.corp_id,
			main.corp_nm,
			main.score_dt,
			a.interval_text_adjusted,
			-- nvl(a.synth_warnlevel,'0') as synth_warnlevel, --综合预警等级
			main.dimension,    --维度编码
			sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,f_cfg.dimension) as dim_contrib_ratio,
			nvl(f_cfg.dimension,'') as dimension_ch,  --维度名称
			main.type,  	-- used
			main.idx_name,  -- used 
			main.idx_value,  -- used
			main.last_idx_value, -- used in 简报wy
			main.idx_unit,  -- used 
			main.idx_score,  -- used
			nvl(f_cfg.feature_name_target,'') as feature_name_target,  --特征名称-目标(系统)  used
			main.contribution_ratio,
			main.factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
			nvl(ru.category,'') as category_nvl,
			nvl(ru.reason,'') as reason_nvl
		from RMP_WARNING_SCORE_DETAIL_Batch main
		left join feat_CFG f_cfg 	
			on main.idx_name=f_cfg.feature_cd
		join RMP_WARNING_SCORE_MODEL_Batch a
			on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
		left join warn_adj_rule_cfg  ru
			on a.corp_id = ru.corp_id
	)A
),
s_report_Data_dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		max(category_nvl) as category_nvl,
		max(reason_nvl) as reason_nvl,
		interval_text_adjusted,  --原始模型产出的预警等级
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		-- concat_ws('、',collect_set(feature_name_target))  as abnormal_idx_desc, -- hive
		group_concat(feature_name_target,'、')  as abnormal_idx_desc,  -- impala 
		''  as abnormal_risk_info_desc
	from s_report_Data_Prepare
	where factor_evaluate = 0
	group by batch_dt,corp_id,corp_nm,score_dt,interval_text_adjusted,dimension,dimension_ch,dim_contrib_ratio
	order by dim_contrib_ratio desc
),
s_report_msg as 
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat(		
			'最新信用违约预警处于',interval_text_adjusted,'等级','，',
			if(reason_nvl<>'',concat('主要由于发生',reason_nvl,'同时'),''),
			'风险涉及',dimension_ch,'（','贡献度占比',cast(dim_contrib_ratio as string),'%','）','，',
			case 
				when  abnormal_idx_desc<>'' then 
					concat('异常指标包括：',abnormal_idx_desc)
				else 
					''
			end,'，',
			case 
				when  abnormal_risk_info_desc<>'' then 
					concat('异常事件包括：',abnormal_risk_info_desc)
				else 
					''
			end
		) as msg
	from s_report_Data_dim
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
-- insert into pth_rmp.WARNING_SCORE_S_REPORT_ZX 
select 
	-- concat(corp_id,md5(concat(batch_dt,corp_id))) as sid_kw,  -- hive
	'' as sid_kw,  -- impala
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg as report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_report_msg
;

