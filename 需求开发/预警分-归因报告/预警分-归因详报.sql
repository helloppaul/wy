-- RMP_WARNING_SCORE_REPORT (同步方式：一天多批次插入) --
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业 的特殊corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  where cid1.etl_date = (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance)
			-- on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  where b1.etl_date = (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main )
		  	-- on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
_RMP_WARNING_SCORE_MODEL_ as   -- 预警分-模型结果表 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
_RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
),
-- -- 特征贡献度 --
-- _rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --特征贡献度_高频
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --特征贡献度_低频
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --特征贡献度_中频城投
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --特征贡献度_中频产业
-- (
-- 	select *
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf
-- ),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
_warn_level_ratio_cfg_ as -- 综合预警等级等级划分档位-配置表
(
	select '-5' as warn_lv,'前10%' as percent_desc,'风险已暴露' as warn_lv_desc
	union all 
	select '-4' as warn_lv,'10%-30%' as percent_desc,'红色预警等级' as warn_lv_desc
	union all 
	select '-3' as warn_lv,'30%-60%' as percent_desc,'橙色预警等级' as warn_lv_desc
	union all 
	select '-2' as warn_lv,'60%-80%' as percent_desc,'黄色预警等级' as warn_lv_desc
	union all 
	select '-1' as warn_lv,'80%-100%' as percent_desc,'绿色预警等级' as warn_lv_desc
),
-- _warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
-- (
-- 	select 60 as low_contribution_ratio,100 as high_contribution_ratio,'高风险' as risk_lv_desc   --[60,100)
-- 	union all  
-- 	select 40 as low_contribution_ratio,60 as high_contribution_ratio,'中风险' as risk_lv_desc   --[40,60)
-- 	union all  
-- 	select 0 as low_contribution_ratio, 40 as high_contribution_ratio,'低风险' as risk_lv_desc   --<40
-- ),
feat_CFG as --特征手工配置表
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,
        feature_name_target,  --used
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type<>'中频城投'
    union all 
    select 
        feature_cd,
        feature_name,
        '中频-城投' as sub_model_type,
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type='中频城投'
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
First_Part_Data as  --适用 预警分-归因简报的数据
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.credit_cd,
		main.synth_warnlevel,  --综合预警等级 used
		chg.bond_type,  --1:产业债 2:城投债
		case chg.bond_type
			when 2 then '城投平台'
			else '产业主体'
		end as corp_bond_type,  --主体属性 used
		cfg.warn_lv_desc, --预警等级描述 used
		cfg.percent_desc  --预警等级档位百分比划分 used
	from _RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='FI') chg
		on main.corp_id=chg.corp_id
	join _warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv
),
Second_Part_Data_Prepare as 
(
	select 
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		nvl(a.synth_warnlevel,'0') as synth_warnlevel, --综合预警等级
		main.dimension,
		main.type,  	-- used
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.idx_unit,  -- used
		f_cfg.feature_name_target,  --特征名称-目标(系统)  used
		main.contribution_ratio,
		main.factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
		main.dim_warn_level  --维度风险等级(难点)
	from _RMP_WARNING_SCORE_DETAIL_ main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join _RMP_WARNING_SCORE_MODEL_ a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
),
Second_Part_Data as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dim_contrib_ratio,
		dim_risk_lv,  --维度风险等级(难点)
		type,
		factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
		dim_evalu_contribution_ratio,  --个维度且因子评价下的 因子贡献度占比汇总
		idx_name,  -- 异常因子/异常指标
		factor_evaluate		
	from
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
First_Msg as --第一段信息
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			'该主体预测风险水平处于',corp_bond_type,'中',percent_desc,',',
			'属',warn_lv_desc
		) as sentence_1  --第一句话
	from First_Part_Data
)

