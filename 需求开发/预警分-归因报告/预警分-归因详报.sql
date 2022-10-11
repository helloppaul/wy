-- RMP_WARNING_SCORE_REPORT (同步方式：一天多批次插入) --
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业 的特殊corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
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
RMP_WARNING_SCORE_MODEL_ as   -- 预警分-模型结果表 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
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
warn_level_ratio_cfg_ as -- 综合预警等级等级划分档位-配置表
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
warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
(
	select 60 as low_contribution_percent,100 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc   --(60,100]
	union all  
	select 40 as low_contribution_percent,60 as high_contribution_percent,-2 as risk_lv,'中风险' as risk_lv_desc   --(40,60]
	union all  
	select 0 as low_contribution_percent, 40 as high_contribution_percent,-1 as risk_lv,'低风险' as risk_lv_desc   --(0,40]
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
-- 第一段数据 --
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
	from RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='FI') chg
		on main.corp_id=chg.corp_id
	join warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv
),
-- 第二段数据 --
RMP_WARNING_SCORE_DETAIL_Batch as 
(
	select
	from RMP_WARNING_SCORE_DETAIL_
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.delete_flag=0
),
_RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数（只有贡献度占比数据，其余均为空，不下钻至因子层面据
(
	select *
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_MODEL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.delete_flag=0
),
Second_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		nvl(a.synth_warnlevel,'0') as synth_warnlevel, --综合预警等级
		main.dimension,    --维度编码
		f_cfg.dimension as dimension_ch,  --维度名称
		main.type,  	-- used
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.idx_unit,  -- used
		f_cfg.feature_name_target,  --特征名称-目标(系统)  used
		main.contribution_ratio,
		main.factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
		cfg.risk_lv_desc as dim_warn_level_desc  --维度风险等级(难点)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join _RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)

),
Second_Part_Data as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		-- sum(contribution_ratio) as dim_contrib_ratio,
		sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
		sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate) as dim_factorEvalu_contrib_ratio,
		dim_warn_level_desc,  --维度风险等级(难点)
		type,
		factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
		idx_name,  -- 异常因子/异常指标
		idx_value,
		idx_unit,
		concat(idx_name,'为',cast(idx_value as string),idx_unit) as idx_desc,
		count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
		count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
	from Second_Part_Data_Prepare 
	order by corp_id,score_dt desc,dim_contrib_ratio desc
),
Second_Part_Data_Dimension as -- 按维度层汇总描述用数据
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		dim_factorEvalu_contrib_ratio,
		dim_warn_level_desc,
		dim_factor_cnt,
		dim_factorEvalu_factor_cnt
	from Second_Part_Data
	where factor_evaluate = 0
),
Second_Part_Data_Dimension_Type as -- 按维度层 以及 类别层汇总描述用数据
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		-- concat_ws('、',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive 
		group_concat(idx_desc,'、') as idx_desc_in_one_type    -- impala
	from Second_Part_Data
	where factor_evaluate = 0
	group by corp_id,corp_nm,batch_dt,score_dt,dimension,dimension_ch,type
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 第一段信息 --
First_Msg as --
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
),
-- 第二段信息 --
Second_Msg_Dimension as  -- 维度层的信息描述
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		concat(
			dimension_ch,'维度','（','贡献度占比',cast(round(dim_contrib_ratio*100,0) as string),'%','）','，',
			'该维度当前处于',dim_warn_level_desc,'风险等级','，',
			dimension_ch,'维度','纳入的',cast(dim_factor_cnt as string),'个指标中','，',cast(dim_factorEvalu_factor_cnt as string),'个指标表现异常','，',
			'异常指标对主体总体风险贡献度为',cast(round(dim_factorEvalu_contrib_ratio*100,0) as string) ,'%','，'
		) as dim_msg
	from Second_Part_Data_Dimension
),
Second_Msg_Dimension_Type as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		-- concat(concat_ws('；',collect_set(dim_type_msg)),'。') as idx_desc_one_row   -- hive 
		concat(group_concat(dim_type_msg,'；'),'。') as idx_desc_in_one_dimension  --impala
	from
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			concat(
				type,'异常：',idx_desc_in_one_type
			) as dim_type_msg
		from Second_Part_Data_Dimension_Type
	)A 
	group by corp_id,corp_nm,batch_dt,score_dt,dimension,dimension_ch
),
Second_Msg_I as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.corp_id=b.corp_id and a.dimension=b.dimension
),
-- Second_Msg as 
-- (
-- 	select *
-- 	from Second_Msg_I
-- 	group by 
-- )