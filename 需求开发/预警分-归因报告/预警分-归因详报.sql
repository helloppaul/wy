-- RMP_WARNING_SCORE_REPORT (同步方式：一天多批次插入) --
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
-- rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_HIS_
-- (

-- ),
RMP_WARNING_SCORE_MODEL_ as  --预警分-模型结果表
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 预警分
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
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
-- RMP_WARNING_SCORE_MODEL_ as   -- 预警分-模型结果表 原始接口
-- (
-- 	select * 
-- 	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
-- ),
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --预警分--归因详情历史 原始接口
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
rmp_cs_compy_region_ as   -- 区域经营数据 (每日全量采集)
(
	select a.*
	from hds.t_ods_rmp_cs_compy_region a 
	where a.isdel=0
	  and a.etl_date in (select max(etl_date) as max_etl_date from hds.t_ods_rmp_cs_compy_region)
),
RMP_WARNING_SCORE_CHG_ as 
(
	select *
	from app_ehzh.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	where delete_flag=0
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
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
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
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --维度风险等级(难点)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
),
Second_Part_Data as 
(
	select distinct *
	from 
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
			dim_warn_level,
			dim_warn_level_desc,  --维度风险等级(难点)
			type,
			factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
			idx_name,  -- 异常因子/异常指标
			idx_value,
			idx_unit,
			idx_score,   --指标评分 used
			concat(idx_name,'为',cast(idx_value as string),idx_unit) as idx_desc,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
Second_Part_Data_Dimension as -- 按维度层汇总描述用数据
(
	select 
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
-- 第三段数据 --
mid_rmp_cs_compy_region_ as 
(
	select distinct
		b.corp_id,
		b.corp_name as corp_nm,
		a.region_cd,
		a.client_id
	from rmp_cs_compy_region_ a 
	join (select * from corp_chg where source_code='CSCS') b 
		on a.company_id=b.source_id 
),
Third_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		main.synth_warnlevel,  -- 综合预警等级 used
		chg.bond_type,
		chg.zjh_industry_l1
	from RMP_WARNING_SCORE_MODEL_Batch main 
	join (select * from corp_chg where source_code='ZXZX') chg 
		on main.corp_id=chg.corp_id
),
Third_Part_Data_CY_Prepare as   -- 主体为产业的数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type,   -- 属性1：产业
		'' as bond_type_desc,
		a.zjh_industry_l1 as corp_property,  -- 属性2：行业
		a.zjh_industry_l1 as corp_property_desc,
		b.corp_id as same_property_corp_id,   --主体为产业债性质 的 同行业且综合预警等级相等 的 企业
		b.corp_name as same_property_corp_nm
	from Third_Part_Data_Prepare a
	join (select * from Third_Part_Data_Prepare where bond_type = 1) b 
		on  a.zjh_industry_l1= b.zjh_industry_l1 and a.synth_warnlevel=b.synth_warnlevel  --综合预警等级相同的企业
	where a.bond_type = 1  --产业债
	  and a.corp_id<>b.corp_id
),
Third_Part_Data_CY as    -- 和产业主体相同属性的 其他企业数量 计算
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as rm,
		count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CY_Prepare
),
Third_Part_Data_CT_Prepare_I as -- 主体 为 城投的数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 
		b.region_cd
	from Third_Part_Data_Prepare a
	join mid_rmp_cs_compy_region_ b
		on  a.corp_id = b.corp_id
	where a.bond_type=2  -- 城投 
),
Third_Part_Data_CT_Prepare_II as 
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 	 -- 属性1：城投 
		a.region_cd as corp_property,   
		'同区域、同行政级别且' as corp_property_desc,    -- 属性2：同区域、同行政级别
		b.corp_id as same_property_corp_id,
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_CT_Prepare_I a 
	join Third_Part_Data_CT_Prepare_I b
		on a.region_cd=b.region_cd and a.synth_warnlevel=b.synth_warnlevel
	where a.corp_id<>b.corp_id
),
Third_Part_Data_CT as -- 和城投主体相同属性的 其他企业数量 计算
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as rm,
		count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CT_Prepare_II
),
Third_Part_Data as 
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		rm,
		corp_id_cnt
	from 
	(
		select *
		from Third_Part_Data_CY
		UNION ALL 
		select *
		from Third_Part_Data_CT
	) A where rm<=5
),
-- 第四段数据 --
RMP_WARNING_SCORE_CHG_Batch as  --取每天最新批次的预警变动等级数据
(
	select *
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
Fourth_Part_Data_synth_warnlevel as   --综合预警 等级变动
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_date as score_dt,
		a.synth_warnlevel,   --当日综合预警等级
		chg.warn_lv_desc as synth_warnlevel_desc,   -- used
		a.chg_direction,
		a.synth_warnlevel_l  --昨日综合预警等级
		cfg_l.warn_lv as synth_warnlevel_l_desc   -- used
	from RMP_WARNING_SCORE_CHG_Batch a 
	join warn_level_ratio_cfg_ cfg 
		on a.synth_warnlevel=b.warn_lv
	join warn_level_ratio_cfg_ cfg_l
		on a.synth_warnlevel_l=cfg_l.warn_lv
	where a.chg_direction='上升'
),
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --取每天最新批次的维度风险等级变动 以及 特征评分变动 数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dim_contrib_ratio,   --维度贡献度占比(排序用) used
		a.dim_warn_level,	  --今日维度风险等级
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --昨日维度风险等级
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) >0 then '上升'
			else ''
		end as dim_warn_level_chg_desc,
		
		a.idx_name, 
		a.idx_value,
		a.idx_unit,
		a.idx_score,   -- 今日指标打分
		b.idx_score as idx_score_1, -- 昨日指标打分
		case 
			when cast(a.idx_score as int)-cast(b.idx_score as int) >0 then '恶化'  --特征评分卡得分变高则为恶化
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join RMP_WARNING_SCORE_DETAIL_HIS_ b
		on a.corp_id=b.corp_id and unix_timestamp(to_date(a.score_dt),'yyyy-MM-dd')-1=unix_timestamp(to_date(b.score_dt),'yyyy-MM-dd') and a.dimension=b.dimension
),
Fourth_Part_Data_dim_warn_level_And_idx_score as  
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dim_contrib_ratio,  --维度贡献度占比(排序用) used
		dim_warn_level,  --今日维度风险等级
		dim_warn_level_desc,
		dim_warn_level_1,  --昨日维度风险等级
		dim_warn_level_1_desc,
		dim_warn_level_chg_desc,  --维度风险等级变动 描述
		idx_name,
		idx_value,
		idx_unit,
		idx_score,
		idx_score_1,
		idx_score_chg_desc,
		row_number() over(partition by corp_id,score_dt,dimension order by dim_contrib_ratio desc) as dim_contrib_ratio_rank
	from RMP_WARNING_dim_warn_lv_And_idx_score_chg
),
Fourth_Part_Data as 
(
	select 
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.synth_warnlevel_desc,
		a.chg_direction as chg_direction_desc,
		a.synth_warnlevel_l,
		a.synth_warnlevel_l_desc,
		b.dimension,
		b.dim_contrib_ratio,  --维度贡献度占比(排序用) used
		b.dim_warn_level,  --今日维度风险等级
		b.dim_warn_level_desc,
		b.dim_warn_level_1,  --昨日维度风险等级
		b.dim_warn_level_1_desc,
		b.dim_warn_level_chg_desc,  --维度风险等级变动 描述
		b.idx_name,
		b.idx_value,
		b.idx_unit,
		b.idx_score,
		b.idx_score_1,
		b.idx_score_chg_desc,
		b.dim_contrib_ratio_rank
	from Fourth_Part_Data_synth_warnlevel a 
	join Fourth_Part_Data_dim_warn_level_And_idx_score b 
		on a.corp_id=b.corp_id and a.score_dt=b.score_dt
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
Second_Msg_Dim as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg_dim
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.corp_id=b.corp_id and  a.batch_dt=b.batch_dt and a.dimension=b.dimension
),
Second_Msg as    --！！！还未对 贡献度占比 从大到小排序
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('\\r\\n',collect_set(msg_dim)) as msg
		group_concat(msg_dim,'\\r\\n') as msg
	from Second_Msg_Dim
	group by corp_id,corp_nm,batch_dt,score_dt
),
-- 第三段信息 --
Third_Msg_Corp as --将 和主体相同属性的企业合并为一行
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		group_concat(same_property_corp_nm,'、') as same_property_corp_nm_in_one_row
	from Third_Part_Data
	group by batch_dt,corp_id,corp_nm,score_dt
),
Third_Msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		concat(
			a.bond_type_desc,'中',a.corp_property_desc,
			'总体风险水平表现一致的企业还包括：',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'等',''),
			cast(corp_id_cnt as string),'家企业。'
		) as msg
	from Third_Part_Data a 
	join Third_Msg_Corp b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id
),
-- 第四段信息 --
Fourth_Msg_Dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		-- case 
		-- 	when dim_contrib_ratio_rank = 1 then 
		-- 		concat('主要由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		-- end as first_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 2 then 
		-- 		concat('其次由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		-- end as second_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 3 then 
		-- 		concat('第三由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		-- end as third_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 4 then 
		-- 		concat('第四由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		-- end as fourth_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 5 then 
		-- 		concat('第五由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		-- end as fifth_reason, 

		concat(
			case 
				when dim_contrib_ratio_rank = 1 then 
					concat('主要由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 2 then 
					concat('其次由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 3 then 
					concat('第三由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 4 then 
					concat('第四由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 5 then 
					concat('第五由于',dimension,'由',dim_warn_level_1_desc,dim_warn_level_chg_desc,'至',dim_warn_level_desc)
		) as msg_dim
	from Fourth_Part_Data

)



