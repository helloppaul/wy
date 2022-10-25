-- RMP_WARNING_SCORE_REPORT 第四段-归因变动 --
-- drop table if exists app_ehzh.rmp_warning_score_report4;  
-- create table app_ehzh.rmp_warning_score_report4 as  --@pth_rmp.rmp_warning_score_report4
--还差 预警等级变动的数据接入进一步验证
--综合预警等级变动层：综合预警等级变动表   因子变动层数据：归因详情当日(主表)+归因详情历史表+预警分模型结果表当日(综合预警等级字段来源)
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
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --预警分--归因详情历史 原始接口
(
	-- 时间限制部分 --
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
	union all 
	-- 非时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 预警等级变动 --
RMP_WARNING_SCORE_CHG_ as 
(
	-- 时间限制部分 --
	select  batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt
	from app_ehzh.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_date) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt
    from app_ehzh.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --特征贡献度_综合预警等级(用于限制当日特征名称)
(
	-- 时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
warn_level_ratio_cfg_ as -- 综合预警等级等级划分档位-配置表
(
	select 
		property_cd,  --1:产业  2:城投
		property,  -- '城投' , '产业'
		warn_lv,   -- '-5','-4','-3','-2','-1'
		percent_desc,  -- 前1% 前1%-10% ...
		warn_lv_desc   -- 绿色预警等级  ...
	from pth_rmp.rmp_warn_level_ratio_cfg
),
warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
(
	select
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- 高风险 ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
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
	where operator = '自动-风险已暴露规则'
	  and ETL_DATE in (select max(etl_date) from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
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
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	-- join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
	-- 	on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
-- 归因详情类数据 -- 
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --取每天最新批次 综合预警-特征贡献度(用于限制今天特征范围，昨天的不用限制)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据（当天数据特征做范围限制）
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
mid_RMP_WARNING_SCORE_DETAIL_HIS as 
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_ main
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
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
		main.last_idx_value, -- used in 简报wy
		main.idx_unit,  -- used
		main.idx_score,  -- used
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
Second_Part_Data as   --因子层数据
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
			contribution_ratio,
			sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
			sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate) as dim_factorEvalu_contrib_ratio,
			dim_warn_level,
			dim_warn_level_desc,  --维度风险等级(难点)
			type,
			factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
			idx_name,  -- 异常因子/异常指标
			feature_name_target,
			idx_value,
			last_idx_value,
			idx_unit,
			idx_score,   --指标评分 used
			concat(feature_name_target,'为',cast(idx_value as string),idx_unit) as idx_desc,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
-- 综合预警等级别动类数据 --
RMP_WARNING_SCORE_CHG_Batch as  --取每天最新批次的预警变动等级数据
(
	select a.*
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
Fourth_Part_Data_synth_warnlevel as   --综合预警 等级变动(限定了预警等级变动为上升，如果没有上升，则第四段落不显示)
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_date as score_dt,
		a.synth_warnlevel,   --当日综合预警等级
		cfg.warn_lv_desc as synth_warnlevel_desc,   -- used
		a.chg_direction,
		a.synth_warnlevel_l,  --昨日综合预警等级
		cfg_l.warn_lv_desc as synth_warnlevel_l_desc   -- used
	from RMP_WARNING_SCORE_CHG_Batch a 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg 
		on cast(a.synth_warnlevel as string)=cfg.warn_lv 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_l
		on cast(a.synth_warnlevel_l as string)=cfg_l.warn_lv
	where a.chg_direction='1' -- 上升(恶化)  --a.chg_direction='上升'
),
-- 维度风险等级变动类数据 & 因子特征评分变动类数据 --
RMP_WARNING_dim_warn_lv_And_idx_score_chg as   --取每天最新批次的维度风险等级变动 以及 特征评分变动 数据 - 因子层
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.dimension,
		a.dimension_ch,
		a.type,
		a.dim_contrib_ratio,   --维度贡献度占比(排序用) used
		a.dim_warn_level,	  --今日维度风险等级
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --昨日维度风险等级
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) >0 then '上升'
			else ''
		end as dim_warn_level_chg_desc,
		a.factor_evaluate,		
		a.idx_name, 
		a.idx_value,
		a.last_idx_value,
		a.feature_name_target,
		a.idx_unit,
		a.contribution_ratio,
		a.idx_score,   -- 今日指标打分
		b.idx_score as idx_score_1, -- 昨日指标打分
		case 
			when cast(a.idx_score as float)-cast(b.idx_score as float) >0 then '恶化'  --指标层 特征评分卡得分变高则为恶化
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join mid_RMP_WARNING_SCORE_DETAIL_HIS b
		on  a.corp_id=b.corp_id 
			and to_date(date_add(a.score_dt,-1)) = b.score_dt
			and a.dimension=b.dimension
),
Fourth_Part_Data_dim_warn_level_And_idx_score as    --因子层，（1）计算某个维度是否恶化 （2）统计异常因子和正常因子数量
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		first_value(dimension_ch) over(partition by batch_dt,corp_id,score_dt order by dim_contrib_ratio desc) as max_dimension,
		type,
		dim_contrib_ratio,  --维度贡献度占比(排序用) used
		dim_warn_level,  --今日维度风险等级
		dim_warn_level_desc,
		dim_warn_level_1,  --昨日维度风险等级
		dim_warn_level_1_desc,
		dim_warn_level_chg_desc,  --维度风险等级变动 描述
		idx_name,
		feature_name_target,
		idx_value,
		idx_unit,
		contribution_ratio,
		idx_score,
		idx_score_1,
		idx_score_chg_desc,
		max(idx_score_chg_desc) over(partition by batch_dt,corp_id,score_dt,dimension) as dim_idx_score_chg_desc,  --维度层指标是否恶化
		count(idx_name) over(partition by batch_dt,corp_id,score_dt,dimension,idx_score_chg_desc) as dim_idx_score_cnt  --按照得分恶化和非恶化分别统计指标数量
		-- row_number() over(partition by batch_dt,corp_id,score_dt order by dim_contrib_ratio desc) as dim_contrib_ratio_rank
	from RMP_WARNING_dim_warn_lv_And_idx_score_chg
),
-- 大宽表 关联 综合预警等级数据 & 维度变动和因子变动类数据 --
Fourth_Part_Data_idx_name as   --关联 综合预警等级数据 & 维度变动和因子变动类数据  （大宽表）
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.synth_warnlevel_desc,
		'上升' as chg_direction_desc,  --只展示预警等级上升了的企业，否则第四段整段不展示
		a.chg_direction,
		a.synth_warnlevel_l,
		a.synth_warnlevel_l_desc,
		b.dimension,
		b.dimension_ch,
		b.type,
		b.dim_contrib_ratio,  --维度贡献度占比(排序用) used
		b.dim_warn_level,  --今日维度风险等级
		b.dim_warn_level_desc,
		b.dim_warn_level_1,  --昨日维度风险等级
		b.dim_warn_level_1_desc,
		b.dim_warn_level_chg_desc,  --维度风险等级变动 描述
		b.idx_name,
		b.feature_name_target,
		b.idx_value,
		b.idx_unit,
		b.contribution_ratio,
		concat(b.feature_name_target,'为',cast(b.idx_value as string),b.idx_unit) as idx_desc,
		b.idx_score,
		b.idx_score_1,
		b.idx_score_chg_desc,    --指标层，恶化
		b.dim_idx_score_chg_desc, ----维度层，有一个恶化则为恶化
		b.dim_idx_score_cnt,    --used
		case 
			when b.idx_score_chg_desc='恶化' then 
				concat('有',cast(b.dim_idx_score_cnt as string),'个指标发生',b.dim_idx_score_chg_desc)
			else 
				''
		end as dim_idx_score_desc  --维度层 打分 情况描述  used
		-- b.dim_contrib_ratio_rank
	from Fourth_Part_Data_synth_warnlevel a   --综合预警等级变动类数据
	left join Fourth_Part_Data_dim_warn_level_And_idx_score b --维度风险等级以及指标类数据
		on  a.batch_dt=b.batch_dt
			and a.corp_id=b.corp_id 
			and a.score_dt=b.score_dt
			and cast(a.synth_warnlevel as string)=cast(b.synth_warnlevel as string)   --维度层风险等级变化 要求 限定与当日预警等级相等
),
MID_Data_Summ as   --与Fourth_Part_Data_idx_name数据结构基本相同，选取了后续加工必要字段
(
	select distinct 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,

		synth_warnlevel,
		synth_warnlevel_desc,
		chg_direction_desc,
		synth_warnlevel_l,
		synth_warnlevel_l_desc,

		dimension,
		dimension_ch,
		dim_contrib_ratio,

		dim_warn_level,  --今日维度风险等级
		dim_warn_level_desc,
		dim_warn_level_chg_desc,   --今日相比昨日风险等级变动描述
		dim_warn_level_1,  --昨日维度风险等级
		dim_warn_level_1_desc,

		dim_idx_score_cnt,
		dim_idx_score_desc,   -- 样例：有x个指标发生恶化

		type,

		idx_desc,   --指标描述
		idx_score_chg_desc,
		contribution_ratio
	from Fourth_Part_Data_idx_name
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
Fourth_Part_Data_Dim_type_ as   --恶化指标数据
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		type,
		idx_desc
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
			type,
			idx_desc,
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from MID_Data_Summ
		where idx_score_chg_desc = '恶化'
	)A where rm<=5  --取贡献度排名前5大的恶化指标作为展示
),
Fourth_Part_Data_Dim_type as  --恶化指标汇总到type层描述
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		type,
		-- concat_ws('、',collect_set(idx_desc)) as idx_desc_in_one_type  -- hive
		group_concat(distinct idx_desc,'、') as idx_desc_in_one_type    -- impala
	from Fourth_Part_Data_Dim_type_
	group by batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel,dimension,dimension_ch,type
),
Fourth_Part_Data_Dim as -- 汇总到维度层 （仅展示）
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,

		synth_warnlevel,
		synth_warnlevel_desc,
		chg_direction_desc,
		synth_warnlevel_l,
		synth_warnlevel_l_desc,

		dimension,
		dimension_ch,
		dim_contrib_ratio,

		dim_warn_level,  --今日维度风险等级
		dim_warn_level_desc,
		dim_warn_level_chg_desc,   --今日相比昨日风险等级变动描述
		dim_warn_level_1,  --昨日维度风险等级
		dim_warn_level_1_desc,

		dim_idx_score_cnt,
		dim_idx_score_desc    -- 样例：有x个指标发生恶化
	from MID_Data_Summ
),
-- 第四段信息 --
Fourth_Msg_Dim as --恶化指标汇总到维度层描述
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		-- concat(concat_ws('；',collect_set(dim_type_msg)),'。') as idx_desc_one_row   -- hive 
		concat(group_concat(distinct dim_type_msg,'；'),'。') as idx_desc_in_one_dimension  --impala
	from
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			synth_warnlevel,
			dimension,
			dimension_ch,
			type,
			concat(
				type,'恶化：',idx_desc_in_one_type
			) as dim_type_msg,
			idx_desc_in_one_type
		from Fourth_Part_Data_Dim_type
	) A 
	group by batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel,dimension,dimension_ch
),
Fourth_Msg_ as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.synth_warnlevel_desc,
		a.chg_direction_desc,
		a.synth_warnlevel_l,
		a.synth_warnlevel_l_desc,
		a.dimension,
		a.dimension_ch,
		case 
			when b.dimension_ch is null then --说明此时无维度风险描述，则罗列每一个维度的情况
				concat(
					'风险水平上升的主要维度为','财务维度。'
				)
			when a.dim_warn_level_chg_desc<>'上升'  then 
				concat(
					'风险水平上升的主要维度为','经营维度。'
				)
			else
				concat(
					a.dimension_ch,'维度','由',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'至',a.dim_warn_level_desc,'，',
					if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'中',a.dim_idx_score_desc)),
					'具体表现为：',b.idx_desc_in_one_dimension
				) 
		end  as msg_dim
	from Fourth_Part_Data_Dim a   -- 大宽表
	left join Fourth_Msg_Dim b   -- 仅 维度风险等级以及异常指标汇到维度层后数据
		on  a.corp_id=b.corp_id 
			and a.batch_dt=b.batch_dt 
			and a.dimension=b.dimension 
),
Fourth_Msg_corp_ as    --汇总到企业层（维度风险变动+指标恶化 数据）
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel_desc,
		chg_direction_desc,
		synth_warnlevel_l_desc,
		-- concat_ws('。',collect_set(msg_dim)) as msg_corp_  -- hive
		group_concat(distinct msg_dim,'。') as msg_corp_   -- impala
	from Fourth_Msg_
	group by batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel_desc,chg_direction_desc,synth_warnlevel_l_desc
),
Fourth_Msg_Corp as  --汇总到企业层（预警等级变动+维度风险变动+指标恶化 数据）
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel_desc,
		chg_direction_desc,
		synth_warnlevel_l_desc,
		concat( 
			'相较于前一天，','预警等级由',synth_warnlevel_l_desc,chg_direction_desc,'至',synth_warnlevel_desc,'，',
			msg_corp_
		) as msg_no_color,
		concat( 
			'相较于前一天，','预警等级由','<span class="RED"><span class="WEIGHT">',synth_warnlevel_l_desc,chg_direction_desc,'至',synth_warnlevel_desc,'</span></span>','，',
			msg_corp_
		) as msg
	from Fourth_Msg_corp_
)
select * from Fourth_Msg_Corp
;
