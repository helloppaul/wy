-- rmp_warning_score_s_report 归因简报 --
--/* 2022-10-24 新增颜色逻辑 */
--/* 2022-10-24 新增外挂规则逻辑 */
--/* 2022-11-03 归因详情历史表接口改用归因详情表，保证归因详情表有一天的历史数据即可 */


set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
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
-- 模型版本控制 --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
    select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --低频模型
    union all
    select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --中频-城投模型
    union all 
    select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --中频-产业模型
    union all 
    select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --高频-评分卡模型(高频)
    union all 
    select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --高频-无监督模型
    union all 
    select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --信用风险综合模型
    -- select 
    --     notes,
    --     model_name,
    --     model_version,
    --     status,
    --     etl_date
    -- from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf a
    -- where a.etl_date in (select max(etl_date) from t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf)
    --   and status='active'
    -- group by notes,model_name,model_version,status,etl_date
),
-- 预警分 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --预警分_融合调整后综合  原始接口
(
	select a.*
    from 
    (
		-- 时间限制部分 --
		select * 
		from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
		where 1 in (select max(flag) from timeLimit_switch) 
		and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
		union all
		-- 非时间限制部分 --
		select * 
		from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
		where 1 in (select not max(flag) from timeLimit_switch) 
	) a join model_version_intf_ b
		on a.model_version = b.model_version and a.model_name=b.model_name
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
	where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(score_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 非时间限制部分 --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch) 
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --预警分--归因详情历史 原始接口
(
	-- 时间限制部分 --
	select * 
	from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
	union all 
	-- 非时间限制部分 --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --特征贡献度_综合预警等级(用于限制当日特征名称)
(
	select a.*
    from 
    (
		-- 时间限制部分 --
		select * 
		from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
		where 1 in (select max(flag) from timeLimit_switch) 
		and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
		union all 
		-- 非时间限制部分 --
		select * 
		from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
		where 1 in (select not max(flag) from timeLimit_switch) 
	) a join model_version_intf_ b
		on a.model_version = b.model_version and a.model_name=b.model_name
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
	from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
	join corp_chg b 
		on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
	where a.operator = '自动-风险已暴露规则'
	  and a.ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
(
	select
        dimension,
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- 高风险 ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
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
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --取每天最新批次 综合预警-特征贡献度(用于限制今天特征范围，昨天的不用限制)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据（当天数据做范围限制）
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
RMP_WARNING_SCORE_DETAIL_HIS_Batch as --取历史归因详情 最大批次(取自归因详情当日表，所以需要最大批次处理)
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_HIS_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_HIS_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)

),
mid_RMP_WARNING_SCORE_DETAIL_HIS as 
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_Batch main
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string) and main.dimension=cfg.dimension
),
-- 第二段数据 --
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
		main.dim_submodel_contribution_ratio as dim_abnormal_idx_contribution_ratio,   --维度 异常指标 贡献度占比
		main.type,  	-- used
		main.idx_name,  -- used   用于展示的指标名称
		main.ori_idx_name,  --原始模型提供的指标名称，主要用于关联而非展示
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
		on main.ori_idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string) and main.dimension=cfg.dimension
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
			dim_abnormal_idx_contribution_ratio,
			contribution_ratio,
			-- sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
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
		order by corp_id,score_dt desc--,dim_contrib_ratio desc
	) A
),
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --取每天最新批次的维度风险等级变动 以及 特征评分变动 数据，因子层面
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.dimension,
		a.dimension_ch,
		a.dim_abnormal_idx_contribution_ratio,
		a.type,
		-- a.dim_contrib_ratio,   --维度贡献度占比(排序用) used
		a.dim_warn_level,	  --今日维度风险等级
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --昨日维度风险等级
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) <0 then '上升'
			else ''
		end as dim_warn_level_chg_desc,
		a.factor_evaluate,		
		a.idx_name, 
		a.idx_value,
		case 
			when a.idx_unit = '%' then 
				cast(cast(round(a.idx_value,2) as decimal(10,2)) as string)
			-- when a.idx_unit <>'%' or a.idx_unit<>'' then 
			else
				cast(cast(round(a.idx_value,0) as decimal(10,0)) as string)
		end as idx_value_str,   --对指标值根据不同的单位做四舍五入
		a.last_idx_value,
		case 
			when a.idx_unit = '%' then 
				cast(cast(round(a.last_idx_value,2) as decimal(10,2)) as string)
			-- when a.idx_unit <>'%' or a.idx_unit<>'' then 
			else
				cast(cast(round(a.last_idx_value,0) as decimal(10,0)) as string)
		end as last_idx_value_str,   --对昨日指标值根据不同的单位做四舍五入
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
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 简报数据 --
Warn_lv_Feat_score_Idx_value_Summ as --合并 维度风险等级，特征评分 以及 指标变动(简报wy用) 数据
(
	select *,
		case 
			when factor_evaluate=0 and dim_idx_score_chg_desc='恶化'  then 
				'异常'
			else
				NULL
		end as s_dim_desc  --简报维度层 '异常'字样输出逻辑
	from 
	(
		select
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			max(idx_score_chg_desc) over(partition by batch_dt,corp_id,score_dt,dimension) as dim_idx_score_chg_desc, 
			type,
			factor_evaluate,
			idx_name, 
			idx_value,
			last_idx_value,
			idx_unit,
			contribution_ratio,
			idx_score_chg_desc,
			concat(
				case 
					when factor_evaluate=0 and idx_score_chg_desc<>'恶化' then 
						concat(feature_name_target,'为',idx_value_str,idx_unit)
					when factor_evaluate=0 and idx_score_chg_desc='恶化' then 
						concat(
							concat(feature_name_target,'为',idx_value_str,idx_unit),'，','且发生恶化','，','由',
							case 
								when last_idx_value<=idx_value then 
									concat(
											concat(last_idx_value_str,idx_unit),
											'变化至',
											concat(idx_value_str,idx_unit)
									)
								else 
									concat(
											concat(last_idx_value_str,idx_unit),
											'变化至',
											concat(idx_value_str,idx_unit)
									)
							end
						)
					else ''
				end
			) as s_report_idx_desc_no_color,   --简报用到的指标层的指标描述数据
			concat(
				case 
					when factor_evaluate=0 and idx_score_chg_desc<>'恶化' then 
						concat(feature_name_target,'为','<span class="RED">',idx_value_str,idx_unit,'</span>')
					when factor_evaluate=0 and idx_score_chg_desc='恶化' then 
						concat(
							concat(feature_name_target,'为','<span class="RED">',idx_value_str,idx_unit),'</span>','，','且发生恶化','，','由',
							case 
								when last_idx_value<=idx_value then 
									concat(	'<span class="RED">',
											concat(last_idx_value_str,idx_unit),
											'变化至',
											concat(idx_value_str,idx_unit),
											'</span>'
									)
								else 
									concat(
											'<span class="RED">',
											concat(last_idx_value_str,idx_unit),
											'变化至',
											concat(idx_value_str,idx_unit),
											'</span>'
									)
							end
						)
					else ''
				end
			) as s_report_idx_desc,   --简报用到的指标层的指标描述数据
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from rmp_warning_dim_warn_lv_and_idx_score_chg
	) A where rm<=5 
),
s_datg_dim_type as   --汇总到维度，类别层数据 
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		s_dim_desc,
		dim_idx_score_chg_desc,
		-- dimension_ch,
		type,
		-- idx_desc_in_one_type,
		-- idx_desc,
		-- factor_evaluate,
		concat_ws('；',collect_set(s_report_idx_desc)) as s_report_idx_desc_in_one_type  -- hive
		-- group_concat(distinct s_report_idx_desc,'；') as s_report_idx_desc_in_one_type  -- impala 
		  --简报维度层 '异常'字样输出逻辑
	from Warn_lv_Feat_score_Idx_value_Summ 
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,s_dim_desc,dim_idx_score_chg_desc,type
),
-- 简报信息wy --
s_msg_dim_type as 
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		type,
		concat(
			case s_dim_desc
				when '异常' then 
					concat(
						type,s_dim_desc,'：',s_report_idx_desc_in_one_type
					)
				else 
					s_dim_desc
			end
		) as s_msg_in_one_type_no_color,
		concat(
			case s_dim_desc
				when '异常' then 
					concat(
						'<span class="WEIGHT">',type,s_dim_desc,'：','</span>',s_report_idx_desc_in_one_type
					)
				else 
					s_dim_desc
			end
		) as s_msg_in_one_type
	from s_datg_dim_type
),
s_msg as   --最终信息展示汇总到企业层
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		case 
			when nvl(ru.reason,'') = '' then 
				if(a.corp_msg_='' or a.corp_msg_ is null,
					'该主体当前无显著风险点。',
					corp_msg_
				) 
			else 
				if(a.corp_msg_='' or a.corp_msg_ is null,
					'该主体当前无显著风险点。',
					concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',
					a.corp_msg_
					)
				) 
		end as corp_msg   -- hive执行将会返回''，impala返回NULL或''
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			concat_ws('\\r\\n',collect_set(s_msg_in_one_type)) as corp_msg_  -- hive
			-- group_concat(distinct s_msg_in_one_type,'\\r\\n') as corp_msg_  -- impala
		from s_msg_dim_type
		group by batch_dt,corp_id,corp_nm,score_dt
	)A 
	left join warn_adj_rule_cfg  ru
			on a.corp_id = ru.corp_id

)
------------------------------------以上部分为临时表-------------------------------------------------------------------
insert into pth_rmp.WARNING_SCORE_S_REPORT partition(etl_date=${ETL_DATE})
select distinct
	concat(corp_id,md5(concat(batch_dt,corp_id))) as sid_kw,  -- hive
	-- '' as sid_kw,  -- impala
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	corp_msg as report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_msg
where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;
