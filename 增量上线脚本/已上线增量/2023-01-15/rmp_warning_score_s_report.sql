-- rmp_warning_score_s_report 归因简报 --
--/* 2022-10-24 新增颜色逻辑 */
--/* 2022-10-24 新增外挂规则逻辑 */
--/* 2022-11-03 归因详情历史表接口改用归因详情表，保证归因详情表有一天的历史数据即可 */
--/* 2022-11-18 归因简报Wy 新版 */
--/* 2022-12-04 外挂规则取值修复，取最新create_dt的数据 */
-- /* 2023-01-01 model_version_intf_ 改取用视图数据 */
-- /* 2023-01-03 warn_adj_rule_cfg 模型外挂规则create_dt<= 改为 = 同时对数据按照corp_id分组后，再排序*/
-- /* 2023-01-06 归因详情历史和预警等级变动表读入接口表取最大update_time,防止追批产生的重复数据的影响 */
-- /* 2023-01-09  逻辑调整为仅展示异常指标 */
-- /* 2023-01-09  增加 无监督逻辑 */
-- /* 2023-01-09  修复 有type没有指标的情况，排除无异常指标的type */
-- /* 2023-01-09  修复 去除无监督话术最后的句号以及无显著异常风险最后的句号，和其余段落保持一致 */






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
	where a.delete_flag=0 and b.delete_flag=0 and a.source_code='ZXZX'
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
	select * from pth_rmp.v_model_version  --见 预警分-配置表中的视图
    -- select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --低频模型
    -- union all
    -- select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --中频-城投模型
    -- union all 
    -- select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --中频-产业模型
    -- union all 
    -- select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --高频-评分卡模型(高频)
    -- union all 
    -- select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --高频-无监督模型
    -- union all 
    -- select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --信用风险综合模型
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
	select * 
	from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --预警分--归因详情历史(取归因详情表，归因详情表会保证存放连读两天的数据) 原始接口
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

	-- -- 时间限制部分 --
	-- select * 
	-- from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	-- where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
    --   and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
	-- union all 
	-- -- 非时间限制部分 --
    -- select * 
    -- from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    -- where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 预警等级变动 --
RMP_WARNING_SCORE_CHG_ as 
(
	-- 时间限制部分 --
	select  batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt,update_time
	from pth_rmp.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_date) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt,update_time
    from pth_rmp.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --特征贡献度_综合预警等级(用于限制当日特征名称)
(
	select a.*
    from 
    (
		select m.*
		from
		(
			-- 时间限制部分 --
			select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
			where 1 in (select max(flag) from timeLimit_switch) 
			and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
			union all 
			-- 非时间限制部分 --
			select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
			where 1 in (select not max(flag) from timeLimit_switch) 
		)m where rm=1
	) a join model_version_intf_ b
		on a.model_version = b.model_version and a.model_name=b.model_name
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
        dimension,
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- 高风险 ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
-- 模型外挂规则 --
warn_adj_rule_cfg as --预警分-模型外挂规则配置表   取最新etl_date的数据 (更新频率:日度更新)
(
	select distinct m.*
	from 
	(
		select 
			a.etl_date,
			b.corp_id, 
			b.corp_name as corp_nm,
			a.category,
			a.reason,
			rank() over(partition by b.corp_id order by a.create_dt desc ,a.etl_date desc,a.reason desc) rm
		from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
		join corp_chg b 
			on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
		where a.operator = '自动-风险已暴露规则'
		  and to_date(a.create_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	)m where rm=1 
	  --and ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
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
	join (select max(batch_dt) as max_batch_dt,score_dt,max(update_time) as max_update_time from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- RMP_WARNING_SCORE_DETAIL_HIS_Batch as --取历史归因详情 最大批次(取自归因详情当日表，所以需要最大批次处理)
-- (
-- 	select a.*
-- 	from RMP_WARNING_SCORE_DETAIL_HIS_ a
-- 	join (select max(batch_dt) as max_batch_dt,score_dt,max(update_time) as max_update_time from RMP_WARNING_SCORE_DETAIL_HIS_ group by score_dt) b
-- 		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
-- 	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)

-- ),
mid_RMP_WARNING_SCORE_DETAIL_HIS as 
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_Batch main
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string) and main.dimension=cfg.dimension
),
-- 综合预警等级别动类数据 --
RMP_WARNING_SCORE_CHG_Batch as  --取每天最新批次的预警变动等级数据
(
	select a.*
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date,max(update_time) as max_update_time from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date and a.update_time=b.max_update_time
),
--大宽表 基础数据 --
Basic_data as 	-- 综合预警等级变动+当日归因详情+昨日归因详情 （指标层最细粒度）
(
	select distinct
		b.batch_dt,
		b.corp_id,
		b.corp_nm,
		b.score_dt,
		a.synth_warnlevel, --当日预警等级
		a.chg_direction,  --预警等级变动方向 1:上升/恶化 2:下降/减轻
		a.synth_warnlevel_l as last_synth_warnlevel,--昨日预警等级
		b.dimension,
		b.dim_warn_level,
		-- c.dim_warn_level as last_dim_warn_level,
		b.type,
		b.sub_model_name,
		b.idx_name,
		b.idx_value,
		b.last_idx_value,
		b.idx_unit,
		b.idx_score,
		-- c.idx_score as last_idx_score,
		b.contribution_ratio,
		b.factor_evaluate,
		b.dim_submodel_contribution_ratio   --异常指标贡献度占比
		-- c.dim_submodel_contribution_ratio as last_dim_submodel_contribution_ratio  --昨日异常指标贡献度占比
	from  RMP_WARNING_SCORE_DETAIL_Batch b   --归因详情今日
	left join  RMP_WARNING_SCORE_CHG_Batch a   --预警等级变动表 
		on a.corp_id = b.corp_id and a.score_date=b.score_dt 
	-- left join RMP_WARNING_SCORE_DETAIL_HIS_Batch c  --归因详情昨日
	-- 	on 	b.corp_id=c.corp_id 
	-- 		and to_date(date_add(b.score_dt,-1))= c.score_dt 
	-- 		and b.dimension=c.dimension 
	-- 		and b.type=c.type 
	-- 		and b.sub_model_name=c.sub_model_name
	-- 		and b.ori_idx_name=c.ori_idx_name
	-- where a.chg_direction='1'    --综合预警等级须发生恶化必须要的话，才展示第四段，否则整段不展示
),
Basic_data_I as  -- 生成 是否维度恶化 + 是否维度异常指标占比恶化 + 是否指标恶化 数据
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		-- last_synth_warnlevel,
		chg_direction, 

		dimension,
		case dimension 
			when 1 then '财务' 
			when 2 then '经营'
			when 3 then '市场'
			when 4 then '舆情'
			when 5 then '异常风险检测'
		end as dimension_ch,
		
		dim_warn_level,
		-- last_dim_warn_level,
		
		-- case 
		-- 	when cast(dim_warn_level as int) < cast(last_dim_warn_level as int) then   --维度发生恶化
		-- 		1 
		-- 	else 
		-- 		0
		-- end as dim_warn_level_worsen_flag,  --是否维度恶化 

		dim_submodel_contribution_ratio,
		-- last_dim_submodel_contribution_ratio,
		-- case 
		-- 	when dim_submodel_contribution_ratio>last_dim_submodel_contribution_ratio then 
		-- 		1 
		-- 	else 
		-- 		0
		-- end as dim_submodel_contribution_ratio_worsen_flag, --是否维度异常指标占比恶化
		
		type,
		factor_evaluate,
		-- min(factor_evaluate) over(partition by batch_dt,corp_id,score_dt,dimension,type) as type_unabnormal_flag,  --type是否异常判断  0:异常  1:正常

		idx_score,
		-- last_idx_score, 
		-- case 
		-- 	when idx_score>last_idx_score then 
		-- 		1 
		-- 	else 
		-- 		0
		-- end as idx_score_worsen_flag ,	--是否恶化指标
		idx_name,
		idx_value,
		case 
			when idx_unit='%' then 
				cast(cast(round(idx_value,2) as decimal(10,2)) as string) 
			when idx_unit in ('元','万元','亿元','倍','万人','次') then 
				cast(cast(round(idx_value,2) as decimal(10,2)) as string) 
			else 	
				cast(idx_value as string)
		end as idx_value_str,
		-- last_idx_value,
		-- case 
		-- 	when idx_unit='%' then 
		-- 		cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
		-- 	when idx_unit in ('元','万元','亿元','倍','万人','次') then 
		-- 		cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
		-- 	else 	
		-- 		cast(last_idx_value as string)
		-- end as last_idx_value_str,
		idx_unit,
		contribution_ratio
	from Basic_data 
),
Basic_data_II as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		corp_dimension5_flag,
		dimension,
		dim_contrib_ratio,
		type,
		idx_name,
		factor_evaluate,
		contribution_ratio,
		case 
			when factor_evaluate =0 and dim_contrib_ratio>0 then 
				concat(idx_name,'为','<span class="RED">',idx_value_str,idx_unit,'</span>')
			else 
				NULL
		end as idx_desc
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			max(if(dimension=5 and contribution_ratio>0,1,0)) over(partition by batch_dt,corp_id,score_dt) as corp_dimension5_flag,  --是否最终输出异常风险监测话术的标识
			dimension,
			sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
			type,
			idx_name,
			idx_value,
			idx_value_str,
			idx_unit,
			factor_evaluate,
			contribution_ratio
		from Basic_data_I
	) A
	-- select 
	-- 	a.*,
	-- 	-- cfg_syn.warn_lv_desc as synth_warnlevel_desc,
	-- 	-- cfg_syn_l.warn_lv_desc as last_synth_warnlevel_desc,
	-- 	-- cfg.risk_lv_desc as dim_warn_level_desc,
	-- 	-- cfg_l.risk_lv_desc as last_dim_warn_level_desc,
	-- 	if(min(a.factor_evaluate)=0 and dimension=5 and contribution_ratio>0) as corp_dimension5_flag,  --企业有异常风险检测维度且维度贡献度占比大于0的标识
	-- 	count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension,a.idx_score_worsen_flag) as worsen_dim_idx_cnt, --恶化指标数量
	-- 	count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension) as dim_idx_cnt, --维度指标数量
	-- 	case 
	-- 		when (a.factor_evaluate = 0 and a.contribution_ratio>0) and (a.chg_direction='1' and (a.dim_warn_level_worsen_flag=1 or  dim_submodel_contribution_ratio_worsen_flag=1) and a.idx_score_worsen_flag=1) then --指标异常 且 指标恶化
	-- 			concat(a.idx_name,'为','<span class="RED">',a.idx_value_str,a.idx_unit,'</span>','，且发生恶化，','由','<span class="RED">',a.last_idx_value_str,a.idx_unit,'变化至',a.idx_value_str,a.idx_unit,'</span>')
	-- 		when (a.factor_evaluate = 0 and a.contribution_ratio>0) and  idx_score_worsen_flag=0 then --指标异常 但 指标未恶化
	-- 			concat(a.idx_name,'为','<span class="RED">',a.idx_value_str,a.idx_unit,'</span>')
	-- 		when (a.factor_evaluate = 1 or a.factor_evaluate=0 and a.contribution_ratio is null)  and (a.chg_direction='1' and (a.dim_warn_level_worsen_flag=1 or  dim_submodel_contribution_ratio_worsen_flag=1) and a.idx_score_worsen_flag=1) then --指标正常 但 指标恶化
	-- 			concat(a.idx_name,'由','<span class="RED">',a.last_idx_value_str,a.idx_unit,'</span>','变化至','<span class="RED">',a.idx_value_str,a.idx_unit,'</span>')
	-- 		else 
	-- 			NULL
	-- 	end as idx_desc
	-- from Basic_data_I a
),
-- 第四段 type层数据汇总 --
s_msg_type as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		corp_dimension5_flag,
		dimension,
		type,
		dim_contrib_ratio,
		concat_ws('、',collect_set(idx_desc)) as worsen_idx_desc_in_one_type
		-- group_concat(distinct idx_desc,'、') as worsen_idx_desc_in_one_type 
	from Basic_data_II
	group by batch_dt,corp_id,corp_nm,score_dt,corp_dimension5_flag,dimension,dim_contrib_ratio,type
),
-- s_msg_type as 
-- (
-- 	select 
-- 		batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type,
-- 		concat_ws('、',collect_set(idx_desc)) as worsen_idx_desc_in_one_type  --hive
-- 		-- group_concat(distinct idx_desc,'、') as worsen_idx_desc_in_one_type 
-- 	from 
-- 	(
-- 		select distinct
-- 			batch_dt,
-- 			corp_id,
-- 			corp_nm,
-- 			score_dt,
-- 			dimension_ch,
-- 			worsen_dim_idx_cnt,
-- 			dim_idx_cnt,
-- 			type,
-- 			idx_desc
-- 		from 
-- 		(
-- 			select
-- 				batch_dt,
-- 				corp_id,
-- 				corp_nm,
-- 				score_dt,
-- 				dimension_ch,
-- 				worsen_dim_idx_cnt,
-- 				dim_idx_cnt,
-- 				type,
-- 				idx_desc,
-- 				row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by 1) as rm
-- 			from Basic_data_II
-- 			-- where idx_score_worsen_flag = 1  
-- 		)A where rm<=5  --取贡献度排名前5大的恶化指标作为展示
-- 	)B group by batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type
-- ),
s_msg as 
(	
	select 
		b.batch_dt,b.corp_id,b.corp_nm,b.score_dt,
		concat('该主体需关注排查风险点包括：\\r\\n',
			case 
				when ru.reason is null and (b.report_msg_='' or b.report_msg_ is null)  then
					if(b.corp_dimension5_flag=1,b.corp_dimension5_desc,'该主体当前无显著风险点')  
				when ru.reason is not null and b.report_msg_='' then 
					if(b.corp_dimension5_flag=1, concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',b.corp_dimension5_desc),concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n') )
				when ru.reason is not null and b.report_msg_<>''  then 
					if(b.corp_dimension5_flag=1,concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',b.report_msg_,'\\r\\n',b.corp_dimension5_desc),concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',b.report_msg_))
					-- concat('该主体触发','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',b.report_msg_)
				when ru.reason is null and (b.report_msg_<>'' or b.report_msg_ is not null)  then 
					if(b.corp_dimension5_flag=1,concat(b.report_msg_,'\\r\\n',b.corp_dimension5_desc),b.report_msg_)
					-- b.report_msg_
			end
		) as report_msg
	from 
	(
		select 
			batch_dt,corp_id,corp_nm,score_dt,
			corp_dimension5_flag,  --,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,
			case 
				when corp_dimension5_flag = 1 then 
					'通过异常检测机器学习模型，捕捉到该主体在新闻、公告、司法、诚信、价格等特征层面，相较于历史未发生信用恶化的主体具有显著的离群表现，即该主体与历史发生非标违约、债券违约、评级下调、展望下调等信用恶化事件的主体表现更趋近'
				else 
					'' 
			end as corp_dimension5_desc,
			concat_ws('\\r\\n',collect_set(msg_type)) as report_msg_  --impala:null 或 长度大于0的字符串 hive:'' 或者 长度大于0的字符串
			-- group_concat(distinct msg_type,'\\r\\n') as report_msg_
		from 
		(
			select 
				batch_dt,corp_id,corp_nm,score_dt,corp_dimension5_flag, --,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,
				case 
					when worsen_idx_desc_in_one_type <> '' and worsen_idx_desc_in_one_type  is not null  then
						concat('<span class="WEIGHT">',type,'异常：','</span>',worsen_idx_desc_in_one_type) 
					else 
						NULL
				end as msg_type
			from s_msg_type 
		) A
		group by batch_dt,corp_id,corp_nm,score_dt,corp_dimension5_flag--,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt
	)B left join warn_adj_rule_cfg ru
			on b.corp_id = ru.corp_id 
)
insert into pth_rmp.rmp_warning_score_s_report partition(etl_date=${ETL_DATE})
select 
	md5(concat(batch_dt,corp_id)) as sid_kw,  -- hive
	-- '' as sid_kw,  -- impala
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_msg
;