-- RMP_WARNING_SCORE_REPORT 第四段-归因变动 --
-- /*2022-11-13 归因详情历史接口层调整，用归因详情当日表的数据，归因详情当日表确保会存放连续两天的数据 */
-- /* 2022-12-04 外挂规则取值修复，取最新create_dt的数据 */
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */
-- /* 2023-01-01 model_version_intf_ 改取用视图数据 */
-- /* 2023-01-03 warn_adj_rule_cfg 模型外挂规则create_dt<= 改为 = 同时对数据按照corp_id分组后，再排序*/
-- /* 2023-01-03 新增 恶化到风险已暴露等级的数据也要输出的逻辑 */
-- /* 2023-01-04 归因详情历史和预警等级变动表读入接口表取最大update_time,防止追批产生的重复数据的影响 */
-- /* 2023-01-05 修复 风险已暴露上升至风险已暴露的问题 */
-- /* 2023-01-05 修复 同一家企业出现多条第四段信息描述的问题，原因：没有对企业各个维度信息聚合到到企业层 (Fourth_msg_corp_II表的最内层子查询) */
-- /* 2023-01-05 修复 缺少 当维度发生恶化且维度异常占比也发生恶化时的第四段描述 (Fourth_msg_corp_I表case when条件判断的处理) */
-- /* 2023-01-06 修复 Fourth_msg_corp_II表，在msg_corp_字段为''是，会多出一个逗号的情形 */



--综合预警等级变动层：综合预警等级变动表   因子变动层数据：归因详情当日(主表)+归因详情历史表+预警分模型结果表当日(综合预警等级字段来源)
--（1）恶化指标判断错误 （2）维度和恶化指标没有挂上钩  （3）风险水平上升的主要维度，需要和昨天的异常占比对比，发生升高的才展示

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=12;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

-- drop table if exists pth_rmp.rmp_warning_score_report4;  
-- create table pth_rmp.rmp_warning_score_report4 as  --@pth_rmp.rmp_warning_score_report4
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
RMP_WARNING_SCORE_DETAIL_HIS_Batch as --取历史归因详情 最大批次(取自归因详情当日表，所以需要最大批次处理)
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_HIS_ a
	join (select max(batch_dt) as max_batch_dt,score_dt,max(update_time) as max_update_time from RMP_WARNING_SCORE_DETAIL_HIS_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)

),
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
		a.corp_id,
		a.corp_nm,
		b.score_dt,
		a.synth_warnlevel, --当日预警等级
		a.chg_direction,  --预警等级变动方向 1:上升/恶化 2:下降/减轻
		a.synth_warnlevel_l as last_synth_warnlevel,--昨日预警等级
		b.dimension,
		b.dim_warn_level,
		c.dim_warn_level as last_dim_warn_level,
		b.type,
		b.sub_model_name,
		b.idx_name,
		b.idx_value,
		b.last_idx_value,
		b.idx_unit,
		b.idx_score,
		c.idx_score as last_idx_score,
		-- b.contribution_ratio,
		b.factor_evaluate,
		b.dim_submodel_contribution_ratio,   --异常指标贡献度占比
		c.dim_submodel_contribution_ratio as last_dim_submodel_contribution_ratio  --昨日异常指标贡献度占比
	from RMP_WARNING_SCORE_CHG_Batch a  --预警等级变动表
	join RMP_WARNING_SCORE_DETAIL_Batch b  --归因详情今日
		on a.corp_id = b.corp_id and a.score_date=b.score_dt 
	join RMP_WARNING_SCORE_DETAIL_HIS_Batch c  --归因详情昨日
		on 	b.corp_id=c.corp_id 
			and to_date(date_add(b.score_dt,-1))= c.score_dt 
			and b.dimension=c.dimension 
			and b.type=c.type 
			and b.sub_model_name=c.sub_model_name
			and b.ori_idx_name=c.ori_idx_name
	where a.chg_direction='1'    --综合预警等级须发生恶化必须要的话，才展示第四段，否则整段不展示
),
Basic_data_I as  -- 生成 是否维度恶化 + 是否维度异常指标占比恶化 + 是否指标恶化 数据
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		last_synth_warnlevel,
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
		last_dim_warn_level,
		
		case 
			when cast(dim_warn_level as int) < cast(last_dim_warn_level as int) then   --维度发生恶化
				1 
			else 
				0
		end as dim_warn_level_worsen_flag,  --是否维度恶化 

		dim_submodel_contribution_ratio,
		last_dim_submodel_contribution_ratio,
		case 
			when dim_submodel_contribution_ratio>last_dim_submodel_contribution_ratio then 
				1 
			else 
				0
		end as dim_submodel_contribution_ratio_worsen_flag, --是否维度异常指标占比恶化
		
		type,

		idx_score,
		last_idx_score, 
		case 
			when idx_score>last_idx_score then 
				1 
			else 
				0
		end as idx_score_worsen_flag ,	--是否恶化指标
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
		last_idx_value,
		case 
			when idx_unit='%' then 
				cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
			when idx_unit in ('元','万元','亿元','倍','万人','次') then 
				cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
			else 	
				cast(last_idx_value as string)
		end as last_idx_value_str,
		idx_unit
	from Basic_data 
	
),
Basic_data_II as 
(
	select 
		a.*,
		cfg_syn.warn_lv_desc as synth_warnlevel_desc,
		cfg_syn_l.warn_lv_desc as last_synth_warnlevel_desc,
		cfg.risk_lv_desc as dim_warn_level_desc,
		cfg_l.risk_lv_desc as last_dim_warn_level_desc,
		count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension,a.idx_score_worsen_flag) as worsen_dim_idx_cnt, --恶化指标数量
		count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension) as dim_idx_cnt, --维度指标数量
		concat(a.idx_name,'由',a.last_idx_value_str,a.idx_unit,'变化至',a.idx_value_str,a.idx_unit) as worsen_idx_desc  --恶化的指标描述
	from Basic_data_I a
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_syn
		on cast(a.synth_warnlevel as string)=cfg_syn.warn_lv 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_syn_l
		on cast(a.last_synth_warnlevel as string)=cfg_syn_l.warn_lv  
	join warn_dim_risk_level_cfg_ cfg 
		on a.dim_warn_level=cast(cfg.risk_lv as string) and a.dimension=cfg.dimension
	join warn_dim_risk_level_cfg_ cfg_l
		on a.last_dim_warn_level=cast(cfg_l.risk_lv as string) and a.dimension=cfg_l.dimension
	where a.synth_warnlevel='-5' or a.dim_warn_level_worsen_flag=1 or a.dim_submodel_contribution_ratio_worsen_flag=1   --PS: 综合预警等级恶化到风险已暴露 或者 维度须发生恶化 或者 维度异常指标发生恶化, 才展示第四段，否则整段不展示
),
-- 第四段 type层数据汇总 --
Fourth_msg_type as 
(
	select 
		batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type,
		concat_ws('、',collect_set(worsen_idx_desc)) as worsen_idx_desc_in_one_type  --hive
		-- group_concat(distinct worsen_idx_desc,'、') as worsen_idx_desc_in_one_type 
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension_ch,
			worsen_dim_idx_cnt,
			dim_idx_cnt,
			type,
			worsen_idx_desc
		from 
		(
			select
				batch_dt,
				corp_id,
				corp_nm,
				score_dt,
				dimension_ch,
				worsen_dim_idx_cnt,
				dim_idx_cnt,
				type,
				worsen_idx_desc,
				row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by 1) as rm
			from Basic_data_II
			where idx_score_worsen_flag = 1  
		)A where rm<=5  --取贡献度排名前5大的恶化指标作为展示
	)B group by batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type
),
Fourth_msg_dim as 
(
	select 
		*,
		case 
			when worsen_dim_idx_cnt>0 then 
				concat('，',dimension_ch,'维度中','有',cast(worsen_dim_idx_cnt as string),'个指标发生恶化','，','具体表现为',worsen_idx_desc_in_one_type)
			else 
				'。'
		end dim_msg  --xxx维度中有y个指标发生恶化
	from Fourth_msg_type
),
-- 第四段 企业层数据汇总 --
Fourth_msg_corp_I as --肯定是 综合预警等级恶化到-5 或者 维度发生变化 或者 是维度异常占比 满足条件的数据
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel_desc,
		a.last_synth_warnlevel_desc,
		a.dimension_ch,
		a.dim_warn_level_desc,
		a.last_dim_warn_level_desc,
		a.dim_warn_level_worsen_flag,
		a.dim_submodel_contribution_ratio_worsen_flag,
	   	b.dim_msg,

		case 
			when dim_warn_level_worsen_flag=1 and dim_submodel_contribution_ratio_worsen_flag in (0,1)  then 
				concat(
					a.dimension_ch,'维度','由',a.last_dim_warn_level_desc,'上升至',a.dim_warn_level_desc,nvl(b.dim_msg,'')
				)
			when dim_warn_level_worsen_flag=0 and dim_submodel_contribution_ratio_worsen_flag=1 then 
				concat('风险水平上升的维度为',a.dimension_ch,'维度',nvl(b.dim_msg,'')
				)
			else 
				NULL
		end as corp_dim_msg
	from Basic_data_II a 
	left join Fourth_msg_dim b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.score_dt=b.score_dt and a.dimension_ch=b.dimension_ch
),
Fourth_msg_corp_II as 
(
	select 
		a.*,
		ru.reason,
		case 
			when ru.reason is not null then  
				concat('相较于前一天，','预警等级由',a.last_synth_warnlevel_desc,'上升至','风险已暴露预警等级','，','主要由于触发',ru.reason,nvl(concat('，',if(a.msg_corp_='',null,a.msg_corp_) ),''),'。')
			else 
				concat('相较于前一天，','预警等级由',a.last_synth_warnlevel_desc,'上升至',a.synth_warnlevel_desc,nvl(concat('，',if(a.msg_corp_='',null,a.msg_corp_)),''),'。')
		end as msg4_with_no_color,
		case 
			when ru.reason is not null then  
				concat('相较于前一天，','预警等级由','<span class="RED"><span class="WEIGHT">',a.last_synth_warnlevel_desc,'上升至','风险已暴露预警等级','，','主要由于触发',ru.reason,'</span></span>',nvl(concat('，',if(a.msg_corp_='',null,a.msg_corp_)),''),'。')
			else 
				concat('相较于前一天，','预警等级由','<span class="RED"><span class="WEIGHT">',a.last_synth_warnlevel_desc,'上升至',a.synth_warnlevel_desc,'</span></span>',nvl(concat('，',if(a.msg_corp_='',null,a.msg_corp_)),''),'。')
		end as msg4
	from 
	(
		select 
			batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel_desc,last_synth_warnlevel_desc,--corp_dim_msg
			concat_ws('；',collect_set(corp_dim_msg)) as msg_corp_
			-- group_concat(distinct corp_dim_msg,'；') as msg_corp_   -- impala
		from Fourth_msg_corp_I
		group by batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel_desc,last_synth_warnlevel_desc--,corp_dim_msg
	)A left join warn_adj_rule_cfg ru
		on a.corp_id = ru.corp_id 
)
insert overwrite table pth_rmp.rmp_warning_score_report4
select
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg4_with_no_color,
	msg4
from Fourth_msg_corp_II
;
