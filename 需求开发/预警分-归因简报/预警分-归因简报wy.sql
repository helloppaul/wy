-- WARNING_SCORE_S_REPORT 归因简报 --
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
	where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
),
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(score_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 非时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch) 
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --预警分--归因详情历史 原始接口
(
	-- 时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
    union all
    -- 非时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select not max(flag) from timeLimit_switch) 

	-- select * 
	-- from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	-- where delete_flag=0
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --特征贡献度_综合预警等级
(
	-- 时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch) 

	-- select *
	-- from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
(
	select
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- 高风险 ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
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
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- RMP_WARNING_SCORE_DETAIL_Batch as 
-- (
-- 	select a.*
-- 	from RMP_WARNING_SCORE_DETAIL_Batch_Tmp a 
-- 	join rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch c 
-- 		on a.idx_name=c.feature_name  --特征范围限制
-- ),
mid_RMP_WARNING_SCORE_DETAIL_HIS as --！！！检查
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_ main
	left join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
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
		main.type,  	-- used
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.last_idx_value, -- used
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
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --取每天最新批次的维度风险等级变动 以及 特征评分变动 数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
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
		a.idx_name, 
		a.idx_value,
		a.feature_name_target,
		a.idx_unit,
		a.idx_score,   -- 今日指标打分
		b.idx_score as idx_score_1, -- 昨日指标打分
		concat(
			'由',
			case 
				when last_idx_value>=idx_value then 
					concat(
							concat(cast(last_idx_value as string),idx_unit)
							'升至',
							concat(cast(idx_value as string),idx_unit)
					)
				else 
					concat(
							concat(cast(last_idx_value as string),idx_unit)
							'降至',
							concat(cast(idx_value as string),idx_unit)
					)
			end
		) as last_idx_chg_desc,
		case 
			when cast(a.idx_score as int)-cast(b.idx_score as int) >0 then '恶化'  --指标层 特征评分卡得分变高则为恶化
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join mid_RMP_WARNING_SCORE_DETAIL_HIS b
		on  a.corp_id=b.corp_id 
			and to_date(date_add(a.score_dt,-1)) = b.score_dt
			-- and to_date(date_add(from_unixtime(unix_timestamp(a.score_dt,'yyyyMMdd')),-1)) = to_date(date_add(from_unixtime(unix_timestamp(b.score_dt,'yyyyMMdd')),0))
			and a.dimension=b.dimension
		-- from_unixtime(unix_timestamp(to_date(a.score_dt),'yyyy-MM-dd'))=unix_timestamp(to_date(b.score_dt),'yyyy-MM-dd') and a.dimension=b.dimension
),
Fourth_Part_Data_dim_warn_level_And_idx_score as  
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
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
		idx_score,
		idx_score_1,
		idx_score_chg_desc,
		max(idx_score_chg_desc) over(partition by corp_id,score_dt,dimension) as dim_idx_score_chg_desc,  --维度层指标是否恶化
		count(idx_name) over(partition by corp_id,score_dt,dimension,idx_score_chg_desc) as dim_idx_score_cnt,  --按照得分恶化和非恶化分别统计指标数量
		row_number() over(partition by corp_id,score_dt,dimension order by dim_contrib_ratio desc) as dim_contrib_ratio_rank
	from RMP_WARNING_dim_warn_lv_And_idx_score_chg
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- Second_Part_Data_Dimension as -- 按维度层汇总描述用数据
-- (
-- 	select 
-- 		batch_dt,
-- 		corp_id,
-- 		corp_nm,
-- 		score_dt,
-- 		dimension,
-- 		dimension_ch,
-- 		dim_contrib_ratio,
-- 		dim_factorEvalu_contrib_ratio,
-- 		dim_warn_level_desc,
-- 		dim_factor_cnt,
-- 		dim_factorEvalu_factor_cnt
-- 	from Second_Part_Data
-- 	where factor_evaluate = 0  --因子评价，因子是否异常的字段 0：异常 1：正常
-- ),
S_Second_Part_Data as --简报wy专用数据
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		factor_evaluate,
		idx_desc,
		last_idx_chg_desc,
	from Second_Part_Data
)

-- Second_Part_Data_Dimension_Type as -- 按维度层 以及 类别层汇总描述用数据
-- (
-- 	select
-- 		batch_dt,
-- 		corp_id,
-- 		corp_nm,
-- 		score_dt,
-- 		dimension,
-- 		dimension_ch,
-- 		type,
-- 		factor_evaluate,
-- 		last_idx_chg_desc,
-- 		-- concat_ws('、',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive 
-- 		group_concat(idx_desc,'、') as idx_desc_in_one_type,    -- impala
		
-- 		group_concat(
-- 			concat(
-- 				idx_desc,
-- 				case 
-- 					when factor_evaluate
-- 			)
-- 		)
-- 	from Second_Part_Data
-- 	-- where factor_evaluate = 0
-- 	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type,factor_evaluate
-- ),
-- 简报数据 --
s_datg_dim_type as   --汇总到维度，类别层数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dimension_ch,
		a.type,
		-- a.idx_desc_in_one_type,
		a.idx_desc,
		a.last_idx_chg_desc,
		a.factor_evaluate,
		b.dim_idx_score_chg_desc,
		case 
			when a.factor_evaluate=0 and b.dim_idx_score_chg_desc='恶化'  then 
				'异常'
			else
				''
		end as s_dim_desc  --简报维度层 '异常'字样输出逻辑
	from Second_Part_Data a --当日归因结果
	join Fourth_Part_Data_dim_warn_level_And_idx_score b --当日和前一日维度各因子打分对比
		on  a.batch_dt=b.batch_dt 
			and a.corp_id=b.corp_id 
			and a.score_dt=b.score_dt 
			and a.dimension=b.dimension
			and a.type=b.type
) ,
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
						type,s_dim_desc,'：'
						case 
							when factor_evaluate=0 and dim_idx_score_chg_desc<>'恶化' then 
								last_idx_chg_desc
							when factor_evaluate=0 and dim_idx_score_chg_desc='恶化' then 
								concat(last_idx_chg_desc,'且发生恶化，',last_idx_chg_desc)
					)
				else 
					s_dim_desc
			end
		) as dim_type_msg
	from s_datg_dim_type
),
s_msg as   --最终信息展示汇总到企业层
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		if(corp_msg_='','该主体当前无显著风险点。',corp_msg_) as corp_msg
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			-- concat_ws('\\r\\n',collect_set(dim_type_msg)) as corp_msg  -- hive
			group_concat(distinct dim_type_msg,'\\r\\n') as corp_msg_  -- impala
		from s_msg_dim_type
		group by batch_dt,corp_id,corp_nm,score_dt
	)A
)

