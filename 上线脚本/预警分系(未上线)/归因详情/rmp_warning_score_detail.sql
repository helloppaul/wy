-- RMP_WARNING_SCORE_DETAIL (同步方式：一天对批次插入) --
-- /* 2022-10-27 修复 中位数计算逻辑 来自于证监会一级行业而非国标一级分类 */
-- /* 2022-10-27 修复 归因个数计算，是基于该时点的企业下type层的归因个数统计而非脱离企业层的统计 */
-- /* 2022-10-28 以高中低频合并的特征贡献度表的指标名称为准 */
-- /* 2022-11-01 归因详情数据逻辑优化，不依赖归因详情历史表获取昨日指标值，直接从上游高中低频合并的特征原始值获取 */
-- /* 2022-11-08 增加模型版本控制接口表 */
-- /* 2022-11-08 新增 根据中证企业敞口分类 获取对应企业低频子模型分类的指标释义 */
-- /* 2022-11-09 维度风险等级问题修复，调整后预警等级风险 已暴露和红色预警 映射调整为-3 */
-- /* 2022-11-10 修复 维度风险等级都显示为风险最高的问题  */
-- /* 2022-11-12 维度风险等级逻辑调整优化，改用 异常指标占比(%) 作为维度风险等级划分依据，并相应调整维度风险等级配置表  */
-- /* 2022-11-12 修复 idx_name取值调整为feature_name_target */
-- /* 2022-11-12 修复 上游模型和目标表输出指标数量不一致的问题 */
-- /* 2022-11-12 新增 idx_value 根据目标单位转换的逻辑 */
-- /* 2022-11-12 修复 contribution_cnt 统计维度的问题，统计维度调整为type */
-- /* 2022-11-12 修复 指标中位数计算的问题 */
-- /* 2022-11-12 修复 企业数量与上游综合预警等级模型结果表企业数量不一致的问题 */
-- 依赖 模型 综合预警分，特征原始值高中低，特征贡献度高中低无监督以及综合，评分卡高中低，归因详情及其历史 PS:不依赖pth_rmp.模型结果表
--q1：维度风险等级的计算依靠贡献度占比，贡献度占比特征会少于特征原始值，此时最后关联将会产生某些维度关联补上维度风险等级，导致为NULL(暂时决定踢掉)
--q2：特征值以高中低频合并的特征贡献度表为基准，主表有特征原始值切换为高中低频合并的特征贡献度表
-- 测试用例 20221102 安吉县城西北开发有限公司
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业 的特殊corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.zjh_industry_l1 as industryphy_name  --证监会行业 
    ,b.exposure  --中正敞口 used 用于取特征手工配置表唯一特征释义
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
      and a.source_code='ZXZX'   --控制项
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
        and to_date(rating_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
-- 特征原始值(取今昨两天数据) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --特征原始值_高频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select *
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --特征原始值_低频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --特征原始值_中频_城投 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --特征原始值_中频_产业债 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --特征贡献度_融合调整后综合 原始接口（增加了无监督特征：creditrisk_highfreq_unsupervised  ）
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
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --特征贡献度_高频
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --特征贡献度_低频
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --特征贡献度_中频城投
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --特征贡献度_中频产业
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
-- 特征得分(特征打分卡) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --特征得分_高频 原始接口 
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
    
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --特征得分_低频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --特征得分_中频_城投 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch)
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --特征得分_中频_产业债 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- 非时间限制部分 --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
    on a.model_version = b.model_version and a.model_name=b.model_name 
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
        sub_model_type,  --低频-金融平台、低频-医药制造 ...
        -- substr(sub_model_type,1,6) as sub_model_type,  --取前两个中文字符
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
--映射后 特征手工配置表 --
warn_feat_CFG as
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,    --低频-金融平台、低频-医药制造 ...
        feature_name_target,
        case dimension 
            when '财务' then 1
            when '经营' then 2
            when '市场' then 3
            when '舆情' then 4
            when '异常风险检测' then 5
        end as dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
        -- count(feature_cd) over(partition by dimension,type) as contribution_cnt
    from feat_CFG
),
-- 映射后的 带有企业敞口的 特征手工配置表 --
warn_feat_corp_property_CFG as  --通过低频分类数据的sub_model_type获取对应敞口的企业    使用范围:高中低频合并的特征贡献度表
(
    select 
        b.corp_id,
        b.source_id as corp_code,
        max(b.corp_name) as corp_nm,
        '低频' as big_sub_model_type,
        a.sub_model_type,
        a.feature_cd,
        a.feature_name
    from warn_feat_CFG a 
    join corp_chg b 
        on substr(a.sub_model_type,8) = b.exposure and b.source_code='ZXZX'
    where substr(a.sub_model_type,1,6) = '低频'
    group by b.corp_id,b.source_id,a.sub_model_type,a.feature_cd,a.feature_name   --去除重复数据
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
RMP_WARNING_SCORE_MODEL_ as  --预警分-模型结果表（已是最新批次）
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
			when a.interval_text_adjusted  in ('红色预警','风险已暴露') then 
				'-3'  --高风险
			-- when a.interval_text_adjusted  ='风险已暴露' then 
			-- 	'-4'   --风险已暴露
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a 
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
	-- where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    -- from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
-- 特征原始值(取今昨两天数据) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch as  --特征原始值_高频 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch as  --特征原始值_低频 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch as  --特征原始值_中频_城投 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch as  --特征原始值_中频_产业债 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch as  --特征贡献度_融合调整后综合 原始接口（增加了无监督特征：creditrisk_highfreq_unsupervised  ）
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch as --特征贡献度_高频
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch as  --特征贡献度_低频
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch as  --特征贡献度_中频城投
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch as  --特征贡献度_中频产业
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
-- 特征得分(特征打分卡) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch as  --特征得分_高频 原始接口 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch as  --特征得分_低频 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch as  --特征得分_中频_城投 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch as  --特征得分_中频_产业债 原始接口
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
warn_union_adj_sync_score as --取最新批次的预警分-模型结果表
(
    select distinct
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_date as score_dt,
        a.synth_score as adj_score,
        a.synth_warnlevel as adj_synth_level,
        a.adjust_warnlevel,
        a.model_version
    from RMP_WARNING_SCORE_MODEL_ a
),
-- 特征原始值 --
warn_feature_value_two_days as --原始特征值_合并高中低频(包含今昨两天数据，行形式)
(
    SELECT
        cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --以高频更新的数据为批次时间
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_value as idx_value,
        '' as idx_unit,
        model_freq_type,  --特征所属子模型分类/模型频率分类
        model_name as sub_model_name
    from
    (
        --高频
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '高频' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch 
        union all 
        --低频
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '低频' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch  
        union all 
        --中频_城投
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '中频-城投' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch 
        union all 
        --中频_产业债
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '中频-产业' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch 
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_value as --原始特征值_合并高中低频(包含今昨两天数据，列形式 used)
(
    select 
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_dt,
        a.idx_name,
        a.idx_value,
        b.score_dt as lst_score_dt,  --昨日日期
        b.idx_value as lst_idx_value,  --昨日指标值
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name
    from warn_feature_value_two_days a   --今
    join warn_feature_value_two_days b   --昨
        on  a.corp_id = b.corp_id 
            and date_add(a.score_dt,-1)=b.score_dt 
            and a.sub_model_name=b.sub_model_name  
            and a.idx_name=b.idx_name
),
warn_feature_value_with_median as --原始特征值_合并高中低频+中位数计算
(
    select distinct
        a.batch_dt,
        a.score_dt,
        a.corp_id,
        a.corp_nm,
        a.idx_name,
        a.idx_value,
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name,
        nvl(b.industryphy_name,'') as zjh,
        nvl(b.bond_type,0) as bond_type,  --0：非产业和城投 1：产业债 2：城投债
        case 
            when nvl(b.bond_type,0)=2 then 
                ''
            when nvl(b.bond_type,0)<>2 then 
                nvl(b.industryphy_name,'')
        end as zjh_cal   --计算用证监会一级行业分类
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='ZXZX') b 
        on a.corp_id=b.corp_id 
),
warn_feature_value_with_median_cal as 
(
    select 
        a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
        ,appx_median(b.idx_value) as median  --impala
        -- ,percentile_approx(b.idx_value,0.5) as median  --hive
    from warn_feature_value_with_median a 
    join warn_feature_value_with_median b 
        on a.batch_dt=b.batch_dt and a.score_dt=b.score_dt and a.zjh_cal=b.zjh_cal and a.idx_name=b.idx_name  --获取 与当前行的企业 同时间点 同行业 同指标的指标中位数
    group by a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
    -- select 
    --     batch_dt,
    --     score_dt,
    --     corp_id,
    --     bond_type,
    --     sub_model_name,
    --     idx_name,
    --     appx_median(idx_value) as median
    --     -- percentile_approx(idx_value,0.5) as median  --hive
    -- from warn_feature_value_with_median
    -- where bond_type=2
    -- group by bond_type,corp_id,batch_dt,score_dt,sub_model_name,idx_name
    -- union all 
    -- select 
    --     batch_dt,
    --     score_dt,
    --     corp_id,
    --     bond_type,
    --     sub_model_name,
    --     idx_name,
    --     appx_median(idx_value) as median
    --     -- percentile_approx(idx_value,0.5) as median  --hive
    -- from warn_feature_value_with_median
    -- where bond_type<>2 and zjh <> ''
    -- group by  bond_type,zjh,corp_id,batch_dt,score_dt,sub_model_name,idx_name
),
warn_feature_value_with_median_res as -- used
(
    select 
        b.batch_dt,  --以高频更新的数据为批次时间
        b.corp_id,
        b.corp_nm,
        b.score_dt,
        b.idx_name,
        b.idx_value,
        b.lst_idx_value,
        b.idx_unit,
        b.model_freq_type,  --特征所属子模型分类/模型频率分类
        b.sub_model_name,
        cal.median
    from warn_feature_value_with_median_cal cal 
    join warn_feature_value b 
        on cal.corp_id=b.corp_id and cal.batch_dt=b.batch_dt and cal.sub_model_name=b.sub_model_name and cal.idx_name=b.idx_name 
),
-- 特征贡献度 --
warn_contribution_ratio as 
(
    select distinct
        cast(a.end_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name,
        feature_pct*100 as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --异常标识 
        sub_model_name
    from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_contrib as --特征贡献度-合并高中低频
(
	select 
		cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --以高频更新的数据为批次时间
		chg.corp_id,
		chg.corp_name as corp_nm,
		to_date(end_dt) as score_dt,
		feature_name,
		feature_pct,   --已经*100
        model_freq_type,  --特征所属子模型分类/模型频率分类
		feature_risk_interval,  --特征异常标识
		model_name as sub_model_name,
		model_version
	from 
	(
		--高频
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --特征贡献度
			'高频' as model_freq_type,
			feature_risk_interval,  --特征异常标识（0/1,1代表异常）
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch
		union all 
		--低频
		select distinct
			k1.end_dt as batch_dt,
			k1.corp_code,
			k1.end_dt,
			k1.feature_name,
			cast(k1.feature_pct*100 as float) as feature_pct,  --特征贡献度
            k2.sub_model_type as model_freq_type,  --具体根据中正敞口分类的低频分类，例如 低频-金融平台，低频-房地产 ..
			-- '低频' as model_freq_type,
			k1.feature_risk_interval,  --特征异常标识（0/1,1代表异常）
			k1.model_name,
			k1.model_version
		from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch k1 
        join warn_feat_corp_property_CFG k2 
            on cast(k1.corp_code as string) = k2.corp_code
		union all 
		--中频-城投
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --特征贡献度
			'中频-城投' as model_freq_type,
			feature_risk_interval,  --特征异常标识（0/1,1代表异常）
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch 
		union all 
		--中频-产业
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --特征贡献度
			'中频-产业' as model_freq_type,
			feature_risk_interval,  --特征异常标识（0/1,1代表异常）
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch
	)A join corp_chg chg 
            on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_contrib_res1 as  --带有 维度贡献度占比 的特征贡献度-合并高中低频  
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        -- model_freq_type,  ----特征所属子模型分类/模型频率分类
        -- sum(feature_pct) as dim_submodel_contribution_ratio  --维度贡献度占比
       total_idx_is_abnormal_cnt/total_idx_cnt*100 as dim_submodel_contribution_ratio   --各维度 异常指标占比 (dim_submodel_contribution_ratio字段名沿用之前维度贡献度占比)
    from
    (
        select 
            a.batch_dt,
            a.corp_id,
            a.corp_nm,
            a.score_dt,
            f_cfg.dimension,
            a.feature_name,
            a.feature_pct,  --贡献度占比 %
            -- a.model_freq_type,
            a.feature_risk_interval,   --异常指标

            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension,a.feature_risk_interval) as total_idx_is_abnormal_cnt,   --对于每家企业每个时点各维度下的异常指标 以及 非异常指标之和  2022-11-12 新增
            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension) as total_idx_cnt,          --对于每家企业每个时点各维度下的指标之和 2022-11-12 新增
            -- a.model_name,
            a.sub_model_name
        from warn_feature_contrib a 
        join warn_feat_CFG f_cfg    --讨论后，直接采用join做关联，特征原始值没有的不考虑展示
        -- left join warn_feat_CFG f_cfg 
            on a.feature_name=f_cfg.feature_cd and a.model_freq_type=f_cfg.sub_model_type --and a.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    )B where feature_risk_interval = 1 --异常指标
    group by batch_dt,corp_id,corp_nm,score_dt,dimension,total_idx_is_abnormal_cnt,total_idx_cnt   --数据去重    --,model_freq_type
),
warn_feature_contrib_res2 as  -- 带有 维度风险等级 的特征贡献度-合并高中低频
(
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_risk_lv,
        dim_risk_lv_desc  --调整前维度风险等级 used
    from
    ( 
        select 
            *,
            first_value(risk_lv) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv,  --调整前维度风险等级（数值型）
            first_value(risk_lv_desc) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv_desc  --调整前维度风险等级
        from 
        (
            select distinct
                main.batch_dt,
                main.corp_id,
                main.corp_nm,
                main.score_dt,
                main.dimension,
                -- main.model_freq_type,
                main.dim_submodel_contribution_ratio,   --各子模型对应维度贡献度占比，used by 归因报告第二段
                b.risk_lv,
                b.risk_lv_desc   -- 原始风险等级描述
            from warn_feature_contrib_res1 main 
            join warn_dim_risk_level_cfg_ b 
                on main.dimension=b.dimension   --2022-11-12 维度风险等级配置表 新增dimension字段做阈值划分
            where main.dim_submodel_contribution_ratio>b.low_contribution_percent and main.dim_submodel_contribution_ratio<=b.high_contribution_percent
        )C 
    )D
),
warn_feature_contrib_res3_tmp as 
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.dimension,
        main.dim_risk_lv,
        main.dim_risk_lv_desc,  --维度风险等级 高风险，中风险，低风险
        nvl(b.adj_synth_level,'') as adj_synth_level,  --综合预警等级
        nvl(b.adjust_warnlevel,'') as adjust_warnlevel --调整后等级
    from warn_feature_contrib_res2 main
    left join warn_union_adj_sync_score b --预警分-模型结果表
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id
),
warn_feature_contrib_res3 as  -- 根据综合预警等级调整后的维度风险水平 的特征贡献度-合并高中低频
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_warn_level  --最终调整后的维度风险等级
    from 
    (
        select 
            a.*,
            case 
                when cast(a.dim_risk_lv as string)<>a.adjust_warnlevel then 
                    a.adjust_warnlevel
                else 
                    cast(a.dim_risk_lv as string)
            end as dim_warn_level  --根据综合预警等级调整后的维度风险水平
        from warn_feature_contrib_res3_tmp a 
        where a.dim_risk_lv in (select min(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp)  --风险最高的
        -- join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --获取最高风险水平对应的维度
        --     on a.dim_risk_lv=b.max_dim_risk_lv
        union all 
        select 
            a.*,
            cast(a.dim_risk_lv as string) as dim_warn_level
        from warn_feature_contrib_res3_tmp a 
        where a.dim_risk_lv not in (select min(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp)  --非风险最高的
        -- join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --获取除最高风险水平对应的维度
        -- where a.dim_risk_lv <> b.max_dim_risk_lv
    )C group by batch_dt,corp_id,corp_nm,score_dt,dimension,dim_warn_level  --去重
),
warn_contribution_ratio_with_factor_evl as  --带因子评价的特征贡献度应用层数据(不包含无监督)
(
    SELECT distinct
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_dt,
        a.feature_name,
        a.contribution_ratio,
        case 
            when a.abnormal_flag = 1 and b.idx_value is not null then 
                0  --异常 
            else 1 --正常 
        end as factor_evaluate,
        a.sub_model_name
    from (select * from warn_contribution_ratio where feature_name <> 'creditrisk_highfreq_unsupervised') a 
    left join (select * from warn_feature_value where idx_value is not null) b 
        on  a.corp_id=b.corp_id 
            and a.batch_dt=b.batch_dt 
            and a.sub_model_name=b.sub_model_name 
            and a.feature_name=b.idx_name
),
-- 评分卡 --
warn_score_card as 
(
    select 
        cast(max(a.batch_dt) over() as string) as batch_dt,  --以高频更新的数据为批次时间
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_score as idx_score,  --指标评分
        model_name as sub_model_name
    from 
    (
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch --高频-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch --低频-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch --中频_城投-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch --中频_产业债-频分卡
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
-- 结果集 --
res0 as   --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)  慢:1min  67万条
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        c.feature_name as idx_name,
        b.idx_value,   --今日指标值  ps:若为空，直接保留NULL，不随意对特征原始值赋默认值
        b.lst_idx_value as last_idx_value,  --昨日指标值
        '' as idx_unit,   --！！！待配置表补充完整
        c.model_freq_type,   --改用高中低频合并特征贡献度的 代码手工维护的中文名称的模型分类 2022-11-12
        c.sub_model_name,   --改用高中低频合并特征贡献度的 上游模型自带的子模型英文名称 2022-11-12
        b.median  
    from warn_feature_contrib c   --三频合并的特征贡献度  
    join  warn_union_adj_sync_score main --预警分
        on main.batch_dt=c.batch_dt and main.corp_id=c.corp_id
    left join warn_feature_value_with_median_res b  --三频合并的特征原始值
        on c.corp_id=b.corp_id and c.batch_dt=b.batch_dt and c.feature_name=b.idx_name
),
res1 as   --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合特征贡献度(无监督) 
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        b.contribution_ratio,  --贡献度占比
        b.factor_evaluate,  --因子评价
        b.sub_model_name as sub_model_name_zhgxd   --综合贡献度的子模型名称
    from res0 main
    left join warn_contribution_ratio_with_factor_evl b  
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.feature_name
    union all 
    --特征贡献度的无监督子模型 特殊处理  （只有贡献度占比数据，其余均为空，不下钻至因子层面，停留在dimension层）
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        feature_name as idx_name,
        NULL as idx_value,
        NULL as last_idx_value,
        '' as idx_unit,
        '无监督' model_freq_type,
        sub_model_name,
        NULL as median,
        contribution_ratio,
        NULL as factor_evaluate, 
        '' as sub_model_name_zhgxd 
    from ( select  a1.* FROM warn_contribution_ratio a1
            where a1.feature_name = 'creditrisk_highfreq_unsupervised'
        --    where a1.batch_dt in (select max(batch_dt) as max_batch_dt from warn_contribution_ratio_with_factor_evl)
                -- on a1.batch_dt and a2.batch_dt   --a1表的batch_dt和a2表需保持一致
            -- and a1.feature_name = 'creditrisk_highfreq_unsupervised'
        ) A 
),
res2 as --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合贡献度+指标评分卡 慢:1min20s  67万条
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,   --综合贡献度的子模型名称
        b.idx_score,
        b.sub_model_name as sub_model_name_zbpfk  --指标评分卡的字模型名称
    from  res1 main 
    left join warn_score_card b 
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.idx_name
),
res3 as   --预警分+特征原始值+综合贡献度+指标评分卡+特征配置表  慢:1min20s  40万条
(
    select  
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        f_cfg.unit_target as idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,  --综合贡献度的子模型名称
        main.idx_score,
        main.sub_model_name_zbpfk,
        f_cfg.sub_model_type,
        f_cfg.feature_name_target,
        f_cfg.dimension,
        f_cfg.type,
        f_cfg.cal_explain as idx_cal_explain,
        f_cfg.feature_explain as idx_explain,
        f_cfg.unit_origin,
        f_cfg.unit_target,
        count(*) over(partition by main.batch_dt,main.corp_id,main.score_dt,f_cfg.dimension,f_cfg.type) as  contribution_cnt  --归因个数计算，基于该时点企业对应type层的指标个数统计
        -- f_cfg.contribution_cnt  --归因个数
    from res2 main
    join warn_feat_CFG f_cfg
        on main.idx_name=f_cfg.feature_cd and main.model_freq_type=f_cfg.sub_model_type --and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    -- left join warn_feat_CFG f_cfg
),
res4 as -- --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合贡献度+指标评分卡+特征配置表+各维度风险水平(高中低频贡献度求得)   慢:1min20s  34万条
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.feature_name_target as idx_name,  --最后一步将idx_name调成为最终页面展示形式的指标名称
        case 
            when main.unit_origin='元' and main.unit_target='亿元' then 
                main.idx_value/100000000
            when main.unit_origin='元' and main.unit_target='万元' then 
                main.idx_value/10000
            when main.unit_origin='数值' and main.unit_target='%' then 
                main.idx_value*100
            when main.unit_origin='人' and main.unit_target='万人' then 
                main.idx_value/10000
            else 
                main.idx_value
        end as idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,  --综合贡献度的子模型名称
        main.idx_score,
        main.sub_model_name_zbpfk,
        main.sub_model_type,
        main.feature_name_target,
        main.dimension,
        b.dim_warn_level,  --最终调整后的维度风险等级(重难点)
        main.type,
        main.idx_cal_explain,
        main.idx_explain,
        main.last_idx_value,
        main.unit_origin,
        main.unit_target,
        main.contribution_cnt  --归因个数
    from res3 main
    left join warn_feature_contrib_res3 b  --获取维度风险等级数据，left join 以免丢失无监督数据
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id and main.dimension=b.dimension
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
insert into pth_rmp.RMP_WARNING_SCORE_DETAIL partition(etl_date=${ETL_DATE})
select 
    concat(corp_id,'_',MD5(concat(batch_dt,dimension,type,sub_model_name,idx_name))) as sid_kw,  --hive
    batch_dt,
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    dim_warn_level,  
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --！！！指标值最终需要转换为目标输出展示形态，和配置表的单位列有关，暂时输出原始值
    idx_unit,  
    idx_score,   
    cast(contribution_ratio as float) as contribution_ratio,   --贡献度占比 已转换为 百分比
    contribution_cnt,  
    factor_evaluate,
    median,  --！！！ 待测试
    last_idx_value,  --！！！
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from res4
where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
; 

