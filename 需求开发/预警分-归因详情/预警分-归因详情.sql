-- RMP_WARNING_SCORE_DETAIL (同步方式：一天对批次插入) --
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as --带有 城投/产业判断和国标一级行业 的特殊corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name

	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --预警分_融合调整后综合  原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
),
-- 特征原始值 --
_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --特征原始值_高频 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --特征原始值_低频 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --特征原始值_中频_城投 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --特征原始值_中频_产业债 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
),
-- 特征贡献度 --
_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --特征贡献度_融合调整后综合 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
),
-- 特征得分(特征打分卡) --
_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --特征得分_高频 原始接口 
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --特征得分_低频 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --特征得分_中频_城投 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --特征得分_中频_产业债 原始接口
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
),
-- _RMP_WARNING_SCORE_DETAIL_HIS_ as   --归因详情历史表(用于获取上一日指标值)
-- (
--     select  distinct
--         corp_id,
--         corp_nm,
--         score_dt,
--         sub_model_name,
--         idx_name,
--         idx_value,
--         idx_unint
--     from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS   --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
--     --where score_dt=to_date(date_add(current_timestamp(),-1))
-- ),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
feat_CFG as 
(
    select 
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
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
warn_union_adj_sync_score as --取最新批次的融合调整后综合预警分
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.rating_dt) as score_dt,
        a.total_score_adjusted as adj_score,
		case a.interval_text_adjusted
			when '绿色等级' then '-1' 
			when '黄色等级' then '-2'
			when '橙色等级' then '-3'
			when '红色等级' then '-4'
			when '风险已暴露' then '-5'
		end as adj_synth_level,
		a.model_name,
		a.model_version
    from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ ) b
        on a.rating_dt=b.max_rating_dt
    join corp_chg chg
        on chg.source_code='FI' and chg.source_id=cast(a.corp_code as string)
),
-- 特征原始值 --
warn_feature_value as --原始特征值_合并高中低频
(
    SELECT
        cast(max(a.batch_dt) over() as string) as batch_dt,  --以高频更新的数据为批次时间
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
        from _rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ 
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
        from _rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ 
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
        from _rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ 
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
        from _rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ 
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
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
        nvl(b.industryphy_name,'') as gb,
        nvl(b.bond_type,0) as bond_type  --0：非产业和城投 1：产业债 2：城投债
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='FI') b 
        on a.corp_id=b.corp_id 
),
warn_feature_value_with_median_cal as 
(
    select 
        batch_dt,
        score_dt,
        corp_id,
        bond_type,
        sub_model_name,
        idx_name,
        appx_median(idx_value) as median
        -- percentile(idx_value,0.5) as median  --hive
    from warn_feature_value_with_median
    where bond_type=2
    group by bond_type,corp_id,batch_dt,score_dt,sub_model_name,idx_name
    union all 
    select 
        batch_dt,
        score_dt,
        corp_id,
        bond_type,
        sub_model_name,
        idx_name,
        appx_median(idx_value) as median
    from warn_feature_value_with_median
    where bond_type<>2 and gb <> ''
    group by  bond_type,gb,corp_id,batch_dt,score_dt,sub_model_name,idx_name
),
warn_feature_value_with_median_res as 
(
    select 
        b.batch_dt,  --以高频更新的数据为批次时间
        b.corp_id,
        b.corp_nm,
        b.score_dt,
        b.idx_name,
        b.idx_value,
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
        feature_pct as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --异常标识 
        sub_model_name
    from _rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
),
warn_contribution_ratio_with_factor_evl as  --带因子评价的特征贡献度应用层数据
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
    from warn_contribution_ratio a 
    left join warn_feature_value b 
        on a.corp_id=b.corp_id and a.batch_dt=b.batch_dt and a.sub_model_name=b.sub_model_name
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
        from _rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ --高频-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ --低频-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ --中频_城投-频分卡
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ --中频_产业债-频分卡
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
),
warn_feat_CFG as 
(
    select 
        *,
        count(feature_cd) over(partition by dimension) as contribution_cnt
    from feat_CFG
),
-- -- 上一日指标值 --
-- warn_lastday_idx_value as 
-- (
--     select * 
--     from _RMP_WARNING_SCORE_DETAIL_HIS_
--     where score_dt=to_date(date_add(current_timestamp(),-1))
-- ),
-- 结果集 --
res0 as   --预警分+特征原始值
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        b.idx_name,
        b.idx_value, 
        '' as idx_unit,   --！！！待配置表补充完整
        b.model_freq_type,
        b.sub_model_name,
        b.median
    from warn_union_adj_sync_score main --预警分
    left join warn_feature_value_with_median_res b  --三频合并的特征原始值
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt
),
res1 as   --预警分+特征原始值+综合贡献度
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        b.contribution_ratio,  --贡献度占比
        b.factor_evaluate,  --因子评价
        b.sub_model_name as sub_model_name_zhgxd   --综合贡献度的子模型名称
    from res0 main
    left join warn_contribution_ratio_with_factor_evl b 
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt and main.sub_model_name=b.sub_model_name
),
res2 as --预警分+特征原始值+综合贡献度+指标评分卡
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
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
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt and main.sub_model_name=b.sub_model_name
),
res3 as   --预警分+特征原始值+综合贡献度+指标评分卡+特征配置表
(
    select distinct 
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
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
        case f_cfg.dimension 
            when '财务' then 1
            when '经营' then 2
            when '市场' then 3
            when '舆情' then 4
        end as dimension,
        f_cfg.type,
        f_cfg.cal_explain as idx_cal_explain,
        f_cfg.feature_explain as idx_explain,
        f_cfg.unit_origin,
        f_cfg.unit_target,
        f_cfg.contribution_cnt  --归因个数
    from res2 main
    join warn_feat_CFG f_cfg
        on main.idx_name=f_cfg.feature_cd and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
select 
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    '' as  dim_warn_level,  --！！！和模型融合方案有关，待定
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --！！！指标值最终需要转换为目标输出展示形态，和配置表的单位列有关，暂时输出原始值
    idx_unit,  
    idx_score,   
    contribution_ratio,
    contribution_cnt,  
    factor_evaluate,
    median,  --！！！ 待测试
    0 as last_idx_value,  --！！！
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from res3


