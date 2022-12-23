
-- RMP_WARNING_SCORE_DETAIL_YSZ (同步方式：一天多批次覆盖更新) 50min--
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16; 
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

-- 特征原始值+中位数计算 -- 
-- drop table if exists pth_rmp.rmp_warn_feature_value_with_median_res;
-- create table pth_rmp.rmp_warn_feature_value_with_median_res stored as parquet as 
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
-- 特征原始值(取今昨两天数据) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --特征原始值_高频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
            -- group by to_date(end_dt) 
        union all 
        -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --特征原始值_低频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --特征原始值_中频_城投 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --特征原始值_中频_产业债 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
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
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
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
    left join warn_feature_value_two_days b   --昨
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
                '城投'
            when nvl(b.bond_type,0)<>2 then 
                nvl(b.industryphy_name,'')
        end as zjh_cal   --计算用证监会一级行业分类
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='ZXZX') b 
        on a.corp_id=b.corp_id 
),
hy_median as --行业中位数
(
    select score_dt,zjh_cal,sub_model_name,idx_name
    -- ,appx_median(idx_value) as median  --impala
    ,percentile_approx(idx_value,0.5) as median  --hive
    from warn_feature_value_with_median
    group by score_dt,zjh_cal,sub_model_name,idx_name
), 
warn_feature_value_with_median_cal as 
(
    select a.batch_dt,a.corp_id,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name,b.median
    from warn_feature_value_with_median a 
    join hy_median b 
        on a.score_dt=b.score_dt and a.zjh_cal=b.zjh_cal and a.sub_model_name=b.sub_model_name and a.idx_name=b.idx_name
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
    from warn_feature_value b 
    left join warn_feature_value_with_median_cal cal    --特征原始值数据主表来源于合并的高中低频特征原始值，而非计算中位数的表，以免在res0结果中丢失特征原始值
    -- from warn_feature_value_with_median_cal cal 
    -- join [SHUFFLE] warn_feature_value b 
        on cal.corp_id=b.corp_id and cal.score_dt=b.score_dt and cal.sub_model_name=b.sub_model_name and cal.idx_name=b.idx_name 
)
insert overwrite table pth_rmp.rmp_warn_feature_value_with_median_res
select * 
from warn_feature_value_with_median_res
where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;