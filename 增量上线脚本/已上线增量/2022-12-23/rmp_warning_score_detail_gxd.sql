-- RMP_WARNING_SCORE_DETAIL_GXD (同步方式：一天多批次覆盖更新) --
-- /* 2022-12-06 修复 hive中执行warn_feat_corp_property_CFG返回空数据的问题，hive对于中文字符长度识别和Impala标准不同 */

-- part1 高中低频合并的 特征贡献度 --
set hive.exec.parallel=true;
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_feature_contrib;
create table pth_rmp.rmp_warn_feature_contrib as 
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
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --特征贡献度_融合调整后综合 原始接口（增加了无监督特征：creditrisk_highfreq_unsupervised  ）
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --特征贡献度_高频
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --特征贡献度_低频
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --特征贡献度_中频城投
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1              
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --特征贡献度_中频产业
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
         ) m  where rm=1 
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
        b.source_id as corp_code,
        '低频' as big_sub_model_type,
        a.sub_model_type,
        a.feature_cd,
        a.feature_name
    from warn_feat_CFG a 
    join corp_chg b 
        on substr(a.sub_model_type,instr(a.sub_model_type,'-')+1) = b.exposure
        -- on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX' 
    where instr(a.sub_model_type,'低频')>0
    -- where substr(a.sub_model_type,1,6) = '低频'
    group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name
    -- select m.*
    -- from
    -- (
    --     select 
    --         -- b.corp_id,
    --         b.source_id as corp_code,
    --         b.etl_date,
    --         -- max(b.corp_name) as corp_nm,
    --         '低频' as big_sub_model_type,
    --         a.sub_model_type,
    --         a.feature_cd,
    --         a.feature_name,
    --         rank() over(partition by b.source_id,a.feature_cd,a.feature_name order by b.etl_date desc) as rm
    --     from warn_feat_CFG a 
    --     join corp_exposure b 
    --         on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX'
    --     where substr(a.sub_model_type,1,6) = '低频'
    -- ) m where rm=1
    -- group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name,b.etl_date   --去除重复数据
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
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
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
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
)
select * from warn_feature_contrib
;



-- part2 综合预警 特征贡献度 --
set hive.exec.parallel=true;
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_contribution_ratio;
create table pth_rmp.rmp_warn_contribution_ratio as 
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
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --特征贡献度_融合调整后综合 原始接口（增加了无监督特征：creditrisk_highfreq_unsupervised  ）
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 非时间限制部分 --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch as  --特征贡献度_融合调整后综合 原始接口（增加了无监督特征：creditrisk_highfreq_unsupervised  ）
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
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
)
select * from warn_contribution_ratio
;



-- part3 --
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_feature_contrib_res3;
create table pth_rmp.rmp_warn_feature_contrib_res3 as 
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
        b.source_id as corp_code,
        '低频' as big_sub_model_type,
        a.sub_model_type,
        a.feature_cd,
        a.feature_name
    from warn_feat_CFG a 
    join corp_chg b 
        on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX' 
    where substr(a.sub_model_type,1,6) = '低频'
    group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name
    -- select m.*
    -- from
    -- (
    --     select 
    --         -- b.corp_id,
    --         b.source_id as corp_code,
    --         b.etl_date,
    --         -- max(b.corp_name) as corp_nm,
    --         '低频' as big_sub_model_type,
    --         a.sub_model_type,
    --         a.feature_cd,
    --         a.feature_name,
    --         rank() over(partition by b.source_id,a.feature_cd,a.feature_name order by b.etl_date desc) as rm
    --     from warn_feat_CFG a 
    --     join corp_exposure b 
    --         on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX'
    --     where substr(a.sub_model_type,1,6) = '低频'
    -- ) m where rm=1
    -- group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name,b.etl_date   --去除重复数据
),
--—————————————————————————————————————————————————————— 加工后接口 ————————————————————————————————————————————————————————————————————————————————--
warn_feature_contrib as 
(
    select * 
    from pth_rmp.rmp_warn_feature_contrib
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
warn_feature_contrib_res1 as  --带有 维度异常指标占比 的特征贡献度-合并高中低频  
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        -- feature_risk_interval,
        -- model_freq_type,  ----特征所属子模型分类/模型频率分类
        -- sum(feature_pct) as dim_submodel_contribution_ratio  --维度贡献度占比
        nvl(total_idx_is_abnormal_cnt/total_idx_cnt*100,0) as dim_submodel_contribution_ratio
    --    nvl(total_idx_is_abnormal_cnt/total_idx_cnt*100,0) as dim_submodel_contribution_ratio   --各维度 异常指标占比 (dim_submodel_contribution_ratio字段名沿用之前维度贡献度占比)
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
            sum(feature_risk_interval) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension ) as total_idx_is_abnormal_cnt,

            -- count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension,a.feature_risk_interval) as total_idx_is_abnormal_cnt,   --对于每家企业每个时点各维度下的异常指标 以及 非异常指标之和  2022-11-12 新增
            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension) as total_idx_cnt,          --对于每家企业每个时点各维度下的指标之和 2022-11-12 新增
            -- a.model_name,
            a.sub_model_name
        from warn_feature_contrib a 
        join warn_feat_CFG f_cfg    --讨论后，直接采用join做关联，特征原始值没有的不考虑展示
        -- left join warn_feat_CFG f_cfg 
            on a.feature_name=f_cfg.feature_cd and a.model_freq_type=f_cfg.sub_model_type --and a.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    )B --where feature_risk_interval = 1 --异常指标
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
        dim_submodel_contribution_ratio,  --各维度 异常指标占比
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
                main.dim_submodel_contribution_ratio,   ----各维度 异常指标占比 used by 归因报告第二段
                b.risk_lv,
                b.risk_lv_desc   -- 原始风险等级描述
            from warn_feature_contrib_res1 main 
            join warn_dim_risk_level_cfg_ b 
                on main.dimension=b.dimension   --2022-11-12 维度风险等级配置表 新增dimension字段做阈值划分
            where main.dim_submodel_contribution_ratio>b.low_contribution_percent and main.dim_submodel_contribution_ratio<=b.high_contribution_percent
        )C 
    )D
),
warn_feature_contrib_res3 as
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.dimension,
        main.dim_submodel_contribution_ratio,
        main.dim_risk_lv,
        main.dim_risk_lv_desc,  --维度风险等级 高风险，中风险，低风险
        cast(main.dim_risk_lv as string) as dim_warn_level
        -- nvl(b.adj_synth_level,'') as adj_synth_level,  --综合预警等级
        -- nvl(b.adjust_warnlevel,'') as adjust_warnlevel --调整后等级
    from warn_feature_contrib_res2 main
    -- left join warn_union_adj_sync_score b --预警分-模型结果表
    --     on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id
)
select * from warn_feature_contrib_res3
;