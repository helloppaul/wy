-- RMP_WARNING_SCORE_DETAIL_DFK (同步方式：一天对批次插入) --

drop table if exists pth_rmp.rmp_warn_score_card_test;
create table pth_rmp.rmp_warn_score_card_test as 
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
-- 特征得分(特征打分卡) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --特征得分_高频 原始接口 
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
            where end_dt=${END_DT}
            -- and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))  
         ) m  where rm=1  
    ) a join pth_rmp.model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
    
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --特征得分_低频 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
            where 1=1
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))   
         ) m  where rm=1 
    ) a join pth_rmp.model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --特征得分_中频_城投 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
            where 1=1
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))     
         ) m  where rm=1 
    ) a join pth_rmp.model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --特征得分_中频_产业债 原始接口
(
    select a.*
    from 
    (
        -- 时间限制部分 --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
            where 1=1
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
         ) m  where rm=1 
    ) a join pth_rmp.model_version_intf_ b
    on a.model_version = b.model_version and a.model_name=b.model_name 
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
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
-- 评分卡 --
warn_score_card as 
(
    select 
        cast(max(a.batch_dt) over(partition by to_date(a.end_dt)) as string) as batch_dt,  --以高频更新的数据为批次时间
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
)
select * 
from warn_score_card
;