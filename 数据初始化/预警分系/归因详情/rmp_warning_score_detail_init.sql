--??1??DDL RMP_WARNING_SCORE_DETAIL_INIT hive峇佩 --
-- 拷咀?蠻蘋?雰 RMP_WARNING_SCORE_DETAIL_INIT --
drop table if exists pth_rmp.RMP_WARNING_SCORE_DETAIL_INIT;
create table pth_rmp.RMP_WARNING_SCORE_DETAIL_INIT
(
    sid_kw  string,
    corp_id string,
    corp_nm string,
    score_dt timestamp,
    dimension int,
    dim_warn_level string,
    type_cd int,
    type string,
    sub_model_name string,
    idx_name string,
    idx_value float,
    idx_unit string,
    idx_score float,
    contribution_ratio float,
    contribution_cnt bigint,
    factor_evaluate int,
    median  float,
    last_idx_value float,
    idx_cal_explain string,
    idx_explain string,
	delete_flag	int,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	int
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;

--重云介蛍 warn_feature_contrib+warn_contribution_ratio+warn_feature_contrib_res3    warn_feature_value_with_median_res  warn_score_card  warn_feature_contrib_res3
--??2??sql兜兵晒 RMP_WARNING_SCORE_DETAIL_INIT_IMPALA impala峇佩 --
create table pth_rmp.RMP_WARNING_SCORE_DETAIL_INIT_IMPALA as 
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 児云佚連 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
with
corp_chg as  --揮嗤 廓誘/恢匍登僅才忽炎匯雫佩匍 議蒙歩corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.zjh_industry_l1 as industryphy_name  --屬酌氏佩匍
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 俊笥蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 扮寂?渣匿?購 --
timeLimit_switch as 
(
    select false as flag   --TRUE:扮寂埃崩??FLASE:扮寂音恂埃崩??宥械喘噐兜兵晒
    -- select False as flag
),
-- 圓少蛍 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --圓少蛍_蛮栽距屁朔忝栽  圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
-- 蒙尢圻兵峙(函書恍曾爺方象) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --蒙尢圻兵峙_互撞 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
      and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select *
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --蒙尢圻兵峙_詰撞 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
      and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --蒙尢圻兵峙_嶄撞_廓誘 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
      and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --蒙尢圻兵峙_嶄撞_恢匍娥 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
      and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 蒙尢恒?弑? --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --蒙尢恒?弑?_蛮栽距屁朔忝栽 圻兵俊笥??奐紗阻涙酌興蒙尢??creditrisk_highfreq_unsupervised  ??
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --蒙尢恒?弑?_互撞
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --蒙尢恒?弑?_詰撞
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --蒙尢恒?弑?_嶄撞廓誘
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --蒙尢恒?弑?_嶄撞恢匍
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 蒙尢誼蛍(蒙尢嬉蛍触) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --蒙尢誼蛍_互撞 圻兵俊笥 
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --蒙尢誼蛍_詰撞 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --蒙尢誼蛍_嶄撞_廓誘 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --蒙尢誼蛍_嶄撞_恢匍娥 圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 塘崔燕 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
warn_dim_risk_level_cfg_ as  -- 略業恒?弑晩識閥墫Ψ舅嬲?峠-塘崔燕
(
	select
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- 互欠?? ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
feat_CFG as  --蒙尢返垢塘崔燕
(
    select distinct
        feature_cd,
        feature_name,
        substr(sub_model_type,1,6) as sub_model_type,  --函念曾倖嶄猟忖憲
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type not in ('嶄撞-恢匍','嶄撞-廓誘','涙酌興')
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
    where sub_model_type in ('嶄撞-恢匍','嶄撞-廓誘','涙酌興')
),
--啌符朔 蒙尢返垢塘崔燕 --
warn_feat_CFG as 
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,
        feature_name_target,
        case dimension 
            when '夏暦' then 1
            when '将唔' then 2
            when '偏魁' then 3
            when '嘸秤' then 4
            when '呟械欠?媼豌?' then 5
        end as dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
        -- count(feature_cd) over(partition by dimension,type) as contribution_cnt
    from feat_CFG
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 圓少蛍 --
RMP_WARNING_SCORE_MODEL_ as  --圓少蛍-庁侏潤惚燕??厮頁恷仟答肝??
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 圓少蛍
		case a.interval_text_adjusted
			when '駄弼圓少' then '-1' 
			when '仔弼圓少' then '-2'
			when '拡弼圓少' then '-3'
			when '碕弼圓少' then '-4'
			when '欠?孀儕?其' then '-5'
		end as synth_warnlevel,  -- 忝栽圓少吉雫,
		case
			when a.interval_text_adjusted in ('駄弼圓少','仔弼圓少') then 
				'-1'   --詰欠??
			when a.interval_text_adjusted  = '拡弼圓少' then 
				'-2'  --嶄欠??
			when a.interval_text_adjusted  ='碕弼圓少' then 
				'-3'  --互欠??
			when a.interval_text_adjusted  ='欠?孀儕?其' then 
				'-4'   --欠?孀儕?其
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a 
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
	-- where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    -- from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
-- 蒙尢圻兵峙(函書恍曾爺方象) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch as  --蒙尢圻兵峙_互撞 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch as  --蒙尢圻兵峙_詰撞 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch as  --蒙尢圻兵峙_嶄撞_廓誘 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch as  --蒙尢圻兵峙_嶄撞_恢匍娥 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
-- 蒙尢恒?弑?(函書恍曾爺方象??茅阻忝栽圓少蒙尢恒?弑?) --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch as  --蒙尢恒?弑?_蛮栽距屁朔忝栽 圻兵俊笥??奐紗阻涙酌興蒙尢??creditrisk_highfreq_unsupervised  ??
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch as --蒙尢恒?弑?_互撞
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch as  --蒙尢恒?弑?_詰撞
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch as  --蒙尢恒?弑?_嶄撞廓誘
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch as  --蒙尢恒?弑?_嶄撞恢匍
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
-- 蒙尢誼蛍(蒙尢嬉蛍触) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch as  --蒙尢誼蛍_互撞 圻兵俊笥 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch as  --蒙尢誼蛍_詰撞 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch as  --蒙尢誼蛍_嶄撞_廓誘 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch as  --蒙尢誼蛍_嶄撞_恢匍娥 圻兵俊笥
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 圓少蛍 --
warn_union_adj_sync_score as --函恷仟答肝議圓少蛍-庁侏潤惚燕
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
-- 蒙尢圻兵峙 --
warn_feature_value_two_days as --圻兵蒙尢峙_栽旺互嶄詰撞(淫根書恍曾爺方象??佩侘塀)
(
    SELECT
        cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --參互撞厚仟議方象葎答肝扮寂
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_value as idx_value,
        '' as idx_unit,
        model_freq_type,  --蒙尢侭奉徨庁侏蛍窃/庁侏撞楕蛍窃
        model_name as sub_model_name
    from
    (
        --互撞
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '互撞' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch 
        union all 
        --詰撞
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '詰撞' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch  
        union all 
        --嶄撞_廓誘
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '嶄撞-廓誘' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch 
        union all 
        --嶄撞_恢匍娥
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '嶄撞-恢匍' as model_freq_type,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch 
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_value as --圻兵蒙尢峙_栽旺互嶄詰撞(淫根書恍曾爺方象??双侘塀 used)
(
    select 
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_dt,
        a.idx_name,
        a.idx_value,
        b.score_dt as lst_score_dt,  --恍晩晩豚
        b.idx_value as lst_idx_value,  --恍晩峺炎峙
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name
    from warn_feature_value_two_days a   --書
    join warn_feature_value_two_days b   --恍
        on  a.corp_id = b.corp_id 
            and date_add(a.score_dt,-1)=b.score_dt 
            and a.sub_model_name=b.sub_model_name  
            and a.idx_name=b.idx_name
),
warn_feature_value_with_median as --圻兵蒙尢峙_栽旺互嶄詰撞+嶄了方柴麻
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
        nvl(b.bond_type,0) as bond_type  --0?嵯撚?匍才廓誘 1?魂?匍娥 2?些罵欣?
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='ZXZX') b 
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
    where bond_type<>2 and zjh <> ''
    group by  bond_type,zjh,corp_id,batch_dt,score_dt,sub_model_name,idx_name
),
warn_feature_value_with_median_res as 
(
    select 
        b.batch_dt,  --參互撞厚仟議方象葎答肝扮寂
        b.corp_id,
        b.corp_nm,
        b.score_dt,
        b.idx_name,
        b.idx_value,
        b.lst_idx_value,
        b.idx_unit,
        b.model_freq_type,  --蒙尢侭奉徨庁侏蛍窃/庁侏撞楕蛍窃
        b.sub_model_name,
        cal.median
    from warn_feature_value_with_median_cal cal 
    join warn_feature_value b 
        on cal.corp_id=b.corp_id and cal.batch_dt=b.batch_dt and cal.sub_model_name=b.sub_model_name and cal.idx_name=b.idx_name 
),
-- 蒙尢恒?弑? --
warn_contribution_ratio as 
(
    select distinct
        cast(a.end_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name,
        feature_pct*100 as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --呟械炎紛 
        sub_model_name
    from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_contrib as --蒙尢恒?弑?-栽旺互嶄詰撞
(
	select 
		cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --參互撞厚仟議方象葎答肝扮寂
		chg.corp_id,
		chg.corp_name as corp_nm,
		to_date(end_dt) as score_dt,
		feature_name,
		feature_pct,   --厮将*100
        model_freq_type,  --蒙尢侭奉徨庁侏蛍窃/庁侏撞楕蛍窃
		feature_risk_interval,  --蒙尢呟械炎紛
		model_name as sub_model_name,
		model_version
	from 
	(
		--互撞
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --蒙尢恒?弑?
			'互撞' as model_freq_type,
			feature_risk_interval,  --蒙尢呟械炎紛??0/1,1旗燕呟械??
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch
		union all 
		--詰撞
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --蒙尢恒?弑?
			'詰撞' as model_freq_type,
			feature_risk_interval,  --蒙尢呟械炎紛??0/1,1旗燕呟械??
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch
		union all 
		--嶄撞-廓誘
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --蒙尢恒?弑?
			'嶄撞-廓誘' as model_freq_type,
			feature_risk_interval,  --蒙尢呟械炎紛??0/1,1旗燕呟械??
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch 
		union all 
		--嶄撞-恢匍
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --蒙尢恒?弑?
			'嶄撞-恢匍' as model_freq_type,
			feature_risk_interval,  --蒙尢呟械炎紛??0/1,1旗燕呟械??
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch
	)A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_contrib_res1 as  --揮嗤 略業恒?弑晩識? 議蒙尢恒?弑?-栽旺互嶄詰撞  
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        model_freq_type,  ----蒙尢侭奉徨庁侏蛍窃/庁侏撞楕蛍窃
        sum(feature_pct) as dim_submodel_contribution_ratio  --略業恒?弑晩識?
    from
    (
        select distinct
            a.batch_dt,
            a.corp_id,
            a.corp_nm,
            a.score_dt,
            f_cfg.dimension,
            a.feature_name,
            a.feature_pct,  --恒?弑晩識? %
            a.model_freq_type,
            a.feature_risk_interval,
            -- a.model_name,
            a.sub_model_name
        from warn_feature_contrib a 
        join warn_feat_CFG f_cfg    --網胎朔??岷俊寡喘join恂購選??蒙尢圻兵峙短嗤議音深打婢幣
        -- left join warn_feat_CFG f_cfg 
            on a.feature_name=f_cfg.feature_cd and a.model_freq_type=f_cfg.sub_model_type --and a.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    )B group by batch_dt,corp_id,corp_nm,score_dt,dimension,model_freq_type
),
warn_feature_contrib_res2 as  -- 揮嗤 略業欠?婬伴? 議蒙尢恒?弑?-栽旺互嶄詰撞
(
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_risk_lv,
        dim_risk_lv_desc  --距屁念略業欠?婬伴? used
    from
    ( 
        select 
            *,
            first_value(risk_lv) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv,  --距屁念略業欠?婬伴僑?方峙侏??
            first_value(risk_lv_desc) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv_desc  --距屁念略業欠?婬伴?
        from 
        (
            select distinct
                main.batch_dt,
                main.corp_id,
                main.corp_nm,
                main.score_dt,
                main.dimension,
                main.model_freq_type,
                main.dim_submodel_contribution_ratio,   --光徨庁侏斤哘略業恒?弑晩識硲?used by 拷咀烏御及屈粁
                b.risk_lv,
                b.risk_lv_desc   -- 圻兵欠?婬伴驚菠?
            from warn_feature_contrib_res1 main 
            join warn_dim_risk_level_cfg_ b 
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
        main.dim_risk_lv_desc,  --略業欠?婬伴? 互欠?妝?嶄欠?妝?詰欠??
        nvl(b.adj_synth_level,'') as adj_synth_level,  --忝栽圓少吉雫
        nvl(b.adjust_warnlevel,'') as adjust_warnlevel --距屁朔吉雫
    from warn_feature_contrib_res2 main
    left join warn_union_adj_sync_score b --圓少蛍-庁侏潤惚燕
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id
),
warn_feature_contrib_res3 as  -- 功象忝栽圓少吉雫距屁朔議略業欠?嬲?峠 議蒙尢恒?弑?-栽旺互嶄詰撞
(
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_warn_level  --恷嶮距屁朔議略業欠?婬伴?
    from 
    (
        select 
            *,
            case 
                when cast(dim_risk_lv as string)<>adjust_warnlevel then 
                    adjust_warnlevel
                else 
                    cast(dim_risk_lv as string)
            end as dim_warn_level  --功象忝栽圓少吉雫距屁朔議略業欠?嬲?峠
        from warn_feature_contrib_res3_tmp a 
        join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --資函資函恷互欠?嬲?峠斤哘議略業
            on a.dim_risk_lv=b.max_dim_risk_lv
        union all 
        select 
            *,
            cast(dim_risk_lv as string) as dim_warn_level
        from warn_feature_contrib_res3_tmp a 
        join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --資函茅恷互欠?嬲?峠斤哘議略業
        where a.dim_risk_lv <> b.max_dim_risk_lv
    )C
),
warn_contribution_ratio_with_factor_evl as  --揮咀徨得勺議蒙尢恒?弑飯τ嘆稱?象(音淫根涙酌興)
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
                0  --呟械 
            else 1 --屎械 
        end as factor_evaluate,
        a.sub_model_name
    from (select * from warn_contribution_ratio where feature_name <> 'creditrisk_highfreq_unsupervised') a 
    left join (select * from warn_feature_value where idx_value is not null) b 
        on  a.corp_id=b.corp_id 
            and a.batch_dt=b.batch_dt 
            and a.sub_model_name=b.sub_model_name 
            and a.feature_name=b.idx_name
),
-- 得蛍触 --
warn_score_card as 
(
    select 
        cast(max(a.batch_dt) over() as string) as batch_dt,  --參互撞厚仟議方象葎答肝扮寂
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_score as idx_score,  --峺炎得蛍
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
        from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch --互撞-撞蛍触
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch --詰撞-撞蛍触
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch --嶄撞_廓誘-撞蛍触
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch --嶄撞_恢匍娥-撞蛍触
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
-- 潤惚鹿 --
res0 as   --圓少蛍+蒙尢圻兵峙(蒙尢圻兵峙兆各參互嶄詰撞栽旺議蒙尢恒?弑髪輹亠通慱?兆各葎彈)
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        c.feature_name as idx_name,
        b.idx_value,   --書晩峺炎峙  ps:飛葎腎??岷俊隠藻NULL??音昧吭斤蒙尢圻兵峙験潮範峙
        b.lst_idx_value as last_idx_value,  --恍晩峺炎峙
        '' as idx_unit,   --?。。ヾ?塘崔燕温割頼屁
        b.model_freq_type,
        b.sub_model_name,
        b.median  
    from warn_feature_contrib c   --眉撞栽旺議蒙尢恒?弑?  
    join  warn_union_adj_sync_score main --圓少蛍
        on main.batch_dt=c.batch_dt and main.corp_id=c.corp_id
    left join warn_feature_value_with_median_res b  --眉撞栽旺議蒙尢圻兵峙
        on c.corp_id=b.corp_id and c.batch_dt=b.batch_dt and c.feature_name=b.idx_name
),
res1 as   --圓少蛍+蒙尢圻兵峙(蒙尢圻兵峙兆各參互嶄詰撞栽旺議蒙尢恒?弑髪輹亠通慱?兆各葎彈)+忝栽恒?弑?
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
        b.contribution_ratio,  --恒?弑晩識?
        b.factor_evaluate,  --咀徨得勺
        b.sub_model_name as sub_model_name_zhgxd   --忝栽恒?弑筏鍔喞Ｐ傭?各
    from res0 main
    left join warn_contribution_ratio_with_factor_evl b  
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.feature_name
    union all 
    --蒙尢恒?弑筏栂渕犇竣喞Ｐ? 蒙歩侃尖  ??峪嗤恒?弑晩識畔?象??凪噫譲葎腎??音和怺崛咀徨蚊中??唯藻壓dimension蚊??
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        feature_name as idx_name,
        NULL as idx_value,
        NULL as last_idx_value,
        '' as idx_unit,
        '涙酌興' model_freq_type,
        sub_model_name,
        NULL as median,
        contribution_ratio,
        NULL as factor_evaluate, 
        '' as sub_model_name_zhgxd 
    from ( select  a1.* FROM warn_contribution_ratio a1
            where a1.feature_name = 'creditrisk_highfreq_unsupervised'
        --    where a1.batch_dt in (select max(batch_dt) as max_batch_dt from warn_contribution_ratio_with_factor_evl)
                -- on a1.batch_dt and a2.batch_dt   --a1燕議batch_dt才a2燕俶隠隔匯崑
            -- and a1.feature_name = 'creditrisk_highfreq_unsupervised'
        ) A 
),
res2 as --圓少蛍+蒙尢圻兵峙(蒙尢圻兵峙兆各參互嶄詰撞栽旺議蒙尢恒?弑髪輹亠通慱?兆各葎彈)+忝栽恒?弑?+峺炎得蛍触
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
        main.contribution_ratio,  --恒?弑晩識?
        main.factor_evaluate,  --咀徨得勺
        main.sub_model_name_zhgxd,   --忝栽恒?弑筏鍔喞Ｐ傭?各
        b.idx_score,
        b.sub_model_name as sub_model_name_zbpfk  --峺炎得蛍触議忖庁侏兆各
    from  res1 main 
    left join warn_score_card b 
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.idx_name
),
res3 as   --圓少蛍+蒙尢圻兵峙+忝栽恒?弑?+峺炎得蛍触+蒙尢塘崔燕
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
        main.contribution_ratio,  --恒?弑晩識?
        main.factor_evaluate,  --咀徨得勺
        main.sub_model_name_zhgxd,  --忝栽恒?弑筏鍔喞Ｐ傭?各
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
        count() over(partition by main.batch_dt,main.corp_id,main.score_dt,f_cfg.dimension) as  contribution_cnt  --拷咀倖方柴麻??児噐乎扮泣二匍斤哘type蚊議峺炎倖方由柴
        -- f_cfg.contribution_cnt  --拷咀倖方
    from res2 main
    join warn_feat_CFG f_cfg
    -- left join warn_feat_CFG f_cfg
        on main.idx_name=f_cfg.feature_cd and main.model_freq_type=f_cfg.sub_model_type --and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
),
res4 as -- --圓少蛍+蒙尢圻兵峙(蒙尢圻兵峙兆各參互嶄詰撞栽旺議蒙尢恒?弑髪輹亠通慱?兆各葎彈)+忝栽恒?弑?+峺炎得蛍触+蒙尢塘崔燕+光略業欠?嬲?峠(互嶄詰撞恒?弑版鶺?)
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
        main.contribution_ratio,  --恒?弑晩識?
        main.factor_evaluate,  --咀徨得勺
        main.sub_model_name_zhgxd,  --忝栽恒?弑筏鍔喞Ｐ傭?各
        main.idx_score,
        main.sub_model_name_zbpfk,
        main.sub_model_type,
        main.feature_name_target,
        main.dimension,
        b.dim_warn_level,  --恷嶮距屁朔議略業欠?婬伴?(嶷佃泣)
        main.type,
        main.idx_cal_explain,
        main.idx_explain,
        main.last_idx_value,
        main.unit_origin,
        main.unit_target,
        main.contribution_cnt  --拷咀倖方
    from (select distinct * from res3) main
    join warn_feature_contrib_res3 b  --參互嶄詰撞栽旺議蒙尢恒?弑筏通慱?峙葎児彈
    -- left join warn_feature_contrib_res3 b
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id and main.dimension=b.dimension
)
------------------------------------參貧何蛍葎匝扮燕-------------------------------------------------------------------
select distinct
    -- concat(corp_id,'_',MD5(concat(batch_dt,dimension,type,sub_model_name,idx_name))) as sid_kw,  --hive
    -- batch_dt,
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    dim_warn_level,  
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --?。。「険袂久釀孃菻?廬算葎朕炎補竃婢幣侘蓑??才塘崔燕議汽了双嗤購??壙扮補竃圻兵峙
    idx_unit,  
    idx_score,   
    cast(contribution_ratio as float) as contribution_ratio,   --恒?弑晩識? 厮廬算葎 為蛍曳
    contribution_cnt,  
    factor_evaluate,
    median,  --?。。? 棋霞編
    last_idx_value,  --?。。?
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from res4
where score_dt >= '2022-09-24'
  and score_dt <= '2022-10-14'
; 


--??3??sql兜兵晒 rmp_warning_score_detail_init hive峇佩 --
insert into pth_rmp.rmp_warning_score_detail_init partition(etl_date=19900101)
select 
    concat(corp_id,'_',MD5(concat(score_dt,dimension,type,sub_model_name,idx_name))) as sid_kw ,
    corp_id ,
    corp_nm ,
    score_dt ,
    dimension ,
    dim_warn_level ,
    type_cd ,
    type ,
    sub_model_name ,
    idx_name ,
    idx_value ,
    idx_unit ,
    idx_score ,
    contribution_ratio ,
    contribution_cnt ,
    factor_evaluate ,
    median  ,
    last_idx_value ,
    idx_cal_explain ,
    idx_explain ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version	
from pth_rmp.rmp_warning_score_detail_init_impala
;