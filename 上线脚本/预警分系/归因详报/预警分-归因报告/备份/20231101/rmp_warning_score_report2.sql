-- RMP_WARNING_SCORE_REPORT 及屈粁才及励粁-輝念吉雫拷咀才秀咏購廣欠?? 
-- /* 2022-11-25 遍枠葎->麼勣葎 */--
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16; 
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;



-- drop table if exists pth_rmp.rmp_warning_score_report2;    
-- create table pth_rmp.rmp_warning_score_report2 as      --@pth_rmp.rmp_warning_score_report2
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 児云佚連 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
with
corp_chg as  --揮嗤 廓誘/恢匍登僅才忽炎匯雫佩匍/屬酌氏匯雫佩匍 議蒙歩corp_chg  (蒙歩2)
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,  --1 恢匍娥 2 廓誘娥
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 俊笥蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 扮寂?渣匿?購 --
timeLimit_switch as 
(
    select True as flag   --TRUE:扮寂埃崩??FLASE:扮寂音恂埃崩??宥械喘噐兜兵晒
    -- select False as flag
),
-- 庁侏井云陣崙 --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
    select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --詰撞庁侏
    union all
    select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --嶄撞-廓誘庁侏
    union all 
    select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --嶄撞-恢匍庁侏
    union all 
    select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --互撞-得蛍触庁侏(互撞)
    union all 
    select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --互撞-涙酌興庁侏
    union all 
    select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --佚喘欠?孥杠歪Ｐ?
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
-- 圓少蛍 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --圓少蛍_蛮栽距屁朔忝栽  圻兵俊笥
(
	select a.*
    from 
    (
		select m.*
		from
		(
			-- 扮寂?渣堂新? --
			select *,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 in (select max(flag) from timeLimit_switch) 
			and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
			union all
			-- 掲扮寂?渣堂新? --
			select * ,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 in (select not max(flag) from timeLimit_switch) 
		) m where rm=1
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
-- 拷咀?蠻? --
RMP_WARNING_SCORE_DETAIL_ as  --圓少蛍--拷咀?蠻? 圻兵俊笥
(
	-- 扮寂?渣堂新? --
	select * 
	from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 掲扮寂?渣堂新? --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 蒙尢恒?弑? --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --蒙尢恒?弑?_蛮栽距屁朔忝栽 圻兵俊笥??奐紗阻涙酌興蒙尢??creditrisk_highfreq_unsupervised  ??
(
    select a.*
    from 
    (
        -- 扮寂?渣堂新? --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- 掲扮寂?渣堂新? --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
-- 仟療巷御 --
news_intf_ as 
(
	-- 扮寂?渣堂新? --
    select *
    from pth_rmp.rmp_opinion_risk_info_07 --@pth_rmp.rmp_opinion_risk_info_07
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- 除12倖埖議仟療方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from pth_rmp.rmp_opinion_risk_info_07 --@pth_rmp.rmp_opinion_risk_info_07
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 穫佚 --
cx_intf_ as 
(
	-- 扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議仟療方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 望隈 --
sf_ktts_inft_ as --蝕優優蕪
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議蝕優優蕪方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --加登猟慕
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議蝕優優蕪方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 塘崔燕 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
warn_dim_risk_level_cfg_ as  -- 略業恒?弑晩識閥墫Ψ舅嬲?峠-塘崔燕
(
	select
        dimension,
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
        sub_model_type,  --詰撞-署蛮峠岬、詰撞-匳勞崙夛 ...
        -- substr(sub_model_type,1,6) as sub_model_type,  --函念曾倖嶄猟忖憲
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 圓少蛍 --
RMP_WARNING_SCORE_MODEL_ as  --圓少蛍-庁侏潤惚燕
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
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 函耽爺恷仟答肝方象
(
	select *
	from RMP_WARNING_SCORE_MODEL_
	-- select a.*
	-- from RMP_WARNING_SCORE_MODEL_ a 
	-- join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
	-- 	on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --函耽爺恷仟答肝 忝栽圓少-蒙尢恒?弑?(喘噐?渣峠駝賁慱?袈律??恍爺議音喘?渣?)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_DETAIL_Batch as -- 函耽爺恷仟答肝方象?┻洩貶?象恂袈律?渣藤?
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- 仟療巷御窃方象 --
mid_news as 
(
	--除6倖埖曳除12倖埖_仟療_炎禰_諒儂購廣_方楚(last6Mto12M_news_label_6008001_num)
	select distinct
		'last6Mto12M_news_label_6008001_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6008001' --諒儂購廣
	union all 
	--佩匍?犇?_除6倖埖曳除12倖埖_仟療_炎禰_凪麿夏暦圓少_秤湖峠譲峙(indus_rela_last6Mto12M_news_label_6002012_meanimportance)
	select distinct
		'indus_rela_last6Mto12M_news_label_6002012_meanimportance' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --凪麿夏暦圓少
	union all 
	--佩匍?犇?_除12倖埖_仟療_炎禰_凪麿夏暦圓少_方楚(indus_rela_last12M_news_label_6002012_num)
	select distinct
		'indus_rela_last12M_news_label_6002012_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --凪麿夏暦圓少
	union all
	--除1巓_仟療_方楚(last1W_news_count)
	select distinct
		'last1W_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-7))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--佩匍?犇?_除2巓_仟療_方楚(indus_rela_last2W_news_count)
	select distinct
		'indus_rela_last2W_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-14))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--佩匍?犇?_除1倖埖_仟療_炎禰_夏暦雛鱒_媼曳(indus_rela_last1M_news_label_6002001_rate)
	select distinct
		'indus_rela_last1M_news_label_6002001_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --夏暦雛鱒
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--佩匍?犇?_除3倖埖_仟療_炎禰_夏暦雛鱒_媼曳(indus_rela_last3M_news_label_6002001_rate)
	select distinct
		'indus_rela_last3M_news_label_6002001_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --夏暦雛鱒
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--除6倖埖_仟療_炎禰_夏暦雛鱒_方楚(last6M_news_label_6002001_num)
	select distinct
		'last6M_news_label_6002001_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --夏暦雛鱒
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--佩匍?犇?_除3倖埖_仟療_炎禰_購選二匍竃?嵶別?_方楚(indus_rela_last3M_news_label_6003007_num)
	select distinct
		'indus_rela_last3M_news_label_6003007_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003007' --購選二匍竃?嵶別?
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--除12倖埖_仟療_炎禰_送強來欠??_媼曳(last12M_news_label_6002002_rate)
	select distinct
		'last12M_news_label_6002002_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002002' --送強來欠??
	union all 
	--除12倖埖_仟療_炎禰_得雫和距_方楚(last12M_news_label_6001002_num)
	select distinct
		'last12M_news_label_6001002_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6001002' --得雫和距
	union all 
	--除12倖埖_仟療_方楚(last12M_news_count)
	select distinct
		'last12M_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	union all 
	--除12倖埖_仟療_炎禰_凪麿砿尖圓少_方楚(last12M_news_label_6004024_num)
	select distinct
		'last12M_news_label_6004024_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6004024' --凪麿砿尖圓少
	union all 
	--除12倖埖_仟療_炎禰_毅隠狛謹_媼曳(last12M_news_label_6007002_rate)
	select distinct
		'last12M_news_label_6007002_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6007002' --毅隠狛謹
	union all 
	--除12倖埖_仟療_炎禰_凪麿将唔圓少_方楚(last12M_news_label_6003064_num)
	select distinct
		'last12M_news_label_6003064_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003064' --凪麿将唔圓少
	union all 
	--除6倖埖曳除12倖埖_仟療_炎禰_凪麿将唔圓少_方楚(last6Mto12M_news_label_6003064_num)
	select distinct
		'last6Mto12M_news_label_6003064_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		case_type_ii as msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003064' --凪麿将唔圓少
),
-- 穫佚窃方象 --
mid_cx_ as 
(
	--除3倖埖曳除12倖埖_穫佚_侃沓糞仏彜蓑_糞縞侃沓_方楚(last3Mto12M_honesty_penaltystatus_2_num)
	select distinct
		'last3Mto12M_honesty_penaltystatus_2_num' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and it0026_013='2'
	union all 
	--除6倖埖曳除12倖埖_穫佚_屈雫蛍窃_追秘瓜峇佩繁_媼曳(last6Mto12M_honesty_secclass_22000078_rate)
	select distinct
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='追秘瓜峇佩繁'
	union all 
	--除6倖埖_穫佚_方楚(last6M_honesty_num)
	select distinct
		'last6M_honesty_num' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	--   and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='追秘瓜峇佩繁'
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
mid_cx as --肇茅穫佚方象戦中嶷鹸來議msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_cx_
	) A where rm=1
),
mid_sf_cpws_ as  --加登猟慕/隈垪盆墨/cr0055
(
	--除6倖埖曳除12倖埖_隈垪盆墨_宛喇苧聾_択沢栽揖樟計_媼曳(last6Mto12M_lawsuit_detailedreason_4_rate)
	select distinct
		'last6Mto12M_lawsuit_detailedreason_4_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='択沢栽揖樟計'
	union all 
	--除3倖埖曳除12倖埖_隈垪盆墨_宛喇苧聾_栽揖樟計_媼曳(last3Mto12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last3Mto12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='栽揖樟計'
	union all 
	--除6倖埖曳除12倖埖_隈垪盆墨_宛喇苧聾_栽揖樟計_方楚(last6Mto12M_lawsuit_detailedreason_7_num)
	select distinct
		'last6Mto12M_lawsuit_detailedreason_7_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='栽揖樟計'
	union all 
	--除12倖埖_隈垪盆墨_宛周窃侏_峇佩窃宛周_媼曳(last12M_lawsuit_casetype_3_rate)
	select distinct
		'last12M_lawsuit_casetype_3_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_003='峇佩窃宛周'
	union all 
	--除12倖埖_隈垪盆墨_輝並繁窃侏_瓜峇佩繁_媼曳(last12M_lawsuit_partyrole_8_rate)
	select distinct
		'last12M_lawsuit_partyrole_8_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='8'  --瓜峇佩繁
	union all 
	--除12倖埖_隈垪盆墨_宛喇苧聾_署蛮処錘栽揖樟計_媼曳(last12M_lawsuit_detailedreason_0_rate)
	select distinct
		'last12M_lawsuit_detailedreason_0_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='署蛮処錘栽揖樟計'
	union all 
	--除12倖埖_隈垪盆墨_宛喇苧聾_栽揖樟計_媼曳(last12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='栽揖樟計' 
	union all 
	--除12倖埖_隈垪盆墨_膚宛署駆_峠譲峙(last12M_lawsuit_lawsuitamt_mean)
	select distinct
		'last12M_lawsuit_lawsuitamt_mean' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	union all
	--除12倖埖_隈垪盆墨_輝並繁窃侏_瓜賦萩繁_媼曳(last12M_lawsuit_partyrole_4_rate)
	select distinct
		'last12M_lawsuit_partyrole_4_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='4'  --瓜賦萩繁
),
mid_sf_cpws as --肇茅望隈_加登猟慕方象戦中嶷鹸來議msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_sf_cpws_
	) A where rm=1
),
mid_sf_ktts_ as 
(
	--除6倖埖曳除12倖埖_蝕優優蕪_盆墨仇了旗鷹_貧盆繁_媼曳(last6Mto12M_courttrial_trialstatus_5_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --貧盆繁
	union all 
	--除6倖埖曳除12倖埖_蝕優優蕪_盆墨仇了旗鷹_輝並繁_媼曳(last6Mto12M_courttrial_trialstatus_10_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --輝並繁
	union all
	--除1倖埖_蝕優優蕪_盆墨仇了旗鷹_圻蕪瓜御_媼曳(last1M_courttrial_trialstatus_2_rate)
	select distinct
		'last1M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --圻蕪瓜御
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--除3倖埖_蝕優優蕪_盆墨仇了旗鷹_圻蕪瓜御_媼曳(last3M_courttrial_trialstatus_2_rate)
	select distinct
		'last3M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --圻蕪瓜御
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--除3倖埖_蝕優優蕪_盆墨仇了旗鷹_圻蕪瓜御_方楚(last3M_courttrial_trialstatus_2_num)
	select distinct
		'last3M_courttrial_trialstatus_2_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --圻蕪瓜御
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--除6倖埖_蝕優優蕪_盆墨仇了旗鷹_輝並繁_媼曳(last6M_courttrial_trialstatus_10_rate)
	select distinct
		'last6M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --圻蕪瓜御
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--除12倖埖_蝕優優蕪_盆墨仇了旗鷹_圻蕪瓜御_媼曳(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --圻蕪瓜御
	union all
	--除12倖埖_蝕優優蕪_盆墨仇了旗鷹_圻蕪瓜御_媼曳(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --圻蕪瓜御
	union all 
	--除12倖埖_蝕優優蕪_盆墨仇了旗鷹_貧盆繁_媼曳(last12M_courttrial_trialstatus_5_rate)
	select distinct
		'last12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --貧盆繁
	union all 
	--除12倖埖_蝕優優蕪_盆墨仇了旗鷹_輝並繁_媼曳(last12M_courttrial_trialstatus_10_rate)
	select distinct
		'last12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --輝並繁
	union all
	--除6倖埖_隈垪盆墨_方楚(last6M_lawsuit_num)
	select distinct
		'last6M_lawsuit_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where 1=1
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
mid_sf_ktts as --肇茅望隈_蝕優優蕪方象戦中嶷鹸來議msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_sf_ktts_
	) A where rm=1
),
mid_risk_info as   --栽旺仟療、穫佚、望隈方象
(
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_news
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_cx
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_sf_cpws
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_sf_ktts
),
-- 及屈粁方象 --
Second_Part_Data_Prepare as 
(
	select 
		T.*,rinfo.msg_title
	from 
	(
		select 
			main.batch_dt,
			main.corp_id,
			main.corp_nm,
			main.score_dt,
			nvl(a.synth_warnlevel,'0') as synth_warnlevel, --忝栽圓少吉雫
			main.dimension,    --略業園鷹
			case main.dimension 
				when 1 then '夏暦'
				when 2 then '将唔'
				when 3 then '偏魁'
				when 4 then '嘸秤'
				when 5 then '呟械欠?媼豌?'
			end as dimension_ch,
			sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,main.dimension) as dim_contrib_ratio,
			sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,main.dimension,main.factor_evaluate) as dim_factorEvalu_contrib_ratio,
			count(idx_name) over(partition by main.corp_id,main.batch_dt,main.score_dt,main.dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by main.corp_id,main.batch_dt,main.score_dt,main.dimension,main.factor_evaluate)  as dim_factorEvalu_factor_cnt,
			-- f_cfg.dimension_ch as dimension_ch,  --略業兆各
			main.type,  	-- used
			main.factor_evaluate,  --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
			main.ori_idx_name,
			main.idx_name,  -- used 
			main.idx_value,  -- used
			main.last_idx_value, -- used in 酒烏wy
			main.idx_unit,  -- used 
			main.idx_score,  -- used
			-- rinfo.msg_title,    --欠?孃渡■?匯倖峺炎斤哘謹倖欠?嬖村???
			main.idx_name as feature_name_target,
			-- f_cfg.feature_name_target,  --蒙尢兆各-朕炎(狼由)  used
			main.contribution_ratio,
			main.dim_warn_level,
			cfg.risk_lv_desc as dim_warn_level_desc  --略業欠?婬伴?(佃泣)  used
		from RMP_WARNING_SCORE_DETAIL_Batch main
		-- left join warn_feat_CFG f_cfg 	
		-- 	on main.ori_idx_name=f_cfg.feature_cd and main.dimension=f_cfg.dimension
		left join RMP_WARNING_SCORE_MODEL_Batch a
			on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
		join warn_dim_risk_level_cfg_ cfg 
			on main.dim_warn_level=cast(cfg.risk_lv as string) and main.dimension=cfg.dimension
	)T left join mid_risk_info rinfo 
		on T.corp_id=rinfo.corp_id and T.ori_idx_name=rinfo.feature_cd
	where T.score_dt>=rinfo.notice_date 
	-- 	on main.corp_id=rinfo.corp_id and main.score_dt>=rinfo.notice_date and main.ori_idx_name=rinfo.feature_cd
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
			dim_contrib_ratio,
			dim_factorEvalu_contrib_ratio,
			-- sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
			-- sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate) as dim_factorEvalu_contrib_ratio,
			contribution_ratio,
			dim_warn_level,
			dim_warn_level_desc,  --略業欠?婬伴?(佃泣)
			type,
			factor_evaluate,  --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
			idx_name,  -- 呟械咀徨/呟械峺炎
			feature_name_target,
			idx_value,
			last_idx_value,
			idx_unit,
			idx_score,   --峺炎得蛍 used
			msg_title,    --欠?孃渡■?匯倖峺炎斤哘謹倖欠?嬖村???
			case idx_unit
				when '%' then 
					concat(feature_name_target,'葎',cast(cast(round(idx_value,2) as decimal(10,2))as string),idx_unit)
				else 
					concat(feature_name_target,'葎',cast(idx_value as string),idx_unit)
			end as idx_desc,	
			dim_factor_cnt,			
			dim_factorEvalu_factor_cnt
		from (select distinct * from Second_Part_Data_Prepare ) t
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
Second_Part_Data_Dimension as -- 梓略業蚊祉悳宙峰喘方象
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		dim_factorEvalu_contrib_ratio,
		dim_warn_level_desc,
		dim_factor_cnt,
		dim_factorEvalu_factor_cnt
	from Second_Part_Data
	where factor_evaluate = 0
),
Second_Part_Data_Dimension_Type_idx as --梓峺炎蚊祉悳方象??喘噐祉悳謹倖 欠?嬖村? 欺匯倖峺炎貧
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		idx_desc,
		contribution_ratio,  --峺炎蚊議恒?弑晩識?
		concat_ws('、',collect_set(msg_title)) as risk_info_desc_in_one_idx   -- hive 
		-- group_concat(distinct msg_title,'、') as risk_info_desc_in_one_idx    -- impala 
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			idx_desc,
			msg_title,
			contribution_ratio,
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type,idx_desc order by contribution_ratio desc) as rm
		from Second_Part_Data
		where factor_evaluate = 0
	)A where rm<=10  --梓孚恒?弑氾澱?貫互欺詰電會朔??函竃念噴訳欠?嬖村?恬葎婢幣
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type,idx_desc,contribution_ratio
),
Second_Part_Data_Dimension_Type as -- 梓略業蚊 參式 窃艶蚊祉悳宙峰喘方象
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		concat_ws('、',collect_set(risk_info_desc_in_one_idx)) as risk_info_desc_in_one_type,   -- hive 
		-- nvl(group_concat( risk_info_desc_in_one_idx,'、'),'') as  risk_info_desc_in_one_type,   --impala  壅繍欠?嬖村?祉悳欺type蚊
		concat_ws('、',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive (憧俊峙葎NULL??卦指'')
		-- nvl(group_concat(distinct idx_desc,'、'),'') as idx_desc_in_one_type    -- impala  (憧俊峙畠何葎NULL??卦指NULL)
	from 
	(
		select
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			idx_desc,
			risk_info_desc_in_one_idx,    --祉悳欺匯倖峺炎貧議欠?孃渡?
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from Second_Part_Data_Dimension_Type_idx
		-- where factor_evaluate = 0
		-- group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type
	) A where rm<=5   --函恒?弑氾澱?恷互議5倖呟械咀徨
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type

),
-- 及屈粁佚連 --
Second_Msg_Dimension as  -- 略業蚊議佚連宙峰
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		row_number() over(partition by batch_dt,corp_id,score_dt order by dim_contrib_ratio desc) as dim_contrib_ratio_rank,   --貫寄欺弌電双
		dim_factorEvalu_factor_cnt,
		concat(
			dimension_ch,'略業','??','恒?弑晩識?',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','??','??',
			'乎略業輝念侃噐',dim_warn_level_desc,'吉雫','??',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('涙?墻?呟械峺炎式並周','。')
				else 
					concat(
						dimension_ch,'略業','追秘議',cast(dim_factor_cnt as string),'倖峺炎嶄','??',cast(dim_factorEvalu_factor_cnt as string),'倖峺炎燕?嶢豎?','??',
						'呟械峺炎斤麼悶悳悶欠?婢穎弑販?',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','??'
					)
			end
		) as dim_msg_no_color,
		concat(
			'<span class="WEIGHT">',dimension_ch,'略業','??','恒?弑晩識?',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','??','</span>','??',
		
			'乎略業輝念侃噐',
				case 
					when dim_warn_level_desc ='互欠??' then 
						concat('<span class="RED"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='嶄欠??' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='詰欠??' then 
						concat('<span class="GREEN"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
				end,
				'吉雫','??',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('涙?墻?呟械峺炎式並周','。')
				else 
					concat(
						dimension_ch,'略業','追秘議',cast(dim_factor_cnt as string),'倖峺炎嶄','??',cast(dim_factorEvalu_factor_cnt as string),'倖峺炎燕?嶢豎?','??',
						'呟械峺炎斤麼悶悳悶欠?婢穎弑販?',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','??'
					)
			end
		) as dim_msg
	from Second_Part_Data_Dimension
),
Second_Msg_Dimension_Type as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		concat(concat_ws('??',collect_set(dim_type_msg)),'。') as idx_desc_risk_info_desc_in_one_dimension   -- hive 
		-- concat(group_concat(distinct dim_type_msg,'??'),'。') as idx_desc_risk_info_desc_in_one_dimension  --impala
	from
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			concat(
				type,'呟械??',   --箭泌??'仟療巷御窃呟械??'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"膚式欠?嬖村?麼勣淫凄??",risk_info_desc_in_one_type,'??',
							'麼勣呟械峺炎淫凄??',idx_desc_in_one_type
						)
				end				
			) as dim_type_msg_no_color,
			concat(
				'<span class="WEIGHT">',type,'呟械??','</span>',   --箭泌??'仟療巷御窃呟械??'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"膚式欠?嬖村?麼勣淫凄??",risk_info_desc_in_one_type,'??',
							'麼勣呟械峺炎淫凄??',idx_desc_in_one_type
						)
				end				
			) as dim_type_msg
		from Second_Part_Data_Dimension_Type
	)A 
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch
),
Second_Msg_Dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dim_rank,
		case 	
			when a.dim_factorEvalu_factor_cnt=0 then  --涙呟械峺炎扮??三宝岷俊補竃略業蚊軸辛??潤崩囂葎'涙?墻?呟械峺炎式並周'
					a.dim_msg
				else
					concat(
						case 
							when dim_rank='001' then 
								concat('<',dim_rank,'>','麼勣葎',dim_msg,'麼勣淫凄',idx_desc_risk_info_desc_in_one_dimension )
							when dim_rank='002' then 
								concat('<',dim_rank,'>','凪肝葎',dim_msg,'麼勣淫凄',idx_desc_risk_info_desc_in_one_dimension )
							when dim_rank='003' then 
								concat('<',dim_rank,'>','及眉葎',dim_msg,'麼勣淫凄',idx_desc_risk_info_desc_in_one_dimension )
							when dim_rank='004' then 
								concat('<',dim_rank,'>','及膨葎',dim_msg,'麼勣淫凄',idx_desc_risk_info_desc_in_one_dimension )	
							when dim_rank='005' then  
								concat('<',dim_rank,'>','恷朔葎',dim_msg,'麼勣淫凄',idx_desc_risk_info_desc_in_one_dimension )	
						end
					) 
		end as msg_dim
	from 
	(
		select 
			a.batch_dt,
			a.corp_id,
			a.corp_nm,
			a.score_dt,
			a.dimension,
			a.dim_factorEvalu_factor_cnt,
			b.idx_desc_risk_info_desc_in_one_dimension,
			a.dim_msg,
			lpad(cast(a.dim_contrib_ratio_rank as string),3,'0') as dim_rank  --略業?塋硝覚鯏澱???001 002 003 004 005
			-- case 	
			-- 	when a.dim_factorEvalu_factor_cnt=0 then  --涙呟械峺炎扮??三宝岷俊補竃略業蚊軸辛??潤崩囂葎'涙?墻?呟械峺炎式並周'
			-- 		a.dim_msg
			-- 	else
			-- 		concat(
			-- 			lpad(cast(a.dim_contrib_ratio_rank as string),3,'0'),'_',a.dim_msg,'麼勣淫凄',b.idx_desc_risk_info_desc_in_one_dimension
			-- 		) 
			-- end as msg_dim
		from Second_Msg_Dimension a
		join Second_Msg_Dimension_Type b 
			on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.dimension=b.dimension
		order by batch_dt,corp_id,score_dt,dim_rank
	)A 
),
Second_Msg as    --?。。〇肯感? 恒?弑晩識? 貫寄欺弌電會
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('\\r\\n',sort_array(collect_set(msg_dim))) as msg
		-- group_concat(distinct msg_dim,'\\r\\n') as msg
	from Second_Msg_Dim
	group by batch_dt,corp_id,corp_nm,score_dt
),
Fifth_Data as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',collect_set(dimension_ch)) as abnormal_dim_msg  -- hive
		-- group_concat(dimension_ch,'、') as abnormal_dim_msg -- impala
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension_ch as dimension_ch_no_color,
			concat('<span class="RED"><span class="WEIGHT">',dimension_ch,'</span></span>') as dimension_ch
		from (select distinct * from Second_Part_Data_Prepare) t
		where factor_evaluate = 0   --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
	)A 
	group by batch_dt,corp_id,corp_nm,score_dt
),
Fifth_Msg as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat('秀咏購廣巷望',abnormal_dim_msg,'略業','揮栖議欠?奸?') as msg
	from Fifth_Data
)
------------------------------------參貧何蛍葎匝扮燕-------------------------------------------------------------------
insert overwrite table pth_rmp.rmp_warning_score_report2
select distinct
	a.batch_dt,
	a.corp_id,
	a.corp_nm,
	a.score_dt,
	a.msg as msg2,
	b.msg as msg5
from Second_Msg a 
join Fifth_Msg b 
	on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.score_dt=b.score_dt
;

