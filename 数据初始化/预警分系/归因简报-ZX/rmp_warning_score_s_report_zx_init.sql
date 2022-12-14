--??1??DDL rmp_warning_score_s_report_zx_init hive峇佩--
drop table if exists pth_rmp.rmp_warning_score_s_report_zx_init ;
create table pth_rmp.rmp_warning_score_s_report_zx_init
(
	sid_kw string,
	corp_id string,
	corp_nm string,
	score_dt timestamp,
	report_msg string,
	model_version string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int) 
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



-- ??2??rmp_warning_score_s_report_zx_init_impala impala峇佩 --
create table pth_rmp.rmp_warning_score_s_report_zx_init_impala as 
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
	where a.delete_flag=0 and b.delete_flag=0
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 俊笥蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 扮寂?渣匿?購 --
timeLimit_switch as 
(
    select True as flag   --TRUE:扮寂埃崩??FLASE:扮寂音恂埃崩??宥械喘噐兜兵晒
    -- select False as flag
),
-- 圓少蛍 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --圓少蛍_蛮栽距屁朔忝栽  圻兵俊笥
(
    -- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 拷咀?蠻? --
RMP_WARNING_SCORE_DETAIL_ as  --圓少蛍--拷咀?蠻? 圻兵俊笥
(
	-- 扮寂?渣堂新? --
	select * ,score_dt as batch_dt
	from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 掲扮寂?渣堂新? --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 仟療巷御 --
news_intf_ as 
(
	-- 扮寂?渣堂新? --
    select *
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- 除12倖埖議仟療方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select * 
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 穫佚 --
cx_intf_ as 
(
	-- 扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議仟療方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 望隈 --
sf_ktts_inft_ as --蝕優優蕪
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議蝕優優蕪方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --加登猟慕
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 除12倖埖議蝕優優蕪方象 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 掲扮寂?渣堂新? --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 塘崔燕 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
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
-- 庁侏翌航号夸 --
warn_adj_rule_cfg as --圓少蛍-庁侏翌航号夸塘崔燕   函恷仟etl_date議方象 (厚仟撞楕:晩業厚仟)
(
	select distinct
		a.etl_date,
		b.corp_id, 
		b.corp_name as corp_nm,
		a.category,
		a.reason
	from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
	join corp_chg b 
		on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
	where a.operator = '徭強-欠?孀儕?其号夸'
	  and a.ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 圓少蛍 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
RMP_WARNING_SCORE_MODEL_ as  --圓少蛍-庁侏潤惚燕
(
    select distinct
		cast(to_date(a.rating_dt) as string) as batch_dt,  --兜兵晒重云蒙歩侃尖??斤凪凪麿兜兵晒方象
        -- cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 圓少蛍
		a.interval_text_adjusted,
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
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 函耽爺恷仟答肝方象
(
	select distinct a.*
	from RMP_WARNING_SCORE_MODEL_ a 
),
-- 拷咀?蠻? --
RMP_WARNING_SCORE_DETAIL_Batch as -- 函耽爺恷仟答肝方象?┻洩貶?象恂袈律?渣藤?
(
	select distinct a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	-- where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		msg_title
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='追秘瓜峇佩繁'
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
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
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
	--除12倖埖_隈垪盆墨_宛喇苧聾_栽揖樟計_媼曳(last12M_lawsuit_lawsuitamt_mean)
	select distinct
		'除12倖埖_隈垪盆墨_膚宛署駆_峠譲峙' as feature_cd,
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
s_report_Data_Prepare_ as 
(
	select DISTINCT
		T.*,
		rinfo.msg_title,   --匯倖峺炎斤哘謹訳欠?嬖村?
		nvl(ru.category,'') as category_nvl,   --匯倖二匍匯爺匯訳翌航号夸
		nvl(ru.reason,'') as reason_nvl
	from 
	(
		select 
			main.batch_dt,
			main.corp_id,
			main.corp_nm,
			main.score_dt,
			a.interval_text_adjusted,
			-- nvl(a.synth_warnlevel,'0') as synth_warnlevel, --忝栽圓少吉雫
			main.dimension,    --略業園鷹
			main.dim_contrib_ratio,
			-- sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,f_cfg.dimension) as dim_contrib_ratio,
			nvl(f_cfg.dimension,'') as dimension_ch,  --略業兆各
			main.type,  	-- used
			main.idx_name,  -- used 
			main.idx_value,  -- used
			main.last_idx_value, -- used in 酒烏wy
			main.idx_unit,  -- used 
			main.idx_score,  -- used
			nvl(f_cfg.feature_name_target,'') as feature_name_target,  --蒙尢兆各-朕炎(狼由)  used
			main.contribution_ratio,
			main.factor_evaluate  --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
		from (select *,sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio from RMP_WARNING_SCORE_DETAIL_Batch) main
		join feat_CFG f_cfg 	
			on main.idx_name=f_cfg.feature_cd
		join RMP_WARNING_SCORE_MODEL_Batch a
			on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	)T 
	left join warn_adj_rule_cfg  ru
		on T.corp_id = ru.corp_id
	left join mid_risk_info rinfo 
		on T.corp_id=rinfo.corp_id and T.score_dt>=rinfo.notice_date and T.idx_name=rinfo.feature_cd
),
s_report_Data_Prepare as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		category_nvl,
		reason_nvl,
		interval_text_adjusted,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		type,
		idx_name,
		idx_value,
		last_idx_value,
		idx_unit,
		idx_score,
		feature_name_target,
		contribution_ratio,
		factor_evaluate,
		-- concat_ws('、',collect_Set(msg_title)) as risk_info_dsec_in_one_idx  -- hive 
		group_concat(distinct msg_title,'、') as risk_info_desc_in_one_idx  -- impala
	from s_report_Data_Prepare_ 
	group by batch_dt,corp_id,corp_nm,score_dt,category_nvl,reason_nvl,interval_text_adjusted,dimension,
			 dimension_ch,dim_contrib_ratio,type,idx_name,idx_value,last_idx_value,idx_unit,idx_score,
			 feature_name_target,contribution_ratio,factor_evaluate
),
s_report_Data_dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		max(category_nvl) as category_nvl,
		max(reason_nvl) as reason_nvl,
		interval_text_adjusted,  --圻兵庁侏恢竃議圓少吉雫
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		-- concat_ws('、',collect_set(feature_name_target))  as abnormal_idx_desc, -- hive
		group_concat(feature_name_target,'、')  as abnormal_idx_desc,  -- impala 
		-- concat_ws('、',collect_set(risk_info_desc_in_one_idx))  as abnormal_risk_info_desc -- hive
		group_concat(distinct risk_info_desc_in_one_idx,'、')  as abnormal_risk_info_desc
	from s_report_Data_Prepare
	where factor_evaluate = 0
	group by batch_dt,corp_id,corp_nm,score_dt,interval_text_adjusted,dimension,dimension_ch,dim_contrib_ratio
	order by dim_contrib_ratio desc
),
s_report_msg as 
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension_ch,
		concat(		
			'恷仟佚喘離埃圓少侃噐',
				case interval_text_adjusted 
					when '駄弼圓少' then 
						concat('<span class="GREEN"><span class="WEIGHT">',interval_text_adjusted,'吉雫','</span></span>')
					when '仔弼圓少' then 
						concat('<span class="YELLO"><span class="WEIGHT">',interval_text_adjusted,'吉雫','</span></span>')
					when '拡弼圓少' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',interval_text_adjusted,'吉雫','</span></span>')
					when '碕弼圓少' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'吉雫','</span></span>')
					when '欠?孀儕?其' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'吉雫','</span></span>')
				end,'??',
			if(reason_nvl<>'',concat('麼勣喇噐乾窟',reason_nvl,'揖扮'),''),
			'欠?嬋羲?','<span class="WEIGHT">',dimension_ch,'略業','??','恒?弑晩識?',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','??','</span>','??',
			case 
				when  abnormal_idx_desc<>'' then 
					concat('呟械峺炎淫凄??',abnormal_idx_desc)
				else 
					''
			end,
			case 
				when  abnormal_risk_info_desc<>'' and abnormal_risk_info_desc is not null then 
					concat('??','呟械並周淫凄??',abnormal_risk_info_desc)
				else 
					''
			end
		) as msg_in_one_dim
	from s_report_Data_dim
),
s_report_msg_corp as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('??',collect_Set(msg_in_one_dim)) as s_msg  -- hive
		group_concat(distinct msg_in_one_dim,'??') as s_msg  -- impala
	from s_report_msg
	group by batch_dt,corp_id,corp_nm,score_dt
)
------------------------------------參貧何蛍葎匝扮燕-------------------------------------------------------------------
select 
	-- concat(corp_id,md5(concat(batch_dt,corp_id))) as sid_kw,  -- hive
	-- '' as sid_kw,  -- impala
	-- batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	s_msg as report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_report_msg_corp
;



-- ??3??sql峇佩  warning_score_s_report_zx_init hive峇佩--
insert into pth_rmp.rmp_warning_score_s_report_zx_init partition(etl_date=19900101)
select 
	concat(corp_id,md5(concat(cast(score_dt as string),corp_id))) as sid_kw ,
	corp_id ,
	corp_nm ,
	score_dt ,
	report_msg ,
	model_version ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version	
from pth_rmp.rmp_warning_score_s_report_zx_init_impala
;