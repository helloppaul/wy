-- RMP_WARNING_SCORE_REPORT 第二段和第五段-当前等级归因和建议关注风险 --
-- drop table if exists app_ehzh.rmp_warning_score_report2;    
-- create table app_ehzh.rmp_warning_score_report2 as      --@pth_rmp.rmp_warning_score_report2
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
      and to_date(rating_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 特征贡献度 --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --特征贡献度_综合预警等级
(
	-- 时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 新闻公告 --
news_intf_ as 
(
	-- 时间限制部分 --
    select *
    from app_ehzh.rmp_opinion_risk_info --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- 近12个月的新闻数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select * 
    from app_ehzh.rmp_opinion_risk_info --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 诚信 --
cx_intf_ as 
(
	-- 时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的新闻数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 司法 --
sf_ktts_inft_ as --开庭庭审
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的开庭庭审数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --裁判文书
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的开庭庭审数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
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
-- 预警分 --
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
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select *
	from RMP_WARNING_SCORE_MODEL_
	-- select a.*
	-- from RMP_WARNING_SCORE_MODEL_ a 
	-- join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
	-- 	on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --取每天最新批次 综合预警-贡献度排行榜(用于限制今天特征范围，昨天的不用限制)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据（当天数据做范围限制）
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- 新闻公告类数据 --
mid_news as 
(
	--近6个月比近12个月_新闻_标签_问询关注_数量(last6Mto12M_news_label_6008001_num)
	select distinct
		'last6Mto12M_news_label_6008001_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6008001' --问询关注
	union all 
	--行业相对_近6个月比近12个月_新闻_标签_其他财务预警_情感平均值(indus_rela_last6Mto12M_news_label_6002012_meanimportance)
	select distinct
		'indus_rela_last6Mto12M_news_label_6002012_meanimportance' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --其他财务预警
	union all 
	--行业相对_近12个月_新闻_标签_其他财务预警_数量(indus_rela_last12M_news_label_6002012_num)
	select distinct
		'indus_rela_last12M_news_label_6002012_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --其他财务预警
	union all
	--近1周_新闻_数量(last1W_news_count)
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
	--行业相对_近2周_新闻_数量(indus_rela_last2W_news_count)
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
	--行业相对_近1个月_新闻_标签_财务亏损_占比(indus_rela_last1M_news_label_6002001_rate)
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
	  and case_type_ii_cd='6002001' --财务亏损
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--行业相对_近3个月_新闻_标签_财务亏损_占比(indus_rela_last3M_news_label_6002001_rate)
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
	  and case_type_ii_cd='6002001' --财务亏损
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--近6个月_新闻_标签_财务亏损_数量(last6M_news_label_6002001_num)
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
	  and case_type_ii_cd='6002001' --财务亏损
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--行业相对_近3个月_新闻_标签_关联企业出现问题_数量(indus_rela_last3M_news_label_6003007_num)
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
	  and case_type_ii_cd='6003007' --关联企业出现问题
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--近12个月_新闻_标签_流动性风险_占比(last12M_news_label_6002002_rate)
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
	  and case_type_ii_cd='6002002' --流动性风险
	union all 
	--近12个月_新闻_标签_评级下调_数量(last12M_news_label_6001002_num)
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
	  and case_type_ii_cd='6001002' --评级下调
	union all 
	--近12个月_新闻_数量(last12M_news_count)
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
	--近12个月_新闻_标签_其他管理预警_数量(last12M_news_label_6004024_num)
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
	  and case_type_ii_cd='6004024' --其他管理预警
	union all 
	--近12个月_新闻_标签_担保过多_占比(last12M_news_label_6007002_rate)
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
	  and case_type_ii_cd='6007002' --担保过多
	union all 
	--近12个月_新闻_标签_其他经营预警_数量(last12M_news_label_6003064_num)
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
	  and case_type_ii_cd='6003064' --其他经营预警
	union all 
	--近6个月比近12个月_新闻_标签_其他经营预警_数量(last6Mto12M_news_label_6003064_num)
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
	  and case_type_ii_cd='6003064' --其他经营预警
),
-- 诚信类数据 --
mid_cx_ as 
(
	--近3个月比近12个月_诚信_处罚实施状态_实际处罚_数量(last3Mto12M_honesty_penaltystatus_2_num)
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
	--近6个月比近12个月_诚信_二级分类_纳入被执行人_占比(last6Mto12M_honesty_secclass_22000078_rate)
	select distinct
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='纳入被执行人'
	union all 
	--近6个月_诚信_数量(last6M_honesty_num)
	select distinct
		'last6M_honesty_num' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='纳入被执行人'
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
mid_cx as --去除诚信数据里面重复性的msg_id
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
mid_sf_cpws_ as  --裁判文书/法院诉讼/cr0055
(
	--近6个月比近12个月_法院诉讼_案由明细_买卖合同纠纷_占比(last6Mto12M_lawsuit_detailedreason_4_rate)
	select distinct
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='买卖合同纠纷'
	union all 
	--近3个月比近12个月_法院诉讼_案由明细_合同纠纷_占比(last3Mto12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last3Mto12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='合同纠纷'
	union all 
	--近6个月比近12个月_法院诉讼_案由明细_合同纠纷_数量(last6Mto12M_lawsuit_detailedreason_7_num)
	select distinct
		'last6Mto12M_lawsuit_detailedreason_7_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='合同纠纷'
	union all 
	--近12个月_法院诉讼_案件类型_执行类案件_占比(last12M_lawsuit_casetype_3_rate)
	select distinct
		'last12M_lawsuit_casetype_3_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_003='执行类案件'
	union all 
	--近12个月_法院诉讼_当事人类型_被执行人_占比(last12M_lawsuit_partyrole_8_rate)
	select distinct
		'last12M_lawsuit_partyrole_8_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='8'  --被执行人
	union all 
	--近12个月_法院诉讼_案由明细_金融借款合同纠纷_占比(last12M_lawsuit_detailedreason_0_rate)
	select distinct
		'last12M_lawsuit_detailedreason_0_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='金融借款合同纠纷'
	union all 
	--近12个月_法院诉讼_案由明细_合同纠纷_占比(last12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='合同纠纷' 
	union all 
	--近12个月_法院诉讼_案由明细_合同纠纷_占比(last12M_lawsuit_lawsuitamt_mean)
	select distinct
		'近12个月_法院诉讼_涉案金额_平均值' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	union all
	--近12个月_法院诉讼_当事人类型_被申请人_占比(last12M_lawsuit_partyrole_4_rate)
	select distinct
		'last12M_lawsuit_partyrole_4_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='4'  --被申请人
),
mid_sf_cpws as --去除司法_裁判文书数据里面重复性的msg_id
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
	--近6个月比近12个月_开庭庭审_诉讼地位代码_上诉人_占比(last6Mto12M_courttrial_trialstatus_5_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --上诉人
	union all 
	--近6个月比近12个月_开庭庭审_诉讼地位代码_当事人_占比(last6Mto12M_courttrial_trialstatus_10_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --当事人
	union all
	--近1个月_开庭庭审_诉讼地位代码_原审被告_占比(last1M_courttrial_trialstatus_2_rate)
	select distinct
		'last1M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --原审被告
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--近3个月_开庭庭审_诉讼地位代码_原审被告_占比(last3M_courttrial_trialstatus_2_rate)
	select distinct
		'last3M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --原审被告
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--近3个月_开庭庭审_诉讼地位代码_原审被告_数量(last3M_courttrial_trialstatus_2_num)
	select distinct
		'last3M_courttrial_trialstatus_2_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --原审被告
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--近6个月_开庭庭审_诉讼地位代码_当事人_占比(last6M_courttrial_trialstatus_10_rate)
	select distinct
		'last6M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --原审被告
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--近12个月_开庭庭审_诉讼地位代码_原审被告_占比(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --原审被告
	union all
	--近12个月_开庭庭审_诉讼地位代码_原审被告_占比(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --原审被告
	union all 
	--近12个月_开庭庭审_诉讼地位代码_上诉人_占比(last12M_courttrial_trialstatus_5_rate)
	select distinct
		'last12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --上诉人
	union all 
	--近12个月_开庭庭审_诉讼地位代码_当事人_占比(last12M_courttrial_trialstatus_10_rate)
	select distinct
		'last12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --当事人
),
mid_sf_ktts as --去除司法_开庭庭审数据里面重复性的msg_id
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
mid_risk_info as   --合并新闻、诚信、司法数据
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
		main.factor_evaluate,  --因子评价，因子是否异常的字段 0：异常 1：正常
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.last_idx_value, -- used in 简报wy
		main.idx_unit,  -- used 
		main.idx_score,  -- used
		rinfo.msg_title,    --风险信息（一个指标对应多个风险事件）
		f_cfg.feature_name_target,  --特征名称-目标(系统)  used
		main.contribution_ratio,
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --维度风险等级(难点)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
	left join mid_risk_info rinfo 
		on main.corp_id=rinfo.corp_id and main.score_dt=rinfo.notice_date and main.idx_name=rinfo.feature_cd
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
			contribution_ratio,
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
			msg_title,    --风险信息（一个指标对应多个风险事件）
			case idx_unit
				when '%' then 
					concat(feature_name_target,'为',cast(cast(round(idx_value,2) as decimal(10,2))as string),idx_unit)
				else 
					concat(feature_name_target,'为',cast(idx_value as string),idx_unit)
			end as idx_desc,				
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
Second_Part_Data_Dimension as -- 按维度层汇总描述用数据
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
Second_Part_Data_Dimension_Type_idx as --按指标层汇总数据，用于汇总多个 风险事件 到一个指标上
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
		contribution_ratio,  --指标层的贡献度占比
		-- concat_ws('、',collect_set(msg_title)) as risk_info_desc_in_one_idx   -- hive 
		group_concat(distinct msg_title,'、') as risk_info_desc_in_one_idx    -- impala 
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
	)A where rm<=10  --按照贡献度排名从高到低排序后，取出前十条风险事件作为展示
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type,idx_desc,contribution_ratio
),
Second_Part_Data_Dimension_Type as -- 按维度层 以及 类别层汇总描述用数据
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		-- concat_ws('、',collect_set(risk_info_desc_in_one_idx)) as risk_info_desc_in_one_type,   -- hive 
		nvl(group_concat( risk_info_desc_in_one_idx,'、'),'') as  risk_info_desc_in_one_type,   --impala  再将风险事件汇总到type层
		-- concat_ws('、',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive (拼接值为NULL，返回'')
		nvl(group_concat(distinct idx_desc,'、'),'') as idx_desc_in_one_type    -- impala  (拼接值全部为NULL，返回NULL)
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
			risk_info_desc_in_one_idx,    --汇总到一个指标上的风险信息
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from Second_Part_Data_Dimension_Type_idx
		-- where factor_evaluate = 0
		-- group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type
	) A where rm<=5   --取贡献度排名最高的5个异常因子
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type

),
-- 第二段信息 --
Second_Msg_Dimension as  -- 维度层的信息描述
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		row_number() over(partition by batch_dt,corp_id,score_dt order by dim_contrib_ratio desc) as dim_contrib_ratio_rank,   --从大到小排列
		dim_factorEvalu_factor_cnt,
		concat(
			dimension_ch,'维度','（','贡献度占比',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','）','，',
			'该维度当前处于',dim_warn_level_desc,'等级','，',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('无显著异常指标及事件','。')
				else 
					concat(
						dimension_ch,'维度','纳入的',cast(dim_factor_cnt as string),'个指标中','，',cast(dim_factorEvalu_factor_cnt as string),'个指标表现异常','，',
						'异常指标对主体总体风险贡献度为',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','，'
					)
			end
		) as dim_msg_no_color,
		concat(
			'<span class="WEIGHT">',dimension_ch,'维度','（','贡献度占比',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','）','</span>','，',
		
			'该维度当前处于',
				case 
					when dim_warn_level_desc ='高风险' then 
						concat('<span class="RED"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='中风险' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='低风险' then 
						concat('<span class="GREEN"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
				end,
				'等级','，',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('无显著异常指标及事件','。')
				else 
					concat(
						dimension_ch,'维度','纳入的',cast(dim_factor_cnt as string),'个指标中','，',cast(dim_factorEvalu_factor_cnt as string),'个指标表现异常','，',
						'异常指标对主体总体风险贡献度为',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','，'
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
		-- concat(concat_ws('；',collect_set(dim_type_msg)),'。') as idx_desc_risk_info_desc_in_one_dimension   -- hive 
		concat(group_concat(distinct dim_type_msg,'；'),'。') as idx_desc_risk_info_desc_in_one_dimension  --impala
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
				type,'异常：',   --例如：'新闻公告类异常：'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"涉及风险事件主要包括：",risk_info_desc_in_one_type,'，',
							'主要异常指标包括：',idx_desc_in_one_type
						)
				end				
			) as dim_type_msg_no_color,
			concat(
				'<span class="WEIGHT">',type,'异常：','</span>',   --例如：'新闻公告类异常：'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"涉及风险事件主要包括：",risk_info_desc_in_one_type,'，',
							'主要异常指标包括：',idx_desc_in_one_type
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
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		lpad(cast(a.dim_contrib_ratio_rank as string),3,'0') as dim_rank,  --维度显示顺序排名，001 002 003 004 005
		case 	
			when a.dim_factorEvalu_factor_cnt=0 then  --无异常指标时，话术直接输出维度层即可，结束语为'无显著异常指标及事件'
				a.dim_msg
			else
				concat(
					lpad(cast(a.dim_contrib_ratio_rank as string),3,'0'),'_',a.dim_msg,'主要包括',b.idx_desc_risk_info_desc_in_one_dimension
				) 
		end as msg_dim
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.dimension=b.dimension
	order by batch_dt,corp_id,score_dt,dim_rank
),
Second_Msg as    --！！！还未对 贡献度占比 从大到小排序
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('\\r\\n',sort_array(collect_set(msg_dim))) as msg
		group_concat(distinct msg_dim,'\\r\\n') as msg
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
		-- concat_ws('、',collect_set(dimension_ch)) as abnormal_dim_msg  -- hive
		group_concat(dimension_ch,'、') as abnormal_dim_msg -- impala
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension_ch as dimension_ch_no_color,
			concat('<span class="RED"><span class="WEIGHT">',dimension_ch,'</span></span>') as dimension_ch
		from Second_Part_Data_Prepare
		where factor_evaluate = 0   --因子评价，因子是否异常的字段 0：异常 1：正常
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
		concat('建议关注公司',abnormal_dim_msg,'维度','带来的风险。') as msg
	from Fifth_Data
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
select 
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

