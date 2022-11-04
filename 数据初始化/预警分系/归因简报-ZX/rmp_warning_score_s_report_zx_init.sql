--（1）DDL rmp_warning_score_s_report_zx_init hive执行--
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



-- （2）rmp_warning_score_s_report_zx_init_impala impala执行 --
create table pth_rmp.rmp_warning_score_s_report_zx_init_impala as 
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
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 非时间限制部分 --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_ as  --预警分--归因详情 原始接口
(
	-- 时间限制部分 --
	select * ,score_dt as batch_dt
	from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- 非时间限制部分 --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- 新闻公告 --
news_intf_ as 
(
	-- 时间限制部分 --
    select *
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- 近12个月的新闻数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select * 
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 诚信 --
cx_intf_ as 
(
	-- 时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的新闻数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 司法 --
sf_ktts_inft_ as --开庭庭审
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的开庭庭审数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --裁判文书
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- 近12个月的开庭庭审数据 --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- 非时间限制部分 --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
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
-- 模型外挂规则 --
warn_adj_rule_cfg as --预警分-模型外挂规则配置表   取最新etl_date的数据 (更新频率:日度更新)
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
	where a.operator = '自动-风险已暴露规则'
	  and a.ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
-- 预警分 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
RMP_WARNING_SCORE_MODEL_ as  --预警分-模型结果表
(
    select distinct
		cast(to_date(a.rating_dt) as string) as batch_dt,  --初始化脚本特殊处理，对其其他初始化数据
        -- cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 预警分
		a.interval_text_adjusted,
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
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select distinct a.*
	from RMP_WARNING_SCORE_MODEL_ a 
),
-- 归因详情 --
RMP_WARNING_SCORE_DETAIL_Batch as -- 取每天最新批次数据（当天数据做范围限制）
(
	select distinct a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	-- where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
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
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
s_report_Data_Prepare_ as 
(
	select DISTINCT
		T.*,
		rinfo.msg_title,   --一个指标对应多条风险事件
		nvl(ru.category,'') as category_nvl,   --一个企业一天一条外挂规则
		nvl(ru.reason,'') as reason_nvl
	from 
	(
		select 
			main.batch_dt,
			main.corp_id,
			main.corp_nm,
			main.score_dt,
			a.interval_text_adjusted,
			-- nvl(a.synth_warnlevel,'0') as synth_warnlevel, --综合预警等级
			main.dimension,    --维度编码
			main.dim_contrib_ratio,
			-- sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,f_cfg.dimension) as dim_contrib_ratio,
			nvl(f_cfg.dimension,'') as dimension_ch,  --维度名称
			main.type,  	-- used
			main.idx_name,  -- used 
			main.idx_value,  -- used
			main.last_idx_value, -- used in 简报wy
			main.idx_unit,  -- used 
			main.idx_score,  -- used
			nvl(f_cfg.feature_name_target,'') as feature_name_target,  --特征名称-目标(系统)  used
			main.contribution_ratio,
			main.factor_evaluate  --因子评价，因子是否异常的字段 0：异常 1：正常
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
		interval_text_adjusted,  --原始模型产出的预警等级
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
			'最新信用违约预警处于',
				case interval_text_adjusted 
					when '绿色预警' then 
						concat('<span class="GREEN"><span class="WEIGHT">',interval_text_adjusted,'等级','</span></span>')
					when '黄色预警' then 
						concat('<span class="YELLO"><span class="WEIGHT">',interval_text_adjusted,'等级','</span></span>')
					when '橙色预警' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',interval_text_adjusted,'等级','</span></span>')
					when '红色预警' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'等级','</span></span>')
					when '风险已暴露' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'等级','</span></span>')
				end,'，',
			if(reason_nvl<>'',concat('主要由于触发',reason_nvl,'同时'),''),
			'风险涉及','<span class="WEIGHT">',dimension_ch,'维度','（','贡献度占比',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','）','</span>','，',
			case 
				when  abnormal_idx_desc<>'' then 
					concat('异常指标包括：',abnormal_idx_desc)
				else 
					''
			end,
			case 
				when  abnormal_risk_info_desc<>'' and abnormal_risk_info_desc is not null then 
					concat('，','异常事件包括：',abnormal_risk_info_desc)
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
		-- concat_ws('；',collect_Set(msg_in_one_dim)) as s_msg  -- hive
		group_concat(distinct msg_in_one_dim,'；') as s_msg  -- impala
	from s_report_msg
	group by batch_dt,corp_id,corp_nm,score_dt
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
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



-- （3）sql执行  warning_score_s_report_zx_init hive执行--
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