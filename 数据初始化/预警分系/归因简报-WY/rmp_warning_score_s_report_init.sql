--??1?? DDL RMP_WARNING_SCORE_S_REPORT_INIT hive峇佩 --
-- 圓少蛍-拷咀酒烏煽雰wy RMP_WARNING_SCORE_S_REPORT_INIT --
drop table if exists pth_rmp.RMP_WARNING_SCORE_S_REPORT_INIT ;
create table pth_rmp.RMP_WARNING_SCORE_S_REPORT_INIT
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
)
partitioned by (etl_date int) 
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;

-- ??2??rmp_warning_score_s_report_init_imapala impala峇佩 --
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 児云佚連 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
create table pth_rmp.rmp_warning_score_s_report_init_imapala as 
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
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
	where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
),
-- 拷咀?蠻? --
RMP_WARNING_SCORE_DETAIL_ as  --圓少蛍--拷咀?蠻? 圻兵俊笥
(
	-- 扮寂?渣堂新? --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(score_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 掲扮寂?渣堂新? --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch) 
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --圓少蛍--拷咀?蠻蘋?雰 圻兵俊笥
(
	-- 扮寂?渣堂新? --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
    union all
    -- 掲扮寂?渣堂新? --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- 蒙尢恒?弑? --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --蒙尢恒?弑?_忝栽圓少吉雫
(
	-- 扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf   --hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- 掲扮寂?渣堂新? --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --函耽爺恷仟答肝 忝栽圓少-蒙尢恒?弑?(喘噐?渣峠駝賁慱?袈律??恍爺議音喘?渣?)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_MODEL_Batch as  -- 函耽爺恷仟答肝方象
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
RMP_WARNING_SCORE_DETAIL_Batch as -- 函耽爺恷仟答肝方象?┻洩貶?象恂袈律?渣藤?
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- RMP_WARNING_SCORE_DETAIL_Batch as 
-- (
-- 	select a.*
-- 	from RMP_WARNING_SCORE_DETAIL_Batch_Tmp a 
-- 	join rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch c 
-- 		on a.idx_name=c.feature_name  --蒙尢袈律?渣?
-- ),
mid_RMP_WARNING_SCORE_DETAIL_HIS as --?。。ー豌?
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_ main
	left join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
),
-- 及屈粁方象 --
Second_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		nvl(a.synth_warnlevel,'0') as synth_warnlevel, --忝栽圓少吉雫
		main.dimension,    --略業園鷹
		f_cfg.dimension as dimension_ch,  --略業兆各
		main.type,  	-- used
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.last_idx_value, -- used
		main.idx_unit,  -- used
		main.idx_score,  -- used
		f_cfg.feature_name_target,  --蒙尢兆各-朕炎(狼由)  used
		main.contribution_ratio,
		main.factor_evaluate,  --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --略業欠?婬伴?(佃泣)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
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
			dim_warn_level,
			dim_warn_level_desc,  --略業欠?婬伴?(佃泣)
			type,
			factor_evaluate,  --咀徨得勺??咀徨頁倦呟械議忖粁 0?災豎? 1?砕?械
			idx_name,  -- 呟械咀徨/呟械峺炎
			feature_name_target,
			idx_value,
			last_idx_value,
			idx_unit,
			contribution_ratio,
			idx_score,   --峺炎得蛍 used
			concat(feature_name_target,'葎',cast(idx_value as string),idx_unit) as idx_desc,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --函耽爺恷仟答肝議略業欠?婬伴怯箒? 參式 蒙尢得蛍延強 方象??咀徨蚊中
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dimension_ch,
		a.type,
		a.dim_contrib_ratio,   --略業恒?弑晩識?(電會喘) used
		a.dim_warn_level,	  --書晩略業欠?婬伴?
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --恍晩略業欠?婬伴?
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) >0 then '貧幅'
			else ''
		end as dim_warn_level_chg_desc,
		a.factor_evaluate,
		a.idx_name, 
		a.idx_value,
		case 
			when a.idx_unit = '%' then 
				cast(cast(round(a.idx_value,2) as decimal(10,2)) as string)
			-- when a.idx_unit <>'%' or a.idx_unit<>'' then 
			else
				cast(cast(round(a.idx_value,0) as decimal(10,0)) as string)
		end as idx_value_str,   --斤峺炎峙功象音揖議汽了恂膨普励秘
		a.last_idx_value,
		case 
			when a.idx_unit = '%' then 
				cast(cast(round(a.last_idx_value,2) as decimal(10,2)) as string)
			-- when a.idx_unit <>'%' or a.idx_unit<>'' then 
			else
				cast(cast(round(a.last_idx_value,0) as decimal(10,0)) as string)
		end as last_idx_value_str,   --斤恍晩峺炎峙功象音揖議汽了恂膨普励秘
		a.feature_name_target,
		a.idx_unit,
		a.contribution_ratio,
		a.idx_score,   -- 書晩峺炎嬉蛍
		b.idx_score as idx_score_1, -- 恍晩峺炎嬉蛍
		case 
			when cast(a.idx_score as float)-cast(b.idx_score as float) >0 then '具晒'  --峺炎蚊 蒙尢得蛍触誼蛍延互夸葎具晒
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join mid_RMP_WARNING_SCORE_DETAIL_HIS b
		on  a.corp_id=b.corp_id 
			and to_date(date_add(a.score_dt,-1)) = b.score_dt
			and a.dimension=b.dimension
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 酒烏方象 --
Warn_lv_Feat_score_Idx_value_Summ as --栽旺 略業欠?婬伴僑?蒙尢得蛍 參式 峺炎延強(酒烏wy喘) 方象
(
	select *,
		case 
			when factor_evaluate=0 and dim_idx_score_chg_desc='具晒'  then 
				'呟械'
			else
				NULL
		end as s_dim_desc  --酒烏略業蚊 '呟械'忖劔補竃貸辞
	from 
	(
		select
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			max(idx_score_chg_desc) over(partition by batch_dt,corp_id,score_dt,dimension) as dim_idx_score_chg_desc, 
			type,
			factor_evaluate,
			idx_name, 
			idx_value,
			last_idx_value,
			idx_unit,
			contribution_ratio,
			idx_score_chg_desc,
			concat(
				case 
					when factor_evaluate=0 and idx_score_chg_desc<>'具晒' then 
						concat(feature_name_target,'葎',idx_value_str,idx_unit)
					when factor_evaluate=0 and idx_score_chg_desc='具晒' then 
						concat(
							concat(feature_name_target,'葎',idx_value_str,idx_unit),'??','拝窟伏具晒','??','喇',
							case 
								when last_idx_value<=idx_value then 
									concat(
											concat(last_idx_value_str,idx_unit),
											'幅崛',
											concat(idx_value_str,idx_unit)
									)
								else 
									concat(
											concat(last_idx_value_str,idx_unit),
											'週崛',
											concat(idx_value_str,idx_unit)
									)
							end
						)
					else ''
				end
			) as s_report_idx_desc_no_color,   --酒烏喘欺議峺炎蚊議峺炎宙峰方象
			concat(
				case 
					when factor_evaluate=0 and idx_score_chg_desc<>'具晒' then 
						concat(feature_name_target,'葎','<span class="RED">',idx_value_str,idx_unit,'</span>')
					when factor_evaluate=0 and idx_score_chg_desc='具晒' then 
						concat(
							concat(feature_name_target,'葎','<span class="RED">',idx_value_str,idx_unit),'</span>','??','拝窟伏具晒','??','喇',
							case 
								when last_idx_value<=idx_value then 
									concat(	'<span class="RED">',
											concat(last_idx_value_str,idx_unit),
											'幅崛',
											concat(idx_value_str,idx_unit),
											'</span>'
									)
								else 
									concat(
											'<span class="RED">',
											concat(last_idx_value_str,idx_unit),
											'週崛',
											concat(idx_value_str,idx_unit),
											'</span>'
									)
							end
						)
					else ''
				end
			) as s_report_idx_desc,   --酒烏喘欺議峺炎蚊議峺炎宙峰方象
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from rmp_warning_dim_warn_lv_and_idx_score_chg
	) A where rm<=5 
),
s_datg_dim_type as   --祉悳欺略業??窃艶蚊方象 
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		s_dim_desc,
		dim_idx_score_chg_desc,
		-- dimension_ch,
		type,
		-- idx_desc_in_one_type,
		-- idx_desc,
		-- factor_evaluate,
		-- concat_ws('??',collect_set(s_report_idx_desc)) as s_report_idx_desc_in_one_type  -- hive
		group_concat(distinct s_report_idx_desc,'??') as s_report_idx_desc_in_one_type  -- impala 
		  --酒烏略業蚊 '呟械'忖劔補竃貸辞
	from Warn_lv_Feat_score_Idx_value_Summ 
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,s_dim_desc,dim_idx_score_chg_desc,type
),
-- 酒烏佚連wy --
s_msg_dim_type as 
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		type,
		concat(
			case s_dim_desc
				when '呟械' then 
					concat(
						type,s_dim_desc,'??',s_report_idx_desc_in_one_type
					)
				else 
					s_dim_desc
			end
		) as s_msg_in_one_type_no_color,
		concat(
			case s_dim_desc
				when '呟械' then 
					concat(
						'<span class="WEIGHT">',type,s_dim_desc,'??','</span>',s_report_idx_desc_in_one_type
					)
				else 
					s_dim_desc
			end
		) as s_msg_in_one_type
	from s_datg_dim_type
),
s_msg as   --恷嶮佚連婢幣祉悳欺二匍蚊
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		case 
			when nvl(ru.reason,'') = '' then 
				if(a.corp_msg_='' or a.corp_msg_ is null,
					'乎麼悶輝念涙?墻?欠?婬磧?',
					corp_msg_
				) 
			else 
				if(a.corp_msg_='' or a.corp_msg_ is null,
					'乎麼悶輝念涙?墻?欠?婬磧?',
					concat('乎麼悶乾窟','<span class="WEIGHT">',nvl(ru.reason,''),'</span>\\r\\n',
					a.corp_msg_
					)
				) 
		end as corp_msg   -- hive峇佩繍氏卦指''??impala卦指NULL賜''
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			-- concat_ws('\\r\\n',collect_set(s_msg_in_one_type)) as corp_msg_  -- hive
			group_concat(distinct s_msg_in_one_type,'\\r\\n') as corp_msg_  -- impala
		from s_msg_dim_type
		group by batch_dt,corp_id,corp_nm,score_dt
	)A 
	left join warn_adj_rule_cfg  ru
			on a.corp_id = ru.corp_id

)
------------------------------------參貧何蛍葎匝扮燕-------------------------------------------------------------------
select distinct
	-- concat(corp_id,md5(concat(batch_dt,corp_id))) as sid_kw,  -- hive
	-- '' as sid_kw,  -- impala
	-- batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	corp_msg as report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_msg
where score_dt >= '2022-09-09'
  and score_dt <= '2022-10-14'
;

--??3??sql兜兵晒 hive峇佩
insert into pth_rmp.rmp_warning_score_s_report_init partition(etl_date=19900101)
select 
	concat(corp_id,md5(concat(cast(score_dt as string),corp_id))) as sid_kw,
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
from pth_rmp.rmp_warning_score_s_report_init_imapala
;