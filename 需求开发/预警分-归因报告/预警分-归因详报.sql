-- RMP_WARNING_SCORE_REPORT (揖化圭塀�災嗣豢狹�肝峨秘) --
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 児云佚連 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
with
corp_chg as  --揮嗤 廓誘/恢匍登僅才忽炎匯雫佩匍 議蒙歩corp_chg
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
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 俊笥蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
_RMP_WARNING_SCORE_MODEL_ as   -- 圓少蛍-庁侏潤惚燕
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
_RMP_WARNING_SCORE_DETAIL_ as 
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 塘崔燕 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
_warn_level_ratio_cfg_ as -- 忝栽圓少吉雫吉雫皿蛍亀了-塘崔燕
(
	select '-5' as warn_lv,'念10%' as percent_desc,'欠�孀儕�其' as warn_lv_desc
	union all 
	select '-4' as warn_lv,'10%-30%' as percent_desc,'碕弼圓少吉雫' as warn_lv_desc
	union all 
	select '-3' as warn_lv,'30%-60%' as percent_desc,'拡弼圓少吉雫' as warn_lv_desc
	union all 
	select '-2' as warn_lv,'60%-80%' as percent_desc,'仔弼圓少吉雫' as warn_lv_desc
	union all 
	select '-1' as warn_lv,'80%-100%' as percent_desc,'駄弼圓少吉雫' as warn_lv_desc
),
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
    where sub_model_type<>'嶄撞廓誘'
    union all 
    select 
        feature_cd,
        feature_name,
        '嶄撞-廓誘' as sub_model_type,
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type='嶄撞廓誘'
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
First_Part_Data as  --癖喘 圓少蛍-拷咀酒烏議方象
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.credit_cd,
		main.synth_warnlevel,  --忝栽圓少吉雫 used
		chg.bond_type,  --1:恢匍娥 2:廓誘娥
		case chg.bond_type
			when 2 then '廓誘峠岬'
			else '恢匍麼悶'
		end as corp_bond_type,  --麼悶奉來 used
		cfg.warn_lv_desc, --圓少吉雫宙峰 used
		cfg.percent_desc  --圓少吉雫亀了為蛍曳皿蛍 used
	from _RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='FI') chg
		on main.corp_id=chg.corp_id
	join _warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv
),
Second_Part_Data as 
(
	select 
		dimension,
		dim_contrib_ratio,
		risk_lv,  --欠�婬伴�
		idx_name,

	from

)
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
First_Msg as --及匯粁佚連
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			'乎麼悶圓霞欠�嬲�峠侃噐',corp_bond_type,'嶄',percent_desc,',',
			'奉',warn_lv_desc
		) as sentence_1  --及匯鞘三
	from First_Part_Data
)

