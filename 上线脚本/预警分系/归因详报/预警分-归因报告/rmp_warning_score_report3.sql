-- RMP_WARNING_SCORE_REPORT 及眉粁-揖窃欠?嫺麭? --
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */
-- /* 2023-01-01 model_version_intf_ 個函喘篇夕方象 */
-- /* 2023-01-09 恷嶮補竃潤惚奐紗distinct肇嶷 */
-- /* 2023-01-08  旗鷹丼楕單晒拝奐紗曾倖歌方單晒囂鞘 */



set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15; 
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


-- drop table if exists pth_rmp.rmp_warning_score_report3;  
-- create table pth_rmp.rmp_warning_score_report3 as  --@pth_rmp.rmp_warning_score_report3
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
-- 庁侏井云陣崙 --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
	select * from pth_rmp.v_model_version  --需 圓少蛍-塘崔燕嶄議篇夕
    -- select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --詰撞庁侏
    -- union all
    -- select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --嶄撞-廓誘庁侏
    -- union all 
    -- select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --嶄撞-恢匍庁侏
    -- union all 
    -- select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --互撞-得蛍触庁侏(互撞)
    -- union all 
    -- select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --互撞-涙酌興庁侏
    -- union all 
    -- select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --佚喘欠?孥杠歪Ｐ?
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
			where 1 = 1--in (select max(flag) from timeLimit_switch) 
			  and etl_date=${ETL_DATE}
			  and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
		) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rmp_cs_compy_region_ as   -- 曝囃将唔方象 (耽晩畠楚寡鹿)
(
	select a.*
	from hds.t_ods_rmp_cs_compy_region a 
	where a.isdel=0
	  and a.etl_date in (select max(etl_date) as max_etl_date from hds.t_ods_rmp_cs_compy_region)
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
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 嶄寂蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
RMP_WARNING_SCORE_MODEL_Batch as  -- 函耽爺恷仟答肝方象
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
-- 及眉粁方象 --
mid_rmp_cs_compy_region_ as 
(
	select distinct
		b.corp_id,
		b.corp_name as corp_nm,
		a.region_cd,
		a.client_id
	from rmp_cs_compy_region_ a 
	join (select * from corp_chg where source_code='CSCS') b 
		on cast(a.company_id as string)=b.source_id 
),
Third_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.synth_warnlevel,  -- 忝栽圓少吉雫 used
		chg.bond_type,
		chg.zjh_industry_l1
	from RMP_WARNING_SCORE_MODEL_Batch main 
	join (select * from corp_chg where source_code='ZXZX') chg 
		on main.corp_id=chg.corp_id
),
Third_Part_Data_CY_Prepare as   -- 麼悶葎恢匍議方象
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type,   -- 奉來1?魂?匍
		'' as bond_type_desc,
		a.zjh_industry_l1 as corp_property,  -- 奉來2?歳侑?
		concat(a.zjh_industry_l1,'嶄') as corp_property_desc,
		b.corp_id as same_property_corp_id,   --麼悶葎恢匍娥來嵎 議 揖佩匍拝忝栽圓少吉雫?犁? 議 二匍
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_Prepare a
	join (select * from Third_Part_Data_Prepare where bond_type <>2 ) b 
		on  a.zjh_industry_l1= b.zjh_industry_l1 and a.synth_warnlevel=b.synth_warnlevel  --忝栽圓少吉雫?猴?議二匍
	where a.bond_type <>2  --恢匍娥
	  and a.corp_id<>b.corp_id
),
Third_Part_Data_CY as    -- 才恢匍麼悶?猴?奉來議 凪麿二匍方楚 柴麻
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CY_Prepare
),
Third_Part_Data_CT_Prepare_I as -- 麼悶 葎 廓誘議方象
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 
		'廓誘峠岬絵笥' as bond_type_desc,
		b.region_cd
	from Third_Part_Data_Prepare a
	join mid_rmp_cs_compy_region_ b
		on  a.corp_id = b.corp_id
	where a.bond_type=2  -- 廓誘 
),
Third_Part_Data_CT_Prepare_II as 
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 	 -- 奉來1?些罵? 
		a.bond_type_desc,
		cast(a.region_cd as string) as corp_property,   
		'揖曝囃、揖佩屓雫艶拝' as corp_property_desc,    -- 奉來2?才?曝囃、揖佩屓雫艶
		b.corp_id as same_property_corp_id,
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_CT_Prepare_I a 
	join Third_Part_Data_CT_Prepare_I b
		on a.region_cd=b.region_cd and a.synth_warnlevel=b.synth_warnlevel
	where a.corp_id<>b.corp_id
),
Third_Part_Data_CT as -- 才廓誘麼悶?猴?奉來議 凪麿二匍方楚 柴麻
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CT_Prepare_II
),
Third_Part_Data_SUMM as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		corp_id_cnt
	from 
	(
		select 
			*
			--row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel order by 1) as rm_rep_data
		from 
		(
			select
				*,
				row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
				count(same_property_corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
			from 
			(
				select *
				from Third_Part_Data_CY
				UNION ALL 
				select *
				from Third_Part_Data_CT
			) A --where rm<=5
		) B where rm<=5
	) C --where rm_rep_data=1
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
-- 及眉粁佚連 --
Third_Msg_Corp as --繍 才麼悶?猴?奉來議二匍栽旺葎匯佩
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',collect_set(same_property_corp_nm)) as same_property_corp_nm_in_one_row  --hive
		-- group_concat(same_property_corp_nm,'、') as same_property_corp_nm_in_one_row  --impala
	from Third_Part_Data_SUMM
	group by batch_dt,corp_id,corp_nm,score_dt
),
Third_Msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'嶄'),''),a.corp_property_desc,
			'悳悶欠?嬲?峠燕?嶢志袖墜麭技弘?凄??',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'吉',''),
			cast(corp_id_cnt as string),'社二匍。'
		) as msg_no_color,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'嶄'),''),a.corp_property_desc,
			'悳悶欠?嬲?峠燕?嶢志袖墜麭技弘?凄??','<span class="WEIGHT">',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'吉',''),'</span>',
			cast(corp_id_cnt as string),'社二匍。'
		) as msg3
	from Third_Part_Data_SUMM a 
	join Third_Msg_Corp b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id
)
insert overwrite table pth_rmp.rmp_warning_score_report3
select distinct
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg_no_color,
	msg3
from
(
	select 
		*,row_number() over(partition by batch_dt,corp_id,score_dt order by 1) as rm
	from Third_Msg
) A where rm=1  --肇嶷嶷鹸方象??參窒竃?嶬猜?象??
;




