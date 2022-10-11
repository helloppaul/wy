-- RMP_WARNING_SCORE_MODEL (揖化圭塀�災嗣豢狹�肝峨秘) --
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 児云佚連 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
with
corp_chg as  --揮嗤 廓誘/恢匍登僅才忽炎匯雫佩匍 議蒙歩corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
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
-- 圓少蛍 --
_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --圓少蛍_蛮栽距屁朔忝栽  圻兵俊笥
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
),
--！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！ 哘喘蚊 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！--
warn_union_adj_sync_score as --函恷仟答肝議蛮栽距屁朔忝栽圓少蛍
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
			when '欠�孀儕�其' then '-5'
		end as synth_warnlevel,  -- 忝栽圓少吉雫,
		case
			when a.interval_text_adjusted in ('駄弼圓少','仔弼圓少') then 
				'-1'   --詰欠��
			when a.interval_text_adjusted  = '拡弼圓少' then 
				'-2'  --嶄欠��
			when a.interval_text_adjusted  ='碕弼圓少' then 
				'-3'  --互欠��
			when a.interval_text_adjusted  ='欠�孀儕�其' then 
				'-4'   --欠�孀儕�其
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
)
------------------------------------參貧何蛍葎匝扮燕-------------------------------------------------------------------
-- insert into pth_rmp.RMP_WARNING_SCORE_MODEL
select 
	'' as sid_kw,  --@impala
	-- md5(concat(batch_dt,corp_id,cast(score_date as string),model_version)) as sid_kw,  --hive
	batch_dt,
	corp_id,
	corp_nm,
	credit_cd,
	score_date,
	synth_warnlevel,  -- 忝栽圓少吉雫
	synth_score,  -- 圓少蛍
	model_version,
	adjust_warnlevel,   -- 距屁朔吉雫
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from warn_union_adj_sync_score
WHERE score_date=to_date(date_add(from_unixtime(unix_timestamp(cast(${DAYPRO_1} as string),'yyyyMMdd')),1))
;