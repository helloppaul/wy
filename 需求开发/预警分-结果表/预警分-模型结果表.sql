-- RMP_WARNING_SCORE_MODEL (Í¬²½·½Ê½£ºÒ»Ìì¶àÅú´Î²åÈë) --
--¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª »ù±¾ÐÅÏ¢ ¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª--
with
corp_chg as  --´øÓÐ ³ÇÍ¶/²úÒµÅÐ¶ÏºÍ¹ú±êÒ»¼¶ÐÐÒµ µÄÌØÊâcorp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  join (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main ) b2
		  	on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
--¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª ½Ó¿Ú²ã ¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª--
-- Ô¤¾¯·Ö --
_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ô¤¾¯·Ö_ÈÚºÏµ÷Õûºó×ÛºÏ  Ô­Ê¼½Ó¿Ú
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
),
--¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª Ó¦ÓÃ²ã ¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª¡ª--
warn_union_adj_sync_score as --È¡×îÐÂÅú´ÎµÄÈÚºÏµ÷Õûºó×ÛºÏÔ¤¾¯·Ö
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- Ô¤¾¯·Ö
		case a.interval_text_adjusted
			when 'ÂÌÉ«Ô¤¾¯' then '-1' 
			when '»ÆÉ«Ô¤¾¯' then '-2'
			when '³ÈÉ«Ô¤¾¯' then '-3'
			when 'ºìÉ«Ô¤¾¯' then '-4'
			when '·çÏÕÒÑ±©Â¶' then '-5'
		end as synth_warnlevel,  -- ×ÛºÏÔ¤¾¯µÈ¼¶,
		case
			when a.interval_text_adjusted in ('ÂÌÉ«Ô¤¾¯','»ÆÉ«Ô¤¾¯') then 
				'-1'   --µÍ·çÏÕ
			when a.interval_text_adjusted  = '³ÈÉ«Ô¤¾¯' then 
				'-2'  --ÖÐ·çÏÕ
			when a.interval_text_adjusted  ='ºìÉ«Ô¤¾¯' then 
				'-3'  --¸ß·çÏÕ
			when a.interval_text_adjusted  ='·çÏÕÒÑ±©Â¶' then 
				'-4'   --·çÏÕÒÑ±©Â¶
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ ) b
        on a.rating_dt=b.max_rating_dt
    join corp_chg chg
        on chg.source_code='FI' and chg.source_id=cast(a.corp_code as string)
)
------------------------------------ÒÔÉÏ²¿·ÖÎªÁÙÊ±±í-------------------------------------------------------------------
-- insert into pth_rmp.RMP_WARNING_SCORE_MODEL
select 
	'' as sid_kw,  --@impala
	-- md5(concat(batch_dt,corp_id,cast(score_date as string),model_version)) as sid_kw,  --hive
	batch_dt,
	corp_id,
	corp_nm,
	credit_cd,
	score_date,
	synth_warnlevel,  -- ×ÛºÏÔ¤¾¯µÈ¼¶
	synth_score,  -- Ô¤¾¯·Ö
	model_version,
	adjust_warnlevel,   -- µ÷ÕûºóµÈ¼¶
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from warn_union_adj_sync_score