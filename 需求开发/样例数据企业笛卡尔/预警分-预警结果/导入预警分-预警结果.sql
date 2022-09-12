create table pth_rmp.rmp_warning_score_model_HIS as 
with 
_corp_sample_ as
(
	select distinct
		*
	from
	(
		select  corp_id,corp_nm,credit_cd ,ROW_NUMBER()over(partition by corp_id order by 1) as id
		from pth_rmp.corp_sample
	) A
) 
select distinct
	corp_id,
	corp_nm,
	score_date,
	credit_cd,
	synth_warnlevel,
	synth_score,
	model_version,
	adjust_warnlevel,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from 
(
	select b.*,a.*
	from rmp_warning_score_model_HIS1 a
	cross join (select distinct corp_id,corp_nm,credit_cd from _corp_sample_ where id>=1 and id<=400) b

	union all 

	select b.*,a.*
	from rmp_warning_score_model_HIS2 a
	cross join (select distinct corp_id,corp_nm,credit_cd from _corp_sample_ where id>=401 and id<=800) b

	union all 

	select b.*,a.*
	from rmp_warning_score_model_HIS3 a
	cross join (select distinct corp_id,corp_nm,credit_cd from _corp_sample_ where id>=801 and id<=1200) b
)F
