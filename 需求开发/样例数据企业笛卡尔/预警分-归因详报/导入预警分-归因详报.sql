--暂未导入
create table pth_rmp.RMP_WARNING_SCORE_REPORT_HIS as 
with _corp_sample_ as 
(
    select distinct corp_id,corp_nm,credit_cd
    from corp_sample
)
select 
    b.corp_id,
    b.corp_nm,
    b.credit_cd,
    a.score_dt,
    a.report_msg1,
    a.report_msg2,
    a.report_msg3,
    a.report_msg4,
    a.report_msg5,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_WARNING_SCORE_REPORT_HIS_TMP a 
cross join  _corp_sample_ b;