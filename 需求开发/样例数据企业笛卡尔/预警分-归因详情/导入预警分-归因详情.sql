--暂未导入
create table pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS as 
with _corp_sample_ as 
(
    select distinct corp_id,corp_nm
    from corp_sample
)
select 
    b.corp_id,
    b.corp_nm,
    a.score_dt,
    a.dimension,
    a.dim_warn_level,
    a.type_cd,
    a.type,
    a.sub_model_name,
    a.idx_name,
    a.idx_value,
    a.idx_unit,
    a.idx_score,
    a.contribution_ratio,
    a.contribution_cnt,
    a.factor_evaluate,
    a.median,
    a.last_idx_value,
    a.idx_cal_explain,
    a.idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS_TMP a 
cross join  _corp_sample_ b