-- ‘§æØ∑÷-πÈ“ÚœÍ«È RMP_WARNING_SCORE_DETAIL --
drop table if exists pth_rmp.RMP_WARNING_SCORE_DETAIL;
create table pth_rmp.RMP_WARNING_SCORE_DETAIL
(
    sid_kw  string,
    batch_dt string,
    corp_id string,
    corp_nm string,
    score_dt timestamp,
    dimension int,
    dim_warn_level string,
    type_cd int,
    type string,
    sub_model_name string,
    idx_name string,
    idx_value double,
    idx_unit string,
    idx_score double,
    contribution_ratio double,
    contribution_cnt bigint,
    factor_evaluate int,
    median  double,
    last_idx_value double,
    idx_cal_explain string,
    idx_explain string,
	delete_flag	int,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	int
)stored as Parquet;
