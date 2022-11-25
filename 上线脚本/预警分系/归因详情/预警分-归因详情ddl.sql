-- 归因详情 RMP_WARNING_SCORE_DETAIL --
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
    idx_value float,
    idx_unit string,
    idx_score float,
    contribution_ratio float,
    contribution_cnt bigint,
    factor_evaluate int,
    median  float,
    last_idx_value float,
    idx_cal_explain string,
    idx_explain string,
	delete_flag	int,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	int,
    ori_idx_name string,
    dim_submodel_contribution_ratio float
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;
