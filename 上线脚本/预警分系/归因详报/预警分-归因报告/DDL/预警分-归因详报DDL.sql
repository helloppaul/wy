-- 预警分-归因详报 --
drop table if exists pth_rmp.rmp_warning_score_report;
create table pth_rmp.rmp_warning_score_report
(	
	sid_kw	string,
	batch_dt  string,
	corp_id	string,
	corp_nm	string,
	credit_cd	string,
	score_dt	TIMESTAMP,
	report_msg1	string,
	report_msg2	string,
	report_msg3	string,
	report_msg4	string,
	report_msg5	string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;


--预警分-归因详报第2段临时表
drop table if exists  pth_rmp.rmp_second_part_data;
create table pth_rmp.rmp_second_part_data
(
	batch_dt string,
	corp_id string,
	corp_nm string,
	score_dt timestamp,
	synth_warnlevel string,
	dimension int ,
	dimension_ch string,
	dim_contrib_ratio double,
	dim_factorEvalu_contrib_ratio double,
	contribution_ratio double,
	dim_warn_level string,
	dim_warn_level_desc string, 
	type string,
	factor_evaluate int,  
	idx_name string,  
	feature_name_target string,
	idx_value float,
	last_idx_value float,
	idx_unit string,
	idx_score float,   
	msg_title string,   
	idx_desc string,
	dim_factor_cnt bigint,			
	dim_factorEvalu_factor_cnt bigint
)stored as parquet
;