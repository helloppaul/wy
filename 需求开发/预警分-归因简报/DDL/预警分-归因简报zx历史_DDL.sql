-- 预警分-归因简报zx历史 RMP_WARNING_SCORE_S_REPORT_ZX_HIS --
drop table if exists pth_rmp.RMP_WARNING_SCORE_S_REPORT_ZX_HIS ;
create table pth_rmp.RMP_WARNING_SCORE_S_REPORT_ZX_HIS
(
	sid_kw string,
	corp_id string,
	corp_nm string,
	score_dt timestamp,
	report_msg string,
	model_version string,
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