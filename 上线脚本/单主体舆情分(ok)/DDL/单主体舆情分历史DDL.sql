drop table if exists pth_rmp.RMP_ALERT_SCORE_SUMM_HIS;
create table pth_rmp.RMP_ALERT_SCORE_SUMM_HIS
(	
	sid_kw string,
	corp_id string,
	corp_nm  string,
	credit_code  string,
	score_dt  TIMESTAMP,
	score  double,
	yq_num int,
	score_hit_ci tinyint,
	score_hit_yq tinyint,
	score_hit tinyint,
	label_hit  tinyint,
	alert  tinyint,
	fluctuated  double,
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
