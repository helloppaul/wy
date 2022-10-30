drop table if exists pth_rmp.RMP_ALERT_COMPREHS_SCORE_INIT;
create table pth_rmp.RMP_ALERT_COMPREHS_SCORE_INIT
(	
	sid_kw string,
	corp_id	string,
	corp_nm	string,
	credit_code	string,
	score_dt	timestamp,
	comprehensive_score double,
	score_hit	tinyint,
	label_hit	tinyint,
	alert	tinyint,
	fluctuated	double,
	model_version	string,
	adjust_warnlevel  string,
	delete_flag	tinyint,
	create_by	string,
	create_time	timestamp,
	update_by	string,
	update_time	timestamp,
	version	tinyint
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;