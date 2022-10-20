drop table if exists pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP;
create table pth_rmp.RMP_ALERT_COMPREHS_SCORE_TEMP
(	
	sid_kw string,
	batch_dt string,
	corp_id	string,
	corp_nm	string,
	credit_code	string,
	score_dt	timestamp,
	score	double,
	relation_id	string,
	relation_nm	string,
	r_score	double,
	r	double,
	r_score_cal	double,
	news_duplicates_ratio	double,
	second_score	double,
	third_score	double,
	origin_comprehensive_score double,
	comprehensive_score	double,
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
)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;
