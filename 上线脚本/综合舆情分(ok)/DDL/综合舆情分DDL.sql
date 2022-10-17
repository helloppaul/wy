drop table if exists pth_rmp.RMP_ALERT_COMPREHS_SCORE;
create table pth_rmp.RMP_ALERT_COMPREHS_SCORE 
(	
	sid_kw string,
	batch_dt string,
	corp_id	string,
	corp_nm	string,
	credit_code	string,
	score_dt	TIMESTAMP,
	comprehensive_score	double,
	score_hit	tinyint,
	label_hit	tinyint,
	alert	tinyint,
	fluctuated	double,
	model_version	string,
	adjust_warnlevel  string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;
