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
