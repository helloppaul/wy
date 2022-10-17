-- 归因报告 ATTRIBUTION_SUMM --
drop table if exists pth_rmp.RMP_ATTRIBUTION_SUMM;
create table pth_rmp.RMP_ATTRIBUTION_SUMM
(
	sid_kw string,
	batch_dt string,
	corp_id string,
	corp_nm string,
	credit_cd string,
	score_dt timestamp,
	report_msg1 string,
	report_msg2 string,
	report_msg5 string,
	delete_flag TINYINT,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
) 
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;
