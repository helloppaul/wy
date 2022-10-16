-- ‘§æØ∑÷-πÈ“ÚºÚ±®wy RMP_WARNING_SCORE_S_REPORT --
drop table if exists pth_rmp.RMP_WARNING_SCORE_S_REPORT ;
create table pth_rmp.RMP_WARNING_SCORE_S_REPORT 
(
	sid_kw string,
	batch_dt string,
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
) stored as textfile;