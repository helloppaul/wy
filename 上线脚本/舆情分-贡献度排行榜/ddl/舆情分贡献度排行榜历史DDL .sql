drop table if exists pth_rmp.RMP_COMPY_CONTRIB_DEGREE_HIS;
create table pth_rmp.RMP_COMPY_CONTRIB_DEGREE_HIS
(
	sid_kw string,
	corp_id  string,
	corp_nm  string,
	score_dt  timestamp,
	relation_id  string,
	relation_nm  string,
	relation_type_l2_line  string,
	contribution_degree  double,
	rank_num  bigint,
	abnormal_flag tinyint,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int)
 stored as textfile;