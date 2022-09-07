-- 归因报告历史 RMP_ATTRIBUTION_SUMM_HIS --
drop table if exists RMP_ATTRIBUTION_SUMM_HIS;
create table RMP_ATTRIBUTION_SUMM_HIS
(
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
)partitioned by (dt string)
 stored as parquet;
