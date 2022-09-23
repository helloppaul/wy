drop table if exists pth_rmp.rmp_COMPANY_CORE_REL_HIS;
create table if not exists pth_rmp.rmp_COMPANY_CORE_REL_HIS
(	
	sid_kw string,
	relation_month timestamp,   
	corp_id STRING,
	relation_id STRING,
	relation_nm STRING,
	rela_party_type TINYINT,
	relation_type_l1_code TINYINT,
	relation_type_l1 STRING,
	relation_type_l2_code TINYINT,
	relation_type_l2 STRING,
	compy_type STRING,
	cum_ratio double,
	type6 TINYINT,
	rel_remark1 string,
	delete_flag TINYINT,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
)partitioned by (etl_date int)
 stored as textfile;