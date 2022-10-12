--舆情风险信息整合表 Parquet (适用impala,原生支持Impala引擎，一种列式存储的二进制格式，有着比text更好的查询和存储效率)
drop table if exists pth_rmp.rmp_opinion_risk_info;
create table pth_rmp.rmp_opinion_risk_info
(
	sid_kw string,
	corp_id STRING,
	corp_nm STRING,
	notice_dt TIMESTAMP,
	msg_id STRING,
	msg_title STRING,
	case_type_cd STRING,
	case_type STRING,
	case_type_ii_cd STRING,
	case_type_ii STRING,
	importance double,
	signal_type TINYINT,
	src_table STRING,
	src_sid STRING,
	url_kw STRING,
	news_from STRING,
	msg STRING,
	CRNW0003_010 as string,
	notice_date timestamp,
	notice_month timestamp,
	delete_flag int,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
)partitioned by (etl_date int,type_ string) 
 stored as textfile;