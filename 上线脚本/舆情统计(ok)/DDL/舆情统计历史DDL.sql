-- 舆情统计历史 RMP_OPINION_STATISTIC_HIS --
drop table if exists pth_rmp.RMP_OPINION_STATISTIC_HIS;
create table pth_rmp.RMP_OPINION_STATISTIC_HIS 
(
	sid_kw string,
	score_dt timestamp,   --！！！推送oracle时，不推送该字段
	statistic_dim int,  --统计维度，1:'行业' 2:'地区'
	industry_class int,  -- -1:地区 1:'申万行业' 2:'wind行业' 3:'国标行业' 4:'证监会行业'  99:未知行业
	importance int,
	level_type_list string,
	level_type_ii string,
	opinion_cnt bigint,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;