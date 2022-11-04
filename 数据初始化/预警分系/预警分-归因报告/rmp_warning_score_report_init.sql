--（1） DDL hive执行 --
drop table if exists pth_rmp.rmp_warning_score_report_init;
create table pth_rmp.rmp_warning_score_report_init
(	
	sid_kw	string,
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




--（3） rmp_warning_score_report_init hive执行 --
insert into pth_rmp.rmp_warning_score_report_init partition(etl_date=19900101)
select 
	-- '' as sid_kw,  --impala
	md5(concat(a.batch_dt,a.corp_id,cast(a.score_dt as string))) as sid_kw,  --hive
	-- a.batch_dt,
	a.corp_id,
	a.corp_nm,
	a.credit_cd,
	a.score_dt,
	a.msg1 as report_msg1,
	b.msg2 as report_msg2,
	c.msg3 as report_msg3,
	d.msg4 as report_msg4,
	b.msg5 as report_msg5,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.rmp_warning_score_report1_init_impala a
left join pth_rmp.rmp_warning_score_report2_init_impala b
	on  a.corp_id=b.corp_id and a.score_dt=b.score_dt
left join pth_rmp.rmp_warning_score_report3_init_impala c
	on a.corp_id=c.corp_id and a.score_dt=c.score_dt
left join pth_rmp.rmp_warning_score_report4_init_impala d
	on  a.corp_id=d.corp_id and a.score_dt=d.score_dt
-- where a.score_date=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
;