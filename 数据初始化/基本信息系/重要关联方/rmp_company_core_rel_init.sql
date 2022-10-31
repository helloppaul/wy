-- RMP_COMPANY_CORE_REL_INIT,rmp_COMPANY_CORE_REL_HIS_INIT --
--————————————————————————————————————DDL——————————————————————————————————————————
-- 当日 --
drop table if exists pth_rmp.RMP_COMPANY_CORE_REL_INIT;
create table if not exists pth_rmp.RMP_COMPANY_CORE_REL_INIT
(	
	sid_kw string,
	relation_dt timestamp,   
	corp_id STRING,
	relation_id STRING,
	relation_nm STRING,
	rela_party_type TINYINT,
	relation_type_l1_code TINYINT,
	relation_type_l1 STRING,
	relation_type_l2_code TINYINT,
	relation_type_l2 STRING,
	cum_ratio double,
	compy_type STRING,
	type6 string,
	rel_remark1 string,
	delete_flag TINYINT,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
)partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;

-- 历史 --
drop table if exists pth_rmp.rmp_COMPANY_CORE_REL_HIS_INIT;
create table if not exists pth_rmp.rmp_COMPANY_CORE_REL_HIS_INIT
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
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



--————————————————————————————————————SQL——————————————————————————————————————————
-- 月度表 relation_dt='2022-10-14' --
insert into pth_rmp.RMP_COMPANY_CORE_REL_INIT partition(etl_date=19900101)
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_dt ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from 
(	
	select 	
		*,
		to_date('2022-10-14') as relation_date,
		row_number() over(partition by sid_kw order by 1) as rm
	from pth_rmp.rmp_COMPANY_CORE_REL
	where relation_dt='2022-10-25'
) a 
where rm=1 and relation_type_l1<>'相同实控人'
;


-- 历史表 relation_month（12个月，每个月最后一天日期） --
insert into pth_rmp.RMP_COMPANY_CORE_REL_HIS_INIT partition(etl_date=19900101)
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-01-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-02-28') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-03-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-04-30') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-05-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-06-30') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-07-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-08-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-09-30') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-10-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-11-30') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
union all 
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_date as string))) as sid_kw ,
	relation_date as relation_month ,   
	corp_id ,
	relation_id ,
	relation_nm ,
	rela_party_type ,
	relation_type_l1_code ,
	relation_type_l1 ,
	relation_type_l2_code ,
	relation_type_l2 ,
	cum_ratio ,
	compy_type ,
	type6 ,
	rel_remark1 ,
	delete_flag ,
	create_by ,
	create_time ,
	update_by ,
	update_time ,
	version int
from (select  to_date('2022-12-31') as relation_date, * from pth_rmp.RMP_COMPANY_CORE_REL_INIT where relation_dt='2022-10-14') a
where relation_type_l1<>'相同实控人'
;

