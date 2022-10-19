-- 舆情司法诚信整合(2) --
--入参：${ETL_DATE}(20220818 int)  -> to_date(notice_dt)，给NULL初始化全部日期数据
--hive执行以下脚本

-- drop table if exists app_ehzh.rmp_opinion_risk_info;
create table app_ehzh.rmp_opinion_risk_info as
select 
	concat(corp_id,'_',md5(concat(cast(notice_dt as string),msg_title,case_type_ii,msg))) as sid_kw,
	corp_id,
	corp_nm,
	notice_dt,
	concat(corp_id,'_',md5(concat(cast(notice_dt as string),msg_title,case_type_ii,msg))) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
	msg_title,
	case_type_cd,
	case_type,
	case_type_ii_cd,
	case_type_ii,
	importance,
	signal_type,
	src_table,
	src_sid,
	url_kw,
	news_from,
	msg,
	CRNW0003_010,
	notice_date, 
	notice_month,
	delete_flag,
	create_by,
	create_time,
	update_by,
	update_time,
	version
from app_ehzh.rmp_opinion_risk_info_
where signal_type<>0
union all 
select 
	sid_kw,
	corp_id,
	corp_nm,
	notice_dt,
	msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
	msg_title,
	case_type_cd,
	case_type,
	case_type_ii_cd,
	case_type_ii,
	importance,
	signal_type,
	src_table,
	src_sid,
	url_kw,
	news_from,
	msg,
	CRNW0003_010,
	notice_date, 
	notice_month,
	delete_flag,
	create_by,
	create_time,
	update_by,
	update_time,
	version
from app_ehzh.rmp_opinion_risk_info_
where signal_type=0
;
