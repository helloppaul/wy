-- 重要关联方历史 rmp_company_core_rel_his (同步方式：一个月每天覆盖)--
insert overwrite table pth_rmp.rmp_company_core_rel_his partition(etl_date=${ETL_DATE})
select 
	sid_kw,
	last_day(CURRENT_TIMESTAMP()) relation_month,  --存放当天所在本月最后一天日期,2022-08-31
	corp_id,
	relation_id,
	relation_nm,
	rela_party_type,
	relation_type_l1_code,
	relation_type_l1,
	relation_type_l2_code,
	relation_type_l2,
	cum_ratio,
	compy_type,
	type6,
	rel_remark1,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.rmp_company_core_rel
where relation_dt=last_day(CURRENT_TIMESTAMP())
  and delete_flag=0
;