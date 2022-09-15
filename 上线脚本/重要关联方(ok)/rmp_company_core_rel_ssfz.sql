-- 同集团内上市发债企业 (同步方式：一天单批次插入) --
-- 入参：${ETL_DATE}(20220818 int)  ; ${BATCH} ('20220818' string)
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态


with 
compy_range as 
(
	select distinct c.corp_id,c.source_id
	from hds.tr_ods_rmp_fi_x_news_tcrnwitcode o
	join pth_rmp.rmp_company_id_relevance c 
		on o.itcode2=c.source_id and c.source_code='FI'
	where o.flag<>''
),
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
cm_property as
(	select 
		corp_id,
		max(corp_name) as corp_name,
		--group_concat(Compy_type,'\;') as Compy_type
		concat_ws('\;',collect_set(Compy_type)) as Compy_type
	FROM
	(
		select 
			chg.corp_id,
			chg.corp_name,
			o.Compy_type
		from 
		(	select DISTINCT
				corp_code,
				'金融机构' as  Compy_Type
				--org_type as Compy_Type_detail
			from hds.t_ods_fic_ic_corp_basic_info --全量采及
			where etl_date=${ETL_DATE}
			  and isvalid=1
			  and unified_social_credit_code <> '' -- 统一社会信用代码
			  and corp_code is not null  -- 机构ID
			  and (org_type like '%银行%'
			   or org_type like '%保险%'
			   or org_type like '%证券%'
			   or org_type like '%基金%')
			UNION ALL 
			select DISTINCT
				corp_code,
				'发债' as Compy_Type
				--sec_type as Compy_Type_detail
			from hds.t_ods_fic_ic_sec_basic_info
			where etl_date=${ETL_DATE}
			  and isvalid=1
			  And sec_type_code like '002%'
			UNION ALL
			select DISTINCT
				corp_code,
				'新三板' as Compy_type
				--sec_type as compy_type_detail
			from hds.t_ods_fic_ic_sec_basic_info --全量采集
			where etl_date=${ETL_DATE} 
			  and isvalid=1
			  and listed_board_name = '三板'
		)o 
		join (select * from corp_chg where source_code='ZXZX') chg
			on cast(o.corp_code as string) = chg.source_id 
	) A	group by corp_id
),
listing_issuingBonds as  --上市发债企业
(
	select distinct
		chg.corp_id ,
		a.sec_type
	from hds.t_ods_fic_hb_sec_basic_info a
	join corp_chg chg
		on cast(a.corp_code AS STRING) = chg.source_id  and chg.source_code='ZXZX' 
	where etl_date=${ETL_DATE}
	  and isvalid=1
	  and (sec_type in ('A股','B股','H股') and is_listing=1) or sec_type_code like '002%'
),
Group_Corp as  --集团企业关系表(集团表)
(
	select * from hds.t_ods_ckg_am_hive_cmp_corp_map_new where batch=cast(${ETL_DATE} as string) 
),
t as
(
	select 
	o.eid as cm_id,
	--cr.corp_name as cm_nm,
	regexp_extract(o.corp_list,'"corp_id"\:"(.*"),"corp_name"\:"(.*)"',1) as cp_id , -- 集团ID
	regexp_extract(o.corp_list,'"corp_id"\:"(.*"),"corp_name"\:"(.*)"',1) as cp  -- 集团 
	--rel_type   --关联方类型
	--compy_type    --企业类型
	from compy_range cr
	join Group_Corp o
		on cr.corp_id=o.eid   --企业范围限制
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(dt=${ETL_DATE},type_='ssfz')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select 
	to_date(CURRENT_TIMESTAMP()) relation_dt,
	L.corp_id,
	L.relation_id,
	chg.corp_name as relation_nm,
	case L.rela_party_type
		WHEN 'E' then 2  --企业 
		WHEN 'P' THEN 1 --个人
		WHEN 'O' THEN 3  --产品
		ELSE 99
	end as rela_party_type,
	L.relation_type_l1_code,
	L.relation_type_l1,
	L.relation_type_l2_code,
	L.relation_type_l2,
	cmp.compy_type,
	L.cum_ratio,
	L.type6,
	L.rel_remark1,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
	-- to_date(CURRENT_TIMESTAMP()) as dt,
	-- '同集团内上市发债企业' as type_
FROM
(
	select distinct
		a.cm_id as corp_id,
		b.cm_id as relation_id,
		cm_p.corp_name as relation_nm,
		'E' as rela_party_type,
		7 as relation_type_l1_code,
		'同集团内上市发债企业' as relation_type_l1,
		71 as relation_type_l2_code,
		'同集团内上市发债企业' as relation_type_l2,
		cm_p.compy_type as compy_type,
		0 as cum_ratio,
		0 as type6, --此类型关联方不需要该字段
		'' as rel_remark1  -- 此类型关联方不需要该字段
	from t a join t b 
		on a.cp_id=b.cp_id  
	join cm_property cm_p on b.cm_id = cm_p.corp_id
	join listing_issuingBonds ssfz on b.cm_id=ssfz.corp_id
	where a.cm_id<>b.cm_id
) L join compy_range cr on cr.corp_id=L.corp_id
	left join cm_property cmp on L.corp_id = cmp.corp_id
	LEFT JOIN corp_chg chg on L.relation_id=chg.corp_id
;
