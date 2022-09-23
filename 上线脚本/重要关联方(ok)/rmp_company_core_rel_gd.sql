-- 股东关系(直接股东+间接股东（3层穿透）) (同步方式：一天单批次插入) --
-- 入参：${ETL_DATE}(20220818 int) 
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
		--group_concat(Compy_type,';') as Compy_type
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
one as 
(
	select 
		eid as cm_id,
		max(name) as cm,
		entity_eid gd_id,
		max(entity_name) gd,
		max(cast(percent as double)) as gd_p,  --股东持股比例
		entity_type as gd_type,  -- 股东类型
		1 as lv        --层级标识(第一层)
	from hds.t_ods_ckg_am_rel_shareholder where cast(percent as double)<=1 
	 and eid<>entity_eid   --排除循环持有1
	group by eid,entity_eid,entity_type
),
two AS
(
	select distinct
		a.cm_id,
		a.cm,
		
		a.gd_id,
		a.gd,
		a.gd_p,
		a.gd_type,
		
		b.gd_id as gd_gd_id,   --主体股东的股东
		b.gd as gd_gd,
		nvl(a.gd_p*b.gd_p,0) as gd_gd_p,  --股东的股东的持股比例
		b.gd_type as gd_gd_type,  --股东的股东的类型
		2 as lv
	from one a join one b on a.gd_id=b.cm_id 
	 where a.cm_id<>b.gd_id  --排除循环持有2
),
three AS
(
	select distinct
		a.cm_id,
		a.cm,
		
		a.gd_id,
		a.gd,
		a.gd_p,
		a.gd_type,
		
		a.gd_gd_id,
		a.gd_gd,
		a.gd_gd_p,
		a.gd_gd_type,
		
		b.gd_id as gd_gd_gd_id,   --主体股东的股东的股东
		b.gd as gd_gd_gd,
		nvl(a.gd_gd_p*b.gd_p,0) as gd_gd_gd_p,  --股东的股东的股东的持股比例
		b.gd_type as gd_gd_gd_type,  --股东的股东的类型
		3 as lv
	from two a join one b on a.gd_gd_id=b.cm_id 
	 where a.gd_id <> b.gd_id   --排除循环持有3
),
three_cum AS
(
	select 
		cm_id,
		cm,
		gd_id,
		gd,
		gd_type,
		min(lv) as lv,
		sum(gd_p) as cum_ratio
	from
	(
		select DISTINCT * from
		(
			select distinct
				cm_id,
				cm,
				gd_id,
				gd,
				gd_p,
				gd_type,
				lv
			from one
			union all
			select distinct
				cm_id,
				cm,
				gd_gd_id as gd_id,
				gd_gd as gd,
				gd_gd_p as gd_p,
				gd_gd_type as gd_type,
				lv
			from two
			union all
			select distinct
				cm_id,
				cm,
				gd_gd_gd_id as gd_id,
				gd_gd_gd as gd,
				gd_gd_gd_p as gd_p,
				gd_gd_gd_type as gd_type,
				lv
			from three
		)b
	) A
	group by cm_id,cm,gd_id,gd,gd_type
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(etl_date=${ETL_DATE},type_='gd')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select 
	md5(concat(L.corp_id,L.relation_id,cast(L.relation_type_l2_code as string),L.type6)) as sid_kw,
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
	-- 'gd' as type_
from	
(
	select 
		Final.corp_id,
		Final.relation_id,
		max(Final.relation_nm) as relation_nm,
		Final.rela_party_type,
		Final.relation_type_l1_code,
		Final.relation_type_l1,
		Final.relation_type_l2_code,
		Final.relation_type_l2,
		max(Final.compy_type) as compy_type,
		if(max(Final.cum_ratio)>1,1,max(Final.cum_ratio)) as cum_ratio,
		max(Final.type6) as type6,
		max(Final.rel_remark1) as rel_remark1
	from 
	(	
		select distinct
			cm_id as corp_id,
			gd_id as relation_id,
			gd as relation_nm,
			gd_type as rela_party_type,
			2 as relation_type_l1_code,
			'直接股东' as relation_type_l1,
			CASE
				when cum_ratio>=0.3 then 21
				when cum_ratio>=0.1 then 22
				else 23
			END as relation_type_l2_code,
			CASE
				when cum_ratio>=0.3 then '累积持股30%以上'
				when cum_ratio>=0.1 then '累计持股比例10%-30%'
				else '累计持股比例5%-10%'
			END as relation_type_l2,
			'' as compy_type,  --关联方企业类型
			cum_ratio,
			0 as type6,
			'' as rel_remark1
		from three_cum where cum_ratio>=0.05 and lv=1
		UNION ALL 
		select distinct 
			cm_id as corp_id,
			gd_id as relation_id,
			gd as relation_nm,
			gd_type as rela_party_type,
			3 as relation_type_l1_code,
			'间接股东（3层穿透）'  as relation_type_l1,
			CASE
				when cum_ratio>=0.3 then 31
				when cum_ratio>=0.1 then 32
				when cum_ratio>=0.05 then 33
			END as relation_type_l2_code,
			CASE
				when cum_ratio>=0.3 then '累积持股30%以上'
				when cum_ratio>=0.1 then '累计持股比例10%-30%'
				else '累计持股比例5%-10%'
			END as relation_type_l2,
			'' as compy_type,  --关联方企业类型
			cum_ratio,
			0 as type6,
			'' as rel_remark1
		from three_cum 
		where (gd_id is not null or gd_id<>'') --去重无效关联不到的数据 
		  and cum_ratio>=0.05 and lv>=2
	)Final join compy_range cr on Final.corp_id=cr.corp_id
	group by Final.corp_id,Final.relation_id,Final.rela_party_type,Final.relation_type_l1_code,Final.relation_type_l1,Final.relation_type_l2_code,Final.relation_type_l2--,Final.cum_ratio
) L join compy_range cr on cr.corp_id=L.corp_id
	left join cm_property cmp on L.corp_id = cmp.corp_id
	LEFT JOIN corp_chg chg on L.relation_id=chg.corp_id
;