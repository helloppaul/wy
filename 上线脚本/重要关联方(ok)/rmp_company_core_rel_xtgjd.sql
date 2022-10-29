-- 相同根节点 (同步方式：一天单批次插入) --
-- 入参：${ETL_DATE}(20220818 int) ; ${BATCH} ('20220818' string)
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
		  where cid1.etl_date in (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance)
			-- on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  where b1.etl_date in (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main )
		  	-- on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
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
gd as -- 加工后股东表
(
	select 
		eid as cm_id, --企业ID
		name as cm , -- 企业名称
		entity_eid as gd_id, --股东ID
		entity_name as gd, -- 股东
		cast(PERCENT as double) as p,  -- 股东对企业的持股比例
		entity_type as gd_type  -- 股东类型
	from hds.t_ods_ckg_am_rel_shareholder a 
	join (select * from corp_chg where source_code='FI') s2
		on a.eid=s2.corp_id
	-- join hds.tr_ods_rmp_fi_x_news_tcrnwitcode c
		-- on s2.source_id = c.itcode2
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(etl_date=${ETL_DATE},type_='xtgjd')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_dt as string))) as sid_kw,
	T.* 
from 
(
	select distinct
		-- md5(concat(L.corp_id,L.relation_id,cast(L.relation_type_l2_code as string),L.type6)) as sid_kw,
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
		L.cum_ratio,
		cmp.compy_type,
		cast(L.type6 as string)  as type6,
		L.rel_remark1,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
		-- to_date(CURRENT_TIMESTAMP()) as dt,
		-- 'xtgjd' as type_
	FROM
	(
		select distinct
			T.cm1_id as corp_id,
			-- cm1 as corp_nm,
			T.cm2_id as relation_id,
			T.cm2 as relation_nm,
			'E' as rela_party_type,  --！！！确认是否需要关联方类型，若要填什么
			6 as relation_type_l1_code,
			'相同实控人' as relation_type_l1,
			61 as relation_type_l2_code,
			'相同实控人' as relation_type_l2,
			-- cm_p.compy_type as compy_type,  --关联方企业的性质
			'' as compy_type,
			0 as cum_ratio,
			2 as type6,  -- 1：相同实控人 2：相同根节点
			CONCAT(T.root_id,'\;',T.root) as rel_remark1 
		from 
		(	
			select 
				o.corp_id as root_id, --集团ID（集团根节点ID） as 根节点ID
				gd1.gd as root, -- 根节点名称
				
				gd1.cm_id as cm1_id,  -- 企业1 ID 
				gd1.cm as cm1,  --企业1 名称
				
				gd2.cm_id as cm2_id,
				gd2.cm as cm2   -- 企业2 名称
			from (select * from hds.t_ods_ckg_am_hive_corp_detail_new where batch=cast(${ETL_DATE} as string)) o
			join gd gd1 on o.corp_id = gd1.gd_id 
			join gd gd2 on o.corp_id = gd2.gd_id
			where gd1.p>=0.5 and gd2.p>=0.5 and gd1.cm_id<>gd2.cm_id 
		)T --left join cm_property cm_p on t.cm2_id =cm_p.corp_id 
		join compy_range cr on cr.corp_id = T.cm1_id
	) L join compy_range cr on cr.corp_id=L.corp_id
		left join cm_property cmp on L.corp_id = cmp.corp_id
		LEFT JOIN corp_chg chg on L.relation_id=chg.corp_id
)T
; 
