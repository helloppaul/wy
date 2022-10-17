-- 对外投资 (同步方式：一天单批次插入) --
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
one_ as 
(
	select distinct
		a.entity_eid as cm_id,  
		a.entity_name as cm,
		
		a.eid as inv_id,   --主体企业对外投资企业ID
		a.name as inv,   --主体企业对外投资企业
		cast(a.percent as double) as inv_p,  --股东持股比例
		b.entity_type as inv_type,  -- 主体对外投资企业的类型: 'P':个人 'E':企业  'o':产品
		1 as lv        --层级标识(第一层)
	from hds.t_ods_ckg_am_rel_shareholder a join hds.t_ods_ckg_am_rel_shareholder b  on a.eid = b.entity_eid
	where cast(a.percent as double)<=1
	 and a.entity_eid<>a.eid  --排除循环持有1
),
two_ AS
(
	select distinct
		a.cm_id,
		a.cm,
		
		a.inv_id,
		a.inv,
		a.inv_p,
		a.inv_type,
		
		b.inv_id as inv_inv_id,   
		b.inv as inv_inv,			--主体对外投资企业的对外投资企业
		nvl(a.inv_p*b.inv_p,0) as inv_inv_p,  --主体对外投资企业的对外投资企业的持股比例
		b.inv_type as inv_inv_type,  --对外投资企业的对外投资企业的类型
		2 as lv
	from one_ a join one_ b on a.inv_id=b.cm_id
	 where a.cm_id<>b.inv_id  --排除循环持有2
),
three_ AS
(
	select distinct
		a.cm_id,
		a.cm,
		
		a.inv_id,
		a.inv,
		a.inv_type,
		a.inv_p,
		
		a.inv_inv_id,
		a.inv_inv,
		a.inv_inv_p,
		a.inv_inv_type,
		
		b.inv_id as inv_inv_inv_id,   
		b.inv as inv_inv_inv,  
		nvl(a.inv_inv_p*b.inv_p,0) as inv_inv_inv_p,  
		b.inv_type as inv_inv_inv_type,  
		3 as lv
	from two_ a join one_ b on a.inv_inv_id=b.cm_id
	 where a.inv_id <> b.inv_id   --排除循环持有3
),
three_cum_ as 
(
	select 
		cm_id,
		cm,
		inv_id,
		inv,
		inv_type,
		min(lv) as lv,
		sum(inv_p) as cum_ratio
	from
	(
		select distinct
			cm_id,
			cm,
			inv_id,
			inv,
			inv_p,
			inv_type,
			lv
		from one_
		union all
		select distinct
			cm_id,
			cm,
			inv_inv_id as inv_id,
			inv_inv as inv,
			inv_inv_p as inv_p,
			inv_inv_type as inv_type,
			lv
		from two_
		union all
		select distinct
			cm_id,
			cm,
			inv_inv_inv_id as inv_id,
			inv_inv_inv as inv,
			inv_inv_inv_p as inv_p,
			inv_inv_inv_type as inv_type,
			lv
		from three_
	) A
	group by cm_id,cm,inv_id,inv,inv_type
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(etl_date=${ETL_DATE},type_='dwtz')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select 
	md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6)) as sid_kw,
	T.*
from 
(
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
		-- 'dwtz' as type_
	FROM
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
		FROM
		(
			select distinct
				cm_id as corp_id,
				inv_id as relation_id,
				inv as relation_nm,
				inv_type as rela_party_type,
				4 as relation_type_l1_code,
				'直接对外投资' as relation_type_l1,
				CASE
					when cum_ratio>=0.5 then 41
					when cum_ratio>=0.3 then 42
					else 43
				END as relation_type_l2_code,
				CASE
					when cum_ratio>=0.5 then '累积持股50%以上'
					when cum_ratio>=0.3 then '累计持股比例30%-50%'
					else '累计持股比例20%-30%'
				END as relation_type_l2,
				'' as compy_type,  --关联方企业类型
				cum_ratio as cum_ratio,
				0 as type6,
				'' as rel_remark1
			from three_cum_ where cum_ratio>=0.2 and lv=1
			UNION ALL 
			select distinct 
				cm_id as corp_id,
				inv_id as relation_id,
				inv as relation_nm,
				inv_type as rela_party_type,
				5 as relation_type_l1_code,
				'间接对外投资（3层穿透）' as relation_type_l1,
				CASE
					when cum_ratio>=0.5  THEN 51
					when cum_ratio>=0.3  THEN 52
					when cum_ratio>=0.2  THEN 53
				END as relation_type_l2_code,
				CASE
					when cum_ratio>=0.5 then '累积持股50%以上'
					when cum_ratio>=0.3 then '累计持股比例30%-50%'
					else '累计持股比例20%-30%'
				END as relation_type_l2,
				'' as compy_type,  --关联方企业类型
				cum_ratio,
				0 as type6,
				'' as rel_remark1
			from three_cum_ 
			where (inv_id is not null or inv_id<>'') and lv>=2 --去重无效关联不到的数据 
			and cum_ratio>=0.2 and lv>=2
		)Final
		group by Final.corp_id,Final.relation_id,Final.rela_party_type,Final.relation_type_l1_code,Final.relation_type_l1,Final.relation_type_l2_code,Final.relation_type_l2--,Final.cum_ratio
	) L join compy_range cr on cr.corp_id=L.corp_id
		left join cm_property cmp on L.corp_id = cmp.corp_id
		LEFT JOIN corp_chg chg on L.relation_id=chg.corp_id
) T
;