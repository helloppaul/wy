-- 实际控制人 rmp_company_core_rel_skr (同步方式：一天单批次插入) --
-- /* 2022-12-10 创建corp_chg，hds.tr_ods_rmp_fi_x_news_tcrnwitcoded的副本，降低锁表几率*/
-- /* 2023-1-11 增加 SQL 效率优化参数*/

-- 入参：${ETL_DATE}(20220818 int)
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态


--Part1 副本 --
drop table if exists pth_rmp.tr_ods_rmp_fi_x_news_tcrnwitcode_skr;
create table pth_rmp.tr_ods_rmp_fi_x_news_tcrnwitcode_skr stored as parquet
as 
	select * from hds.tr_ods_rmp_fi_x_news_tcrnwitcode
;

set hive.exec.parallel=true;

drop table if exists pth_rmp.corp_chg_skr;
create table pth_rmp.corp_chg_skr stored as parquet 
as 
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
;


set hive.exec.parallel=true;
-- Part2 --
with 
compy_range as 
(
	select distinct o.itcode2 as corp_id--,c.source_id
	from pth_rmp.tr_ods_rmp_fi_x_news_tcrnwitcode_skr o
	where o.flag<>''
),
corp_chg as 
(
	select * 
	from pth_rmp.corp_chg_skr
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
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(etl_date=${ETL_DATE},type_='skr')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select 
	sid_kw,
	relation_dt,
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
	delete_flag,
	create_by,
	create_time,
	update_by,
	update_time,
	version
from 
(
	select 
		md5(concat(corp_id,relation_id,cast(relation_type_l2_code as string),type6,cast(relation_dt as string))) as sid_kw,
		row_number() over(partition by corp_id,relation_id,relation_type_l2_code,type6,relation_dt) as rm,
		T.*
	from 
	(
		select DISTINCT
			-- md5(concat(chg_main.corp_id,L.relation_id,cast(L.relation_type_l2_code as string),L.type6)) as sid_kw,
			from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd') as relation_dt,
			chg_main.corp_id,
			case 
				when chg.corp_id is null then L.relation_id 
				else chg.corp_id 
			end as relation_id,
			-- L.relation_id,
			-- chg.corp_id as relation_id,
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
			--to_date(CURRENT_TIMESTAMP()) as dt,
			-- 'skr' as type_
		FROM
		(
			select DISTINCT
				cast(corp_code as string) as corp_id,
				actual_controller_id as relation_id,
				actual_controller as relation_nm,
				'' as rela_party_type,  --关联方类型 1:个人 2:企业 3:产品 99:其他 0:自身
				1 as relation_type_l1_code,
				'实际控制人' as relation_type_l1,
				11 as relation_type_l2_code,
				'实际控制人' as relation_type_l2,
				'' as compy_type,
				0 as cum_ratio,
				0 as type6,
				'' as rel_remark1
			from (	select *,max(announcement_date) over(partition by corp_code) as newest_date 
					from hds.t_ods_fic_hb_corp_actual_controller
					where etl_date=${ETL_DATE}
					and isvalid=1
					--and controller_type='个人'   不要加实际控制人类型限制
				)k 
			where announcement_date=newest_date and actual_controller<>'无实际控制人'
			and announcement_date >= add_months(to_date(CURRENT_TIMESTAMP()),-60)
		) L join compy_range cr on cr.corp_id=L.corp_id
			left join cm_property cmp on L.corp_id = cmp.corp_id
			LEFT JOIN corp_chg chg on L.relation_id=chg.source_id --and chg.source_code='FI'
			join corp_chg chg_main 
				on L.corp_id=chg_main.source_id and chg_main.source_code='FI'
	)T
)T1 where rm=1
; 