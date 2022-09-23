-- 相同实控人 (同步方式：一天单批次插入) --
-- 入参：${ETL_DATE}(20220818 int) 
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态


with 
compy_range as 
(
	select distinct itcode2 as corp_id--,c.source_id
	from hds.tr_ods_rmp_fi_x_news_tcrnwitcode o
	-- join pth_rmp.rmp_company_id_relevance c 
	-- 	on o.itcode2=c.source_id and c.source_code='FI'
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
)
insert into pth_rmp.rmp_COMPANY_CORE_REL partition(etl_date=${ETL_DATE},type_='xtskr')
------------------------------ 以上部分为临时表 ---------------------------------------------------------
select 
	md5(concat(chg_main.corp_id,L.relation_id,cast(L.relation_type_l2_code as string),L.type6)) as sid_kw,
	to_date(CURRENT_TIMESTAMP()) relation_dt,
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
	-- 'xtskr' as type_,
FROM
(
	select distinct
		cast(main_A as string) as corp_id,
		cast(main_B as string) as relation_id,
		'' as relation_nm,
		'P' as rela_party_type,
		6 as relation_type_l1_code ,  
		'相同实控人' as relation_type_l1,
		61 as relation_type_l2_code ,
		'相同实控人' as relation_type_l2,
		'' as compy_type,
		0 as cum_ratio,
		1 as type6,
		concat (act_contro_id, '\;',act_contro) as rel_remark1
	from 
	(
		select 
			a.corp_code as main_A, 
			a.actual_controller_id as act_contro_id,
			a.actual_controller as act_contro,
			b.corp_code as main_B 
		from (	select * 
				from (	select * , max (announcement_date) over (partition by corp_code) newest_date 
						from hds.t_ods_fic_hb_corp_actual_controller
						where etl_date=${ETL_DATE}
						-- and controller_type='个人'
						  and controller_type not in ('国资委','地方国资委')
						  and isvalid=1
					 ) k 
				where newest_date= announcement_date) a
				join (select * from 
							(	select *,max(announcement_date) over(partition by corp_code) newest_date 
								from hds.t_ods_fic_hb_corp_actual_controller
								where etl_date=${ETL_DATE}
								  and controller_type='个人'and isvalid=1
							) k1 where newest_date= announcement_date
					  ) b on a.actual_controller_id=b.actual_controller_id --and a.announcement_date=最新发布日and b.annc
	)c where main_B<>main_A 
) L join compy_range cr on cr.corp_id=L.corp_id
	left join cm_property cmp on L.corp_id = cmp.corp_id
	LEFT JOIN corp_chg chg on L.relation_id=chg.corp_id
	join corp_chg chg_main 
		on L.corp_id=chg_main.source_id and chg_main.source_code='FI'
; 