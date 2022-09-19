-- ����ͳ���ձ� RMP_OPINION_STATISTIC_DAY (ͬ����ʽ��һ������β���)  --
--��Σ�${ETL_DATE}(20220818 int)������ɸѡscore_dt
-- /*2022-9-19 ���޳���Ѷ������Ԥ������ ��Ѷ������Ԥ�� ����ͳ�Ʒ�Χ*/
with 
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
_rmp_company_info_main_ as 
(
	select 
		corp_id,credit_code as credit_cd,
		is_list,is_bond,
		list_board,bond_type,
		regorg_prov,   --ʡ
		industryphy_name as gb_tag,
		zjh_industry_l1 as zjh_tag,
		sw_industry_l1 as sw_tag,
		wind_industry_l1 as wind_tag
	from pth_rmp.rmp_company_info_main a 
	join (select max(etl_date) as max_etl_date from pth_rmp.rmp_company_info_main where delete_flag=0) b
		on a.etl_date=b.max_etl_date 
	group by corp_id,credit_code,is_list,is_bond,list_board,bond_type,regorg_prov,industryphy_name,zjh_industry_l1,sw_industry_l1,wind_industry_l1
),
_rmp_company_info_main_union as --�ϲ��������к͵�����ծ��ҵ������ȡ����������ҵ��Ӧ�İ��(�����壬��ҵ��...)�͵�����ծ��ҵ��Ӧ�İ��(��Ͷծ����ҵծ)
(
	select distinct
		corp_id,
		--credit_cd,
		1 as statistic_dim,  --1:'��ҵ' 2:'����' 
		case list_board
			when 1 then '����'
			when 2 then '��С��'
			when 3 then '��ҵ��'
			when 4 then  '������'
			when 5 then '�ƴ���'
			else 'δ֪���а��'
		end as level_type_list,  -- ��ҵ����/���о�����
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from _rmp_company_info_main_ 
	where is_list=1 and is_bond=0
	union all 
	select 	distinct
		corp_id,
		--credit_cd,
		1 as statistic_dim,  --1:'��ҵ' 2:'����'
		case bond_type
			when 1 then '��ҵծ'
			when 2 then '��Ͷծ'
			else 'δ֪��ծ'
		end as level_type_list,		 -- ��ҵ����/��ծ�������	
		gb_tag,	zjh_tag,
		sw_tag,	wind_tag
	from _rmp_company_info_main_ 
	where is_list=0 and is_bond=1
),
_rmp_company_info_main_union_hy as 
(
	select distinct
		corp_id,
		statistic_dim,
		3 as industry_class,    --������ҵ
		level_type_list,
		gb_tag as level_type_ii
	from _rmp_company_info_main_union 
	union all 
	select distinct
		corp_id,
		statistic_dim,
		4 as industry_class,   --֤�����ҵ
		level_type_list,
		zjh_tag as level_type_ii
	from _rmp_company_info_main_union 
	union all
	select distinct
		corp_id,
		statistic_dim,
		1 as industry_class,  --������ҵ
		level_type_list,
		sw_tag as level_type_ii
	from _rmp_company_info_main_union
	union all 
	select distinct
		corp_id,
		statistic_dim,
		2 as industry_class,  --wind��ҵ
		level_type_list,
		wind_tag as level_type_ii
	from _rmp_company_info_main_union  
),
main_news_without_kxun_region as  --��������(��������)
(
	select distinct
		chg.corp_id,
		chg.corp_name as corp_nm,
		a.newsdate as news_dt,
		a.newscode as news_id,
		--a.CRNW0003_001 as index_code,  --ָ�����
		--a.CRNW0003_010 as data_type, --�������  '1':'����' ,'2':'�Ա�','3':'ȫ��','4':'��Ѷ'
		cast(a.CRNW0003_006 as int) as news_importance,  --Ԥ��������Ҫ������(�����ж��Ƿ��Ǹ�������);-3:'���ظ���',-2:'�ش���',-3:'һ�㸺��'
		cast(a.CRNW0003_006 as int) as importance
	from hds.tr_ods_rmp_fi_x_news_tcrnw0003_all_v2 a 
	join corp_chg chg 
		on a.itcode2=chg.source_id and chg.source_code='FI'
	where a.flag<>'1'
	  --and a.CRNW0003_001<>'6012000'  --����Ԥ��'6012000'
	  --and a.CRNW0003_010<>'4'
	  and cast(a.CRNW0003_006 as int)<0   --����������������
),
industry_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		main.importance,  --���س̶�  1:'һ�㸺��',2:'һ�㸺��',3:'���ظ���'
		b.statistic_dim,
		b.industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join _rmp_company_info_main_union_hy b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.industry_class,b.level_type_list,b.level_type_ii
),
_rmp_company_info_main_union_region as
(
	select distinct
		corp_id,
		2 as statistic_dim,  --1:'��ҵ' 2:'����'
		'' as level_type_list,
		regorg_prov as level_type_ii
	from _rmp_company_info_main_
),
region_class_yq as 
(
	select 
		to_date(main.news_dt) as score_dt,
		importance,  --���س̶�  1:'һ�㸺��',2:'һ�㸺��',3:'���ظ���'
		b.statistic_dim,
		-1 as industry_class,
		b.level_type_list,
		b.level_type_ii,
		count(main.news_id) as opinion_cnt
	from main_news_without_kxun_region main
	join _rmp_company_info_main_union_region b 
		on main.corp_id=b.corp_id
	group by to_date(main.news_dt),main.importance,b.statistic_dim,b.level_type_list,b.level_type_ii
)
------------------------------ ���ϲ���Ϊ��ʱ�� ---------------------------------------------------------
-- insert into pth_rmp.RMP_OPINION_STATISTIC_DAY
select distinct
	from_unixtime(unix_timestamp(cast(current_timestamp() as string),'yyyy-MM-dd HH:mm:ss')) as batch_dt,
	score_dt,   --����������oracleʱ�������͸��ֶ�
	statistic_dim,  --ͳ��ά�ȣ�1:'��ҵ' 2:'����'
	industry_class,  -- -1:���� 1:'������ҵ' 2:'wind��ҵ' 3:'������ҵ' 4:'֤�����ҵ'  99:δ֪��ҵ
	importance,
	level_type_list,
	level_type_ii,
	opinion_cnt,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from
(
	select * from industry_class_yq
	union all 
	select * from region_class_yq
)A where score_dt= to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
;
 

