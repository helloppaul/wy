-- RMP_WARNING_SCORE_REPORT ������-ͬ�������ҵ --
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */
-- /* 2023-01-01 model_version_intf_ ��ȡ����ͼ���� */
-- /* 2023-01-09 ��������������distinctȥ�� */
-- /* 2023-01-08  ����Ч���Ż����������������Ż���� */



set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15; 
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


-- drop table if exists pth_rmp.rmp_warning_score_report3;  
-- create table pth_rmp.rmp_warning_score_report3 as  --@pth_rmp.rmp_warning_score_report3
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
corp_chg as  --���� ��Ͷ/��ҵ�жϺ͹���һ����ҵ/֤���һ����ҵ ������corp_chg  (����2)
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,  --1 ��ҵծ 2 ��Ͷծ
	b.industryphy_name,
	b.zjh_industry_l1 
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
--������������������������������������������������������������������������������������������������������������ �ӿڲ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ʱ�����ƿ��� --
timeLimit_switch as 
(
    select True as flag   --TRUE:ʱ��Լ����FLASE:ʱ�䲻��Լ����ͨ�����ڳ�ʼ��
    -- select False as flag
),
-- ģ�Ͱ汾���� --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
	select * from pth_rmp.v_model_version  --�� Ԥ����-���ñ��е���ͼ
    -- select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --��Ƶģ��
    -- union all
    -- select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-��Ͷģ��
    -- union all 
    -- select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-��ҵģ��
    -- union all 
    -- select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-���ֿ�ģ��(��Ƶ)
    -- union all 
    -- select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-�޼ලģ��
    -- union all 
    -- select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --���÷����ۺ�ģ��
),
-- Ԥ���� --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ԥ����_�ںϵ������ۺ�  ԭʼ�ӿ�
(
	select a.*
    from 
    (
		select m.*
		from
		(
			-- ʱ�����Ʋ��� --
			select *,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 = 1--in (select max(flag) from timeLimit_switch) 
			  and etl_date=${ETL_DATE}
			  and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
		) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rmp_cs_compy_region_ as   -- ����Ӫ���� (ÿ��ȫ���ɼ�)
(
	select a.*
	from hds.t_ods_rmp_cs_compy_region a 
	where a.isdel=0
	  and a.etl_date in (select max(etl_date) as max_etl_date from hds.t_ods_rmp_cs_compy_region)
),
RMP_WARNING_SCORE_MODEL_ as  --Ԥ����-ģ�ͽ����
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- Ԥ����
		case a.interval_text_adjusted
			when '��ɫԤ��' then '-1' 
			when '��ɫԤ��' then '-2'
			when '��ɫԤ��' then '-3'
			when '��ɫԤ��' then '-4'
			when '�����ѱ�¶' then '-5'
		end as synth_warnlevel,  -- �ۺ�Ԥ���ȼ�,
		case
			when a.interval_text_adjusted in ('��ɫԤ��','��ɫԤ��') then 
				'-1'   --�ͷ���
			when a.interval_text_adjusted  = '��ɫԤ��' then 
				'-2'  --�з���
			when a.interval_text_adjusted  ='��ɫԤ��' then 
				'-3'  --�߷���
			when a.interval_text_adjusted  ='�����ѱ�¶' then 
				'-4'   --�����ѱ�¶
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
-- ���������� --
mid_rmp_cs_compy_region_ as 
(
	select distinct
		b.corp_id,
		b.corp_name as corp_nm,
		a.region_cd,
		a.client_id
	from rmp_cs_compy_region_ a 
	join (select * from corp_chg where source_code='CSCS') b 
		on cast(a.company_id as string)=b.source_id 
),
Third_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.synth_warnlevel,  -- �ۺ�Ԥ���ȼ� used
		chg.bond_type,
		chg.zjh_industry_l1
	from RMP_WARNING_SCORE_MODEL_Batch main 
	join (select * from corp_chg where source_code='ZXZX') chg 
		on main.corp_id=chg.corp_id
),
Third_Part_Data_CY_Prepare as   -- ����Ϊ��ҵ������
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type,   -- ����1����ҵ
		'' as bond_type_desc,
		a.zjh_industry_l1 as corp_property,  -- ����2����ҵ
		concat(a.zjh_industry_l1,'��') as corp_property_desc,
		b.corp_id as same_property_corp_id,   --����Ϊ��ҵծ���� �� ͬ��ҵ���ۺ�Ԥ���ȼ���� �� ��ҵ
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_Prepare a
	join (select * from Third_Part_Data_Prepare where bond_type <>2 ) b 
		on  a.zjh_industry_l1= b.zjh_industry_l1 and a.synth_warnlevel=b.synth_warnlevel  --�ۺ�Ԥ���ȼ���ͬ����ҵ
	where a.bond_type <>2  --��ҵծ
	  and a.corp_id<>b.corp_id
),
Third_Part_Data_CY as    -- �Ͳ�ҵ������ͬ���Ե� ������ҵ���� ����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CY_Prepare
),
Third_Part_Data_CT_Prepare_I as -- ���� Ϊ ��Ͷ������
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 
		'��Ͷƽ̨����' as bond_type_desc,
		b.region_cd
	from Third_Part_Data_Prepare a
	join mid_rmp_cs_compy_region_ b
		on  a.corp_id = b.corp_id
	where a.bond_type=2  -- ��Ͷ 
),
Third_Part_Data_CT_Prepare_II as 
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 	 -- ����1����Ͷ 
		a.bond_type_desc,
		cast(a.region_cd as string) as corp_property,   
		'ͬ����ͬ����������' as corp_property_desc,    -- ����2��ͬ����ͬ��������
		b.corp_id as same_property_corp_id,
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_CT_Prepare_I a 
	join Third_Part_Data_CT_Prepare_I b
		on a.region_cd=b.region_cd and a.synth_warnlevel=b.synth_warnlevel
	where a.corp_id<>b.corp_id
),
Third_Part_Data_CT as -- �ͳ�Ͷ������ͬ���Ե� ������ҵ���� ����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CT_Prepare_II
),
Third_Part_Data_SUMM as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		corp_id_cnt
	from 
	(
		select 
			*
			--row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel order by 1) as rm_rep_data
		from 
		(
			select
				*,
				row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
				count(same_property_corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
			from 
			(
				select *
				from Third_Part_Data_CY
				UNION ALL 
				select *
				from Third_Part_Data_CT
			) A --where rm<=5
		) B where rm<=5
	) C --where rm_rep_data=1
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ��������Ϣ --
Third_Msg_Corp as --�� ��������ͬ���Ե���ҵ�ϲ�Ϊһ��
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('��',collect_set(same_property_corp_nm)) as same_property_corp_nm_in_one_row  --hive
		-- group_concat(same_property_corp_nm,'��') as same_property_corp_nm_in_one_row  --impala
	from Third_Part_Data_SUMM
	group by batch_dt,corp_id,corp_nm,score_dt
),
Third_Msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'��'),''),a.corp_property_desc,
			'�������ˮƽ����һ�µ���ҵ��������',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'��',''),
			cast(corp_id_cnt as string),'����ҵ��'
		) as msg_no_color,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'��'),''),a.corp_property_desc,
			'�������ˮƽ����һ�µ���ҵ��������','<span class="WEIGHT">',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'��',''),'</span>',
			cast(corp_id_cnt as string),'����ҵ��'
		) as msg3
	from Third_Part_Data_SUMM a 
	join Third_Msg_Corp b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id
)
insert overwrite table pth_rmp.rmp_warning_score_report3
select distinct
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg_no_color,
	msg3
from
(
	select 
		*,row_number() over(partition by batch_dt,corp_id,score_dt order by 1) as rm
	from Third_Msg
) A where rm=1  --ȥ���ظ����ݣ�������������ݣ�
;




