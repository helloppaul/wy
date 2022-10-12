-- RMP_WARNING_SCORE_REPORT (ͬ����ʽ��һ������β���) --
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
-- Ԥ���� --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ԥ����_�ںϵ������ۺ�  ԭʼ�ӿ�
(
    -- ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_HIS_
-- (

-- ),
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
-- RMP_WARNING_SCORE_MODEL_ as   -- Ԥ����-ģ�ͽ���� ԭʼ�ӿ�
-- (
-- 	select * 
-- 	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
-- ),
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --Ԥ����--����������ʷ ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
rmp_cs_compy_region_ as   -- ����Ӫ���� (ÿ��ȫ���ɼ�)
(
	select a.*
	from hds.t_ods_rmp_cs_compy_region a 
	where a.isdel=0
	  and a.etl_date in (select max(etl_date) as max_etl_date from hds.t_ods_rmp_cs_compy_region)
),
RMP_WARNING_SCORE_CHG_ as 
(
	select *
	from app_ehzh.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	where delete_flag=0
),
-- -- �������׶� --
-- _rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --�������׶�_��Ƶ
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --�������׶�_��Ƶ
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --�������׶�_��Ƶ��Ͷ
-- (
-- 	select * 
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf
-- ),
-- _rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --�������׶�_��Ƶ��ҵ
-- (
-- 	select *
-- 	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf
-- ),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_level_ratio_cfg_ as -- �ۺ�Ԥ���ȼ��ȼ����ֵ�λ-���ñ�
(
	select '-5' as warn_lv,'ǰ10%' as percent_desc,'�����ѱ�¶' as warn_lv_desc
	union all 
	select '-4' as warn_lv,'10%-30%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all 
	select '-3' as warn_lv,'30%-60%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all 
	select '-2' as warn_lv,'60%-80%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all 
	select '-1' as warn_lv,'80%-100%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
),
warn_dim_risk_level_cfg_ as  -- ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ�
(
	select 60 as low_contribution_percent,100 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc   --(60,100]
	union all  
	select 40 as low_contribution_percent,60 as high_contribution_percent,-2 as risk_lv,'�з���' as risk_lv_desc   --(40,60]
	union all  
	select 0 as low_contribution_percent, 40 as high_contribution_percent,-1 as risk_lv,'�ͷ���' as risk_lv_desc   --(0,40]
),
-- _warn_dim_risk_level_cfg_ as  -- ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ�
-- (
-- 	select 60 as low_contribution_ratio,100 as high_contribution_ratio,'�߷���' as risk_lv_desc   --[60,100)
-- 	union all  
-- 	select 40 as low_contribution_ratio,60 as high_contribution_ratio,'�з���' as risk_lv_desc   --[40,60)
-- 	union all  
-- 	select 0 as low_contribution_ratio, 40 as high_contribution_ratio,'�ͷ���' as risk_lv_desc   --<40
-- ),
feat_CFG as --�����ֹ����ñ�
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,
        feature_name_target,  --used
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type<>'��Ƶ��Ͷ'
    union all 
    select 
        feature_cd,
        feature_name,
        '��Ƶ-��Ͷ' as sub_model_type,
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type='��Ƶ��Ͷ'
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ��һ������ --
First_Part_Data as  --���� Ԥ����-����򱨵�����
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.credit_cd,
		main.synth_warnlevel,  --�ۺ�Ԥ���ȼ� used
		chg.bond_type,  --1:��ҵծ 2:��Ͷծ
		case chg.bond_type
			when 2 then '��Ͷƽ̨'
			else '��ҵ����'
		end as corp_bond_type,  --�������� used
		cfg.warn_lv_desc, --Ԥ���ȼ����� used
		cfg.percent_desc  --Ԥ���ȼ���λ�ٷֱȻ��� used
	from RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='FI') chg
		on main.corp_id=chg.corp_id
	join warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv
),
-- �ڶ������� --
RMP_WARNING_SCORE_DETAIL_Batch as -- ȡÿ��������������
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
),
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
Second_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		nvl(a.synth_warnlevel,'0') as synth_warnlevel, --�ۺ�Ԥ���ȼ�
		main.dimension,    --ά�ȱ���
		f_cfg.dimension as dimension_ch,  --ά������
		main.type,  	-- used
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.idx_unit,  -- used
		f_cfg.feature_name_target,  --��������-Ŀ��(ϵͳ)  used
		main.contribution_ratio,
		main.factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --ά�ȷ��յȼ�(�ѵ�)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
),
Second_Part_Data as 
(
	select distinct *
	from 
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			synth_warnlevel,
			dimension,
			dimension_ch,
			-- sum(contribution_ratio) as dim_contrib_ratio,
			sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio,
			sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate) as dim_factorEvalu_contrib_ratio,
			dim_warn_level,
			dim_warn_level_desc,  --ά�ȷ��յȼ�(�ѵ�)
			type,
			factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
			idx_name,  -- �쳣����/�쳣ָ��
			idx_value,
			idx_unit,
			idx_score,   --ָ������ used
			concat(idx_name,'Ϊ',cast(idx_value as string),idx_unit) as idx_desc,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
Second_Part_Data_Dimension as -- ��ά�Ȳ��������������
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		dim_factorEvalu_contrib_ratio,
		dim_warn_level_desc,
		dim_factor_cnt,
		dim_factorEvalu_factor_cnt
	from Second_Part_Data
	where factor_evaluate = 0
),
Second_Part_Data_Dimension_Type as -- ��ά�Ȳ� �Լ� �����������������
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		-- concat_ws('��',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive 
		group_concat(idx_desc,'��') as idx_desc_in_one_type    -- impala
	from Second_Part_Data
	where factor_evaluate = 0
	group by corp_id,corp_nm,batch_dt,score_dt,dimension,dimension_ch,type
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
		on a.company_id=b.source_id 
),
Third_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_dt,
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
		a.zjh_industry_l1 as corp_property_desc,
		b.corp_id as same_property_corp_id,   --����Ϊ��ҵծ���� �� ͬ��ҵ���ۺ�Ԥ���ȼ���� �� ��ҵ
		b.corp_name as same_property_corp_nm
	from Third_Part_Data_Prepare a
	join (select * from Third_Part_Data_Prepare where bond_type = 1) b 
		on  a.zjh_industry_l1= b.zjh_industry_l1 and a.synth_warnlevel=b.synth_warnlevel  --�ۺ�Ԥ���ȼ���ͬ����ҵ
	where a.bond_type = 1  --��ҵծ
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
		same_property_corp_nm,
		row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as rm,
		count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
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
		a.region_cd as corp_property,   
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
		same_property_corp_nm,
		row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as rm,
		count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CT_Prepare_II
),
Third_Part_Data as 
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
		rm,
		corp_id_cnt
	from 
	(
		select *
		from Third_Part_Data_CY
		UNION ALL 
		select *
		from Third_Part_Data_CT
	) A where rm<=5
),
-- ���Ķ����� --
RMP_WARNING_SCORE_CHG_Batch as  --ȡÿ���������ε�Ԥ���䶯�ȼ�����
(
	select *
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
Fourth_Part_Data_synth_warnlevel as   --�ۺ�Ԥ�� �ȼ��䶯
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_date as score_dt,
		a.synth_warnlevel,   --�����ۺ�Ԥ���ȼ�
		chg.warn_lv_desc as synth_warnlevel_desc,   -- used
		a.chg_direction,
		a.synth_warnlevel_l  --�����ۺ�Ԥ���ȼ�
		cfg_l.warn_lv as synth_warnlevel_l_desc   -- used
	from RMP_WARNING_SCORE_CHG_Batch a 
	join warn_level_ratio_cfg_ cfg 
		on a.synth_warnlevel=b.warn_lv
	join warn_level_ratio_cfg_ cfg_l
		on a.synth_warnlevel_l=cfg_l.warn_lv
	where a.chg_direction='����'
),
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --ȡÿ���������ε�ά�ȷ��յȼ��䶯 �Լ� �������ֱ䶯 ����
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dim_contrib_ratio,   --ά�ȹ��׶�ռ��(������) used
		a.dim_warn_level,	  --����ά�ȷ��յȼ�
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --����ά�ȷ��յȼ�
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) >0 then '����'
			else ''
		end as dim_warn_level_chg_desc,
		
		a.idx_name, 
		a.idx_value,
		a.idx_unit,
		a.idx_score,   -- ����ָ����
		b.idx_score as idx_score_1, -- ����ָ����
		case 
			when cast(a.idx_score as int)-cast(b.idx_score as int) >0 then '��'  --�������ֿ��÷ֱ����Ϊ��
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join RMP_WARNING_SCORE_DETAIL_HIS_ b
		on a.corp_id=b.corp_id and unix_timestamp(to_date(a.score_dt),'yyyy-MM-dd')-1=unix_timestamp(to_date(b.score_dt),'yyyy-MM-dd') and a.dimension=b.dimension
),
Fourth_Part_Data_dim_warn_level_And_idx_score as  
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dim_contrib_ratio,  --ά�ȹ��׶�ռ��(������) used
		dim_warn_level,  --����ά�ȷ��յȼ�
		dim_warn_level_desc,
		dim_warn_level_1,  --����ά�ȷ��յȼ�
		dim_warn_level_1_desc,
		dim_warn_level_chg_desc,  --ά�ȷ��յȼ��䶯 ����
		idx_name,
		idx_value,
		idx_unit,
		idx_score,
		idx_score_1,
		idx_score_chg_desc,
		row_number() over(partition by corp_id,score_dt,dimension order by dim_contrib_ratio desc) as dim_contrib_ratio_rank
	from RMP_WARNING_dim_warn_lv_And_idx_score_chg
),
Fourth_Part_Data as 
(
	select 
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.synth_warnlevel_desc,
		a.chg_direction as chg_direction_desc,
		a.synth_warnlevel_l,
		a.synth_warnlevel_l_desc,
		b.dimension,
		b.dim_contrib_ratio,  --ά�ȹ��׶�ռ��(������) used
		b.dim_warn_level,  --����ά�ȷ��յȼ�
		b.dim_warn_level_desc,
		b.dim_warn_level_1,  --����ά�ȷ��յȼ�
		b.dim_warn_level_1_desc,
		b.dim_warn_level_chg_desc,  --ά�ȷ��յȼ��䶯 ����
		b.idx_name,
		b.idx_value,
		b.idx_unit,
		b.idx_score,
		b.idx_score_1,
		b.idx_score_chg_desc,
		b.dim_contrib_ratio_rank
	from Fourth_Part_Data_synth_warnlevel a 
	join Fourth_Part_Data_dim_warn_level_And_idx_score b 
		on a.corp_id=b.corp_id and a.score_dt=b.score_dt
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ��һ����Ϣ --
First_Msg as --
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		concat(
			'������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,',',
			'��',warn_lv_desc
		) as sentence_1  --��һ�仰
	from First_Part_Data
),
-- �ڶ�����Ϣ --
Second_Msg_Dimension as  -- ά�Ȳ����Ϣ����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		concat(
			dimension_ch,'ά��','��','���׶�ռ��',cast(round(dim_contrib_ratio*100,0) as string),'%','��','��',
			'��ά�ȵ�ǰ����',dim_warn_level_desc,'���յȼ�','��',
			dimension_ch,'ά��','�����',cast(dim_factor_cnt as string),'��ָ����','��',cast(dim_factorEvalu_factor_cnt as string),'��ָ������쳣','��',
			'�쳣ָ�������������չ��׶�Ϊ',cast(round(dim_factorEvalu_contrib_ratio*100,0) as string) ,'%','��'
		) as dim_msg
	from Second_Part_Data_Dimension
),
Second_Msg_Dimension_Type as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		-- concat(concat_ws('��',collect_set(dim_type_msg)),'��') as idx_desc_one_row   -- hive 
		concat(group_concat(dim_type_msg,'��'),'��') as idx_desc_in_one_dimension  --impala
	from
	(
		select 
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			concat(
				type,'�쳣��',idx_desc_in_one_type
			) as dim_type_msg
		from Second_Part_Data_Dimension_Type
	)A 
	group by corp_id,corp_nm,batch_dt,score_dt,dimension,dimension_ch
),
Second_Msg_Dim as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg_dim
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.corp_id=b.corp_id and  a.batch_dt=b.batch_dt and a.dimension=b.dimension
),
Second_Msg as    --��������δ�� ���׶�ռ�� �Ӵ�С����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('\\r\\n',collect_set(msg_dim)) as msg
		group_concat(msg_dim,'\\r\\n') as msg
	from Second_Msg_Dim
	group by corp_id,corp_nm,batch_dt,score_dt
),
-- ��������Ϣ --
Third_Msg_Corp as --�� ��������ͬ���Ե���ҵ�ϲ�Ϊһ��
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		group_concat(same_property_corp_nm,'��') as same_property_corp_nm_in_one_row
	from Third_Part_Data
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
			a.bond_type_desc,'��',a.corp_property_desc,
			'�������ˮƽ����һ�µ���ҵ��������',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'��',''),
			cast(corp_id_cnt as string),'����ҵ��'
		) as msg
	from Third_Part_Data a 
	join Third_Msg_Corp b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id
),
-- ���Ķ���Ϣ --
Fourth_Msg_Dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		-- case 
		-- 	when dim_contrib_ratio_rank = 1 then 
		-- 		concat('��Ҫ����',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		-- end as first_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 2 then 
		-- 		concat('�������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		-- end as second_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 3 then 
		-- 		concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		-- end as third_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 4 then 
		-- 		concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		-- end as fourth_reason, 
		-- case 
		-- 	when dim_contrib_ratio_rank = 5 then 
		-- 		concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		-- end as fifth_reason, 

		concat(
			case 
				when dim_contrib_ratio_rank = 1 then 
					concat('��Ҫ����',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 2 then 
					concat('�������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 3 then 
					concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 4 then 
					concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
				when dim_contrib_ratio_rank = 5 then 
					concat('��������',dimension,'��',dim_warn_level_1_desc,dim_warn_level_chg_desc,'��',dim_warn_level_desc)
		) as msg_dim
	from Fourth_Part_Data

)



