-- RMP_WARNING_SCORE_REPORT (ͬ����ʽ��һ������β���) --
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
corp_chg as  --���� ��Ͷ/��ҵ�жϺ͹���һ����ҵ ������corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
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
_RMP_WARNING_SCORE_MODEL_ as   -- Ԥ����-ģ�ͽ���� ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
_RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
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
_warn_level_ratio_cfg_ as -- �ۺ�Ԥ���ȼ��ȼ����ֵ�λ-���ñ�
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
	from _RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='FI') chg
		on main.corp_id=chg.corp_id
	join _warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv
),
-- �ڶ������� --
Second_Part_Data_Prepare as 
(
	select distinct
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
		main.dim_warn_level  --ά�ȷ��յȼ�(�ѵ�)  used
	from _RMP_WARNING_SCORE_DETAIL_ main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join _RMP_WARNING_SCORE_MODEL_ a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
),
Second_Part_Data as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		dimension,
		dimension_ch,
		-- sum(contribution_ratio) as dim_contrib_ratio,
		sum(contribution_ratio) over(partition by corp_id,score_dt,dimension) as dim_contrib_ratio,
		sum(contribution_ratio) over(partition by corp_id,score_dt,dimension,factor_evaluate) as dim_factorEvalu_contrib_ratio,
		dim_warn_level,  --ά�ȷ��յȼ�(�ѵ�)
		type,
		factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		idx_name,  -- �쳣����/�쳣ָ��
		idx_value,
		idx_unit,
		concat(idx_name,'Ϊ',cast(idx_value as string),idx_unit) as idx_desc,
		count(idx_name) over(partition by corp_id,score_dt,dimension)  as dim_factor_cnt,
		count(idx_name) over(partition by corp_id,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
	from Second_Part_Data_Prepare 
	order by corp_id,score_dt desc,dim_contrib_ratio desc
),
Second_Part_Data_Dimension as -- ��ά�Ȳ��������������
(
	select distinct
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		dim_factorEvalu_contrib_ratio,
		dim_warn_level,
		dim_factor_cnt,
		dim_factorEvalu_factor_cnt
	from Second_Part_Data
	where factor_evaluate = 0
),
Second_Part_Data_Dimension_Type as -- ��ά�Ȳ� �Լ� �����������������
(
	select
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
	group by corp_id,corp_nm,score_dt,dimension,dimension_ch,type
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
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		concat(
			dimension_ch,'ά��','��','���׶�ռ��',cast(round(dim_contrib_ratio*100,0) as string),'%','��','��',
			'��ά�ȵ�ǰ����',dim_warn_level,'���յȼ�','��',
			dimension_ch,'ά��','�����',cast(dim_factor_cnt as string),'��ָ����','��',cast(dim_factorEvalu_factor_cnt as string),'��ָ������쳣','��',
			'�쳣ָ�������������չ��׶�Ϊ',cast(round(dim_factorEvalu_contrib_ratio*100,0) as string) ,'%','��'
		) as dim_msg
	from Second_Part_Data_Dimension
),
Second_Msg_Dimension_Type as 
(
	select 
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
	group by corp_id,corp_nm,score_dt,dimension,dimension_ch
),
Second_Msg as 
(
	select 
		a.corp_id,
		a.corp_nm,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.corp_id=b.corp_id and a.dimension=b.dimension
)
