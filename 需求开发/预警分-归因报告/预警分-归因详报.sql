-- RMP_WARNING_SCORE_REPORT (ͬ����ʽ��һ������β���) --
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
corp_chg as  --���� ��Ͷ/��ҵ�жϺ͹���һ����ҵ ������corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  join (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main ) b2
		  	on b1.etl_date=b2.etl_date
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
        feature_name_target,
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
-- warn_feature_contrib as --�������׶�-�ϲ����е�Ƶ
-- (
-- 	select 
-- 		cast(max(a.batch_dt) over() as string) as batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
-- 		chg.corp_id,
-- 		chg.corp_name as corp_nm,
-- 		to_date(end_dt) as score_dt,
-- 		feature_name,
-- 		feature_pct,
--         model_freq_type,  --����������ģ�ͷ���/ģ��Ƶ�ʷ���
-- 		feature_risk_interval,
-- 		model_name,
-- 		model_version as sub_model_name
-- 	from 
-- 	(
-- 		--��Ƶ
-- 		select distinct
-- 			end_dt as batch_dt,
-- 			corp_code,
-- 			end_dt,
-- 			feature_name,
-- 			cast(feature_pct as float) as feature_pct,  --�������׶�
-- 			'��Ƶ' as model_freq_type,
-- 			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
-- 			model_name,
-- 			model_version
-- 		from _rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_
-- 		union all 
-- 		--��Ƶ
-- 		select distinct
-- 			end_dt as batch_dt,
-- 			corp_code,
-- 			end_dt,
-- 			feature_name,
-- 			cast(feature_pct as float) as feature_pct,  --�������׶�
-- 			'��Ƶ' as model_freq_type,
-- 			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
-- 			model_name,
-- 			model_version
-- 		from _rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_
-- 		union all 
-- 		--��Ƶ-��Ͷ
-- 		select distinct
-- 			end_dt as batch_dt,
-- 			corp_code,
-- 			end_dt,
-- 			feature_name,
-- 			cast(feature_pct as float) as feature_pct,  --�������׶�
-- 			'��Ƶ-��Ͷ' as model_freq_type,
-- 			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
-- 			model_name,
-- 			model_version
-- 		from _rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ 
-- 		union all 
-- 		--��Ƶ-��ҵ
-- 		select distinct
-- 			end_dt as batch_dt,
-- 			corp_code,
-- 			end_dt,
-- 			feature_name,
-- 			cast(feature_pct as float) as feature_pct,  --�������׶�
-- 			'��Ƶ-��ҵ' as model_freq_type,
-- 			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
-- 			model_name,
-- 			model_version
-- 		from _rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_
-- 	)A join corp_chg chg 
--         on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
-- ),
Second_Part_Data_Prepare as 
(
	select 
		main.corp_id,
		main.corp_nm,
		main.score_dt,
		nvl(a.synth_warnlevel,'0') as synth_warnlevel, --�ۺ�Ԥ���ȼ�
		main.dimension,
		main.type,
		main.idx_name,
		main.feature_name_target,
		main.contribution_ratio,
		main.factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		c.risk_lv_desc as dim_risk_lv  --ά�ȷ��յȼ�(�ѵ�)
	from _RMP_WARNING_SCORE_DETAIL_ main
	left join _RMP_WARNING_SCORE_MODEL_ a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	-- left join warn_feature_contrib b
	left join _warn_dim_risk_level_cfg_ c
	
),
Second_Part_Data as 
(
	select 
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dim_contrib_ratio,
		dim_risk_lv,  --ά�ȷ��յȼ�(�ѵ�)
		type,
		factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		dim_evalu_contribution_ratio,  --��ά�������������µ� ���ӹ��׶�ռ�Ȼ���
		idx_name,  -- �쳣����/�쳣ָ��
		factor_evaluate		
	from
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
First_Msg as --��һ����Ϣ
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
)

