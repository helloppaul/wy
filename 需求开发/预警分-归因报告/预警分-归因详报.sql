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
RMP_WARNING_SCORE_MODEL_ as   -- Ԥ����-ģ�ͽ���� ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
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
RMP_WARNING_SCORE_DETAIL_Batch as 
(
	select
	from RMP_WARNING_SCORE_DETAIL_
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.delete_flag=0
),
_RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������ֻ�й��׶�ռ�����ݣ������Ϊ�գ������������Ӳ����
(
	select *
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_MODEL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.delete_flag=0
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
		cfg.risk_lv_desc as dim_warn_level_desc  --ά�ȷ��յȼ�(�ѵ�)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join _RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)

),
Second_Part_Data as 
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
		dim_warn_level_desc,  --ά�ȷ��յȼ�(�ѵ�)
		type,
		factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		idx_name,  -- �쳣����/�쳣ָ��
		idx_value,
		idx_unit,
		concat(idx_name,'Ϊ',cast(idx_value as string),idx_unit) as idx_desc,
		count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
		count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
	from Second_Part_Data_Prepare 
	order by corp_id,score_dt desc,dim_contrib_ratio desc
),
Second_Part_Data_Dimension as -- ��ά�Ȳ��������������
(
	select distinct
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
Second_Msg_I as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.corp_id=b.corp_id and a.dimension=b.dimension
),
-- Second_Msg as 
-- (
-- 	select *
-- 	from Second_Msg_I
-- 	group by 
-- )