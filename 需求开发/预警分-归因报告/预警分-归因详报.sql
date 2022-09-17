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
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
--������������������������������������������������������������������������������������������������������������ �ӿڲ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
_RMP_WARNING_SCORE_MODEL_ as   -- Ԥ����-ģ�ͽ����
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
_RMP_WARNING_SCORE_DETAIL_ as 
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
),
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
feat_CFG as 
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
Second_Part_Data as 
(
	select 
		dimension,
		dim_contrib_ratio,
		risk_lv,  --���յȼ�
		idx_name,

	from

)
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

