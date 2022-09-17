-- RMP_WARNING_SCORE_DETAIL (ͬ����ʽ��һ������β���) --
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
corp_chg as --���� ��Ͷ/��ҵ�жϺ͹���һ����ҵ ������corp_chg
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
-- Ԥ���� --
_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ԥ����_�ںϵ������ۺ�  ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
),
-- ����ԭʼֵ --
_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --����ԭʼֵ_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
),
_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --����ԭʼֵ_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
),
-- �������׶� --
_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
),
-- �����÷�(������ֿ�) --
_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ� 
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --�����÷�_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
),
_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --�����÷�_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
),
-- _RMP_WARNING_SCORE_DETAIL_HIS_ as   --����������ʷ��(���ڻ�ȡ��һ��ָ��ֵ)
-- (
--     select  distinct
--         corp_id,
--         corp_nm,
--         score_dt,
--         sub_model_name,
--         idx_name,
--         idx_value,
--         idx_unint
--     from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS   --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
--     --where score_dt=to_date(date_add(current_timestamp(),-1))
-- ),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
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
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- Ԥ���� --
warn_union_adj_sync_score as --ȡ�������ε��ںϵ������ۺ�Ԥ����
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.rating_dt) as score_dt,
        a.total_score_adjusted as adj_score,
		case a.interval_text_adjusted
			when '��ɫ�ȼ�' then '-1' 
			when '��ɫ�ȼ�' then '-2'
			when '��ɫ�ȼ�' then '-3'
			when '��ɫ�ȼ�' then '-4'
			when '�����ѱ�¶' then '-5'
		end as adj_synth_level,
		a.model_name,
		a.model_version
    from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ ) b
        on a.rating_dt=b.max_rating_dt
    join corp_chg chg
        on chg.source_code='FI' and chg.source_id=cast(a.corp_code as string)
),
-- ����ԭʼֵ --
warn_feature_value as --ԭʼ����ֵ_�ϲ����е�Ƶ
(
    SELECT
        cast(max(a.batch_dt) over() as string) as batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_value as idx_value,
        '' as idx_unit,
        model_freq_type,  --����������ģ�ͷ���/ģ��Ƶ�ʷ���
        model_name as sub_model_name
    from
    (
        --��Ƶ
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '��Ƶ' as model_freq_type,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ 
        union all 
        --��Ƶ
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '��Ƶ' as model_freq_type,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ 
        union all 
        --��Ƶ_��Ͷ
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '��Ƶ-��Ͷ' as model_freq_type,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ 
        union all 
        --��Ƶ_��ҵծ
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            cast(feature_value as float) as feature_value,
            '��Ƶ-��ҵ' as model_freq_type,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ 
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
),
warn_feature_value_with_median as --ԭʼ����ֵ_�ϲ����е�Ƶ+��λ������
(
    select distinct
        a.batch_dt,
        a.score_dt,
        a.corp_id,
        a.corp_nm,
        a.idx_name,
        a.idx_value,
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name,
        nvl(b.industryphy_name,'') as gb,
        nvl(b.bond_type,0) as bond_type  --0���ǲ�ҵ�ͳ�Ͷ 1����ҵծ 2����Ͷծ
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='FI') b 
        on a.corp_id=b.corp_id 
),
warn_feature_value_with_median_cal as 
(
    select 
        batch_dt,
        score_dt,
        corp_id,
        bond_type,
        sub_model_name,
        idx_name,
        appx_median(idx_value) as median
        -- percentile(idx_value,0.5) as median  --hive
    from warn_feature_value_with_median
    where bond_type=2
    group by bond_type,corp_id,batch_dt,score_dt,sub_model_name,idx_name
    union all 
    select 
        batch_dt,
        score_dt,
        corp_id,
        bond_type,
        sub_model_name,
        idx_name,
        appx_median(idx_value) as median
    from warn_feature_value_with_median
    where bond_type<>2 and gb <> ''
    group by  bond_type,gb,corp_id,batch_dt,score_dt,sub_model_name,idx_name
),
warn_feature_value_with_median_res as 
(
    select 
        b.batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
        b.corp_id,
        b.corp_nm,
        b.score_dt,
        b.idx_name,
        b.idx_value,
        b.idx_unit,
        b.model_freq_type,  --����������ģ�ͷ���/ģ��Ƶ�ʷ���
        b.sub_model_name,
        cal.median
    from warn_feature_value_with_median_cal cal 
    join warn_feature_value b 
        on cal.corp_id=b.corp_id and cal.batch_dt=b.batch_dt and cal.sub_model_name=b.sub_model_name and cal.idx_name=b.idx_name 
),
-- �������׶� --
warn_contribution_ratio as 
(
    select distinct
        cast(a.end_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name,
        feature_pct as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --�쳣��ʶ 
        sub_model_name
    from _rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
),
warn_contribution_ratio_with_factor_evl as  --���������۵��������׶�Ӧ�ò�����
(
    SELECT distinct
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_dt,
        a.feature_name,
        a.contribution_ratio,
        case 
            when a.abnormal_flag = 1 and b.idx_value is not null then 
                0  --�쳣 
            else 1 --���� 
        end as factor_evaluate,
        a.sub_model_name
    from warn_contribution_ratio a 
    left join warn_feature_value b 
        on a.corp_id=b.corp_id and a.batch_dt=b.batch_dt and a.sub_model_name=b.sub_model_name
),
-- ���ֿ� --
warn_score_card as 
(
    select 
        cast(max(a.batch_dt) over() as string) as batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name as idx_name,
        feature_score as idx_score,  --ָ������
        model_name as sub_model_name
    from 
    (
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ --��Ƶ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ --��Ƶ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ --��Ƶ_��Ͷ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from _rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ --��Ƶ_��ҵծ-Ƶ�ֿ�
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='FI'
),
warn_feat_CFG as 
(
    select 
        *,
        count(feature_cd) over(partition by dimension) as contribution_cnt
    from feat_CFG
),
-- -- ��һ��ָ��ֵ --
-- warn_lastday_idx_value as 
-- (
--     select * 
--     from _RMP_WARNING_SCORE_DETAIL_HIS_
--     where score_dt=to_date(date_add(current_timestamp(),-1))
-- ),
-- ����� --
res0 as   --Ԥ����+����ԭʼֵ
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        b.idx_name,
        b.idx_value, 
        '' as idx_unit,   --�����������ñ�������
        b.model_freq_type,
        b.sub_model_name,
        b.median
    from warn_union_adj_sync_score main --Ԥ����
    left join warn_feature_value_with_median_res b  --��Ƶ�ϲ�������ԭʼֵ
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt
),
res1 as   --Ԥ����+����ԭʼֵ+�ۺϹ��׶�
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        b.contribution_ratio,  --���׶�ռ��
        b.factor_evaluate,  --��������
        b.sub_model_name as sub_model_name_zhgxd   --�ۺϹ��׶ȵ���ģ������
    from res0 main
    left join warn_contribution_ratio_with_factor_evl b 
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt and main.sub_model_name=b.sub_model_name
),
res2 as --Ԥ����+����ԭʼֵ+�ۺϹ��׶�+ָ�����ֿ�
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,   --�ۺϹ��׶ȵ���ģ������
        b.idx_score,
        b.sub_model_name as sub_model_name_zbpfk  --ָ�����ֿ�����ģ������
    from  res1 main 
    left join warn_score_card b 
        on main.corp_id=b.corp_id and main.batch_dt=b.batch_dt and main.sub_model_name=b.sub_model_name
),
res3 as   --Ԥ����+����ԭʼֵ+�ۺϹ��׶�+ָ�����ֿ�+�������ñ�
(
    select distinct 
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        f_cfg.unit_target as idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,  --�ۺϹ��׶ȵ���ģ������
        main.idx_score,
        main.sub_model_name_zbpfk,
        f_cfg.sub_model_type,
        f_cfg.feature_name_target,
        case f_cfg.dimension 
            when '����' then 1
            when '��Ӫ' then 2
            when '�г�' then 3
            when '����' then 4
        end as dimension,
        f_cfg.type,
        f_cfg.cal_explain as idx_cal_explain,
        f_cfg.feature_explain as idx_explain,
        f_cfg.unit_origin,
        f_cfg.unit_target,
        f_cfg.contribution_cnt  --�������
    from res2 main
    join warn_feat_CFG f_cfg
        on main.idx_name=f_cfg.feature_cd and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
)
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
select 
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    '' as  dim_warn_level,  --��������ģ���ںϷ����йأ�����
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --������ָ��ֵ������Ҫת��ΪĿ�����չʾ��̬�������ñ�ĵ�λ���йأ���ʱ���ԭʼֵ
    idx_unit,  
    idx_score,   
    contribution_ratio,
    contribution_cnt,  
    factor_evaluate,
    median,  --������ ������
    0 as last_idx_value,  --������
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from res3


