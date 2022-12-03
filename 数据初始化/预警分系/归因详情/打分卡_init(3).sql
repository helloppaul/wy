-- ������ֿ�-��ʼ�� -- 
set mem_limit=7000000000;
create table pth_rmp.warn_score_card as 
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
corp_chg as  --���� ��Ͷ/��ҵ�жϺ͹���һ����ҵ ������corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.zjh_industry_l1 as industryphy_name  --֤�����ҵ 
    ,b.exposure  --�������� used ����ȡ�����ֹ����ñ�Ψһ��������
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
      and a.source_code='ZXZX'   --������
),
--������������������������������������������������������������������������������������������������������������ �ӿڲ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ʱ�����ƿ��� --
timeLimit_switch as 
(
    select false as flag   --TRUE:ʱ��Լ����FLASE:ʱ�䲻��Լ����ͨ�����ڳ�ʼ��
    -- select False as flag
),
-- ģ�Ͱ汾���� --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
    select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --��Ƶģ��
    union all
    select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-��Ͷģ��
    union all 
    select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-��ҵģ��
    union all 
    select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-���ֿ�ģ��(��Ƶ)
    union all 
    select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-�޼ලģ��
    union all 
    select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --���÷����ۺ�ģ��
    -- select 
    --     notes,
    --     model_name,
    --     model_version,
    --     status,
    --     etl_date
    -- from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf a
    -- where a.etl_date in (select max(etl_date) from t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf)
    --   and status='active'
    -- group by notes,model_name,model_version,status,etl_date
),
-- �����÷�(������ֿ�) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ� 
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))  
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
         ) m  where rm=1  
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
    
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))   
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
         ) m  where rm=1 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --�����÷�_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))     
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
            where 1 in (select not max(flag) from timeLimit_switch)
         ) m  where rm=1 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --�����÷�_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
         ) m  where rm=1 
    ) a join model_version_intf_ b
    on a.model_version = b.model_version and a.model_name=b.model_name 
),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_dim_risk_level_cfg_ as  -- ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ�
(
	select
        dimension,
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- �߷��� ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
feat_CFG as  --�����ֹ����ñ�
(
    select distinct
        feature_cd,
        feature_name,
        sub_model_type,  --��Ƶ-����ƽ̨����Ƶ-ҽҩ���� ...
        -- substr(sub_model_type,1,6) as sub_model_type,  --ȡǰ���������ַ�
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type not in ('��Ƶ-��ҵ','��Ƶ-��Ͷ','�޼ල')
    union all 
    select distinct
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
    where sub_model_type in ('��Ƶ-��ҵ','��Ƶ-��Ͷ','�޼ල')
),
--ӳ��� �����ֹ����ñ� --
warn_feat_CFG as
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,    --��Ƶ-����ƽ̨����Ƶ-ҽҩ���� ...
        feature_name_target,
        case dimension 
            when '����' then 1
            when '��Ӫ' then 2
            when '�г�' then 3
            when '����' then 4
            when '�쳣���ռ��' then 5
        end as dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
        -- count(feature_cd) over(partition by dimension,type) as contribution_cnt
    from feat_CFG
),
-- ӳ���� ������ҵ���ڵ� �����ֹ����ñ� --
warn_feat_corp_property_CFG as  --ͨ����Ƶ�������ݵ�sub_model_type��ȡ��Ӧ���ڵ���ҵ    ʹ�÷�Χ:���е�Ƶ�ϲ����������׶ȱ�
(
    select 
        b.source_id as corp_code,
        '��Ƶ' as big_sub_model_type,
        a.sub_model_type,
        a.feature_cd,
        a.feature_name
    from warn_feat_CFG a 
    join corp_chg b 
        on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX' 
    where substr(a.sub_model_type,1,6) = '��Ƶ'
    group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name
    -- select m.*
    -- from
    -- (
    --     select 
    --         -- b.corp_id,
    --         b.source_id as corp_code,
    --         b.etl_date,
    --         -- max(b.corp_name) as corp_nm,
    --         '��Ƶ' as big_sub_model_type,
    --         a.sub_model_type,
    --         a.feature_cd,
    --         a.feature_name,
    --         rank() over(partition by b.source_id,a.feature_cd,a.feature_name order by b.etl_date desc) as rm
    --     from warn_feat_CFG a 
    --     join corp_exposure b 
    --         on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX'
    --     where substr(a.sub_model_type,1,6) = '��Ƶ'
    -- ) m where rm=1
    -- group by b.source_id,a.sub_model_type,a.feature_cd,a.feature_name,b.etl_date   --ȥ���ظ�����
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- �����÷�(������ֿ�) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch as  --�����÷�_��Ƶ ԭʼ�ӿ� 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch as  --�����÷�_��Ƶ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch as  --�����÷�_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch as  --�����÷�_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
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
        from rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_batch --��Ƶ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_batch --��Ƶ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_batch --��Ƶ_��Ͷ-Ƶ�ֿ�
        union all
        select distinct
            end_dt as batch_dt,
            corp_code,
            end_dt,
            feature_name,
            feature_score,
            model_name,
            model_version
        from rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_batch --��Ƶ_��ҵծ-Ƶ�ֿ�
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
)
select * 
from warn_score_card
;