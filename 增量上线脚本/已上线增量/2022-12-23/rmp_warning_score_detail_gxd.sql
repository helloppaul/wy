-- RMP_WARNING_SCORE_DETAIL_GXD (ͬ����ʽ��һ������θ��Ǹ���) --
-- /* 2022-12-06 �޸� hive��ִ��warn_feat_corp_property_CFG���ؿ����ݵ����⣬hive���������ַ�����ʶ���Impala��׼��ͬ */

-- part1 ���е�Ƶ�ϲ��� �������׶� --
set hive.exec.parallel=true;
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_feature_contrib;
create table pth_rmp.rmp_warn_feature_contrib as 
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
    select True as flag   --TRUE:ʱ��Լ����FLASE:ʱ�䲻��Լ����ͨ�����ڳ�ʼ��
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
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿڣ��������޼ල������creditrisk_highfreq_unsupervised  ��
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --�������׶�_��Ƶ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --�������׶�_��Ƶ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --�������׶�_��Ƶ��Ͷ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1              
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --�������׶�_��Ƶ��ҵ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
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
        on substr(a.sub_model_type,instr(a.sub_model_type,'-')+1) = b.exposure
        -- on substr(a.sub_model_type,8) = b.exposure --and b.source_code='ZXZX' 
    where instr(a.sub_model_type,'��Ƶ')>0
    -- where substr(a.sub_model_type,1,6) = '��Ƶ'
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
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿڣ��������޼ල������creditrisk_highfreq_unsupervised  ��
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch as --�������׶�_��Ƶ
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch as  --�������׶�_��Ƶ
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch as  --�������׶�_��Ƶ��Ͷ
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch as  --�������׶�_��Ƶ��ҵ
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_feature_contrib as --�������׶�-�ϲ����е�Ƶ
(
	select 
		cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
		chg.corp_id,
		chg.corp_name as corp_nm,
		to_date(end_dt) as score_dt,
		feature_name,
		feature_pct,   --�Ѿ�*100
        model_freq_type,  --����������ģ�ͷ���/ģ��Ƶ�ʷ���
		feature_risk_interval,  --�����쳣��ʶ
		model_name as sub_model_name,
		model_version
	from 
	(
		--��Ƶ
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --�������׶�
			'��Ƶ' as model_freq_type,
			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_batch
		union all 
		--��Ƶ
		select distinct
			k1.end_dt as batch_dt,
			k1.corp_code,
			k1.end_dt,
			k1.feature_name,
			cast(k1.feature_pct*100 as float) as feature_pct,  --�������׶�
            k2.sub_model_type as model_freq_type,  --��������������ڷ���ĵ�Ƶ���࣬���� ��Ƶ-����ƽ̨����Ƶ-���ز� ..
			-- '��Ƶ' as model_freq_type,
			k1.feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
			k1.model_name,
			k1.model_version
		from rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_batch k1 
        join warn_feat_corp_property_CFG k2 
            on cast(k1.corp_code as string) = k2.corp_code
		union all 
		--��Ƶ-��Ͷ
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --�������׶�
			'��Ƶ-��Ͷ' as model_freq_type,
			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_batch 
		union all 
		--��Ƶ-��ҵ
		select distinct
			end_dt as batch_dt,
			corp_code,
			end_dt,
			feature_name,
			cast(feature_pct*100 as float) as feature_pct,  --�������׶�
			'��Ƶ-��ҵ' as model_freq_type,
			feature_risk_interval,  --�����쳣��ʶ��0/1,1�����쳣��
			model_name,
			model_version
		from rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_batch
	)A join corp_chg chg 
            on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
)
select * from warn_feature_contrib
;



-- part2 �ۺ�Ԥ�� �������׶� --
set hive.exec.parallel=true;
set hive.auto.convert.join = true;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_contribution_ratio;
create table pth_rmp.rmp_warn_contribution_ratio as 
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
    select True as flag   --TRUE:ʱ��Լ����FLASE:ʱ�䲻��Լ����ͨ�����ڳ�ʼ��
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
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿڣ��������޼ල������creditrisk_highfreq_unsupervised  ��
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿڣ��������޼ල������creditrisk_highfreq_unsupervised  ��
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_contribution_ratio as 
(
    select distinct
        cast(a.end_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
        to_date(a.end_dt) as score_dt,
        feature_name,
        feature_pct*100 as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --�쳣��ʶ 
        sub_model_name
    from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
)
select * from warn_contribution_ratio
;



-- part3 --
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warn_feature_contrib_res3;
create table pth_rmp.rmp_warn_feature_contrib_res3 as 
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
--������������������������������������������������������������������������������������������������������������ �ӹ���ӿ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_feature_contrib as 
(
    select * 
    from pth_rmp.rmp_warn_feature_contrib
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_feature_contrib_res1 as  --���� ά���쳣ָ��ռ�� ���������׶�-�ϲ����е�Ƶ  
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        -- feature_risk_interval,
        -- model_freq_type,  ----����������ģ�ͷ���/ģ��Ƶ�ʷ���
        -- sum(feature_pct) as dim_submodel_contribution_ratio  --ά�ȹ��׶�ռ��
        nvl(total_idx_is_abnormal_cnt/total_idx_cnt*100,0) as dim_submodel_contribution_ratio
    --    nvl(total_idx_is_abnormal_cnt/total_idx_cnt*100,0) as dim_submodel_contribution_ratio   --��ά�� �쳣ָ��ռ�� (dim_submodel_contribution_ratio�ֶ�������֮ǰά�ȹ��׶�ռ��)
    from
    (
        select 
            a.batch_dt,
            a.corp_id,
            a.corp_nm,
            a.score_dt,
            f_cfg.dimension,
            a.feature_name,
            a.feature_pct,  --���׶�ռ�� %
            -- a.model_freq_type,
            a.feature_risk_interval,   --�쳣ָ��
            sum(feature_risk_interval) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension ) as total_idx_is_abnormal_cnt,

            -- count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension,a.feature_risk_interval) as total_idx_is_abnormal_cnt,   --����ÿ����ҵÿ��ʱ���ά���µ��쳣ָ�� �Լ� ���쳣ָ��֮��  2022-11-12 ����
            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension) as total_idx_cnt,          --����ÿ����ҵÿ��ʱ���ά���µ�ָ��֮�� 2022-11-12 ����
            -- a.model_name,
            a.sub_model_name
        from warn_feature_contrib a 
        join warn_feat_CFG f_cfg    --���ۺ�ֱ�Ӳ���join������������ԭʼֵû�еĲ�����չʾ
        -- left join warn_feat_CFG f_cfg 
            on a.feature_name=f_cfg.feature_cd and a.model_freq_type=f_cfg.sub_model_type --and a.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    )B --where feature_risk_interval = 1 --�쳣ָ��
    group by batch_dt,corp_id,corp_nm,score_dt,dimension,total_idx_is_abnormal_cnt,total_idx_cnt   --����ȥ��    --,model_freq_type
),
warn_feature_contrib_res2 as  -- ���� ά�ȷ��յȼ� ���������׶�-�ϲ����е�Ƶ
(
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_submodel_contribution_ratio,  --��ά�� �쳣ָ��ռ��
        dim_risk_lv,
        dim_risk_lv_desc  --����ǰά�ȷ��յȼ� used
    from
    ( 
        select 
            *,
            first_value(risk_lv) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv,  --����ǰά�ȷ��յȼ�����ֵ�ͣ�
            first_value(risk_lv_desc) over(partition by batch_dt,corp_id,corp_nm,score_dt,dimension order by risk_lv asc) as dim_risk_lv_desc  --����ǰά�ȷ��յȼ�
        from 
        (
            select distinct
                main.batch_dt,
                main.corp_id,
                main.corp_nm,
                main.score_dt,
                main.dimension,
                -- main.model_freq_type,
                main.dim_submodel_contribution_ratio,   ----��ά�� �쳣ָ��ռ�� used by ���򱨸�ڶ���
                b.risk_lv,
                b.risk_lv_desc   -- ԭʼ���յȼ�����
            from warn_feature_contrib_res1 main 
            join warn_dim_risk_level_cfg_ b 
                on main.dimension=b.dimension   --2022-11-12 ά�ȷ��յȼ����ñ� ����dimension�ֶ�����ֵ����
            where main.dim_submodel_contribution_ratio>b.low_contribution_percent and main.dim_submodel_contribution_ratio<=b.high_contribution_percent
        )C 
    )D
),
warn_feature_contrib_res3 as
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.dimension,
        main.dim_submodel_contribution_ratio,
        main.dim_risk_lv,
        main.dim_risk_lv_desc,  --ά�ȷ��յȼ� �߷��գ��з��գ��ͷ���
        cast(main.dim_risk_lv as string) as dim_warn_level
        -- nvl(b.adj_synth_level,'') as adj_synth_level,  --�ۺ�Ԥ���ȼ�
        -- nvl(b.adjust_warnlevel,'') as adjust_warnlevel --������ȼ�
    from warn_feature_contrib_res2 main
    -- left join warn_union_adj_sync_score b --Ԥ����-ģ�ͽ����
    --     on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id
)
select * from warn_feature_contrib_res3
;