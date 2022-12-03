-- 2022-11-24 �ܺ� --
-- ����ԭʼֵ-��ʼ�� -- 
set mem_limit=12000000000;

-- ��һ���� 3min(����)--
create table pth_rmp.warn_feature_value as 
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
-- ����ԭʼֵ(ȡ������������) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
            -- group by to_date(end_dt) 
        union all 
        -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --����ԭʼֵ_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --����ԭʼֵ_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select m.* 
        from 
        (
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
            where 1 in (select max(flag) from timeLimit_switch) 
            and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
            and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
            -- ��ʱ�����Ʋ��� --
            select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
            from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
            where 1 in (select not max(flag) from timeLimit_switch) 
        ) m  where rm=1   
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ����ԭʼֵ(ȡ������������) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch as  --����ԭʼֵ_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch as  --����ԭʼֵ_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ a 
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ����ԭʼֵ --
warn_feature_value_two_days as --ԭʼ����ֵ_�ϲ����е�Ƶ(���������������ݣ�����ʽ)
(
    SELECT
        cast(max(a.batch_dt) over(partition by chg.corp_id,to_date(a.end_dt)) as string) as batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
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
        from rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_batch 
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
        from rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_batch  
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
        from rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_batch 
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
        from rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_batch 
    )A join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
warn_feature_value as --ԭʼ����ֵ_�ϲ����е�Ƶ(���������������ݣ�����ʽ used)
(
    select STRAIGHT_JOIN
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_dt,
        a.idx_name,
        a.idx_value,
        b.score_dt as lst_score_dt,  --��������
        b.idx_value as lst_idx_value,  --����ָ��ֵ
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name
    from warn_feature_value_two_days a   --��
    left join [SHUFFLE] warn_feature_value_two_days b   --��
        on  a.corp_id = b.corp_id 
            and date_add(a.score_dt,-1)=b.score_dt 
            and a.sub_model_name=b.sub_model_name  
            and a.idx_name=b.idx_name
)
select * from warn_feature_value
;


-- �ڶ����� 3min impala (����)--
set mem_limit=11000000000;
create table pth_rmp.warn_feature_value_with_median as 
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
warn_feature_value_with_median as --ԭʼ����ֵ_�ϲ����е�Ƶ+��λ������
(
    select distinct STRAIGHT_JOIN
        a.batch_dt,
        a.score_dt,
        a.corp_id,
        a.corp_nm,
        a.idx_name,
        a.idx_value,
        a.idx_unit,
        a.model_freq_type,
        a.sub_model_name,
        nvl(b.industryphy_name,'') as zjh,
        nvl(b.bond_type,0) as bond_type,  --0���ǲ�ҵ�ͳ�Ͷ 1����ҵծ 2����Ͷծ
        case 
            when nvl(b.bond_type,0)=2 then 
                '��Ͷ'
            when nvl(b.bond_type,0)<>2 then 
                nvl(b.industryphy_name,'')
        end as zjh_cal   --������֤���һ����ҵ����
    from warn_feature_value a 
    left join [BROADCAST] (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='ZXZX') b 
        on a.corp_id=b.corp_id 
)
select * from warn_feature_value_with_median
;


-- �������� impala�� 4min(һ����)--
create table pth_rmp.warn_feature_value_with_median_res as 
with
hy_median as --��ҵ��λ��
(
    select score_dt,zjh_cal,sub_model_name,idx_name,appx_median(idx_value) as median
    from warn_feature_value_with_median
    group by score_dt,zjh_cal,sub_model_name,idx_name
), 
warn_feature_value_with_median_cal as 
(
    select a.batch_dt,a.corp_id,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name,b.median
    from warn_feature_value_with_median a 
    join hy_median b 
        on a.score_dt=b.score_dt and a.zjh_cal=b.zjh_cal and a.sub_model_name=b.sub_model_name and a.idx_name=b.idx_name
),
-- warn_feature_value_with_median_cal as 
-- (
--     select STRAIGHT_JOIN 
--         a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
--         ,appx_median(b.idx_value) as median  --impala
--         -- ,percentile_approx(b.idx_value,0.5) as median  --hive
--     from warn_feature_value_with_median a 
--     join [SHUFFLE] warn_feature_value_with_median b 
--         on a.score_dt=b.score_dt and a.zjh_cal=b.zjh_cal and a.idx_name=b.idx_name  --��ȡ �뵱ǰ�е���ҵ ͬʱ��� ͬ��ҵ ָͬ���ָ����λ��
--     group by a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
-- ),
warn_feature_value_with_median_res as -- used
(
    select STRAIGHT_JOIN
        b.batch_dt,  --�Ը�Ƶ���µ�����Ϊ����ʱ��
        b.corp_id,
        b.corp_nm,
        b.score_dt,
        b.idx_name,
        b.idx_value,
        b.lst_idx_value,
        b.idx_unit,
        b.model_freq_type,  --����������ģ�ͷ���/ģ��Ƶ�ʷ���
        b.sub_model_name,
        cal.median
    from warn_feature_value b 
    left join [SHUFFLE] warn_feature_value_with_median_cal cal
    -- from warn_feature_value_with_median_cal cal 
    -- join [SHUFFLE] warn_feature_value b 
        on cal.corp_id=b.corp_id and cal.score_dt=b.score_dt and cal.sub_model_name=b.sub_model_name and cal.idx_name=b.idx_name 
)
select * 
from warn_feature_value_with_median_res;