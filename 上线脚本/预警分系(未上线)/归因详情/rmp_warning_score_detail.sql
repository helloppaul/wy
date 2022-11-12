-- RMP_WARNING_SCORE_DETAIL (ͬ����ʽ��һ������β���) --
-- /* 2022-10-27 �޸� ��λ�������߼� ������֤���һ����ҵ���ǹ���һ������ */
-- /* 2022-10-27 �޸� ����������㣬�ǻ��ڸ�ʱ�����ҵ��type��Ĺ������ͳ�ƶ���������ҵ���ͳ�� */
-- /* 2022-10-28 �Ը��е�Ƶ�ϲ����������׶ȱ��ָ������Ϊ׼ */
-- /* 2022-11-01 �������������߼��Ż�������������������ʷ���ȡ����ָ��ֵ��ֱ�Ӵ����θ��е�Ƶ�ϲ�������ԭʼֵ��ȡ */
-- /* 2022-11-08 ����ģ�Ͱ汾���ƽӿڱ� */
-- /* 2022-11-08 ���� ������֤��ҵ���ڷ��� ��ȡ��Ӧ��ҵ��Ƶ��ģ�ͷ����ָ������ */
-- /* 2022-11-09 ά�ȷ��յȼ������޸���������Ԥ���ȼ����� �ѱ�¶�ͺ�ɫԤ�� ӳ�����Ϊ-3 */
-- /* 2022-11-10 �޸� ά�ȷ��յȼ�����ʾΪ������ߵ�����  */
-- /* 2022-11-12 ά�ȷ��յȼ��߼������Ż������� �쳣ָ��ռ��(%) ��Ϊά�ȷ��յȼ��������ݣ�����Ӧ����ά�ȷ��յȼ����ñ�  */
-- /* 2022-11-12 �޸� idx_nameȡֵ����Ϊfeature_name_target */
-- /* 2022-11-12 �޸� ����ģ�ͺ�Ŀ������ָ��������һ�µ����� */
-- /* 2022-11-12 ���� idx_value ����Ŀ�굥λת�����߼� */
-- /* 2022-11-12 �޸� contribution_cnt ͳ��ά�ȵ����⣬ͳ��ά�ȵ���Ϊtype */
-- /* 2022-11-12 �޸� ָ����λ����������� */
-- /* 2022-11-12 �޸� ��ҵ�����������ۺ�Ԥ���ȼ�ģ�ͽ������ҵ������һ�µ����� */
-- ���� ģ�� �ۺ�Ԥ���֣�����ԭʼֵ���еͣ��������׶ȸ��е��޼ල�Լ��ۺϣ����ֿ����еͣ��������鼰����ʷ PS:������pth_rmp.ģ�ͽ����
--q1��ά�ȷ��յȼ��ļ����������׶�ռ�ȣ����׶�ռ����������������ԭʼֵ����ʱ�������������ĳЩά�ȹ�������ά�ȷ��յȼ�������ΪNULL(��ʱ�����ߵ�)
--q2������ֵ�Ը��е�Ƶ�ϲ����������׶ȱ�Ϊ��׼������������ԭʼֵ�л�Ϊ���е�Ƶ�ϲ����������׶ȱ�
-- �������� 20221102 �����س������������޹�˾
set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
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
-- Ԥ���� --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ԥ����_�ںϵ������ۺ�  ԭʼ�ӿ�
( 
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(rating_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
-- ����ԭʼֵ(ȡ������������) --
rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select *
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_hfreqscard_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf_ as  --����ԭʼֵ_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_lfreqconcat_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf_ as  --����ԭʼֵ_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqcityinv_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf_ as  --����ԭʼֵ_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
        and to_date(end_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_feat_mfreqgen_val_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as  --�������׶�_�ںϵ������ۺ� ԭʼ�ӿڣ��������޼ල������creditrisk_highfreq_unsupervised  ��
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_hfreqscard_pct_intf_ as --�������׶�_��Ƶ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_hfreqscard_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_pct_intf_ as  --�������׶�_��Ƶ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_lfreqconcat_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_pct_intf_ as  --�������׶�_��Ƶ��Ͷ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqcityinv_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_intp_mfreqgen_featpct_intf_ as  --�������׶�_��Ƶ��ҵ
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_mfreqgen_fp_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
-- �����÷�(������ֿ�) --
rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ� 
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_hfreqscard_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
    
),
rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf_ as  --�����÷�_��Ƶ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_lfreqconcat_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf_ as  --�����÷�_��Ƶ_��Ͷ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqcityinv_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch)
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name 
),
rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf_ as  --�����÷�_��Ƶ_��ҵծ ԭʼ�ӿ�
(
    select a.*
    from 
    (
        -- ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
        where 1 in (select max(flag) from timeLimit_switch) 
        and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
        union all 
        -- ��ʱ�����Ʋ��� --
        select * 
        from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_mfreqgen_fsc_intf
        where 1 in (select not max(flag) from timeLimit_switch) 
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
        b.corp_id,
        b.source_id as corp_code,
        max(b.corp_name) as corp_nm,
        '��Ƶ' as big_sub_model_type,
        a.sub_model_type,
        a.feature_cd,
        a.feature_name
    from warn_feat_CFG a 
    join corp_chg b 
        on substr(a.sub_model_type,8) = b.exposure and b.source_code='ZXZX'
    where substr(a.sub_model_type,1,6) = '��Ƶ'
    group by b.corp_id,b.source_id,a.sub_model_type,a.feature_cd,a.feature_name   --ȥ���ظ�����
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- Ԥ���� --
RMP_WARNING_SCORE_MODEL_ as  --Ԥ����-ģ�ͽ���������������Σ�
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
			when a.interval_text_adjusted  in ('��ɫԤ��','�����ѱ�¶') then 
				'-3'  --�߷���
			-- when a.interval_text_adjusted  ='�����ѱ�¶' then 
			-- 	'-4'   --�����ѱ�¶
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a 
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
	-- where score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    -- from app_ehzh.RMP_WARNING_SCORE_MODEL  --@pth_rmp.RMP_WARNING_SCORE_MODEL
),
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
-- Ԥ���� --
warn_union_adj_sync_score as --ȡ�������ε�Ԥ����-ģ�ͽ����
(
    select distinct
        a.batch_dt,
        a.corp_id,
        a.corp_nm,
        a.score_date as score_dt,
        a.synth_score as adj_score,
        a.synth_warnlevel as adj_synth_level,
        a.adjust_warnlevel,
        a.model_version
    from RMP_WARNING_SCORE_MODEL_ a
),
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
    select 
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
    join warn_feature_value_two_days b   --��
        on  a.corp_id = b.corp_id 
            and date_add(a.score_dt,-1)=b.score_dt 
            and a.sub_model_name=b.sub_model_name  
            and a.idx_name=b.idx_name
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
        nvl(b.industryphy_name,'') as zjh,
        nvl(b.bond_type,0) as bond_type,  --0���ǲ�ҵ�ͳ�Ͷ 1����ҵծ 2����Ͷծ
        case 
            when nvl(b.bond_type,0)=2 then 
                ''
            when nvl(b.bond_type,0)<>2 then 
                nvl(b.industryphy_name,'')
        end as zjh_cal   --������֤���һ����ҵ����
    from warn_feature_value a 
    left join (select corp_id,corp_name,bond_type,industryphy_name from corp_chg where source_code='ZXZX') b 
        on a.corp_id=b.corp_id 
),
warn_feature_value_with_median_cal as 
(
    select 
        a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
        ,appx_median(b.idx_value) as median  --impala
        -- ,percentile_approx(b.idx_value,0.5) as median  --hive
    from warn_feature_value_with_median a 
    join warn_feature_value_with_median b 
        on a.batch_dt=b.batch_dt and a.score_dt=b.score_dt and a.zjh_cal=b.zjh_cal and a.idx_name=b.idx_name  --��ȡ �뵱ǰ�е���ҵ ͬʱ��� ͬ��ҵ ָͬ���ָ����λ��
    group by a.corp_id,a.batch_dt,a.score_dt,a.zjh_cal,a.sub_model_name,a.idx_name
    -- select 
    --     batch_dt,
    --     score_dt,
    --     corp_id,
    --     bond_type,
    --     sub_model_name,
    --     idx_name,
    --     appx_median(idx_value) as median
    --     -- percentile_approx(idx_value,0.5) as median  --hive
    -- from warn_feature_value_with_median
    -- where bond_type=2
    -- group by bond_type,corp_id,batch_dt,score_dt,sub_model_name,idx_name
    -- union all 
    -- select 
    --     batch_dt,
    --     score_dt,
    --     corp_id,
    --     bond_type,
    --     sub_model_name,
    --     idx_name,
    --     appx_median(idx_value) as median
    --     -- percentile_approx(idx_value,0.5) as median  --hive
    -- from warn_feature_value_with_median
    -- where bond_type<>2 and zjh <> ''
    -- group by  bond_type,zjh,corp_id,batch_dt,score_dt,sub_model_name,idx_name
),
warn_feature_value_with_median_res as -- used
(
    select 
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
        feature_pct*100 as contribution_ratio,
        feature_risk_interval as abnormal_flag,  --�쳣��ʶ 
        sub_model_name
    from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_batch a 
    join corp_chg chg 
        on cast(a.corp_code as string)=chg.source_id and chg.source_code='ZXZX'
),
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
),
warn_feature_contrib_res1 as  --���� ά�ȹ��׶�ռ�� ���������׶�-�ϲ����е�Ƶ  
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        -- model_freq_type,  ----����������ģ�ͷ���/ģ��Ƶ�ʷ���
        -- sum(feature_pct) as dim_submodel_contribution_ratio  --ά�ȹ��׶�ռ��
       total_idx_is_abnormal_cnt/total_idx_cnt*100 as dim_submodel_contribution_ratio   --��ά�� �쳣ָ��ռ�� (dim_submodel_contribution_ratio�ֶ�������֮ǰά�ȹ��׶�ռ��)
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

            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension,a.feature_risk_interval) as total_idx_is_abnormal_cnt,   --����ÿ����ҵÿ��ʱ���ά���µ��쳣ָ�� �Լ� ���쳣ָ��֮��  2022-11-12 ����
            count(a.feature_name) over(partition by a.corp_id,a.score_dt,a.batch_dt,f_cfg.dimension) as total_idx_cnt,          --����ÿ����ҵÿ��ʱ���ά���µ�ָ��֮�� 2022-11-12 ����
            -- a.model_name,
            a.sub_model_name
        from warn_feature_contrib a 
        join warn_feat_CFG f_cfg    --���ۺ�ֱ�Ӳ���join������������ԭʼֵû�еĲ�����չʾ
        -- left join warn_feat_CFG f_cfg 
            on a.feature_name=f_cfg.feature_cd and a.model_freq_type=f_cfg.sub_model_type --and a.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    )B where feature_risk_interval = 1 --�쳣ָ��
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
                main.dim_submodel_contribution_ratio,   --����ģ�Ͷ�Ӧά�ȹ��׶�ռ�ȣ�used by ���򱨸�ڶ���
                b.risk_lv,
                b.risk_lv_desc   -- ԭʼ���յȼ�����
            from warn_feature_contrib_res1 main 
            join warn_dim_risk_level_cfg_ b 
                on main.dimension=b.dimension   --2022-11-12 ά�ȷ��յȼ����ñ� ����dimension�ֶ�����ֵ����
            where main.dim_submodel_contribution_ratio>b.low_contribution_percent and main.dim_submodel_contribution_ratio<=b.high_contribution_percent
        )C 
    )D
),
warn_feature_contrib_res3_tmp as 
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.dimension,
        main.dim_risk_lv,
        main.dim_risk_lv_desc,  --ά�ȷ��յȼ� �߷��գ��з��գ��ͷ���
        nvl(b.adj_synth_level,'') as adj_synth_level,  --�ۺ�Ԥ���ȼ�
        nvl(b.adjust_warnlevel,'') as adjust_warnlevel --������ȼ�
    from warn_feature_contrib_res2 main
    left join warn_union_adj_sync_score b --Ԥ����-ģ�ͽ����
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id
),
warn_feature_contrib_res3 as  -- �����ۺ�Ԥ���ȼ��������ά�ȷ���ˮƽ ���������׶�-�ϲ����е�Ƶ
(
    select 
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        dimension,
        dim_warn_level  --���յ������ά�ȷ��յȼ�
    from 
    (
        select 
            a.*,
            case 
                when cast(a.dim_risk_lv as string)<>a.adjust_warnlevel then 
                    a.adjust_warnlevel
                else 
                    cast(a.dim_risk_lv as string)
            end as dim_warn_level  --�����ۺ�Ԥ���ȼ��������ά�ȷ���ˮƽ
        from warn_feature_contrib_res3_tmp a 
        where a.dim_risk_lv in (select min(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp)  --������ߵ�
        -- join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --��ȡ��߷���ˮƽ��Ӧ��ά��
        --     on a.dim_risk_lv=b.max_dim_risk_lv
        union all 
        select 
            a.*,
            cast(a.dim_risk_lv as string) as dim_warn_level
        from warn_feature_contrib_res3_tmp a 
        where a.dim_risk_lv not in (select min(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp)  --�Ƿ�����ߵ�
        -- join (select max(dim_risk_lv) as max_dim_risk_lv from warn_feature_contrib_res3_tmp) b  --��ȡ����߷���ˮƽ��Ӧ��ά��
        -- where a.dim_risk_lv <> b.max_dim_risk_lv
    )C group by batch_dt,corp_id,corp_nm,score_dt,dimension,dim_warn_level  --ȥ��
),
warn_contribution_ratio_with_factor_evl as  --���������۵��������׶�Ӧ�ò�����(�������޼ල)
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
    from (select * from warn_contribution_ratio where feature_name <> 'creditrisk_highfreq_unsupervised') a 
    left join (select * from warn_feature_value where idx_value is not null) b 
        on  a.corp_id=b.corp_id 
            and a.batch_dt=b.batch_dt 
            and a.sub_model_name=b.sub_model_name 
            and a.feature_name=b.idx_name
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
),
-- ����� --
res0 as   --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)  ��:1min  67����
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        c.feature_name as idx_name,
        b.idx_value,   --����ָ��ֵ  ps:��Ϊ�գ�ֱ�ӱ���NULL�������������ԭʼֵ��Ĭ��ֵ
        b.lst_idx_value as last_idx_value,  --����ָ��ֵ
        '' as idx_unit,   --�����������ñ�������
        c.model_freq_type,   --���ø��е�Ƶ�ϲ��������׶ȵ� �����ֹ�ά�����������Ƶ�ģ�ͷ��� 2022-11-12
        c.sub_model_name,   --���ø��е�Ƶ�ϲ��������׶ȵ� ����ģ���Դ�����ģ��Ӣ������ 2022-11-12
        b.median  
    from warn_feature_contrib c   --��Ƶ�ϲ����������׶�  
    join  warn_union_adj_sync_score main --Ԥ����
        on main.batch_dt=c.batch_dt and main.corp_id=c.corp_id
    left join warn_feature_value_with_median_res b  --��Ƶ�ϲ�������ԭʼֵ
        on c.corp_id=b.corp_id and c.batch_dt=b.batch_dt and c.feature_name=b.idx_name
),
res1 as   --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺ��������׶�(�޼ල) 
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        b.contribution_ratio,  --���׶�ռ��
        b.factor_evaluate,  --��������
        b.sub_model_name as sub_model_name_zhgxd   --�ۺϹ��׶ȵ���ģ������
    from res0 main
    left join warn_contribution_ratio_with_factor_evl b  
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.feature_name
    union all 
    --�������׶ȵ��޼ල��ģ�� ���⴦��  ��ֻ�й��׶�ռ�����ݣ������Ϊ�գ������������Ӳ��棬ͣ����dimension�㣩
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        feature_name as idx_name,
        NULL as idx_value,
        NULL as last_idx_value,
        '' as idx_unit,
        '�޼ල' model_freq_type,
        sub_model_name,
        NULL as median,
        contribution_ratio,
        NULL as factor_evaluate, 
        '' as sub_model_name_zhgxd 
    from ( select  a1.* FROM warn_contribution_ratio a1
            where a1.feature_name = 'creditrisk_highfreq_unsupervised'
        --    where a1.batch_dt in (select max(batch_dt) as max_batch_dt from warn_contribution_ratio_with_factor_evl)
                -- on a1.batch_dt and a2.batch_dt   --a1���batch_dt��a2���豣��һ��
            -- and a1.feature_name = 'creditrisk_highfreq_unsupervised'
        ) A 
),
res2 as --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺϹ��׶�+ָ�����ֿ� ��:1min20s  67����
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
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
        on  main.corp_id=b.corp_id 
            and main.batch_dt=b.batch_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.idx_name
),
res3 as   --Ԥ����+����ԭʼֵ+�ۺϹ��׶�+ָ�����ֿ�+�������ñ�  ��:1min20s  40����
(
    select  
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
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
        f_cfg.dimension,
        f_cfg.type,
        f_cfg.cal_explain as idx_cal_explain,
        f_cfg.feature_explain as idx_explain,
        f_cfg.unit_origin,
        f_cfg.unit_target,
        count(*) over(partition by main.batch_dt,main.corp_id,main.score_dt,f_cfg.dimension,f_cfg.type) as  contribution_cnt  --����������㣬���ڸ�ʱ����ҵ��Ӧtype���ָ�����ͳ��
        -- f_cfg.contribution_cnt  --�������
    from res2 main
    join warn_feat_CFG f_cfg
        on main.idx_name=f_cfg.feature_cd and main.model_freq_type=f_cfg.sub_model_type --and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    -- left join warn_feat_CFG f_cfg
),
res4 as -- --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺϹ��׶�+ָ�����ֿ�+�������ñ�+��ά�ȷ���ˮƽ(���е�Ƶ���׶����)   ��:1min20s  34����
(
    select distinct
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.feature_name_target as idx_name,  --���һ����idx_name����Ϊ����ҳ��չʾ��ʽ��ָ������
        case 
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                main.idx_value/100000000
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                main.idx_value/10000
            when main.unit_origin='��ֵ' and main.unit_target='%' then 
                main.idx_value*100
            when main.unit_origin='��' and main.unit_target='����' then 
                main.idx_value/10000
            else 
                main.idx_value
        end as idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,  --�ۺϹ��׶ȵ���ģ������
        main.idx_score,
        main.sub_model_name_zbpfk,
        main.sub_model_type,
        main.feature_name_target,
        main.dimension,
        b.dim_warn_level,  --���յ������ά�ȷ��յȼ�(���ѵ�)
        main.type,
        main.idx_cal_explain,
        main.idx_explain,
        main.last_idx_value,
        main.unit_origin,
        main.unit_target,
        main.contribution_cnt  --�������
    from res3 main
    left join warn_feature_contrib_res3 b  --��ȡά�ȷ��յȼ����ݣ�left join ���ⶪʧ�޼ල����
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id and main.dimension=b.dimension
)
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
insert into pth_rmp.RMP_WARNING_SCORE_DETAIL partition(etl_date=${ETL_DATE})
select 
    concat(corp_id,'_',MD5(concat(batch_dt,dimension,type,sub_model_name,idx_name))) as sid_kw,  --hive
    batch_dt,
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    dim_warn_level,  
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --������ָ��ֵ������Ҫת��ΪĿ�����չʾ��̬�������ñ�ĵ�λ���йأ���ʱ���ԭʼֵ
    idx_unit,  
    idx_score,   
    cast(contribution_ratio as float) as contribution_ratio,   --���׶�ռ�� ��ת��Ϊ �ٷֱ�
    contribution_cnt,  
    factor_evaluate,
    median,  --������ ������
    last_idx_value,  --������
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from res4
where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
; 

