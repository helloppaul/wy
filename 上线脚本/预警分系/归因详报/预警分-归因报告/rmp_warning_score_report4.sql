-- RMP_WARNING_SCORE_REPORT ���Ķ�-����䶯 --
-- /*2022-11-13 ����������ʷ�ӿڲ�������ù������鵱�ձ�����ݣ��������鵱�ձ�ȷ������������������� */
-- /* 2022-12-04 ��ҹ���ȡֵ�޸���ȡ����create_dt������ */
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */
-- /* 2023-01-01 model_version_intf_ ��ȡ����ͼ���� */
-- /* 2023-01-03 warn_adj_rule_cfg ģ����ҹ���create_dt<= ��Ϊ = ͬʱ�����ݰ���corp_id�����������*/
-- /* 2023-01-03 ���� �񻯵������ѱ�¶�ȼ�������ҲҪ������߼� */
-- /* 2023-01-04 ����������ʷ��Ԥ���ȼ��䶯�����ӿڱ�ȡ���update_time,��ֹ׷���������ظ����ݵ�Ӱ�� */
-- /* 2023-01-05 �޸� �����ѱ�¶�����������ѱ�¶������ */
-- /* 2023-01-05 �޸� ͬһ����ҵ���ֶ������Ķ���Ϣ���������⣬ԭ��û�ж���ҵ����ά����Ϣ�ۺϵ�����ҵ�� (Fourth_msg_corp_II������ڲ��Ӳ�ѯ) */
-- /* 2023-01-05 �޸� ȱ�� ��ά�ȷ�������ά���쳣ռ��Ҳ������ʱ�ĵ��Ķ����� (Fourth_msg_corp_I��case when�����жϵĴ���) */
-- /* 2023-01-06 �޸� Fourth_msg_corp_II����msg_corp_�ֶ�Ϊ''�ǣ�����һ�����ŵ����� */



--�ۺ�Ԥ���ȼ��䶯�㣺�ۺ�Ԥ���ȼ��䶯��   ���ӱ䶯�����ݣ��������鵱��(����)+����������ʷ��+Ԥ����ģ�ͽ������(�ۺ�Ԥ���ȼ��ֶ���Դ)
--��1����ָ���жϴ��� ��2��ά�ȺͶ�ָ��û�й��Ϲ�  ��3������ˮƽ��������Ҫά�ȣ���Ҫ��������쳣ռ�ȶԱȣ��������ߵĲ�չʾ

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=12;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;

-- drop table if exists pth_rmp.rmp_warning_score_report4;  
-- create table pth_rmp.rmp_warning_score_report4 as  --@pth_rmp.rmp_warning_score_report4
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
	where a.delete_flag=0 and b.delete_flag=0 and a.source_code='ZXZX'
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
	select * from pth_rmp.v_model_version  --�� Ԥ����-���ñ��е���ͼ
    -- select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --��Ƶģ��
    -- union all
    -- select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-��Ͷģ��
    -- union all 
    -- select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-��ҵģ��
    -- union all 
    -- select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --��Ƶ-���ֿ�ģ��(��Ƶ)
    -- union all 
    -- select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --��Ƶ-�޼ලģ��
    -- union all 
    -- select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --���÷����ۺ�ģ��
),
-- �������� --
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	-- ʱ�����Ʋ��� --
	select * 
	from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- ��ʱ�����Ʋ��� --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --Ԥ����--����������ʷ(ȡ������������������ᱣ֤����������������) ԭʼ�ӿ�
(
	-- ʱ�����Ʋ��� --
	select * 
	from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
	union all 
	-- ��ʱ�����Ʋ��� --
    select * 
    from pth_rmp.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0

	-- -- ʱ�����Ʋ��� --
	-- select * 
	-- from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	-- where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
    --   and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))
	-- union all 
	-- -- ��ʱ�����Ʋ��� --
    -- select * 
    -- from pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
    -- where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- Ԥ���ȼ��䶯 --
RMP_WARNING_SCORE_CHG_ as 
(
	-- ʱ�����Ʋ��� --
	select  batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt,update_time
	from pth_rmp.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_date) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- ��ʱ�����Ʋ��� --
    select batch_dt,corp_id,corp_nm,credit_cd,score_date,synth_warnlevel,chg_direction,synth_warnlevel_l,model_version,score_date as score_dt,update_time
    from pth_rmp.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --�������׶�_�ۺ�Ԥ���ȼ�(�������Ƶ�����������)
(
	select a.*
    from 
    (
		select m.*
		from
		(
			-- ʱ�����Ʋ��� --
			select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
			where 1 in (select max(flag) from timeLimit_switch) 
			and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
			union all 
			-- ��ʱ�����Ʋ��� --
			select *,rank() over(partition by to_date(end_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
			where 1 in (select not max(flag) from timeLimit_switch) 
		)m where rm=1
	) a join model_version_intf_ b
		on a.model_version = b.model_version and a.model_name=b.model_name
),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_level_ratio_cfg_ as -- �ۺ�Ԥ���ȼ��ȼ����ֵ�λ-���ñ�
(
	select 
		property_cd,  --1:��ҵ  2:��Ͷ
		property,  -- '��Ͷ' , '��ҵ'
		warn_lv,   -- '-5','-4','-3','-2','-1'
		percent_desc,  -- ǰ1% ǰ1%-10% ...
		warn_lv_desc   -- ��ɫԤ���ȼ�  ...
	from pth_rmp.rmp_warn_level_ratio_cfg
),
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
-- ģ����ҹ��� --
warn_adj_rule_cfg as --Ԥ����-ģ����ҹ������ñ�   ȡ����etl_date������ (����Ƶ��:�նȸ���)
(
	select distinct m.*
	from 
	(
		select 
			a.etl_date,
			b.corp_id, 
			b.corp_name as corp_nm,
			a.category,
			a.reason,
			rank() over(partition by b.corp_id order by a.create_dt desc ,a.etl_date desc,a.reason desc) rm
		from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
		join corp_chg b 
			on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
		where a.operator = '�Զ�-�����ѱ�¶����'
		  and to_date(a.create_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	)m where rm=1 
	  --and ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
feat_CFG as  --�����ֹ����ñ�
(
    select distinct
        feature_cd,
        feature_name,
        substr(sub_model_type,1,6) as sub_model_type,  --ȡǰ���������ַ�
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
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- �������������� -- 
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --ȡÿ���������� �ۺ�Ԥ��-�������׶�(�������ƽ���������Χ������Ĳ�������)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_DETAIL_Batch as -- ȡÿ�������������ݣ�����������������Χ���ƣ�
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt,max(update_time) as max_update_time from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
RMP_WARNING_SCORE_DETAIL_HIS_Batch as --ȡ��ʷ�������� �������(ȡ�Թ������鵱�ձ�������Ҫ������δ���)
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_HIS_ a
	join (select max(batch_dt) as max_batch_dt,score_dt,max(update_time) as max_update_time from RMP_WARNING_SCORE_DETAIL_HIS_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt and a.update_time=b.max_update_time
	where a.ori_idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)

),
mid_RMP_WARNING_SCORE_DETAIL_HIS as 
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_Batch main
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string) and main.dimension=cfg.dimension
),
-- �ۺ�Ԥ���ȼ��������� --
RMP_WARNING_SCORE_CHG_Batch as  --ȡÿ���������ε�Ԥ���䶯�ȼ�����
(
	select a.*
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date,max(update_time) as max_update_time from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date and a.update_time=b.max_update_time
),
--���� �������� --
Basic_data as 	-- �ۺ�Ԥ���ȼ��䶯+���չ�������+���չ������� ��ָ�����ϸ���ȣ�
(
	select distinct
		b.batch_dt,
		a.corp_id,
		a.corp_nm,
		b.score_dt,
		a.synth_warnlevel, --����Ԥ���ȼ�
		a.chg_direction,  --Ԥ���ȼ��䶯���� 1:����/�� 2:�½�/����
		a.synth_warnlevel_l as last_synth_warnlevel,--����Ԥ���ȼ�
		b.dimension,
		b.dim_warn_level,
		c.dim_warn_level as last_dim_warn_level,
		b.type,
		b.sub_model_name,
		b.idx_name,
		b.idx_value,
		b.last_idx_value,
		b.idx_unit,
		b.idx_score,
		c.idx_score as last_idx_score,
		-- b.contribution_ratio,
		b.factor_evaluate,
		b.dim_submodel_contribution_ratio,   --�쳣ָ�깱�׶�ռ��
		c.dim_submodel_contribution_ratio as last_dim_submodel_contribution_ratio  --�����쳣ָ�깱�׶�ռ��
	from RMP_WARNING_SCORE_CHG_Batch a  --Ԥ���ȼ��䶯��
	join RMP_WARNING_SCORE_DETAIL_Batch b  --�����������
		on a.corp_id = b.corp_id and a.score_date=b.score_dt 
	join RMP_WARNING_SCORE_DETAIL_HIS_Batch c  --������������
		on 	b.corp_id=c.corp_id 
			and to_date(date_add(b.score_dt,-1))= c.score_dt 
			and b.dimension=c.dimension 
			and b.type=c.type 
			and b.sub_model_name=c.sub_model_name
			and b.ori_idx_name=c.ori_idx_name
	where a.chg_direction='1'    --�ۺ�Ԥ���ȼ��뷢���񻯱���Ҫ�Ļ�����չʾ���ĶΣ��������β�չʾ
),
Basic_data_I as  -- ���� �Ƿ�ά�ȶ� + �Ƿ�ά���쳣ָ��ռ�ȶ� + �Ƿ�ָ��� ����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		last_synth_warnlevel,
		chg_direction, 

		dimension,
		case dimension 
			when 1 then '����' 
			when 2 then '��Ӫ'
			when 3 then '�г�'
			when 4 then '����'
			when 5 then '�쳣���ռ��'
		end as dimension_ch,
		
		dim_warn_level,
		last_dim_warn_level,
		
		case 
			when cast(dim_warn_level as int) < cast(last_dim_warn_level as int) then   --ά�ȷ�����
				1 
			else 
				0
		end as dim_warn_level_worsen_flag,  --�Ƿ�ά�ȶ� 

		dim_submodel_contribution_ratio,
		last_dim_submodel_contribution_ratio,
		case 
			when dim_submodel_contribution_ratio>last_dim_submodel_contribution_ratio then 
				1 
			else 
				0
		end as dim_submodel_contribution_ratio_worsen_flag, --�Ƿ�ά���쳣ָ��ռ�ȶ�
		
		type,

		idx_score,
		last_idx_score, 
		case 
			when idx_score>last_idx_score then 
				1 
			else 
				0
		end as idx_score_worsen_flag ,	--�Ƿ��ָ��
		idx_name,
		idx_value,
		case 
			when idx_unit='%' then 
				cast(cast(round(idx_value,2) as decimal(10,2)) as string) 
			when idx_unit in ('Ԫ','��Ԫ','��Ԫ','��','����','��') then 
				cast(cast(round(idx_value,2) as decimal(10,2)) as string) 
			else 	
				cast(idx_value as string)
		end as idx_value_str,
		last_idx_value,
		case 
			when idx_unit='%' then 
				cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
			when idx_unit in ('Ԫ','��Ԫ','��Ԫ','��','����','��') then 
				cast(cast(round(last_idx_value,2) as decimal(10,2)) as string) 
			else 	
				cast(last_idx_value as string)
		end as last_idx_value_str,
		idx_unit
	from Basic_data 
	
),
Basic_data_II as 
(
	select 
		a.*,
		cfg_syn.warn_lv_desc as synth_warnlevel_desc,
		cfg_syn_l.warn_lv_desc as last_synth_warnlevel_desc,
		cfg.risk_lv_desc as dim_warn_level_desc,
		cfg_l.risk_lv_desc as last_dim_warn_level_desc,
		count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension,a.idx_score_worsen_flag) as worsen_dim_idx_cnt, --��ָ������
		count(a.idx_name) over(partition by a.corp_id,a.score_dt,a.dimension) as dim_idx_cnt, --ά��ָ������
		concat(a.idx_name,'��',a.last_idx_value_str,a.idx_unit,'�仯��',a.idx_value_str,a.idx_unit) as worsen_idx_desc  --�񻯵�ָ������
	from Basic_data_I a
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_syn
		on cast(a.synth_warnlevel as string)=cfg_syn.warn_lv 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_syn_l
		on cast(a.last_synth_warnlevel as string)=cfg_syn_l.warn_lv  
	join warn_dim_risk_level_cfg_ cfg 
		on a.dim_warn_level=cast(cfg.risk_lv as string) and a.dimension=cfg.dimension
	join warn_dim_risk_level_cfg_ cfg_l
		on a.last_dim_warn_level=cast(cfg_l.risk_lv as string) and a.dimension=cfg_l.dimension
	where a.synth_warnlevel='-5' or a.dim_warn_level_worsen_flag=1 or a.dim_submodel_contribution_ratio_worsen_flag=1   --PS: �ۺ�Ԥ���ȼ��񻯵������ѱ�¶ ���� ά���뷢���� ���� ά���쳣ָ�귢����, ��չʾ���ĶΣ��������β�չʾ
),
-- ���Ķ� type�����ݻ��� --
Fourth_msg_type as 
(
	select 
		batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type,
		concat_ws('��',collect_set(worsen_idx_desc)) as worsen_idx_desc_in_one_type  --hive
		-- group_concat(distinct worsen_idx_desc,'��') as worsen_idx_desc_in_one_type 
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension_ch,
			worsen_dim_idx_cnt,
			dim_idx_cnt,
			type,
			worsen_idx_desc
		from 
		(
			select
				batch_dt,
				corp_id,
				corp_nm,
				score_dt,
				dimension_ch,
				worsen_dim_idx_cnt,
				dim_idx_cnt,
				type,
				worsen_idx_desc,
				row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by 1) as rm
			from Basic_data_II
			where idx_score_worsen_flag = 1  
		)A where rm<=5  --ȡ���׶�����ǰ5��Ķ�ָ����Ϊչʾ
	)B group by batch_dt,corp_id,corp_nm,score_dt,dimension_ch,worsen_dim_idx_cnt,dim_idx_cnt,type
),
Fourth_msg_dim as 
(
	select 
		*,
		case 
			when worsen_dim_idx_cnt>0 then 
				concat('��',dimension_ch,'ά����','��',cast(worsen_dim_idx_cnt as string),'��ָ�귢����','��','�������Ϊ',worsen_idx_desc_in_one_type)
			else 
				'��'
		end dim_msg  --xxxά������y��ָ�귢����
	from Fourth_msg_type
),
-- ���Ķ� ��ҵ�����ݻ��� --
Fourth_msg_corp_I as --�϶��� �ۺ�Ԥ���ȼ��񻯵�-5 ���� ά�ȷ����仯 ���� ��ά���쳣ռ�� ��������������
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel_desc,
		a.last_synth_warnlevel_desc,
		a.dimension_ch,
		a.dim_warn_level_desc,
		a.last_dim_warn_level_desc,
		a.dim_warn_level_worsen_flag,
		a.dim_submodel_contribution_ratio_worsen_flag,
	   	b.dim_msg,

		case 
			when dim_warn_level_worsen_flag=1 and dim_submodel_contribution_ratio_worsen_flag in (0,1)  then 
				concat(
					a.dimension_ch,'ά��','��',a.last_dim_warn_level_desc,'������',a.dim_warn_level_desc,nvl(b.dim_msg,'')
				)
			when dim_warn_level_worsen_flag=0 and dim_submodel_contribution_ratio_worsen_flag=1 then 
				concat('����ˮƽ������ά��Ϊ',a.dimension_ch,'ά��',nvl(b.dim_msg,'')
				)
			else 
				NULL
		end as corp_dim_msg
	from Basic_data_II a 
	left join Fourth_msg_dim b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.score_dt=b.score_dt and a.dimension_ch=b.dimension_ch
),
Fourth_msg_corp_II as 
(
	select 
		a.*,
		ru.reason,
		case 
			when ru.reason is not null then  
				concat('�����ǰһ�죬','Ԥ���ȼ���',a.last_synth_warnlevel_desc,'������','�����ѱ�¶Ԥ���ȼ�','��','��Ҫ���ڴ���',ru.reason,nvl(concat('��',if(a.msg_corp_='',null,a.msg_corp_) ),''),'��')
			else 
				concat('�����ǰһ�죬','Ԥ���ȼ���',a.last_synth_warnlevel_desc,'������',a.synth_warnlevel_desc,nvl(concat('��',if(a.msg_corp_='',null,a.msg_corp_)),''),'��')
		end as msg4_with_no_color,
		case 
			when ru.reason is not null then  
				concat('�����ǰһ�죬','Ԥ���ȼ���','<span class="RED"><span class="WEIGHT">',a.last_synth_warnlevel_desc,'������','�����ѱ�¶Ԥ���ȼ�','��','��Ҫ���ڴ���',ru.reason,'</span></span>',nvl(concat('��',if(a.msg_corp_='',null,a.msg_corp_)),''),'��')
			else 
				concat('�����ǰһ�죬','Ԥ���ȼ���','<span class="RED"><span class="WEIGHT">',a.last_synth_warnlevel_desc,'������',a.synth_warnlevel_desc,'</span></span>',nvl(concat('��',if(a.msg_corp_='',null,a.msg_corp_)),''),'��')
		end as msg4
	from 
	(
		select 
			batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel_desc,last_synth_warnlevel_desc,--corp_dim_msg
			concat_ws('��',collect_set(corp_dim_msg)) as msg_corp_
			-- group_concat(distinct corp_dim_msg,'��') as msg_corp_   -- impala
		from Fourth_msg_corp_I
		group by batch_dt,corp_id,corp_nm,score_dt,synth_warnlevel_desc,last_synth_warnlevel_desc--,corp_dim_msg
	)A left join warn_adj_rule_cfg ru
		on a.corp_id = ru.corp_id 
)
insert overwrite table pth_rmp.rmp_warning_score_report4
select
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg4_with_no_color,
	msg4
from Fourth_msg_corp_II
;
