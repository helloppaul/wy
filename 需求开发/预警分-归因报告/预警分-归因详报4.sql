-- RMP_WARNING_SCORE_REPORT ���Ķ�-����䶯 --
drop table if exists app_ehzh.rmp_warning_score_report4;  
create table app_ehzh.rmp_warning_score_report4 as  --@pth_rmp.rmp_warning_score_report4
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
	where a.delete_flag=0 and b.delete_flag=0
),
--������������������������������������������������������������������������������������������������������������ �ӿڲ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ʱ�����ƿ��� --
timeLimit_switch as 
(
    select True as flag   --TRUE:ʱ��Լ����FLASE:ʱ�䲻��Լ����ͨ�����ڳ�ʼ��
    -- select False as flag
),
-- Ԥ���� --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --Ԥ����_�ںϵ������ۺ�  ԭʼ�ӿ�
(
    -- ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
RMP_WARNING_SCORE_MODEL_ as  --Ԥ����-ģ�ͽ����
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
			when a.interval_text_adjusted  ='��ɫԤ��' then 
				'-3'  --�߷���
			when a.interval_text_adjusted  ='�����ѱ�¶' then 
				'-4'   --�����ѱ�¶
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where delete_flag=0
),
RMP_WARNING_SCORE_DETAIL_HIS_ as  --Ԥ����--����������ʷ ԭʼ�ӿ�
(
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL_HIS  --@pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS
	where delete_flag=0
),
RMP_WARNING_SCORE_CHG_ as 
(
	select *
	from app_ehzh.RMP_WARNING_SCORE_CHG  --@pth_rmp.RMP_WARNING_SCORE_CHG
	-- where delete_flag=0
),
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --�������׶�_�ۺ�Ԥ���ȼ�
(
	select *
	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
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
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- �߷��� ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
warn_adj_rule_cfg as --Ԥ����-ģ����ҹ������ñ�   ȡ����etl_date������ (����Ƶ��:�նȸ���)
(
	select distinct
		a.etl_date,
		b.corp_id, 
		b.corp_name as corp_nm,
		a.category,
		a.reason
	from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
	join corp_chg b 
		on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
	where operator = '�Զ�-�����ѱ�¶����'
	  and ETL_DATE in (select max(etl_date) from app_ehzh.rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
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
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
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
RMP_WARNING_SCORE_DETAIL_Batch as -- ȡÿ�������������ݣ�������������Χ���ƣ�
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
mid_RMP_WARNING_SCORE_DETAIL_HIS as 
(
	select main.*,cfg.risk_lv_desc as dim_warn_level_desc
	from RMP_WARNING_SCORE_DETAIL_HIS_ main
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
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
		main.last_idx_value, -- used in ��wy
		main.idx_unit,  -- used
		main.idx_score,  -- used
		f_cfg.feature_name_target,  --��������-Ŀ��(ϵͳ)  used
		main.contribution_ratio,
		main.factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --ά�ȷ��յȼ�(�ѵ�)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
),
Second_Part_Data as 
(
	select distinct *
	from 
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
			dim_warn_level,
			dim_warn_level_desc,  --ά�ȷ��յȼ�(�ѵ�)
			type,
			factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
			idx_name,  -- �쳣����/�쳣ָ��
			feature_name_target,
			idx_value,
			last_idx_value,
			idx_unit,
			idx_score,   --ָ������ used
			concat(feature_name_target,'Ϊ',cast(idx_value as string),idx_unit) as idx_desc,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
-- �ۺ�Ԥ���ȼ��������� --
RMP_WARNING_SCORE_CHG_Batch as  --ȡÿ���������ε�Ԥ���䶯�ȼ�����
(
	select a.*
	from RMP_WARNING_SCORE_CHG_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_CHG_ group by score_date) b 
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
Fourth_Part_Data_synth_warnlevel as   --�ۺ�Ԥ�� �ȼ��䶯(�޶���Ԥ���ȼ��䶯Ϊ���������û������������Ķ��䲻��ʾ)
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_date as score_dt,
		a.synth_warnlevel,   --�����ۺ�Ԥ���ȼ�
		cfg.warn_lv_desc as synth_warnlevel_desc,   -- used
		a.chg_direction,
		a.synth_warnlevel_l,  --�����ۺ�Ԥ���ȼ�
		cfg_l.warn_lv as synth_warnlevel_l_desc   -- used
	from RMP_WARNING_SCORE_CHG_Batch a 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg 
		on cast(a.synth_warnlevel as string)=cfg.warn_lv and 
	join (select distinct warn_lv,warn_lv_desc from warn_level_ratio_cfg_) cfg_l
		on cast(a.synth_warnlevel_l as string)=cfg_l.warn_lv
	where a.chg_direction='����'
),
-- ά�ȷ��յȼ��䶯������ & �����������ֱ䶯������(used by ������������) --
RMP_WARNING_dim_warn_lv_And_idx_score_chg as --ȡÿ���������ε�ά�ȷ��յȼ��䶯 �Լ� �������ֱ䶯 ���ݣ����Ӳ���
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dimension_ch,
		a.type,
		a.dim_contrib_ratio,   --ά�ȹ��׶�ռ��(������) used
		a.dim_warn_level,	  --����ά�ȷ��յȼ�
		a.dim_warn_level_desc,
		b.dim_warn_level as dim_warn_level_1,   --����ά�ȷ��յȼ�
		b.dim_warn_level_desc as dim_warn_level_1_desc,
		case 
			when cast(a.dim_warn_level as int)-cast(b.dim_warn_level as int) >0 then '����'
			else ''
		end as dim_warn_level_chg_desc,
		a.factor_evaluate,
		a.idx_name, 
		a.idx_value,
		a.last_idx_value,
		a.feature_name_target,
		a.idx_unit,
		a.idx_score,   -- ����ָ����
		b.idx_score as idx_score_1, -- ����ָ����
		case 
			when cast(a.idx_score as float)-cast(b.idx_score as float) >0 then '��'  --ָ��� �������ֿ��÷ֱ����Ϊ��
			else ''
		end as idx_score_chg_desc
	from Second_Part_Data a 
	join mid_RMP_WARNING_SCORE_DETAIL_HIS b
		on  a.corp_id=b.corp_id 
			and to_date(date_add(a.score_dt,-1)) = b.score_dt
			and a.dimension=b.dimension
),
-- ά�����Ӵ�ֱ䶯������(used by ������������) --
Fourth_Part_Data_dim_warn_level_And_idx_score as  
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		dim_contrib_ratio,  --ά�ȹ��׶�ռ��(������) used
		dim_warn_level,  --����ά�ȷ��յȼ�
		dim_warn_level_desc,
		dim_warn_level_1,  --����ά�ȷ��յȼ�
		dim_warn_level_1_desc,
		dim_warn_level_chg_desc,  --ά�ȷ��յȼ��䶯 ����
		idx_name,
		feature_name_target,
		idx_value,
		idx_unit,
		idx_score,
		idx_score_1,
		idx_score_chg_desc,
		max(idx_score_chg_desc) over(partition by corp_id,score_dt,dimension) as dim_idx_score_chg_desc,  --ά�Ȳ�ָ���Ƿ��
		count(idx_name) over(partition by corp_id,score_dt,dimension,idx_score_chg_desc) as dim_idx_score_cnt,  --���յ÷ֶ񻯺ͷǶ񻯷ֱ�ͳ��ָ������
		row_number() over(partition by corp_id,score_dt,dimension order by dim_contrib_ratio desc) as dim_contrib_ratio_rank
	from RMP_WARNING_dim_warn_lv_And_idx_score_chg
),
Fourth_Part_Data_idx_name as   --��ÿ������ά�ȣ�ÿ��ָ��
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.synth_warnlevel_desc,
		a.chg_direction as chg_direction_desc,
		a.synth_warnlevel_l,
		a.synth_warnlevel_l_desc,
		b.dimension,
		b.dimension_ch,
		b.type,
		b.dim_contrib_ratio,  --ά�ȹ��׶�ռ��(������) used
		b.dim_warn_level,  --����ά�ȷ��յȼ�
		b.dim_warn_level_desc,
		b.dim_warn_level_1,  --����ά�ȷ��յȼ�
		b.dim_warn_level_1_desc,
		b.dim_warn_level_chg_desc,  --ά�ȷ��յȼ��䶯 ����
		b.idx_name,
		b.feature_name_target,
		b.idx_value,
		b.idx_unit,
		concat(b.feature_name_target,'Ϊ',cast(b.idx_value as string),b.idx_unit) as idx_desc,
		b.idx_score,
		b.idx_score_1,
		b.idx_score_chg_desc,    --ָ��㣬��
		b.dim_idx_score_chg_desc, ----ά�Ȳ㣬��һ������Ϊ��
		b.dim_idx_score_cnt,    --used
		case 
			when b.idx_score_chg_desc='��' then 
				concat('��',cast(b.dim_idx_score_cnt as string),'��ָ�귢��',b.dim_idx_score_chg_desc)
			else 
				''
		end as dim_idx_score_desc,
		b.dim_contrib_ratio_rank
	from Fourth_Part_Data_synth_warnlevel a 
	join Fourth_Part_Data_dim_warn_level_And_idx_score b 
		on a.corp_id=b.corp_id and a.score_dt=b.score_dt
	-- where idx_score_chg_desc='��' 
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
Fourth_Part_Data_Dim_type as 
(
	select
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		-- concat_ws('��',collect_set(idx_desc)) as idx_desc_in_one_type  -- hive
		group_concat(idx_desc,'��') as idx_desc_in_one_type    -- impala
	from Fourth_Part_Data_idx_name
	where idx_score_chg_desc = '��'
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type
),
Fourth_Part_Data_Dim as -- ���ܵ�ά�Ȳ�
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		dim_contrib_ratio_rank,
		max(synth_warnlevel) as synth_warnlevel,
		max(synth_warnlevel_desc) as synth_warnlevel_desc,
		max(chg_direction_desc) as chg_direction_desc,   -- '����' or ''
		max(synth_warnlevel_l) as synth_warnlevel_l,
		max(synth_warnlevel_l_desc) as synth_warnlevel_l_desc,
		max(dim_contrib_ratio) as dim_contrib_ratio,
		max(dim_warn_level) as dim_warn_level,
		max(dim_warn_level_desc) as dim_warn_level_desc,
		max(dim_warn_level_1) as dim_warn_level_1,
		max(dim_warn_level_1_desc) as dim_warn_level_1_desc,
		max(dim_warn_level_chg_desc) as dim_warn_level_chg_desc,  -- '����' or ''
		-- concat_ws('��',collect_set(idx_desc)) as dim_idx_desc,  -- hive 
		group_concat(idx_desc,'��') as dim_idx_desc,  -- impala
		max(idx_score_chg_desc) as dim_idx_score_chg_desc,   --'��' or  ''
		max(dim_idx_score_cnt) as dim_idx_score_cnt,
		max(dim_idx_score_desc) as dim_idx_score_desc
	from Fourth_Part_Data_idx_name 
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,dim_contrib_ratio_rank
),
-- ���Ķ���Ϣ --
Fourth_Msg_Dim as 
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
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension,
			dimension_ch,
			type,
			concat(
				type,'�񻯣�',idx_desc_in_one_type
			) as dim_type_msg,
			idx_desc_in_one_type
		from Fourth_Part_Data_Dim_type
	) A 
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch
),
Fourth_Msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		a.dimension_ch,
		cast(a.dim_contrib_ratio_rank as string) as msg_dim_order,  --�����õ�dim_msg
		concat(
			case 
				when a.dim_contrib_ratio_rank = 1 then 
					concat(
						'��Ҫ����',a.dimension_ch,'��',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'��',a.dim_warn_level_desc,'��',
						if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'��',a.dim_idx_score_desc)),
						'�������Ϊ��',b.idx_desc_in_one_dimension
					)
				when a.dim_contrib_ratio_rank = 2 then 
					concat(
						'�������',a.dimension_ch,'��',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'��',a.dim_warn_level_desc,'��',
						if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'��',a.dim_idx_score_desc)),
						'�������Ϊ��',b.idx_desc_in_one_dimension
					)
				when a.dim_contrib_ratio_rank = 3 then 
					concat(
						'��������',a.dimension_ch,'��',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'��',a.dim_warn_level_desc,'��',
						if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'��',a.dim_idx_score_desc)),
						'�������Ϊ��',b.idx_desc_in_one_dimension
					)
				when a.dim_contrib_ratio_rank = 4 then 
					concat(
						'��������',a.dimension_ch,'��',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'��',a.dim_warn_level_desc,'��',
						if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'��',a.dim_idx_score_desc)),
						'�������Ϊ��',b.idx_desc_in_one_dimension
					)
				when a.dim_contrib_ratio_rank = 5 then 
					concat(
							'��������',a.dimension_ch,'��',a.dim_warn_level_1_desc,a.dim_warn_level_chg_desc,'��',a.dim_warn_level_desc,'��',
							if(a.dim_idx_score_desc='','',concat(a.dimension_ch,'��',a.dim_idx_score_desc)),
							'�������Ϊ��',b.idx_desc_in_one_dimension
					)
			end,'��'
		) as msg_dim
	from Fourth_Part_Data_Dim a
	join Fourth_Msg_Dim b 
		on a.corp_id=b.corp_id and a.batch_dt=b.batch_dt and a.dimension=b.dimension
)
select * from Fourth_Msg
;


