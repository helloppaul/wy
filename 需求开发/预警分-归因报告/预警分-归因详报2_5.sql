-- RMP_WARNING_SCORE_REPORT �ڶ��κ͵����-��ǰ�ȼ�����ͽ����ע���� --
drop table if exists app_ehzh.rmp_warning_score_report2;    
create table app_ehzh.rmp_warning_score_report2 as      --@pth_rmp.rmp_warning_score_report2
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
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --�������׶�_�ۺ�Ԥ���ȼ�
(
	select *
	from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf  --@hds.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
warn_dim_risk_level_cfg_ as  -- ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ�
(
	select
		low_contribution_percent,   --60 ...
		high_contribution_percent,  --100  ...
		risk_lv,   -- -3 ...
		risk_lv_desc  -- �߷��� ...
	from pth_rmp.rmp_warn_dim_risk_level_cfg
),
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
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --ȡÿ���������� �ۺ�Ԥ��-���׶����а�(�������ƽ���������Χ������Ĳ�������)
(
	select distinct a.feature_name,cfg.feature_name_target
	from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ a
	join (select max(end_dt) as max_end_dt,to_date(end_dt) as score_dt from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ group by to_date(end_dt)) b
		on a.end_dt=b.max_end_dt and to_date(a.end_dt)=b.score_dt
	join feat_CFG cfg
		on a.feature_name=cfg.feature_cd
),
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
RMP_WARNING_SCORE_DETAIL_Batch as -- ȡÿ�������������ݣ�������������Χ���ƣ�
(
	select a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
),
-- �ڶ������� --
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
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
Second_Part_Data_Dimension as -- ��ά�Ȳ��������������
(
	select 
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
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type
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
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch
),
Second_Msg_Dim as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.dimension,
		concat(
			a.dim_msg,b.idx_desc_in_one_dimension
		) as msg_dim
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.dimension=b.dimension
),
Second_Msg as    --��������δ�� ���׶�ռ�� �Ӵ�С����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('\\r\\n',collect_set(msg_dim)) as msg
		group_concat(msg_dim,'\\r\\n') as msg
	from Second_Msg_Dim
	group by batch_dt,corp_id,corp_nm,score_dt
),
Fifth_Data as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('��',collect_set(dimension_ch)) as abnormal_dim_msg  -- hive
		group_concat(dimension_ch,'��') as abnormal_dim_msg -- impala
	from 
	(
		select distinct
			batch_dt,
			corp_id,
			corp_nm,
			score_dt,
			dimension_ch
		from Second_Part_Data_Prepare
		where factor_evaluate = 0   --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
	)A 
	group by batch_dt,corp_id,corp_nm,score_dt
),
Fifth_Msg as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat('�����ע��˾',abnormal_dim_msg,'�����ķ��ա�') as msg
	from Fifth_Data
)
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
select 
	a.batch_dt,
	a.corp_id,
	a.corp_nm,
	a.score_dt,
	a.msg as msg2,
	b.msg as msg5
from Second_Msg a 
join Fifth_Msg b 
	on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.score_dt=b.score_dt
;

