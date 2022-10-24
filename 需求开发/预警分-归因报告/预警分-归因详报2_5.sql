-- RMP_WARNING_SCORE_REPORT �ڶ��κ͵����-��ǰ�ȼ�����ͽ����ע���� --
-- drop table if exists app_ehzh.rmp_warning_score_report2;    
-- create table app_ehzh.rmp_warning_score_report2 as      --@pth_rmp.rmp_warning_score_report2
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
      and to_date(rating_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all
    -- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
-- �������� --
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	-- ʱ�����Ʋ��� --
	select * 
	from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.RMP_WARNING_SCORE_DETAIL  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- �������׶� --
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_ as --�������׶�_�ۺ�Ԥ���ȼ�
(
	-- ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(end_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- ���Ź��� --
news_intf_ as 
(
	-- ʱ�����Ʋ��� --
    select *
    from app_ehzh.rmp_opinion_risk_info --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- ��12���µ��������� --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select * 
    from app_ehzh.rmp_opinion_risk_info --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- ���� --
cx_intf_ as 
(
	-- ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µ��������� --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CX --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- ˾�� --
sf_ktts_inft_ as --��ͥͥ��
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µĿ�ͥͥ������ --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_KTGG --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --��������
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µĿ�ͥͥ������ --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from app_ehzh.RMP_WARNING_SCORE_CPWS --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
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
-- Ԥ���� --
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
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select *
	from RMP_WARNING_SCORE_MODEL_
	-- select a.*
	-- from RMP_WARNING_SCORE_MODEL_ a 
	-- join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
	-- 	on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch as --ȡÿ���������� �ۺ�Ԥ��-���׶����а�(�������ƽ���������Χ������Ĳ�������)
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
-- ���Ź��������� --
mid_news as 
(
	--��6���±Ƚ�12����_����_��ǩ_��ѯ��ע_����(last6Mto12M_news_label_6008001_num)
	select distinct
		'last6Mto12M_news_label_6008001_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6008001' --��ѯ��ע
	union all 
	--��ҵ���_��6���±Ƚ�12����_����_��ǩ_��������Ԥ��_���ƽ��ֵ(indus_rela_last6Mto12M_news_label_6002012_meanimportance)
	select distinct
		'indus_rela_last6Mto12M_news_label_6002012_meanimportance' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --��������Ԥ��
	union all 
	--��ҵ���_��12����_����_��ǩ_��������Ԥ��_����(indus_rela_last12M_news_label_6002012_num)
	select distinct
		'indus_rela_last12M_news_label_6002012_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where case_type_ii_cd='6002012'  --��������Ԥ��
	union all
	--��1��_����_����(last1W_news_count)
	select distinct
		'last1W_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-7))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--��ҵ���_��2��_����_����(indus_rela_last2W_news_count)
	select distinct
		'indus_rela_last2W_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-14))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��ҵ���_��1����_����_��ǩ_�������_ռ��(indus_rela_last1M_news_label_6002001_rate)
	select distinct
		'indus_rela_last1M_news_label_6002001_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --�������
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--��ҵ���_��3����_����_��ǩ_�������_ռ��(indus_rela_last3M_news_label_6002001_rate)
	select distinct
		'indus_rela_last3M_news_label_6002001_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --�������
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��6����_����_��ǩ_�������_����(last6M_news_label_6002001_num)
	select distinct
		'last6M_news_label_6002001_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002001' --�������
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--��ҵ���_��3����_����_��ǩ_������ҵ��������_����(indus_rela_last3M_news_label_6003007_num)
	select distinct
		'indus_rela_last3M_news_label_6003007_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003007' --������ҵ��������
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	--��12����_����_��ǩ_�����Է���_ռ��(last12M_news_label_6002002_rate)
	select distinct
		'last12M_news_label_6002002_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6002002' --�����Է���
	union all 
	--��12����_����_��ǩ_�����µ�_����(last12M_news_label_6001002_num)
	select distinct
		'last12M_news_label_6001002_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6001002' --�����µ�
	union all 
	--��12����_����_����(last12M_news_count)
	select distinct
		'last12M_news_count' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	union all 
	--��12����_����_��ǩ_��������Ԥ��_����(last12M_news_label_6004024_num)
	select distinct
		'last12M_news_label_6004024_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6004024' --��������Ԥ��
	union all 
	--��12����_����_��ǩ_��������_ռ��(last12M_news_label_6007002_rate)
	select distinct
		'last12M_news_label_6007002_rate' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6007002' --��������
	union all 
	--��12����_����_��ǩ_������ӪԤ��_����(last12M_news_label_6003064_num)
	select distinct
		'last12M_news_label_6003064_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003064' --������ӪԤ��
	union all 
	--��6���±Ƚ�12����_����_��ǩ_������ӪԤ��_����(last6Mto12M_news_label_6003064_num)
	select distinct
		'last6Mto12M_news_label_6003064_num' as feature_cd,
		corp_id,
		-- corp_nm,
		notice_date,
		msg_id,
		msg_title
		-- msg
	from news_intf_
	where 1=1
	  and case_type_ii_cd='6003064' --������ӪԤ��
),
-- ���������� --
mid_cx_ as 
(
	--��3���±Ƚ�12����_����_����ʵʩ״̬_ʵ�ʴ���_����(last3Mto12M_honesty_penaltystatus_2_num)
	select distinct
		'last3Mto12M_honesty_penaltystatus_2_num' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and it0026_013='2'
	union all 
	--��6���±Ƚ�12����_����_��������_���뱻ִ����_ռ��(last6Mto12M_honesty_secclass_22000078_rate)
	select distinct
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='���뱻ִ����'
	union all 
	--��6����_����_����(last6M_honesty_num)
	select distinct
		'last6M_honesty_num' as feature_cd,
		corp_id,
		notice_date,
		tit0026_1id as msg_id,
		msg_title
	from cx_intf_
	where 1=1
	  and tit0026_typelevel6='22000078'  --tIT0026_TypeLevel7='���뱻ִ����'
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
),
mid_cx as --ȥ���������������ظ��Ե�msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_cx_
	) A where rm=1
),
mid_sf_cpws_ as  --��������/��Ժ����/cr0055
(
	--��6���±Ƚ�12����_��Ժ����_������ϸ_������ͬ����_ռ��(last6Mto12M_lawsuit_detailedreason_4_rate)
	select distinct
		'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='������ͬ����'
	union all 
	--��3���±Ƚ�12����_��Ժ����_������ϸ_��ͬ����_ռ��(last3Mto12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last3Mto12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='��ͬ����'
	union all 
	--��6���±Ƚ�12����_��Ժ����_������ϸ_��ͬ����_����(last6Mto12M_lawsuit_detailedreason_7_num)
	select distinct
		'last6Mto12M_lawsuit_detailedreason_7_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='��ͬ����'
	union all 
	--��12����_��Ժ����_��������_ִ���స��_ռ��(last12M_lawsuit_casetype_3_rate)
	select distinct
		'last12M_lawsuit_casetype_3_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_003='ִ���స��'
	union all 
	--��12����_��Ժ����_����������_��ִ����_ռ��(last12M_lawsuit_partyrole_8_rate)
	select distinct
		'last12M_lawsuit_partyrole_8_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='8'  --��ִ����
	union all 
	--��12����_��Ժ����_������ϸ_���ڽ���ͬ����_ռ��(last12M_lawsuit_detailedreason_0_rate)
	select distinct
		'last12M_lawsuit_detailedreason_0_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='���ڽ���ͬ����'
	union all 
	--��12����_��Ժ����_������ϸ_��ͬ����_ռ��(last12M_lawsuit_detailedreason_7_rate)
	select distinct
		'last12M_lawsuit_detailedreason_7_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0055_030='��ͬ����' 
	union all 
	--��12����_��Ժ����_������ϸ_��ͬ����_ռ��(last12M_lawsuit_lawsuitamt_mean)
	select distinct
		'��12����_��Ժ����_�永���_ƽ��ֵ' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	union all
	--��12����_��Ժ����_����������_��������_ռ��(last12M_lawsuit_partyrole_4_rate)
	select distinct
		'last12M_lawsuit_partyrole_4_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_cpws_inft_ 
	where cr0081_003='4'  --��������
),
mid_sf_cpws as --ȥ��˾��_�����������������ظ��Ե�msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_sf_cpws_
	) A where rm=1
),
mid_sf_ktts_ as 
(
	--��6���±Ƚ�12����_��ͥͥ��_���ϵ�λ����_������_ռ��(last6Mto12M_courttrial_trialstatus_5_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --������
	union all 
	--��6���±Ƚ�12����_��ͥͥ��_���ϵ�λ����_������_ռ��(last6Mto12M_courttrial_trialstatus_10_rate)
	select distinct
		'last6Mto12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --������
	union all
	--��1����_��ͥͥ��_���ϵ�λ����_ԭ�󱻸�_ռ��(last1M_courttrial_trialstatus_2_rate)
	select distinct
		'last1M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --ԭ�󱻸�
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-30))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��3����_��ͥͥ��_���ϵ�λ����_ԭ�󱻸�_ռ��(last3M_courttrial_trialstatus_2_rate)
	select distinct
		'last3M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --ԭ�󱻸�
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��3����_��ͥͥ��_���ϵ�λ����_ԭ�󱻸�_����(last3M_courttrial_trialstatus_2_num)
	select distinct
		'last3M_courttrial_trialstatus_2_num' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --ԭ�󱻸�
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-90))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��6����_��ͥͥ��_���ϵ�λ����_������_ռ��(last6M_courttrial_trialstatus_10_rate)
	select distinct
		'last6M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --ԭ�󱻸�
	  and notice_date>=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-180))
	  and notice_date<=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all
	--��12����_��ͥͥ��_���ϵ�λ����_ԭ�󱻸�_ռ��(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --ԭ�󱻸�
	union all
	--��12����_��ͥͥ��_���ϵ�λ����_ԭ�󱻸�_ռ��(last12M_courttrial_trialstatus_2_rate)
	select distinct
		'last12M_courttrial_trialstatus_2_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='2'  --ԭ�󱻸�
	union all 
	--��12����_��ͥͥ��_���ϵ�λ����_������_ռ��(last12M_courttrial_trialstatus_5_rate)
	select distinct
		'last12M_courttrial_trialstatus_5_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='5'  --������
	union all 
	--��12����_��ͥͥ��_���ϵ�λ����_������_ռ��(last12M_courttrial_trialstatus_10_rate)
	select distinct
		'last12M_courttrial_trialstatus_10_rate' as feature_cd,
		corp_id,
		notice_date,
		source_id as msg_id,
		msg_title
	from sf_ktts_inft_
	where cr0169_002='10'  --������
),
mid_sf_ktts as --ȥ��˾��_��ͥͥ�����������ظ��Ե�msg_id
(
	select distinct 
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from 
	(
		select 
			*,
			row_number() over(partition by feature_cd,corp_id,notice_date,msg_title order by msg_id desc) as rm
		from mid_sf_ktts_
	) A where rm=1
),
mid_risk_info as   --�ϲ����š����š�˾������
(
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_news
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_cx
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_sf_cpws
	union all 
	select
		feature_cd,
		corp_id,
		notice_date,
		msg_id,
		msg_title
	from mid_sf_ktts
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
		main.factor_evaluate,  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		main.idx_name,  -- used 
		main.idx_value,  -- used
		main.last_idx_value, -- used in ��wy
		main.idx_unit,  -- used 
		main.idx_score,  -- used
		rinfo.msg_title,    --������Ϣ��һ��ָ���Ӧ��������¼���
		f_cfg.feature_name_target,  --��������-Ŀ��(ϵͳ)  used
		main.contribution_ratio,
		main.dim_warn_level,
		cfg.risk_lv_desc as dim_warn_level_desc  --ά�ȷ��յȼ�(�ѵ�)  used
	from RMP_WARNING_SCORE_DETAIL_Batch main
	left join feat_CFG f_cfg 	
		on main.idx_name=f_cfg.feature_cd
	left join RMP_WARNING_SCORE_MODEL_Batch a
		on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	join warn_dim_risk_level_cfg_ cfg 
		on main.dim_warn_level=cast(cfg.risk_lv as string)
	left join mid_risk_info rinfo 
		on main.corp_id=rinfo.corp_id and main.score_dt=rinfo.notice_date and main.idx_name=rinfo.feature_cd
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
			contribution_ratio,
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
			msg_title,    --������Ϣ��һ��ָ���Ӧ��������¼���
			case idx_unit
				when '%' then 
					concat(feature_name_target,'Ϊ',cast(cast(round(idx_value,2) as decimal(10,2))as string),idx_unit)
				else 
					concat(feature_name_target,'Ϊ',cast(idx_value as string),idx_unit)
			end as idx_desc,				
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension)  as dim_factor_cnt,
			count(idx_name) over(partition by corp_id,batch_dt,score_dt,dimension,factor_evaluate)  as dim_factorEvalu_factor_cnt
		from Second_Part_Data_Prepare 
		order by corp_id,score_dt desc,dim_contrib_ratio desc
	) A
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
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
Second_Part_Data_Dimension_Type_idx as --��ָ���������ݣ����ڻ��ܶ�� �����¼� ��һ��ָ����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension,
		dimension_ch,
		type,
		idx_desc,
		contribution_ratio,  --ָ���Ĺ��׶�ռ��
		-- concat_ws('��',collect_set(msg_title)) as risk_info_desc_in_one_idx   -- hive 
		group_concat(distinct msg_title,'��') as risk_info_desc_in_one_idx    -- impala 
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
			idx_desc,
			msg_title,
			contribution_ratio,
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type,idx_desc order by contribution_ratio desc) as rm
		from Second_Part_Data
		where factor_evaluate = 0
	)A where rm<=10  --���չ��׶������Ӹߵ��������ȡ��ǰʮ�������¼���Ϊչʾ
	group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type,idx_desc,contribution_ratio
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
		-- concat_ws('��',collect_set(risk_info_desc_in_one_idx)) as risk_info_desc_in_one_type,   -- hive 
		nvl(group_concat( risk_info_desc_in_one_idx,'��'),'') as  risk_info_desc_in_one_type,   --impala  �ٽ������¼����ܵ�type��
		-- concat_ws('��',collect_set(idx_desc)) as idx_desc_in_one_type   -- hive (ƴ��ֵΪNULL������'')
		nvl(group_concat(distinct idx_desc,'��'),'') as idx_desc_in_one_type    -- impala  (ƴ��ֵȫ��ΪNULL������NULL)
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
			idx_desc,
			risk_info_desc_in_one_idx,    --���ܵ�һ��ָ���ϵķ�����Ϣ
			row_number() over(partition by batch_dt,corp_id,score_dt,dimension,type order by contribution_ratio desc) as rm
		from Second_Part_Data_Dimension_Type_idx
		-- where factor_evaluate = 0
		-- group by batch_dt,corp_id,corp_nm,score_dt,dimension,dimension_ch,type
	) A where rm<=5   --ȡ���׶�������ߵ�5���쳣����
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
		row_number() over(partition by batch_dt,corp_id,score_dt order by dim_contrib_ratio desc) as dim_contrib_ratio_rank,   --�Ӵ�С����
		dim_factorEvalu_factor_cnt,
		concat(
			dimension_ch,'ά��','��','���׶�ռ��',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','��','��',
			'��ά�ȵ�ǰ����',dim_warn_level_desc,'�ȼ�','��',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('�������쳣ָ�꼰�¼�','��')
				else 
					concat(
						dimension_ch,'ά��','�����',cast(dim_factor_cnt as string),'��ָ����','��',cast(dim_factorEvalu_factor_cnt as string),'��ָ������쳣','��',
						'�쳣ָ�������������չ��׶�Ϊ',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','��'
					)
			end
		) as dim_msg_no_color,
		concat(
			'<span class="WEIGHT">',dimension_ch,'ά��','��','���׶�ռ��',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','��','</span>','��',
		
			'��ά�ȵ�ǰ����',
				case 
					when dim_warn_level_desc ='�߷���' then 
						concat('<span class="RED"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='�з���' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
					when dim_warn_level_desc ='�ͷ���' then 
						concat('<span class="GREEN"><span class="WEIGHT">',dim_warn_level_desc,'</span></span>')
				end,
				'�ȼ�','��',
			case 
				when dim_factorEvalu_factor_cnt=0 then 
					concat('�������쳣ָ�꼰�¼�','��')
				else 
					concat(
						dimension_ch,'ά��','�����',cast(dim_factor_cnt as string),'��ָ����','��',cast(dim_factorEvalu_factor_cnt as string),'��ָ������쳣','��',
						'�쳣ָ�������������չ��׶�Ϊ',cast(cast(round(dim_factorEvalu_contrib_ratio,0) as decimal(10,0)) as string) ,'%','��'
					)
			end
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
		-- concat(concat_ws('��',collect_set(dim_type_msg)),'��') as idx_desc_risk_info_desc_in_one_dimension   -- hive 
		concat(group_concat(distinct dim_type_msg,'��'),'��') as idx_desc_risk_info_desc_in_one_dimension  --impala
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
				type,'�쳣��',   --���磺'���Ź������쳣��'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"�漰�����¼���Ҫ������",risk_info_desc_in_one_type,'��',
							'��Ҫ�쳣ָ�������',idx_desc_in_one_type
						)
				end				
			) as dim_type_msg_no_color,
			concat(
				'<span class="WEIGHT">',type,'�쳣��','</span>',   --���磺'���Ź������쳣��'
				case 
					when  risk_info_desc_in_one_type='' then 
						idx_desc_in_one_type
					else 
						concat(
							"�漰�����¼���Ҫ������",risk_info_desc_in_one_type,'��',
							'��Ҫ�쳣ָ�������',idx_desc_in_one_type
						)
				end				
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
		lpad(cast(a.dim_contrib_ratio_rank as string),3,'0') as dim_rank,  --ά����ʾ˳��������001 002 003 004 005
		case 	
			when a.dim_factorEvalu_factor_cnt=0 then  --���쳣ָ��ʱ������ֱ�����ά�Ȳ㼴�ɣ�������Ϊ'�������쳣ָ�꼰�¼�'
				a.dim_msg
			else
				concat(
					lpad(cast(a.dim_contrib_ratio_rank as string),3,'0'),'_',a.dim_msg,'��Ҫ����',b.idx_desc_risk_info_desc_in_one_dimension
				) 
		end as msg_dim
	from Second_Msg_Dimension a
	join Second_Msg_Dimension_Type b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id and a.dimension=b.dimension
	order by batch_dt,corp_id,score_dt,dim_rank
),
Second_Msg as    --��������δ�� ���׶�ռ�� �Ӵ�С����
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('\\r\\n',sort_array(collect_set(msg_dim))) as msg
		group_concat(distinct msg_dim,'\\r\\n') as msg
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
			dimension_ch as dimension_ch_no_color,
			concat('<span class="RED"><span class="WEIGHT">',dimension_ch,'</span></span>') as dimension_ch
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
		concat('�����ע��˾',abnormal_dim_msg,'ά��','�����ķ��ա�') as msg
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

