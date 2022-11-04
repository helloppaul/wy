--��1��DDL rmp_warning_score_s_report_zx_init hiveִ��--
drop table if exists pth_rmp.rmp_warning_score_s_report_zx_init ;
create table pth_rmp.rmp_warning_score_s_report_zx_init
(
	sid_kw string,
	corp_id string,
	corp_nm string,
	score_dt timestamp,
	report_msg string,
	model_version string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int) 
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



-- ��2��rmp_warning_score_s_report_zx_init_impala impalaִ�� --
create table pth_rmp.rmp_warning_score_s_report_zx_init_impala as 
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
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select max(flag) from timeLimit_switch) 
      and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
    union all
    -- ��ʱ�����Ʋ��� --
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- �������� --
RMP_WARNING_SCORE_DETAIL_ as  --Ԥ����--�������� ԭʼ�ӿ�
(
	-- ʱ�����Ʋ��� --
	select * ,score_dt as batch_dt
	from pth_rmp.rmp_warning_score_detail_init  --@pth_rmp.RMP_WARNING_SCORE_DETAIL
	where 1 in (select max(flag) from timeLimit_switch)  and delete_flag=0
      and to_date(score_dt) = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	union all 
	-- ��ʱ�����Ʋ��� --
    select * ,score_dt as batch_dt
    from pth_rmp.rmp_warning_score_detail_init  
    where 1 in (select not max(flag) from timeLimit_switch)  and delete_flag=0
),
-- ���Ź��� --
news_intf_ as 
(
	-- ʱ�����Ʋ��� --
    select *
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select max(flag) from timeLimit_switch) and crnw0003_010 in ('1','4') 
	  -- ��12���µ��������� --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select * 
    from pth_rmp.rmp_opinion_risk_info_init --@pth_rmp.rmp_opinion_risk_info
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- ���� --
cx_intf_ as 
(
	-- ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µ��������� --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cx_init --@pth_rmp.RMP_WARNING_SCORE_CX
    where 1 in (select not max(flag) from timeLimit_switch) 
),
-- ˾�� --
sf_ktts_inft_ as --��ͥͥ��
(
	select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µĿ�ͥͥ������ --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_ktgg_init --@pth_rmp.RMP_WARNING_SCORE_KTGG
    where 1 in (select not max(flag) from timeLimit_switch) 
),
sf_cpws_inft_ as --��������
(
	 select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select max(flag) from timeLimit_switch)
	  -- ��12���µĿ�ͥͥ������ --
      and to_date(notice_dt) >= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-365))
	  and to_date(notice_dt)<= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
    union all 
    -- ��ʱ�����Ʋ��� --
    select 
		*,
		to_date(notice_dt) as notice_date
    from pth_rmp.rmp_warning_score_cpws_init --@pth_rmp.RMP_WARNING_SCORE_CPWS
    where 1 in (select not max(flag) from timeLimit_switch) 
),
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
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
-- ģ����ҹ��� --
warn_adj_rule_cfg as --Ԥ����-ģ����ҹ������ñ�   ȡ����etl_date������ (����Ƶ��:�նȸ���)
(
	select distinct
		a.etl_date,
		b.corp_id, 
		b.corp_name as corp_nm,
		a.category,
		a.reason
	from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
	join corp_chg b 
		on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
	where a.operator = '�Զ�-�����ѱ�¶����'
	  and a.ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- Ԥ���� --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_batch as 
(
    select a.*
	from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a 
	join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
		on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
),
RMP_WARNING_SCORE_MODEL_ as  --Ԥ����-ģ�ͽ����
(
    select distinct
		cast(to_date(a.rating_dt) as string) as batch_dt,  --��ʼ���ű����⴦������������ʼ������
        -- cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- Ԥ����
		a.interval_text_adjusted,
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
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
RMP_WARNING_SCORE_MODEL_Batch as  -- ȡÿ��������������
(
	select distinct a.*
	from RMP_WARNING_SCORE_MODEL_ a 
),
-- �������� --
RMP_WARNING_SCORE_DETAIL_Batch as -- ȡÿ�������������ݣ�������������Χ���ƣ�
(
	select distinct a.*
	from RMP_WARNING_SCORE_DETAIL_ a
	join (select max(batch_dt) as max_batch_dt,score_dt from RMP_WARNING_SCORE_DETAIL_ group by score_dt) b
		on a.batch_dt=b.max_batch_dt and a.score_dt=b.score_dt
	-- where a.idx_name in (select feature_name from rsk_rmp_warncntr_dftwrn_intp_union_featpct_intf_Batch)
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
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
		tit0026_1id  as msg_id,
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
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
s_report_Data_Prepare_ as 
(
	select DISTINCT
		T.*,
		rinfo.msg_title,   --һ��ָ���Ӧ���������¼�
		nvl(ru.category,'') as category_nvl,   --һ����ҵһ��һ����ҹ���
		nvl(ru.reason,'') as reason_nvl
	from 
	(
		select 
			main.batch_dt,
			main.corp_id,
			main.corp_nm,
			main.score_dt,
			a.interval_text_adjusted,
			-- nvl(a.synth_warnlevel,'0') as synth_warnlevel, --�ۺ�Ԥ���ȼ�
			main.dimension,    --ά�ȱ���
			main.dim_contrib_ratio,
			-- sum(contribution_ratio) over(partition by main.corp_id,main.batch_dt,main.score_dt,f_cfg.dimension) as dim_contrib_ratio,
			nvl(f_cfg.dimension,'') as dimension_ch,  --ά������
			main.type,  	-- used
			main.idx_name,  -- used 
			main.idx_value,  -- used
			main.last_idx_value, -- used in ��wy
			main.idx_unit,  -- used 
			main.idx_score,  -- used
			nvl(f_cfg.feature_name_target,'') as feature_name_target,  --��������-Ŀ��(ϵͳ)  used
			main.contribution_ratio,
			main.factor_evaluate  --�������ۣ������Ƿ��쳣���ֶ� 0���쳣 1������
		from (select *,sum(contribution_ratio) over(partition by corp_id,batch_dt,score_dt,dimension) as dim_contrib_ratio from RMP_WARNING_SCORE_DETAIL_Batch) main
		join feat_CFG f_cfg 	
			on main.idx_name=f_cfg.feature_cd
		join RMP_WARNING_SCORE_MODEL_Batch a
			on main.corp_id=a.corp_id and main.batch_dt=a.batch_dt
	)T 
	left join warn_adj_rule_cfg  ru
		on T.corp_id = ru.corp_id
	left join mid_risk_info rinfo 
		on T.corp_id=rinfo.corp_id and T.score_dt>=rinfo.notice_date and T.idx_name=rinfo.feature_cd
),
s_report_Data_Prepare as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		category_nvl,
		reason_nvl,
		interval_text_adjusted,
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		type,
		idx_name,
		idx_value,
		last_idx_value,
		idx_unit,
		idx_score,
		feature_name_target,
		contribution_ratio,
		factor_evaluate,
		-- concat_ws('��',collect_Set(msg_title)) as risk_info_dsec_in_one_idx  -- hive 
		group_concat(distinct msg_title,'��') as risk_info_desc_in_one_idx  -- impala
	from s_report_Data_Prepare_ 
	group by batch_dt,corp_id,corp_nm,score_dt,category_nvl,reason_nvl,interval_text_adjusted,dimension,
			 dimension_ch,dim_contrib_ratio,type,idx_name,idx_value,last_idx_value,idx_unit,idx_score,
			 feature_name_target,contribution_ratio,factor_evaluate
),
s_report_Data_dim as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		max(category_nvl) as category_nvl,
		max(reason_nvl) as reason_nvl,
		interval_text_adjusted,  --ԭʼģ�Ͳ�����Ԥ���ȼ�
		dimension,
		dimension_ch,
		dim_contrib_ratio,
		-- concat_ws('��',collect_set(feature_name_target))  as abnormal_idx_desc, -- hive
		group_concat(feature_name_target,'��')  as abnormal_idx_desc,  -- impala 
		-- concat_ws('��',collect_set(risk_info_desc_in_one_idx))  as abnormal_risk_info_desc -- hive
		group_concat(distinct risk_info_desc_in_one_idx,'��')  as abnormal_risk_info_desc
	from s_report_Data_Prepare
	where factor_evaluate = 0
	group by batch_dt,corp_id,corp_nm,score_dt,interval_text_adjusted,dimension,dimension_ch,dim_contrib_ratio
	order by dim_contrib_ratio desc
),
s_report_msg as 
(
	select distinct
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		dimension_ch,
		concat(		
			'��������ΥԼԤ������',
				case interval_text_adjusted 
					when '��ɫԤ��' then 
						concat('<span class="GREEN"><span class="WEIGHT">',interval_text_adjusted,'�ȼ�','</span></span>')
					when '��ɫԤ��' then 
						concat('<span class="YELLO"><span class="WEIGHT">',interval_text_adjusted,'�ȼ�','</span></span>')
					when '��ɫԤ��' then 
						concat('<span class="ORANGE"><span class="WEIGHT">',interval_text_adjusted,'�ȼ�','</span></span>')
					when '��ɫԤ��' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'�ȼ�','</span></span>')
					when '�����ѱ�¶' then 
						concat('<span class="RED"><span class="WEIGHT">',interval_text_adjusted,'�ȼ�','</span></span>')
				end,'��',
			if(reason_nvl<>'',concat('��Ҫ���ڴ���',reason_nvl,'ͬʱ'),''),
			'�����漰','<span class="WEIGHT">',dimension_ch,'ά��','��','���׶�ռ��',cast(cast(round(dim_contrib_ratio,0) as decimal(10,0)) as string),'%','��','</span>','��',
			case 
				when  abnormal_idx_desc<>'' then 
					concat('�쳣ָ�������',abnormal_idx_desc)
				else 
					''
			end,
			case 
				when  abnormal_risk_info_desc<>'' and abnormal_risk_info_desc is not null then 
					concat('��','�쳣�¼�������',abnormal_risk_info_desc)
				else 
					''
			end
		) as msg_in_one_dim
	from s_report_Data_dim
),
s_report_msg_corp as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		-- concat_ws('��',collect_Set(msg_in_one_dim)) as s_msg  -- hive
		group_concat(distinct msg_in_one_dim,'��') as s_msg  -- impala
	from s_report_msg
	group by batch_dt,corp_id,corp_nm,score_dt
)
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
select 
	-- concat(corp_id,md5(concat(batch_dt,corp_id))) as sid_kw,  -- hive
	-- '' as sid_kw,  -- impala
	-- batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	s_msg as report_msg,
	'v1.0' as model_version,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version
from s_report_msg_corp
;



-- ��3��sqlִ��  warning_score_s_report_zx_init hiveִ��--
insert into pth_rmp.rmp_warning_score_s_report_zx_init partition(etl_date=19900101)
select 
	concat(corp_id,md5(concat(cast(score_dt as string),corp_id))) as sid_kw ,
	corp_id ,
	corp_nm ,
	score_dt ,
	report_msg ,
	model_version ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version	
from pth_rmp.rmp_warning_score_s_report_zx_init_impala
;