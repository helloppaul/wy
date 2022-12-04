-- RMP_WARNING_SCORE_REPORT ��һ�� --
--/**2022-10-22 �׶Σ�������ҹ����߼�/
--/* 2022-10-22 �׶Σ����� ��ʾ��ɫ */
--/* 2022-12-04 ��ҹ���ȡֵ�޸���ȡ����create_dt������ */

set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warning_score_report1;  
create table pth_rmp.rmp_warning_score_report1 as    --@pth_rmp.rmp_warning_score_report1
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
		select m.*
		from
		(
			-- ʱ�����Ʋ��� --
			select *,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 in (select max(flag) from timeLimit_switch) 
			and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
			union all
			-- ��ʱ�����Ʋ��� --
			select * ,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 in (select not max(flag) from timeLimit_switch) 
		) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
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
		a.interval_text_adjusted,  --ԭʼģ���ṩ���ۺ�Ԥ���ȼ�
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
warn_adj_rule_cfg as --Ԥ����-ģ����ҹ������ñ�   ȡ����etl_date������ (����Ƶ��:�նȸ���)
(
	select m.*
	from 
	(
		select 
			a.etl_date,
			b.corp_id, 
			b.corp_name as corp_nm,
			a.category,
			a.reason,
			rank() over(order by a.create_dt desc ,a.etl_date desc,a.reason desc) rm
		from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
		join corp_chg b 
			on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
		where a.operator = '�Զ�-�����ѱ�¶����'
		  and to_date(a.create_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	)m where rm=1 
	  --and ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
warn_color_cfg as --Ԥ������ϵ��ר��-��ɫ���ã������ο�������Ϊ�������ã�
(
	select	1 as id,' ��ɫԤ��' as ori_msg,'<span class="GREEN">��ɫ����</span>' as color_msg
	union all
	select	2 as id, '��ɫԤ��' as ori_msg,'<span class="YELLOW">��ɫ����</span>' as color_msg
	union all
	select	3 as id, '��ɫԤ��' as ori_msg,'<span class="ORANGE">��ɫ����</span>' as color_msg
	union all
	select	4 as id, '��ɫԤ��' as ori_msg,'<span class="RED">��ɫ����</span>' as color_msg
	union all
	select	5 as id, '�����ѱ�¶' as ori_msg,'<span class="RED">�����ѱ�¶����</span>' as color_msg
	union all
	select	6 as id, '�Ӵ�+��ɫ' as ori_msg,'<span class="RED"><span class="WEIGHT">��ɫ�Ӵ�����</span></span>' as color_msg 
),
--������������������������������������������������������������������������������������������������������������ �м�� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ��һ������ --
First_Part_Data as  --���� Ԥ����-����򱨵����� �������Σ�
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		nvl(ru.category,'') as category_nvl,
		nvl(ru.reason,'') as reason_nvl,
		main.credit_cd,
		main.synth_warnlevel,  --�ۺ�Ԥ���ȼ� used
		chg.bond_type,  --1:��ҵծ 2:��Ͷծ
		case chg.bond_type
			when 2 then '��Ͷƽ̨'
			else '��ҵ����'
		end as corp_bond_type,  --�������� used
		cfg.warn_lv_desc, --Ԥ���ȼ����� used
		cfg.percent_desc  --Ԥ���ȼ���λ�ٷֱȻ��� used
	from RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='ZXZX') chg
		on main.corp_id=chg.corp_id
	join warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv and chg.bond_type=cfg.property_cd
	left join warn_adj_rule_cfg  ru
		on main.corp_id = ru.corp_id
),
--������������������������������������������������������������������������������������������������������������ Ӧ�ò� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ��һ����Ϣ --
First_Msg as --
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		credit_cd,
		concat(
			case 
				when  reason_nvl<>'' then 
					concat('�������򴥷�',reason_nvl,'��','��ǰ����','�����ѱ�¶Ԥ���ȼ�','��')
				else 
					concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��',warn_lv_desc,'��')
			end
		) as msg1_no_color,  --��һ�仰
		concat(
			case 
				when  reason_nvl<>'' then 
					concat('�������򴥷�',reason_nvl,'��','��ǰ����','<span class="RED"><span class="WEIGHT">','�����ѱ�¶Ԥ���ȼ�','</span></span>','��')
				else 
					case warn_lv_desc
						when '��ɫԤ���ȼ�' then
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��','<span class="GREEN"><span class="WEIGHT">',warn_lv_desc,'</span></span>','��')
						when '��ɫԤ���ȼ�' then
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��','<span class="YELLO"><span class="WEIGHT">',warn_lv_desc,'</span></span>','��')
						when '��ɫԤ���ȼ�' then
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��','<span class="ORANGE"><span class="WEIGHT">',warn_lv_desc,'</span></span>','��')
						when '��ɫԤ���ȼ�' then
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��','<span class="RED"><span class="WEIGHT">',warn_lv_desc,'</span></span>','��')
						when '�����ѱ�¶' then 
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��','<span class="RED"><span class="WEIGHT">',warn_lv_desc,'</span></span>','��')
						else 
							concat('������Ԥ�����ˮƽ����',corp_bond_type,'��',percent_desc,'��','��',warn_lv_desc,'��')
					end
			end
		) as msg1  --����ɫ��һ�仰
	from First_Part_Data
)
select distinct
	* 
from First_Msg
;
