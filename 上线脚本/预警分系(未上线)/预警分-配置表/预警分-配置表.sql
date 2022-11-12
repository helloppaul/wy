-- Ԥ���������ñ� --
--������������������������������������������������������������������������������������������������������������ �ۺ�Ԥ���ȼ��ȼ����ֵ�λ-���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
drop table if exists pth_rmp.rmp_warn_level_ratio_cfg; 
create table pth_rmp.rmp_warn_level_ratio_cfg as  
with
warn_level_ratio_cfg_ as -- �ۺ�Ԥ���ȼ��ȼ����ֵ�λ-���ñ�
(
	select 2 property_cd,'��Ͷ' as property,'-5' as warn_lv,'ǰ1%' as percent_desc,'�����ѱ�¶' as warn_lv_desc
	union all
	select 2 property_cd,'��Ͷ' as property,'-4' as warn_lv,'ǰ1%-10%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 2 property_cd,'��Ͷ' as property,'-3' as warn_lv,'ǰ10%-20%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 2 property_cd,'��Ͷ' as property,'-2' as warn_lv,'ǰ20%-30%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 2 property_cd,'��Ͷ' as property,'-1' as warn_lv,'��30%-100%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc 
	
	union all

	select 1 property_cd,'��ҵ' as property,'-5' as warn_lv,'ǰ1%' as percent_desc,'�����ѱ�¶' as warn_lv_desc
	union all
	select 1 property_cd,'��ҵ' as property,'-4' as warn_lv,'ǰ1%-10%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 1 property_cd,'��ҵ' as property,'-3' as warn_lv,'ǰ10%-20%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 1 property_cd,'��ҵ' as property,'-2' as warn_lv,'ǰ20%-30%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc
	union all
	select 1 property_cd,'��ҵ' as property,'-1' as warn_lv,'��30%-100%' as percent_desc,'��ɫԤ���ȼ�' as warn_lv_desc 
)
select * from warn_level_ratio_cfg_
;
--������������������������������������������������������������������������������������������������������������ ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
drop table if exists pth_rmp.rmp_warn_dim_risk_level_cfg; 
create table pth_rmp.rmp_warn_dim_risk_level_cfg as  
with
warn_dim_risk_level_cfg_ as  -- ά�ȹ��׶�ռ�ȶ�Ӧ����ˮƽ-���ñ�
(	
	select 1 as dimension,'����' as remark,50 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc
	union all 
	select 1 as dimension,'����' as remark,35 as low_contribution_percent,50 as high_contribution_percent,-2 as risk_lv ,'�з���' as risk_lv_desc  
	union all 
	select 1 as dimension,'����' as remark,0 as low_contribution_percent,35 as high_contribution_percent,-1 as risk_lv ,'�ͷ���' as risk_lv_desc   
	
	union all 
	select 2 as dimension,'��Ӫ' as remark,50 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc   
	union all 
	select 2 as dimension,'��Ӫ' as remark,35 as low_contribution_percent,50 as high_contribution_percent,-2 as risk_lv ,'�з���' as risk_lv_desc   
	union all 
	select 2 as dimension,'��Ӫ' as remark,0 as low_contribution_percent,35 as high_contribution_percent,-1 as risk_lv ,'�ͷ���' as risk_lv_desc   
	
	union all 
	select 3 as dimension,'�г�' as remark,15 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc   
	union all 
	select 3 as dimension,'�г�' as remark,8 as low_contribution_percent,15 as high_contribution_percent,-2 as risk_lv ,'�з���' as risk_lv_desc   
	union all 
	select 3 as dimension,'�г�' as remark,0 as low_contribution_percent,8 as high_contribution_percent,-1 as risk_lv ,'�ͷ���' as risk_lv_desc   

	union all 
	select 4 as dimension,'����' as remark,15 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc   
	union all 
	select 4 as dimension,'����' as remark,8 as low_contribution_percent,15 as high_contribution_percent,-2 as risk_lv ,'�з���' as risk_lv_desc   
	union all 
	select 4 as dimension,'����' as remark,0 as low_contribution_percent,8 as high_contribution_percent,-1 as risk_lv ,'�ͷ���' as risk_lv_desc   


	-- select 60 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'�߷���' as risk_lv_desc   --(60,100]
	-- union all
	-- select 40 as low_contribution_percent,60 as high_contribution_percent,-2 as risk_lv,'�з���' as risk_lv_desc   --(40,60]
	-- union all
	-- select 0 as low_contribution_percent, 40 as high_contribution_percent,-1 as risk_lv,'�ͷ���' as risk_lv_desc   --(0,40]
)
select * from warn_dim_risk_level_cfg_
;
--������������������������������������������������������������������������������������������������������������ �������ֹ����ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ps:excel����
-- select * from pth_rmp.rmp_warning_score_feature_cfg;
drop table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_;
CREATE TABLE pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_
(
	sid_kw int,
	feature_cd string,
	feature_name string,
	sub_model_type string,
	feature_name_target string,
	dimension string,
	type string,
	cal_explain string,
	feature_explain string,
	unit_origin string,
	unit_target string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties
(
	"separatorChar="=",",
	"quoteChar"="'",
	"escapeChar"="\\"
)stored as textfile;

GRANT ALL ON URI "hdfs://htsecnew/user/pth_rmp/" TO ROLE pth_rmp;  --����û���allȨ�ޣ��������ݻ���ҪURI��Ȩ��
load data inpath '/user/pth_rmp/importfile/Ԥ����-�����ֹ���.csv' 
into table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_;  

drop table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_BAC;
create table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_BAC as select * from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;
drop table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;
CREATE table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG as select * from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_;
------------------------------ ------------------------------------------------------------------------------------

-- show grant role pth_rmp;  --�鿴�û�pth_rmp��Ȩ��
GRANT ALL ON URI "hdfs://htsecnew/user/pth_rmp/" TO ROLE pth_rmp;  --����û���allȨ�ޣ��������ݻ���ҪURI��Ȩ��

load data inpath '/user/pth_rmp/importfile/Ԥ����-�����ֹ���.csv' 
into table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;  --��������

export table pth_rmp.alert_score_summ to '/user/pth_rmp/alert_score_summ';  --��������

