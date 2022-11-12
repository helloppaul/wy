-- 预警分用配置表 --
--—————————————————————————————————————————————————————— 综合预警等级等级划分档位-配置表 ————————————————————————————————————————————————————————————————————————————————--
drop table if exists pth_rmp.rmp_warn_level_ratio_cfg; 
create table pth_rmp.rmp_warn_level_ratio_cfg as  
with
warn_level_ratio_cfg_ as -- 综合预警等级等级划分档位-配置表
(
	select 2 property_cd,'城投' as property,'-5' as warn_lv,'前1%' as percent_desc,'风险已暴露' as warn_lv_desc
	union all
	select 2 property_cd,'城投' as property,'-4' as warn_lv,'前1%-10%' as percent_desc,'红色预警等级' as warn_lv_desc
	union all
	select 2 property_cd,'城投' as property,'-3' as warn_lv,'前10%-20%' as percent_desc,'橙色预警等级' as warn_lv_desc
	union all
	select 2 property_cd,'城投' as property,'-2' as warn_lv,'前20%-30%' as percent_desc,'黄色预警等级' as warn_lv_desc
	union all
	select 2 property_cd,'城投' as property,'-1' as warn_lv,'后30%-100%' as percent_desc,'绿色预警等级' as warn_lv_desc 
	
	union all

	select 1 property_cd,'产业' as property,'-5' as warn_lv,'前1%' as percent_desc,'风险已暴露' as warn_lv_desc
	union all
	select 1 property_cd,'产业' as property,'-4' as warn_lv,'前1%-10%' as percent_desc,'红色预警等级' as warn_lv_desc
	union all
	select 1 property_cd,'产业' as property,'-3' as warn_lv,'前10%-20%' as percent_desc,'橙色预警等级' as warn_lv_desc
	union all
	select 1 property_cd,'产业' as property,'-2' as warn_lv,'前20%-30%' as percent_desc,'黄色预警等级' as warn_lv_desc
	union all
	select 1 property_cd,'产业' as property,'-1' as warn_lv,'后30%-100%' as percent_desc,'绿色预警等级' as warn_lv_desc 
)
select * from warn_level_ratio_cfg_
;
--—————————————————————————————————————————————————————— 维度贡献度占比对应风险水平-配置表 ————————————————————————————————————————————————————————————————————————————————--
drop table if exists pth_rmp.rmp_warn_dim_risk_level_cfg; 
create table pth_rmp.rmp_warn_dim_risk_level_cfg as  
with
warn_dim_risk_level_cfg_ as  -- 维度贡献度占比对应风险水平-配置表
(	
	select 1 as dimension,'财务' as remark,50 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc
	union all 
	select 1 as dimension,'财务' as remark,35 as low_contribution_percent,50 as high_contribution_percent,-2 as risk_lv ,'中风险' as risk_lv_desc  
	union all 
	select 1 as dimension,'财务' as remark,0 as low_contribution_percent,35 as high_contribution_percent,-1 as risk_lv ,'低风险' as risk_lv_desc   
	
	union all 
	select 2 as dimension,'经营' as remark,50 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc   
	union all 
	select 2 as dimension,'经营' as remark,35 as low_contribution_percent,50 as high_contribution_percent,-2 as risk_lv ,'中风险' as risk_lv_desc   
	union all 
	select 2 as dimension,'经营' as remark,0 as low_contribution_percent,35 as high_contribution_percent,-1 as risk_lv ,'低风险' as risk_lv_desc   
	
	union all 
	select 3 as dimension,'市场' as remark,15 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc   
	union all 
	select 3 as dimension,'市场' as remark,8 as low_contribution_percent,15 as high_contribution_percent,-2 as risk_lv ,'中风险' as risk_lv_desc   
	union all 
	select 3 as dimension,'市场' as remark,0 as low_contribution_percent,8 as high_contribution_percent,-1 as risk_lv ,'低风险' as risk_lv_desc   

	union all 
	select 4 as dimension,'舆情' as remark,15 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc   
	union all 
	select 4 as dimension,'舆情' as remark,8 as low_contribution_percent,15 as high_contribution_percent,-2 as risk_lv ,'中风险' as risk_lv_desc   
	union all 
	select 4 as dimension,'舆情' as remark,0 as low_contribution_percent,8 as high_contribution_percent,-1 as risk_lv ,'低风险' as risk_lv_desc   


	-- select 60 as low_contribution_percent,101 as high_contribution_percent,-3 as risk_lv ,'高风险' as risk_lv_desc   --(60,100]
	-- union all
	-- select 40 as low_contribution_percent,60 as high_contribution_percent,-2 as risk_lv,'中风险' as risk_lv_desc   --(40,60]
	-- union all
	-- select 0 as low_contribution_percent, 40 as high_contribution_percent,-1 as risk_lv,'低风险' as risk_lv_desc   --(0,40]
)
select * from warn_dim_risk_level_cfg_
;
--—————————————————————————————————————————————————————— 特征后手工配置表 ————————————————————————————————————————————————————————————————————————————————--
-- ps:excel导入
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

GRANT ALL ON URI "hdfs://htsecnew/user/pth_rmp/" TO ROLE pth_rmp;  --如果用户有all权限，导入数据还需要URI的权限
load data inpath '/user/pth_rmp/importfile/预警分-特征手工表.csv' 
into table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_;  

drop table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_BAC;
create table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_BAC as select * from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;
drop table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;
CREATE table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG as select * from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG_;
------------------------------ ------------------------------------------------------------------------------------

-- show grant role pth_rmp;  --查看用户pth_rmp的权限
GRANT ALL ON URI "hdfs://htsecnew/user/pth_rmp/" TO ROLE pth_rmp;  --如果用户有all权限，导入数据还需要URI的权限

load data inpath '/user/pth_rmp/importfile/预警分-特征手工表.csv' 
into table pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG;  --导入数据

export table pth_rmp.alert_score_summ to '/user/pth_rmp/alert_score_summ';  --导出数据

