CREATE TABLE pth_rmp.WARNING_SCORE_FEATURE_CFG
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


-- show grant role pth_rmp;  --查看用户pth_rmp的权限
GRANT ALL ON URI "hdfs://htsecnew/user/pth_rmp/" TO ROLE pth_rmp;  --如果用户有all权限，导入数据还需要URI的权限

load data inpath '/user/path_rmp/importfile/WARNING_SCORE_FEATURE_CFG.csv' 
into table pth_rmp.WARNING_SCORE_FEATURE_CFG;  --导入数据

export table pth_rmp.alert_score_summ to '/user/pth_rmp/alert_score_summ';  --导出数据

