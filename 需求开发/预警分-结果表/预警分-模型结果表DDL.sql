-- 预警分-模型结果表 RMP_WARNING_SCORE_MODEL  --
create table pth_rmp.RMP_WARNING_SCORE_MODEL 
(
	sid_kw string,
	batch_dt string,
	corp_id string,
	corp_nm string,
	credit_cd string,
	score_date timestamp,
	synth_warnlevel string,
	synth_score double,
	model_version string,
	adjust_warnlevel string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
) stored as Parquet;