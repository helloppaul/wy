create or replace procedure sp_refresh_history_data_wy (out_msg out varchar2)
as
/***
	存储过程：sp_refresh_history_data
	创建时间：20221103
	创建人：hz
	源表：
	目标表：所有历史表
	功能：同步 当日表数据至历史表(注意：同步调度时间为，当天凌晨00:10分，清除的数据为当天前一天的数据)
***/
	v_error_msg varchar2(500);
	v_proc_name varchar2(100);
	v_exec_step varchar2(500);
	v_error_cd varchar2(100);
begin
	v_proc_name:='sp_refresh_history_data_wy';
	out_msg:='RUNNING';
	--重要关联方历史 
	delete from COMPANY_CORE_REL_HIS where relation_month=to_date(LAST_DAY(sysdate-1));   --清除历史表脏数据，如果有日期为昨日的，需要清除掉
	insert into COMPANY_CORE_REL_HIS (sid_kw,relation_month,corp_id,relation_id,relation_nm,rela_party_type,relation_type_l1_code,relation_type_l1,relation_type_l2_code,relation_type_l2,cum_ratio,compy_type,type6,rel_remark1,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			sid_kw,
			relation_dt,
			corp_id,
			relation_id,
			relation_nm,
			rela_party_type,
			relation_type_l1_code,
			relation_type_l1,
			relation_type_l2_code,
			relation_type_l2,
			cum_ratio,
			compy_type,
			type6,
			rel_remark1,
			delete_flag,
			create_by,
			create_time,
			update_by,
			sysdate,
			version
		from COMPANY_CORE_REL where relation_dt=to_date(LAST_DAY(sysdate-1)) and to_date(LAST_DAY(sysdate-1)) = to_date(sysdate-1);   --将当月表月底的数据插入到历史表
	delete from COMPANY_CORE_REL where relation_dt=to_date(LAST_DAY(sysdate-1)) and to_date(LAST_DAY(sysdate-1)) = to_date(sysdate-1);   --清除当日表昨天的数据
	commit;
	v_exec_step:='重要关联方历史执行成功！';

	--单主体舆情分历史
	delete from ALERT_SCORE_SUMM_HIS where score_dt=to_date(sysdate-1); 
	insert into ALERT_SCORE_SUMM_HIS (sid_kw,corp_id,corp_nm,credit_code,score_dt,score,score_hit,label_hit,alert,fluctuated,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			sid_kw,
			corp_id,
			corp_nm,
			credit_code,
			score_dt,
			score,
			score_hit,
			label_hit,
			alert,
			fluctuated,
			delete_flag,
			create_by,
			create_time,
			update_by,
			sysdate,
			version      
		from ALERT_SCORE_SUMM a 
		where a.batch_dt = (select max(batch_dt) from ALERT_SCORE_SUMM where score_dt = to_date(sysdate-1) ); 	
	delete from ALERT_SCORE_SUMM where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='单主体舆情分历史执行成功！';

	--综合舆情分历史
	delete from ALERT_COMPREHS_SCORE_HIS where score_dt=to_date(sysdate-1); 
	insert into ALERT_COMPREHS_SCORE_HIS (sid_kw,corp_id,corp_nm,credit_code,score_dt,comprehensive_score,score_hit,label_hit,alert,fluctuated,model_version,adjust_warnlevel,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.credit_code,
			a.score_dt,
			a.comprehensive_score,
			a.score_hit,
			a.label_hit,
			a.alert,
			a.fluctuated,
			a.model_version,
			a.adjust_warnlevel,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from ALERT_COMPREHS_SCORE a 
		where a.batch_dt = (select max(batch_dt) from ALERT_COMPREHS_SCORE where score_dt = to_date(sysdate-1) ); 
	delete from ALERT_COMPREHS_SCORE where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='综合舆情分历史执行成功！';

	--舆情分-归因详报历史
	delete from ATTRIBUTION_SUMM_HIS where score_dt=to_date(sysdate-1); 
	insert into ATTRIBUTION_SUMM_HIS (sid_kw,corp_id,corp_nm,credit_cd,score_dt,report_msg1,report_msg2,report_msg5,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.credit_cd,
			a.score_dt,
			a.report_msg1,
			a.report_msg2,
			a.report_msg5,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from ATTRIBUTION_SUMM a 
		where a.batch_dt = (select max(batch_dt) from ATTRIBUTION_SUMM where score_dt = to_date(sysdate-1) ); 
	delete from ATTRIBUTION_SUMM where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='舆情分-归因详报历史 执行成功！';

	--舆情分-贡献度排行榜历史
	delete from COMPY_CONTRIB_DEGREE_HIS where score_dt=to_date(sysdate-1);
	insert into COMPY_CONTRIB_DEGREE_HIS (sid_kw,corp_id,corp_nm,score_dt,relation_id,relation_nm,relation_type_l2_line,contribution_degree,rank_num,abnormal_flag,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.score_dt,
			a.relation_id,
			a.relation_nm,
			a.relation_type_l2_line,
			a.contribution_degree,
			a.rank_num,
			a.abnormal_flag,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from COMPY_CONTRIB_DEGREE a 
		where a.batch_dt = (select max(batch_dt) from COMPY_CONTRIB_DEGREE where score_dt = to_date(sysdate-1) ); 
	delete from COMPY_CONTRIB_DEGREE where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='舆情分-贡献度排行榜历史 执行成功！';

	--预警分-归因详情历史
	delete from WARNING_SCORE_DETAIL_HIS where score_dt=to_date(sysdate-1);
	insert into WARNING_SCORE_DETAIL_HIS (sid_kw,corp_id,corp_nm,score_dt,dimension,dim_warn_level,type_cd,type,sub_model_name,idx_name,idx_value,idx_unit,idx_score,contribution_ratio,contribution_cnt,factor_evaluate,median,last_idx_value,idx_cal_explain,idx_explain,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.score_dt,
			a.dimension,
			a.dim_warn_level,
			a.type_cd,
			a.type,
			a.sub_model_name,
			a.idx_name,
			a.idx_value,
			a.idx_unit,
			a.idx_score,
			a.contribution_ratio,
			a.contribution_cnt,
			a.factor_evaluate,
			a.median,
			a.last_idx_value,
			a.idx_cal_explain,
			a.idx_explain,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from WARNING_SCORE_DETAIL a 
		where a.batch_dt = (select max(batch_dt) from WARNING_SCORE_DETAIL where score_dt = to_date(sysdate-1) ); 
	delete from WARNING_SCORE_DETAIL where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='预警分-归因详情历史 执行成功！';

	--预警分-归因详报历史
	delete from WARNING_SCORE_REPORT_HIS where score_dt=to_date(sysdate-1); 
	insert into WARNING_SCORE_REPORT_HIS (sid_kw,corp_id,corp_nm,credit_cd,score_dt,report_msg1,report_msg2,report_msg3,report_msg4,report_msg5,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.credit_cd,
			a.score_dt,
			a.report_msg1,
			a.report_msg2,
			a.report_msg3,
			a.report_msg4,
			a.report_msg5,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from WARNING_SCORE_REPORT a 
		where a.batch_dt = (select max(batch_dt) from WARNING_SCORE_REPORT where score_dt = to_date(sysdate-1) ); 
	delete from WARNING_SCORE_REPORT where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='预警分-归因详报历史 执行成功！';

	--预警分-归因简报历史
	delete from WARNING_SCORE_S_REPORT_HIS where score_dt=to_date(sysdate-1);
	insert into WARNING_SCORE_S_REPORT_HIS (sid_kw,corp_id,corp_nm,score_dt,report_msg,model_version,delete_flag,create_by,create_time,update_by,update_time,version)
		select 
			a.sid_kw,
			a.corp_id,
			a.corp_nm,
			a.score_dt,
			a.report_msg,
			a.model_version,
			a.delete_flag,
			a.create_by,
			a.create_time,
			a.update_by,
			sysdate,
			a.version
		from WARNING_SCORE_S_REPORT a 
		where a.batch_dt = (select max(batch_dt) from WARNING_SCORE_S_REPORT where score_dt = to_date(sysdate-1) ); 
	delete from WARNING_SCORE_S_REPORT where score_dt=to_date(sysdate-1);
	commit;
	v_exec_step:='预警分-归因简报历史 执行成功！';

	out_msg:='SUCESSS';
	exception 
		when others then 
			rollback;
		v_error_cd:=sqlcode;
		v_error_msg:=substr(sqlerrm,1,300) || v_exec_step;
		commit;
  	dbms_output.put_line('sp_refresh_history_data_wy procedure execute success!');
end sp_refresh_history_data_wy;