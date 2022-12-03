create or replace procedure sp_company_core_rel_clean (out_msg out varchar2)
as
/***
	存储过程：sp_company_core_rel_clean
	创建时间：20221103
	创建人：hz
	源表：
	目标表：所有历史表
	功能：清理 重要关联方月度表（PS：只保留最新一天数据，且调度执行时间为凌晨01:10） 
***/
	v_error_msg varchar2(500);
	v_proc_name varchar2(100);
	v_exec_step varchar2(500);
	v_error_cd varchar2(100);
begin
	v_proc_name:='sp_company_core_rel_clean';
	out_msg:='RUNNING';

	delete from COMPANY_CORE_REL where relation_dt < (select max(relation_dt) from COMPANY_CORE_REL);
	commit;

	out_msg:='SUCESSS';
	exception 
		when others then 
			rollback;
		v_error_cd:=sqlcode;
		v_error_msg:=substr(sqlerrm,1,300) || v_exec_step;
		commit;
  	dbms_output.put_line('sp_company_core_rel_clean procedure execute success!');
end sp_company_core_rel_clean;