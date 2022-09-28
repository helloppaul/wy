-- RMP_WARNING_SCORE_MODEL_HIS (ͬ����ʽ��һ�쵥���β���) --
--������������������������������������������������������������������������������������������������������������ ������Ϣ ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
insert into pth_rmp.RMP_WARNING_SCORE_MODEL_HIS partition(dt=${ETL_DATE})
select 
	sid_kw,  --@impala
	corp_id,
	corp_nm,
	credit_cd,
	score_date,
	synth_warnlevel,  -- �ۺ�Ԥ���ȼ�
	synth_score,  -- Ԥ����
	model_version,
	adjust_warnlevel,   -- ������ȼ�
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_WARNING_SCORE_MODEL a
join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_WARNING_SCORE_MODEL where delete_flag=0) b
	on a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
--   and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --����һ��ʼ����������������ε�����ͬ������ʷ��
;

-- truncate table pth_rmp.RMP_WARNING_SCORE_MODEL; --��ʷ��������ɣ�ɾ��ǰһ����ձ�����