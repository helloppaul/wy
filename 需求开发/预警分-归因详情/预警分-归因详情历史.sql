-- RMP_WARNING_SCORE_DETAIL_HIS (ͬ����ʽ��һ�쵥���β���) --
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
insert into pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS partition(dt=${ETL_DATE})
select distinct
    a.sid_kw,
    a.corp_id,
    a.corp_nm,
    a.score_dt,
    a.dimension,
    a.dim_warn_level,  --��������ģ���ںϷ����йأ�����
    a.type_cd,
    a.type,
    a.sub_model_name,
    a.idx_name,
    a.idx_value,   --������ָ��ֵ������Ҫת��ΪĿ�����չʾ��̬�������ñ�ĵ�λ���йأ���ʱ���ԭʼֵ
    a.idx_unit,  
    a.idx_score,   
    a.contribution_ratio,
    a.contribution_cnt,  
    a.factor_evaluate,
    a.median,  --������ ������
    a.last_idx_value,  
    a.idx_cal_explain,
    a.idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
  -- cast(from_unixtime(unix_timestamp(to_date(a.score_dt),'yyyy-MM-dd') ,'yyyyMMdd') as int) as dt
from pth_rmp.RMP_WARNING_SCORE_DETAIL a
join (select score_dt,max(batch_dt) as max_batch_dt from pth_rmp.RMP_WARNING_SCORE_DETAIL where delete_flag=0 group by score_dt) b
	on a.score_dt=b.score_dt and a.batch_dt=b.max_batch_dt
-- join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_WARNING_SCORE_DETAIL where delete_flag=0) b
-- 	on a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
  -- and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --����һ��ʼ����������������ε�����ͬ������ʷ��
;

