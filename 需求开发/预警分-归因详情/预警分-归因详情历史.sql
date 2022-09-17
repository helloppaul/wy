-- RMP_WARNING_SCORE_DETAIL_HIS (ͬ����ʽ��һ�쵥���β���) --
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
insert into pth_rmp.RMP_WARNING_SCORE_DETAIL_HIS partition(dt=${ETL_DATE})
select 
    sid_kw,
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    dim_warn_level,  --��������ģ���ںϷ����йأ�����
    type_cd,
    type,
    sub_model_name,
    idx_name,
    idx_value,   --������ָ��ֵ������Ҫת��ΪĿ�����չʾ��̬�������ñ�ĵ�λ���йأ���ʱ���ԭʼֵ
    idx_unit,  
    idx_score,   
    contribution_ratio,
    contribution_cnt,  
    factor_evaluate,
    median,  --������ ������
    last_idx_value,  
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_WARNING_SCORE_DETAIL a
join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_WARNING_SCORE_DETAIL where delete_flag=0) b
	on a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --����һ��ʼ����������������ε�����ͬ������ʷ��
;

