-- ����ͳ����ʷ RMP_OPINION_STATISTIC_HIS (ͬ����ʽ��һ�쵥����) --
insert into pth_rmp.RMP_OPINION_STATISTIC_HIS partition(dt=${ETL_DATE})
select 
	select distinct
	current_timestamp() as batch_dt,
	score_dt,   --����������oracleʱ�������͸��ֶ�
	statistic_dim,  --ͳ��ά�ȣ�1:'��ҵ' 2:'����'
	industry_class,  -- -1:���� 1:'������ҵ' 2:'wind��ҵ' 3:'������ҵ' 4:'֤�����ҵ'  99:δ֪��ҵ
	importance,
	level_type_list,
	level_type_ii,
	opinion_cnt,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from pth_rmp.RMP_OPINION_STATISTIC_DAY a 
join (select max(batch_dt) as max_batch_dt from pth_rmp.RMP_OPINION_STATISTIC_DAY where delete_flag=0) b
	on a.batch_dt=b.max_batch_dt
where a.delete_flag=0
  and a.score_dt=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),-1))  --����һ��ʼ����������������ε�����ͬ������ʷ��