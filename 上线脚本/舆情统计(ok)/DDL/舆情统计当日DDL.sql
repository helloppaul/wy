-- ����ͳ�Ƶ��� RMP_OPINION_STATISTIC_DAY --
drop table  if exists pth_rmp.RMP_OPINION_STATISTIC_DAY ;
create table pth_rmp.RMP_OPINION_STATISTIC_DAY 
(	
	sid_kw string,
	batch_dt string,
	score_dt timestamp,   --����������oracleʱ�������͸��ֶ�
	statistic_dim int,  --ͳ��ά�ȣ�1:'��ҵ' 2:'����'
	industry_class int,  -- -1:���� 1:'������ҵ' 2:'wind��ҵ' 3:'������ҵ' 4:'֤�����ҵ'  99:δ֪��ҵ
	importance int,
	level_type_list string,
	level_type_ii string,
	opinion_cnt bigint,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)partitioned by (etl_date int)
 stored as textfile
;