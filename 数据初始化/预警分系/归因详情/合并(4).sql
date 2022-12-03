-- RMP_WARNING_SCORE_DETAIL_INIT 9min --

create table pth_rmp.rmp_warning_score_detail_init_impala stored as parquet as 
--������������������������������������������������������������������������������������������������������������ ���ñ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
with
feat_CFG as  --�����ֹ����ñ�
(
    select distinct
        feature_cd,
        feature_name,
        sub_model_type,  --��Ƶ-����ƽ̨����Ƶ-ҽҩ���� ...
        -- substr(sub_model_type,1,6) as sub_model_type,  --ȡǰ���������ַ�
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type not in ('��Ƶ-��ҵ','��Ƶ-��Ͷ','�޼ල')
    union all 
    select distinct
        feature_cd,
        feature_name,
        sub_model_type,
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type in ('��Ƶ-��ҵ','��Ƶ-��Ͷ','�޼ල')
),
--ӳ��� �����ֹ����ñ� --
warn_feat_CFG as
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,    --��Ƶ-����ƽ̨����Ƶ-ҽҩ���� ...
        feature_name_target,
        case dimension 
            when '����' then 1
            when '��Ӫ' then 2
            when '�г�' then 3
            when '����' then 4
            when '�쳣���ռ��' then 5
        end as dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
        -- count(feature_cd) over(partition by dimension,type) as contribution_cnt
    from feat_CFG
),
--������������������������������������������������������������������������������������������������������������ �ӿڲ� ����������������������������������������������������������������������������������������������������������������������������������������������������������������--
-- ����� --
res0 as   --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)  ��:1min  67����
(
    select distinct STRAIGHT_JOIN
        c.batch_dt,
        c.corp_id,
        c.corp_nm,
        c.score_dt,
        c.feature_name as idx_name,
        c.feature_risk_interval,  --���е�Ƶ�ϲ����������׶ȱ�� �쳣ָ���ʶ(ģ��ֱ���ṩ)
        case 
            when c.feature_risk_interval=1 and b.idx_value is not null then 
                0  --�쳣 
            else 1 --����
        end as factor_evaluate,
        b.idx_value,   --����ָ��ֵ  ps:��Ϊ�գ�ֱ�ӱ���NULL�������������ԭʼֵ��Ĭ��ֵ
        b.lst_idx_value as last_idx_value,  --����ָ��ֵ
        '' as idx_unit,   
        c.model_freq_type,   --���ø��е�Ƶ�ϲ��������׶ȵ� �����ֹ�ά�����������Ƶ�ģ�ͷ��� 2022-11-12
        c.sub_model_name,   --���ø��е�Ƶ�ϲ��������׶ȵ� ����ģ���Դ�����ģ��Ӣ������ 2022-11-12
        b.median  
    from warn_feature_contrib c   --��Ƶ�ϲ����������׶�  
    -- join  warn_union_adj_sync_score main --Ԥ����
    --     on main.batch_dt=c.batch_dt and main.corp_id=c.corp_id
    left join [SHUFFLE] warn_feature_value_with_median_res b  --��Ƶ�ϲ�������ԭʼֵ
        on  c.corp_id=b.corp_id 
        -- and c.batch_dt=b.batch_dt 
        and c.score_dt=b.score_dt
        and c.feature_name=b.idx_name 
        and c.sub_model_name=b.sub_model_name
),
res1 as   --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺ��������׶�(�޼ල) 
(
    select distinct STRAIGHT_JOIN
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        b.contribution_ratio,  --�ۺ�Ԥ���ȼ�-�������׶� �Ĺ��׶�ռ��
        main.factor_evaluate,  --��������
        b.sub_model_name as sub_model_name_zhgxd   --�ۺ�Ԥ���ȼ�-�������׶ȵ���ģ������
    from res0 main
    left join [SHUFFLE] (select * from warn_contribution_ratio where feature_name <> 'creditrisk_highfreq_unsupervised') b
        on  main.corp_id=b.corp_id 
            -- and main.batch_dt=b.batch_dt
            and main.score_dt=b.score_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.feature_name
    union all 
    --�������׶ȵ��޼ල��ģ�� ���⴦��  ��ֻ�й��׶�ռ�����ݣ������Ϊ�գ������������Ӳ��棬ͣ����dimension�㣩
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        feature_name as idx_name,
        NULL as idx_value,
        NULL as last_idx_value,
        '' as idx_unit,
        '�޼ල' model_freq_type,
        sub_model_name,
        NULL as median,
        contribution_ratio,
        NULL as factor_evaluate, 
        '' as sub_model_name_zhgxd 
    from ( select  a1.* FROM warn_contribution_ratio a1
            where a1.feature_name = 'creditrisk_highfreq_unsupervised'
        ) A 
),
res2 as --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺϹ��׶�+ָ�����ֿ� ��:1min20s  67����
(
    select distinct STRAIGHT_JOIN
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,   --�ۺϹ��׶ȵ���ģ������
        b.idx_score,
        b.sub_model_name as sub_model_name_zbpfk  --ָ�����ֿ�����ģ������
    from  res1 main 
    left join [SHUFFLE] warn_score_card b 
        on  main.corp_id=b.corp_id 
            -- and main.batch_dt=b.batch_dt 
            and main.score_dt=b.score_dt
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.idx_name
),
res3 as   --Ԥ����+����ԭʼֵ+�ۺϹ��׶�+ָ�����ֿ�+�������ñ�  ��:1min20s  40����
(
    select  STRAIGHT_JOIN
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.idx_name,
        main.idx_value,
        main.last_idx_value,
        f_cfg.unit_target as idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        main.median,
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,  --�ۺϹ��׶ȵ���ģ������
        main.idx_score,
        main.sub_model_name_zbpfk,
        f_cfg.sub_model_type,
        f_cfg.feature_name_target,
        f_cfg.dimension,
        f_cfg.type,
        f_cfg.cal_explain as idx_cal_explain,
        f_cfg.feature_explain as idx_explain,
        f_cfg.unit_origin,
        f_cfg.unit_target,
        count(*) over(partition by main.batch_dt,main.corp_id,main.score_dt,f_cfg.dimension,f_cfg.type) as  contribution_cnt  --����������㣬���ڸ�ʱ����ҵ��Ӧtype���ָ�����ͳ��
        -- f_cfg.contribution_cnt  --�������
    from res2 main
    join [BROADCAST] warn_feat_CFG f_cfg
        on  main.idx_name=f_cfg.feature_cd and main.model_freq_type=f_cfg.sub_model_type --and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    -- left join warn_feat_CFG f_cfg
),
res4 as -- --Ԥ����+����ԭʼֵ(����ԭʼֵ�����Ը��е�Ƶ�ϲ����������׶ȱ��е���������Ϊ׼)+�ۺϹ��׶�+ָ�����ֿ�+�������ñ�+��ά�ȷ���ˮƽ(���е�Ƶ���׶����)   ��:1min20s  34����
(
    select distinct STRAIGHT_JOIN
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.feature_name_target as idx_name,  --���һ����idx_name����Ϊ����ҳ��չʾ��ʽ��ָ������
        case 
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.idx_value/100000000,2) as decimal(10,2))
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.idx_value/10000,2) as decimal(10,2))
            when main.unit_origin='��ֵ' and main.unit_target='%' then 
                cast(round(main.idx_value*100,2) as decimal(10,2))
            when main.unit_origin='��' and main.unit_target='����' then 
                cast(round(main.idx_value/10000,2) as decimal(10,2))
            else 
                cast(round(main.idx_value,2) as decimal(10,2))
        end as idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        case 
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.median/100000000,2) as decimal(10,2))
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.median/10000,2) as decimal(10,2))
            when main.unit_origin='��ֵ' and main.unit_target='%' then 
                cast(round(main.median*100,2) as decimal(10,2))
            when main.unit_origin='��' and main.unit_target='����' then 
                cast(round(main.median/10000,2) as decimal(10,2))
            else 
                cast(round(main.median,2) as decimal(10,2))
        end as median, 
        main.contribution_ratio,  --���׶�ռ��
        main.factor_evaluate,  --��������
        main.sub_model_name_zhgxd,  --�ۺϹ��׶ȵ���ģ������
        main.idx_score,
        main.sub_model_name_zbpfk,
        main.sub_model_type,
        main.feature_name_target,
        main.dimension,
        b.dim_warn_level,  --���յ������ά�ȷ��յȼ�(���ѵ�)
        main.type,
        main.idx_cal_explain,
        main.idx_explain,
        case 
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.last_idx_value/100000000,2) as decimal(10,2))
            when main.unit_origin='Ԫ' and main.unit_target='��Ԫ' then 
                cast(round(main.last_idx_value/10000,2) as decimal(10,2))
            when main.unit_origin='��ֵ' and main.unit_target='%' then 
                cast(round(main.last_idx_value*100,2) as decimal(10,2))
            when main.unit_origin='��' and main.unit_target='����' then 
                cast(round(main.last_idx_value/10000,2) as decimal(10,2))
            else 
                cast(round(main.last_idx_value,2) as decimal(10,2))
        end as last_idx_value,
        main.unit_origin,
        main.unit_target,
        main.contribution_cnt,  --�������
        main.idx_name as ori_idx_name,   --ԭʼָ������
        b.dim_submodel_contribution_ratio  --��ά���쳣ָ��ռ��
    from res3 main
    left join [SHUFFLE] warn_feature_contrib_res3 b  --��ȡά�ȷ��յȼ����ݣ�left join ���ⶪʧ�޼ල����
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id and main.dimension=b.dimension
)
------------------------------------���ϲ���Ϊ��ʱ��-------------------------------------------------------------------
-- insert into pth_rmp.RMP_WARNING_SCORE_DETAIL partition(etl_date=${ETL_DATE})
select 
    -- concat(corp_id,'_',MD5(concat(batch_dt,dimension,type,sub_model_name,idx_name))) as sid_kw,  --hive
    batch_dt,
    corp_id,
    corp_nm,
    score_dt,
    dimension,
    dim_warn_level,  
    0 as type_cd,
    type,
    sub_model_name,
    idx_name,   --ת����Ϊ����ҳ��չʾ��ʽ��ָ������
    idx_value,   --������ָ��ֵ������Ҫת��ΪĿ�����չʾ��̬�������ñ�ĵ�λ���йأ���ʱ���ԭʼֵ
    idx_unit,  
    idx_score,   
    cast(contribution_ratio as float) as contribution_ratio,   --���׶�ռ�� ��ת��Ϊ �ٷֱ�
    contribution_cnt,  
    factor_evaluate,
    median,  --������ ������
    last_idx_value,  --������
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version,
    ori_idx_name,  --ԭʼָ�����ƣ������걨�ڶ��Σ����Ķ�ʹ��
    dim_submodel_contribution_ratio  --��ά���쳣ָ��ռ�ȣ�uesed ���򱨸���Ķ� �Լ� �����wy
from res4
-- where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
; 





--��2��DDL RMP_WARNING_SCORE_DETAIL_INIT hiveִ�� --
-- �������� RMP_WARNING_SCORE_DETAIL --
drop table if exists pth_rmp.rmp_warning_score_detail_init;
create table pth_rmp.rmp_warning_score_detail_init
(
    sid_kw  string,
    corp_id string,
    corp_nm string,
    score_dt timestamp,
    dimension int,
    dim_warn_level string,
    type_cd int,
    type string,
    sub_model_name string,
    idx_name string,
    idx_value float,
    idx_unit string,
    idx_score float,
    contribution_ratio float,
    contribution_cnt bigint,
    factor_evaluate int,
    median  float,
    last_idx_value float,
    idx_cal_explain string,
    idx_explain string,
	delete_flag	int,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	int,
    ori_idx_name string,
    dim_submodel_contribution_ratio float
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;


--��3�� hvieִ��
insert into pth_rmp.rmp_warning_score_detail_init partition(etl_date=19900101) 
select 
    concat(corp_id,'_',MD5(concat(batch_dt,dimension,type,sub_model_name,idx_name))) as sid_kw,  --hive
    corp_id ,
    corp_nm ,
    score_dt ,
    dimension ,
    dim_warn_level ,
    type_cd ,
    type ,
    sub_model_name ,
    idx_name ,
    idx_value ,
    idx_unit ,
    idx_score ,
    contribution_ratio ,
    contribution_cnt ,
    factor_evaluate ,
    median  ,
    last_idx_value ,
    idx_cal_explain ,
    idx_explain ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version	,
    ori_idx_name,
    dim_submodel_contribution_ratio
from pth_rmp.rmp_warning_score_detail_init_impala
;