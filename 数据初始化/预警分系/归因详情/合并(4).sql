-- RMP_WARNING_SCORE_DETAIL_INIT 9min --

create table pth_rmp.rmp_warning_score_detail_init_impala stored as parquet as 
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
with
feat_CFG as  --特征手工配置表
(
    select distinct
        feature_cd,
        feature_name,
        sub_model_type,  --低频-金融平台、低频-医药制造 ...
        -- substr(sub_model_type,1,6) as sub_model_type,  --取前两个中文字符
        feature_name_target,
        dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
    from pth_rmp.RMP_WARNING_SCORE_FEATURE_CFG
    where sub_model_type not in ('中频-产业','中频-城投','无监督')
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
    where sub_model_type in ('中频-产业','中频-城投','无监督')
),
--映射后 特征手工配置表 --
warn_feat_CFG as
(
    select 
        feature_cd,
        feature_name,
        sub_model_type,    --低频-金融平台、低频-医药制造 ...
        feature_name_target,
        case dimension 
            when '财务' then 1
            when '经营' then 2
            when '市场' then 3
            when '舆情' then 4
            when '异常风险检测' then 5
        end as dimension,
        type,
        cal_explain,
        feature_explain,
        unit_origin,
        unit_target
        -- count(feature_cd) over(partition by dimension,type) as contribution_cnt
    from feat_CFG
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
-- 结果集 --
res0 as   --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)  慢:1min  67万条
(
    select distinct STRAIGHT_JOIN
        c.batch_dt,
        c.corp_id,
        c.corp_nm,
        c.score_dt,
        c.feature_name as idx_name,
        c.feature_risk_interval,  --高中低频合并的特征贡献度表的 异常指标标识(模型直接提供)
        case 
            when c.feature_risk_interval=1 and b.idx_value is not null then 
                0  --异常 
            else 1 --正常
        end as factor_evaluate,
        b.idx_value,   --今日指标值  ps:若为空，直接保留NULL，不随意对特征原始值赋默认值
        b.lst_idx_value as last_idx_value,  --昨日指标值
        '' as idx_unit,   
        c.model_freq_type,   --改用高中低频合并特征贡献度的 代码手工维护的中文名称的模型分类 2022-11-12
        c.sub_model_name,   --改用高中低频合并特征贡献度的 上游模型自带的子模型英文名称 2022-11-12
        b.median  
    from warn_feature_contrib c   --三频合并的特征贡献度  
    -- join  warn_union_adj_sync_score main --预警分
    --     on main.batch_dt=c.batch_dt and main.corp_id=c.corp_id
    left join [SHUFFLE] warn_feature_value_with_median_res b  --三频合并的特征原始值
        on  c.corp_id=b.corp_id 
        -- and c.batch_dt=b.batch_dt 
        and c.score_dt=b.score_dt
        and c.feature_name=b.idx_name 
        and c.sub_model_name=b.sub_model_name
),
res1 as   --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合特征贡献度(无监督) 
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
        b.contribution_ratio,  --综合预警等级-特征贡献度 的贡献度占比
        main.factor_evaluate,  --因子评价
        b.sub_model_name as sub_model_name_zhgxd   --综合预警等级-特征贡献度的子模型名称
    from res0 main
    left join [SHUFFLE] (select * from warn_contribution_ratio where feature_name <> 'creditrisk_highfreq_unsupervised') b
        on  main.corp_id=b.corp_id 
            -- and main.batch_dt=b.batch_dt
            and main.score_dt=b.score_dt 
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.feature_name
    union all 
    --特征贡献度的无监督子模型 特殊处理  （只有贡献度占比数据，其余均为空，不下钻至因子层面，停留在dimension层）
    select distinct
        batch_dt,
        corp_id,
        corp_nm,
        score_dt,
        feature_name as idx_name,
        NULL as idx_value,
        NULL as last_idx_value,
        '' as idx_unit,
        '无监督' model_freq_type,
        sub_model_name,
        NULL as median,
        contribution_ratio,
        NULL as factor_evaluate, 
        '' as sub_model_name_zhgxd 
    from ( select  a1.* FROM warn_contribution_ratio a1
            where a1.feature_name = 'creditrisk_highfreq_unsupervised'
        ) A 
),
res2 as --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合贡献度+指标评分卡 慢:1min20s  67万条
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
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,   --综合贡献度的子模型名称
        b.idx_score,
        b.sub_model_name as sub_model_name_zbpfk  --指标评分卡的字模型名称
    from  res1 main 
    left join [SHUFFLE] warn_score_card b 
        on  main.corp_id=b.corp_id 
            -- and main.batch_dt=b.batch_dt 
            and main.score_dt=b.score_dt
            and main.sub_model_name=b.sub_model_name 
            and main.idx_name=b.idx_name
),
res3 as   --预警分+特征原始值+综合贡献度+指标评分卡+特征配置表  慢:1min20s  40万条
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
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,  --综合贡献度的子模型名称
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
        count(*) over(partition by main.batch_dt,main.corp_id,main.score_dt,f_cfg.dimension,f_cfg.type) as  contribution_cnt  --归因个数计算，基于该时点企业对应type层的指标个数统计
        -- f_cfg.contribution_cnt  --归因个数
    from res2 main
    join [BROADCAST] warn_feat_CFG f_cfg
        on  main.idx_name=f_cfg.feature_cd and main.model_freq_type=f_cfg.sub_model_type --and  main.model_freq_type=substr(f_cfg.sub_model_type,1,6)
    -- left join warn_feat_CFG f_cfg
),
res4 as -- --预警分+特征原始值(特征原始值名称以高中低频合并的特征贡献度表中的特征名称为准)+综合贡献度+指标评分卡+特征配置表+各维度风险水平(高中低频贡献度求得)   慢:1min20s  34万条
(
    select distinct STRAIGHT_JOIN
        main.batch_dt,
        main.corp_id,
        main.corp_nm,
        main.score_dt,
        main.feature_name_target as idx_name,  --最后一步将idx_name调成为最终页面展示形式的指标名称
        case 
            when main.unit_origin='元' and main.unit_target='亿元' then 
                cast(round(main.idx_value/100000000,2) as decimal(10,2))
            when main.unit_origin='元' and main.unit_target='万元' then 
                cast(round(main.idx_value/10000,2) as decimal(10,2))
            when main.unit_origin='数值' and main.unit_target='%' then 
                cast(round(main.idx_value*100,2) as decimal(10,2))
            when main.unit_origin='人' and main.unit_target='万人' then 
                cast(round(main.idx_value/10000,2) as decimal(10,2))
            else 
                cast(round(main.idx_value,2) as decimal(10,2))
        end as idx_value,
        main.idx_unit,
        main.model_freq_type,
        main.sub_model_name,
        case 
            when main.unit_origin='元' and main.unit_target='亿元' then 
                cast(round(main.median/100000000,2) as decimal(10,2))
            when main.unit_origin='元' and main.unit_target='万元' then 
                cast(round(main.median/10000,2) as decimal(10,2))
            when main.unit_origin='数值' and main.unit_target='%' then 
                cast(round(main.median*100,2) as decimal(10,2))
            when main.unit_origin='人' and main.unit_target='万人' then 
                cast(round(main.median/10000,2) as decimal(10,2))
            else 
                cast(round(main.median,2) as decimal(10,2))
        end as median, 
        main.contribution_ratio,  --贡献度占比
        main.factor_evaluate,  --因子评价
        main.sub_model_name_zhgxd,  --综合贡献度的子模型名称
        main.idx_score,
        main.sub_model_name_zbpfk,
        main.sub_model_type,
        main.feature_name_target,
        main.dimension,
        b.dim_warn_level,  --最终调整后的维度风险等级(重难点)
        main.type,
        main.idx_cal_explain,
        main.idx_explain,
        case 
            when main.unit_origin='元' and main.unit_target='亿元' then 
                cast(round(main.last_idx_value/100000000,2) as decimal(10,2))
            when main.unit_origin='元' and main.unit_target='万元' then 
                cast(round(main.last_idx_value/10000,2) as decimal(10,2))
            when main.unit_origin='数值' and main.unit_target='%' then 
                cast(round(main.last_idx_value*100,2) as decimal(10,2))
            when main.unit_origin='人' and main.unit_target='万人' then 
                cast(round(main.last_idx_value/10000,2) as decimal(10,2))
            else 
                cast(round(main.last_idx_value,2) as decimal(10,2))
        end as last_idx_value,
        main.unit_origin,
        main.unit_target,
        main.contribution_cnt,  --归因个数
        main.idx_name as ori_idx_name,   --原始指标名称
        b.dim_submodel_contribution_ratio  --各维度异常指标占比
    from res3 main
    left join [SHUFFLE] warn_feature_contrib_res3 b  --获取维度风险等级数据，left join 以免丢失无监督数据
        on main.batch_dt=b.batch_dt and main.corp_id=b.corp_id and main.dimension=b.dimension
)
------------------------------------以上部分为临时表-------------------------------------------------------------------
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
    idx_name,   --转换成为最终页面展示形式的指标名称
    idx_value,   --！！！指标值最终需要转换为目标输出展示形态，和配置表的单位列有关，暂时输出原始值
    idx_unit,  
    idx_score,   
    cast(contribution_ratio as float) as contribution_ratio,   --贡献度占比 已转换为 百分比
    contribution_cnt,  
    factor_evaluate,
    median,  --！！！ 待测试
    last_idx_value,  --！！！
    idx_cal_explain,
    idx_explain,
    0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() as update_time,
	0 as version,
    ori_idx_name,  --原始指标名称，用于详报第二段，第四段使用
    dim_submodel_contribution_ratio  --各维度异常指标占比，uesed 归因报告第四段 以及 归因简报wy
from res4
-- where score_dt = to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
; 





--（2）DDL RMP_WARNING_SCORE_DETAIL_INIT hive执行 --
-- 归因详情 RMP_WARNING_SCORE_DETAIL --
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


--（3） hvie执行
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