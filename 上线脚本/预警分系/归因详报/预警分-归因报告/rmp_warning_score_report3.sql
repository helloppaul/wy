-- RMP_WARNING_SCORE_REPORT 第三段-同类风险企业 --
-- /* 2022-12-20 drop+create table -> insert into overwrite table xxx */
-- /* 2023-01-01 model_version_intf_ 改取用视图数据 */
-- /* 2023-01-09 最终输出结果增加distinct去重 */
-- /* 2023-01-08  代码效率优化且增加两个参数优化语句 */



set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15; 
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


-- drop table if exists pth_rmp.rmp_warning_score_report3;  
-- create table pth_rmp.rmp_warning_score_report3 as  --@pth_rmp.rmp_warning_score_report3
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业/证监会一级行业 的特殊corp_chg  (特殊2)
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,  --1 产业债 2 城投债
	b.industryphy_name,
	b.zjh_industry_l1 
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  where cid1.etl_date in (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance)
			-- on cid1.etl_date=cid2.etl_date
		 )	a 
	join (select b1.* from pth_rmp.rmp_company_info_main b1 
		  where b1.etl_date in (select max(etl_date) etl_date from pth_rmp.rmp_company_info_main )
		  	-- on b1.etl_date=b2.etl_date
		) b 
		on a.corp_id=b.corp_id --and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
-- 时间限制开关 --
timeLimit_switch as 
(
    select True as flag   --TRUE:时间约束，FLASE:时间不做约束，通常用于初始化
    -- select False as flag
),
-- 模型版本控制 --
model_version_intf_ as   --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf   @app_ehzh.rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf
(
	select * from pth_rmp.v_model_version  --见 预警分-配置表中的视图
    -- select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --低频模型
    -- union all
    -- select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --中频-城投模型
    -- union all 
    -- select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --中频-产业模型
    -- union all 
    -- select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --高频-评分卡模型(高频)
    -- union all 
    -- select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --高频-无监督模型
    -- union all 
    -- select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --信用风险综合模型
),
-- 预警分 --
rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --预警分_融合调整后综合  原始接口
(
	select a.*
    from 
    (
		select m.*
		from
		(
			-- 时间限制部分 --
			select *,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 = 1--in (select max(flag) from timeLimit_switch) 
			  and etl_date=${ETL_DATE}
			  and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
		) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
),
rmp_cs_compy_region_ as   -- 区域经营数据 (每日全量采集)
(
	select a.*
	from hds.t_ods_rmp_cs_compy_region a 
	where a.isdel=0
	  and a.etl_date in (select max(etl_date) as max_etl_date from hds.t_ods_rmp_cs_compy_region)
),
RMP_WARNING_SCORE_MODEL_ as  --预警分-模型结果表
(
    select distinct
        cast(a.rating_dt as string) as batch_dt,
        chg.corp_id,
        chg.corp_name as corp_nm,
		chg.credit_code as credit_cd,
        to_date(a.rating_dt) as score_date,
        a.total_score_adjusted as synth_score,  -- 预警分
		case a.interval_text_adjusted
			when '绿色预警' then '-1' 
			when '黄色预警' then '-2'
			when '橙色预警' then '-3'
			when '红色预警' then '-4'
			when '风险已暴露' then '-5'
		end as synth_warnlevel,  -- 综合预警等级,
		case
			when a.interval_text_adjusted in ('绿色预警','黄色预警') then 
				'-1'   --低风险
			when a.interval_text_adjusted  = '橙色预警' then 
				'-2'  --中风险
			when a.interval_text_adjusted  ='红色预警' then 
				'-3'  --高风险
			when a.interval_text_adjusted  ='风险已暴露' then 
				'-4'   --风险已暴露
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
RMP_WARNING_SCORE_MODEL_Batch as  -- 取每天最新批次数据
(
	select a.*
	from RMP_WARNING_SCORE_MODEL_ a 
	join (select max(batch_dt) as max_batch_dt,score_date from RMP_WARNING_SCORE_MODEL_ group by score_date) b
		on a.batch_dt=b.max_batch_dt and a.score_date=b.score_date
),
-- 第三段数据 --
mid_rmp_cs_compy_region_ as 
(
	select distinct
		b.corp_id,
		b.corp_name as corp_nm,
		a.region_cd,
		a.client_id
	from rmp_cs_compy_region_ a 
	join (select * from corp_chg where source_code='CSCS') b 
		on cast(a.company_id as string)=b.source_id 
),
Third_Part_Data_Prepare as 
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		main.synth_warnlevel,  -- 综合预警等级 used
		chg.bond_type,
		chg.zjh_industry_l1
	from RMP_WARNING_SCORE_MODEL_Batch main 
	join (select * from corp_chg where source_code='ZXZX') chg 
		on main.corp_id=chg.corp_id
),
Third_Part_Data_CY_Prepare as   -- 主体为产业的数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type,   -- 属性1：产业
		'' as bond_type_desc,
		a.zjh_industry_l1 as corp_property,  -- 属性2：行业
		concat(a.zjh_industry_l1,'中') as corp_property_desc,
		b.corp_id as same_property_corp_id,   --主体为产业债性质 的 同行业且综合预警等级相等 的 企业
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_Prepare a
	join (select * from Third_Part_Data_Prepare where bond_type <>2 ) b 
		on  a.zjh_industry_l1= b.zjh_industry_l1 and a.synth_warnlevel=b.synth_warnlevel  --综合预警等级相同的企业
	where a.bond_type <>2  --产业债
	  and a.corp_id<>b.corp_id
),
Third_Part_Data_CY as    -- 和产业主体相同属性的 其他企业数量 计算
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CY_Prepare
),
Third_Part_Data_CT_Prepare_I as -- 主体 为 城投的数据
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 
		'城投平台敞口' as bond_type_desc,
		b.region_cd
	from Third_Part_Data_Prepare a
	join mid_rmp_cs_compy_region_ b
		on  a.corp_id = b.corp_id
	where a.bond_type=2  -- 城投 
),
Third_Part_Data_CT_Prepare_II as 
(
	select distinct
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		a.synth_warnlevel,
		a.bond_type, 	 -- 属性1：城投 
		a.bond_type_desc,
		cast(a.region_cd as string) as corp_property,   
		'同区域、同行政级别且' as corp_property_desc,    -- 属性2：同区域、同行政级别
		b.corp_id as same_property_corp_id,
		b.corp_nm as same_property_corp_nm
	from Third_Part_Data_CT_Prepare_I a 
	join Third_Part_Data_CT_Prepare_I b
		on a.region_cd=b.region_cd and a.synth_warnlevel=b.synth_warnlevel
	where a.corp_id<>b.corp_id
),
Third_Part_Data_CT as -- 和城投主体相同属性的 其他企业数量 计算
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm
		-- row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
		-- count(corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
	from Third_Part_Data_CT_Prepare_II
),
Third_Part_Data_SUMM as 
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		synth_warnlevel,
		bond_type,
		bond_type_desc,
		corp_property,
		corp_property_desc,
		same_property_corp_id,
		same_property_corp_nm,
		corp_id_cnt
	from 
	(
		select 
			*
			--row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel order by 1) as rm_rep_data
		from 
		(
			select
				*,
				row_number() over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property order by 1) as rm,
				count(same_property_corp_id) over(partition by batch_dt,corp_id,score_dt,synth_warnlevel,bond_type,corp_property) as corp_id_cnt
			from 
			(
				select *
				from Third_Part_Data_CY
				UNION ALL 
				select *
				from Third_Part_Data_CT
			) A --where rm<=5
		) B where rm<=5
	) C --where rm_rep_data=1
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 第三段信息 --
Third_Msg_Corp as --将 和主体相同属性的企业合并为一行
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		concat_ws('、',collect_set(same_property_corp_nm)) as same_property_corp_nm_in_one_row  --hive
		-- group_concat(same_property_corp_nm,'、') as same_property_corp_nm_in_one_row  --impala
	from Third_Part_Data_SUMM
	group by batch_dt,corp_id,corp_nm,score_dt
),
Third_Msg as 
(
	select 
		a.batch_dt,
		a.corp_id,
		a.corp_nm,
		a.score_dt,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'中'),''),a.corp_property_desc,
			'总体风险水平表现一致的企业还包括：',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'等',''),
			cast(corp_id_cnt as string),'家企业。'
		) as msg_no_color,
		concat(
			if(a.bond_type_desc <>'',concat(bond_type_desc,'中'),''),a.corp_property_desc,
			'总体风险水平表现一致的企业还包括：','<span class="WEIGHT">',b.same_property_corp_nm_in_one_row,if(a.corp_id_cnt>5,'等',''),'</span>',
			cast(corp_id_cnt as string),'家企业。'
		) as msg3
	from Third_Part_Data_SUMM a 
	join Third_Msg_Corp b 
		on a.batch_dt=b.batch_dt and a.corp_id=b.corp_id
)
insert overwrite table pth_rmp.rmp_warning_score_report3
select distinct
	batch_dt,
	corp_id,
	corp_nm,
	score_dt,
	msg_no_color,
	msg3
from
(
	select 
		*,row_number() over(partition by batch_dt,corp_id,score_dt order by 1) as rm
	from Third_Msg
) A where rm=1  --去重重复数据（以免出现脏数据）
;




