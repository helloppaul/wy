-- RMP_WARNING_SCORE_REPORT 第一段 --
--/**2022-10-22 首段，新增外挂规则逻辑/
--/* 2022-10-22 首段，新增 提示颜色 */
--/* 2022-12-04 外挂规则取值修复，取最新create_dt的数据 */

set hive.exec.parallel=true;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;  

drop table if exists pth_rmp.rmp_warning_score_report1;  
create table pth_rmp.rmp_warning_score_report1 as    --@pth_rmp.rmp_warning_score_report1
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
    select 'creditrisk_lowfreq_concat' model_name,'v1.0.4' model_version,'active' status  --低频模型
    union all
    select 'creditrisk_midfreq_cityinv' model_name,'v1.0.4' model_version,'active' status  --中频-城投模型
    union all 
    select 'creditrisk_midfreq_general' model_name,'v1.0.2' model_version,'active' status  --中频-产业模型
    union all 
    select 'creditrisk_highfreq_scorecard' model_name,'v1.0.4' model_version,'active' status  --高频-评分卡模型(高频)
    union all 
    select 'creditrisk_highfreq_unsupervised' model_name,'v1.0.2' model_version,'active' status  --高频-无监督模型
    union all 
    select 'creditrisk_union' model_name,'v1.0.2' model_version,'active' status  --信用风险综合模型
    -- select 
    --     notes,
    --     model_name,
    --     model_version,
    --     status,
    --     etl_date
    -- from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf a
    -- where a.etl_date in (select max(etl_date) from t_ods_ais_me_rsk_rmp_warncntr_dftwrn_conf_modl_ver_intf)
    --   and status='active'
    -- group by notes,model_name,model_version,status,etl_date
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
			where 1 in (select max(flag) from timeLimit_switch) 
			and to_date(rating_dt) = to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))
			union all
			-- 非时间限制部分 --
			select * ,rank() over(partition by to_date(rating_dt) order by etl_date desc ) as rm
			from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
			where 1 in (select not max(flag) from timeLimit_switch) 
		) m where rm=1
    ) a join model_version_intf_ b
        on a.model_version = b.model_version and a.model_name=b.model_name
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
		a.interval_text_adjusted,  --原始模型提供的综合预警等级
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
--—————————————————————————————————————————————————————— 配置表 ————————————————————————————————————————————————————————————————————————————————--
warn_level_ratio_cfg_ as -- 综合预警等级等级划分档位-配置表
(
	select 
		property_cd,  --1:产业  2:城投
		property,  -- '城投' , '产业'
		warn_lv,   -- '-5','-4','-3','-2','-1'
		percent_desc,  -- 前1% 前1%-10% ...
		warn_lv_desc   -- 绿色预警等级  ...
	from pth_rmp.rmp_warn_level_ratio_cfg
),
warn_adj_rule_cfg as --预警分-模型外挂规则配置表   取最新etl_date的数据 (更新频率:日度更新)
(
	select m.*
	from 
	(
		select 
			a.etl_date,
			b.corp_id, 
			b.corp_name as corp_nm,
			a.category,
			a.reason,
			rank() over(order by a.create_dt desc ,a.etl_date desc,a.reason desc) rm
		from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf a  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
		join corp_chg b 
			on cast(a.corp_code as string)=b.source_id and b.source_code='ZXZX'
		where a.operator = '自动-风险已暴露规则'
		  and to_date(a.create_dt) <= to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))
	)m where rm=1 
	  --and ETL_DATE in (select max(etl_date) from hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf)  --@hds.t_ods_ais_me_rsk_rmp_warncntr_dftwrn_modl_adjrule_list_intf
),
warn_color_cfg as --预警报告系列专用-颜色配置（仅供参考，不作为代码引用）
(
	select	1 as id,' 绿色预警' as ori_msg,'<span class="GREEN">绿色内容</span>' as color_msg
	union all
	select	2 as id, '黄色预警' as ori_msg,'<span class="YELLOW">黄色内容</span>' as color_msg
	union all
	select	3 as id, '橙色预警' as ori_msg,'<span class="ORANGE">黄色内容</span>' as color_msg
	union all
	select	4 as id, '红色预警' as ori_msg,'<span class="RED">红色内容</span>' as color_msg
	union all
	select	5 as id, '风险已暴露' as ori_msg,'<span class="RED">风险已暴露内容</span>' as color_msg
	union all
	select	6 as id, '加粗+红色' as ori_msg,'<span class="RED"><span class="WEIGHT">红色加粗内容</span></span>' as color_msg 
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
-- 第一段数据 --
First_Part_Data as  --适用 预警分-归因简报的数据 （主体层次）
(
	select distinct
		main.batch_dt,
		main.corp_id,
		main.corp_nm,
		main.score_date as score_dt,
		nvl(ru.category,'') as category_nvl,
		nvl(ru.reason,'') as reason_nvl,
		main.credit_cd,
		main.synth_warnlevel,  --综合预警等级 used
		chg.bond_type,  --1:产业债 2:城投债
		case chg.bond_type
			when 2 then '城投平台'
			else '产业主体'
		end as corp_bond_type,  --主体属性 used
		cfg.warn_lv_desc, --预警等级描述 used
		cfg.percent_desc  --预警等级档位百分比划分 used
	from RMP_WARNING_SCORE_MODEL_ main 
	left join (select * from corp_chg where source_code='ZXZX') chg
		on main.corp_id=chg.corp_id
	join warn_level_ratio_cfg_ cfg
		on main.synth_warnlevel=cfg.warn_lv and chg.bond_type=cfg.property_cd
	left join warn_adj_rule_cfg  ru
		on main.corp_id = ru.corp_id
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 第一段信息 --
First_Msg as --
(
	select 
		batch_dt,
		corp_id,
		corp_nm,
		score_dt,
		credit_cd,
		concat(
			case 
				when  reason_nvl<>'' then 
					concat('该主体因触发',reason_nvl,'，','当前处于','风险已暴露预警等级','。')
				else 
					concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属',warn_lv_desc,'。')
			end
		) as msg1_no_color,  --第一句话
		concat(
			case 
				when  reason_nvl<>'' then 
					concat('该主体因触发',reason_nvl,'，','当前处于','<span class="RED"><span class="WEIGHT">','风险已暴露预警等级','</span></span>','。')
				else 
					case warn_lv_desc
						when '绿色预警等级' then
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属','<span class="GREEN"><span class="WEIGHT">',warn_lv_desc,'</span></span>','。')
						when '黄色预警等级' then
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属','<span class="YELLO"><span class="WEIGHT">',warn_lv_desc,'</span></span>','。')
						when '橙色预警等级' then
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属','<span class="ORANGE"><span class="WEIGHT">',warn_lv_desc,'</span></span>','。')
						when '红色预警等级' then
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属','<span class="RED"><span class="WEIGHT">',warn_lv_desc,'</span></span>','。')
						when '风险已暴露' then 
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属','<span class="RED"><span class="WEIGHT">',warn_lv_desc,'</span></span>','。')
						else 
							concat('该主体预测风险水平处于',corp_bond_type,'中',percent_desc,'，','属',warn_lv_desc,'。')
					end
			end
		) as msg1  --带颜色第一句话
	from First_Part_Data
)
select distinct
	* 
from First_Msg
;
