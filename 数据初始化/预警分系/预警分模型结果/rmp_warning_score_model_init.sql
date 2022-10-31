--（1）DDL rmp_warning_score_model_init --
drop table if exists pth_rmp.rmp_warning_score_model_init;
create table pth_rmp.rmp_warning_score_model_init 
(
	sid_kw string,
	corp_id string,
	corp_nm string,
	credit_cd string,
	score_date timestamp,
	synth_warnlevel string,
	synth_score double,
	model_version string,
	adjust_warnlevel string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;


--（2）sql初始化 impala执行 --
drop table if exists pth_rmp.rmp_warning_score_model_init_impala;
create table pth_rmp.rmp_warning_score_model_init_impala as 
--—————————————————————————————————————————————————————— 基本信息 ————————————————————————————————————————————————————————————————————————————————--
with
corp_chg as  --带有 城投/产业判断和国标一级行业 的特殊corp_chg
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
    ,b.bond_type,b.industryphy_name
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
-- 预警分 --
_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_  as --预警分_融合调整后综合  原始接口
(
    select * 
    from hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf  --@hds.tr_ods_ais_me_rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf
	where 1=1
	  and to_date(rating_dt) >= to_date('2020-01-01') 
	  and to_date(rating_dt) <= to_date('2022-10-30')
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
warn_union_adj_sync_score as --取最新批次的融合调整后综合预警分
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
			when a.interval_text_adjusted in ('绿色预警') then 
				'R1'   --低风险
			when a.interval_text_adjusted in ('黄色预警') then
				'R3'
			when a.interval_text_adjusted  = '橙色预警' then 
				'R4'  --中风险
			when a.interval_text_adjusted  ='红色预警' then 
				'R5'  --高风险
			when a.interval_text_adjusted  ='风险已暴露' then 
				'R6'   --风险已暴露
		end as adjust_warnlevel,
		a.model_name,
		a.model_version
    from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ a   
    join (select max(rating_dt) as max_rating_dt,to_date(rating_dt) as score_dt from _rsk_rmp_warncntr_dftwrn_rslt_union_adj_intf_ group by to_date(rating_dt)) b
        on a.rating_dt=b.max_rating_dt and to_date(a.rating_dt)=b.score_dt
    join corp_chg chg
        on chg.source_code='ZXZX' and chg.source_id=cast(a.corp_code as string)
)
------------------------------------以上部分为临时表（生成的每天的数据都是基于每天中的一个批次）-------------------------------------------------------------------
select distinct
	-- md5(concat(batch_dt,corp_id,cast(score_date as string),model_version)) as sid_kw,  --hive
	batch_dt,
	corp_id,
	corp_nm,
	credit_cd,
	score_date,
	synth_warnlevel,  -- 综合预警等级
	synth_score,  -- 预警分
	model_version,
	adjust_warnlevel,   -- 调整后等级
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
from warn_union_adj_sync_score
WHERE score_date>=to_date('2022-09-24') 
;

--（3）sql初始化 RMP_WARNING_SCORE_MODEL_INIT impala执行 --
insert into pth_rmp.rmp_warning_score_model_init partition(etl_date=19900101) 
select 
	md5(concat(batch_dt,corp_id,cast(score_date as string),model_version))  as sid_kw,
	corp_id ,
	corp_nm ,
	credit_cd ,
	score_date ,
	synth_warnlevel ,
	synth_score ,
	model_version ,
	adjust_warnlevel ,
	delete_flag	,
	create_by	,
	create_time	,
	update_by	,
	update_time	,
	version	
from pth_rmp.rmp_warning_score_model_init_impala
;
