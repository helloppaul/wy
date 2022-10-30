---- 债项基本信息 RMP_BOND_BASICINFO (同步方式：一天单批次插入) --
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
t_ods_fic_hb_bond_issuer_subject_cr_ as  --债券发行人主体信用评级(全量更新)
(
    select *
    from hds.t_ods_fic_hb_bond_issuer_subject_cr  
    where isvalid=1
),
t_ods_fic_ic_tq_bd_basicinfo_ as  --债项基本要素(全量更新)
(
    select * 
    from hds.t_ods_fic_ic_tq_bd_basicinfo 
    where isvalid='1'
),
t_ods_fic_hb_tq_bd_stockchg_ as --债券存量变动表(增量更新)
(
    select * 
    from hds.t_ods_fic_hb_tq_bd_stockchg
    where isvalid=1
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
mid_bond_issuer_sub_cr as --债券发行人主体信用评级
(
    select distinct
        issuer_corp_code,  --债券发行人ID/主体ID
        issuer_name,  --发行人名称
        rating_type,  --评级类型
        cast(publish_date as timestamp) as publish_date,  --发布日期
        rating_date  --评级日期
    from t_ods_fic_hb_bond_issuer_subject_cr_ a 
    join (select max(etl_date) max_etl_date from t_ods_fic_hb_bond_issuer_subject_cr_)b 
        on a.etl_date=b.max_etl_date
),
mid_bond_basic_info as --债项基本要素
(
    select distinct
        secode,  --证券内码
        `symbol`, --债券代码
        bondname,  --债券全称
        issuecompcode,  --发型机构代码
        from_unixtime(unix_timestamp(maturitydate,'yyyyMMdd'),'yyyy-MM-dd') as maturitydate, --到期日 yyyy-mm-dd 00:00:00
        leaduwer
        -- if(leaduwer<>'',concat(leaduwer,','),'') as leaduwer
    from t_ods_fic_ic_tq_bd_basicinfo_ a 
    join (select max(etl_date) max_etl_date from t_ods_fic_ic_tq_bd_basicinfo_) b 
        on a.etl_date=b.max_etl_date 
),
mid_bond_stockchg as 
(
    select distinct
        secode,  --证券内码
        changedate,  --变动日期
        changedamt,  --本次变动金额(万元)
        aftchangedamt  --变动后金额(万元)
    from t_ods_fic_hb_tq_bd_stockchg_
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
corp_bond_stock_chg as   --主体_债券_债券存量变动
(
    select distinct
        chg.corp_id,  
        to_date(c.changedate) as natural_dt,
        b.`symbol` as bond_cd,  --债券代码
        b.bondname as bond_nm,   --债券名称
        to_date(b.maturitydate) as maturity_date,  --到期日
        b.leaduwer as lead_underwriter,
        c.aftchangedamt/1000 as chg_amt  --变动后金额(亿元)
    from mid_bond_issuer_sub_cr a -- 主体和债券关系
    join mid_bond_basic_info b   --债券的属性(债券和到期日，主承销商的关系)
        on a.issuer_corp_code=cast(b.issuecompcode as int)
    join mid_bond_stockchg c    
        on cast(b.secode as bigint) = c.secode
    join corp_chg chg 
        on cast(a.issuer_corp_code as string)=chg.source_id and chg.source_code='ZXZX'
    where to_date(c.changedate)<=b.maturitydate --还未到期的债券
      and c.changedate = a.publish_date
),
res as 
(
    select  
        corp_id,
        natural_dt,
        recent_maturity_date,
        max(recent_lead_underwriter) as recent_lead_underwriter,
        concat_ws('，',collect_set(lead_underwriter)) as lead_underwriter,  -- hive
        -- group_concat(distinct lead_underwriter,'，') as lead_underwriter,  -- impala
        max(stock_bond_count) as stock_bond_count,
        max(stock_bond_balance) as stock_bond_balance
    from 
    (
        select 
            corp_id,
            natural_dt,
            max(maturity_date) over(partition by corp_id,natural_dt) as recent_maturity_date,  --最近到期日
            last_value(lead_underwriter) over(partition by corp_id,natural_dt order by maturity_date asc) as recent_lead_underwriter,
            -- max(maturity_date) as maturity_date,  --最近到期日
            lead_underwriter,  --所有主承销商
            count(bond_cd) over(partition by corp_id,natural_dt) as stock_bond_count,  --存量债券只数
            sum(chg_amt) over(partition by corp_id,natural_dt) as stock_bond_balance  --存量债券余额
        from corp_bond_stock_chg
        -- group by corp_id,natural_dt
    ) A group by corp_id,natural_dt,recent_maturity_date
)
------------------------------------temp table above-------------------------------------------------------------------
insert into pth_rmp.RMP_BOND_BASICINFO PARTITION(ETL_DATE=${ETL_DATE})
select 
    -- '' as sid_kw,  -- impala
    md5(concat(corp_id,cast(natural_dt as string),lead_underwriter)) as sid_kw,  -- hive
    corp_id,
    natural_dt,
    recent_maturity_date,
    recent_lead_underwriter,
    lead_underwriter,
    stock_bond_count,
    stock_bond_balance,
    0 as delete_flag,
    '' as create_by,
    current_timestamp() as create_time,
    '' as update_by,
    current_timestamp() update_time,
    0 as version
from res 
-- where natural_dt= from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd' ),'yyyy-MM-dd')
;
