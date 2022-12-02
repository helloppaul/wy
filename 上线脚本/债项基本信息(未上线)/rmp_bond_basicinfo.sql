---- 债项基本信息 RMP_BOND_BASICINFO (同步方式：一天单批次插入) --

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15; 
set hive.auto.convert.join = true;
-- set hive.mapjoin.smalltable.filesize=25000000;  --默认25MB
set hive.ignore.mapjoin.hint = false;  

-- part1 --
drop table if exists pth_rmp.rmp_corp_bond_chg_res;
create table pth_rmp.rmp_corp_bond_chg_res as 
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
	where a.delete_flag=0 and b.delete_flag=0 and a.source_code='ZXZX'
),
--—————————————————————————————————————————————————————— 接口层 ————————————————————————————————————————————————————————————————————————————————--
t_ods_fic_ic_tq_bd_basicinfo_ as  --债项基本要素(全量更新)
(
    select issuecompcode,secode,`symbol`, bondname,maturitydate,leaduwer,etl_date
    from hds.t_ods_fic_ic_tq_bd_basicinfo
    where isvalid='1'
      and etl_date> cast(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')-(60*60*24)*365,'yyyyMMdd') as int)
      and etl_date<=${ETL_DATE} 
    group by issuecompcode,secode,`symbol`, bondname,maturitydate,leaduwer,etl_date
),
t_ods_fic_hb_tq_bd_stockchg_ as --债券存量变动表(增量更新)
(
    select secode,changedate,changedamt,aftchangedamt,etl_date
    from hds.t_ods_fic_hb_tq_bd_stockchg
    where isvalid=1
      and etl_date> cast(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')-(60*60*24)*365,'yyyyMMdd') as int)
      and etl_date<=${ETL_DATE} 
    group by secode,changedate,changedamt,aftchangedamt,etl_date
),
--—————————————————————————————————————————————————————— 中间层 ————————————————————————————————————————————————————————————————————————————————--
mid_bond_basic_info as --债项基本要素
(
    select 
        etl_date,
        issuecompcode,  --发型机构代码
        secode,  --证券内码
        `symbol`, --债券代码
        bondname,  --债券全称
        maturitydate,   --到期日 '20221122'
        -- from_unixtime(unix_timestamp(maturitydate,'yyyyMMdd'),'yyyy-MM-dd') as maturitydate, --到期日 yyyy-mm-dd 00:00:00
        leaduwer
        -- if(leaduwer<>'',concat(leaduwer,','),'') as leaduwer
    from t_ods_fic_ic_tq_bd_basicinfo_ 
),
mid_bond_stockchg as 
(
    select 
        etl_date,
        secode,  --证券内码
        changedate,  --变动日期
        changedamt,  --本次变动金额(万元)
        aftchangedamt  --变动后金额(万元)
    from t_ods_fic_hb_tq_bd_stockchg_
),
--—————————————————————————————————————————————————————— 应用层 ————————————————————————————————————————————————————————————————————————————————--
-- 企业-债券 --
corp_bond as   --发行人和存量债券关系数据
(   --发行人发行多只债，每一只债对应一条到期日，但每一只债会有多个主承销商。
    select 
        b.etl_date,
        b.issuecompcode,
        count(b.secode) over(partition by b.etl_date,b.issuecompcode) as  stock_bond_count,  --存续债券只数
        b.secode,b.bondname,
        b.maturitydate,   --到期日
        b.leaduwer
    from mid_bond_basic_info b
    -- from mid_bond_issuer_sub_cr 
    -- join mid_bond_basic_info b 
        -- on cast(a.issuecompcode as string)=b.issuecompcode and a.etl_date=b.etl_date
    where cast(b.maturitydate as int) >= b.etl_date
),
corp_bond_cal_all_lead_underwriter as 
(
    select 
        etl_date,
        issuecompcode,
        -- group_concat(distinct leaduwer,',') as  lead_underwriter  --所有主承销商(主体层)  impala
        concat_ws(',',sort_array(collect_set(leaduwer_))) as  lead_underwriter   --所有主承销商(主体层)  hive
    from (select *,if(leaduwer='',NULL,leaduwer) as leaduwer_ from corp_bond) A  --统计完债券存量只数，排除掉为''的主承销商数据
    group by etl_date,issuecompcode
),
corp_bond_cal_recent_lead_underwriter as 
(   --发行人对应一个最近到期日，最近到期日对应一个最近到期主承销商，最近到期日对应一个存续债券只数，最近到期日对应多个所有主承销商(所有债券对应的主承销商)
    
    select 
        etl_date,
        issuecompcode,
        maturitydate as recent_maturity_date,  --最近到期日 
        stock_bond_count,
        max(leaduwer) as recent_lead_underwriter --最近到期日对应的那些主承销商
    from 
    (
        select 
            etl_date,
            issuecompcode,
            maturitydate,
            stock_bond_count,
            leaduwer_ as leaduwer,
            rank() over(partition by etl_date,issuecompcode order by maturitydate asc,leaduwer desc) as rk_recent_maturitydate
        from (select *,if(leaduwer='',NULL,leaduwer) as leaduwer_ from corp_bond) A --统计完债券存量只数，排除掉为''的主承销商数据
    )B where rk_recent_maturitydate = 1  --取最近到期日对应的数据
    group by etl_date,issuecompcode,maturitydate,stock_bond_count
),
corp_bond_cal as 
(
    select 
        b.etl_date,
        b.issuecompcode,
        b.recent_maturity_date,
        b.stock_bond_count,
        b.recent_lead_underwriter,
        a.lead_underwriter
    from corp_bond_cal_all_lead_underwriter a 
    join corp_bond_cal_recent_lead_underwriter b 
        on a.etl_date=b.etl_date and a.issuecompcode=b.issuecompcode
),
-- 企业+债券变动 --
corp_bond_chg as 
(   --每只存量债券对应多个变动数据
    select
        b.etl_date,
        b.issuecompcode,
        b.secode,b.bondname,
        c.changedate,
        nvl(c.aftchangedamt,0)/10000 as chg_amt,
        rank() over(partition by c.etl_date,b.issuecompcode,c.secode order by c.changedate desc) as rk
    from corp_bond b  
    left join (select * from mid_bond_stockchg where changedate <= from_unixtime(unix_timestamp(cast(etl_date as string),'yyyyMMdd'),'yyyy-MM-dd'))c 
        on b.secode=cast(c.secode as string) and b.etl_date=c.etl_date
    -- where c.changedate <= from_unixtime(unix_timestamp(cast(c.etl_date as string),'yyyyMMdd'),'yyyy-MM-dd')
),
corp_bond_chg_cal as 
(   --一家发行人主体对应多只债券,每只债券对应多个变动数据,取每只债券对应最大变动日期的数据
    select
        etl_date,
        issuecompcode,
        -- secode,
        -- bondname,
        -- changedate,
        sum(chg_amt) as stock_bond_balance  --存续债金额
    from corp_bond_chg
    where rk=1   --取每只债券对应的最大变动日期数据
    group by etl_date,issuecompcode
),
-- 企业+债券只数+债券余额 结果集-- 
corp_bond_chg_res as 
(   --企业+存量债券只数+存量债券变动余额
    select 
        from_unixtime(unix_timestamp(cast(b.etl_date as string),'yyyyMMdd' ),'yyyy-MM-dd') as natural_dt,
        chg.corp_id,
        from_unixtime(unix_timestamp(b.recent_maturity_date,'yyyyMMdd'),'yyyy-MM-dd') as recent_maturity_date,
        b.lead_underwriter,
        b.recent_lead_underwriter,
        b.stock_bond_count,
        c.stock_bond_balance
        -- c.changedate
    from corp_bond_cal b
    join corp_bond_chg_cal c
        on b.issuecompcode=c.issuecompcode and b.etl_date=c.etl_date
    join corp_chg chg
        on cast(b.issuecompcode as string)=chg.source_id
    group by b.etl_date,chg.corp_id,b.recent_maturity_date,b.recent_lead_underwriter,b.lead_underwriter,b.stock_bond_count,c.stock_bond_balance
)
select * from corp_bond_chg_res
;




-- part2 去除全部主承销商字段里面重复主承销商--
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10; 


insert into pth_rmp.rmp_bond_basicinfo partition(etl_date=${ETL_DATE})
select 
    md5(concat(corp_id,cast(natural_dt as string))) as sid_kw,  --hive
    corp_id,
    natural_dt,
    recent_maturity_date,
    concat_ws(',',collect_set(lead_underwriter_item)) as lead_underwriter,
    nvl(recent_lead_underwriter,'') as recent_lead_underwriter,
    stock_bond_count,
    stock_bond_balance,
    0 as delete_flag,
    '' as create_by,
    current_timestamp() as create_time,
    '' as update_by,
    current_timestamp() update_time,
    0 as version
from 
(
    select 
        a.*,
        lead_underwriter_item
    from pth_rmp.rmp_corp_bond_chg_res a 
    lateral view explode(split(lead_underwriter,',')) t as lead_underwriter_item
) B group by corp_id,natural_dt,recent_maturity_date,recent_lead_underwriter,stock_bond_count,stock_bond_balance
;