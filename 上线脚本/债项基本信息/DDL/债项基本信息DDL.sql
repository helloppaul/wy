-- 债项基本信息 RMP_BOND_BASICINFO --
drop table  if exists pth_rmp.RMP_BOND_BASICINFO ;
create table pth_rmp.RMP_BOND_BASICINFO 
(	
	sid_kw string,
	corp_id string,
    natural_dt timestamp,
    maturity_date timestamp,
    lead_underwriter string,
    stock_bond_count bigint,
    stock_bond_balance double,
	delete_flag	int,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	int
)partitioned by (etl_date int)
 stored as textfile
;