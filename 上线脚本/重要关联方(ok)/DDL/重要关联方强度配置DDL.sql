create table pth_rmp.rmp_compy_core_rel_degree_cfg as 
select 1 as sid_kw,1 as rel_type_cd,'实际控制人' as rel_type,11 as rel_type_ii_cd,'实际控制人' as rel_type_ii,3 as importance,'实际控制人' as rel_type_desc_division,'实际控制人' as rel_type_desc 

union all select 2 as sid_kw,2 as rel_type_cd,'直接股东' as rel_type,21 as rel_type_ii_cd,'累积持股30%以上' as rel_type_ii,3 as importance,'股东' as rel_type_desc_division,'股东<直接持股且累积持股30%以上>' as rel_type_desc 

union all select 3 as sid_kw,2 as rel_type_cd,'直接股东' as rel_type,22 as rel_type_ii_cd,'累积持股10%-30%' as rel_type_ii,2 as importance,'股东' as rel_type_desc_division,'股东<直接持股且累积持股10%-30%>' as rel_type_desc 

union all select 4 as sid_kw,2 as rel_type_cd,'直接股东' as rel_type,23 as rel_type_ii_cd,'累积持股5%-10%' as rel_type_ii,1 as importance,'股东' as rel_type_desc_division,'股东<直接持股且累积持股5%-10%>' as rel_type_desc 

union all select 5 as sid_kw,3 as rel_type_cd,'间接股东<3层穿透>' as rel_type,31 as rel_type_ii_cd,'累积持股30%以上' as rel_type_ii,3 as importance,'股东' as rel_type_desc_division,'股东<间接持股且累积持股30%以上>' as rel_type_desc 

union all select 6 as sid_kw,3 as rel_type_cd,'间接股东<3层穿透>' as rel_type,32 as rel_type_ii_cd,'累积持股10%-30%' as rel_type_ii,2 as importance,'股东' as rel_type_desc_division,'股东<间接持股且累积持股10%-30%>' as rel_type_desc 

union all select 7 as sid_kw,3 as rel_type_cd,'间接股东<3层穿透>' as rel_type,33 as rel_type_ii_cd,'累积持股5%-10%' as rel_type_ii,1 as importance,'股东' as rel_type_desc_division,'股东<间接持股且累积持股5%-10%>' as rel_type_desc 

union all select 8 as sid_kw,4 as rel_type_cd,'直接对外投资' as rel_type,41 as rel_type_ii_cd,'累积持股50%以上' as rel_type_ii,3 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<直接投资且累积持股50%以上>' as rel_type_desc 

union all select 9 as sid_kw,4 as rel_type_cd,'直接对外投资' as rel_type,42 as rel_type_ii_cd,'累积持股30%-50%' as rel_type_ii,2 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<直接投资且累积持股30%-50%>' as rel_type_desc 

union all select 10 as sid_kw,4 as rel_type_cd,'直接对外投资' as rel_type,43 as rel_type_ii_cd,'累积持股20-30%' as rel_type_ii,1 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<直接投资且累积持股20-30%>' as rel_type_desc 

union all select 11 as sid_kw,5 as rel_type_cd,'间接对外投资<3层穿透>' as rel_type,51 as rel_type_ii_cd,'累积持股50%以上' as rel_type_ii,3 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<间接投资且累积持股50%以上>' as rel_type_desc 

union all select 12 as sid_kw,5 as rel_type_cd,'间接对外投资<3层穿透>' as rel_type,52 as rel_type_ii_cd,'累积持股30%-50%' as rel_type_ii,2 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<间接投资且累积持股30%-50%>' as rel_type_desc 

union all select 13 as sid_kw,5 as rel_type_cd,'间接对外投资<3层穿透>' as rel_type,53 as rel_type_ii_cd,'累积持股20-30%' as rel_type_ii,1 as importance,'对外投资' as rel_type_desc_division,'对外投资企业<间接投资且累积持股20%-30%>' as rel_type_desc 

union all select 14 as sid_kw,6 as rel_type_cd,'相同实控人' as rel_type,61 as rel_type_ii_cd,'相同实控人' as rel_type_ii,3 as importance,'相同实控人' as rel_type_desc_division,'相同实控人控股公司' as rel_type_desc 

union all select 15 as sid_kw,7 as rel_type_cd,'同集团内上市发债企业' as rel_type,71 as rel_type_ii_cd,'同集团内上市发债企业' as rel_type_ii,3 as importance,'同集团内上市发债企业' as rel_type_desc_division,'同集团内上市发债企业' as rel_type_desc 
