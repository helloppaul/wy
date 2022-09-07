--生成自然日序列表，用于综合舆情分的 企业日期范围，解决主体不包含关联方，以及主体长时间没有发生舆情现象
-- drop table if exists pth_rmp.rmp_calendar;
create table pth_rmp.rmp_calendar as 
with dates AS
(
	select date_add("2022-07-01",a.pos) as dt from 
	(
		select posexplode(split(repeat("m",datediff("2022-08-17","2022-07-01" )),"m" ))
	)a
)select * from dates;