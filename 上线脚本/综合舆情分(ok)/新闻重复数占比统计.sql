-- 综合舆情分中间表 rmp_alert_comprehs_score_temp--
--入参：${ETL_DATE}(20220818 int)
with
news as(   --！！！注意此处notice_dt 处理为日期型，当日数据需要实时处理
	select distinct corp_id,corp_nm,to_date(notice_dt) as notice_dt,msg_id,0 as today_flag
	from pth_rmp.rmp_opinion_risk_info  --@rmp_opinion_risk_info
	where signal_type=0 and delete_flag=0 
	  and notice_dt<to_date(current_timestamp())
	UNION ALL 
	select distinct corp_id,corp_nm,to_date(current_timestamp()) as notice_dt,msg_id,1 as today_flag
	from pth_rmp.rmp_opinion_risk_info  --@rmp_opinion_risk_info
	where signal_type=0 and delete_flag=0    
	  and notice_dt>= from_unixtime((unix_timestamp()-3600*24)) and  notice_dt<= current_timestamp()
),
Single_news as (
	select corp_id,corp_nm,notice_dt,count(*) as yq_num from news group by corp_id,corp_nm,notice_dt
),
Cross_news as (
	select a.corp_id,a.corp_nm,a.notice_dt,count(*) as cross_yq_num ,b.corp_id as rel_corp_id, b.corp_nm as rel_corp_nm
	from news a join news b
		on a.notice_dt = b.notice_dt and a.msg_id=b.msg_id 
	where a.corp_id<>b.corp_id
	group by a.corp_id ,a.corp_nm,b.corp_id ,b.corp_nm ,a.notice_dt
),
news_duplicates_static as
(
	select 
		distinct sn.corp_id, sn.notice_dt ,cn.rel_corp_id, cn.rel_corp_nm, sn.yq_num ,cn.cross_yq_num, 
		(cn.cross_yq_num/sn.yq_num) as news_duplicates_ratio
	from Cross_news cn
	join Single_news sn
		on cn.corp_id=sn.corp_id and cn.notice_dt=sn.notice_dt
)