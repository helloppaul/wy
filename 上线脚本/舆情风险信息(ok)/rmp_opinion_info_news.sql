--新闻舆情 (同步方式：一天多批次覆盖)- --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态



with 
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
	from (select cid1.* from pth_rmp.rmp_company_id_relevance cid1 
		  join (select max(etl_date) as etl_date from pth_rmp.rmp_company_id_relevance) cid2
			on cid1.etl_date=cid2.etl_date
		 )	a 
	join pth_rmp.rmp_company_info_main B 
		on a.corp_id=b.corp_id and a.etl_date = b.etl_date
	where a.delete_flag=0 and b.delete_flag=0
)
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(dt=${ETL_DATE},type_='news')
select distinct
	cid_chg.corp_id as corp_id,
	Final.corp_nm,
	Final.notice_dt,
	Final.msg_id,
	Final.msg_title,
	Final.case_type_cd,
	Final.case_type,
	Final.case_type_ii_cd,
	Final.case_type_ii,
	Final.importance,
	Final.signal_type,
	Final.src_table,
	Final.src_sid,
	nvl(Final.url_kw,'') as url_kw,
	nvl(Final.news_from,'') as news_from,
	Final.msg,
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
	-- cast(from_unixtime(unix_timestamp(to_date(Final.notice_dt),'yyyy-MM-dd'),'yyyyMMdd') as int) as dt,
	-- 'news' as type_
from 
(
	SELECT distinct
		corp_id,corp_nm,
		notice_dt,
		-- '' as msg_id,  -- impala
		concat(corp_id,'_',MD5(concat(corp_id,msg_id_,case_type_cd,case_type_ii_cd))) AS msg_id,  -- hive
		msg_title,
		case_type_cd,case_type,
		case_type_ii_cd,case_type_ii,
		importance,
		signal_type,
		src_table,
		src_sid,
		url_kw,
		news_from,
		msg
	FROM 
	(	
			--新闻舆情
		select 
			company_id as corp_id,
			company_nm as corp_nm,
			notice_dt,
			cast(msg_id as string) as msg_id_,
			msg_title,
			case_type_cd,
			case_type,
			case_type_ii_cd,
			case_type_ii,
			case 
				when cal_importance<0 and abs(cal_importance)>2 then
					-3 -- 严重负面
				when cal_importance<0 and abs(cal_importance)>1 then
					-2
				when cal_importance<0 and abs(cal_importance)<=1 then
					-1
				else
					cal_importance
			end as importance,
			signal_type,
			src_table,
			cast(src_sid as string) as src_sid,
			url as url_kw,
			news_from,
			msg
		from 
		(
			select t0.*,0.5*t0.case_importance+0.5*t0.origin_importance as cal_importance
			from 
			(
				select 
					o1.itcode2 as company_id,  --机构代码(企业库) as 公司代码
					o2.itname as company_nm,  -- 公司名称
					o.crnw0001_002 as notice_dt,  -- 新闻时间 as  消息时间
					o.newscode as msg_id,  -- 新闻id as 消息id
					o.crnw0001_003 as  msg_title,  -- 新闻标题 as  消息标题
					idx.f_indexcode as case_type_cd,  -- 一级标签编码 as 事件类型代码  
					idx.f_indexname as case_type,   -- 一级标签 as 事件类型
					idx.indexcode as case_type_ii_cd,  -- 二级标签编码 as  事件二级类型代码
					idx.indexname as case_type_ii,  -- 二级标签编 as 事件二级类型
					case 
						when idx.importance=-3 then  
							-3
						when idx.importance=-2 THEN
							-2
						ELSE              --原始新闻重要度 as 未命中事件负面体系的重要程度
							cast(o1.crnw0003_006 as int)
					End as case_importance,   -- as 负面事件重要程度
					cast(o1.crnw0003_006 as int) as  origin_importance, -- 原始新闻重要程度
					0 as signal_type,   -- 信号类型  0:新闻舆情 1:诚信 2:司法
					upper('tr_ods_rmp_fi_x_news_tcrnw0003_all_v2') as SRC_TABLE,
					o1.newscode as SRC_SID,
					Case 
						when o.crnw0001_010='' and o.crnw0001_007 <> '' THEN
							concat('无新闻链接，选用无版权链接',o.crnw0001_017)
						else o.crnw0001_010
					end as URL,
					o.crnw0001_007 as news_from,
					'' as msg
				from (select * from hds.tr_ods_rmp_fi_x_news_tcrnw0001 where flag<>'1') o,
					 (select * from hds.tr_ods_rmp_fi_x_news_tcrnw0003_all_v2 where flag<>'1') o1,
					 (select * from hds.tr_ods_rmp_fi_x_news_tcrnwitcode where flag<>'1' ) o2,
					 (select * from 
					    (select v1.*,
								--v2.index_f_code as f_indexcode,
								v2.indexcode as f_indexcode,
								v2.indexname as f_indexname 
						 from hds.tr_ods_rmp_fi_x_news_index_tree_v2 v1 left join hds.tr_ods_rmp_fi_x_news_index_tree_v2 v2 on v1.index_f_code=v2.indexcode 
						 where v1.flag<>'1' and v2.flag<>'1' and v1.indexlevel in ('3','4') 
						 ) a  
					     left join (select tag,tag_cd,tag_ii,tag_ii_cd,importance,tag_type from pth_rmp.rmp_opinion_risk_info_tag) tag
							on a.indexname=tag.tag_ii and tag.tag_type=0
					  -- where   a.flag<>'1'  and a.indexlevel in ('3','4')
					) idx
					 -- (select newscode,NEWSDATE,'' msg-- CRNW0002_001 msg*/ 
					  -- from hds.tr_ods_rmp_fi_x_news_tcrnw0002 where flag<>'1') o3
				where o.newscode=o1.newscode and o1.itcode2=o2.itcode2 and idx.indexcode=o1.crnw0003_001 --and o.newscode=o3.newscode 
				  and cast(o1.crnw0003_006 as int)<0
			)t0
		)Final_Part
	)Final_News
)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI' 
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  ;