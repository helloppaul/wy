-- 限制高消费 (同步方式：一天多批次覆盖) --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt)
--/* 2022-8-29 \r\n 全局替换为 \\r\\n */


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
),
A as 
(
	SELECT 
		ITCODE2 as COMPANY_ID, -- 公司代码(企业库)/当事人 as 公司代码
		max(ITCODE) as COMPANY_ID2, -- 公司代码(金融库) AS 公司代码2
		max(ITNAME) as COMPANY_NM,
		to_date(cast(CR0164_009 as TIMESTAMP)) as NOTICE_DT,  -- 限令发布日期 as 发生时间
		to_date(cast(max(CR0164_003) as TIMESTAMP)) as register_dt,-- 立案时间 as 立案时间
		
		max(CR0164_004) as execed_man, -- 被执行人 as 被执行人
		max(CR0164_002) as case_title,  -- 案件标题
		CR0164_006 as case_no,  -- 案号 as 案号
		max(CR0164_008) as apply_exec, --  as  申请执行人
		max(CR0164_007) as exec_court , -- 执行法院
		--count(*) as case_no_cnt,  --当天某公司的案号数量
		max(ID)as SRC_SID, -- 流水号 as 源头SID数据
		upper('tr_ods_rmp_fi_TCR0164') as SRC_TABLE, -- 源头表名
		current_timestamp() as UPDT_DT   -- 更新时间,
	FROM  hds.tr_ods_rmp_fi_TCR0164  --开庭庭审当事人表
	where IsCD = '0'  --是否撤销，考虑未撤销 限制高消费的
	  and ITCODE2 <> ''  -- 当事人非空/企业非空
	group by ITCODE2,CR0164_009,CR0164_006   --根据 公司,时间,案号 分组	
)
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(etl_date=${ETL_DATE},type_='sf_xzgxf')
select msg_id as sid_kw,* 
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		-- Final.msg_id,  --impala
		concat(Final.corp_id,'_',md5(concat(cast(Final.notice_dt as string),Final.msg_title,Final.case_type_ii,Final.msg))) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
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
		to_date(Final.notice_dt) as notice_date, 
		last_day(Final.notice_dt) as notice_month,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
		-- cast(from_unixtime(unix_timestamp(to_date(Final.notice_dt),'yyyy-MM-dd'),'yyyyMMdd') as int) as dt,
		-- 'sf_xzgxf' as type_
	from 
	(
		SELECT distinct
			Final_CXSF.corp_id,Final_CXSF.corp_nm,
			Final_CXSF.notice_dt,
			Final_CXSF.msg_id,Final_CXSF.msg_title,
			tag.tag_cd as case_type_cd,
			tag.tag as case_type,
			tag.tag_ii_cd as case_type_ii_cd,
			Final_CXSF.case_type_ii,
			tag.importance,
			Final_CXSF.signal_type,
			Final_CXSF.src_table,
			Final_CXSF.src_sid,
			Final_CXSF.url_kw,
			Final_CXSF.news_from,
			Final_CXSF.msg
		FROM 
		(	
			--限制高消费
			select 
				0 AS sid_kw,
				COMPANY_ID as corp_id,
				COMPANY_NM as corp_nm,
				notice_dt,
				'' as msg_id,
				'' as msg_title,  -- 诚信司法该title为空
				'' as case_type_cd,  -- 最外层sql再和舆情风险规则标签表关联
				'' as  case_type,
				RISK_TYPE_CD as case_type_ii_cd,
				RISK_TYPE as case_type_ii,
				importance,
				2 AS signal_type,  -- 信号类型  0:新闻舆情 1:诚信 2:司法
				src_table,
				cast(src_sid as string) as src_sid,
				NULL AS url_kw,
				NULL AS news_from,
				RISK_DESC as msg
			FROM
			(
				SELECT  --SEQ_COMPY_RISK_SUMM.NEXTVAL,
					COMPANY_ID,
					--COMPANY_ID2,
					COMPANY_NM,
					NOTICE_DT,
					RISK_TYPE,
					RISK_TYPE_CD,  --可考虑不要
					IMPORTANCE,
					concat(cast(year(NOTICE_DT) as string),'年',cast(month(NOTICE_DT) as string),'月',cast(day(NOTICE_DT) as string),'日',',',
							max(COMPANY_NM),'被采取限制消费措施','，',
							'近1年内有限制消费记录',cast(max(limit_cnt_year) as string),'条，',
							'历史限制消费记录共',cast(max(limit_cnt) as string),'条。','\\r\\n',
							'最新案件详情如下：','\\r\\n','\\r\\n',
						-- group_concat(RISK_DESC_tmp,'')
						concat_ws('',collect_set(RISK_DESC_tmp))
						)
					as RISK_DESC,
					max(SRC_TABLE) as SRC_TABLE,
					max(SRC_SID) as SRC_SID,
					0 as IS_DEL,
					NULL as CREATE_DT,
					max(UPDT_DT) as UPDT_DT
				FROM 
				(
					select 
						COMPANY_ID,
						--COMPANY_ID2,
						COMPANY_NM,
						NOTICE_DT,
						'被限制高消费' AS RISK_TYPE,
						'JU023' AS RISK_TYPE_CD,  --可考虑不要
						-2 AS IMPORTANCE, 
						register_dt,
						execed_man,
						case_title,
						limit_cnt_year,
						--sum(case when NOTICE_DT>= years_sub(NOTICE_DT,1 )  THEN 1 ELSE 0 END) over(partition by COMPANY_ID,NOTICE_DT) as limit_cnt_year,
						count(*) over(partition by COMPANY_ID) as limit_cnt,
						case_no,
						apply_exec,
						exec_court,
						concat(
								Case when case_title <> '' then concat('案件标题：',cast(case_title as string),'\\r\\n') else '' END,
								Case when register_dt is not null then concat('立案日期：',cast(register_dt as string),'\\r\\n') else '' END,
								Case when execed_man <> '' then concat('被执行人:',execed_man,'\\r\\n') else '' END,
								Case when case_no <> '' then concat('案号：',case_no,'\\r\\n') else '' END,
								Case when exec_court <> '' then concat('申请执行人：',exec_court,'\\r\\n') else '' END,'\\r\\n')
						as RISK_DESC_tmp,
						SRC_TABLE,
						SRC_SID,
						UPDT_DT,
						ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as RM
					FROM
					(
						select 
							tm1.COMPANY_ID,
							tm1.company_nm,
							tm1.NOTICE_DT,
							tm1.register_dt,
							tm1.execed_man,
							tm1.case_title,
							tm1.case_no,
							tm1.apply_exec ,
							tm1.exec_court ,
							tm1.SRC_SID,
							tm1.SRC_TABLE,
							tm1.UPDT_DT,
							count(*) as limit_cnt_year  --最近一年案件数量
						FROM A tm1 join A tm2 on tm1.COMPANY_ID= tm2.COMPANY_ID
						where tm2.NOTICE_DT<tm1.NOTICE_DT and tm2.NOTICE_DT> add_months(tm1.NOTICE_DT,-36)   -- 限制案件为最近一年
						group by tm1.COMPANY_ID,tm1.company_nm,tm1.NOTICE_DT,tm1.register_dt,tm1.execed_man,tm1.case_title,tm1.case_no,tm1.apply_exec,tm1.exec_court,tm1.SRC_SID,tm1.SRC_TABLE,tm1.UPDT_DT
					) B
				) T where T.rm<=10 group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE 
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  
;
	