-- 对外持股冻结 (同步方式：一天多批次覆盖) --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
--/* 2022-8-29 \r\n 全局替换为 \\r\\n */


-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态withwith


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
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(etl_date=${ETL_DATE},type_='sf_dwcgdj')
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
		-- 'sf_dwcgdj' as type_
	from 
	(
		SELECT distinct
			Final_CXSF.corp_id,Final_CXSF.corp_nm,
			Final_CXSF.notice_dt,
			Final_CXSF.msg_id,Final_CXSF.msg_title,
			tag.tag_cd as case_type_cd,
			tag.tag as case_type,
			tag.tag_ii_cd as case_type_ii_cd,Final_CXSF.case_type_ii,
			tag.importance,
			Final_CXSF.signal_type,
			Final_CXSF.src_table,
			Final_CXSF.src_sid,
			Final_CXSF.url_kw,
			Final_CXSF.news_from,
			Final_CXSF.msg
		FROM 
		(	
			--对外持股冻结
			select 
				0 AS sid_kw,
				corp_id,
				corp_nm,
				notice_dt,
				'' as  msg_id,
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
					COMPANY_ID as corp_id,
					COMPANY_NM as corp_nm,
					NOTICE_DT,
					RISK_TYPE,
					RISK_TYPE_CD,  --可考虑不要
					IMPORTANCE,
					concat('根据最新司法冻结公告显示，',
							company_nm,
							'对外持有的部分公司股权涉及司法冻结，',
							'冻结详情如下：','\\r\\n','\\r\\n',
						-- group_concat(RISK_DESC_tmp,'')
						concat_ws('', collect_set(RISK_DESC_tmp))
						)
					as RISK_DESC,
					MAX(SRC_TABLE) AS SRC_TABLE,
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
						'对外持股份被冻结' AS RISK_TYPE,
						'JU003001' AS RISK_TYPE_CD,  --可考虑不要
						-3 AS IMPORTANCE, 
						FROZEN_START_DT,
						FROZEN_END_DT,
						SHAREHD_AMT,
						FROZEN_TYPE,
						EXECUTE_NOTICE_NO,
						EXECUTIVE_COURT,
						concat(
								Case when NOTICE_DT is not null then concat('公示日期:',cast(NOTICE_DT as string),'\\r\\n') else '' END,
								Case when FROZEN_START_DT is not null then concat('冻结起始日:',if(cast(FROZEN_START_DT as string)='1990-01-01','-',cast(FROZEN_START_DT as string)),'\\r\\n') else '' END,
								Case when FROZEN_END_DT is not null then concat('冻结终止日:',if(cast(FROZEN_END_DT as string)='1990-01-01','-',cast(FROZEN_END_DT as string)),'\\r\\n') else '' END,
								Case when SHAREHD_AMT <> '' then concat('冻结金额:',SHAREHD_AMT,'\\r\\n') else '' END,
								Case when FROZEN_TYPE <> '' then concat('冻结类型：',FROZEN_TYPE,'\\r\\n') else '' END,
								
								Case when EXECUTE_NOTICE_NO <> '' then concat('执行通知书文号：',EXECUTE_NOTICE_NO,'\\r\\n') else '' END,
								
								Case when EXECUTIVE_COURT <> '' then concat('执行法院：',EXECUTIVE_COURT,'\\r\\n') else '' END,'\\r\\n'
							) 
						as RISK_DESC_tmp,
						SRC_TABLE,
						SRC_SID,
						UPDT_DT,
						ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as RM
					FROM
					(
						SELECT 
							o.CR0122_007 as COMPANY_ID, -- 被执行人代码2 as 公司代码
							--o.ITCODE as COMPANY_ID2, -- 被执行人 AS 公司代码2
							o.CR0122_001 as COMPANY_NM,  --被执行人 as 公司名称
							to_date(cast(o1.CR0123_012 as TIMESTAMP)) as NOTICE_DT,  -- 公示日期 as 发生时间
							
							to_date(cast(o1.CR0123_009 as TIMESTAMP)) as FROZEN_START_DT , --冻结期限自 as 冻结起始日
							to_date(cast(o1.CR0123_010 as TIMESTAMP)) as FROZEN_END_DT , -- 冻结期限至 as 冻结终止日
							o1.CR0123_006 as SHAREHD_AMT, -- 被执行人持有股权、其它投资权益的数额 as 冻结金额(包含数字+单位)
							cast(regexp_extract(o1.CR0123_006,'([0-9]+.[0-9]+)',1) as double) as SHAREHD_AMT_NUM, --冻结金额(仅包含数字)
							translate(regexp_extract(o1.CR0123_006,'([^0-9.])',1),'\\r\\n','') as SHAREHD_AMT_UNIT, --冻结金额(单位)
							o1.CR0123_002 as FROZEN_TYPE, -- 执行事项 as  冻结类型
							o1.CR0123_004 as EXECUTE_NOTICE_NO, -- 执行通知书文号 as 执行通知书文号
							o1.CR0123_001 as EXECUTIVE_COURT, -- 执行法院 as 执行法院
							o1.CR0122_ID as SRC_SID, -- 司法协助ID as 源头SID数据
							upper('tr_ods_rmp_fi_TCR0122,tr_ods_rmp_fi_TCR0123') as SRC_TABLE, -- 源头表名
							current_timestamp() as UPDT_DT   -- 更新时间,
						FROM  hds.tr_ods_rmp_fi_TCR0122 o  --司法协助表
						join hds.tr_ods_rmp_fi_TCR0123 o1  --司法协助冻结信息
						where o.CR0122_005 in ('股权冻结','股权冻结|已冻结','股权冻结|冻结','股权冻结丨冻结','股权冻结|','冻结','已冻结','完全冻结') --冻结状态限制
						and o.id=o1.CR0122_ID
						and o.ITCODE2 <> ''  -- 当事人非空/企业非空
						and o.CR0122_007<>''  -- 被执行人企业ID不为空，为空的为自然人，预警的目标范围是企业
						and (o1.CR0123_006 <> '' or CR0123_006 <> '0万人民币元') -- 被执行人持有股权、其它投资权益的数额 （冻结金额）
						and (o1.CR0123_012 <> '' and o1.CR0123_009 <> '')  --公示日期 和 冻结期限 不同时 为空
						and o1.flag <>'1' and o.flag<>'1'  -- 仅保留有效数据 
					) A where (SHAREHD_AMT_NUM>1000 and SHAREHD_AMT_UNIT in ('万元人民币','万人民币','万人民币元','万元人民币','万','万股'))  
						or (SHAREHD_AMT_NUM>0.1 and SHAREHD_AMT_UNIT in ('万万元','万万股'))
				) T where T.rm<=10 and t.NOTICE_DT>'1990-01-01'
					group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE 
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  
;