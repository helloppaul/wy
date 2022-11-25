-- 诚信 (同步方式：一天多批次覆盖)--
--入参：${ETL_DATE}(20220818 int)  -> to_date(notice_dt)，给NULL初始化全部日期数据
--/* 2022-8-29 \r\n 全局替换为 \\r\\n */
--/* 2022-9-25 新增 notice_date,notice_month 字段


-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态


with 
corp_chg as 
(
	select distinct a.corp_id,b.corp_name,b.credit_code,a.source_id,a.source_code
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
)
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(etl_date=${ETL_DATE},type_='cx')
select 
	msg_id as sid_kw,
	*
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		-- Final.msg_id,  --impala
		md5(concat(cid_chg.corp_id,cast(Final.notice_dt as string),Final.case_type_ii_cd)) as msg_id, 
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
		'' as CRNW0003_010,
		to_date(Final.notice_dt) as notice_date, 
		last_day(Final.notice_dt) as notice_month,
		0 as delete_flag,
		'' as create_by,
		current_timestamp() as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
		-- cast(from_unixtime(unix_timestamp(to_date(Final.notice_dt),'yyyy-MM-dd'),'yyyyMMdd') as int) as dt,
		-- 'cx' type_
	from 
	(
		SELECT distinct
			Final_CXSF.corp_id,Final_CXSF.corp_nm,
			Final_CXSF.notice_dt,
			Final_CXSF.msg_id,  
			Final_CXSF.msg_title,
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
			select 
				corp_id,
				corp_nm,
				notice_dt,
				'' as msg_id,
				'' as msg_title,  -- 诚信司法该title为空
				'' as case_type_cd,  -- 最外层sql再和舆情风险规则标签表关联
				'' as  case_type,
				RISK_TYPE_CD as case_type_ii_cd,
				RISK_TYPE as case_type_ii,
				importance,
				1 AS signal_type,  -- 信号类型  0:新闻舆情 1:诚信 2:司法
				src_table,
				cast(src_sid as string) as src_sid,
				NULL AS url_kw,
				NULL AS news_from,
				RISK_DESC as msg
			FROM
			(
				SELECT  
						COMPANY_ID as corp_id,
						COMPANY_NM as corp_nm,
						NOTICE_DT,
						RISK_TYPE,
						max(RISK_TYPE_CD) as RISK_TYPE_CD,
						IMPORTANCE,
						concat(cast(year(NOTICE_DT) as string),'年',cast(month(NOTICE_DT) as string),'月',cast(day(NOTICE_DT) as string),'日',',',
						COMPANY_NM,max(RISK_DESC_TMP),'的处罚措施。','\\r\\n', 
						'最新涉及处罚详情如下：','\\r\\n','\\r\\n',
						--group_concat(RISK_DESC,'\\r\\n')
						concat_ws('\\r\\n', collect_set( RISK_DESC))  
						,'')
						as RISK_DESC,
						max(SRC_TABLE) as SRC_TABLE,
						max(SRC_SID) as SRC_SID,
						0 as IS_DEL,
						NULL as CREATE_DT,
						max(UPDT_DT) as UPDT_DTS
					FROM (SELECT COMPANY_ID,
								COMPANY_NM,
								NOTICE_DT AS NOTICE_DT,
								OPERATER AS RISK_TYPE,
								'' as RISK_TYPE_CD,
								IMPORTANCE as IMPORTANCE,
								RISK_DESC_TMP AS RISK_DESC_TMP,
								RISK_DESC AS RISK_DESC,
								SRC_TABLE AS SRC_TABLE,
								SRC_SID AS SRC_SID,
								0 AS IS_DEL ,
								NULL AS CREATE_DT,
								UPDT_DT  -- 更新时间					 
							FROM (SELECT COMPANY_ID,
										COMPANY_NM,
										NOTICE_DT,
										PUNISH_DT,
										concat('被',OPERATER_TMP,'处以',translate(translate(PUNISH_TYPE,'被',''),'采取','')) as RISK_DESC_TMP,
										concat(PUNISH_TYPE,'(',OPERATER_TMP,')') as OPERATER,  
										CASE OPERATER_TMP
											WHEN '证监会' THEN
												if(PUNISH_TYPE in ('被立案调查','被市场禁入'),-3,-2)
											WHEN '交易所' THEN
												if(PUNISH_TYPE in ('被立案调查'),-3,-2)
											WHEN '银保监会' THEN
												if(PUNISH_TYPE in ('被立案调查'),-3,-2)
											WHEN '全国股转系统' THEN
												-2
											WHEN '交易商协会' THEN
												if(PUNISH_TYPE in ('被立案调查','被取消资格'),-3,-2)
											WHEN '公安及检察机关' THEN
												if(PUNISH_TYPE in ('被立案调查'),-3,-2)
											WHEN '公安及检察机关' THEN
												if(PUNISH_TYPE in ('其他机构'),-2,-1)
										END AS IMPORTANCE,
										concat(
											CASE WHEN case_title <> '' THEN concat('案件标题：' , case_title ,'\\r\\n') ELSE '' END,
											CASE WHEN DECISION_GOV <> '' THEN concat('处罚机构：',DECISION_GOV,'\\r\\n') ELSE '' END,
											CASE WHEN OPERATER_TMP <> '' THEN concat('处罚机构类型：',OPERATER_TMP,'\\r\\n') ELSE '' END,
											CASE WHEN PUNISH_TYPE <> '' THEN concat('处罚类型：',PUNISH_TYPE,'\\r\\n') ELSE '' END,
											CASE WHEN VIOLA_ACTION <> '' THEN concat('违规行为：',VIOLA_ACTION,'\\r\\n') ELSE '' END,
											CASE WHEN PUNISH_CONTENT <> '' THEN concat('处罚内容：',PUNISH_CONTENT,'\\r\\n') ELSE '' END,
											CASE WHEN REF_NUMBER <> '' THEN concat('发文号：',REF_NUMBER,'\\r\\n') ELSE '' END 						  
										) AS RISK_DESC,
										SRC_TABLE,
										SRC_SID,
										RM,
										UPDT_DT
										--count(1) over(partition by company_id) COMM
										--ROW_NUMBER() OVER(PARTITION BY COMPANY_ID ORDER BY SRC_TABLE DESC, SRC_SID DESC) RM
							FROM (
									select *,ROW_NUMBER() OVER(PARTITION BY COMPANY_ID,NOTICE_DT ORDER BY 1) RM from 
									(
										SELECT -- ID,
											ITCODE2 as COMPANY_ID,  -- 当事人代码(企业库) as 主体ID
											--IT0026_004 as COMPANY_ID2,  -- 当事人代码(金融库) as 主体ID2
											max(IT0026_005) as COMPANY_NM,  --当事人名称 as 主体名称
											to_date(cast(IT0026_006 as timestamp)) as NOTICE_DT, -- 公告日期 as 发生时间
											max(to_date(cast(IT0026_053 as timestamp))) as PUNISH_DT,  -- 处罚日期 as 处罚日期  (同字段 PUNISH_DT)
											max(IT0026_030) as PUNISH_OBJECT,  -- 主体公司机构代码(金融库) as 处罚对象
											max(IT0026_050) as PUNISH_OBJECT2, -- 主体公司机构代码(企业库) as 处罚对象
											-- CASE IT0026_029 
												-- WHEN '1' THEN '公司本身'
												-- WHEN '2' THEN '董监高'
												-- WHEN '3' THEN '参控股公司'
												-- WHEN '4' THEN '控股股东'
												-- WHEN '5' THEN '实际控制人'
												-- WHEN '6' THEN '主办券商'
												-- WHEN '7' THEN '中介结构'
												-- WHEN '8' THEN '基金经理'
												-- WHEN '9' THEN '其他'
												-- WHEN '10' THEN '非控股股东'
												-- ELSE NULL
											-- END as RELATION,  -- 与主体公司关系 as 与公司关系 
											IT0026_007 as case_title, -- 案件标题 as 案件标题
											max(IT0026_008) as PUNISH_RESULT, -- 处罚结果 as 处罚结果
											--IT0026_008 as VIOLA_LTYPE ,-- 处罚结果 as 违规类型
											max(IT0026_009) as VIOLA_ACTION, -- 违规行为
											--IT0026_009 as PENALTY_REASON,  -- 违规原因 as 处罚原因
											max(IT0026_016) as DECISION_GOV,  -- 处罚机构 用处颗粒度016字段，018字段是更新颗粒度处罚机构，比如某个地方的xxx处罚机构
											max(IT0026_011) AS PUNISH_CONTENT, -- 处罚内容
											CASE
											WHEN IT0026_018 LIKE '%股份转让%' OR
													IT0026_018 LIKE '%股转%' THEN
												'全国股转公司'
											WHEN IT0026_018 LIKE '%证券监督%' OR
													IT0026_018 LIKE '%证监%' THEN
												'证监会'
											WHEN IT0026_018 LIKE '%交易所%' or 
													IT0026_018 LIKE '%上交所%' OR
													IT0026_018 LIKE '%深交所%' OR
													IT0026_018 LIKE '%联交所%' OR
													IT0026_018 LIKE '%中金所%' OR
													IT0026_018 LIKE '%上期所%' OR
													IT0026_018 LIKE '%郑商所%' THEN
												'交易所'
											WHEN IT0026_018 LIKE '%保监%' OR
													IT0026_018 LIKE '%银监%' OR
													IT0026_018 LIKE '%保险监督%' OR
													IT0026_018 LIKE '%银行监督%' OR
													IT0026_018 LIKE '%银行业监督%' THEN
												'银保监会'
											WHEN IT0026_018 LIKE '%公安%' OR
													IT0026_018 LIKE '%警察%' OR
													IT0026_018 LIKE '%检察%' OR
													IT0026_018 LIKE '%司法%' OR
													IT0026_018 LIKE '%法院%' THEN
												'公安及检察机关'   
											WHEN IT0026_018 LIKE '%协会%' OR
													IT0026_018 LIKE '%消费%' THEN
												'交易商协会'
											ELSE
												'其他机构'
											END AS OPERATER_TMP,  --处罚机构
											CASE 
												WHEN IT0026_008 LIKE  '%立案调查%' THEN
													'被立案调查'
												WHEN  IT0026_008 LIKE '%市场禁入%' THEN   
													'被市场禁入'
												WHEN IT0026_008 like '%司法机关%' or IT0026_008 like '%拘留%' THEN   
													'被采取强制措施或逮捕'
												WHEN IT0026_008 like '%警示%'  THEN   
													'被监管警示'
												WHEN IT0026_008 like '%限制%' or IT0026_008 like '%停止接受%' or IT0026_008 like '%禁止进入%' or IT0026_008 like '责令停产停业' THEN   
													'被采取监管措施'
												WHEN IT0026_008 like '%谴责%'  or IT0026_008 like '%批评%' or IT0026_008 like '%警告%' THEN   
													'被公开谴责'
												WHEN (IT0026_008 like '%取消%'  and IT0026_008 like '%资格%' ) or IT0026_008 like '%吊销%' THEN 
													'被取消资格'
												WHEN IT0026_008 like '%监管关注%'  THEN 
													'被监管关注'
												ELSE '其他'
											END AS PUNISH_TYPE,  -- 处罚类型
											max(IT0026_017) as REF_NUMBER,  -- 发文批号 as 处罚文号
											max(IT0026_002) as SRC_SID, -- 违规记录主表ID as 源头SID数据
											upper('tr_ods_rmp_fi_TIT0026_V2_1') as SRC_TABLE, -- 源头表名
											current_timestamp() as UPDT_DT  -- 更新时间,
											FROM hds.tr_ods_rmp_fi_TIT0026_V2_1
											WHERE FLAG <> '1'  -- 数据更新标识 （仅保留有效的）
											AND IT0026_028 = '0'  -- 是否个人 (排除个人)
											AND IT0026_030 <>''  -- 处罚对象
											AND ITCODE2 <>''  -- 主体ID非空
											AND IT0026_005<>''  --企业名称非空，为空的为数据质量问题
											GROUP BY ITCODE2,IT0026_006,IT0026_007,IT0026_018,IT0026_008  --按照企业ID，日期，案件标题,处罚机构,处罚类型 去重
									) A	
						)B
						where B.RM <= 10 and B.PUNISH_TYPE<>'其他' and B.COMPANY_NM<>''
						)BAS
					)T where T.IMPORTANCE IS NOT NULL and RISK_TYPE IS NOT NULL 
					group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,IMPORTANCE
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0)) 
;