-- 裁判文书 (同步方式：一天多批次覆盖)- --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
--/* 2022-8-29 \r\n 全局替换为 \\r\\n */


-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态with 



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
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(dt=${ETL_DATE},type_='sf_cpws')
select distinct
	cid_chg.corp_id as corp_id,
	Final.corp_nm,
	Final.notice_dt,
	-- Final.msg_id,   --impala
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
	0 as delete_flag,
	'' as create_by,
	current_timestamp() as create_time,
	'' as update_by,
	current_timestamp() update_time,
	0 as version
	-- cast(from_unixtime(unix_timestamp(to_date(Final.notice_dt),'yyyy-MM-dd'),'yyyyMMdd') as int) as dt,
	-- 'sf_cpws' as type_
from 
(
	SELECT distinct
		Final_CXSF.corp_id,Final_CXSF.corp_nm,
		Final_CXSF.notice_dt,
		Final_CXSF.msg_id,Final_CXSF.msg_title,
		tag.tag_cd as case_type_cd,
		tag.tag as case_type,
		tag.tag_ii_cd AS case_type_ii_cd,Final_CXSF.case_type_ii,
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
						company_nm,
						'新增',cast(count(case_no) as string),'条',translate(translate(RISK_TYPE,'作为被告',''),'(裁判文书)',''),'案件',',',
						'最新涉及案件详情如下：','\\r\\n','\\r\\n',
						--group_concat(RISK_DESC_tmp,'')
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
					COMPANY_ID2,
					COMPANY_NM,
					NOTICE_DT,
					CASE 
						WHEN (case_reason like '%借贷%' OR case_reason like '%借款%' OR case_reason like '%拆借%' OR case_reason like '%债务%' or  case_reason like '%债权%' or  case_reason like '%金融不良债权%')  
							  AND case_reason NOT like '%破产%'  
							  AND case_reason NOT like '%清偿%'  
							  AND case_reason NOT like '%追偿%'   
							  AND case_reason NOT like '%追收%'   
							  AND case_reason NOT like '%请求确认债务人行为无效纠纷%' THEN '作为被告涉借贷纠纷(裁判文书)'  --JU019
						WHEN case_reason like '%票据%' THEN '作为被告涉票据纠纷(裁判文书)' -- JU022
						WHEN case_reason like '%破产%'  
							 OR case_reason like '%清偿%'   
							 OR case_reason like '%追收%'  
							 OR case_reason like '%请求确认债务人行为无效纠纷%' 
							 OR case_reason like '%别除权纠纷%' 
							 OR case_reason like '%损害债务人利益赔偿纠纷%' 
							 OR case_reason like '%管理人责任纠纷%' 
							 OR case_reason like '%取回权%' THEN '作为被告涉破产相关纠纷(裁判文书)'  --JU023
						WHEN (case_reason like '%保全%' 
								and (case_reason like '%财产%' or case_reason like '%行为%' or case_reason like '%证据%') 
								and case_reason not like '%纠纷%' 
								and (case_reason like '%申请诉前%' or  case_reason like '%申请仲裁%' or case_reason like '%仲裁程序%' or case_reason like '%申请执行%' )
							 ) 
							 or case_reason like '%申请中止支付信用证项下款项%' 
							 or case_reason like '%申请中止支付保函项下款项%' THEN '作为被告涉申请保全相关纠纷(裁判文书)' --JU024
						WHEN case_reason like '%证券%' 
							 or case_reason like '%股票%' 
							 or case_reason like '%国债%' 
							 or case_reason like '%期货%'  
							 or case_reason like '%信托%'  
							 or case_reason like '%保险%' 
							 or case_reason like '%金融委托理财%'  
							 or  case_reason like '%欺诈客户责任纠纷%' 
							 or case_reason like '%融资融券交易纠纷%'  
							 or case_reason like '%客户交易结算资金纠纷%' 
							 or case_reason like '%银行卡纠纷%'  
							 or case_reason like '%信用卡纠纷%'  
							 or case_reason like '%储蓄存款合同纠纷%' 
							 or case_reason like '%信用证%'  
							 or case_reason like '%独立保函%'  THEN '作为被告涉金融类纠纷(裁判文书)' --JU025
						WHEN case_reason like '%建设工程%' 
							 OR case_reason like '%装饰%' 
							 OR case_reason like '%建房%' 
							 OR case_reason like '%土地租赁%'   
							 OR case_reason like '%房屋租赁%'   
							 OR case_reason like '%建筑设备租赁%'   
							 OR case_reason LIKE '租赁合同纠纷' 
							 OR case_reason like '%房地产开发经营合同纠纷%' 
							 OR case_reason like '%委托代建合同纠纷%' 
							 OR case_reason like '%合作开发房地产%' 
							 OR case_reason like '%项目转让合同纠纷%' 
							 OR case_reason like '%临时用地合同纠纷%' 
							 OR case_reason like '%建设用地%' 
							 OR case_reason like '%拆迁%' 
							 OR case_reason like '%商品房%' 
							 OR case_reason like '%经济适用房转让合同纠纷%' 
							 OR case_reason like '%房屋买卖%' THEN '作为被告涉房地产建筑类合同纠纷(裁判文书)' --JU020
						WHEN (case_reason like '%抵押权纠纷%' OR case_reason like '%质权纠纷%' ) THEN '作为被告涉担保物权纠纷案件(裁判文书)'  --JU021
						WHEN  CASE_REASON like '%合同纠纷%' THEN '作为被告涉其他合同纠纷(裁判文书)' --JU026
						ELSE '作为被告涉其他纠纷(裁判文书)' --JU027
					END AS RISK_TYPE,
					CASE 
						WHEN (case_reason like '%借贷%' OR case_reason like '%借款%' OR case_reason like '%拆借%' OR case_reason like '%债务%' or  case_reason like '%债权%' or  case_reason like '%金融不良债权%')  
							  AND case_reason NOT like '%破产%'  
							  AND case_reason NOT like '%清偿%'  
							  AND case_reason NOT like '%追偿%'   
							  AND case_reason NOT like '%追收%'   
							  AND case_reason NOT like '%请求确认债务人行为无效纠纷%' THEN 'JU004001'  
						WHEN case_reason like '%票据%' THEN 'JU007001' 
						WHEN case_reason like '%破产%'  
							 OR case_reason like '%清偿%'   
							 OR case_reason like '%追收%'  
							 OR case_reason like '%请求确认债务人行为无效纠纷%' 
							 OR case_reason like '%别除权纠纷%' 
							 OR case_reason like '%损害债务人利益赔偿纠纷%' 
							 OR case_reason like '%管理人责任纠纷%' 
							 OR case_reason like '%取回权%' THEN 'JU008001' 
						WHEN (case_reason like '%保全%' 
								and (case_reason like '%财产%' or case_reason like '%行为%' or case_reason like '%证据%') 
								and case_reason not like '%纠纷%' 
								and (case_reason like '%申请诉前%' or  case_reason like '%申请仲裁%' or case_reason like '%仲裁程序%' or case_reason like '%申请执行%' )
							 ) 
							 or case_reason like '%申请中止支付信用证项下款项%' 
							 or case_reason like '%申请中止支付保函项下款项%' THEN 'JU009001' 
						WHEN case_reason like '%证券%' 
							 or case_reason like '%股票%' 
							 or case_reason like '%国债%' 
							 or case_reason like '%期货%'  
							 or case_reason like '%信托%'  
							 or case_reason like '%保险%' 
							 or case_reason like '%金融委托理财%'  
							 or  case_reason like '%欺诈客户责任纠纷%' 
							 or case_reason like '%融资融券交易纠纷%'  
							 or case_reason like '%客户交易结算资金纠纷%' 
							 or case_reason like '%银行卡纠纷%'  
							 or case_reason like '%信用卡纠纷%'  
							 or case_reason like '%储蓄存款合同纠纷%' 
							 or case_reason like '%信用证%'  
							 or case_reason like '%独立保函%'  THEN 'JU010001' 
						WHEN case_reason like '%建设工程%' 
							 OR case_reason like '%装饰%' 
							 OR case_reason like '%建房%' 
							 OR case_reason like '%土地租赁%'   
							 OR case_reason like '%房屋租赁%'   
							 OR case_reason like '%建筑设备租赁%'   
							 OR case_reason LIKE '租赁合同纠纷' 
							 OR case_reason like '%房地产开发经营合同纠纷%' 
							 OR case_reason like '%委托代建合同纠纷%' 
							 OR case_reason like '%合作开发房地产%' 
							 OR case_reason like '%项目转让合同纠纷%' 
							 OR case_reason like '%临时用地合同纠纷%' 
							 OR case_reason like '%建设用地%' 
							 OR case_reason like '%拆迁%' 
							 OR case_reason like '%商品房%' 
							 OR case_reason like '%经济适用房转让合同纠纷%' 
							 OR case_reason like '%房屋买卖%' THEN 'JU005001' 
						WHEN (case_reason like '%抵押权纠纷%' OR case_reason like '%质权纠纷%' ) THEN 'JU006001'  --JU021
						WHEN  CASE_REASON like '%合同纠纷%' THEN 'JU011001' --JU026
						ELSE 'JU012001' --JU027
					END AS RISK_TYPE_CD,  --可考虑不要
					CASE 
						WHEN (case_reason like '%借贷%' OR case_reason like '%借款%' OR case_reason like '%拆借%' OR case_reason like '%债务%' or  case_reason like '%债权%' or  case_reason like '%金融不良债权%')  
							  AND case_reason NOT like '%破产%'  
							  AND case_reason NOT like '%清偿%'  
							  AND case_reason NOT like '%追偿%'   
							  AND case_reason NOT like '%追收%'   
							  AND case_reason NOT like '%请求确认债务人行为无效纠纷%' THEN -2  --JU019
						WHEN case_reason like '%票据%' THEN -2 -- JU022
						WHEN case_reason like '%破产%'  
							 OR case_reason like '%清偿%'   
							 OR case_reason like '%追收%'  
							 OR case_reason like '%请求确认债务人行为无效纠纷%' 
							 OR case_reason like '%别除权纠纷%' 
							 OR case_reason like '%损害债务人利益赔偿纠纷%' 
							 OR case_reason like '%管理人责任纠纷%' 
							 OR case_reason like '%取回权%' THEN -3  --JU023
						WHEN (case_reason like '%保全%' 
								and (case_reason like '%财产%' or case_reason like '%行为%' or case_reason like '%证据%') 
								and case_reason not like '%纠纷%' 
								and (case_reason like '%申请诉前%' or  case_reason like '%申请仲裁%' or case_reason like '%仲裁程序%' or case_reason like '%申请执行%' )
							 ) 
							 or case_reason like '%申请中止支付信用证项下款项%' 
							 or case_reason like '%申请中止支付保函项下款项%' THEN -2 --JU024
						WHEN case_reason like '%证券%' 
							 or case_reason like '%股票%' 
							 or case_reason like '%国债%' 
							 or case_reason like '%期货%'  
							 or case_reason like '%信托%'  
							 or case_reason like '%保险%' 
							 or case_reason like '%金融委托理财%'  
							 or  case_reason like '%欺诈客户责任纠纷%' 
							 or case_reason like '%融资融券交易纠纷%'  
							 or case_reason like '%客户交易结算资金纠纷%' 
							 or case_reason like '%银行卡纠纷%'  
							 or case_reason like '%信用卡纠纷%'  
							 or case_reason like '%储蓄存款合同纠纷%' 
							 or case_reason like '%信用证%'  
							 or case_reason like '%独立保函%'  THEN -2 --JU025
						WHEN case_reason like '%建设工程%' 
							 OR case_reason like '%装饰%' 
							 OR case_reason like '%建房%' 
							 OR case_reason like '%土地租赁%'   
							 OR case_reason like '%房屋租赁%'   
							 OR case_reason like '%建筑设备租赁%'   
							 OR case_reason LIKE '租赁合同纠纷' 
							 OR case_reason like '%房地产开发经营合同纠纷%' 
							 OR case_reason like '%委托代建合同纠纷%' 
							 OR case_reason like '%合作开发房地产%' 
							 OR case_reason like '%项目转让合同纠纷%' 
							 OR case_reason like '%临时用地合同纠纷%' 
							 OR case_reason like '%建设用地%' 
							 OR case_reason like '%拆迁%' 
							 OR case_reason like '%商品房%' 
							 OR case_reason like '%经济适用房转让合同纠纷%' 
							 OR case_reason like '%房屋买卖%' THEN -2 --JU020
						WHEN (case_reason like '%抵押权纠纷%' OR case_reason like '%质权纠纷%' ) THEN -1  --JU021
						WHEN  CASE_REASON like '%合同纠纷%' THEN -1 --JU026
						ELSE -1 --JU027
					END AS IMPORTANCE, 
					judge_dt,
					involve,
					involve_role,
					involve_judgement,
					case_reason,
					case_no,
					case_type,
					involve_money,
					judge_link,		
					concat(
						Case when judge_dt is not null then concat('判决日期：',cast(judge_dt as string),'\\r\\n') else '' END,
						Case when involve <> '' then concat('涉案公司：',involve,'\\r\\n') else '' END,
						Case when involve_role <> '' then concat('涉案公司角色：',involve_role,'\\r\\n') else '' END,
						Case when involve_judgement <> '' then concat('涉案公司判决结果：',involve_judgement,'\\r\\n') else '' END,
						Case when case_reason <> '' then concat('案由：',case_reason,'\\r\\n') else '' END,
						Case when case_no <> '' then concat('案号：',case_no,'\\r\\n') else '' END,
						Case when case_type <> '' then concat('案件类型：',case_type,'\\r\\n') else '' END,
						Case when involve_money is not null then concat('涉案判决金额：',cast(involve_money as string),'\\r\\n') else '' END,
						Case when judge_link <> '' then concat('裁判文书链接：',judge_link,'\\r\\n') else '' END,'\\r\\n'
						)
					as RISK_DESC_tmp,
					SRC_TABLE,
					SRC_SID,
					UPDT_DT,
					--count(*) over(partition by COMPANY_ID,NOTICE_DT) as case_no_cnt,
					ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as RM
				FROM
				(
					SELECT 
						o.ITCODE2 as COMPANY_ID, -- 公司代码(企业库)/当事人 as 公司代码
						max(o.ITCODE) as COMPANY_ID2, -- 公司代码(金融库) AS 公司代码2
						max(o.ITNAME) as COMPANY_NM,
						to_date(cast(o1.CR0055_015 as TIMESTAMP)) as NOTICE_DT,  -- 公告日期 as 发生时间
						max(to_date(cast(o2.CR0014_005 as TIMESTAMP))) as judge_dt,-- 判决日期 as 判决日期
						max(o.CR0081_005) as involve, -- 当事人 as 涉案公司
						max(o.CR0081_004) as involve_role, --当事人类型 as  涉案公司角色
						'' as involve_judgement, -- 缺失！！！ 涉案公司判决结果
						max(o1.CR0055_030) as case_reason, --案件类型（案由） as 案由
						o1.CR0055_005 as case_no,  -- 案号 as 案号
						max(o1.CR0055_002) as case_type , -- 文书类型 as 案件类型(案由)
						cast(max(o1.CR0055_034) as double) as involve_money, --涉案金额(标准) as 涉案判决金额
						max(o2.CR0014_006) as judge_link, -- 裁判文书地址 as 裁判文书链接 
						--count(*) as case_no_cnt,  --当天某公司的案号数量
						max(o1.CR0055_001) as SRC_SID, -- 案件流水号 as 源头SID数据
						upper('tr_ods_rmp_fi_TCR0081_V3,tr_ods_rmp_fi_TCR0055_V3') as SRC_TABLE, -- 源头表名
						current_timestamp() as UPDT_DT   -- 更新时间,
					FROM hds.tr_ods_rmp_fi_TCR0081_V3 o 
					join hds.tr_ods_rmp_fi_TCR0055_V3 o1 
						on o.CR0081_001=o1.CR0055_001  --通过案件流水号关联
					join hds.tr_ods_rmp_fi_TCR0014_V3 o2		
						on o1.CR0055_001 = cast(o2.ID as bigint)
					where o.CR0081_006 = '0'  -- 只考虑企业的裁判文书，非个人
					  and o.ITCODE2 <> ''  -- 当事人非空/企业非空
					  and o.CR0081_003 in ('2','4','6','8')   --当事人类型为：被告，被申请人，被执行人，被上诉人
					  and o.FLAG<>'1'  AND  o1.FLAG<>'1' AND o2.FLAG<>'1'  --只取有效
					  and o.ITNAME<>''  --企业名称非空处理
					group by o.ITCODE2,o1.CR0055_015,o1.CR0055_005
				) A --where case_no_cnt<=20  --某天某家公司的案件数量超过20,则该块数据剔除
			) T where T.rm<=10 group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE 
		)Final_Part
	)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  ;	