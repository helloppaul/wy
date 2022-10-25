-- 开庭公告 (同步方式：一天多批次覆盖) --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态withwith with 

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
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(etl_date=${ETL_DATE},type_='sf_ktgg')
select 
	concat(corp_id,'_',md5(concat(cast(notice_dt as string),msg_title,case_type_ii,msg))) as sid_kw,
	* 
from  
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		-- Final.msg_id,  --impala
		concat(md5(concat(cast(Final.notice_dt as string),Final.msg))) as msg_id,  
		-- concat(Final.corp_id,'_',md5(concat(cast(Final.notice_dt as string),Final.msg_title,Final.case_type_ii,Final.msg))) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
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
		-- 'sf_ktgg' as type_
	from 
	(
		SELECT distinct
			Final_CXSF.corp_id,Final_CXSF.corp_nm,
			Final_CXSF.notice_dt,
			Final_CXSF.msg_id,Final_CXSF.msg_title,
			tag.tag_cd as case_type_cd,
			tag.tag as case_type,
			Final_CXSF.case_type_ii_cd,Final_CXSF.case_type_ii,
			tag.importance,
			Final_CXSF.signal_type,
			Final_CXSF.src_table,
			Final_CXSF.src_sid,
			Final_CXSF.url_kw,
			Final_CXSF.news_from,
			Final_CXSF.msg
		FROM 
		(	
			--开庭公告
			select 
				0 AS sid_kw,
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
					concat(cast(year(NOTICE_DT) as string),'年',cast(month(NOTICE_DT) as string),'月',cast(day(NOTICE_DT) as string),'日',',',
							company_nm,
							'新增',cast(count(*) as string),'条',translate(translate(RISK_TYPE,'作为被告',''),'(开庭公告)',''),'案件。','\\r\\n',
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
						concat(
								Case when NOTICE_DT is not null then concat('开庭日期:',cast(NOTICE_DT as string),'\\r\\n') else '' END,
								Case when involve <> '' then concat('涉案公司:',involve,'\\r\\n') else '' END,
								Case when involve_role <> '' then concat('涉案公司角色：',involve_role,'\\r\\n') else '' END,
								Case when case_reason <> '' then concat('案由：',case_reason,'\\r\\n') else '' END,
								Case when case_no <> '' then concat('案号：',case_no,'\\r\\n') else '' END,
								Case when court <> '' then concat('法院：',cast(court as string),'\\r\\n') else '' END,
								Case when court_link <> '' then concat('开庭公告链接：(',court_link,') ') else '' END,'\\r\\n','\\r\\n')
						as RISK_DESC_tmp,
						CASE 
							WHEN (case_reason like '%借贷%' OR case_reason like '%借款%' OR case_reason like '%拆借%' OR case_reason like '%债务%' or  case_reason like '%债权%' or  case_reason like '%金融不良债权%')  
								AND case_reason NOT like '%破产%'  
								AND case_reason NOT like '%清偿%'  
								AND case_reason NOT like '%追偿%'   
								AND case_reason NOT like '%追收%'   
								AND case_reason NOT like '%请求确认债务人行为无效纠纷%' THEN '作为被告涉借贷纠纷(开庭公告)'  --JU028
							WHEN case_reason like '%票据%' THEN '作为被告涉票据纠纷(开庭公告)' -- JU031
							WHEN case_reason like '%破产%'  
								OR case_reason like '%清偿%'   
								OR case_reason like '%追收%'  
								OR case_reason like '%请求确认债务人行为无效纠纷%' 
								OR case_reason like '%别除权纠纷%' 
								OR case_reason like '%损害债务人利益赔偿纠纷%' 
								OR case_reason like '%管理人责任纠纷%' 
								OR case_reason like '%取回权%' THEN '作为被告涉破产相关纠纷(开庭公告)'  --JU032
							WHEN (case_reason like '%保全%' 
									and (case_reason like '%财产%' or case_reason like '%行为%' or case_reason like '%证据%') 
									and case_reason not like '%纠纷%' 
									and (case_reason like '%申请诉前%' or  case_reason like '%申请仲裁%' or case_reason like '%仲裁程序%' or case_reason like '%申请执行%' )
								) 
								or case_reason like '%申请中止支付信用证项下款项%' 
								or case_reason like '%申请中止支付保函项下款项%' THEN '作为被告涉申请保全相关纠纷(开庭公告)' --JU033
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
								or case_reason like '%独立保函%'  THEN '作为被告涉金融类纠纷(开庭公告)' --JU034
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
								OR case_reason like '%房屋买卖%' THEN '作为被告涉房地产建筑类合同纠纷(开庭公告)' --JU029
							WHEN (case_reason like '%抵押权纠纷%' OR case_reason like '%质权纠纷%' ) THEN '作为被告涉担保物权纠纷案件(开庭公告)'  --JU030
							WHEN  CASE_REASON like '%合同纠纷%' THEN '作为被告涉其他合同纠纷(开庭公告)' --JU035
							ELSE '作为被告涉其他纠纷(开庭公告)' --JU036
						END AS RISK_TYPE,
						CASE 
							WHEN (case_reason like '%借贷%' OR case_reason like '%借款%' OR case_reason like '%拆借%' OR case_reason like '%债务%' or  case_reason like '%债权%' or  case_reason like '%金融不良债权%')  
								AND case_reason NOT like '%破产%'  
								AND case_reason NOT like '%清偿%'  
								AND case_reason NOT like '%追偿%'   
								AND case_reason NOT like '%追收%'   
								AND case_reason NOT like '%请求确认债务人行为无效纠纷%' THEN 'JU028'  
							WHEN case_reason like '%票据%' THEN 'JU031' 
							WHEN case_reason like '%破产%'  
								OR case_reason like '%清偿%'   
								OR case_reason like '%追收%'  
								OR case_reason like '%请求确认债务人行为无效纠纷%' 
								OR case_reason like '%别除权纠纷%' 
								OR case_reason like '%损害债务人利益赔偿纠纷%' 
								OR case_reason like '%管理人责任纠纷%' 
								OR case_reason like '%取回权%' THEN 'JU032'  
							WHEN (case_reason like '%保全%' 
									and (case_reason like '%财产%' or case_reason like '%行为%' or case_reason like '%证据%') 
									and case_reason not like '%纠纷%' 
									and (case_reason like '%申请诉前%' or  case_reason like '%申请仲裁%' or case_reason like '%仲裁程序%' or case_reason like '%申请执行%' )
								) 
								or case_reason like '%申请中止支付信用证项下款项%' 
								or case_reason like '%申请中止支付保函项下款项%' THEN 'JU033' 
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
								or case_reason like '%独立保函%'  THEN 'JU034' 
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
								OR case_reason like '%房屋买卖%' THEN 'JU029' 
							WHEN (case_reason like '%抵押权纠纷%' OR case_reason like '%质权纠纷%' ) THEN 'JU030'  
							WHEN  CASE_REASON like '%合同纠纷%' THEN 'JU035' 
							ELSE 'JU036' 
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
						involve,
						involve_role,
						case_reason,
						case_no,
						--case_no_cnt,
						court,
						court_link,		
						SRC_TABLE,
						SRC_SID,
						UPDT_DT,
						ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as RM
					FROM
					(
						SELECT 
							o.ITCODE2 as COMPANY_ID, -- 公司代码(企业库)/当事人 as 公司代码
							max(o.ITCODE) as COMPANY_ID2, -- 公司代码(金融库) AS 公司代码2
							max(o.ITNAME) as COMPANY_NM,
							to_date(cast(o1.CR0168_003 as TIMESTAMP)) as NOTICE_DT,  -- 开庭日期 as 发生时间

							max(o.CR0169_004) as involve, -- 当事人 as 涉案公司
							max(o.CR0169_003) as involve_role, --诉讼地位 as  涉案公司角色
							max(o1.CR0168_007) as case_reason, --！！！缺失CR0168_007字段 案由 as 案由
							--'' as case_reason,
							o1.CR0168_006 as case_no,  -- 案号 as 案号
							max(o1.CR0168_008) as court, -- 开庭法院 as  法院
							'' as court_link, -- 缺失 ！！！ as 开庭公告链接 
							--count(*) as case_no_cnt,  --当天某公司的案号数量
							max(o.CR0169_001) as SRC_SID, -- 案件流水号 as 源头SID数据
							upper('tr_ods_rmp_fi_TCR0169,tr_ods_rmp_fi_TCR0168') as SRC_TABLE, -- 源头表名
							current_timestamp() as UPDT_DT   -- 更新时间,
						FROM  hds.tr_ods_rmp_fi_TCR0169 o  --开庭庭审当事人表
						join hds.tr_ods_rmp_fi_TCR0168 o1  --开庭庭审要素表
							on o.CR0169_001=cast(o1.ID as bigint)  -- 缺失！！！ 无法关联 ！！！ 通过案件流水号关联
						where o.CR0169_005 = '0'
						and o.ITCODE2 <> ''  -- 当事人非空/企业非空
						and CR0169_002 IN ('2','4','6','8') --诉讼地位代码
						group by o.ITCODE2,o1.CR0168_003,o1.CR0168_006   --根据 公司,时间,案号 分组
					) A where A.COMPANY_NM <> ''  -- where case_no_cnt<=20  --某天某家公司的案件数量超过20,则该块数据剔除
				) T where T.rm<=10 and T.NOTICE_DT<>'1990-01-01' group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)=to_date(date_add(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')),0))  
;
	