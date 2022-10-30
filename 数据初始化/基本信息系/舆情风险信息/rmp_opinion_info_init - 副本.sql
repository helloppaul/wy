-- DDL --
drop table if exists pth_rmp.rmp_opinion_risk_info_init;
create table pth_rmp.rmp_opinion_risk_info_init
(
	sid_kw string,
	corp_id STRING,
	corp_nm STRING,
	notice_dt TIMESTAMP,
	msg_id STRING,
	msg_title STRING,
	case_type_cd STRING,
	case_type STRING,
	case_type_ii_cd STRING,
	case_type_ii STRING,
	importance double,
	signal_type TINYINT,
	src_table STRING,
	src_sid STRING,
	url_kw STRING,
	news_from STRING,
	msg STRING,
	CRNW0003_010 string,
	notice_date timestamp,
	notice_month timestamp,
	delete_flag int,
	create_by STRING,
	create_time TIMESTAMP,
	update_by STRING,
	update_time TIMESTAMP,
	version int
)partitioned by (etl_date int,type_ string) 
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;



-- 初始化sql impala执行 --
-- set mem_limit=16600000000;
-- drop table if exists pth_rmp.rmp_opinion_risk_info_init_impala;
create table pth_rmp.rmp_opinion_risk_info_init_impala as 
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
	  and to_date(CR0164_009)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
	  and to_date(CR0164_009)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
	group by ITCODE2,CR0164_009,CR0164_006   --根据 公司,时间,案号 分组	
)
--诚信数据
select msg_id as sid_kw, *
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,  --impala
		-- concat(Final.corp_id,'_',md5(concat(cast(Final.notice_dt as string),Final.msg_title,Final.case_type_ii,Final.msg))) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
		Final.msg_title,
		Final.case_type_cd,
		Final.case_type,
		Final.case_type_ii_cd,
		Final.case_type_ii,
		cast(Final.importance as tinyint) as importance,
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
						group_concat(RISK_DESC,'\\r\\n')
						-- concat_ws('\\r\\n', collect_set( RISK_DESC))  
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
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
union all 
--司法_被执行人
select msg_id as sid_kw,*
from
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,  --impala
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
		-- 'sf_bzxr' as type_
	from 
	(
		SELECT 
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
				--暂定数据提供执行金额 单位（元），还需找娟姐或耕桥确认 
				SELECT  --SEQ_COMPY_RISK_SUMM.NEXTVAL,
					COMPANY_ID as corp_id,
					COMPANY_NM as corp_nm,
					NOTICE_DT,
					RISK_TYPE,
					RISK_TYPE_CD,  --可考虑不要
					IMPORTANCE,
					concat(cast(year(NOTICE_DT) as string),'年',cast(month(NOTICE_DT) as string),'月',cast(day(NOTICE_DT) as string),'日',',',
							'根据中国执行信息公开网最新公告显示,',company_nm,
							'被列入被执行人名单，截止当前，近3年的累计执行金额为',if(cast(round(cast(max(accum_amount)/10000 as decimal(20,5)),2) as string)='0.00','-',cast(round(cast(max(accum_amount)/10000 as decimal(20,5)),2) as string)),'万元。 ',
							'最新涉及案件详情如下：','\\r\\n','\\r\\n',
						group_concat(RISK_DESC_tmp,',')
						-- concat_ws(',',collect_set(RISK_DESC_tmp))
						)
					as RISK_DESC,
					max(SRC_TABLE) as SRC_TABLE,
					max(SRC_SID) as SRC_SID,
					0 as IS_DEL,
					NULL as CREATE_DT,
					max(UPDT_DT) as UPDT_DT
				FROM 
				(
					select *,
					concat(
							Case when case_no <> '' then concat('案号:',case_no,'\\r\\n') else '' END,
							Case when jud_organ <> '' then concat('执行法院:',jud_organ,'\\r\\n') else '' END,
							Case when exec_money is not null then concat('执行金额:',if(cast(round(cast(exec_money/10000 as decimal(20,5)),2) as string)='0.00','-',cast(round(cast(exec_money/10000 as decimal(20,5)),2) as string)),'万元 ') else '' END,'\\r\\n'
						) 
					as RISK_DESC_tmp,
					ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as rm,count(*) over(partition by COMPANY_ID,NOTICE_DT) as case_no_cnt
					from 
					(
						select 
							COMPANY_ID,
							COMPANY_ID2,
							company_nm,
							NOTICE_DT ,
							'被列入被执行人' as RISK_TYPE,
							'JU001001' as RISK_TYPE_CD,  --可考虑不要
							-2 IMPORTANCE,
							case_no,
							jud_organ,
							nvl(exec_money,0) as exec_money,  --单位(元) 
							sum(exec_money) over(partition by COMPANY_ID)  as accum_amount, -- sum(执行金额) as 累计执行金额 (元)
							SRC_SID,
							SRC_TABLE,
							UPDT_DT
						from 
						(
							SELECT 
								ITCODE2 as COMPANY_ID, -- 公司代码(企业库) as 公司代码
								max(ITCODE) as COMPANY_ID2, -- 公司代码(金融库) AS 公司代码2
								max(ITNAME) as company_nm,  --公司名称 as 公司名称(被执行人名称)
								cast(CR0037_004 as timestamp) as NOTICE_DT,  -- 立案时间 as 发生时间
								CR0037_001 as case_no,  --案号 as 案号
								--count(*) as case_no_cnt,  --当天某公司的案号数量
								max(CR0037_002) as jud_organ, --执行法院 as 执法机构(法院)
								max(cast(CR0037_006 as double)) as exec_money,  -- 执行标的 as 执行金额 (若有多条去金额最大的且非空的)
								max(ID) as SRC_SID, -- 违规记录主表ID as 源头SID数据
								upper('tr_ods_rmp_fi_TCR0037_V2') as SRC_TABLE, -- 源头表名
								current_timestamp() as UPDT_DT   -- 更新时间,	
							FROM hds.tr_ods_rmp_fi_TCR0037_V2
							where FLAG<>'1'  -- 只保留生效数据
							and ITCODE2 <> ''  -- 企业非空
							and (CR0037_004 <> '' or CR0037_004 is not null)  -- 立案时间非空
							and CR0037_006 <> '' -- 执行金额非空
							group by ITCODE2,CR0037_001,CR0037_004  --根据这三个字段，数据去重
						) A where ADD_MONTHS(UPDT_DT,-36)<= NOTICE_DT  -- 汇总 近3年 的 执行金额
							--and case_no_cnt<=20  --某天某家公司的案件数量超过20,则该块数据剔除
					)B where B.accum_amount>10000000   --累计三年执行金额超过1000万的 才显示记录
				)T where rm<=10 and case_no_cnt<=20 group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE --仅展示最多10个案件详情
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
union all 
--司法_裁判文书
select msg_id as sid_kw,*
from
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,   --impala
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
							group_concat(RISK_DESC_tmp,'')
							-- concat_ws('',collect_set(RISK_DESC_tmp))
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
)Fi
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))union all 
--司法_对外持股冻结
select msg_id as sid_kw,*
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,  --impala
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
						group_concat(RISK_DESC_tmp,'')
						-- concat_ws('', collect_set(RISK_DESC_tmp))
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
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
--司法_限制高消费
union all
select msg_id as sid_kw,* 
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,  --impala
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
						group_concat(RISK_DESC_tmp,'')
						-- concat_ws('',collect_set(RISK_DESC_tmp))
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
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
;

---______________________________part2____________________________________________________-------------
insert into pth_rmp.rmp_opinion_risk_info_init_impala
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
--司法_开庭公告
select msg_id as sid_kw,* 
from  
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id,  --impala
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
						group_concat(RISK_DESC_tmp,'')
						-- concat_ws('',collect_set(RISK_DESC_tmp))
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
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
union all 
--司法_失信被执行人 
select msg_id as sid_kw,* 
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		Final.msg_id, --impala
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
		-- 'sf_sxbzxr' as type_
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
							'根据中国执行信息公开网最新公告显示,',company_nm,
							'被列入失信被执行人名单。','\\r\\n',
							'最新涉及案件详情如下：','\\r\\n','\\r\\n',
						group_concat(RISK_DESC_tmp,'')
						-- concat_ws('',collect_set(RISK_DESC_tmp))
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
								Case when register_dt is not null then concat('立案时间:',cast(register_dt as string),'\\r\\n') else '' END,
								Case when case_no <> '' then concat('案号:',case_no,'\\r\\n') else '' END,
								Case when exec_case_no <> '' then concat('执行依据文号：',exec_case_no,'\\r\\n') else '' END,
								Case when exec_state <> '' then concat('履行情况：',exec_state,'\\r\\n') else '' END,
								Case when exec_organ <> '' then concat('做出执行的依据单位：',exec_organ,'\\r\\n') else '' END,
								Case when exec_state_state <> '' then concat('失信被执行人行为具体情形：',exec_state_state,'\\r\\n') else '' END,'\\r\\n')
						as RISK_DESC_tmp,
						register_dt,
						case_no,
						exec_case_no,
						exec_state,
						exec_organ,
						exec_state_state,
						'被列入失信被执行人' as RISK_TYPE,
						'JU017' as RISK_TYPE_CD,  --可考虑不要
						-3 as IMPORTANCE,
						SRC_TABLE,
						SRC_SID,
						UPDT_DT,
						count(*) over(partition by COMPANY_ID,NOTICE_DT) as case_no_cnt,
						ROW_NUMBER() over(partition by COMPANY_ID,NOTICE_DT order by 1) as RM
					FROM
					(
						SELECT 
							ITCODE2 as COMPANY_ID, -- 公司代码(企业库) as 公司代码
							max(ITCODE) as COMPANY_ID2, -- 公司代码(金融库) AS 公司代码2
							max(ITNAME) as COMPANY_NM,
							to_date(cast(CR0033_007 as TIMESTAMP)) as NOTICE_DT,  -- 发布时间 as 发生时间
							to_date(cast(max(CR0033_011) as timestamp)) as register_dt,-- 立案时间 as 立案时间
							-- as exec_money, -- as 被执行金额  (可省略)
							CR0033_013 as case_no,  --案号 as 案号
							--count(*) as case_no_cnt,  --当天某公司的案号数量
							max(CR0033_002) as exec_case_no, -- as 执行依据文号
							max(CR0033_010) as exec_state, -- as 履行情况
							max(CR0033_008) as exec_organ , -- as 做出执行的依据单位
							max(CR0033_012) as exec_state_state, -- as 失信被执行人行为具体情形
							max(ID) as SRC_SID, -- 违规记录主表ID as 源头SID数据
							upper('tr_ods_rmp_fi_TCR0033_V2') as SRC_TABLE, -- 源头表名
							current_timestamp() as UPDT_DT   -- 更新时间,
							
						FROM  hds.tr_ods_rmp_fi_TCR0033_V2
						where FLAG<>'1'  -- 只保留生效数据
						and ITCODE2 <> ''  -- 企业非空
						and (CR0033_007 <> '' or CR0033_007 is not null)  -- 发布时间非空
						group by ITCODE2,CR0033_007,CR0033_013
					) A --where case_no_cnt<=20  --某天某家公司的案件数量超过20,则该块数据剔除
				) T where T.rm<=10 and T.case_no_cnt<=20 group by COMPANY_ID,COMPANY_NM,NOTICE_DT,RISK_TYPE,RISK_TYPE_CD,IMPORTANCE 
			)Final_Part
		)Final_CXSF join pth_rmp.RMP_OPINION_RISK_INFO_TAG tag on Final_CXSF.case_type_ii = tag.tag_ii and tag.tag_type in (1,2) -- 仅司法诚信替换标签，新闻舆情已单独处理 
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI'
)Fi
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
union all 
--新闻数据
select msg_id as sid_kw,*
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		cid_chg.corp_name as corp_nm,
		Final.notice_dt,
		Final.msg_id,
		Final.msg_title,
		Final.case_type_cd,
		Final.case_type,
		Final.case_type_ii_cd,
		Final.case_type_ii,
		cast(Final.importance as tinyint) as importance,
		Final.signal_type,
		Final.src_table,
		Final.src_sid,
		nvl(Final.url_kw,'') as url_kw,
		nvl(Final.news_from,'') as news_from,
		Final.msg,
		Final.CRNW0003_010,
		to_date(Final.notice_dt) as notice_date, 
		last_day(Final.notice_dt) as notice_month,
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
		SELECT 
			corp_id,corp_nm,
			notice_dt,
			-- '' as msg_id,  -- impala
			msg_id_ as msg_id, 
			-- concat(corp_id,'_',MD5(concat(corp_id,msg_id_,case_type_cd,case_type_ii_cd))) AS msg_id,  -- hive
			msg_title,
			case_type_cd,case_type,
			case_type_ii_cd,case_type_ii,
			importance,
			signal_type,
			src_table,
			src_sid,
			url_kw,
			news_from,
			msg,
			CRNW0003_010,
			ROW_NUMBER() over(partition by corp_id,msg_id_,case_type_ii_cd order by 1) as rm1 --去除重复数据
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
				msg,
				CRNW0003_010
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
						o3.CRNW0002_001 as msg,
						o1.CRNW0003_010
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
						) idx,
						 (select distinct newscode,NEWSDATE,CRNW0002_001 --正文数据
						  from hds.tr_ods_rmp_fi_x_news_tcrnw0002 where flag<>'1') o3
					where o.newscode=o1.newscode and o1.itcode2=o2.itcode2 and idx.indexcode=o1.crnw0003_001 and o.newscode=o3.newscode 
					and cast(o1.crnw0003_006 as int)<0
				)t0
			)Final_Part
		)Final_News
	)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI' and rm1=1
)Fi
where to_date(notice_dt)>=to_date(date_add(from_unixtime(unix_timestamp(cast(${begt} as string),'yyyyMMdd')),0)) 
  and to_date(notice_dt)<=to_date(date_add(from_unixtime(unix_timestamp(cast(${endt} as string),'yyyyMMdd')),0))
;




-- 初始化sql hive执行 --
-- drop table if exists pth_rmp.rmp_opinion_risk_info;
insert into pth_rmp.rmp_opinion_risk_info_init partition(etl_date=19900101)
select 
	concat(corp_id,'_',md5(concat(cast(notice_dt as string),msg_title,case_type_ii,msg))) as sid_kw,
	corp_id,
	corp_nm,
	notice_dt,
	concat(corp_id,'_',md5(concat(cast(notice_dt as string),msg_title,case_type_ii,msg))) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
	msg_title,
	case_type_cd,
	case_type,
	case_type_ii_cd,
	case_type_ii,
	importance,
	signal_type,
	src_table,
	src_sid,
	url_kw,
	news_from,
	msg,
	CRNW0003_010,
	notice_date, 
	notice_month,
	delete_flag,
	create_by,
	create_time,
	update_by,
	update_time,
	version
from pth_rmp.rmp_opinion_risk_info_init_impala
where signal_type<>0
union all 
select 
	sid_kw,
	corp_id,
	corp_nm,
	notice_dt,
	msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
	msg_title,
	case_type_cd,
	case_type,
	case_type_ii_cd,
	case_type_ii,
	importance,
	signal_type,
	src_table,
	src_sid,
	url_kw,
	news_from,
	msg,
	CRNW0003_010,
	notice_date, 
	notice_month,
	delete_flag,
	create_by,
	create_time,
	update_by,
	update_time,
	version
from pth_rmp.rmp_opinion_risk_info_init_impala
where signal_type=0
;
