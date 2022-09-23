-- 失信被执行人 (同步方式：一天多批次覆盖) --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
-- set hive.execution.engine=spark;  --编排很好mr
-- set hive.exec.dynamic.partition=true;  --开启动态分区功能
-- set hive.exec.dynamic.partition.mode=nostrick;  --允许全部分区都为动态withwith with with 

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
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(etl_date=${ETL_DATE},type_='sf_sxbzxr')
select msg_id as sid_kw,* 
from 
(
	select distinct
		cid_chg.corp_id as corp_id,
		Final.corp_nm,
		Final.notice_dt,
		-- Final.msg_id, --impala
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
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd'))) 
;