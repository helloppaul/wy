-- 被执行人 (同步方式：一天多批次覆盖)- --
--入参：${ETL_DATE}(20220818 int) -> to_date(notice_dt) 
--/* 2022-8-29 \r\n 全局替换为 \\r\\n */



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
insert overwrite table pth_rmp.rmp_opinion_risk_info partition(dt=${ETL_DATE})
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
	Final.url_kw,
	Final.news_from,
	Final.msg,
	Final.delete_flag,
	Final.create_by,
	Final.create_time,
	Final.update_by,
	Final.update_time,
	Final.version
	-- to_date(Final.notice_dt) as dt
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
		Final_CXSF.msg,
		0 as delete_flag,
		'' as create_by,
		cast('2022-08-01' as timestamp) as create_time,
		'' as update_by,
		current_timestamp() update_time,
		0 as version
	FROM 
	(	
		select 
			0 AS sid_kw,
			corp_id,
			corp_nm,
			notice_dt,
			-- '' as msg_id,
			concat(corp_id,'_',md5(RISK_DESC)) as msg_id,   -- hive版本支持：MD5(corp_id,notice_dt,case_type_ii,RISK_DESC)*/
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
					--group_concat(RISK_DESC_tmp,',')
					concat_ws(',',collect_set(RISK_DESC_tmp))
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
)Final join corp_chg cid_chg on Final.corp_id = cid_chg.source_id and cid_chg.source_code='FI';
where to_date(notice_dt)=to_date(from_unixtime(unix_timestamp(cast(${ETL_DATE} as string),'yyyyMMdd')))  ;	