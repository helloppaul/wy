
--create table pth_rmp.WARNING_SCORE_FEATURE_CFG as

INSERT INTO pth_rmp.WARNING_SCORE_FEATURE_CFG 
select 1 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 2 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 3 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 4 as sid_kw,'factor_058' as feature_cd,'产量' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'年产量，单位：万吨' as cal_explain,'衡量企业生产规模的重要指标，同等条件下，规模大的企业通常能取得一定的规模效益。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 5 as sid_kw,'factor_582' as feature_cd,'利润波动情况' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'营业利润波动情况、利润总额波动情况、净利润波动情况' as cal_explain,'从营业利润、利润总额和净利润三个维度考察发行人近三年的利润波动情况，利润多年为正，风险越小' as feature_explain,'' as unit_origin,'' as unit_target

union all select 6 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 7 as sid_kw,'Operation12' as feature_cd,'资产总额周转率' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'营业收入*2/(期初资产总计+资产总计)' as cal_explain,'资产总额周转率综合反映了企业整体资产的营运能力，该指标越大，说明总资产周转越快，反映出销售能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 8 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 9 as sid_kw,'Structure2' as feature_cd,'有息债务/(有息债务+所有者权益)' as feature_name,'低频-采矿' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/(有息债务+所有者权益)' as cal_explain,'比值越大说明财务杠杆风险越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 10 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 11 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 12 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 13 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 14 as sid_kw,'factor_582' as feature_cd,'利润波动情况' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'营业利润波动情况、利润总额波动情况、净利润波动情况' as cal_explain,'从营业利润、利润总额和净利润三个维度考察发行人近三年的利润波动情况，利润多年为正，风险越小' as feature_explain,'' as unit_origin,'' as unit_target

union all select 15 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 16 as sid_kw,'Leverage18' as feature_cd,'有息债务/EBITDA' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'有息债务/EBITDA' as cal_explain,'有息债务/EBITDA' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 17 as sid_kw,'Operation3' as feature_cd,'存货周转率' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'减:营业成本*2/(期初存货+存货)' as cal_explain,'存货周转率反映用来衡量企业生产经营各环节中存货运营效率，该指标越高，表明企业存货资产变现能力越强，存货及占用在存货上的资金周转速度越快' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 18 as sid_kw,'Profitability3' as feature_cd,'净资产收益率' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 19 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-原材料制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 20 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 21 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 22 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 23 as sid_kw,'factor_067' as feature_cd,'客户集中度' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'客户集中度' as cal_explain,'如果企业客户集中度很高，那么单一客户的流失将会给企业销售造成很大压力' as feature_explain,'' as unit_origin,'' as unit_target

union all select 24 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 25 as sid_kw,'factor_009' as feature_cd,'应收账款坏账准备率' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款坏账准备合计(亿元)/应收账款账面余额（亿元）' as cal_explain,'坏账准备金率反映企业应收账款的整体坏账程度。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 26 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 27 as sid_kw,'factor_582' as feature_cd,'利润波动情况' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'营业利润波动情况、利润总额波动情况、净利润波动情况' as cal_explain,'从营业利润、利润总额和净利润三个维度考察发行人近三年的利润波动情况，利润多年为正，风险越小' as feature_explain,'' as unit_origin,'' as unit_target

union all select 28 as sid_kw,'Leverage13' as feature_cd,'EBIT利息保障倍数' as feature_name,'低频-化工' as sub_model_type,'流动比率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBIT/财务费用' as cal_explain,'利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强。' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 29 as sid_kw,'Operation1' as feature_cd,'营业周期' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as cal_explain,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 30 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 31 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-化工' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 32 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 33 as sid_kw,'factor_079' as feature_cd,'服务能力' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业主要项目服务能力，电力考虑装机容量，水务考虑日供水能力和日污水处理能力，燃气考虑售气量' as cal_explain,'服务能力代表企业资产实现经济效益的能力' as feature_explain,'' as unit_origin,'' as unit_target

union all select 34 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 35 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 36 as sid_kw,'Leverage9' as feature_cd,'经营活动产生的现金流量净额/短期有息债务' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'经营活动产生的现金流量净额/短期有息债务' as cal_explain,'经营活动产生的现金流量净额/短期有息债务从现金流的角度反应了短期偿债能力，比值越大，偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 37 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 38 as sid_kw,'Operation12' as feature_cd,'资产总额周转率' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'营业收入*2/(期初资产总计+资产总计)' as cal_explain,'资产总额周转率综合反映了企业整体资产的营运能力，该指标越大，说明总资产周转越快，反映出销售能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 39 as sid_kw,'Profitability10' as feature_cd,'EBITDA/营业收入' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'EBITDA/营业收入' as cal_explain,'EBITDA/营业收入' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 40 as sid_kw,'Size1' as feature_cd,'资产总额' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'资产总计' as cal_explain,'资产总计' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 41 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-公用事业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 42 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 43 as sid_kw,'factor_002' as feature_cd,'受限资产占比' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限资产合计（亿元）/资产总额（亿元）' as cal_explain,'受限资产占比越高，企业未来通过抵质押贷款的空间越小。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 44 as sid_kw,'factor_077' as feature_cd,'区域经济发展水平' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部环境' as type,'所在区域的经济发达程度。
经济发达包括：航空公司-全国性运营；港口-9大干线港港口；铁路-全国性运营；其他-全国性运营；航运-全国性运营；收费路桥-北京、上海、江苏、天津、浙江、福建、广东；机场-一线城市；
经济发展水平中上游包括：航空公司-北京、上海、江苏、天津、浙江、福建、广东；港口-北京、上海、江苏、天津、浙江、福建、广东；铁路-东部；其他-东部；航运-东部；收费路桥-山东、海南、河北、中部地区省份；机场-二线城市；
经济发展水平中下游包括：航空公司-山东、海南、河北、中部地区省份；港口-山东、海南、河北、中部地区省份；铁路-中部；其他-中部；航运-中部；收费路桥-东北；机场-三线城市；
经济欠发达包括：航空公司-东北、西部；港口-东北、西部；铁路-东北、西部；其他-东北、西部；航运-东北、西部；收费路桥-西部；机场-四线城市。' as cal_explain,'交通运输是支撑国民经济发展的基础性行业，公司所在的区域经济对公司的业务量具有重要影响。区域经济发展程度越高，公司的市场需求就越大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 45 as sid_kw,'factor_078' as feature_cd,'经营实力' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'公司业务规模、经营效率等在同行业中的档次' as cal_explain,'公司业务规模、经营效益是公司在同行业中的核心竞争之一。规模越大，经营效率越高，则公司抵抗经营风险的能力越强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 46 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 47 as sid_kw,'Operation14' as feature_cd,'经营活动产生的现金流量净额/营业收入' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'经营活动产生的现金流量净额/营业收入' as cal_explain,'经营活动产生的现金流量净额/营业收入' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 48 as sid_kw,'Size4' as feature_cd,'营业收入' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'营业收入' as cal_explain,'营业收入' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 49 as sid_kw,'Structure1' as feature_cd,'资产负债率' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'负债合计/资产总计' as cal_explain,'负债合计/资产总额 反应了企业的负债水平，值越大，负债水平越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 50 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-交通运输' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 51 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 52 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 53 as sid_kw,'factor_015' as feature_cd,'合同销售金额' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'当年合同销售金额（亿元）' as cal_explain,'该项指标能够较为全面地反映受评企业的业务规模和市场份额，衡量了受评企业所开发项目的市场真实需求情况，具有较强的可比性。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 54 as sid_kw,'factor_017' as feature_cd,'房地产开发企业排名' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'由中国房地产业协会与中国房地产测评中心联合发布的中国房地产500强测评成果内的排名' as cal_explain,'沪深交易所对房地产发债的发行主体准入条件做了限制，其中一条为中国房地产业协会排名前100名的其他民营非上市房地产企业' as feature_explain,'' as unit_origin,'' as unit_target

union all select 55 as sid_kw,'factor_020' as feature_cd,'土地储备规模（万平方米）' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'年末合计土地储备规模' as cal_explain,'土地储备直接反映了受评企业未来的发展潜力和潜在的业务增长空间，在很大程度上决定了受评企业未来的资产和业务规模' as feature_explain,'' as unit_origin,'' as unit_target

union all select 56 as sid_kw,'factor_026' as feature_cd,'在建项目区域分布' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'当年在建项目的地域分布' as cal_explain,'项目分布区域影响房地产开发的成本以及盈利能力。国内房地产行业将国内区域划分为一、二、三、四线城市，不同城市的拿地成本不同，价格趋势也不尽相同。不同城市经济发展情况不同，相应的，房地产价格走势也不同。相对而言，一二线城市的房地产价格呈上涨态势，三四线城市的房地产价格上升空间不大，所以经营区域也是影响房地产企业竞争力的重要因素。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 57 as sid_kw,'factor_027' as feature_cd,'主营业务行业相关度' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'主营业务涉及行业之间的相关度（考察主营业务中除房地产行业相关的业务是否属于房地产的上下游行业）' as cal_explain,'考虑企业多元化经营的运营能力。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 58 as sid_kw,'factor_190' as feature_cd,'土地储备区域分布2' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'土地储备规划建筑面积在各线城市分布的占比情况' as cal_explain,'项目分布区域影响房地产开发的成本以及盈利能力。国内房地产行业将国内区域划分为一、二、三、四线城市，不同城市的拿地成本不同，价格趋势也不尽相同。不同城市经济发展情况不同，相应的，房地产价格走势也不同。相对而言，一二线城市的房地产价格呈上涨态势，三四线城市的房地产价格上升空间不大，所以经营区域也是影响房地产企业竞争力的重要因素。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 59 as sid_kw,'factor_192' as feature_cd,'受限资产占比2' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限资产合计（亿元）/（所有者权益（亿元）-其他权益工具（亿元））' as cal_explain,'受限资产占比越高，企业未来通过抵质押贷款的空间越小。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 60 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 61 as sid_kw,'Leverage4' as feature_cd,'现金比率' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(货币资金+交易性金融资产)/流动负债合计' as cal_explain,'现金比率反应了企业的流动性，比值越大，流动性越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 62 as sid_kw,'Profitability11' as feature_cd,'期间费用率' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'(减:销售费用+减:管理费用+减:财务费用+减：研发费用)/营业收入' as cal_explain,'期间费用与营业收入的比值，值越大，盈利能力越弱' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 63 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 64 as sid_kw,'Structure13' as feature_cd,'应收账款/资产总额' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'应收账款/资产总计' as cal_explain,'应收账款/资产总额指标越高说明公司现金流承压较大，所带来坏账风险等风险较高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 65 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-房地产' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 66 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 67 as sid_kw,'factor_002' as feature_cd,'受限资产占比' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限资产合计（亿元）/资产总额（亿元）' as cal_explain,'受限资产占比越高，企业未来通过抵质押贷款的空间越小。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 68 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 69 as sid_kw,'factor_010' as feature_cd,'应收账款集中度' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款集中度' as cal_explain,'应收账款占比越大，应收账款前五大占比越大，则更容易受单一客户应收账款的影响，风险更大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 70 as sid_kw,'factor_086' as feature_cd,'核心竞争力' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'核心竞争力是指能保持企业持续获得超过行业平均利润水平的能力。核心竞争力可以是业务牌照、人力资源、渠道资源、技术等。' as cal_explain,'企业所拥有的核心竞争力越强，在行业竞争中获得超出行业平均回报的可能性和规模就越大，相应地抗击行业风险的能力也越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 71 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 72 as sid_kw,'Operation1' as feature_cd,'营业周期' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as cal_explain,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 73 as sid_kw,'Profitability10' as feature_cd,'EBITDA/营业收入' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'EBITDA/营业收入' as cal_explain,'EBITDA/营业收入' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 74 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 75 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-服务业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 76 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 77 as sid_kw,'factor_005' as feature_cd,'银行授信额度占比' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'银行授信总额度（亿元）/所有者权益（亿元）' as cal_explain,'从相对层面考察银行对企业资质的认可程度，银行授信总额/所有者权益越高，银行对其资质的认可度越大，侧面反映企业资质越优秀。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 78 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 79 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 80 as sid_kw,'factor_067' as feature_cd,'客户集中度' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'客户集中度' as cal_explain,'如果企业客户集中度很高，那么单一客户的流失将会给企业销售造成很大压力' as feature_explain,'' as unit_origin,'' as unit_target

union all select 81 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 82 as sid_kw,'factor_075' as feature_cd,'技术先进性' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业生产工艺、技术水平、设备的先进情况' as cal_explain,'企业的技术水平越高，在行业中获得的产品技术优势、成本优势越大，在行业中越具有竞争力' as feature_explain,'' as unit_origin,'' as unit_target

union all select 83 as sid_kw,'Leverage14' as feature_cd,'EBITDA利息保障倍数' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/减:财务费用' as cal_explain,'(EBIT＋折旧＋摊销)/财务费用；利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 84 as sid_kw,'Operation3' as feature_cd,'存货周转率' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'减:营业成本*2/(期初存货+存货)' as cal_explain,'存货周转率反映用来衡量企业生产经营各环节中存货运营效率，该指标越高，表明企业存货资产变现能力越强，存货及占用在存货上的资金周转速度越快' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 85 as sid_kw,'Profitability11' as feature_cd,'期间费用率' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'(减:销售费用+减:管理费用+减:财务费用+减：研发费用)/营业收入' as cal_explain,'期间费用与营业收入的比值，值越大，盈利能力越弱' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 86 as sid_kw,'Profitability3' as feature_cd,'净资产收益率' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 87 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 88 as sid_kw,'Structure18' as feature_cd,'有息债务/负债合计' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/负债合计' as cal_explain,'有息债务/负债合计指标反映企业举债成本，该指标越低，证明企业举债成本越低，相对来说利润也就会提高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 89 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-工业设备制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 90 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 91 as sid_kw,'factor_011' as feature_cd,'应收账款账龄' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款账龄' as cal_explain,'一年以上的应收账款占比越高，应收账款回收风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 92 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 93 as sid_kw,'factor_031' as feature_cd,'施工资质' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业获得住建部颁发的建筑施工资质情况' as cal_explain,'施工资质是企业参与业务的进入门槛，资质级别越高、越丰富，企业能参与的业务范围越多，企业的竞争力越高' as feature_explain,'' as unit_origin,'' as unit_target

union all select 94 as sid_kw,'factor_032' as feature_cd,'获奖情况' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'近五年企业获得“鲁班奖”、“国家优质工程奖”、“詹天佑奖”的获奖情况' as cal_explain,'高级别奖项越多，企业受认可程度越高，企业的技术水平越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 95 as sid_kw,'factor_034' as feature_cd,'在手合同金额' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'在手合同金额' as cal_explain,'在手合同额越高，市场规模越大' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 96 as sid_kw,'factor_035' as feature_cd,'新签合同金额' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'新签合同金额' as cal_explain,'新签合同金额越高，企业未来项目储备越充足。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 97 as sid_kw,'Profitability3' as feature_cd,'净资产收益率' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 98 as sid_kw,'Structure18' as feature_cd,'有息债务/负债合计' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/负债合计' as cal_explain,'有息债务/负债合计指标反映企业举债成本，该指标越低，证明企业举债成本越低，相对来说利润也就会提高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 99 as sid_kw,'Leverage7' as feature_cd,'(货币资金+交易性金融资产)/短期有息债务' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(货币资金+交易性金融资产)/短期有息债务' as cal_explain,'货币资金和交易性金融资产能否覆盖短期有息债务，占比越大说明公司资金链充足，短期偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 100 as sid_kw,'Operation13' as feature_cd,'销售获现比率' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'销售商品、提供劳务收到的现金/营业收入' as cal_explain,'销售获现比率反映企业从主营业务收入中获得现金的能力' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 101 as sid_kw,'Size1' as feature_cd,'资产总额' as feature_name,'低频-建筑' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'资产总计' as cal_explain,'资产总计' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 102 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 103 as sid_kw,'factor_010' as feature_cd,'应收账款集中度' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款集中度' as cal_explain,'应收账款占比越大，应收账款前五大占比越大，则更容易受单一客户应收账款的影响，风险更大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 104 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 105 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 106 as sid_kw,'factor_087' as feature_cd,'客户需求情况' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'零售：企业主要区域内居民人均可支配收入批发：客户集中度' as cal_explain,'区域居民人均可支配收入水平越高，购买力越强，对于零售行业发展越有利；区域居民人均可支配收入水平越低，对于零售行业发展越不利;客户集中度越高，批发企业面临的经营风险越高；客户集中度越低，批发企业面临的经营风险越低' as feature_explain,'' as unit_origin,'' as unit_target

union all select 107 as sid_kw,'factor_089' as feature_cd,'销售业态多元化' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'销售业态的种类多少' as cal_explain,'销售业态越多，越能抵御市场变化以及消费习惯变化的风险' as feature_explain,'' as unit_origin,'' as unit_target

union all select 108 as sid_kw,'Growth5' as feature_cd,'归属于母公司股东的净利率同比增长率' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'[(归属于母公司股东的净利润-上期归属于母公司股东的净利润)/上期归属于母公司股东的净利润+(上期归属于母公司股东的净利润-上上期归属于母公司股东的净利润)/上上期归属于母公司股东的净利润]/2' as cal_explain,'反应企业盈利能力变化情况，比率为正且值越大反应企业盈利能力增强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 109 as sid_kw,'Profitability8' as feature_cd,'销售净利率' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润/营业收入' as cal_explain,'指标值越大，反映公司销售获利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 110 as sid_kw,'Size1' as feature_cd,'资产总额' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'资产总计' as cal_explain,'资产总计' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 111 as sid_kw,'Size5' as feature_cd,'营业利润' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'营业利润' as cal_explain,'指标值越大，反映公司营业利润规模越大' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 112 as sid_kw,'Structure18' as feature_cd,'有息债务/负债合计' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/负债合计' as cal_explain,'有息债务/负债合计指标反映企业举债成本，该指标越低，证明企业举债成本越低，相对来说利润也就会提高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 113 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-批发' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 114 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 115 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 116 as sid_kw,'factor_011' as feature_cd,'应收账款账龄' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款账龄' as cal_explain,'一年以上的应收账款占比越高，应收账款回收风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 117 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 118 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 119 as sid_kw,'factor_087' as feature_cd,'客户需求情况' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'零售：企业主要区域内居民人均可支配收入批发：客户集中度' as cal_explain,'区域居民人均可支配收入水平越高，购买力越强，对于零售行业发展越有利；区域居民人均可支配收入水平越低，对于零售行业发展越不利;客户集中度越高，批发企业面临的经营风险越高；客户集中度越低，批发企业面临的经营风险越低' as feature_explain,'' as unit_origin,'' as unit_target

union all select 120 as sid_kw,'Growth2' as feature_cd,'营业利润同比增长率' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'[(营业利润-上期营业利润)/上期营业利润+(上期营业利润-上上期营业利润)/上上期营业利润]/2' as cal_explain,'反映营业利润变动趋势，指标值为正则反映营业利润呈增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 121 as sid_kw,'Leverage7' as feature_cd,'(货币资金+交易性金融资产)/短期有息债务' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(货币资金+交易性金融资产)/短期有息债务' as cal_explain,'货币资金和交易性金融资产能否覆盖短期有息债务，占比越大说明公司资金链充足，短期偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 122 as sid_kw,'Operation2' as feature_cd,'净营业周期' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2-360*(期初应付账款+应付账款)/减:营业成本/2' as cal_explain,'净营业周期是指从购买存货支付现金到收回现金的周期长度，周期越短，资金周转速度越快' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 123 as sid_kw,'Profitability6' as feature_cd,'营业毛利率' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'(营业收入-减:营业成本)/营业收入' as cal_explain,'营业毛利率是指毛利润与营业收入的比率，反映了企业的盈利能力，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 124 as sid_kw,'Size3' as feature_cd,'归属于母公司股东的权益' as feature_name,'低频-零售' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'归属于母公司股东权益合计' as cal_explain,'归母公司股东权益越大，企业规模越大' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 125 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 126 as sid_kw,'factor_002' as feature_cd,'受限资产占比' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限资产合计（亿元）/资产总额（亿元）' as cal_explain,'受限资产占比越高，企业未来通过抵质押贷款的空间越小。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 127 as sid_kw,'factor_006' as feature_cd,'融资成本' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*（EBITDA/EBITDA利息保障倍数）/（本期期末有息债务（亿元）+上期期末有息债务（亿元）)' as cal_explain,'企业债务成本越高，一方面企业承担的还息压力越大，企业面临的财务风险越高，另一方面，融资成本越高，反映了市场对企业信用资质的评价越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 128 as sid_kw,'factor_010' as feature_cd,'应收账款集中度' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款集中度' as cal_explain,'应收账款占比越大，应收账款前五大占比越大，则更容易受单一客户应收账款的影响，风险更大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 129 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 130 as sid_kw,'Leverage18' as feature_cd,'有息债务/EBITDA' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'有息债务/EBITDA' as cal_explain,'有息债务/EBITDA' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 131 as sid_kw,'Leverage9' as feature_cd,'经营活动产生的现金流量净额/短期有息债务' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'经营活动产生的现金流量净额/短期有息债务' as cal_explain,'经营活动产生的现金流量净额/短期有息债务从现金流的角度反应了短期偿债能力，比值越大，偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 132 as sid_kw,'Operation1' as feature_cd,'营业周期' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as cal_explain,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 133 as sid_kw,'Profitability6' as feature_cd,'营业毛利率' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'(营业收入-减:营业成本)/营业收入' as cal_explain,'营业毛利率是指毛利润与营业收入的比率，反映了企业的盈利能力，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 134 as sid_kw,'Size1' as feature_cd,'资产总额' as feature_name,'低频-消费品制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'资产总计' as cal_explain,'资产总计' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 135 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 136 as sid_kw,'factor_029' as feature_cd,'竞争程度' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部环境' as type,'企业所在建筑业细分领域的竞争程度' as cal_explain,'专业性程度越高的行业竞争程度越低，企业的运营环境越好。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 137 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 138 as sid_kw,'factor_091' as feature_cd,'原材料供应状况' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'原材料供应的集中程度' as cal_explain,'原材料供应的集中度越高，企业面临的原材料供应短缺的风险越高' as feature_explain,'' as unit_origin,'' as unit_target

union all select 139 as sid_kw,'Leverage10' as feature_cd,'非筹资活动产生的现金流量净额/短期有息债务' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'非筹资活动产生的现金流量净额/短期有息债务' as cal_explain,'非筹资活动产生的现金流量净额/短期有息债务反映了公司的非筹资活动产生的现金流量净额对于公司短期有息债务的偿还能力所具有的保障水平，该数值越高，说明公司的短期偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 140 as sid_kw,'Operation1' as feature_cd,'营业周期' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as cal_explain,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 141 as sid_kw,'Operation10' as feature_cd,'固定资产周转率' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'营业收入*2/(期初固定资产+固定资产)' as cal_explain,'固定资产周转率反映公司对固定资产的利用效率，该指标越高，说明公司的固定资产利用效率越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 142 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 143 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-医药制造' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 144 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 145 as sid_kw,'factor_002' as feature_cd,'受限资产占比' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限资产合计（亿元）/资产总额（亿元）' as cal_explain,'受限资产占比越高，企业未来通过抵质押贷款的空间越小。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 146 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 147 as sid_kw,'factor_007' as feature_cd,'审计机构资质' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'审计机构的资质情况' as cal_explain,'审计机构的资质在一定程度上反映企业财务报告的质量，审计机构资质越好，财报的信息质量越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 148 as sid_kw,'factor_008' as feature_cd,'对外担保占比' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'对外担保额（亿元）/所有者权益（亿元）' as cal_explain,'对外担保占比越高，企业的或有负债程度越高，企业风险越大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 149 as sid_kw,'factor_009' as feature_cd,'应收账款坏账准备率' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款坏账准备合计(亿元)/应收账款账面余额（亿元）' as cal_explain,'坏账准备金率反映企业应收账款的整体坏账程度。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 150 as sid_kw,'factor_011' as feature_cd,'应收账款账龄' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收账款账龄' as cal_explain,'一年以上的应收账款占比越高，应收账款回收风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 151 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 152 as sid_kw,'Size2' as feature_cd,'所有者权益（亿元）' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 153 as sid_kw,'Leverage18' as feature_cd,'有息债务/EBITDA' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'有息债务/EBITDA' as cal_explain,'有息债务/EBITDA' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 154 as sid_kw,'Leverage9' as feature_cd,'经营活动产生的现金流量净额/短期有息债务' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'经营活动产生的现金流量净额/短期有息债务' as cal_explain,'经营活动产生的现金流量净额/短期有息债务从现金流的角度反应了短期偿债能力，比值越大，偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 155 as sid_kw,'Operation1' as feature_cd,'营业周期' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as cal_explain,'360*(期初存货+存货)/减:营业成本/2+360*(期初应收账款+应收账款)/营业收入/2' as feature_explain,'天' as unit_origin,'天' as unit_target

union all select 156 as sid_kw,'Profitability3' as feature_cd,'净资产收益率' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 157 as sid_kw,'Structure2' as feature_cd,'有息债务/(有息债务+所有者权益)' as feature_name,'低频-一般工商通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/(有息债务+所有者权益)' as cal_explain,'比值越大说明财务杠杆风险越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 158 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 159 as sid_kw,'factor_003' as feature_cd,'长期信用借款占比' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'长期信用借款（亿元）/长期借款（亿元）' as cal_explain,'长期信用借款占比越高说明银行对企业的信用资质的信心越高，企业的信用风险越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 160 as sid_kw,'factor_058' as feature_cd,'产量' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'年产量，单位：万吨' as cal_explain,'衡量企业生产规模的重要指标，同等条件下，规模大的企业通常能取得一定的规模效益。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 161 as sid_kw,'factor_060' as feature_cd,'可采储量' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'可采储量' as cal_explain,'不仅反映了企业的资源储存情况和实际生产限额，更体现了企业所掌握的资源价值。可采储量越大，说明企业所拥有的资源越丰富，资源的价值越大，企业的风险水平也就越低' as feature_explain,'' as unit_origin,'' as unit_target

union all select 162 as sid_kw,'factor_068' as feature_cd,'产地多元化' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'考察产地分布区域是否广泛' as cal_explain,'产地分布区域广泛的企业面临不同的区域因素，通常煤种比较齐全，能够缓解某一特定区域因素变化或者某一煤种市场行情大幅波动对企业经营业绩的影响；在一定程度上规避因单一区域发生安全生产事故造成企业停产整顿而对企业生产经营产生的风险。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 163 as sid_kw,'Leverage5' as feature_cd,'经营活动产生的现金流量净额/流动负债' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'经营活动产生的现金流量净额/流动负债合计' as cal_explain,'经营活动产生的现金流量净额/流动负债是经营现金净流量与流动负债的比率，从现金流量角度反映企业当期偿付短期负债的能力' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 164 as sid_kw,'Operation4' as feature_cd,'应收账款周转率' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'营业收入*2/(期初应收账款+应收账款)' as cal_explain,'应收账款周转率是一定时期内应收账款转为现金的平均次数，该指标越高，说明账款回收越快；反之，说明营运资金过多呆滞在应收账款上，影响正常资金周转及偿债能力' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 165 as sid_kw,'Profitability7' as feature_cd,'营业利润率' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'营业利润率是指营业利润与收入的比率，反映了企业的盈利能力，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 166 as sid_kw,'Size2' as feature_cd,'所有者权益' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股东权益合计' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 167 as sid_kw,'Structure19' as feature_cd,'短期有息债务/有息债务' as feature_name,'低频-煤炭' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'短期有息债务/有息债务' as cal_explain,'短期有息债务/有息债务' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 168 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 169 as sid_kw,'factor_008' as feature_cd,'对外担保占比' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'对外担保额（亿元）/所有者权益（亿元）' as cal_explain,'对外担保占比越高，企业的或有负债程度越高，企业风险越大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 170 as sid_kw,'factor_012' as feature_cd,'受限货币资金占比' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'受限货币资金（亿元）/货币资金（亿元）' as cal_explain,'受限货币资金占比越高，企业能使用的资金额度越小，企业的付现能力越差。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 171 as sid_kw,'factor_071' as feature_cd,'市场地位' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在行业中的市场地位' as cal_explain,'市场地位越高，企业竞争实力越强，企业抵抗市场风险的能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 172 as sid_kw,'factor_582' as feature_cd,'利润波动情况' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'营业利润波动情况、利润总额波动情况、净利润波动情况' as cal_explain,'从营业利润、利润总额和净利润三个维度考察发行人近三年的利润波动情况，利润多年为正，风险越小' as feature_explain,'' as unit_origin,'' as unit_target

union all select 173 as sid_kw,'factor_660' as feature_cd,'产量' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'生铁、粗钢、钢材、钢坯产量加总' as cal_explain,'衡量企业生产规模的重要指标，同等条件下，规模大的企业通常能取得一定的规模效益。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 174 as sid_kw,'Growth5' as feature_cd,'归属于母公司股东的净利率同比增长率' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'[(归属于母公司股东的净利润-上期归属于母公司股东的净利润)/上期归属于母公司股东的净利润+(上期归属于母公司股东的净利润-上上期归属于母公司股东的净利润)/上上期归属于母公司股东的净利润]/2' as cal_explain,'反应企业盈利能力变化情况，比率为正且值越大反应企业盈利能力增强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 175 as sid_kw,'Leverage24' as feature_cd,'负债合计/筹资活动前产生的现金流量净额' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'负债合计/筹资活动前产生的现金流量净额' as cal_explain,'反映了公司的筹资活动前产生的现金流量净额对于公司负债的偿还能力所具有的保障水平，该数值越低，说明公司的偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 176 as sid_kw,'Operation13' as feature_cd,'销售获现比率' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'销售商品、提供劳务收到的现金/营业收入' as cal_explain,'销售获现比率反映企业从主营业务收入中获得现金的能力' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 177 as sid_kw,'Profitability10' as feature_cd,'EBITDA/营业收入' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'EBITDA/营业收入' as cal_explain,'EBITDA/营业收入' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 178 as sid_kw,'Size3' as feature_cd,'归属于母公司股东的权益' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'归属于母公司股东权益合计' as cal_explain,'归母公司股东权益越大，企业规模越大' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 179 as sid_kw,'Structure1' as feature_cd,'资产负债率' as feature_name,'低频-钢铁' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'负债合计/资产总计' as cal_explain,'负债合计/资产总额 反应了企业的负债水平，值越大，负债水平越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 180 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 181 as sid_kw,'factor_426' as feature_cd,'不良贷款率' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'不良贷款余额/贷款余额' as cal_explain,'直接衡量银行贷款的资产质量,不良贷款率越低,银行贷款的资产质量越好,面对的信用风险越小。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 182 as sid_kw,'factor_450' as feature_cd,'资本充足率' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'资本充足率' as cal_explain,'是一个银行的资本总额对其风险加权资产的比率，各国金融管理当局一般都有对商业银行资本充足率的管制，目的是监测银行抵御风险的能力。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 183 as sid_kw,'factor_614' as feature_cd,'区域经济实力' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'总部所在省份经济实力' as cal_explain,'总部所在省份的经济实力越强，政府对该银行的支持能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 184 as sid_kw,'factor_617' as feature_cd,'上市情况' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'是否上市 ' as cal_explain,'商业银行上市表明其具有更强的融资能力。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 185 as sid_kw,'factor_619' as feature_cd,'银行类别' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'银监会银行分类' as cal_explain,'银监会商业银行分类中不同类型的银行应对风险的能力不同，国有商业银行、股份制商业银行、城市商业银行、农村商业银行的风险管理水平依次下降。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 186 as sid_kw,'factor_623' as feature_cd,'核心一级资本净额' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'经营' as dimension,'企业规模' as type,'核心一级资本-核心一级资本扣减项' as cal_explain,'商业银行的核心一级资本净额直接反映其抵御风险的能力，核心一级资本净额越高，其风险承受能力越强。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 187 as sid_kw,'zs_Bank_Leverage1' as feature_cd,'银行_资产负债率' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'负债合计 / 资产总计' as cal_explain,'体现银行杠杆水平，比值越高反映杠杆风险越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 188 as sid_kw,'zs_Bank_Liquidity4' as feature_cd,'银行_存贷款比率' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'发放贷款和垫款/吸收存款' as cal_explain,'发放贷款及垫款/吸收存款' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 189 as sid_kw,'zs_Bank_Profitability11' as feature_cd,'银行_资产减值损失收入比' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'减:资产减值损失/(利息净收入+手续费及佣金净收入+其他业务收入-其他业务成本)' as cal_explain,'指标值越小，反映公司资产减值损失控制越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 190 as sid_kw,'zs_Bank_Profitability2' as feature_cd,'银行_成本收入比率1' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'业务及管理费/营业收入' as cal_explain,'指标值越小，反映公司业务成本控制越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 191 as sid_kw,'zs_Bank_Profitability4' as feature_cd,'银行_总资产收益率1' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'2*净利润/(资产总计+上期资产总计)' as cal_explain,'反映银行盈利能力，指标值越大体现银行获利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 192 as sid_kw,'zs_Bank_Size1' as feature_cd,'银行_资产总额' as feature_name,'低频-银行' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'资产总计' as cal_explain,'指标值越大反映银行资产规模越大' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 193 as sid_kw,'factor_039' as feature_cd,'代理买卖证券业务净收入排名' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'代理买卖证券业务净收入排名' as cal_explain,'代理买卖证券业务净收入排名反应了经纪业务的市场地位，排名越前，经纪业务市场地位越高' as feature_explain,'' as unit_origin,'' as unit_target

union all select 194 as sid_kw,'factor_044' as feature_cd,'投资银行业务净收入排名' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'投资银行业务净收入排名' as cal_explain,'投资银行业务净收入排名反应了投行业务规模，排名越前，规模越大' as feature_explain,'' as unit_origin,'' as unit_target

union all select 195 as sid_kw,'factor_047' as feature_cd,'资产管理业务净收入排名' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'资产管理业务净收入排名' as cal_explain,'资产管理业务净收入排名反应了资管业务的规模，排名越前，规模越大' as feature_explain,'' as unit_origin,'' as unit_target

union all select 196 as sid_kw,'factor_050' as feature_cd,'投资收益排名' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'自营业务投资收益排名' as cal_explain,'投资收益排名反应了自营业务的投资实力，排名越前，投资实力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 197 as sid_kw,'factor_054' as feature_cd,'证监会分类结果' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'证监会分类结果是证券监管部门根据审慎监管的需要，以证券公司风险管理能力为基础，结合公司市场竞争力和合规管理水平，对证券公司进行的综合性评价。' as cal_explain,'证监会分类结果综合反应了券商风险管理能力，评级结果越好，风险管理能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 198 as sid_kw,'factor_056' as feature_cd,'主要股东背景' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'主要股东性质与持股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 199 as sid_kw,'Secu_Growth7' as feature_cd,'券商_净利润增长率' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'((当期净利润-上期净利润)/上期净利润+(上期净利润-上上期净利润)/上上期净利润)/2' as cal_explain,'反映券商盈利能力成长情况，指标值为正且越大反映券商的净利润呈增长趋势且趋势越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 200 as sid_kw,'Secu_Leverage3' as feature_cd,'券商_现金等价物/短期债务' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(货币资金-客户存款+拆出资金+融出资金+交易性金融资产合计)/(短期借款+衍生金融负债+交易性金融负债合计+拆入资金+卖出回购金融资产+应付短期融资款 )' as cal_explain,'反映券商短期偿债能力，指标值越大反映券商短期偿债能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 201 as sid_kw,'Secu_Profitability2' as feature_cd,'券商_净资产收益率' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润/(期初净资产+期末净资产)*2' as cal_explain,'反映券商股东权益的收益水平，用以衡量公司运用自有资本的效率，指标值越大反映企业通过自有资本获得净收益的能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 202 as sid_kw,'Secu_Regulatory03' as feature_cd,'券商_净资本/负债' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'净资本/负债' as cal_explain,'值越大，债务压力越小' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 203 as sid_kw,'Secu_Size010' as feature_cd,'券商_净资本' as feature_name,'低频-证券公司' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'净资本' as cal_explain,'净资本值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 204 as sid_kw,'factor_721' as feature_cd,'认可资产负债率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'资本结构' as type,'认可负债（亿元）/认可资产（亿元）' as cal_explain,'认可资产负债率是保险公司在偿付能力体系下的举债经营比率。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 205 as sid_kw,'factor_724' as feature_cd,'风险综合评价' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'银保监协会根据相关信息，从操作风险、战略风险、声誉风险和流动性风险共四类难以量化的固有风险，结合综合偿付能力充足率进行分类监管的评价。' as cal_explain,'风险综合评价体现监管对保险公司偿付能力风险的整体状况的认可程度，包括资本充足状况和其他偿付能力风险状况。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 206 as sid_kw,'factor_725' as feature_cd,'市场占有率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'保费业务收入（亿元）/所在行业原保费收入（亿元）' as cal_explain,'保险公司在其子行业的市场份额越大，其市场地位越高。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 207 as sid_kw,'factor_726' as feature_cd,'险种类别' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'产险公司：企业产险，机动车辆保险，货物运输保险责任保险，工程保险，信用保险，农业保险，短期意健险，船舶险，家庭财产保险' as cal_explain,'保险险种的个数越多，产品多样化越高，公司竞争能力、经营实力越强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 208 as sid_kw,'Insurance_Solvency2' as feature_cd,'核心偿付能力充足率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'偿债能力' as type,'寿险公司：个人人寿保险，团体人寿保险，分红险，投资连结险，万能险，意外伤害险，健康险，年金' as cal_explain,'偿付能力充足率越高，说明一家保险公司的破产概率越低' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 209 as sid_kw,'Insurance_Solvency3' as feature_cd,'核心资本' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'企业规模' as type,'核心资本' as cal_explain,'核心资本' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 210 as sid_kw,'QualFac13' as feature_cd,'经营年限' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业成立至今的年限' as cal_explain,'企业成立的时间越久，企业累计的行业经验，公司管理运营经验越丰富，公司经营风险相对越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 211 as sid_kw,'Insurance_Growth5' as feature_cd,'净利润增长率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'（本期净利润-上期净利润）/上期净利润' as cal_explain,'值越大，说明净利润增长越快，企业发展越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 212 as sid_kw,'Insurance_Leverage3' as feature_cd,'承保放大倍数' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'（保险业务收入-分出保费）/(总债务+所有者权益)' as cal_explain,'承保放大倍数值越大，说明单位资产获得的业务收入越大' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 213 as sid_kw,'Insurance_Leverage4' as feature_cd,'总资本化率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'总债务/(总债务+所有者权益)' as cal_explain,'值越小，债务压力越小' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 214 as sid_kw,'Insurance_Profitability1' as feature_cd,'总资产收益率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'2*本期净利润/（期末总资产余额+期初总资产余额）' as cal_explain,'总资产报酬率反映每单位资产创造的净利润情况，值越大，说明企业盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 215 as sid_kw,'Insurance_Profitability15' as feature_cd,'综合成本率2' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'综合赔付率+综合费用率' as cal_explain,'指标值越小，反映公司综合成本控制越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 216 as sid_kw,'Insurance_Profitability3' as feature_cd,'投资收益率' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'2*本期投资收益/（本期投资资产+上期投资资产）' as cal_explain,'保险公司投资收益为一大收益来源，投资收益率越高，企业盈利能力越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 217 as sid_kw,'Insurance_Size3' as feature_cd,'所有者权益' as feature_name,'低频-保险' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'期末所有者权益' as cal_explain,'期末所有者权益' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 218 as sid_kw,'QualFac35' as feature_cd,'股东背景' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'信托公司最大持股比例股东的背景' as cal_explain,'不同的股东性质对信托公司经营特点具有不同的影响，民营背景股东一般要求较高业绩回报，风险偏好较高，国营或金融机构股东大多风险偏好较低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 219 as sid_kw,'QualFac41' as feature_cd,'股权结构稳定性' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'近三年前10大股东名单是否有过变动（股份额度变动不计）' as cal_explain,'股权结构的稳定有利于公司的持续稳健经营，稳定性由强到弱的顺序依次为：前10大股东名单近三年内没有变动>近三年前10大股东名单有一次变动>近三年前10大股东名单有二次及以上变动' as feature_explain,'' as unit_origin,'' as unit_target

union all select 220 as sid_kw,'QualFac49' as feature_cd,'主动管理型资产占比' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'(本期期末主动管理型资产（万元）/本期期末信托资产（万元）+上期期末主动管理型资产（万元）/上期期末信托资产（万元）)/2' as cal_explain,'考虑信托资产中主动管理型资产的占比，比例考察最近连续两年的平均值；' as feature_explain,'' as unit_origin,'' as unit_target

union all select 221 as sid_kw,'QualFac52' as feature_cd,'集合类资产占比（结合运营期考虑）' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'考虑信托资产中集合类信托资产的占比，比例考察最近连续两年的平均值；结合运营期考虑上述占比' as cal_explain,'考察结合运营期的集合类信托资产占比情况，运营期越长表明产品运营越稳定，短期内风险较低' as feature_explain,'' as unit_origin,'' as unit_target

union all select 222 as sid_kw,'Trust_AssetAdequacy2' as feature_cd,'净资本除各项业务风险资本之和' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'净资本=净资产-各类资产的风险扣除项-或有负债的风险扣除项-中国银行保险业监督管理委员会认定的其他风险扣除项；' as cal_explain,'净资本对各项业务风险资本的覆盖程度越高，表明信托公司资本实力更强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 223 as sid_kw,'Trust_Size8_ln' as feature_cd,'手续费及佣金净收入' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'手续费及佣金净收入' as cal_explain,'手续费及佣金净收入规模，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'' as unit_target

union all select 224 as sid_kw,'Trust_Size3_ln' as feature_cd,'固有资产平均值' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'信托公司的固有资产，包括贷款、信托产品及其他理财产品投资、长期股权投资、证券投资等业务；
（本期信托公司固有资产+上期信托公司固有资产）/2；' as cal_explain,'固有资产规模越大，抗风险能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 225 as sid_kw,'Trust_Growth1' as feature_cd,'营业收入增长率' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'本期营业收入/上期营业收入-1；' as cal_explain,'营业收入增长率越大，收入规模增长越快' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 226 as sid_kw,'Trust_Leverage1' as feature_cd,'资产负债率平均值' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'（本期值+上期值）/2' as cal_explain,'值越小，债务压力越小' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 227 as sid_kw,'Trust_AssetQuality1' as feature_cd,'不良率平均值' as feature_name,'低频-信托' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'信托资产的五级分类后三类资产占比,（本期值+上期值）/2' as cal_explain,'不良率平均值越大，信托资产不良率越大，业务风险越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 228 as sid_kw,'QualFact1' as feature_cd,'运营时间' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'基金公司运营年限' as cal_explain,'基金公司运营年限越长，反映基金公司运营成熟度及稳定性越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 229 as sid_kw,'QualFact2' as feature_cd,'股东背景' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'基金公司股东及持股比例' as cal_explain,'考虑基金公司最大持股比例股东的背景，股东背景由强到弱的顺序依次为：国有资产监督委员会（北京/上海）、全国性国有企业，以及大型券商>国有资产监督委员会（其他地方省级）、地方国有企业、合资企业、中型券商，以及大型信托公司>民营企业、小型券商，以及中小型信托公司' as feature_explain,'' as unit_origin,'' as unit_target

union all select 230 as sid_kw,'QualFact3' as feature_cd,'基金个数' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'(本期基金个数+上期基金个数)/2' as cal_explain,'近两期在管基金平均数' as feature_explain,'' as unit_origin,'' as unit_target

union all select 231 as sid_kw,'QualFact5' as feature_cd,'基金经理平均年限' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'本期平均年限=本期基金经理年限之和/本期基金经理数，计算最近两年的平均值；' as cal_explain,'平均年限越长，反映基金公司关键人员越稳定' as feature_explain,'' as unit_origin,'' as unit_target

union all select 232 as sid_kw,'QuantFact1' as feature_cd,'所管理基金的资产总值平均值' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'(本期所管理基金资产总值（亿元）+上期所管理基金资产总值（亿元）)/2' as cal_explain,'近两期基金资产总值的平均值' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 233 as sid_kw,'QuantFact12' as feature_cd,'股票持仓时间平均值' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'(本期股票持仓时间+上期股票持仓时间)/2' as cal_explain,'近两期股票持仓时间的算数平均值' as feature_explain,'' as unit_origin,'' as unit_target

union all select 234 as sid_kw,'QuantFact18' as feature_cd,'股票基金资产总值平均值' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'股票基金资产总值平均值（亿元）' as cal_explain,'股票型基金总值平均值' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 235 as sid_kw,'QuantFact5' as feature_cd,'所管理基金资产总值增长率' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'(本期所管理基金资产总值（亿元）-上期所管理基金资产总值（亿元）)/上期所管理基金资产总值（亿元）' as cal_explain,'比值为正且值越大反映基金公司基金管理规模有增长趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 236 as sid_kw,'QuantFact8' as feature_cd,'股票和债券基金的资产总值增长率' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'股票和债券基金资产总值增长率' as cal_explain,'比值为正且值越大反映基金公司股票及债券基金管理规模有增长趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 237 as sid_kw,'QuantFact9' as feature_cd,'持股市盈率平均值' as feature_name,'低频-基金' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'(本期持股市盈率+上期持股市盈率)/2' as cal_explain,'反映基金公司的投资质量，值越大反映投资质量越好。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 238 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 239 as sid_kw,'factor_004' as feature_cd,'融资渠道多样性' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'融资能力' as type,'平均值=(本期值+上期值)/2；' as cal_explain,'企业在通过银行获取资金的同时，若还能通过股票、债券等方式筹集资金，则说明其融资渠道越宽，风险越小。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 240 as sid_kw,'factor_135' as feature_cd,'银行授信总额' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'担保公司的银行网络' as cal_explain,'较大的银行授信额度有利于融资担保公司贷款担保业务的拓展，也说明融资担保公司能获得银行认可；与银行达成风险分担机制能够有效减少融资担保公司代偿损失。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 241 as sid_kw,'factor_094' as feature_cd,'净资产收益率' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初所有者权益+期末所有者权益）*100%' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 242 as sid_kw,'factor_095' as feature_cd,'担保业务规模' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'公司担保业务的责任余额' as cal_explain,'只有实现一定业务规模，担保机构才能积累基本的风险管理经验，并逐步取得市场认可，提升市场地位，促进业务扩张。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 243 as sid_kw,'factor_096' as feature_cd,'实收资本规模' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'企业规模' as type,'投资者作为资本投入企业的各种资产' as cal_explain,'可以立刻运用来吸收损失的资本金，体现担保机构代偿能力的核心' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 244 as sid_kw,'factor_098' as feature_cd,'担保业务杠杆' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'担保业务杠杆' as cal_explain,'担保业务杠杆倍数合理，表明担保公司资金运营效率高，能较好地平衡风险和收益' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 245 as sid_kw,'factor_099' as feature_cd,'累计代偿损失率' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'累计代偿损失/累计代偿金额' as cal_explain,'累计代偿损失率，代表了担保公司的追偿能力；代偿损失率越低，公司的追偿能力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 246 as sid_kw,'factor_100' as feature_cd,'累计担保代偿率' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'累计担保代偿率=累计代偿金额/累计解除担保金额（根据收数情况确定年限口径）' as cal_explain,'累计担保代偿率，代表了担保公司的风险管理能力。累计担保代偿率越低，表明担保业务的标的筛选能力和风险管理能力越强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 247 as sid_kw,'factor_102' as feature_cd,'流动资产占比' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'流动资产（亿元）/总资产（亿元）*100%' as cal_explain,'流动资产占比，一定程度上衡量担保机构在短期（一年内）内进行代偿能力；' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 248 as sid_kw,'factor_103' as feature_cd,'风险准备金覆盖率' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'（未到期责任准备+担保赔偿准备+一般风险准备）/当年末在保责任余额*100%' as cal_explain,'既反映了担保机构对风险的审慎态度，又反映担保机构对担保资金积累的重视程度。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 249 as sid_kw,'factor_136' as feature_cd,'新增在保责任余额' as feature_name,'低频-融资性担保' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'新增在保责任余额（亿元）' as cal_explain,'担保业务收入增速越快，表明公司的担保业务越有竞争力' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 250 as sid_kw,'factor_125' as feature_cd,'股权结构_金融' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 251 as sid_kw,'factor_005' as feature_cd,'银行授信额度占比' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'银行授信总额度（亿元）/所有者权益（亿元）' as cal_explain,'从相对层面考察银行对企业资质的认可程度，银行授信总额/所有者权益越高，银行对其资质的认可度越大，侧面反映企业资质越优秀。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 252 as sid_kw,'factor_106' as feature_cd,'行业地位' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在细分领域中的地位（金融租赁、融资租赁、汽车金融、财务公司、小额贷款、AMC）' as cal_explain,'一方面行业地位越高反映了企业的经营实力越强，信用风险越低；另一方面行业地位越高企业越容易获得相关资源，有利于企业的经营，企业信用风险相对较低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 253 as sid_kw,'factor_107' as feature_cd,'放款规模' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'应收租赁款（亿元）' as cal_explain,'应收租赁款项越多，企业对租赁方话语权越弱，业务运营能力越弱' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 254 as sid_kw,'factor_108' as feature_cd,'经营年限' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业成立至今的年限' as cal_explain,'企业成立的时间越久，企业累计的行业经验，公司管理运营经验越丰富，公司经营风险相对越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 255 as sid_kw,'factor_109' as feature_cd,'投向行业集中度' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'租赁业务投向最大单一行业占比' as cal_explain,'行业集中程度越高，受到单个行业的景气程度以及政策影响的风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 256 as sid_kw,'factor_110' as feature_cd,'投向行业前景' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业主要投向行业景气度' as cal_explain,'行业集中程度越高，受到单个行业的景气程度以及政策影响的风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 257 as sid_kw,'factor_126' as feature_cd,'客户集中度_金融' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'综合考量前十大客户集中度及单一最大客户集中度的情况' as cal_explain,'集中度越高，抗风险能力相对较弱' as feature_explain,'' as unit_origin,'' as unit_target

union all select 258 as sid_kw,'factor_111' as feature_cd,'不良资产率' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'五级分类后三类的资产合计/贷款总额（不良资产/全部融资租赁资产）' as cal_explain,'一方面行业地位越高反映了企业的经营实力越强，信用风险越低；另一方面行业地位越高企业越容易获得相关资源，有利于企业的经营，企业信用风险相对较低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 259 as sid_kw,'factor_112' as feature_cd,'拨备覆盖率' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'经营' as dimension,'风险管理' as type,'收息资产损失准备余额/不良收息资产余额*100%' as cal_explain,'一方面行业地位越高反映了企业的经营实力越强，信用风险越低；另一方面行业地位越高企业越容易获得相关资源，有利于企业的经营，企业信用风险相对较低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 260 as sid_kw,'factor_113' as feature_cd,'所有者权益（亿元）' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'所有者权益' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 261 as sid_kw,'factor_114' as feature_cd,'资产负债率' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'负债总计/资产总计' as cal_explain,'资产负债率反映了企业的债务水平，值越大，债务水平越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 262 as sid_kw,'factor_115' as feature_cd,'营业收入平均增长率' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'((本期营业收入-上期营业收入）/上期营业收入+(上期营业收入-上上期营业收入）/上上期营业收入）/2' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 263 as sid_kw,'factor_116' as feature_cd,'所有者权益/风险资产' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'所有者权益/风险资产' as cal_explain,'资本充足率越高，风险越小' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 264 as sid_kw,'factor_117' as feature_cd,'高流动性资产占比' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'高流动性资产/资产总计' as cal_explain,'高流动性资产占比越高，企业资产流动性越强，风险越低' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 265 as sid_kw,'factor_094' as feature_cd,'净资产收益率' as feature_name,'低频-收息企业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/（本期所有者权益+上期所有者权益）' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 266 as sid_kw,'factor_125' as feature_cd,'股权结构_金融' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'控股股东类型' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 267 as sid_kw,'factor_134' as feature_cd,'行业地位_股权投资' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'企业在股权投资相应细分领域中的地位' as cal_explain,'一方面行业地位越高反映了企业的经营实力越强，信用风险越低；另一方面行业地位越高企业越容易获得相关资源，有利于企业的经营，企业信用风险相对较低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 268 as sid_kw,'factor_127' as feature_cd,'管理资产规模（亿元）' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'在投项目规模+管理基金规模' as cal_explain,'投资规模越大，经营实力越强' as feature_explain,'' as unit_origin,'' as unit_target

union all select 269 as sid_kw,'factor_128' as feature_cd,'基金类型' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'募投资金投向的股权的阶段' as cal_explain,'不同阶段的股权投资的风险阶段不一致，投资初期企业的风险相对较高' as feature_explain,'' as unit_origin,'' as unit_target

union all select 270 as sid_kw,'factor_129' as feature_cd,'投资集中度' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'募集基金投向的行业集中度越高' as cal_explain,'行业集中程度越高，受到单个行业的景气程度以及政策影响的风险越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 271 as sid_kw,'factor_130' as feature_cd,'资产收益' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'企业过去投资项目的收益情况，（资产收益（亿元）+其他综合收益（亿元））/（长期股权投资（亿元）+可供出售金融资产（亿元）+交易性金融资产（亿元））' as cal_explain,'投资收益衡量企业的盈利能力，投资收益越高，企业的投资能力越好。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 272 as sid_kw,'factor_120' as feature_cd,'经营区域范围' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业主要的服务范围' as cal_explain,'企业经营服务的范围越广，企业的规模越大，企业潜在的服务客户越大，客户的信用风险越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 273 as sid_kw,'factor_108' as feature_cd,'经营年限' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业成立至今的年限' as cal_explain,'企业成立的时间越久，企业累计的行业经验，公司管理运营经验越丰富，公司经营风险相对越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 274 as sid_kw,'factor_004' as feature_cd,'融资渠道多样性' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'经营' as dimension,'融资能力' as type,'上市发债历史' as cal_explain,'企业在通过银行获取资金的同时，若还能通过股票、债券等方式筹集资金，则说明其融资渠道越宽，风险越小。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 275 as sid_kw,'factor_113' as feature_cd,'所有者权益（亿元）' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'所有者权益' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 276 as sid_kw,'factor_132' as feature_cd,'资本化率' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'有息债务/(有息债务+所有者权益）' as cal_explain,'有息债务/(有息债务+所有者权益)是指有息债务与投入资本的比率，反映了企业的债务水平，值越大，债务水平越高' as feature_explain,'' as unit_origin,'' as unit_target

union all select 277 as sid_kw,'factor_094' as feature_cd,'净资产收益率' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(期初股东权益合计+股东权益合计)' as cal_explain,'净资产收益率是指净利润与净资产的比率，它反映每1元净资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 278 as sid_kw,'factor_133' as feature_cd,'现金比率' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(货币资金+交易性金融资产)/流动负债' as cal_explain,'现金比率反应了企业的流动性，比值越大，流动性越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 279 as sid_kw,'factor_122' as feature_cd,'净资产增长率' as feature_name,'低频-股权投资' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'净资产增长率' as cal_explain,'所有者权益同比增长率指所有者权益增长率，比率越大，所有者权益增长越快' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 280 as sid_kw,'factor_005' as feature_cd,'银行授信额度占比' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'银行授信总额度/所有者权益' as cal_explain,'从相对层面考察银行对企业资质的认可程度，银行授信总额/所有者权益越高，银行对其资质的认可度越大，侧面反映企业资质越优秀。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 281 as sid_kw,'factor_108' as feature_cd,'经营年限' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业成立至今的年限' as cal_explain,'企业成立的时间越久，企业累计的行业经验，公司管理运营经验越丰富，公司经营风险相对越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 282 as sid_kw,'factor_113' as feature_cd,'所有者权益（亿元）' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'所有者权益' as cal_explain,'所有者权益，值越大，抗风险能力越强' as feature_explain,'亿元' as unit_origin,'亿元' as unit_target

union all select 283 as sid_kw,'factor_114' as feature_cd,'资产负债率' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'负债总计/资产总计' as cal_explain,'资产负债率反映了企业的债务水平，值越大，债务水平越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 284 as sid_kw,'factor_120' as feature_cd,'经营区域范围' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'营运能力' as type,'企业主要的服务范围' as cal_explain,'企业经营服务的范围越广，企业的规模越大，企业潜在的服务客户越大，客户的信用风险越低。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 285 as sid_kw,'factor_121' as feature_cd,'归属于母公司的所有者权益/所有者权益' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'归属于母公司的所有者权益（亿元）/所有者权益（亿元）' as cal_explain,'母公司对子公司的控股力度越强，子公司对母公司的收入和利润贡献越有保障，母公司的信用资质越高' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 286 as sid_kw,'factor_122' as feature_cd,'净资产增长率' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'（（本期期末所有者权益（亿元）-上期期末所有者权益（亿元））/上期期末所有者权益（亿元）+（上期期末所有者权益（亿元）-上上期期末所有者权益（亿元））/上上期期末所有者权益（亿元））/2' as cal_explain,'所有者权益同比增长率指所有者权益增长率，比率越大，所有者权益增长越快' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 287 as sid_kw,'factor_123' as feature_cd,'总资产收益率' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'净利润*2/(本期期末资产总计+上期期末资产总计）' as cal_explain,'净资产收益率是指净利润与资产总计的比率，它反映每1元总资产创造的净利润，值越大，盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 288 as sid_kw,'factor_124' as feature_cd,'流动比率' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'流动资产/流动负债' as cal_explain,'流动比率反应了企业的流动性，比值越大，流动性越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 289 as sid_kw,'factor_125' as feature_cd,'股权结构_金融' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'外部支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 290 as sid_kw,'factor_118' as feature_cd,'金融牌照质量' as feature_name,'低频-金融通用' as sub_model_type,'' as feature_name_target,'经营' as dimension,'竞争实力' as type,'金融牌照数量和种类' as cal_explain,'企业拥有的金融牌照种类越丰富，数量越多，开展业务的能力和服务范围越广，企业的竞争实力越强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 291 as sid_kw,'Finance_092' as feature_cd,'EBITDA债务保障倍数' as feature_name,'中频城投' as sub_model_type,'EBITDA债务保障倍数' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBITDA/有息债务' as cal_explain,'反映偿债能力，该比值越大，企业偿债能力越强。' as feature_explain,'倍' as unit_origin,'倍' as unit_target

union all select 292 as sid_kw,'Leverage13' as feature_cd,'流动比率3' as feature_name,'中频城投' as sub_model_type,'流动比率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'EBIT/财务费用' as cal_explain,'利息保障倍数反映企业的获利能力大小，也是衡量企业长期偿债能力大小的重要标志。要维持正常偿债能力，倍数至少应大于1，且比值越高，企业长期偿债能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 293 as sid_kw,'Leverage2' as feature_cd,'速动比率' as feature_name,'中频城投' as sub_model_type,'速动比率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'(流动资产合计-存货)/流动负债合计' as cal_explain,'速动比率反映了一个单位能够立即还债的能力和水平，直接反映企业的短期偿债能力。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 294 as sid_kw,'zs_factor_29' as feature_cd,'短期债务利息支付覆盖率' as feature_name,'中频城投' as sub_model_type,'短期债务利息支付覆盖率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'货币资金/(短期债务2+分配股利利润或偿付利息支付的货币资金）
其中：短期债务2=短期借款+应付票据+一年内到期非流动负债+其他流动负债' as cal_explain,'覆盖率越高反映企业对短期债务利息的偿还能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 295 as sid_kw,'Specific7' as feature_cd,'（货币资金+交易性金融资产）/（流动负债合计-预收款项）' as feature_name,'中频城投' as sub_model_type,'（货币资金+交易性金融资产）/（流动负债合计-预收款项）' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（货币资金+交易性金融资产）/（流动负债合计-预收款项）' as cal_explain,'货币资金和交易性金融资产能否覆盖流动负债，占比越大说明公司资金链充足，短期偿债能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 296 as sid_kw,'Operation8' as feature_cd,'营运资本周转率' as feature_name,'中频城投' as sub_model_type,'营运资本周转率' as feature_name_target,'财务' as dimension,'营运能力' as type,'营业收入*2 / [ ( 期初流动资产合计 - 期初流动负债合计) + (流动资产合计 - 流动负债合计)]' as cal_explain,'营运资本周转率表明企业营运资本的经营效率，该指标越高说明企业营运资本的运用效率也就越高，反之则说明运用效率越低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 297 as sid_kw,'factor_186' as feature_cd,'短期债务利息支出覆盖率2' as feature_name,'中频城投' as sub_model_type,'短期债务利息支出覆盖率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（现金+交易性金融资产）/(短期债务2+利息支出）
其中：短期债务2=短期借款+应付票据+一年内到期非流动负债+其他流动负债' as cal_explain,'短期债务利息支出覆盖率越高，企业短期偿债能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 298 as sid_kw,'factor_530' as feature_cd,'固定投资额占比' as feature_name,'中频城投' as sub_model_type,'固定投资额占比' as feature_name_target,'经营' as dimension,'区域经济' as type,'当年固定投资额/地区生产总值' as cal_explain,'该指标反映了GDP的构成。一方面若固定投资占比过高，则当地经济发展依赖固定投资拉动程度过高，经济发展不稳定；另一方面固定投资占比过低则表明当地经济发展乏力。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 299 as sid_kw,'factor_713' as feature_cd,'固定投资额' as feature_name,'中频城投' as sub_model_type,'固定投资额' as feature_name_target,'经营' as dimension,'区域经济' as type,'固定投资额' as cal_explain,'固定投资可以拉动当地区域的经济发展，固定投资过小，当地经济发展乏力表现，持续稳定的固定投资额能够为当地经济发展提供持续稳健的驱动力。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 300 as sid_kw,'indus_rela_invest_income_year_on_year' as feature_cd,'行业相对_投资收益同比增长率' as feature_name,'中频城投' as sub_model_type,'行业相对_投资收益同比增长率' as feature_name_target,'财务' as dimension,'盈利能力' as type,'投资收益同比增长率-投资收益同比增长率敞口中位数
其中：投资收益同比增长率=（本期投资收益-上年同期投资收益）/ABS（上年同期投资收益）' as cal_explain,'反映投资收益增长率相对行业的偏离水平，该指标为正且值越大代表企业投资收益增长率超过行业中等水平。' as feature_explain,'%' as unit_origin,'%' as unit_target

union all select 301 as sid_kw,'indus_rela_mom12_current_asset_tot_asset' as feature_cd,'行业相对_环比流动资产比率(流动资产／总资产)' as feature_name,'中频城投' as sub_model_type,'行业相对_环比流动资产比率(流动资产／总资产)' as feature_name_target,'财务' as dimension,'资本结构' as type,'流动资产比率环比变动率-流动资产比率环比变动率敞口中位数，其中：
流动资产比率环比变动率=（最新定报流动资产比率-上年同期定报流动资产比率）/上年同期定报流动资产比率；
流动资产比率=流动资产合计/资产合计' as cal_explain,'反映资产变现能力变化相对行业中等水平的表现，指标值为正且越大说明企业相对行业中等水平，资产变现能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 302 as sid_kw,'indus_rela_mom12_equity_atsopc_tot_invest' as feature_cd,'行业相对_环比归属母公司股东的权益／全部投入资本' as feature_name,'中频城投' as sub_model_type,'行业相对_环比归属母公司股东的权益／全部投入资本' as feature_name_target,'财务' as dimension,'资本结构' as type,'归属母公司股东的权益／全部投入资本环比变动率-归属母公司股东的权益／全部投入资本环比变动率敞口中位数，其中：
归属母公司股东的权益／全部投入资本环比变动率=（最新定报归属母公司股东的权益／全部投入资本-上年同期定报归属母公司股东的权益／全部投入资本）/(上年同期定报归属母公司股东的权益／全部投入资本)；
全部投入资本=股东权益（不含少数股东权益）+带息债务' as cal_explain,'反映企业杠杆水平变动相对行业中等水平的表现，指标值小于0且越小说明企业杠杆风险越大。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 303 as sid_kw,'indus_rela_mom12_noncurrent_assets_to_ta' as feature_cd,'行业相对_环比非流动资产／总资产' as feature_name,'中频城投' as sub_model_type,'行业相对_环比非流动资产／总资产' as feature_name_target,'财务' as dimension,'资本结构' as type,'非流动资产／总资产环比变动率-非流动资产／总资产环比变动率敞口中位数，其中：
非流动资产／总资产环比变动率=（最新定报非流动资产／总资产-上年同期定报非流动资产／总资产）/（上年同期定报非流动资产／总资产）' as cal_explain,'反映资产变现能力变化相对行业中等水平的表现，指标值为正且越大说明企业相对行业中等水平，资产变现能力越弱。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 304 as sid_kw,'yoy12_dwi_toall_invest_capital' as feature_cd,'1年同比增长带息债务／全部投入资本' as feature_name,'中频城投' as sub_model_type,'1年同比增长带息债务／全部投入资本' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（最新定报带息债务／全部投入资本-上年同期定报带息债务／全部投入资本）/（上年同期定报带息债务／全部投入资本）' as cal_explain,'主要反映带息债务占比的变动率，变动率越大说明债务比重及杠杆风险有增长趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 305 as sid_kw,'yoy12_lt_debt_to_oc' as feature_cd,'1年同比增长长期债务与营运资金比率' as feature_name,'中频城投' as sub_model_type,'1年同比增长长期债务与营运资金比率' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（最新定报非流动负债合计/营运资金-上年同期定报非流动负债合计/营运资金）/（上年同期定报非流动负债合计/营运资金）' as cal_explain,'反映长期债务压力的变动，指标值为正且越大说明长期债务压力有增长趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 306 as sid_kw,'indus_rela_yoy12_noncurrent_assets_to_ta' as feature_cd,'行业相对_1年同比增长非流动资产／总资产' as feature_name,'中频城投' as sub_model_type,'行业相对_1年同比增长非流动资产／总资产' as feature_name_target,'财务' as dimension,'资本结构' as type,'非流动资产／总资产1年同比增长率-非流动资产／总资产1年同比增长率敞口中位数，其中：
非流动资产／总资产1年同比增长率=（最新定报非流动资产／总资产-上年同期定报非流动资产／总资产）/（上年同期定报非流动资产／总资产）' as cal_explain,'反映资产变现能力变化相对行业中等水平的表现，指标值为正且越大说明企业相对行业中等水平，资产变现能力越弱。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 307 as sid_kw,'yoy13_dwi_toall_invest_capital' as feature_cd,'2年同比增长带息债务／全部投入资本' as feature_name,'中频城投' as sub_model_type,'2年同比增长带息债务／全部投入资本' as feature_name_target,'财务' as dimension,'偿债能力' as type,'+/-√[ABS（最新定报带息债务／全部投入资本-上上年同期定报带息债务／全部投入资本）/ABS（上上年同期定报资产减值损失／营业总收入）]
其中：全部投入资本=股东权益（不含少数股东权益）+带息债务' as cal_explain,'主要反映带息债务占比的变动率，变动率为正且越大说明债务比重及杠杆风险有增长趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 308 as sid_kw,'value_chg_ni_to_profit' as feature_cd,'价值变动净收益／利润总额' as feature_name,'中频城投' as sub_model_type,'价值变动净收益／利润总额' as feature_name_target,'财务' as dimension,'盈利能力' as type,'旧准则：投资净收益／利润总额
新准则：（公允价值变动净收益+投资净收益+汇兑净收益）/利润总额' as cal_explain,'价值变动净收益是非经营性损益，反映了非经营性损益对利润的贡献程度，比值越高，非经营性损益对利润的贡献程度越高。' as feature_explain,'%' as unit_origin,'%' as unit_target

union all select 309 as sid_kw,'yoy13_operating_cycle' as feature_cd,'2年同比增长营业周期' as feature_name,'中频城投' as sub_model_type,'2年同比增长营业周期' as feature_name_target,'财务' as dimension,'营运能力' as type,'+/-√[ABS（最新定报营业周期-上上年同期定报营业周期）/ABS(上上年同期定报营业周期)]
其中：营业周期=存货周转天数+应收账款周转天数' as cal_explain,'反映企业对应收账款及存货的管理效率变动情况，变动率越大说明企业对应收账款及存货的回收效率有降低趋势。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 310 as sid_kw,'zs_factor_11' as feature_cd,'吸收投资现金占比' as feature_name,'中频城投' as sub_model_type,'吸收投资现金占比' as feature_name_target,'财务' as dimension,'融资能力' as type,'吸收投资收到的货币资金/总资产' as cal_explain,'反映企业筹资活动获取现金的能力，该指标值越大，说明企业融资能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 311 as sid_kw,'last6Mto12M_news_label_6008001_num' as feature_cd,'近6个月比近12个月_新闻_标签_问询关注_数量' as feature_name,'高频' as sub_model_type,'负面新闻增长率(问询关注类)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'[(近6个月标签为问询关注的负面新闻总量/6)-(近12个月标签为问询关注的负面新闻总量/12)]/(近12个月标签为问询关注的负面新闻总量/12)' as cal_explain,'反映主体涉及问询关注类负面新闻的数量变化趋势，指标值越大反映主体涉及问询关注类负面新闻有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 312 as sid_kw,'indus_rela_last6Mto12M_news_label_6002012_meanimportance' as feature_cd,'行业相对_近6个月比近12个月_新闻_标签_其他财务预警_情感平均值' as feature_name,'高频' as sub_model_type,'新闻负面程度增长率_超出行业平均(其他财务预警类)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'主体近6个月比近12个月_新闻_标签_其他财务预警_情感平均值-行业内所有主体的近6个月比近12个月_新闻_标签_其他财务预警_情感平均值中位数，其中：近6个月比近12个月_新闻_标签_其他财务预警_情感平均值=(近6个月标签为其他财务预警的负面新闻严重程度平均值-近12个月标签为其他财务预警的负面新闻严重程度平均值)/近12个月标签为其他财务预警的负面新闻严重程度平均值' as cal_explain,'反映主体涉及其他财务预警类负面新闻的严重程度变化趋势相对行业平均水平的表现，指标值越大反映主体涉及其他财务预警类负面新闻严重程度恶化趋势相较行业平均水平越严重' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 313 as sid_kw,'last1W_news_count' as feature_cd,'近1周_新闻_数量' as feature_name,'高频' as sub_model_type,'负面新闻量_近1周' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近1周内主体负面新闻的数量统计' as cal_explain,'值越大反映主体近1周内新增负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 314 as sid_kw,'indus_rela_last2W_news_count' as feature_cd,'行业相对_近2周_新闻_数量' as feature_name,'高频' as sub_model_type,'负面新闻量_超出行业平均_近2周' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近2周内主体负面新闻的数量统计-行业内所有主体近2周内负面新闻的数量统计中位数' as cal_explain,'反映主体近2周内新增负面新闻数量相对行业平均水平的表现，指标值越大反映主体近2周新增负面新闻数量相对行业平均水平越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 315 as sid_kw,'indus_rela_last1M_news_label_6002001_rate' as feature_cd,'行业相对_近1个月_新闻_标签_财务亏损_占比' as feature_name,'高频' as sub_model_type,'财务亏损类负面新闻占比_超出行业平均_近1个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'主体近1个月_新闻_标签_财务亏损_占比-行业内所有主体的近1个月_新闻_标签_财务亏损_占比中位数，其中：近1个月_新闻_标签_财务亏损_占比=近1个月标签为财务亏损的负面新闻数量/近1个月所有负面新闻数量' as cal_explain,'反映主体近1个月内负面新闻中财务亏损类负面新闻占比相对行业平均水平的表现，指标值越大反映主体近1个月涉及财务亏损类负面新闻的比重相对行业平均水平越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 316 as sid_kw,'indus_rela_last6Mto12M_news_label_-1_meanimportance' as feature_cd,'行业相对_近6个月比近12个月_新闻_标签_其他_情感平均值' as feature_name,'高频' as sub_model_type,'新闻负面程度增长率_超出行业平均(其他类)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'主体近6个月比近12个月_新闻_标签_其他_情感平均值-行业内所有主体的近6个月比近12个月_新闻_标签_其他_情感平均值中位数，其中：近6个月比近12个月_新闻_标签_其他_情感平均值=(近6个月标签为其他的负面新闻严重程度平均值-近12个月标签为其他的负面新闻严重程度平均值)/近12个月标签为其他的负面新闻严重程度平均值' as cal_explain,'反映主体涉及其他类负面新闻的严重程度变化趋势相对行业平均水平的表现，指标值越大反映主体涉及其他类负面新闻严重程度恶化趋势相较行业平均水平越严重' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 317 as sid_kw,'indus_rela_last3M_news_label_6002001_rate' as feature_cd,'行业相对_近3个月_新闻_标签_财务亏损_占比' as feature_name,'高频' as sub_model_type,'财务亏损类负面新闻占比_超出行业平均_近3个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'主体近3个月_新闻_标签_财务亏损_占比-行业内所有主体的近3个月_新闻_标签_财务亏损_占比中位数，其中：近3个月_新闻_标签_财务亏损_占比=近3个月标签为财务亏损的负面新闻数量/近3个月所有负面新闻数量' as cal_explain,'反映主体近3个月内负面新闻中财务亏损类负面新闻占比相对行业平均水平的表现，指标值越大反映主体近3个月涉及财务亏损类负面新闻的比重相对行业平均水平越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 318 as sid_kw,'indus_rela_last3M_news_label_6003007_num' as feature_cd,'行业相对_近3个月_新闻_标签_关联企业出现问题_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(关联企业出现问题类)_超出行业平均_近3个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'主体近3个月内标签为关联企业出现问题的负面新闻数量统计-行业内所有主体近3个月内标签为关联企业出现问题的负面新闻数量统计中位数' as cal_explain,'反映主体近3个月内涉及关联企业出现问题负面新闻数量相对行业平均水平的表现，指标值越大反映主体3个月内涉及关联企业出现问题负面新闻数量相对行业平均水平越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 319 as sid_kw,'last6M_news_label_6002002_rate' as feature_cd,'近6个月_新闻_标签_流动性风险_占比' as feature_name,'高频' as sub_model_type,'流动性风险类负面新闻占比_近6个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近6个月内标签为流动性风险的负面新闻数量/近6个月内所有负面新闻数量' as cal_explain,'反映主体近6个月内负面新闻中流动性风险类负面新闻占比，指标值越大反映主体近6个月涉及流动性风险类负面新闻的比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 320 as sid_kw,'last6M_news_label_6002001_num' as feature_cd,'近6个月_新闻_标签_财务亏损_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(财务亏损类)_近6个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近6个月内标签为财务亏损的负面新闻数量统计' as cal_explain,'值越大反映主体近6个月内涉及财务亏损类负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 321 as sid_kw,'indus_rela_last12M_news_label_6009002_num' as feature_cd,'行业相对_近12个月_新闻_标签_股份减持_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(股份减持类)_超出行业平均_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内主体涉及标签为股份减持负面新闻的数量统计-行业内所有主体近12个月内标签为股份减持负面新闻的数量统计中位数' as cal_explain,'反映主体近12个月内涉及股份减持负面新闻数量相对行业平均水平的表现，指标值越大反映主体12个月内涉及股份减持负面新闻数量相对行业平均水平越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 322 as sid_kw,'indus_rela_last12M_news_label_6002012_num' as feature_cd,'行业相对_近12个月_新闻_标签_其他财务预警_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(其他财务预警类)_超出行业平均_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内主体涉及标签为其他财务预警负面新闻的数量统计-行业内所有主体近12个月内标签为其他财务预警负面新闻的数量统计中位数' as cal_explain,'反映主体近12个月内涉及其他财务预警负面新闻数量相对行业平均水平的表现，指标值越大反映主体12个月内涉及其他财务预警负面新闻数量相对行业平均水平越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 323 as sid_kw,'indus_rela_last12M_news_label_6009003_num' as feature_cd,'行业相对_近12个月_新闻_标签_证券价格异常波动_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(证券价格异常波动类)_超出行业平均_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内主体涉及标签为证券价格异常波动负面新闻的数量统计-行业内所有主体近12个月内标签为证券价格异常波动负面新闻的数量统计中位数' as cal_explain,'反映主体近12个月内涉及证券价格异常负面新闻数量相对行业平均水平的表现，指标值越大反映主体12个月内涉及证券价格异常负面新闻数量相对行业平均水平越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 324 as sid_kw,'last12M_news_label_6002002_rate' as feature_cd,'近12个月_新闻_标签_流动性风险_占比' as feature_name,'高频' as sub_model_type,'流动性风险类负面新闻占比_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内标签为流动性风险的负面新闻数量/近12个月内所有负面新闻数量' as cal_explain,'反映主体近12个月内负面新闻中流动性风险类负面新闻占比，指标值越大反映主体近12个月涉及流动性风险类负面新闻的比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 325 as sid_kw,'last12M_news_label_6001002_num' as feature_cd,'近12个月_新闻_标签_评级下调_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(评级下调类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内标签为评级下调的负面新闻数量统计' as cal_explain,'值越大反映主体近12个月内涉及评级下调类负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 326 as sid_kw,'last12M_news_label_-1_num' as feature_cd,'近12个月_新闻_标签_其他_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(其他类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内标签为其他的负面新闻数量统计' as cal_explain,'值越大反映主体近12个月内其他类负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 327 as sid_kw,'last12M_news_label_6003011_meanimportance' as feature_cd,'近12个月_新闻_标签_出售、变卖资产_情感平均值' as feature_name,'高频' as sub_model_type,'新闻负面程度(出售、变卖资产类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月所有标签为出售、变卖资产负面新闻的情感加总/近12个月所有标签为出售、变卖资产负面新闻数量' as cal_explain,'反映主体近12个月内涉出售、变卖资产类负面新闻严重程度表现，指标值越低反映相关新闻越严重' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 328 as sid_kw,'last12M_news_count' as feature_cd,'近12个月_新闻_数量' as feature_name,'高频' as sub_model_type,'负面新闻量_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内所有负面新闻数量统计' as cal_explain,'指标值越大反映主体近12个月内负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 329 as sid_kw,'last12M_news_label_6004024_num' as feature_cd,'近12个月_新闻_标签_其他管理预警_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(其他管理预警类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月内标签为其他管理预警的负面新闻数量统计' as cal_explain,'指标值越大反映主体近12个月内涉及其他管理预警类负面新闻越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 330 as sid_kw,'last12M_news_label_6007002_rate' as feature_cd,'近12个月_新闻_标签_担保过多_占比' as feature_name,'高频' as sub_model_type,'担保过多类负面新闻占比_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月标签为担保过多的负面新闻数量/近12个月所有的负面新闻数量' as cal_explain,'指标值越大反映主体在近12个月内负面新闻中担保过多负面新闻的比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 331 as sid_kw,'last12M_news_label_6003064_num' as feature_cd,'近12个月_新闻_标签_其他经营预警_数量' as feature_name,'高频' as sub_model_type,'负面新闻量(其他经营预警类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月标签为其他经营预警的负面新闻数量' as cal_explain,'指标值越大反映主体在近12个月涉及其他经营预警负面新闻的数量越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 332 as sid_kw,'last6Mto12M_news_label_6003064_num' as feature_cd,'近6个月比近12个月_新闻_标签_其他经营预警_数量' as feature_name,'高频' as sub_model_type,'负面新闻增长率(其他经营预警类)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'(近6个月标签为其他经营预警的负面新闻数量/6-近12个月标签为其他经营预警的负面新闻数量/12)/(近12个月标签为其他经营预警的负面新闻数量/12)' as cal_explain,'反映主体涉其他经营预警类负面新闻数量的变化趋势，指标值越大反映主体涉其他经营预警类负面新闻有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 333 as sid_kw,'last6M_announce_typename_003_num' as feature_cd,'近6个月_公告_类别名称_其它临时公告_数量' as feature_name,'高频' as sub_model_type,'公告量(其他临时公告类)_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近6个月内主体涉其它临时公告的数量统计' as cal_explain,'指标值越大反映主体近6个月内涉及其它临时公告越多' as feature_explain,'条' as unit_origin,'条' as unit_target

union all select 334 as sid_kw,'last12M_announce_typename_053_rate' as feature_cd,'近12个月_公告_类别名称_变更高级管理人员公告_占比' as feature_name,'高频' as sub_model_type,'变更高级管理人员类公告占比_近12个月' as feature_name_target,'舆情' as dimension,'新闻公告' as type,'近12个月涉变更高级管理人员公告数量/近12个月涉及的所有公告数量' as cal_explain,'指标值越大反映主体近12个月内发布公告中涉及变更高级管理人员类公告的比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 335 as sid_kw,'last6Mto12M_lawsuit_detailedreason_4_rate' as feature_cd,'近6个月比近12个月_法院诉讼_案由明细_买卖合同纠纷_占比' as feature_name,'高频' as sub_model_type,'买卖合同纠纷类案件占比的增长率(法院诉讼)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月涉买卖合同纠纷类法院诉讼案件数量/近6个月所有涉及的法院诉讼案件数量)-(近12个月涉买卖合同纠纷类法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)]/(近12个月涉买卖合同纠纷类法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)' as cal_explain,'反映主体涉买卖合同纠纷类法院诉讼案件占比的变化趋势，指标值越大反映主体涉买卖合同纠纷类法院诉讼案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 336 as sid_kw,'last3Mto12M_lawsuit_detailedreason_7_rate' as feature_cd,'近3个月比近12个月_法院诉讼_案由明细_合同纠纷_占比' as feature_name,'高频' as sub_model_type,'合同纠纷类案件占比的增长率(法院诉讼)_近3个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近3个月涉合同纠纷类法院诉讼案件数量/近3个月所有涉及的法院诉讼案件数量)-(近12个月涉合同纠纷类法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)]/(近12个月涉合同纠纷类法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)' as cal_explain,'反映主体涉合同纠纷类法院诉讼案件占比的变化趋势，指标值越大反映主体涉合同纠纷类法院诉讼案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 337 as sid_kw,'last6Mto12M_lawsuit_detailedreason_7_num' as feature_cd,'近6个月比近12个月_法院诉讼_案由明细_合同纠纷_数量' as feature_name,'高频' as sub_model_type,'法院诉讼增长率(合同纠纷类)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月涉合同纠纷类法院诉讼案件数量/6)-(近12个月涉合同纠纷类法院诉讼案件数量/12)]/(近12个月涉合同纠纷类法院诉讼案件数量/12)' as cal_explain,'反映主体涉合同纠纷类法院诉讼案件数量的变化趋势，指标值越大反映主体涉合同纠纷类法院诉讼案件数量有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 338 as sid_kw,'last6Mto12M_lawsuit_partyrole_1_rate' as feature_cd,'近6个月比近12个月_法院诉讼_当事人类型_原告_占比' as feature_name,'高频' as sub_model_type,'作为原告涉案占比的增长率(法院诉讼)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月作为原告涉法院诉讼案件数量/近6个月所有涉及的法院诉讼案件数量)-(近12个月作为原告涉法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)]/(近12个月作为原告涉法院诉讼案件数量/近12个月所有涉及的法院诉讼案件数量)' as cal_explain,'反映主体作为原告涉法院诉讼案件占比的变化趋势，指标值越大反映主体作为原告涉法院诉讼案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 339 as sid_kw,'last6Mto12M_lawsuit_lawsuitstatus_2_num' as feature_cd,'近6个月比近12个月_法院诉讼_诉讼阶段_民事二审_数量' as feature_name,'高频' as sub_model_type,'法院诉讼增长率(民事二审阶段)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月处于民事二审阶段法院诉讼案件数量/6)-(近12个月处于民事二审阶段法院诉讼案件数量/12)]/(近12个月处于民事二审阶段法院诉讼案件数量/12)' as cal_explain,'反映主体涉法院诉讼且处于民事二审阶段案件数量的变化趋势，指标值越大反映主体处于民事二审阶段涉法院诉讼案件数量有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 340 as sid_kw,'last6M_lawsuit_partyrole_4_num' as feature_cd,'近6个月_法院诉讼_当事人类型_被申请人_数量' as feature_name,'高频' as sub_model_type,'法院诉讼量(作为被申请人)_近6个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近6个月作为被申请人涉法院诉讼案件数量' as cal_explain,'指标值越大反映近6个月内主体作为被申请人涉法院诉讼案件数量越多' as feature_explain,'件' as unit_origin,'件' as unit_target

union all select 341 as sid_kw,'last6M_lawsuit_detailedreason_-99_rate' as feature_cd,'近6个月_法院诉讼_案由明细_其他_占比' as feature_name,'高频' as sub_model_type,'其他类案件占比(法院诉讼)_近6个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近6个月涉其他类法院诉讼案件数量/近6个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近6个月内主体涉其他类法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 342 as sid_kw,'last12M_lawsuit_casetype_3_rate' as feature_cd,'近12个月_法院诉讼_案件类型_执行类案件_占比' as feature_name,'高频' as sub_model_type,'执行类案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉执行类法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉执行类法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 343 as sid_kw,'last12M_lawsuit_lawsuitstatus_1_rate' as feature_cd,'近12个月_法院诉讼_诉讼阶段_执行实施_占比' as feature_name,'高频' as sub_model_type,'已处执行实施阶段的案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月处于执行实施阶段法院诉讼案件数量/近12个月所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体处于执行实施阶段涉法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 344 as sid_kw,'last12M_lawsuit_casetype_5_rate' as feature_cd,'近12个月_法院诉讼_案件类型_财产保全_占比' as feature_name,'高频' as sub_model_type,'财产保全类案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉财产保全类法院诉讼案件数量/近12个月所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉财产保全类法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 345 as sid_kw,'last12M_lawsuit_partyrole_8_rate' as feature_cd,'近12个月_法院诉讼_当事人类型_被执行人_占比' as feature_name,'高频' as sub_model_type,'作为被执行人涉案占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月作为被执行人涉法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体作为被执行人涉法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 346 as sid_kw,'last12M_lawsuit_num' as feature_cd,'近12个月_法院诉讼_数量' as feature_name,'高频' as sub_model_type,'法院诉讼量_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉法院诉讼案件数量越多' as feature_explain,'件' as unit_origin,'件' as unit_target

union all select 347 as sid_kw,'last12M_lawsuit_casetype_2_rate' as feature_cd,'近12个月_法院诉讼_案件类型_非讼程序案件案由_占比' as feature_name,'高频' as sub_model_type,'非讼程序类案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉非讼程序类法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉非讼程序类法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 348 as sid_kw,'last12M_lawsuit_detailedreason_0_rate' as feature_cd,'近12个月_法院诉讼_案由明细_金融借款合同纠纷_占比' as feature_name,'高频' as sub_model_type,'金融借款合同类案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉金融借款合同纠纷法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉金融借款合同纠纷法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 349 as sid_kw,'last12M_lawsuit_detailedreason_7_rate' as feature_cd,'近12个月_法院诉讼_案由明细_合同纠纷_占比' as feature_name,'高频' as sub_model_type,'合同纠纷类案件占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月涉合同纠纷法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体涉合同纠纷法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 350 as sid_kw,'last12M_lawsuit_lawsuitamt_mean' as feature_cd,'近12个月_法院诉讼_涉案金额_平均值' as feature_name,'高频' as sub_model_type,'涉案金额(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月法院诉讼案件涉案金额加总/近12个月法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体法院诉讼涉案金额越高' as feature_explain,'元' as unit_origin,'万元' as unit_target

union all select 351 as sid_kw,'last12M_lawsuit_partyrole_4_rate' as feature_cd,'近12个月_法院诉讼_当事人类型_被申请人_占比' as feature_name,'高频' as sub_model_type,'作为被申请人涉案占比(法院诉讼)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月作为被申请人涉法院诉讼案件数量/近12个月涉及的所有法院诉讼案件数量' as cal_explain,'指标值越大反映近12个月内主体作为被申请人涉法院诉讼案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 352 as sid_kw,'last6Mto12M_courttrial_trialstatus_5_rate' as feature_cd,'近6个月比近12个月_开庭庭审_诉讼地位代码_上诉人_占比' as feature_name,'高频' as sub_model_type,'作为上诉人涉案占比的增长率(开庭庭审)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月作为上诉人涉开庭庭审案件数量/近6个月所有涉及的开庭庭审案件数量)-(近12个月作为上诉人涉开庭庭审案件数量/近12个月所有涉及的开庭庭审案件数量)]/(近12个月作为上诉人涉开庭庭审案件数量/近12个月所有涉及的开庭庭审案件数量)' as cal_explain,'反映主体作为上诉人涉开庭庭审案件占比的变化趋势，指标值越大反映主体作为上诉人涉开庭庭审案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 353 as sid_kw,'last6Mto12M_courttrial_trialstatus_10_rate' as feature_cd,'近6个月比近12个月_开庭庭审_诉讼地位代码_当事人_占比' as feature_name,'高频' as sub_model_type,'作为当事人涉案占比的增长率(开庭庭审)_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近6个月作为当事人涉开庭庭审案件数量/近6个月所有涉及的开庭庭审案件数量)-(近12个月作为当事人涉开庭庭审案件数量/近12个月所有涉及的开庭庭审案件数量)]/(近12个月作为当事人涉开庭庭审案件数量/近12个月所有涉及的开庭庭审案件数量)' as cal_explain,'反映主体作为当事人涉开庭庭审案件占比的变化趋势，指标值越大反映主体作为当事人涉开庭庭审案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 354 as sid_kw,'last1M_courttrial_trialstatus_2_rate' as feature_cd,'近1个月_开庭庭审_诉讼地位代码_原审被告_占比' as feature_name,'高频' as sub_model_type,'作为原审被告涉案占比(开庭庭审)_近1个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近1个月作为原审被告涉开庭庭审案件数量/近1个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近1个月内主体作为原审被告涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 355 as sid_kw,'last3Mto12M_courttrial_trialstatus_2_num' as feature_cd,'近3个月比近12个月_开庭庭审_诉讼地位代码_原审被告_数量' as feature_name,'高频' as sub_model_type,'开庭庭审增长率(作为原审被告)_近3个月相对12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'[(近3个月作为原审被告涉开庭庭审案件数量/3)-(近12个月作为原审被告涉开庭庭审案件数量/12)]/(近12个月作为原审被告涉开庭庭审案件数量/12)' as cal_explain,'反映主体作为原审被告涉开庭庭审案件数量的变化趋势，指标值越大反映主体作为原审被告涉开庭庭审案件数量有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 356 as sid_kw,'last3M_courttrial_trialstatus_2_rate' as feature_cd,'近3个月_开庭庭审_诉讼地位代码_原审被告_占比' as feature_name,'高频' as sub_model_type,'作为原审被告涉案占比(开庭庭审)_近3个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近3个月作为原审被告涉开庭庭审案件数量/近3个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近3个月内主体作为原审被告涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 357 as sid_kw,'last3M_courttrial_trialstatus_2_num' as feature_cd,'近3个月_开庭庭审_诉讼地位代码_原审被告_数量' as feature_name,'高频' as sub_model_type,'开庭庭审量(作为原审被告)_近3个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近3个月作为原审被告涉开庭庭审案件数量统计' as cal_explain,'指标值越大反映近3个月内主体作为原审被告涉开庭庭审案件数量越多' as feature_explain,'件' as unit_origin,'件' as unit_target

union all select 358 as sid_kw,'last6M_courttrial_trialstatus_10_rate' as feature_cd,'近6个月_开庭庭审_诉讼地位代码_当事人_占比' as feature_name,'高频' as sub_model_type,'作为当事人涉案占比(开庭庭审)_近6个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近6个月作为当事人涉开庭庭审案件数量/近6个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近6个月内主体作为当事人涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 359 as sid_kw,'last12M_courttrial_trialstatus_2_rate' as feature_cd,'近12个月_开庭庭审_诉讼地位代码_原审被告_占比' as feature_name,'高频' as sub_model_type,'作为原审被告涉案占比(开庭庭审)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月作为原审被告涉开庭庭审案件数量/近12个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近12个月内主体作为原审被告涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 360 as sid_kw,'last12M_courttrial_trialstatus_5_rate' as feature_cd,'近12个月_开庭庭审_诉讼地位代码_上诉人_占比' as feature_name,'高频' as sub_model_type,'作为上诉人涉案占比(开庭庭审)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月作为上诉人涉开庭庭审案件数量/近12个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近12个月内主体作为上诉人涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 361 as sid_kw,'last12M_courttrial_trialstatus_10_rate' as feature_cd,'近12个月_开庭庭审_诉讼地位代码_当事人_占比' as feature_name,'高频' as sub_model_type,'作为当事人涉案占比(开庭庭审)_近12个月' as feature_name_target,'舆情' as dimension,'司法诉讼' as type,'近12个月作为当事人涉开庭庭审案件数量/近12个月涉及的所有开庭庭审案件数量' as cal_explain,'指标值越大反映近12个月内主体作为当事人涉开庭庭审案件比重越大' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 362 as sid_kw,'last3Mto12M_honesty_penaltystatus_2_num' as feature_cd,'近3个月比近12个月_诚信_处罚实施状态_实际处罚_数量' as feature_name,'高频' as sub_model_type,'诚信处罚增长率(已被实际处罚)_近3个月相对12个月' as feature_name_target,'舆情' as dimension,'诚信处罚' as type,'[(近3个月受实际处罚诚信案件数量/3)-(近12个月受实际处罚诚信案件数量/12)]/(近12个月受实际处罚诚信案件数量/12)' as cal_explain,'反映主体受实际处罚的诚信案件数量变化趋势，指标值越大反映主体受实际处罚的诚信案件数量有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 363 as sid_kw,'last6Mto12M_honesty_secclass_22000078_rate' as feature_cd,'近6个月比近12个月_诚信_二级分类_纳入被执行人_占比' as feature_name,'高频' as sub_model_type,'纳入被执行人处罚占比的增长率_近6个月相对12个月' as feature_name_target,'舆情' as dimension,'诚信处罚' as type,'[(近6个月纳入被执行人诚信案件数量/近6个月涉及的所有诚信案件数量)-(近12个月纳入被执行人诚信案件数量/近12个月涉及的所有诚信案件数量)]/(近12个月纳入被执行人诚信案件数量/近12个月涉及的所有诚信案件数量)' as cal_explain,'反映主体涉纳入被执行人诚信案件占比的变化趋势，指标值越大反映主体纳入被执行人诚信案件比重有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 364 as sid_kw,'last6M_honesty_num' as feature_cd,'近6个月_诚信_数量' as feature_name,'高频' as sub_model_type,'诚信处罚量_近6个月' as feature_name_target,'舆情' as dimension,'诚信处罚' as type,'近6个月涉及的所有诚信案件数量' as cal_explain,'值越大反映主体近6个月内新增诚信案件数量越多' as feature_explain,'件' as unit_origin,'件' as unit_target

union all select 365 as sid_kw,'last1Mto6M_cbvaluation_basis_min' as feature_cd,'近1个月比近6个月_中债估值_估价基点价值_最小值' as feature_name,'高频' as sub_model_type,'估价基点价值增长率(最小值对比)_近1个月相对6个月' as feature_name_target,'市场' as dimension,'估值变动' as type,'(近1个月发行人旗下债券估价基点价值最小值-近6个月发行人旗下债券估价基点价值最小值)/近6个月发行人旗下债券估价基点价值最小值' as cal_explain,'反映债券价格对于利率变动敏感度的变化，体现利率风险的变化趋势，指标值为正且越大反映主体旗下债券价格对于利率变动敏感度有增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 366 as sid_kw,'last1Mto6M_cbvaluation_yield_min' as feature_cd,'近1个月比近6个月_中债估值_估价收益率_最小值' as feature_name,'高频' as sub_model_type,'估价收益率增长率(最小值对比)_近1个月相对6个月' as feature_name_target,'市场' as dimension,'估值变动' as type,'(近1个月发行人旗下债券估价收益率最小值-近6个月发行人旗下债券估价收益率最小值)/近6个月发行人旗下债券估价收益率最小值' as cal_explain,'反映债券收益率波动趋势，指标值为正且越大反映主体旗下债券估价收益率呈增长趋势' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 367 as sid_kw,'factor_514' as feature_cd,'一般公共预算收入（全口径）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'全市口径的一般公共预算收入当年预算执行值，包括税收、非税收' as cal_explain,'一般公共预算收入越高，地方政府稳定的财政收入水平越高，政府稳定的偿债来源越高。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 368 as sid_kw,'factor_520' as feature_cd,'财政自给率（全口径）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'一般公共预算收入（全口径）/一般公共预算支出（全口径）' as cal_explain,'自给率越高，政府财政平衡情况越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 369 as sid_kw,'zs_factor_16' as feature_cd,'调整后所有者权益' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'所有者权益-其他权益工具' as cal_explain,'调整后的所有者权益越大，企业可以承担的风险越大，企业信用风险越低。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 370 as sid_kw,'factor_527' as feature_cd,'GDP' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'地区生产总值' as cal_explain,'指标反映了当地经济发展规模，经济规模越大，地方政府偿债基础越高。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 371 as sid_kw,'Finance_527' as feature_cd,'融资成本' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'融资能力' as type,'2*分配股利、利润或偿付利息支付的现金/((本期期末短期借款+本期期末应付票据+本期期末一年内到期的非流动负债+本期期末其他流动负债+本期期末应付短期债券+本期期末长期借款+本期期末应付债券+本期期末长期应付款+本期期末其他权益工具+本期期末租赁负债)+(上期期末短期借款+上期期末应付票据+上期期末一年内到期的非流动负债+上期期末其他流动负债+上期期末应付短期债券+上期期末长期借款+上期期末应付债券+上期期末长期应付款+上期期末其他权益工具+上期期末租赁负债))' as cal_explain,'融资成本越高，企业在市场上的融资能力越差，市场对其资质认可也较差' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 372 as sid_kw,'zs_factor_38' as feature_cd,'应收款项占比' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'（应收账款+其他应收款+存货）/总资产' as cal_explain,'城投企业在参与政府建设过程中的建设款是计入应收账款。应收账款越高表明地方政府对平台的资金占用程度越高，企业的资产质量越差，信用风险越高。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 373 as sid_kw,'factor_516' as feature_cd,'地方政府可支配财力（全口径）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'一般公共预算收入（全口径）+转移支付（全口径）+政府性基金收入（全口径）' as cal_explain,'地方全口径的可支配财力越高，地方政府财政实力越强，偿债能力越高。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 374 as sid_kw,'factor_526' as feature_cd,'人均GDP' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'人均GDP' as cal_explain,'指标反映当地经济发展质量。经济发展质量越高，地方政府偿债基础越高。' as feature_explain,'元' as unit_origin,'元' as unit_target

union all select 375 as sid_kw,'factor_528' as feature_cd,'第二、三产业产值占比' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'（第二产业+第三产业）/地区生产总值' as cal_explain,'第二、三产业对税收的贡献较为明显，指标越高，政府税收保障越高。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 376 as sid_kw,'factor_907' as feature_cd,'政府性基金预算收入（全口径）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'政府性基金预算收入（全口径）' as cal_explain,'政府性基金收入（全口径）越大，平台的风险相对较低' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 377 as sid_kw,'Size17' as feature_cd,'其他收益' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'政府支持' as type,'' as cal_explain,'其他收益越大，表明地方政府对该平台的支持力度越大。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 378 as sid_kw,'factor_522' as feature_cd,'行政级别' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'地方政府行政级别' as cal_explain,'行政级别影响区域地位、内部资源禀赋以及可获取的外部支持、财政自由度。行政级别越高，违约的可能性更小。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 379 as sid_kw,'factor_533' as feature_cd,'常住人口' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'常住人口' as cal_explain,'人口越多，经济发展潜在规模越大' as feature_explain,'万人' as unit_origin,'万人' as unit_target

union all select 380 as sid_kw,'factor_771' as feature_cd,'所在省金融实力' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'所在省金融实力' as cal_explain,'城投公司所在省份的银行总规模占全国银行总规模之比，占比越大，政府可用的金融资源越多，政府腾挪空间更大' as feature_explain,'' as unit_origin,'' as unit_target

union all select 381 as sid_kw,'zs_factor_12' as feature_cd,'吸收投资现金占比' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'政府支持' as type,'吸收投资收到的货币资金/平台有息债务' as cal_explain,'融资平台中吸收投资收到现金占有息债务比越高，平台盈利能力越强' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 382 as sid_kw,'factor_518' as feature_cd,'税收占一般公共预算收入之比（全口径）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'税收收入（全口径）/一般公共预算收入（全口径）' as cal_explain,'税收占比越高，财政收入越稳定。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 383 as sid_kw,'factor_521' as feature_cd,'财政自给率（本级）' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'地方财政' as type,'一般公共预算收入（本级）/一般公共预算支出（本级）' as cal_explain,'自给率越高，本级政府的财政平衡情况越好' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 384 as sid_kw,'factor_888' as feature_cd,'地方政府平台债务率' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域债务' as type,'地方政府有息债/一般公共预算收入（本级）' as cal_explain,'地方政府平台债务率越高，政府债务程度越高，地方政府信用风险越高。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 385 as sid_kw,'zs_factor_09' as feature_cd,'资本公积与股本变动' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'政府支持' as type,'资本公积+股本（或者实收资本）-上期资本公积-上期股本（或者上期实收资本）' as cal_explain,'资本公积和股本变动情况能够反映地方政府的支持力度。增加额越高，表明地方政府支持力度大。' as feature_explain,'元' as unit_origin,'亿元' as unit_target

union all select 386 as sid_kw,'zs_factor_23' as feature_cd,'现金/短期债务' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'货币资金/(短期借款+应付票据+一年内到期非流动负债）' as cal_explain,'比例越高说明企业的资金利用率越低，盈利能力越差' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 387 as sid_kw,'zs_factor_30' as feature_cd,'短期债务利息支付覆盖率' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（货币资金+交易性金融资产）/(短期债务2+分配股利利润或偿付利息支付的货币资金）' as cal_explain,'短期债务利息支付覆盖率越高，企业短期偿债能力越强。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 388 as sid_kw,'factor_001' as feature_cd,'股权结构' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'政府支持' as type,'实际控制人的性质及控股比例' as cal_explain,'拥有国家或地方政府背景的企业，更容易获得各种优惠政策，同时业务的运营发展也会受益于政府支持，在面临经济的不确定因素时，更容易得到政府救助。且通常持股比例越大，其救助意愿更强。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 389 as sid_kw,'factor_181' as feature_cd,'常住人口增长率' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'参照上年常住人口数以及本年常住人口数数计算常住人口增长率' as cal_explain,'常住人口增长率较大表明人口增长或者流入的规模越大，未来经济发展的规模越大。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 390 as sid_kw,'factor_512' as feature_cd,'平台重要性' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'政府支持' as type,'该平台在当地政府所辖平台中的重要程度' as cal_explain,'平台重要性程度越高，获得政府救助的可能性以及政府救助的力度越大，企业的信用资质越好。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 391 as sid_kw,'factor_524' as feature_cd,'区域金融中心' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'经营' as dimension,'区域经济' as type,'是否为区域性经济金融中心' as cal_explain,'区域金融中心政府的金融资源相对较多，政府腾挪空间更大。' as feature_explain,'' as unit_origin,'' as unit_target

union all select 392 as sid_kw,'zs_factor_35' as feature_cd,'累计经营净现金流利息覆盖率' as feature_name,'低频-城投' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'（经营活动产生的货币资金流量净额+上期经营活动产生的货币资金流量净额+上上期经营活动产生的货币资金流量净额）/(分配股利利润或偿付利息支付的货币资金+上期分配股利利润或偿付利息支付的货币资金+上上期分配股利利润或偿付利息支付的货币资金)' as cal_explain,'累计经营净现金流利息覆盖率越高，企业的现金流越充足，企业的信用风险相对较低。' as feature_explain,'数值' as unit_origin,'%' as unit_target

union all select 393 as sid_kw,'indus_rela_yoy12_total_profit_yoy' as feature_cd,'行业相对_1年同比增长利润总额同比增长率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 394 as sid_kw,'indus_rela_yoy12_dwi_toall_invest_capital' as feature_cd,'行业相对_1年同比增长带息债务／全部投入资本' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 395 as sid_kw,'indus_rela_yoy12_annual_tot_ass_reward_rate' as feature_cd,'行业相对_1年同比增长年化总资产报酬率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 396 as sid_kw,'indus_rela_yoy12_free_cf_ps' as feature_cd,'行业相对_1年同比增长每股股东自由现金流量' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 397 as sid_kw,'indus_rela_yoy12_ncf_from_oa_to_revenue' as feature_cd,'行业相对_1年同比增长经营活动产生的现金流量净额／营业收入' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 398 as sid_kw,'indus_rela_yoy12_revenue_yoy' as feature_cd,'行业相对_1年同比增长营业总收入同比增长率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 399 as sid_kw,'indus_rela_yoy12_sales_dur_fee_rate' as feature_cd,'行业相对_1年同比增长销售期间费用率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 400 as sid_kw,'indus_rela_yoy13_ar_and_br_turnover' as feature_cd,'行业相对_2年同比增长应收账款及应收票据周转率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 401 as sid_kw,'indus_rela_yoy13_op_to_revenue' as feature_cd,'行业相对_2年同比增长营业利润／营业总收入' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 402 as sid_kw,'indus_rela_yoy13_gross_selling_rate' as feature_cd,'行业相对_2年同比增长销售毛利率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 403 as sid_kw,'indus_rela_yoy13_lt_debt_to_oc' as feature_cd,'行业相对_2年同比增长长期债务与营运资金比率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 404 as sid_kw,'indus_rela_avg_roe' as feature_cd,'行业相对_平均净资产收益率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 405 as sid_kw,'indus_rela_act_receivable_turnover' as feature_cd,'行业相对_应收账款周转率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'' as cal_explain,'' as feature_explain,'次' as unit_origin,'' as unit_target

union all select 406 as sid_kw,'indus_rela_net_profit_atsopc_yoy' as feature_cd,'行业相对_归属母公司股东的净利润同比增长率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 407 as sid_kw,'indus_rela_ta_to_net_debt' as feature_cd,'行业相对_有形资产／净债务' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'' as cal_explain,'' as feature_explain,'数值' as unit_origin,'' as unit_target

union all select 408 as sid_kw,'indus_rela_np_per_share' as feature_cd,'行业相对_每股净资产' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'' as cal_explain,'' as feature_explain,'元' as unit_origin,'' as unit_target

union all select 409 as sid_kw,'indus_rela_cash_cycle' as feature_cd,'行业相对_现金循环周期' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'' as cal_explain,'' as feature_explain,'天' as unit_origin,'' as unit_target

union all select 410 as sid_kw,'indus_rela_holder_equity' as feature_cd,'行业相对_股东权益比率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 411 as sid_kw,'indus_rela_op_to_revenue' as feature_cd,'行业相对_营业利润／营业总收入' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 412 as sid_kw,'indus_rela_revenue_yoy' as feature_cd,'行业相对_营业总收入同比增长率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 413 as sid_kw,'indus_rela_operate_income_growth' as feature_cd,'行业相对_营业收入增长率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'规模水平' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 414 as sid_kw,'indus_rela_operating_capital_turnover' as feature_cd,'行业相对_营运资金周转率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'营运能力' as type,'' as cal_explain,'' as feature_explain,'次' as unit_origin,'' as unit_target

union all select 415 as sid_kw,'indus_rela_finance_cost_tot_revenue' as feature_cd,'行业相对_财务费用／营业总收入' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'盈利能力' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target

union all select 416 as sid_kw,'yoy12_asset_liab_ratio' as feature_cd,'1年同比增长资产负债率' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'资本结构' as type,'' as cal_explain,'' as feature_explain,'' as unit_origin,'' as unit_target

union all select 417 as sid_kw,'dwi_toall_invest_capital' as feature_cd,'带息债务／全部投入资本' as feature_name,'中频-产业' as sub_model_type,'' as feature_name_target,'财务' as dimension,'偿债能力' as type,'' as cal_explain,'' as feature_explain,'%' as unit_origin,'' as unit_target
