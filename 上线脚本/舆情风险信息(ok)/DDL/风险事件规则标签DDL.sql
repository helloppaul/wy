-- drop table if exists pth_rmp.rmp_opinion_risk_info_tag;
create table pth_rmp.rmp_opinion_risk_info_tag as 

select 1 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001001' as tag_ii_cd,'债券违约' as tag_ii,-3 as importance,0 as tag_type

union all select 2 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001002' as tag_ii_cd,'评级下调' as tag_ii,-3 as importance,0 as tag_type

union all select 3 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001004' as tag_ii_cd,'承诺失信' as tag_ii,-3 as importance,0 as tag_type

union all select 4 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001006' as tag_ii_cd,'欠账赖账、逃废债务' as tag_ii,-3 as importance,0 as tag_type

union all select 5 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001007' as tag_ii_cd,'债务逾期' as tag_ii,-3 as importance,0 as tag_type

union all select 6 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001014' as tag_ii_cd,'兑付不确定' as tag_ii,-3 as importance,0 as tag_type

union all select 7 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001015' as tag_ii_cd,'债务展期' as tag_ii,-3 as importance,0 as tag_type

union all select 8 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001018' as tag_ii_cd,'评级关注' as tag_ii,-3 as importance,0 as tag_type

union all select 9 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001019' as tag_ii_cd,'评级列入观察名单' as tag_ii,-3 as importance,0 as tag_type

union all select 10 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001020' as tag_ii_cd,'评级展望负面' as tag_ii,-3 as importance,0 as tag_type

union all select 11 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001021' as tag_ii_cd,'推迟评级' as tag_ii,-3 as importance,0 as tag_type

union all select 12 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001024' as tag_ii_cd,'被执行人' as tag_ii,-2 as importance,0 as tag_type

union all select 13 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001025' as tag_ii_cd,'失信被执行人' as tag_ii,-3 as importance,0 as tag_type

union all select 14 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001027' as tag_ii_cd,'终止评级' as tag_ii,-3 as importance,0 as tag_type

union all select 15 as sid_kw,'6001000' as tag_cd,'信用预警' as tag,'6001028' as tag_ii_cd,'撤销评级' as tag_ii,-3 as importance,0 as tag_type

union all select 16 as sid_kw,'6003000' as tag_cd,'经营预警' as tag,'6003001' as tag_ii_cd,'资产重组' as tag_ii,-2 as importance,0 as tag_type

union all select 17 as sid_kw,'6003000' as tag_cd,'经营预警' as tag,'6003004' as tag_ii_cd,'资产查封/扣押/冻结' as tag_ii,-3 as importance,0 as tag_type

union all select 18 as sid_kw,'6003000' as tag_cd,'经营预警' as tag,'6003006' as tag_ii_cd,'破产清算、解散' as tag_ii,-3 as importance,0 as tag_type

union all select 19 as sid_kw,'6003000' as tag_cd,'经营预警' as tag,'6003024' as tag_ii_cd,'债务重组' as tag_ii,-3 as importance,0 as tag_type

union all select 20 as sid_kw,'6003000' as tag_cd,'经营预警' as tag,'6003065' as tag_ii_cd,'破产重整' as tag_ii,-3 as importance,0 as tag_type

union all select 21 as sid_kw,'6007000' as tag_cd,'担保预警' as tag,'6007004' as tag_ii_cd,'违规担保' as tag_ii,-2 as importance,0 as tag_type

union all select 22 as sid_kw,'6008000' as tag_cd,'监管预警' as tag,'6008002' as tag_ii_cd,'监管措施' as tag_ii,-2 as importance,0 as tag_type

union all select 23 as sid_kw,'6008000' as tag_cd,'监管预警' as tag,'6008003' as tag_ii_cd,'行政处罚' as tag_ii,-2 as importance,0 as tag_type

union all select 24 as sid_kw,'6008000' as tag_cd,'监管预警' as tag,'6008007' as tag_ii_cd,'立案调查' as tag_ii,-3 as importance,0 as tag_type

union all select 25 as sid_kw,'6008000' as tag_cd,'监管预警' as tag,'6008009' as tag_ii_cd,'被监管降级' as tag_ii,-3 as importance,0 as tag_type

union all select 26 as sid_kw,'6009000' as tag_cd,'市场预警' as tag,'6009001' as tag_ii_cd,'暂停上市' as tag_ii,-3 as importance,0 as tag_type

union all select 27 as sid_kw,'6009000' as tag_cd,'市场预警' as tag,'6009005' as tag_ii_cd,'停牌' as tag_ii,-2 as importance,0 as tag_type

union all select 28 as sid_kw,'6009000' as tag_cd,'市场预警' as tag,'6009006' as tag_ii_cd,'终止上市' as tag_ii,-3 as importance,0 as tag_type

union all select 29 as sid_kw,'JU001000' as tag_cd,'被执行' as tag,'JU001001' as tag_ii_cd,'被列入被执行人' as tag_ii,-2 as importance,2 as tag_type

union all select 30 as sid_kw,'JU002000' as tag_cd,'失信被执行' as tag,'JU002001' as tag_ii_cd,'被列入失信被执行人' as tag_ii,-3 as importance,2 as tag_type

union all select 31 as sid_kw,'JU003000' as tag_cd,'股权冻结' as tag,'JU003001' as tag_ii_cd,'对外持股份被冻结' as tag_ii,-3 as importance,2 as tag_type

union all select 32 as sid_kw,'JU004000' as tag_cd,'借贷纠纷' as tag,'JU004001' as tag_ii_cd,'作为被告涉借贷纠纷(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 33 as sid_kw,'JU005000' as tag_cd,'房地产类合同纠纷' as tag,'JU005001' as tag_ii_cd,'作为被告涉房地产建筑类合同纠纷(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 34 as sid_kw,'JU006000' as tag_cd,'担保物权纠纷' as tag,'JU006001' as tag_ii_cd,'作为被告涉担保物权纠纷案件(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 35 as sid_kw,'JU007000' as tag_cd,'票据纠纷' as tag,'JU007001' as tag_ii_cd,'作为被告涉票据纠纷(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 36 as sid_kw,'JU008000' as tag_cd,'破产纠纷' as tag,'JU008001' as tag_ii_cd,'作为被告涉破产相关纠纷(裁判文书)' as tag_ii,-3 as importance,2 as tag_type

union all select 37 as sid_kw,'JU009000' as tag_cd,'申请保全纠纷' as tag,'JU009001' as tag_ii_cd,'作为被告涉申请保全相关纠纷(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 38 as sid_kw,'JU010000' as tag_cd,'金融类纠纷' as tag,'JU010001' as tag_ii_cd,'作为被告涉金融类纠纷(裁判文书)' as tag_ii,-2 as importance,2 as tag_type

union all select 39 as sid_kw,'JU011000' as tag_cd,'其他合同纠纷' as tag,'JU011001' as tag_ii_cd,'作为被告涉其他合同纠纷(裁判文书)' as tag_ii,-1 as importance,2 as tag_type

union all select 40 as sid_kw,'JU012000' as tag_cd,'其他纠纷' as tag,'JU012001' as tag_ii_cd,'作为被告涉其他纠纷(裁判文书)' as tag_ii,-1 as importance,2 as tag_type

union all select 41 as sid_kw,'JU004000' as tag_cd,'借贷纠纷' as tag,'JU004002' as tag_ii_cd,'作为被告涉借贷纠纷(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 42 as sid_kw,'JU005000' as tag_cd,'房地产类合同纠纷' as tag,'JU005002' as tag_ii_cd,'作为被告涉房地产建筑类合同纠纷(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 43 as sid_kw,'JU006000' as tag_cd,'担保物权纠纷' as tag,'JU006002' as tag_ii_cd,'作为被告涉担保物权纠纷案件(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 44 as sid_kw,'JU007000' as tag_cd,'票据纠纷' as tag,'JU007002' as tag_ii_cd,'作为被告涉票据纠纷(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 45 as sid_kw,'JU008000' as tag_cd,'破产纠纷' as tag,'JU008002' as tag_ii_cd,'作为被告涉破产相关纠纷(开庭公告)' as tag_ii,-3 as importance,2 as tag_type

union all select 46 as sid_kw,'JU009000' as tag_cd,'申请保全纠纷' as tag,'JU009002' as tag_ii_cd,'作为被告涉申请保全相关纠纷(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 47 as sid_kw,'JU010000' as tag_cd,'金融类纠纷' as tag,'JU010002' as tag_ii_cd,'作为被告涉金融类纠纷(开庭公告)' as tag_ii,-2 as importance,2 as tag_type

union all select 48 as sid_kw,'JU011000' as tag_cd,'其他合同纠纷' as tag,'JU011002' as tag_ii_cd,'作为被告涉其他合同纠纷(开庭公告)' as tag_ii,-1 as importance,2 as tag_type

union all select 49 as sid_kw,'JU012000' as tag_cd,'其他纠纷' as tag,'JU012002' as tag_ii_cd,'作为被告涉其他纠纷(开庭公告)' as tag_ii,-1 as importance,1 as tag_type

union all select 50 as sid_kw,'JU013000' as tag_cd,'限制消费' as tag,'JU013001' as tag_ii_cd,'被限制高消费' as tag_ii,-3 as importance,1 as tag_type

union all select 51 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001001' as tag_ii_cd,'被监管警示(证监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 52 as sid_kw,'HN002000' as tag_cd,'交易所处罚' as tag,'HN002001' as tag_ii_cd,'被监管警示(交易所)' as tag_ii,-2 as importance,1 as tag_type

union all select 53 as sid_kw,'HN003000' as tag_cd,'全国股转系统处罚' as tag,'HN003001' as tag_ii_cd,'被监管警示(全国股转系统)' as tag_ii,-2 as importance,1 as tag_type

union all select 54 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007001' as tag_ii_cd,'被监管警示(其他机构)' as tag_ii,-1 as importance,1 as tag_type

union all select 55 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001002' as tag_ii_cd,'被立案调查(证监会)' as tag_ii,-3 as importance,1 as tag_type

union all select 56 as sid_kw,'HN002000' as tag_cd,'交易所处罚' as tag,'HN002002' as tag_ii_cd,'被立案调查(交易所)' as tag_ii,-3 as importance,1 as tag_type

union all select 57 as sid_kw,'HN004000' as tag_cd,'交易商协会处罚' as tag,'HN004001' as tag_ii_cd,'被立案调查(交易商协会)' as tag_ii,-3 as importance,1 as tag_type

union all select 58 as sid_kw,'HN005000' as tag_cd,'银保监会处罚' as tag,'HN005001' as tag_ii_cd,'被立案调查(银保监会)' as tag_ii,-3 as importance,1 as tag_type

union all select 59 as sid_kw,'HN006000' as tag_cd,'公安及检察机关处罚' as tag,'HN006001' as tag_ii_cd,'被立案调查(公安及检察机关)' as tag_ii,-3 as importance,1 as tag_type

union all select 60 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007002' as tag_ii_cd,'被立案调查(其他机构)' as tag_ii,-2 as importance,1 as tag_type

union all select 61 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001003' as tag_ii_cd,'被市场禁入(证监会)' as tag_ii,-3 as importance,1 as tag_type

union all select 62 as sid_kw,'HN006000' as tag_cd,'公安及检察机关处罚' as tag,'HN006002' as tag_ii_cd,'被采取强制措施或逮捕(公安及检察机关)' as tag_ii,-2 as importance,1 as tag_type

union all select 63 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007003' as tag_ii_cd,'被采取强制措施或逮捕(其他机构)' as tag_ii,-1 as importance,1 as tag_type

union all select 64 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001004' as tag_ii_cd,'被采取监管措施(证监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 65 as sid_kw,'HN005000' as tag_cd,'银保监会处罚' as tag,'HN005002' as tag_ii_cd,'被采取监管措施(银保监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 66 as sid_kw,'HN002000' as tag_cd,'交易所处罚' as tag,'HN002003' as tag_ii_cd,'被采取监管措施(交易所)' as tag_ii,-2 as importance,1 as tag_type

union all select 67 as sid_kw,'HN004000' as tag_cd,'交易商协会处罚' as tag,'HN004002' as tag_ii_cd,'被采取监管措施(交易商协会)' as tag_ii,-2 as importance,1 as tag_type

union all select 68 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007004' as tag_ii_cd,'被采取监管措施(其他机构)' as tag_ii,-1 as importance,1 as tag_type

union all select 69 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001005' as tag_ii_cd,'被公开谴责(证监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 70 as sid_kw,'HN005000' as tag_cd,'银保监会处罚' as tag,'HN005003' as tag_ii_cd,'被公开谴责(银保监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 71 as sid_kw,'HN002000' as tag_cd,'交易所处罚' as tag,'HN002004' as tag_ii_cd,'被公开谴责(交易所)' as tag_ii,-2 as importance,1 as tag_type

union all select 72 as sid_kw,'HN004000' as tag_cd,'交易商协会处罚' as tag,'HN004003' as tag_ii_cd,'被公开谴责(交易商协会)' as tag_ii,-2 as importance,1 as tag_type

union all select 73 as sid_kw,'HN003000' as tag_cd,'全国股转系统处罚' as tag,'HN003002' as tag_ii_cd,'被公开谴责(全国股转系统)' as tag_ii,-2 as importance,1 as tag_type

union all select 74 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007005' as tag_ii_cd,'被公开谴责(其他机构)' as tag_ii,-1 as importance,1 as tag_type

union all select 75 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001006' as tag_ii_cd,'被取消资格(证监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 76 as sid_kw,'HN003000' as tag_cd,'全国股转系统处罚' as tag,'HN003003' as tag_ii_cd,'被取消资格(全国股转系统)' as tag_ii,-2 as importance,1 as tag_type

union all select 77 as sid_kw,'HN004000' as tag_cd,'交易商协会处罚' as tag,'HN004004' as tag_ii_cd,'被取消资格(交易商协会)' as tag_ii,-3 as importance,1 as tag_type

union all select 78 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007006' as tag_ii_cd,'被取消资格(其他机构)' as tag_ii,-1 as importance,1 as tag_type

union all select 79 as sid_kw,'HN001000' as tag_cd,'证监会处罚' as tag,'HN001007' as tag_ii_cd,'被监管关注(证监会)' as tag_ii,-2 as importance,1 as tag_type

union all select 80 as sid_kw,'HN002000' as tag_cd,'交易所处罚' as tag,'HN002005' as tag_ii_cd,'被监管关注(交易所)' as tag_ii,-2 as importance,1 as tag_type

union all select 81 as sid_kw,'HN003000' as tag_cd,'全国股转系统处罚' as tag,'HN003004' as tag_ii_cd,'被监管关注(全国股转系统)' as tag_ii,-2 as importance,1 as tag_type

union all select 82 as sid_kw,'HN007000' as tag_cd,'其他机构处罚' as tag,'HN007007' as tag_ii_cd,'被监管关注(其他机构)' as tag_ii,-1 as importance,1 as tag_type
