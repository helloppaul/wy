create table pth_rmp.rmp_opinion_featpct_desc_cfg as 
select 1 as sid_kw,'total_num' as feature_name,'舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'一天内负面新闻数量达到' as index_desc,'条' as index_unit

union all select 2 as sid_kw,'importance_-3_rate' as feature_name,'情感-3舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重负面新闻占比为' as index_desc,'%' as index_unit

union all select 3 as sid_kw,'importance_-2_rate' as feature_name,'情感-2舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'重点负面新闻占比为' as index_desc,'%' as index_unit

union all select 4 as sid_kw,'importance_-1_rate' as feature_name,'情感-1舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'一般负面新闻占比为' as index_desc,'%' as index_unit

union all select 5 as sid_kw,'importance_max_abs' as feature_name,'情感最大值_绝对值' as feature_name_cn,1 as index_min_val,1 as index_max_val,'存在一般负面新闻' as index_desc,'' as index_unit

union all select 6 as sid_kw,'importance_max_abs' as feature_name,'情感最大值_绝对值' as feature_name_cn,2 as index_min_val,2 as index_max_val,'新闻集中于重点负面及以上新闻' as index_desc,'' as index_unit

union all select 7 as sid_kw,'importance_max_abs' as feature_name,'情感最大值_绝对值' as feature_name_cn,3 as index_min_val,3 as index_max_val,'新闻集中于严重负面新闻' as index_desc,'' as index_unit

union all select 8 as sid_kw,'importance_min_abs' as feature_name,'情感最小值_绝对值' as feature_name_cn,1 as index_min_val,1 as index_max_val,'新闻集中于一般负面新闻' as index_desc,'' as index_unit

union all select 9 as sid_kw,'importance_min_abs' as feature_name,'情感最小值_绝对值' as feature_name_cn,2 as index_min_val,2 as index_max_val,'新闻集中于重点负面及以下新闻' as index_desc,'' as index_unit

union all select 10 as sid_kw,'importance_min_abs' as feature_name,'情感最小值_绝对值' as feature_name_cn,3 as index_min_val,3 as index_max_val,'存在严重负面新闻' as index_desc,'' as index_unit

union all select 11 as sid_kw,'importance_med_abs' as feature_name,'情感中位数_绝对值' as feature_name_cn,1 as index_min_val,1 as index_max_val,'新闻集中于一般负面新闻' as index_desc,'' as index_unit

union all select 12 as sid_kw,'importance_med_abs' as feature_name,'情感中位数_绝对值' as feature_name_cn,2 as index_min_val,2 as index_max_val,'新闻集中于重点负面新闻' as index_desc,'' as index_unit

union all select 13 as sid_kw,'importance_med_abs' as feature_name,'情感中位数_绝对值' as feature_name_cn,3 as index_min_val,3 as index_max_val,'新闻集中于严重负面新闻' as index_desc,'' as index_unit

union all select 14 as sid_kw,'importance_avg_abs' as feature_name,'情感均值_绝对值' as feature_name_cn,0 as index_min_val,2 as index_max_val,'新闻集中于一般负面新闻' as index_desc,'' as index_unit

union all select 15 as sid_kw,'importance_avg_abs' as feature_name,'情感均值_绝对值' as feature_name_cn,2 as index_min_val,3 as index_max_val,'新闻集中于重点负面新闻' as index_desc,'' as index_unit

union all select 16 as sid_kw,'importance_avg_abs' as feature_name,'情感均值_绝对值' as feature_name_cn,3 as index_min_val,3.1 as index_max_val,'新闻集中于严重负面新闻' as index_desc,'' as index_unit

union all select 17 as sid_kw,'importance_3to2' as feature_name,'情感-3/-2舆情数量比值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重负面新闻较重点负面新闻的比值为' as index_desc,'p' as index_unit

union all select 18 as sid_kw,'importance_3to1' as feature_name,'情感-3/-1舆情数量比值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重负面新闻较一般负面新闻的比值为' as index_desc,'p' as index_unit

union all select 19 as sid_kw,'importance_2to1' as feature_name,'情感-2/-1舆情数量比值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'重点负面新闻较一般负面新闻的比值为' as index_desc,'p' as index_unit

union all select 20 as sid_kw,'latest17to24_rate' as feature_name,'近17-24小时舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近17-24小时负面新闻占比为' as index_desc,'%' as index_unit

union all select 21 as sid_kw,'latest9to16_rate' as feature_name,'近9-16小时舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近9-16小时负面新闻占比为' as index_desc,'%' as index_unit

union all select 22 as sid_kw,'latest0to8_rate' as feature_name,'近8小时舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近8小时负面新闻占比为' as index_desc,'%' as index_unit

union all select 23 as sid_kw,'latest17to24_num' as feature_name,'近17-24小时舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近17-24小时负面新闻数量为' as index_desc,'条' as index_unit

union all select 24 as sid_kw,'latest9to16_num' as feature_name,'近9-16小时舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近9-16小时负面新闻数量为' as index_desc,'条' as index_unit

union all select 25 as sid_kw,'latest0to8_num' as feature_name,'近8小时舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'近8小时负面新闻数量为' as index_desc,'条' as index_unit

union all select 26 as sid_kw,'importance_-3_num' as feature_name,'情感-3舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重负面新闻数量为' as index_desc,'条' as index_unit

union all select 27 as sid_kw,'importance_-2_num' as feature_name,'情感-2舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'重点负面新闻数量为' as index_desc,'条' as index_unit

union all select 28 as sid_kw,'importance_-1_num' as feature_name,'情感-1舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'一般负面新闻数量为' as index_desc,'条' as index_unit

union all select 29 as sid_kw,'label_risk_middle_num' as feature_name,'中风险标签舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'重点风险标签数量为' as index_desc,'条' as index_unit

union all select 30 as sid_kw,'label_risk_high_num' as feature_name,'高风险标签舆情数量' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重风险标签数量为' as index_desc,'条' as index_unit

union all select 31 as sid_kw,'label_risk_middle_rate' as feature_name,'中风险标签舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'重点风险标签占比为' as index_desc,'%' as index_unit

union all select 32 as sid_kw,'label_risk_high_rate' as feature_name,'高风险标签舆情占比' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'严重风险标签占比为' as index_desc,'%' as index_unit

union all select 33 as sid_kw,'label_nm_level2产品预警_平均_importance_abs' as feature_name,'产品预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于产品预警类事件' as index_desc,'' as index_unit

union all select 34 as sid_kw,'label_nm_level2信用预警_平均_importance_abs' as feature_name,'信用预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于信用预警类事件' as index_desc,'' as index_unit

union all select 35 as sid_kw,'label_nm_level2其他预警_平均_importance_abs' as feature_name,'其他预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于其他预警类事件' as index_desc,'' as index_unit

union all select 36 as sid_kw,'label_nm_level2市场预警_平均_importance_abs' as feature_name,'市场预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于市场预警类事件' as index_desc,'' as index_unit

union all select 37 as sid_kw,'label_nm_level2担保预警_平均_importance_abs' as feature_name,'担保预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于担保预警类事件' as index_desc,'' as index_unit

union all select 38 as sid_kw,'label_nm_level2政府预警_平均_importance_abs' as feature_name,'政府预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于政府预警类事件' as index_desc,'' as index_unit

union all select 39 as sid_kw,'label_nm_level2数据安全预警_平均_importance_abs' as feature_name,'数据安全预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于数据安全预警类事件' as index_desc,'' as index_unit

union all select 40 as sid_kw,'label_nm_level2环保预警_平均_importance_abs' as feature_name,'环保预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于环保预警类事件' as index_desc,'' as index_unit

union all select 41 as sid_kw,'label_nm_level2监管预警_平均_importance_abs' as feature_name,'监管预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于监管预警类事件' as index_desc,'' as index_unit

union all select 42 as sid_kw,'label_nm_level2管理预警_平均_importance_abs' as feature_name,'管理预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于管理预警类事件' as index_desc,'' as index_unit

union all select 43 as sid_kw,'label_nm_level2经营预警_平均_importance_abs' as feature_name,'经营预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于经营预警类事件' as index_desc,'' as index_unit

union all select 44 as sid_kw,'label_nm_level2股票市场_平均_importance_abs' as feature_name,'股票市场标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于股票市场类事件' as index_desc,'' as index_unit

union all select 45 as sid_kw,'label_nm_level2财务预警_平均_importance_abs' as feature_name,'财务预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于财务预警类事件' as index_desc,'' as index_unit

union all select 46 as sid_kw,'label_nm_level2资本运作_平均_importance_abs' as feature_name,'资本运作标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于资本运作类事件' as index_desc,'' as index_unit

union all select 47 as sid_kw,'label_nm_level2银行_平均_importance_abs' as feature_name,'银行标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于银行类事件类事件' as index_desc,'' as index_unit

union all select 48 as sid_kw,'label_nm_level2项目预警_平均_importance_abs' as feature_name,'项目预警标签情感均值_绝对值' as feature_name_cn,-1 as index_min_val,-1 as index_max_val,'新闻主要集中于项目预警类事件' as index_desc,'' as index_unit
