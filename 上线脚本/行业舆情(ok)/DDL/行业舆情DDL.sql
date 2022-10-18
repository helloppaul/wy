-- 行业舆情 RMP_INDUSTRY_NEWS_INFO --
create table pth_rmp.RMP_INDUSTRY_NEWS_INFO 
(
	sid_kw string,
	notice_dt TIMESTAMP,
	news_id string,
	news_title string,
	gb_industry_tag_cd string,
	gb_industry_tag string,
	gb_industry_tag_ii_cd string,
	gb_industry_tag_ii string,
	news_from string,
	news_url string,
	news string,
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)
partitioned by (etl_date int)
row format
delimited fields terminated by '\16' escaped by '\\'
stored as textfile;