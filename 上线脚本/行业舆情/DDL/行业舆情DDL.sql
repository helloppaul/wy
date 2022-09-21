-- ÐÐÒµÓßÇé RMP_INDUSTRY_NEWS_INFO --
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
	delete_flag	tinyint,
	create_by	string,
	create_time	TIMESTAMP,
	update_by	string,
	update_time	TIMESTAMP,
	version	tinyint
)
partitioned by (dt int)
stored as Parquet;