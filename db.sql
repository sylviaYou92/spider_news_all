-- mysql>source db.sql
-- DROP语句慎用，第一次创建数据库的时候请取消注释
DROP DATABASE IF EXISTS news;
CREATE DATABASE news;
USE news;
CREATE TABLE news_all(
	linkmd5id VARCHAR(255) NOT NULL PRIMARY KEY,
	day TIMESTAMP NOT NULL,
	title VARCHAR(255) NOT NULL,
	site VARCHAR(255) NOT NULL,
	keywords VARCHAR(255),
	type VARCHAR(255),
	url VARCHAR(255) NOT NULL,
	article TEXT,
	markdown TEXT
) DEFAULT CHARSET=UTF8;

CREATE TABLE url_record(
	start_url VARCHAR(255),
	latest_url VARCHAR(255),
	site_name VARCHAR(255)
) DEFAULT CHARSET=UTF8;
