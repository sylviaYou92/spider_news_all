# -*- coding: utf-8 -*-

# Scrapy settings for spider_news_all project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'spider_news_all'
COMMANDS_MODULE = 'spider_news_all.commands'
SPIDER_MODULES = ['spider_news_all.spiders']
NEWSPIDER_MODULE = 'spider_news_all.spiders'
ITEM_PIPELINES = {
    'spider_news_all.pipelines.SpiderNewsAllPipeline':10,
}

LOG_LEVEL = 'INFO'

# start MySQL database configure setting
MYSQL_HOST = 'localhost'
MYSQL_DBNAME = 'news'
MYSQL_USER = 'root'
MYSQL_PASSWD = '1234'
MYSQL_PORT = 3306
# end of MySQL database configure setting

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:24.0) Gecko/20100101 Firefox/24.0"
CONCURRENT_REQUESTS_PER_DOMAIN = 4
COOKIES_ENABLED = False
