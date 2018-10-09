# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import threading

import MySQLdb
from scrapy import log
from config import SpiderNewsAllConfig
from hashlib import md5
import time

class SpiderNewsAllPipeline(object):
    
    INSERT_NEWS_ALL = ("INSERT INTO news_all (linkmd5id,title, day, type1, type2, type3, url, keywords, article, site, markdown) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    def insert(self, title, day, type1, type2, type3, url, keywords, article, site, markdown):
        self.lock.acquire()
        
        linkmd5id = self._get_linkmd5id(url)
        news = (linkmd5id, title, day, type1, type2, type3, url, keywords, article, site, markdown)
        self.cursor.execute("select * from news_all where linkmd5id = %s", (linkmd5id, ))
        ret = self.cursor.fetchone()

        if ret:
            pass
        else:
#            self.cursor.execute(self.INSERT_NEWS_ALL, news)
            try:
                self.cursor.execute(self.INSERT_NEWS_ALL, news)
                log.msg(title + " saved successfully", level=log.INFO)
            except:
                log.msg("MySQL exception !!!", level=log.ERROR)
            self.lock.release()

    def process_item(self, item, spider):
        title = item['title']
        day = item['day']
        day = time.localtime(day)
        day = time.strftime("%Y-%m-%d %H:%M:%S", day) 
        type1 = item['type1']
        type2 = item['type2']
        type3 = item['type3']
        url = item['url']
        keywords = item['keywords']
        article = item['article']
        site = item['site']
        markdown = item['markdown']
        self.insert(title, day, type1, type2, type3, url, keywords, article, site, markdown)
        return item

    def _get_linkmd5id(self, url):
        return md5(url).hexdigest()