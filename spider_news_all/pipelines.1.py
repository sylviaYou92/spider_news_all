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



class SpiderNewsAllPipeline(object):

    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    SELECT_LAST_ID = ("select last_insert_id()")
    INSERT_TYPE_ID = ("INSERT INTO dede_arctype (typename) VALUES (%s)")
    INSERT_ARCTINY = ("INSERT INTO dede_acrtiny (typeid,typeid2) VALUES (%s,%s)")
    INSERT_ADDONARTICLE = ("INSERT INTO dede_addonarticle (aid,typeid,body,redirecturl) VALUES (%s,%s,%s,%s)")
    INSERT_ARCHIVES = ("INSERT INTO dede_archives (id, typeid, typeid2, title, source, pubdate, keywords) VALUES (%s,%s,%s,%s,%s,%s,%s)")

    def insert(self, title, day, _type, url, keywords, article, site, markdown):
        self.lock.acquire()
        
        linkmd5id = self._get_linkmd5id(url)
        self.cursor.execute("select * from news_record where linkmd5id = %s", (linkmd5id, ))
        ret = self.cursor.fetchone()

        if ret:
            pass
        else:
            # if type exists
            self.cursor.execute("select id from dede_arctype where typename = %s",(_type,))
            typeid = self.cursor.fetchone()
            if typeid:
                pass
            else:
                self.cursor.execute(self.INSERT_TYPE_ID, _type)
                self.cursor.execute("SELECT_LAST_ID")
                typeid = self.cursor.fetchone()
            
            self.cursor.fetchone("select id from dede_arctype where typename = %s",(site,))
            typeid2 = self.cursor.fetchone()
            if typeid2:
                pass
            else:
                self.cursor.execute(self.INSERT_TYPE_ID, site)
                self.cursor.execute("SELECT_LAST_ID")
                typeid2 = self.cursor.fetchone()
            
            self.cursor.execute(self.INSERT_ARCTINY, (typeid,typeid2))
            self.cursor.execute("SELECT_LAST_ID")
            articleid = self.cursor.fetchone()

            
            self.cursor.execute(self.INSERT_ADDONARTICLE, (articleid, typeid, markdown, url))

            self.cursor.execute(self.INSERT_ARCHIVES, (articleid, typeid, typeid2, title, url, day, keywords) VALUES))

            

            """try:
                self.cursor.execute(self.INSERT_NEWS_ALL, news)
                log.msg(title + " saved successfully", level=log.INFO)
            except:
                log.msg("MySQL exception !!!", level=log.ERROR)
            self.lock.release()"""

    def process_item(self, item, spider):
        title = item['title']
        day = item['day']
        _type = item['_type']
        url = item['url']
        keywords = item['keywords']
        article = item['article']
        site = item['site']
        markdown = item['markdown']
        self.insert(title, day, _type, url, keywords, article, site, markdown)
        return item

    def _get_linkmd5id(self, url):
        return md5(url).hexdigest()