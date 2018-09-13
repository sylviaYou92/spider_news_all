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
import sys



class SpiderNewsAllPipeline(object):
    reload(sys)
    sys.setdefaultencoding("utf-8")

    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    
    INSERT_NEWS_RECORD = ("INSERT INTO news_record (linkmd5id) VALUES (%s)")
    INSERT_TYPE_ID = ("INSERT INTO dede_arctype (reid, topid, typename,typedir,tempindex,templist,temparticle,namerule,namerule2,isdefault) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    INSERT_ARCTINY = ("INSERT INTO dede_arctiny (typeid,typeid2,mid) VALUES (%s,%s,%s)")
    INSERT_ADDONARTICLE = ("INSERT INTO dede_addonarticle (aid,typeid,body,redirecturl) VALUES (%s,%s,%s,%s)")
    INSERT_ARCHIVES = ("INSERT INTO dede_archives ( id, typeid, typeid2, sortrank, flag, ismake, channel, arcrank, click, money, title, shorttitle, color, writer, source, litpic, pubdate, senddate, mid, keywords, lastpost, scores, goodpost, badpost, voteid, notpost, description, filename, dutyadmin, tackid, mtype, weight) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

    def insert(self, title, day, type1, type2, type3, url, keywords, article, site, markdown):
        self.lock.acquire()
        
        linkmd5id = self._get_linkmd5id(url)
        self.cursor.execute("select * from news_record where linkmd5id = %s", (linkmd5id, ))
        ret = self.cursor.fetchone()
        
        tempindex = '{style}/index_article.htm'
        templist = '{style}/list_article.htm'
        temparticle = '{style}/article_article.htm'
        namerule = '{typedir}/{Y}/{M}{D}/{aid}.html'
        namerule2 = '{typedir}/list_{tid}_{page}.html'
        isdefault = '-1'

        if ret:
            pass
        else:
            self.cursor.execute(self.INSERT_NEWS_RECORD,(linkmd5id,))
            # type1 validation
            QUERY_TYPE = ("select id from dede_arctype where typename = %s")
            self.cursor.execute(QUERY_TYPE,(type1,))
            type1_id = self.cursor.fetchone()
            if type1_id:
                pass
            else:
                typedir = "{cmspath}/a/"+self._get_linkmd5id(type1)
                self.cursor.execute(self.INSERT_TYPE_ID, ("0","0",type1,typedir,tempindex,templist,temparticle,namerule,namerule2,isdefault))
                self.cursor.execute("select max(id) from dede_arctype")
                type1_id = self.cursor.fetchone()
            # type2 validation
            self.cursor.execute(QUERY_TYPE,(type2,))
            type2_id = self.cursor.fetchone()
            if type2_id:
                pass
            else:
                typedir = "{cmspath}/a/"+self._get_linkmd5id(type1)+"/"+self._get_linkmd5id(type2)
                self.cursor.execute(self.INSERT_TYPE_ID, (type1_id,type1_id,type2,typedir,tempindex,templist,temparticle,namerule,namerule2,isdefault))
                self.cursor.execute("select max(id) from dede_arctype")
                type2_id = self.cursor.fetchone()
            # type3 validation
            self.cursor.execute(QUERY_TYPE,(type3,))
            type3_id = self.cursor.fetchone()
            if type3_id:
                pass
            else:
                typedir = "{cmspath}/a/"+self._get_linkmd5id(type3)
                self.cursor.execute(self.INSERT_TYPE_ID, ("0","0",type3,typedir,tempindex,templist,temparticle,namerule,namerule2,isdefault))
                self.cursor.execute("select max(id) from dede_arctype")
                type3_id = self.cursor.fetchone()

            self.cursor.execute(self.INSERT_ARCTINY, (type2_id,type3_id,'0'))
            self.cursor.execute("select max(id) from dede_arctiny")
            articleid = self.cursor.fetchone()
           
            self.cursor.execute(self.INSERT_ADDONARTICLE, (articleid, type2_id, markdown, url))
            self.cursor.execute(self.INSERT_ARCHIVES,(articleid, type2_id, type3_id, day, "", "-1", "1","0", "0", "0", title, "", "", "", site, "", day, day, "0", keywords, "0", "0", "0", "0", "0", "0", "", "", "0", "0", "0", "0"))
            

            """try:
                self.cursor.execute(self.INSERT_NEWS_ALL, news)
                log.msg(title + " saved successfully", level=log.INFO)
            except:
                log.msg("MySQL exception !!!", level=log.ERROR)
            self.lock.release()"""

    def process_item(self, item, spider):
        title = item['title']
        day = item['day']
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
