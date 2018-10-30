# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 10:37:40 2018

@author: yangwn
"""

import scrapy
from bs4 import BeautifulSoup
from scrapy import log
from datetime import timedelta
import re
from spider_news_all.items import SpiderNewsAllItem
import datetime
import time
#from tomd import Tomd
import MySQLdb
import threading
from spider_news_all.config import SpiderNewsAllConfig


#https://www.vc.cn/investments?action=index&controller=investments&page=2&type=investment

class InfoqSpider(scrapy.Spider):
    name = "vc_combined"
    site_name = "vc"
    allowed_domains = ["vc.cn"]
    start_urls = (
            "https://www.vc.cn/investments",
    )
    handle_httpstatus_list = [521]
 
    FLAG_INTERRUPT = False
 
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
    INSERT_ARCTINY = ("INSERT INTO dede_arctiny (typeid,typeid2,mid,sortrank) VALUES (%s,%s,%s,%s)")
    INSERT_ADDONARTICLE = ("INSERT INTO dede_addonarticle (aid,typeid,body,redirecturl) VALUES (%s,%s,%s,%s)")
    INSERT_ARCHIVES = ("INSERT INTO dede_archives ( id, typeid, typeid2, sortrank, flag, ismake, channel, arcrank, click, money, title, shorttitle, color, writer, source, litpic, pubdate, senddate, mid, keywords, lastpost, scores, goodpost, badpost, voteid, notpost, description, filename, dutyadmin, tackid, mtype, weight) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

    
    def __init__(self):
        self.lock.acquire()
        self.cursor.execute("SELECT start_url, latest_url FROM url_record WHERE site_name='%s'"%self.site_name)
        self.record_url = dict(self.cursor.fetchall())
        self.lock.release()
        for start_url in [re.match("https://www.vc.cn/investments",url).group(0) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("https://www.vc.cn/investments",url).group(0)
        items = []
        current_time = datetime.datetime.now()
        current_year = current_time.year
        current_month = current_time.month


        response = response.body
        soup = BeautifulSoup(response)
        need_parse_next_page = True
        soup.table['class'] = 'invest'
        content = soup.find("table")
        content.thead.tr.th['width'] = '200px'
        links = soup.find("tbody",class_="table-list").find_all("tr")

        prefix = 'https://www.vc.cn'
        
        if len(links) > 0:
            for i in range(0,len(links)):
                day = links[i].find("td",class_ = "invest-time").get_text().strip()
                news_year = day.split(".")[0]
                news_month = day.split(".")[1]
                flag = 1

                links[i].find("td", class_="cover-info").find("div",class_="info").name = "divcontent"
                content.td.divcontent["class"] = 'invest-info'

                links[i].a["href"] = prefix + links[i].a["href"]
                links[i].td.divcontent.a["href"] = prefix + links[i].td.divcontent.a["href"]
                taglists = links[i].find("div",class_="taglist").find_all("span")
                for j in range(0,len(taglists)):
                    taglists[j].a['href'] = prefix + taglists[j].a['href']
                links[i].find("td",class_='link-list').a["href"] = prefix + links[i].find("td",class_='link-list').a["href"]
                links[i].find("td",class_='link-list').name = 'round-link-list'
                invests_links = links[i].find("td",class_='link-list').find_all("a")
                links[i].find("td",class_='link-list').name = 'td'
                for j in range(0,len(invests_links)):
                    invests_links[j]['href'] = prefix + invests_links[j]['href']
                

                if flag:
                    if news_year == current_year and news_month == current_month:
                        pass
                    else:
                        index = i
                        flag = 0
                

            
            for _ in range(index,len(links)):
                content.find("tbody",class_="table-list").find_all("tr")[index].decompose()
            
            
            article = content.text.strip()
            markdown = content.prettify()

            day = str(current_time).split('.')[0]
            day = int(time.mktime(time.strptime(day, "%Y-%m-%d %H:%M:%S")))
            title = str(current_year) + u'年' + str(current_month) + u'月' + ' 投融信息汇总'
            type1 = u'投融事件'
            type2 = u''
            type3 = u''
            site = u'创投圈'
            url = "https://www.vc.cn/investments"
            keywords = ''



        
        
        QUERY_TYPE = 'select id from dede_archives where title = %s'
        self.cursor.execute(QUERY_TYPE,(title,))
        res_id = self.cursor.fetchone()
        if res_id:
            UPDATE = "update dede_archives SET sortrank = %s, pubdate = %s, senddate = %s where id = %s"
            self.cursor.execute(UPDATE,(day,day,day,res_id))
            UPDATE2 = "update dede_addonarticle set body = %s where aid = %s"
            self.cursor.execute(UPDATE2,(markdown,res_id))
        else:
            QUERY_TYPE = ("select id from dede_arctype where typename = %s")
            self.cursor.execute(QUERY_TYPE,(type1,))
            type_id = self.cursor.fetchone()

            self.cursor.execute(self.INSERT_ARCTINY, (type_id,type_id,'0',day))
            self.cursor.execute("select max(id) from dede_arctiny")
            articleid = self.cursor.fetchone()
           
            self.cursor.execute(self.INSERT_ADDONARTICLE, (articleid, type_id, markdown, url))
            self.cursor.execute(self.INSERT_ARCHIVES,(articleid, type_id, type_id, day, "", "-1", "1","0", "0", "0", title, "", "", "", site, "", day, day, "0", keywords, "0", "0", "0", "0", "0", "0", "", "", "0", "0", "0", "0"))


        return items

        
        