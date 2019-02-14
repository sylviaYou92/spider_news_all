# -*- coding: utf-8 -*-
"""
@author: chuangxinyanjiubu
"""

import scrapy
from bs4 import BeautifulSoup
import bs4
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

class InfoqSpider(scrapy.Spider):
    name = "gov"
    site_name = "gov"
    allowed_domains = ["gov.cn"]
    start_urls = (
            "http://sousuo.gov.cn/column/30469/0.htm",
    )
    handle_httpstatus_list = [521]
 
    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')


    def __init__(self):
        self.lock.acquire()
        self.cursor.execute("SELECT start_url, latest_url FROM url_record WHERE site_name='%s'"%self.site_name)
        self.record_url = dict(self.cursor.fetchall())
        self.lock.release()

        for start_url in [re.search("(.*)/",url).group(0) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.*)/",url).group(0)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = soup.find("div",class_="news_box").find_all("h4")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True # true: need record
            for i in range(0, len(links)):
                url_news = links[i].find("a").get("href").strip()
  
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    need_parse_next_page = False
                    break

                
                title = links[i].a.get_text()
                wordincluded = ['互联网','信息化','网站','智能','联网','网络','互联网','信息技术']
                flag = 0
                for word in wordincluded:
                    if word in title:
                        flag = 1
                if flag == 0:
                    continue
                            
                type3 = ''
                    
                day = links[i].find("span",class_ = "date").get_text().strip()
                day = time.mktime(time.strptime(day, "%Y.%m.%d")) # convert to timestamp
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            

            page = int(re.search("(.*)/(\d+)\.htm",url).group(2))
            if need_parse_next_page and page < 45:#need_parse_next_page:
                page = page + 1
                page_next = re.search("(.*)/(.)",url).group(1) + '/' + str(page) + '.htm'
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            else:
                self.lock.acquire()
                self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
                self.lock.release()
            return items


    def parse_news(self, response):
        log.msg("Start to parse news " + response.url, level=log.INFO)
        item = SpiderNewsAllItem()
        day = title = type3 = keywords = url = article = markdown = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response)


        if 'http://www.gov.cn/zhengce/content/' in url:
            try:
                content = soup.find("div",class_="wrap")
                content.table['class'] = 'gov-info'
                del content.table['style']
                del content.table['width']
                del content.table.table['style']
                del content.table.table['width']
                for e in range(0,len(content.table.table.tr.find_all('td'))):
                    del content.table.table.tr.find_all('td')[e]['width']
               
                article = content.text.strip()
                markdown = content.prettify()
                url = [url,'http://www.gov.cn/govweb/xhtml/2016gov/images/public/icon_9.jpg']
            except:
                log.msg("News " + title + " dont has article!", level=log.INFO)
        else:
            try:
                content = soup.find("div",class_="pages_content")
                article = content.text.strip()
                markdown = content.prettify()
                url = [url,'http://www.gov.cn/govweb/xhtml/2016gov/images/public/icon_9.jpg']
            except:
                log.msg("News " + title + " dont has article!", level=log.INFO)
            
        item['title'] = title
        item['day'] = day
        item['type1'] = u'政府文件'
        item['type2'] = u'国务院'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'国务院'
        item['markdown'] = markdown
        item['abstract'] = ''
        return item
