# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 14:57:19 2018

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


class AnalysysSpider(scrapy.Spider):
    name = "analysys"
    site_name = "analysys"
    allowed_domains = ["analysys.com"]###?
    start_urls = (
            "https://www.analysys.cn/article/analysis/1",
    )
    handle_httpstatus_list = [521]###?
 
    FLAG_INTERRUPT = False
 
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
        for start_url in [re.search("(.*/)\d+",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()



    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.*/)\d+",url).group(1)   
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response,'lxml')
            links = soup.find_all("li",class_='clearfix')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news = "https://www.analysys.cn"+ links[i].find("a").get("href")
                
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    need_parse_next_page = False
                    break
              
                title = links[i].find("h1").text
                day = links[i].find_all("span")[1].text.strip()
                day = datetime.datetime.strptime(day,'%Y-%m-%d')
                day = int(time.mktime(day.timetuple()))
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'day': day, 'title': title}))

            page = int(re.search("(.*)/(\d+)",url).group(2))
            if need_parse_next_page and page < 100:#need_parse_next_page:
                page += 1
                page_next = re.sub("\d+",str(page),url)
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
        day = title = keywords = url = article = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        response = response.body
        soup = BeautifulSoup(response,'lxml')
        try:
            content = soup.find("div",class_="left_content")
            imgs = content.find_all('img')
            for j in range(0,len(imgs)):
                if not re.match('https://www.analysys.cn',imgs[j]['src']):
                    imgs[j]['src'] = 'https://www.analysys.cn'+imgs[j]['src']
            article = content.text.strip()
            markdown = content.prettify()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        
        url = [url,'https://www.analysys.cn/images/logob.svg']

        item['title'] = title
        item['day'] = day
        item['type1'] = u'行业分析'
        item['type2'] = u'易观'
        item['type3'] = u'综合新闻'
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = '易观'
        item['markdown'] = markdown
        item['abstract'] = ''
        return item

        
        