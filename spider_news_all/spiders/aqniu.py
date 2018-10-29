# -*- coding: utf-8 -*-
"""
@author: ysy
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
    name = "aqniu"
    site_name = "aqniu"
    allowed_domains = ["aqniu.com"]
    start_urls = (
            "https://www.aqniu.com/category/industry",
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

        for start_url in self.start_urls:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    
    def get_type_from_url(self, url,url_news):
        if 'industry' in url:
            return u'网络安全'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("https://www.aqniu.com/category/industry",url).group(0)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = soup.find_all("div",class_ = "row post")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True # true: need record
            for i in range(0, len(links)):
                    url_news = links[i].find("div",class_ = "layout_3--item").find("div",class_="col-md-7 col-sm-6").find("h4").find("a").get("href").strip()
                        
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = self.get_type_from_url(url,url_news)
                    
                    day = links[i].find("div",class_= "meta").find("span",class_ = "date").get_text().strip()
                    month_map = {'一月':1,'二月':2,'三月':3,'四月':4,'五月':5,'六月':6,'七月':7,'八月':8,'九月':9,'十月':10,'十一月':11,'十二月':12}
                    month = int(month_map[day[day.find(',')+2:][:(day[day.find(',')+2:].find(' '))].encode('utf-8')])
                    dayth = int(day[day.rfind(',')-2:day.rfind(',')])
                    year = int(day[-4:])
                    day = int(time.mktime((year,month,dayth,0,0,0,0,0,0)))
                    title = links[i].find("h4").find("a").get_text().strip()
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            

            if 'page' in url:
                page = int(url[url.rfind('page/')+5:])
            else:
                page = 1
            
            if need_parse_next_page and page < 2:#need_parse_next_page:
                page = page + 1
                page_next = re.match("https://www.aqniu.com/category/industry",url).group(0) + '/page/' + str(page)
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

        
        content = soup.find("div",class_="blog-excerpt")





        
        article = content.text.strip()
        markdown = content.prettify()
        print article
        #except:
        #    log.msg("News " + title + " dont has article!", level=log.INFO)
            
        item['title'] = title
        item['day'] = day
        item['type1'] = u'源站资讯'
        item['type2'] = u'安全牛'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'安全牛'
        item['markdown'] = markdown
        return item
