# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 17:12:36 2018

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
import json

class AWSBlogSpider(scrapy.Spider):
    name = "aws_blog"
    site_name = "aws_blog"
    allowed_domains = ["aws.amazon.com"]###?
    start_urls = (
            "https://aws.amazon.com/api/dirs/blog-posts/items?order_by=SortOrderValue&sort_ascending=false&limit=250&locale=en_US",   #setting parameter limit for number of blogs to crawl, upper bound is 250
    )
    handle_httpstatus_list = [521]###?

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
        start_url = "https://aws.amazon.com"
        if self.record_url.get(start_url)==None:
            self.record_url.setdefault(start_url,None)
            self.lock.acquire()
            self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
            self.lock.release()
        self.updated_record_url = self.record_url.copy()

    def parse_news(self, response):
        log.msg("Start to parse news " + response.url, level=log.INFO)
        item = SpiderNewsAllItem()
        day = title = type3 = keywords = url = article = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response)
        
        try:
            content = soup.find("section",class_="blog-post-content")
            for tag in content.find_all(style=re.compile("^padding")):
                del tag['style']    

            article = content.text.strip()
#            markdown = Tomd(str(content)).markdown.decode('utf-8')
            markdown = str(content).decode('utf-8')
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        
        url = [url,'http://s14.sinaimg.cn/mw690/006DE4Lyzy7evP3PYBfed']

        item['title'] = title
        item['day'] = day
        item['type1'] = u'友商资讯'
        item['type2'] = 'AWS Cloudfront'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'AWS'
        item['markdown'] = markdown
        item['abstract'] = ''
        return item


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = "https://aws.amazon.com"  
        items = []
        try:
            response = response.body
            links = json.loads(response)['items']######################?
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
#        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                item = links[i]['additionalFields']
                url_news = item['link'] 
#                    if not re.match("http",url_news): 
#                        url_news = start_url + url_news
                        
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
#                    need_parse_next_page = False
                    break

                type3 = u"云计算"
                day = item['modifiedDate']
                day = re.sub("Z","",re.sub("T"," ",day))    
                day = (datetime.datetime.strptime(day, "%Y-%m-%d %H:%M:%S")+timedelta(hours = 8)) # convert time_zone
                day = int(time.mktime(day.timetuple())) # convert to timestamp
                title = item["title"] #获取首页新闻标题
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))

            self.lock.acquire()
            self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
            self.lock.release()
                        
            return items
        
        