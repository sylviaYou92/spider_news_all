# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 13:53:43 2018

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
import MySQLdb
import threading
from spider_news_all.config import SpiderNewsAllConfig


class AWSBlogChinaSpider(scrapy.Spider):
    name = "aws_blog_china"
    site_name = "aws_blog_china"
    allowed_domains = ["aws.amazon.com"]
    start_urls = (
            "https://aws.amazon.com/cn/blogs/china/",
    )
    handle_httpstatus_list = [521]###?
 

    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True,use_unicode = True,charset="utf8")
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
#        try:
#            items_keywords = soup.find("div",class_='footer-tags').find_all('a')
#            for i in range(0, len(items_keywords)):
#                keywords += items_keywords[i].text.strip() + ' '
#        except:
#            log.msg("News " + title + " dont has keywords!", level=log.INFO)
#        
        try:
            content = soup.find("section",class_ = "blog-post-content")
            article = content.text.strip()
            markdown = content.prettify() # html-code
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['type1'] = u'友商资讯'
        item['type2'] = 'AWS Cloudfront'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'aws.amazon'
        item['markdown'] = markdown
        return item


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = self.start_urls[0]
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = links = soup.find_all("article",class_='blog-post')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                    title = links[i].find('h2').text.strip()
                    if title == u'AWS 官方博客目录':
                        continue
                    url_news= links[i].find('h2').find('a').get('href')
                    if url_news =="https://aws.amazon.com/cn/blogs/china/all/" or url_news == "https://aws.amazon.com/cn/blogs/china/":
                        continue
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = u"云计算"
                    day = links[i].find("time").get("datetime")
                    day = re.sub("T",' ',re.sub("\+00:00$","",day))
                    day = datetime.datetime.strptime(day, "%Y-%m-%d %H:%M:%S")+timedelta(hours = 8) # convert time format and time_zone
                    day = int(time.mktime(day.timetuple())) # convert to timestamp
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            
            if url == start_url:
                page = 1
            else:
                page = int(re.search("page/(\d+)",url).group(1))
            
            if need_parse_next_page and page < 2: #need_parse_next_page:
                page += 1
                if page == 2:
                    page_next = 'https://aws.amazon.com/cn/blogs/china/page/2/'
                else:
                    page_next = re.sub("\d+",str(page),url)
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            else:
                self.lock.acquire()
                self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
                self.lock.release()
            return items
        
        