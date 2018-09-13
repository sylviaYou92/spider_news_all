# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 16:01:11 2018

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


class FastlySpider(scrapy.Spider):
    name = "fastly_blog"
    site_name = "fastly_blog"
    allowed_domains = ["fastly.com"]
    start_urls = (
            "https://www.fastly.com/blog",
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
        day = title = _type = keywords = url = article = markdown = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        _type = response.meta['_type']
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
            content = soup.find("div",class_="column")
            article = content.text.strip().encode('iso-8859-1').decode('utf-8')
#            markdown = Tomd(str(content)).markdown.decode('utf-8')
#            markdown = markdown.encode('iso-8859-1').decode('utf-8')
            markdown = unicode(content).encode('iso-8859-1').decode('utf-8')# html-code
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['_type'] = _type
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'Fastly'
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
            links = soup.find_all("span",class_='card')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                    url_news = links[i].find('a').get('href') 
                    if not re.match("http",url_news): 
                        url_news = "https://www.fastly.com" + url_news
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    _type = u"友商官方"
                    day = links[i].find("div",class_="post-footer").find("p").text.strip()
                    day = datetime.datetime.strptime(day, "%B %d, %Y")
                    day = int(time.mktime(day.timetuple())) # convert to timestamp
                    title = links[i].find("p",class_="post-title").text.strip()
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'_type': _type, 'day': day, 'title': title}))
            
            if url == start_url or url == start_url+"/":
                page = 0
            else:
                page = int(re.search("(\d+)",url).group(1))
            
            if need_parse_next_page and page < 1: #need_parse_next_page:
                page += 1
                if page == 1:
                    page_next = 'https://www.fastly.com/blog/1'
                else:
                    page_next = re.sub("\d+",str(page),url)
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            else:
                self.lock.acquire()
                self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
                self.lock.release()
            return items
        
        