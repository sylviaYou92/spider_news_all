# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 09:34:21 2018

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
import json

class CDN77Spider(scrapy.Spider):
    name = "cdn77_blog"
    site_name = "cdn77_blog"
    allowed_domains = ["cdn77.com"]
    start_urls = (
            "https://www.cdn77.com/blog/wp-admin/admin-ajax.php?action=alm_query_posts&posts_per_page=9",
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
        start_url = re.search("(.+)&",self.start_urls[0]).group(1)
        if self.record_url.get(start_url)==None:
            self.record_url.setdefault(start_url,None)
            self.lock.acquire()
            self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
            self.lock.release()
        self.updated_record_url = self.record_url.copy()

    def unicode_to_utf8(self,matched):
        value = (matched.group('value'))
        return value.decode('unicode-escape').encode('utf-8')


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.+)&",self.start_urls[0]).group(1)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(json.loads(response)['html'],'lxml')
            links = soup.find_all("article")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
#        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news = links[i].find('a').get('href')

                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
#                    need_parse_next_page = False
                    break

                type3 = u"综合新闻"
                day = links[i].find(class_='Post-date').text
                day = datetime.datetime.strptime(day, "%d. %B %Y")
                day = int(time.mktime(day.timetuple())) # convert to timestamp
                title = links[i].find("h2").text.strip()
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
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
       
        try:
            items_keywords = soup.find_all("a",class_='Post-tag')
            keywords = [tag.text.strip() for tag in items_keywords]
            keywords = ''.join(keywords)
            keywords = re.sub("#","",keywords)
        except:
            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        
        try:
            content = soup.find("div",class_='entry-content')
            article = content.text.strip().encode('unicode-escape').decode("string-escape")
            article = re.sub('(?P<value>\\\u\d{4})',self.unicode_to_utf8,article).decode('utf-8','ignore')
        
            markdown = content.prettify().encode('unicode-escape').decode("string-escape")
            markdown = re.sub('(?P<value>\\\u\d{4})',self.unicode_to_utf8,markdown).decode('utf-8','ignore')
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        
        if content.img:
            url = [url,content.img['src']]
        else:
            url = [url,'https://www.cdn77.com/blog/wp-content/themes/twentyseventeen-child/assets/images/cdn77_logo.svg']
        item['title'] = title
        item['day'] = day
        item['type1'] = u'友商资讯'
        item['type2'] = 'CDN77'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'CDN77'
        item['markdown'] = markdown
        return item
        