# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 14:48:03 2018

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


class GcorelabsSpider(scrapy.Spider):
    name = "gcorelabs_blog"
    site_name = "gcorelabs_blog"
    allowed_domains = ["gcorelabs.com"]
    start_urls = (
            'https://gcorelabs.com/category/media-platform/',
            'https://gcorelabs.com/category/security/',
            'https://gcorelabs.com/blog/',
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

    def unicode_to_utf8(self,matched):
        value = (matched.group('value'))
        return value.decode('unicode-escape').encode('utf-8')
    
    def get_type_from_url(self, url):
        if 'media-platform' in url:
            return u'图像音视频压缩'
        elif 'security' in url:
            return u'网络安全'
        else:
            return u'综合新闻'
        
        
    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        if url in self.start_urls:
            start_url = url
        else:
            start_url = re.match('(.+)page',url).group(1)
            
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response,"lxml")
            links = soup.find("ul",class_ = 'list_grid').find_all('li')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                    url_news = links[i].find("a",class_ = 'news_one_link').get('href') 

                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = self.get_type_from_url(url)
                    day = links[i].find("div",class_='preview_grid_date').text
                    day = datetime.datetime.strptime(day, "%d.%m.%Y") # convert time format
                    day = int(time.mktime(day.timetuple())) # convert to timestamp
                    title = links[i].find("div",class_ = "preview_grid_title").text 
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            
            if need_parse_next_page and soup.find("a",class_= 'next_page_arrow'):#need_parse_next_page and have next page:
                page_next = soup.find("a",class_= 'next_page_arrow').get('href')
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
        soup = BeautifulSoup(response,"lxml")
#        try:
#            items_keywords = soup.find("div",class_='footer-tags').find_all('a')
#            keywords = [tag.text.strip() for tag in items_keywords]
#            keywords = ','.join(keywords)
#        except:
#            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        
        try:
            content = soup.find('div',class_="one_blog")
            if content and content.find(class_ = 'back_to_blog'):
                content.find(class_ = 'back_to_blog').extract()
            if content and content.find('div',class_ = 'link_title_blog'):
                content.find("div",class_='link_title_blog').extract()
            if content and content.find("div",class_ = "date_blog"):
                content.find("div",class_ = "date_blog").extract()

            article = content.text.strip().replace(u'\xc2\xa0', u' ').replace(u'\xa0', u' ').replace(u'\xb0',' ')
            article = article.encode('unicode-escape').decode('string-escape')
            article = re.sub('(?P<value>\\\u\d{4})',self.unicode_to_utf8,article).decode('utf-8')
    
            markdown = content.prettify().replace(u'\xc2\xa0', u' ').replace(u'\xa0', u' ').replace(u'\xb0',' ')
            markdown = markdown.encode('unicode-escape').decode('string-escape')
            markdown = re.sub('(?P<value>\\\u\d{4})',self.unicode_to_utf8,markdown).decode('utf-8')
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['type1'] = u'友商资讯'
        item['type2'] = 'Gcorelabs'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'Gcorelabs'
        item['markdown'] = markdown
        return item



        
        