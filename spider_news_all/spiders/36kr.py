# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 09:50:36 2018

@author: yangwn
"""

import scrapy
from bs4 import BeautifulSoup
from scrapy import log
import re
from spider_news_all.items import SpiderNewsAllItem
import datetime
import time
#from tomd import Tomd
import MySQLdb
import threading
from spider_news_all.config import SpiderNewsAllConfig
import json
import HTMLParser


class Thirty_six_KrSpider(scrapy.Spider):
    name = "36kr"
    site_name = "36kr"
    allowed_domains = ["36kr.com"]###?
    start_urls = (
            "https://36kr.com/api/search-column/218?per_page=20&page=1",
            "https://36kr.com/api/search-column/208?per_page=20&page=1",
    )
    handle_httpstatus_list = [521]###?
    html_parser = HTMLParser.HTMLParser()


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
        for start_url in [re.match("(.+)\?per_page",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    

    def get_type_from_url(self, url):
        if '208' in url:
            return u'区块链'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("(.+)\?per_page",url).group(1)
        items = []
        try:
            response = response.body
            links = json.loads(response)['data']['items']
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news = "https://36kr.com/api/post/"+str(links[i]['id'])
                    
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    need_parse_next_page = False
                    break
    
                type3 = self.get_type_from_url(start_url)
                day =re.sub('T',' ',links[i]['published_at'])
                day = re.search('(\d+-\d+-\d+ \d+:\d+:\d+)',day).group(1)
                day = datetime.datetime.strptime(day, "%Y-%m-%d %H:%M:%S") # convert time format
                day = int(time.mktime(day.timetuple())) # convert to timestamp
                title = links[i]['title']
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
           
            page = int(re.search("&page=(\d+)",url).group(1))
            if need_parse_next_page and page < 3:#need_parse_next_page:
                page += 1
                page_next = re.sub("&page=(\d+)","&page=%s"%str(page),url)
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
        day = title = type3 = keywords = url = article = ''
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        data_dict = json.loads(response)['data']
        
        try:
            keywords = json.loads(data_dict['extraction_tags_extend'])
            keywords =  [tag for tag in keywords]
            keywords = ','.join(keywords)
        except:
            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        
        try:
            content = BeautifulSoup(self.html_parser.unescape(data_dict['content']),'lxml')
            for i in range(0,len(content.find_all(class_='detect-string'))):
                content.find(class_='detect-string').decompose()
            if content.img:
                url = ['https://36kr.com/p/%s.html'%str(data_dict['id']),content.img["src"]]
                print "**********************************"
                print url
            article = content.text.strip()
            markdown = content.prettify()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)

        item['title'] = title
        item['day'] = day
        item['type1'] = u'源站资讯'
        item['type2'] = u'36氪'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'36氪'
        item['markdown'] = markdown
        return item

        
        