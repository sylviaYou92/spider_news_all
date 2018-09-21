# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 16:08:42 2018

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


class IresearchSpider(scrapy.Spider):
    name = "iresearch_report"
    site_name = "iresearch_report"
    allowed_domains = ["iresearch.cn"]###?
    start_urls = (
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=59&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=60&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=61&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=62&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=63&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=64&sid=2&yid=0',# cloud-service
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=65&sid=2&yid=0',# AI
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=66&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=67&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=68&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=69&sid=2&yid=0',# Intellegence hardware
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=70&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=73&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=74&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=75&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=76&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=77&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=79&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=80&sid=2&yid=0',
            'http://report.iresearch.cn/common/page/rsprocess.ashx?work=csearch&vid=81&sid=2&yid=0',
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
        for start_url in self.start_urls:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    

    def get_type_from_url(self, url):
        if 'vid=65' in url or 'vid=69' in url:
            return u'人工智能'
        elif 'vid=64' in url:
            return u'云计算'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
#        response.encoding = 'gbk'
        url = response.url
        start_url = url
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response,"lxml")
            links = soup.find_all("li")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news = links[i].find('h3').find('a').get("href")
                if url_news == '':
                    break
                
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    break
    
                type3 = self.get_type_from_url(start_url)
                day = links[i].find('div',class_='time').text.strip()
                day = datetime.datetime.strptime(day,'%Y/%m/%d %H:%M:%S')
                day = int(time.mktime(day.timetuple()))
                title = links[i].find("h3").text
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
           
            self.lock.acquire()
            self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
            self.lock.release()
          
            return items


    def parse_news(self, response):
        log.msg("Start to parse news " + response.url, level=log.INFO)
        item = SpiderNewsAllItem()
        day = title = type3 = keywords = article = ''
        day = response.meta['day']
        url = response.url
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response,'lxml')
#        
#        try:
#            keywords = json.loads(data_dict['extraction_tags_extend'])
#            keywords =  [tag for tag in keywords]
#            keywords = ','.join(keywords)
#        except:
#            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        
        try:
            content = soup.find("div",class_="m-article")
            article = content.text.strip()
            markdown = content.prettify()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)

        item['title'] = title
        item['day'] = day
        item['type1'] = u'行业分析'
        item['type2'] = u'艾瑞网'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'艾瑞网'
        item['markdown'] = markdown
        return item

        
        