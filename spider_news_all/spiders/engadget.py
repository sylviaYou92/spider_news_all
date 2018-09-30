# -*- coding: utf-8 -*-
"""
Created on Sat Sep 29 14:50:43 2018

@author: yangwn
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 16:36:32 2018

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


class EngadgetSpider(scrapy.Spider):
    name = "engadget"
    site_name = "engadget"
    allowed_domains = ["engadget.com"]###?
    start_urls = (
            "https://cn.engadget.com/topics/vr-ar/page/1/",
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
        for start_url in [re.search("(.*)page",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()



    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.*)page",url).group(1)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response,'lxml')
            links = soup.find_all('div',class_='grid@m+')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                    url_news = "https://cn.engadget.com"+links[i].find('a').get('href')
                        
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = 'VR.AR'
                    day = links[i].find('span',class_=re.compile('^ hide@')).text.strip()
                    day = self.time_convert(day)
                    title = links[i].find('h2').find('span').text.strip()
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))

            page = int(re.search("(.*)/(\d+)",url).group(2))
            if need_parse_next_page and page < 3:#need_parse_next_page:
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
        day = title = type3 = keywords = url = article = markdown = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response)
        
        try:
            keywords = soup.find('section',class_='t-meta').find_all('div',class_='mt-5')[-1].span.find_all('a')
            keywords = [tag.text.strip() for tag in keywords]
            keywords = ','.join(keywords)
        except:
            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        

        try:
            content = soup.find_all('div',class_='o-article_block')
            article = [tag.text for tag in content]
            article = '\n'.join(article)
            markdown = [tag.prettify() for tag in content]    
            markdown = '\n'.join(markdown)
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['type1'] = u'源站资讯'
        item['type2'] = u'engadget'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'engadget'
        item['markdown'] = markdown
        return item

        
    def time_convert(self,old_string):
        if type(old_string)==unicode:
            old_string = old_string.encode('utf-8')
        new_string = old_string
        new_string = re.sub('凌晨','AM',new_string)
        new_string = re.sub('早上','AM',new_string)
        new_string = re.sub('中午','AM',new_string)
        new_string = re.sub('下午','PM',new_string)
        new_string = re.sub('傍晚','PM',new_string)
        new_string = re.sub('晚上','PM',new_string)
        date_time = datetime.datetime.strptime(new_string,'%Y 年 %m 月 %d 日, %p %H:%M')
        time_stamp = int(time.mktime(date_time.timetuple()))
        return time_stamp
            