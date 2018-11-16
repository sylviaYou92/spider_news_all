# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 16:46:54 2018

@author: yangwn
"""

import scrapy
from bs4 import BeautifulSoup
from scrapy import log
#from datetime import timedelta
import re
from spider_news_all.items import SpiderNewsAllItem
import datetime
import time
import MySQLdb
import threading
from spider_news_all.config import SpiderNewsAllConfig


class MicrosoftAzureBlogSpider(scrapy.Spider):
    name = "Microsoft_Azure_blog"
    site_name = "Microsoft_Azure_blog"
    allowed_domains = ["azure.microsoft.com"]
    # some topic are dropped
    topics = ['artificial-intelligence', 'business-intelligence', 'azure-maps', 'big-data', 'datascience', 'data-warehouse', 'database', 'blockchain', 'cloud-strategy', 'internet-of-things', 'media-services', 'mobile', 'security', 'it-pro', 'storage-backup-and-recovery', 'announcements', 'hybrid', 'last-week-in-azure', 'monitor', 'networking', 'supportability', 'virtual-machines','identity-access-management', 'web']
    start_urls = tuple(["https://azure.microsoft.com/en-us/blog/topics/%s/?page=1"%topic for topic in topics])
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
        for start_url in [re.match("(.+)\?page",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()

    def get_type_from_url(self, url):
        if 'intelligence' in url:
            return u'人工智能'
        elif 'data' in url or 'maps' in url:
            return u'大数据'
        elif 'cloud-strategy' in url:
            return u'云计算'
        elif 'blockchain' in url:
            return u'区块链'
        elif 'internet-of-things' in url:
            return u'物联网'
        elif 'media' in url:
            return u'图像音视频压缩'
        elif 'mobile' in url:
            return u'移动加速'
        elif 'security' in url:
            return u'网络安全'
        elif 'it-pro' in url or 'storage' in url:
            return u'技术博文'
        else:
            return u'综合新闻'


    def get_type_from_tags(self, keywords):
        if 'Intelligence' in keywords:
            return u'人工智能'
        elif 'Data' in keywords or 'Maps' in keywords:
            return u'大数据'
        elif 'Cloud Strategy' in keywords:
            return u'云计算'
        elif 'Blockchain' in keywords:
            return u'区块链'
        elif 'Internet of Things' in keywords:
            return u'物联网'
        elif 'Media' in keywords:
            return u'图像音视频压缩'
        elif 'Mobile' in keywords:
            return u'移动加速'
        elif 'Security' in keywords:
            return u'网络安全'
        elif 'It Pro' in keywords or 'Storage' in keywords:
            return u'技术博文'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("(.+)\?page",url).group(1)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response,'lxml')
            links = soup.find("div",class_='blog-posts').find_all("article",class_ = "blog-postItem")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news= url_news = "https://azure.microsoft.com" + links[i].find('h2').find('a')['href']
                title = links[i].find("h2").text
    
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    need_parse_next_page = False
                    break
    
                type3 = self.get_type_from_url(url)
                day = links[i].find("p",class_='text-body5').text.strip()
                day = datetime.datetime.strptime(day,'%A, %B %d, %Y')
                day = int(time.mktime(day.timetuple())) # convert to timestamp
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3':type3,'day': day, 'title': title}))
            
        page = int(re.search("page=(\d+)",url).group(1))
        if soup.find("link",rel='next'):
            has_next_page = True
        else:
            has_next_page = False
        
        if need_parse_next_page and has_next_page and page < 2: #need_parse_next_page:
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
        day = title = keywords = url = article = markdown = keywords = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response)
        try:
            keywords = [tag.text.strip() for tag in soup.find_all(class_="blog-topicLabel")]
            keywords = ','.join(keywords)
            keywords = re.sub('&','and',keywords)
            type3 = self.get_type_from_tags(keywords)
        except:
            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        
        try:
            content = soup.find("div",class_="blog-postContent")
            article = content.text.strip()
            markdown = content.prettify() # html-code
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)

        if content.img:
            url = [url,content.img['src']]
        else:
            url = [url,'https://vignette.wikia.nocookie.net/logopedia/images/f/fa/Microsoft_Azure.svg/revision/latest/scale-to-width-down/640?cb=20170928200148']

        item['title'] = title
        item['day'] = day
        item['type1'] = u'友商资讯'
        item['type2'] = 'Azure'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'Azure'
        item['markdown'] = markdown
        return item


        
        