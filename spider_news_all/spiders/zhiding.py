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
    name = "zhiding"
    site_name = "zhiding"
    allowed_domains = ["zhiding.cn"]
    start_urls = (
            "http://net.zhiding.cn/list-9-800-0-0-1.htm",
            "http://security.zhiding.cn/list-125-800-0-0-1.htm",
            "http://cloud.zhiding.cn/list-141-0-0-0-1.htm",
            "http://big-data.zhiding.cn/list-0-1-0-0-1.htm",
            "http://ai.zhiding.cn/list-0-512-0-0-1.htm",
            "http://iot.zhiding.cn/list-0-502-0-0-1.htm",
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

        for start_url in [re.search("(.*)/",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    
    def get_type_from_url(self, url,url_news):
        if '/ai.zhiding' in url:
            return u'人工智能'
        elif '/big-data.zhiding' in url:
            return u'大数据'
        elif '/cloud.zhiding' in url:
            return u'云计算'
        elif '/net.zhiding' in url:
            return u'系统&网络'
        elif '/iot.zhiding' in url:
            return u'物联网'
        elif '/security.zhiding' in url:
            return u'网络安全'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.*)/",url).group(1)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = soup.find_all("div",class_ = ["qu_loop"])
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True # true: need record
            for i in range(0, len(links)):
                    url_news = links[i].find("div",class_ = "qu_tix").find("b").find("a").get("href").strip()
                        
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = self.get_type_from_url(url,url_news)
                    
                    day = links[i].find("div",class_ = "qu_times").get_text().strip()
                    day = time.mktime(time.strptime(day, "%Y-%m-%d %H:%M:%S")) # convert to timestamp
                    title = links[i].find("div",class_ = "qu_tix").find("b").find("a").get("title").strip()
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            

            page = int(re.search("(.*)-(.)",url).group(2))
            if need_parse_next_page and page < 2:#need_parse_next_page:
                page = page + 1
                page_next = re.search("(.*)-(.)",url).group(1) + '-' + str(page) + '.htm'
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

        if '行业会议' in soup.find("div",class_="qu_weizhi").text.strip():
            pass
        else:
            page_links = soup.find_all("a",class_="fancybox_content")
            
            for page_link in page_links:
                page_str = page_link['href']
                ind = page_str.find("?")
                page_link['href'] = page_str[:ind]
            
            try:
                content = soup.find("div",class_="qu_ocn")
                article = content.text.strip()
                markdown = content.prettify()
                url = [url,'http://icon.zhiding.cn/zdnet/2015/images/e44_03.jpg']
            except:
                log.msg("News " + title + " dont has article!", level=log.INFO)
            
            item['title'] = title
            item['day'] = day
            item['type1'] = u'源站资讯'
            item['type2'] = u'至顶网'
            item['type3'] = type3
            item['url'] = url
            item['keywords'] = keywords
            item['article'] = article
            item['site'] = u'至顶网'
            item['markdown'] = markdown
            return item
