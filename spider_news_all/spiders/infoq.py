# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 16:36:32 2018

@author: yangwn
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
    name = "infoq"
    site_name = "infoq"
    allowed_domains = ["infoq.com"]###?
    start_urls = (
            "http://www.infoq.com/cn/ai-ml-data-eng/news/0",
            "http://www.infoq.com/cn/bigdata/news/0",
            "http://www.infoq.com/cn/cloud-computing/news/0",
            "http://www.infoq.com/cn/qukuailian/news/0",
            "http://www.infoq.com/cn/ai-ml-data-eng/articles/0",
            "http://www.infoq.com/cn/bigdata/articles/0",
            "http://www.infoq.com/cn/cloud-computing/articles/0",
            "http://www.infoq.com/cn/qukuailian/articles/0",
            "http://www.infoq.com/cn/development/news/0",
            "http://www.infoq.com/cn/architecture-design/news/0",
            "http://www.infoq.com/cn/development/articles/0",
            "http://www.infoq.com/cn/architecture-design/articles/0",
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
        for start_url in [re.search("(.*)/\d+",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()


    def get_type_from_url(self, url,url_news):
        if 'ai-ml-data-eng' in url:
            return u'人工智能'
        elif 'bigdata' in url:
            return u'大数据'
        elif 'cloud-computing' in url:
            return u'云计算'
        elif 'qukuailian' in url:
            return u'区块链'
        else:
            return u'综合新闻'



    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.search("(.*)/\d+",url).group(1)   
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = soup.find_all("div",class_ = ["news_type_block","news_type_block last","new_type1","news_type1 last","news_type2 full_screen"])
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                    url_news = links[i].find("h2").find("a").get("href").strip() 
                    if not re.match("http",url_news): 
                        url_news = "http://www.infoq.com"+url_news
                        
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    type3 = self.get_type_from_url(url,url_news)
                    
                    day = re.findall(u"(\d+年\d+月\d+日)",links[i].find("span",class_ = "author").text)[-1].strip() 
                    day = datetime.datetime.strptime(day.encode('utf-8'),'%Y年%m月%d日')
                    day = int(time.mktime(day.timetuple()))
                    title = links[i].find("h2").get_text().strip() 
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title}))
            
            if "articles" in url:
                page_lag = 12
            elif "news" in url:
                page_lag = 15
            page = int(re.search("(.*)/(\d+)",url).group(2))
            if need_parse_next_page and page < 2*page_lag:#need_parse_next_page:
                page += page_lag
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
        day = title = type3 = keywords = url = article = ''
        url = response.url
        url = re.sub('(?P<value>\?useSponsorshipSuggestions=true$)','',url)
        day = response.meta['day']
        title = response.meta['title']
        type3 = response.meta['type3']
        response = response.body
        soup = BeautifulSoup(response)
        try:
            content_paragraph = soup.find("div",class_="text_info")
            content = []
            for tag in content_paragraph.find("div",id ="contentRatingWidget").previous_siblings:
                if type(tag)== bs4.element.NavigableString:
                    content.insert(0,tag)
                else:
                    content.insert(0,tag.prettify())
            
            content = ''.join(content).strip()
            content = BeautifulSoup(content,'lxml')
            if content.find("div",class_="related_sponsors"):
                content.find("div",class_="related_sponsors").decompose()
                if len(content.find_all("script"))==1:
                    content.find("script").decompose()
            content = content.find("body")
            content.name = "div"
            content['class'] = "text_info"
            article = content.text.strip()
            markdown = content.prettify()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['type1'] = u'源站资讯'
        item['type2'] = u'InfoQ'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = 'InfoQ'
        item['markdown'] = markdown
        return item

        
