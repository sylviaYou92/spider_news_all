# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 09:39:41 2018

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


class FromgeekSpider(scrapy.Spider):
    name = "fromgeek"
    site_name = "fromgeek"
    allowed_domains = ["fromgeek.com"]###?
    start_urls = (
            "http://www.fromgeek.com/api.php?op=autoload&page=1&pagesize=30&posid=10",
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
        for start_url in [re.match("(.+)&page=",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    

    def get_type_from_catname(self, catname):
        if u'智能' in catname:
            return u'人工智能'
        elif u'区块链' in catname:
            return u'区块链'
        else:
            return u'综合新闻'


    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("(.+)&page=",url).group(1)
        items = []
        try:
            response = response.body
            links = json.loads(response)
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                url_news = links[i]['nurl']
                    
                if url in self.start_urls and is_first:
                    self.updated_record_url[start_url] = url_news
                    is_first = False
                if url_news == self.record_url[start_url]:
                    need_parse_next_page = False
                    break
                
                catname = links[i]['catname']
                type3 = self.get_type_from_catname(catname)
                day = links[i]['inputtime']

                title = links[i]['title']
                keywords = links[i]['keys']
                keywords = re.sub(' ',',',keywords)
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'type3': type3, 'day': day, 'title': title,'keywords':keywords}))
           
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
        day = title = type3 = keywords = url = article = markdown = ''
        day = response.meta['day']
        url = response.url
        title = response.meta['title']
        type3 = response.meta['type3']
        keywords = response.meta['keywords']
        response = response.body
        soup = BeautifulSoup(response)
        if soup.find('li',class_='date'):
            date = soup.find('li',class_='date').find('span').text.strip()
            day = re.sub('\d+-\d+',date,day)
            day = datetime.datetime.strptime(day,'%Y-%m-%d %H:%M')
            day = int(time.mktime(day.timetuple()))
        else:
            day = self.time_convert(day,datetime.datetime.now())

            
        try:
            content = soup.find('article')
            if content==None:
                content = soup.find('div',class_='selfshow')
            if content.find("div",class_="contribute"):
                content.find("div",class_="contribute").decompose()
            content = content.prettify()
            pat = re.compile('(<article>.+)<p>\n  <a href="http://www.fromgeek.com/corp/',re.DOTALL)
            if re.match(pat,content):
                content = re.match(pat,content).group(1)+'</article>' 
            content = BeautifulSoup(content,'lxml')
            article = content.text.strip()
            markdown = content.find(['div','article']).prettify()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)

        item['title'] = title
        item['day'] = day
        item['type1'] = u'源站资讯'
        item['type2'] = u'极客网'
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'极客网'
        item['markdown'] = markdown
        return item

        
    def time_convert(self,old_string,time_now):
        if type(old_string)==unicode:
            old_string = old_string.encode("utf-8")
        old_string = re.sub("：",":",old_string)
        new_string = old_string
        
        if re.match("\d+-\d+",old_string) and len(re.findall("-",old_string)) == 1:
            month = int(re.match("(\d+)-(\d+)",old_string).group(1))
            if month > time_now.month:
                year = time_now.year-1
            else:
                year = time_now.year
            new_string = str(year) + '-' + old_string
        
        if re.match("\d{4}-\d+-\d+ \d+:\d+:\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d %H:%M:%S")))
        elif re.match("\d{4}-\d+-\d+ \d+:\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d %H:%M")))
        elif re.match("\d{4}-\d+-\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d")))

        return time_stamp
        