# -*- coding: utf-8 -*-
import threading

import MySQLdb
import scrapy
from bs4 import BeautifulSoup
from scrapy import log

from spider_news_all.config import SpiderNewsAllConfig
from spider_news_all.items import SpiderNewsAllItem


class CjwSpider(scrapy.Spider):
    name = "cjw"
    allowed_domains = ["caijing.com.cn"]
    start_urls = (
        'http://economy.caijing.com.cn/economynews/',
        'http://economy.caijing.com.cn/observation/',
        'http://economy.caijing.com.cn/economics/',
        'http://economy.caijing.com.cn/region/',
        'http://economy.caijing.com.cn/policy/',
        'http://economy.caijing.com.cn/report/',
        'http://industry.caijing.com.cn/industrianews/',
        'http://industry.caijing.com.cn/steel/index.html',
        'http://industry.caijing.com.cn/energy/',
        'http://industry.caijing.com.cn/aviations/',
        'http://industry.caijing.com.cn/traffic/',
        'http://industry.caijing.com.cn/food/',
        'http://industry.caijing.com.cn/medicals/',
        'http://industry.caijing.com.cn/consumption/',
        'http://industry.caijing.com.cn/industrys/',
    )

    handle_httpstatus_list = [521]

    FLAG_INTERRUPT = False
    SELECT_NEWS_BY_TITLE_AND_URL = "SELECT COUNT(*) FROM news_all WHERE title='%s' AND url='%s'"

    lock = threading.RLock()
    cfg = SpiderNewsAllConfig.news_db_addr
    conn=MySQLdb.connect(host= cfg['host'],user=cfg['user'], passwd=cfg['password'], db=cfg['db'], autocommit=True)
    conn.set_character_set('utf8')
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    def is_news_not_saved(self, title, url):
        if self.FLAG_INTERRUPT:
            self.lock.acquire()
            rows = self.cursor.execute(self.SELECT_NEWS_BY_TITLE_AND_URL % (title, url))
            print self.cursor.fetchone()
            if rows > 0:
                log.msg("News saved all finished.", level=log.INFO)
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse_news(self, response):
        log.msg("Start to parse news " + response.url, level=log.INFO)
        item = SpiderNewsAllItem()
        day = title = _type = keywords = url = article = ''
        url = response.url
        day = response.meta['day']
        title = response.meta['title']
        _type = response.meta['_type']
        response = response.body
        soup = BeautifulSoup(response)
        try:
            items_keywords = soup.find(class_='ar_keywords').find_all('a')
            for i in range(0, len(items_keywords)):
                keywords += items_keywords[i].text.strip() + ' '
        except:
            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        try:
            article = soup.find(class_='article-content').text.strip()
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = "2010-01-01"
        item['_type'] = _type
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'财经网'
        item['markdown'] = "markdown"
        return item

    def get_type_from_url(self, url):
        if 'economynews' in url:
            return u'宏观频道首页.每日要闻宏观'
        elif 'observation' in url:
            return u'宏观频道首页.观察'
        else:
            return ''

    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        _type = self.get_type_from_url(url)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            lists = soup.find(class_='list')
            links = lists.find_all('li')
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            for i in range(0, len(links)):
                url_news = links[i].a['href']
                day = links[i].find(class_='time').text.strip()
                title = links[i].a.text.strip()
                need_parse_next_page = self.is_news_not_saved(title, url_news)
                if not need_parse_next_page:
                    break
                items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'_type': _type, 'day': day, 'title': title}))
            if (soup.find('a', text=u'下一页')['href'].startswith('http://')):
                page_next = soup.find('a', text=u'下一页')['href']
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            return items