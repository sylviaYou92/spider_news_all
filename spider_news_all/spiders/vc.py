# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 10:37:40 2018

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


#https://www.vc.cn/investments?action=index&controller=investments&page=2&type=investment

class InfoqSpider(scrapy.Spider):
    name = "vc"
    site_name = "vc"
    allowed_domains = ["vc.cn"]
    start_urls = (
            "https://www.vc.cn/investments",
    )
    handle_httpstatus_list = [521]
 
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
        for start_url in [re.match("https://www.vc.cn/investments",url).group(0) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()



    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
        start_url = re.match("https://www.vc.cn/investments",url).group(0)
        items = []
        try:
            response = response.body
            soup = BeautifulSoup(response)
            links = soup.find("tbody",class_="table-list").find_all("tr")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        
        need_parse_next_page = True
        prefix = 'https://www.vc.cn'
        if len(links) > 0:
            is_first = True # true: need record
            for i in range(0, len(links)):
                company_url = 'https://www.vc.cn'+links[i].find("td",class_ = "cover-info").find("div",class_="info").find("div",class_="name").find("a").get("href").strip()
                day = links[i].find("td",class_ = "invest-time").get_text().strip()
                day = time.mktime(time.strptime(day, "%Y.%m.%d")) # convert to timestamp  
                company_name = links[i].find("td",class_ = "cover-info").find("div",class_="info").find("div",class_="name").find("a").get_text().strip()
                round_th = links[i].find("td",class_ = "link-list").find("li",class_="round").find("a").get_text().strip()
                    
                title = company_name + '(' + round_th + ")"


                 # type2
                invests = links[i].find_all("td",class_ = "link-list")[1].find_all("span")
                for invest_company in invests:
                    invest_company = invest_company.get_text().strip()
                    if invest_company == '腾讯':
                        type2 = u"腾讯"
                    elif invest_company == '阿里巴巴':
                        type2 = u"阿里巴巴"
                    elif invest_company == '百度风投':
                        type2 = u"百度风投"
                    elif invest_company == '软银中国':
                        type2 = u'软银中国'
                    else:
                        type2 = u""
                
                invests = links[i].find_all("td",class_ = "link-list")[1].find_all("a")
                for invest_company in invests:
                    invest_company = invest_company.get_text().strip()
                    if invest_company == '腾讯':
                        type2 = u"腾讯"
                    elif invest_company == '阿里巴巴':
                        type2 = u"阿里巴巴"
                    elif invest_company == '百度':
                        type2 = u"百度"
                    elif invest_company == '软银中国':
                        type2 = u'软银中国'
                    else:
                        type2 = u""
                
                soup2 = BeautifulSoup(response)
                soup2.table['class'] = 'invest'
                content =  soup2.find("table")
                content.thead.tr.th['width'] = '200px'
                for index in range(i):
                    content.find("tbody",class_="table-list").find_all("tr")[0].decompose()
                for _ in range(len(links)-i-1):
                    content.find("tbody",class_="table-list").find_all("tr")[1].decompose()

                content.find("div",class_="info").name = "divcontent"
                #content.find("div",class_="avatar").decompose()
                content.td.divcontent["class"] = 'invest-info'

                content.a["href"] = prefix + content.a["href"]
                content.img['src'] = content.img['data-echo']

                content.img['style'] = 'width:60px'

                content.td.divcontent.a["href"] = prefix + content.td.divcontent.a["href"]
                taglists = content.find("div",class_="taglist").find_all("span")
                for j in range(0,len(taglists)):
                    taglists[j].a['href'] = prefix + taglists[j].a['href']
                content.find("td",class_='link-list').a["href"] = prefix + content.find("td",class_='link-list').a["href"]
                content.find("td",class_='link-list').name = 'round-link-list'
                invests_links = content.find("td",class_='link-list').find_all("a")
                content.find("round-link-list",class_='link-list').name = 'td'
                for j in range(0,len(invests_links)):
                    invests_links[j]['href'] = prefix + invests_links[j]['href']
                
                firstpic = content.img['src']
              
                article = content.text.strip()
                markdown = content.prettify()
                    
                type3 = u""

                items.append(self.make_requests_from_url(company_url).replace(callback=self.parse_news, meta={'day': day, 'title': title,'url':[company_url,firstpic],'article':article,'markdown':markdown,'type2':type2,'type3':type3}))
            if url == 'https://www.vc.cn/investments':
                page = 1
            else:
                page = int(re.search("(.*)(\d)",url).group(2))
            
            if need_parse_next_page and page < 2:#need_parse_next_page:
                page = page + 1
                if page == 2:
                    page_next = "https://www.vc.cn/investments?action=index&controller=investments&page=2&type=investment"
                else:
                    page_next = re.search("(.*)(\d)",url).group(1) + str(page) + '&type=investment'
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
        day = title = keywords = url = article = ''
        url = response.meta['url']
        day = response.meta['day']
        title = response.meta['title']
        type2 = response.meta['type2']
        type3 = response.meta['type3']
        article = response.meta['article']
        markdown = response.meta['markdown']


        item['title'] = title
        item['day'] = day
        item['type1'] = u'投融事件'
        item['type2'] = type2
        item['type3'] = type3
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'创投圈'
        item['markdown'] = markdown
        item['abstract'] = ''
        return item

        
        