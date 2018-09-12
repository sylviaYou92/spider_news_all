# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 09:56:29 2018

@author: yangwn
"""

import scrapy
from bs4 import BeautifulSoup
from scrapy import log
from datetime import date, timedelta
import re
from spider_news_all.items import SpiderNewsAllItem
import datetime
import time
from tomd import Tomd
import MySQLdb
import threading
from spider_news_all.config import SpiderNewsAllConfig

class OschinaSpider(scrapy.Spider):
    name = "oschina"
    site_name = "oschina"
    allowed_domains = ["oschina.net"]###?
    start_urls = (
            "https://www.oschina.net/news/widgets/_news_index_generic_list?p=1&type=ajax",
            "https://www.oschina.net/news/widgets/_news_index_industry_list?p=1&type=ajax",
            "https://www.oschina.net/news/widgets/_news_index_programming_language_list?p=1&type=ajax",
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
        for start_url in [re.match("(.+)\?p=\d+&type=ajax",url).group(1) for url in self.start_urls]:
            if self.record_url.get(start_url)==None:
                self.record_url.setdefault(start_url,None)
                self.lock.acquire()
                self.cursor.execute("INSERT INTO url_record (site_name, start_url, latest_url) VALUES ('%s','%s','%s')"%(self.site_name,start_url,None))
                self.lock.release()
        self.updated_record_url = self.record_url.copy()
    
    def time_convert(self,old_string,time_now):
        if type(old_string)==unicode:
            old_string = old_string.encode("utf-8")
        old_string = re.sub("：",":",old_string)
        new_string = old_string
        #new_stirng = time.strftime("%Y-%m-%d %H:%M:%S")
        if re.match("今天",old_string):
            new_string = re.sub("今天",time_now.strftime("%Y-%m-%d"),old_string)
        elif re.match("昨天",old_string):
            new_string = re.sub("昨天",(time_now + timedelta(days = -1)).strftime("%Y-%m-%d"),old_string)
        elif re.match("前天",old_string):
            new_string = re.sub("前天",(time_now + timedelta(days = -2)).strftime("%Y-%m-%d"),old_string)
        elif re.search("(\d+)天前",old_string):
            delta_day = int(re.search("(\d+)天前",old_string).group(1))
            new_string = re.sub("\d+天前",(time_now + timedelta(days = -delta_day)).strftime("%Y-%m-%d"),old_string)
        elif re.search("(\d+)小时前",old_string):
            delta_hour = int(re.search("(\d+)小时前",old_string).group(1))
            new_string = re.sub("\d+小时前",(time_now + timedelta(hours = -delta_hour)).strftime("%Y-%m-%d %H:%M"),old_string)
        elif re.search("(\d+)分钟前",old_string):
            delta_min =  int(re.search("(\d+)分钟前",old_string).group(1))
            new_string = (time_now-datetime.timedelta(minutes=delta_min)).strftime("%Y-%m-%d %H:%M")
        elif re.match("\d+/\d+",old_string):
            if len(re.findall("/",old_string)) == 1:
                month = int(re.match("(\d+)/(\d+)",old_string).group(1))
                date = int(re.match("(\d+)/(\d+)",old_string).group(2))
                if month > time_now.month:
                    year = time_now.year-1
                else:
                    year = time_now.year
                new_string = re.sub("\d+/\d+",datetime.datetime(year,month,date).strftime("%Y-%m-%d"),old_string)
            elif len(re.findall("/"),old_string) == 2:
                month = int(re.match("(\d+)/(\d+)/(\d+)",old_string).group(2))
                date = int(re.match("(\d+)/(\d+)/(\d+)",old_string).group(3))
                if len(re.match("(\d+)/(\d+)/(\d+)","old_string").group(1))==2:
                    year = time_now.year/100*100+int(re.match("(\d+)/(\d+)/(\d+)",old_string).group(1))
                elif len(re.match("(\d+)/(\d+)/(\d+)","old_string").group(1))==4:
                    year = int(re.match("(\d+)/(\d+)/(\d+)",old_string).group(1))
                new_string = re.sub("\d+/\d+/\d+",datetime.datetime(year,month,date).strftime("%Y-%m-%d"),old_string)
        elif re.match("\d+年\d+月\d+日",old_string):
            year = int(re.match("(\d+)年(\d+)月(\d+)日",old_string).group(1))
            month = int(re.match("(\d+)年(\d+)月(\d+)日",old_string).group(2))
            date = int(re.match("(\d+)年(\d+)月(\d+)日",old_string).group(3))
            new_string = re.sub("\d+年\d+月\d+日",datetime.datetime(year,month,date).strftime("%Y-%m-%d"),old_string)
        elif re.match("刚刚",old_string):
            new_string = time_now.strftime("%Y-%m-%d %H:%M:%S")
        
        if re.match("\d{4}-\d+-\d+ \d+:\d+:\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d %H:%M:%S")))
        elif re.match("\d{4}-\d+-\d+ \d+:\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d %H:%M")))
        elif re.match("\d{4}-\d+-\d+",new_string):
            time_stamp = int(time.mktime(time.strptime(new_string,"%Y-%m-%d")))

        return time_stamp


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
#        try:
#            items_keywords = soup.find(class_='ar_keywords').find_all('a')
#            for i in range(0, len(items_keywords)):
#                keywords += items_keywords[i].text.strip() + ' '
#        except:
#            log.msg("News " + title + " dont has keywords!", level=log.INFO)
        

        
        try:
        ##################################
        # 分情况获取储存新闻内容的标签
                # "码云推荐"，获取项目简介（通常是 README.md 文档内容）
            if re.search("translate",url):
                content = soup.find_all("div",class_ = "translate-content")
                article = [str(tag) for tag in content]
                markdown = "".join((article)).decode('utf-8')  # html code
#                markdown = Tomd(markdown).markdown  # 转markdown
                article = [tag.text.strip() for tag in content]
                article = ''.join(article)
            else:
                if re.match("https://gitee.com",url):
                    article = soup.find("div",class_="file_content markdown-body")# CSS选择器：#git-readme > div > div.file_content.markdown-body
                # "码云周刊"
                elif re.match("https://blog.gitee.com",url):
                    article = soup.find("div",class_="entry-content")
                elif re.search("translate",url):
                    article = soup.find_all("div",class_ = "translate-content")
                # 其他常见页面
                elif soup.find("div",class_= ["content","box-aw main"]):
                    article = soup.find("div",class_= ["content","box-aw main"])
                else:
                    article = soup.find("section",class_= ["wrap cke_editable cke_editable_themed cke_contents_ltr cke_show_borders clearfix"])
                    
                if article and not article.find("div",class_="ad-wrap")==None:
                    article.find("div",class_="ad-wrap").extract()
                
                markdown = str(article).decode('utf-8') # html code 
#                markdown = Tomd(str(article)).markdown.decode("utf-8")
                article = article.text.strip() #提取标签文本
        except:
            log.msg("News " + title + " dont has article!", level=log.INFO)
        item['title'] = title
        item['day'] = day
        item['_type'] = _type
        item['url'] = url
        item['keywords'] = keywords
        item['article'] = article
        item['site'] = u'开源中国'
        item['markdown'] = markdown
        return item

    def get_type_from_url(self, url,url_news):
        if re.match("https://www.oschina.net/event/",url_news):
            return u'活动资讯'
        elif re.match("https://www.oschina.net/p",url_news):
            return u'开源项目'
        elif 'generic' in url:
            return u'综合资讯'
        elif 'industry' in url:
            return u'行业资讯'
        elif 'programming_language' in url:
            return u'编程语言'
        else:
            return ''



    def parse(self, response):
        log.msg("Start to parse page " + response.url, level=log.INFO)
        url = response.url
#        if url in self.start_urls:
#            self.crawl_index[self.start_urls.index(url)]=True
#            self.all_crawled = not False in self.crawl_index
        start_url = re.match("(.+)\?p=\d+&type=ajax",url).group(1)    
        items = []
        time_now = datetime.datetime.now()
        try:
            response = response.body
            soup = BeautifulSoup(response)
#            lists = soup.find(class_='list')
            links = soup.find_all("div",class_ = "item news-item")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        need_parse_next_page = True
        if len(links) > 0:
            is_first = True
            for i in range(0, len(links)):
                if not u"广告" in links[i].find("div",class_ = "extra").get_text() and not "topic" in links[i].a['href'] and not "blog" in links[i].a['href']: #如果是广告、话题、博客（乱弹），就不存
                    url_news = links[i].a['href'].strip() #获取新闻内容页链接
                    if re.match("https://www.oschina.net/question",url_news):
                        continue
                    if not re.match("http",url_news): #必要时对不完整的新闻链接作补充修改
                        url_news = "https://www.oschina.net"+url_news
                        
                    if url in self.start_urls and is_first:
                        self.updated_record_url[start_url] = url_news
                        is_first = False
                    if url_news == self.record_url[start_url]:
                        need_parse_next_page = False
                        break

                    _type = self.get_type_from_url(url,url_news)
                    
                    day = links[i].select('div.item')[1].text.strip() ##获取新闻发布时间
                    day = self.time_convert(day,time_now)
                    title = links[i].a['title'] #获取首页新闻标题
                    items.append(self.make_requests_from_url(url_news).replace(callback=self.parse_news, meta={'_type': _type, 'day': day, 'title': title}))
            #这里是动态加载，没有下一页按钮
            page = int(re.search("list\?p=(\d+)+&type=ajax",url).group(1))
            if need_parse_next_page and page < 3:#need_parse_next_page:
                page += 1
                page_next = re.sub("list\?p=(\d)+&type=ajax","list?p=%s&type=ajax"%str(page),url)
                if need_parse_next_page:
                    items.append(self.make_requests_from_url(page_next))
            else:
                self.lock.acquire()
                self.cursor.execute("UPDATE url_record SET latest_url='%s' WHERE site_name='%s' AND start_url='%s'"%(self.updated_record_url[start_url],self.site_name,start_url))
                self.lock.release()
            
#            if (soup.find('a', text=u'下一页')['href'].startswith('http://')):
#                page_next = soup.find('a', text=u'下一页')['href']
#                if need_parse_next_page:
#                    items.append(self.make_requests_from_url(page_next))
            
            return items
        
        