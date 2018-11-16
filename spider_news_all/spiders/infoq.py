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
            if content.img:
                url = [url,content.img["src"]]
            else:
                url = [url,'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAN4AAABCCAYAAAA4w2iPAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAA3NpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuNi1jMDY3IDc5LjE1Nzc0NywgMjAxNS8wMy8zMC0yMzo0MDo0MiAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0UmVmPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VSZWYjIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtcE1NOk9yaWdpbmFsRG9jdW1lbnRJRD0ieG1wLmRpZDpkODJhNjkwMy02N2Y0LTQzZDUtYjQ4Zi01YWY3MDFmYzYwMjgiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6QkE3RDVDM0RBOEREMTFFOEE4NDZFOTE1MUQ2NkUyQjkiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6QkE3RDVDM0NBOEREMTFFOEE4NDZFOTE1MUQ2NkUyQjkiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENDIChNYWNpbnRvc2gpIj4gPHhtcE1NOkRlcml2ZWRGcm9tIHN0UmVmOmluc3RhbmNlSUQ9InhtcC5paWQ6Y2Q4MTJmYzctNDhhNy00YzM5LWE0MzgtYjk3ZTdkMTFhYjFhIiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOmQ4MmE2OTAzLTY3ZjQtNDNkNS1iNDhmLTVhZjcwMWZjNjAyOCIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PqHPQo8AABDkSURBVHja7F0JdBRFGq6eJBBAhSQgImfwFkUhoLJRwWNBwX3itSCXQNgFvH0r6j4xBFxXcXmIq7siDyIoKHIIrLKugqIswyIeBLwPToUEkggBEpYlM73fn65gGKZnuqu6p3tI/e/9r5PprrP/r/6/qv76W2OKFPmcWjyRm4FLN3AO+AJwW86Z4JPBAf7o/8CHwD+Dd4C3gH8Arwd/VPpo8IBf2qS1vmP2UsG083fOGT5fiYVB6MczcOkCPht8OrgpFwqiKnAFuAS8FfwN+HP03yHVc6ZgOweX28D9wJfUAZcohcEbwIvACwHCzV4DTxdMOxGCU1DPwXYeLmPBt3Cw2aEQeCD6cJGC2VGwEbhuBd8J7ulycUHwM+AlAGE40W1NVa9bCHBk+kwFD5fIJgV8kurNo6Aj7TYRfF6Ciszl/APKfhjge0MBz9+ga4/L++COqjccAVw2Li+Cf+1RFc4EL0Y9VuB6FwD4fSIKDahXbwt0pKHeUaBzDHR34PKlh6CrS1SHItQpT2k8/xGZQueobpAGXAM+v7rTZ1VrDJ6J+nXn2i+kNJ732u5UXO5WPSENOhLuZT4EXV0aDV7CBwgFPI9pMLiB6gZp0L0Fvi4JqvsbGiDcAp8CnnXqp7pACnS0ivsq+KokqjYNEHNQd00Bzxszkzr+EtUTUvQE+MYkrPdA8HinM1WLK9aoFfvFC0WEipjhtrSdGV4stbS+nmi7G3B5OImbUIA2rCl9NLhKAS+x1FYwHbmE9ds5Z/iq+tpxENgsXGY6nC2tNn4M/gK8DVzJf6fBsQO4EzN8O1MctAxnoy2dAL6DdhLOf/XlNFzSBg4aVqWAZ58aCab7e30GHacnwS0dyossh7+CF8VzeAZITmGGryetRF/sQNntuMn5iAWwkfP2BGa4ErameSLjXk64txiXsJrjWSNRM3Nlfe40CH9nXEY5kBWdNhgC7grAvWTllAGe2Q+eRWnAw3gesnQf2tTWgoaj934Pn1qURDzyLrhHUmo87itJHUoeJOSc3Jhrb3ohu8Hf0fwJ2sapYyCVgulKXO4HajOdiKCFn4u4mUUubeRh06SOWUb9sIebZZv53PIj9M8ul18VORzIrghuJHMdINopkhjp6BDAKwDMB8zYyugsUZd08CTwiBjP9OLvpBfMyw8BxMjTPz+RzKa6LBjpVlRzFJoOoSiJyIuEaigzvNcvtPBCw0izGtdC8GvIr1qiKQd8NOiQldIHPIgZWxwZFpJlckB2j8iLjsksIVMI/bPDYW1Hx6NkVzGpflcBPBWy9UEeP6JOV+LPD/kgJUqDuVP1HpP7pAgOEOhM7tN7OOK2xkvntq5dWlqrLbhTMs0Tfmtzshzgow/xeOQzCsL17zhCPQaX06LcyhBs/xjkGUvrLUWdimxot5HgPzDjzJ8T1IXzBORPAHwc9dnkUN5jJbVdMfh6J0BXB3wVAM31XIu2EMyGTMnfg/9kcv9bmppA0+UAfJ9GmKEkWw+C17h6Hg95N8Nlr6BAUOfcy0HXyIF+pzNXd6HO02PUt0hyNLRLI1Cf2RZAR2fTnmfG6Ws3ifqI5kUPo157RTOBcJNwkhnbXKIufQCUd12ae/bjZqcobUfdOpjM8TQ+x/sV+HU+DdgPXsfnqbToc7VfF1cIaAvA0xwCXa0GfAFCPCKJ5rJpYBp4ViUAdLV99DvwJpR7pUQ+V0mCboFboOOabzkub0pk0R7gjbpSCi1Hiuxm8GK+qEPnCy+lhRlm7OH2xzNr/Aq8uXwu5wZNh1BdkASgo5FxOZ8jawkuvg2BHXUQPSJzg0TZJLj5CWijrDeK6fwVwKoAD+H9SD6ft4MvB5+B3/9Jz/h1VdPN827k9DqDmwJ+BR3NKd/jJrdXRIPyTNSlBczOp2ymvUKi3OXQSN+63TiUsYmvdPYSzKI3M1ZtWQwA7uIm93FUXzfQe0Cg+kCg3vEh6Go9+Lv4pEpPok6l6KtZFudPtOcps2Q/L8GWlSjwuqKtqQBwdcQcj0zQDXHSbqzPG+h+PQ9W6ENtPMPGnK8HE3e+D3HzOlH0D4m06SbzblqFXRaFV/P7tIC3qj67jPWlVVeM5Pt8pO3Iy2OAD/uKgPQq6tcZ/RXPA0Rmq+OTRMa+RFml0FpfM/EASxdxINU1Lyl8Y/9oD0Mb0lbCH8Ev1mfgUdspzsbCiBEw2r4aeYLcIlAGrZzFEtQf6oCO9nimOtAu8kqh0HXkrnSYGZvn5HBwLZNbaSSfw8nMWPV0a37uxWmNTySA187OwwDlFICPLIfnkwF4K7jdT8K0CyNuFQ86RPOIm7nJKLrlcHld4CHvfBNN1EEQePlWN8iZsV8pc/SI4nOOR3nfmrSBFpXobBktlLQSLCMP+byAMj5zCXhfeSBfX0ukbSeQ5j/gSX4G3hHwsGjRqvEbHc1YSwxBmM2MFcBTBcrI8YmJSSHuhgkmp03v0eiTmEdvcJ/Cm7+MspZzkIosKtC2xng+4MXSjKK0zYPu3+Yk8KDRSCl0M+k72l6gyGqH/Ly4km8lRDyeoTNZowXL8EvEsPskFiRGxgNdRH+VM8PHc51gef0B3o5xFh1EabcHfV8qkbZhlN8oTueqKEyxWF/mMveEX4FHGu1ZG8JEvp0bBMppDiFq4rG2S5PQdi+i7XPsJiJznRln1UT8IGnkHhrjvoy5vNeDVyBzXKhZtO4FP2DCdDaQfDgn+9XUfEvggx5kPonsfZGJutXDtpLJJ+KETZpLOJwC+vcngJ4c2KcJJCevIrPN4yYsuUjmuwlpURZQSq30qV+B94lAmo8Fy8ryGHjXC6abBvDIeu6TB89jvA/s0AUAbRsCL6vfZHrAAHO9PnyAovn7SgDyKf47LeBV+NXUFIlf/51gWV5/OKSH4AsvlC2YWxWvCSa/zOT3yiQDj0xclmoT0FF4in8x4/T91eBz69ymRa6xfgXefoE0FUn2wml+Ry+9q0DSzxw8PS7qKdLN5Pcqibo09+A1ZEmkrYoCOpruUNgHOqlO30iM9I6hLZOOfgVeRX0AHjOW3kUiFa9xsA5rBdOZrWwelKjLqR68gxYSaaN5PV3IAVkA83K/iYWV6lfg2T6cCw2gJyHwOgim+9KpCqDfSDiKBZK2N/n9R4nqZHvwDmTKjNZvZczYUsmIog1pRZhWsL9RUca8pUzBdHscrodIfmbaSSZ2y/kevAN77mK6wbrB0eaztGdH04B3ALQBHIBN8De5J77NjHg5z6aGjvAvEWm1F+2YY5eapsW8r0iKRPe8nHbsFsmvocnv2yXqcWmiX0A4xD+goh9vb+lx7C8tcHwfwLw8BJD15XPnug4gtQe76fNkM1J1XY8ozJrFpgW0XnEHh7DesCZ/jQP2aGIFYk6iK2pOz2dFTgQ0NvndniPDseLXpfnjua3KHgsWJ6LzsybmXqGHhK0Okt2okcQAvs8BPtKkFKDrMj5I0cb6Ytyr8XN1dR8PoEsPh+ztT5KGBahp7lNku7ywfhy4j+0n3wFdNORgM4dn1C2OGXD1aI/olmbfGGc36qFjn4unOepQAFqEonflJaj/75dJrBlfB2Ym4CMz9CXOx5HvNtBJQwIgDUXShqrtf8AzkBKgE8MfxDZHwi2PAXWMNxExiLSONYDU5KvrMTWBibXRI16dbfZbZ9N6mA+QUTVe2fjgvqyC3EPIrpHgIHBzooCHOgp/pw/64UBZfrA8ygJKNjcnY9FW9e0Ea5RmSTAjHtFStHib8wctAfr4ci52VgB1Ecfm/8YYgL5AHbsLgqFZ1qTcAeX5wdddNTMn5Y6BZm4soe7MVpZp7y5eIN+NCnjeUpFgOsdOVbQaUngONK8mIHg/x7hHjtvdhSsVrokh6irwUMZTUuk19jcTE7Mo2kQHmvBkvthC175qO8FDgrn2qaA5drZjldCFzbptMcywGRA9YedjaL3mWRNzp7rV78h7BspoKgG6I9DIc+0kASBpAYsOU9OG/RQFPA9p1ysjqgG+A/YFU28ETdXNGdzpN4nJnmYaDr/sseARgG+dVL3C7H6Yg9e4YGJeh7ylvmCEtq0WSQfwkXlO0RQGKeB5rvbEQg8AfH9xwMzsiDnmmYL1nhvn/jjZngFA3gZQchwEXXfk+SaTXdvWas7V2SaYm+QXShHkGqs5nvfm5kJoHdvfVwdgegI4bYvnjhR20QJ4ZwvWuRLlxgw6C1NsLUy67RD09hLqOA3p1wEwtyO/RZKgG4K8ZiNPqa/EagG2CXX5Jga4rMTVXKSA5z09x4zoXXatDw3AoaMnnQS1Xf9wKCwW8VmzuJWh1Zh0KyRt4VQ9xBYCxKuQHwFwt03AtUQe85FHL0csc63m5H4sqo2rGT09Y7SBPlUBz2OC5jh82uBZRdBgto8HIc35rQYXziqeNzLPJujORdoFElra0ncHAJKVAMwGaBrpqNjIgz6Esgv5rYfw02C1APlXm4CNTnzcDjG/G4DLYQ65TUDbLUOZ38WZx5nG1axLCnj+MDfvgbkZFEkbDodHArinlMzLu80i6K4B6JZDW6aJCZ/2PQaLIuttY/10je0gzeVAVwUAQHLBIp6bVZBbybc1DvP7DVFOJsDWhDnso4R27Leg7aw3RIm9L7TeWgi06Al60ny3njZoVjlANToG4M4CQFfCvFwJ0DWUGCTG2Hm+LD9YDE3xkBvjla6zkwDEduCzOLej35g7joF0Wj/TqcyUxvOP1rsJWu8LUaEBmDDS69NbDpr5PPIih9xirgmaQwu0AeCaStcxoK3DIPG+3XQwz56BidgXwLg2Wd8PAN2Shdl2mLGD0Z43lMY7cbTeVxDsQnkJYanQgO3Bl9HKJ7gTQNnUgYHhMFj8u3ca6wPNtzmpX5LO0mHGLsYgshoAzFTAO0EI87RRAN8WX2rkgDYAg0O5aHpoiTDmSReBdyf7e4LmvgJcCgAuBQBbK+CdGCZnDnifn+oUCAQmAHTLZPPBfK8SEncmwLcz6V+UXrPQcyM04E8A4Bbw0wBhtgJe8pqc+6BdOgN8B30CuueK542c5FR+0HwHIXUdYHauP1HeGQCYDR4HEG7JLMg9nFWQWwwQ9lTASz7w/QjwZQN8XppleiAl8AhAd6/TGdP+W/mE4KUA3xQmENjK55qwgW6EKQwq4CUn+MoAvjbgFYkuG4CvAuh6ow6T3SwH4BunpbCuAOC2E2u6wN4z29xXwEsO8FWXzMvrDRAMBRj2J6BIHUB/F9wCZa9MRBshoEUAYDYAOLZmkzrxIIFpz15xVPNqLK6VoICXHACcCzBkYL41xaW5HwHuYwC8M4DeB+VVJbqNAOD08oJgUwBwOICww3XABdhmlJWHMjMA/GH4e5QT4KN847mVEaWiw5cKmiNWQn/vFcx/o1ijhcqy4qq1NZHtMAEfHSylozbjWg0pHKrr+j3gi8mDX8KkLAEvwwidj/z3+GGQgdDS6fU53Ln5UfzdX9fZ6bKnCtDGELQbmbRv4e8nI52t8X8hyizRw2ypTJ8i7z9be0xRUhNAmANBGagznUIttMHfFEA1nVszJKw016Cv6x7E2y7B9XuAjQKrLvRCs4kSnaVD2yhcHgW9bY+/KaBumm6EzgtwWQ5pRntrfTjJYbkIfy8BsD61WE428n4fAOwgYLZWQoOepICnSJEo0CfmPg1N+4Ad526YmbNgtlo63a7meIoURTN5JwQfApAywEugnqzEP6Wzeg9at0gVKVIUz/xsAFhNAg+tmW9G13braW9SAU+RIndASM7RowHC3uDzdfooic7StBTWE/NIy0GQ/i/AAKbItKgRaN9XAAAAAElFTkSuQmCC']
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

        
