# -*- coding: utf-8 -*-

from scrapy import cmdline
import time

# 爬虫列表
spider_name_list = [
    "cjw",
    "oschina"
]

if __name__ == '__main__':
    for spider_name in spider_name_list:
        args = ["scrapy", "crawl", spider_name]
        try:
            cmdline.execute(args)
            time.sleep(180)
        except:
            pass
