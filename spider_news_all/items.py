# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SpiderNewsAllItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    day = scrapy.Field()
    title = scrapy.Field()
    type1 = scrapy.Field()
    type2 = scrapy.Field()
    type3 = scrapy.Field()
    keywords = scrapy.Field()
    url = scrapy.Field()
    article = scrapy.Field()
    site = scrapy.Field()
    markdown = scrapy.Field()
    pass
