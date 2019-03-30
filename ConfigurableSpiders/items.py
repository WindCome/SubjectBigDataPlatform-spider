# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ConfigurablespidersItem(scrapy.Item):
    url = scrapy.Field()
    data_item = scrapy.Field()
