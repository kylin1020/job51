# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Job51Item(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class Job51CompanyIndustry(scrapy.Item):
    tag = scrapy.Field()
    city = scrapy.Field()
    district_name = scrapy.Field()
    company_name = scrapy.Field()
    address = scrapy.Field()
    category = scrapy.Field()
    industry = scrapy.Field()
    release_time = scrapy.Field()
    longitude = scrapy.Field()
    latitude = scrapy.Field()
    create_time = scrapy.Field()
    url = scrapy.Field()
    category_industry_map = scrapy.Field()
    position = scrapy.Field()
    