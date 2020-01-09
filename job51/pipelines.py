# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime

import pymongo

from job51.items import Job51CompanyIndustry


class Job51Pipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipelines(object):
    def __init__(self, host, port, db):
        self.mongo_db = pymongo.MongoClient(host=host, port=port)[db]

    @classmethod
    def from_crawler(cls, engine):
        return cls(
            engine.setting.get('MONGO_HOST'),
            engine.setting.get('MONGO_PORT'),
            engine.setting.get('MONGO_DB')
        )

    def process_item(self, item, spider):
        item['create_time'] = datetime.now()

        if item.__class__.__name__ == Job51CompanyIndustry.__name__:
            self.handle_company_industry(item)
        return item

    def handle_company_industry(self, item):
        remote_item = self.mongo_db[Job51CompanyIndustry.__name__].find_one(
                {
                    'url': item['url'],
                    'tag': item['tag']
                }
            )
        if remote_item is None:
            self.mongo_db[Job51CompanyIndustry.__name__].insert(dict(item))
