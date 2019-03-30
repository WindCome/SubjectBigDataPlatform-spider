# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import json
import os

from redis import Redis

from ConfigurableSpiders.items import ConfigurablespidersItem
from ConfigurableSpiders.supports.configure import OutputConfig
from ConfigurableSpiders.supports.tools import FileHelper


class CSVPipeline(object):
    # 导入csv
    def __init__(self):
        self.files = {}
        self.writers = {}
        self.csv_output_path = FileHelper.get_project_root_path() + os.sep + 'output'
        FileHelper.check_exist_or_create(self.csv_output_path)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        for key in self.files.keys():
            self.files[key].close()

    def process_item(self, item, spider):
        data = item
        item_form_url = None
        csv_filename = None
        if isinstance(item, ConfigurablespidersItem):
            data = item['data_item']
            item_form_url = item['url']
        if isinstance(spider, OutputConfig):
            csv_filename = spider.get_output_config("csv", item_form_url)
        if csv_filename is not None and len(csv_filename) != 0:
            file = self.__get_file_object(csv_filename)
            writer = self.__get_writer_object(csv_filename)
            if file is None:
                file = open(self.csv_output_path + os.sep + csv_filename + '.csv', 'w', newline="",
                            encoding='utf-8-sig')
                writer = csv.DictWriter(file, fieldnames=data.keys())
                writer.writeheader()
                self.files[csv_filename] = file
                self.writers[csv_filename] = writer
            writer.writerow(data)
            file.flush()
        return item

    def __get_file_object(self, url):
        if url in self.files.keys():
            return self.files[url]

    def __get_writer_object(self, url):
        if url in self.writers.keys():
            return self.writers[url]


class RedisPipeline(object):
    # 导入redis

    def __init__(self, host, password, port):
        self.host = host
        self.password = password
        self.port = port
        self.redis_db = Redis(host=host, password=password, port=port, db=0)
        self.first = []  # 标记是否为第一个处理的item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('REDIS_HOST'),
            password=crawler.settings.get('REDIS_PASSWORD'),
            port=crawler.settings.get('REDIS_PORT')
        )

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        data = item
        item_form_url = None
        redis_key = None
        if isinstance(item, ConfigurablespidersItem):
            data = item['data_item']
            item_form_url = item['url']
        if isinstance(spider, OutputConfig):
            redis_key = spider.get_output_config("redis", item_form_url)
        if redis_key is not None and len(redis_key) != 0:
            if redis_key not in self.first:
                # 存入第一项数据前先清空原有数据
                self.first.append(redis_key)
                self.redis_db.delete(redis_key)
            json_data = json.dumps(data, ensure_ascii=False)
            self.redis_db.lpush(redis_key, json_data)
        return item
