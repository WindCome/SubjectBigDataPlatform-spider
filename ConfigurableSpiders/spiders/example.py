# -*- coding: utf-8 -*-
import scrapy
from lxml import etree

from ConfigurableSpiders.items import ConfigurablespidersItem
from ConfigurableSpiders.supports.analyze import StructuringParser
from ConfigurableSpiders.supports.configure import ConfigPool, OutputConfig
from ConfigurableSpiders.supports.tools import LxmlHelper, FileHelper


class ExampleSpider(scrapy.Spider, OutputConfig):
    name = 'ConfigurableSpiders'

    def __init__(self, classes=None, id=None, **kwargs):
        super().__init__(**kwargs)
        self.config_pool = ConfigPool(classes=classes, id=id)

    def start_requests(self):
        requests = []
        for url in self.config_pool.get_root_paths():
            requests.append(scrapy.Request(url))
        return requests

    def parse(self, response):
        current_url = response.request.url
        print("requesting " + response.request.url)
        steps = self.config_pool.get_steps(current_url)
        if steps is None:
            print("获取" + current_url + "的解析步骤时发生错误")
            yield

        # 找出进一步爬取的链接
        if 'xpath' in steps.keys():
            doc = etree.HTML(response.body)
            xpath_strs = steps['xpath']
            for xpath_str in xpath_strs:
                a_tags = doc.xpath(xpath_str)
                if len(a_tags) == 0:
                    print("xpath:" + xpath_str + "在" + response.url + "中无匹配节点")
                for a_tag in a_tags:
                    target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                    target_url = response.urljoin(target_url)
                    self.config_pool.accpet_xpath_crawl_result(current_url, xpath_str, target_url)
                    yield scrapy.Request(target_url)

        # 解析当前页面
        if 'mapping' in steps.keys():
            structuring_data = StructuringParser.parse(response)
            data_mapper = self.config_pool.get_data_mapper(current_url)
            mapper_name = data_mapper.set_structuring_data(structuring_data)
            print(current_url + "使用的数据映射器类型为" + mapper_name)
            for record in data_mapper:
                item = ConfigurablespidersItem()
                item['url'] = current_url
                item['data_item'] = record
                yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_config = self.config_pool.get_output_config(item_from_url)
        if output_config is not None and pipeline_mark in output_config.keys():
            return output_config[pipeline_mark]

    def close(self, reason):
        FileHelper.clear_tmp_file()
