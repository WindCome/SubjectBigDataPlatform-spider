# -*- coding: utf-8 -*-
import re
import urllib

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
        else:
            # 找出子页面的链接
            if steps.has_child_page():
                xpath_str = 'xpath'
                regex_str = 'regex'
                doc = etree.HTML(response.body.decode(response.encoding))
                search_child_page_conditions = steps.search_conditions
                for condition in search_child_page_conditions:
                    if xpath_str in condition.keys():
                        a_tags = doc.xpath(condition[xpath_str])
                        if len(a_tags) == 0:
                            print("xpath:" + condition[xpath_str] + "在" + response.url + "中无匹配节点")
                        for a_tag in a_tags:
                            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                            target_url = response.urljoin(target_url)
                            # url含中文字符预处理
                            target_url = urllib.parse.quote(target_url, safe=";/?:@&=+$,", encoding="utf-8")
                            if regex_str in condition.keys() and ExampleSpider.is_node_match_regex(a_tag, condition[regex_str]) or\
                                    regex_str not in condition.keys():
                                self.config_pool.accpet_xpath_crawl_result(current_url, condition, target_url)
                                yield scrapy.Request(target_url)

            # 解析当前页面
            if steps.is_data_page():
                structuring_data = StructuringParser.parse(response)
                data_mapper = steps.data_mapper
                mapper_name = data_mapper.set_structuring_data(structuring_data)
                print(current_url + "使用的数据映射器类型为" + mapper_name)
                for record in data_mapper:
                    item = ConfigurablespidersItem()
                    item['url'] = current_url
                    item['data_item'] = record
                    yield item

    @staticmethod
    def is_node_match_regex(node, regex_str):
        text = LxmlHelper.get_text_of_node(node)
        search = re.search(regex_str, text)
        if search is not None:
            return True
        return False

    def get_output_config(self, pipeline_mark, item_from_url):
        steps = self.config_pool.get_steps(item_from_url)
        if steps is None:
            return None
        return steps.get_output_config(pipeline_mark,item_from_url)

    def close(self, reason):
        FileHelper.clear_tmp_file()
