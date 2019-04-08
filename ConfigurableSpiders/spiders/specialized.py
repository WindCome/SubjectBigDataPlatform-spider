import re
from lxml import etree

import scrapy

from ConfigurableSpiders.items import ConfigurablespidersItem
from ConfigurableSpiders.supports.analyze import StructuringParser
from ConfigurableSpiders.supports.configure import OutputConfig
from ConfigurableSpiders.supports.mapper import DataMapper
from ConfigurableSpiders.supports.tools import LxmlHelper, StringHelper, FileHelper


class GG_JX_GJJGHJC(scrapy.Spider, OutputConfig):
    # 1-国家级规划教材
    name = 'GG_JX_GJJGHJC'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.gov.cn/was5/web/search?searchword=%E6%95%99%E8%82%B2%E6%9C%AC%E7%A7%91%E5%9B%BD%E5%AE%B6%E7%BA%A7%E8%A7%84%E5%88%92%E6%95%99%E6%9D%90%E4%B9%A6%E7%9B%AE&channelid=224838"

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="m_search_list"]//a')
        regex_str = '第.批.*十二五.*普通高等教育本科国家级规划教材书目'
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search(regex_str, text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.find_data)

    def find_data(self, response):
        a_tags = response.xpath('//*[@id="xxgk_content_div"]//a')
        for a_tag in a_tags:
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'书名': 'SM',
                           '主要作者': 'ZYZZ',
                           '第一作者单位': 'XXMC',
                           '出版社': 'CBS',
                           '备注': 'BZ'}
        structuring_data = StructuringParser.parse(response)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        mapper_name = data_mapper.set_structuring_data(structuring_data)
        print(response.request.url + "使用的数据映射器类型为" + mapper_name)
        for record in data_mapper:
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade1'}  # 'csv': 'upgrade1',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_PT_GJKJJD(scrapy.Spider, OutputConfig):
    # 6-国家国际科技合作基地
    name = 'GG_PT_GJKJJD'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.cistc.gov.cn/InterCooperationBase/details.asp?column=741&id=81467"
        self.type_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//a')
        target_type = ['国家国际创新园', '国家国际联合研究中心',
                       '国家国际技术转移中心', '示范型国家国际科技合作基地']
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            if text is not None:
                for i in range(0, len(target_type)):
                    if StringHelper.match_string(text, target_type[i]):
                        self.type_mapping[target_url] = target_type[i]
                        yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'国合基地名称': 'JDMC',
                           '依托单位名称': 'XXMC',
                           '所在省份': 'SZSF',
                           '国合基地认定年份': 'PDSJ'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['JDLB'] = '国家国际科技合作基地-'+self.type_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade6'}  #'csv': 'upgrade6',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class PostgraduateEduAward(scrapy.Spider):
    # 9-研究生教育成果奖
    name = 'PostgraduateEduAward'

    start_urls = ['http://www.csadge.edu.cn/column/hjcg_jj/3']

    def parse(self, response):
        pass


class GG_JX_JPSPGKK(scrapy.Spider, OutputConfig):
    # 10-精品视频公共课
    name = 'GG_JX_JPSPGKK'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.gov.cn/jyb_xxgk/zdgk_sxml/sxml_gdjy/gdjy_jpgkk/jpspgkk_kcmd/"
        self.pc_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="list"]//a')
        regex_str = '关于公布(第.批|.批)“精品视频公开课”名单的通知'
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search(regex_str, text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.find_data)

    def find_data(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="xxgk_content_div"]//a')
        regex_str = '第.批|.批'
        found = False
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search(regex_str, text)
            if search is not None:
                found = True
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.pc_mapping[target_url] = search.group()
                yield scrapy.Request(url=target_url, callback=self.parse)
        if not found:
            body = doc.xpath('//body')[0]
            text = LxmlHelper.get_text_of_node(body)
            search = re.search(regex_str, text)
            if search is not None:
                self.pc_mapping[response.request.url] = search.group()
            for record in self.parse(response):
                item = ConfigurablespidersItem()
                item['url'] = response.request.url
                item['data_item'] = record
                yield item

    def parse(self, response):
        mapping_setting = {'学校': 'XXMC',
                           '课程名称': 'KCMC',
                           '讲数': 'JS',
                           '主讲教师': 'FZR'}
        structuring_data = StructuringParser.parse(response)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        mapper_name = data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        print(current_url + "使用的数据映射器类型为" + mapper_name)
        for record in data_mapper:
            js = record.pop('JS')
            if js is not None and StringHelper.filter_illegal_char(js) != 0:
                record['KCMC'] = record['KCMC'] + '（1～' + js + '讲）'
            record['PC'] = self.pc_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade10'}  #'csv': 'upgrade10',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_JX_JPZYGXK(scrapy.Spider, OutputConfig):
    # 11-资源共享课
    name = 'GG_JX_JPZYGXK'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url_pc = {'http://www.moe.gov.cn/srcsite/A08/s5664/s7209/s6872/201607/W020160715503665399086.xlsx': '第一批',
                       'http://www.moe.gov.cn/srcsite/A10/s7011/201702/W020170217387726331521.doc': '第二批'}
        self.mapping_setting = {'课程名称': 'KCMC',
                                '负责人': 'FZR',
                                '学校名称': 'XXMC',
                                '类型': 'ZB1',
                                '@t1': 'ZB2',
                                '学制': 'ZB2'}

    def start_requests(self):
        requests = []
        for key in self.url_pc.keys():
            requests.append(scrapy.Request(key))
        return requests

    def parse(self, response):
        structuring_data = StructuringParser.parse(response)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(self.mapping_setting)
        mapper_name = data_mapper.set_structuring_data(structuring_data)
        print(response.request.url + "使用的数据映射器类型为" + mapper_name)
        for record in data_mapper:
            record['PC'] = self.url_pc[response.request.url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        if pipeline_mark == 'csv':
            return 'test11'
        elif pipeline_mark == 'redis':
            return 'upgrade11'


class GG_JX_LHLXYYPPK(scrapy.Spider, OutputConfig):
    # 12-来华留学英语授课品牌课
    name = 'GG_JX_LHLXYYPPK'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pdsj_mapping = {'http://www.ceaie.edu.cn/uploads/201612/21/GC21024969793124.zip': '2016'}

    def start_requests(self):
        for key in self.pdsj_mapping.keys():
            yield scrapy.Request(url=key, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'学科门类': 'XK',
                           '课程名': 'KCMC',
                           '申报单位': 'XXMC',
                           '课程负责人': 'FZR'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.pdsj_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade12'}  #'csv': 'upgrade12',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_HJ_LXLYKJJ(scrapy.Spider, OutputConfig):
    # 15-梁希林业科学技术奖
    name = 'GG_HJ_LXLYKJJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.csf.org.cn/Search.aspx?word=%E6%A2%81%E5%B8%8C%E6%9E%97%E4%B8%9A%E7%A7%91%E5%AD%A6%E6%8A%80%E6%9C%AF%E5%A5%96%E8%AF%84%E9%80%89%E7%BB%93%E6%9E%9C%E7%9A%84%E9%80%9A%E6%8A%A5"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="news_list"]//a')
        for a_tag in a_tags:
            i_tags = a_tag.xpath('i')
            if i_tags is not None and len(i_tags) != 0:
                i_tag = i_tags[0]
                text = LxmlHelper.get_text_of_node(i_tag)
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                search = re.search('\d{4}', text)
                if search is not None:
                    self.year_mapping[target_url] = search.group()
                    yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="news_content"]//a')
        current_url = response.request.url
        for a_tag in a_tags:
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            self.year_mapping[target_url] = self.year_mapping[current_url]
            yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'编号': 'HJBH',
                           '序号': 'HJBH',
                           '项目': 'XMMC',
                           '完成人': 'WCR',
                           '申报人': 'WCR',
                           '获奖等级': 'HJDJ',
                           '题目': 'XXMC',
                           '主要完成单位': 'XXMC'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade15'}  #'csv': 'upgrade15',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_TD_GJJTD(scrapy.Spider, OutputConfig):
    # 19-国家级教学团队名单
    name = 'GG_TD_GJJTD'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.gov.cn/s78/A08/gjs_left/s5664/moe_1623/s3849/"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="index_bar_line4"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            search = re.search('\d{4}(?=年)', text)
            if search is not None:
                self.year_mapping[target_url] = search.group()
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="xxgk_content_div"]//a')
        current_url = response.request.url
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            search = re.search('名单', text)
            if search is not None:
                self.year_mapping[target_url] = self.year_mapping[current_url]
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'团队名称': 'TDMC',
                           '带头人': 'FZR',
                           '所在高校': 'XXMC',
                           '所在学校': 'XXMC'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade19'}  #'csv': 'upgrade19',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]

    def redirect(self, from_url, to_url):
        self.year_mapping[to_url] = self.year_mapping[from_url]


class GG_ZJ_JYBRC(scrapy.Spider, OutputConfig):
    # 20-新世纪优秀人才支持计划-教育部新世纪优秀人才
    name = 'GG_ZJ_JYBRC'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.gov.cn/was5/web/search?searchword=%E6%95%99%E8%82%B2%E9%83%A8%E5%85%B3%E4%BA%8E%E5%85%AC%E5%B8%83%E6%96%B0%E4%B8%96%E7%BA%AA%E4%BC%98%E7%A7%80%E4%BA%BA%E6%89%8D%E6%94%AF%E6%8C%81%E8%AE%A1%E5%88%92&channelid=224838"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="m_search_list"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            search = re.search('新世纪优秀人才支持计划\d{4}年度入选人员名单', text)
            year_search = re.search('\d{4}(?=年)', text)
            if search is not None and year_search is not None:
                self.year_mapping[target_url] = year_search.group()
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="content_body_xxgk"]//a')
        current_url = response.request.url
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            search = re.search('名单', text)
            if search is not None:
                self.year_mapping[target_url] = self.year_mapping[current_url]
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        print("===requesting=====")
        print(response.request.url)
        mapping_setting = {'编号': 'ZSBH',
                           '申请人姓名': 'XM',
                           '学校': 'XXMC',
                           '资助时间': 'ZZSJ',
                           '资助经费': 'ZZJF'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            record['LB'] = '新世纪优秀人才支持计划-教育部新世纪优秀人才'
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade20'}  #'csv': 'upgrade20',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]

    def redirect(self, from_url, to_url):
        self.year_mapping[to_url] = self.year_mapping[from_url]


class GG_ZJ_JYBQNJS(scrapy.Spider, OutputConfig):
    # 21-高校青年教师奖
    name = 'GG_ZJ_JYBQNJS'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.edu.cn/edu/shi_fan/zonghe/shi_fan_zhuan_ti/gao_jiao_shi/200603/t20060323_24422.shtml"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'姓名': 'XM',
                           '单位': 'XXMC',
                           '专业': 'XKLB',
                           '获奖年度': 'PDSJ'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        for record in data_mapper:
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="pagenav"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
            target_url = response.urljoin(target_url)
            if text == '下一页':
                yield scrapy.Request(url=target_url, callback=self.parse)

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade21'}  #'csv': 'upgrade21',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_ZJ_GYDS(scrapy.Spider, OutputConfig):
    # 25-国医大师
    # 针对爬取
    name = 'GG_ZJ_GYDS'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.360doc.com/content/17/1220/20/40398747_714887120.shtml"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.parse)

    def parse(self, response):
        doc = etree.HTML(response.body)
        content_node = doc.xpath('//td[@id="artContent"]')[0]
        structuring_data = StructuringParser.handle_html_node(content_node)[1]
        pc = None
        year = None
        for x in structuring_data:
            search = re.search('(第.届)国医大师（(\d{4})）', x)
            name_search = re.search('\d(、|[.])(.*?)，', x)
            school_search = re.search('(，|。)(((?!(，|。)).)*?)(?=(主任|教授|研究员|名誉院长|院长|原副院长))', x)
            if search is not None:
                pc = search.group(1)
                year = search.group(2)
            elif name_search is not None or school_search is not None:
                record = {'JS': pc, 'NF': year, 'XM': "", 'XXMC': ""}
                if name_search is not None:
                    record['XM'] = name_search.group(2)
                if school_search is not None:
                    record['XXMC'] = school_search.group(2)
                item = ConfigurablespidersItem()
                item['url'] = response.request.url
                item['data_item'] = record
                yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'csv': 'upgrade25'}  #,'redis': 'upgrade26'
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_ZJ_GCYYS(scrapy.Spider, OutputConfig):
    # 26-工程院院士
    name = 'GG_ZJ_GCYYS'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.cae.cn/cae/html/main/col48/column_48_1.html"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.parse)

    def parse(self, response):
        doc = etree.HTML(response.body)
        content_node = doc.xpath('//*[@class="right_md_ysmd"]')[0]
        children = list(content_node)
        current_type = None
        for child in children:
            class_type = LxmlHelper.get_attribute_of_element(child, "class")
            if(class_type == 'ysmd_bt clearfix'):
                text = LxmlHelper.get_text_of_node(child)
                search = re.search('、(.*)[(]', text)
                if search is not None:
                    current_type = search.group(1)
            elif(class_type == 'ysxx_namelist clearfix'):
                a_tags = child.xpath(".//a")
                for a_tag in a_tags:
                    text = LxmlHelper.get_text_of_node(a_tag)
                    record = {"YSLB": "中国工程院院士",
                              "XB": current_type,
                              "XM": text}
                    item = ConfigurablespidersItem()
                    item['url'] = response.request.url
                    item['data_item'] = record
                    yield item


    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade26'}  #'csv': 'upgrade26',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_JX_GJJGHJCW(scrapy.Spider, OutputConfig):
    # 29-国家级规划教材人文用
    name = 'GG_JX_GJJGHJCW'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.gov.cn/was5/web/search?searchword=%E6%95%99%E8%82%B2%E9%83%A8%E5%85%B3%E4%BA%8E%E5%8D%B0%E5%8F%91%E3%80%8A%E2%80%9C%E5%8D%81%E4%BA%8C%E4%BA%94%E2%80%9D%E6%99%AE%E9%80%9A%E9%AB%98%E7%AD%89%E6%95%99%E8%82%B2%E6%9C%AC%E7%A7%91%E5%9B%BD%E5%AE%B6%E7%BA%A7%E8%A7%84%E5%88%92%E6%95%99%E6%9D%90%E4%B9%A6%E7%9B%AE%E3%80%8B%E7%9A%84%E9%80%9A%E7%9F%A5&channelid=224838"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self,response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="m_search_list"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('国家级规划教材书目',text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

        a_tags = doc.xpath('//*[@class="m_page_a m_page_btn"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            if text == '下一页':
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.find_info_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="content_body_xxgk"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('国家级规划教材书目', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'书名': 'SM',
                           '作者': 'ZYZZ',
                           '单位': 'XXMC',
                           '出版社': 'CBS'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        for record in data_mapper:
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade29'}  #'csv': 'upgrade29',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_HJ_HXJSKJJ(scrapy.Spider, OutputConfig):
    # 30-华夏建设科学技术奖
    name = 'GG_HJ_HXJSKJJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url_after_2018 = 'http://stc.chinagb.net/index.asp'
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url_after_2018, callback=self.find_data_page_after_2018)

    def find_data_page_after_2018(self, response):
        doc = etree.HTML(response.body.decode(response.encoding))
        a_tags = doc.xpath('//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            print(text)
            search = re.search('(\d{4})年.*华夏建设科学技术奖', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                import urllib
                target_url = urllib.parse.quote(target_url, safe=";/?:@&=+$,", encoding="utf-8")
                self.year_mapping[target_url] = search.group(1)
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping = {'项目名称': 'XMMC',
                   '完成人': 'WCR',
                   '拟推荐等级': 'HJDJ',
                   '完成单位': 'XXMC'}
        structuring_data = StructuringParser.parse(response)
        print(structuring_data)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade30'}  #'csv': 'upgrade30',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]

    def redirect(self, from_url, to_url):
        if from_url in self.year_mapping.keys():
            self.year_mapping[to_url] = self.year_mapping[from_url]


class GG_HJ_GJZRKXJ(scrapy.Spider, OutputConfig):
    # 31-国家自然科学奖
    name = 'GG_HJ_GJZRKXJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://znjs.most.gov.cn/wasdemo/search"
        self.year_mapping = {}

    def start_requests(self):
        form_data = {'searchword': '国家自然科学奖获奖项目目录',
                     'prepage': '10',
                     'channelid': '44374', 'sortfield': '-DOCRELTIME'}
        yield scrapy.FormRequest(url=self.root_url, formdata=form_data,callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//a')
        filter_year = set()
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('(\d*)年.*国家自然科学奖获奖', text)
            if search is not None and search.group(1) not in filter_year:
                filter_year.add(search.group(1))
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.year_mapping[target_url] = search.group(1)
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'编号': 'ZSBH',
                           '项目名称': 'XMMC',
                           '主要完成人': 'WCR',
                           '提名单位': 'TJDW',
                           '推荐单位' : 'TJDW'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        current_url = response.request.url
        structuring_data = StructuringParser.parse(response)
        print(structuring_data)
        data_mapper.set_structuring_data(structuring_data)
        for record in data_mapper:
            if current_url in self.year_mapping.keys():
                record['PDSJ'] = self.year_mapping[response.request.url]
            description = data_mapper.get_description_of_current_table()
            search = re.search('.等奖', description)
            if search is not None:
                record['HJDJ'] = search.group()
                item = ConfigurablespidersItem()
                item['url'] = response.request.url
                item['data_item'] = record
                yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'csv': 'upgrade31'}  #,'redis': 'upgrade31'
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_ZJ_GJJJXMS(scrapy.Spider, OutputConfig):
    # 33-国家级教学名师
    name = 'GG_ZJ_GJJJXMS'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "https://baike.baidu.com/item/%E5%9B%BD%E5%AE%B6%E7%BA%A7%E6%95%99%E5%AD%A6%E5%90%8D%E5%B8%88%E5%A5%96#4"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.parse)

    def parse(self, response):
        doc = etree.HTML(response.body)
        content = doc.xpath('//table')[0]
        StructuringParser.fix_row_and_col_span(content)
        structuring_data = StructuringParser.handle_html_node(content)
        type = '国家级教学名师'
        for i in range(1, len(structuring_data)):
            row_data = structuring_data[i]
            year = re.search('\d{4}', row_data[0][1]).group()
            pc = row_data[0][0]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = {'XM': row_data[1], 'LB': type, 'PC': pc, 'PDSJ': year, 'XXMC': row_data[2]}
            yield item
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = {'XM': row_data[3], 'LB': type, 'PC': pc, 'PDSJ': year, 'XXMC': row_data[4]}
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade33'}  #'csv': 'upgrade33',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_TD_JYBTD(scrapy.Spider, OutputConfig):
    # 34-教育部创新团队
    name = 'GG_TD_JYBTD'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.moe.edu.cn/was5/web/search?searchword=%27%E2%80%9C%E5%88%9B%E6%96%B0%E5%9B%A2%E9%98%9F%E5%8F%91%E5%B1%95%E8%AE%A1%E5%88%92%E2%80%9D%E6%BB%9A%E5%8A%A8%E6%94%AF%E6%8C%81%E5%90%8D%E5%8D%95%E7%9A%84%E9%80%9A%E7%9F%A5%27&btn_search=&channelid=224838&timescope=&timescopecolumn=&orderby=-DOCRELTIME&perpage=20&searchscope="
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="m_search_list"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('(\d{4})年.*创新团队发展计划.*名单', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.year_mapping[target_url] = search.group(1)
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@id="xxgk_content_div"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('创新团队发展计划', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.year_mapping[target_url] = self.year_mapping[response.request.url]
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'带头人': 'FZR',
                           '学校': 'XXMC',
                           '资助金额': 'ZZJF',
                           '研究方向': 'YJFX',
                           '资助期限': 'ZZSJ'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade34'}  #'csv': 'upgrade34',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_HJ_GJJSFMJ(scrapy.Spider, OutputConfig):
    # 35-国家技术发明奖
    name = 'GG_HJ_GJJSFMJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://znjs.most.gov.cn/wasdemo/search"
        self.year_mapping = {}

    def start_requests(self):
        form_data = {'searchword': '国家技术发明奖获奖项目目录',
                     'prepage': '10',
                     'channelid': '44374', 'sortfield': '-DOCRELTIME'}
        yield scrapy.FormRequest(url=self.root_url, formdata=form_data,callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//a')
        filter_year = set()
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('(\d*)年.*国家技术发明奖获奖', text)
            if search is not None and search.group(1) not in filter_year:
                filter_year.add(search.group(1))
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.year_mapping[target_url] = search.group(1)
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'编号': 'XMBH',
                           '项目名称': 'XMMC',
                           '主要完成人': 'WCR',
                           '提名单位': 'TMDW'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        current_url = response.request.url
        structuring_data = StructuringParser.parse(response)
        print(structuring_data)
        data_mapper.set_structuring_data(structuring_data)
        for record in data_mapper:
            if current_url in self.year_mapping.keys():
                record['PDSJ'] = self.year_mapping[response.request.url]
            description = data_mapper.get_description_of_current_table()
            search = re.search('.等奖', description)
            if search is not None:
                record['HJDJ'] = search.group()
                item = ConfigurablespidersItem()
                item['url'] = response.request.url
                item['data_item'] = record
                yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade35'}  #'csv': 'upgrade35',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_HJ_GJKJJBJ(scrapy.Spider, OutputConfig):
    # 36-国家科技进步奖
    name = 'GG_HJ_GJKJJBJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://znjs.most.gov.cn/wasdemo/search"
        self.year_mapping = {}

    def start_requests(self):
        form_data = {'searchword': '国家科学技术进步奖获奖项目目录',
                     'prepage': '10',
                     'channelid': '44374', 'sortfield': '-DOCRELTIME'}
        yield scrapy.FormRequest(url=self.root_url, formdata=form_data,callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//a')
        filter_year = set()
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('(\d*)年.*国家科学技术进步奖获奖', text)
            if search is not None and search.group(1) not in filter_year:
                filter_year.add(search.group(1))
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.year_mapping[target_url] = search.group(1)
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'编号': 'XMBH',
                           '项目名称': 'XMMC',
                           '主要完成人': 'WCR',
                           '推荐单位': 'TJDW',
                           '主要完成单位': 'XXMC'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        current_url = response.request.url
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        for record in data_mapper:
            if current_url in self.year_mapping.keys():
                record['PDSJ'] = self.year_mapping[response.request.url]
            description = data_mapper.get_description_of_current_table()
            search = re.search('.等奖', description)
            if search is not None:
                record['HJDJ'] = search.group()
                item = ConfigurablespidersItem()
                item['url'] = response.request.url
                item['data_item'] = record
                yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade36'}  #'csv': 'upgrade36',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]


class GG_HJ_ZGZLJ(scrapy.Spider, OutputConfig):
    # 37-中国专利奖
    name = 'GG_HJ_ZGZLJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.sipo.gov.cn/ztzl/zgzlj/tz_zgzlj/index.htm"
        self.year_mapping = {}
        self.pc_mapping = {}
        self.type_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.find_info_page)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="index_articl_list"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('(第.*届)中国专利奖授奖.*决定', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.pc_mapping[target_url] = search.group(1)
                year = a_tag.getparent().xpath('span')[0].text
                self.year_mapping[target_url] = year
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        attachment_tags = doc.xpath('//*[@class="index_art_con"]//a')
        current_url = response.request.url
        for a_tag in attachment_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('组织', text)  # 排除组织奖附件
            if search is None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.redirect(current_url, target_url)
                search_type = re.search('(?<=中国).*(?=项目名单)', text)
                self.type_mapping[target_url] = search_type.group()
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'序号': 'no',
                           '专利号': 'ZLH',
                           '专利名称': 'ZLMC',
                           '专利权人': 'XXMC',
                           '发明人': 'FMR'}
        structuring_data = StructuringParser.parse(response)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            record['HJDJ'] = self.type_mapping[current_url]
            record['PC'] = self.pc_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade37'}  #'csv': 'upgrade37',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]

    def redirect(self, from_url, to_url):
        self.year_mapping[to_url] = self.year_mapping[from_url]
        self.pc_mapping[to_url] = self.pc_mapping[from_url]


class GG_HJ_ZHNYKJJ(scrapy.Spider, OutputConfig):
    # 38-中华农业科技奖
    # 2014年之前的采用针对爬取
    name = 'GG_HJ_ZHNYKJJ'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.year_mapping = {'http://www.moa.gov.cn/gk/tzgg_1/tz/201112/P020111227350595289062.xls': '2010-2011',
                             'http://www.caass.org.cn/agrihr/xhdt41/15293/index.html': '2008',
                             'http://www.caass.org.cn/agrihr/_1458/_3048/4906/2016072123473282809.doc': '2006-2007'}
        self.level_mapping = {}

    def start_requests(self):
        self.root_url_after_2014 = 'http://www.moa.gov.cn/was5/web/search?%3Asearchscope=&searchscope=&channelid=233424&orsen=%E4%B8%AD%E5%8D%8E%E5%86%9C%E4%B8%9A%E7%A7%91%E6%8A%80%E5%A5%96%E7%9A%84%E8%A1%A8%E5%BD%B0%E5%86%B3%E5%AE%9A'
        yield scrapy.Request(url=self.root_url_after_2014, callback=self.find_info_page)
        # 2014年之前
        for url in self.year_mapping.keys():
            yield scrapy.Request(url=url, callback=self.parse_2010)

    def find_info_page(self, response):
        doc = etree.HTML(response.body)
        a_tags = doc.xpath('//*[@class="cx_list ft_st"]//a')
        for a_tag in a_tags:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('中华农业科技奖的表彰决定', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                yield scrapy.Request(url=target_url, callback=self.find_data_page)

    def find_data_page(self, response):
        doc = etree.HTML(response.body)
        attachment_tag = doc.xpath('//*[@class="fujian_download_nr"]//a')
        for a_tag in attachment_tag:
            text = LxmlHelper.get_text_of_node(a_tag)
            search = re.search('中华农业科技奖.*等奖', text)
            level_search = re.search('.等', text)
            year_search = re.search('\d{4}－\d{4}', text)
            if search is not None:
                target_url = LxmlHelper.get_attribute_of_element(a_tag, 'href')
                target_url = response.urljoin(target_url)
                self.level_mapping[target_url] = level_search.group()
                self.year_mapping[target_url] = year_search.group()
                yield scrapy.Request(url=target_url, callback=self.parse)

    def parse(self, response):
        mapping_setting = {'成果名称': 'XMMC',
                           '主要完成人': 'WCR',
                           '主要完成单位': 'XXMC'}
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        structuring_data = StructuringParser.parse(response)
        data_mapper.set_structuring_data(structuring_data)
        current_url = response.request.url
        for record in data_mapper:
            record['PDSJ'] = self.year_mapping[current_url]
            record['HJDJ'] = self.level_mapping[current_url]
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def parse_2010(self, response):
        mapping_setting = {'序号': 'no',
                           '项目名称': 'XMMC',
                           '主要完成人': 'WCR',
                           '主要完成单位': 'XXMC'}
        structuring_data = StructuringParser.parse(response)
        data_mapper = DataMapper()
        data_mapper.init_setting_by_dir(mapping_setting)
        data_mapper.set_structuring_data(structuring_data)
        level = ['三等', '二等', '一等']
        current_level = None
        last_index = 100
        for record in data_mapper:
            no = str(record.pop('no'))
            if no is None or not no.isdigit():
                continue
            current_index = int(float(no))
            if current_index < last_index:
                if len(level) == 0:
                    break
                current_level = level.pop()
            record['PDSJ'] = self.year_mapping[response.request.url]
            record['HJDJ'] = current_level
            last_index = current_index
            item = ConfigurablespidersItem()
            item['url'] = response.request.url
            item['data_item'] = record
            yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade38'}  #'csv': 'upgrade38',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]

    def redirect(self, from_url, to_url):
        self.year_mapping[to_url] = self.year_mapping[from_url]
        self.level_mapping[to_url] = self.level_mapping[from_url]


class GG_ZJ_KXYYS(scrapy.Spider, OutputConfig):
    # 39-中科院院士
    name = 'GG_ZJ_KXYYS'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.root_url = "http://www.casad.cas.cn/chnl/371/index.html"
        self.year_mapping = {}

    def start_requests(self):
        yield scrapy.Request(url=self.root_url, callback=self.parse)

    def parse(self, response):
        doc = etree.HTML(response.body)
        content_node = doc.xpath('//*[@id="allNameBar"]')[0]
        children = list(content_node)
        current_class = None
        for child in children:
            if child.tag == 'dt':
                current_class = child.text
            elif child.tag == 'dd':
                structuring_data = StructuringParser.handle_html_node(child)
                if isinstance(structuring_data, list):
                    for x in structuring_data:
                        item = ConfigurablespidersItem()
                        item['url'] = response.request.url
                        item['data_item'] = {'XM': x, 'XBMC': current_class}
                        yield item
                else:
                    item = ConfigurablespidersItem()
                    item['url'] = response.request.url
                    item['data_item'] = {'XM': structuring_data, 'XBMC': current_class}
                    yield item

    def get_output_config(self, pipeline_mark, item_from_url):
        output_setting = {'redis': 'upgrade39'}  #'csv': 'upgrade39',
        if pipeline_mark in output_setting.keys():
            return output_setting[pipeline_mark]
