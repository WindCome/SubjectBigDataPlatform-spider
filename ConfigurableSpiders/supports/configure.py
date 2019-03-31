import abc
import os

from lxml import etree

from ConfigurableSpiders.supports.mapper import DataMapper
from ConfigurableSpiders.supports.tools import LxmlHelper, FileHelper


class ConfigPool:

    def __init__(self, classes=None, id=None):
        """
        初始化配置池，加载符合条件的配置
        :param classes: 启动的爬虫组号
        :param id: 启动的爬虫id
        """
        self.configs = []
        self.config_dir_path = FileHelper.get_project_root_path() + os.sep + 'config'
        file_paths = FileHelper.get_files_in_dir(self.config_dir_path)
        valid = False
        for file in file_paths:
            if os.path.isfile(file):
                with open(file, 'r', encoding='utf-8') as f:
                    config = Configure(f)
                    if config.is_active:
                        if classes is not None and config.class_str != classes:
                            continue
                        elif id is not None and config.id_str != id:
                            continue
                        self.configs.append(config)
                        print("使用配置文件:" + file)
                        valid = True
        if not valid:
            raise Exception("不存在符合条件的配置文件: id=" + id + " classes=" + classes)

    def get_root_paths(self):
        """
        获取所有根路径
        """
        result = []
        for config in self.configs:
            root_urls = config.get_root_urls()
            result.extend(root_urls)
        return result

    def get_steps(self, url):
        """
        获取指定url的解析步骤
        """
        for config in self.configs:
            steps = config.get_steps_of_url(url)
            if steps is not None:
                return steps

    def accpet_xpath_crawl_result(self, current_url, xpath, target_url):
        for config in self.configs:
            config.set_xpath_results(current_url, xpath, target_url)

    def redirect_url(self, retry_url, original_url):
        """
        url重定向
        :param retry_url: 重定向后的url
        :param original_url: 重定向前的url
        """
        for config in self.configs:
            config.set_redirect_url(retry_url, original_url)


class Configure:
    """
    配置实体类
    """

    def __init__(self, configure_file):
        self.top_url_step = []  # 根url节点
        self.url_to_step = {}  # url对应的解析步骤{'url':CrawlStep}
        self.is_active = True  # 是否启用该配置
        self.class_str = None
        self.id_str = None
        self.__parse_configure_file(configure_file)

    def __parse_configure_file(self, configure_file):
        doc = etree.parse(configure_file)
        root = doc.xpath('/spider')[0]
        self.__parse_root_node(root)
        children = list(root)
        data_mapper_dir = {}
        default_data_mapper = False
        output_config_dir = {}
        default_output_config = False
        url_node = []
        for child in children:
            if child.tag == 'url':
                url_node.append(child)
            elif child.tag == 'mapping':
                data_mapper = DataMapper()
                table_node = child.xpath("//table")[0]
                data_mapper.init_setting_by_xml(table_node)
                setting_id = LxmlHelper.get_attribute_of_element(child, "id")
                if setting_id is not None:
                    data_mapper_dir[setting_id] = data_mapper
                elif not default_data_mapper:
                    default_data_mapper = True
                    data_mapper_dir["default"] = data_mapper
                else:
                    raise Exception("存在多个默认数据映射器")
            elif child.tag == 'outputs':
                output_config = LxmlHelper.convert_children_to_dir(child)
                outputs_id = LxmlHelper.get_attribute_of_element(child, 'id')
                if outputs_id is not None:
                    output_config_dir[outputs_id] = output_config
                elif not default_output_config:
                    default_output_config = True
                    output_config_dir["default"] = output_config
                else:
                    raise Exception("存在多个默认输出配置")
            else:
                print("unknown tag " + child.tag + " in config file")

        for node in url_node:
            self.top_url_step.append(self.__parse_url_node(node, data_mapper_dir, output_config_dir))

    def __parse_url_node(self, url_node, data_mapper_dir, output_dir):
        step = CrawlStep()
        url = url_node.text
        if url is None:
            url = LxmlHelper.get_attribute_of_element(url_node, 'value')
        mapper_id = LxmlHelper.get_attribute_of_element(url_node, 'mapping')
        output_id = LxmlHelper.get_attribute_of_element(url_node, 'output')
        if url is not None:
            url = url.strip()
            step.url = url
            self.url_to_step[url] = step
        if mapper_id is not None:
            if mapper_id not in data_mapper_dir.keys():
                raise Exception("不存在id为" + mapper_id + "的数据映射器")
            data_mapper = data_mapper_dir[mapper_id]
            if output_id is None or output_id not in output_dir.keys():
                output_config = output_dir["default"]
            else:
                output_config = output_dir[output_id]
            step.data_mapper = data_mapper
            step.output_config = output_config
        children = list(url_node)
        for child in children:
            search_condition = {}
            xpath_str = LxmlHelper.get_attribute_of_element(child, 'xpath')
            if xpath_str is not None:
                search_condition['xpath'] = xpath_str
            regex_str = LxmlHelper.get_attribute_of_element(child, 'regex')
            if regex_str is not None:
                search_condition['regex'] = regex_str
            if child.tag == 'url':
                step.search_child_page_by_condition(search_condition,
                                                    self.__parse_url_node(child, data_mapper_dir, output_dir))
            elif child.tag == 'next':
                step.set_next_page(search_condition)
        return step

    def __parse_root_node(self, root_node):
        is_active = LxmlHelper.get_attribute_of_element(root_node, "is_active")
        if is_active is not None:
            self.is_active = str(True) == is_active
        class_str = LxmlHelper.get_attribute_of_element(root_node, "class")
        if class_str is not None:
            self.class_str = class_str
        id_str = LxmlHelper.get_attribute_of_element(root_node, "id")
        if id_str is not None:
            self.id_str = id_str

    def get_root_urls(self):
        result = []
        for x in self.top_url_step:
            url = x.url
            if url is None or url.isspace():
                raise Exception("未指定爬取根路径")
            result.append(url)
        return result

    def get_steps_of_url(self, url):
        if url not in self.url_to_step.keys():
            return None
        return self.url_to_step[url]

    # 设置通过xpath和regex解析的url结果
    def set_xpath_results(self, current_url, search_condition, target_url):
        if current_url not in self.url_to_step.keys():
            return False
        step = self.url_to_step[current_url]
        child_step = step.get_step_by_condition(search_condition)
        self.url_to_step[target_url] = child_step
        return True

    def set_redirect_url(self, retry_url, original_url):
        if original_url not in self.url_to_step.keys():
            return False
        step = self.url_to_step[original_url]
        self.url_to_step[retry_url] = step
        return True


class OutputConfig(metaclass=abc.ABCMeta):
    """
    输出配置
    当Spider继承这个接口后，PipeLine会自动调用get_output_config方法获取输出配置
    """

    # pipeline获取输出位置
    @abc.abstractmethod
    def get_output_config(self, pipeline_mark, item_from_url):
        """
        :param pipeline_mark: pipeline的标志，表明item正在被哪个pipeline处理
        :param item_from_url: pipeline当前处理的item的来源url
        :return: 输出信息
        """
        pass

    def redirect(self, from_url, to_url):
        """
        要关注重定向的爬虫可以实现这个方法
        当某个链接发生了重定向时被调用
        :param from_url: 重定向前的url
        :param to_url: 重定向后的url
        """
        pass


class CrawlStep(OutputConfig):
    """
    该类用于记录某个url页面的解析步骤,包括下一页、子页面查找和当前页面的数据映射等
    """

    def get_output_config(self, pipeline_mark, item_from_url):
        if self.output_config is not None and pipeline_mark in self.output_config.keys():
            return self.output_config[pipeline_mark]

    def __init__(self):
        self.url = None
        self.search_conditions = []
        self.child_page_step = None
        self.__next_page_condition_index = None
        self.data_mapper = None  # 当前url的数据映射器
        self.output_config = None  # 当前url的输出配置

    def is_data_page(self):
        """
        判断当前url是否需要数据解析
        """
        return self.data_mapper is not None

    def has_child_page(self):
        """
        判断当前url是否有子页面
        """
        return len(self.search_conditions) != 0

    def set_next_page(self, search_condition):
        """
        设置下一页链接的搜索条件
        :param search_condition: {"regex":String,"xpath":String}
        """
        if self.__next_page_condition_index is None:
            self.__next_page_condition_index = {}
        self.__next_page_condition_index = self.__append_search_condition(search_condition)
        return self

    def __append_search_condition(self, search_condition):
        self.search_conditions.append(search_condition)
        return len(self.search_conditions) - 1

    def search_child_page_by_condition(self, search_condition, crawl_step):
        """
        设置搜索子页面的搜索条件，以及设置子页面的解析步骤
        :param search_condition: {"regex":String,"xpath":String}
        :param crawl_step: CrawlStep
        """
        if self.child_page_step is None:
            self.child_page_step = {}
        self.child_page_step[self.__append_search_condition(search_condition)] = crawl_step
        return self

    def get_step_by_condition(self, search_condition):
        """
        根据搜索条件查找符合条件的页面的解析步骤
        :param search_condition: {"regex":String,"xpath":String}
        :return: CrawlStep
        """
        index = self.search_conditions.index(search_condition)
        if index != -1:
            if self.__next_page_condition_index == index:
                return self
            return self.child_page_step[index]
