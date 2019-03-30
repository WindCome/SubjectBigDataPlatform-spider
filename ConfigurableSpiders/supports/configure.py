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
            raise Exception("不存在符合条件的配置文件: id="+id+" classes="+classes)

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

    def accpet_xpath_crawl_result(self, current_url, xpath, urls):
        for config in self.configs:
            config.set_xpath_results(current_url, xpath, urls)

    def get_data_mapper(self, url):
        """
        获取指定url的数据映射器
        """
        data_mapper = None
        for config in self.configs:
            data_mapper = config.get_data_mapper(url)
            if data_mapper is not None:
                break
        if data_mapper is None:
            raise Exception("找不到合适的数据映射器")
        return data_mapper

    def get_output_config(self, url):
        output_config = None
        for config in self.configs:
            output_config = config.get_output_config_by_url(url)
            if output_config is not None:
                break
        if output_config is None:
            print(url + "找不到合适的数据输出配置")
        return output_config

    def redirect_url(self, retry_url, original_url):
        """
        url重定向
        :param retry_url: 重定向后的url
        :param original_url: 重定向前的url
        """
        for config in self.configs:
            config.set_redirect_url(retry_url, original_url)


class Configure:
    def __init__(self, configure_file):
        self.top_url_node = []  # 根url节点
        self.url_to_node = {}  # url对应的配置节点{'url':Element}
        self.data_mapper_dir = {}  # 数据映射器字典{'setting_id':DataMapper}
        self.url_node_to_data_mapper = {}  # url配置节点对应的数据映射器{Element:DataMapper}
        self.default_data_mapper = None  # 默认数据映射器
        self.output_config_dir = {}  # 输出配置字典{"output_id":{}}
        self.default_output_config = None  # 默认的输出配置
        self.mapper_to_output = {}  # 记录数据映射器对应的输出配置 {DataMapper:"output_id"}
        self.is_active = True  # 是否启用该配置
        self.class_str = None
        self.id_str = None
        self.__parse_configure_file(configure_file)

    def __parse_configure_file(self, configure_file):
        doc = etree.parse(configure_file)
        root = doc.xpath('/spider')[0]
        self.__parse_root_node(root)
        children = list(root)
        for child in children:
            if child.tag == 'url':
                self.top_url_node.append(child)
            elif child.tag == 'mapping':
                data_mapper = DataMapper()
                table_node = child.xpath("//table")[0]
                data_mapper.init_setting_by_xml(table_node)
                setting_id = LxmlHelper.get_attribute_of_element(child, "id")
                if setting_id is not None:
                    self.data_mapper_dir[setting_id] = data_mapper
                elif self.default_data_mapper is not None:
                    self.default_data_mapper = data_mapper
                output_id = LxmlHelper.get_attribute_of_element(child, "outputs")
                if output_id is not None:
                    self.mapper_to_output[data_mapper] = output_id
            elif child.tag == 'outputs':
                output_config = LxmlHelper.convert_children_to_dir(child)
                outputs_id = LxmlHelper.get_attribute_of_element(child, 'id')
                if outputs_id is not None:
                    self.output_config_dir[outputs_id] = output_config
                elif self.default_output_config is None:
                    self.default_output_config = output_config
            else:
                print("unknown tag " + child.tag + " in config file")

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

    # 解析一个url配置节点定义的动作
    def __parse_url_node(self, url_node):
        result = {}
        children = list(url_node)
        for child in children:
            if child.tag == 'url':
                xpath_str = LxmlHelper.get_attribute_of_element(child, 'value')
                if xpath_str is not None:
                    if 'xpath' not in result.keys():
                        result['xpath'] = []
                    result['xpath'].append(xpath_str)
            elif child.tag == 'mapping':
                self.url_node_to_data_mapper[url_node] = self.data_mapper_dir[child.text]
        attributes_map = url_node.attrib
        for key in attributes_map.keys():
            result[key] = attributes_map[key]
            if key == 'mapping':
                self.url_node_to_data_mapper[url_node] = self.data_mapper_dir[attributes_map[key]]
        return result

    def get_root_urls(self):
        result = []
        for x in self.top_url_node:
            root_url = LxmlHelper.get_attribute_of_element(x, 'value')
            self.url_to_node[root_url] = x
            result.append(root_url)
        return result

    def get_steps_of_url(self, url):
        if url not in self.url_to_node.keys():
            return None
        url_node = self.url_to_node[url]
        return self.__parse_url_node(url_node)

    # 设置解析当前url的xpath时抓取到的url结果
    def set_xpath_results(self, current_url, xpath, urls):
        if current_url not in self.url_to_node.keys():
            return False
        url_node = self.url_to_node[current_url]
        children = list(url_node)
        for child in children:
            xpath_of_child = LxmlHelper.get_attribute_of_element(child, 'value')
            if xpath == xpath_of_child:
                if isinstance(urls, list):
                    for url in urls:
                        self.url_to_node[url] = child
                else:
                    self.url_to_node[urls] = child
        return True

    # 获取Url对应的数据映射器
    def get_data_mapper(self, url):
        if url not in self.url_to_node.keys():
            return None
        url_node = self.url_to_node[url]
        result = None
        if url_node in self.url_node_to_data_mapper.keys():
            result = self.url_node_to_data_mapper[url_node]
        else:
            result = self.default_data_mapper
        return result

    # 获取url对应的输出配置
    def get_output_config_by_url(self, url):
        data_mapper = self.get_data_mapper(url)
        if data_mapper is None:
            return None
        config = None
        if data_mapper in self.mapper_to_output.keys():
            output_id = self.output_config_dir[data_mapper]
            config = self.output_config_dir[output_id]
        if config is None:
            config = self.default_output_config
        return config

    def set_redirect_url(self, retry_url, original_url):
        if original_url not in self.url_to_node.keys():
            return False
        node = self.url_to_node[original_url]
        self.url_to_node[retry_url] = node
        return True


class OutputConfig(metaclass=abc.ABCMeta):

    # pipeline获取输出位置
    @abc.abstractmethod
    def get_output_config(self, pipeline_mark, item_from_url):
        pass

    def redirect(self, from_url, to_url):
        pass
