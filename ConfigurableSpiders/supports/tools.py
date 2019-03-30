import inspect
import os
import random
import string
import zipfile

from win32com import client
from lxml import etree


class LxmlHelper:
    # 获得节点的指定属性值
    @staticmethod
    def get_attribute_of_element(element, key):
        attributes_map = element.attrib
        if key in attributes_map.keys():
            return attributes_map[key]

    # 将指定节点的子节点转化为字典{tag:(text|{tag,text})}
    @staticmethod
    def convert_children_to_dir(element):
        text = StringHelper.filter_illegal_char(element.text)
        if text is not None and len(text) != 0:
            return text
        result = {}
        children = list(element)
        for child in children:
            x = LxmlHelper.convert_children_to_dir(child)
            result[child.tag] = x
        return result

    @staticmethod
    def get_text_of_node(lxml_node):
        stringify = etree.XPath("string()")
        try:
            result = stringify(lxml_node)
        except ValueError:
            result = lxml_node.text
        return result


class FileHelper:
    # 获取项目根目录
    @staticmethod
    def get_project_root_path():
        pwd = os.getcwd()
        return os.path.abspath(os.path.dirname(pwd) + os.path.sep + ".")

    # 获取指定目录下所有文件路径
    @staticmethod
    def get_files_in_dir(filepath):
        pathDir = os.listdir(filepath)
        result = []
        for file in pathDir:
            child = filepath + os.sep + file
            result.append(child)
        return result

    # 检查目录是否已存在，不存在则创建
    @staticmethod
    def check_exist_or_create(file_path):
        if not os.path.exists(file_path):
            os.mkdir(file_path)

    @staticmethod
    # 将字节流保存在临时文件中
    def create_temp_file_by_bytes(bytes_data, postfix):
        tmp_dir_path = FileHelper.__get_tmp_file_path()
        FileHelper.check_exist_or_create(tmp_dir_path)
        tmp_file_path = tmp_dir_path + os.path.sep + StringHelper.random_str(10) + '.' + postfix
        with open(tmp_file_path, "wb") as f:
            f.write(bytes_data)
            f.flush()
        return tmp_file_path

    @staticmethod
    def unpack_zip(zip_file_path):
        f = zipfile.ZipFile(zip_file_path, 'r')
        tmp_file_path = FileHelper.__get_tmp_file_path()
        result = []
        for file_name in f.namelist():
            f.extract(file_name, tmp_file_path)
            result.append(tmp_file_path + os.path.sep + file_name)
        return result


    @staticmethod
    def __get_tmp_file_path():
        return FileHelper.get_project_root_path() + os.path.sep + "tmp"

    @staticmethod
    def clear_tmp_file():
        file_paths = FileHelper.get_files_in_dir(FileHelper.__get_tmp_file_path())
        for file in file_paths:
            os.remove(file)

    @staticmethod
    def get_file_name(file_path):
        file_name_with_postfix = os.path.basename(file_path)
        index = file_name_with_postfix.rfind('.')
        if index != -1:
            return file_name_with_postfix[:index]
        return file_name_with_postfix

    @staticmethod
    def get_postfix(url):
        if url is None or len(url) == 0:
            return ""
        index = url.rfind('.')
        if index != -1:
            return url[index + 1:]
        return ""

    @staticmethod
    def convert_file_to_docx(file_path):
        tmp_dir_path = FileHelper.__get_tmp_file_path()
        FileHelper.check_exist_or_create(tmp_dir_path)
        docx_file_path = tmp_dir_path + os.path.sep + StringHelper.random_str(10) + '.docx'
        w = client.Dispatch('word.application')
        doc = w.Documents.Open(file_path)
        doc.SaveAs(docx_file_path, 12)
        doc.Close()
        w.Quit()
        return docx_file_path


class ArrayHelper:
    @staticmethod
    # 计算值在不确定维度数组中的位置
    def locate_value(arrays, value):
        result = []
        for i in range(0, len(arrays)):
            x = arrays[i]
            if isinstance(x, str) and isinstance(value, str) and StringHelper.match_string(x, value):
                result.append([i])
            elif x == value:
                result.append([i])
            elif isinstance(x, list):
                sub_result_list = ArrayHelper.locate_value(x, value)
                if len(sub_result_list) != 0:
                    for sub_result in sub_result_list:
                        if isinstance(sub_result, list):
                            sub_result.insert(0, i)
                            result.append(sub_result)
                        else:
                            result.append([i, sub_result])
        return result

    @staticmethod
    def get_value_by_coordinate(arrays, coordinate):
        result = arrays
        for i in range(0, len(coordinate)):
            result = result[coordinate[i]]
        return result

    @staticmethod
    def remove_duplicate_item(array):
        result = list(set(array))
        result.sort(key=array.index)
        return result

    # 将list拼接为字符串
    # 额外编写这个函数是因为有时数据的list中会嵌套list，这种情况下直接调用join会报错
    @staticmethod
    def join_array(array, spilt_mark=''):
        if not isinstance(array, list):
            return array
        for i in range(0, len(array)):
            if isinstance(array[i], list):
                array[i] = ArrayHelper.join_array(array[i])
        return spilt_mark.join(array)


class StringHelper:
    @staticmethod
    # 用于去除某些的字符
    def filter_illegal_char(string):
        if not isinstance(string, str):
            return string
        result = string.replace("\n", "")
        result = result.replace("\t", "")
        result = result.replace("\r", "")
        result = result.replace(u"\xa0", "")
        result = result.replace(u"\u3000", "")
        result = result.replace(u" ", "")
        return result

    @staticmethod
    # 用于确定某个字符串是否符合正则表达式
    def match_string(string, regex):
        # return re.match(regex, string) is not None or re.search(regex, string) is not None
        if string is None or regex is None or len(regex) > len(string):
            return False
        start = 0
        fail = True
        for i in range(0, len(regex)):
            for j in range(start, len(string)):
                if regex[i] == string[j]:
                    start = start + 1
                    fail = False
                    break
            if fail:
                return False
            fail = True
        return True

    @staticmethod
    # 生成随机字符串
    def random_str(length=8):
        return ''.join(random.sample(string.ascii_letters + string.digits, length))


class ClassHelper:
    @staticmethod
    def get_all_classes(super_class):
        """
        获取父类的所有子类
        """
        all_subclasses = {}
        for subclass in super_class.__subclasses__():
            if (subclass.__name__ not in all_subclasses.keys()) and (not inspect.isabstract(subclass)):
                all_subclasses[subclass.__name__] = subclass
        return all_subclasses
