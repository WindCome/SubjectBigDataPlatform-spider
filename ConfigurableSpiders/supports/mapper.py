import abc
import re
from collections import Iterable, Iterator

from ConfigurableSpiders.supports.tools import ArrayHelper, StringHelper, ClassHelper


class DataMapper:
    def __init__(self):
        self.mapper = None
        self.attribute_setting = {}
        self.select_setting = {}
        self.regex_setting = {}
        self.structuring_data = None
        self.original_data = None

    def init_setting_by_xml(self, table_node):
        children = list(table_node)
        select_str = "select"
        regex_str = "regex"
        attribute_str = "attri"
        for child in children:
            target_attribute = child.text
            attributes = child.attrib
            if select_str in attributes.keys():
                attribute_value = attributes[select_str]
                self.select_setting[attribute_value] = target_attribute
                if regex_str in attributes.keys():
                    self.regex_setting[attribute_value] = attributes[regex_str]
            elif attribute_str in attributes.keys():
                self.attribute_setting[attributes[attribute_str]] = target_attribute

    def init_setting_by_dir(self, setting_dir):
        for key in setting_dir:
            if key.startswith('@'):
                self.select_setting[key] = setting_dir[key]
            else:
                self.attribute_setting[key] = setting_dir[key]

    def init_regex(self, regex_dir):
        self.regex_setting = regex_dir

    def set_structuring_data(self, structuring_data):
        self.structuring_data = structuring_data
        return self.__init_mapper(structuring_data)

    def set_original_data(self, original_data):
        self.original_data = original_data

    def __iter__(self):
        data_mapper = self

        class RecordIterator(Iterator):
            def __next__(self):
                if data_mapper.mapper.has_next_record():
                    if data_mapper.select_setting is None or not data_mapper:
                        return data_mapper.mapper.next_record()
                    else:
                        record = data_mapper.mapper.next_record()
                        for key in data_mapper.select_setting.keys():
                            if data_mapper.select_setting[key] not in record.keys() \
                                    or record[data_mapper.select_setting[key]] is None:
                                record[data_mapper.select_setting[key]] = data_mapper.get_select_value(key)
                        return record
                else:
                    raise StopIteration

        return RecordIterator()

    def get_select_value(self, select_key):
        value = None
        if select_key.startswith('@h'):
            # 匹配表头
            header_result = re.search('(?<=@h)[0-9]*', select_key)
            if header_result is not None:
                header_index = int(header_result.group())
                value = self.mapper.get_original_attribute(header_index)

        elif self.original_data is not None:
            search_result = re.search(select_key, self.original_data)
            if search_result is not None:
                value = search_result.group()
            else:
                print("原数据无法匹配正则表达式:" + select_key)
                value = None
        # 正则表达式处理
        if value is not None and select_key in self.regex_setting.keys():
            regex_str = self.regex_setting[select_key]
            search_result = re.search(regex_str, value)
            if search_result is not None:
                value = search_result.group()
            else:
                print(value + "无法匹配正则表达式:" + regex_str)
                value = None
        return value

    def get_description_of_current_table(self):
        return self.mapper.get_description_of_current_table()

    def __init_mapper(self, structuring_data):
        if self.mapper is not None:
            return
        subclass_abstract_data_mapper = ClassHelper.get_all_classes(AbstractDataMapper)
        for class_name in subclass_abstract_data_mapper:
            data_mapper = subclass_abstract_data_mapper[class_name](self.attribute_setting)
            if data_mapper.is_data_accepted(structuring_data):
                self.mapper = data_mapper
                return class_name
        raise RuntimeError("找不到合适的数据映射器")


class AbstractDataMapper(Iterable, metaclass=abc.ABCMeta):
    def __init__(self, attributes_map):
        self.attributes_map = attributes_map  # 列属性映射{"源属性":["目的属性"]}

    # 判断该映射器能否支持映射指定的结构化数据
    @abc.abstractmethod
    def is_data_accepted(self, structuring_data):
        pass

    # 记录迭代
    @abc.abstractmethod
    def __iter__(self):
        pass

    # 获取属性
    @abc.abstractmethod
    def get_original_attribute(self, index):
        pass

    # 获取当前表格的描述信息
    @abc.abstractmethod
    def get_description_of_current_table(self):
        pass


class ColDataMapper(AbstractDataMapper):
    # 列映射器

    def __init__(self, attributes_map):
        super().__init__(attributes_map)
        self.attributes_row_coordinates = None
        self.data = None
        self.current_attribute_row_index = 0  # 当前属性行在attributes_row_coordinates中的下标
        self.current_data_row_index = None
        self.original_attributes = None
        self.index_to_attribute = None  # 下标与目的属性的映射
        self.last_data_row_coordinate = None

    # 获取目的属性
    def get_target_attributes(self):
        result = []
        for value in self.attributes_map.values():
            if isinstance(value, list):
                result.extend(value)
            else:
                result.append(value)
        return ArrayHelper.remove_duplicate_item(result)

    def is_data_accepted(self, structuring_data):
        # 计算所有属性行的位置
        self.attributes_row_coordinates = self.__find_attribute_row_coordinate(structuring_data)
        if self.attributes_row_coordinates is None or len(self.attributes_row_coordinates) == 0:
            return False
        self.data = structuring_data
        return True

    def __iter__(self):
        col_data_mapper = self

        class ColRecordIterator(Iterator):
            def __next__(self):
                if not col_data_mapper.has_next_record():
                    raise StopIteration
                else:
                    return col_data_mapper.next_record()

        return ColRecordIterator()

    def get_original_attribute(self, index):
        if index < len(self.original_attributes):
            return self.original_attributes[index]
        return None

    # 找到属性行的位置
    def __find_attribute_row_coordinate(self, structuring_data):
        attributes_row_index_coordinate = {}
        for key in self.attributes_map:
            coordinates = ArrayHelper.locate_value(structuring_data, key)
            for coordinate in coordinates:
                x = tuple(coordinate[0:len(coordinate) - 1])
                if x in attributes_row_index_coordinate.keys():
                    counter = attributes_row_index_coordinate[x]
                    attributes_row_index_coordinate[x] = counter + 1
                else:
                    attributes_row_index_coordinate[x] = 1

        attributes_row_coordinate = []
        number_of_target_attributes = len(self.get_target_attributes())
        for key in attributes_row_index_coordinate:
            if attributes_row_index_coordinate[key] >= min(number_of_target_attributes, 3):
                attributes_row_coordinate.append(list(key))
        if len(attributes_row_coordinate) == 0:
            print("找不到符合的属性行")
            if len(attributes_row_coordinate) != 0:
                print("所有潜在属性行如下:")
                for a in attributes_row_coordinate:
                    print(ArrayHelper.get_value_by_coordinate(structuring_data, a))
        return attributes_row_coordinate

    def __get_current_attribute_row_coordinate(self):
        if self.current_attribute_row_index < len(self.attributes_row_coordinates):
            return self.attributes_row_coordinates[self.current_attribute_row_index]
        return None

    def __get_current_table_coordinate(self):
        current_attributes_row_coordinate = self.__get_current_attribute_row_coordinate()
        if current_attributes_row_coordinate is None:
            return None
        return current_attributes_row_coordinate[0:len(current_attributes_row_coordinate) - 1]

    def __get_current_data_row_coordinate(self):
        current_table_coordinate = self.__get_current_table_coordinate()
        if current_table_coordinate is None:
            return None
        current_table_coordinate = list(current_table_coordinate)
        current_table_coordinate.append(self.current_data_row_index)
        return current_table_coordinate

    def __get_first_data_row_index_in_current_table(self):
        table_coordinate = self.__get_current_table_coordinate()
        if table_coordinate is None:
            return None
        current_attribute_row_coordinate = self.__get_current_attribute_row_coordinate()
        attribute_row_index = current_attribute_row_coordinate[len(current_attribute_row_coordinate) - 1]
        data_row_index = attribute_row_index + 1
        if not self.__is_data_row_valid(data_row_index):
            return None
        return data_row_index

    def __is_data_row_valid(self, data_row_index):
        table_coordinate = self.__get_current_table_coordinate()
        table = ArrayHelper.get_value_by_coordinate(self.data, table_coordinate)
        if data_row_index >= len(table):
            return False
        data_row_coordinate = list(table_coordinate)
        data_row_coordinate.append(data_row_index)
        data_row = ArrayHelper.get_value_by_coordinate(self.data, data_row_coordinate)
        attribute_row_coordinate = self.__get_current_attribute_row_coordinate()
        attribute_row = ArrayHelper.get_value_by_coordinate(self.data, attribute_row_coordinate)
        if len(attribute_row) != len(data_row) or data_row_coordinate in self.attributes_row_coordinates:
            return False
        return True

    def has_next_record(self):
        if self.current_data_row_index is not None:
            last_data_row_coordinate = self.__get_current_data_row_coordinate()
            self.current_data_row_index = self.current_data_row_index + 1
            if not self.__is_data_row_valid(self.current_data_row_index):
                self.last_data_row_coordinate = last_data_row_coordinate
                self.current_attribute_row_index = self.current_attribute_row_index + 1
                self.current_data_row_index = None
            else:
                return True

        while self.current_data_row_index is None \
                and self.current_attribute_row_index < len(self.attributes_row_coordinates):
            self.current_data_row_index = self.__get_first_data_row_index_in_current_table()
            if self.current_data_row_index is None:
                self.current_attribute_row_index = self.current_attribute_row_index + 1

        if self.current_data_row_index is None or \
                self.current_attribute_row_index >= len(self.attributes_row_coordinates):
            return False
        return True

    def next_record(self):
        data_row_coordinate = self.__get_current_data_row_coordinate()
        if data_row_coordinate is None:
            return None
        data_row = ArrayHelper.get_value_by_coordinate(self.data, data_row_coordinate)
        result = {}
        index_to_attribute = self.__get_index_to_attribute()
        for j in range(0, len(data_row)):
            data_cell = data_row[j]
            if isinstance(data_cell, list):
                data_cell = ArrayHelper.join_array(data_cell)
            if j in index_to_attribute.keys():
                result[index_to_attribute[j]] = data_cell
        target_attribute = self.get_target_attributes()
        for attribute in target_attribute:
            if attribute not in result.keys():
                result[attribute] = ''
        return result

    def __get_index_to_attribute(self):
        if self.index_to_attribute is not None \
                and self.current_attribute_row_index == self.index_to_attribute_for_row_index:
            return self.index_to_attribute
        attribute_row = ArrayHelper.get_value_by_coordinate(self.data, self.__get_current_attribute_row_coordinate())
        index_to_attribute = {}
        self.original_attributes = []
        for i in range(0, len(attribute_row)):
            original_attribute = attribute_row[i]
            target_attribute = None
            if isinstance(original_attribute, list):
                original_attribute = ''.join(original_attribute)
            for key in self.attributes_map.keys():
                if StringHelper.match_string(original_attribute, key):
                    target_attribute = self.attributes_map[key]
            if target_attribute is not None:
                index_to_attribute[i] = target_attribute
                self.original_attributes.append(original_attribute)
        self.index_to_attribute = index_to_attribute
        self.index_to_attribute_for_row_index = self.current_attribute_row_index
        return self.index_to_attribute

    def get_description_of_current_table(self):
        result = ''
        table_coordinate = self.__get_current_table_coordinate()
        table = ArrayHelper.get_value_by_coordinate(self.data, table_coordinate)
        start_index = 0
        current_attribute_coordinate = self.attributes_row_coordinates[self.current_attribute_row_index]
        end_index = current_attribute_coordinate[len(current_attribute_coordinate)-1]
        if self.last_data_row_coordinate is not None:
            len1 = len(self.last_data_row_coordinate)
            last_table_coordinate = self.last_data_row_coordinate[0: len1 - 1]
            current_table_coordinate = self.__get_current_table_coordinate()
            import operator
            if operator.eq(last_table_coordinate, current_table_coordinate):
                last_data_row_index = self.last_data_row_coordinate[len1 - 1]
                start_index = last_data_row_index + 1
        for i in range(start_index, end_index):
            result = result + ArrayHelper.join_array(table[i])
        return result


class RowDataMapper(AbstractDataMapper):
    # 行映射器
    def is_data_accepted(self, structuring_data):
        pass

    def __iter__(self):
        pass

    def get_title(self, index):
        pass

    def get_original_attribute(self, index):
        pass

    def get_description_of_current_table(self):
        pass
